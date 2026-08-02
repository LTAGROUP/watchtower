package dashboard

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LTAGROUP/watchtower/internal/config"
	"github.com/LTAGROUP/watchtower/internal/logging"
	"github.com/LTAGROUP/watchtower/internal/model"
	"github.com/LTAGROUP/watchtower/internal/scraper"
	"github.com/LTAGROUP/watchtower/internal/service"
	"github.com/LTAGROUP/watchtower/internal/store"
)

type failingCatalogSearcher struct{}

func (failingCatalogSearcher) Search(context.Context, scraper.Query, int) ([]model.Release, error) {
	return nil, errors.New("stop after direct request persistence")
}

type dormantSearcher struct{ calls atomic.Int32 }

func (s *dormantSearcher) Search(context.Context, scraper.Query, int) ([]model.Release, error) {
	s.calls.Add(1)
	return nil, errors.New("scheduler should be the only resolver launcher")
}

func TestLogsReturnsBufferedEntries(t *testing.T) {
	logs := logging.NewBuffer(25)
	slog.New(logs.Handler(slog.LevelDebug)).Warn("provider unavailable", "component", "resolver", "provider", "torbox")
	handler := (&Handler{Logs: logs, Username: "admin", Password: "secret"}).Routes()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/logs", nil)
	req.SetBasicAuth("admin", "secret")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, req)

	if response.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", response.Code)
	}
	var result struct {
		Entries  []logging.Entry `json:"entries"`
		Capacity int             `json:"capacity"`
	}
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if result.Capacity != 25 || len(result.Entries) != 1 || result.Entries[0].Component != "resolver" || result.Entries[0].Fields["provider"] != "torbox" {
		t.Fatalf("unexpected log response: %#v", result)
	}
}

func TestDashboardRequiresBasicAuthAndReturnsSummary(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertMedia(&model.Media{ID: 1, Title: "Ready", Status: "ready", ScrapedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if err := st.AddFiles(&model.File{ID: "f", MediaID: 1, Path: "Movies/Ready.mkv", Size: 1024}); err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{SettingsFile: filepath.Join(dir, "settings.json"), Providers: []string{"torbox"}, Qualities: []string{"1080p"}, StremioAddons: []string{"x|http://x/manifest.json"}, PollInterval: time.Minute, ResolveTimeout: time.Minute, StreamURLTTL: time.Minute, MaxResults: 20}
	settings, err := config.OpenManager(cfg)
	if err != nil {
		t.Fatal(err)
	}
	handler := (&Handler{Store: st, Settings: settings, Username: "admin", Password: "secret"}).Routes()

	unauthorized := httptest.NewRecorder()
	handler.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, "/api/v1/summary", nil))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", unauthorized.Code)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/summary", nil)
	req.SetBasicAuth("admin", "secret")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	if response.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", response.Code)
	}
	var result struct {
		Indexed, Scraped, Files int
		Bytes                   int64
		Statuses                map[string]int
	}
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if result.Indexed != 1 || result.Scraped != 1 || result.Files != 1 || result.Bytes != 1024 || result.Statuses["ready"] != 1 {
		t.Fatalf("unexpected summary: %#v", result)
	}
}

func TestLibraryUsesFrontendJSONFieldNames(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertMedia(&model.Media{ID: 8, RequestID: 9, Title: "Example", Status: "partial", UpdatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if err := st.AddFiles(&model.File{ID: "file", MediaID: 8, Path: "Movies/Example.mkv", Quality: "1080p", Provider: "torbox"}); err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{SettingsFile: filepath.Join(dir, "settings.json"), Providers: []string{"torbox"}, Qualities: []string{"1080p"}, StremioAddons: []string{"x|http://x/manifest.json"}, PollInterval: time.Minute, ResolveTimeout: time.Minute, StreamURLTTL: time.Minute, MaxResults: 20}
	settings, err := config.OpenManager(cfg)
	if err != nil {
		t.Fatal(err)
	}
	handler := (&Handler{Store: st, Settings: settings, Username: "admin", Password: "secret"}).Routes()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/library", nil)
	req.SetBasicAuth("admin", "secret")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	var result struct {
		Media []map[string]any `json:"media"`
		Files []map[string]any `json:"files"`
	}
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if result.Media[0]["id"] != float64(8) || result.Media[0]["status"] != "ready" || result.Files[0]["path"] != "Movies/Example.mkv" {
		t.Fatalf("unexpected library JSON: %#v", result)
	}
	availability, ok := result.Media[0]["qualityAvailability"].([]any)
	if !ok || len(availability) != 1 {
		t.Fatalf("unexpected quality availability: %#v", result.Media[0]["qualityAvailability"])
	}
}

func TestCreateRequestQueuesDirectlyWithoutPostingToSeerr(t *testing.T) {
	var methods []string
	catalog := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		methods = append(methods, r.Method+" "+r.URL.Path)
		if r.Method != http.MethodGet {
			http.Error(w, "unexpected", http.StatusBadRequest)
			return
		}
		switch r.URL.Path {
		case "/api/v1/tv/99":
			_, _ = w.Write([]byte(`{"id":99,"name":"Direct Show","overview":"A direct request.","firstAirDate":"2025-01-02","posterPath":"/poster.jpg","externalIds":{"imdbId":"tt123"},"seasons":[{"seasonNumber":1,"name":"Season 1","airDate":"2025-01-02","episodeCount":8},{"seasonNumber":2,"name":"Season 2","airDate":"2025-06-01","episodeCount":6}]}`))
		case "/api/v1/tv/99/season/1":
			_, _ = w.Write([]byte(`{"seasonNumber":1,"episodes":[{"episodeNumber":1,"airDate":"2025-01-02"}]}`))
		case "/api/v1/tv/99/season/2":
			_, _ = w.Write([]byte(`{"seasonNumber":2,"episodes":[{"episodeNumber":1,"airDate":"2025-06-01"}]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer catalog.Close()
	st, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{SeerrURL: catalog.URL, SeerrAPIKey: "key", Qualities: []string{"1080p"}, Providers: []string{"torbox"}, MaxResults: 1, ResolveTimeout: time.Second}
	resolver := &service.Resolver{Config: cfg, Store: st, Scraper: failingCatalogSearcher{}}
	seerr := &service.Seerr{Config: cfg, Store: st, Resolver: resolver, Client: catalog.Client()}
	lifecycle := runLifecycle(t, &service.Lifecycle{Store: st, Resolver: resolver, Interval: time.Millisecond})
	handler := (&Handler{Store: st, Resolver: resolver, Seerr: seerr, Scheduler: lifecycle, Username: "admin", Password: "secret"}).Routes()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/requests", strings.NewReader(`{"mediaType":"tv","mediaId":99,"seasons":[1,2],"is4k":false}`))
	req.SetBasicAuth("admin", "secret")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	if response.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", response.Code, response.Body.String())
	}
	media, ok := st.FindMediaByTMDB("tv", 99)
	if !ok || media.Title != "Direct Show" || len(media.Seasons) != 2 || media.EpisodeCounts[1] != 8 || media.EpisodeCounts[2] != 6 || media.RequestID != 0 {
		t.Fatalf("direct request was not persisted correctly: %#v", media)
	}
	deadline := time.Now().Add(time.Second)
	for media.Status != "failed" && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
		media, _ = st.FindMediaByTMDB("tv", 99)
	}
	if media.Status != "failed" {
		t.Fatalf("background resolver did not finish: %#v", media)
	}
	for _, method := range methods {
		if !strings.HasPrefix(method, "GET ") {
			t.Fatalf("request unexpectedly posted to Seerr: %#v", methods)
		}
	}
}

func TestCatalogRefreshCommitsEpisodeScheduleAsAPair(t *testing.T) {
	for _, test := range []struct {
		name          string
		scheduleFails bool
		wantCount     int
		wantDates     int
	}{
		{name: "successful schedule", wantCount: 2, wantDates: 2},
		{name: "failed schedule preserves prior pair", scheduleFails: true, wantCount: 1, wantDates: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			catalog := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/api/v1/tv/99":
					_, _ = w.Write([]byte(`{"id":99,"name":"Refreshed","overview":"New overview","posterPath":"/poster.jpg","backdropPath":"/backdrop.jpg","seasons":[{"seasonNumber":1,"episodeCount":2}]}`))
				case "/api/v1/tv/99/season/1":
					if test.scheduleFails {
						http.Error(w, "temporarily unavailable", http.StatusBadGateway)
						return
					}
					_, _ = w.Write([]byte(`{"seasonNumber":1,"episodes":[{"episodeNumber":1,"airDate":"2026-01-01"},{"episodeNumber":2,"airDate":"2026-01-08"}]}`))
				default:
					http.NotFound(w, r)
				}
			}))
			defer catalog.Close()
			state, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
			if err != nil {
				t.Fatal(err)
			}
			if err := state.UpsertMedia(&model.Media{
				ID: 99, Type: "tv", TMDBID: 99, Title: "Existing", Seasons: []int{1},
				EpisodeCounts: map[int]int{1: 1}, EpisodeAirDates: []model.EpisodeAirDate{{Season: 1, Episode: 1, AirDate: "2025-01-01"}},
				Status: "partial",
			}); err != nil {
				t.Fatal(err)
			}
			cfg := config.Config{SeerrURL: catalog.URL, SeerrAPIKey: "key"}
			seerr := &service.Seerr{Config: cfg, Store: state, Client: catalog.Client()}
			handler := (&Handler{Store: state, Seerr: seerr, Username: "admin", Password: "secret"}).Routes()
			request := httptest.NewRequest(http.MethodGet, "/api/v1/catalog/tv/99", nil)
			request.SetBasicAuth("admin", "secret")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusOK {
				t.Fatalf("catalog returned %d: %s", response.Code, response.Body.String())
			}
			refreshed, ok := state.MediaByID(99)
			if !ok || refreshed.EpisodeCounts[1] != test.wantCount || len(refreshed.EpisodeAirDates) != test.wantDates {
				t.Fatalf("catalog schedule pair was not preserved: %#v", refreshed)
			}
			if test.scheduleFails && refreshed.EpisodeAirDates[0].AirDate != "2025-01-01" {
				t.Fatalf("failed schedule partially replaced existing dates: %#v", refreshed.EpisodeAirDates)
			}
		})
	}
}

func TestCatalogRefreshDoesNotOverwriteConcurrentLifecycleState(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	catalog := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/tv/444":
			started <- struct{}{}
			<-release
			_, _ = w.Write([]byte(`{"id":444,"name":"Refreshed","overview":"Fresh overview","posterPath":"/fresh-poster.jpg","backdropPath":"/fresh-backdrop.jpg","seasons":[{"seasonNumber":1,"episodeCount":2}]}`))
		case "/api/v1/tv/444/season/1":
			_, _ = w.Write([]byte(`{"seasonNumber":1,"episodes":[{"episodeNumber":1,"airDate":"2026-01-01"},{"episodeNumber":2,"airDate":"2026-01-08"}]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer catalog.Close()
	defer unblock()
	state, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := state.UpsertMedia(&model.Media{
		ID: 44, Type: "tv", TMDBID: 444, Title: "Existing", Seasons: []int{1},
		EpisodeCounts: map[int]int{1: 1}, EpisodeAirDates: []model.EpisodeAirDate{{Season: 1, Episode: 1, AirDate: "2025-01-01"}},
		Status: "queued", Work: model.MediaWork{Mode: "resolve", Generation: 1},
	}); err != nil {
		t.Fatal(err)
	}
	seerr := &service.Seerr{Config: config.Config{SeerrURL: catalog.URL, SeerrAPIKey: "key"}, Store: state, Client: catalog.Client()}
	handler := (&Handler{Store: state, Seerr: seerr, Username: "admin", Password: "secret"}).Routes()
	request := httptest.NewRequest(http.MethodGet, "/api/v1/catalog/tv/444", nil)
	request.SetBasicAuth("admin", "secret")
	response := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(response, request)
		close(done)
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("catalog request did not reach the blocking external call")
	}
	want := installNewerLifecycleState(t, state, 44)
	unblock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("catalog request did not finish")
	}
	if response.Code != http.StatusOK {
		t.Fatalf("catalog returned %d: %s", response.Code, response.Body.String())
	}
	got, ok := state.MediaByID(44)
	if !ok {
		t.Fatal("catalog refresh removed media")
	}
	if got.Overview != "Fresh overview" || got.PosterPath != "/fresh-poster.jpg" || got.BackdropPath != "/fresh-backdrop.jpg" || got.EpisodeCounts[1] != 2 || len(got.EpisodeAirDates) != 2 {
		t.Fatalf("catalog metadata was not refreshed: %#v", got)
	}
	assertLifecycleStateUnchanged(t, got, want)
}

func TestPosterRefreshDoesNotOverwriteConcurrentLifecycleState(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	catalog := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/movie/445" {
			http.NotFound(w, r)
			return
		}
		started <- struct{}{}
		<-release
		_, _ = w.Write([]byte(`{"id":445,"title":"Refreshed","overview":"Remote overview","posterPath":"/fresh-poster.jpg","backdropPath":"/remote-backdrop.jpg"}`))
	}))
	defer catalog.Close()
	defer unblock()
	state, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := state.UpsertMedia(&model.Media{
		ID: 45, Type: "movie", TMDBID: 445, Title: "Existing", Overview: "Keep overview", BackdropPath: "/keep-backdrop.jpg",
		Status: "queued", Work: model.MediaWork{Mode: "resolve", Generation: 1},
	}); err != nil {
		t.Fatal(err)
	}
	seerr := &service.Seerr{Config: config.Config{SeerrURL: catalog.URL, SeerrAPIKey: "key"}, Store: state, Client: catalog.Client()}
	handler := (&Handler{Store: state, Seerr: seerr, Username: "admin", Password: "secret"}).Routes()
	request := httptest.NewRequest(http.MethodGet, "/api/v1/media/45/poster", nil)
	request.SetBasicAuth("admin", "secret")
	response := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(response, request)
		close(done)
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("poster request did not reach the blocking external call")
	}
	want := installNewerLifecycleState(t, state, 45)
	unblock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("poster request did not finish")
	}
	if response.Code != http.StatusFound || response.Header().Get("Location") != "https://image.tmdb.org/t/p/w500/fresh-poster.jpg" {
		t.Fatalf("poster fallback returned %d with location %q", response.Code, response.Header().Get("Location"))
	}
	got, ok := state.MediaByID(45)
	if !ok {
		t.Fatal("poster refresh removed media")
	}
	if got.PosterPath != "/fresh-poster.jpg" || got.Overview != "Keep overview" || got.BackdropPath != "/keep-backdrop.jpg" {
		t.Fatalf("poster refresh changed non-poster metadata: %#v", got)
	}
	assertLifecycleStateUnchanged(t, got, want)
}

func installNewerLifecycleState(t *testing.T, state *store.Store, id int64) *model.Media {
	t.Helper()
	now := time.Date(2026, time.August, 2, 12, 0, 0, 0, time.UTC)
	updated, err := state.UpdateMedia(id, func(media *model.Media) error {
		media.RequestID = 902
		media.RequestIDs = []int64{901, 902}
		media.SeerrMediaID = 9902
		media.Status = "resolving"
		media.Error = "newer resolver owns this work"
		media.ScrapedAt = now
		media.UpdatedAt = now
		media.Work = model.MediaWork{Mode: "resolve", Generation: 9, Attempts: 4, NextAt: now.Add(10 * time.Minute), LeaseUntil: now.Add(20 * time.Minute)}
		media.PlexIntent = model.DurableIntent{Generation: 7, CompletedGeneration: 6, Attempts: 2, NextAt: now.Add(30 * time.Minute), LeaseUntil: now.Add(40 * time.Minute), LeaseGeneration: 7}
		media.AvailabilityIntent = model.DurableIntent{Generation: 5, CompletedGeneration: 4, Attempts: 3, NextAt: now.Add(50 * time.Minute), LeaseUntil: now.Add(60 * time.Minute), LeaseGeneration: 5}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return updated
}

func assertLifecycleStateUnchanged(t *testing.T, got, want *model.Media) {
	t.Helper()
	if got == nil || want == nil {
		t.Fatalf("cannot compare lifecycle state: got=%#v want=%#v", got, want)
	}
	if got.RequestID != want.RequestID || !reflect.DeepEqual(got.RequestIDs, want.RequestIDs) || got.SeerrMediaID != want.SeerrMediaID || got.Status != want.Status || got.Error != want.Error || got.ScrapedAt != want.ScrapedAt || got.UpdatedAt != want.UpdatedAt || got.Work != want.Work || got.PlexIntent != want.PlexIntent || got.AvailabilityIntent != want.AvailabilityIntent {
		t.Fatalf("metadata GET overwrote newer lifecycle state: got=%#v want=%#v", got, want)
	}
}

func TestRerequestEpisodeEndpointQueuesTargetAndKeepsExistingFileOnFailure(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	media := &model.Media{ID: 21, Type: "tv", Title: "Example", Seasons: []int{1}, EpisodeCounts: map[int]int{1: 2}, Status: "ready"}
	if err := st.UpsertMedia(media); err != nil {
		t.Fatal(err)
	}
	existing := &model.File{ID: "existing", MediaID: media.ID, Path: "TV/Example/Season 01/Example - S01E02 [1080p].mkv", Quality: "1080p"}
	if err := st.AddFiles(existing); err != nil {
		t.Fatal(err)
	}
	resolver := &service.Resolver{
		Config: config.Config{Qualities: []string{"1080p"}, MaxResults: 20, ResolveTimeout: time.Second},
		Store:  st, Scraper: failingCatalogSearcher{},
	}
	lifecycle := runLifecycle(t, &service.Lifecycle{Store: st, Resolver: resolver, Interval: time.Millisecond})
	handler := (&Handler{Store: st, Resolver: resolver, Scheduler: lifecycle, Username: "admin", Password: "secret"}).Routes()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/media/21/rerequest", strings.NewReader(`{"season":1,"episode":2}`))
	req.SetBasicAuth("admin", "secret")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	if response.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", response.Code, response.Body.String())
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		updated, _ := st.MediaByID(media.ID)
		if updated.Status == "failed" {
			break
		}
		time.Sleep(time.Millisecond)
	}
	updated, _ := st.MediaByID(media.ID)
	if updated.Status != "failed" {
		t.Fatalf("background re-request did not finish: %#v", updated)
	}
	if _, ok := st.File(existing.ID); !ok {
		t.Fatal("failed re-request removed the existing episode")
	}
}

func TestCreateRequestOnlyPersistsAndWakesScheduler(t *testing.T) {
	catalog := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/movie/77" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(`{"id":77,"title":"Queued","releaseDate":"2025-01-02","externalIds":{"imdbId":"tt77"}}`))
	}))
	defer catalog.Close()
	state, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	searcher := &dormantSearcher{}
	resolver := &service.Resolver{Config: config.Config{Qualities: []string{"1080p"}, Providers: []string{"torbox"}, MaxResults: 1, ResolveTimeout: time.Second}, Store: state, Scraper: searcher}
	seerr := &service.Seerr{Config: config.Config{SeerrURL: catalog.URL, SeerrAPIKey: "key"}, Store: state, Resolver: resolver, Client: catalog.Client()}
	handler := (&Handler{Store: state, Resolver: resolver, Seerr: seerr, Username: "admin", Password: "secret"}).Routes()
	request := httptest.NewRequest(http.MethodPost, "/api/v1/requests", strings.NewReader(`{"mediaType":"movie","mediaId":77}`))
	request.SetBasicAuth("admin", "secret")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("create returned %d: %s", response.Code, response.Body.String())
	}
	media, ok := state.FindMediaByTMDB("movie", 77)
	if !ok || media.Work.Mode != "resolve" || !media.Work.LeaseUntil.IsZero() {
		t.Fatalf("create did not persist durable work: %#v", media)
	}
	// Without a running Lifecycle this request must not create a detached
	// context.Background resolver job.
	time.Sleep(25 * time.Millisecond)
	if searcher.calls.Load() != 0 {
		t.Fatalf("dashboard spawned resolver work instead of waking scheduler: calls=%d", searcher.calls.Load())
	}
}

func runLifecycle(t *testing.T, lifecycle *service.Lifecycle) *service.Lifecycle {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		lifecycle.Run(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("lifecycle did not stop")
		}
	})
	return lifecycle
}

func TestResetEndpointQueuesCurrentFinishStateWithoutStaleOverwrite(t *testing.T) {
	state, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	finishedAt := time.Date(2026, time.August, 2, 15, 4, 5, 0, time.UTC)
	if err := state.UpsertMedia(&model.Media{
		ID: 51, RequestID: 501, RequestIDs: []int64{501}, SeerrMediaID: 1501,
		Type: "tv", TMDBID: 5051, ExternalID: "tt005051", Title: "Before finish", Year: 2025,
		Seasons: []int{1}, EpisodeCounts: map[int]int{1: 1}, ReleaseDate: "2025-01-01",
		Status: "resolving", Error: "old resolver state", Work: model.MediaWork{Mode: "resolve", Generation: 8},
	}); err != nil {
		t.Fatal(err)
	}
	resolver := &service.Resolver{Store: state}
	handler := (&Handler{Store: state, Resolver: resolver, Username: "admin", Password: "secret"}).Routes()

	finishStarted := make(chan struct{})
	releaseFinish := make(chan struct{})
	finishDone := make(chan error, 1)
	var releaseOnce sync.Once
	unblockFinish := func() { releaseOnce.Do(func() { close(releaseFinish) }) }
	defer unblockFinish()
	go func() {
		_, updateErr := state.UpdateMediaAtomic(51, func(current *model.Media, transaction *store.MediaTransaction) error {
			close(finishStarted)
			<-releaseFinish
			// This is the durable state a resolution completion publishes while
			// the dashboard's reset is waiting. A reset must derive from this
			// record rather than commit the stale state it observed before finish.
			current.RequestID = 502
			current.RequestIDs = []int64{501, 502}
			current.SeerrMediaID = 1502
			current.Type = "tv"
			current.TMDBID = 5051
			current.ExternalID = "tt005051-finished"
			current.Title = "Finished show"
			current.Year = 2026
			current.Overview = "Fresh completion metadata"
			current.PosterPath = "/finished-poster.jpg"
			current.BackdropPath = "/finished-backdrop.jpg"
			current.Seasons = []int{1, 2}
			current.EpisodeCounts = map[int]int{1: 8, 2: 6}
			current.EpisodeAirDates = []model.EpisodeAirDate{{Season: 1, Episode: 8, AirDate: "2026-07-01"}, {Season: 2, Episode: 1, AirDate: "2026-08-01"}}
			current.ReleaseDate = "2026-07-01"
			current.Status = "ready"
			current.Error = ""
			current.ScrapedAt = finishedAt
			current.UpdatedAt = finishedAt
			current.Work = model.MediaWork{}
			current.PlexIntent = model.DurableIntent{Generation: 7, CompletedGeneration: 6, Attempts: 2, NextAt: finishedAt.Add(time.Minute), LeaseUntil: finishedAt.Add(2 * time.Minute), LeaseGeneration: 7}
			current.AvailabilityIntent = model.DurableIntent{Generation: 5, CompletedGeneration: 4, Attempts: 3, NextAt: finishedAt.Add(3 * time.Minute), LeaseUntil: finishedAt.Add(4 * time.Minute), LeaseGeneration: 5}
			transaction.MarkProcessed(501, 502)
			return nil
		})
		finishDone <- updateErr
	}()
	select {
	case <-finishStarted:
	case <-time.After(time.Second):
		t.Fatal("resolution finish did not acquire the store transaction")
	}

	request := httptest.NewRequest(http.MethodPost, "/api/v1/media/51/reset", nil)
	request.SetBasicAuth("admin", "secret")
	response := httptest.NewRecorder()
	resetDone := make(chan struct{})
	go func() {
		handler.ServeHTTP(response, request)
		close(resetDone)
	}()

	// The reset starts while the completion owns the store transaction. Releasing
	// the completion first deterministically exercises the former stale
	// reset/Commit/Queue window.
	unblockFinish()
	select {
	case updateErr := <-finishDone:
		if updateErr != nil {
			t.Fatalf("finish update: %v", updateErr)
		}
	case <-time.After(time.Second):
		t.Fatal("resolution finish did not complete")
	}
	select {
	case <-resetDone:
	case <-time.After(time.Second):
		t.Fatal("reset request did not complete")
	}
	if response.Code != http.StatusAccepted {
		t.Fatalf("reset returned %d: %s", response.Code, response.Body.String())
	}

	got, ok := state.MediaByID(51)
	if !ok {
		t.Fatal("reset removed media")
	}
	if got.RequestID != 502 || !reflect.DeepEqual(got.RequestIDs, []int64{501, 502}) || got.SeerrMediaID != 1502 {
		t.Fatalf("reset restored stale request association: %#v", got)
	}
	if got.Type != "tv" || got.TMDBID != 5051 || got.ExternalID != "tt005051-finished" || got.Title != "Finished show" || got.Year != 2026 || got.Overview != "Fresh completion metadata" || got.PosterPath != "/finished-poster.jpg" || got.BackdropPath != "/finished-backdrop.jpg" || got.ReleaseDate != "2026-07-01" || !reflect.DeepEqual(got.Seasons, []int{1, 2}) || !reflect.DeepEqual(got.EpisodeCounts, map[int]int{1: 8, 2: 6}) || !reflect.DeepEqual(got.EpisodeAirDates, []model.EpisodeAirDate{{Season: 1, Episode: 8, AirDate: "2026-07-01"}, {Season: 2, Episode: 1, AirDate: "2026-08-01"}}) {
		t.Fatalf("reset restored stale catalog state: %#v", got)
	}
	if got.PlexIntent != (model.DurableIntent{Generation: 7, CompletedGeneration: 6, Attempts: 2, NextAt: finishedAt.Add(time.Minute), LeaseUntil: finishedAt.Add(2 * time.Minute), LeaseGeneration: 7}) || got.AvailabilityIntent != (model.DurableIntent{Generation: 5, CompletedGeneration: 4, Attempts: 3, NextAt: finishedAt.Add(3 * time.Minute), LeaseUntil: finishedAt.Add(4 * time.Minute), LeaseGeneration: 5}) {
		t.Fatalf("reset restored stale durable intents: %#v", got)
	}
	if got.Status != "queued" || got.Error != "" || !got.ScrapedAt.IsZero() || got.Work.Mode != "resolve" || got.Work.Generation != 1 || got.Work.Season != 0 || got.Work.Episode != 0 || got.Work.Attempts != 0 || !got.Work.NextAt.IsZero() || !got.Work.LeaseUntil.IsZero() {
		t.Fatalf("reset did not create a fresh resolve command: %#v", got)
	}
	if state.IsProcessed(501) || state.IsProcessed(502) {
		t.Fatal("reset did not atomically unmark every current request ID")
	}
}

func TestDeleteAndLifecycleCommandsDoNotResurrectOrDeleteQueuedMedia(t *testing.T) {
	tests := []struct {
		name     string
		workMode string
		queue    func(*service.Resolver, int64) (*model.Media, error)
		request  func(int64) *http.Request
	}{
		{
			name:     "reset",
			workMode: "resolve",
			queue: func(resolver *service.Resolver, id int64) (*model.Media, error) {
				return resolver.QueueReset(id)
			},
			request: func(id int64) *http.Request {
				return httptest.NewRequest(http.MethodPost, "/api/v1/media/"+strconv.FormatInt(id, 10)+"/reset", nil)
			},
		},
		{
			name:     "rerequest",
			workMode: "rerequest",
			queue: func(resolver *service.Resolver, id int64) (*model.Media, error) {
				return resolver.QueueExistingRerequest(id, 1, 2)
			},
			request: func(id int64) *http.Request {
				return httptest.NewRequest(http.MethodPost, "/api/v1/media/"+strconv.FormatInt(id, 10)+"/rerequest", strings.NewReader(`{"season":1,"episode":2}`))
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name+" wins before delete", func(t *testing.T) {
			state, resolver, handler, id := newLifecycleDeleteFixture(t)
			// This mirrors the stale inactive record the legacy dashboard delete
			// path used to hold while a lifecycle action queued fresh work.
			stale, ok := state.MediaByID(id)
			if !ok || stale.Work.Mode != "" || stale.Status != "ready" {
				t.Fatalf("fixture was not inactive: %#v", stale)
			}
			queued, err := test.queue(resolver, id)
			if err != nil {
				t.Fatalf("queue %s: %v", test.name, err)
			}
			if queued.Work.Mode != test.workMode {
				t.Fatalf("queued wrong work: %#v", queued)
			}

			deleteRequest := httptest.NewRequest(http.MethodDelete, "/api/v1/media/"+strconv.FormatInt(id, 10), nil)
			deleteRequest.SetBasicAuth("admin", "secret")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, deleteRequest)
			if response.Code != http.StatusConflict {
				t.Fatalf("delete after queued %s returned %d: %s", test.name, response.Code, response.Body.String())
			}
			stored, ok := state.MediaByID(id)
			if !ok || stored.Work.Mode != test.workMode {
				t.Fatalf("delete removed or overwrote queued %s: %#v", test.name, stored)
			}
		})

		t.Run("delete wins before "+test.name, func(t *testing.T) {
			state, _, handler, id := newLifecycleDeleteFixture(t)
			deleteRequest := httptest.NewRequest(http.MethodDelete, "/api/v1/media/"+strconv.FormatInt(id, 10), nil)
			deleteRequest.SetBasicAuth("admin", "secret")
			deleted := httptest.NewRecorder()
			handler.ServeHTTP(deleted, deleteRequest)
			if deleted.Code != http.StatusNoContent {
				t.Fatalf("delete returned %d: %s", deleted.Code, deleted.Body.String())
			}

			queueRequest := test.request(id)
			queueRequest.SetBasicAuth("admin", "secret")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, queueRequest)
			if response.Code != http.StatusNotFound {
				t.Fatalf("%s after delete returned %d: %s", test.name, response.Code, response.Body.String())
			}
			if _, ok := state.MediaByID(id); ok {
				t.Fatalf("%s recreated deleted media", test.name)
			}
		})
	}
}

func newLifecycleDeleteFixture(t *testing.T) (*store.Store, *service.Resolver, http.Handler, int64) {
	t.Helper()
	state, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	const id int64 = 61
	if err := state.UpsertMedia(&model.Media{
		ID: id, Type: "tv", TMDBID: 6061, Title: "Lifecycle race", Seasons: []int{1}, EpisodeCounts: map[int]int{1: 2},
		Status: "ready", RequestID: 601, RequestIDs: []int64{601}, SeerrMediaID: 1601,
	}); err != nil {
		t.Fatal(err)
	}
	if err := state.AddFiles(&model.File{ID: "lifecycle-file", MediaID: id, Path: "TV/Lifecycle race/Season 01/Lifecycle race - S01E02.mkv"}); err != nil {
		t.Fatal(err)
	}
	resolver := &service.Resolver{Store: state}
	handler := (&Handler{Store: state, Resolver: resolver, Username: "admin", Password: "secret"}).Routes()
	return state, resolver, handler, id
}
