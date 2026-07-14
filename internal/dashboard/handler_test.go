package dashboard

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
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
	if result.Media[0]["id"] != float64(8) || result.Media[0]["status"] != "partial" || result.Files[0]["path"] != "Movies/Example.mkv" {
		t.Fatalf("unexpected library JSON: %#v", result)
	}
}

func TestCreateRequestQueuesDirectlyWithoutPostingToSeerr(t *testing.T) {
	var methods []string
	catalog := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		methods = append(methods, r.Method+" "+r.URL.Path)
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/tv/99" {
			http.Error(w, "unexpected", http.StatusBadRequest)
			return
		}
		_, _ = w.Write([]byte(`{"id":99,"name":"Direct Show","overview":"A direct request.","firstAirDate":"2025-01-02","posterPath":"/poster.jpg","externalIds":{"imdbId":"tt123"},"seasons":[{"seasonNumber":1,"name":"Season 1","airDate":"2025-01-02","episodeCount":8},{"seasonNumber":2,"name":"Season 2","airDate":"2025-06-01","episodeCount":6}]}`))
	}))
	defer catalog.Close()
	st, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{SeerrURL: catalog.URL, SeerrAPIKey: "key", Qualities: []string{"1080p"}, Providers: []string{"torbox"}, MaxResults: 1, ResolveTimeout: time.Second}
	resolver := &service.Resolver{Config: cfg, Store: st, Scraper: failingCatalogSearcher{}}
	seerr := &service.Seerr{Config: cfg, Store: st, Resolver: resolver, Client: catalog.Client()}
	handler := (&Handler{Store: st, Resolver: resolver, Seerr: seerr, Username: "admin", Password: "secret"}).Routes()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/requests", strings.NewReader(`{"mediaType":"tv","mediaId":99,"seasons":[1,2],"is4k":false}`))
	req.SetBasicAuth("admin", "secret")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	if response.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", response.Code, response.Body.String())
	}
	media, ok := st.FindMediaByTMDB("tv", 99)
	if !ok || media.Title != "Direct Show" || len(media.Seasons) != 2 || media.RequestID != 0 {
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
	if len(methods) != 1 || methods[0] != "GET /api/v1/tv/99" {
		t.Fatalf("request unexpectedly posted to Seerr: %#v", methods)
	}
}
