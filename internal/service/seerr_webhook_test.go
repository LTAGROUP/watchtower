package service

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LTAGROUP/watchtower/internal/config"
	"github.com/LTAGROUP/watchtower/internal/model"
	"github.com/LTAGROUP/watchtower/internal/store"
)

func TestSeerrWebhookHandlerWakesWithoutAuthentication(t *testing.T) {
	s := &Seerr{}
	handler := s.WebhookHandler()

	accepted := httptest.NewRecorder()
	handler.ServeHTTP(accepted, httptest.NewRequest(http.MethodPost, "/webhooks/seerr", nil))
	if accepted.Code != http.StatusAccepted {
		t.Fatalf("webhook returned %d", accepted.Code)
	}
	select {
	case <-s.wakeChannel():
	default:
		t.Fatal("webhook did not wake the importer")
	}

	methodNotAllowed := httptest.NewRecorder()
	handler.ServeHTTP(methodNotAllowed, httptest.NewRequest(http.MethodGet, "/webhooks/seerr", nil))
	if methodNotAllowed.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET webhook returned %d", methodNotAllowed.Code)
	}
}

func TestSeerrWakeTriggersImmediatePoll(t *testing.T) {
	polls := make(chan struct{}, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/request" {
			http.NotFound(w, r)
			return
		}
		if r.Header.Get("X-Api-Key") != "key" {
			t.Error("poll did not include Seerr API key")
		}
		polls <- struct{}{}
		_, _ = io.WriteString(w, `{"results":[]}`)
	}))
	defer server.Close()
	state, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}

	s := &Seerr{
		Config: config.Config{SeerrURL: server.URL, SeerrAPIKey: "key", PollInterval: time.Hour},
		Store:  state,
		Client: server.Client(),
		Log:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.Run(ctx)
		close(done)
	}()

	waitForPoll := func() {
		t.Helper()
		select {
		case <-polls:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for Seerr poll")
		}
	}
	waitForPoll()
	s.Wake()
	waitForPoll()

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Seerr importer did not stop")
	}
}

func TestSeerrPollFetchesAllRequestPages(t *testing.T) {
	var skips []int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/request" {
			http.NotFound(w, r)
			return
		}
		skip, err := strconv.Atoi(r.URL.Query().Get("skip"))
		if err != nil {
			t.Fatalf("invalid skip: %v", err)
		}
		skips = append(skips, skip)
		if r.URL.Query().Get("sortDirection") != "asc" {
			t.Errorf("request list was not made stable with ascending IDs: %s", r.URL.RawQuery)
		}
		page := seerrPage{PageInfo: seerrPageInfo{Pages: 2, PageSize: 2, Page: 1}}
		if skip == 0 {
			page.Results = []seerrRequest{{ID: 1, Status: 1}, {ID: 2, Status: 1}}
		} else {
			page.PageInfo.Page = 2
			page.Results = []seerrRequest{{ID: 3, Status: 1}}
		}
		if err := json.NewEncoder(w).Encode(page); err != nil {
			t.Error(err)
		}
	}))
	defer server.Close()
	state, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}

	s := &Seerr{
		Config: config.Config{SeerrURL: server.URL, SeerrAPIKey: "key"},
		Store:  state,
		Client: server.Client(),
		Log:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	s.poll(context.Background())

	if len(skips) != 2 || skips[0] != 0 || skips[1] != 2 {
		t.Fatalf("expected request pages at skips 0 and 2, got %v", skips)
	}
}

func TestSeerrPollKeepsDurablyImportedWorkUntouched(t *testing.T) {
	var phase atomic.Int32
	var catalogCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/request":
			if phase.Load() == 0 {
				_, _ = io.WriteString(w, `{"results":[{"id":51,"status":2,"type":"movie","media":{"id":951,"tmdbId":851,"mediaType":"movie"}}]}`)
				return
			}
			_, _ = io.WriteString(w, `{"results":[{"id":52,"status":2,"type":"movie","media":{"id":951,"tmdbId":851,"mediaType":"movie"}}]}`)
		case "/api/v1/movie/851":
			catalogCalls.Add(1)
			_, _ = io.WriteString(w, `{"id":851,"title":"Queued","releaseDate":"2025-01-01"}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	state, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	resolver := &Resolver{Store: state}
	seerr := &Seerr{
		Config:    config.Config{SeerrURL: server.URL, SeerrAPIKey: "key"},
		Store:     state,
		Resolver:  resolver,
		Scheduler: &Lifecycle{},
		Client:    server.Client(),
	}

	seerr.poll(context.Background())
	seerr.tasks.Wait()
	initial, ok := state.FindMediaByTMDB("movie", 851)
	if !ok || initial.Work.Generation != 1 || len(initial.RequestIDs) != 1 || initial.RequestIDs[0] != 51 {
		t.Fatalf("initial request was not durably queued: %#v", initial)
	}
	nextAt := time.Now().UTC().Add(30 * time.Minute).Round(0)
	leaseUntil := time.Now().UTC().Add(time.Hour).Round(0)
	if _, err := state.UpdateMedia(initial.ID, func(media *model.Media) error {
		media.Work.Attempts = 3
		media.Work.NextAt = nextAt
		media.Work.LeaseUntil = leaseUntil
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	expected, _ := state.MediaByID(initial.ID)

	// The same unprocessed Seerr request must be skipped locally. It may still
	// be in its retry window, but polling must not reset or supersede that work.
	seerr.poll(context.Background())
	seerr.tasks.Wait()
	unchanged, _ := state.MediaByID(initial.ID)
	if unchanged.Work != expected.Work {
		t.Fatalf("unchanged request superseded durable work: got %#v want %#v", unchanged.Work, expected.Work)
	}
	if catalogCalls.Load() != 1 {
		t.Fatalf("unchanged request fetched catalog again: calls=%d", catalogCalls.Load())
	}

	// A genuinely new request ID for the same media remains importable and
	// intentionally supersedes the previous command after merging association.
	phase.Store(1)
	seerr.poll(context.Background())
	seerr.tasks.Wait()
	merged, _ := state.MediaByID(initial.ID)
	if merged.Work.Generation != expected.Work.Generation+1 || merged.Work.Attempts != 0 || !merged.Work.NextAt.IsZero() || !merged.Work.LeaseUntil.IsZero() {
		t.Fatalf("new request did not queue fresh durable work: %#v", merged.Work)
	}
	if len(merged.RequestIDs) != 2 || merged.RequestIDs[0] != 51 || merged.RequestIDs[1] != 52 {
		t.Fatalf("new request ID was not merged: %#v", merged.RequestIDs)
	}
	if catalogCalls.Load() != 2 {
		t.Fatalf("new request did not import: calls=%d", catalogCalls.Load())
	}
}

func TestSeerrTVScheduleFailureLeavesRequestUnassociatedForRetry(t *testing.T) {
	state, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	var scheduleFails atomic.Bool
	scheduleFails.Store(true)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/tv/852":
			_, _ = io.WriteString(w, `{"id":852,"name":"Weekly","firstAirDate":"2025-01-01","seasons":[{"seasonNumber":1,"airDate":"2025-01-01","episodeCount":2}]}`)
		case "/api/v1/tv/852/season/1":
			if scheduleFails.Load() {
				http.Error(w, "temporary schedule failure", http.StatusBadGateway)
				return
			}
			_, _ = io.WriteString(w, `{"seasonNumber":1,"episodes":[{"episodeNumber":1,"airDate":"2025-01-01"},{"episodeNumber":2,"airDate":"2099-01-08"}]}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	resolver := &Resolver{Store: state}
	seerr := &Seerr{
		Config:    config.Config{SeerrURL: server.URL, SeerrAPIKey: "key"},
		Store:     state,
		Resolver:  resolver,
		Scheduler: &Lifecycle{},
		Client:    server.Client(),
		Log:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	request := seerrRequest{ID: 52, Type: "tv", RequestIDs: []int64{52}, Seasons: []seerrSeasonRequest{{SeasonNumber: 1}}}
	request.Media.ID = 952
	request.Media.TMDBID = 852
	request.Media.MediaType = "tv"

	seerr.handle(context.Background(), request)
	if media, ok := state.FindMediaByTMDB("tv", 852); ok || media != nil {
		t.Fatalf("failed schedule was durably associated: %#v", media)
	}
	if state.IsProcessed(request.ID) {
		t.Fatal("failed schedule marked the request processed")
	}

	scheduleFails.Store(false)
	seerr.handle(context.Background(), request)
	queued, ok := state.FindMediaByTMDB("tv", 852)
	if !ok {
		t.Fatal("retry after schedule recovery was not queued")
	}
	if queued.Work.Mode != workModeResolve || queued.Work.Generation != 1 || queued.EpisodeCounts[1] != 2 || len(queued.EpisodeAirDates) != 2 {
		t.Fatalf("retry did not persist the complete TV scheduling unit: %#v", queued)
	}
}

func TestSeerrDurableRequestAssociationDetectsExpandedTVScope(t *testing.T) {
	state, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := state.UpsertMedia(&model.Media{
		ID: 951, SeerrMediaID: 951, RequestID: 51, RequestIDs: []int64{51},
		Type: "tv", TMDBID: 851, Seasons: []int{1}, Status: "queued",
	}); err != nil {
		t.Fatal(err)
	}
	request := seerrRequest{ID: 51, Type: "tv", RequestIDs: []int64{51}, Seasons: []seerrSeasonRequest{{SeasonNumber: 1}}}
	request.Media.ID = 951
	request.Media.TMDBID = 851
	request.Media.MediaType = "tv"
	seerr := &Seerr{Store: state}
	if !seerr.requestIsDurablyImported(request) {
		t.Fatal("tracked request and season should be locally idempotent")
	}
	request.Seasons = append(request.Seasons, seerrSeasonRequest{SeasonNumber: 2})
	if seerr.requestIsDurablyImported(request) {
		t.Fatal("new requested season must be imported and queued")
	}
	request.Seasons = request.Seasons[:1]
	request.RequestIDs = []int64{51, 52}
	if seerr.requestIsDurablyImported(request) {
		t.Fatal("new request ID must be imported and queued")
	}
}
