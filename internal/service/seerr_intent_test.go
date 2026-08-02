package service

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/LTAGROUP/watchtower/internal/config"
	"github.com/LTAGROUP/watchtower/internal/model"
	"github.com/LTAGROUP/watchtower/internal/store"
)

func TestSeerrAvailabilityIntentRetriesAndProtectsNewerGeneration(t *testing.T) {
	state, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	media := &model.Media{ID: 81, SeerrMediaID: 901, RequestID: 41, RequestIDs: []int64{41}, Type: "movie", Status: "ready"}
	if err := state.UpsertMedia(media); err != nil {
		t.Fatal(err)
	}
	if err := state.AddFiles(&model.File{ID: "movie", MediaID: media.ID, Path: "Movies/Example.mkv", Quality: "1080p"}); err != nil {
		t.Fatal(err)
	}
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/media/901/available" {
			http.NotFound(w, r)
			return
		}
		attempts++
		if attempts == 1 {
			http.Error(w, "temporary", http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	seerr := &Seerr{Config: config.Config{SeerrURL: server.URL, SeerrAPIKey: "key"}, Store: state, Client: server.Client()}
	seerr.completeResolved(context.Background())
	created, _ := state.MediaByID(media.ID)
	if !state.IsProcessed(41) || created.AvailabilityIntent.Generation != 1 || created.AvailabilityIntent.CompletedGeneration != 0 {
		t.Fatalf("completion was not atomically recorded before availability call: %#v", created)
	}
	seerr.retryAvailability(context.Background())
	afterFailure, _ := state.MediaByID(media.ID)
	if afterFailure.AvailabilityIntent.Attempts != 1 || afterFailure.AvailabilityIntent.NextAt.IsZero() || afterFailure.AvailabilityIntent.CompletedGeneration != 0 {
		t.Fatalf("failed availability intent was lost: %#v", afterFailure.AvailabilityIntent)
	}
	if _, err := state.UpdateMedia(media.ID, func(m *model.Media) error {
		m.AvailabilityIntent.NextAt = time.Time{}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	seerr.retryAvailability(context.Background())
	afterSuccess, _ := state.MediaByID(media.ID)
	if afterSuccess.AvailabilityIntent.CompletedGeneration != 1 {
		t.Fatalf("availability intent was not acknowledged: %#v", afterSuccess.AvailabilityIntent)
	}
	if _, err := state.UpdateMedia(media.ID, func(m *model.Media) error {
		m.AvailabilityIntent.Generation = 2
		m.AvailabilityIntent.LeaseGeneration = 2
		m.AvailabilityIntent.LeaseUntil = time.Now().Add(time.Minute)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	seerr.completeAvailabilityIntent(availabilityIntentClaim{mediaID: media.ID, generation: 1, seerrID: 901}, time.Now(), nil)
	newer, _ := state.MediaByID(media.ID)
	if newer.AvailabilityIntent.Generation != 2 || newer.AvailabilityIntent.CompletedGeneration != 1 || newer.AvailabilityIntent.LeaseGeneration != 2 {
		t.Fatalf("older availability success acknowledged newer generation: %#v", newer.AvailabilityIntent)
	}
}

func TestSeerrDoesNotProcessFutureEpisodeRequestPrematurely(t *testing.T) {
	state, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	media := &model.Media{
		ID: 82, SeerrMediaID: 902, RequestID: 42, RequestIDs: []int64{42}, Type: "tv", Status: "queued",
		Seasons: []int{1}, EpisodeCounts: map[int]int{1: 2},
		Work: model.MediaWork{Mode: workModeResolve, NextAt: time.Now().Add(24 * time.Hour)},
	}
	if err := state.UpsertMedia(media); err != nil {
		t.Fatal(err)
	}
	if err := state.AddFiles(&model.File{ID: "episode-one", MediaID: media.ID, Path: "TV/Weekly/Season 01/Weekly - S01E01 [1080p].mkv", Quality: "1080p"}); err != nil {
		t.Fatal(err)
	}
	seerr := &Seerr{Store: state}
	seerr.completeResolved(context.Background())
	updated, _ := state.MediaByID(media.ID)
	if state.IsProcessed(42) || updated.AvailabilityIntent.Generation != 0 {
		t.Fatalf("future TV request completed early: %#v", updated)
	}
}

func TestSeerrCompleteResolvedDoesNotOverwriteNewerWork(t *testing.T) {
	state, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	availability := model.DurableIntent{Generation: 5, CompletedGeneration: 3, Attempts: 2, NextAt: time.Now().UTC().Add(time.Hour).Round(0), LeaseGeneration: 5, LeaseUntil: time.Now().UTC().Add(2 * time.Hour).Round(0)}
	plexIntent := model.DurableIntent{Generation: 6, CompletedGeneration: 4, Attempts: 1, NextAt: time.Now().UTC().Add(3 * time.Hour).Round(0), LeaseGeneration: 6, LeaseUntil: time.Now().UTC().Add(4 * time.Hour).Round(0)}
	media := &model.Media{
		ID: 83, RequestID: 43, RequestIDs: []int64{43}, SeerrMediaID: 903,
		Type: "movie", TMDBID: 803, Title: "Initial catalog", Status: "ready",
		AvailabilityIntent: availability, PlexIntent: plexIntent,
	}
	if err := state.UpsertMedia(media); err != nil {
		t.Fatal(err)
	}
	if err := state.AddFiles(&model.File{ID: "movie", MediaID: media.ID, Path: "Movies/Example.mkv", Quality: "1080p"}); err != nil {
		t.Fatal(err)
	}

	resolver := &Resolver{Store: state}
	seerr := &Seerr{Store: state}
	var queueErr error
	seerr.beforeCompleteResolved = func() {
		seerr.beforeCompleteResolved = nil
		_, queueErr = resolver.Queue(&model.Media{
			ID: media.ID, RequestID: 44, RequestIDs: []int64{44}, SeerrMediaID: 904,
			Type: "movie", TMDBID: media.TMDBID, Title: "N+1 catalog", Overview: "new catalog data",
		})
	}
	seerr.completeResolved(context.Background())
	if queueErr != nil {
		t.Fatalf("queue N+1: %v", queueErr)
	}

	updated, _ := state.MediaByID(media.ID)
	wantWork := model.MediaWork{Mode: workModeResolve, Generation: 1}
	if updated.Work != wantWork || updated.Status != "queued" || updated.Error != "" {
		t.Fatalf("stale Seerr completion replaced N+1 work: %#v", updated)
	}
	if state.IsProcessed(43) || state.IsProcessed(44) {
		t.Fatal("stale Seerr completion marked requests despite newer work")
	}
	if updated.AvailabilityIntent != availability || updated.PlexIntent != plexIntent {
		t.Fatalf("stale Seerr completion replaced current intents: availability=%#v plex=%#v", updated.AvailabilityIntent, updated.PlexIntent)
	}
	if updated.Title != "N+1 catalog" || updated.Overview != "new catalog data" || updated.RequestID != 44 || updated.SeerrMediaID != 904 || len(updated.RequestIDs) != 2 {
		t.Fatalf("stale Seerr completion replaced current catalog/request data: %#v", updated)
	}
}

func TestSeerrRunCancelsAndWaitsForTrackedHandlers(t *testing.T) {
	catalogStarted := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/request":
			_, _ = w.Write([]byte(`{"results":[{"id":51,"status":2,"type":"movie","media":{"id":951,"tmdbId":851,"mediaType":"movie"}}]}`))
		case "/api/v1/movie/851":
			select {
			case catalogStarted <- struct{}{}:
			case <-r.Context().Done():
			}
			<-r.Context().Done()
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	state, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	seerr := &Seerr{Config: config.Config{SeerrURL: server.URL, SeerrAPIKey: "key", PollInterval: time.Hour}, Store: state, Client: server.Client()}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		seerr.Run(ctx)
		close(done)
	}()
	select {
	case <-catalogStarted:
	case <-time.After(time.Second):
		t.Fatal("request handler did not start")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Seerr Run returned without waiting for a canceled handler")
	}
}
