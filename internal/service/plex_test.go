package service

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LTAGROUP/watchtower/internal/config"
	"github.com/LTAGROUP/watchtower/internal/model"
	"github.com/LTAGROUP/watchtower/internal/store"
)

func TestPlexRefreshRequestsMovieAndTVLibrarySections(t *testing.T) {
	requests := map[string]int{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests[r.URL.Path]++
		if r.Header.Get("X-Plex-Token") != "secret" {
			t.Error("missing Plex token header")
		}
		if r.URL.Path == "/library/sections" {
			_, _ = w.Write([]byte(`<MediaContainer><Directory key="1" type="movie"/><Directory key="2" type="show"/><Directory key="3" type="artist"/></MediaContainer>`))
			return
		}
		if r.URL.Path != "/library/sections/1/refresh" && r.URL.Path != "/library/sections/2/refresh" {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	plex := &Plex{Config: config.Config{PlexURL: server.URL, PlexToken: "secret"}, Client: server.Client()}
	if err := plex.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/library/sections", "/library/sections/1/refresh", "/library/sections/2/refresh"} {
		if requests[path] != 1 {
			t.Fatalf("expected one request to %s, got %#v", path, requests)
		}
	}
	if requests["/library/sections/3/refresh"] != 0 {
		t.Fatalf("non-video library was refreshed: %#v", requests)
	}
}

func TestPlexRunDebouncesLibraryChanges(t *testing.T) {
	refreshed := make(chan struct{}, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/library/sections" {
			_, _ = w.Write([]byte(`<MediaContainer><Directory key="1" type="movie"/></MediaContainer>`))
			return
		}
		refreshed <- struct{}{}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	plex := &Plex{Config: config.Config{PlexURL: server.URL, PlexToken: "secret", PlexScanDelay: 10 * time.Millisecond}, Client: server.Client()}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go plex.Run(ctx)
	plex.Notify()
	plex.Notify()

	select {
	case <-refreshed:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Plex refresh")
	}
	select {
	case <-refreshed:
		t.Fatal("library changes were not debounced")
	case <-time.After(30 * time.Millisecond):
	}
}

func TestPlexIntentRetriesAndDoesNotAcknowledgeNewerGeneration(t *testing.T) {
	state, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	media := &model.Media{ID: 71, Type: "movie", PlexIntent: model.DurableIntent{Generation: 1}}
	if err := state.UpsertMedia(media); err != nil {
		t.Fatal(err)
	}
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/library/sections" {
			requests++
			if requests == 1 {
				http.Error(w, "temporary", http.StatusBadGateway)
				return
			}
			_, _ = w.Write([]byte(`<MediaContainer><Directory key="1" type="movie"/></MediaContainer>`))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	plex := &Plex{Config: config.Config{PlexURL: server.URL, PlexToken: "secret"}, Store: state, Client: server.Client()}
	if !plex.processIntents(context.Background()) {
		t.Fatal("due Plex intent was not processed")
	}
	afterFailure, _ := state.MediaByID(media.ID)
	if afterFailure.PlexIntent.Attempts != 1 || afterFailure.PlexIntent.NextAt.IsZero() || afterFailure.PlexIntent.CompletedGeneration != 0 {
		t.Fatalf("failed intent was not retained: %#v", afterFailure.PlexIntent)
	}
	if _, err := state.UpdateMedia(media.ID, func(m *model.Media) error {
		m.PlexIntent.NextAt = time.Time{}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	plex.processIntents(context.Background())
	afterSuccess, _ := state.MediaByID(media.ID)
	if afterSuccess.PlexIntent.CompletedGeneration != 1 || afterSuccess.PlexIntent.Generation != 1 {
		t.Fatalf("successful intent was not acknowledged: %#v", afterSuccess.PlexIntent)
	}
	newDue := time.Now().UTC().Add(time.Minute)
	if _, err := state.UpdateMedia(media.ID, func(m *model.Media) error {
		m.PlexIntent.Generation = 2
		m.PlexIntent.Attempts = 0
		m.PlexIntent.NextAt = newDue
		m.PlexIntent.LeaseGeneration = 2
		m.PlexIntent.LeaseUntil = time.Now().Add(time.Minute)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// An old refresh may acknowledge only the generation it observed. It must
	// leave generation two pending.
	plex.completeIntentClaims([]plexIntentClaim{{mediaID: media.ID, generation: 1}}, time.Now(), nil)
	newer, _ := state.MediaByID(media.ID)
	if newer.PlexIntent.CompletedGeneration != 1 || newer.PlexIntent.Generation != 2 || newer.PlexIntent.LeaseGeneration != 2 || newer.PlexIntent.Attempts != 0 || !newer.PlexIntent.NextAt.Equal(newDue) {
		t.Fatalf("older success acknowledged newer intent: %#v", newer.PlexIntent)
	}
}

func TestPlexRunReconcilesPendingIntentOnStartup(t *testing.T) {
	state, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := state.UpsertMedia(&model.Media{ID: 72, Type: "movie", PlexIntent: model.DurableIntent{Generation: 1}}); err != nil {
		t.Fatal(err)
	}
	refreshed := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/library/sections" {
			_, _ = w.Write([]byte(`<MediaContainer><Directory key="1" type="movie"/></MediaContainer>`))
			return
		}
		select {
		case refreshed <- struct{}{}:
		default:
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	plex := &Plex{Config: config.Config{PlexURL: server.URL, PlexToken: "secret"}, Store: state, Client: server.Client()}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		plex.Run(ctx)
		close(done)
	}()
	select {
	case <-refreshed:
	case <-time.After(time.Second):
		t.Fatal("startup scan did not refresh pending Plex intent")
	}
	deadline := time.Now().Add(time.Second)
	for {
		media, _ := state.MediaByID(72)
		if media.PlexIntent.CompletedGeneration == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("startup refresh did not durably acknowledge intent: %#v", media.PlexIntent)
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Plex Run did not stop")
	}
	media, _ := state.MediaByID(72)
	if media.PlexIntent.CompletedGeneration != 1 {
		t.Fatalf("startup intent remained pending: %#v", media.PlexIntent)
	}
}

func TestPlexRunHonorsPersistedIntentDeadlineAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	state, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	delay := 250 * time.Millisecond
	dueAt := time.Now().UTC().Add(delay)
	if err := state.UpsertMedia(&model.Media{ID: 73, Type: "movie", PlexIntent: model.DurableIntent{Generation: 1, NextAt: dueAt}}); err != nil {
		t.Fatal(err)
	}

	// Reopen the serialized state as a fresh process would. The deadline must
	// remain durable rather than being reconstructed from an in-memory debounce.
	restarted, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	persisted, ok := restarted.MediaByID(73)
	if !ok || !persisted.PlexIntent.NextAt.Equal(dueAt) {
		t.Fatalf("Plex intent deadline was not persisted across restart: %#v", persisted)
	}

	refreshed := make(chan time.Time, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/library/sections" {
			_, _ = w.Write([]byte(`<MediaContainer><Directory key="1" type="movie"/></MediaContainer>`))
			return
		}
		select {
		case refreshed <- time.Now().UTC():
		default:
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	plex := &Plex{Config: config.Config{PlexURL: server.URL, PlexToken: "secret", PlexScanDelay: delay}, Store: restarted, Client: server.Client()}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		plex.Run(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("Plex Run did not stop")
		}
	})

	select {
	case refreshedAt := <-refreshed:
		t.Fatalf("Plex refreshed at %s before persisted deadline %s", refreshedAt, dueAt)
	case <-time.After(75 * time.Millisecond):
	}
	select {
	case refreshedAt := <-refreshed:
		if refreshedAt.Before(dueAt) {
			t.Fatalf("Plex refreshed at %s before persisted deadline %s", refreshedAt, dueAt)
		}
	case <-time.After(time.Second):
		t.Fatal("Plex did not refresh when the persisted deadline became due")
	}

	deadline := time.Now().Add(time.Second)
	for {
		media, _ := restarted.MediaByID(73)
		if media.PlexIntent.CompletedGeneration == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("due intent was not acknowledged: %#v", media.PlexIntent)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestPlexRunDoesNotSpinDueIntentTimerWhileDebouncing(t *testing.T) {
	state, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := state.UpsertMedia(&model.Media{ID: 74, Type: "movie", PlexIntent: model.DurableIntent{Generation: 1, NextAt: time.Now().UTC().Add(50 * time.Millisecond)}}); err != nil {
		t.Fatal(err)
	}

	var timers atomic.Int32
	plex := &Plex{
		Config: config.Config{PlexScanDelay: 400 * time.Millisecond},
		Store:  state,
		newIntentTimer: func(delay time.Duration) *time.Timer {
			timers.Add(1)
			return time.NewTimer(delay)
		},
	}
	// Queue the notify before Run begins so the debounce is definitely active
	// when the durable deadline passes.
	plex.Notify()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		plex.Run(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("Plex Run did not stop")
		}
	})

	deadline := time.Now().Add(time.Second)
	for timers.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("Plex did not schedule its durable intent timer")
		}
		time.Sleep(time.Millisecond)
	}
	// The durable deadline now passes while the longer debounce is active. The
	// old loop recreated a one-millisecond timer until that debounce expired.
	time.Sleep(125 * time.Millisecond)
	if got := timers.Load(); got != 1 {
		t.Fatalf("due intent timer spun while debounce was active: created %d timers", got)
	}
}
