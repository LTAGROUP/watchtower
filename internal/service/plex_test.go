package service

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/LTAGROUP/watchtower/internal/config"
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
