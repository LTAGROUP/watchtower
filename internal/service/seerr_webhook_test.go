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
	"testing"
	"time"

	"github.com/LTAGROUP/watchtower/internal/config"
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
