package service

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
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
