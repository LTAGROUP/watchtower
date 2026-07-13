package service

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LTAGROUP/watchtower/internal/debrid"
	"github.com/LTAGROUP/watchtower/internal/model"
	"github.com/LTAGROUP/watchtower/internal/store"
)

type rotatingProvider struct {
	mu    sync.Mutex
	url   string
	calls int
}

func (p *rotatingProvider) Name() string { return "test" }
func (p *rotatingProvider) Resolve(context.Context, model.Release) (model.Resolved, error) {
	return model.Resolved{}, nil
}
func (p *rotatingProvider) StreamURL(context.Context, *model.File) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	return p.url, nil
}

func TestStreamerRefreshesURLAfterProviderServerError(t *testing.T) {
	var requests int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if requests == 1 {
			http.Error(w, "temporary CDN failure", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("video"))
	}))
	defer upstream.Close()

	st, err := store.Open(t.TempDir() + "/state.json")
	if err != nil {
		t.Fatal(err)
	}
	file := &model.File{ID: "file", Path: "Movies/Test/Test.mkv", Provider: "test", Size: 5}
	if err = st.AddFiles(file); err != nil {
		t.Fatal(err)
	}
	provider := &rotatingProvider{url: upstream.URL}
	var logs bytes.Buffer
	streamer := &Streamer{Store: st, Providers: map[string]debrid.Provider{"test": provider}, Client: upstream.Client(), TTL: time.Hour, Log: slog.New(slog.NewJSONHandler(&logs, nil))}
	req := httptest.NewRequest(http.MethodGet, "http://watchtower/dav/Movies/Test/Test.mkv", nil)
	req.Header.Set("Range", "bytes=0-4")
	rec := httptest.NewRecorder()
	streamer.Serve(rec, req, file)
	if rec.Code != http.StatusPartialContent {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	if provider.calls != 2 {
		t.Fatalf("expected refreshed provider URL, got %d calls", provider.calls)
	}
	if requests != 2 {
		t.Fatalf("expected two upstream attempts, got %d", requests)
	}
	for _, event := range []string{"stream link refresh started", "stream link obtained", "stream link rejected by upstream", "stream request completed"} {
		if !strings.Contains(logs.String(), event) {
			t.Errorf("expected %q log event; logs: %s", event, logs.String())
		}
	}
	if strings.Contains(logs.String(), upstream.URL) {
		t.Errorf("signed upstream URL leaked into logs: %s", logs.String())
	}
}

func TestStreamerConvertsRepeatedProviderErrorsToBadGateway(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "satellite HTML", http.StatusInternalServerError)
	}))
	defer upstream.Close()
	st, err := store.Open(t.TempDir() + "/state.json")
	if err != nil {
		t.Fatal(err)
	}
	file := &model.File{ID: "file", Path: "Movies/Test/Test.mkv", Provider: "test"}
	if err = st.AddFiles(file); err != nil {
		t.Fatal(err)
	}
	provider := &rotatingProvider{url: upstream.URL}
	streamer := &Streamer{Store: st, Providers: map[string]debrid.Provider{"test": provider}, Client: upstream.Client(), TTL: time.Hour}
	rec := httptest.NewRecorder()
	streamer.Serve(rec, httptest.NewRequest(http.MethodGet, "http://watchtower/file", nil), file)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	if provider.calls != 3 {
		t.Fatalf("expected 3 URL refreshes, got %d", provider.calls)
	}
}
