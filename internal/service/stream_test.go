package service

import (
	"bytes"
	"context"
	"fmt"
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

type healingProvider struct {
	url   string
	calls int
}

type transientLinkProvider struct {
	url      string
	calls    int
	failures int
}

type blockingLinkProvider struct {
	mu      sync.Mutex
	url     string
	calls   int
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

type rateLimitedLinkProvider struct {
	calls int
}

type streamRoundTripper func(*http.Request) (*http.Response, error)

func (roundTrip streamRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return roundTrip(request)
}

type disconnectWriter struct {
	header http.Header
	status int
}

func (w *disconnectWriter) Header() http.Header {
	if w.header == nil {
		w.header = make(http.Header)
	}
	return w.header
}
func (w *disconnectWriter) WriteHeader(status int) { w.status = status }
func (w *disconnectWriter) Write([]byte) (int, error) {
	return 0, fmt.Errorf("write tcp 172.25.0.2:8080->172.25.0.3:1234: connection reset by peer")
}

func (p *healingProvider) Name() string { return "test" }
func (p *healingProvider) Resolve(context.Context, model.Release) (model.Resolved, error) {
	return model.Resolved{}, nil
}
func (p *healingProvider) StreamURL(_ context.Context, file *model.File) (string, error) {
	p.calls++
	if file.ProviderItemID == "stale" {
		return "", debrid.ErrStaleItem
	}
	return p.url, nil
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

func (p *transientLinkProvider) Name() string { return "test" }
func (p *transientLinkProvider) Resolve(context.Context, model.Release) (model.Resolved, error) {
	return model.Resolved{}, nil
}
func (p *transientLinkProvider) StreamURL(context.Context, *model.File) (string, error) {
	p.calls++
	if p.calls <= p.failures {
		return "", fmt.Errorf("%w: gateway unavailable", debrid.ErrTransient)
	}
	return p.url, nil
}

func (p *blockingLinkProvider) Name() string { return "test" }
func (p *blockingLinkProvider) Resolve(context.Context, model.Release) (model.Resolved, error) {
	return model.Resolved{}, nil
}
func (p *blockingLinkProvider) StreamURL(ctx context.Context, _ *model.File) (string, error) {
	p.mu.Lock()
	p.calls++
	p.once.Do(func() { close(p.started) })
	p.mu.Unlock()
	select {
	case <-p.release:
		return p.url, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func (p *rateLimitedLinkProvider) Name() string { return "test" }
func (p *rateLimitedLinkProvider) Resolve(context.Context, model.Release) (model.Resolved, error) {
	return model.Resolved{}, nil
}
func (p *rateLimitedLinkProvider) StreamURL(context.Context, *model.File) (string, error) {
	p.calls++
	return "", fmt.Errorf("%w: too many requests", debrid.ErrRateLimited)
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

func TestStreamerRejectsNonHTTPProviderURLs(t *testing.T) {
	for _, test := range []struct {
		name string
		url  string
	}{
		{name: "malformed", url: "://signed-provider.example/stream?token=malformed-secret"},
		{name: "relative", url: "/stream?token=relative-secret"},
		{name: "ftp", url: "ftp://signed-provider.example/stream?token=ftp-secret"},
	} {
		t.Run(test.name, func(t *testing.T) {
			st, err := store.Open(t.TempDir() + "/state.json")
			if err != nil {
				t.Fatal(err)
			}
			file := &model.File{ID: "file", Path: "Movies/Test/Test.mkv", Provider: "test"}
			if err = st.AddFiles(file); err != nil {
				t.Fatal(err)
			}
			provider := &rotatingProvider{url: test.url}
			var logs bytes.Buffer
			streamer := &Streamer{
				Store: st, Providers: map[string]debrid.Provider{"test": provider},
				TTL: time.Hour, RetryBackoff: time.Nanosecond,
				Log: slog.New(slog.NewJSONHandler(&logs, nil)),
			}
			recorder := httptest.NewRecorder()
			streamer.Serve(recorder, httptest.NewRequest(http.MethodGet, "http://watchtower/file", nil), file)
			if recorder.Code != http.StatusBadGateway {
				t.Fatalf("expected 502, got %d: %s", recorder.Code, recorder.Body.String())
			}
			if provider.calls != 3 {
				t.Fatalf("expected three bounded URL attempts, got %d", provider.calls)
			}
			if strings.Contains(recorder.Body.String(), test.url) || strings.Contains(logs.String(), test.url) || strings.Contains(recorder.Body.String(), "secret") || strings.Contains(logs.String(), "secret") {
				t.Fatalf("provider URL leaked into response or logs: response=%s logs=%s", recorder.Body.String(), logs.String())
			}
			cached, _ := st.File(file.ID)
			if cached.StreamURL != "" {
				t.Fatalf("invalid provider URL was cached: %#v", cached.StreamURL)
			}
		})
	}
}

func TestStreamerSanitizesTransportErrors(t *testing.T) {
	st, err := store.Open(t.TempDir() + "/state.json")
	if err != nil {
		t.Fatal(err)
	}
	file := &model.File{ID: "file", Path: "Movies/Test/Test.mkv", Provider: "test"}
	if err = st.AddFiles(file); err != nil {
		t.Fatal(err)
	}
	signedURL := "https://signed-provider.example/stream?token=transport-secret"
	provider := &rotatingProvider{url: signedURL}
	transportCalls := 0
	client := &http.Client{Transport: streamRoundTripper(func(*http.Request) (*http.Response, error) {
		transportCalls++
		return nil, fmt.Errorf("transport private detail for %s", signedURL)
	})}
	var logs bytes.Buffer
	streamer := &Streamer{
		Store: st, Providers: map[string]debrid.Provider{"test": provider}, Client: client,
		TTL: time.Hour, RetryBackoff: time.Nanosecond,
		Log: slog.New(slog.NewJSONHandler(&logs, nil)),
	}
	recorder := httptest.NewRecorder()
	streamer.Serve(recorder, httptest.NewRequest(http.MethodGet, "http://watchtower/file", nil), file)
	if recorder.Code != http.StatusBadGateway {
		t.Fatalf("expected 502, got %d: %s", recorder.Code, recorder.Body.String())
	}
	if provider.calls != 3 || transportCalls != 3 {
		t.Fatalf("expected three bounded transport attempts, provider=%d transport=%d", provider.calls, transportCalls)
	}
	for _, secret := range []string{signedURL, "transport private detail"} {
		if strings.Contains(recorder.Body.String(), secret) || strings.Contains(logs.String(), secret) {
			t.Fatalf("transport detail leaked into response or logs: response=%s logs=%s", recorder.Body.String(), logs.String())
		}
	}
	if !strings.Contains(recorder.Body.String(), errProviderStreamUnavailable.Error()) || !strings.Contains(logs.String(), errProviderStreamUnavailable.Error()) {
		t.Fatalf("transport failure was not sanitized: response=%s logs=%s", recorder.Body.String(), logs.String())
	}
}

func TestStreamerRetriesTransientLinkGenerationFailures(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
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
	provider := &transientLinkProvider{url: upstream.URL, failures: 2}
	streamer := &Streamer{Store: st, Providers: map[string]debrid.Provider{"test": provider}, Client: upstream.Client(), TTL: time.Hour, RetryBackoff: time.Nanosecond}
	recorder := httptest.NewRecorder()
	streamer.Serve(recorder, httptest.NewRequest(http.MethodGet, "http://watchtower/file", nil), file)
	if recorder.Code != http.StatusPartialContent || recorder.Body.String() != "video" {
		t.Fatalf("unexpected response %d: %s", recorder.Code, recorder.Body.String())
	}
	if provider.calls != 3 {
		t.Fatalf("expected three link attempts, got %d", provider.calls)
	}
}

func TestStreamerCoalescesConcurrentLinkRefreshes(t *testing.T) {
	st, err := store.Open(t.TempDir() + "/state.json")
	if err != nil {
		t.Fatal(err)
	}
	file := &model.File{ID: "file", Path: "Movies/Test/Test.mkv", Provider: "test"}
	if err = st.AddFiles(file); err != nil {
		t.Fatal(err)
	}
	provider := &blockingLinkProvider{url: "https://example.test/stream", started: make(chan struct{}), release: make(chan struct{})}
	streamer := &Streamer{Store: st, Providers: map[string]debrid.Provider{"test": provider}, TTL: time.Hour}
	type result struct {
		url string
		err error
	}
	results := make(chan result, 2)
	go func() {
		u, e := streamer.url(context.Background(), file, false)
		results <- result{url: u, err: e}
	}()
	select {
	case <-provider.started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first link refresh")
	}
	go func() {
		u, e := streamer.url(context.Background(), file, false)
		results <- result{url: u, err: e}
	}()
	close(provider.release)
	for i := 0; i < 2; i++ {
		got := <-results
		if got.err != nil || got.url != provider.url {
			t.Fatalf("unexpected coalesced result: %#v", got)
		}
	}
	provider.mu.Lock()
	calls := provider.calls
	provider.mu.Unlock()
	if calls != 1 {
		t.Fatalf("expected one provider link request, got %d", calls)
	}
}

func TestStreamerDoesNotRetryRateLimitedLinkImmediately(t *testing.T) {
	st, err := store.Open(t.TempDir() + "/state.json")
	if err != nil {
		t.Fatal(err)
	}
	file := &model.File{ID: "file", Path: "Movies/Test/Test.mkv", Provider: "test"}
	if err = st.AddFiles(file); err != nil {
		t.Fatal(err)
	}
	provider := &rateLimitedLinkProvider{}
	streamer := &Streamer{Store: st, Providers: map[string]debrid.Provider{"test": provider}, TTL: time.Hour}
	recorder := httptest.NewRecorder()
	streamer.Serve(recorder, httptest.NewRequest(http.MethodGet, "http://watchtower/file", nil), file)
	if recorder.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d", recorder.Code)
	}
	if provider.calls != 1 {
		t.Fatalf("expected one provider request, got %d", provider.calls)
	}
}

func TestStreamerContinuesWhenFileIsReplacedDuringRetry(t *testing.T) {
	st, err := store.Open(t.TempDir() + "/state.json")
	if err != nil {
		t.Fatal(err)
	}
	file := &model.File{ID: "old", MediaID: 7, Path: "Movies/Test/Test.mkv", Provider: "test", Size: 5}
	if err = st.AddFiles(file); err != nil {
		t.Fatal(err)
	}
	requests := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++
		if requests == 1 {
			replacement := &model.File{ID: "new", MediaID: 7, Path: file.Path, Provider: "test", Size: 5}
			if replaceErr := st.ReplaceFilesForMedia(7, replacement); replaceErr != nil {
				t.Errorf("replace file: %v", replaceErr)
			}
			http.Error(w, "temporary CDN failure", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("video"))
	}))
	defer upstream.Close()
	provider := &rotatingProvider{url: upstream.URL}
	streamer := &Streamer{Store: st, Providers: map[string]debrid.Provider{"test": provider}, Client: upstream.Client(), TTL: time.Hour}
	recorder := httptest.NewRecorder()
	streamer.Serve(recorder, httptest.NewRequest(http.MethodGet, "http://watchtower/file", nil), file)
	if recorder.Code != http.StatusPartialContent || recorder.Body.String() != "video" {
		t.Fatalf("unexpected response %d: %s", recorder.Code, recorder.Body.String())
	}
	if provider.calls != 2 || requests != 2 {
		t.Fatalf("expected retry through original source, provider calls=%d requests=%d", provider.calls, requests)
	}
}

func TestStreamerTreatsDownstreamDisconnectAsExpectedCancellation(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
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
	logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	streamer := &Streamer{Store: st, Providers: map[string]debrid.Provider{"test": provider}, Client: upstream.Client(), TTL: time.Hour, Log: logger}
	writer := &disconnectWriter{}
	streamer.Serve(writer, httptest.NewRequest(http.MethodGet, "http://watchtower/file", nil), file)
	if writer.status != http.StatusPartialContent {
		t.Fatalf("unexpected status %d", writer.status)
	}
	if !strings.Contains(logs.String(), "stream transfer canceled by client") || strings.Contains(logs.String(), "stream transfer interrupted") {
		t.Fatalf("unexpected disconnect logs: %s", logs.String())
	}
}

func TestStreamerRepairsStaleProviderItemBeforeServing(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("video"))
	}))
	defer upstream.Close()
	st, err := store.Open(t.TempDir() + "/state.json")
	if err != nil {
		t.Fatal(err)
	}
	file := &model.File{ID: "file", Path: "Movies/Test/Test.mkv", Provider: "test", ProviderItemID: "stale", Size: 5}
	if err := st.AddFiles(file); err != nil {
		t.Fatal(err)
	}
	provider := &healingProvider{url: upstream.URL}
	repairs := 0
	streamer := &Streamer{
		Store: st, Providers: map[string]debrid.Provider{"test": provider}, Client: upstream.Client(), TTL: time.Hour,
		Repair: func(_ context.Context, stale *model.File) (*model.File, error) {
			repairs++
			updated := *stale
			updated.ProviderItemID = "fresh"
			return &updated, nil
		},
	}
	recorder := httptest.NewRecorder()
	streamer.Serve(recorder, httptest.NewRequest(http.MethodGet, "http://watchtower/file", nil), file)
	if recorder.Code != http.StatusPartialContent || recorder.Body.String() != "video" {
		t.Fatalf("unexpected response %d: %s", recorder.Code, recorder.Body.String())
	}
	if repairs != 1 || provider.calls != 2 {
		t.Fatalf("expected one repair and two link attempts, got repairs=%d calls=%d", repairs, provider.calls)
	}
}
