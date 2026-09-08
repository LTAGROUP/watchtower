package service

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LTAGROUP/watchtower/internal/config"
	"github.com/LTAGROUP/watchtower/internal/debrid"
	"github.com/LTAGROUP/watchtower/internal/model"
	"github.com/LTAGROUP/watchtower/internal/scraper"
	"github.com/LTAGROUP/watchtower/internal/store"
)

func TestPartialResponseValidation(t *testing.T) {
	for _, tc := range []struct {
		name, requested, returned string
		length                    int64
		valid                     bool
	}{
		{"valid", "bytes=10-19", "bytes 10-19/100", 10, true},
		{"missing", "bytes=10-19", "", 10, false},
		{"malformed", "bytes=10-19", "garbage", 10, false},
		{"unsatisfied", "bytes=10-19", "bytes */100", 10, false},
		{"end past total", "bytes=90-", "bytes 90-100/100", 11, false},
		{"exceeds requested end", "bytes=10-19", "bytes 10-29/100", 20, false},
		{"valid suffix", "bytes=-10", "bytes 90-99/100", 10, true},
		{"wrong suffix", "bytes=-10", "bytes 0-9/100", 10, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "http://watchtower/file", nil)
			req.Header.Set("Range", tc.requested)
			resp := &http.Response{Header: http.Header{"Content-Range": {tc.returned}}, ContentLength: tc.length}
			reason := invalidPartialResponse(req, resp, &model.File{Size: 100})
			if (reason == "") != tc.valid {
				t.Fatalf("valid=%v reason=%q", tc.valid, reason)
			}
		})
	}
}

type truncatedBody struct{ sent bool }

func (b *truncatedBody) Read(p []byte) (int, error) {
	if !b.sent {
		b.sent = true
		return copy(p, "partial"), nil
	}
	return 0, io.ErrUnexpectedEOF
}
func (*truncatedBody) Close() error { return nil }

func TestStreamerAbortsTruncatedTransfer(t *testing.T) {
	st, err := store.Open(t.TempDir() + "/state.json")
	if err != nil {
		t.Fatal(err)
	}
	f := &model.File{ID: "file", Provider: "test"}
	if err := st.AddFiles(f); err != nil {
		t.Fatal(err)
	}
	s := &Streamer{Store: st, Providers: map[string]debrid.Provider{"test": &rotatingProvider{url: "https://cdn.example/video"}}, TTL: time.Hour,
		Client: &http.Client{Transport: streamRoundTripper(func(r *http.Request) (*http.Response, error) {
			if r.Header.Get("Accept-Encoding") != "identity" {
				t.Error("upstream request must preserve media byte offsets")
			}
			return &http.Response{StatusCode: 200, Header: make(http.Header), ContentLength: -1, Body: &truncatedBody{}}, nil
		})}}
	defer func() {
		if got := recover(); got != http.ErrAbortHandler {
			t.Errorf("truncated stream completed normally: panic=%v", got)
		}
	}()
	s.Serve(httptest.NewRecorder(), httptest.NewRequest("GET", "http://watchtower/file", nil), f)
}

func TestRepairRejectsDifferentTorrentBytes(t *testing.T) {
	for _, tc := range []struct {
		name, hash string
		size       int64
	}{
		{"different torrent", "other", 100}, {"different size", "original", 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st, err := store.Open(t.TempDir() + "/state.json")
			if err != nil {
				t.Fatal(err)
			}
			media := &model.Media{ID: 1, Type: "movie", Title: "Example"}
			if err := st.UpsertMedia(media); err != nil {
				t.Fatal(err)
			}
			f := &model.File{ID: "file", MediaID: 1, Path: "Movies/Example/Example.mkv", Provider: "test", Quality: "1080p", InfoHash: "original", Size: 100}
			if err := st.AddFiles(f); err != nil {
				t.Fatal(err)
			}
			r := &Resolver{Store: st, Config: config.Config{Providers: []string{"test"}, ResolveTimeout: time.Second},
				Scraper:   &recordingSearcher{releases: []model.Release{{Title: "Example 1080p", InfoHash: tc.hash, Seeders: 10}}},
				Providers: map[string]debrid.Provider{"test": fixedProvider{resolved: model.Resolved{ItemID: "new", Files: []model.RemoteFile{{ID: "0", Name: "Example.mkv", Size: tc.size}}}}},
			}
			if _, err := r.Repair(context.Background(), f); err == nil {
				t.Fatal("repair switched an existing virtual file to different bytes")
			}
			got, _ := st.File(f.ID)
			if got.ProviderItemID != "" || got.InfoHash != "original" || got.Size != 100 {
				t.Fatalf("failed repair mutated source: %+v", got)
			}
		})
	}
}

func TestMovieCandidatesFilteredBeforeLimit(t *testing.T) {
	st, err := store.Open(t.TempDir() + "/state.json")
	if err != nil {
		t.Fatal(err)
	}
	searcher := &limitSearcher{}
	r := &Resolver{Store: st, Config: config.Config{MaxResults: 1}, Scraper: searcher}
	got, err := r.manualReleases(context.Background(), &model.Media{Type: "movie"}, ManualTarget{Quality: "2160p"}, r.Config)
	if err != nil || len(got) != 1 || !strings.Contains(got[0].Title, "2160p") {
		t.Fatalf("quality excluded by global limit: %+v %v", got, err)
	}
}

type limitSearcher struct{}

func (*limitSearcher) Search(_ context.Context, _ scraper.Query, limit int) ([]model.Release, error) {
	rows := []model.Release{{Title: "Example 1080p", Seeders: 100}, {Title: "Example 2160p", Seeders: 10}}
	if limit > 0 && len(rows) > limit {
		rows = rows[:limit]
	}
	return rows, nil
}

func TestRepairUsesOriginalSourceWithoutScraper(t *testing.T) {
	st, err := store.Open(t.TempDir() + "/state.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertMedia(&model.Media{ID: 1, Type: "movie", Title: "Example"}); err != nil {
		t.Fatal(err)
	}
	f := &model.File{ID: "file", MediaID: 1, Path: "Movies/Example/Example.mkv", Provider: "test", Quality: "1080p", InfoHash: "original", SourceURI: "magnet:?xt=urn:btih:original", Size: 100}
	if err := st.AddFiles(f); err != nil {
		t.Fatal(err)
	}
	r := &Resolver{Store: st, Config: config.Config{Providers: []string{"test"}, ResolveTimeout: time.Second}, Scraper: panicSearcher{},
		Providers: map[string]debrid.Provider{"test": fixedProvider{resolved: model.Resolved{ItemID: "new", Files: []model.RemoteFile{{ID: "0", Name: "Example.mkv", Size: 100}}}}}}
	got, err := r.Repair(context.Background(), f)
	if err != nil || got.ProviderItemID != "new" || got.ID != f.ID || got.InfoHash != f.InfoHash {
		t.Fatalf("original source repair failed: %+v %v", got, err)
	}
}

func TestSuccessfulRefreshPreservesConcurrentCooldown(t *testing.T) {
	st, err := store.Open(t.TempDir() + "/state.json")
	if err != nil {
		t.Fatal(err)
	}
	f := &model.File{ID: "file", Provider: "test"}
	if err := st.AddFiles(f); err != nil {
		t.Fatal(err)
	}
	p := &blockingLinkProvider{url: "https://cdn.example/video", started: make(chan struct{}), release: make(chan struct{})}
	s := &Streamer{Store: st, Providers: map[string]debrid.Provider{"test": p}, TTL: time.Hour}
	result := make(chan error, 1)
	go func() { _, err := s.url(context.Background(), f, false); result <- err }()
	<-p.started
	s.blockProvider("test", time.Minute)
	close(p.release)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if _, err := s.url(context.Background(), f, false); !errors.Is(err, debrid.ErrRateLimited) {
		t.Fatalf("concurrent cooldown was erased: %v", err)
	}
}

type observedContext struct {
	context.Context
	observed chan struct{}
	once     sync.Once
}

func (c *observedContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.observed) })
	return c.Context.Done()
}

func TestCanceledRefreshDoesNotFailWaitingPlayback(t *testing.T) {
	st, err := store.Open(t.TempDir() + "/state.json")
	if err != nil {
		t.Fatal(err)
	}
	f := &model.File{ID: "file", Provider: "test"}
	if err := st.AddFiles(f); err != nil {
		t.Fatal(err)
	}
	p := &blockingLinkProvider{url: "https://cdn.example/video", started: make(chan struct{}), release: make(chan struct{})}
	s := &Streamer{Store: st, Providers: map[string]debrid.Provider{"test": p}, TTL: time.Hour}
	leader, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := make(chan error, 1)
	second := make(chan error, 1)
	go func() { _, err := s.url(leader, f, false); first <- err }()
	<-p.started
	parent, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	waiter := &observedContext{Context: parent, observed: make(chan struct{})}
	go func() { _, err := s.url(waiter, f, false); second <- err }()
	<-waiter.observed
	cancel()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatalf("leader: %v", err)
	}
	close(p.release)
	if err := <-second; err != nil {
		t.Fatalf("live playback inherited canceled probe: %v", err)
	}
}

func TestRepairedProviderReceivesCDNCooldown(t *testing.T) {
	st, err := store.Open(t.TempDir() + "/state.json")
	if err != nil {
		t.Fatal(err)
	}
	f := &model.File{ID: "file", Provider: "test", ProviderItemID: "stale", Size: 5}
	if err := st.AddFiles(f); err != nil {
		t.Fatal(err)
	}
	s := &Streamer{Store: st, Providers: map[string]debrid.Provider{"test": &healingProvider{}, "backup": &rotatingProvider{url: "https://cdn.example/video"}}, TTL: time.Hour,
		Client: &http.Client{Transport: streamRoundTripper(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 429, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(""))}, nil
		})},
		Repair: func(_ context.Context, old *model.File) (*model.File, error) {
			replacement := *old
			replacement.Provider = "backup"
			replacement.ProviderItemID = "fresh"
			return st.ReplaceFileSource(old.ID, &replacement)
		}}
	w := httptest.NewRecorder()
	s.Serve(w, httptest.NewRequest("GET", "http://watchtower/file", nil), f)
	if w.Code != 429 || !s.rateLimitedUntil["backup"].After(time.Now()) || !s.rateLimitedUntil["test"].IsZero() {
		t.Fatalf("cooldown applied to wrong provider: status=%d cooldowns=%v", w.Code, s.rateLimitedUntil)
	}
}

func TestStreamerAbortsShortChunkedRange(t *testing.T) {
	st, err := store.Open(t.TempDir() + "/state.json")
	if err != nil {
		t.Fatal(err)
	}
	f := &model.File{ID: "file", Provider: "test", Size: 100}
	if err := st.AddFiles(f); err != nil {
		t.Fatal(err)
	}
	s := &Streamer{Store: st, Providers: map[string]debrid.Provider{"test": &rotatingProvider{url: "https://cdn.example/video"}}, TTL: time.Hour,
		Client: &http.Client{Transport: streamRoundTripper(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 206, Header: http.Header{"Content-Range": {"bytes 10-19/100"}}, ContentLength: -1, Body: io.NopCloser(strings.NewReader("short"))}, nil
		})}}
	defer func() {
		if got := recover(); got != http.ErrAbortHandler {
			t.Errorf("short range marked complete: %v", got)
		}
	}()
	req := httptest.NewRequest("GET", "http://watchtower/file", nil)
	req.Header.Set("Range", "bytes=10-19")
	s.Serve(httptest.NewRecorder(), req, f)
}

func TestRefreshDoesNotCacheURLAgainstReplacedSource(t *testing.T) {
	st, err := store.Open(t.TempDir() + "/state.json")
	if err != nil {
		t.Fatal(err)
	}
	f := &model.File{ID: "file", Provider: "test", ProviderItemID: "old"}
	if err := st.AddFiles(f); err != nil {
		t.Fatal(err)
	}
	p := &blockingLinkProvider{url: "https://cdn.example/old", started: make(chan struct{}), release: make(chan struct{})}
	s := &Streamer{Store: st, Providers: map[string]debrid.Provider{"test": p}, TTL: time.Hour}
	done := make(chan error, 1)
	go func() { _, err := s.url(context.Background(), f, false); done <- err }()
	<-p.started
	replacement := *f
	replacement.ProviderItemID = "new"
	if _, err := st.ReplaceFileSource(f.ID, &replacement); err != nil {
		t.Fatal(err)
	}
	close(p.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	got, _ := st.File(f.ID)
	if got.StreamURL != "" {
		t.Fatalf("old URL cached for new source: %+v", got)
	}
}

func TestMaterializeUsesEpisodeFilenameBeforePackDirectory(t *testing.T) {
	r := &Resolver{}
	files := r.materialize(&model.Media{ID: 1, Type: "tv", Title: "Show", Seasons: []int{1}}, "1080p", "test", model.Release{}, model.Resolved{ItemID: "1", Files: []model.RemoteFile{
		{ID: "1", Name: "Show.S01E01-E10/Show.S01E02.mkv", Size: 100},
		{ID: "2", Name: "Show.S01E01-E10/Show.S01E03/video.mkv", Size: 100},
	}})
	if len(files) != 2 || !strings.Contains(files[0].Path, "S01E02") || !strings.Contains(files[1].Path, "S01E03") {
		t.Fatalf("pack folder overrode episode identity: %+v", files)
	}
}

func TestMultipartRangeResponseRemainsSupported(t *testing.T) {
	req := httptest.NewRequest("GET", "http://watchtower/file", nil)
	req.Header.Set("Range", "bytes=0-9,90-99")
	resp := &http.Response{Header: http.Header{"Content-Type": {"multipart/byteranges; boundary=media"}}, ContentLength: -1}
	if reason := invalidPartialResponse(req, resp, &model.File{Size: 100}); reason != "" {
		t.Fatalf("valid multipart rejected: %s", reason)
	}
}

func TestLinkAPICooldownPreservesCachedPlayback(t *testing.T) {
	st, err := store.Open(t.TempDir() + "/state.json")
	if err != nil {
		t.Fatal(err)
	}
	active := &model.File{ID: "active", Provider: "test", Size: 5, StreamURL: "https://cdn.example/video", StreamExpiresAt: time.Now().Add(time.Hour)}
	missing := &model.File{ID: "missing", Provider: "test"}
	if err := st.AddFiles(active, missing); err != nil {
		t.Fatal(err)
	}
	p := &rateLimitedLinkProvider{}
	downloads := 0
	s := &Streamer{Store: st, Providers: map[string]debrid.Provider{"test": p}, TTL: time.Hour,
		Client: &http.Client{Transport: streamRoundTripper(func(*http.Request) (*http.Response, error) {
			downloads++
			return &http.Response{StatusCode: 200, Header: make(http.Header), ContentLength: 5, Body: io.NopCloser(strings.NewReader("video"))}, nil
		})}}
	request := func(f *model.File) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		s.Serve(w, httptest.NewRequest("GET", "http://watchtower/file", nil), f)
		return w
	}
	if w := request(missing); w.Code != 429 {
		t.Fatalf("missing link: %d", w.Code)
	}
	if w := request(active); w.Code != 200 || w.Body.String() != "video" {
		t.Fatalf("cached playback: %d %s", w.Code, w.Body.String())
	}
	if w := request(missing); w.Code != 429 {
		t.Fatalf("API cooldown: %d", w.Code)
	}
	if p.calls != 1 || downloads != 1 {
		t.Fatalf("calls: API=%d downloads=%d", p.calls, downloads)
	}
	s.mu.Lock()
	s.linkRateLimitedUntil["test"] = time.Now().Add(-time.Second)
	s.mu.Unlock()
	request(missing)
	if p.calls != 2 {
		t.Fatalf("API did not recover after cooldown: %d", p.calls)
	}
}

func TestFailedLinkBackoffIsPerSourceAndRecovers(t *testing.T) {
	st, err := store.Open(t.TempDir() + "/state.json")
	if err != nil {
		t.Fatal(err)
	}
	f := &model.File{ID: "failed", Provider: "test"}
	other := &model.File{ID: "other", Provider: "test"}
	if err := st.AddFiles(f, other); err != nil {
		t.Fatal(err)
	}
	p := &transientLinkProvider{failures: 3, url: "https://cdn.example/video"}
	s := &Streamer{Store: st, Providers: map[string]debrid.Provider{"test": p}, TTL: time.Hour, RetryBackoff: time.Nanosecond}
	w := httptest.NewRecorder()
	s.Serve(w, httptest.NewRequest("GET", "http://watchtower/file", nil), f)
	if w.Code != 502 || p.calls != 3 {
		t.Fatalf("initial retries: status=%d calls=%d", w.Code, p.calls)
	}
	w = httptest.NewRecorder()
	s.Serve(w, httptest.NewRequest("GET", "http://watchtower/file", nil), f)
	if w.Code != 503 || w.Header().Get("Retry-After") == "" || p.calls != 3 {
		t.Fatalf("backoff: status=%d calls=%d", w.Code, p.calls)
	}
	if _, err := s.url(context.Background(), other, false); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	failure := s.linkFailures[f.ID]
	failure.until = time.Now().Add(-time.Second)
	s.linkFailures[f.ID] = failure
	s.mu.Unlock()
	if _, err := s.url(context.Background(), f, false); err != nil {
		t.Fatal(err)
	}
	if len(s.linkFailures) != 0 {
		t.Fatal("successful refresh retained failures")
	}

	// A durable replacement must not inherit the previous source's failure.
	s.linkFailures[f.ID] = streamLinkFailure{source: streamSource(f), count: 3, until: time.Now().Add(time.Minute)}
	replacement := *f
	replacement.ProviderItemID = "replacement"
	if _, err := st.ReplaceFileSource(f.ID, &replacement); err != nil {
		t.Fatal(err)
	}
	if _, err := s.url(context.Background(), f, false); err != nil {
		t.Fatal(err)
	}
}
