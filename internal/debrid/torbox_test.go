package debrid

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/LTAGROUP/watchtower/internal/model"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestTorBoxStreamURLUsesPermanentRedirectLink(t *testing.T) {
	called := false
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		called = true
		return nil, errors.New("unexpected request")
	})}
	provider := &TorBox{Token: "token with spaces", Client: client}
	got, err := provider.StreamURL(context.Background(), &model.File{ProviderItemID: "1", ProviderFileID: "2"})
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(got)
	if err != nil {
		t.Fatal(err)
	}
	query := parsed.Query()
	if query.Get("token") != provider.Token || query.Get("torrent_id") != "1" || query.Get("file_id") != "2" || query.Get("redirect") != "true" {
		t.Fatalf("unexpected permanent link: %s", got)
	}
	if called {
		t.Fatal("stream URL generation made an API request")
	}
}

func TestTorBoxClassifiesCacheCheckRateLimitsSeparately(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusTooManyRequests,
			Status:     http.StatusText(http.StatusTooManyRequests),
			Header:     http.Header{"Retry-After": []string{"7"}},
			Body:       io.NopCloser(strings.NewReader(`{"detail":"rate limit exceeded"}`)),
		}, nil
	})}
	provider := &TorBox{Token: "token", Client: client}
	_, err := provider.Resolve(context.Background(), model.Release{InfoHash: "abc123"})
	if !errors.Is(err, ErrRateLimited) {
		t.Fatalf("expected cache-check rate-limit error, got %v", err)
	}
	if delay := RateLimitDelay(err); delay < 6*time.Second || delay > 7*time.Second {
		t.Fatalf("expected Retry-After to be preserved, got %s", delay)
	}
}

func TestTorBoxCachedOnlyResolutionRequiresHash(t *testing.T) {
	calls := 0
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls++
		return nil, errors.New("unexpected request")
	})}
	provider := &TorBox{Token: "token", Client: client}
	_, err := provider.Resolve(context.Background(), model.Release{DownloadURL: "magnet:?xt=urn:btih:abc"})
	if err == nil || !strings.Contains(err.Error(), "requires an info hash") {
		t.Fatalf("expected missing-hash error, got %v", err)
	}
	if calls != 0 {
		t.Fatalf("cached-only resolution made %d API calls without a hash", calls)
	}
}

func TestTorBoxGuardPacesRequestsAndSharesCooldown(t *testing.T) {
	guard := NewTorBoxGuard(10*time.Millisecond, time.Minute, time.Minute)
	if err := guard.Wait(context.Background(), "checkcached", false); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if err := guard.Wait(context.Background(), "checkcached", false); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed < 9*time.Millisecond {
		t.Fatalf("requests were not paced, elapsed=%s", elapsed)
	}

	guard.Block("checkcached", time.Minute)
	started = time.Now()
	err := guard.Wait(context.Background(), "checkcached", false)
	if !errors.Is(err, ErrRateLimited) {
		t.Fatalf("expected shared cooldown error, got %v", err)
	}
	if time.Since(started) > 100*time.Millisecond {
		t.Fatalf("cooldown check blocked instead of failing fast")
	}
}

func TestProviderGuardSharesProviderUnavailableCooldown(t *testing.T) {
	guard := NewProviderGuard(time.Minute)
	guard.Block(2 * time.Second)
	started := time.Now()
	err := guard.Wait(context.Background(), "alldebrid API")
	if !errors.Is(err, ErrProviderUnavailable) {
		t.Fatalf("expected provider unavailable error, got %v", err)
	}
	if time.Since(started) > 100*time.Millisecond {
		t.Fatalf("provider cooldown check blocked instead of failing fast")
	}
}

func TestAllDebridClassifies503AndBlocksSharedGuard(t *testing.T) {
	calls := 0
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls++
		return &http.Response{
			StatusCode: http.StatusServiceUnavailable,
			Status:     http.StatusText(http.StatusServiceUnavailable),
			Header:     http.Header{"Retry-After": []string{"7"}},
			Body:       io.NopCloser(strings.NewReader("temporarily unavailable")),
		}, nil
	})}
	guard := NewProviderGuard(time.Minute)
	provider := &AllDebrid{Token: "token", Client: client, Guard: guard}
	_, err := provider.StreamURL(context.Background(), &model.File{ProviderFileID: "file"})
	if !errors.Is(err, ErrProviderUnavailable) {
		t.Fatalf("expected provider unavailable error, got %v", err)
	}
	if delay, ok := ProviderCooldown(err); !ok || delay < 6*time.Second || delay > 7*time.Second {
		t.Fatalf("expected Retry-After to be preserved, ok=%v delay=%s", ok, delay)
	}
	if calls != 1 {
		t.Fatalf("expected one API call, got %d", calls)
	}
	_, err = (&AllDebrid{Token: "token", Client: client, Guard: guard}).StreamURL(context.Background(), &model.File{ProviderFileID: "file"})
	if !errors.Is(err, ErrProviderUnavailable) || calls != 1 {
		t.Fatalf("expected shared cooldown to prevent second API call, calls=%d err=%v", calls, err)
	}
}
