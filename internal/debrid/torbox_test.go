package debrid

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/LTAGROUP/watchtower/internal/model"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestTorBoxClassifiesGatewayFailuresAsTransient(t *testing.T) {
	tests := []struct {
		name   string
		status int
		body   string
	}{
		{name: "gateway detail in successful response", status: http.StatusOK, body: `{"data":null,"detail":"502 Bad Gateway"}`},
		{name: "gateway HTTP status", status: http.StatusBadGateway, body: `<!doctype html>`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: test.status, Status: http.StatusText(test.status), Body: io.NopCloser(strings.NewReader(test.body)), Header: make(http.Header)}, nil
			})}
			provider := &TorBox{Token: "token", Client: client}
			_, err := provider.StreamURL(context.Background(), &model.File{ProviderItemID: "1", ProviderFileID: "2"})
			if !errors.Is(err, ErrTransient) {
				t.Fatalf("expected transient error, got %v", err)
			}
		})
	}
}

func TestTorBoxClassifiesRateLimitsSeparately(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusTooManyRequests,
			Status:     http.StatusText(http.StatusTooManyRequests),
			Body:       io.NopCloser(strings.NewReader(`{"detail":"rate limit exceeded"}`)),
			Header:     make(http.Header),
		}, nil
	})}
	provider := &TorBox{Token: "token", Client: client}
	_, err := provider.StreamURL(context.Background(), &model.File{ProviderItemID: "1", ProviderFileID: "2"})
	if !errors.Is(err, ErrRateLimited) {
		t.Fatalf("expected rate-limit error, got %v", err)
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
