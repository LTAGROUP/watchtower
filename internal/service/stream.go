package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/LTAGROUP/watchtower/internal/config"
	"github.com/LTAGROUP/watchtower/internal/debrid"
	"github.com/LTAGROUP/watchtower/internal/model"
	"github.com/LTAGROUP/watchtower/internal/store"
)

type Streamer struct {
	Store            *store.Store
	Providers        map[string]debrid.Provider
	ProviderFactory  func(config.Config) map[string]debrid.Provider
	Settings         func() config.Config
	Repair           func(context.Context, *model.File) (*model.File, error)
	Client           *http.Client
	TTL              time.Duration
	RetryBackoff     time.Duration
	Log              *slog.Logger
	mu               sync.Mutex
	refreshes        map[string]*streamRefresh
	rateLimitedUntil map[string]time.Time
}

type streamRefresh struct {
	done chan struct{}
	url  string
	err  error
}

var (
	errInvalidProviderStreamURL  = errors.New("invalid provider stream URL")
	errProviderStreamUnavailable = errors.New("provider stream unavailable")
	errProviderStreamRateLimited = errors.New("provider stream rate limited")
)

func (s *Streamer) Serve(w http.ResponseWriter, r *http.Request, f *model.File) {
	const maxAttempts = 3
	started := time.Now()
	if s.Log != nil {
		s.Log.Info("stream request started", "component", "stream", "file", f.Path, "provider", f.Provider, "method", r.Method, "range", r.Header.Get("Range"))
	}
	for attempt := 0; attempt < maxAttempts; attempt++ {
		u, e := s.url(r.Context(), f, attempt > 0)
		if e != nil {
			if errors.Is(e, debrid.ErrRateLimited) {
				delay := debrid.RateLimitDelay(e)
				if delay <= 0 {
					delay = 30 * time.Second
				}
				w.Header().Set("Retry-After", retryAfterHeader(delay))
				http.Error(w, streamSafeError(e).Error(), http.StatusTooManyRequests)
				return
			}
			willRetry := retryableStreamLinkError(e) && attempt+1 < maxAttempts
			safeErr := streamSafeError(e)
			if s.Log != nil {
				attrs := []any{"component", "stream", "file", f.Path, "provider", f.Provider, "attempt", attempt + 1, "will_retry", willRetry, "error", safeErr}
				if willRetry {
					s.Log.Warn("stream link temporarily unavailable", attrs...)
				} else {
					s.Log.Error("stream link unavailable", attrs...)
				}
			}
			if willRetry {
				if !s.waitForRetry(r.Context(), attempt) {
					return
				}
				continue
			}
			http.Error(w, safeErr.Error(), http.StatusBadGateway)
			return
		}
		req, reqErr := http.NewRequestWithContext(r.Context(), r.Method, u, nil)
		if reqErr != nil {
			// Keep provider-generated URLs out of logs and response bodies. The
			// error returned by net/http includes the rejected URL, which may
			// contain signed credentials.
			requestErr := errInvalidProviderStreamURL
			if s.Log != nil {
				s.Log.Warn("stream upstream request failed", "component", "stream", "file", f.Path, "provider", f.Provider, "attempt", attempt+1, "will_refresh", attempt+1 < maxAttempts, "error", requestErr)
			}
			if attempt+1 < maxAttempts {
				continue
			}
			http.Error(w, requestErr.Error(), http.StatusBadGateway)
			return
		}
		for _, h := range []string{"Range", "If-Range", "If-Modified-Since", "If-None-Match", "User-Agent"} {
			req.Header.Set(h, r.Header.Get(h))
		}
		client := s.Client
		if client == nil {
			client = http.DefaultClient
		}
		resp, e := client.Do(req)
		if e != nil {
			transportErr := errProviderStreamUnavailable
			if s.Log != nil {
				s.Log.Warn("stream upstream request failed", "component", "stream", "file", f.Path, "provider", f.Provider, "attempt", attempt+1, "will_refresh", attempt+1 < maxAttempts, "error", transportErr)
			}
			if attempt+1 < maxAttempts {
				continue
			}
			http.Error(w, transportErr.Error(), http.StatusBadGateway)
			return
		}
		if retryableStatus(resp.StatusCode) {
			resp.Body.Close()
			if s.Log != nil {
				s.Log.Warn("stream link rejected by upstream", "component", "stream", "file", f.Path, "provider", f.Provider, "attempt", attempt+1, "status", resp.StatusCode, "will_refresh", attempt+1 < maxAttempts)
			}
			if attempt+1 < maxAttempts {
				continue
			}
			http.Error(w, fmt.Sprintf("provider stream unavailable after %d attempts", maxAttempts), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()
		for k, vs := range resp.Header {
			if hopHeader(k) {
				continue
			}
			for _, v := range vs {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(resp.StatusCode)
		var written int64
		if r.Method != http.MethodHead {
			written, e = io.Copy(w, resp.Body)
		}
		if s.Log != nil {
			attrs := []any{"component", "stream", "file", f.Path, "provider", f.Provider, "status", resp.StatusCode, "bytes", written, "attempts", attempt + 1, "duration", time.Since(started).String()}
			if e != nil {
				attrs = append(attrs, "error", errProviderStreamUnavailable)
				if clientClosedConnection(r.Context(), e) {
					s.Log.Debug("stream transfer canceled by client", attrs...)
				} else {
					s.Log.Warn("stream transfer interrupted", attrs...)
				}
			} else {
				s.Log.Info("stream request completed", attrs...)
			}
		}
		return
	}
	http.Error(w, "unable to refresh stream URL", http.StatusBadGateway)
}

func retryAfterHeader(delay time.Duration) string {
	seconds := int(delay / time.Second)
	if delay%time.Second != 0 {
		seconds++
	}
	if seconds < 1 {
		seconds = 1
	}
	return strconv.Itoa(seconds)
}

func clientClosedConnection(ctx context.Context, err error) bool {
	if ctx.Err() != nil || errors.Is(err, context.Canceled) {
		return true
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "write tcp") && (strings.Contains(message, "connection reset by peer") || strings.Contains(message, "broken pipe"))
}

func (s *Streamer) waitForRetry(ctx context.Context, attempt int) bool {
	d := s.RetryBackoff
	if d <= 0 {
		d = 500 * time.Millisecond
	}
	timer := time.NewTimer(d * time.Duration(1<<attempt))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func retryableStatus(status int) bool {
	return status == http.StatusRequestTimeout || status == http.StatusTooEarly || status == http.StatusTooManyRequests ||
		status == http.StatusUnauthorized || status == http.StatusForbidden || status == http.StatusNotFound || status >= 500
}

func retryableStreamLinkError(err error) bool {
	return errors.Is(err, debrid.ErrTransient) || errors.Is(err, debrid.ErrProviderUnavailable) || errors.Is(err, errInvalidProviderStreamURL)
}

func streamSafeError(err error) error {
	switch {
	case errors.Is(err, errInvalidProviderStreamURL):
		return errInvalidProviderStreamURL
	case errors.Is(err, debrid.ErrRateLimited):
		return errProviderStreamRateLimited
	default:
		return errProviderStreamUnavailable
	}
}

func validProviderStreamURL(raw string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed == nil {
		return "", errInvalidProviderStreamURL
	}
	scheme := strings.ToLower(parsed.Scheme)
	if (scheme != "http" && scheme != "https") || parsed.Host == "" || parsed.Hostname() == "" {
		return "", errInvalidProviderStreamURL
	}
	return parsed.String(), nil
}

func (s *Streamer) url(ctx context.Context, f *model.File, force bool) (string, error) {
	s.mu.Lock()
	current, attached := s.Store.File(f.ID)
	if !attached {
		copy := *f
		current = &copy
		if s.Log != nil {
			s.Log.Warn("stream file replaced during active request; continuing with original source", "component", "stream", "file", f.Path, "provider", f.Provider)
		}
	}
	if !force && current.StreamURL != "" && time.Now().Before(current.StreamExpiresAt) {
		if u, err := validProviderStreamURL(current.StreamURL); err == nil {
			expiresAt := current.StreamExpiresAt
			s.mu.Unlock()
			if s.Log != nil {
				s.Log.Debug("using cached stream link", "component", "stream", "file", current.Path, "provider", current.Provider, "expires_in", time.Until(expiresAt).Round(time.Second).String())
			}
			return u, nil
		}
	}
	if until := s.rateLimitedUntil[current.Provider]; time.Now().Before(until) {
		s.mu.Unlock()
		return "", debrid.NewRateLimitError(current.Provider, time.Until(until), "cooldown active")
	}
	if s.refreshes == nil {
		s.refreshes = map[string]*streamRefresh{}
	}
	if s.rateLimitedUntil == nil {
		s.rateLimitedUntil = map[string]time.Time{}
	}
	if refresh := s.refreshes[current.ID]; refresh != nil {
		s.mu.Unlock()
		select {
		case <-refresh.done:
			return refresh.url, refresh.err
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	refresh := &streamRefresh{done: make(chan struct{})}
	s.refreshes[current.ID] = refresh
	s.mu.Unlock()

	u, err := s.refreshURL(ctx, current, attached, force)
	s.mu.Lock()
	delete(s.refreshes, current.ID)
	if errors.Is(err, debrid.ErrRateLimited) {
		delay := debrid.RateLimitDelay(err)
		if delay <= 0 {
			delay = 30 * time.Second
		}
		s.rateLimitedUntil[current.Provider] = time.Now().Add(delay)
	} else if err == nil {
		delete(s.rateLimitedUntil, current.Provider)
	}
	refresh.url, refresh.err = u, err
	close(refresh.done)
	s.mu.Unlock()
	return u, err
}

func (s *Streamer) refreshURL(ctx context.Context, current *model.File, attached, force bool) (string, error) {
	reason := "missing"
	if force && current.StreamURL != "" {
		reason = "upstream rejected previous link"
	} else if force {
		reason = "retry after provider error"
	} else if current.StreamURL != "" {
		reason = "expired"
	}
	if s.Log != nil {
		s.Log.Info("stream link refresh started", "component", "stream", "file", current.Path, "provider", current.Provider, "reason", reason)
	}
	providers := s.Providers
	ttl := s.TTL
	if s.Settings != nil {
		cfg := s.Settings()
		ttl = cfg.StreamURLTTL
		if s.ProviderFactory != nil {
			providers = s.ProviderFactory(cfg)
		}
	}
	p := providers[current.Provider]
	if p == nil {
		return "", fmt.Errorf("provider %q unavailable", current.Provider)
	}
	u, e := p.StreamURL(ctx, current)
	if errors.Is(e, debrid.ErrStaleItem) && s.Repair != nil && attached {
		if s.Log != nil {
			s.Log.Warn("stream source is stale; attempting automatic repair", "component", "stream", "file", current.Path, "provider", current.Provider)
		}
		repaired, repairErr := s.Repair(ctx, current)
		if repairErr != nil {
			return "", fmt.Errorf("automatic stream repair failed: %w", repairErr)
		}
		current = repaired
		if s.Settings != nil {
			cfg := s.Settings()
			ttl = cfg.StreamURLTTL
			if s.ProviderFactory != nil {
				providers = s.ProviderFactory(cfg)
			}
		}
		p = providers[current.Provider]
		if p == nil {
			return "", fmt.Errorf("repaired provider %q unavailable", current.Provider)
		}
		u, e = p.StreamURL(ctx, current)
	}
	if e != nil {
		if s.Log != nil {
			s.Log.Warn("stream link refresh failed", "component", "stream", "file", current.Path, "provider", current.Provider, "reason", reason, "error", streamSafeError(e))
		}
		return "", e
	}
	u, e = validProviderStreamURL(u)
	if e != nil {
		if s.Log != nil {
			s.Log.Warn("stream link refresh failed", "component", "stream", "file", current.Path, "provider", current.Provider, "reason", reason, "error", errInvalidProviderStreamURL)
		}
		return "", e
	}
	expires := time.Now().Add(ttl)
	s.Store.SetStream(current.ID, u, expires)
	if s.Log != nil {
		s.Log.Info("stream link obtained", "component", "stream", "file", current.Path, "provider", current.Provider, "valid_for", ttl.String())
	}
	return u, nil
}
func hopHeader(k string) bool {
	switch strings.ToLower(k) {
	case "connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailer", "transfer-encoding", "upgrade":
		return true
	}
	return false
}
