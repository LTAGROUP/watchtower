package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/LTAGROUP/watchtower/internal/config"
	"github.com/LTAGROUP/watchtower/internal/debrid"
	"github.com/LTAGROUP/watchtower/internal/model"
	"github.com/LTAGROUP/watchtower/internal/store"
)

type Streamer struct {
	Store           *store.Store
	Providers       map[string]debrid.Provider
	ProviderFactory func(config.Config) map[string]debrid.Provider
	Settings        func() config.Config
	Repair          func(context.Context, *model.File) (*model.File, error)
	Client          *http.Client
	TTL             time.Duration
	RetryBackoff    time.Duration
	Log             *slog.Logger
	mu              sync.Mutex
}

func (s *Streamer) Serve(w http.ResponseWriter, r *http.Request, f *model.File) {
	const maxAttempts = 3
	started := time.Now()
	if s.Log != nil {
		s.Log.Info("stream request started", "component", "stream", "file", f.Path, "provider", f.Provider, "method", r.Method, "range", r.Header.Get("Range"))
	}
	for attempt := 0; attempt < maxAttempts; attempt++ {
		u, e := s.url(r.Context(), f, attempt > 0)
		if e != nil {
			willRetry := errors.Is(e, debrid.ErrTransient) && attempt+1 < maxAttempts
			if s.Log != nil {
				attrs := []any{"component", "stream", "file", f.Path, "provider", f.Provider, "attempt", attempt + 1, "will_retry", willRetry, "error", e}
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
			http.Error(w, e.Error(), http.StatusBadGateway)
			return
		}
		req, _ := http.NewRequestWithContext(r.Context(), r.Method, u, nil)
		for _, h := range []string{"Range", "If-Range", "If-Modified-Since", "If-None-Match", "User-Agent"} {
			req.Header.Set(h, r.Header.Get(h))
		}
		resp, e := s.Client.Do(req)
		if e != nil {
			if s.Log != nil {
				s.Log.Warn("stream upstream request failed", "component", "stream", "file", f.Path, "provider", f.Provider, "attempt", attempt+1, "will_refresh", attempt+1 < maxAttempts, "error", e)
			}
			if attempt+1 < maxAttempts {
				continue
			}
			http.Error(w, e.Error(), http.StatusBadGateway)
			return
		}
		if retryableStatus(resp.StatusCode) {
			resp.Body.Close()
			if s.Log != nil {
				s.Log.Warn("stream link rejected by upstream", "component", "stream", "file", f.Path, "provider", f.Provider, "attempt", attempt+1, "status", resp.Status, "will_refresh", attempt+1 < maxAttempts)
			}
			if attempt+1 < maxAttempts {
				continue
			}
			http.Error(w, fmt.Sprintf("provider stream unavailable after %d attempts (%s)", maxAttempts, resp.Status), http.StatusBadGateway)
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
			attrs := []any{"component", "stream", "file", f.Path, "provider", f.Provider, "status", resp.Status, "bytes", written, "attempts", attempt + 1, "duration", time.Since(started).String()}
			if e != nil {
				attrs = append(attrs, "error", e)
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
		u := current.StreamURL
		expiresAt := current.StreamExpiresAt
		s.mu.Unlock()
		if s.Log != nil {
			s.Log.Debug("using cached stream link", "component", "stream", "file", current.Path, "provider", current.Provider, "expires_in", time.Until(expiresAt).Round(time.Second).String())
		}
		return u, nil
	}
	s.mu.Unlock()
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
			s.Log.Warn("stream source is stale; attempting automatic repair", "component", "stream", "file", current.Path, "provider", current.Provider, "error", e)
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
			s.Log.Warn("stream link refresh failed", "component", "stream", "file", current.Path, "provider", current.Provider, "reason", reason, "error", e)
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
