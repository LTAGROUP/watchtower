package service

import (
	"context"
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
	Client          *http.Client
	TTL             time.Duration
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
			if s.Log != nil {
				s.Log.Error("stream link unavailable", "component", "stream", "file", f.Path, "provider", f.Provider, "attempt", attempt+1, "error", e)
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
				s.Log.Warn("stream transfer interrupted", attrs...)
			} else {
				s.Log.Info("stream request completed", attrs...)
			}
		}
		return
	}
	http.Error(w, "unable to refresh stream URL", http.StatusBadGateway)
}
func retryableStatus(status int) bool {
	return status == http.StatusRequestTimeout || status == http.StatusTooEarly || status == http.StatusTooManyRequests ||
		status == http.StatusUnauthorized || status == http.StatusForbidden || status == http.StatusNotFound || status >= 500
}
func (s *Streamer) url(ctx context.Context, f *model.File, force bool) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.Store.File(f.ID)
	if !ok {
		return "", fmt.Errorf("file disappeared")
	}
	if !force && current.StreamURL != "" && time.Now().Before(current.StreamExpiresAt) {
		if s.Log != nil {
			s.Log.Debug("using cached stream link", "component", "stream", "file", current.Path, "provider", current.Provider, "expires_in", time.Until(current.StreamExpiresAt).Round(time.Second).String())
		}
		return current.StreamURL, nil
	}
	reason := "missing"
	if force {
		reason = "upstream rejected previous link"
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
