package service

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/LTAGROUP/watchtower/internal/debrid"
	"github.com/LTAGROUP/watchtower/internal/model"
	"github.com/LTAGROUP/watchtower/internal/store"
)

type Streamer struct {
	Store     *store.Store
	Providers map[string]debrid.Provider
	Client    *http.Client
	TTL       time.Duration
	mu        sync.Mutex
}

func (s *Streamer) Serve(w http.ResponseWriter, r *http.Request, f *model.File) {
	const maxAttempts = 3
	for attempt := 0; attempt < maxAttempts; attempt++ {
		u, e := s.url(r.Context(), f, attempt > 0)
		if e != nil {
			http.Error(w, e.Error(), http.StatusBadGateway)
			return
		}
		req, _ := http.NewRequestWithContext(r.Context(), r.Method, u, nil)
		for _, h := range []string{"Range", "If-Range", "If-Modified-Since", "If-None-Match", "User-Agent"} {
			req.Header.Set(h, r.Header.Get(h))
		}
		resp, e := s.Client.Do(req)
		if e != nil {
			if attempt+1 < maxAttempts {
				continue
			}
			http.Error(w, e.Error(), http.StatusBadGateway)
			return
		}
		if retryableStatus(resp.StatusCode) {
			resp.Body.Close()
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
		if r.Method != http.MethodHead {
			_, _ = io.Copy(w, resp.Body)
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
		return current.StreamURL, nil
	}
	p := s.Providers[current.Provider]
	if p == nil {
		return "", fmt.Errorf("provider %q unavailable", current.Provider)
	}
	u, e := p.StreamURL(ctx, current)
	if e != nil {
		return "", e
	}
	s.Store.SetStream(current.ID, u, time.Now().Add(s.TTL))
	return u, nil
}
func hopHeader(k string) bool {
	switch strings.ToLower(k) {
	case "connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailer", "transfer-encoding", "upgrade":
		return true
	}
	return false
}
