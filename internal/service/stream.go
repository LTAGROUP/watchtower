package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
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
	Store                *store.Store
	Providers            map[string]debrid.Provider
	ProviderFactory      func(config.Config) map[string]debrid.Provider
	Settings             func() config.Config
	Repair               func(context.Context, *model.File) (*model.File, error)
	Client               *http.Client
	TTL                  time.Duration
	RetryBackoff         time.Duration
	Log                  *slog.Logger
	mu                   sync.Mutex
	refreshes            map[string]*streamRefresh
	rateLimitedUntil     map[string]time.Time // Video download cooldowns.
	linkRateLimitedUntil map[string]time.Time
	linkFailures         map[string]streamLinkFailure
}

// Failures are scoped to a durable source, so replacing a file bypasses backoff.
type streamLinkFailure struct {
	source model.File
	count  int
	until  time.Time
}
type streamLinkBackoff struct{ delay time.Duration }

func (e *streamLinkBackoff) Error() string { return "stream link temporarily unavailable; retry later" }
func streamSource(f *model.File) model.File {
	source := *f
	source.StreamURL = ""
	source.StreamExpiresAt = time.Time{}
	return source
}

type streamRefresh struct {
	done chan struct{}
	url  string
	file *model.File
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
		u, source, e := s.urlWithFile(r.Context(), f, attempt > 0)
		f = source
		if e != nil {
			var backoff *streamLinkBackoff
			if errors.As(e, &backoff) {
				w.Header().Set("Retry-After", retryAfterHeader(backoff.delay))
				http.Error(w, backoff.Error(), http.StatusServiceUnavailable)
				return
			}
			if errors.Is(e, debrid.ErrRateLimited) {
				delay := debrid.RateLimitDelay(e)
				if delay <= 0 {
					delay = s.rateLimitCooldown(f.Provider)
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
		// The WebDAV ETag and modification date belong to WatchTower's virtual
		// file, not the provider's representation. Forwarding those validators
		// can make a CDN ignore Range and return the entire file as 200 OK.
		for _, h := range []string{"Range", "User-Agent"} {
			req.Header.Set(h, r.Header.Get(h))
		}
		// Byte ranges refer to the original media representation. Do not let
		// the transport negotiate gzip and transparently change those bytes.
		req.Header.Set("Accept-Encoding", "identity")
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
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := parseRetryAfter(resp.Header.Get("Retry-After"))
			if delay <= 0 {
				delay = s.rateLimitCooldown(f.Provider)
			}
			resp.Body.Close()
			delay = s.blockProvider(f.Provider, delay)
			if s.Log != nil {
				s.Log.Warn("stream provider rate limited", "component", "stream", "file", f.Path, "provider", f.Provider, "status", resp.StatusCode, "retry_after", delay.Round(time.Second).String())
			}
			w.Header().Set("Retry-After", retryAfterHeader(delay))
			http.Error(w, errProviderStreamRateLimited.Error(), http.StatusTooManyRequests)
			return
		}
		if resp.StatusCode == http.StatusRequestedRangeNotSatisfiable {
			willRetry := attempt+1 < maxAttempts && shouldRetryRange(r, resp, f)
			if willRetry {
				contentRange := resp.Header.Get("Content-Range")
				resp.Body.Close()
				s.invalidateStreamURL(f.ID)
				if s.Log != nil {
					attrs := []any{"component", "stream", "file", f.Path, "provider", f.Provider, "attempt", attempt + 1, "status", resp.StatusCode, "will_refresh", true}
					if contentRange != "" {
						attrs = append(attrs, "content_range", contentRange)
					}
					s.Log.Warn("stream range rejected by upstream", attrs...)
				}
				if !s.waitForRetry(r.Context(), attempt) {
					return
				}
				continue
			}
		} else if retryableStatus(resp.StatusCode) {
			resp.Body.Close()
			willRetry := attempt+1 < maxAttempts
			if s.Log != nil {
				s.Log.Warn("stream link rejected by upstream", "component", "stream", "file", f.Path, "provider", f.Provider, "attempt", attempt+1, "status", resp.StatusCode, "will_refresh", willRetry)
			}
			if willRetry {
				if !s.waitForRetry(r.Context(), attempt) {
					return
				}
				continue
			}
			http.Error(w, fmt.Sprintf("provider stream unavailable after %d attempts", maxAttempts), http.StatusBadGateway)
			return
		}
		if resp.StatusCode == http.StatusPartialContent {
			if reason := invalidPartialResponse(r, resp, f); reason != "" {
				willRetry := attempt+1 < maxAttempts
				contentRange := resp.Header.Get("Content-Range")
				resp.Body.Close()
				s.invalidateStreamURL(f.ID)
				if s.Log != nil {
					attrs := []any{"component", "stream", "file", f.Path, "provider", f.Provider, "attempt", attempt + 1, "will_refresh", willRetry, "reason", reason}
					if contentRange != "" {
						attrs = append(attrs, "content_range", contentRange)
					}
					if willRetry {
						s.Log.Warn("stream partial response rejected", attrs...)
					} else {
						s.Log.Error("stream partial response invalid", attrs...)
					}
				}
				if willRetry {
					if !s.waitForRetry(r.Context(), attempt) {
						return
					}
					continue
				}
				http.Error(w, errProviderStreamUnavailable.Error(), http.StatusBadGateway)
				return
			}
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
		// Keep GET/HEAD validators consistent with the WebDAV listing. CDN
		// validators may change every time a signed URL is regenerated.
		w.Header().Set("ETag", fmt.Sprintf(`"%s-%d"`, f.ID, f.Size))
		modified := f.CreatedAt
		if modified.IsZero() {
			modified = time.Unix(0, 0)
		}
		w.Header().Set("Last-Modified", modified.UTC().Format(http.TimeFormat))
		w.WriteHeader(resp.StatusCode)
		var written int64
		if r.Method != http.MethodHead {
			written, e = io.Copy(w, resp.Body)
			if e == nil {
				expected := resp.ContentLength
				if resp.StatusCode == http.StatusPartialContent {
					if bounds, ok := parseContentRange(resp.Header.Get("Content-Range")); ok {
						expected = bounds.end - bounds.start + 1
					}
				} else if resp.StatusCode == http.StatusOK && f.Size > 0 {
					expected = f.Size
				}
				if expected >= 0 && written != expected {
					e = io.ErrUnexpectedEOF
				}
			}
		}
		if s.Log != nil {
			attrs := []any{"component", "stream", "file", f.Path, "provider", f.Provider, "status", resp.StatusCode, "bytes", written, "attempts", attempt + 1, "duration", time.Since(started).String()}
			if contentRange := resp.Header.Get("Content-Range"); contentRange != "" {
				attrs = append(attrs, "content_range", contentRange)
			}
			if e != nil {
				if clientClosedConnection(r.Context(), e) {
					attrs = append(attrs, "reason", "downstream closed connection")
					s.Log.Debug("stream transfer closed by downstream client", attrs...)
				} else {
					attrs = append(attrs, "error", errProviderStreamUnavailable)
					s.Log.Warn("stream transfer interrupted", attrs...)
				}
			} else if resp.StatusCode >= http.StatusBadRequest {
				s.Log.Warn("stream request returned upstream error", attrs...)
			} else {
				s.Log.Info("stream request completed", attrs...)
			}
		}
		if e != nil {
			// Headers have already been sent. Returning normally would mark a
			// chunked response complete and let rclone cache a truncated read.
			panic(http.ErrAbortHandler)
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

func parseRetryAfter(value string) time.Duration {
	value = strings.TrimSpace(value)
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	if at, err := http.ParseTime(value); err == nil {
		if delay := time.Until(at); delay > 0 {
			return delay
		}
	}
	return 0
}

func (s *Streamer) rateLimitCooldown(provider string) time.Duration {
	if s.Settings != nil {
		cfg := s.Settings()
		switch strings.ToLower(strings.TrimSpace(provider)) {
		case "torbox":
			if cfg.TorBoxRateLimitCooldown > 0 {
				return cfg.TorBoxRateLimitCooldown
			}
		case "alldebrid":
			if cfg.AllDebridProviderCooldown > 0 {
				return cfg.AllDebridProviderCooldown
			}
		}
	}
	return 30 * time.Second
}

func (s *Streamer) blockProvider(provider string, delay time.Duration) time.Duration {
	if delay <= 0 {
		delay = s.rateLimitCooldown(provider)
	}
	until := time.Now().Add(delay)
	s.mu.Lock()
	if s.rateLimitedUntil == nil {
		s.rateLimitedUntil = map[string]time.Time{}
	}
	if current := s.rateLimitedUntil[provider]; current.After(until) {
		until = current
	}
	s.rateLimitedUntil[provider] = until
	s.mu.Unlock()
	return time.Until(until)
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
	return status == http.StatusBadRequest || status == http.StatusRequestTimeout || status == http.StatusTooEarly ||
		status == http.StatusUnauthorized || status == http.StatusForbidden || status == http.StatusNotFound || status == http.StatusGone || status >= 500
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

type parsedContentRange struct {
	start       int64
	end         int64
	total       int64
	totalKnown  bool
	unsatisfied bool
}

func parseContentRange(raw string) (parsedContentRange, bool) {
	unit, value, ok := strings.Cut(strings.TrimSpace(raw), " ")
	if !ok || !strings.EqualFold(unit, "bytes") {
		return parsedContentRange{}, false
	}
	rangeValue, totalValue, ok := strings.Cut(strings.TrimSpace(value), "/")
	if !ok {
		return parsedContentRange{}, false
	}
	result := parsedContentRange{start: -1, end: -1}
	totalValue = strings.TrimSpace(totalValue)
	if totalValue != "*" {
		total, err := strconv.ParseInt(totalValue, 10, 64)
		if err != nil || total < 0 {
			return parsedContentRange{}, false
		}
		result.total = total
		result.totalKnown = true
	}
	rangeValue = strings.TrimSpace(rangeValue)
	if rangeValue == "*" {
		result.unsatisfied = true
		return result, true
	}
	startValue, endValue, ok := strings.Cut(rangeValue, "-")
	if !ok {
		return parsedContentRange{}, false
	}
	start, err := strconv.ParseInt(strings.TrimSpace(startValue), 10, 64)
	if err != nil || start < 0 {
		return parsedContentRange{}, false
	}
	end, err := strconv.ParseInt(strings.TrimSpace(endValue), 10, 64)
	if err != nil || end < start {
		return parsedContentRange{}, false
	}
	result.start, result.end = start, end
	return result, true
}

func requestedRangeStart(raw string) (int64, bool) {
	unit, value, ok := strings.Cut(strings.TrimSpace(raw), "=")
	if !ok || !strings.EqualFold(unit, "bytes") || strings.Contains(value, ",") {
		return 0, false
	}
	startValue, _, ok := strings.Cut(strings.TrimSpace(value), "-")
	if !ok || strings.TrimSpace(startValue) == "" {
		return 0, false
	}
	start, err := strconv.ParseInt(strings.TrimSpace(startValue), 10, 64)
	if err != nil || start < 0 {
		return 0, false
	}
	return start, true
}

func shouldRetryRange(r *http.Request, resp *http.Response, f *model.File) bool {
	start, ok := requestedRangeStart(r.Header.Get("Range"))
	if !ok {
		return false
	}
	if f.Size > 0 && start >= f.Size {
		return false
	}
	if contentRange, ok := parseContentRange(resp.Header.Get("Content-Range")); ok && contentRange.unsatisfied && contentRange.totalKnown {
		if (f.Size <= 0 || contentRange.total == f.Size) && start >= contentRange.total {
			return false
		}
	}
	return true
}

func invalidPartialResponse(r *http.Request, resp *http.Response, f *model.File) string {
	// A multi-range response carries Content-Range in each MIME part instead
	// of the top-level headers. Preserve that valid passthrough behavior.
	if strings.Contains(r.Header.Get("Range"), ",") {
		kind, params, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
		if err == nil && kind == "multipart/byteranges" && params["boundary"] != "" {
			return ""
		}
	}
	contentRange, ok := parseContentRange(resp.Header.Get("Content-Range"))
	if !ok || contentRange.unsatisfied {
		return "missing or invalid content range"
	}
	if contentRange.totalKnown && contentRange.end >= contentRange.total {
		return "content range extends beyond file size"
	}
	if f.Size > 0 && contentRange.end >= f.Size {
		return "content range extends beyond virtual file size"
	}
	if requestedStart, ok := requestedRangeStart(r.Header.Get("Range")); ok && contentRange.start != requestedStart {
		return "content range start does not match requested range"
	}
	if contentRange.totalKnown && f.Size > 0 && contentRange.total != f.Size {
		return fmt.Sprintf("content range total %d differs from file size %d", contentRange.total, f.Size)
	}
	if unit, value, ok := strings.Cut(strings.TrimSpace(r.Header.Get("Range")), "="); ok && strings.EqualFold(unit, "bytes") && !strings.Contains(value, ",") {
		start, end, ok := strings.Cut(strings.TrimSpace(value), "-")
		if ok && end != "" {
			if n, err := strconv.ParseInt(end, 10, 64); err == nil && n >= 0 {
				if start == "" && n > 0 && contentRange.totalKnown {
					wanted := contentRange.total - n
					if wanted < 0 {
						wanted = 0
					}
					if contentRange.start != wanted {
						return "content range does not match requested suffix"
					}
				} else if start != "" && contentRange.end > n {
					return "content range exceeds requested end"
				}
			}
		}
	}
	if resp.ContentLength >= 0 && resp.ContentLength != contentRange.end-contentRange.start+1 {
		return "content length does not match content range"
	}
	return ""
}

func (s *Streamer) invalidateStreamURL(id string) {
	if s.Store != nil {
		s.Store.SetStream(id, "", time.Time{})
	}
}

func (s *Streamer) url(ctx context.Context, f *model.File, force bool) (string, error) {
	u, _, err := s.urlWithFile(ctx, f, force)
	return u, err
}

func (s *Streamer) urlWithFile(ctx context.Context, f *model.File, force bool) (string, *model.File, error) {
	s.mu.Lock()
	current, attached := s.Store.File(f.ID)
	if !attached {
		copy := *f
		current = &copy
		if s.Log != nil {
			s.Log.Warn("stream file replaced during active request; continuing with original source", "component", "stream", "file", f.Path, "provider", f.Provider)
		}
	}
	if until := s.rateLimitedUntil[current.Provider]; time.Now().Before(until) {
		s.logCooldown(current, "video_download", until)
		s.mu.Unlock()
		return "", current, debrid.NewRateLimitError(current.Provider, time.Until(until), "cooldown active")
	}
	if !force && current.StreamURL != "" && time.Now().Before(current.StreamExpiresAt) {
		if u, err := validProviderStreamURL(current.StreamURL); err == nil {
			expiresAt := current.StreamExpiresAt
			s.mu.Unlock()
			if s.Log != nil {
				s.Log.Debug("using cached stream link", "component", "stream", "file", current.Path, "provider", current.Provider, "expires_in", time.Until(expiresAt).Round(time.Second).String())
			}
			return u, current, nil
		}
	}
	// API limits only block generating new links, never an existing valid URL.
	if until := s.linkRateLimitedUntil[current.Provider]; time.Now().Before(until) {
		s.logCooldown(current, "link_generation", until)
		s.mu.Unlock()
		return "", current, debrid.NewRateLimitError(current.Provider, time.Until(until), "link API cooldown active")
	}
	if failure, ok := s.linkFailures[current.ID]; ok {
		if failure.source != streamSource(current) {
			delete(s.linkFailures, current.ID)
		} else if failure.count >= 3 && time.Now().Before(failure.until) {
			s.logCooldown(current, "file_link_backoff", failure.until)
			s.mu.Unlock()
			return "", current, &streamLinkBackoff{delay: time.Until(failure.until)}
		}
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
			// A Plex probe can cancel while another request is waiting for its
			// refresh. That cancellation must not fail the surviving request.
			if ctx.Err() == nil && (errors.Is(refresh.err, context.Canceled) || errors.Is(refresh.err, context.DeadlineExceeded)) {
				return s.urlWithFile(ctx, f, force)
			}
			return refresh.url, refresh.file, refresh.err
		case <-ctx.Done():
			return "", current, ctx.Err()
		}
	}
	refresh := &streamRefresh{done: make(chan struct{})}
	s.refreshes[current.ID] = refresh
	s.mu.Unlock()

	refreshID := current.ID
	u, err := s.refreshURL(ctx, current, attached, force)
	s.mu.Lock()
	delete(s.refreshes, refreshID)
	if errors.Is(err, debrid.ErrRateLimited) {
		delay := debrid.RateLimitDelay(err)
		if delay <= 0 {
			delay = s.rateLimitCooldown(current.Provider)
		}
		until := time.Now().Add(delay)
		scope := "link_generation"
		if repairEndpointRateLimit(err) {
			// Torrent listing/creation limits during repair do not limit requestdl.
			scope = "file_link_backoff"
			if s.linkFailures == nil {
				s.linkFailures = map[string]streamLinkFailure{}
			}
			s.linkFailures[current.ID] = streamLinkFailure{source: streamSource(current), count: 3, until: until}
		} else {
			if s.linkRateLimitedUntil == nil {
				s.linkRateLimitedUntil = map[string]time.Time{}
			}
			if until.After(s.linkRateLimitedUntil[current.Provider]) {
				s.linkRateLimitedUntil[current.Provider] = until
			}
		}
		// Record the first rejection too, not just subsequent requests that
		// encounter the cooldown. Provider error details can contain signed URLs.
		if s.Log != nil {
			s.Log.Warn("stream link request rate limited", "component", "stream", "file", current.Path,
				"provider", current.Provider, "cooldown_scope", scope, "retry_after", retryAfterHeader(delay))
		}
	} else if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		if s.linkFailures == nil {
			s.linkFailures = map[string]streamLinkFailure{}
		}
		// Prune old entries so removed library files cannot accumulate forever.
		for id, failure := range s.linkFailures {
			if !failure.until.After(time.Now()) {
				delete(s.linkFailures, id)
			}
		}
		failure := s.linkFailures[current.ID]
		source := streamSource(current)
		if failure.source != source {
			failure = streamLinkFailure{source: source}
		}
		failure.count++
		failure.until = time.Now().Add(30 * time.Second)
		s.linkFailures[current.ID] = failure
	} else if err == nil {
		delete(s.linkFailures, current.ID)
	}
	// Neither successful refreshes nor API failures erase download cooldowns.
	refresh.url, refresh.err = u, err
	refresh.file = current
	close(refresh.done)
	s.mu.Unlock()
	return u, current, err
}

// Only known non-playback endpoints may bypass the shared link cooldown.
func repairEndpointRateLimit(err error) bool {
	var limited *debrid.RateLimitError
	if !errors.As(err, &limited) {
		return false
	}
	switch limited.Endpoint {
	case "torbox mylist", "torbox createtorrent", "torbox checkcached":
		return true
	default:
		return false
	}
}

func (s *Streamer) logCooldown(f *model.File, scope string, until time.Time) {
	if s.Log != nil {
		s.Log.Info("stream request deferred", "component", "stream", "file", f.Path,
			"provider", f.Provider, "cooldown_scope", scope,
			"retry_after", retryAfterHeader(time.Until(until)), "cached_link", f.StreamURL != "",
			"link_unexpired", time.Now().Before(f.StreamExpiresAt))
	}
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
		*current = *repaired
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
			attrs := []any{"component", "stream", "file", current.Path, "provider", current.Provider, "reason", reason, "error", streamSafeError(e)}
			var limited *debrid.RateLimitError
			if errors.As(e, &limited) {
				origin := "provider_response"
				if limited.Detail == "cooldown active" || limited.Detail == "link API cooldown active" {
					origin = "local_cooldown"
				}
				scope := "link_generation"
				if repairEndpointRateLimit(e) {
					scope = "source_repair"
				}
				attrs = append(attrs, "rate_limit_source", origin, "cooldown_scope", scope, "retry_after", debrid.RateLimitDelay(e).Round(time.Second).String())
			}
			s.Log.Warn("stream link refresh failed", attrs...)
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
	s.Store.SetStreamForSource(current, u, expires)
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
