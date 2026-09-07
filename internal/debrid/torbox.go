package debrid

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/LTAGROUP/watchtower/internal/model"
)

type TorBox struct {
	Token         string
	Client        *http.Client
	AllowUncached bool
	Poll          time.Duration
	Guard         *TorBoxGuard
}

func (t *TorBox) Name() string { return "torbox" }
func (t *TorBox) Resolve(ctx context.Context, r model.Release) (model.Resolved, error) {
	if !t.AllowUncached {
		if strings.TrimSpace(r.InfoHash) == "" {
			return model.Resolved{}, fmt.Errorf("cached-only resolution requires an info hash")
		}
		ok, err := t.cached(ctx, r.InfoHash)
		if err != nil {
			return model.Resolved{}, err
		}
		if !ok {
			return model.Resolved{}, fmt.Errorf("not cached")
		}
	}
	if err := t.waitRequest(ctx, "createtorrent", t.AllowUncached); err != nil {
		return model.Resolved{}, err
	}
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	if len(r.TorrentData) > 0 {
		p, _ := mw.CreateFormFile("file", "release.torrent")
		_, _ = p.Write(r.TorrentData)
	} else {
		_ = mw.WriteField("magnet", r.DownloadURL)
	}
	_ = mw.WriteField("seed", "3")
	_ = mw.Close()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.torbox.app/v1/api/torrents/createtorrent", &buf)
	req.Header.Set("Authorization", "Bearer "+t.Token)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	resp, err := t.Client.Do(req)
	if err != nil {
		return model.Resolved{}, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if resp.StatusCode == http.StatusTooManyRequests {
		return model.Resolved{}, t.rateLimited("createtorrent", resp, string(b))
	}
	if resp.StatusCode/100 != 2 {
		return model.Resolved{}, fmt.Errorf("torbox create: %s: %s", resp.Status, string(b))
	}
	var raw map[string]any
	if err = json.Unmarshal(b, &raw); err != nil {
		return model.Resolved{}, err
	}
	id := num(object(raw["data"])["torrent_id"])
	if id == 0 {
		id = num(raw["data"])
	}
	if id == 0 {
		return model.Resolved{}, fmt.Errorf("torbox create returned no torrent id")
	}
	return t.wait(ctx, id)
}
func (t *TorBox) cached(ctx context.Context, hash string) (bool, error) {
	if err := t.waitRequest(ctx, "checkcached", false); err != nil {
		return false, err
	}
	u := "https://api.torbox.app/v1/api/torrents/checkcached?format=object&list_files=true&hash=" + url.QueryEscape(hash)
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	req.Header.Set("Authorization", "Bearer "+t.Token)
	resp, e := t.Client.Do(req)
	if e != nil {
		return false, e
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusTooManyRequests {
		return false, t.rateLimited("checkcached", resp, string(b))
	}
	if resp.StatusCode/100 != 2 {
		return false, fmt.Errorf("torbox cache check: %s", resp.Status)
	}
	var raw map[string]any
	if err := json.Unmarshal(b, &raw); err != nil {
		return false, err
	}
	if success, _ := raw["success"].(bool); !success {
		return false, fmt.Errorf("torbox cache check failed: %s", torboxMessage(b))
	}
	for key, value := range object(raw["data"]) {
		if strings.EqualFold(key, strings.TrimSpace(hash)) && len(object(value)) > 0 {
			return true, nil
		}
	}
	return false, nil
}
func (t *TorBox) wait(ctx context.Context, id int64) (model.Resolved, error) {
	p := t.Poll
	if p <= 0 {
		p = 3 * time.Second
	}
	tick := time.NewTicker(p)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return model.Resolved{}, ctx.Err()
		case <-tick.C:
			if err := t.waitRequest(ctx, "mylist", false); err != nil {
				return model.Resolved{}, err
			}
			u := "https://api.torbox.app/v1/api/torrents/mylist?id=" + strconv.FormatInt(id, 10) + "&bypass_cache=true"
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
			req.Header.Set("Authorization", "Bearer "+t.Token)
			resp, e := t.Client.Do(req)
			if e != nil {
				continue
			}
			if resp.StatusCode == http.StatusTooManyRequests {
				body, _ := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
				resp.Body.Close()
				return model.Resolved{}, t.rateLimited("mylist", resp, string(body))
			}
			if resp.StatusCode/100 != 2 {
				resp.Body.Close()
				continue
			}
			var raw map[string]any
			e = json.NewDecoder(resp.Body).Decode(&raw)
			resp.Body.Close()
			if e != nil || raw["success"] != true {
				continue
			}
			data := raw["data"]
			m := object(data)
			if len(m) == 0 {
				a := array(data)
				if len(a) > 0 {
					m = object(a[0])
				}
			}
			state := strings.ToLower(str(m["download_state"]))
			if state == "failed" || state == "error" {
				return model.Resolved{}, fmt.Errorf("torbox torrent failed")
			}
			files := array(m["files"])
			// Torrent metadata includes file names while bytes are still being
			// downloaded. Only publish files once the provider can serve them.
			present, _ := m["download_present"].(bool)
			finished, _ := m["download_finished"].(bool)
			if len(files) > 0 && present && finished {
				out := model.Resolved{ItemID: strconv.FormatInt(id, 10), Cached: true}
				for _, v := range files {
					f := object(v)
					out.Files = append(out.Files, model.RemoteFile{ID: fmt.Sprint(f["id"]), Name: str(f["name"]), Size: num(f["size"])})
				}
				return out, nil
			}
		}
	}
}
func (t *TorBox) StreamURL(ctx context.Context, f *model.File) (string, error) {
	if strings.TrimSpace(f.ProviderItemID) == "" || strings.TrimSpace(f.ProviderFileID) == "" {
		return "", fmt.Errorf("torbox stream link requires torrent and file IDs")
	}
	if err := t.waitRequest(ctx, "requestdl", false); err != nil {
		return "", err
	}
	// Resolve the permanent requestdl permalink once per Streamer cache period.
	// Passing that permalink through for every byte range makes every Plex/rclone
	// read hit TorBox's API and can trigger its request limit. The final CDN URL
	// is short-lived, so Streamer refreshes it when it expires or is rejected.
	values := url.Values{}
	values.Set("token", t.Token)
	values.Set("torrent_id", f.ProviderItemID)
	values.Set("file_id", f.ProviderFileID)
	values.Set("redirect", "true")
	endpoint := "https://api.torbox.app/v1/api/torrents/requestdl?" + values.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", fmt.Errorf("torbox request download: %w", err)
	}
	req.Header.Set("Cache-Control", "no-cache")

	client := t.Client
	if client == nil {
		client = http.DefaultClient
	}
	resolver := *client
	resolver.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}
	resp, err := resolver.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		return "", fmt.Errorf("%w: torbox request download: %v", ErrTransient, err)
	}
	defer resp.Body.Close()

	if isRedirectStatus(resp.StatusCode) {
		location := strings.TrimSpace(resp.Header.Get("Location"))
		if !validHTTPURL(location) {
			return "", fmt.Errorf("%w: torbox request download returned invalid redirect", ErrTransient)
		}
		return location, nil
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return "", fmt.Errorf("%w: torbox request download response: %v", ErrTransient, err)
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		return "", t.rateLimited("requestdl", resp, string(body))
	}
	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusGone {
		return "", fmt.Errorf("%w: torbox download item returned %s", ErrStaleItem, resp.Status)
	}
	if transientHTTPStatus(resp.StatusCode) {
		return "", fmt.Errorf("%w: torbox request download returned %s: %s", ErrTransient, resp.Status, strings.TrimSpace(string(body)))
	}
	if resp.StatusCode/100 != 2 {
		detail := torboxMessage(body)
		if staleTorboxMessage(detail) {
			return "", fmt.Errorf("%w: %s", ErrStaleItem, detail)
		}
		if strings.Contains(strings.ToLower(detail), "rate limit") {
			return "", t.rateLimited("requestdl", resp, detail)
		}
		if transientTorboxMessage(detail) {
			return "", fmt.Errorf("%w: %s", ErrTransient, detail)
		}
		return "", fmt.Errorf("torbox request download: %s: %s", resp.Status, detail)
	}

	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err == nil {
		d := raw["data"]
		if s := str(d); validHTTPURL(s) {
			return s, nil
		}
		m := object(d)
		for _, key := range []string{"download_link", "link", "url"} {
			if s := str(m[key]); validHTTPURL(s) {
				return s, nil
			}
		}
	}
	if s := strings.TrimSpace(string(body)); validHTTPURL(s) {
		return s, nil
	}
	detail := torboxMessage(body)
	if staleTorboxMessage(detail) {
		return "", fmt.Errorf("%w: %s", ErrStaleItem, detail)
	}
	if strings.Contains(strings.ToLower(detail), "rate limit") {
		return "", t.rateLimited("requestdl", resp, detail)
	}
	if transientTorboxMessage(detail) {
		return "", fmt.Errorf("%w: %s", ErrTransient, detail)
	}
	return "", fmt.Errorf("%w: torbox request download returned no stream URL", ErrTransient)
}

func validHTTPURL(raw string) bool {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed == nil || parsed.Host == "" || parsed.Hostname() == "" {
		return false
	}
	scheme := strings.ToLower(parsed.Scheme)
	return scheme == "http" || scheme == "https"
}

func isRedirectStatus(status int) bool {
	switch status {
	case http.StatusMovedPermanently, http.StatusFound, http.StatusSeeOther, http.StatusTemporaryRedirect, http.StatusPermanentRedirect:
		return true
	default:
		return false
	}
}

func (t *TorBox) waitRequest(ctx context.Context, endpoint string, uncachedCreate bool) error {
	if t.Guard == nil {
		return nil
	}
	return t.Guard.Wait(ctx, endpoint, uncachedCreate)
}

func (t *TorBox) rateLimited(endpoint string, resp *http.Response, detail string) error {
	delay := retryAfter(resp.Header.Get("Retry-After"))
	if t.Guard != nil {
		t.Guard.Block(endpoint, delay)
	}
	return NewRateLimitError("torbox "+endpoint, delay, strings.TrimSpace(detail))
}

func transientHTTPStatus(status int) bool {
	return status == http.StatusRequestTimeout || status == http.StatusTooEarly || status >= 500
}

func torboxMessage(body []byte) string {
	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err == nil {
		for _, key := range []string{"detail", "error", "message"} {
			if value := strings.TrimSpace(fmt.Sprint(raw[key])); value != "" && value != "<nil>" {
				return value
			}
		}
	}
	return strings.TrimSpace(string(body))
}

func staleTorboxMessage(message string) bool {
	message = strings.ToLower(message)
	for _, fragment := range []string{"not found", "does not exist", "no longer", "invalid torrent", "invalid file"} {
		if strings.Contains(message, fragment) {
			return true
		}
	}
	return false
}

func transientTorboxMessage(message string) bool {
	message = strings.ToLower(message)
	for _, fragment := range []string{"bad gateway", "gateway timeout", "service unavailable", "internal server error", "temporarily unavailable", "try again", "timed out", "timeout", "rate limit", "invalid presigned token"} {
		if strings.Contains(message, fragment) {
			return true
		}
	}
	return false
}
