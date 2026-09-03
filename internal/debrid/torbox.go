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
	s := strings.ToLower(string(b))
	return strings.Contains(s, strings.ToLower(hash)) && !strings.Contains(s, "\"data\":{}"), nil
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
			var raw map[string]any
			e = json.NewDecoder(resp.Body).Decode(&raw)
			resp.Body.Close()
			if e != nil {
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
			if len(files) > 0 {
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
func (t *TorBox) StreamURL(_ context.Context, f *model.File) (string, error) {
	if strings.TrimSpace(f.ProviderItemID) == "" || strings.TrimSpace(f.ProviderFileID) == "" {
		return "", fmt.Errorf("torbox stream link requires torrent and file IDs")
	}
	// TorBox's redirect=true request URL is permanent. Returning it directly
	// avoids generating a short-lived CDN URL for every Plex probe, seek, and
	// playback retry. The Streamer follows the redirect while forwarding the
	// caller's Range headers to the resulting file host.
	values := url.Values{}
	values.Set("token", t.Token)
	values.Set("torrent_id", f.ProviderItemID)
	values.Set("file_id", f.ProviderFileID)
	values.Set("redirect", "true")
	return "https://api.torbox.app/v1/api/torrents/requestdl?" + values.Encode(), nil
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
