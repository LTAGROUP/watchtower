package debrid

import (
	"bytes"
	"context"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/LTAGROUP/watchtower/internal/model"
)

type AllDebrid struct {
	Token         string
	Client        *http.Client
	AllowUncached bool
	Poll          time.Duration
}

func (a *AllDebrid) Name() string { return "alldebrid" }
func (a *AllDebrid) Resolve(ctx context.Context, r model.Release) (model.Resolved, error) {
	var raw map[string]any
	var err error
	if len(r.TorrentData) > 0 {
		var b bytes.Buffer
		mw := multipart.NewWriter(&b)
		p, _ := mw.CreateFormFile("files[]", "release.torrent")
		_, _ = p.Write(r.TorrentData)
		_ = mw.Close()
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.alldebrid.com/v4/magnet/upload/file", &b)
		req.Header.Set("Authorization", "Bearer "+a.Token)
		req.Header.Set("Content-Type", mw.FormDataContentType())
		resp, e := a.Client.Do(req)
		if e != nil {
			return model.Resolved{}, e
		}
		defer resp.Body.Close()
		err = jsonDecode(resp.Body, &raw)
	} else {
		raw, err = doForm(ctx, a.Client, http.MethodPost, "https://api.alldebrid.com/v4/magnet/upload", a.Token, url.Values{"magnets[]": {r.DownloadURL}})
	}
	if err != nil {
		return model.Resolved{}, err
	}
	d := object(raw["data"])
	mags := array(d["magnets"])
	if len(mags) == 0 {
		mags = array(d["files"])
	}
	if len(mags) == 0 {
		return model.Resolved{}, fmt.Errorf("alldebrid returned no magnet")
	}
	m := object(mags[0])
	id := num(m["id"])
	ready, _ := m["ready"].(bool)
	if !ready && !a.AllowUncached {
		return model.Resolved{}, fmt.Errorf("not cached")
	}
	return a.wait(ctx, id)
}
func (a *AllDebrid) wait(ctx context.Context, id int64) (model.Resolved, error) {
	p := a.Poll
	if p <= 0 {
		p = 5 * time.Second
	}
	tick := time.NewTicker(p)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return model.Resolved{}, ctx.Err()
		case <-tick.C:
			raw, e := doForm(ctx, a.Client, http.MethodPost, "https://api.alldebrid.com/v4.1/magnet/status", a.Token, url.Values{"id": {strconv.FormatInt(id, 10)}})
			if e != nil {
				continue
			}
			d := object(raw["data"])
			m := object(d["magnets"])
			if len(m) == 0 {
				arr := array(d["magnets"])
				if len(arr) > 0 {
					m = object(arr[0])
				}
			}
			status := strings.ToLower(str(m["status"]))
			if strings.Contains(status, "error") || strings.Contains(status, "fail") {
				return model.Resolved{}, fmt.Errorf("alldebrid magnet failed: %s", status)
			}
			if status == "ready" || num(m["statusCode"]) == 4 {
				fr, e := doForm(ctx, a.Client, http.MethodPost, "https://api.alldebrid.com/v4/magnet/files", a.Token, url.Values{"id": {strconv.FormatInt(id, 10)}})
				if e != nil {
					return model.Resolved{}, e
				}
				out := model.Resolved{ItemID: strconv.FormatInt(id, 10), Cached: true}
				walkAD(array(object(fr["data"])["files"]), &out.Files)
				if len(out.Files) == 0 {
					walkAD(array(object(fr["data"])[strconv.FormatInt(id, 10)]), &out.Files)
				}
				return out, nil
			}
		}
	}
}
func walkAD(nodes []any, out *[]model.RemoteFile) {
	for _, v := range nodes {
		m := object(v)
		if kids := array(m["e"]); len(kids) > 0 {
			walkAD(kids, out)
		} else if l := str(m["l"]); l != "" {
			*out = append(*out, model.RemoteFile{ID: l, Name: str(m["n"]), Size: num(m["s"])})
		}
	}
}
func (a *AllDebrid) StreamURL(ctx context.Context, f *model.File) (string, error) {
	raw, e := doForm(ctx, a.Client, http.MethodPost, "https://api.alldebrid.com/v4/link/unlock", a.Token, url.Values{"link": {f.ProviderFileID}})
	if e != nil {
		return "", e
	}
	d := object(raw["data"])
	if s := str(d["link"]); s != "" {
		return s, nil
	}
	return "", fmt.Errorf("alldebrid returned no stream URL")
}
