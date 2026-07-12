package indexer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/LTAGROUP/watchtower/internal/model"
)

type Prowlarr struct {
	BaseURL, APIKey string
	Client          *http.Client
}
type searchResult struct {
	Title       string `json:"title"`
	DownloadURL string `json:"downloadUrl"`
	MagnetURL   string `json:"magnetUrl"`
	InfoHash    string `json:"infoHash"`
	Indexer     string `json:"indexer"`
	Size        int64  `json:"size"`
	Seeders     int    `json:"seeders"`
}

func (p *Prowlarr) Search(ctx context.Context, query string, limit int) ([]model.Release, error) {
	body, _ := json.Marshal(map[string]any{"query": query, "type": "search", "indexerIds": []int{}, "categories": []int{2000, 5000}})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, p.BaseURL+"/api/v1/search", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Api-Key", p.APIKey)
	resp, err := p.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("prowlarr search: %s", resp.Status)
	}
	var rows []searchResult
	if err = json.NewDecoder(resp.Body).Decode(&rows); err != nil {
		return nil, err
	}
	if len(rows) > limit {
		rows = rows[:limit]
	}
	out := make([]model.Release, 0, len(rows))
	for _, r := range rows {
		d := r.DownloadURL
		if r.MagnetURL != "" {
			d = r.MagnetURL
		}
		h := strings.ToLower(r.InfoHash)
		if h == "" {
			h = hashFromMagnet(d)
		}
		out = append(out, model.Release{Title: r.Title, DownloadURL: d, InfoHash: h, Indexer: r.Indexer, Size: r.Size, Seeders: r.Seeders})
	}
	return out, nil
}
func hashFromMagnet(s string) string {
	u, e := url.Parse(s)
	if e != nil {
		return ""
	}
	xt := u.Query().Get("xt")
	const p = "urn:btih:"
	if strings.HasPrefix(strings.ToLower(xt), p) {
		return strings.ToLower(xt[len(p):])
	}
	return ""
}

func (p *Prowlarr) FetchTorrent(ctx context.Context, r model.Release) (model.Release, error) {
	if strings.HasPrefix(r.DownloadURL, "magnet:") {
		return r, nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.DownloadURL, nil)
	if err != nil {
		return r, err
	}
	req.Header.Set("X-Api-Key", p.APIKey)
	resp, err := p.Client.Do(req)
	if err != nil {
		return r, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return r, fmt.Errorf("prowlarr download: %s", resp.Status)
	}
	r.TorrentData, err = io.ReadAll(io.LimitReader(resp.Body, 10<<20))
	return r, err
}
