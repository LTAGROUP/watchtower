package scraper

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/LTAGROUP/watchtower/internal/model"
)

type Query struct {
	MediaType  string
	ExternalID string
	TMDBID     int64
	Season     int
	Episode    int
}

type Searcher interface {
	Search(context.Context, Query, int) ([]model.Release, error)
}

type Addon struct{ Name, BaseURL string }
type Aggregator struct {
	Addons []Addon
	Client *http.Client
	Log    *slog.Logger
}

type streamResponse struct {
	Streams []stream `json:"streams"`
}
type stream struct {
	Name, Title, Description, InfoHash string
	Sources                            []string `json:"sources"`
	BehaviorHints                      struct {
		Filename  string `json:"filename"`
		VideoSize int64  `json:"videoSize"`
	} `json:"behaviorHints"`
}

var sizeRE = regexp.MustCompile(`(?i)([0-9]+(?:\.[0-9]+)?)\s*(gib|gb|mib|mb)`)
var seedRE = regexp.MustCompile(`(?i)(?:👤\s*|seeders?\D*|seeds?\D*)([0-9]+)`)

func ParseAddons(values []string) ([]Addon, error) {
	out := make([]Addon, 0, len(values))
	for i, raw := range values {
		name, endpoint, ok := strings.Cut(raw, "|")
		if !ok {
			name = fmt.Sprintf("addon-%d", i+1)
			endpoint = raw
		}
		name = strings.TrimSpace(name)
		endpoint = strings.TrimSpace(endpoint)
		endpoint = strings.TrimPrefix(endpoint, "stremio://")
		if !strings.Contains(endpoint, "://") {
			endpoint = "https://" + endpoint
		}
		u, err := url.Parse(endpoint)
		if err != nil || u.Host == "" {
			return nil, fmt.Errorf("invalid addon endpoint %q", raw)
		}
		u.Path = strings.TrimSuffix(u.Path, "/manifest.json")
		u.RawQuery = ""
		u.Fragment = ""
		out = append(out, Addon{Name: name, BaseURL: strings.TrimRight(u.String(), "/")})
	}
	return out, nil
}

func (a *Aggregator) Search(ctx context.Context, q Query, limit int) ([]model.Release, error) {
	mediaType := "movie"
	id := q.ExternalID
	if id == "" && q.TMDBID > 0 {
		id = "tmdb:" + strconv.FormatInt(q.TMDBID, 10)
	}
	if q.MediaType == "tv" {
		mediaType = "series"
		episode := q.Episode
		if episode <= 0 {
			episode = 1
		}
		if q.Season <= 0 {
			q.Season = 1
		}
		id = fmt.Sprintf("%s:%d:%d", id, q.Season, episode)
	}
	if id == "" {
		return nil, fmt.Errorf("no external media id")
	}
	type result struct {
		addon string
		rows  []model.Release
		err   error
	}
	ch := make(chan result, len(a.Addons))
	var wg sync.WaitGroup
	for _, addon := range a.Addons {
		addon := addon
		wg.Add(1)
		go func() {
			defer wg.Done()
			rows, err := a.searchAddon(ctx, addon, mediaType, id)
			ch <- result{addon: addon.Name, rows: rows, err: err}
		}()
	}
	wg.Wait()
	close(ch)
	byHash := map[string]model.Release{}
	var errs []string
	for result := range ch {
		if result.err != nil {
			if a.Log != nil {
				a.Log.Warn("scraper search failed", "component", "scraper", "addon", result.addon, "media_type", mediaType, "media_id", id, "error", result.err)
			}
			errs = append(errs, result.err.Error())
			continue
		}
		if a.Log != nil {
			a.Log.Info("scraper search completed", "component", "scraper", "addon", result.addon, "media_type", mediaType, "media_id", id, "streams", len(result.rows))
		}
		for _, r := range result.rows {
			h := strings.ToLower(r.InfoHash)
			if old, ok := byHash[h]; !ok || r.Seeders > old.Seeders {
				byHash[h] = r
			}
		}
	}
	out := make([]model.Release, 0, len(byHash))
	for _, r := range byHash {
		out = append(out, r)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Seeders > out[j].Seeders })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	if a.Log != nil {
		a.Log.Info("scraper aggregation completed", "component", "scraper", "media_type", mediaType, "media_id", id, "unique_streams", len(byHash), "returned_streams", len(out), "addons", len(a.Addons))
	}
	if len(out) == 0 && len(errs) > 0 {
		return nil, fmt.Errorf("scrapers failed: %s", strings.Join(errs, "; "))
	}
	return out, nil
}

func (a *Aggregator) searchAddon(ctx context.Context, addon Addon, mediaType, id string) ([]model.Release, error) {
	u := addon.BaseURL + "/stream/" + url.PathEscape(mediaType) + "/" + url.PathEscape(id) + ".json"
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "WatchTower/1.0")
	resp, err := a.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", addon.Name, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("%s: %s", addon.Name, resp.Status)
	}
	var payload streamResponse
	if err = json.NewDecoder(io.LimitReader(resp.Body, 8<<20)).Decode(&payload); err != nil {
		return nil, fmt.Errorf("%s: %w", addon.Name, err)
	}
	out := make([]model.Release, 0, len(payload.Streams))
	for _, s := range payload.Streams {
		hash := strings.ToLower(strings.TrimSpace(s.InfoHash))
		if hash == "" {
			continue
		}
		title := strings.TrimSpace(strings.Join([]string{s.Name, s.Title, s.Description, s.BehaviorHints.Filename}, " "))
		if title == "" {
			title = hash
		}
		size := s.BehaviorHints.VideoSize
		if size == 0 {
			size = parseSize(title)
		}
		seeders := -1
		if m := seedRE.FindStringSubmatch(title); len(m) > 1 {
			seeders, _ = strconv.Atoi(m[1])
		}
		magnet := magnetURL(hash, s.Sources)
		out = append(out, model.Release{Title: title, DownloadURL: magnet, InfoHash: hash, Source: addon.Name, Size: size, Seeders: seeders})
	}
	return out, nil
}

func magnetURL(hash string, sources []string) string {
	v := url.Values{"xt": {"urn:btih:" + hash}}
	for _, source := range sources {
		if tracker, ok := strings.CutPrefix(source, "tracker:"); ok {
			v.Add("tr", tracker)
		}
	}
	return "magnet:?" + v.Encode()
}
func parseSize(s string) int64 {
	matches := sizeRE.FindAllStringSubmatch(s, -1)
	if len(matches) == 0 {
		return 0
	}
	m := matches[len(matches)-1]
	n, _ := strconv.ParseFloat(m[1], 64)
	unit := strings.ToLower(m[2])
	mult := float64(1 << 20)
	if unit == "gb" || unit == "gib" {
		mult = 1 << 30
	}
	return int64(n * mult)
}
