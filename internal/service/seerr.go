package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/LTAGROUP/watchtower/internal/config"
	"github.com/LTAGROUP/watchtower/internal/model"
	"github.com/LTAGROUP/watchtower/internal/store"
)

type Seerr struct {
	Config   config.Config
	Store    *store.Store
	Resolver *Resolver
	Client   *http.Client
	Log      *slog.Logger
	inflight sync.Map
}
type seerrPage struct {
	Results []seerrRequest `json:"results"`
}
type seerrRequest struct {
	ID     int64  `json:"id"`
	Status int    `json:"status"`
	Type   string `json:"type"`
	Is4K   bool   `json:"is4k"`
	Media  struct {
		ID        int64  `json:"id"`
		TMDBID    int64  `json:"tmdbId"`
		MediaType string `json:"mediaType"`
	} `json:"media"`
	Seasons []struct {
		SeasonNumber int `json:"seasonNumber"`
	} `json:"seasons"`
}
type details struct {
	ID           int64  `json:"id"`
	Title        string `json:"title"`
	Name         string `json:"name"`
	ReleaseDate  string `json:"releaseDate"`
	FirstAirDate string `json:"firstAirDate"`
}

func (s *Seerr) Run(ctx context.Context) {
	s.poll(ctx)
	t := time.NewTicker(s.Config.PollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.poll(ctx)
		}
	}
}
func (s *Seerr) poll(ctx context.Context) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, s.Config.SeerrURL+"/api/v1/request?take=100&skip=0&sort=added", nil)
	req.Header.Set("X-Api-Key", s.Config.SeerrAPIKey)
	resp, e := s.Client.Do(req)
	if e != nil {
		s.Log.Error("seerr poll", "error", e)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		s.Log.Error("seerr poll", "status", resp.Status)
		return
	}
	var page seerrPage
	if e = json.NewDecoder(resp.Body).Decode(&page); e != nil {
		s.Log.Error("seerr decode", "error", e)
		return
	}
	for _, x := range page.Results {
		if x.Status != 2 || s.Store.IsProcessed(x.ID) {
			continue
		}
		x := x
		go s.handle(context.Background(), x)
	}
}
func (s *Seerr) handle(ctx context.Context, x seerrRequest) {
	if _, loaded := s.inflight.LoadOrStore(x.ID, struct{}{}); loaded {
		return
	}
	defer s.inflight.Delete(x.ID)
	kind := strings.ToLower(x.Type)
	if kind == "" {
		kind = strings.ToLower(x.Media.MediaType)
	}
	if kind != "tv" {
		kind = "movie"
	}
	d, e := s.details(ctx, kind, x.Media.TMDBID)
	if e != nil {
		s.Log.Error("seerr details", "request", x.ID, "error", e)
		return
	}
	year := yearOf(d.ReleaseDate)
	if kind == "tv" {
		year = yearOf(d.FirstAirDate)
	}
	title := d.Title
	if title == "" {
		title = d.Name
	}
	seasons := []int{}
	for _, v := range x.Seasons {
		if v.SeasonNumber > 0 {
			seasons = append(seasons, v.SeasonNumber)
		}
	}
	m := &model.Media{ID: x.Media.ID, RequestID: x.ID, Type: kind, TMDBID: x.Media.TMDBID, Title: title, Year: year, Seasons: seasons, Status: "queued", CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	if e = s.Store.UpsertMedia(m); e == nil {
		e = s.Resolver.Resolve(ctx, m)
	}
	if e != nil {
		s.Log.Error("resolve", "request", x.ID, "error", e)
		return
	}
	if m.Status == "ready" || m.Status == "partial" {
		_ = s.Store.MarkProcessed(x.ID)
		s.markAvailable(ctx, x.Media.ID)
	}
}
func (s *Seerr) details(ctx context.Context, kind string, id int64) (details, error) {
	u := fmt.Sprintf("%s/api/v1/%s/%d", s.Config.SeerrURL, kind, id)
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	req.Header.Set("X-Api-Key", s.Config.SeerrAPIKey)
	resp, e := s.Client.Do(req)
	if e != nil {
		return details{}, e
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return details{}, fmt.Errorf("seerr details: %s", resp.Status)
	}
	var d details
	e = json.NewDecoder(resp.Body).Decode(&d)
	return d, e
}
func (s *Seerr) markAvailable(ctx context.Context, id int64) {
	u := s.Config.SeerrURL + "/api/v1/media/" + strconv.FormatInt(id, 10) + "/available"
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, u, strings.NewReader(url.Values{}.Encode()))
	req.Header.Set("X-Api-Key", s.Config.SeerrAPIKey)
	resp, e := s.Client.Do(req)
	if e == nil {
		resp.Body.Close()
	}
}
func yearOf(s string) int {
	if len(s) >= 4 {
		v, _ := strconv.Atoi(s[:4])
		return v
	}
	return 0
}
