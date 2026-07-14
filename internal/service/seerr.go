package service

import (
	"bytes"
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
	Config          config.Config
	Settings        func() config.Config
	Store           *store.Store
	Resolver        *Resolver
	Client          *http.Client
	Log             *slog.Logger
	inflight        sync.Map
	releaseInflight sync.Map
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
type CatalogSeason struct {
	SeasonNumber int    `json:"seasonNumber"`
	Name         string `json:"name"`
	AirDate      string `json:"airDate"`
	EpisodeCount int    `json:"episodeCount"`
	Overview     string `json:"overview"`
	PosterPath   string `json:"posterPath"`
}

type CatalogEpisode struct {
	EpisodeNumber int    `json:"episodeNumber"`
	AirDate       string `json:"airDate"`
}

type CatalogSeasonDetails struct {
	SeasonNumber int              `json:"seasonNumber"`
	AirDate      string           `json:"airDate"`
	Episodes     []CatalogEpisode `json:"episodes"`
}

type CatalogDetails struct {
	ID              int64           `json:"id"`
	IMDBID          string          `json:"imdbId"`
	Title           string          `json:"title"`
	Name            string          `json:"name"`
	Overview        string          `json:"overview"`
	PosterPath      string          `json:"posterPath"`
	BackdropPath    string          `json:"backdropPath"`
	ReleaseDate     string          `json:"releaseDate"`
	FirstAirDate    string          `json:"firstAirDate"`
	NumberOfSeasons int             `json:"numberOfSeasons"`
	Seasons         []CatalogSeason `json:"seasons"`
	Genres          []struct {
		ID   int64  `json:"id"`
		Name string `json:"name"`
	} `json:"genres"`
	ExternalIDs struct {
		IMDBID string `json:"imdbId"`
	} `json:"externalIds"`
}

func (s *Seerr) Run(ctx context.Context) {
	s.poll(ctx)
	for {
		wait := s.currentConfig().PollInterval
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
			s.poll(ctx)
		}
	}
}
func (s *Seerr) poll(ctx context.Context) {
	defer s.releaseDue(ctx)
	cfg := s.currentConfig()
	if cfg.SeerrURL == "" || cfg.SeerrAPIKey == "" {
		return
	}
	started := time.Now()
	s.Log.Info("seerr poll started", "component", "seerr")
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, cfg.SeerrURL+"/api/v1/request?take=100&skip=0&sort=added", nil)
	req.Header.Set("X-Api-Key", cfg.SeerrAPIKey)
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
	queued := 0
	for _, x := range page.Results {
		if x.Status != 2 || s.Store.IsProcessed(x.ID) {
			continue
		}
		x := x
		queued++
		go s.handle(context.Background(), x)
	}
	s.Log.Info("seerr poll completed", "component", "seerr", "requests", len(page.Results), "new_approved_requests", queued, "duration", time.Since(started).String())
}
func (s *Seerr) handle(ctx context.Context, x seerrRequest) {
	if _, loaded := s.inflight.LoadOrStore(x.ID, struct{}{}); loaded {
		return
	}
	defer s.inflight.Delete(x.ID)
	started := time.Now()
	s.Log.Info("seerr request processing started", "component", "seerr", "request", x.ID, "tmdb_id", x.Media.TMDBID)
	kind := strings.ToLower(x.Type)
	if kind == "" {
		kind = strings.ToLower(x.Media.MediaType)
	}
	if kind != "tv" {
		kind = "movie"
	}
	d, e := s.Catalog(ctx, kind, x.Media.TMDBID)
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
	externalID := d.IMDBID
	if externalID == "" {
		externalID = d.ExternalIDs.IMDBID
	}
	m := &model.Media{ID: x.Media.ID, RequestID: x.ID, Type: kind, TMDBID: x.Media.TMDBID, ExternalID: externalID, Title: title, Year: year, Overview: d.Overview, PosterPath: d.PosterPath, BackdropPath: d.BackdropPath, Seasons: seasons, ReleaseDate: s.MediaReleaseDate(ctx, kind, x.Media.TMDBID, d, seasons), Status: "queued", CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	s.Log.Info("seerr media details obtained", "component", "seerr", "request", x.ID, "title", title, "type", kind, "imdb_id", externalID, "tmdb_id", x.Media.TMDBID, "seasons", seasons)
	if e = s.Store.UpsertMedia(m); e == nil {
		e = s.Resolver.Resolve(ctx, m)
	}
	if e != nil {
		s.Log.Error("resolve", "request", x.ID, "error", e)
		return
	}
	if m.Status == "ready" || m.Status == "partial" || m.Status == "unreleased" {
		_ = s.Store.MarkProcessed(x.ID)
		if m.Status != "unreleased" {
			s.markAvailable(ctx, x.Media.ID)
		}
	}
	s.Log.Info("seerr request processing completed", "component", "seerr", "request", x.ID, "title", title, "status", m.Status, "duration", time.Since(started).String())
}
func (s *Seerr) Catalog(ctx context.Context, kind string, id int64) (CatalogDetails, error) {
	if kind != "movie" && kind != "tv" {
		return CatalogDetails{}, fmt.Errorf("media type must be movie or tv")
	}
	cfg := s.currentConfig()
	if cfg.SeerrURL == "" || cfg.SeerrAPIKey == "" {
		return CatalogDetails{}, fmt.Errorf("Seerr is not configured as a catalog source")
	}
	u := fmt.Sprintf("%s/api/v1/%s/%d", cfg.SeerrURL, kind, id)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return CatalogDetails{}, err
	}
	req.Header.Set("X-Api-Key", cfg.SeerrAPIKey)
	resp, e := s.Client.Do(req)
	if e != nil {
		return CatalogDetails{}, e
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return CatalogDetails{}, fmt.Errorf("seerr details: %s", resp.Status)
	}
	var d CatalogDetails
	e = json.NewDecoder(resp.Body).Decode(&d)
	return d, e
}

func (s *Seerr) CatalogSeason(ctx context.Context, id int64, season int) (CatalogSeasonDetails, error) {
	if id <= 0 || season <= 0 {
		return CatalogSeasonDetails{}, fmt.Errorf("TV and season IDs must be positive")
	}
	cfg := s.currentConfig()
	if cfg.SeerrURL == "" || cfg.SeerrAPIKey == "" {
		return CatalogSeasonDetails{}, fmt.Errorf("Seerr is not configured as a catalog source")
	}
	u := fmt.Sprintf("%s/api/v1/tv/%d/season/%d", cfg.SeerrURL, id, season)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return CatalogSeasonDetails{}, err
	}
	req.Header.Set("X-Api-Key", cfg.SeerrAPIKey)
	resp, err := s.Client.Do(req)
	if err != nil {
		return CatalogSeasonDetails{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return CatalogSeasonDetails{}, fmt.Errorf("seerr season details: %s", resp.Status)
	}
	var details CatalogSeasonDetails
	err = json.NewDecoder(resp.Body).Decode(&details)
	return details, err
}

// MediaReleaseDate returns the first day any requested content is expected to
// be available. TV season dates represent the first episode; when Seerr omits
// that summary field, the episode list is used as a fallback.
func (s *Seerr) MediaReleaseDate(ctx context.Context, kind string, id int64, details CatalogDetails, seasons []int) string {
	if kind == "movie" {
		return validDate(details.ReleaseDate)
	}
	if kind != "tv" {
		return ""
	}
	if len(seasons) == 0 {
		return validDate(details.FirstAirDate)
	}
	var dates []string
	for _, wanted := range seasons {
		date := ""
		for _, season := range details.Seasons {
			if season.SeasonNumber == wanted {
				date = validDate(season.AirDate)
				break
			}
		}
		if date == "" {
			season, err := s.CatalogSeason(ctx, id, wanted)
			if err == nil {
				date = validDate(season.AirDate)
				for _, episode := range season.Episodes {
					date = earlierDate(date, validDate(episode.AirDate))
				}
			} else if s.Log != nil {
				s.Log.Warn("Seerr episode release dates unavailable", "component", "seerr", "tmdb_id", id, "season", wanted, "error", err)
			}
		}
		if date != "" {
			dates = append(dates, date)
		}
	}
	date := ""
	for _, candidate := range dates {
		date = earlierDate(date, candidate)
	}
	if date == "" {
		date = validDate(details.FirstAirDate)
	}
	return date
}
func (s *Seerr) markAvailable(ctx context.Context, id int64) {
	cfg := s.currentConfig()
	u := cfg.SeerrURL + "/api/v1/media/" + strconv.FormatInt(id, 10) + "/available"
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, u, strings.NewReader(url.Values{}.Encode()))
	req.Header.Set("X-Api-Key", cfg.SeerrAPIKey)
	resp, e := s.Client.Do(req)
	if e != nil {
		s.Log.Warn("seerr availability update failed", "component", "seerr", "media", id, "error", e)
		return
	}
	resp.Body.Close()
	if resp.StatusCode/100 == 2 {
		s.Log.Info("seerr media marked available", "component", "seerr", "media", id)
	} else {
		s.Log.Warn("seerr availability update rejected", "component", "seerr", "media", id, "status", resp.Status)
	}
}

type DiscoverOptions struct {
	Query, MediaType, Genre, Year, Sort string
	Page                                int
}

type CreateRequestInput struct {
	MediaType string `json:"mediaType"`
	MediaID   int64  `json:"mediaId"`
	Seasons   []int  `json:"seasons,omitempty"`
	Is4K      bool   `json:"is4k"`
}

func (s *Seerr) Discover(ctx context.Context, options DiscoverOptions) (json.RawMessage, error) {
	cfg := s.currentConfig()
	if cfg.SeerrURL == "" || cfg.SeerrAPIKey == "" {
		return nil, fmt.Errorf("Seerr is not configured")
	}
	page := options.Page
	if page < 1 {
		page = 1
	}
	values := url.Values{"page": {strconv.Itoa(page)}}
	var endpoint string
	if strings.TrimSpace(options.Query) != "" {
		endpoint = "/api/v1/search"
		values.Set("query", strings.TrimSpace(options.Query))
	} else {
		kind := "movies"
		if strings.EqualFold(options.MediaType, "tv") {
			kind = "tv"
		}
		endpoint = "/api/v1/discover/" + kind
		if options.Genre != "" {
			values.Set("genre", options.Genre)
		}
		if options.Sort != "" {
			values.Set("sortBy", options.Sort)
		}
		if len(options.Year) == 4 {
			if kind == "tv" {
				values.Set("firstAirDateGte", options.Year+"-01-01")
				values.Set("firstAirDateLte", options.Year+"-12-31")
			} else {
				values.Set("primaryReleaseDateGte", options.Year+"-01-01")
				values.Set("primaryReleaseDateLte", options.Year+"-12-31")
			}
		}
	}
	query := values.Encode()
	if endpoint == "/api/v1/search" {
		// Seerr's OpenAPI validator follows RFC 3986 and rejects the '+' that
		// url.Values uses for spaces. A literal plus is already encoded as %2B,
		// so replacing separators here preserves the user's query.
		query = strings.ReplaceAll(query, "+", "%20")
	}
	return s.seerrJSON(ctx, http.MethodGet, endpoint+"?"+query, nil)
}

func (s *Seerr) CreateRequest(ctx context.Context, input CreateRequestInput) (json.RawMessage, error) {
	if input.MediaID <= 0 || (input.MediaType != "movie" && input.MediaType != "tv") {
		return nil, fmt.Errorf("mediaId and a movie or tv mediaType are required")
	}
	if input.MediaType == "tv" && len(input.Seasons) == 0 {
		return nil, fmt.Errorf("choose at least one season")
	}
	body, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	return s.seerrJSON(ctx, http.MethodPost, "/api/v1/request", body)
}

func (s *Seerr) Retry(ctx context.Context, item *model.Media) error {
	if _, loaded := s.inflight.LoadOrStore(item.RequestID, struct{}{}); loaded {
		return fmt.Errorf("media is already being processed")
	}
	defer s.inflight.Delete(item.RequestID)
	if err := s.Resolver.Resolve(ctx, item); err != nil {
		return err
	}
	if item.Status == "ready" || item.Status == "partial" {
		if err := s.Store.MarkProcessed(item.RequestID); err != nil {
			return err
		}
		s.markAvailable(ctx, item.ID)
	}
	return nil
}

func (s *Seerr) releaseDue(ctx context.Context) {
	for _, media := range s.Store.Media() {
		if media == nil || media.Status != "unreleased" || IsUnreleased(media, time.Now()) {
			continue
		}
		if _, loaded := s.releaseInflight.LoadOrStore(media.ID, struct{}{}); loaded {
			continue
		}
		go func(id int64) {
			defer s.releaseInflight.Delete(id)
			item, err := s.Store.ResetMedia(id)
			if err == nil {
				if item.RequestID > 0 {
					err = s.Retry(context.Background(), item)
				} else {
					err = s.Resolver.Resolve(context.Background(), item)
				}
			}
			if err != nil && s.Log != nil {
				s.Log.Error("released media retry failed", "component", "seerr", "media", id, "error", err)
			}
		}(media.ID)
	}
}

func (s *Seerr) seerrJSON(ctx context.Context, method, path string, body []byte) (json.RawMessage, error) {
	cfg := s.currentConfig()
	if cfg.SeerrURL == "" || cfg.SeerrAPIKey == "" {
		return nil, fmt.Errorf("Seerr is not configured")
	}
	req, err := http.NewRequestWithContext(ctx, method, cfg.SeerrURL+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Api-Key", cfg.SeerrAPIKey)
	if len(body) > 0 {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := s.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var raw json.RawMessage
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, fmt.Errorf("Seerr returned %s", resp.Status)
	}
	if resp.StatusCode/100 != 2 {
		var detail struct {
			Message string `json:"message"`
		}
		if json.Unmarshal(raw, &detail) == nil && detail.Message != "" {
			return nil, fmt.Errorf("%s", detail.Message)
		}
		return nil, fmt.Errorf("Seerr returned %s", resp.Status)
	}
	return raw, nil
}

func (s *Seerr) currentConfig() config.Config {
	if s.Settings != nil {
		return s.Settings()
	}
	return s.Config
}
func yearOf(s string) int {
	if len(s) >= 4 {
		v, _ := strconv.Atoi(s[:4])
		return v
	}
	return 0
}

func validDate(value string) string {
	if len(value) < len("2006-01-02") {
		return ""
	}
	value = value[:len("2006-01-02")]
	if _, err := time.Parse("2006-01-02", value); err != nil {
		return ""
	}
	return value
}

func earlierDate(current, candidate string) string {
	if candidate != "" && (current == "" || candidate < current) {
		return candidate
	}
	return current
}
