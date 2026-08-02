package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/LTAGROUP/watchtower/internal/config"
	"github.com/LTAGROUP/watchtower/internal/model"
	"github.com/LTAGROUP/watchtower/internal/store"
)

type Seerr struct {
	Config                 config.Config
	Settings               func() config.Config
	Store                  *store.Store
	Resolver               *Resolver
	Scheduler              *Lifecycle
	Client                 *http.Client
	Log                    *slog.Logger
	inflight               sync.Map
	mediaInflight          sync.Map
	releaseInflight        sync.Map
	wakeOnce               sync.Once
	wake                   chan struct{}
	tasks                  sync.WaitGroup
	beforeCompleteResolved func()
}

const (
	seerrRequestApproved  = 2
	seerrRequestCompleted = 5
	seerrRequestPageSize  = 100
)

var errSeerrCompletionSkipped = errors.New("Seerr completion is no longer ready")

type seerrPageInfo struct {
	Pages    int `json:"pages"`
	PageSize int `json:"pageSize"`
	Page     int `json:"page"`
}

type seerrPage struct {
	Results  []seerrRequest `json:"results"`
	PageInfo seerrPageInfo  `json:"pageInfo"`
}
type seerrRequest struct {
	ID         int64   `json:"id"`
	Status     int     `json:"status"`
	Type       string  `json:"type"`
	Is4K       bool    `json:"is4k"`
	RequestIDs []int64 `json:"-"`
	Media      struct {
		ID        int64  `json:"id"`
		TMDBID    int64  `json:"tmdbId"`
		MediaType string `json:"mediaType"`
	} `json:"media"`
	Seasons []seerrSeasonRequest `json:"seasons"`
}

type seerrSeasonRequest struct {
	SeasonNumber int `json:"seasonNumber"`
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
	defer s.tasks.Wait()
	for {
		wait := s.nextPollDelay(time.Now().UTC())
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-s.wakeChannel():
			if !t.Stop() {
				select {
				case <-t.C:
				default:
				}
			}
			s.poll(ctx)
		case <-t.C:
			s.poll(ctx)
		}
	}
}

func (s *Seerr) nextPollDelay(now time.Time) time.Duration {
	wait := s.currentConfig().PollInterval
	if wait <= 0 {
		wait = 2 * time.Minute
	}
	if s.Store == nil {
		return wait
	}
	next := now.Add(wait)
	for _, media := range s.Store.Media() {
		if media == nil {
			continue
		}
		intent := media.AvailabilityIntent
		if intent.Generation <= intent.CompletedGeneration {
			continue
		}
		candidate := intent.NextAt
		if intent.LeaseUntil.After(now) && (candidate.IsZero() || intent.LeaseUntil.Before(candidate)) {
			candidate = intent.LeaseUntil
		}
		if candidate.IsZero() || !candidate.After(now) {
			return time.Millisecond
		}
		if candidate.Before(next) {
			next = candidate
		}
	}
	return time.Until(next)
}

// Wake asks the Seerr importer to poll immediately. A buffered channel keeps
// webhook delivery non-blocking and coalesces bursts of notifications.
func (s *Seerr) Wake() {
	select {
	case s.wakeChannel() <- struct{}{}:
	default:
	}
}

func (s *Seerr) wakeChannel() chan struct{} {
	s.wakeOnce.Do(func() {
		s.wake = make(chan struct{}, 1)
	})
	return s.wake
}

// WebhookHandler accepts Seerr notification webhooks without requiring an
// authorization header and wakes the importer.
// The body is intentionally not part of the import contract: polling Seerr
// remains authoritative, so this also works with any valid Seerr webhook
// payload and avoids coupling WatchTower to notification template changes.
func (s *Seerr) WebhookHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
			return
		}
		s.Wake()
		w.WriteHeader(http.StatusAccepted)
	})
}

func (s *Seerr) poll(ctx context.Context) {
	defer s.reconcile(ctx)
	cfg := s.currentConfig()
	if cfg.SeerrURL == "" || cfg.SeerrAPIKey == "" {
		return
	}
	started := time.Now()
	if s.Log != nil {
		s.Log.Info("seerr poll started", "component", "seerr")
	}
	queued := 0
	requestCount := 0
	pageCount := 0
	duplicateRequests := 0
	pending := map[string]seerrRequest{}
	for skip := 0; ; {
		page, e := s.requestPage(ctx, cfg, skip)
		if e != nil {
			if s.Log != nil && ctx.Err() == nil {
				s.Log.Error("seerr poll", "error", e, "skip", skip)
			}
			return
		}
		pageCount++
		requestCount += len(page.Results)
		for _, x := range page.Results {
			if !importableSeerrRequestStatus(x.Status) || s.Store.IsProcessed(x.ID) {
				continue
			}
			key := seerrRequestMediaKey(x)
			if existing, ok := pending[key]; ok {
				existing.Seasons = mergeSeerrSeasons(existing.Seasons, x.Seasons)
				if x.ID > 0 {
					existing.RequestIDs = append(existing.RequestIDs, x.ID)
				}
				pending[key] = existing
				duplicateRequests++
				continue
			}
			if x.ID > 0 {
				x.RequestIDs = []int64{x.ID}
			}
			pending[key] = x
		}
		if !seerrPageHasNext(page) {
			break
		}
		skip += len(page.Results)
	}
	for _, x := range pending {
		if s.requestIsDurablyImported(x) {
			continue
		}
		queued++
		s.startHandle(ctx, x)
	}
	if s.Log != nil {
		s.Log.Info("seerr poll completed", "component", "seerr", "requests", requestCount, "pages", pageCount, "new_importable_requests", queued, "duplicate_requests_collapsed", duplicateRequests, "duration", time.Since(started).String())
	}
}

func (s *Seerr) startHandle(ctx context.Context, request seerrRequest) {
	s.tasks.Add(1)
	go func() {
		defer s.tasks.Done()
		s.handle(ctx, request)
	}()
}

func (s *Seerr) requestPage(ctx context.Context, cfg config.Config, skip int) (seerrPage, error) {
	values := url.Values{
		"take":          {strconv.Itoa(seerrRequestPageSize)},
		"skip":          {strconv.Itoa(skip)},
		"sortDirection": {"asc"},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, cfg.SeerrURL+"/api/v1/request?"+values.Encode(), nil)
	if err != nil {
		return seerrPage{}, err
	}
	req.Header.Set("X-Api-Key", cfg.SeerrAPIKey)
	resp, err := s.Client.Do(req)
	if err != nil {
		return seerrPage{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return seerrPage{}, fmt.Errorf("Seerr request list returned %s", resp.Status)
	}
	var page seerrPage
	if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
		return seerrPage{}, fmt.Errorf("decode Seerr request list: %w", err)
	}
	return page, nil
}

func seerrPageHasNext(page seerrPage) bool {
	if len(page.Results) == 0 {
		return false
	}
	if page.PageInfo.Pages > 0 && page.PageInfo.Page > 0 {
		return page.PageInfo.Page < page.PageInfo.Pages
	}
	pageSize := page.PageInfo.PageSize
	if pageSize <= 0 {
		pageSize = seerrRequestPageSize
	}
	return len(page.Results) >= pageSize
}

func importableSeerrRequestStatus(status int) bool {
	return status == seerrRequestApproved || status == seerrRequestCompleted
}

func seerrRequestKind(x seerrRequest) string {
	kind := strings.ToLower(x.Type)
	if kind == "" {
		kind = strings.ToLower(x.Media.MediaType)
	}
	if kind != "tv" {
		kind = "movie"
	}
	return kind
}

func seerrRequestMediaKey(x seerrRequest) string {
	if x.Media.TMDBID > 0 {
		return fmt.Sprintf("%s:%d", seerrRequestKind(x), x.Media.TMDBID)
	}
	return fmt.Sprintf("%s:media:%d", seerrRequestKind(x), x.Media.ID)
}

// requestIsDurablyImported reports whether every request collapsed into x is
// already associated with the local media and x does not expand its TV scope.
// ProcessedRequests only records the final availability transition, so polling
// must use the persisted media association as the earlier idempotency marker.
func (s *Seerr) requestIsDurablyImported(x seerrRequest) bool {
	if s.Store == nil {
		return false
	}
	ids := seerrRequestIDs(x)
	if len(ids) == 0 {
		return false
	}
	media := s.mediaForImportedRequest(x, ids)
	if media == nil || !mediaTracksRequestIDs(media, ids) {
		return false
	}
	if seerrRequestKind(x) != "tv" {
		return true
	}
	trackedSeasons := make(map[int]bool, len(media.Seasons))
	for _, season := range media.Seasons {
		if season > 0 {
			trackedSeasons[season] = true
		}
	}
	for _, season := range x.Seasons {
		if season.SeasonNumber > 0 && !trackedSeasons[season.SeasonNumber] {
			return false
		}
	}
	return true
}

func (s *Seerr) mediaForImportedRequest(x seerrRequest, ids []int64) *model.Media {
	// A request ID is the durable association. Prefer it over identity fields
	// so legacy records with incomplete Seerr metadata remain idempotent.
	for _, media := range s.Store.Media() {
		if media != nil && mediaTracksAnyRequestID(media, ids) {
			return media
		}
	}
	if x.Media.TMDBID > 0 {
		if media, ok := s.Store.FindMediaByTMDB(seerrRequestKind(x), x.Media.TMDBID); ok {
			return media
		}
	}
	if x.Media.ID > 0 {
		for _, media := range s.Store.Media() {
			if media != nil && media.SeerrMediaID == x.Media.ID && media.Type == seerrRequestKind(x) {
				return media
			}
		}
	}
	return nil
}

func mediaTracksAnyRequestID(media *model.Media, ids []int64) bool {
	if media == nil {
		return false
	}
	tracked := make(map[int64]bool, len(media.RequestIDs)+1)
	for _, id := range media.RequestIDs {
		if id > 0 {
			tracked[id] = true
		}
	}
	if media.RequestID > 0 {
		tracked[media.RequestID] = true
	}
	for _, id := range ids {
		if tracked[id] {
			return true
		}
	}
	return false
}

func mediaTracksRequestIDs(media *model.Media, ids []int64) bool {
	if media == nil || len(ids) == 0 {
		return false
	}
	tracked := make(map[int64]bool, len(media.RequestIDs)+1)
	for _, id := range media.RequestIDs {
		if id > 0 {
			tracked[id] = true
		}
	}
	if media.RequestID > 0 {
		tracked[media.RequestID] = true
	}
	for _, id := range ids {
		if id <= 0 || !tracked[id] {
			return false
		}
	}
	return true
}

func mergeSeerrSeasons(left, right []seerrSeasonRequest) []seerrSeasonRequest {
	seen := make(map[int]bool, len(left)+len(right))
	merged := make([]seerrSeasonRequest, 0, len(left)+len(right))
	for _, season := range append(append([]seerrSeasonRequest(nil), left...), right...) {
		if season.SeasonNumber > 0 && !seen[season.SeasonNumber] {
			seen[season.SeasonNumber] = true
			merged = append(merged, season)
		}
	}
	sort.Slice(merged, func(i, j int) bool { return merged[i].SeasonNumber < merged[j].SeasonNumber })
	return merged
}

func (s *Seerr) handle(ctx context.Context, x seerrRequest) {
	if _, loaded := s.inflight.LoadOrStore(x.ID, struct{}{}); loaded {
		return
	}
	defer s.inflight.Delete(x.ID)
	mediaKey := seerrRequestMediaKey(x)
	if _, loaded := s.mediaInflight.LoadOrStore(mediaKey, struct{}{}); loaded {
		return
	}
	defer s.mediaInflight.Delete(mediaKey)
	started := time.Now()
	if s.Log != nil {
		s.Log.Info("seerr request processing started", "component", "seerr", "request", x.ID, "tmdb_id", x.Media.TMDBID)
	}
	kind := seerrRequestKind(x)
	d, e := s.Catalog(ctx, kind, x.Media.TMDBID)
	if e != nil {
		if s.Log != nil {
			s.Log.Error("seerr details", "request", x.ID, "error", e)
		}
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
	releaseDate := s.MediaReleaseDate(ctx, kind, x.Media.TMDBID, d, seasons)
	var counts map[int]int
	var airDates []model.EpisodeAirDate
	if kind == "tv" {
		refreshedCounts, dates, firstAir, scheduleErr := s.EpisodeSchedule(ctx, x.Media.TMDBID, seasons, d)
		if scheduleErr != nil {
			// Counts and air dates are one scheduling unit. Persisting catalog
			// counts without their per-episode dates would make future weekly
			// episodes immediately due, so leave this request unassociated for
			// the next poll to retry.
			if s.Log != nil {
				s.Log.Warn("Seerr episode schedule unavailable", "component", "seerr", "request", x.ID, "error", scheduleErr)
			}
			return
		}
		counts, airDates = refreshedCounts, dates
		releaseDate = earlierDate(releaseDate, firstAir)
	} else {
		counts = CatalogEpisodeCounts(seasons, d.Seasons)
	}
	m := &model.Media{ID: x.Media.ID, RequestID: x.ID, RequestIDs: seerrRequestIDs(x), SeerrMediaID: x.Media.ID, Type: kind, TMDBID: x.Media.TMDBID, ExternalID: externalID, Title: title, Year: year, Overview: d.Overview, PosterPath: d.PosterPath, BackdropPath: d.BackdropPath, Seasons: seasons, EpisodeCounts: counts, EpisodeAirDates: airDates, ReleaseDate: releaseDate, Status: "queued", CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	if s.Log != nil {
		s.Log.Info("seerr media details obtained", "component", "seerr", "request", x.ID, "title", title, "type", kind, "imdb_id", externalID, "tmdb_id", x.Media.TMDBID, "seasons", seasons)
	}
	if s.Resolver == nil {
		e = errors.New("media resolver is unavailable")
	} else {
		var queued *model.Media
		queued, e = s.Resolver.Queue(m)
		if e == nil && s.Scheduler != nil {
			s.Scheduler.Wake()
		} else if e == nil {
			e = s.Resolver.RunDue(ctx, queued.ID)
		}
	}
	if e != nil {
		if s.Log != nil {
			s.Log.Error("resolve", "request", x.ID, "error", e)
		}
		return
	}
	if s.Log != nil {
		s.Log.Info("seerr request processing queued", "component", "seerr", "request", x.ID, "title", title, "status", m.Status, "duration", time.Since(started).String())
	}
}

func seerrRequestIDs(x seerrRequest) []int64 {
	if len(x.RequestIDs) > 0 {
		return x.RequestIDs
	}
	if x.ID > 0 {
		return []int64{x.ID}
	}
	return nil
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

// EpisodeSchedule returns counts and individual known air dates together. A
// caller only persists the pair after this function succeeds, avoiding a state
// where a refreshed count makes not-yet-aired episodes look immediately due.
func (s *Seerr) EpisodeSchedule(ctx context.Context, id int64, seasons []int, catalog CatalogDetails) (map[int]int, []model.EpisodeAirDate, string, error) {
	if id <= 0 {
		return nil, nil, "", fmt.Errorf("TV ID must be positive")
	}
	counts := CatalogEpisodeCounts(seasons, catalog.Seasons)
	if counts == nil {
		counts = map[int]int{}
	}
	dates := make([]model.EpisodeAirDate, 0)
	firstAir := ""
	for _, wanted := range mergeSeasons(nil, seasons) {
		details, err := s.CatalogSeason(ctx, id, wanted)
		if err != nil {
			return nil, nil, "", err
		}
		if len(details.Episodes) > counts[wanted] {
			counts[wanted] = len(details.Episodes)
		}
		for _, episode := range details.Episodes {
			if episode.EpisodeNumber <= 0 {
				continue
			}
			date := validDate(episode.AirDate)
			if date == "" {
				continue
			}
			dates = append(dates, model.EpisodeAirDate{Season: wanted, Episode: episode.EpisodeNumber, AirDate: date})
			firstAir = earlierDate(firstAir, date)
		}
		if firstAir == "" {
			firstAir = earlierDate(firstAir, validDate(details.AirDate))
		}
	}
	return counts, mergeEpisodeAirDates(nil, dates), firstAir, nil
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

// reconcile is deliberately local-only before it attempts an availability
// request: markers and the durable intent are committed together first.
func (s *Seerr) reconcile(ctx context.Context) {
	s.completeResolved(ctx)
	s.retryAvailability(ctx)
	if s.Scheduler != nil {
		s.Scheduler.Wake()
	}
}

func (s *Seerr) completeResolved(_ context.Context) {
	if s.Store == nil {
		return
	}
	for _, media := range s.Store.Media() {
		if media == nil || media.Work.Mode != "" || !mediaReadyForSeerr(media, s.Store.FilesForMedia(media.ID)) {
			continue
		}
		ids := pendingRequestIDs(s.Store, media)
		if len(ids) == 0 {
			continue
		}
		if s.beforeCompleteResolved != nil {
			s.beforeCompleteResolved()
		}
		_, err := s.Store.UpdateMediaAtomic(media.ID, func(current *model.Media, transaction *store.MediaTransaction) error {
			// The outer scan is only a scheduling hint. Recheck every readiness
			// input while the media, files, and markers share one candidate state.
			if current.Work.Mode != "" || !mediaReadyForSeerr(current, transaction.FilesForMedia()) {
				return errSeerrCompletionSkipped
			}
			currentIDs := pendingRequestIDsInTransaction(transaction, current)
			if len(currentIDs) == 0 {
				return errSeerrCompletionSkipped
			}
			// A prior atomic completion marks all pending IDs. A new current ID
			// gets a new intent generation without replacing catalog/work state.
			current.RequestIDs = mergeRequestIDs(current.RequestIDs, []int64{current.RequestID})
			intent := current.AvailabilityIntent
			intent.Generation++
			if intent.Generation <= 0 {
				intent.Generation = 1
			}
			intent.Attempts = 0
			intent.NextAt = time.Time{}
			intent.LeaseUntil = time.Time{}
			intent.LeaseGeneration = 0
			current.AvailabilityIntent = intent
			current.UpdatedAt = time.Now().UTC()
			transaction.MarkProcessed(currentIDs...)
			return nil
		})
		if err != nil && !errors.Is(err, errSeerrCompletionSkipped) && s.Log != nil {
			s.Log.Error("persist Seerr availability intent", "component", "seerr", "media", media.ID, "error", err)
		}
	}
}

func pendingRequestIDs(state *store.Store, media *model.Media) []int64 {
	if state == nil || media == nil {
		return nil
	}
	all := mergeRequestIDs(media.RequestIDs, []int64{media.RequestID})
	pending := make([]int64, 0, len(all))
	for _, id := range all {
		if !state.IsProcessed(id) {
			pending = append(pending, id)
		}
	}
	return pending
}

func pendingRequestIDsInTransaction(transaction *store.MediaTransaction, media *model.Media) []int64 {
	if transaction == nil || media == nil {
		return nil
	}
	all := mergeRequestIDs(media.RequestIDs, []int64{media.RequestID})
	pending := make([]int64, 0, len(all))
	for _, id := range all {
		if !transaction.IsProcessed(id) {
			pending = append(pending, id)
		}
	}
	return pending
}

func mediaReadyForSeerr(media *model.Media, files []*model.File) bool {
	if media == nil || media.Status == "unreleased" || media.Status == "queued" || media.Status == "scraping" || media.Status == "resolving" || media.Status == "failed" {
		return false
	}
	if media.Type != "tv" {
		return len(files) > 0
	}
	known := false
	for _, season := range media.Seasons {
		count := media.EpisodeCounts[season]
		if count <= 0 {
			continue
		}
		known = true
		for episode := 1; episode <= count; episode++ {
			found := false
			for _, file := range files {
				fileSeason, fileEpisodeNumber := fileEpisode(file)
				if fileSeason == season && fileEpisodeNumber == episode {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
	}
	// Legacy TV records lacking counts retain the former status-based behavior.
	return !known || len(files) > 0
}

type availabilityIntentClaim struct {
	mediaID    int64
	generation int64
	seerrID    int64
}

var errAvailabilityIntentNotDue = errors.New("Seerr availability intent is not due")

func (s *Seerr) retryAvailability(ctx context.Context) {
	if s.Store == nil {
		return
	}
	now := time.Now().UTC()
	claims := make([]availabilityIntentClaim, 0)
	for _, media := range s.Store.Media() {
		if media == nil {
			continue
		}
		claim, ok := s.claimAvailabilityIntent(media.ID, now)
		if ok {
			claims = append(claims, claim)
		}
	}
	for _, claim := range claims {
		attempt, cancel := context.WithTimeout(ctx, 30*time.Second)
		err := s.postAvailable(attempt, claim.seerrID)
		cancel()
		s.completeAvailabilityIntent(claim, time.Now().UTC(), err)
		if err != nil && s.Log != nil && ctx.Err() == nil {
			s.Log.Warn("seerr availability update failed", "component", "seerr", "media", claim.seerrID, "error", err)
		}
	}
}

func (s *Seerr) claimAvailabilityIntent(id int64, now time.Time) (availabilityIntentClaim, bool) {
	claim := availabilityIntentClaim{}
	_, err := s.Store.UpdateMedia(id, func(media *model.Media) error {
		intent := &media.AvailabilityIntent
		if intent.Generation <= intent.CompletedGeneration || intent.LeaseUntil.After(now) || (!intent.NextAt.IsZero() && intent.NextAt.After(now)) {
			return errAvailabilityIntentNotDue
		}
		intent.LeaseGeneration = intent.Generation
		intent.LeaseUntil = now.Add(2 * time.Minute)
		seerrID := media.SeerrMediaID
		if seerrID <= 0 {
			seerrID = media.ID
		}
		claim = availabilityIntentClaim{mediaID: media.ID, generation: intent.Generation, seerrID: seerrID}
		return nil
	})
	return claim, err == nil
}

func (s *Seerr) completeAvailabilityIntent(claim availabilityIntentClaim, now time.Time, callErr error) {
	_, _ = s.Store.UpdateMedia(claim.mediaID, func(media *model.Media) error {
		intent := &media.AvailabilityIntent
		if callErr == nil {
			if claim.generation > intent.CompletedGeneration {
				intent.CompletedGeneration = claim.generation
			}
			if intent.LeaseGeneration == claim.generation {
				intent.LeaseGeneration = 0
				intent.LeaseUntil = time.Time{}
			}
			if intent.Generation == claim.generation {
				intent.Attempts = 0
				intent.NextAt = time.Time{}
			}
			return nil
		}
		if intent.LeaseGeneration == claim.generation {
			intent.LeaseGeneration = 0
			intent.LeaseUntil = time.Time{}
		}
		if intent.Generation == claim.generation {
			intent.Attempts++
			intent.NextAt = now.Add(retryAfter(intent.Attempts))
		}
		return nil
	})
}

func (s *Seerr) postAvailable(ctx context.Context, id int64) error {
	cfg := s.currentConfig()
	if strings.TrimSpace(cfg.SeerrURL) == "" || strings.TrimSpace(cfg.SeerrAPIKey) == "" {
		return errors.New("Seerr is not configured")
	}
	u := cfg.SeerrURL + "/api/v1/media/" + strconv.FormatInt(id, 10) + "/available"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, strings.NewReader(url.Values{}.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("X-Api-Key", cfg.SeerrAPIKey)
	client := s.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("Seerr availability update returned %s", resp.Status)
	}
	return nil
}

// markAvailable remains for package compatibility; durable callers use
// retryAvailability so failures survive process restart.
func (s *Seerr) markAvailable(ctx context.Context, id int64) {
	if err := s.postAvailable(ctx, id); err != nil && s.Log != nil {
		s.Log.Warn("seerr availability update failed", "component", "seerr", "media", id, "error", err)
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
	if item == nil {
		return errors.New("media is required")
	}
	if _, loaded := s.inflight.LoadOrStore(item.RequestID, struct{}{}); loaded {
		return fmt.Errorf("media is already being processed")
	}
	defer s.inflight.Delete(item.RequestID)
	if err := s.RefreshEpisodeCounts(ctx, item); err != nil && s.Log != nil {
		s.Log.Warn("refresh episode schedule", "component", "seerr", "media", item.ID, "error", err)
	}
	if s.Resolver == nil {
		return errors.New("media resolver is unavailable")
	}
	queued, err := s.Resolver.Queue(item)
	if err != nil {
		return err
	}
	if s.Scheduler != nil {
		s.Scheduler.Wake()
		return nil
	}
	if err := s.Resolver.RunDue(ctx, queued.ID); err != nil {
		return err
	}
	s.reconcile(ctx)
	return nil
}

// releaseDue is retained as an internal compatibility helper. The durable
// scheduler now recovers due unreleased records rather than launching detached
// background jobs with context.Background().
func (s *Seerr) releaseDue(ctx context.Context) {
	if s.Scheduler != nil {
		s.Scheduler.Wake()
		return
	}
	if s.Store == nil || s.Resolver == nil {
		return
	}
	for _, media := range s.Store.Media() {
		if media != nil && media.Status == "unreleased" && !IsUnreleased(media, time.Now()) {
			_ = s.Resolver.RunDue(ctx, media.ID)
		}
	}
}

func (s *Seerr) RefreshEpisodeCounts(ctx context.Context, item *model.Media) error {
	if item == nil || item.Type != "tv" || item.TMDBID <= 0 {
		return nil
	}
	details, err := s.Catalog(ctx, item.Type, item.TMDBID)
	if err != nil {
		return err
	}
	counts, dates, firstAir, err := s.EpisodeSchedule(ctx, item.TMDBID, item.Seasons, details)
	if err != nil {
		return err
	}
	if s.Store == nil {
		item.EpisodeCounts, item.EpisodeAirDates = counts, dates
		if firstAir != "" {
			item.ReleaseDate = earlierDate(item.ReleaseDate, firstAir)
		}
		return nil
	}
	stored, err := s.Store.UpdateMedia(item.ID, func(current *model.Media) error {
		current.EpisodeCounts = counts
		current.EpisodeAirDates = dates
		if firstAir != "" {
			current.ReleaseDate = earlierDate(current.ReleaseDate, firstAir)
		}
		current.UpdatedAt = time.Now().UTC()
		return nil
	})
	if err != nil {
		return err
	}
	*item = *stored
	return nil
}

func CatalogEpisodeCounts(seasons []int, available []CatalogSeason) map[int]int {
	wanted := make(map[int]bool, len(seasons))
	for _, season := range seasons {
		if season > 0 {
			wanted[season] = true
		}
	}
	counts := make(map[int]int, len(wanted))
	for _, season := range available {
		if wanted[season.SeasonNumber] && season.EpisodeCount > 0 {
			counts[season.SeasonNumber] = season.EpisodeCount
		}
	}
	if len(counts) == 0 {
		return nil
	}
	return counts
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
