package dashboard

import (
	"crypto/sha256"
	"crypto/subtle"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/LTAGROUP/watchtower/internal/config"
	"github.com/LTAGROUP/watchtower/internal/logging"
	"github.com/LTAGROUP/watchtower/internal/model"
	"github.com/LTAGROUP/watchtower/internal/service"
	"github.com/LTAGROUP/watchtower/internal/store"
)

//go:embed web/*
var webFiles embed.FS

type Handler struct {
	Store     *store.Store
	Settings  *config.Manager
	Resolver  *service.Resolver
	Seerr     *service.Seerr
	Scheduler *service.Lifecycle
	Username  string
	Password  string
	Log       *slog.Logger
	Logs      *logging.Buffer
}

type libraryMediaView struct {
	*model.Media
	QualityAvailability []model.QualityAvailability `json:"qualityAvailability"`
}

func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/summary", h.summary)
	mux.HandleFunc("GET /api/v1/library", h.library)
	mux.HandleFunc("GET /api/v1/queue", h.queue)
	mux.HandleFunc("GET /api/v1/logs", h.logs)
	mux.HandleFunc("GET /api/v1/settings", h.getSettings)
	mux.HandleFunc("PUT /api/v1/settings", h.putSettings)
	mux.HandleFunc("GET /api/v1/discover", h.discover)
	mux.HandleFunc("GET /api/v1/catalog/{type}/{id}", h.catalogDetails)
	mux.HandleFunc("POST /api/v1/requests", h.createRequest)
	mux.HandleFunc("GET /api/v1/media/{id}/poster", h.mediaPoster)
	mux.HandleFunc("POST /api/v1/media/{id}/reset", h.resetMedia)
	mux.HandleFunc("POST /api/v1/media/{id}/rerequest", h.rerequestMedia)
	mux.HandleFunc("DELETE /api/v1/media/{id}", h.deleteMedia)
	root, _ := fs.Sub(webFiles, "web")
	files := http.FileServer(http.FS(root))
	mux.Handle("GET /", files)
	return h.basicAuth(securityHeaders(mux))
}

func (h *Handler) logs(w http.ResponseWriter, _ *http.Request) {
	if h.Logs == nil {
		writeJSON(w, http.StatusOK, map[string]any{"entries": []logging.Entry{}, "capacity": 0})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"entries": h.Logs.Entries(), "capacity": h.Logs.Capacity()})
}

func (h *Handler) summary(w http.ResponseWriter, _ *http.Request) {
	files := h.Store.Files()
	media := h.mediaViews(h.Store.Media(), files)
	statuses := map[string]int{"queued": 0, "unreleased": 0, "scraping": 0, "resolving": 0, "ready": 0, "partial": 0, "failed": 0}
	scraped := 0
	var bytes int64
	for _, item := range media {
		statuses[item.Status]++
		if !item.ScrapedAt.IsZero() {
			scraped++
		}
	}
	for _, file := range files {
		bytes += file.Size
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"indexed": len(media), "scraped": scraped, "files": len(files), "bytes": bytes,
		"statuses": statuses, "updatedAt": time.Now().UTC(),
	})
}

func (h *Handler) library(w http.ResponseWriter, _ *http.Request) {
	files := h.Store.Files()
	writeJSON(w, http.StatusOK, map[string]any{"media": h.mediaViews(h.Store.Media(), files), "files": files})
}

func (h *Handler) mediaViews(items []*model.Media, files []*model.File) []libraryMediaView {
	qualities := []string(nil)
	if h.Settings != nil {
		qualities = h.Settings.Snapshot().Qualities
	}
	byMedia := make(map[int64][]*model.File)
	for _, file := range files {
		byMedia[file.MediaID] = append(byMedia[file.MediaID], file)
	}
	views := make([]libraryMediaView, 0, len(items))
	for _, item := range items {
		copy := *item
		mediaFiles := byMedia[item.ID]
		copy.Status = service.EffectiveMediaStatus(&copy, mediaFiles, qualities)
		views = append(views, libraryMediaView{Media: &copy, QualityAvailability: service.QualityAvailability(&copy, mediaFiles, qualities)})
	}
	return views
}

func (h *Handler) queue(w http.ResponseWriter, _ *http.Request) {
	files := h.Store.Files()
	items := h.mediaViews(h.Store.Media(), files)
	sort.Slice(items, func(i, j int) bool { return items[i].UpdatedAt.After(items[j].UpdatedAt) })
	active := make([]libraryMediaView, 0)
	for _, item := range items {
		if item.Status != "ready" {
			active = append(active, item)
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": active})
}

func (h *Handler) getSettings(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"settings": h.Settings.Public()})
}

func (h *Handler) putSettings(w http.ResponseWriter, r *http.Request) {
	var update config.SettingsUpdate
	if err := decodeJSON(w, r, &update); err != nil {
		return
	}
	if err := h.Settings.Update(update); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"settings": h.Settings.Public()})
}

func (h *Handler) discover(w http.ResponseWriter, r *http.Request) {
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	result, err := h.Seerr.Discover(r.Context(), service.DiscoverOptions{
		Query: r.URL.Query().Get("query"), MediaType: r.URL.Query().Get("mediaType"),
		Genre: r.URL.Query().Get("genre"), Year: r.URL.Query().Get("year"),
		Sort: r.URL.Query().Get("sort"), Page: page,
	})
	if err != nil {
		writeError(w, http.StatusBadGateway, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(result)
}

func (h *Handler) createRequest(w http.ResponseWriter, r *http.Request) {
	var input service.CreateRequestInput
	if err := decodeJSON(w, r, &input); err != nil {
		return
	}
	if input.MediaID <= 0 || (input.MediaType != "movie" && input.MediaType != "tv") {
		writeError(w, http.StatusBadRequest, errors.New("mediaId and a movie or tv mediaType are required"))
		return
	}
	if _, exists := h.Store.FindMediaByTMDB(input.MediaType, input.MediaID); exists {
		writeError(w, http.StatusConflict, errors.New("this title is already in the WatchTower library"))
		return
	}
	details, err := h.Seerr.Catalog(r.Context(), input.MediaType, input.MediaID)
	if err != nil {
		writeError(w, http.StatusBadGateway, err)
		return
	}
	if input.MediaType == "tv" {
		input.Seasons = validSeasons(input.Seasons, details.Seasons)
		if len(input.Seasons) == 0 {
			writeError(w, http.StatusBadRequest, errors.New("choose at least one available season"))
			return
		}
	}
	title := details.Title
	year := yearFromDate(details.ReleaseDate)
	if input.MediaType == "tv" {
		title = details.Name
		year = yearFromDate(details.FirstAirDate)
	}
	externalID := details.IMDBID
	if externalID == "" {
		externalID = details.ExternalIDs.IMDBID
	}
	now := time.Now().UTC()
	item := &model.Media{
		ID: directMediaID(input.MediaType, input.MediaID), Type: input.MediaType, TMDBID: input.MediaID,
		ExternalID: externalID, Title: title, Year: year, Overview: details.Overview,
		PosterPath: details.PosterPath, BackdropPath: details.BackdropPath,
		Seasons: input.Seasons, ReleaseDate: h.Seerr.MediaReleaseDate(r.Context(), input.MediaType, input.MediaID, details, input.Seasons), Status: "queued", CreatedAt: now, UpdatedAt: now,
	}
	if input.MediaType == "tv" {
		if counts, dates, firstAir, scheduleErr := h.Seerr.EpisodeSchedule(r.Context(), input.MediaID, input.Seasons, details); scheduleErr == nil {
			item.EpisodeCounts, item.EpisodeAirDates = counts, dates
			if firstAir != "" && (item.ReleaseDate == "" || firstAir < item.ReleaseDate) {
				item.ReleaseDate = firstAir
			}
		} else if h.Log != nil {
			h.Log.Warn("direct request episode schedule unavailable", "media", input.MediaID, "error", scheduleErr)
		}
	}
	if h.Resolver == nil {
		writeError(w, http.StatusServiceUnavailable, errors.New("media resolver is unavailable"))
		return
	}
	queued, err := h.Resolver.Queue(item)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	h.wakeScheduler()
	writeJSON(w, http.StatusAccepted, map[string]any{"media": queued})
}

func (h *Handler) catalogDetails(w http.ResponseWriter, r *http.Request) {
	kind := strings.ToLower(r.PathValue("type"))
	tmdbID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || tmdbID <= 0 || (kind != "movie" && kind != "tv") {
		writeError(w, http.StatusBadRequest, errors.New("invalid catalog item"))
		return
	}
	// Keep this pre-fetch record only as a catalog/schedule snapshot. It must
	// never be written back wholesale after an HTTP call: a resolver can claim
	// newer durable work while Seerr is responding.
	media, inLibrary := h.Store.FindMediaByTMDB(kind, tmdbID)
	details, err := h.Seerr.Catalog(r.Context(), kind, tmdbID)
	catalogFetched := err == nil
	if err != nil {
		if !inLibrary {
			writeError(w, http.StatusBadGateway, err)
			return
		}
		details = catalogFromMedia(media)
	}
	if inLibrary && catalogFetched {
		var schedule *episodeSchedule
		if media.Type == "tv" {
			counts, dates, _, scheduleErr := h.Seerr.EpisodeSchedule(r.Context(), tmdbID, media.Seasons, details)
			if scheduleErr == nil {
				schedule = &episodeSchedule{counts: counts, dates: dates}
			} else if h.Log != nil {
				h.Log.Warn("catalog episode schedule unavailable", "media", tmdbID, "error", scheduleErr)
			}
		}
		media, inLibrary = h.patchCatalogMetadata(media, details, schedule)
	}
	files := []detailFile{}
	if inLibrary {
		files = detailFiles(h.Store.FilesForMedia(media.ID))
	}
	writeJSON(w, http.StatusOK, map[string]any{"details": details, "inLibrary": inLibrary, "media": media, "files": files})
}

type episodeSchedule struct {
	counts map[int]int
	dates  []model.EpisodeAirDate
}

// patchCatalogMetadata updates only fields owned by catalog enrichment. The
// stored record is loaded while UpdateMedia holds the store lock, so a newer
// Work lease, status, request association, or durable intent is retained.
func (h *Handler) patchCatalogMetadata(snapshot *model.Media, details service.CatalogDetails, schedule *episodeSchedule) (*model.Media, bool) {
	if snapshot == nil {
		return nil, false
	}
	updated, err := h.Store.UpdateMedia(snapshot.ID, func(current *model.Media) error {
		if current.Type != snapshot.Type || current.TMDBID != snapshot.TMDBID {
			// The original item was deleted and its local ID reused. A GET must
			// not recreate or enrich a different media record.
			return os.ErrNotExist
		}
		current.Overview = details.Overview
		current.PosterPath = details.PosterPath
		current.BackdropPath = details.BackdropPath
		if schedule != nil {
			// Episode counts and dates are a single scheduling unit. Publish both
			// only after the complete EpisodeSchedule request succeeded.
			current.EpisodeCounts = schedule.counts
			current.EpisodeAirDates = schedule.dates
		}
		return nil
	})
	if err == nil {
		return updated, true
	}
	if !errors.Is(err, os.ErrNotExist) && h.Log != nil {
		h.Log.Warn("catalog metadata refresh unavailable", "media", snapshot.ID, "error", err)
	}
	return nil, false
}

func catalogFromMedia(media *model.Media) service.CatalogDetails {
	details := service.CatalogDetails{ID: media.TMDBID, Overview: media.Overview, PosterPath: media.PosterPath, BackdropPath: media.BackdropPath}
	if media.Type == "tv" {
		details.Name = media.Title
		if media.Year > 0 {
			details.FirstAirDate = strconv.Itoa(media.Year) + "-01-01"
		}
		for _, season := range media.Seasons {
			details.Seasons = append(details.Seasons, service.CatalogSeason{SeasonNumber: season, Name: fmt.Sprintf("Season %d", season), EpisodeCount: media.EpisodeCounts[season]})
		}
	} else {
		details.Title = media.Title
		if media.Year > 0 {
			details.ReleaseDate = strconv.Itoa(media.Year) + "-01-01"
		}
	}
	return details
}

func (h *Handler) mediaPoster(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		http.NotFound(w, r)
		return
	}
	media, ok := h.Store.MediaByID(id)
	if !ok {
		http.NotFound(w, r)
		return
	}
	if media.PosterPath == "" && media.TMDBID > 0 {
		if details, err := h.Seerr.Catalog(r.Context(), media.Type, media.TMDBID); err == nil {
			media, ok = h.patchPosterMetadata(media, details)
			if !ok {
				http.NotFound(w, r)
				return
			}
		}
	}
	if media.PosterPath == "" || !strings.HasPrefix(media.PosterPath, "/") {
		http.NotFound(w, r)
		return
	}
	http.Redirect(w, r, "https://image.tmdb.org/t/p/w500"+media.PosterPath, http.StatusFound)
}

// patchPosterMetadata deliberately fills only the missing poster field. The
// fallback is a GET convenience and must not overwrite newer catalog metadata
// or any resolver-owned lifecycle state.
func (h *Handler) patchPosterMetadata(snapshot *model.Media, details service.CatalogDetails) (*model.Media, bool) {
	if snapshot == nil {
		return nil, false
	}
	updated, err := h.Store.UpdateMedia(snapshot.ID, func(current *model.Media) error {
		if current.Type != snapshot.Type || current.TMDBID != snapshot.TMDBID {
			return os.ErrNotExist
		}
		if current.PosterPath == "" {
			current.PosterPath = details.PosterPath
		}
		return nil
	})
	if err == nil {
		return updated, true
	}
	if !errors.Is(err, os.ErrNotExist) && h.Log != nil {
		h.Log.Warn("poster metadata refresh unavailable", "media", snapshot.ID, "error", err)
	}
	return nil, false
}

func (h *Handler) resetMedia(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		writeError(w, http.StatusBadRequest, errors.New("invalid media id"))
		return
	}
	if h.Resolver == nil {
		writeError(w, http.StatusServiceUnavailable, errors.New("media resolver is unavailable"))
		return
	}
	// QueueReset derives the current record and unmarks its request markers in
	// the same durable transaction. Do not snapshot/reset/queue separately:
	// a resolver completion between those steps would otherwise be overwritten.
	queued, err := h.Resolver.QueueReset(id)
	if errors.Is(err, os.ErrNotExist) {
		writeError(w, http.StatusNotFound, errors.New("media item not found"))
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	h.wakeScheduler()
	writeJSON(w, http.StatusAccepted, map[string]any{"media": queued})
}

type rerequestInput struct {
	Season  int `json:"season"`
	Episode int `json:"episode,omitempty"`
}

func (h *Handler) rerequestMedia(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		writeError(w, http.StatusBadRequest, errors.New("invalid media id"))
		return
	}
	var input rerequestInput
	if err := decodeJSON(w, r, &input); err != nil {
		return
	}
	if input.Season <= 0 || input.Episode < 0 {
		writeError(w, http.StatusBadRequest, errors.New("choose a tracked TV season and optional episode"))
		return
	}
	if h.Resolver == nil {
		writeError(w, http.StatusServiceUnavailable, errors.New("media resolver is unavailable"))
		return
	}
	// Validation and queueing both run against the current stored record. A
	// stale handler snapshot must not recreate a record that was just deleted.
	queued, err := h.Resolver.QueueExistingRerequest(id, input.Season, input.Episode)
	if errors.Is(err, os.ErrNotExist) {
		writeError(w, http.StatusNotFound, errors.New("media item not found"))
		return
	}
	if errors.Is(err, service.ErrInvalidRerequestScope) {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	h.wakeScheduler()
	writeJSON(w, http.StatusAccepted, map[string]any{"media": queued, "season": input.Season, "episode": input.Episode})
}

func (h *Handler) deleteMedia(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		writeError(w, http.StatusBadRequest, errors.New("invalid media id"))
		return
	}
	// DeleteInactiveMedia validates current durable work and removes media/files
	// while holding one store transaction. A reset or re-request that wins the
	// race therefore becomes a conflict rather than being deleted afterward.
	if err := h.Store.DeleteInactiveMedia(id); errors.Is(err, os.ErrNotExist) {
		writeError(w, http.StatusNotFound, errors.New("media item not found"))
		return
	} else if errors.Is(err, store.ErrMediaActive) {
		writeError(w, http.StatusConflict, errors.New("wait for active resolution to finish before deleting"))
		return
	} else if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type detailFile struct {
	ID              string    `json:"id"`
	Path            string    `json:"path"`
	Quality         string    `json:"quality"`
	Provider        string    `json:"provider"`
	Size            int64     `json:"size"`
	CreatedAt       time.Time `json:"createdAt"`
	StreamState     string    `json:"streamState"`
	StreamExpiresAt time.Time `json:"streamExpiresAt,omitempty"`
}

func detailFiles(files []*model.File) []detailFile {
	out := make([]detailFile, 0, len(files))
	for _, file := range files {
		state := "on demand"
		if file.StreamURL != "" && time.Now().Before(file.StreamExpiresAt) {
			state = "warm"
		}
		out = append(out, detailFile{ID: file.ID, Path: file.Path, Quality: file.Quality, Provider: file.Provider, Size: file.Size, CreatedAt: file.CreatedAt, StreamState: state, StreamExpiresAt: file.StreamExpiresAt})
	}
	return out
}

func (h *Handler) wakeScheduler() {
	if h.Scheduler != nil {
		h.Scheduler.Wake()
	}
}

func directMediaID(kind string, tmdbID int64) int64 {
	if kind == "tv" {
		return 2_000_000_000_000 + tmdbID
	}
	return 1_000_000_000_000 + tmdbID
}

func validSeasons(requested []int, available []service.CatalogSeason) []int {
	valid := map[int]bool{}
	for _, season := range available {
		if season.SeasonNumber > 0 {
			valid[season.SeasonNumber] = true
		}
	}
	seen := map[int]bool{}
	out := make([]int, 0, len(requested))
	for _, season := range requested {
		if valid[season] && !seen[season] {
			seen[season] = true
			out = append(out, season)
		}
	}
	sort.Ints(out)
	return out
}

func yearFromDate(value string) int {
	if len(value) < 4 {
		return 0
	}
	year, _ := strconv.Atoi(value[:4])
	return year
}

func containsInt(values []int, wanted int) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func (h *Handler) basicAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		username, password, ok := r.BasicAuth()
		expectedUser := sha256.Sum256([]byte(h.Username))
		actualUser := sha256.Sum256([]byte(username))
		expectedPassword := sha256.Sum256([]byte(h.Password))
		actualPassword := sha256.Sum256([]byte(password))
		if !ok || subtle.ConstantTimeCompare(actualUser[:], expectedUser[:]) != 1 || subtle.ConstantTimeCompare(actualPassword[:], expectedPassword[:]) != 1 {
			w.Header().Set("WWW-Authenticate", `Basic realm="WatchTower", charset="UTF-8"`)
			http.Error(w, "authentication required", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Referrer-Policy", "same-origin")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; img-src 'self' https://image.tmdb.org data:; style-src 'self'; script-src 'self'; connect-src 'self'")
		if r.Method != http.MethodGet && r.Method != http.MethodHead && r.Method != http.MethodOptions {
			if origin := r.Header.Get("Origin"); origin != "" {
				parsed, err := url.Parse(origin)
				if err != nil || !strings.EqualFold(parsed.Host, r.Host) {
					writeError(w, http.StatusForbidden, errors.New("cross-origin request rejected"))
					return
				}
			}
		}
		next.ServeHTTP(w, r)
	})
}

func decodeJSON(w http.ResponseWriter, r *http.Request, target any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		writeError(w, http.StatusBadRequest, errors.New("invalid request body"))
		return err
	}
	return nil
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": strings.TrimSpace(err.Error())})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
