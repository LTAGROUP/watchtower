package dashboard

import (
	"context"
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
	"sync"
	"time"

	"github.com/LTAGROUP/watchtower/internal/config"
	"github.com/LTAGROUP/watchtower/internal/model"
	"github.com/LTAGROUP/watchtower/internal/service"
	"github.com/LTAGROUP/watchtower/internal/store"
)

//go:embed web/*
var webFiles embed.FS

type Handler struct {
	Store    *store.Store
	Settings *config.Manager
	Resolver *service.Resolver
	Seerr    *service.Seerr
	Username string
	Password string
	Log      *slog.Logger
	direct   sync.Map
}

func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/summary", h.summary)
	mux.HandleFunc("GET /api/v1/library", h.library)
	mux.HandleFunc("GET /api/v1/queue", h.queue)
	mux.HandleFunc("GET /api/v1/settings", h.getSettings)
	mux.HandleFunc("PUT /api/v1/settings", h.putSettings)
	mux.HandleFunc("GET /api/v1/discover", h.discover)
	mux.HandleFunc("GET /api/v1/catalog/{type}/{id}", h.catalogDetails)
	mux.HandleFunc("POST /api/v1/requests", h.createRequest)
	mux.HandleFunc("GET /api/v1/media/{id}/poster", h.mediaPoster)
	mux.HandleFunc("POST /api/v1/media/{id}/reset", h.resetMedia)
	mux.HandleFunc("DELETE /api/v1/media/{id}", h.deleteMedia)
	root, _ := fs.Sub(webFiles, "web")
	files := http.FileServer(http.FS(root))
	mux.Handle("GET /", files)
	return h.basicAuth(securityHeaders(mux))
}

func (h *Handler) summary(w http.ResponseWriter, _ *http.Request) {
	media := h.Store.Media()
	files := h.Store.Files()
	statuses := map[string]int{"queued": 0, "scraping": 0, "resolving": 0, "ready": 0, "partial": 0, "failed": 0}
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
	writeJSON(w, http.StatusOK, map[string]any{"media": h.Store.Media(), "files": h.Store.Files()})
}

func (h *Handler) queue(w http.ResponseWriter, _ *http.Request) {
	items := h.Store.Media()
	sort.Slice(items, func(i, j int) bool { return items[i].UpdatedAt.After(items[j].UpdatedAt) })
	active := make([]*model.Media, 0)
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
		Seasons: input.Seasons, Status: "queued", CreatedAt: now, UpdatedAt: now,
	}
	if err := h.Store.UpsertMedia(item); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	go h.resolveDirect(item)
	writeJSON(w, http.StatusAccepted, map[string]any{"media": item})
}

func (h *Handler) catalogDetails(w http.ResponseWriter, r *http.Request) {
	kind := strings.ToLower(r.PathValue("type"))
	tmdbID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || tmdbID <= 0 || (kind != "movie" && kind != "tv") {
		writeError(w, http.StatusBadRequest, errors.New("invalid catalog item"))
		return
	}
	media, inLibrary := h.Store.FindMediaByTMDB(kind, tmdbID)
	details, err := h.Seerr.Catalog(r.Context(), kind, tmdbID)
	if err != nil {
		if !inLibrary {
			writeError(w, http.StatusBadGateway, err)
			return
		}
		details = catalogFromMedia(media)
	}
	if inLibrary {
		changed := media.Overview == "" || media.PosterPath == "" || media.BackdropPath == ""
		media.Overview, media.PosterPath, media.BackdropPath = details.Overview, details.PosterPath, details.BackdropPath
		if changed {
			_ = h.Store.UpsertMedia(media)
		}
	}
	files := []detailFile{}
	if inLibrary {
		files = detailFiles(h.Store.FilesForMedia(media.ID))
	}
	writeJSON(w, http.StatusOK, map[string]any{"details": details, "inLibrary": inLibrary, "media": media, "files": files})
}

func catalogFromMedia(media *model.Media) service.CatalogDetails {
	details := service.CatalogDetails{ID: media.TMDBID, Overview: media.Overview, PosterPath: media.PosterPath, BackdropPath: media.BackdropPath}
	if media.Type == "tv" {
		details.Name = media.Title
		if media.Year > 0 {
			details.FirstAirDate = strconv.Itoa(media.Year) + "-01-01"
		}
		for _, season := range media.Seasons {
			details.Seasons = append(details.Seasons, service.CatalogSeason{SeasonNumber: season, Name: fmt.Sprintf("Season %d", season)})
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
			media.Overview, media.PosterPath, media.BackdropPath = details.Overview, details.PosterPath, details.BackdropPath
			_ = h.Store.UpsertMedia(media)
		}
	}
	if media.PosterPath == "" || !strings.HasPrefix(media.PosterPath, "/") {
		http.NotFound(w, r)
		return
	}
	http.Redirect(w, r, "https://image.tmdb.org/t/p/w500"+media.PosterPath, http.StatusFound)
}

func (h *Handler) resetMedia(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		writeError(w, http.StatusBadRequest, errors.New("invalid media id"))
		return
	}
	item, err := h.Store.ResetMedia(id)
	if errors.Is(err, os.ErrNotExist) {
		writeError(w, http.StatusNotFound, errors.New("media item not found"))
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	go func() {
		var err error
		if item.RequestID > 0 {
			err = h.Seerr.Retry(context.Background(), item)
		} else {
			err = h.Resolver.Resolve(context.Background(), item)
		}
		if err != nil && h.Log != nil {
			h.Log.Error("dashboard retry failed", "media", item.ID, "error", err)
		}
	}()
	writeJSON(w, http.StatusAccepted, map[string]any{"media": item})
}

func (h *Handler) deleteMedia(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		writeError(w, http.StatusBadRequest, errors.New("invalid media id"))
		return
	}
	media, ok := h.Store.MediaByID(id)
	if !ok {
		writeError(w, http.StatusNotFound, errors.New("media item not found"))
		return
	}
	if media.Status == "queued" || media.Status == "scraping" || media.Status == "resolving" {
		writeError(w, http.StatusConflict, errors.New("wait for active resolution to finish before deleting"))
		return
	}
	if err := h.Store.DeleteMedia(id); err != nil {
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

func (h *Handler) resolveDirect(item *model.Media) {
	if _, loaded := h.direct.LoadOrStore(item.ID, struct{}{}); loaded {
		return
	}
	defer h.direct.Delete(item.ID)
	if err := h.Resolver.Resolve(context.Background(), item); err != nil && h.Log != nil {
		h.Log.Error("direct media request failed", "media", item.ID, "error", err)
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
