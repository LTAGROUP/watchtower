package dashboard

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"embed"
	"encoding/json"
	"errors"
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
}

func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/summary", h.summary)
	mux.HandleFunc("GET /api/v1/library", h.library)
	mux.HandleFunc("GET /api/v1/queue", h.queue)
	mux.HandleFunc("GET /api/v1/settings", h.getSettings)
	mux.HandleFunc("PUT /api/v1/settings", h.putSettings)
	mux.HandleFunc("GET /api/v1/discover", h.discover)
	mux.HandleFunc("POST /api/v1/requests", h.createRequest)
	mux.HandleFunc("POST /api/v1/media/{id}/reset", h.resetMedia)
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
	result, err := h.Seerr.CreateRequest(r.Context(), input)
	if err != nil {
		writeError(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"request": json.RawMessage(result)})
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
		if err := h.Seerr.Retry(context.Background(), item); err != nil && h.Log != nil {
			h.Log.Error("dashboard retry failed", "media", item.ID, "error", err)
		}
	}()
	writeJSON(w, http.StatusAccepted, map[string]any{"media": item})
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
