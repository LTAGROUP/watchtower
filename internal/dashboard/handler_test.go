package dashboard

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/LTAGROUP/watchtower/internal/config"
	"github.com/LTAGROUP/watchtower/internal/model"
	"github.com/LTAGROUP/watchtower/internal/store"
)

func TestDashboardRequiresBasicAuthAndReturnsSummary(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertMedia(&model.Media{ID: 1, Title: "Ready", Status: "ready", ScrapedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if err := st.AddFiles(&model.File{ID: "f", MediaID: 1, Path: "Movies/Ready.mkv", Size: 1024}); err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{SettingsFile: filepath.Join(dir, "settings.json"), Providers: []string{"torbox"}, Qualities: []string{"1080p"}, StremioAddons: []string{"x|http://x/manifest.json"}, PollInterval: time.Minute, ResolveTimeout: time.Minute, StreamURLTTL: time.Minute, MaxResults: 20}
	settings, err := config.OpenManager(cfg)
	if err != nil {
		t.Fatal(err)
	}
	handler := (&Handler{Store: st, Settings: settings, Username: "admin", Password: "secret"}).Routes()

	unauthorized := httptest.NewRecorder()
	handler.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, "/api/v1/summary", nil))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", unauthorized.Code)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/summary", nil)
	req.SetBasicAuth("admin", "secret")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	if response.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", response.Code)
	}
	var result struct {
		Indexed, Scraped, Files int
		Bytes                   int64
		Statuses                map[string]int
	}
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if result.Indexed != 1 || result.Scraped != 1 || result.Files != 1 || result.Bytes != 1024 || result.Statuses["ready"] != 1 {
		t.Fatalf("unexpected summary: %#v", result)
	}
}

func TestLibraryUsesFrontendJSONFieldNames(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertMedia(&model.Media{ID: 8, RequestID: 9, Title: "Example", Status: "partial", UpdatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if err := st.AddFiles(&model.File{ID: "file", MediaID: 8, Path: "Movies/Example.mkv", Quality: "1080p", Provider: "torbox"}); err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{SettingsFile: filepath.Join(dir, "settings.json"), Providers: []string{"torbox"}, Qualities: []string{"1080p"}, StremioAddons: []string{"x|http://x/manifest.json"}, PollInterval: time.Minute, ResolveTimeout: time.Minute, StreamURLTTL: time.Minute, MaxResults: 20}
	settings, err := config.OpenManager(cfg)
	if err != nil {
		t.Fatal(err)
	}
	handler := (&Handler{Store: st, Settings: settings, Username: "admin", Password: "secret"}).Routes()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/library", nil)
	req.SetBasicAuth("admin", "secret")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	var result struct {
		Media []map[string]any `json:"media"`
		Files []map[string]any `json:"files"`
	}
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if result.Media[0]["id"] != float64(8) || result.Media[0]["status"] != "partial" || result.Files[0]["path"] != "Movies/Example.mkv" {
		t.Fatalf("unexpected library JSON: %#v", result)
	}
}
