package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LTAGROUP/watchtower/internal/config"
	"github.com/LTAGROUP/watchtower/internal/store"
)

func TestReadinessUsesValidatedLocalStateWithoutProviderProbes(t *testing.T) {
	var probes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		probes.Add(1)
		http.Error(w, "should not be called", http.StatusInternalServerError)
	}))
	defer server.Close()
	dir := t.TempDir()
	settings, err := config.OpenManager(config.Config{
		SettingsFile: filepath.Join(dir, "settings.json"), SeerrURL: server.URL, SeerrAPIKey: "key", TorBoxToken: "token",
		Providers: []string{"torbox"}, Qualities: []string{"1080p"}, StremioAddons: []string{"x|http://addon.invalid/manifest.json"},
		PollInterval: time.Minute, ResolveTimeout: time.Minute, StreamURLTTL: time.Minute, PlexScanDelay: time.Second, MaxResults: 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	state, err := store.Open(filepath.Join(dir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	ready := httptest.NewRecorder()
	readinessHandler(settings, state).ServeHTTP(ready, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if ready.Code != http.StatusOK || probes.Load() != 0 {
		t.Fatalf("readyz should be local-only: code=%d probes=%d body=%s", ready.Code, probes.Load(), ready.Body.String())
	}
	var result map[string]any
	if err := json.NewDecoder(ready.Body).Decode(&result); err != nil || result["status"] != "ready" {
		t.Fatalf("unexpected readiness body: %#v, %v", result, err)
	}
	notReady := httptest.NewRecorder()
	readinessHandler(nil, state).ServeHTTP(notReady, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if notReady.Code != http.StatusServiceUnavailable || !containsReadinessError(notReady.Body.Bytes(), "settings") {
		t.Fatalf("not-ready response was not useful: %d %s", notReady.Code, notReady.Body.String())
	}
}

func containsReadinessError(body []byte, wanted string) bool {
	var result struct {
		Errors []string `json:"errors"`
	}
	if json.Unmarshal(body, &result) != nil {
		return false
	}
	for _, value := range result.Errors {
		if len(value) >= len(wanted) && value[:len(wanted)] == wanted {
			return true
		}
	}
	return false
}
