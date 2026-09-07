package webdav

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/LTAGROUP/watchtower/internal/model"
	"github.com/LTAGROUP/watchtower/internal/store"
)

func TestPropfindBuildsVirtualTree(t *testing.T) {
	d := t.TempDir()
	st, err := store.Open(d + "/state.json")
	if err != nil {
		t.Fatal(err)
	}
	if err = st.AddFiles(&model.File{ID: "x", Path: "Movies/Example (2025)/Example (2025) - 1080p.mkv", Size: 42, CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	h := &Handler{Store: st, Prefix: "/dav"}
	req := httptest.NewRequest("PROPFIND", "http://example/dav/Movies", nil)
	req.Header.Set("Depth", "1")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != 207 {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
	body := w.Body.String()
	if !strings.Contains(body, "Example%20%282025%29") {
		t.Fatalf("missing child in %s", body)
	}
	if _, err = os.Stat(d + "/state.json"); err != nil {
		t.Fatal(err)
	}
}

func TestPropfindPreservesLiteralPercentPaths(t *testing.T) {
	for _, name := range []string{"100% Fun", "Literal%20Title", "Literal%2FTitle"} {
		t.Run(name, func(t *testing.T) {
			st, err := store.Open(t.TempDir() + "/state.json")
			if err != nil {
				t.Fatal(err)
			}
			file := &model.File{ID: "x", Path: "Movies/" + name + "/movie.mkv", Size: 42}
			if err := st.AddFiles(file); err != nil {
				t.Fatal(err)
			}
			h := &Handler{Store: st, Prefix: "/dav"}
			req := httptest.NewRequest("PROPFIND", "http://example/dav/Movies/"+url.PathEscape(name), nil)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)
			if w.Code != 207 || !strings.Contains(w.Body.String(), "movie.mkv") {
				t.Fatalf("path not found: %d %s", w.Code, w.Body.String())
			}
		})
	}
}

func TestPropfindTracksSameSizeReplacement(t *testing.T) {
	st, err := store.Open(t.TempDir() + "/state.json")
	if err != nil {
		t.Fatal(err)
	}
	h := &Handler{Store: st, Prefix: "/dav"}
	for i := 0; i < 2; i++ {
		published := time.Date(2026, 1, 1, 0, i, 0, 0, time.UTC)
		f := &model.File{ID: fmt.Sprint(i), MediaID: 1, Path: "Movies/Example/movie.mkv", Size: 42, CreatedAt: published}
		if err := st.ReplaceFilesForMedia(1, f); err != nil {
			t.Fatal(err)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("PROPFIND", "http://example/dav/"+f.Path, nil))
		if !strings.Contains(w.Body.String(), published.Format(http.TimeFormat)) {
			t.Fatalf("publication time missing: %s", w.Body.String())
		}
	}
}

func TestEmptyLibraryRootsExist(t *testing.T) {
	st, err := store.Open(t.TempDir() + "/state.json")
	if err != nil {
		t.Fatal(err)
	}
	h := &Handler{Store: st, Prefix: "/dav"}
	for _, root := range []string{"Movies", "TV"} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("PROPFIND", "http://example/dav/"+root, nil))
		if w.Code != 207 {
			t.Fatalf("empty %s root missing: %d", root, w.Code)
		}
	}
}
