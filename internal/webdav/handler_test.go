package webdav

import (
	"net/http/httptest"
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
