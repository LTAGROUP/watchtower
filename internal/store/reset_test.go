package store

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/LTAGROUP/watchtower/internal/model"
)

func TestResetMediaKeepsFilesAvailableAndRemovesProcessedMarker(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	m := &model.Media{ID: 7, RequestID: 11, Title: "Example", Status: "failed", Error: "no release", ScrapedAt: time.Now(), CreatedAt: time.Now(), UpdatedAt: time.Now()}
	if err := s.UpsertMedia(m); err != nil {
		t.Fatal(err)
	}
	if err := s.AddFiles(&model.File{ID: "file", MediaID: 7, Path: "Movies/Example.mkv"}); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkProcessed(11); err != nil {
		t.Fatal(err)
	}
	reset, err := s.ResetMedia(7)
	if err != nil {
		t.Fatal(err)
	}
	if reset.Status != "queued" || reset.Error != "" || !reset.ScrapedAt.IsZero() {
		t.Fatalf("unexpected reset media: %#v", reset)
	}
	if len(s.Files()) != 1 || s.IsProcessed(11) {
		t.Fatal("reset removed active files or retained processed marker")
	}
}

func TestReplaceFilesForMediaSwapsOnlyTargetMedia(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	old := &model.File{ID: "old", MediaID: 7, Path: "Movies/Old.mkv"}
	other := &model.File{ID: "other", MediaID: 8, Path: "Movies/Other.mkv"}
	if err := s.AddFiles(old, other); err != nil {
		t.Fatal(err)
	}
	replacement := &model.File{ID: "new", MediaID: 7, Path: "Movies/New.mkv"}
	if err := s.ReplaceFilesForMedia(7, replacement); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.File("old"); ok {
		t.Fatal("old media file remained after replacement")
	}
	if _, ok := s.File("new"); !ok {
		t.Fatal("replacement media file was not stored")
	}
	if _, ok := s.File("other"); !ok {
		t.Fatal("unrelated media file was removed")
	}
}

func TestOpenRepairsLegacyMediaIDs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	legacy := `{
  "media": {"42": {"type":"movie","title":"Legacy"}},
  "files": {"file": {"id":"file","mediaId":42,"Path":"Movies/Legacy.mkv"}},
  "processedRequests": {}
}`
	if err := os.WriteFile(path, []byte(legacy), 0600); err != nil {
		t.Fatal(err)
	}
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	media := s.Media()
	if len(media) != 1 || media[0].ID != 42 || media[0].Status != "ready" {
		t.Fatalf("legacy media was not repaired: %#v", media)
	}
	reset, err := s.ResetMedia(42)
	if err != nil {
		t.Fatal(err)
	}
	if reset.ID != 42 || reset.Status != "queued" {
		t.Fatalf("repaired media could not be reset: %#v", reset)
	}
}

func TestDeleteMediaRemovesLibraryFiles(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertMedia(&model.Media{ID: 12, RequestID: 44, Status: "ready"}); err != nil {
		t.Fatal(err)
	}
	if err := s.AddFiles(&model.File{ID: "file", MediaID: 12}); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkProcessed(44); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteMedia(12); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.MediaByID(12); ok || len(s.Files()) != 0 {
		t.Fatal("media or files remained after delete")
	}
	if !s.IsProcessed(44) {
		t.Fatal("Seerr processed marker should remain after deletion")
	}
}
