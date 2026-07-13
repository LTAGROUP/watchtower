package store

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/LTAGROUP/watchtower/internal/model"
)

func TestResetMediaRemovesFilesAndProcessedMarker(t *testing.T) {
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
	if len(s.Files()) != 0 || s.IsProcessed(11) {
		t.Fatal("reset did not clear files and processed marker")
	}
}
