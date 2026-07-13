package service

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/LTAGROUP/watchtower/internal/config"
	"github.com/LTAGROUP/watchtower/internal/debrid"
	"github.com/LTAGROUP/watchtower/internal/model"
	"github.com/LTAGROUP/watchtower/internal/scraper"
	"github.com/LTAGROUP/watchtower/internal/store"
)

type fixedSearcher struct {
	releases []model.Release
}

func (s fixedSearcher) Search(context.Context, scraper.Query, int) ([]model.Release, error) {
	return append([]model.Release(nil), s.releases...), nil
}

type fixedProvider struct {
	resolved model.Resolved
}

func (p fixedProvider) Name() string { return "test" }
func (p fixedProvider) Resolve(context.Context, model.Release) (model.Resolved, error) {
	return p.resolved, nil
}
func (p fixedProvider) StreamURL(context.Context, *model.File) (string, error) {
	return "", nil
}

func TestMaterializeMovieAndSeasonPack(t *testing.T) {
	r := Resolver{}
	movie := &model.Media{ID: 1, Type: "movie", Title: "A/B: Story", Year: 2025, ExternalID: "tt1234567", TMDBID: 123}
	got := r.materialize(movie, "1080p", "torbox", model.Release{}, model.Resolved{ItemID: "1", Files: []model.RemoteFile{{ID: "a", Name: "sample.mkv", Size: 1}, {ID: "b", Name: "feature.mkv", Size: 100}}})
	if len(got) != 1 || got[0].Path != "Movies/A-B - Story (2025) {imdb-tt1234567}/A-B - Story (2025) {imdb-tt1234567} [1080p].mkv" {
		t.Fatalf("movie path: %#v", got)
	}
	tv := &model.Media{ID: 2, Type: "tv", Title: "Example", Year: 2024, TMDBID: 456, Seasons: []int{1}}
	got = r.materialize(tv, "2160p", "alldebrid", model.Release{}, model.Resolved{ItemID: "2", Files: []model.RemoteFile{{ID: "1", Name: "Example.S01E02.mkv", Size: 10}, {ID: "2", Name: "Example.S02E01.mkv", Size: 10}, {ID: "3", Name: "notes.txt", Size: 1}}})
	if len(got) != 1 || got[0].Path != "TV/Example (2024) {tmdb-456}/Season 01/Example (2024) - S01E02 [2160p].mkv" {
		t.Fatalf("tv paths: %#v", got)
	}
}

func TestPlexFolderNameOmitsUnknownYearAndID(t *testing.T) {
	m := &model.Media{Title: "Unknown Year"}
	if got := plexFolderName(m); got != "Unknown Year" {
		t.Fatalf("folder name: %q", got)
	}
}

func TestQualityAliases(t *testing.T) {
	if !matchesQuality("Example UHD 4K REMUX", "2160p") {
		t.Fatal("4K should satisfy 2160p")
	}
	if !matchesQuality("Example FHD WEB-DL", "1080p") {
		t.Fatal("FHD should satisfy 1080p")
	}
	if matchesQuality("Example 720p", "1080p") {
		t.Fatal("720p should not satisfy 1080p")
	}
}

func TestResolverAtomicallyReplacesSuccessfulSlotsAndKeepsFailedSlots(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	media := &model.Media{ID: 7, Type: "movie", Title: "Example", Year: 2025}
	if err := st.UpsertMedia(media); err != nil {
		t.Fatal(err)
	}
	old2160 := &model.File{ID: "old-2160", MediaID: 7, Path: "Movies/Example/Example [2160p].mkv", Quality: "2160p"}
	old1080 := &model.File{ID: "old-1080", MediaID: 7, Path: "Movies/Example/Example [1080p].mkv", Quality: "1080p"}
	if err := st.AddFiles(old2160, old1080); err != nil {
		t.Fatal(err)
	}
	resolver := &Resolver{
		Config:  config.Config{Qualities: []string{"2160p", "1080p"}, Providers: []string{"test"}, MaxResults: 20, ResolveTimeout: time.Second},
		Store:   st,
		Scraper: fixedSearcher{releases: []model.Release{{Title: "Example 2160p", DownloadURL: "magnet:new", Seeders: 10}}},
		Providers: map[string]debrid.Provider{
			"test": fixedProvider{resolved: model.Resolved{ItemID: "new-item", Cached: true, Files: []model.RemoteFile{{ID: "new-file", Name: "Example.2160p.mkv", Size: 100}}}},
		},
	}
	if err := resolver.Resolve(context.Background(), media); err != nil {
		t.Fatal(err)
	}
	if media.Status != "partial" {
		t.Fatalf("expected partial result, got %s", media.Status)
	}
	if _, ok := st.File(old2160.ID); ok {
		t.Fatal("successfully resolved 2160p slot retained its old file")
	}
	if _, ok := st.File(old1080.ID); !ok {
		t.Fatal("failed 1080p slot lost its previously working file")
	}
	files := st.FilesForMedia(media.ID)
	if len(files) != 2 {
		t.Fatalf("expected one replacement and one retained file, got %#v", files)
	}
}
