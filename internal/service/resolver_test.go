package service

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
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

type panicSearcher struct{}

func (panicSearcher) Search(context.Context, scraper.Query, int) ([]model.Release, error) {
	panic("unreleased media must not be scraped")
}

func (s fixedSearcher) Search(context.Context, scraper.Query, int) ([]model.Release, error) {
	return append([]model.Release(nil), s.releases...), nil
}

type fixedProvider struct {
	resolved model.Resolved
}

type episodeSearcher struct {
	queries []scraper.Query
}

func (s *episodeSearcher) Search(_ context.Context, query scraper.Query, _ int) ([]model.Release, error) {
	s.queries = append(s.queries, query)
	return []model.Release{{Title: fmt.Sprintf("Example.S%02dE%02d.1080p", query.Season, query.Episode), DownloadURL: strconv.Itoa(query.Episode), Seeders: 10}}, nil
}

type episodeProvider struct{}

func (episodeProvider) Name() string { return "test" }
func (episodeProvider) Resolve(_ context.Context, release model.Release) (model.Resolved, error) {
	episode, _ := strconv.Atoi(release.DownloadURL)
	return model.Resolved{ItemID: release.DownloadURL, Cached: true, Files: []model.RemoteFile{{ID: release.DownloadURL, Name: fmt.Sprintf("Example.S01E%02d.mkv", episode), Size: 100}}}, nil
}
func (episodeProvider) StreamURL(context.Context, *model.File) (string, error) {
	return "", nil
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

func TestResolverDefersUnreleasedMediaWithoutScraping(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	media := &model.Media{ID: 11, Type: "movie", Title: "Coming Soon", ReleaseDate: time.Now().UTC().AddDate(0, 0, 2).Format("2006-01-02")}
	resolver := &Resolver{Store: st, Scraper: panicSearcher{}}
	if err := resolver.Resolve(context.Background(), media); err != nil {
		t.Fatal(err)
	}
	if media.Status != "unreleased" || len(st.FilesForMedia(media.ID)) != 0 {
		t.Fatalf("unexpected deferred media: %#v", media)
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
	libraryChanges := 0
	resolver := &Resolver{
		Config:  config.Config{Qualities: []string{"2160p", "1080p"}, Providers: []string{"test"}, MaxResults: 20, ResolveTimeout: time.Second},
		Store:   st,
		Scraper: fixedSearcher{releases: []model.Release{{Title: "Example 2160p", DownloadURL: "magnet:new", Seeders: 10}}},
		Providers: map[string]debrid.Provider{
			"test": fixedProvider{resolved: model.Resolved{ItemID: "new-item", Cached: true, Files: []model.RemoteFile{{ID: "new-file", Name: "Example.2160p.mkv", Size: 100}}}},
		},
		LibraryChanged: func() { libraryChanges++ },
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
	if libraryChanges != 1 {
		t.Fatalf("expected one library change notification, got %d", libraryChanges)
	}
}

func TestResolverSearchesEveryKnownEpisode(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	media := &model.Media{ID: 8, Type: "tv", Title: "Example", Year: 2025, Seasons: []int{1}, EpisodeCounts: map[int]int{1: 3}}
	if err := st.UpsertMedia(media); err != nil {
		t.Fatal(err)
	}
	searcher := &episodeSearcher{}
	resolver := &Resolver{
		Config:    config.Config{Qualities: []string{"1080p"}, Providers: []string{"test"}, MaxResults: 20, ResolveTimeout: time.Second},
		Store:     st,
		Scraper:   searcher,
		Providers: map[string]debrid.Provider{"test": episodeProvider{}},
	}
	if err := resolver.Resolve(context.Background(), media); err != nil {
		t.Fatal(err)
	}
	if media.Status != "ready" {
		t.Fatalf("expected ready result, got %s: %s", media.Status, media.Error)
	}
	if len(searcher.queries) != 3 {
		t.Fatalf("expected one search per episode, got %#v", searcher.queries)
	}
	for i, query := range searcher.queries {
		if query.Season != 1 || query.Episode != i+1 {
			t.Fatalf("unexpected episode query %d: %#v", i, query)
		}
	}
	files := st.FilesForMedia(media.ID)
	if len(files) != 3 {
		t.Fatalf("expected all three episode files, got %#v", files)
	}
}

func TestResolverSeasonPackSatisfiesRemainingEpisodeJobs(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	media := &model.Media{ID: 9, Type: "tv", Title: "Example", Seasons: []int{1}, EpisodeCounts: map[int]int{1: 3}}
	if err := st.UpsertMedia(media); err != nil {
		t.Fatal(err)
	}
	searcher := &episodeSearcher{}
	resolver := &Resolver{
		Config:  config.Config{Qualities: []string{"1080p"}, Providers: []string{"test"}, MaxResults: 20, ResolveTimeout: time.Second},
		Store:   st,
		Scraper: searcher,
		Providers: map[string]debrid.Provider{"test": fixedProvider{resolved: model.Resolved{ItemID: "season-pack", Cached: true, Files: []model.RemoteFile{
			{ID: "1", Name: "Example.S01E01.mkv", Size: 100},
			{ID: "2", Name: "Example.S01E02.mkv", Size: 100},
			{ID: "3", Name: "Example.S01E03.mkv", Size: 100},
		}}}},
	}
	if err := resolver.Resolve(context.Background(), media); err != nil {
		t.Fatal(err)
	}
	if media.Status != "ready" || len(st.FilesForMedia(media.ID)) != 3 {
		t.Fatalf("season pack did not resolve the full season: status=%s files=%#v", media.Status, st.FilesForMedia(media.ID))
	}
	if len(searcher.queries) != 1 || searcher.queries[0].Episode != 1 {
		t.Fatalf("expected the season pack to skip later episode searches, got %#v", searcher.queries)
	}
}
