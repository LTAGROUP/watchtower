package service

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/LTAGROUP/watchtower/internal/config"
	"github.com/LTAGROUP/watchtower/internal/debrid"
	"github.com/LTAGROUP/watchtower/internal/model"
	"github.com/LTAGROUP/watchtower/internal/scraper"
	"github.com/LTAGROUP/watchtower/internal/store"
)

type recordingSearcher struct {
	queries  []scraper.Query
	limits   []int
	releases []model.Release
	err      error
}

type panicSearcher struct{}

func (panicSearcher) Search(context.Context, scraper.Query, int) ([]model.Release, error) {
	panic("unreleased media must not be scraped")
}

func (s *recordingSearcher) Search(_ context.Context, query scraper.Query, limit int) ([]model.Release, error) {
	s.queries = append(s.queries, query)
	s.limits = append(s.limits, limit)
	return append([]model.Release(nil), s.releases...), s.err
}

type fixedProvider struct {
	resolved model.Resolved
}

type recordingProvider struct {
	titles  []string
	results map[string]model.Resolved
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

func (p *recordingProvider) Name() string { return "test" }
func (p *recordingProvider) Resolve(_ context.Context, release model.Release) (model.Resolved, error) {
	p.titles = append(p.titles, release.Title)
	return p.results[release.DownloadURL], nil
}
func (p *recordingProvider) StreamURL(context.Context, *model.File) (string, error) {
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

func TestSeasonPackDetection(t *testing.T) {
	for _, title := range []string{
		"Example.S01.1080p.BluRay",
		"Example Season 1 Complete 1080p",
		"Example.S01E01-E10.1080p",
	} {
		if !isSeasonPack(title, 1) {
			t.Errorf("expected season pack: %q", title)
		}
	}
	for _, title := range []string{
		"Example.S01E02.1080p",
		"Example.S02.1080p",
		"Example.1080p",
	} {
		if isSeasonPack(title, 1) {
			t.Errorf("unexpected season pack: %q", title)
		}
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

func TestResolverSkipsExistingSlotsAndFillsOnlyMissingOnes(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	media := &model.Media{ID: 7, Type: "movie", Title: "Example", Year: 2025}
	if err := st.UpsertMedia(media); err != nil {
		t.Fatal(err)
	}
	old2160 := &model.File{ID: "old-2160", MediaID: 7, Path: "Movies/Example/Example [2160p].mkv", Quality: "2160p"}
	if err := st.AddFiles(old2160); err != nil {
		t.Fatal(err)
	}
	libraryChanges := 0
	searcher := &recordingSearcher{releases: []model.Release{{Title: "Example 1080p", DownloadURL: "magnet:new", Seeders: 10}}}
	resolver := &Resolver{
		Config:  config.Config{Qualities: []string{"2160p", "1080p"}, Providers: []string{"test"}, MaxResults: 20, ResolveTimeout: time.Second},
		Store:   st,
		Scraper: searcher,
		Providers: map[string]debrid.Provider{
			"test": fixedProvider{resolved: model.Resolved{ItemID: "new-item", Cached: true, Files: []model.RemoteFile{{ID: "new-file", Name: "Example.1080p.mkv", Size: 100}}}},
		},
		LibraryChanged: func() { libraryChanges++ },
	}
	if err := resolver.Resolve(context.Background(), media); err != nil {
		t.Fatal(err)
	}
	if media.Status != "ready" {
		t.Fatalf("expected ready result, got %s: %s", media.Status, media.Error)
	}
	if len(searcher.queries) != 1 {
		t.Fatalf("expected only the missing 1080p slot to be searched, got %#v", searcher.queries)
	}
	if _, ok := st.File(old2160.ID); !ok {
		t.Fatal("existing 2160p file was replaced during incremental retry")
	}
	files := st.FilesForMedia(media.ID)
	if len(files) != 2 {
		t.Fatalf("expected one existing and one newly resolved file, got %#v", files)
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

func TestResolverRetrySearchesOnlyMissingEpisodes(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	media := &model.Media{ID: 10, Type: "tv", Title: "Example", Seasons: []int{1}, EpisodeCounts: map[int]int{1: 3}}
	if err := st.UpsertMedia(media); err != nil {
		t.Fatal(err)
	}
	if err := st.AddFiles(
		&model.File{ID: "e1", MediaID: media.ID, Path: "TV/Example/Season 01/Example - S01E01 [1080p].mkv", Quality: "1080p"},
		&model.File{ID: "e2", MediaID: media.ID, Path: "TV/Example/Season 01/Example - S01E02 [1080p].mkv", Quality: "1080p"},
	); err != nil {
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
	if media.Status != "ready" || len(st.FilesForMedia(media.ID)) != 3 {
		t.Fatalf("incremental retry did not complete the season: status=%s files=%#v", media.Status, st.FilesForMedia(media.ID))
	}
	if len(searcher.queries) != 1 || searcher.queries[0].Episode != 3 {
		t.Fatalf("expected only episode 3 to be searched, got %#v", searcher.queries)
	}
}

func TestResolverStopsRemainingJobsAfterRateLimit(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	media := &model.Media{ID: 12, Type: "tv", Title: "Example", Seasons: []int{1}, EpisodeCounts: map[int]int{1: 3}}
	if err := st.UpsertMedia(media); err != nil {
		t.Fatal(err)
	}
	if err := st.AddFiles(&model.File{ID: "e1", MediaID: media.ID, Path: "TV/Example/Season 01/Example - S01E01 [1080p].mkv", Quality: "1080p"}); err != nil {
		t.Fatal(err)
	}
	searcher := &recordingSearcher{err: fmt.Errorf("%w: torrentio", scraper.ErrRateLimited)}
	resolver := &Resolver{
		Config:  config.Config{Qualities: []string{"1080p"}, MaxResults: 20, ResolveTimeout: time.Second},
		Store:   st,
		Scraper: searcher,
	}
	if err := resolver.Resolve(context.Background(), media); err != nil {
		t.Fatal(err)
	}
	if media.Status != "partial" || !strings.Contains(media.Error, "S01E02") || strings.Contains(media.Error, "S01E03") {
		t.Fatalf("unexpected rate-limited result: status=%s error=%q", media.Status, media.Error)
	}
	if len(searcher.queries) != 1 || searcher.queries[0].Episode != 2 {
		t.Fatalf("expected rate limiting to stop after the first missing episode, got %#v", searcher.queries)
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

func TestResolverExpandsSeasonPackWhenEpisodeCountIsUnknown(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	media := &model.Media{ID: 17, Type: "tv", Title: "Example", Seasons: []int{1}}
	if err := st.UpsertMedia(media); err != nil {
		t.Fatal(err)
	}
	searcher := &recordingSearcher{releases: []model.Release{{Title: "Example.S01.1080p", Seeders: 10}}}
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
	if len(st.FilesForMedia(media.ID)) != 3 {
		t.Fatalf("season pack was not fully expanded: %#v", st.FilesForMedia(media.ID))
	}
}

func TestResolverPrefersSeasonPackOverHigherSeededEpisode(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	media := &model.Media{ID: 13, Type: "tv", Title: "Example", Seasons: []int{1}, EpisodeCounts: map[int]int{1: 3}}
	if err := st.UpsertMedia(media); err != nil {
		t.Fatal(err)
	}
	searcher := &recordingSearcher{releases: []model.Release{
		{Title: "Example.S01E01.1080p", DownloadURL: "episode", Seeders: 500},
		{Title: "Example.S01.1080p", DownloadURL: "pack", Seeders: 2},
	}}
	provider := &recordingProvider{results: map[string]model.Resolved{
		"episode": {ItemID: "episode", Cached: true, Files: []model.RemoteFile{{ID: "1", Name: "Example.S01E01.mkv", Size: 100}}},
		"pack": {ItemID: "pack", Cached: true, Files: []model.RemoteFile{
			{ID: "1", Name: "Example.S01E01.mkv", Size: 100},
			{ID: "2", Name: "Example.S01E02.mkv", Size: 100},
			{ID: "3", Name: "Example.S01E03.mkv", Size: 100},
		}},
	}}
	resolver := &Resolver{
		Config: config.Config{Qualities: []string{"1080p"}, Providers: []string{"test"}, MaxResults: 1, ResolveTimeout: time.Second},
		Store:  st, Scraper: searcher, Providers: map[string]debrid.Provider{"test": provider},
	}
	if err := resolver.Resolve(context.Background(), media); err != nil {
		t.Fatal(err)
	}
	if len(provider.titles) != 1 || provider.titles[0] != "Example.S01.1080p" {
		t.Fatalf("season pack was not tried first: %#v", provider.titles)
	}
	if len(searcher.queries) != 1 || searcher.limits[0] != 0 || len(st.FilesForMedia(media.ID)) != 3 {
		t.Fatalf("preferred pack did not complete the season: queries=%#v files=%#v", searcher.queries, st.FilesForMedia(media.ID))
	}
}

func TestRerequestSeasonReplacesWholeSeasonFromPack(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	media := &model.Media{ID: 16, Type: "tv", Title: "Example", Seasons: []int{1}, EpisodeCounts: map[int]int{1: 2}, Status: "ready"}
	if err := st.UpsertMedia(media); err != nil {
		t.Fatal(err)
	}
	if err := st.AddFiles(
		&model.File{ID: "old-1", MediaID: media.ID, Path: "TV/Example/Season 01/Example - S01E01 [1080p].mkv", Quality: "1080p", Provider: "old"},
		&model.File{ID: "old-2", MediaID: media.ID, Path: "TV/Example/Season 01/Example - S01E02 [1080p].mkv", Quality: "1080p", Provider: "old"},
	); err != nil {
		t.Fatal(err)
	}
	searcher := &recordingSearcher{releases: []model.Release{{Title: "Example.S01.1080p", DownloadURL: "pack", Seeders: 2}}}
	provider := &recordingProvider{results: map[string]model.Resolved{
		"pack": {ItemID: "pack", Cached: true, Files: []model.RemoteFile{
			{ID: "1", Name: "Example.S01E01.mkv", Size: 100},
			{ID: "2", Name: "Example.S01E02.mkv", Size: 100},
		}},
	}}
	resolver := &Resolver{
		Config: config.Config{Qualities: []string{"1080p"}, Providers: []string{"test"}, MaxResults: 20, ResolveTimeout: time.Second},
		Store:  st, Scraper: searcher, Providers: map[string]debrid.Provider{"test": provider},
	}
	if err := resolver.Rerequest(context.Background(), media, 1, 0); err != nil {
		t.Fatal(err)
	}
	if len(searcher.queries) != 1 || len(st.FilesForMedia(media.ID)) != 2 {
		t.Fatalf("season pack did not replace the season: queries=%#v files=%#v", searcher.queries, st.FilesForMedia(media.ID))
	}
	if _, ok := st.File("old-1"); ok {
		t.Fatal("episode 1 kept its old source")
	}
	if _, ok := st.File("old-2"); ok {
		t.Fatal("episode 2 kept its old source")
	}
}

func TestRerequestSingleEpisodeReplacesOnlyThatEpisode(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	media := &model.Media{ID: 14, Type: "tv", Title: "Example", Seasons: []int{1}, EpisodeCounts: map[int]int{1: 3}, Status: "ready"}
	if err := st.UpsertMedia(media); err != nil {
		t.Fatal(err)
	}
	if err := st.AddFiles(
		&model.File{ID: "old-1", MediaID: media.ID, Path: "TV/Example/Season 01/Example - S01E01 [1080p].mkv", Quality: "1080p", Provider: "old"},
		&model.File{ID: "old-2", MediaID: media.ID, Path: "TV/Example/Season 01/Example - S01E02 [1080p].mkv", Quality: "1080p", Provider: "old"},
		&model.File{ID: "old-3", MediaID: media.ID, Path: "TV/Example/Season 01/Example - S01E03 [1080p].mkv", Quality: "1080p", Provider: "old"},
	); err != nil {
		t.Fatal(err)
	}
	searcher := &episodeSearcher{}
	resolver := &Resolver{
		Config: config.Config{Qualities: []string{"1080p"}, Providers: []string{"test"}, MaxResults: 20, ResolveTimeout: time.Second},
		Store:  st, Scraper: searcher, Providers: map[string]debrid.Provider{"test": episodeProvider{}},
	}
	if err := resolver.Rerequest(context.Background(), media, 1, 2); err != nil {
		t.Fatal(err)
	}
	if len(searcher.queries) != 1 || searcher.queries[0].Episode != 2 {
		t.Fatalf("expected only episode 2 to be searched, got %#v", searcher.queries)
	}
	files := st.FilesForMedia(media.ID)
	if len(files) != 3 {
		t.Fatalf("expected three files after re-request, got %#v", files)
	}
	if _, ok := st.File("old-1"); !ok {
		t.Fatal("episode 1 was replaced")
	}
	if _, ok := st.File("old-3"); !ok {
		t.Fatal("episode 3 was replaced")
	}
	if _, ok := st.File("old-2"); ok {
		t.Fatal("episode 2 kept its old source")
	}
}

func TestRerequestKeepsExistingEpisodeWhenReplacementFails(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	media := &model.Media{ID: 15, Type: "tv", Title: "Example", Seasons: []int{1}, EpisodeCounts: map[int]int{1: 1}, Status: "ready"}
	if err := st.UpsertMedia(media); err != nil {
		t.Fatal(err)
	}
	existing := &model.File{ID: "old", MediaID: media.ID, Path: "TV/Example/Season 01/Example - S01E01 [1080p].mkv", Quality: "1080p", Provider: "old"}
	if err := st.AddFiles(existing); err != nil {
		t.Fatal(err)
	}
	resolver := &Resolver{
		Config: config.Config{Qualities: []string{"1080p"}, MaxResults: 20, ResolveTimeout: time.Second},
		Store:  st, Scraper: &recordingSearcher{},
	}
	if err := resolver.Rerequest(context.Background(), media, 1, 1); err != nil {
		t.Fatal(err)
	}
	if media.Status != "failed" {
		t.Fatalf("expected failed re-request status, got %s", media.Status)
	}
	if _, ok := st.File(existing.ID); !ok {
		t.Fatal("existing episode was removed after failed re-request")
	}
}
