package service

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/LTAGROUP/watchtower/internal/config"
	"github.com/LTAGROUP/watchtower/internal/debrid"
	"github.com/LTAGROUP/watchtower/internal/model"
	"github.com/LTAGROUP/watchtower/internal/scraper"
	"github.com/LTAGROUP/watchtower/internal/store"
)

type gatedSearcher struct {
	mu      sync.Mutex
	active  int
	max     int
	queries []scraper.Query
	started chan struct{}
	release <-chan struct{}
}

func (s *gatedSearcher) Search(ctx context.Context, query scraper.Query, _ int) ([]model.Release, error) {
	s.mu.Lock()
	s.active++
	if s.active > s.max {
		s.max = s.active
	}
	s.queries = append(s.queries, query)
	s.mu.Unlock()
	select {
	case s.started <- struct{}{}:
	case <-ctx.Done():
	}
	select {
	case <-s.release:
	case <-ctx.Done():
		s.mu.Lock()
		s.active--
		s.mu.Unlock()
		return nil, ctx.Err()
	}
	s.mu.Lock()
	s.active--
	s.mu.Unlock()
	return []model.Release{{Title: "Example 1080p", DownloadURL: "release", Seeders: 10}}, nil
}

func TestDurableWorkSerializesSameMediaAndKeepsCrossMediaConcurrency(t *testing.T) {
	state := openLifecycleStore(t)
	release := make(chan struct{})
	searcher := &gatedSearcher{started: make(chan struct{}, 3), release: release}
	resolver := &Resolver{
		Config: config.Config{Qualities: []string{"1080p"}, Providers: []string{"test"}, MaxResults: 5, ResolveTimeout: time.Second, ResolutionConcurrency: 2},
		Store:  state, Scraper: searcher,
		Providers:             map[string]debrid.Provider{"test": fixedProvider{resolved: model.Resolved{ItemID: "item", Cached: true, Files: []model.RemoteFile{{ID: "file", Name: "Example.mkv", Size: 1}}}}},
		ResolutionConcurrency: 2,
	}
	first, err := resolver.Queue(&model.Media{ID: 101, Type: "movie", TMDBID: 501, Title: "First"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := resolver.Queue(&model.Media{ID: 102, Type: "movie", TMDBID: 502, Title: "Second"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var workers sync.WaitGroup
	for _, id := range []int64{first.ID, first.ID, second.ID} {
		workers.Add(1)
		go func(id int64) {
			defer workers.Done()
			if err := resolver.RunDue(ctx, id); err != nil {
				t.Errorf("RunDue(%d): %v", id, err)
			}
		}(id)
	}
	for i := 0; i < 2; i++ {
		select {
		case <-searcher.started:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for cross-media searches")
		}
	}
	select {
	case <-searcher.started:
		t.Fatal("same media work overlapped")
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	workers.Wait()
	searcher.mu.Lock()
	defer searcher.mu.Unlock()
	if len(searcher.queries) != 2 || searcher.max != 2 {
		t.Fatalf("want two cross-media searches and max concurrency two; queries=%d max=%d", len(searcher.queries), searcher.max)
	}
}

func TestScopedWorkSurvivesRestartAndExpiredLeaseRecovers(t *testing.T) {
	path := t.TempDir() + "/state.json"
	state, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	media := &model.Media{ID: 201, Type: "tv", TMDBID: 601, Title: "Example", Seasons: []int{1}, EpisodeCounts: map[int]int{1: 2}, Status: "ready"}
	if err := state.UpsertMedia(media); err != nil {
		t.Fatal(err)
	}
	queue := &Resolver{Store: state}
	queued, err := queue.QueueRerequest(media, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	if queued.Work.Mode != workModeRerequest || queued.Work.Season != 1 || queued.Work.Episode != 2 {
		t.Fatalf("scoped work was not persisted: %#v", queued.Work)
	}
	if _, err := state.UpdateMedia(queued.ID, func(m *model.Media) error {
		m.Status = "resolving"
		m.Work.LeaseUntil = time.Now().Add(-time.Minute)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// A fresh Store/Resolver simulates a process restart after the lease owner
	// crashed. The expired persisted lease must be reclaimable.
	restarted, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	searcher := &episodeSearcher{}
	resolver := &Resolver{
		Config: config.Config{Qualities: []string{"1080p"}, Providers: []string{"test"}, MaxResults: 5, ResolveTimeout: time.Second},
		Store:  restarted, Scraper: searcher, Providers: map[string]debrid.Provider{"test": episodeProvider{}},
	}
	if err := resolver.RunDue(context.Background(), queued.ID); err != nil {
		t.Fatal(err)
	}
	if len(searcher.queries) != 1 || searcher.queries[0].Season != 1 || searcher.queries[0].Episode != 2 {
		t.Fatalf("recovered work lost its scope: %#v", searcher.queries)
	}
	updated, ok := restarted.MediaByID(queued.ID)
	if !ok || updated.Work.Mode != "" {
		t.Fatalf("completed work was not cleared: %#v", updated)
	}
}

func TestRerequestScopeSatisfactionIgnoresUnrelatedPartialMedia(t *testing.T) {
	media := &model.Media{
		Type: "tv", Seasons: []int{1, 2}, EpisodeCounts: map[int]int{1: 2, 2: 2},
		Work: model.MediaWork{Mode: workModeRerequest, Season: 1, Episode: 2},
	}
	files := []*model.File{{Path: "TV/Example/Season 01/Example - S01E02 [1080p].mkv", Quality: "1080p"}}
	if !rerequestScopeSatisfied(media, files, []string{"1080p"}) {
		t.Fatal("single scoped episode should be satisfied despite missing other episodes")
	}
	media.Work.Episode = 0
	files = append(files, &model.File{Path: "TV/Example/Season 01/Example - S01E01 [1080p].mkv", Quality: "1080p"})
	if !rerequestScopeSatisfied(media, files, []string{"1080p"}) {
		t.Fatal("season scoped re-request should be satisfied when every scoped episode exists")
	}
	files[0].Quality = "720p"
	if rerequestScopeSatisfied(media, files, []string{"1080p"}) {
		t.Fatal("a scoped episode in an unconfigured quality must remain pending")
	}
}

func TestDurableWorkDefersCooldownAndFutureEpisodes(t *testing.T) {
	t.Run("provider cooldown", func(t *testing.T) {
		state := openLifecycleStore(t)
		resolver := &Resolver{
			Config: config.Config{Qualities: []string{"1080p"}, Providers: []string{"test"}, ResolveTimeout: time.Second},
			Store:  state, Providers: map[string]debrid.Provider{"test": fixedProvider{}},
		}
		resolver.markProviderCooldown("test", time.Minute)
		queued, err := resolver.Queue(&model.Media{ID: 301, Type: "movie", TMDBID: 701, Title: "Cooling"})
		if err != nil {
			t.Fatal(err)
		}
		if err := resolver.RunDue(context.Background(), queued.ID); err != nil {
			t.Fatal(err)
		}
		updated, _ := state.MediaByID(queued.ID)
		if updated.Status != "queued" || updated.Work.NextAt.Before(time.Now()) || updated.Work.Attempts == 0 {
			t.Fatalf("cooldown was not durably deferred: %#v", updated)
		}
	})
	t.Run("future episode", func(t *testing.T) {
		state := openLifecycleStore(t)
		today := time.Now().UTC()
		futureAirDate := today.AddDate(0, 0, 2).Format("2006-01-02")
		media := &model.Media{
			ID: 302, Type: "tv", TMDBID: 702, Title: "Weekly", Seasons: []int{1}, EpisodeCounts: map[int]int{1: 2},
			EpisodeAirDates: []model.EpisodeAirDate{{Season: 1, Episode: 1, AirDate: today.AddDate(0, 0, -1).Format("2006-01-02")}, {Season: 1, Episode: 2, AirDate: futureAirDate}},
		}
		searcher := &episodeSearcher{}
		resolver := &Resolver{
			Config: config.Config{Qualities: []string{"1080p"}, Providers: []string{"test"}, MaxResults: 5, ResolveTimeout: time.Second},
			Store:  state, Scraper: searcher, Providers: map[string]debrid.Provider{"test": episodeProvider{}},
		}
		if err := resolver.Resolve(context.Background(), media); err != nil {
			t.Fatal(err)
		}
		if len(searcher.queries) != 1 || searcher.queries[0].Episode != 1 {
			t.Fatalf("future episode was scraped: %#v", searcher.queries)
		}
		updated, _ := state.MediaByID(media.ID)
		if updated.Status != "queued" || updated.Work.Mode == "" || !updated.Work.NextAt.Equal(releaseDueAt(futureAirDate)) || updated.Work.Attempts != 0 {
			t.Fatalf("future work was not retained with a due date: %#v", updated)
		}
	})
	t.Run("failed due episode retries before future episode", func(t *testing.T) {
		state := openLifecycleStore(t)
		today := time.Now().UTC()
		futureAirDate := today.AddDate(0, 0, 3).Format("2006-01-02")
		media := &model.Media{
			ID: 303, Type: "tv", TMDBID: 703, Title: "Mixed", Seasons: []int{1}, EpisodeCounts: map[int]int{1: 2},
			EpisodeAirDates: []model.EpisodeAirDate{{Season: 1, Episode: 1, AirDate: today.AddDate(0, 0, -1).Format("2006-01-02")}, {Season: 1, Episode: 2, AirDate: futureAirDate}},
		}
		searcher := &recordingSearcher{err: scraper.ErrRateLimited}
		resolver := &Resolver{
			Config: config.Config{Qualities: []string{"1080p"}, MaxResults: 5, ResolveTimeout: time.Second},
			Store:  state, Scraper: searcher,
		}
		if err := resolver.Resolve(context.Background(), media); err != nil {
			t.Fatal(err)
		}
		if len(searcher.queries) != 1 || searcher.queries[0].Episode != 1 {
			t.Fatalf("expected only the due episode to be attempted: %#v", searcher.queries)
		}
		updated, _ := state.MediaByID(media.ID)
		futureAt := releaseDueAt(futureAirDate)
		if updated.Work.Mode == "" || updated.Work.Attempts != 1 || updated.Work.NextAt.IsZero() || !updated.Work.NextAt.Before(futureAt) {
			t.Fatalf("due retry was deferred until future episode: %#v", updated)
		}
	})
}

func openLifecycleStore(t *testing.T) *store.Store {
	t.Helper()
	state, err := store.Open(t.TempDir() + "/state.json")
	if err != nil {
		t.Fatal(err)
	}
	return state
}
