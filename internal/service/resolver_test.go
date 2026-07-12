package service

import (
	"testing"

	"github.com/LTAGROUP/watchtower/internal/model"
)

func TestMaterializeMovieAndSeasonPack(t *testing.T) {
	r := Resolver{}
	movie := &model.Media{ID: 1, Type: "movie", Title: "A/B: Story", Year: 2025}
	got := r.materialize(movie, "1080p", "torbox", model.Release{}, model.Resolved{ItemID: "1", Files: []model.RemoteFile{{ID: "a", Name: "sample.mkv", Size: 1}, {ID: "b", Name: "feature.mkv", Size: 100}}})
	if len(got) != 1 || got[0].Path != "Movies/A-B - Story (2025)/A-B - Story (2025) - 1080p.mkv" {
		t.Fatalf("movie path: %#v", got)
	}
	tv := &model.Media{ID: 2, Type: "tv", Title: "Example", Seasons: []int{1}}
	got = r.materialize(tv, "2160p", "alldebrid", model.Release{}, model.Resolved{ItemID: "2", Files: []model.RemoteFile{{ID: "1", Name: "Example.S01E02.mkv", Size: 10}, {ID: "2", Name: "Example.S02E01.mkv", Size: 10}, {ID: "3", Name: "notes.txt", Size: 1}}})
	if len(got) != 1 || got[0].Path != "TV/Example/Season 01/Example - S01E02 - 2160p.mkv" {
		t.Fatalf("tv paths: %#v", got)
	}
}
