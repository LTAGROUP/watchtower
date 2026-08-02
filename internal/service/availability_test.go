package service

import (
	"testing"

	"github.com/LTAGROUP/watchtower/internal/model"
)

func TestQualityAvailabilityTreatsCompleteAlternateQualityAsReady(t *testing.T) {
	media := &model.Media{Type: "movie", Status: "partial"}
	files := []*model.File{{Quality: "1080p", Path: "Movies/Example/Example [1080p].mkv"}}
	availability := QualityAvailability(media, files, []string{"2160p", "1080p"})
	if len(availability) != 2 || availability[0].Quality != "2160p" || availability[1].Quality != "1080p" {
		t.Fatalf("unexpected availability: %#v", availability)
	}
	if availability[0].Complete || !availability[1].Complete {
		t.Fatalf("unexpected completeness: %#v", availability)
	}
	if got := ResolvedMediaStatus(media, files, []string{"2160p", "1080p"}); got != "ready" {
		t.Fatalf("status=%q, want ready", got)
	}
}

func TestQualityAvailabilityKeepsMissingEpisodesPartial(t *testing.T) {
	media := &model.Media{Type: "tv", Status: "partial", Seasons: []int{1}, EpisodeCounts: map[int]int{1: 3}}
	files := []*model.File{
		{Quality: "1080p", Path: "TV/Example/Season 01/Example - S01E01 [1080p].mkv"},
		{Quality: "1080p", Path: "TV/Example/Season 01/Example - S01E02 [1080p].mkv"},
	}
	availability := QualityAvailability(media, files, []string{"2160p", "1080p"})
	if availability[1].Available != 2 || availability[1].Expected != 3 || availability[1].Complete {
		t.Fatalf("unexpected 1080p availability: %#v", availability[1])
	}
	if got := ResolvedMediaStatus(media, files, []string{"2160p", "1080p"}); got != "partial" {
		t.Fatalf("status=%q, want partial", got)
	}
}
