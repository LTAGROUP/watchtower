package service

import (
	"regexp"
	"sort"
	"strings"

	"github.com/LTAGROUP/watchtower/internal/model"
)

var availabilityEpisodeRE = regexp.MustCompile(`(?i)S(\d{1,2})E(\d{1,3})`)

// QualityAvailability describes how much of a requested quality is present.
// For movies, Available and Expected are either 0/1. For TV, they count
// distinct season/episode slots.
func QualityAvailability(media *model.Media, files []*model.File, qualities []string) []model.QualityAvailability {
	ordered := make([]string, 0, len(qualities))
	seen := map[string]bool{}
	for _, quality := range qualities {
		quality = strings.TrimSpace(quality)
		key := strings.ToLower(quality)
		if quality == "" || seen[key] {
			continue
		}
		seen[key] = true
		ordered = append(ordered, quality)
	}
	for _, file := range files {
		quality := strings.TrimSpace(file.Quality)
		key := strings.ToLower(quality)
		if quality != "" && !seen[key] {
			seen[key] = true
			ordered = append(ordered, quality)
		}
	}
	sort.SliceStable(ordered, func(i, j int) bool {
		if len(qualities) == 0 {
			return strings.ToLower(ordered[i]) < strings.ToLower(ordered[j])
		}
		return qualityOrder(ordered[i], qualities) < qualityOrder(ordered[j], qualities)
	})

	result := make([]model.QualityAvailability, 0, len(ordered))
	for _, quality := range ordered {
		available := availableSlots(media, files, quality)
		expected := expectedSlots(media)
		if media.Type == "movie" {
			expected = 1
		} else if expected == 0 {
			// When catalog episode counts are unknown, the files themselves are
			// the only reliable lower bound for this quality.
			expected = available
		}
		result = append(result, model.QualityAvailability{
			Quality: quality, Available: available, Expected: expected,
			Complete: expected > 0 && available >= expected,
		})
	}
	return result
}

// EffectiveMediaStatus treats a complete quality as usable media. A missing
// alternate quality is not the same as missing episodes from every quality.
func EffectiveMediaStatus(media *model.Media, files []*model.File, qualities []string) string {
	switch media.Status {
	case "queued", "scraping", "resolving", "unreleased":
		return media.Status
	}
	return ResolvedMediaStatus(media, files, qualities)
}

func ResolvedMediaStatus(media *model.Media, files []*model.File, qualities []string) string {
	availability := QualityAvailability(media, files, qualities)
	for _, quality := range availability {
		if quality.Complete {
			return "ready"
		}
	}
	if len(files) > 0 {
		return "partial"
	}
	return "failed"
}

func qualityOrder(quality string, configured []string) int {
	for index, value := range configured {
		if strings.EqualFold(strings.TrimSpace(value), strings.TrimSpace(quality)) {
			return index
		}
	}
	return len(configured) + 1
}

func expectedSlots(media *model.Media) int {
	if media.Type != "tv" {
		return 1
	}
	total := 0
	for _, season := range media.Seasons {
		if count := media.EpisodeCounts[season]; count > 0 {
			total += count
		}
	}
	return total
}

func availableSlots(media *model.Media, files []*model.File, quality string) int {
	if media.Type != "tv" {
		for _, file := range files {
			if strings.EqualFold(strings.TrimSpace(file.Quality), strings.TrimSpace(quality)) {
				return 1
			}
		}
		return 0
	}
	seen := map[string]bool{}
	for _, file := range files {
		if !strings.EqualFold(strings.TrimSpace(file.Quality), strings.TrimSpace(quality)) {
			continue
		}
		match := availabilityEpisodeRE.FindStringSubmatch(file.Path)
		if len(match) == 3 {
			seen[match[1]+":"+match[2]] = true
		}
	}
	return len(seen)
}
