package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/LTAGROUP/watchtower/internal/config"
	"github.com/LTAGROUP/watchtower/internal/debrid"
	"github.com/LTAGROUP/watchtower/internal/model"
	"github.com/LTAGROUP/watchtower/internal/scraper"
	"github.com/LTAGROUP/watchtower/internal/store"
)

var episodeRE = regexp.MustCompile(`(?i)S(\d{1,2})E(\d{1,3})`)
var videoExt = map[string]bool{".mkv": true, ".mp4": true, ".avi": true, ".m4v": true, ".ts": true, ".mov": true}

type Resolver struct {
	Config    config.Config
	Store     *store.Store
	Scraper   scraper.Searcher
	Providers map[string]debrid.Provider
}

func (r *Resolver) Resolve(ctx context.Context, m *model.Media) error {
	m.Status = "resolving"
	m.UpdatedAt = time.Now().UTC()
	_ = r.Store.UpsertMedia(m)
	var errs []string
	total := 0
	type job struct {
		quality string
		season  int
	}
	var jobs []job
	for _, q := range r.Config.Qualities {
		if m.Type == "tv" && len(m.Seasons) > 0 {
			for _, season := range m.Seasons {
				jobs = append(jobs, job{quality: q, season: season})
			}
		} else {
			jobs = append(jobs, job{quality: q})
		}
	}
	for _, work := range jobs {
		q := work.quality
		label := q
		if m.Type == "tv" && work.season > 0 {
			label = fmt.Sprintf("S%02d %s", work.season, q)
		}
		rels, err := r.Scraper.Search(ctx, scraper.Query{MediaType: m.Type, ExternalID: m.ExternalID, TMDBID: m.TMDBID, Season: work.season}, r.Config.MaxResults)
		if err != nil {
			errs = append(errs, label+": "+err.Error())
			continue
		}
		sort.SliceStable(rels, func(i, j int) bool { return releaseScore(rels[i], q) > releaseScore(rels[j], q) })
		found := false
		for _, rel := range rels {
			if (rel.Seeders >= 0 && rel.Seeders < r.Config.MinSeeders) || !matchesQuality(rel.Title, q) {
				continue
			}
			for _, name := range r.Config.Providers {
				p := r.Providers[name]
				if p == nil {
					continue
				}
				attempt, cancel := context.WithTimeout(ctx, r.Config.ResolveTimeout)
				resolved, e := p.Resolve(attempt, rel)
				cancel()
				if e != nil {
					continue
				}
				target := *m
				if work.season > 0 {
					target.Seasons = []int{work.season}
				}
				files := r.materialize(&target, q, p.Name(), rel, resolved)
				if len(files) == 0 {
					continue
				}
				if e = r.Store.AddFiles(files...); e != nil {
					return e
				}
				total += len(files)
				found = true
				break
			}
			if found {
				break
			}
		}
		if !found {
			errs = append(errs, label+": no acceptable cached release")
		}
	}
	if total == 0 {
		m.Status = "failed"
		m.Error = strings.Join(errs, "; ")
	} else if len(errs) > 0 {
		m.Status = "partial"
		m.Error = strings.Join(errs, "; ")
	} else {
		m.Status = "ready"
		m.Error = ""
	}
	m.UpdatedAt = time.Now().UTC()
	return r.Store.UpsertMedia(m)
}
func releaseScore(x model.Release, q string) int64 {
	s := int64(x.Seeders) * 1000
	if matchesQuality(x.Title, q) {
		s += 1_000_000
	}
	n := strings.ToLower(x.Title)
	if strings.Contains(n, "remux") {
		s += 5000
	}
	if strings.Contains(n, "web-dl") {
		s += 3000
	}
	return s + x.Size/(1<<30)
}
func matchesQuality(title, quality string) bool {
	title = strings.ToLower(title)
	switch strings.ToLower(quality) {
	case "2160p", "4k", "uhd":
		return strings.Contains(title, "2160p") || strings.Contains(title, "4k") || strings.Contains(title, "uhd")
	case "1080p", "fhd":
		return strings.Contains(title, "1080p") || strings.Contains(title, "fhd")
	default:
		return strings.Contains(title, strings.ToLower(quality))
	}
}
func (r *Resolver) materialize(m *model.Media, q, provider string, rel model.Release, res model.Resolved) []*model.File {
	var out []*model.File
	files := append([]model.RemoteFile(nil), res.Files...)
	sort.Slice(files, func(i, j int) bool { return files[i].Size > files[j].Size })
	for _, rf := range files {
		ext := strings.ToLower(filepath.Ext(rf.Name))
		if !videoExt[ext] {
			continue
		}
		var path string
		if m.Type == "movie" {
			if len(out) > 0 {
				break
			}
			base := safe(fmt.Sprintf("%s (%d)", m.Title, m.Year))
			path = fmt.Sprintf("Movies/%s/%s - %s%s", base, base, q, ext)
		} else {
			match := episodeRE.FindStringSubmatch(rf.Name)
			if len(match) != 3 {
				continue
			}
			season, _ := strconv.Atoi(match[1])
			ep, _ := strconv.Atoi(match[2])
			if len(m.Seasons) > 0 && !containsInt(m.Seasons, season) {
				continue
			}
			show := safe(m.Title)
			path = fmt.Sprintf("TV/%s/Season %02d/%s - S%02dE%02d - %s%s", show, season, show, season, ep, q, ext)
		}
		sum := sha256.Sum256([]byte(provider + "|" + res.ItemID + "|" + rf.ID + "|" + path))
		out = append(out, &model.File{ID: hex.EncodeToString(sum[:12]), MediaID: m.ID, Path: path, Quality: q, Provider: provider, SourceURI: rel.DownloadURL, InfoHash: rel.InfoHash, ProviderItemID: res.ItemID, ProviderFileID: rf.ID, Size: rf.Size, CreatedAt: time.Now().UTC()})
	}
	return out
}
func safe(s string) string {
	r := strings.NewReplacer("/", "-", "\\", "-", ":", " -", "*", "", "?", "", `"`, "", "<", "", ">", "", "|", "-")
	return strings.TrimSpace(r.Replace(s))
}
func containsInt(a []int, v int) bool {
	for _, x := range a {
		if x == v {
			return true
		}
	}
	return false
}
