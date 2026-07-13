package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
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
	Config          config.Config
	Settings        func() config.Config
	Store           *store.Store
	Scraper         scraper.Searcher
	ScraperFactory  func(config.Config) (scraper.Searcher, error)
	Providers       map[string]debrid.Provider
	ProviderFactory func(config.Config) map[string]debrid.Provider
	Log             *slog.Logger
	repairMu        sync.Mutex
	repairs         map[string]*repairCall
}

type repairCall struct {
	done chan struct{}
	file *model.File
	err  error
}

func (r *Resolver) Resolve(ctx context.Context, m *model.Media) error {
	cfg := r.Config
	if r.Settings != nil {
		cfg = r.Settings()
	}
	searcher := r.Scraper
	if r.ScraperFactory != nil {
		var err error
		searcher, err = r.ScraperFactory(cfg)
		if err != nil {
			return err
		}
	}
	providers := r.Providers
	if r.ProviderFactory != nil {
		providers = r.ProviderFactory(cfg)
	}
	started := time.Now()
	if r.Log != nil {
		r.Log.Info("media resolution started", "component", "resolver", "title", m.Title, "type", m.Type, "imdb_id", m.ExternalID, "tmdb_id", m.TMDBID, "qualities", cfg.Qualities)
	}
	m.Status = "resolving"
	m.UpdatedAt = time.Now().UTC()
	_ = r.Store.UpsertMedia(m)
	var errs []string
	total := 0
	var resolvedFiles []*model.File
	resolvedSlots := map[string]bool{}
	type job struct {
		quality string
		season  int
	}
	var jobs []job
	for _, q := range cfg.Qualities {
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
		m.Status = "scraping"
		m.UpdatedAt = time.Now().UTC()
		_ = r.Store.UpsertMedia(m)
		rels, err := searcher.Search(ctx, scraper.Query{MediaType: m.Type, ExternalID: m.ExternalID, TMDBID: m.TMDBID, Season: work.season}, cfg.MaxResults)
		if err != nil {
			if r.Log != nil {
				r.Log.Error("media scrape failed", "component", "resolver", "title", m.Title, "target", label, "error", err)
			}
			errs = append(errs, label+": "+err.Error())
			continue
		}
		m.ScrapedAt = time.Now().UTC()
		m.Status = "resolving"
		m.UpdatedAt = m.ScrapedAt
		_ = r.Store.UpsertMedia(m)
		if r.Log != nil {
			r.Log.Info("media scrape completed", "component", "resolver", "title", m.Title, "target", label, "streams", len(rels))
		}
		sort.SliceStable(rels, func(i, j int) bool { return releaseScore(rels[i], q) > releaseScore(rels[j], q) })
		found := false
		for _, rel := range rels {
			if (rel.Seeders >= 0 && rel.Seeders < cfg.MinSeeders) || !matchesQuality(rel.Title, q) {
				continue
			}
			for _, name := range cfg.Providers {
				p := providers[name]
				if p == nil {
					continue
				}
				attemptStarted := time.Now()
				if r.Log != nil {
					r.Log.Info("provider resolution started", "component", "resolver", "title", m.Title, "target", label, "provider", p.Name(), "source", rel.Source, "release", rel.Title, "seeders", rel.Seeders, "size_bytes", rel.Size)
				}
				attempt, cancel := context.WithTimeout(ctx, cfg.ResolveTimeout)
				resolved, e := p.Resolve(attempt, rel)
				cancel()
				if e != nil {
					if r.Log != nil {
						r.Log.Warn("provider resolution failed", "component", "resolver", "title", m.Title, "target", label, "provider", p.Name(), "source", rel.Source, "duration", time.Since(attemptStarted).String(), "error", e)
					}
					continue
				}
				target := *m
				if work.season > 0 {
					target.Seasons = []int{work.season}
				}
				files := r.materialize(&target, q, p.Name(), rel, resolved)
				if len(files) == 0 {
					if r.Log != nil {
						r.Log.Warn("provider release contained no matching video files", "component", "resolver", "title", m.Title, "target", label, "provider", p.Name(), "remote_files", len(resolved.Files))
					}
					continue
				}
				resolvedFiles = append(resolvedFiles, files...)
				resolvedSlots[resolutionSlot(m.Type, q, work.season)] = true
				total += len(files)
				if r.Log != nil {
					r.Log.Info("provider resolution completed", "component", "resolver", "title", m.Title, "target", label, "provider", p.Name(), "source", rel.Source, "files_added", len(files), "cached", resolved.Cached, "duration", time.Since(attemptStarted).String())
				}
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
	if total > 0 {
		finalFiles := append([]*model.File(nil), resolvedFiles...)
		for _, previous := range r.Store.FilesForMedia(m.ID) {
			if !resolvedSlots[fileResolutionSlot(m.Type, previous)] {
				finalFiles = append(finalFiles, previous)
			}
		}
		if err := r.Store.ReplaceFilesForMedia(m.ID, finalFiles...); err != nil {
			return err
		}
	}
	m.UpdatedAt = time.Now().UTC()
	err := r.Store.UpsertMedia(m)
	if r.Log != nil {
		attrs := []any{"component", "resolver", "title", m.Title, "status", m.Status, "files_added", total, "duration", time.Since(started).String()}
		if m.Error != "" {
			attrs = append(attrs, "details", m.Error)
		}
		if err != nil {
			attrs = append(attrs, "error", err)
			r.Log.Error("media resolution persistence failed", attrs...)
		} else {
			r.Log.Info("media resolution completed", attrs...)
		}
	}
	return err
}

func resolutionSlot(kind, quality string, season int) string {
	quality = strings.ToLower(strings.TrimSpace(quality))
	if kind == "tv" {
		return fmt.Sprintf("%s|%d", quality, season)
	}
	return quality
}

func fileResolutionSlot(kind string, file *model.File) string {
	season := 0
	if kind == "tv" {
		if match := episodeRE.FindStringSubmatch(file.Path); len(match) == 3 {
			season, _ = strconv.Atoi(match[1])
		}
	}
	return resolutionSlot(kind, file.Quality, season)
}

func (r *Resolver) Repair(ctx context.Context, stale *model.File) (*model.File, error) {
	r.repairMu.Lock()
	if r.repairs == nil {
		r.repairs = map[string]*repairCall{}
	}
	if call := r.repairs[stale.ID]; call != nil {
		r.repairMu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-call.done:
			return call.file, call.err
		}
	}
	call := &repairCall{done: make(chan struct{})}
	r.repairs[stale.ID] = call
	r.repairMu.Unlock()

	call.file, call.err = r.repair(ctx, stale)
	close(call.done)
	r.repairMu.Lock()
	delete(r.repairs, stale.ID)
	r.repairMu.Unlock()
	return call.file, call.err
}

func (r *Resolver) repair(ctx context.Context, stale *model.File) (*model.File, error) {
	media, ok := r.Store.MediaByID(stale.MediaID)
	if !ok {
		return nil, fmt.Errorf("repair media not found")
	}
	cfg := r.Config
	if r.Settings != nil {
		cfg = r.Settings()
	}
	searcher := r.Scraper
	if r.ScraperFactory != nil {
		var err error
		searcher, err = r.ScraperFactory(cfg)
		if err != nil {
			return nil, err
		}
	}
	providers := r.Providers
	if r.ProviderFactory != nil {
		providers = r.ProviderFactory(cfg)
	}
	season := 0
	if match := episodeRE.FindStringSubmatch(stale.Path); len(match) == 3 {
		season, _ = strconv.Atoi(match[1])
	}
	if r.Log != nil {
		r.Log.Warn("stale stream repair started", "component", "resolver", "title", media.Title, "file", stale.Path, "quality", stale.Quality, "provider", stale.Provider)
	}
	releases, err := searcher.Search(ctx, scraper.Query{MediaType: media.Type, ExternalID: media.ExternalID, TMDBID: media.TMDBID, Season: season}, cfg.MaxResults)
	if err != nil {
		return nil, err
	}
	sort.SliceStable(releases, func(i, j int) bool {
		return releaseScore(releases[i], stale.Quality) > releaseScore(releases[j], stale.Quality)
	})
	providerOrder := append([]string(nil), cfg.Providers...)
	if stale.Provider != "" {
		providerOrder = moveFirst(providerOrder, stale.Provider)
	}
	for _, release := range releases {
		if (release.Seeders >= 0 && release.Seeders < cfg.MinSeeders) || !matchesQuality(release.Title, stale.Quality) {
			continue
		}
		for _, name := range providerOrder {
			provider := providers[name]
			if provider == nil {
				continue
			}
			attempt, cancel := context.WithTimeout(ctx, cfg.ResolveTimeout)
			resolved, resolveErr := provider.Resolve(attempt, release)
			cancel()
			if resolveErr != nil {
				continue
			}
			target := *media
			if season > 0 {
				target.Seasons = []int{season}
			}
			for _, replacement := range r.materialize(&target, stale.Quality, provider.Name(), release, resolved) {
				if !sameMediaFile(media.Type, stale.Path, replacement.Path) {
					continue
				}
				updated, replaceErr := r.Store.ReplaceFileSource(stale.ID, replacement)
				if replaceErr != nil {
					return nil, replaceErr
				}
				if r.Log != nil {
					r.Log.Info("stale stream repair completed", "component", "resolver", "title", media.Title, "file", stale.Path, "provider", provider.Name())
				}
				return updated, nil
			}
		}
	}
	return nil, fmt.Errorf("no replacement cached release found for %s", stale.Path)
}

func moveFirst(values []string, wanted string) []string {
	out := []string{wanted}
	for _, value := range values {
		if value != wanted {
			out = append(out, value)
		}
	}
	return out
}

func sameMediaFile(kind, current, replacement string) bool {
	if !strings.EqualFold(filepath.Ext(current), filepath.Ext(replacement)) {
		return false
	}
	if kind == "movie" {
		return true
	}
	a := episodeRE.FindStringSubmatch(current)
	b := episodeRE.FindStringSubmatch(replacement)
	return len(a) == 3 && len(b) == 3 && a[1] == b[1] && a[2] == b[2]
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
			base := plexFolderName(m)
			path = fmt.Sprintf("Movies/%s/%s [%s]%s", base, base, safe(q), ext)
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
			show := plexTitle(m)
			path = fmt.Sprintf("TV/%s/Season %02d/%s - S%02dE%02d [%s]%s", plexFolderName(m), season, show, season, ep, safe(q), ext)
		}
		sum := sha256.Sum256([]byte(provider + "|" + res.ItemID + "|" + rf.ID + "|" + path))
		out = append(out, &model.File{ID: hex.EncodeToString(sum[:12]), MediaID: m.ID, Path: path, Quality: q, Provider: provider, SourceURI: rel.DownloadURL, InfoHash: rel.InfoHash, ProviderItemID: res.ItemID, ProviderFileID: rf.ID, Size: rf.Size, CreatedAt: time.Now().UTC()})
	}
	return out
}

func plexTitle(m *model.Media) string {
	title := safe(m.Title)
	if m.Year > 0 {
		return fmt.Sprintf("%s (%d)", title, m.Year)
	}
	return title
}

func plexFolderName(m *model.Media) string {
	base := plexTitle(m)
	if imdbID := strings.TrimSpace(m.ExternalID); strings.HasPrefix(strings.ToLower(imdbID), "tt") {
		return fmt.Sprintf("%s {imdb-%s}", base, imdbID)
	}
	if m.TMDBID > 0 {
		return fmt.Sprintf("%s {tmdb-%d}", base, m.TMDBID)
	}
	return base
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
