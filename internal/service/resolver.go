package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
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
var seasonOnlyRE = regexp.MustCompile(`(?i)(?:^|[^a-z0-9])S(\d{1,2})(?:[^a-z0-9]|$)`)
var seasonWordRE = regexp.MustCompile(`(?i)(?:^|[^a-z0-9])Season[ ._-]*0*(\d{1,2})(?:[^a-z0-9]|$)`)
var episodeRangeRE = regexp.MustCompile(`(?i)S(\d{1,2})E\d{1,3}[ ._-]*(?:-|to)[ ._-]*(?:S\d{1,2})?E?\d{1,3}`)
var videoExt = map[string]bool{".mkv": true, ".mp4": true, ".avi": true, ".m4v": true, ".ts": true, ".mov": true}

type Resolver struct {
	Config          config.Config
	Settings        func() config.Config
	Store           *store.Store
	Scraper         scraper.Searcher
	ScraperFactory  func(config.Config) (scraper.Searcher, error)
	Providers       map[string]debrid.Provider
	ProviderFactory func(config.Config) map[string]debrid.Provider
	LibraryChanged  func()
	// WorkCompleted wakes observers (notably Seerr intent reconciliation) after
	// a durable final commit. It must be non-blocking.
	WorkCompleted          func()
	Log                    *slog.Logger
	ResolutionConcurrency  int
	repairMu               sync.Mutex
	repairs                map[string]*repairCall
	resolutionMu           sync.Mutex
	resolutionSlots        chan struct{}
	providerCooldownMu     sync.Mutex
	providerCooldowns      map[string]time.Time
	beforeFinishResolution func()
	// mediaGates serialize work by stable media identity before a global
	// resolution slot is consumed. They intentionally survive for the process
	// lifetime: identities are bounded by the library and retaining a gate
	// avoids a check/delete race between concurrent callers.
	mediaGates sync.Map
}

type repairCall struct {
	done chan struct{}
	file *model.File
	err  error
}

const (
	workModeResolve   = "resolve"
	workModeReset     = "reset"
	workModeRerequest = "rerequest"
	workModeRetryFile = "retry-file"
)

var (
	errWorkNotDue = errors.New("media work is not due")
	// ErrInvalidRerequestScope lets callers map a rejected scoped retry to a
	// client error without matching an implementation-detail message.
	ErrInvalidRerequestScope = errors.New("invalid re-request scope")
	// ErrInvalidFileRetry lets callers distinguish an unknown or mismatched
	// file from an internal queueing failure.
	ErrInvalidFileRetry = errors.New("invalid file retry")
	// ErrInvalidManualTarget identifies a malformed manual scrape target.
	ErrInvalidManualTarget = errors.New("invalid manual scrape target")
	// ErrManualCandidateNotFound means the selected result was not present in
	// the server's fresh scrape and therefore was not trusted for resolution.
	ErrManualCandidateNotFound = errors.New("manual scrape candidate not found")
)

// ManualTarget identifies one resolution slot for an operator-directed scrape.
type ManualTarget struct {
	Quality string `json:"quality"`
	Season  int    `json:"season,omitempty"`
	Episode int    `json:"episode,omitempty"`
}

// ManualCandidate is the safe dashboard representation of a scraper release.
// Download URLs, torrent bytes, and info hashes deliberately remain server-side.
type ManualCandidate struct {
	ID      string `json:"id"`
	Title   string `json:"title"`
	Source  string `json:"source"`
	Size    int64  `json:"size"`
	Seeders int    `json:"seeders"`
}

func (r *Resolver) Resolve(ctx context.Context, m *model.Media) error {
	queued, err := r.Queue(m)
	if err != nil {
		return err
	}
	err = r.RunDue(ctx, queued.ID)
	r.copyStoredMedia(m, queued.ID)
	return err
}

// Rerequest resolves a TV season or a single episode even when matching files
// already exist. Existing files are retained unless a replacement is found.
func (r *Resolver) Rerequest(ctx context.Context, m *model.Media, season, episode int) error {
	if err := validateRerequestScope(m, season, episode); err != nil {
		return err
	}
	queued, err := r.QueueRerequest(m, season, episode)
	if err != nil {
		return err
	}
	err = r.RunDue(ctx, queued.ID)
	r.copyStoredMedia(m, queued.ID)
	return err
}

// Queue persists a full resolution before it is run. Callers that only need to
// wake the durable scheduler should use this method rather than Resolve.
func (r *Resolver) Queue(m *model.Media) (*model.Media, error) {
	return r.queue(m, model.MediaWork{Mode: workModeResolve})
}

// QueueRerequest persists a scoped TV resolution. The scheduler can resume it
// after restart without reconstructing dashboard input.
func (r *Resolver) QueueRerequest(m *model.Media, season, episode int) (*model.Media, error) {
	if err := validateRerequestScope(m, season, episode); err != nil {
		return nil, err
	}
	return r.queue(m, model.MediaWork{Mode: workModeRerequest, Season: season, Episode: episode})
}

// QueueReset atomically resets an existing media item into a fresh full
// resolution. It derives every field from the current record so a dashboard
// retry cannot restore stale catalog, intent, or request-association data.
// All of the current media's Seerr markers are unmarked in the same save.
func (r *Resolver) QueueReset(id int64) (*model.Media, error) {
	if r.Store == nil {
		return nil, errors.New("media store is required")
	}
	if id <= 0 {
		return nil, fmt.Errorf("media ID must be positive")
	}
	now := time.Now().UTC()
	return r.Store.UpdateMediaAtomic(id, func(current *model.Media, transaction *store.MediaTransaction) error {
		transaction.UnmarkProcessed(mergeRequestIDs(current.RequestIDs, []int64{current.RequestID})...)
		queueCurrentWork(current, model.MediaWork{Mode: workModeReset}, now)
		return nil
	})
}

// QueueFileRetry queues only the resolution slot represented by fileID. For a
// movie that is one quality; for TV it is one quality/season/episode tuple.
// The existing file remains readable until a replacement is successfully
// materialized and committed.
func (r *Resolver) QueueFileRetry(mediaID int64, fileID string) (*model.Media, error) {
	if r.Store == nil {
		return nil, errors.New("media store is required")
	}
	if mediaID <= 0 || strings.TrimSpace(fileID) == "" {
		return nil, fmt.Errorf("%w: media and file IDs are required", ErrInvalidFileRetry)
	}
	now := time.Now().UTC()
	return r.Store.UpdateMediaAtomic(mediaID, func(current *model.Media, transaction *store.MediaTransaction) error {
		var target *model.File
		for _, file := range transaction.FilesForMedia() {
			if file.ID == fileID {
				target = file
				break
			}
		}
		if target == nil || strings.TrimSpace(target.Quality) == "" {
			return fmt.Errorf("%w: file does not belong to media or has no quality", ErrInvalidFileRetry)
		}
		season, episode := fileEpisode(target)
		if current.Type == "tv" && (season <= 0 || episode <= 0) {
			return fmt.Errorf("%w: TV file has no episode identity", ErrInvalidFileRetry)
		}
		queueCurrentWork(current, model.MediaWork{Mode: workModeRetryFile, Quality: target.Quality, Season: season, Episode: episode}, now)
		return nil
	})
}

// ManualCandidates scrapes one operator-selected slot without resolving it.
// Results contain only display metadata and an opaque content-derived ID.
func (r *Resolver) ManualCandidates(ctx context.Context, mediaID int64, target ManualTarget) ([]ManualCandidate, error) {
	if r.Store == nil {
		return nil, errors.New("media store is required")
	}
	target.Quality = strings.TrimSpace(target.Quality)
	media, ok := r.Store.MediaByID(mediaID)
	if !ok {
		return nil, os.ErrNotExist
	}
	cfg := r.currentConfig()
	if err := validateManualTarget(media, target); err != nil {
		return nil, err
	}
	releases, err := r.manualReleases(ctx, media, target, cfg)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	_, _ = r.Store.UpdateMedia(mediaID, func(current *model.Media) error {
		current.ScrapedAt = now
		current.UpdatedAt = now
		return nil
	})
	out := make([]ManualCandidate, 0, len(releases))
	for _, release := range releases {
		out = append(out, manualCandidate(release))
	}
	return out, nil
}

// ResolveManual re-scrapes and validates candidateID before asking a provider
// to resolve it. Only the selected slot is replaced, and current files remain
// untouched if the candidate cannot be resolved.
func (r *Resolver) ResolveManual(ctx context.Context, mediaID int64, target ManualTarget, candidateID string) (*model.File, error) {
	if r.Store == nil {
		return nil, errors.New("media store is required")
	}
	target.Quality = strings.TrimSpace(target.Quality)
	media, ok := r.Store.MediaByID(mediaID)
	if !ok {
		return nil, os.ErrNotExist
	}
	if strings.TrimSpace(candidateID) == "" {
		return nil, fmt.Errorf("%w: choose a scrape result", ErrManualCandidateNotFound)
	}
	releaseMedia, err := r.acquireMedia(ctx, media)
	if err != nil {
		return nil, err
	}
	defer releaseMedia()
	releaseSlot, err := r.acquireResolution(ctx)
	if err != nil {
		return nil, err
	}
	defer releaseSlot()

	media, ok = r.Store.MediaByID(mediaID)
	if !ok {
		return nil, os.ErrNotExist
	}
	cfg := r.currentConfig()
	if err := validateManualTarget(media, target); err != nil {
		return nil, err
	}
	releases, err := r.manualReleases(ctx, media, target, cfg)
	if err != nil {
		return nil, err
	}
	var selected *model.Release
	for i := range releases {
		if manualReleaseID(releases[i]) == candidateID {
			selected = &releases[i]
			break
		}
	}
	if selected == nil {
		return nil, ErrManualCandidateNotFound
	}
	providers := r.Providers
	if r.ProviderFactory != nil {
		providers = r.ProviderFactory(cfg)
	}
	if delay, cooling := r.allProvidersCooling(cfg.Providers, providers); cooling {
		return nil, fmt.Errorf("all providers cooling down; retry after %s", formatProviderCooldown(delay))
	}
	targetSlot := resolutionSlot(media.Type, target.Quality, target.Season, target.Episode)
	var lastErr error
	for _, name := range cfg.Providers {
		if r.providerCooling(name) {
			continue
		}
		provider := providers[name]
		if provider == nil {
			continue
		}
		attempt, cancel := context.WithTimeout(ctx, cfg.ResolveTimeout)
		resolved, resolveErr := provider.Resolve(attempt, *selected)
		cancel()
		if resolveErr != nil {
			lastErr = resolveErr
			if delay, cooldown := debrid.ProviderCooldown(resolveErr); cooldown {
				r.markProviderCooldown(name, delay)
			}
			continue
		}
		materializeTarget := *media
		if target.Season > 0 {
			materializeTarget.Seasons = []int{target.Season}
		}
		var replacement *model.File
		for _, file := range r.materialize(&materializeTarget, target.Quality, provider.Name(), *selected, resolved) {
			if fileResolutionSlot(media.Type, file) == targetSlot {
				replacement = file
				break
			}
		}
		if replacement == nil {
			lastErr = errors.New("selected stream did not contain the requested file")
			continue
		}
		stored, replaceErr := r.commitManualReplacement(mediaID, target, replacement, cfg.Qualities)
		if replaceErr != nil {
			return nil, replaceErr
		}
		if r.LibraryChanged != nil {
			r.LibraryChanged()
		}
		if r.WorkCompleted != nil {
			r.WorkCompleted()
		}
		if r.Log != nil {
			r.Log.Info("manual stream selected", "component", "resolver", "title", media.Title, "quality", target.Quality, "season", target.Season, "episode", target.Episode, "provider", provider.Name(), "source", selected.Source)
		}
		return stored, nil
	}
	if lastErr != nil {
		return nil, fmt.Errorf("selected stream could not be resolved: %w", lastErr)
	}
	return nil, errors.New("no configured provider could resolve the selected stream")
}

func (r *Resolver) currentConfig() config.Config {
	cfg := r.Config
	if r.Settings != nil {
		cfg = r.Settings()
	}
	return cfg
}

func validateManualTarget(media *model.Media, target ManualTarget) error {
	target.Quality = strings.TrimSpace(target.Quality)
	if media == nil || target.Quality == "" {
		return fmt.Errorf("%w: quality is required", ErrInvalidManualTarget)
	}
	if media.Type == "movie" {
		if target.Season != 0 || target.Episode != 0 {
			return fmt.Errorf("%w: movie targets cannot include an episode", ErrInvalidManualTarget)
		}
		return nil
	}
	if media.Type != "tv" || target.Season <= 0 || target.Episode <= 0 || !containsInt(media.Seasons, target.Season) {
		return fmt.Errorf("%w: choose a tracked TV season and episode", ErrInvalidManualTarget)
	}
	if count := media.EpisodeCounts[target.Season]; count > 0 && target.Episode > count {
		return fmt.Errorf("%w: episode %d is outside season %d", ErrInvalidManualTarget, target.Episode, target.Season)
	}
	return nil
}

func (r *Resolver) manualReleases(ctx context.Context, media *model.Media, target ManualTarget, cfg config.Config) ([]model.Release, error) {
	searcher := r.Scraper
	if r.ScraperFactory != nil {
		var err error
		searcher, err = r.ScraperFactory(cfg)
		if err != nil {
			return nil, err
		}
	}
	if searcher == nil {
		return nil, errors.New("media scraper is unavailable")
	}
	// Apply the per-quality limit only after filtering and ranking.
	limit := 0
	releases, err := searcher.Search(ctx, scraper.Query{MediaType: media.Type, ExternalID: media.ExternalID, TMDBID: media.TMDBID, Season: target.Season, Episode: target.Episode}, limit)
	if err != nil {
		return nil, err
	}
	candidates := releases[:0]
	for _, release := range releases {
		if (release.Seeders < 0 || release.Seeders >= cfg.MinSeeders) && matchesQuality(release.Title, target.Quality) {
			candidates = append(candidates, release)
		}
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		return releasePreferred(candidates[i], candidates[j], target.Quality, target.Season)
	})
	if cfg.MaxResults > 0 && len(candidates) > cfg.MaxResults {
		candidates = candidates[:cfg.MaxResults]
	}
	return candidates, nil
}

func manualCandidate(release model.Release) ManualCandidate {
	return ManualCandidate{ID: manualReleaseID(release), Title: release.Title, Source: release.Source, Size: release.Size, Seeders: release.Seeders}
}

func manualReleaseID(release model.Release) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{release.Title, release.DownloadURL, release.InfoHash, release.Source, strconv.FormatInt(release.Size, 10)}, "\x00")))
	return hex.EncodeToString(sum[:12])
}

func (r *Resolver) commitManualReplacement(mediaID int64, target ManualTarget, replacement *model.File, qualities []string) (*model.File, error) {
	targetSlot := resolutionSlot("movie", target.Quality, target.Season, target.Episode)
	var storedFile *model.File
	_, err := r.Store.UpdateMediaAtomic(mediaID, func(current *model.Media, transaction *store.MediaTransaction) error {
		targetSlot = resolutionSlot(current.Type, target.Quality, target.Season, target.Episode)
		files := transaction.FilesForMedia()
		finalFiles := make([]*model.File, 0, len(files)+1)
		for _, file := range files {
			if fileResolutionSlot(current.Type, file) != targetSlot {
				finalFiles = append(finalFiles, file)
			}
		}
		storedFile = replacement
		finalFiles = append(finalFiles, replacement)
		if err := transaction.ReplaceFiles(finalFiles...); err != nil {
			return err
		}
		if current.Work.Mode == workModeRetryFile && strings.EqualFold(current.Work.Quality, target.Quality) && current.Work.Season == target.Season && current.Work.Episode == target.Episode {
			current.Work = model.MediaWork{}
		}
		if current.Work.Mode == "" {
			current.Status = ResolvedMediaStatus(current, finalFiles, qualities)
		} else if current.Work.LeaseUntil.After(time.Now().UTC()) {
			current.Status = "resolving"
		} else {
			current.Status = "queued"
		}
		current.Error = ""
		current.ScrapedAt = time.Now().UTC()
		current.UpdatedAt = time.Now().UTC()
		current.PlexIntent.Generation++
		if current.PlexIntent.Generation <= 0 {
			current.PlexIntent.Generation = 1
		}
		current.PlexIntent.Attempts = 0
		current.PlexIntent.NextAt = time.Now().UTC().Add(r.plexScanDelay())
		current.PlexIntent.LeaseUntil = time.Time{}
		current.PlexIntent.LeaseGeneration = 0
		return nil
	})
	if err != nil {
		return nil, err
	}
	return storedFile, nil
}

// QueueExistingRerequest atomically validates and queues a scoped retry for
// an existing local media ID. Unlike QueueRerequest, it never falls back to a
// create/identity merge when a concurrent delete removes that known item.
func (r *Resolver) QueueExistingRerequest(id int64, season, episode int) (*model.Media, error) {
	if r.Store == nil {
		return nil, errors.New("media store is required")
	}
	if id <= 0 {
		return nil, fmt.Errorf("%w: media ID must be positive", ErrInvalidRerequestScope)
	}
	now := time.Now().UTC()
	return r.Store.UpdateMediaAtomic(id, func(current *model.Media, _ *store.MediaTransaction) error {
		if err := validateRerequestScope(current, season, episode); err != nil {
			return err
		}
		queueCurrentWork(current, model.MediaWork{Mode: workModeRerequest, Season: season, Episode: episode}, now)
		return nil
	})
}

func validateRerequestScope(m *model.Media, season, episode int) error {
	if m == nil || m.Type != "tv" || season <= 0 || episode < 0 || !containsInt(m.Seasons, season) {
		return fmt.Errorf("%w: re-request requires a TV season and optional episode", ErrInvalidRerequestScope)
	}
	if count := m.EpisodeCounts[season]; episode > 0 && count > 0 && episode > count {
		return fmt.Errorf("%w: episode %d is outside season %d", ErrInvalidRerequestScope, episode, season)
	}
	return nil
}

func (r *Resolver) queue(incoming *model.Media, work model.MediaWork) (*model.Media, error) {
	if r.Store == nil {
		return nil, errors.New("media store is required")
	}
	if incoming == nil {
		return nil, errors.New("media is required")
	}
	work = freshQueuedWork(work)
	now := time.Now().UTC()

	var existing *model.Media
	if incoming.ID != 0 {
		existing, _ = r.Store.MediaByID(incoming.ID)
	}
	if existing == nil && incoming.TMDBID > 0 && incoming.Type != "" {
		existing, _ = r.Store.FindMediaByTMDB(incoming.Type, incoming.TMDBID)
	}
	if existing == nil {
		candidate := *incoming
		candidate.RequestIDs = mergeRequestIDs(candidate.RequestIDs, []int64{candidate.RequestID})
		work.Generation = 1
		candidate.Work = work
		candidate.Status = "queued"
		candidate.Error = ""
		candidate.ScrapedAt = time.Time{}
		if candidate.CreatedAt.IsZero() {
			candidate.CreatedAt = now
		}
		candidate.UpdatedAt = now
		stored, err := r.Store.Commit(store.Mutation{Media: &candidate})
		if err == nil && stored != nil {
			*incoming = *stored
		}
		return stored, err
	}

	stored, err := r.Store.UpdateMedia(existing.ID, func(current *model.Media) error {
		mergeQueuedMedia(current, incoming)
		queueCurrentWork(current, work, now)
		return nil
	})
	if err == nil && stored != nil {
		*incoming = *stored
	}
	return stored, err
}

func freshQueuedWork(work model.MediaWork) model.MediaWork {
	if work.Mode == "" {
		work.Mode = workModeResolve
	}
	work.Attempts = 0
	work.NextAt = time.Time{}
	work.LeaseUntil = time.Time{}
	return work
}

func queueCurrentWork(current *model.Media, work model.MediaWork, now time.Time) {
	work = freshQueuedWork(work)
	work.Generation = current.Work.Generation + 1
	if work.Generation <= 0 {
		work.Generation = 1
	}
	current.Work = work
	current.Status = "queued"
	current.Error = ""
	current.ScrapedAt = time.Time{}
	current.UpdatedAt = now
}

// RunDue atomically claims and executes work only when it is due. It is safe
// for the scheduler and synchronous compatibility methods to call concurrently.
func (r *Resolver) RunDue(ctx context.Context, id int64) error {
	if r.Store == nil {
		return errors.New("media store is required")
	}
	media, ok := r.Store.MediaByID(id)
	if !ok {
		return fmt.Errorf("media %d not found", id)
	}
	releaseMedia, err := r.acquireMedia(ctx, media)
	if err != nil {
		return err
	}
	defer releaseMedia()

	// Re-read only after holding the stable key. Identity de-duplication or a
	// concurrent dashboard update may have changed both ID and work meanwhile.
	if media.TMDBID > 0 && media.Type != "" {
		if refreshed, found := r.Store.FindMediaByTMDB(media.Type, media.TMDBID); found {
			media = refreshed
		}
	} else if refreshed, found := r.Store.MediaByID(media.ID); found {
		media = refreshed
	}
	claimed, err := r.claimWork(media.ID, time.Now().UTC())
	if errors.Is(err, errWorkNotDue) {
		return nil
	}
	if err != nil {
		return err
	}

	releaseSlot, err := r.acquireResolution(ctx)
	if err != nil {
		_ = r.releaseClaim(claimed.ID, claimed.Work, time.Now().UTC(), err)
		return err
	}
	defer releaseSlot()

	selectedSeason, selectedEpisode, selectedQuality := 0, 0, ""
	replaceExisting := claimed.Work.Mode == workModeReset || claimed.Work.Mode == workModeRerequest || claimed.Work.Mode == workModeRetryFile
	forceReplaceAll := claimed.Work.Mode == workModeReset
	if claimed.Work.Mode == workModeRerequest || claimed.Work.Mode == workModeRetryFile {
		selectedSeason, selectedEpisode = claimed.Work.Season, claimed.Work.Episode
	}
	if claimed.Work.Mode == workModeRetryFile {
		selectedQuality = claimed.Work.Quality
	}
	err = r.resolveUnbounded(ctx, claimed, selectedQuality, selectedSeason, selectedEpisode, replaceExisting, forceReplaceAll)
	if err != nil {
		_ = r.releaseClaim(claimed.ID, claimed.Work, time.Now().UTC(), err)
	}
	return err
}

func (r *Resolver) copyStoredMedia(target *model.Media, id int64) {
	if target == nil || r.Store == nil {
		return
	}
	if stored, ok := r.Store.MediaByID(id); ok {
		*target = *stored
		return
	}
	if target.TMDBID > 0 && target.Type != "" {
		if stored, ok := r.Store.FindMediaByTMDB(target.Type, target.TMDBID); ok {
			*target = *stored
		}
	}
}

func mergeQueuedMedia(dst, src *model.Media) {
	if dst == nil || src == nil {
		return
	}
	if src.Type != "" {
		dst.Type = src.Type
	}
	if src.TMDBID > 0 {
		dst.TMDBID = src.TMDBID
	}
	if src.SeerrMediaID > 0 {
		dst.SeerrMediaID = src.SeerrMediaID
	}
	if src.RequestID > 0 {
		dst.RequestID = src.RequestID
	}
	dst.RequestIDs = mergeRequestIDs(dst.RequestIDs, src.RequestIDs, []int64{src.RequestID})
	if dst.RequestID > 0 {
		dst.RequestIDs = mergeRequestIDs(dst.RequestIDs, []int64{dst.RequestID})
	}
	if src.ExternalID != "" {
		dst.ExternalID = src.ExternalID
	}
	if src.Title != "" {
		dst.Title = src.Title
	}
	if src.Year > 0 {
		dst.Year = src.Year
	}
	if src.Overview != "" {
		dst.Overview = src.Overview
	}
	if src.PosterPath != "" {
		dst.PosterPath = src.PosterPath
	}
	if src.BackdropPath != "" {
		dst.BackdropPath = src.BackdropPath
	}
	if src.ReleaseDate != "" {
		dst.ReleaseDate = src.ReleaseDate
	}
	if len(src.Seasons) > 0 {
		dst.Seasons = mergeSeasons(dst.Seasons, src.Seasons)
	}
	if len(src.EpisodeCounts) > 0 {
		if dst.EpisodeCounts == nil {
			dst.EpisodeCounts = map[int]int{}
		}
		for season, count := range src.EpisodeCounts {
			if count > 0 {
				dst.EpisodeCounts[season] = count
			}
		}
	}
	if len(src.EpisodeAirDates) > 0 {
		dst.EpisodeAirDates = mergeEpisodeAirDates(dst.EpisodeAirDates, src.EpisodeAirDates)
	}
	if dst.CreatedAt.IsZero() && !src.CreatedAt.IsZero() {
		dst.CreatedAt = src.CreatedAt
	}
}

func mergeRequestIDs(values []int64, additions ...[]int64) []int64 {
	seen := make(map[int64]bool, len(values))
	out := make([]int64, 0, len(values))
	for _, id := range values {
		if id > 0 && !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	for _, more := range additions {
		for _, id := range more {
			if id > 0 && !seen[id] {
				seen[id] = true
				out = append(out, id)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func mergeSeasons(left, right []int) []int {
	seen := make(map[int]bool, len(left)+len(right))
	out := make([]int, 0, len(left)+len(right))
	for _, season := range append(append([]int(nil), left...), right...) {
		if season > 0 && !seen[season] {
			seen[season] = true
			out = append(out, season)
		}
	}
	sort.Ints(out)
	return out
}

func mergeEpisodeAirDates(left, right []model.EpisodeAirDate) []model.EpisodeAirDate {
	bySlot := make(map[string]model.EpisodeAirDate, len(left)+len(right))
	for _, date := range append(append([]model.EpisodeAirDate(nil), left...), right...) {
		if date.Season <= 0 || date.Episode <= 0 {
			continue
		}
		date.AirDate = validDate(date.AirDate)
		if date.AirDate != "" {
			bySlot[fmt.Sprintf("%d:%d", date.Season, date.Episode)] = date
		}
	}
	out := make([]model.EpisodeAirDate, 0, len(bySlot))
	for _, date := range bySlot {
		out = append(out, date)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Season != out[j].Season {
			return out[i].Season < out[j].Season
		}
		return out[i].Episode < out[j].Episode
	})
	return out
}

func (r *Resolver) acquireMedia(ctx context.Context, media *model.Media) (func(), error) {
	key := stableMediaKey(media)
	gate := make(chan struct{}, 1)
	actual, _ := r.mediaGates.LoadOrStore(key, gate)
	gate = actual.(chan struct{})
	select {
	case gate <- struct{}{}:
		return func() { <-gate }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func stableMediaKey(media *model.Media) string {
	if media == nil {
		return "media:unknown"
	}
	kind := strings.ToLower(strings.TrimSpace(media.Type))
	if kind == "" {
		kind = "media"
	}
	if media.TMDBID > 0 {
		return fmt.Sprintf("%s:%d", kind, media.TMDBID)
	}
	return fmt.Sprintf("%s:local:%d", kind, media.ID)
}

func (r *Resolver) claimWork(id int64, now time.Time) (*model.Media, error) {
	lease := r.workLease()
	return r.Store.UpdateMedia(id, func(media *model.Media) error {
		ensureLegacyWork(media, now)
		if media.Work.Mode == "" || media.Work.LeaseUntil.After(now) || (!media.Work.NextAt.IsZero() && media.Work.NextAt.After(now)) {
			return errWorkNotDue
		}
		if media.Work.Generation <= 0 {
			media.Work.Generation = 1
		}
		media.Work.LeaseUntil = now.Add(lease)
		media.Status = "resolving"
		media.Error = ""
		media.UpdatedAt = now
		return nil
	})
}

func ensureLegacyWork(media *model.Media, now time.Time) {
	if media == nil || media.Work.Mode != "" {
		return
	}
	switch media.Status {
	case "queued", "scraping", "resolving":
		media.Work = model.MediaWork{Mode: workModeResolve, Generation: 1}
	case "unreleased":
		work := model.MediaWork{Mode: workModeResolve, Generation: 1}
		if IsUnreleased(media, now) {
			work.NextAt = releaseDueAt(media.ReleaseDate)
		}
		media.Work = work
	}
}

func (r *Resolver) workLease() time.Duration {
	cfg := r.Config
	if r.Settings != nil {
		cfg = r.Settings()
	}
	lease := cfg.ResolveTimeout + time.Minute
	if lease <= time.Minute {
		lease = 16 * time.Minute
	}
	if lease > time.Hour {
		lease = time.Hour
	}
	return lease
}

func (r *Resolver) plexScanDelay() time.Duration {
	cfg := r.Config
	if r.Settings != nil {
		cfg = r.Settings()
	}
	if cfg.PlexScanDelay <= 0 {
		return 15 * time.Second
	}
	return cfg.PlexScanDelay
}

func releaseDueAt(date string) time.Time {
	date = validDate(date)
	if date == "" {
		return time.Time{}
	}
	value, err := time.Parse("2006-01-02", date)
	if err != nil {
		return time.Time{}
	}
	return value.UTC()
}

func retryAfter(attempts int) time.Duration {
	if attempts < 1 {
		attempts = 1
	}
	delay := 15 * time.Second
	for i := 1; i < attempts && delay < time.Hour; i++ {
		delay *= 2
	}
	if delay > time.Hour {
		return time.Hour
	}
	return delay
}

func earlierWorkDue(left, right time.Time) time.Time {
	if left.IsZero() || (!right.IsZero() && right.Before(left)) {
		return right
	}
	return left
}

func (r *Resolver) releaseClaim(id int64, claimed model.MediaWork, now time.Time, cause error) error {
	_, err := r.Store.UpdateMedia(id, func(media *model.Media) error {
		if media.Work.Generation != claimed.Generation || media.Work.LeaseUntil != claimed.LeaseUntil {
			return nil
		}
		media.Work.LeaseUntil = time.Time{}
		media.Work.Attempts++
		media.Work.NextAt = now.Add(retryAfter(media.Work.Attempts))
		media.Status = "queued"
		if cause != nil {
			media.Error = cause.Error()
		}
		media.UpdatedAt = now
		return nil
	})
	if err == nil && r.WorkCompleted != nil {
		r.WorkCompleted()
	}
	return err
}

func (r *Resolver) updateProgress(m *model.Media, status string, scrapedAt time.Time) {
	if m == nil || r.Store == nil {
		return
	}
	now := time.Now().UTC()
	_, _ = r.Store.UpdateMedia(m.ID, func(current *model.Media) error {
		// A newer dashboard command supersedes this worker. Do not let an
		// in-flight compatibility status update erase its durable work.
		if current.Work.Generation != m.Work.Generation || current.Work.LeaseUntil != m.Work.LeaseUntil {
			return nil
		}
		current.Status = status
		current.UpdatedAt = now
		if !scrapedAt.IsZero() {
			current.ScrapedAt = scrapedAt
		}
		return nil
	})
}

// finishResolution publishes file changes, the claimed resolution result, and
// a Plex intent through one Store.UpdateMediaAtomic callback. retryAt retains
// the claim for scheduler recovery; zero clears it. The caller has already
// held the stable media gate throughout the actual provider work.
func (r *Resolver) finishResolution(m *model.Media, finalFiles []*model.File, replaceFiles, filesChanged bool, retryAt time.Time, retry bool) error {
	if m == nil {
		return errors.New("media is required")
	}
	if r.Store == nil {
		return errors.New("media store is required")
	}
	expectedWork := m.Work
	resolvedStatus, resolvedError, resolvedScrapedAt := m.Status, m.Error, m.ScrapedAt
	if r.beforeFinishResolution != nil {
		r.beforeFinishResolution()
	}
	scanDelay := r.plexScanDelay()
	stored, err := r.Store.UpdateMediaAtomic(m.ID, func(current *model.Media, transaction *store.MediaTransaction) error {
		claimCurrent := current.Work.Generation == expectedWork.Generation && current.Work.LeaseUntil.Equal(expectedWork.LeaseUntil)
		if replaceFiles {
			if err := transaction.ReplaceFiles(finalFiles...); err != nil {
				return err
			}
		}
		if claimCurrent {
			// A claim owns only the resolution result and its durable work lease.
			// Catalog, request, and other side-effect state remain current.
			current.Status = resolvedStatus
			current.Error = resolvedError
			if !resolvedScrapedAt.IsZero() {
				current.ScrapedAt = resolvedScrapedAt
			}
			if !retryAt.IsZero() {
				current.Work.LeaseUntil = time.Time{}
				current.Work.NextAt = retryAt
				if retry {
					current.Work.Attempts++
				}
			} else {
				current.Work = model.MediaWork{}
			}
		}
		if filesChanged {
			// Always start from the current intent. A superseded worker may still
			// publish useful files, but cannot replace newer lifecycle state.
			intent := current.PlexIntent
			intent.Generation++
			if intent.Generation <= 0 {
				intent.Generation = 1
			}
			intent.Attempts = 0
			intent.NextAt = time.Now().UTC().Add(scanDelay)
			intent.LeaseUntil = time.Time{}
			intent.LeaseGeneration = 0
			current.PlexIntent = intent
		}
		if claimCurrent || replaceFiles || filesChanged {
			current.UpdatedAt = time.Now().UTC()
		}
		return nil
	})
	if err == nil && stored != nil {
		*m = *stored
	}
	if err == nil && r.WorkCompleted != nil {
		r.WorkCompleted()
	}
	return err
}

func episodeAirDate(media *model.Media, season, episode int) string {
	if media == nil {
		return ""
	}
	for _, entry := range media.EpisodeAirDates {
		if entry.Season == season && entry.Episode == episode {
			return validDate(entry.AirDate)
		}
	}
	return ""
}

func futureEpisodeAt(media *model.Media, season, episode int, now time.Time) time.Time {
	date := episodeAirDate(media, season, episode)
	if date == "" || date <= now.UTC().Format("2006-01-02") {
		return time.Time{}
	}
	return releaseDueAt(date)
}

func (r *Resolver) resolveUnbounded(ctx context.Context, m *model.Media, selectedQuality string, selectedSeason, selectedEpisode int, replaceExisting, forceReplaceAll bool) error {
	if IsUnreleased(m, time.Now()) {
		m.Status = "unreleased"
		m.Error = ""
		if r.Log != nil {
			r.Log.Info("media resolution deferred until release", "component", "resolver", "title", m.Title, "release_date", m.ReleaseDate)
		}
		return r.finishResolution(m, nil, false, false, releaseDueAt(m.ReleaseDate), false)
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
			return err
		}
	}
	providers := r.Providers
	if r.ProviderFactory != nil {
		providers = r.ProviderFactory(cfg)
	}
	if delay, cooling := r.allProvidersCooling(cfg.Providers, providers); cooling {
		m.Status = "queued"
		m.Error = fmt.Sprintf("all providers cooling down; retry after %s", formatProviderCooldown(delay))
		if r.Log != nil {
			r.Log.Warn("media resolution deferred during provider cooldown", "component", "resolver", "title", m.Title, "retry_after", formatProviderCooldown(delay))
		}
		return r.finishResolution(m, nil, false, false, time.Now().UTC().Add(delay), true)
	}
	started := time.Now()
	if r.Log != nil {
		r.Log.Info("media resolution started", "component", "resolver", "title", m.Title, "type", m.Type, "imdb_id", m.ExternalID, "tmdb_id", m.TMDBID, "qualities", cfg.Qualities)
	}
	m.Status = "resolving"
	r.updateProgress(m, "resolving", time.Time{})
	var errs []string
	resolvedFilesBySlot := map[string]*model.File{}
	resolvedSlots := map[string]bool{}
	existingFiles := r.Store.FilesForMedia(m.ID)
	completedSlots := make(map[string]bool, len(existingFiles))
	for _, file := range existingFiles {
		completedSlots[fileResolutionSlot(m.Type, file)] = true
	}
	type job struct {
		quality string
		season  int
		episode int
	}
	var jobs []job
	var futureAt time.Time
	qualities := cfg.Qualities
	if selectedQuality != "" {
		qualities = []string{selectedQuality}
	}
	for _, q := range qualities {
		if m.Type == "tv" && len(m.Seasons) > 0 {
			seasons := m.Seasons
			if selectedSeason > 0 {
				seasons = []int{selectedSeason}
			}
			for _, season := range seasons {
				episodes := m.EpisodeCounts[season]
				if episodes <= 0 {
					episodes = 1
				}
				firstEpisode := 1
				if selectedEpisode > 0 {
					firstEpisode, episodes = selectedEpisode, selectedEpisode
				}
				for episode := firstEpisode; episode <= episodes; episode++ {
					if due := futureEpisodeAt(m, season, episode, time.Now()); !due.IsZero() {
						if futureAt.IsZero() || due.Before(futureAt) {
							futureAt = due
						}
						continue
					}
					jobs = append(jobs, job{quality: q, season: season, episode: episode})
				}
			}
		} else {
			jobs = append(jobs, job{quality: q})
		}
	}
	wantedSlots := make(map[string]bool, len(jobs))
	for _, work := range jobs {
		slot := resolutionSlot(m.Type, work.quality, work.season, work.episode)
		wantedSlots[slot] = true
		if forceReplaceAll || (replaceExisting && selectedEpisode > 0) {
			delete(completedSlots, slot)
		}
	}
	if replaceExisting && !forceReplaceAll && selectedEpisode == 0 {
		for _, file := range existingFiles {
			season, _ := fileEpisode(file)
			if season == selectedSeason {
				delete(completedSlots, fileResolutionSlot(m.Type, file))
			}
		}
	}
	rateLimitedProviders := map[string]bool{}
	deferred := false
jobLoop:
	for _, work := range jobs {
		q := work.quality
		label := q
		if m.Type == "tv" && work.season > 0 {
			label = fmt.Sprintf("S%02dE%02d %s", work.season, work.episode, q)
		}
		targetSlot := resolutionSlot(m.Type, q, work.season, work.episode)
		if completedSlots[targetSlot] {
			continue
		}
		m.Status = "scraping"
		r.updateProgress(m, "scraping", time.Time{})
		// Fetch all qualities before applying the per-quality attempt limit.
		searchLimit := 0
		rels, err := searcher.Search(ctx, scraper.Query{MediaType: m.Type, ExternalID: m.ExternalID, TMDBID: m.TMDBID, Season: work.season, Episode: work.episode}, searchLimit)
		if err != nil {
			if r.Log != nil {
				r.Log.Error("media scrape failed", "component", "resolver", "title", m.Title, "target", label, "error", err)
			}
			errs = append(errs, label+": "+err.Error())
			if errors.Is(err, scraper.ErrRateLimited) {
				break jobLoop
			}
			continue
		}
		m.ScrapedAt = time.Now().UTC()
		m.Status = "resolving"
		r.updateProgress(m, "resolving", m.ScrapedAt)
		if r.Log != nil {
			r.Log.Info("media scrape completed", "component", "resolver", "title", m.Title, "target", label, "streams", len(rels))
		}
		candidates := rels[:0]
		for _, rel := range rels {
			if (rel.Seeders < 0 || rel.Seeders >= cfg.MinSeeders) && matchesQuality(rel.Title, q) {
				candidates = append(candidates, rel)
			}
		}
		rels = candidates
		sort.SliceStable(rels, func(i, j int) bool { return releasePreferred(rels[i], rels[j], q, work.season) })
		if cfg.MaxResults > 0 && len(rels) > cfg.MaxResults {
			rels = rels[:cfg.MaxResults]
		}
		found := false
		for _, rel := range rels {
			for _, name := range cfg.Providers {
				if rateLimitedProviders[name] || r.providerCooling(name) {
					continue
				}
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
					if delay, cooldown := debrid.ProviderCooldown(e); cooldown {
						rateLimitedProviders[name] = true
						r.markProviderCooldown(name, delay)
					}
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
				if m.Type == "tv" && !filesContainSlot(m.Type, files, targetSlot) {
					continue
				}
				added := 0
				for _, file := range files {
					if m.Type == "tv" {
						season, episode := fileEpisode(file)
						if due := futureEpisodeAt(m, season, episode, time.Now()); !due.IsZero() {
							continue
						}
					}
					slot := fileResolutionSlot(m.Type, file)
					if (selectedEpisode > 0 && !wantedSlots[slot]) || completedSlots[slot] {
						continue
					}
					resolvedFilesBySlot[slot] = file
					resolvedSlots[slot] = true
					completedSlots[slot] = true
					added++
				}
				if r.Log != nil {
					r.Log.Info("provider resolution completed", "component", "resolver", "title", m.Title, "target", label, "provider", p.Name(), "source", rel.Source, "files_added", added, "cached", resolved.Cached, "duration", time.Since(attemptStarted).String())
				}
				found = completedSlots[targetSlot]
				break
			}
			if found {
				break
			}
		}
		if !found {
			if delay, cooling := r.allProvidersCooling(cfg.Providers, providers); cooling {
				deferred = true
				if r.Log != nil {
					r.Log.Warn("remaining media resolution deferred during provider cooldown", "component", "resolver", "title", m.Title, "retry_after", formatProviderCooldown(delay))
				}
				break jobLoop
			}
			errs = append(errs, label+": no acceptable cached release")
		}
	}
	total := len(resolvedFilesBySlot)
	finalFiles := append([]*model.File(nil), existingFiles...)
	if deferred {
		m.Status = "queued"
		if delay, cooling := r.allProvidersCooling(cfg.Providers, providers); cooling {
			m.Error = fmt.Sprintf("all providers cooling down; retry after %s", formatProviderCooldown(delay))
		} else {
			m.Error = "provider cooldown active; retry later"
		}
	}
	if total > 0 {
		finalFiles = make([]*model.File, 0, total+len(existingFiles))
		for _, file := range resolvedFilesBySlot {
			finalFiles = append(finalFiles, file)
		}
		sort.Slice(finalFiles, func(i, j int) bool { return finalFiles[i].Path < finalFiles[j].Path })
		for _, previous := range existingFiles {
			if !resolvedSlots[fileResolutionSlot(m.Type, previous)] {
				finalFiles = append(finalFiles, previous)
			}
		}
	}
	if !deferred {
		m.Status = ResolvedMediaStatus(m, finalFiles, cfg.Qualities)
		if replaceExisting && len(errs) > 0 {
			if total == 0 {
				m.Status = "failed"
			} else if m.Status == "ready" {
				m.Status = "partial"
			}
		}
		if m.Status == "failed" {
			m.Error = strings.Join(errs, "; ")
		} else if m.Status == "partial" {
			m.Error = strings.Join(errs, "; ")
		} else {
			m.Error = ""
		}
	}
	// completedSlots includes both existing and newly materialized files. A
	// partial media item can be missing only future slots, so only failed due
	// slots earn a transient retry before the next future air date.
	dueIncomplete := false
	for slot := range wantedSlots {
		if !completedSlots[slot] {
			dueIncomplete = true
			break
		}
	}
	scopeSatisfied := replaceExisting && rerequestScopeSatisfied(m, finalFiles, cfg.Qualities)
	transientRetryAt := time.Time{}
	transientRetry := false
	if deferred {
		if delay, cooling := r.allProvidersCooling(cfg.Providers, providers); cooling && delay > 0 {
			transientRetryAt = time.Now().UTC().Add(delay)
		} else {
			transientRetryAt = time.Now().UTC().Add(retryAfter(m.Work.Attempts + 1))
		}
		transientRetry = true
	} else if dueIncomplete && (m.Status == "failed" || m.Status == "partial") && !scopeSatisfied {
		// Keep transiently incomplete work durable. The visible status remains
		// compatible with the dashboard while the scheduler avoids a hot loop.
		transientRetryAt = time.Now().UTC().Add(retryAfter(m.Work.Attempts + 1))
		transientRetry = true
	}
	retryAt := transientRetryAt
	retryWork := transientRetry
	if !futureAt.IsZero() {
		if transientRetryAt.IsZero() {
			// Future-only work is dormant until its first air date. There is no
			// failed due slot to retry before then.
			m.Status = "queued"
			m.Error = "waiting for future episode release"
			retryAt = futureAt
		} else {
			// A failed due slot and a future slot share one durable command. Wake
			// for whichever event comes first, rather than starving retries until
			// the future episode airs.
			retryAt = earlierWorkDue(transientRetryAt, futureAt)
		}
	}
	err := r.finishResolution(m, finalFiles, total > 0, total > 0, retryAt, retryWork)
	if err == nil && total > 0 && r.LibraryChanged != nil {
		r.LibraryChanged()
	}
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

func (r *Resolver) acquireResolution(ctx context.Context) (func(), error) {
	if r.ResolutionConcurrency <= 0 {
		return func() {}, nil
	}
	r.resolutionMu.Lock()
	if r.resolutionSlots == nil {
		r.resolutionSlots = make(chan struct{}, r.ResolutionConcurrency)
	}
	slots := r.resolutionSlots
	r.resolutionMu.Unlock()
	select {
	case slots <- struct{}{}:
		return func() { <-slots }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func IsUnreleased(m *model.Media, now time.Time) bool {
	date := validDate(m.ReleaseDate)
	return date != "" && date > now.UTC().Format("2006-01-02")
}

func resolutionSlot(kind, quality string, season, episode int) string {
	quality = strings.ToLower(strings.TrimSpace(quality))
	if kind == "tv" {
		return fmt.Sprintf("%s|%d|%d", quality, season, episode)
	}
	return quality
}

func fileResolutionSlot(kind string, file *model.File) string {
	season := 0
	episode := 0
	if kind == "tv" {
		season, episode = fileEpisode(file)
	}
	return resolutionSlot(kind, file.Quality, season, episode)
}

func fileEpisode(file *model.File) (int, int) {
	if file == nil {
		return 0, 0
	}
	match := episodeRE.FindStringSubmatch(file.Path)
	if len(match) != 3 {
		return 0, 0
	}
	season, _ := strconv.Atoi(match[1])
	episode, _ := strconv.Atoi(match[2])
	return season, episode
}

func filesContainSlot(kind string, files []*model.File, wanted string) bool {
	for _, file := range files {
		if fileResolutionSlot(kind, file) == wanted {
			return true
		}
	}
	return false
}

// rerequestScopeSatisfied deliberately evaluates the scoped command rather
// than the whole media item. A season/episode re-request can therefore finish
// successfully while another tracked season leaves the overall media status
// partial. At least one configured quality must exist for every scoped slot.
func rerequestScopeSatisfied(media *model.Media, files []*model.File, qualities []string) bool {
	if media == nil || media.Type != "tv" || media.Work.Mode != workModeRerequest || media.Work.Season <= 0 {
		return false
	}
	if media.Work.Episode > 0 {
		return scopedEpisodeAvailable(files, media.Work.Season, media.Work.Episode, qualities)
	}
	count := media.EpisodeCounts[media.Work.Season]
	if count <= 0 {
		// This matches the resolver's legacy unknown-count work shape: one
		// representative episode is the only schedulable slot.
		count = 1
	}
	for episode := 1; episode <= count; episode++ {
		if !scopedEpisodeAvailable(files, media.Work.Season, episode, qualities) {
			return false
		}
	}
	return true
}

func scopedEpisodeAvailable(files []*model.File, season, episode int, qualities []string) bool {
	for _, file := range files {
		fileSeason, fileEpisodeNumber := fileEpisode(file)
		if fileSeason != season || fileEpisodeNumber != episode {
			continue
		}
		if len(qualities) == 0 {
			return true
		}
		for _, quality := range qualities {
			if strings.EqualFold(strings.TrimSpace(file.Quality), strings.TrimSpace(quality)) {
				return true
			}
		}
	}
	return false
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

	media, found := r.Store.MediaByID(stale.MediaID)
	if !found {
		call.err = fmt.Errorf("repair media not found")
	} else {
		releaseMedia, acquireErr := r.acquireMedia(ctx, media)
		if acquireErr != nil {
			call.err = acquireErr
		} else {
			release, slotErr := r.acquireResolution(ctx)
			if slotErr != nil {
				call.err = slotErr
			} else {
				call.file, call.err = r.repair(ctx, stale)
				release()
			}
			releaseMedia()
		}
	}
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
	providers := r.Providers
	if r.ProviderFactory != nil {
		providers = r.ProviderFactory(cfg)
	}
	if delay, cooling := r.allProvidersCooling(cfg.Providers, providers); cooling {
		return nil, fmt.Errorf("all providers cooling down; retry after %s", formatProviderCooldown(delay))
	}
	season := 0
	episode := 0
	if match := episodeRE.FindStringSubmatch(stale.Path); len(match) == 3 {
		season, _ = strconv.Atoi(match[1])
		episode, _ = strconv.Atoi(match[2])
	}
	if r.Log != nil {
		r.Log.Warn("stale stream repair started", "component", "resolver", "title", media.Title, "file", stale.Path, "quality", stale.Quality, "provider", stale.Provider)
	}
	if stale.InfoHash == "" {
		return nil, errors.New("automatic repair requires the original info hash; rescrape this file")
	}
	var releases []model.Release
	if stale.SourceURI != "" {
		// Re-add the known torrent first. Playback must not depend on an
		// addon still listing the original release (or being online).
		releases = []model.Release{{InfoHash: stale.InfoHash, DownloadURL: stale.SourceURI}}
	} else {
		searcher := r.Scraper
		if r.ScraperFactory != nil {
			var err error
			searcher, err = r.ScraperFactory(cfg)
			if err != nil {
				return nil, err
			}
		}
		if searcher == nil {
			return nil, errors.New("media scraper is unavailable")
		}
		var err error
		releases, err = searcher.Search(ctx, scraper.Query{MediaType: media.Type, ExternalID: media.ExternalID, TMDBID: media.TMDBID, Season: season, Episode: episode}, 0)
		if err != nil {
			return nil, err
		}
	}
	candidates := releases[:0]
	for _, release := range releases {
		if stale.InfoHash != "" && strings.EqualFold(release.InfoHash, stale.InfoHash) {
			candidates = append(candidates, release)
		}
	}
	releases = candidates
	sort.SliceStable(releases, func(i, j int) bool {
		return releasePreferred(releases[i], releases[j], stale.Quality, season)
	})
	if cfg.MaxResults > 0 && len(releases) > cfg.MaxResults {
		releases = releases[:cfg.MaxResults]
	}
	providerOrder := append([]string(nil), cfg.Providers...)
	if stale.Provider != "" {
		providerOrder = moveFirst(providerOrder, stale.Provider)
	}
	rateLimitedProviders := map[string]bool{}
	for _, release := range releases {
		for _, name := range providerOrder {
			if rateLimitedProviders[name] || r.providerCooling(name) {
				continue
			}
			provider := providers[name]
			if provider == nil {
				continue
			}
			attempt, cancel := context.WithTimeout(ctx, cfg.ResolveTimeout)
			resolved, resolveErr := provider.Resolve(attempt, release)
			cancel()
			if resolveErr != nil {
				if delay, cooldown := debrid.ProviderCooldown(resolveErr); cooldown {
					rateLimitedProviders[name] = true
					r.markProviderCooldown(name, delay)
				}
				continue
			}
			target := *media
			if season > 0 {
				target.Seasons = []int{season}
			}
			for _, replacement := range r.materialize(&target, stale.Quality, provider.Name(), release, resolved) {
				// An open rclone file can already contain cached byte ranges.
				// Repair must preserve the torrent and size, not just the title.
				if !sameMediaFile(media.Type, stale.Path, replacement.Path) || replacement.Size != stale.Size {
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

func (r *Resolver) providerCooling(name string) bool {
	r.providerCooldownMu.Lock()
	defer r.providerCooldownMu.Unlock()
	if r.providerCooldowns == nil {
		return false
	}
	until := r.providerCooldowns[name]
	if until.IsZero() {
		return false
	}
	if !time.Now().Before(until) {
		delete(r.providerCooldowns, name)
		return false
	}
	return true
}

func (r *Resolver) markProviderCooldown(name string, delay time.Duration) {
	if delay <= 0 {
		delay = time.Minute
	}
	until := time.Now().Add(delay)
	r.providerCooldownMu.Lock()
	if r.providerCooldowns == nil {
		r.providerCooldowns = map[string]time.Time{}
	}
	if until.After(r.providerCooldowns[name]) {
		r.providerCooldowns[name] = until
	}
	r.providerCooldownMu.Unlock()
}

func (r *Resolver) allProvidersCooling(names []string, providers map[string]debrid.Provider) (time.Duration, bool) {
	available := 0
	longest := time.Duration(0)
	for _, name := range names {
		if providers[name] == nil {
			continue
		}
		available++
		r.providerCooldownMu.Lock()
		until := time.Time{}
		if r.providerCooldowns != nil {
			until = r.providerCooldowns[name]
		}
		if !until.IsZero() && !time.Now().Before(until) {
			delete(r.providerCooldowns, name)
			until = time.Time{}
		}
		r.providerCooldownMu.Unlock()
		if until.IsZero() {
			return 0, false
		}
		if delay := time.Until(until); delay > longest {
			longest = delay
		}
	}
	return longest, available > 0
}

func formatProviderCooldown(delay time.Duration) string {
	if rounded := delay.Round(time.Second); rounded > 0 {
		return rounded.String()
	}
	return delay.String()
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

func releasePreferred(a, b model.Release, quality string, season int) bool {
	aQuality := matchesQuality(a.Title, quality)
	bQuality := matchesQuality(b.Title, quality)
	if aQuality != bQuality {
		return aQuality
	}
	aPack := isSeasonPack(a.Title, season)
	bPack := isSeasonPack(b.Title, season)
	if aPack != bPack {
		return aPack
	}
	return releaseScore(a, quality) > releaseScore(b, quality)
}

func isSeasonPack(title string, wantedSeason int) bool {
	if wantedSeason <= 0 {
		return false
	}
	if match := episodeRangeRE.FindStringSubmatch(title); len(match) == 2 {
		season, _ := strconv.Atoi(match[1])
		return season == wantedSeason
	}
	if episodeRE.MatchString(title) {
		return false
	}
	for _, re := range []*regexp.Regexp{seasonOnlyRE, seasonWordRE} {
		if match := re.FindStringSubmatch(title); len(match) == 2 {
			season, _ := strconv.Atoi(match[1])
			if season == wantedSeason {
				return true
			}
		}
	}
	return false
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
			match := remoteEpisodeMatch(rf.Name)
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

func remoteEpisodeMatch(name string) []string {
	// A pack directory can itself contain S01E01-E10. Prefer the actual
	// file's episode marker, then the nearest enclosing directory.
	parts := strings.Split(strings.ReplaceAll(name, "\\", "/"), "/")
	for i := len(parts) - 1; i >= 0; i-- {
		if match := episodeRE.FindStringSubmatch(parts[i]); len(match) == 3 {
			return match
		}
	}
	return nil
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
