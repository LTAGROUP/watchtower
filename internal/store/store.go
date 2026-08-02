package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/LTAGROUP/watchtower/internal/model"
)

// Mutation groups the durable changes that must be published together. Files are
// used only when ReplaceFilesForMedia is non-zero; an empty Files slice then
// removes every file for that media item.
type Mutation struct {
	Media                *model.Media
	ReplaceFilesForMedia int64
	Files                []*model.File
	MarkProcessed        []int64
	UnmarkProcessed      []int64
}

// MediaTransaction stages file and processed-request changes alongside an
// UpdateMediaAtomic callback. Its readers return detached snapshots and its
// mutators only change the transaction, never the Store-owned candidate.
type MediaTransaction struct {
	mediaID         int64
	files           []*model.File
	processed       map[int64]time.Time
	replaceFiles    bool
	replacement     []*model.File
	markProcessed   []int64
	unmarkProcessed []int64
}

type mediaTransactionChange struct {
	replaceFiles    bool
	files           []*model.File
	markProcessed   []int64
	unmarkProcessed []int64
}

func newMediaTransaction(state *model.State, mediaID int64) *MediaTransaction {
	tx := &MediaTransaction{
		mediaID:   mediaID,
		processed: make(map[int64]time.Time, len(state.ProcessedRequests)),
	}
	for id, processedAt := range state.ProcessedRequests {
		tx.processed[id] = processedAt
	}
	for _, file := range state.Files {
		if file != nil && file.MediaID == mediaID {
			tx.files = append(tx.files, cloneFile(file))
		}
	}
	sort.Slice(tx.files, func(i, j int) bool { return tx.files[i].Path < tx.files[j].Path })
	return tx
}

// FilesForMedia returns a detached snapshot of the files that belonged to the
// media item when the atomic callback began.
func (tx *MediaTransaction) FilesForMedia() []*model.File {
	if tx == nil || len(tx.files) == 0 {
		return nil
	}
	files := make([]*model.File, len(tx.files))
	for i, file := range tx.files {
		files[i] = cloneFile(file)
	}
	return files
}

// IsProcessed reports the processed-marker state observed when the atomic
// callback began.
func (tx *MediaTransaction) IsProcessed(id int64) bool {
	if tx == nil {
		return false
	}
	_, ok := tx.processed[id]
	return ok
}

// ReplaceFiles stages a complete replacement for this media item's files.
// The supplied files are copied immediately and must belong to the media item
// being updated. Calling it with no files intentionally clears the item.
func (tx *MediaTransaction) ReplaceFiles(files ...*model.File) error {
	if tx == nil {
		return errors.New("media transaction is required")
	}
	prepared, err := cloneFileInputs(files)
	if err != nil {
		return err
	}
	for _, file := range prepared {
		if file.MediaID != tx.mediaID {
			return fmt.Errorf("replacement file does not belong to media %d", tx.mediaID)
		}
	}
	tx.replaceFiles = true
	tx.replacement = prepared
	return nil
}

// MarkProcessed stages request IDs to mark during the same durable commit.
func (tx *MediaTransaction) MarkProcessed(ids ...int64) {
	if tx != nil {
		tx.markProcessed = append(tx.markProcessed, ids...)
	}
}

// UnmarkProcessed stages request IDs to remove during the same durable commit.
// An unmark wins when the same ID is also marked.
func (tx *MediaTransaction) UnmarkProcessed(ids ...int64) {
	if tx != nil {
		tx.unmarkProcessed = append(tx.unmarkProcessed, ids...)
	}
}

func (tx *MediaTransaction) prepared() (mediaTransactionChange, error) {
	if tx == nil {
		return mediaTransactionChange{}, errors.New("media transaction is required")
	}
	files, err := cloneFileInputs(tx.replacement)
	if err != nil {
		return mediaTransactionChange{}, err
	}
	return mediaTransactionChange{
		replaceFiles:    tx.replaceFiles,
		files:           files,
		markProcessed:   append([]int64(nil), tx.markProcessed...),
		unmarkProcessed: append([]int64(nil), tx.unmarkProcessed...),
	}, nil
}

// storageOps makes persistence failures testable without making the Store API
// depend on a filesystem abstraction. Production stores use the os package.
type storageOps struct {
	mkdirAll  func(string, os.FileMode) error
	writeFile func(string, []byte, os.FileMode) error
	rename    func(string, string) error
	remove    func(string) error
}

type Store struct {
	mu    sync.RWMutex
	path  string
	state model.State
	ops   storageOps
}

// ErrMediaActive reports that a delete was rejected because a media item still
// has durable or legacy in-progress resolution state.
var ErrMediaActive = errors.New("media has active resolution work")

func Open(path string) (*Store, error) {
	loaded := emptyState()
	b, err := os.ReadFile(path)
	if err == nil {
		if err = json.Unmarshal(b, &loaded); err != nil {
			return nil, err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	ensureStateMaps(&loaded)

	// Migrate a detached candidate. Open has not published the Store yet, but
	// using the same candidate-persistence path keeps migration failure from
	// replacing a readable legacy state file.
	candidate := cloneState(loaded)
	migrated := migrateState(&candidate)
	s := &Store{path: path, state: loaded, ops: defaultStorageOps()}
	if migrated {
		if err := s.saveStateLocked(candidate); err != nil {
			return nil, fmt.Errorf("migrate state: %w", err)
		}
		s.state = candidate
	}
	return s, nil
}

func emptyState() model.State {
	return model.State{
		Media:             map[int64]*model.Media{},
		Files:             map[string]*model.File{},
		ProcessedRequests: map[int64]time.Time{},
	}
}

func ensureStateMaps(state *model.State) {
	if state.Media == nil {
		state.Media = map[int64]*model.Media{}
	}
	if state.Files == nil {
		state.Files = map[string]*model.File{}
	}
	if state.ProcessedRequests == nil {
		state.ProcessedRequests = map[int64]time.Time{}
	}
}

func migrateState(state *model.State) bool {
	migrated := false
	// Older state files were written while several Media fields shared the same
	// JSON tag. The map key retained the canonical media ID even though the ID
	// inside the value was omitted. Repair those records when they are loaded so
	// dashboard actions can address them again.
	for id, media := range state.Media {
		if media == nil {
			continue
		}
		if media.ID == 0 {
			media.ID = id
			migrated = true
		}
		if media.Status == "" {
			media.Status = "queued"
			for _, file := range state.Files {
				if file != nil && file.MediaID == id {
					media.Status = "ready"
					break
				}
			}
			migrated = true
		}
	}
	if deduplicateMediaState(state) {
		migrated = true
	}
	return migrated
}

func defaultStorageOps() storageOps {
	return storageOps{
		mkdirAll:  os.MkdirAll,
		writeFile: os.WriteFile,
		rename:    os.Rename,
		remove:    os.Remove,
	}
}

func (s *Store) saveOps() storageOps {
	ops := s.ops
	defaults := defaultStorageOps()
	if ops.mkdirAll == nil {
		ops.mkdirAll = defaults.mkdirAll
	}
	if ops.writeFile == nil {
		ops.writeFile = defaults.writeFile
	}
	if ops.rename == nil {
		ops.rename = defaults.rename
	}
	if ops.remove == nil {
		ops.remove = defaults.remove
	}
	return ops
}

// saveStateLocked writes a complete detached state before its caller makes it
// visible in memory. The caller must hold s.mu when the Store is already live.
func (s *Store) saveStateLocked(candidate model.State) error {
	ops := s.saveOps()
	if err := ops.mkdirAll(filepath.Dir(s.path), 0750); err != nil {
		return err
	}
	b, err := json.MarshalIndent(candidate, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err = ops.writeFile(tmp, b, 0600); err != nil {
		_ = ops.remove(tmp)
		return err
	}
	if err = ops.rename(tmp, s.path); err != nil {
		_ = ops.remove(tmp)
		return err
	}
	return nil
}

// commitStateLocked applies a mutation to a deep-copied state. A failed save
// discards the candidate, leaving both s.state and the durable target intact.
func (s *Store) commitStateLocked(apply func(*model.State) error) error {
	candidate := cloneState(s.state)
	if err := apply(&candidate); err != nil {
		return err
	}
	if err := s.saveStateLocked(candidate); err != nil {
		return err
	}
	s.state = candidate
	return nil
}

func (s *Store) IsProcessed(id int64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.state.ProcessedRequests[id]
	return ok
}

// Commit atomically applies every requested change and persists exactly one
// candidate state. If Media is supplied, its canonical stored representation is
// returned as a detached copy.
func (s *Store) Commit(mutation Mutation) (*model.Media, error) {
	prepared, err := cloneMutation(mutation)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var mediaID int64
	err = s.commitStateLocked(func(candidate *model.State) error {
		// Replace before the media identity merge. If the incoming media is
		// deduplicated to another ID, reassignMediaFilesState moves these files
		// with it instead of leaving them attached to the transient ID.
		if prepared.ReplaceFilesForMedia != 0 {
			replaceFilesForMediaState(candidate, prepared.ReplaceFilesForMedia, prepared.Files)
		}
		if prepared.Media != nil {
			stored := cloneMedia(prepared.Media)
			upsertMediaState(candidate, stored)
			mediaID = stored.ID
		}
		if len(prepared.MarkProcessed) > 0 {
			now := time.Now().UTC()
			for _, id := range prepared.MarkProcessed {
				candidate.ProcessedRequests[id] = now
			}
		}
		// An explicit unmark wins when the same ID is listed in both fields.
		for _, id := range prepared.UnmarkProcessed {
			delete(candidate.ProcessedRequests, id)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if prepared.Media == nil {
		return nil, nil
	}
	return cloneMedia(s.state.Media[mediaID]), nil
}

func cloneMutation(mutation Mutation) (Mutation, error) {
	prepared := Mutation{
		Media:                cloneMedia(mutation.Media),
		ReplaceFilesForMedia: mutation.ReplaceFilesForMedia,
		MarkProcessed:        append([]int64(nil), mutation.MarkProcessed...),
		UnmarkProcessed:      append([]int64(nil), mutation.UnmarkProcessed...),
	}
	if len(mutation.Files) > 0 {
		prepared.Files = make([]*model.File, len(mutation.Files))
		for i, file := range mutation.Files {
			if file == nil {
				return Mutation{}, errors.New("file is required")
			}
			prepared.Files[i] = cloneFile(file)
		}
	}
	if prepared.ReplaceFilesForMedia == 0 {
		if len(prepared.Files) > 0 {
			return Mutation{}, errors.New("files require ReplaceFilesForMedia")
		}
		return prepared, nil
	}
	for _, file := range prepared.Files {
		if file.MediaID != prepared.ReplaceFilesForMedia {
			return Mutation{}, fmt.Errorf("replacement file does not belong to media %d", prepared.ReplaceFilesForMedia)
		}
	}
	return prepared, nil
}

func (s *Store) UpsertMedia(media *model.Media) error {
	if media == nil {
		return errors.New("media is required")
	}
	stored, err := s.Commit(Mutation{Media: media})
	if err != nil {
		return err
	}
	// The old API updated the supplied record when identity de-duplication
	// selected a canonical ID. Preserve that success-path behavior without ever
	// retaining the caller's pointer in Store state.
	if stored != nil {
		copy := cloneMedia(stored)
		*media = *copy
	}
	return nil
}

// UpdateMediaAtomic invokes update with detached media and transaction
// snapshots while s.mu is held. The media and staged transaction inputs are
// cloned again before publication, so retaining either after the callback
// cannot mutate Store-owned state.
func (s *Store) UpdateMediaAtomic(id int64, update func(*model.Media, *MediaTransaction) error) (*model.Media, error) {
	if update == nil {
		return nil, errors.New("media update is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var mediaID int64
	err := s.commitStateLocked(func(candidate *model.State) error {
		current := candidate.Media[id]
		if current == nil {
			return os.ErrNotExist
		}
		editable := cloneMedia(current)
		transaction := newMediaTransaction(candidate, id)
		if err := update(editable, transaction); err != nil {
			return err
		}
		change, err := transaction.prepared()
		if err != nil {
			return err
		}
		// Replace before the media identity merge. If the callback's update is
		// deduplicated to another ID, reassignMediaFilesState moves these files
		// with that canonical record.
		if change.replaceFiles {
			replaceFilesForMediaState(candidate, id, change.files)
		}
		// A State map key is the media's stable local ID. Identity changes are
		// still allowed and may merge this record into another canonical key.
		updated := cloneMedia(editable)
		updated.ID = id
		upsertMediaState(candidate, updated)
		mediaID = updated.ID
		if len(change.markProcessed) > 0 {
			now := time.Now().UTC()
			for _, processedID := range change.markProcessed {
				candidate.ProcessedRequests[processedID] = now
			}
		}
		// An explicit unmark wins when the same ID is listed in both fields.
		for _, processedID := range change.unmarkProcessed {
			delete(candidate.ProcessedRequests, processedID)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return cloneMedia(s.state.Media[mediaID]), nil
}

// UpdateMedia invokes update with a detached copy while s.mu is held. The
// callback's pointer is cloned again before publication so retaining it cannot
// mutate Store-owned state after UpdateMedia returns.
func (s *Store) UpdateMedia(id int64, update func(*model.Media) error) (*model.Media, error) {
	if update == nil {
		return nil, errors.New("media update is required")
	}
	return s.UpdateMediaAtomic(id, func(media *model.Media, _ *MediaTransaction) error {
		return update(media)
	})
}

func (s *Store) AddFiles(files ...*model.File) error {
	prepared, err := cloneFileInputs(files)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.commitStateLocked(func(candidate *model.State) error {
		for _, file := range prepared {
			putFileState(candidate, file, nil)
		}
		return nil
	})
}

func (s *Store) ReplaceFilesForMedia(mediaID int64, files ...*model.File) error {
	prepared, err := cloneFileInputs(files)
	if err != nil {
		return err
	}
	for _, file := range prepared {
		if file.MediaID != mediaID {
			return fmt.Errorf("replacement file does not belong to media %d", mediaID)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.commitStateLocked(func(candidate *model.State) error {
		replaceFilesForMediaState(candidate, mediaID, prepared)
		return nil
	})
}

func cloneFileInputs(files []*model.File) ([]*model.File, error) {
	if len(files) == 0 {
		return nil, nil
	}
	cloned := make([]*model.File, len(files))
	for i, file := range files {
		if file == nil {
			return nil, errors.New("file is required")
		}
		cloned[i] = cloneFile(file)
	}
	return cloned, nil
}

func (s *Store) MarkProcessed(id int64) error {
	_, err := s.Commit(Mutation{MarkProcessed: []int64{id}})
	return err
}

func (s *Store) File(id string) (*model.File, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	file, ok := s.state.Files[id]
	if !ok || file == nil {
		return nil, false
	}
	return cloneFile(file), true
}

func (s *Store) MediaByID(id int64) (*model.Media, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	media, ok := s.state.Media[id]
	if !ok || media == nil {
		return nil, false
	}
	return cloneMedia(media), true
}

func (s *Store) FindMediaByTMDB(kind string, tmdbID int64) (*model.Media, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, media := range s.state.Media {
		if media != nil && media.Type == kind && media.TMDBID == tmdbID {
			return cloneMedia(media), true
		}
	}
	return nil, false
}

// SetStream updates only the in-memory stream cache. cloneState preserves this
// cache across later durable commits even though File's JSON tags omit it.
func (s *Store) SetStream(id, url string, expiresAt time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if file := s.state.Files[id]; file != nil {
		file.StreamURL = url
		file.StreamExpiresAt = expiresAt
	}
}

func (s *Store) Files() []*model.File {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*model.File, 0, len(s.state.Files))
	for _, file := range s.state.Files {
		if file != nil {
			out = append(out, cloneFile(file))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out
}

func (s *Store) FilesForMedia(mediaID int64) []*model.File {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*model.File, 0)
	for _, file := range s.state.Files {
		if file != nil && file.MediaID == mediaID {
			out = append(out, cloneFile(file))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out
}

func (s *Store) FindPath(path string) (*model.File, bool) {
	path = strings.Trim(path, "/")
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, file := range s.state.Files {
		if file != nil && strings.Trim(file.Path, "/") == path {
			return cloneFile(file), true
		}
	}
	return nil, false
}

func (s *Store) Media() []*model.Media {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*model.Media, 0, len(s.state.Media))
	for _, media := range s.state.Media {
		if media != nil {
			out = append(out, cloneMedia(media))
		}
	}
	return out
}

func (s *Store) ResetMedia(id int64) (*model.Media, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.commitStateLocked(func(candidate *model.State) error {
		media := candidate.Media[id]
		if media == nil {
			return os.ErrNotExist
		}
		delete(candidate.ProcessedRequests, media.RequestID)
		media.Status = "queued"
		media.Error = ""
		media.ScrapedAt = time.Time{}
		media.UpdatedAt = time.Now().UTC()
		return nil
	})
	if err != nil {
		return nil, err
	}
	return cloneMedia(s.state.Media[id]), nil
}

func (s *Store) DeleteMedia(id int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.commitStateLocked(func(candidate *model.State) error {
		if candidate.Media[id] == nil {
			return os.ErrNotExist
		}
		deleteMediaState(candidate, id)
		return nil
	})
}

// DeleteInactiveMedia removes a media item and its files only when it has no
// active durable work or legacy in-progress status. The activity check and
// deletion share one candidate save, so a newly queued item cannot be deleted
// through a stale dashboard read.
func (s *Store) DeleteInactiveMedia(id int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.commitStateLocked(func(candidate *model.State) error {
		media := candidate.Media[id]
		if media == nil {
			return os.ErrNotExist
		}
		if mediaHasActiveWork(media) {
			return ErrMediaActive
		}
		deleteMediaState(candidate, id)
		return nil
	})
}

func mediaHasActiveWork(media *model.Media) bool {
	if media == nil {
		return false
	}
	if media.Work.Mode != "" {
		return true
	}
	switch media.Status {
	case "queued", "scraping", "resolving":
		return true
	default:
		return false
	}
}

func deleteMediaState(state *model.State, id int64) {
	delete(state.Media, id)
	for fileID, file := range state.Files {
		if file != nil && file.MediaID == id {
			delete(state.Files, fileID)
		}
	}
	// Keep processed Seerr request markers. Otherwise approved requests could
	// be imported again immediately after a dashboard deletion.
}

func (s *Store) ReplaceFileSource(id string, replacement *model.File) (*model.File, error) {
	if replacement == nil {
		return nil, errors.New("replacement file is required")
	}
	prepared := cloneFile(replacement)
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.commitStateLocked(func(candidate *model.State) error {
		current := candidate.Files[id]
		if current == nil {
			return os.ErrNotExist
		}
		current.Provider = prepared.Provider
		current.SourceURI = prepared.SourceURI
		current.InfoHash = prepared.InfoHash
		current.ProviderItemID = prepared.ProviderItemID
		current.ProviderFileID = prepared.ProviderFileID
		current.Size = prepared.Size
		// A changed source invalidates the previous provider stream URL.
		current.StreamURL = ""
		current.StreamExpiresAt = time.Time{}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return cloneFile(s.state.Files[id]), nil
}

func cloneState(state model.State) model.State {
	cloned := emptyState()
	for id, media := range state.Media {
		cloned.Media[id] = cloneMedia(media)
	}
	for id, file := range state.Files {
		cloned.Files[id] = cloneFile(file)
	}
	for id, processedAt := range state.ProcessedRequests {
		cloned.ProcessedRequests[id] = processedAt
	}
	return cloned
}

// cloneMedia begins with a value copy so future value fields are included
// automatically, then explicitly detaches the known reference fields.
func cloneMedia(media *model.Media) *model.Media {
	if media == nil {
		return nil
	}
	cloned := *media
	if media.RequestIDs != nil {
		cloned.RequestIDs = make([]int64, len(media.RequestIDs))
		copy(cloned.RequestIDs, media.RequestIDs)
	}
	if media.Seasons != nil {
		cloned.Seasons = make([]int, len(media.Seasons))
		copy(cloned.Seasons, media.Seasons)
	}
	cloned.EpisodeCounts = copyEpisodeCounts(media.EpisodeCounts)
	if media.EpisodeAirDates != nil {
		cloned.EpisodeAirDates = make([]model.EpisodeAirDate, len(media.EpisodeAirDates))
		copy(cloned.EpisodeAirDates, media.EpisodeAirDates)
	}
	return &cloned
}

func cloneFile(file *model.File) *model.File {
	if file == nil {
		return nil
	}
	cloned := *file
	return &cloned
}

func copyEpisodeCounts(counts map[int]int) map[int]int {
	if counts == nil {
		return nil
	}
	cloned := make(map[int]int, len(counts))
	for season, count := range counts {
		cloned[season] = count
	}
	return cloned
}

func putFileState(state *model.State, incoming, previous *model.File) {
	next := cloneFile(incoming)
	if previous == nil {
		previous = state.Files[next.ID]
	}
	if sameDurableFile(previous, next) && next.StreamURL == "" && next.StreamExpiresAt.IsZero() {
		next.StreamURL = previous.StreamURL
		next.StreamExpiresAt = previous.StreamExpiresAt
	}
	state.Files[next.ID] = next
}

func replaceFilesForMediaState(state *model.State, mediaID int64, files []*model.File) {
	previous := map[string]*model.File{}
	for fileID, file := range state.Files {
		if file != nil && file.MediaID == mediaID {
			previous[fileID] = file
			delete(state.Files, fileID)
		}
	}
	for _, file := range files {
		old := previous[file.ID]
		if old == nil {
			old = state.Files[file.ID]
		}
		putFileState(state, file, old)
	}
}

func sameDurableFile(left, right *model.File) bool {
	if left == nil || right == nil {
		return false
	}
	return left.ID == right.ID &&
		left.MediaID == right.MediaID &&
		left.Path == right.Path &&
		left.Quality == right.Quality &&
		left.Provider == right.Provider &&
		left.SourceURI == right.SourceURI &&
		left.InfoHash == right.InfoHash &&
		left.ProviderItemID == right.ProviderItemID &&
		left.ProviderFileID == right.ProviderFileID &&
		left.Size == right.Size &&
		left.CreatedAt.Equal(right.CreatedAt)
}

func upsertMediaState(state *model.State, incoming *model.Media) {
	mergeMediaIdentityState(state, incoming)
	state.Media[incoming.ID] = incoming
}

func mergeMediaIdentityState(state *model.State, incoming *model.Media) {
	if incoming.Type == "" || incoming.TMDBID <= 0 {
		return
	}
	ids := []int64{incoming.ID}
	for id, existing := range state.Media {
		if id != incoming.ID && sameMediaIdentity(existing, incoming) {
			ids = append(ids, id)
		}
	}
	if len(ids) == 1 {
		return
	}
	canonical := bestMediaID(state, ids)
	for _, id := range ids {
		if existing := state.Media[id]; existing != nil && existing != incoming {
			mergeMediaMetadata(incoming, existing)
		}
	}
	incoming.ID = canonical
	reassignMediaFilesState(state, canonical, ids)
	for _, id := range ids {
		if id != canonical {
			delete(state.Media, id)
		}
	}
}

func deduplicateMediaState(state *model.State) bool {
	groups := map[string][]int64{}
	for id, media := range state.Media {
		if key := mediaIdentityKey(media); key != "" {
			groups[key] = append(groups[key], id)
		}
	}
	changed := false
	for _, ids := range groups {
		if len(ids) < 2 {
			continue
		}
		canonical := bestMediaID(state, ids)
		primary := state.Media[canonical]
		for _, id := range ids {
			if id != canonical {
				mergeMediaMetadata(primary, state.Media[id])
			}
		}
		reassignMediaFilesState(state, canonical, ids)
		for _, id := range ids {
			if id != canonical {
				delete(state.Media, id)
			}
		}
		changed = true
	}
	return changed
}

func bestMediaID(state *model.State, ids []int64) int64 {
	best := ids[0]
	for _, id := range ids[1:] {
		if mediaIDPreferred(state, id, best) {
			best = id
		}
	}
	return best
}

func mediaIDPreferred(state *model.State, candidate, current int64) bool {
	candidateFiles, currentFiles := 0, 0
	for _, file := range state.Files {
		if file == nil {
			continue
		}
		if file.MediaID == candidate {
			candidateFiles++
		}
		if file.MediaID == current {
			currentFiles++
		}
	}
	if candidateFiles != currentFiles {
		return candidateFiles > currentFiles
	}
	candidateStatus, currentStatus := "", ""
	if media := state.Media[candidate]; media != nil {
		candidateStatus = media.Status
	}
	if media := state.Media[current]; media != nil {
		currentStatus = media.Status
	}
	if mediaStatusRank(candidateStatus) != mediaStatusRank(currentStatus) {
		return mediaStatusRank(candidateStatus) > mediaStatusRank(currentStatus)
	}
	return candidate < current
}

func reassignMediaFilesState(state *model.State, canonical int64, ids []int64) {
	owned := map[int64]bool{}
	for _, id := range ids {
		owned[id] = true
	}
	type fileRef struct {
		id   string
		file *model.File
	}
	refs := make([]fileRef, 0)
	for id, file := range state.Files {
		if file != nil && owned[file.MediaID] {
			refs = append(refs, fileRef{id: id, file: file})
		}
	}
	sort.SliceStable(refs, func(i, j int) bool {
		iCanonical := refs[i].file.MediaID == canonical
		jCanonical := refs[j].file.MediaID == canonical
		if iCanonical != jCanonical {
			return iCanonical
		}
		return refs[i].id < refs[j].id
	})
	paths := map[string]bool{}
	for _, ref := range refs {
		path := strings.Trim(ref.file.Path, "/")
		if path != "" && paths[path] {
			delete(state.Files, ref.id)
			continue
		}
		if path != "" {
			paths[path] = true
		}
		ref.file.MediaID = canonical
	}
}

func sameMediaIdentity(left, right *model.Media) bool {
	return mediaIdentityKey(left) != "" && mediaIdentityKey(left) == mediaIdentityKey(right)
}

func mediaIdentityKey(media *model.Media) string {
	if media == nil || media.TMDBID <= 0 || strings.TrimSpace(media.Type) == "" {
		return ""
	}
	return strings.ToLower(strings.TrimSpace(media.Type)) + ":" + fmt.Sprint(media.TMDBID)
}

func mergeMediaMetadata(dst, src *model.Media) {
	if dst == nil || src == nil {
		return
	}
	dstUpdatedAt, srcUpdatedAt := dst.UpdatedAt, src.UpdatedAt
	dstStatus, dstError := dst.Status, dst.Error
	if dst.Type == "" {
		dst.Type = src.Type
	}
	if dst.TMDBID == 0 {
		dst.TMDBID = src.TMDBID
	}
	if dst.ExternalID == "" {
		dst.ExternalID = src.ExternalID
	}
	if dst.Title == "" {
		dst.Title = src.Title
	}
	if dst.Year == 0 {
		dst.Year = src.Year
	}
	if dst.Overview == "" {
		dst.Overview = src.Overview
	}
	if dst.PosterPath == "" {
		dst.PosterPath = src.PosterPath
	}
	if dst.BackdropPath == "" {
		dst.BackdropPath = src.BackdropPath
	}
	if dst.ReleaseDate == "" {
		dst.ReleaseDate = src.ReleaseDate
	}
	if dst.RequestID == 0 {
		dst.RequestID = src.RequestID
	}
	if dst.SeerrMediaID == 0 {
		dst.SeerrMediaID = src.SeerrMediaID
	}
	dst.RequestIDs = mergeRequestIDs(dst.RequestIDs, src.RequestIDs, []int64{dst.RequestID, src.RequestID})
	if dst.Status == "" {
		dst.Status = src.Status
	}
	if dst.Error == "" && dst.Status == "failed" {
		dst.Error = src.Error
	}
	if dst.CreatedAt.IsZero() || (!src.CreatedAt.IsZero() && src.CreatedAt.Before(dst.CreatedAt)) {
		dst.CreatedAt = src.CreatedAt
	}
	if src.UpdatedAt.After(dst.UpdatedAt) {
		dst.UpdatedAt = src.UpdatedAt
	}
	if src.ScrapedAt.After(dst.ScrapedAt) {
		dst.ScrapedAt = src.ScrapedAt
	}
	seenSeasons := map[int]bool{}
	for _, season := range dst.Seasons {
		seenSeasons[season] = true
	}
	for _, season := range src.Seasons {
		if !seenSeasons[season] {
			dst.Seasons = append(dst.Seasons, season)
			seenSeasons[season] = true
		}
	}
	sort.Ints(dst.Seasons)
	if len(src.EpisodeCounts) > 0 {
		if dst.EpisodeCounts == nil {
			dst.EpisodeCounts = map[int]int{}
		}
		for season, count := range src.EpisodeCounts {
			if dst.EpisodeCounts[season] == 0 || count > dst.EpisodeCounts[season] {
				dst.EpisodeCounts[season] = count
			}
		}
	}
	dst.EpisodeAirDates = mergeEpisodeAirDates(dst.EpisodeAirDates, src.EpisodeAirDates)
	if mergeMediaWork(dst, src, dstUpdatedAt, srcUpdatedAt) {
		// Work is a single versioned command. Its status and error describe the
		// same command, so carry them together when its full state wins.
		if src.Status != "" {
			dst.Status = src.Status
		} else if src.Work.Mode != "" {
			dst.Status = "queued"
		}
		dst.Error = src.Error
	} else if dst.Work.Mode != "" {
		// Metadata fallback above may have filled an empty destination status
		// from an older command. Keep it coupled to the destination work that
		// survived instead.
		if dstStatus != "" {
			dst.Status = dstStatus
		} else {
			dst.Status = "queued"
		}
		dst.Error = dstError
	}
	dst.PlexIntent = mergeDurableIntent(dst.PlexIntent, src.PlexIntent, dstUpdatedAt, srcUpdatedAt)
	dst.AvailabilityIntent = mergeDurableIntent(dst.AvailabilityIntent, src.AvailabilityIntent, dstUpdatedAt, srcUpdatedAt)
}

func mergeRequestIDs(values ...[]int64) []int64 {
	seen := map[int64]bool{}
	merged := make([]int64, 0)
	for _, ids := range values {
		for _, id := range ids {
			if id > 0 && !seen[id] {
				seen[id] = true
				merged = append(merged, id)
			}
		}
	}
	sort.Slice(merged, func(i, j int) bool { return merged[i] < merged[j] })
	return merged
}

func mergeEpisodeAirDates(dst, src []model.EpisodeAirDate) []model.EpisodeAirDate {
	type episodeSlot struct {
		season  int
		episode int
	}
	bySlot := map[episodeSlot]model.EpisodeAirDate{}
	for _, dates := range [][]model.EpisodeAirDate{dst, src} {
		for _, date := range dates {
			slot := episodeSlot{season: date.Season, episode: date.Episode}
			if current, exists := bySlot[slot]; !exists || (current.AirDate == "" && date.AirDate != "") {
				bySlot[slot] = date
			}
		}
	}
	if len(bySlot) == 0 {
		return nil
	}
	merged := make([]model.EpisodeAirDate, 0, len(bySlot))
	for _, date := range bySlot {
		merged = append(merged, date)
	}
	sort.Slice(merged, func(i, j int) bool {
		if merged[i].Season != merged[j].Season {
			return merged[i].Season < merged[j].Season
		}
		return merged[i].Episode < merged[j].Episode
	})
	return merged
}

func mergeMediaWork(dst, src *model.Media, dstUpdatedAt, srcUpdatedAt time.Time) bool {
	if !mediaWorkPreferred(src.Work, dst.Work, srcUpdatedAt, dstUpdatedAt) {
		return false
	}
	dst.Work = src.Work
	return true
}

func mediaWorkPreferred(candidate, current model.MediaWork, candidateUpdatedAt, currentUpdatedAt time.Time) bool {
	if candidate.Generation != current.Generation {
		return candidate.Generation > current.Generation
	}
	candidateActive, currentActive := candidate.Mode != "", current.Mode != ""
	if candidateActive != currentActive {
		return candidateActive
	}
	if !candidateUpdatedAt.Equal(currentUpdatedAt) {
		return candidateUpdatedAt.After(currentUpdatedAt)
	}
	if candidate.Attempts != current.Attempts {
		return candidate.Attempts > current.Attempts
	}
	if !candidate.LeaseUntil.Equal(current.LeaseUntil) {
		return candidate.LeaseUntil.After(current.LeaseUntil)
	}
	if !candidate.NextAt.Equal(current.NextAt) {
		return candidate.NextAt.After(current.NextAt)
	}
	if candidate.Mode != current.Mode {
		return candidate.Mode > current.Mode
	}
	if candidate.Season != current.Season {
		return candidate.Season > current.Season
	}
	return candidate.Episode > current.Episode
}

func mergeDurableIntent(dst, src model.DurableIntent, dstUpdatedAt, srcUpdatedAt time.Time) model.DurableIntent {
	merged := dst
	if durableIntentPreferred(src, dst, srcUpdatedAt, dstUpdatedAt) {
		merged = src
	}
	if src.Generation > merged.Generation {
		merged.Generation = src.Generation
	}
	if dst.Generation > merged.Generation {
		merged.Generation = dst.Generation
	}
	if src.CompletedGeneration > merged.CompletedGeneration {
		merged.CompletedGeneration = src.CompletedGeneration
	}
	if dst.CompletedGeneration > merged.CompletedGeneration {
		merged.CompletedGeneration = dst.CompletedGeneration
	}
	if merged.CompletedGeneration > merged.Generation {
		merged.Generation = merged.CompletedGeneration
	}
	if merged.CompletedGeneration >= merged.Generation {
		merged.Attempts = 0
		merged.NextAt = time.Time{}
		merged.LeaseUntil = time.Time{}
		merged.LeaseGeneration = 0
		return merged
	}
	if merged.LeaseGeneration != merged.Generation || merged.LeaseUntil.IsZero() {
		merged.LeaseUntil = time.Time{}
		merged.LeaseGeneration = 0
	}
	return merged
}

func durableIntentPreferred(candidate, current model.DurableIntent, candidateUpdatedAt, currentUpdatedAt time.Time) bool {
	if candidate.Generation != current.Generation {
		return candidate.Generation > current.Generation
	}
	if !candidateUpdatedAt.Equal(currentUpdatedAt) {
		return candidateUpdatedAt.After(currentUpdatedAt)
	}
	if candidate.Attempts != current.Attempts {
		return candidate.Attempts > current.Attempts
	}
	if !candidate.NextAt.Equal(current.NextAt) {
		return candidate.NextAt.After(current.NextAt)
	}
	candidateLeased := candidate.LeaseGeneration == candidate.Generation && !candidate.LeaseUntil.IsZero()
	currentLeased := current.LeaseGeneration == current.Generation && !current.LeaseUntil.IsZero()
	if candidateLeased != currentLeased {
		return candidateLeased
	}
	if !candidate.LeaseUntil.Equal(current.LeaseUntil) {
		return candidate.LeaseUntil.After(current.LeaseUntil)
	}
	if candidate.CompletedGeneration != current.CompletedGeneration {
		return candidate.CompletedGeneration > current.CompletedGeneration
	}
	return candidate.LeaseGeneration > current.LeaseGeneration
}

func mediaStatusRank(status string) int {
	switch status {
	case "ready":
		return 6
	case "partial":
		return 5
	case "resolving":
		return 4
	case "scraping":
		return 3
	case "queued":
		return 2
	case "unreleased":
		return 1
	default:
		return 0
	}
}
