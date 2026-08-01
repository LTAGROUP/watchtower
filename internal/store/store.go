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

type Store struct {
	mu    sync.RWMutex
	path  string
	state model.State
}

func Open(path string) (*Store, error) {
	s := &Store{path: path, state: model.State{Media: map[int64]*model.Media{}, Files: map[string]*model.File{}, ProcessedRequests: map[int64]time.Time{}}}
	migrated := false
	b, err := os.ReadFile(path)
	if err == nil {
		if err = json.Unmarshal(b, &s.state); err != nil {
			return nil, err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if s.state.Media == nil {
		s.state.Media = map[int64]*model.Media{}
	}
	if s.state.Files == nil {
		s.state.Files = map[string]*model.File{}
	}
	if s.state.ProcessedRequests == nil {
		s.state.ProcessedRequests = map[int64]time.Time{}
	}
	// Older state files were written while several Media fields shared the same
	// JSON tag. The map key retained the canonical media ID even though the ID
	// inside the value was omitted. Repair those records when they are loaded so
	// dashboard actions can address them again.
	for id, media := range s.state.Media {
		if media == nil {
			continue
		}
		if media.ID == 0 {
			media.ID = id
			migrated = true
		}
		if media.Status == "" {
			media.Status = "queued"
			for _, file := range s.state.Files {
				if file.MediaID == id {
					media.Status = "ready"
					break
				}
			}
			migrated = true
		}
	}
	if s.deduplicateMediaLocked() {
		migrated = true
	}
	if migrated {
		if err := s.saveLocked(); err != nil {
			return nil, fmt.Errorf("migrate state: %w", err)
		}
	}
	return s, nil
}

func (s *Store) saveLocked() error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0750); err != nil {
		return err
	}
	b, err := json.MarshalIndent(s.state, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err = os.WriteFile(tmp, b, 0600); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}
func (s *Store) IsProcessed(id int64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.state.ProcessedRequests[id]
	return ok
}
func (s *Store) UpsertMedia(m *model.Media) error {
	if m == nil {
		return errors.New("media is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mergeMediaIdentityLocked(m)
	s.state.Media[m.ID] = m
	return s.saveLocked()
}

func (s *Store) mergeMediaIdentityLocked(incoming *model.Media) {
	if incoming.Type == "" || incoming.TMDBID <= 0 {
		return
	}
	ids := []int64{incoming.ID}
	for id, existing := range s.state.Media {
		if id != incoming.ID && sameMediaIdentity(existing, incoming) {
			ids = append(ids, id)
		}
	}
	if len(ids) == 1 {
		return
	}
	canonical := s.bestMediaIDLocked(ids)
	for _, id := range ids {
		if existing := s.state.Media[id]; existing != nil && existing != incoming {
			mergeMediaMetadata(incoming, existing)
		}
	}
	incoming.ID = canonical
	s.reassignMediaFilesLocked(canonical, ids)
	for _, id := range ids {
		if id != canonical {
			delete(s.state.Media, id)
		}
	}
}

func (s *Store) deduplicateMediaLocked() bool {
	groups := map[string][]int64{}
	for id, media := range s.state.Media {
		if key := mediaIdentityKey(media); key != "" {
			groups[key] = append(groups[key], id)
		}
	}
	changed := false
	for _, ids := range groups {
		if len(ids) < 2 {
			continue
		}
		canonical := s.bestMediaIDLocked(ids)
		primary := s.state.Media[canonical]
		for _, id := range ids {
			if id != canonical {
				mergeMediaMetadata(primary, s.state.Media[id])
			}
		}
		s.reassignMediaFilesLocked(canonical, ids)
		for _, id := range ids {
			if id != canonical {
				delete(s.state.Media, id)
			}
		}
		changed = true
	}
	return changed
}

func (s *Store) bestMediaIDLocked(ids []int64) int64 {
	best := ids[0]
	for _, id := range ids[1:] {
		if s.mediaIDPreferredLocked(id, best) {
			best = id
		}
	}
	return best
}

func (s *Store) mediaIDPreferredLocked(candidate, current int64) bool {
	candidateFiles, currentFiles := 0, 0
	for _, file := range s.state.Files {
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
	if media := s.state.Media[candidate]; media != nil {
		candidateStatus = media.Status
	}
	if media := s.state.Media[current]; media != nil {
		currentStatus = media.Status
	}
	if mediaStatusRank(candidateStatus) != mediaStatusRank(currentStatus) {
		return mediaStatusRank(candidateStatus) > mediaStatusRank(currentStatus)
	}
	return candidate < current
}

func (s *Store) reassignMediaFilesLocked(canonical int64, ids []int64) {
	owned := map[int64]bool{}
	for _, id := range ids {
		owned[id] = true
	}
	type fileRef struct {
		id   string
		file *model.File
	}
	refs := make([]fileRef, 0)
	for id, file := range s.state.Files {
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
			delete(s.state.Files, ref.id)
			continue
		}
		if path != "" {
			paths[path] = true
		}
		ref.file.MediaID = canonical
	}
}

func sameMediaIdentity(a, b *model.Media) bool {
	return mediaIdentityKey(a) != "" && mediaIdentityKey(a) == mediaIdentityKey(b)
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
func (s *Store) AddFiles(files ...*model.File) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, f := range files {
		s.state.Files[f.ID] = f
	}
	return s.saveLocked()
}

func (s *Store) ReplaceFilesForMedia(mediaID int64, files ...*model.File) error {
	for _, f := range files {
		if f == nil || f.MediaID != mediaID {
			return fmt.Errorf("replacement file does not belong to media %d", mediaID)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for fileID, f := range s.state.Files {
		if f.MediaID == mediaID {
			delete(s.state.Files, fileID)
		}
	}
	for _, f := range files {
		s.state.Files[f.ID] = f
	}
	return s.saveLocked()
}
func (s *Store) MarkProcessed(id int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state.ProcessedRequests[id] = time.Now().UTC()
	return s.saveLocked()
}
func (s *Store) File(id string) (*model.File, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	f, ok := s.state.Files[id]
	return f, ok
}
func (s *Store) MediaByID(id int64) (*model.Media, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m, ok := s.state.Media[id]
	if !ok || m == nil {
		return nil, false
	}
	copy := *m
	copy.Seasons = append([]int(nil), m.Seasons...)
	copy.EpisodeCounts = copyEpisodeCounts(m.EpisodeCounts)
	return &copy, true
}
func (s *Store) FindMediaByTMDB(kind string, tmdbID int64) (*model.Media, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, m := range s.state.Media {
		if m != nil && m.Type == kind && m.TMDBID == tmdbID {
			copy := *m
			copy.Seasons = append([]int(nil), m.Seasons...)
			copy.EpisodeCounts = copyEpisodeCounts(m.EpisodeCounts)
			return &copy, true
		}
	}
	return nil, false
}
func (s *Store) SetStream(id, u string, exp time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if f := s.state.Files[id]; f != nil {
		f.StreamURL = u
		f.StreamExpiresAt = exp
	}
}
func (s *Store) Files() []*model.File {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*model.File, 0, len(s.state.Files))
	for _, f := range s.state.Files {
		out = append(out, f)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out
}
func (s *Store) FilesForMedia(mediaID int64) []*model.File {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*model.File, 0)
	for _, f := range s.state.Files {
		if f.MediaID == mediaID {
			copy := *f
			out = append(out, &copy)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out
}
func (s *Store) FindPath(path string) (*model.File, bool) {
	path = strings.Trim(path, "/")
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, f := range s.state.Files {
		if strings.Trim(f.Path, "/") == path {
			return f, true
		}
	}
	return nil, false
}
func (s *Store) Media() []*model.Media {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*model.Media, 0, len(s.state.Media))
	for _, m := range s.state.Media {
		out = append(out, m)
	}
	return out
}

func (s *Store) ResetMedia(id int64) (*model.Media, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.state.Media[id]
	if m == nil {
		return nil, os.ErrNotExist
	}
	delete(s.state.ProcessedRequests, m.RequestID)
	m.Status = "queued"
	m.Error = ""
	m.ScrapedAt = time.Time{}
	m.UpdatedAt = time.Now().UTC()
	if err := s.saveLocked(); err != nil {
		return nil, err
	}
	copy := *m
	copy.Seasons = append([]int(nil), m.Seasons...)
	copy.EpisodeCounts = copyEpisodeCounts(m.EpisodeCounts)
	return &copy, nil
}

func copyEpisodeCounts(counts map[int]int) map[int]int {
	if len(counts) == 0 {
		return nil
	}
	copy := make(map[int]int, len(counts))
	for season, count := range counts {
		copy[season] = count
	}
	return copy
}

func (s *Store) DeleteMedia(id int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state.Media[id] == nil {
		return os.ErrNotExist
	}
	delete(s.state.Media, id)
	for fileID, f := range s.state.Files {
		if f.MediaID == id {
			delete(s.state.Files, fileID)
		}
	}
	// Keep the processed Seerr request marker. Otherwise an approved request
	// would be imported again on the next poll immediately after deletion.
	return s.saveLocked()
}

func (s *Store) ReplaceFileSource(id string, replacement *model.File) (*model.File, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.state.Files[id]
	if current == nil {
		return nil, os.ErrNotExist
	}
	current.Provider = replacement.Provider
	current.SourceURI = replacement.SourceURI
	current.InfoHash = replacement.InfoHash
	current.ProviderItemID = replacement.ProviderItemID
	current.ProviderFileID = replacement.ProviderFileID
	current.Size = replacement.Size
	current.StreamURL = ""
	current.StreamExpiresAt = time.Time{}
	if err := s.saveLocked(); err != nil {
		return nil, err
	}
	copy := *current
	return &copy, nil
}
