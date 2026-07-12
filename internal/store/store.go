package store

import (
	"encoding/json"
	"errors"
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
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state.Media[m.ID] = m
	return s.saveLocked()
}
func (s *Store) AddFiles(files ...*model.File) error {
	s.mu.Lock()
	defer s.mu.Unlock()
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
