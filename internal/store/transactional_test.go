package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/LTAGROUP/watchtower/internal/model"
)

func openTestStore(t *testing.T) (*Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "state.json")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	return s, path
}

func TestDurableWritersCopyCallerOwnedRecords(t *testing.T) {
	s, _ := openTestStore(t)
	media := &model.Media{
		ID:            1,
		Type:          "tv",
		TMDBID:        99,
		Title:         "Stored",
		Status:        "queued",
		RequestIDs:    []int64{11, 12},
		Seasons:       []int{1, 2},
		EpisodeCounts: map[int]int{1: 8, 2: 10},
		EpisodeAirDates: []model.EpisodeAirDate{
			{Season: 1, Episode: 1, AirDate: "2026-01-02"},
		},
	}
	if err := s.UpsertMedia(media); err != nil {
		t.Fatal(err)
	}
	media.Title = "Caller changed"
	media.RequestIDs[0] = 99
	media.Seasons[0] = 99
	media.EpisodeCounts[1] = 99
	media.EpisodeAirDates[0].AirDate = "caller changed"

	storedMedia, ok := s.MediaByID(1)
	if !ok {
		t.Fatal("stored media is missing")
	}
	if storedMedia.Title != "Stored" || storedMedia.RequestIDs[0] != 11 || storedMedia.Seasons[0] != 1 || storedMedia.EpisodeCounts[1] != 8 || storedMedia.EpisodeAirDates[0].AirDate != "2026-01-02" {
		t.Fatalf("store retained caller-owned media: %#v", storedMedia)
	}

	file := &model.File{ID: "file", MediaID: 1, Path: "TV/Stored.mkv", Provider: "one", Size: 1}
	if err := s.AddFiles(file); err != nil {
		t.Fatal(err)
	}
	file.Path = "Caller/Changed.mkv"
	file.Provider = "changed"
	file.Size = 2
	storedFile, ok := s.File("file")
	if !ok {
		t.Fatal("stored file is missing")
	}
	if storedFile.Path != "TV/Stored.mkv" || storedFile.Provider != "one" || storedFile.Size != 1 {
		t.Fatalf("store retained caller-owned file: %#v", storedFile)
	}

	replacement := &model.File{ID: "file", MediaID: 1, Path: "TV/Replaced.mkv", Provider: "two", Size: 3}
	if err := s.ReplaceFilesForMedia(1, replacement); err != nil {
		t.Fatal(err)
	}
	replacement.Path = "Caller/Replaced.mkv"
	replacement.Provider = "changed"
	replacement.Size = 4
	storedFile, _ = s.File("file")
	if storedFile.Path != "TV/Replaced.mkv" || storedFile.Provider != "two" || storedFile.Size != 3 {
		t.Fatalf("replacement retained caller-owned file: %#v", storedFile)
	}

	source := &model.File{Provider: "three", SourceURI: "magnet:?xt=urn:btih:abc", InfoHash: "abc", ProviderItemID: "item", ProviderFileID: "remote", Size: 5}
	if _, err := s.ReplaceFileSource("file", source); err != nil {
		t.Fatal(err)
	}
	source.Provider = "caller"
	source.SourceURI = "caller"
	source.InfoHash = "caller"
	source.Size = 6
	storedFile, _ = s.File("file")
	if storedFile.Provider != "three" || storedFile.SourceURI != "magnet:?xt=urn:btih:abc" || storedFile.InfoHash != "abc" || storedFile.Size != 5 {
		t.Fatalf("source replacement retained caller-owned file: %#v", storedFile)
	}
}

func TestReadersReturnDeepCopies(t *testing.T) {
	s, _ := openTestStore(t)
	media := &model.Media{
		ID:            1,
		Type:          "tv",
		TMDBID:        99,
		Title:         "Original",
		Status:        "ready",
		RequestIDs:    []int64{11},
		Seasons:       []int{1},
		EpisodeCounts: map[int]int{1: 8},
		EpisodeAirDates: []model.EpisodeAirDate{
			{Season: 1, Episode: 1, AirDate: "2026-01-02"},
		},
	}
	file := &model.File{ID: "file", MediaID: 1, Path: "TV/Original.mkv", Provider: "provider"}
	if err := s.UpsertMedia(media); err != nil {
		t.Fatal(err)
	}
	if err := s.AddFiles(file); err != nil {
		t.Fatal(err)
	}

	byID, ok := s.MediaByID(1)
	if !ok {
		t.Fatal("MediaByID did not return media")
	}
	byID.Title = "MediaByID changed"
	byID.RequestIDs[0] = 99
	byID.Seasons[0] = 9
	byID.EpisodeCounts[1] = 99
	byID.EpisodeAirDates[0].AirDate = "MediaByID changed"

	byTMDB, ok := s.FindMediaByTMDB("tv", 99)
	if !ok {
		t.Fatal("FindMediaByTMDB did not return media")
	}
	byTMDB.Title = "FindMediaByTMDB changed"
	byTMDB.RequestIDs[0] = 98
	byTMDB.Seasons[0] = 8
	byTMDB.EpisodeCounts[1] = 98
	byTMDB.EpisodeAirDates[0].Episode = 98

	allMedia := s.Media()
	if len(allMedia) != 1 {
		t.Fatalf("Media returned %d records", len(allMedia))
	}
	allMedia[0].Title = "Media changed"
	allMedia[0].RequestIDs[0] = 97
	allMedia[0].Seasons[0] = 7
	allMedia[0].EpisodeCounts[1] = 97
	allMedia[0].EpisodeAirDates[0].Season = 97

	byFile, ok := s.File("file")
	if !ok {
		t.Fatal("File did not return file")
	}
	byFile.Path = "File changed"
	byFile.Provider = "changed"

	allFiles := s.Files()
	if len(allFiles) != 1 {
		t.Fatalf("Files returned %d records", len(allFiles))
	}
	allFiles[0].Path = "Files changed"
	allFiles[0].Provider = "changed"

	mediaFiles := s.FilesForMedia(1)
	if len(mediaFiles) != 1 {
		t.Fatalf("FilesForMedia returned %d records", len(mediaFiles))
	}
	mediaFiles[0].Path = "FilesForMedia changed"
	mediaFiles[0].Provider = "changed"

	byPath, ok := s.FindPath("TV/Original.mkv")
	if !ok {
		t.Fatal("FindPath did not return file")
	}
	byPath.Path = "FindPath changed"
	byPath.Provider = "changed"

	storedMedia, _ := s.MediaByID(1)
	if storedMedia.Title != "Original" || storedMedia.RequestIDs[0] != 11 || storedMedia.Seasons[0] != 1 || storedMedia.EpisodeCounts[1] != 8 || storedMedia.EpisodeAirDates[0] != (model.EpisodeAirDate{Season: 1, Episode: 1, AirDate: "2026-01-02"}) {
		t.Fatalf("reader mutation reached stored media: %#v", storedMedia)
	}
	storedFile, _ := s.File("file")
	if storedFile.Path != "TV/Original.mkv" || storedFile.Provider != "provider" {
		t.Fatalf("reader mutation reached stored file: %#v", storedFile)
	}
}

func TestCommitAtomicallyMergesIdentityReplacesFilesAndPreservesStreamCache(t *testing.T) {
	s, path := openTestStore(t)
	if err := s.UpsertMedia(&model.Media{ID: 7, Type: "movie", TMDBID: 99, Title: "Existing", Status: "ready"}); err != nil {
		t.Fatal(err)
	}
	if err := s.AddFiles(
		&model.File{ID: "existing", MediaID: 7, Path: "Movies/Existing.mkv", Provider: "one"},
		&model.File{ID: "warm", MediaID: 77, Path: "Movies/Warm.mkv", Provider: "warm"},
	); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkProcessed(100); err != nil {
		t.Fatal(err)
	}
	expires := time.Now().Add(time.Hour).Round(0)
	s.SetStream("warm", "https://stream.example/warm", expires)

	incoming := &model.Media{
		ID:            8,
		Type:          "movie",
		TMDBID:        99,
		Title:         "Incoming",
		Status:        "queued",
		RequestIDs:    []int64{101},
		Seasons:       []int{1},
		EpisodeCounts: map[int]int{1: 1},
		EpisodeAirDates: []model.EpisodeAirDate{
			{Season: 1, Episode: 1, AirDate: "2026-01-02"},
		},
	}
	newFile := &model.File{ID: "new", MediaID: 8, Path: "Movies/New.mkv", Provider: "two"}
	committed, err := s.Commit(Mutation{
		Media:                incoming,
		ReplaceFilesForMedia: 8,
		Files:                []*model.File{newFile},
		MarkProcessed:        []int64{101},
		UnmarkProcessed:      []int64{100},
	})
	if err != nil {
		t.Fatal(err)
	}
	if committed == nil || committed.ID != 7 || committed.Title != "Incoming" {
		t.Fatalf("Commit did not return canonical stored media: %#v", committed)
	}
	if _, ok := s.MediaByID(8); ok {
		t.Fatal("duplicate media ID remained after Commit")
	}
	for _, id := range []string{"existing", "new"} {
		file, ok := s.File(id)
		if !ok || file.MediaID != 7 {
			t.Fatalf("Commit did not reassign %q to the canonical media: %#v", id, file)
		}
	}
	if s.IsProcessed(100) || !s.IsProcessed(101) {
		t.Fatal("Commit did not atomically apply processed-marker changes")
	}
	warm, ok := s.File("warm")
	if !ok || warm.StreamURL != "https://stream.example/warm" || !warm.StreamExpiresAt.Equal(expires) {
		t.Fatalf("durable Commit lost in-memory stream cache: %#v", warm)
	}

	incoming.Title = "caller changed"
	incoming.RequestIDs[0] = 999
	incoming.Seasons[0] = 9
	incoming.EpisodeCounts[1] = 9
	incoming.EpisodeAirDates[0].AirDate = "caller changed"
	newFile.Path = "caller changed"
	committed.Title = "returned changed"
	committed.RequestIDs[0] = 998
	committed.Seasons[0] = 8
	committed.EpisodeCounts[1] = 8
	committed.EpisodeAirDates[0].AirDate = "returned changed"
	stored, _ := s.MediaByID(7)
	if stored.Title != "Incoming" || stored.RequestIDs[0] != 101 || stored.Seasons[0] != 1 || stored.EpisodeCounts[1] != 1 || stored.EpisodeAirDates[0].AirDate != "2026-01-02" {
		t.Fatalf("Commit exposed stored media pointers: %#v", stored)
	}
	storedFile, _ := s.File("new")
	if storedFile.Path != "Movies/New.mkv" {
		t.Fatalf("Commit retained caller-owned replacement file: %#v", storedFile)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var durable model.State
	if err := json.Unmarshal(data, &durable); err != nil {
		t.Fatal(err)
	}
	if durable.Files["warm"].StreamURL != "" || !durable.Files["warm"].StreamExpiresAt.IsZero() {
		t.Fatalf("stream cache was persisted unexpectedly: %#v", durable.Files["warm"])
	}
	if durable.Media[7] == nil || durable.Media[8] != nil || durable.Files["new"].MediaID != 7 || durable.ProcessedRequests[101].IsZero() {
		t.Fatalf("durable state was not the complete committed candidate: %#v", durable)
	}
}

func TestCommitDuplicateIdentityMergesDurableLifecycleState(t *testing.T) {
	base := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	tests := []struct {
		name             string
		existing         *model.Media
		incoming         *model.Media
		incomingWins     bool
		wantID           int64
		wantStatus       string
		wantError        string
		wantWork         model.MediaWork
		wantPlex         model.DurableIntent
		wantAvailability model.DurableIntent
	}{
		{
			name: "existing ID wins while incoming lifecycle generation is newer",
			existing: &model.Media{
				ID:           20,
				RequestID:    501,
				RequestIDs:   []int64{501, 503},
				SeerrMediaID: 9001,
				Type:         "tv",
				TMDBID:       77,
				Status:       "resolving",
				Error:        "existing work retry",
				UpdatedAt:    base.Add(10 * time.Minute),
				Work:         model.MediaWork{Mode: "resolve", Generation: 5, Attempts: 2, NextAt: base.Add(20 * time.Minute), LeaseUntil: base.Add(30 * time.Minute)},
				EpisodeAirDates: []model.EpisodeAirDate{
					{Season: 1, Episode: 1, AirDate: "2026-01-01"},
					{Season: 2, Episode: 1, AirDate: "2026-02-01"},
				},
				PlexIntent:         model.DurableIntent{Generation: 7, CompletedGeneration: 2, Attempts: 3, NextAt: base.Add(40 * time.Minute), LeaseGeneration: 7, LeaseUntil: base.Add(50 * time.Minute)},
				AvailabilityIntent: model.DurableIntent{Generation: 4, CompletedGeneration: 2, Attempts: 4, NextAt: base.Add(60 * time.Minute), LeaseGeneration: 4, LeaseUntil: base.Add(70 * time.Minute)},
			},
			incoming: &model.Media{
				ID:         30,
				RequestID:  502,
				RequestIDs: []int64{502, 503},
				Type:       "tv",
				TMDBID:     77,
				Status:     "queued",
				Error:      "incoming work retry",
				UpdatedAt:  base.Add(11 * time.Minute),
				Work:       model.MediaWork{Mode: "rerequest", Season: 1, Episode: 2, Generation: 6, Attempts: 1, NextAt: base.Add(21 * time.Minute), LeaseUntil: base.Add(31 * time.Minute)},
				EpisodeAirDates: []model.EpisodeAirDate{
					{Season: 1, Episode: 1, AirDate: "2026-01-03"},
					{Season: 1, Episode: 2, AirDate: "2026-01-04"},
				},
				PlexIntent:         model.DurableIntent{Generation: 8, CompletedGeneration: 3, Attempts: 1, NextAt: base.Add(80 * time.Minute), LeaseGeneration: 8, LeaseUntil: base.Add(90 * time.Minute)},
				AvailabilityIntent: model.DurableIntent{Generation: 3, CompletedGeneration: 3, Attempts: 9, NextAt: base.Add(100 * time.Minute), LeaseGeneration: 3, LeaseUntil: base.Add(110 * time.Minute)},
			},
			wantID:     20,
			wantStatus: "queued",
			wantError:  "incoming work retry",
			wantWork:   model.MediaWork{Mode: "rerequest", Season: 1, Episode: 2, Generation: 6, Attempts: 1, NextAt: base.Add(21 * time.Minute), LeaseUntil: base.Add(31 * time.Minute)},
			wantPlex:   model.DurableIntent{Generation: 8, CompletedGeneration: 3, Attempts: 1, NextAt: base.Add(80 * time.Minute), LeaseGeneration: 8, LeaseUntil: base.Add(90 * time.Minute)},
			// Existing generation 4 survives, but completion generation 3 from
			// the duplicate cannot regress; its stale retry/lease state is not
			// allowed to replace generation 4's state.
			wantAvailability: model.DurableIntent{Generation: 4, CompletedGeneration: 3, Attempts: 4, NextAt: base.Add(60 * time.Minute), LeaseGeneration: 4, LeaseUntil: base.Add(70 * time.Minute)},
		},
		{
			name: "incoming ID wins while existing lifecycle generation is newer",
			existing: &model.Media{
				ID:           40,
				RequestID:    601,
				RequestIDs:   []int64{601, 603},
				SeerrMediaID: 9002,
				Type:         "tv",
				TMDBID:       88,
				Status:       "resolving",
				Error:        "existing command",
				UpdatedAt:    base.Add(120 * time.Minute),
				Work:         model.MediaWork{Mode: "resolve", Generation: 9, Attempts: 5, NextAt: base.Add(130 * time.Minute), LeaseUntil: base.Add(140 * time.Minute)},
				EpisodeAirDates: []model.EpisodeAirDate{
					{Season: 1, Episode: 1, AirDate: "2026-03-01"},
					{Season: 2, Episode: 1, AirDate: "2026-04-01"},
				},
				PlexIntent:         model.DurableIntent{Generation: 9, CompletedGeneration: 4, Attempts: 5, NextAt: base.Add(150 * time.Minute), LeaseGeneration: 9, LeaseUntil: base.Add(160 * time.Minute)},
				AvailabilityIntent: model.DurableIntent{Generation: 5, CompletedGeneration: 2, Attempts: 5, NextAt: base.Add(170 * time.Minute), LeaseGeneration: 5, LeaseUntil: base.Add(180 * time.Minute)},
			},
			incoming: &model.Media{
				ID:         10,
				RequestID:  602,
				RequestIDs: []int64{602, 603},
				Type:       "tv",
				TMDBID:     88,
				Status:     "queued",
				Error:      "incoming command",
				UpdatedAt:  base.Add(121 * time.Minute),
				Work:       model.MediaWork{Mode: "rerequest", Season: 1, Episode: 2, Generation: 8, Attempts: 1, NextAt: base.Add(131 * time.Minute), LeaseUntil: base.Add(141 * time.Minute)},
				EpisodeAirDates: []model.EpisodeAirDate{
					{Season: 1, Episode: 1, AirDate: "2026-03-03"},
					{Season: 1, Episode: 2, AirDate: "2026-03-04"},
				},
				PlexIntent:         model.DurableIntent{Generation: 8, CompletedGeneration: 5, Attempts: 8, NextAt: base.Add(190 * time.Minute), LeaseGeneration: 8, LeaseUntil: base.Add(200 * time.Minute)},
				AvailabilityIntent: model.DurableIntent{Generation: 6, CompletedGeneration: 3, Attempts: 6, NextAt: base.Add(210 * time.Minute), LeaseGeneration: 6, LeaseUntil: base.Add(220 * time.Minute)},
			},
			incomingWins: true,
			wantID:       10,
			wantStatus:   "resolving",
			wantError:    "existing command",
			wantWork:     model.MediaWork{Mode: "resolve", Generation: 9, Attempts: 5, NextAt: base.Add(130 * time.Minute), LeaseUntil: base.Add(140 * time.Minute)},
			// Existing current generation survives, while the duplicate's newer
			// completed generation remains acknowledged without taking its retry
			// or lease state.
			wantPlex:         model.DurableIntent{Generation: 9, CompletedGeneration: 5, Attempts: 5, NextAt: base.Add(150 * time.Minute), LeaseGeneration: 9, LeaseUntil: base.Add(160 * time.Minute)},
			wantAvailability: model.DurableIntent{Generation: 6, CompletedGeneration: 3, Attempts: 6, NextAt: base.Add(210 * time.Minute), LeaseGeneration: 6, LeaseUntil: base.Add(220 * time.Minute)},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s, _ := openTestStore(t)
			if err := s.UpsertMedia(test.existing); err != nil {
				t.Fatal(err)
			}
			mutation := Mutation{Media: test.incoming}
			if test.incomingWins {
				mutation.ReplaceFilesForMedia = test.incoming.ID
				mutation.Files = []*model.File{{ID: "incoming-file", MediaID: test.incoming.ID, Path: "TV/Incoming.mkv"}}
			}
			stored, err := s.Commit(mutation)
			if err != nil {
				t.Fatal(err)
			}
			if stored == nil || stored.ID != test.wantID {
				t.Fatalf("unexpected canonical media: %#v", stored)
			}
			if stored.RequestID != test.incoming.RequestID || !reflect.DeepEqual(stored.RequestIDs, []int64{test.existing.RequestID, test.incoming.RequestID, test.existing.RequestIDs[1]}) {
				t.Fatalf("request associations were not merged: %#v", stored)
			}
			if stored.SeerrMediaID != test.existing.SeerrMediaID {
				t.Fatalf("Seerr media association was lost: %#v", stored)
			}
			wantDates := []model.EpisodeAirDate{
				{Season: 1, Episode: 1, AirDate: test.incoming.EpisodeAirDates[0].AirDate},
				{Season: 1, Episode: 2, AirDate: test.incoming.EpisodeAirDates[1].AirDate},
				{Season: 2, Episode: 1, AirDate: test.existing.EpisodeAirDates[1].AirDate},
			}
			if !reflect.DeepEqual(stored.EpisodeAirDates, wantDates) {
				t.Fatalf("episode dates were not merged by episode: got=%#v want=%#v", stored.EpisodeAirDates, wantDates)
			}
			if stored.Status != test.wantStatus || stored.Error != test.wantError || stored.Work != test.wantWork {
				t.Fatalf("work lifecycle did not retain its winning generation: %#v", stored)
			}
			if stored.PlexIntent != test.wantPlex || stored.AvailabilityIntent != test.wantAvailability {
				t.Fatalf("versioned intent state regressed or mixed generations: %#v", stored)
			}
		})
	}
}

func TestUpdateMediaUsesDetachedCallbackAndReturnValues(t *testing.T) {
	s, _ := openTestStore(t)
	if err := s.UpsertMedia(&model.Media{
		ID:            1,
		Type:          "tv",
		TMDBID:        99,
		Title:         "Original",
		Status:        "queued",
		RequestIDs:    []int64{11},
		Seasons:       []int{1},
		EpisodeCounts: map[int]int{1: 8},
		EpisodeAirDates: []model.EpisodeAirDate{
			{Season: 1, Episode: 1, AirDate: "2026-01-02"},
		},
	}); err != nil {
		t.Fatal(err)
	}

	var escaped *model.Media
	updated, err := s.UpdateMedia(1, func(media *model.Media) error {
		escaped = media
		media.ID = 55
		media.Title = "Updated"
		media.RequestIDs[0] = 12
		media.Seasons[0] = 2
		media.EpisodeCounts[1] = 9
		media.EpisodeAirDates[0].AirDate = "2026-01-03"
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if updated.ID != 1 || updated.Title != "Updated" || updated.RequestIDs[0] != 12 || updated.Seasons[0] != 2 || updated.EpisodeCounts[1] != 9 || updated.EpisodeAirDates[0].AirDate != "2026-01-03" {
		t.Fatalf("UpdateMedia did not commit the callback update: %#v", updated)
	}
	escaped.Title = "escaped"
	escaped.RequestIDs[0] = 13
	escaped.Seasons[0] = 3
	escaped.EpisodeCounts[1] = 10
	escaped.EpisodeAirDates[0].AirDate = "escaped"
	updated.Title = "returned"
	updated.RequestIDs[0] = 14
	updated.Seasons[0] = 4
	updated.EpisodeCounts[1] = 11
	updated.EpisodeAirDates[0].AirDate = "returned"
	stored, _ := s.MediaByID(1)
	if stored.Title != "Updated" || stored.RequestIDs[0] != 12 || stored.Seasons[0] != 2 || stored.EpisodeCounts[1] != 9 || stored.EpisodeAirDates[0].AirDate != "2026-01-03" {
		t.Fatalf("UpdateMedia exposed Store-owned media: %#v", stored)
	}

	before := stored.Title
	if _, err := s.UpdateMedia(1, func(media *model.Media) error {
		media.Title = "failed callback"
		return errors.New("stop")
	}); err == nil {
		t.Fatal("UpdateMedia accepted a failed callback")
	}
	stored, _ = s.MediaByID(1)
	if stored.Title != before {
		t.Fatalf("failed callback changed stored media: %#v", stored)
	}
}

func TestUpdateMediaAtomicStagesDetachedFilesAndMarkers(t *testing.T) {
	s, path := openTestStore(t)
	if err := s.UpsertMedia(&model.Media{ID: 1, Type: "movie", TMDBID: 99, Title: "Before", Status: "ready", RequestIDs: []int64{10}}); err != nil {
		t.Fatal(err)
	}
	if err := s.AddFiles(&model.File{ID: "old", MediaID: 1, Path: "Movies/Before.mkv", Provider: "old"}); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkProcessed(10); err != nil {
		t.Fatal(err)
	}

	replacement := &model.File{ID: "new", MediaID: 1, Path: "Movies/After.mkv", Provider: "new"}
	var escapedMedia *model.Media
	var escapedTransaction *MediaTransaction
	updated, err := s.UpdateMediaAtomic(1, func(media *model.Media, transaction *MediaTransaction) error {
		escapedMedia = media
		escapedTransaction = transaction
		files := transaction.FilesForMedia()
		if len(files) != 1 || files[0].ID != "old" || !transaction.IsProcessed(10) || transaction.IsProcessed(11) {
			t.Fatalf("atomic callback did not receive current detached state: files=%#v", files)
		}
		files[0].Path = "caller-mutated.mkv"
		media.Title = "After"
		media.RequestIDs[0] = 11
		if err := transaction.ReplaceFiles(replacement); err != nil {
			return err
		}
		transaction.MarkProcessed(11)
		transaction.UnmarkProcessed(10)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	replacement.Path = "caller-replaced.mkv"
	escapedMedia.Title = "escaped-media"
	updated.Title = "returned-media"
	escapedTransaction.MarkProcessed(12)
	if err := escapedTransaction.ReplaceFiles(&model.File{ID: "escaped", MediaID: 1, Path: "Movies/Escaped.mkv"}); err != nil {
		t.Fatal(err)
	}

	stored, ok := s.MediaByID(1)
	if !ok || stored.Title != "After" || stored.RequestIDs[0] != 11 {
		t.Fatalf("atomic media update was not isolated: %#v", stored)
	}
	file, ok := s.File("new")
	if !ok || file.Path != "Movies/After.mkv" || file.Provider != "new" {
		t.Fatalf("atomic replacement was not isolated: %#v", file)
	}
	if _, ok := s.File("old"); ok {
		t.Fatal("atomic replacement retained old file")
	}
	if s.IsProcessed(10) || !s.IsProcessed(11) || s.IsProcessed(12) {
		t.Fatal("atomic processed-marker changes were not isolated")
	}

	beforeDisk, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	originalOps := s.ops
	s.ops.writeFile = func(string, []byte, os.FileMode) error { return errors.New("write failed") }
	_, err = s.UpdateMediaAtomic(1, func(media *model.Media, transaction *MediaTransaction) error {
		media.Title = "Failed"
		if err := transaction.ReplaceFiles(&model.File{ID: "failed", MediaID: 1, Path: "Movies/Failed.mkv"}); err != nil {
			return err
		}
		transaction.MarkProcessed(13)
		return nil
	})
	s.ops = originalOps
	if err == nil {
		t.Fatal("UpdateMediaAtomic unexpectedly succeeded after failed save")
	}
	after, _ := s.MediaByID(1)
	if after.Title != "After" {
		t.Fatalf("failed atomic save changed live media: %#v", after)
	}
	if _, ok := s.File("failed"); ok || s.IsProcessed(13) {
		t.Fatal("failed atomic save changed live files or markers")
	}
	afterDisk, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(beforeDisk, afterDisk) {
		t.Fatal("failed atomic save changed durable state")
	}
}

func TestFailedSaveRollsBackMemoryAndDurableState(t *testing.T) {
	for _, failure := range []struct {
		name  string
		apply func(*Store)
	}{
		{
			name: "temp write",
			apply: func(s *Store) {
				s.ops.writeFile = func(string, []byte, os.FileMode) error { return errors.New("write failed") }
			},
		},
		{
			name: "rename",
			apply: func(s *Store) {
				s.ops.rename = func(string, string) error { return errors.New("rename failed") }
			},
		},
	} {
		t.Run(failure.name, func(t *testing.T) {
			s, path := openTestStore(t)
			if err := s.UpsertMedia(&model.Media{ID: 1, Type: "movie", TMDBID: 1, Title: "Before", Status: "ready"}); err != nil {
				t.Fatal(err)
			}
			if err := s.AddFiles(&model.File{ID: "old", MediaID: 1, Path: "Movies/Before.mkv", Provider: "one"}); err != nil {
				t.Fatal(err)
			}
			if err := s.MarkProcessed(10); err != nil {
				t.Fatal(err)
			}
			expires := time.Now().Add(time.Hour).Round(0)
			s.SetStream("old", "https://stream.example/old", expires)
			beforeDisk, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}

			originalOps := s.ops
			failure.apply(s)
			_, err = s.Commit(Mutation{
				Media:                &model.Media{ID: 1, Type: "movie", TMDBID: 1, Title: "After", Status: "failed"},
				ReplaceFilesForMedia: 1,
				Files:                []*model.File{{ID: "new", MediaID: 1, Path: "Movies/After.mkv", Provider: "two"}},
				MarkProcessed:        []int64{11},
				UnmarkProcessed:      []int64{10},
			})
			s.ops = originalOps
			if err == nil {
				t.Fatal("Commit unexpectedly succeeded")
			}

			stored, ok := s.MediaByID(1)
			if !ok || stored.Title != "Before" || stored.Status != "ready" {
				t.Fatalf("failed Commit changed live media: %#v", stored)
			}
			if _, ok := s.File("new"); ok {
				t.Fatal("failed Commit added a replacement file to live state")
			}
			old, ok := s.File("old")
			if !ok || old.StreamURL != "https://stream.example/old" || !old.StreamExpiresAt.Equal(expires) {
				t.Fatalf("failed Commit changed live file/cache: %#v", old)
			}
			if !s.IsProcessed(10) || s.IsProcessed(11) {
				t.Fatal("failed Commit changed live processed markers")
			}

			afterDisk, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(beforeDisk, afterDisk) {
				t.Fatalf("failed Commit changed last durable state\nbefore: %s\nafter: %s", beforeDisk, afterDisk)
			}
			if _, err := os.Stat(path + ".tmp"); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("failed Commit left a temp state file: %v", err)
			}
		})
	}
}

func TestLegacyNilRecordsAreSafeForReaders(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	if err := os.WriteFile(path, []byte(`{"media":{"1":null},"files":{"file":null},"processedRequests":{}}`), 0600); err != nil {
		t.Fatal(err)
	}
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := s.File("file"); ok {
		t.Fatal("nil legacy file was returned")
	}
	if _, ok := s.FindPath("anything"); ok {
		t.Fatal("nil legacy file was found by path")
	}
	if _, ok := s.MediaByID(1); ok {
		t.Fatal("nil legacy media was returned")
	}
	if _, ok := s.FindMediaByTMDB("movie", 1); ok {
		t.Fatal("nil legacy media was found by TMDB")
	}
	if len(s.Files()) != 0 || len(s.FilesForMedia(1)) != 0 || len(s.Media()) != 0 {
		t.Fatalf("nil legacy records leaked through collection readers: files=%#v media=%#v", s.Files(), s.Media())
	}
}

func TestDeleteInactiveMediaAtomicallyRejectsActiveState(t *testing.T) {
	s, _ := openTestStore(t)
	media := &model.Media{ID: 71, RequestID: 81, Type: "movie", Status: "ready", Work: model.MediaWork{Mode: "resolve", Generation: 2}}
	if err := s.UpsertMedia(media); err != nil {
		t.Fatal(err)
	}
	if err := s.AddFiles(&model.File{ID: "active-file", MediaID: media.ID, Path: "Movies/Active.mkv"}); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkProcessed(media.RequestID); err != nil {
		t.Fatal(err)
	}

	if err := s.DeleteInactiveMedia(media.ID); !errors.Is(err, ErrMediaActive) {
		t.Fatalf("active durable work delete error = %v, want ErrMediaActive", err)
	}
	if _, ok := s.MediaByID(media.ID); !ok {
		t.Fatal("active media was deleted")
	}
	if _, ok := s.File("active-file"); !ok {
		t.Fatal("active media file was deleted")
	}

	if _, err := s.UpdateMedia(media.ID, func(current *model.Media) error {
		current.Work = model.MediaWork{}
		current.Status = "resolving"
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteInactiveMedia(media.ID); !errors.Is(err, ErrMediaActive) {
		t.Fatalf("legacy active status delete error = %v, want ErrMediaActive", err)
	}
	if _, err := s.UpdateMedia(media.ID, func(current *model.Media) error {
		current.Status = "ready"
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteInactiveMedia(media.ID); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.MediaByID(media.ID); ok {
		t.Fatal("inactive media remained after delete")
	}
	if _, ok := s.File("active-file"); ok {
		t.Fatal("inactive media file remained after delete")
	}
	if !s.IsProcessed(media.RequestID) {
		t.Fatal("inactive delete removed the Seerr processed marker")
	}
}

func TestStoreConcurrentReadsAndWrites(t *testing.T) {
	s, _ := openTestStore(t)
	if err := s.UpsertMedia(&model.Media{ID: 1, Type: "tv", TMDBID: 99, Title: "Initial", Status: "queued", Seasons: []int{1}, EpisodeCounts: map[int]int{1: 1}}); err != nil {
		t.Fatal(err)
	}
	if err := s.AddFiles(&model.File{ID: "file", MediaID: 1, Path: "TV/Initial.mkv", Provider: "one"}); err != nil {
		t.Fatal(err)
	}

	const iterations = 24
	errCh := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			media := &model.Media{ID: 1, Type: "tv", TMDBID: 99, Title: "Writer", Status: "queued", Seasons: []int{i + 1}, EpisodeCounts: map[int]int{i + 1: i + 2}}
			if err := s.UpsertMedia(media); err != nil {
				errCh <- err
				return
			}
			if err := s.AddFiles(&model.File{ID: "file", MediaID: 1, Path: "TV/Writer.mkv", Provider: "writer", Size: int64(i)}); err != nil {
				errCh <- err
				return
			}
			s.SetStream("file", "https://stream.example/current", time.Now().Add(time.Minute))
			if _, err := s.UpdateMedia(1, func(media *model.Media) error {
				media.Status = "ready"
				return nil
			}); err != nil {
				errCh <- err
				return
			}
			if _, err := s.Commit(Mutation{MarkProcessed: []int64{int64(i)}, UnmarkProcessed: []int64{int64(i - 1)}}); err != nil {
				errCh <- err
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < iterations*3; i++ {
			if media, ok := s.MediaByID(1); ok {
				media.Title = "reader"
				if len(media.Seasons) > 0 {
					media.Seasons[0]++
				}
				for season := range media.EpisodeCounts {
					media.EpisodeCounts[season]++
				}
			}
			if media, ok := s.FindMediaByTMDB("tv", 99); ok {
				media.Status = "reader"
			}
			for _, media := range s.Media() {
				media.Title = "reader"
			}
			if file, ok := s.File("file"); ok {
				file.Path = "reader"
			}
			if file, ok := s.FindPath("TV/Writer.mkv"); ok {
				file.Provider = "reader"
			}
			for _, file := range s.Files() {
				file.Path = "reader"
			}
			for _, file := range s.FilesForMedia(1) {
				file.Provider = "reader"
			}
		}
	}()
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
}
