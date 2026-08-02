package model

import "time"

type Media struct {
	ID        int64 `json:"id"`
	RequestID int64 `json:"requestId"`
	// RequestIDs retains every Seerr request collapsed into this media item.
	// RequestID remains the original compatibility field.
	RequestIDs    []int64     `json:"requestIds,omitempty"`
	SeerrMediaID  int64       `json:"seerrMediaId,omitempty"`
	Type          string      `json:"type"`
	TMDBID        int64       `json:"tmdbId"`
	ExternalID    string      `json:"externalId"`
	Title         string      `json:"title"`
	Year          int         `json:"year"`
	Overview      string      `json:"overview,omitempty"`
	PosterPath    string      `json:"posterPath,omitempty"`
	BackdropPath  string      `json:"backdropPath,omitempty"`
	Seasons       []int       `json:"seasons,omitempty"`
	EpisodeCounts map[int]int `json:"episodeCounts,omitempty"`
	ReleaseDate   string      `json:"releaseDate,omitempty"`
	Status        string      `json:"status"`
	Error         string      `json:"error,omitempty"`
	CreatedAt     time.Time   `json:"createdAt"`
	UpdatedAt     time.Time   `json:"updatedAt"`
	ScrapedAt     time.Time   `json:"scrapedAt,omitempty"`
	// Work is a single, durable resolution command. An empty Mode means there
	// is no pending work. It deliberately lives on Media instead of in a
	// general-purpose jobs collection so legacy state remains readable.
	Work               MediaWork        `json:"work,omitempty"`
	EpisodeAirDates    []EpisodeAirDate `json:"episodeAirDates,omitempty"`
	PlexIntent         DurableIntent    `json:"plexIntent,omitempty"`
	AvailabilityIntent DurableIntent    `json:"availabilityIntent,omitempty"`
}

// MediaWork describes a full resolution or a scoped TV re-request. NextAt and
// LeaseUntil are UTC timestamps. A non-expired lease prevents another worker
// from executing the same durable command after it has been claimed.
type MediaWork struct {
	Mode       string    `json:"mode,omitempty"`
	Season     int       `json:"season,omitempty"`
	Episode    int       `json:"episode,omitempty"`
	Generation int64     `json:"generation,omitempty"`
	Attempts   int       `json:"attempts,omitempty"`
	NextAt     time.Time `json:"nextAt,omitempty"`
	LeaseUntil time.Time `json:"leaseUntil,omitempty"`
}

// EpisodeAirDate is intentionally a flat value record. It keeps state JSON
// friendly and avoids a nested map that callers could accidentally retain.
type EpisodeAirDate struct {
	Season  int    `json:"season"`
	Episode int    `json:"episode"`
	AirDate string `json:"airDate"`
}

// DurableIntent tracks a versioned external side effect. A completion may only
// acknowledge the generation it observed; a newer Generation therefore remains
// pending after an older request succeeds.
type DurableIntent struct {
	Generation          int64     `json:"generation,omitempty"`
	CompletedGeneration int64     `json:"completedGeneration,omitempty"`
	Attempts            int       `json:"attempts,omitempty"`
	NextAt              time.Time `json:"nextAt,omitempty"`
	LeaseUntil          time.Time `json:"leaseUntil,omitempty"`
	LeaseGeneration     int64     `json:"leaseGeneration,omitempty"`
}

type QualityAvailability struct {
	Quality   string `json:"quality"`
	Available int    `json:"available"`
	Expected  int    `json:"expected"`
	Complete  bool   `json:"complete"`
}

type File struct {
	ID              string    `json:"id"`
	MediaID         int64     `json:"mediaId"`
	Path            string    `json:"path"`
	Quality         string    `json:"quality"`
	Provider        string    `json:"provider"`
	SourceURI       string    `json:"sourceUri,omitempty"`
	InfoHash        string    `json:"infoHash,omitempty"`
	ProviderItemID  string    `json:"providerItemId"`
	ProviderFileID  string    `json:"providerFileId"`
	Size            int64     `json:"size"`
	StreamURL       string    `json:"-"`
	StreamExpiresAt time.Time `json:"-"`
	CreatedAt       time.Time `json:"createdAt"`
}

type Release struct {
	Title, DownloadURL, InfoHash, Source string
	Size                                 int64
	Seeders                              int
	TorrentData                          []byte
}

type RemoteFile struct {
	ID, Name string
	Size     int64
}
type Resolved struct {
	ItemID string
	Files  []RemoteFile
	Cached bool
}

type State struct {
	Media             map[int64]*Media    `json:"media"`
	Files             map[string]*File    `json:"files"`
	ProcessedRequests map[int64]time.Time `json:"processedRequests"`
}
