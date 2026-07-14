package model

import "time"

type Media struct {
	ID           int64     `json:"id"`
	RequestID    int64     `json:"requestId"`
	Type         string    `json:"type"`
	TMDBID       int64     `json:"tmdbId"`
	ExternalID   string    `json:"externalId"`
	Title        string    `json:"title"`
	Year         int       `json:"year"`
	Overview     string    `json:"overview,omitempty"`
	PosterPath   string    `json:"posterPath,omitempty"`
	BackdropPath string    `json:"backdropPath,omitempty"`
	Seasons      []int     `json:"seasons,omitempty"`
	ReleaseDate  string    `json:"releaseDate,omitempty"`
	Status       string    `json:"status"`
	Error        string    `json:"error,omitempty"`
	CreatedAt    time.Time `json:"createdAt"`
	UpdatedAt    time.Time `json:"updatedAt"`
	ScrapedAt    time.Time `json:"scrapedAt,omitempty"`
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
