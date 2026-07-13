package model

import "time"

type Media struct {
	ID, RequestID        int64     `json:"id"`
	Type                 string    `json:"type"`
	TMDBID               int64     `json:"tmdbId"`
	ExternalID           string    `json:"externalId"`
	Title                string    `json:"title"`
	Year                 int       `json:"year"`
	Seasons              []int     `json:"seasons,omitempty"`
	Status, Error        string    `json:"status"`
	CreatedAt, UpdatedAt time.Time `json:"createdAt"`
}

type File struct {
	ID                                           string `json:"id"`
	MediaID                                      int64  `json:"mediaId"`
	Path, Quality, Provider, SourceURI, InfoHash string
	ProviderItemID                               string    `json:"providerItemId"`
	ProviderFileID                               string    `json:"providerFileId"`
	Size                                         int64     `json:"size"`
	StreamURL                                    string    `json:"-"`
	StreamExpiresAt                              time.Time `json:"-"`
	CreatedAt                                    time.Time `json:"createdAt"`
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
