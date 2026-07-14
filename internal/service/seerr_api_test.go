package service

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/LTAGROUP/watchtower/internal/config"
)

func TestSeerrDiscoverAndCreateRequest(t *testing.T) {
	var requested CreateRequestInput
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Api-Key") != "key" {
			t.Error("missing API key")
		}
		switch r.URL.Path {
		case "/api/v1/discover/movies":
			if r.URL.Query().Get("genre") != "28" || r.URL.Query().Get("primaryReleaseDateGte") != "2025-01-01" {
				t.Errorf("unexpected query: %s", r.URL.RawQuery)
			}
			w.Write([]byte(`{"results":[{"id":1,"title":"Movie"}]}`))
		case "/api/v1/search":
			if !strings.Contains(r.URL.RawQuery, "query=Blade%20Runner") {
				t.Errorf("search query was not percent encoded: %s", r.URL.RawQuery)
			}
			if r.URL.Query().Get("query") != "Blade Runner" {
				t.Errorf("search query changed after decoding: %q", r.URL.Query().Get("query"))
			}
			w.Write([]byte(`{"results":[{"id":2,"title":"Blade Runner"}]}`))
		case "/api/v1/request":
			if err := json.NewDecoder(r.Body).Decode(&requested); err != nil {
				t.Error(err)
			}
			w.WriteHeader(http.StatusCreated)
			w.Write([]byte(`{"id":99}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	s := &Seerr{Config: config.Config{SeerrURL: server.URL, SeerrAPIKey: "key"}, Client: server.Client()}
	raw, err := s.Discover(context.Background(), DiscoverOptions{MediaType: "movie", Genre: "28", Year: "2025", Page: 1})
	if err != nil || !strings.Contains(string(raw), "Movie") {
		t.Fatalf("discover failed: %s %v", raw, err)
	}
	raw, err = s.Discover(context.Background(), DiscoverOptions{MediaType: "movie", Query: "Blade Runner", Page: 1})
	if err != nil || !strings.Contains(string(raw), "Blade Runner") {
		t.Fatalf("search failed: %s %v", raw, err)
	}
	_, err = s.CreateRequest(context.Background(), CreateRequestInput{MediaType: "tv", MediaID: 44, Seasons: []int{1, 2}})
	if err != nil {
		t.Fatal(err)
	}
	if requested.MediaID != 44 || len(requested.Seasons) != 2 {
		t.Fatalf("unexpected request: %#v", requested)
	}
}

func TestMediaReleaseDateUsesRequestedEpisodeSchedule(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/tv/99/season/2" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(`{"seasonNumber":2,"episodes":[{"episodeNumber":1,"airDate":"2099-03-04"},{"episodeNumber":2,"airDate":"2099-03-11"}]}`))
	}))
	defer server.Close()
	s := &Seerr{Config: config.Config{SeerrURL: server.URL, SeerrAPIKey: "key"}, Client: server.Client()}
	details := CatalogDetails{FirstAirDate: "2020-01-01", Seasons: []CatalogSeason{{SeasonNumber: 2}}}
	if got := s.MediaReleaseDate(context.Background(), "tv", 99, details, []int{2}); got != "2099-03-04" {
		t.Fatalf("unexpected episode release date: %q", got)
	}
	if got := s.MediaReleaseDate(context.Background(), "movie", 10, CatalogDetails{ReleaseDate: "2099-05-06"}, nil); got != "2099-05-06" {
		t.Fatalf("unexpected movie release date: %q", got)
	}
}
