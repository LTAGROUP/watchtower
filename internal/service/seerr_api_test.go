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
	_, err = s.CreateRequest(context.Background(), CreateRequestInput{MediaType: "tv", MediaID: 44, Seasons: []int{1, 2}})
	if err != nil {
		t.Fatal(err)
	}
	if requested.MediaID != 44 || len(requested.Seasons) != 2 {
		t.Fatalf("unexpected request: %#v", requested)
	}
}
