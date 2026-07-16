package scraper

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestParseAddonsKeepsConfiguredPath(t *testing.T) {
	got, err := ParseAddons([]string{"comet|stremio://example.test/config-token/manifest.json"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Name != "comet" || got[0].BaseURL != "https://example.test/config-token" {
		t.Fatalf("unexpected addons: %#v", got)
	}
}

func TestAggregatorSearchesAndDeduplicatesAddons(t *testing.T) {
	const hash = "0123456789abcdef0123456789abcdef01234567"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/configured/stream/movie/tt1254207.json" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"streams":[{"name":"Torrentio 4K","title":"Big.Buck.Bunny.2160p 2.5 GB 👤 12","infoHash":"` + hash + `","sources":["tracker:udp://tracker.example:80"]},{"name":"duplicate","title":"2160p 👤 2","infoHash":"` + hash + `"}]}`))
	}))
	defer server.Close()
	a := &Aggregator{Addons: []Addon{{Name: "torrentio", BaseURL: server.URL + "/configured"}}, Client: server.Client()}
	rows, err := a.Search(context.Background(), Query{MediaType: "movie", ExternalID: "tt1254207"}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows: %#v", rows)
	}
	if rows[0].Seeders != 12 || rows[0].Size != 2684354560 {
		t.Fatalf("metadata: %#v", rows[0])
	}
	if !strings.Contains(rows[0].DownloadURL, "urn%3Abtih%3A"+hash) || !strings.Contains(rows[0].DownloadURL, "tracker.example") {
		t.Fatalf("magnet: %s", rows[0].DownloadURL)
	}
}

func TestAggregatorUsesSeriesVideoID(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/stream/series/tt0944947:3:7.json" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"streams":[]}`))
	}))
	defer server.Close()
	a := &Aggregator{Addons: []Addon{{Name: "test", BaseURL: server.URL}}, Client: server.Client()}
	if _, err := a.Search(context.Background(), Query{MediaType: "tv", ExternalID: "tt0944947", Season: 3, Episode: 7}, 10); err != nil {
		t.Fatal(err)
	}
}
