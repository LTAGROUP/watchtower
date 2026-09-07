package debrid

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/LTAGROUP/watchtower/internal/model"
)

func jsonResponse(body string) *http.Response {
	return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}
}

func TestAllDebridResolvesDocumentedFilesResponse(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/v4/magnet/upload":
			return jsonResponse(`{"status":"success","data":{"magnets":[{"id":123,"ready":true}]}}`), nil
		case "/v4.1/magnet/status":
			return jsonResponse(`{"status":"success","data":{"magnets":{"id":123,"statusCode":4,"status":"Ready"}}}`), nil
		case "/v4/magnet/files":
			if err := r.ParseForm(); err != nil {
				t.Fatal(err)
			}
			if r.Form.Get("id[]") != "123" {
				t.Errorf("files request must use id[]: %v", r.Form)
			}
			return jsonResponse(`{"status":"success","data":{"magnets":[{"id":"123","files":[{"n":"Show.S01E01","e":[{"n":"episode.mkv","s":42,"l":"https://alldebrid.com/f/test"}]}]}]}}`), nil
		}
		return nil, fmt.Errorf("unexpected endpoint: %s", r.URL.Path)
	})}
	p := &AllDebrid{Client: client, Poll: time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got, err := p.Resolve(ctx, model.Release{DownloadURL: "magnet:?xt=urn:btih:test"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Files) != 1 || got.Files[0].Name != "Show.S01E01/episode.mkv" || got.Files[0].Size != 42 {
		t.Fatalf("unexpected files: %+v", got)
	}
}

func TestAllDebridClassifiesPlaybackLinkErrors(t *testing.T) {
	for _, tc := range []struct {
		code string
		want error
	}{
		{"LINK_DOWN", ErrStaleItem}, {"LINK_TEMPORARY_UNAVAILABLE", ErrTransient},
	} {
		t.Run(tc.code, func(t *testing.T) {
			p := &AllDebrid{Client: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				return jsonResponse(fmt.Sprintf(`{"status":"error","error":{"code":%q,"message":"link unavailable"}}`, tc.code)), nil
			})}}
			_, err := p.StreamURL(context.Background(), &model.File{ProviderFileID: "link"})
			if !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
}

func TestTorBoxWaitsForDownloadReadiness(t *testing.T) {
	calls := 0
	p := &TorBox{Poll: time.Millisecond, Client: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls++
		ready := calls > 1
		return jsonResponse(fmt.Sprintf(`{"success":true,"data":{"download_present":%t,"download_finished":%t,"download_state":"downloading","files":[{"id":0,"name":"movie.mkv","size":42}]}}`, ready, ready)), nil
	})}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got, err := p.wait(ctx, 123)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 2 || len(got.Files) != 1 || got.Files[0].ID != "0" {
		t.Fatalf("published before ready: calls=%d result=%+v", calls, got)
	}
}

func TestTorBoxCacheCheckUsesStructuredAvailability(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		cached     bool
	}{
		{"available", `{"success":true,"data":{"abc":{"hash":"abc","files":[{"id":0}]}}}`, true},
		{"hash only in message", `{"success":true,"detail":"abc is not cached","data": {}}`, false},
		{"null entry", `{"success":true,"data":{"abc":null}}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &TorBox{Client: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) { return jsonResponse(tc.body), nil })}}
			got, err := p.cached(context.Background(), "abc")
			if err != nil || got != tc.cached {
				t.Fatalf("cached=%v err=%v", got, err)
			}
		})
	}
}

func TestAllDebridMatchesLargeNumericMagnetID(t *testing.T) {
	p := &AllDebrid{Poll: time.Millisecond, Client: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if strings.HasSuffix(r.URL.Path, "/status") {
			return jsonResponse(`{"status":"success","data":{"magnets":{"statusCode":4}}}`), nil
		}
		return jsonResponse(`{"status":"success","data":{"magnets":[{"id":12345678,"files":[{"n":"movie.mkv","s":42,"l":"link"}]}]}}`), nil
	})}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got, err := p.wait(ctx, 12345678)
	if err != nil || len(got.Files) != 1 {
		t.Fatalf("large numeric magnet id not matched: %+v %v", got, err)
	}
}

func TestAllDebridRetriesTransportAndMalformedResponses(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		err        error
	}{
		{name: "transport", err: io.ErrUnexpectedEOF}, {name: "malformed response", body: "not JSON"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &AllDebrid{Client: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				if tc.err != nil {
					return nil, tc.err
				}
				return jsonResponse(tc.body), nil
			})}}
			_, err := p.StreamURL(context.Background(), &model.File{ProviderFileID: "link"})
			if !errors.Is(err, ErrTransient) {
				t.Fatalf("failure not retryable: %v", err)
			}
		})
	}
}
