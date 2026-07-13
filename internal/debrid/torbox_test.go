package debrid

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/LTAGROUP/watchtower/internal/model"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestTorBoxClassifiesGatewayFailuresAsTransient(t *testing.T) {
	tests := []struct {
		name   string
		status int
		body   string
	}{
		{name: "gateway detail in successful response", status: http.StatusOK, body: `{"data":null,"detail":"502 Bad Gateway"}`},
		{name: "gateway HTTP status", status: http.StatusBadGateway, body: `<!doctype html>`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: test.status, Status: http.StatusText(test.status), Body: io.NopCloser(strings.NewReader(test.body)), Header: make(http.Header)}, nil
			})}
			provider := &TorBox{Token: "token", Client: client}
			_, err := provider.StreamURL(context.Background(), &model.File{ProviderItemID: "1", ProviderFileID: "2"})
			if !errors.Is(err, ErrTransient) {
				t.Fatalf("expected transient error, got %v", err)
			}
		})
	}
}
