package debrid

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/LTAGROUP/watchtower/internal/model"
)

var (
	ErrStaleItem   = errors.New("debrid item is no longer available")
	ErrTransient   = errors.New("temporary debrid failure")
	ErrRateLimited = errors.New("debrid provider rate limited")
)

// RateLimitError preserves the provider's suggested cooldown when an API
// endpoint returns HTTP 429. Callers can use errors.Is(err, ErrRateLimited)
// while still honoring Retry-After when it is available.
type RateLimitError struct {
	Endpoint   string
	RetryAfter time.Duration
	Detail     string
}

func (e *RateLimitError) Error() string {
	message := e.Endpoint + ": rate limited"
	if e.Detail != "" {
		message += ": " + e.Detail
	}
	if e.RetryAfter > 0 {
		message += fmt.Sprintf(" (retry after %s)", e.RetryAfter.Round(time.Second))
	}
	return message
}

func (e *RateLimitError) Unwrap() error { return ErrRateLimited }

func NewRateLimitError(endpoint string, retryAfter time.Duration, detail string) error {
	return &RateLimitError{Endpoint: endpoint, RetryAfter: retryAfter, Detail: strings.TrimSpace(detail)}
}

func RateLimitDelay(err error) time.Duration {
	var limited *RateLimitError
	if errors.As(err, &limited) {
		return limited.RetryAfter
	}
	return 0
}

func retryAfter(value string) time.Duration {
	value = strings.TrimSpace(value)
	if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if at, err := http.ParseTime(value); err == nil {
		if delay := time.Until(at); delay > 0 {
			return delay
		}
	}
	return 0
}

type Provider interface {
	Name() string
	Resolve(context.Context, model.Release) (model.Resolved, error)
	StreamURL(context.Context, *model.File) (string, error)
}
