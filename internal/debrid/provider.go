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
	ErrStaleItem           = errors.New("debrid item is no longer available")
	ErrTransient           = errors.New("temporary debrid failure")
	ErrRateLimited         = errors.New("debrid provider rate limited")
	ErrProviderUnavailable = errors.New("debrid provider unavailable")
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

// ProviderUnavailableError preserves a provider outage and any Retry-After
// value supplied by the provider. It is separate from ErrTransient so the
// resolver can pause all work for that provider instead of retrying every
// candidate independently.
type ProviderUnavailableError struct {
	Endpoint   string
	RetryAfter time.Duration
	Detail     string
}

func (e *ProviderUnavailableError) Error() string {
	message := e.Endpoint + ": provider unavailable"
	if e.Detail != "" {
		message += ": " + e.Detail
	}
	if e.RetryAfter > 0 {
		message += fmt.Sprintf(" (retry after %s)", e.RetryAfter.Round(time.Second))
	}
	return message
}

func (e *ProviderUnavailableError) Unwrap() []error {
	return []error{ErrProviderUnavailable, ErrTransient}
}

func NewProviderUnavailableError(endpoint string, retryAfter time.Duration, detail string) error {
	return &ProviderUnavailableError{Endpoint: endpoint, RetryAfter: retryAfter, Detail: strings.TrimSpace(detail)}
}

// ProviderCooldown returns the provider-suggested cooldown, when an error is
// one that should pause provider work globally. A zero duration means callers
// should use their configured fallback.
func ProviderCooldown(err error) (time.Duration, bool) {
	var limited *RateLimitError
	if errors.As(err, &limited) {
		return limited.RetryAfter, true
	}
	if errors.Is(err, ErrRateLimited) {
		return 0, true
	}
	var unavailable *ProviderUnavailableError
	if errors.As(err, &unavailable) {
		return unavailable.RetryAfter, true
	}
	if errors.Is(err, ErrProviderUnavailable) {
		return 0, true
	}
	return 0, false
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
