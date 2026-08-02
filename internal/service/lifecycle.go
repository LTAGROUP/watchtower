package service

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/LTAGROUP/watchtower/internal/model"
	"github.com/LTAGROUP/watchtower/internal/store"
)

// Lifecycle is the lightweight durable work dispatcher. Durable state is
// embedded in Media; this type only wakes, claims, and tracks in-process work.
// It intentionally is not a general-purpose queue.
type Lifecycle struct {
	Store    *store.Store
	Resolver *Resolver
	Log      *slog.Logger
	// Interval is the maximum idle scan interval. Zero defaults to 30 seconds.
	// A known NextAt is always used sooner than Interval.
	Interval time.Duration

	wakeOnce sync.Once
	wake     chan struct{}
	inflight sync.Map
	tasks    sync.WaitGroup
}

// Wake coalesces scheduler notifications. It is safe to call from request
// handlers because it never blocks on provider or scraper work.
func (l *Lifecycle) Wake() {
	select {
	case l.wakeChannel() <- struct{}{}:
	default:
	}
}

func (l *Lifecycle) wakeChannel() chan struct{} {
	l.wakeOnce.Do(func() { l.wake = make(chan struct{}, 1) })
	return l.wake
}

// Run recovers legacy queued records, expired leases, scheduled TV episodes,
// and transient failures. On cancellation it waits for every spawned task so
// the application shutdown budget governs all resolver activity.
func (l *Lifecycle) Run(ctx context.Context) {
	if l.Store == nil || l.Resolver == nil {
		return
	}
	l.dispatch(ctx)
	for {
		wait := l.nextScan(time.Now().UTC())
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			l.tasks.Wait()
			return
		case <-l.wakeChannel():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		case <-timer.C:
		}
		l.dispatch(ctx)
	}
}

// Wait is useful to callers that start Run themselves. It does not cancel
// tasks; callers must cancel their context first.
func (l *Lifecycle) Wait() { l.tasks.Wait() }

func (l *Lifecycle) dispatch(ctx context.Context) {
	now := time.Now().UTC()
	for _, media := range l.Store.Media() {
		if !needsWorkDispatch(media, now) {
			continue
		}
		id := media.ID
		if _, loaded := l.inflight.LoadOrStore(id, struct{}{}); loaded {
			continue
		}
		l.tasks.Add(1)
		go func() {
			defer l.tasks.Done()
			defer l.inflight.Delete(id)
			if err := l.Resolver.RunDue(ctx, id); err != nil && ctx.Err() == nil && l.Log != nil {
				l.Log.Error("durable media work failed", "component", "lifecycle", "media", id, "error", err)
			}
		}()
	}
}

func needsWorkDispatch(media *model.Media, now time.Time) bool {
	if media == nil {
		return false
	}
	if media.Work.Mode != "" {
		return !media.Work.LeaseUntil.After(now) && (media.Work.NextAt.IsZero() || !media.Work.NextAt.After(now))
	}
	// Pre-scheduler state had no work metadata. These are recovered by
	// Resolver.claimWork, which writes a full work record atomically.
	switch media.Status {
	case "queued", "scraping", "resolving":
		return true
	case "unreleased":
		return !IsUnreleased(media, now)
	default:
		return false
	}
}

func (l *Lifecycle) nextScan(now time.Time) time.Duration {
	interval := l.Interval
	if interval <= 0 {
		interval = 30 * time.Second
	}
	next := now.Add(interval)
	for _, media := range l.Store.Media() {
		if media == nil || media.Work.Mode == "" {
			continue
		}
		candidate := media.Work.NextAt
		if media.Work.LeaseUntil.After(now) && (candidate.IsZero() || media.Work.LeaseUntil.Before(candidate)) {
			candidate = media.Work.LeaseUntil
		}
		if !candidate.IsZero() && candidate.Before(next) {
			next = candidate
		}
	}
	if !next.After(now) {
		return time.Millisecond
	}
	return time.Until(next)
}
