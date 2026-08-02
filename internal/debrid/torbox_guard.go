package debrid

import (
	"context"
	"sync"
	"time"
)

const torboxUncachedCreateWindow = time.Hour

// TorBoxGuard coordinates all TorBox provider instances created by the
// application. Provider instances are intentionally short-lived when settings
// are refreshed, so the guard must live outside the provider itself to keep
// request pacing and cooldowns effective across concurrent jobs.
type TorBoxGuard struct {
	mu                     sync.Mutex
	minRequestInterval     time.Duration
	rateLimitCooldown      time.Duration
	uncachedCreateInterval time.Duration
	endpoints              map[string]*torboxEndpointState
	uncachedCreates        []time.Time
}

type torboxEndpointState struct {
	next         time.Time
	blockedUntil time.Time
}

func NewTorBoxGuard(minRequestInterval, rateLimitCooldown, uncachedCreateInterval time.Duration) *TorBoxGuard {
	if minRequestInterval <= 0 {
		minRequestInterval = 250 * time.Millisecond
	}
	if rateLimitCooldown <= 0 {
		rateLimitCooldown = time.Minute
	}
	if uncachedCreateInterval <= 0 {
		uncachedCreateInterval = time.Minute
	}
	return &TorBoxGuard{
		minRequestInterval:     minRequestInterval,
		rateLimitCooldown:      rateLimitCooldown,
		uncachedCreateInterval: uncachedCreateInterval,
		endpoints:              map[string]*torboxEndpointState{},
	}
}

// Wait reserves a request slot. Endpoint pacing stays below TorBox's general
// 300/minute endpoint limit. Uncached torrent creation is additionally kept
// to at most 60/hour, as documented by TorBox.
func (g *TorBoxGuard) Wait(ctx context.Context, endpoint string, uncachedCreate bool) error {
	if g == nil {
		return nil
	}

	now := time.Now()
	g.mu.Lock()
	state := g.endpoints[endpoint]
	if state == nil {
		state = &torboxEndpointState{}
		g.endpoints[endpoint] = state
	}
	if now.Before(state.blockedUntil) {
		delay := state.blockedUntil.Sub(now)
		g.mu.Unlock()
		return NewRateLimitError("torbox "+endpoint, delay, "cooldown active")
	}

	// Drop reservations that can no longer contribute to the rolling hourly
	// limit. Reservations are conservative if a caller later cancels while
	// waiting, which is preferable to accidentally exceeding the provider cap.
	cutoff := now.Add(-torboxUncachedCreateWindow)
	first := 0
	for first < len(g.uncachedCreates) && !g.uncachedCreates[first].After(cutoff) {
		first++
	}
	if first > 0 {
		g.uncachedCreates = append([]time.Time(nil), g.uncachedCreates[first:]...)
	}

	scheduled := now
	if state.next.After(scheduled) {
		scheduled = state.next
	}
	if uncachedCreate {
		if len(g.uncachedCreates) >= 60 {
			if at := g.uncachedCreates[0].Add(torboxUncachedCreateWindow); at.After(scheduled) {
				scheduled = at
			}
		}
		if len(g.uncachedCreates) > 0 {
			if at := g.uncachedCreates[len(g.uncachedCreates)-1].Add(g.uncachedCreateInterval); at.After(scheduled) {
				scheduled = at
			}
		}
		g.uncachedCreates = append(g.uncachedCreates, scheduled)
	}
	state.next = scheduled.Add(g.minRequestInterval)
	g.mu.Unlock()

	if err := waitContext(ctx, time.Until(scheduled)); err != nil {
		return err
	}

	// A 429 may have arrived while this request was queued. Do not let queued
	// work punch through a newly established cooldown.
	g.mu.Lock()
	blockedUntil := state.blockedUntil
	g.mu.Unlock()
	if delay := time.Until(blockedUntil); delay > 0 {
		return NewRateLimitError("torbox "+endpoint, delay, "cooldown active")
	}
	return nil
}

func (g *TorBoxGuard) Block(endpoint string, retryAfter time.Duration) {
	if g == nil {
		return
	}
	if retryAfter <= 0 {
		retryAfter = g.rateLimitCooldown
	}
	blockedUntil := time.Now().Add(retryAfter)
	g.mu.Lock()
	state := g.endpoints[endpoint]
	if state == nil {
		state = &torboxEndpointState{}
		g.endpoints[endpoint] = state
	}
	if blockedUntil.After(state.blockedUntil) {
		state.blockedUntil = blockedUntil
	}
	g.mu.Unlock()
}

func waitContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
