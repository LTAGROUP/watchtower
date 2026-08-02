package debrid

import (
	"context"
	"sync"
	"time"
)

// ProviderGuard shares provider-wide outage cooldowns across all short-lived
// provider instances created by settings refreshes and concurrent jobs.
type ProviderGuard struct {
	mu              sync.Mutex
	defaultCooldown time.Duration
	blockedUntil    time.Time
}

func NewProviderGuard(defaultCooldown time.Duration) *ProviderGuard {
	if defaultCooldown <= 0 {
		defaultCooldown = time.Minute
	}
	return &ProviderGuard{defaultCooldown: defaultCooldown}
}

func (g *ProviderGuard) Wait(ctx context.Context, endpoint string) error {
	if g == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	now := time.Now()
	g.mu.Lock()
	delay := g.blockedUntil.Sub(now)
	g.mu.Unlock()
	if delay > 0 {
		return NewProviderUnavailableError(endpoint, delay, "cooldown active")
	}
	return nil
}

func (g *ProviderGuard) Block(retryAfter time.Duration) {
	if g == nil {
		return
	}
	if retryAfter <= 0 {
		retryAfter = g.defaultCooldown
	}
	blockedUntil := time.Now().Add(retryAfter)
	g.mu.Lock()
	if blockedUntil.After(g.blockedUntil) {
		g.blockedUntil = blockedUntil
	}
	g.mu.Unlock()
}
