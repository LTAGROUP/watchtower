package service

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/LTAGROUP/watchtower/internal/config"
	"github.com/LTAGROUP/watchtower/internal/model"
	"github.com/LTAGROUP/watchtower/internal/store"
)

// Plex batches library changes and refreshes Plex after the rclone directory
// cache has had time to expose them through the mounted filesystem.
type Plex struct {
	Config   config.Config
	Settings func() config.Config
	Store    *store.Store
	Client   *http.Client
	Log      *slog.Logger

	once    sync.Once
	changes chan struct{}

	// newIntentTimer is only overridden by tests that verify a due intent cannot
	// spin timers while an in-memory debounce is active.
	newIntentTimer func(time.Duration) *time.Timer
}

func (p *Plex) Notify() {
	p.init()
	select {
	case p.changes <- struct{}{}:
	default:
	}
}

func (p *Plex) Run(ctx context.Context) {
	p.init()
	if p.Store != nil {
		p.processIntents(ctx)
	}
	var debounce *time.Timer
	var debounced <-chan time.Time
	for {
		var intentTimer *time.Timer
		var intents <-chan time.Time
		// A pending Notify owns the next refresh. Do not recreate an immediately
		// due durable-intent timer while waiting for that debounce to expire.
		if debounced == nil {
			intentTimer = p.nextIntentTimer(p.nextIntentDelay(time.Now().UTC()))
			intents = intentTimer.C
		}
		select {
		case <-ctx.Done():
			stopPlexTimer(intentTimer)
			stopPlexTimer(debounce)
			return
		case <-p.changes:
			stopPlexTimer(intentTimer)
			delay := p.currentConfig().PlexScanDelay
			if delay <= 0 {
				delay = 15 * time.Second
			}
			if debounce == nil {
				debounce = time.NewTimer(delay)
			} else {
				stopPlexTimer(debounce)
				debounce.Reset(delay)
			}
			debounced = debounce.C
			if p.Log != nil {
				p.Log.Info("Plex library refresh scheduled", "component", "plex", "delay", delay.String())
			}
		case <-debounced:
			debounced = nil
			debounce = nil
			if p.Store != nil {
				if !p.processIntents(ctx) {
					p.refreshOnce(ctx)
				}
			} else {
				p.refreshOnce(ctx)
			}
		case <-intents:
			if p.Store != nil {
				p.processIntents(ctx)
			}
		}
	}
}

func (p *Plex) nextIntentTimer(delay time.Duration) *time.Timer {
	if p.newIntentTimer != nil {
		return p.newIntentTimer(delay)
	}
	return time.NewTimer(delay)
}

func stopPlexTimer(timer *time.Timer) {
	if timer == nil || timer.Stop() {
		return
	}
	select {
	case <-timer.C:
	default:
	}
}

func (p *Plex) refreshOnce(parent context.Context) {
	refreshCtx, cancel := context.WithTimeout(parent, 30*time.Second)
	err := p.Refresh(refreshCtx)
	cancel()
	if err != nil && p.Log != nil && parent.Err() == nil {
		p.Log.Warn("Plex library refresh failed", "component", "plex", "error", err)
	}
}

func (p *Plex) Refresh(ctx context.Context) error {
	cfg := p.currentConfig()
	baseURL := strings.TrimRight(strings.TrimSpace(cfg.PlexURL), "/")
	if baseURL == "" || strings.TrimSpace(cfg.PlexToken) == "" {
		return nil
	}
	client := p.Client
	if client == nil {
		client = http.DefaultClient
	}
	req, err := plexRequest(ctx, baseURL+"/library/sections", cfg.PlexToken)
	if err != nil {
		return err
	}
	started := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("Plex returned %s", resp.Status)
	}
	var sections struct {
		Directories []struct {
			Key  string `xml:"key,attr"`
			Type string `xml:"type,attr"`
		} `xml:"Directory"`
	}
	if err := xml.NewDecoder(io.LimitReader(resp.Body, 2<<20)).Decode(&sections); err != nil {
		return fmt.Errorf("decode Plex library sections: %w", err)
	}
	refreshed := 0
	var refreshErrors []error
	for _, section := range sections.Directories {
		if section.Key == "" || (section.Type != "movie" && section.Type != "show") {
			continue
		}
		refreshURL := baseURL + "/library/sections/" + url.PathEscape(section.Key) + "/refresh"
		refreshReq, err := plexRequest(ctx, refreshURL, cfg.PlexToken)
		if err != nil {
			refreshErrors = append(refreshErrors, err)
			continue
		}
		refreshResp, err := client.Do(refreshReq)
		if err != nil {
			refreshErrors = append(refreshErrors, err)
			continue
		}
		refreshResp.Body.Close()
		if refreshResp.StatusCode/100 != 2 {
			refreshErrors = append(refreshErrors, fmt.Errorf("Plex section %s returned %s", section.Key, refreshResp.Status))
			continue
		}
		refreshed++
	}
	if p.Log != nil {
		p.Log.Info("Plex library refresh requested", "component", "plex", "sections", refreshed, "duration", time.Since(started).String())
	}
	return errors.Join(refreshErrors...)
}

type plexIntentClaim struct {
	mediaID    int64
	generation int64
}

// processIntents batches every due media generation into one Plex library
// refresh. Each claim is acknowledged only up to the generation observed
// before the HTTP call, so a later file publication remains pending.
func (p *Plex) processIntents(ctx context.Context) bool {
	if p.Store == nil {
		return false
	}
	now := time.Now().UTC()
	claims := make([]plexIntentClaim, 0)
	for _, media := range p.Store.Media() {
		if media == nil {
			continue
		}
		claim, ok := p.claimIntent(media.ID, now)
		if ok {
			claims = append(claims, claim)
		}
	}
	if len(claims) == 0 {
		return false
	}
	if !p.configured() {
		p.completeIntentClaims(claims, now, errors.New("Plex is not configured"))
		return true
	}
	refreshCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	err := p.Refresh(refreshCtx)
	cancel()
	p.completeIntentClaims(claims, time.Now().UTC(), err)
	if err != nil && p.Log != nil && ctx.Err() == nil {
		p.Log.Warn("Plex library refresh failed", "component", "plex", "error", err)
	}
	return true
}

func (p *Plex) configured() bool {
	cfg := p.currentConfig()
	return strings.TrimSpace(cfg.PlexURL) != "" && strings.TrimSpace(cfg.PlexToken) != ""
}

func (p *Plex) claimIntent(id int64, now time.Time) (plexIntentClaim, bool) {
	var claim plexIntentClaim
	_, err := p.Store.UpdateMedia(id, func(media *model.Media) error {
		intent := &media.PlexIntent
		if intent.Generation <= intent.CompletedGeneration || intent.LeaseUntil.After(now) || (!intent.NextAt.IsZero() && intent.NextAt.After(now)) {
			return errPlexIntentNotDue
		}
		intent.LeaseGeneration = intent.Generation
		intent.LeaseUntil = now.Add(2 * time.Minute)
		claim = plexIntentClaim{mediaID: media.ID, generation: intent.Generation}
		return nil
	})
	return claim, err == nil
}

var errPlexIntentNotDue = errors.New("Plex intent is not due")

func (p *Plex) completeIntentClaims(claims []plexIntentClaim, now time.Time, callErr error) {
	for _, claim := range claims {
		_, _ = p.Store.UpdateMedia(claim.mediaID, func(media *model.Media) error {
			intent := &media.PlexIntent
			if callErr == nil {
				if claim.generation > intent.CompletedGeneration {
					intent.CompletedGeneration = claim.generation
				}
				if intent.LeaseGeneration == claim.generation {
					intent.LeaseGeneration = 0
					intent.LeaseUntil = time.Time{}
				}
				if intent.Generation == claim.generation {
					intent.Attempts = 0
					intent.NextAt = time.Time{}
				}
				return nil
			}
			if intent.LeaseGeneration == claim.generation {
				intent.LeaseGeneration = 0
				intent.LeaseUntil = time.Time{}
			}
			// A newer publication is due immediately; an older failure must not
			// delay or overwrite that generation's retry state.
			if intent.Generation == claim.generation {
				intent.Attempts++
				intent.NextAt = now.Add(retryAfter(intent.Attempts))
			}
			return nil
		})
	}
}

func (p *Plex) nextIntentDelay(now time.Time) time.Duration {
	if p.Store == nil {
		return 24 * time.Hour
	}
	next := now.Add(24 * time.Hour)
	for _, media := range p.Store.Media() {
		if media == nil {
			continue
		}
		intent := media.PlexIntent
		if intent.Generation <= intent.CompletedGeneration {
			continue
		}
		candidate := intent.NextAt
		if intent.LeaseUntil.After(now) && (candidate.IsZero() || intent.LeaseUntil.Before(candidate)) {
			candidate = intent.LeaseUntil
		}
		if candidate.IsZero() || !candidate.After(now) {
			return time.Millisecond
		}
		if candidate.Before(next) {
			next = candidate
		}
	}
	return time.Until(next)
}

func plexRequest(ctx context.Context, endpoint, token string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err == nil {
		req.Header.Set("X-Plex-Token", token)
	}
	return req, err
}

func (p *Plex) init() {
	p.once.Do(func() { p.changes = make(chan struct{}, 1) })
}

func (p *Plex) currentConfig() config.Config {
	if p.Settings != nil {
		return p.Settings()
	}
	return p.Config
}
