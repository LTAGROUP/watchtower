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
)

// Plex batches library changes and refreshes Plex after the rclone directory
// cache has had time to expose them through the mounted filesystem.
type Plex struct {
	Config   config.Config
	Settings func() config.Config
	Client   *http.Client
	Log      *slog.Logger

	once    sync.Once
	changes chan struct{}
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
	var timer *time.Timer
	var ready <-chan time.Time
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.changes:
			delay := p.currentConfig().PlexScanDelay
			if delay <= 0 {
				delay = 45 * time.Second
			}
			if timer == nil {
				timer = time.NewTimer(delay)
			} else {
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(delay)
			}
			ready = timer.C
			if p.Log != nil {
				p.Log.Info("Plex library refresh scheduled", "component", "plex", "delay", delay.String())
			}
		case <-ready:
			ready = nil
			refreshCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			err := p.Refresh(refreshCtx)
			cancel()
			if err != nil && p.Log != nil {
				p.Log.Warn("Plex library refresh failed", "component", "plex", "error", err)
			}
		}
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
