package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// EditableSettings contains settings that can be changed from the dashboard.
// Credentials are persisted with mode 0600 and never returned by PublicSettings.
type EditableSettings struct {
	SeerrURL       string   `json:"seerrUrl"`
	SeerrAPIKey    string   `json:"seerrApiKey"`
	TorBoxToken    string   `json:"torBoxToken"`
	AllDebridToken string   `json:"allDebridToken"`
	Providers      []string `json:"providers"`
	Qualities      []string `json:"qualities"`
	StremioAddons  []string `json:"stremioAddons"`
	PollInterval   string   `json:"pollInterval"`
	ResolveTimeout string   `json:"resolveTimeout"`
	StreamURLTTL   string   `json:"streamUrlTtl"`
	MinSeeders     int      `json:"minSeeders"`
	MaxResults     int      `json:"maxResults"`
	AllowUncached  bool     `json:"allowUncached"`
}

type SettingsUpdate struct {
	SeerrURL       string   `json:"seerrUrl"`
	SeerrAPIKey    *string  `json:"seerrApiKey,omitempty"`
	TorBoxToken    *string  `json:"torBoxToken,omitempty"`
	AllDebridToken *string  `json:"allDebridToken,omitempty"`
	Providers      []string `json:"providers"`
	Qualities      []string `json:"qualities"`
	StremioAddons  []string `json:"stremioAddons"`
	PollInterval   string   `json:"pollInterval"`
	ResolveTimeout string   `json:"resolveTimeout"`
	StreamURLTTL   string   `json:"streamUrlTtl"`
	MinSeeders     int      `json:"minSeeders"`
	MaxResults     int      `json:"maxResults"`
	AllowUncached  bool     `json:"allowUncached"`
}

type PublicSettings struct {
	SeerrURL              string   `json:"seerrUrl"`
	SeerrAPIKeyConfigured bool     `json:"seerrApiKeyConfigured"`
	TorBoxConfigured      bool     `json:"torBoxConfigured"`
	AllDebridConfigured   bool     `json:"allDebridConfigured"`
	Providers             []string `json:"providers"`
	Qualities             []string `json:"qualities"`
	StremioAddons         []string `json:"stremioAddons"`
	PollInterval          string   `json:"pollInterval"`
	ResolveTimeout        string   `json:"resolveTimeout"`
	StreamURLTTL          string   `json:"streamUrlTtl"`
	MinSeeders            int      `json:"minSeeders"`
	MaxResults            int      `json:"maxResults"`
	AllowUncached         bool     `json:"allowUncached"`
}

type Manager struct {
	mu       sync.RWMutex
	base     Config
	settings EditableSettings
	path     string
}

func OpenManager(base Config) (*Manager, error) {
	m := &Manager{base: base, path: base.SettingsFile, settings: editableFrom(base)}
	b, err := os.ReadFile(m.path)
	if err == nil {
		if err := json.Unmarshal(b, &m.settings); err != nil {
			return nil, fmt.Errorf("read settings: %w", err)
		}
		if err := validateEditable(m.settings); err != nil {
			return nil, fmt.Errorf("read settings: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	return m, nil
}

func editableFrom(c Config) EditableSettings {
	return EditableSettings{
		SeerrURL: c.SeerrURL, SeerrAPIKey: c.SeerrAPIKey,
		TorBoxToken: c.TorBoxToken, AllDebridToken: c.AllDebridToken,
		Providers: append([]string(nil), c.Providers...), Qualities: append([]string(nil), c.Qualities...),
		StremioAddons: append([]string(nil), c.StremioAddons...),
		PollInterval:  c.PollInterval.String(), ResolveTimeout: c.ResolveTimeout.String(), StreamURLTTL: c.StreamURLTTL.String(),
		MinSeeders: c.MinSeeders, MaxResults: c.MaxResults, AllowUncached: c.AllowUncached,
	}
}

func (m *Manager) Snapshot() Config {
	m.mu.RLock()
	defer m.mu.RUnlock()
	c := m.base
	s := m.settings
	c.SeerrURL, c.SeerrAPIKey = strings.TrimRight(s.SeerrURL, "/"), s.SeerrAPIKey
	c.TorBoxToken, c.AllDebridToken = s.TorBoxToken, s.AllDebridToken
	c.Providers, c.Qualities = append([]string(nil), s.Providers...), append([]string(nil), s.Qualities...)
	c.StremioAddons = append([]string(nil), s.StremioAddons...)
	c.PollInterval, _ = time.ParseDuration(s.PollInterval)
	c.ResolveTimeout, _ = time.ParseDuration(s.ResolveTimeout)
	c.StreamURLTTL, _ = time.ParseDuration(s.StreamURLTTL)
	c.MinSeeders, c.MaxResults, c.AllowUncached = s.MinSeeders, s.MaxResults, s.AllowUncached
	return c
}

func (m *Manager) Public() PublicSettings {
	c := m.Snapshot()
	return PublicSettings{
		SeerrURL: c.SeerrURL, SeerrAPIKeyConfigured: c.SeerrAPIKey != "",
		TorBoxConfigured: c.TorBoxToken != "", AllDebridConfigured: c.AllDebridToken != "",
		Providers: c.Providers, Qualities: c.Qualities, StremioAddons: c.StremioAddons,
		PollInterval: c.PollInterval.String(), ResolveTimeout: c.ResolveTimeout.String(), StreamURLTTL: c.StreamURLTTL.String(),
		MinSeeders: c.MinSeeders, MaxResults: c.MaxResults, AllowUncached: c.AllowUncached,
	}
}

func (m *Manager) Update(u SettingsUpdate) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	next := m.settings
	next.SeerrURL = strings.TrimRight(strings.TrimSpace(u.SeerrURL), "/")
	next.Providers = normalizeList(u.Providers, true)
	next.Qualities = normalizeList(u.Qualities, true)
	next.StremioAddons = normalizeList(u.StremioAddons, false)
	next.PollInterval, next.ResolveTimeout, next.StreamURLTTL = strings.TrimSpace(u.PollInterval), strings.TrimSpace(u.ResolveTimeout), strings.TrimSpace(u.StreamURLTTL)
	next.MinSeeders, next.MaxResults, next.AllowUncached = u.MinSeeders, u.MaxResults, u.AllowUncached
	if u.SeerrAPIKey != nil && *u.SeerrAPIKey != "" {
		next.SeerrAPIKey = strings.TrimSpace(*u.SeerrAPIKey)
	}
	if u.TorBoxToken != nil && *u.TorBoxToken != "" {
		next.TorBoxToken = strings.TrimSpace(*u.TorBoxToken)
	}
	if u.AllDebridToken != nil && *u.AllDebridToken != "" {
		next.AllDebridToken = strings.TrimSpace(*u.AllDebridToken)
	}
	if err := validateEditable(next); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(m.path), 0750); err != nil {
		return err
	}
	b, err := json.MarshalIndent(next, "", "  ")
	if err != nil {
		return err
	}
	tmp := m.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0600); err != nil {
		return err
	}
	if err := os.Rename(tmp, m.path); err != nil {
		return err
	}
	m.settings = next
	return nil
}

func validateEditable(s EditableSettings) error {
	if len(s.Providers) == 0 {
		return errors.New("choose at least one provider")
	}
	if len(s.Qualities) == 0 {
		return errors.New("choose at least one quality")
	}
	if len(s.StremioAddons) == 0 {
		return errors.New("configure at least one Stremio addon")
	}
	for _, provider := range s.Providers {
		if provider != "torbox" && provider != "alldebrid" {
			return fmt.Errorf("unsupported provider %q", provider)
		}
	}
	for _, addon := range s.StremioAddons {
		parts := strings.SplitN(addon, "|", 2)
		if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" {
			return fmt.Errorf("invalid Stremio addon %q", addon)
		}
		parsed, err := url.Parse(strings.TrimSpace(parts[1]))
		if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
			return fmt.Errorf("invalid Stremio addon URL in %q", addon)
		}
	}
	if s.SeerrURL != "" {
		parsed, err := url.Parse(s.SeerrURL)
		if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
			return errors.New("Seerr URL must be an http or https URL")
		}
	}
	if s.MaxResults < 1 || s.MaxResults > 500 {
		return errors.New("max results must be between 1 and 500")
	}
	if s.MinSeeders < 0 {
		return errors.New("minimum seeders cannot be negative")
	}
	for label, value := range map[string]string{"poll interval": s.PollInterval, "resolve timeout": s.ResolveTimeout, "stream URL TTL": s.StreamURLTTL} {
		d, err := time.ParseDuration(value)
		if err != nil || d <= 0 {
			return fmt.Errorf("%s must be a positive duration", label)
		}
	}
	return nil
}

func normalizeList(values []string, lower bool) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if lower {
			value = strings.ToLower(value)
		}
		if value != "" && !seen[value] {
			seen[value] = true
			out = append(out, value)
		}
	}
	return out
}
