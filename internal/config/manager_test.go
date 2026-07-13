package config

import (
	"path/filepath"
	"testing"
	"time"
)

func TestManagerUpdatesAndRedactsSecrets(t *testing.T) {
	base := Config{
		SettingsFile: filepath.Join(t.TempDir(), "settings.json"),
		SeerrURL:     "http://seerr:5055", SeerrAPIKey: "old-key", TorBoxToken: "old-token",
		Providers: []string{"torbox"}, Qualities: []string{"1080p"},
		StremioAddons: []string{"test|http://scraper/manifest.json"},
		PollInterval:  time.Minute, ResolveTimeout: 10 * time.Minute, StreamURLTTL: 30 * time.Minute,
		MaxResults: 20,
	}
	m, err := OpenManager(base)
	if err != nil {
		t.Fatal(err)
	}
	newKey := "new-key"
	if err := m.Update(SettingsUpdate{
		SeerrURL: "http://new-seerr:5055/", SeerrAPIKey: &newKey,
		Providers: []string{"TORBOX"}, Qualities: []string{"2160P", "1080p"},
		StremioAddons: []string{"source|http://source/manifest.json"},
		PollInterval:  "30s", ResolveTimeout: "5m", StreamURLTTL: "20m", MaxResults: 40,
	}); err != nil {
		t.Fatal(err)
	}
	if got := m.Snapshot(); got.SeerrURL != "http://new-seerr:5055" || got.SeerrAPIKey != newKey || got.Qualities[0] != "2160p" {
		t.Fatalf("unexpected snapshot: %#v", got)
	}
	public := m.Public()
	if !public.SeerrAPIKeyConfigured || !public.TorBoxConfigured {
		t.Fatalf("expected configured credentials: %#v", public)
	}
	reloaded, err := OpenManager(base)
	if err != nil {
		t.Fatal(err)
	}
	if reloaded.Snapshot().SeerrAPIKey != newKey {
		t.Fatal("persisted secret was not reloaded")
	}
}

func TestManagerRejectsInvalidDurations(t *testing.T) {
	base := Config{SettingsFile: filepath.Join(t.TempDir(), "settings.json"), Providers: []string{"torbox"}, Qualities: []string{"1080p"}, StremioAddons: []string{"x|http://x/manifest.json"}, PollInterval: time.Minute, ResolveTimeout: time.Minute, StreamURLTTL: time.Minute, MaxResults: 20}
	m, err := OpenManager(base)
	if err != nil {
		t.Fatal(err)
	}
	err = m.Update(SettingsUpdate{Providers: base.Providers, Qualities: base.Qualities, StremioAddons: base.StremioAddons, PollInterval: "soon", ResolveTimeout: "1m", StreamURLTTL: "1m", MaxResults: 20})
	if err == nil {
		t.Fatal("expected invalid duration error")
	}
}
