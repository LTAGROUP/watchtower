package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	ListenAddr, DataFile                       string
	PublicBaseURL, WebhookSecret               string
	ProwlarrURL, ProwlarrAPIKey                string
	SeerrURL, SeerrAPIKey                      string
	TorBoxToken, AllDebridToken                string
	Providers, Qualities                       []string
	PollInterval, ResolveTimeout, StreamURLTTL time.Duration
	MinSeeders                                 int
	MaxResults                                 int
	AllowUncached                              bool
}

func Load() Config {
	return Config{
		ListenAddr: env("LISTEN_ADDR", ":8080"), DataFile: env("DATA_FILE", "/data/state.json"),
		PublicBaseURL: strings.TrimRight(env("PUBLIC_BASE_URL", "http://watchtower:8080"), "/"),
		WebhookSecret: os.Getenv("WEBHOOK_SECRET"),
		ProwlarrURL:   strings.TrimRight(os.Getenv("PROWLARR_URL"), "/"), ProwlarrAPIKey: os.Getenv("PROWLARR_API_KEY"),
		SeerrURL: strings.TrimRight(os.Getenv("SEERR_URL"), "/"), SeerrAPIKey: os.Getenv("SEERR_API_KEY"),
		TorBoxToken: os.Getenv("TORBOX_TOKEN"), AllDebridToken: os.Getenv("ALLDEBRID_TOKEN"),
		Providers: csv(env("PROVIDERS", "torbox,alldebrid")), Qualities: csv(env("QUALITIES", "2160p,1080p")),
		PollInterval: duration("SEERR_POLL_INTERVAL", 2*time.Minute), ResolveTimeout: duration("RESOLVE_TIMEOUT", 15*time.Minute),
		StreamURLTTL: duration("STREAM_URL_TTL", 45*time.Minute), MinSeeders: integer("MIN_SEEDERS", 1),
		MaxResults: integer("MAX_RESULTS_PER_QUALITY", 20), AllowUncached: boolean("ALLOW_UNCACHED", false),
	}
}

func (c Config) Validate() []string {
	var out []string
	if c.ProwlarrURL == "" || c.ProwlarrAPIKey == "" {
		out = append(out, "PROWLARR_URL and PROWLARR_API_KEY are required")
	}
	if c.SeerrURL == "" || c.SeerrAPIKey == "" {
		out = append(out, "SEERR_URL and SEERR_API_KEY are required")
	}
	if c.TorBoxToken == "" && c.AllDebridToken == "" {
		out = append(out, "at least one debrid token is required")
	}
	return out
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func csv(v string) []string {
	var r []string
	for _, s := range strings.Split(v, ",") {
		if s = strings.TrimSpace(strings.ToLower(s)); s != "" {
			r = append(r, s)
		}
	}
	return r
}
func duration(k string, d time.Duration) time.Duration {
	if v, e := time.ParseDuration(os.Getenv(k)); e == nil && v > 0 {
		return v
	}
	return d
}
func integer(k string, d int) int {
	if v, e := strconv.Atoi(os.Getenv(k)); e == nil {
		return v
	}
	return d
}
func boolean(k string, d bool) bool {
	if v := os.Getenv(k); v != "" {
		b, e := strconv.ParseBool(v)
		if e == nil {
			return b
		}
	}
	return d
}
