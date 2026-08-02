package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/LTAGROUP/watchtower/internal/config"
	"github.com/LTAGROUP/watchtower/internal/dashboard"
	"github.com/LTAGROUP/watchtower/internal/debrid"
	"github.com/LTAGROUP/watchtower/internal/logging"
	"github.com/LTAGROUP/watchtower/internal/scraper"
	"github.com/LTAGROUP/watchtower/internal/service"
	"github.com/LTAGROUP/watchtower/internal/store"
	dav "github.com/LTAGROUP/watchtower/internal/webdav"
)

func main() {
	logs := logging.NewBuffer(2000)
	log := slog.New(logging.NewTeeHandler(
		logging.NewConsoleHandler(os.Stdout, colorLogs()),
		logs.Handler(slog.LevelDebug),
	))
	cfg := config.Load()
	settings, err := config.OpenManager(cfg)
	if err != nil {
		log.Error("open settings", "error", err)
		os.Exit(1)
	}
	for _, e := range settings.Snapshot().Validate() {
		log.Warn("configuration", "error", e)
	}
	if cfg.DashboardPassword == "watchtower" {
		log.Warn("dashboard is using the default password", "environment", "DASHBOARD_PASSWORD")
	}
	st, err := store.Open(cfg.DataFile)
	if err != nil {
		log.Error("open store", "error", err)
		os.Exit(1)
	}
	apiClient := &http.Client{Timeout: 30 * time.Second}
	torboxGuard := debrid.NewTorBoxGuard(cfg.TorBoxRequestInterval, cfg.TorBoxRateLimitCooldown, cfg.TorBoxUncachedCreateInterval)
	alldebridGuard := debrid.NewProviderGuard(cfg.AllDebridProviderCooldown)
	scraperGuard := scraper.NewRateLimitGuard(cfg.ScraperRateLimitCooldown)
	providerFactory := func(current config.Config) map[string]debrid.Provider {
		providers := map[string]debrid.Provider{}
		if current.TorBoxToken != "" {
			providers["torbox"] = &debrid.TorBox{Token: current.TorBoxToken, Client: apiClient, AllowUncached: current.AllowUncached, Guard: torboxGuard}
		}
		if current.AllDebridToken != "" {
			providers["alldebrid"] = &debrid.AllDebrid{Token: current.AllDebridToken, Client: apiClient, AllowUncached: current.AllowUncached, Guard: alldebridGuard}
		}
		return providers
	}
	scraperFactory := func(current config.Config) (scraper.Searcher, error) {
		addons, err := scraper.ParseAddons(current.StremioAddons)
		if err != nil {
			return nil, err
		}
		return &scraper.Aggregator{Addons: addons, Client: apiClient, Log: log, RateLimitGuard: scraperGuard}, nil
	}
	plex := &service.Plex{Config: cfg, Settings: settings.Snapshot, Store: st, Client: apiClient, Log: log}
	resolver := &service.Resolver{Config: cfg, Settings: settings.Snapshot, Store: st, ScraperFactory: scraperFactory, ProviderFactory: providerFactory, ResolutionConcurrency: cfg.ResolutionConcurrency, LibraryChanged: plex.Notify, Log: log}
	lifecycle := &service.Lifecycle{Store: st, Resolver: resolver, Log: log}
	streamClient := &http.Client{Transport: &http.Transport{MaxIdleConns: 100, MaxIdleConnsPerHost: 20, IdleConnTimeout: 90 * time.Second}, Timeout: 0}
	streamer := &service.Streamer{Store: st, Settings: settings.Snapshot, ProviderFactory: providerFactory, Repair: resolver.Repair, Client: streamClient, TTL: cfg.StreamURLTTL, Log: log}
	seerr := &service.Seerr{Config: cfg, Settings: settings.Snapshot, Store: st, Resolver: resolver, Scheduler: lifecycle, Client: apiClient, Log: log}
	resolver.WorkCompleted = func() {
		lifecycle.Wake()
		seerr.Wake()
	}
	mux := http.NewServeMux()
	mux.Handle("/dav/", &dav.Handler{Store: st, Streamer: streamer, Prefix: "/dav"})
	mux.Handle("/dav", &dav.Handler{Store: st, Streamer: streamer, Prefix: "/dav"})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"status": "ok", "files": len(st.Files())})
	})
	mux.HandleFunc("GET /readyz", readinessHandler(settings, st))
	mux.HandleFunc("GET /api/v1/library", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"media": st.Media(), "files": st.Files()})
	})
	mux.Handle("/webhooks/seerr", seerr.WebhookHandler())
	server := &http.Server{Addr: cfg.ListenAddr, Handler: requestLog(log, mux), ReadHeaderTimeout: 10 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var serviceTasks sync.WaitGroup
	serviceTasks.Add(3)
	go func() { defer serviceTasks.Done(); lifecycle.Run(ctx) }()
	go func() { defer serviceTasks.Done(); seerr.Run(ctx) }()
	go func() { defer serviceTasks.Done(); plex.Run(ctx) }()
	dashboardHandler := (&dashboard.Handler{Store: st, Settings: settings, Resolver: resolver, Seerr: seerr, Scheduler: lifecycle, Username: cfg.DashboardUsername, Password: cfg.DashboardPassword, Log: log, Logs: logs}).Routes()
	dashboardServer := &http.Server{Addr: cfg.DashboardAddr, Handler: requestLog(log, dashboardHandler), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		log.Info("listening", "address", cfg.ListenAddr)
		if e := server.ListenAndServe(); e != nil && e != http.ErrServerClosed {
			log.Error("server", "error", e)
			stop()
		}
	}()
	go func() {
		log.Info("dashboard listening", "address", cfg.DashboardAddr)
		if e := dashboardServer.ListenAndServe(); e != nil && e != http.ErrServerClosed {
			log.Error("dashboard server", "error", e)
			stop()
		}
	}()
	<-ctx.Done()
	shutdown, cancelShutdown := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancelShutdown()
	_ = server.Shutdown(shutdown)
	_ = dashboardServer.Shutdown(shutdown)
	servicesStopped := make(chan struct{})
	go func() {
		serviceTasks.Wait()
		close(servicesStopped)
	}()
	select {
	case <-servicesStopped:
	case <-shutdown.Done():
		log.Warn("service shutdown timed out", "error", shutdown.Err())
	}
}

// readinessHandler is intentionally non-probing. /healthz is the Compose
// liveness endpoint; readiness only reflects the live validated settings and
// whether the durable store opened successfully.
func readinessHandler(settings *config.Manager, st *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		errors := make([]string, 0)
		if settings == nil {
			errors = append(errors, "settings are unavailable")
		} else {
			errors = append(errors, settings.Snapshot().Validate()...)
		}
		if st == nil {
			errors = append(errors, "store is unavailable")
		}
		w.Header().Set("Content-Type", "application/json")
		if len(errors) > 0 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "not_ready", "errors": errors})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "ready", "files": len(st.Files())})
	}
}

func colorLogs() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("LOG_COLOR"))) {
	case "0", "false", "no", "off":
		return false
	default:
		return true
	}
}

func requestLog(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		if r.URL.Path != "/healthz" && r.URL.Path != "/readyz" && r.URL.Path != "/api/v1/logs" {
			log.Info("request", "method", r.Method, "path", r.URL.Path, "duration", time.Since(start).String())
		}
	})
}
