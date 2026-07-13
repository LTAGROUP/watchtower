package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
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
	log := slog.New(logging.NewConsoleHandler(os.Stdout, colorLogs()))
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
	providerFactory := func(current config.Config) map[string]debrid.Provider {
		providers := map[string]debrid.Provider{}
		if current.TorBoxToken != "" {
			providers["torbox"] = &debrid.TorBox{Token: current.TorBoxToken, Client: apiClient, AllowUncached: current.AllowUncached}
		}
		if current.AllDebridToken != "" {
			providers["alldebrid"] = &debrid.AllDebrid{Token: current.AllDebridToken, Client: apiClient, AllowUncached: current.AllowUncached}
		}
		return providers
	}
	scraperFactory := func(current config.Config) (scraper.Searcher, error) {
		addons, err := scraper.ParseAddons(current.StremioAddons)
		if err != nil {
			return nil, err
		}
		return &scraper.Aggregator{Addons: addons, Client: apiClient, Log: log}, nil
	}
	resolver := &service.Resolver{Config: cfg, Settings: settings.Snapshot, Store: st, ScraperFactory: scraperFactory, ProviderFactory: providerFactory, Log: log}
	streamClient := &http.Client{Transport: &http.Transport{MaxIdleConns: 100, MaxIdleConnsPerHost: 20, IdleConnTimeout: 90 * time.Second}, Timeout: 0}
	streamer := &service.Streamer{Store: st, Settings: settings.Snapshot, ProviderFactory: providerFactory, Repair: resolver.Repair, Client: streamClient, TTL: cfg.StreamURLTTL, Log: log}
	mux := http.NewServeMux()
	mux.Handle("/dav/", &dav.Handler{Store: st, Streamer: streamer, Prefix: "/dav"})
	mux.Handle("/dav", &dav.Handler{Store: st, Streamer: streamer, Prefix: "/dav"})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"status": "ok", "files": len(st.Files())})
	})
	mux.HandleFunc("GET /api/v1/library", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"media": st.Media(), "files": st.Files()})
	})
	mux.HandleFunc("POST /webhooks/seerr", func(w http.ResponseWriter, r *http.Request) {
		if cfg.WebhookSecret != "" && r.Header.Get("Authorization") != "Bearer "+cfg.WebhookSecret {
			http.Error(w, "unauthorized", 401)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	})
	server := &http.Server{Addr: cfg.ListenAddr, Handler: requestLog(log, mux), ReadHeaderTimeout: 10 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	seerr := &service.Seerr{Config: cfg, Settings: settings.Snapshot, Store: st, Resolver: resolver, Client: apiClient, Log: log}
	go seerr.Run(ctx)
	dashboardHandler := (&dashboard.Handler{Store: st, Settings: settings, Resolver: resolver, Seerr: seerr, Username: cfg.DashboardUsername, Password: cfg.DashboardPassword, Log: log}).Routes()
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
	shutdown, _ := context.WithTimeout(context.Background(), 15*time.Second)
	_ = server.Shutdown(shutdown)
	_ = dashboardServer.Shutdown(shutdown)
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
		if r.URL.Path != "/healthz" {
			log.Info("request", "method", r.Method, "path", r.URL.Path, "duration", time.Since(start).String())
		}
	})
}
