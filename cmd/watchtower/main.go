package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/LTAGROUP/watchtower/internal/config"
	"github.com/LTAGROUP/watchtower/internal/debrid"
	"github.com/LTAGROUP/watchtower/internal/scraper"
	"github.com/LTAGROUP/watchtower/internal/service"
	"github.com/LTAGROUP/watchtower/internal/store"
	dav "github.com/LTAGROUP/watchtower/internal/webdav"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg := config.Load()
	for _, e := range cfg.Validate() {
		log.Warn("configuration", "error", e)
	}
	st, err := store.Open(cfg.DataFile)
	if err != nil {
		log.Error("open store", "error", err)
		os.Exit(1)
	}
	apiClient := &http.Client{Timeout: 30 * time.Second}
	providers := map[string]debrid.Provider{}
	if cfg.TorBoxToken != "" {
		providers["torbox"] = &debrid.TorBox{Token: cfg.TorBoxToken, Client: apiClient, AllowUncached: cfg.AllowUncached}
	}
	if cfg.AllDebridToken != "" {
		providers["alldebrid"] = &debrid.AllDebrid{Token: cfg.AllDebridToken, Client: apiClient, AllowUncached: cfg.AllowUncached}
	}
	addons, err := scraper.ParseAddons(cfg.StremioAddons)
	if err != nil {
		log.Error("configure scrapers", "error", err)
		os.Exit(1)
	}
	scrapers := &scraper.Aggregator{Addons: addons, Client: apiClient, Log: log}
	resolver := &service.Resolver{Config: cfg, Store: st, Scraper: scrapers, Providers: providers, Log: log}
	streamClient := &http.Client{Transport: &http.Transport{MaxIdleConns: 100, MaxIdleConnsPerHost: 20, IdleConnTimeout: 90 * time.Second}, Timeout: 0}
	streamer := &service.Streamer{Store: st, Providers: providers, Client: streamClient, TTL: cfg.StreamURLTTL, Log: log}
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
	seerr := &service.Seerr{Config: cfg, Store: st, Resolver: resolver, Client: apiClient, Log: log}
	if cfg.SeerrURL != "" && cfg.SeerrAPIKey != "" {
		go seerr.Run(ctx)
	}
	go func() {
		log.Info("listening", "address", cfg.ListenAddr)
		if e := server.ListenAndServe(); e != nil && e != http.ErrServerClosed {
			log.Error("server", "error", e)
			stop()
		}
	}()
	<-ctx.Done()
	shutdown, _ := context.WithTimeout(context.Background(), 15*time.Second)
	_ = server.Shutdown(shutdown)
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
