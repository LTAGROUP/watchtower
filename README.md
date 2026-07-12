# WatchTower

WatchTower presents debrid-hosted media to Plex as a normal, read-only filesystem. It searches existing indexers through Prowlarr, selects cached releases according to a configurable policy, adds them to TorBox or AllDebrid, and publishes a WebDAV tree that an rclone sidecar mounts for Plex. Media bytes are proxied on demand; they are not downloaded to disk.

This project is intended for media you are authorized to access. It does not bypass DRM, operate indexers, or ship scraper definitions.

## Current MVP

- TorBox and AllDebrid provider adapters
- Prowlarr as the scraper/indexer aggregation layer
- automatic polling of approved Seerr movie and TV requests
- independent 2160p and 1080p release selection
- season-pack expansion into Plex-compatible episode paths
- cached-only selection by default
- HTTP Range and HEAD passthrough for seeking and Plex analysis
- automatic regeneration of expired URLs after a 401, 403, or 404
- persistent JSON metadata with atomic updates; no media payload storage
- a read-only WebDAV-to-FUSE mount using rclone

## Data flow

```text
Seerr request -> WatchTower -> Prowlarr -> TorBox / AllDebrid
                       |                         |
Plex <- host mount <- rclone <- WebDAV/Range proxy
```

The directory layout is conventional Plex naming:

```text
Movies/Title (Year)/Title (Year) - 2160p.mkv
Movies/Title (Year)/Title (Year) - 1080p.mkv
TV/Show/Season 01/Show - S01E01 - 2160p.mkv
TV/Show/Season 01/Show - S01E01 - 1080p.mkv
```

Plex sees the quality variants as multiple files for one movie or episode and can select a playable version.

## Linux setup

1. Install Docker Engine with Compose and ensure `/dev/fuse` exists.
2. Copy `.env.example` to `.env`, add the Prowlarr and Seerr URLs/API keys, then add the TorBox and/or AllDebrid tokens.
3. Set `MEDIA_MOUNT` to an **absolute host path**. Docker mount propagation must be supported by that filesystem.
4. Start the core stack:

   ```sh
   docker compose up -d --build
   ```

   To include fresh Prowlarr and Seerr containers:

   ```sh
   docker compose --profile bundled up -d --build
   ```

5. Add `${MEDIA_MOUNT}/Movies` and `${MEDIA_MOUNT}/TV` as Plex library roots. If Plex itself runs in Docker, bind the same host path into its container with `rslave` or `rshared` propagation.

6. Configure indexers in Prowlarr. WatchTower searches Prowlarr's standard `/api/v1/search` endpoint and accepts magnet or `.torrent` results.

7. Configure Seerr with working Radarr/Sonarr service entries so its UI can create and approve requests. Disable automatic searching on those entries (`preventSearch`) to avoid a second download workflow. WatchTower polls approved requests and updates Seerr media status when files are ready.

Check operation with:

```sh
docker compose logs -f watchtower mount
curl http://localhost:8080/healthz # only if port 8080 is published for debugging
```

The core port is intentionally not published. Add `ports: ["8080:8080"]` under `watchtower` temporarily if direct diagnostics are needed.

## Policy settings

| Variable | Default | Purpose |
|---|---:|---|
| `PROVIDERS` | `torbox,alldebrid` | Provider preference/failover order |
| `QUALITIES` | `2160p,1080p` | Independent editions to source |
| `ALLOW_UNCACHED` | `false` | Permit a provider to fetch uncached torrents |
| `MIN_SEEDERS` | `1` | Ignore unhealthy search results |
| `MAX_RESULTS_PER_QUALITY` | `20` | Search attempts per edition |
| `RESOLVE_TIMEOUT` | `15m` | Maximum provider wait per candidate |
| `STREAM_URL_TTL` | `45m` | Proactive URL refresh interval |
| `SEERR_POLL_INTERVAL` | `2m` | Approved-request polling interval |

Tokens remain in `.env` and are never included in the persisted library metadata or WebDAV paths. The TorBox playback token is sent only to TorBox; the generated URL remains in memory. AllDebrid links are unlocked on demand and likewise remain in memory.

## Operational notes

- The mount container needs `SYS_ADMIN`, `/dev/fuse`, and an unconfined AppArmor profile solely for its FUSE mount. The WatchTower process itself runs unprivileged.
- Plex must be able to read the mounted directory, and the host bind mount must use shared propagation. A Docker named volume cannot reliably propagate a nested FUSE mount to an unrelated Plex container.
- Keep partial-content caching disabled unless the cache has enough disk capacity for your concurrent streams. The default rclone configuration streams directly.
- State is intentionally simple for this MVP. A database migration should precede multi-replica deployments.
- Provider API shapes occasionally change. Adapter failures are isolated, and the configured provider order provides failover, but live credential tests are still required before production use.

## Development

The application uses only the Go standard library. Build and test with:

```sh
go test ./...
docker build -t watchtower .
docker compose config
```

Useful endpoints inside the Compose network are `GET /healthz`, `GET /api/v1/library`, `PROPFIND /dav/`, and `POST /webhooks/seerr`. Polling is authoritative; the webhook endpoint is available as a lightweight authenticated wake-up target for a later event-driven implementation.

