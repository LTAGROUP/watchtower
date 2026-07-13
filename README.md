# WatchTower

WatchTower presents debrid-hosted media to Plex as a normal, read-only filesystem. It queries configured Stremio-compatible scraper sources directly, selects cached releases according to a configurable policy, adds them to TorBox or AllDebrid, and publishes a WebDAV tree that an rclone sidecar mounts for Plex. Media bytes are proxied on demand; they are not downloaded to disk.

This project is intended for media you are authorized to access. It does not bypass DRM, operate indexers, or ship scraper definitions.

## Current MVP

- TorBox and AllDebrid provider adapters
- direct aggregation of Torrentio, Comet, StremThru, and other Stremio-compatible scraper endpoints
- automatic polling of approved Seerr movie and TV requests
- independent 2160p and 1080p release selection
- season-pack expansion into Plex-compatible episode paths
- cached-only selection by default
- HTTP Range and HEAD passthrough for seeking and Plex analysis
- bounded sparse-file caching for fast Plex analysis, random seeks, and repeated playback
- automatic URL regeneration and retry after stale links, rate limits, timeouts, and provider 5xx responses
- persistent JSON metadata with atomic updates; no media payload storage
- a read-only WebDAV-to-FUSE mount using rclone

## Data flow

```text
Seerr request -> WatchTower -> configured scraper addons -> TorBox / AllDebrid
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
2. Copy `.env.example` to `.env`, add the Seerr URL/API key and TorBox and/or AllDebrid tokens, then configure `STREMIO_ADDONS`.
3. Set `MEDIA_MOUNT` to an **absolute host path**. Docker mount propagation must be supported by that filesystem. Compose creates the directory when it is absent.
4. Start the core stack:

   ```sh
   docker compose up -d --build
   ```

   To include a fresh Seerr container:

   ```sh
   docker compose --profile bundled up -d --build
   ```

5. Add `${MEDIA_MOUNT}/Movies` and `${MEDIA_MOUNT}/TV` as Plex library roots. If Plex itself runs in Docker, bind the same host path into its container with `rslave` or `rshared` propagation.

6. Configure scraper sources with `STREMIO_ADDONS`. Each entry uses `name|manifest-url` and entries are comma-separated. WatchTower removes the trailing `/manifest.json` and calls the standard Stremio `/stream/{type}/{id}.json` resource directly. The default uses Torrentio; configured Comet and StremThru manifest URLs can be added without code changes.

   ```env
   STREMIO_ADDONS=torrentio|https://torrentio.strem.fun/manifest.json,comet|http://comet:8000/manifest.json
   ```

   Use scraper-only addon configurations and do not embed debrid credentials in these URLs. WatchTower performs provider cache checks and creates stream links itself.

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
| `STREMIO_ADDONS` | Torrentio manifest | Direct scraper addon endpoints |
| `QUALITIES` | `2160p,1080p` | Independent editions to source |
| `ALLOW_UNCACHED` | `false` | Permit a provider to fetch uncached torrents |
| `MIN_SEEDERS` | `0` | Optional minimum seed count; cached results do not require active peers |
| `MAX_RESULTS_PER_QUALITY` | `20` | Search attempts per edition |
| `RESOLVE_TIMEOUT` | `15m` | Maximum provider wait per candidate |
| `STREAM_URL_TTL` | `45m` | Proactive URL refresh interval |
| `LOG_COLOR` | `true` | Use colors and text styling in console logs; set to `false` for plain-text log collectors |
| `SEERR_POLL_INTERVAL` | `2m` | Approved-request polling interval |
| `VFS_CACHE_MAX_SIZE` | `20G` | Maximum transient rclone read cache size |
| `VFS_CACHE_MAX_AGE` | `24h` | Evict cached chunks after their last access |
| `VFS_READ_CHUNK_SIZE` | `4M` | Initial remote range-read size |
| `VFS_READ_CHUNK_SIZE_LIMIT` | `128M` | Maximum sequential chunk size |

Tokens remain in `.env` and are never included in the persisted library metadata or WebDAV paths. The TorBox playback token is sent only to TorBox; the generated URL remains in memory. AllDebrid links are unlocked on demand and likewise remain in memory.

## Operational notes

- The mount container needs `SYS_ADMIN`, `/dev/fuse`, and an unconfined AppArmor profile solely for its FUSE mount. The WatchTower process itself runs unprivileged.
- The rclone sidecar uses `--allow-non-empty` because `/media` is already a Docker bind-mount target before FUSE is layered over it. This does not permit writes; the remote and mount remain read-only.
- Plex must be able to read the mounted directory, and the host bind mount must use shared propagation. A Docker named volume cannot reliably propagate a nested FUSE mount to an unrelated Plex container.
- The rclone cache stores sparse, temporary chunks rather than complete permanent media copies. Plex probes and seeks are served from this cache when possible. Size and retention are bounded by `VFS_CACHE_MAX_SIZE` and `VFS_CACHE_MAX_AGE`.
- State is intentionally simple for this MVP. A database migration should precede multi-replica deployments.
- Provider API shapes occasionally change. Adapter failures are isolated, and the configured provider order provides failover, but live credential tests are still required before production use.

### Clearing a stale FUSE mount

If the mount container was killed without unmounting cleanly, stop the stack and detach the old host mount before starting it again:

```sh
docker compose down
sudo fusermount3 -uz /absolute/path/from/MEDIA_MOUNT
docker compose up -d
```

On systems without `fusermount3`, use `sudo umount -l /absolute/path/from/MEDIA_MOUNT` for the cleanup step.

## Development

The application uses only the Go standard library. Build and test with:

```sh
go test ./...
docker build -t watchtower .
docker compose config
```

Useful endpoints inside the Compose network are `GET /healthz`, `GET /api/v1/library`, `PROPFIND /dav/`, and `POST /webhooks/seerr`. Polling is authoritative; the webhook endpoint is available as a lightweight authenticated wake-up target for a later event-driven implementation.
