# Playback code review — 2026-09-07

Reviewed the WebDAV → stream proxy → provider path, source resolution and repair, persistent file metadata, rclone mount configuration, and related lifecycle/dashboard code. The findings below are fixed in the working tree.

## Playback findings and fixes

| Priority | Finding | Fix |
| --- | --- | --- |
| P1 | AllDebrid file resolution used a scalar `id` and read file arrays from the wrong response level, leaving ready torrents without playable files. | Send `id[]`, select the matching `data.magnets` entry, accept numeric/string IDs, and preserve nested file paths. |
| P1 | TorBox published torrent file metadata before the download was available. | Require both `download_present` and `download_finished` before publishing files. |
| P1 | Automatic repair could replace an active file with a different torrent/encode while rclone retained old byte ranges. | Require the original info hash and matching file size. Restore the stored source directly when available, with provider fallback. |
| P1 | Interrupted or short chunked transfers could return normally, marking an incomplete media response as finished. | Verify transferred length against the range/file metadata and abort the HTTP response on transfer failure. |
| P2 | Partial-response validation accepted absent/malformed Content-Range, incorrect suffix offsets, and ranges exceeding the requested end or file size. | Validate those cases before forwarding bytes. Preserve valid multipart range responses and request identity encoding. |
| P2 | Canceling a Plex probe could fail another request waiting for its URL refresh. Successful refreshes could also erase concurrent provider cooldowns. | Retry canceled shared refreshes for surviving callers and retain the longest active cooldown. |
| P2 | After repair switched providers, the stream request still used the old provider's metadata for cooldowns and validation. | Return the source snapshot together with the refreshed URL. |
| P2 | A slow refresh could cache its old URL against a source replaced under the same file ID. | Cache URLs only if the current durable source still matches the snapshot used to generate them. |
| P2 | AllDebrid unavailable links were generic errors, bypassing automatic repair; temporary link/transport failures bypassed retry handling. | Classify stale and transient failures, reject invalid upload IDs, and recognize terminal magnet status codes. |
| P2 | Movie results were capped before quality filtering, allowing popular results in one quality to hide all candidates in another. | Filter and rank before applying the per-quality limit, for automatic and manual resolution. |
| P2 | A season-pack folder containing an episode marker could override the actual episode filename. | Prefer the filename's episode marker, then the nearest enclosing folder. |
| P2 | WebDAV decoded URL paths twice, breaking literal `%`, `%20`, and `%2F` in media titles. | Use net/http's already-decoded URL.Path. |
| P2 | Every WebDAV file had an epoch modification time, and rclone disabled modification-time checks. Same-size replacements could retain stale cached bytes. | Publish each file's creation/publication time, enable mount modtime checks, and keep GET/HEAD validators consistent with PROPFIND. |
| P2 | Stream connection setup and response headers had no explicit deadlines. | Use the standard dial/TLS timeouts and a 30-second response-header timeout, without imposing a whole-movie timeout. |
| P2 | TorBox cache checks searched raw response text for a hash, allowing false positives from messages and null entries. | Parse successful structured cache data. |
| P2 | Empty Movies/TV directories did not exist until files were published. | Expose both library roots even in an empty library. |

## Incidental security fix

The detail dialog interpolated an unvalidated poster path into HTML. It now applies the same restrictive image-path validation used by the discovery grid, preventing the path from injecting HTML attributes or elements.

## Verification

Added 21 regression test functions, including table-driven cases for malformed ranges, repair identity, provider responses, escaped paths, and concurrent URL refreshes. Existing partial-content fixtures now supply valid Content-Range headers.

Passed:

- `go test -race ./...`
- `go vet ./...`
- `GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build ./cmd/watchtower`
- `node --check internal/dashboard/web/app.js`
- `docker compose config --quiet`
- `git diff --check`

Go 1.23.12 was downloaded into a temporary directory because Go was absent from PATH. Tests use local HTTP servers and simulated providers; no live provider accounts, running Plex instance, or FUSE mount were exercised. The changes have not been deployed.

Automatic repair intentionally cannot switch to a different encode. If the original torrent is unavailable, select/rescrape a replacement in the dashboard and reopen playback so the mount can refresh its file/cache state. Older files without an info hash also require explicit replacement rather than automatic byte substitution.

Provider response contracts were checked against the [AllDebrid API documentation](https://docs.alldebrid.com/) and [TorBox API documentation](https://www.postman.com/torbox/torbox-api/documentation/b6l9hbv/main-api). The mount change follows [rclone's documented cache fingerprinting](https://rclone.org/commands/rclone_mount/#fingerprinting).
