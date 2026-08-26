# motif

Automated theme orchestration for Plex via [ThemerrDB](https://github.com/LizardByte/ThemerrDB).

motif replaces the youdl + mover-script + Plex-checker chain with a single Python service that:

1. Syncs ThemerrDB daily and diffs against local state
2. Downloads new and changed themes from YouTube using your cookies
3. Hardlinks themes into the correct Plex movie/show folder using fuzzy edition-aware matching
4. Triggers a targeted Plex `refresh?force=1` per item to pick up the new local theme — never a full library scan
5. Surfaces everything in a CRT-styled web UI with status, search, manual overrides, and a Plex coverage report

## Architecture

```
                        ┌────────────────────────────────────┐
                        │  motif container                 │
                        │  ┌─────────────────────────────┐   │
   ThemerrDB GitHub ───►│  │  Sync (cron, 13:00 UTC)     │   │
                        │  └────────────┬────────────────┘   │
                        │               │                    │
                        │               ▼                    │
                        │  ┌─────────────────────────────┐   │
                        │  │  SQLite (jobs queue, state) │   │
                        │  └────────────┬────────────────┘   │
                        │               │                    │
                        │               ▼                    │
                        │  ┌─────────────────────────────┐   │
   YouTube + cookies ◄──┤  │  Worker: download → place   │──►├─► Plex API
                        │  │  → trigger refresh          │   │   (refresh)
                        │  └─────────────────────────────┘   │
                        │               ▲                    │
                        │               │                    │
                        │  ┌─────────────────────────────┐   │
                        │  │  FastAPI web UI :5309       │◄──┼─◄ NPM (Authentik)
                        │  └─────────────────────────────┘   │
                        └────────────────────────────────────┘
                                        │
                          /themes (rw)  │  /media/movies (rw)
                                        ▼  /media/tv (rw)
                                  Unraid array
                                  (same fs ⇒ hardlinks)
```

## Installation

motif ships as a single container image at [`healzangels/motif`](https://hub.docker.com/r/healzangels/motif). Three supported install paths: Docker Compose, Unraid Community Apps template, or `docker run`.

### Volume model (since v1.4.0)

motif uses a **two-volume layout** that mirrors what your Plex container sees:

| Container path | Host path (typical) | What it holds |
|----------------|---------------------|---------------|
| `/config`      | `/mnt/user/appdata/motif` | SQLite DB, motif.yaml, cookies.txt, session key |
| `/data`        | `/mnt/user/data`          | Single unified mount that mirrors what Plex sees |

That's it. There are no per-library mounts (`/media/movies`, `/media/tv`, `/themes`). After first boot you choose where motif writes themes from the **/settings page in the UI** — typically `/data/media/themes` — and motif creates `movies/` and `tv/` subdirectories inside it. This setup makes hardlinks work for free because everything lives on the same Unraid User Share.

### Option 1 — Docker Compose

```bash
# Create the appdata directory and drop in cookies + .env
mkdir -p /mnt/user/appdata/motif
cp .env.example /mnt/user/appdata/motif/.env
# Edit .env and set MOTIF_PLEX_TOKEN at minimum

# Drop your YouTube cookies.txt (Netscape format — see Cookies section below)
cp /path/to/cookies.txt /mnt/user/appdata/motif/cookies.txt
chmod 600 /mnt/user/appdata/motif/cookies.txt

# Make sure the appdata is owned by the PUID:PGID motif will run as
# (default 99:100; the entrypoint also chowns /config on start). See
# the Permissions section below if your media share isn't 99:100-owned.
chown -R 99:100 /mnt/user/appdata/motif

# Pull the image and start
cd /mnt/user/appdata/motif
docker compose up -d
docker compose logs -f motif
```

The compose file pulls `healzangels/motif:nightly` from Docker Hub by default. To build locally (for development), edit `docker-compose.yml` and uncomment the `build:` block.

### Option 2 — Unraid Community Apps template

A template XML is provided at `unraid/motif.xml`.

**A. Add the repo URL to Community Applications:**

1. Open Community Applications in the Unraid UI
2. Go to Settings → Template Authors → Add the repository URL
3. Search for "motif" and click Install

**B. Manual template install:**

1. Copy `unraid/motif.xml` to `/boot/config/plugins/dockerMan/templates-user/`
2. In the Unraid UI, go to Docker → Add Container → Template dropdown → "User templates" → motif
3. Fill in the Plex URL and token; the rest defaults to standard Unraid paths
4. Click Apply

### Option 3 — Plain `docker run`

```bash
docker run -d \
  --name motif \
  --restart unless-stopped \
  -p 192.168.1.10:5309:5309 \
  -v /mnt/user/appdata/motif:/config \
  -v /mnt/user/data:/data \
  -e MOTIF_PLEX_URL=http://192.168.1.10:32400 \
  -e MOTIF_PLEX_TOKEN=YOUR_TOKEN_HERE \
  -e PUID=99 -e PGID=100 \
  healzangels/motif:nightly
```

> **Set `PUID`/`PGID` to whatever owns your `/data` media share** (see [Permissions](#permissions--puidpgid) below). The defaults `99:100` are the Unraid `nobody:users` standard; if your Sonarr/Radarr stack runs as `1000`, use `1000` here too — otherwise motif can't write themes or placements into the share.

### Permissions / PUID/PGID

motif runs as a non-root user and writes into your `/data` media tree (theme files, hardlinked sidecars). On a permission-enforcing share — notably **Unraid User Shares (`shfs`), which mount with `default_permissions`** — motif can only write where its **UID/GID owns the files**. If motif's user doesn't match the share's owner, every download, placement, and backup fails with `Permission denied`.

**Set `PUID`/`PGID` to match whoever owns your media share.** Check it:

```bash
# Who owns the media tree motif writes into?
ls -lan /mnt/user/data/media | head
# What does motif currently run as?
docker exec motif id
```

The two must agree. The container defaults to `PUID=99 PGID=100` (Unraid `nobody:users`) — the same identity Sonarr/Radarr/Plex use on a standard Unraid box, so on a normal setup the defaults are correct and you only need to keep your `/data` share owned `99:100`. If your stack runs as a different uid, set `PUID`/`PGID` to match it. motif's entrypoint adopts these at runtime (it `chown`s `/config` to them automatically; it never touches `/data` — that ownership is yours to manage). At startup motif logs a loud `WRITABILITY:` error naming the exact uid/owner mismatch if it can't write, so check the container log first if writes silently do nothing.

**`UMASK` (default `022`)** controls the mode of new files/folders motif creates. The Unraid convention (matching the `*arr`/linuxserver stack) is **`UMASK=002`** — group-writable (dirs `775`, files `664`) — so the whole `99:100` group can cooperatively write the shared `/data` tree **without resorting to `777`**. Set `UMASK=002` to match your stack; leave `022` (owner-only) if motif is the only writer. To keep a share consistently `99:100`-owned, normalize it once with Unraid's **Tools → New Permissions** (or `chown -R 99:100 /mnt/user/<share>`).

> Legacy `--user X:Y` overrides still work and take precedence (PUID/PGID are ignored when an explicit user is forced). Prefer `PUID`/`PGID` env vars — they survive image pulls; a `--user` in a template can silently reset on update.

### First-time setup

1. The web UI lives at `http://YOUR_HOST:5309/`. The first time you load it, you'll be redirected to a one-shot setup page to create the admin username and password.
2. After login, you'll see a red banner: **CONFIGURE THEMES DIRECTORY**. Click into `/settings`, switch to the PATHS tab, and set `THEMES DIRECTORY` to `/data/media/themes` (or wherever you want motif to write theme files inside the /data tree).
3. motif starts in DRY-RUN mode by default. It'll sync ThemerrDB and analyze what to do, but won't download or place anything until you disable dry-run from the banner. Use this to verify matches before committing.
4. Once dry-run is off and the path is set, motif will start downloading.

### First sync

The first sync takes 5–15 minutes. It fetches roughly 4,300 movies and 3,000 TV shows, populates SQLite, and enqueues download jobs for everything. The worker drains that queue at the rate set in **/settings → Downloads → Rate Per Hour** (default 30/hour). Seeding a fresh library to completion takes about 10 days at the default rate; bump to 60–120/hour for faster seeding if your cookies are fresh.

You can kick off a sync immediately from the dashboard or via the API:

```bash
# Generate an admin token at /settings → Tokens first
curl -X POST -H "Authorization: Bearer mtf_..." \
  http://192.168.1.10:5309/api/sync/now
```

## Configuration: motif.yaml + env vars

motif's settings live in **`/config/motif.yaml`** on disk, written by the web UI and persisted across container restarts. The file is automatically created on first boot.

Resolution order (highest priority wins):

1. **Environment variables** (`MOTIF_*`) — always win
2. **motif.yaml** — what the UI writes
3. **Built-in defaults**

This means:
- Set `MOTIF_PLEX_TOKEN` in your env and the UI's token field becomes display-only with a `// ENV OVERRIDE` badge — convenient for keeping secrets out of files
- Don't set the env var and the UI is the source of truth — your saved settings persist across container restarts via motif.yaml
- The UI surfaces which fields are env-locked so you always know

**Migrating from v1.3.x:** drop the old `MOTIF_MOVIES_ROOT` / `MOTIF_TV_ROOT` env vars and the `/themes`, `/media/movies`, `/media/tv` mounts. Add a single `/data:/mnt/user/data` mount. On first boot motif writes a seed motif.yaml from your remaining env vars, then the UI takes over. Visit `/settings → Paths` and set `THEMES DIRECTORY` to `/data/media/themes`.

## Authentication

motif supports two auth modes that coexist, plus API tokens:

**1. Local auth (default).** First time you load the UI, a setup page asks you to create an admin username and password. After that, the app requires a session cookie obtained via `/login`. Sessions last 30 days and live in SQLite. Passwords are bcrypt with cost factor 12. Logout button is the `⏻` icon in the top-right.

**2. Authentik forward-auth.** Set `MOTIF_TRUST_FORWARD_AUTH=true` and motif will trust the `X-Authentik-Username` header (or `X-Forwarded-User`) set by NPM. The local login page is skipped — Authentik handles auth, motif trusts it. You still need to complete the first-run setup once, because:

- the local admin is your fallback if Authentik ever goes down or `MOTIF_TRUST_FORWARD_AUTH` is set back to false
- changing the admin password and managing API tokens still requires a real interactive session for the password change endpoint specifically

**3. API tokens.** From `/settings`, generate tokens scoped `read` (stats + browse only) or `admin` (everything). Pass as `Authorization: Bearer thmr_...` or `?api_key=thmr_...`. Tokens are stored hashed (bcrypt over sha256) — once created, the raw value is shown once and never again.

Two endpoints are public: `/healthz` (liveness — the Docker healthcheck; returns `{"status":"ok"|"degraded", "checks": {…}}` with 503 when a check fails) and `/readyz` (v0.51.268 — local operational readiness: adds config/themes writability and paths-configured, 503s when not ready and names the failing check). Everything else — including `/api/public/stats` — returns 401 (JSON paths) or redirects to `/login` (HTML paths) when unauthenticated.

## Homepage integration

motif exposes `/api/public/stats` for [Homepage](https://gethomepage.dev) and similar dashboards. The payload is counters only — no titles, no paths — but the endpoint requires a `read`-scope API token so a passerby can't probe your library size or sync status. Generate one at `/settings#tokens` and pass it in the `Authorization` header (or `?api_key=` query string):

```yaml
# services.yaml
- Themes:
    - motif:
        href: https://motif.example.com
        icon: mdi-music-note-eighth
        widget:
          type: customapi
          url: https://motif.example.com/api/public/stats
          refreshInterval: 30000
          display: list
          headers:
            Authorization: Bearer thmr_YOUR_READ_TOKEN_HERE
          mappings:
            - field: movies_downloaded
              label: Movies w/ themes
              format: number
            - field: movies_missing
              label: Missing
              format: number
            - field: tv_downloaded
              label: TV w/ themes
              format: number
            - field: queue_pending
              label: Queue
              format: number
            - field: storage_copies_bytes
              label: Wasted on copies
              format: bytes
```

If you want richer authenticated stats, the full `/api/stats` payload (with detailed breakdowns) accepts the same token.

## Dry-run mode

motif ships with `MOTIF_DRY_RUN=true` as the default for fresh deployments. When dry-run is on:

- ThemerrDB sync still runs normally (populates the DB)
- Folder matching still happens (you can see what it *would* place where)
- Plex still gets queried for existing themes
- **No** YouTube downloads happen — cookies aren't touched, no rate-limit tokens consumed
- **No** files are placed into Plex media folders
- **No** Plex refresh API calls

Every "would have done X" decision lands in the event log under `component=dryrun`, so you can review the full picture from `/queue` before flipping the switch.

The dry-run banner appears at the top of every page when active. Disable it from `/settings` (or the inline banner button on any page). The toggle is stored in SQLite, not env, so your choice persists across restarts. The env var only seeds the initial value when the DB is fresh — once you've toggled it once, the env value is ignored.

**Recommended first-run flow:**

1. Deploy with `MOTIF_DRY_RUN=true` (the default)
2. Trigger a sync from the dashboard
3. Wait for the queue to drain (each item produces a `dryrun` event explaining what would happen)
4. Browse `/movies` and `/queue` to spot-check the matching decisions
5. Open a few items via the DETAILS button; verify the planned target folders look right
6. When satisfied, hit DISABLE in the banner. Pending jobs immediately start executing.



motif places themes via `os.link()` first and falls back to `shutil.copy2()` only when the source and destination are on different filesystems. The placement table records which one happened, and the UI surfaces it in three places:

- **Browse table** — the `LINK` column shows `=` (hardlink, green) or `C` (copy, amber) per item
- **Item dialog** — each placement row gets a `[hardlink]` or `[copy]` badge, with a per-item RELINK button on copies
- **Coverage page** — a "STORAGE WASTE" section lists every copy with its byte size, totalled at the top, plus a `RE-LINK ALL` button

If your User Share allocation forces themes and media onto different filesystems, every placement falls back to copy and you get 2x storage. The fix is to fix the share layout (so themes live on the same disk/cache as your movies), then click `RE-LINK ALL`. The relink job walks every copy-mode placement, retries `os.link()`, and atomically swaps the copy for a hardlink where it succeeds. Failures (still cross-FS) leave the copy in place and skip silently.

## Security model

motif is designed to live behind your existing NPM + Authentik setup, with these layers:

1. **Container hardening** — starts as root only to apply PUID/PGID (default 99:100) then `gosu`-drops for the app process; read-only root filesystem, all capabilities dropped except the three the drop itself needs (`SETUID`, `SETGID`, `CHOWN`), `no-new-privileges` set. Only `/config`, `/themes`, and Plex media volumes are writable.
2. **Network exposure** — binds to `192.168.1.10:5309` only (LAN IP), not `0.0.0.0`.
3. **Reverse proxy** — NPM with the standard `Local+VPN only` ACL block applied via custom location config.
4. **App-layer auth** — local password (bcrypt, 30-day session cookies) or Authentik forward-auth, your choice
5. **API tokens** — hashed at rest, scoped per-token, revocable
6. **Secret handling** — Plex token via env, YouTube cookies via volume mount with `0600` perms. The events log scrubs anything matching `token`, `secret`, `password`, `cookie`, or `auth`.

### NPM proxy host (matches your existing pattern)

Create a new proxy host:

* **Domain**: `motif.example.com`
* **Forward Hostname/IP**: `192.168.1.10`
* **Forward Port**: `5309`
* **Block Common Exploits**: ON
* **Cache Assets**: OFF
* **SSL**: Force SSL, HTTP/2, HSTS

In **Custom Locations**, add a single location `/` with this advanced config:

```nginx
# Forward auth via Authentik
auth_request /outpost.goauthentik.io/auth/nginx;
error_page 401 = @goauthentik_proxy_signin;
auth_request_set $authentik_username $upstream_http_x_authentik_username;
auth_request_set $authentik_groups   $upstream_http_x_authentik_groups;
auth_request_set $authentik_email    $upstream_http_x_authentik_email;

proxy_set_header X-Authentik-Username $authentik_username;
proxy_set_header X-Authentik-Groups   $authentik_groups;
proxy_set_header X-Authentik-Email    $authentik_email;
proxy_set_header X-Forwarded-User     $authentik_username;

# (your standard goauthentik_proxy_signin location and outpost.goauthentik.io
#  block from your other Authentik-protected hosts)
```

Then set `MOTIF_TRUST_FORWARD_AUTH=true` in `.env` and restart the container.

If you don't want SSO yet, leave it off and rely on the IP allow list.

## Volume layout & hardlinks

motif stores one MP3 per YouTube video ID in your configured themes directory (e.g. `/data/media/themes/movies/<video_id>.mp3`), then **hardlinks** that file into the matching media folder as `theme.mp3`. Hardlinks have two desirable properties: only one inode of disk usage, and Plex sees a real local file (no symlink weirdness).

In the v1.4.0+ path model with a single `/data` mount mirroring your Unraid User Share, hardlinks "just work" — everything's on the same filesystem. You don't have to think about it.

If you split your data across mounts (don't do this, but it's possible) and end up with hardlinks failing, motif logs:

```
WARNING  placement   Hardlink failed (Invalid cross-device link), falling back to copy
```

The /coverage page surfaces a STORAGE WASTE section listing all such fallback-copies, with a RE-LINK ALL button to retry once you've fixed the layout. If you can't fix it, accept the 2× usage — themes are small (~3 MB each), so 4,300 movies × 3 MB = ~13 GB extra. Not the end of the world.

## Cookies

YouTube's bot detection has gotten worse over time, and cookieless yt-dlp now fails on a non-trivial fraction of videos with a "Sign in to confirm you're not a bot" error. motif requires `cookies.txt` to be present.

**Recommended workflow:**

1. Create a throwaway Google account (don't use your main).
2. Open YouTube in a fresh browser profile, log in to that account.
3. Install [Get cookies.txt LOCALLY](https://chromewebstore.google.com/detail/get-cookies-txt-locally/cclelndahbckbenkjhflpdbgdldlbecc) (or any equivalent).
4. Export cookies for `youtube.com` to Netscape format.
5. Copy to `/mnt/user/appdata/motif/cookies.txt`, `chmod 600`.
6. Refresh the cookies every ~30 days; YouTube rotates session tokens and stale cookies eventually fail.

When cookies expire you'll see download failures with a `CookiesMissingError` event. The worker pauses retries for that item and surfaces it in the Failed filter on the queue page.

## Configuration reference

Most settings are configured from the web UI at **/settings** and saved to `/config/motif.yaml`. Env vars are reserved for first-boot bootstrapping and 12-factor overrides — when set, they always win and the corresponding UI field is shown as `// ENV OVERRIDE`.

### Settings tabs (`/settings`)

| Tab        | What it controls                                                          |
|------------|---------------------------------------------------------------------------|
| PATHS      | `themes_dir` (where motif writes), `cookies_file`                         |
| PLEX       | URL, token, section include/exclude, refresh-after-placement              |
| DOWNLOADS  | rate per hour, concurrency, audio quality, max retries                    |
| MATCHING   | strict edition match, plus-mode                                           |
| SCHEDULE   | sync cron expression, ThemerrDB URL                                        |
| RUNTIME    | dry-run mode, log level                                                   |
| TOKENS     | API token management                                                       |
| PASSWORD   | admin password change                                                      |
| HOMEPAGE   | Custom API widget config example for Homepage dashboard                   |

### Bootstrap env vars

These can be set in `.env` for first-boot bootstrapping. After motif.yaml is written on first boot, env vars become 12-factor overrides — they win over UI saves.

| Variable                      | UI equivalent             | Notes                                  |
|-------------------------------|---------------------------|----------------------------------------|
| `MOTIF_PLEX_URL`              | Plex URL                  | Required                               |
| `MOTIF_PLEX_TOKEN`            | Plex Token                | Required                               |
| `MOTIF_THEMES_DIR`            | Themes Directory          | If unset, must be set via UI on first run |
| `MOTIF_PLEX_SECTION_EXCLUDE`  | Section Exclude           | Comma-separated titles                 |
| `MOTIF_PLEX_SECTION_INCLUDE`  | Section Include           | Comma-separated whitelist              |
| `MOTIF_TVDB_API_KEY`          | TVDB API Key              | Optional, used during scans for orphan resolution |
| `MOTIF_DL_RATE_HOUR`          | Rate per hour             |                                        |
| `MOTIF_DRY_RUN`               | Dry-run                   | Initial value only on fresh DB         |
| `MOTIF_TRUST_FORWARD_AUTH`    | (no UI equivalent)        | Trust X-Authentik-Username             |
| `MOTIF_LOG_LEVEL`             | Log Level                 |                                        |

### Container paths (set by image, not configurable)

| Container path | Purpose                          |
|----------------|----------------------------------|
| `/config`      | Appdata: DB, motif.yaml, cookies |
| `/data`        | Unified data root (mirrors Plex) |

## Libraries

motif auto-discovers your Plex library sections at startup and again before each daily sync. The `/libraries` page lists every section with a per-section placement count, copy count, location paths, and an include checkbox.

By default, motif manages every `movie` and `show` section it finds. To opt out individual libraries, either:

- Set `MOTIF_PLEX_SECTION_EXCLUDE=Home Movies,Music Videos` in your env (titles, comma-separated), or
- Untick the **MGD** checkbox on `/libraries` for the section you want to ignore

If you want a strict whitelist, set `MOTIF_PLEX_SECTION_INCLUDE=Movies,4K Movies,TV Shows,Anime` — only those titles will be managed.

**Note**: env vars apply only on **first discovery** of a section. Once a section is in motif's cache, the UI checkbox wins. This way an env-var change won't clobber your manual UI choices.

motif treats anime libraries (or any section named "Anime") the same as TV — Plex doesn't have a separate "anime" section type, so the section's actual library type (`show`) is what matters. ThemerrDB has anime entries under `tv_shows/` upstream.

## Theme updates

When ThemerrDB updates the YouTube URL for something you already have downloaded, motif does NOT auto-replace it. Instead, the change goes into a `pending_updates` queue, and you decide:

- **ACCEPT** — re-download with the new URL (replaces the existing local file)
- **KEEP** — keep the current theme; mark the update as declined so motif won't nag again until ThemerrDB updates yet again

The topbar shows a cyan **UPD <count>** badge whenever there are pending updates. Click it to land on `/movies?status=updates`. Each row in the updates view has inline ACCEPT and KEEP buttons.

If you've manually overridden a theme via `/settings` and ThemerrDB later updates upstream, the update STILL appears in your queue — but accepting it will only re-download. The override URL in `user_overrides` continues to take precedence at download time. To actually replace a manual theme with the upstream version, clear the override from the item's detail dialog AND accept the update.

## Plex Scans (adopting existing themes)

If your Plex folders already have `theme.mp3` files in them — from a previous Themerr install, manual placement, or another tool — motif can scan them and adopt them rather than re-downloading.

Trigger from the **`/scans`** page (formerly under `/coverage`, moved when that route was retired): click `// SCAN PLEX FOLDERS →`. This enqueues a single-shot scan job that walks every managed section's `location_paths`, hashes every `theme.mp3` in immediate subfolders, and classifies each finding into one of:

| Kind | Meaning | What happens on adopt |
|------|---------|-----------------------|
| `exact_match` | File is already a hardlink to motif's canonical copy | Nothing — already done |
| `hash_match` | Same content as a known motif file but different inode | Re-hardlink so they share an inode |
| `content_mismatch` | Folder has a `theme.mp3` but motif has a different canonical file for this item | User chooses: ADOPT (use Plex's), REPLACE (download motif's), or KEEP (mark as manual override) |
| `orphan_resolvable` | Folder has no matching ThemerrDB record, but `.nfo` or TVDB lookup found IDs | Adopt creates a `themes` row with `upstream_source='plex_orphan'` and the resolved IDs |
| `orphan_unresolved` | No record, no metadata at all | Adopt allocates a synthetic negative `tmdb_id` |

Findings appear on the **`/scans`** page with a triage table. Each row has a per-item decision dropdown, plus a `// BULK ADOPT (HASH MATCHES)` button that auto-collects all hash_match findings on the current page and adopts them in one click. Multi-select with checkboxes for bulk operations on other kinds.

### Orphan resolution

For folders with no matching ThemerrDB record, motif tries two metadata sources in order:

1. **NFO sidecar files** (`<folder>.nfo`, `movie.nfo`, or `tvshow.nfo`) — Sonarr and Radarr write these with `<imdbid>`, `<tmdbid>`, `<tvdbid>`, and `<uniqueid>` blocks. No external API calls needed.
2. **TVDB API** (if you've set `MOTIF_TVDB_API_KEY`) — falls back to `https://api4.thetvdb.com/v4` to search by title+year. Requires a paid TVDB v4 subscription (~$12/year). Without a key, unresolved orphans get a synthetic negative `tmdb_id` and you can still adopt them locally.

### Orphan promotion

When you adopt an orphan with a synthetic negative `tmdb_id`, motif keeps watching during sync. If ThemerrDB later adds a record matching the orphan's `imdb_id`, or its `(title, year)` tuple, sync **promotes** the orphan in place: the negative `tmdb_id` becomes the real one, `upstream_source` flips from `plex_orphan` to `imdb`/`themoviedb`, and the FK'd rows in `local_files` and `placements` follow along. No manual intervention.

### Browse SRC column

Adopted rows show up in `/movies` / `/tv` / `/anime` with a green `A` in the SRC column. Full SRC axis (`T` ThemerrDB / `A` adopted / `U` user URL / `M` manual sidecar / `P` Plex-served / `–` none) is documented in the [Web UI tour](#web-ui-tour) section above. Filter by clicking any SRC letter in the filter row.

## Theme failures

When a download fails, motif classifies the failure:

| Kind | Meaning | What you can do |
|------|---------|-----------------|
| `cookies_expired` | YouTube wants you to "sign in to confirm you're not a bot" | Refresh `/config/cookies.txt` |
| `video_private` | Upstream YouTube video is private | Provide a different URL via override |
| `video_removed` | Video was deleted or made unavailable | Provide a different URL via override |
| `video_age_restricted` | Video requires age verification | Provide a different URL or use a logged-in cookies file |
| `geo_blocked` | Video is region-locked | Use a different network egress |
| `network_error` | Couldn't reach the source | Usually transient — motif retries |
| `rate_limited` | The source is throttling this IP (HTTP 429 / YouTube's rate-limit page) | Transient — motif retries; probes render amber `?`, never a red ✗ (v0.51.269) |
| `unknown` | Something else | Check the event log |

Items in the four "video unavailable" states (`private`, `removed`, `age_restricted`, `geo_blocked`) get a red `⚠` in the **STATUS** column on `/movies` / `/tv` / `/anime`. The topbar shows a red `N FAIL` badge with the unavailable count, linking to `/movies?attn_pills=fail` (migrated from `?status=failures` in v1.13.68 along with the ATTN chip axis). Open the item's detail dialog and paste a working YouTube URL into the override field to fix.

`cookies_expired` is treated as a config-level state, not a per-item problem — affected rows surface with a `⚿` (yellow key) pill in the **TDB** column rather than the red `⚠`. Topbar carries a `cookies` chip; click it to filter to the affected rows. Once cookies are refreshed, all the affected items succeed on retry (or `// REPROBE FAILURES` on `/settings → DOWNLOADS` to re-run them in bulk, bypassing the 24h probe cooldown).

If you've configured Apprise (see [Notifications](#notifications-v1170) below), motif also fires a `cookies_needed` notification on first detect (rate-limited to once per 6 hours so a sync run hitting many cookie-failed rows pings once).

## Web UI tour

* `/` — dashboard with rolled-up stats, last sync results, recent activity, plus a SYNC HISTORY chart and STORAGE WASTE section (copy-mode placements + `// RE-LINK ALL` button — moved here from `/coverage` in v1.8.x)
* `/movies` — searchable, filterable browse with per-item details, manual re-download, and overrides. Column axes:
  - **SRC** — source of the theme: `T` (ThemerrDB-managed), `U` (user URL override), `A` (adopted sidecar — motif owns the inode), `M` (manual sidecar — someone else put theme.mp3 there), `P` (Plex serves its own), `–` (none / dropped). `+P` composites carry a yellow dot indicating Plex also serves an independent theme alongside motif's.
  - **LINK** — placement type: `HL` (hardlink), `C` (copy fallback — different filesystem), `M` (mismatch — file on disk diverges from motif's canonical), `PS` (Plex-serving — let-plex-serve state), `–` (none).
  - **DL** / **PL** — download / placement state (green / amber / red dots).
  - **TDB** — ThemerrDB tracking state (green / blue ↑ / amber ⚠ / red ✗ pills).
  - **STATUS** — pulse-glyph attention markers (un-acked failure, pending update, mismatch, awaiting placement, canonical broken).
* `/tv` — same for TV
* `/anime` — same for Anime (separate tab since v1.8.0)
* `/collections` — same browse for Plex **Collections** (Harry Potter Collection, Spider-Man Collection, etc.) — added in v1.18.0. Collections have no folder on disk; their theme lives inside Plex's metadata bundle, so motif's worker uploads via `POST /library/metadata/{rk}/themes` (the same endpoint themerr-plex used) instead of hardlinking. The SRC axis applies (`T`/`U`/`P`/`–`); `A`/`M` never fire on collections because there's no folder to adopt-from or sidecar-into. Sourced from ThemerrDB's `movie_collections/themoviedb/<tmdb_collection_id>.json` tree.
* `/libraries` — Plex sections with managed/excluded toggles, per-section placement counts, refresh button
* `/queue` — live job queue and full event stream (auto-refreshes every 5s)
* `/scans` — Plex folder scan history and findings triage (adopt/replace/keep/ignore)
* `/settings` — manage API tokens, change admin password, view Homepage widget config, toggle dry-run, configure paths / Plex / downloads / matching / schedule / import / runtime / notifications

## Notifications (v1.17.0+)

motif uses [Apprise](https://github.com/caronc/apprise#supported-notifications) to fan out lifecycle alerts to Discord, Slack, Telegram, Pushover, Gotify, Matrix, email, generic webhooks, and 100+ other services. Configure in `/settings → NOTIFICATIONS`:

* **Apprise URLs** (textarea, one per line) — embedded dispatch. motif's container ships the `apprise` package and sends in-process. Each URL is a notification target with service-schema-first form (e.g. `discord://webhook_id/webhook_token`, `pushover://user_key@app_token`, `tgram://bot_token/chat_id`). See the [Apprise wiki](https://github.com/caronc/apprise/wiki) for service-specific URL examples.
* **External Apprise API URL** (optional) — points at a [caronc/apprise-api](https://github.com/caronc/apprise-api) container's `/notify/{key}` endpoint. motif POSTs `{title, body, type}` to it; the external service handles fan-out via its own configured URLs. Useful if you already run apprise-api for other homelab apps.

Both sinks can be populated. The 🧪 TEST NOTIFICATION button sends a `🧪 motif vX.Y.Z — notification test` message through every configured sink and reports per-sink outcome inline.

Subject lines use a cohesive emoji set so each event type is identifiable at a glance in Discord / Slack / Telegram scrollback (v1.17.19):

| Event | Subject |
|---|---|
| Sync completed | `Motif sync — <state summary>` (the `✅ Sync complete` line closes the body — v1.19.55) |
| Sync failed | `❌ Sync failed` |
| Bulk action completed | `✅ Bulk PROBE TDB done — N/M` / `✅ Bulk LPS done — N` |
| Themes added by sync | `🎵 N new themes added by sync` |
| Theme added | `🎵 Theme added — <title>` |
| Theme removed | `🗑️ Theme unmanaged / forgotten / deleted — <title>` |
| Cookies needed | `🍪 YouTube cookies expired or missing` |
| Disk space low | `💾 Low disk space — XMB free` |
| Worker restarted | `⚠️ Worker restarted (unclean shutdown)` |
| Release update available | `🆕 motif vX.Y.Z available` |

For per-item events (theme added / removed), the body includes a `Source: <provenance> · <platform>` line + the theme URL plain. Discord auto-embeds YouTube + SoundCloud URLs with a rich preview card (thumbnail + title + channel + play); deletion events wrap the URL in `<...>` to suppress the embed since you don't usually want a giant preview on every deletion event.

Per-event toggles control what fires — **20 kinds** as of v0.51.259, every one listed with a one-line hint under **Settings → NOTIFICATIONS** (grouped by family since v0.51.263). The highlights below are a sample, not the full set; theme-loss/recovery kinds (`plex_theme_lost`, `theme_lost_backup_ready`, `theme_lost_sidecar_available`, `theme_auto_restored`, `backup_ready_to_deploy`, `theme_pushed`, `theme_backed_up`, `plex_item_arrived_themed`) live in the settings panel with their defaults. Events that can recur frequently are dedupe-gated (cadence noted) so enabling a toggle doesn't spam your channel:

**ON-by-default (aggregate / actionable):**

* **Sync completed** — after each ThemerrDB sync, with counts of items processed / new / updated / errors
* **Sync failed** — when a sync throws, with the error string + recovery hint
* **Bulk action completed** — one summary per bulk LPS / PROBE TDB
* **Cookies needed** — YouTube cookies expired or missing. Rate-limited to once per 6 hours so a sync run hitting many cookie-failed rows pings once, not N times
* **Disk space low** — themes directory below `paths.min_free_disk_mb`. Rate-limited to once per 12 hours; downloads continue retrying hourly until disk frees up
* **Worker restarted** — fires only when motif's boot zombie-sweep finds stuck-running jobs from a prior session (signals unclean prior shutdown — SIGKILL, OOM, container crash). Clean restarts don't ping

**OFF-by-default (opt-in):**

* **Themes added by sync** — lists titles of newly-added themes after each scheduled sync (sample-capped at 10)
* **Theme added** (per-row) — fires when a place job successfully places a theme. Body includes the source + URL so Discord auto-embeds the YouTube preview. No dedupe — a bulk download of N rows fires N notifications
* **Theme deleted** (per-row) — UNMANAGE / FORGET / DELETE actions. Useful for audit-trail surfaces
* **Theme available to add** (opt-in) — after a Plex refresh, fires when a *newly-added* Plex item matches a ThemerrDB theme but has no theme yet (no Plex theme, nothing in motif). Tells you that you could add one. Bulk-shaped (one digest per refresh) + per-title dedupe; only genuinely-new items fire (your existing themeless library stays surfaced via the in-app `// UPD` badge)
* **Release update available** — daily release-check found a new motif version on GitHub. Tag-deduped — each new release pings exactly once across motif process restarts

**In-app inbox.** Alongside the external sinks, the topbar **INBOX** pill collects notifications in-app (10 kinds, per-kind toggles). Reading is per-notification since v0.51.266 — clicking (or pressing Enter on) a row marks that row read, `// MARK ALL READ` does the bulk action, and `// CLEAR ALL` dismisses. The badge counts unread until you act, not until you peek.

All notification events are UI-surfaced under **Settings → NOTIFICATIONS**; each toggle also maps to a `notifications.events.<kind>` key reachable via `PATCH /api/config`. (v1.21.5 shipped `new_tdb_theme_available` — the "theme available to add" event above — by hooking the sparse plex_enum refresh path instead of `resolve_theme_ids`'s hot loop, which is what kept it deferred from v1.17.0.)

The notification severity (info / warning / failure) is automatically routed per event kind so notification services with severity-based filtering (Discord embed color, Pushover priority, etc.) render `sync_failed` / `cookies_needed` / `disk_low` / `worker_restarted` with the correct urgency.

Apprise URLs are masked (`<scheme>://***`) in the GET `/api/config` response so an admin-scope API token leak doesn't expose embedded webhook credentials (v1.17.13). The PATCH handler positionally substitutes masked entries with the existing URL at the same slot — same `***` ↔ "keep existing" contract as `plex.token` / `tmdb_api_key`. Schemes outside the safe set (`file://`, `javascript:`, etc.) are rejected at save time.

If a notification fails to dispatch (network blip, bad URL, service down), motif logs the failure to `/queue events` with `component='notify'` rather than retrying or surfacing a topbar pill. Check there to debug if a test send works but live events don't.

## API endpoints

All endpoints return JSON unless they're HTML pages. Authentication is via session cookie (interactive), `Authorization: Bearer` token (scripts), or the forward-auth header. Only `/healthz` and `/readyz` are public.

| Method | Path                                            | Auth     | Purpose                                |
|--------|-------------------------------------------------|----------|----------------------------------------|
| GET    | `/healthz`                                      | public   | Liveness                               |
| GET    | `/readyz`                                       | public   | Local readiness (names failing checks) |
| GET    | `/api/public/stats`                             | read     | Counters for Homepage etc.             |
| GET    | `/api/stats`                                    | read     | Full dashboard JSON                    |
| GET    | `/api/library?media_type=movie&page=1&q=…&attn_pills=…&src_pills=…` | read   | Paginated browse (replaced `/api/items` LIST in v1.14.41) |
| GET    | `/api/items/{movie\|tv}/{tmdb_id}`              | read     | Item detail                            |
| GET    | `/api/libraries`                                | read     | List Plex sections                     |
| POST   | `/api/libraries/refresh`                        | admin    | Re-discover from Plex                  |
| POST   | `/api/libraries/{id}/include`                   | admin    | Toggle managed status                  |
| GET    | `/api/updates`                                  | read     | List pending upstream updates          |
| GET    | `/api/updates/count`                            | read     | Pending update count                   |
| POST   | `/api/updates/{movie\|tv}/{tmdb_id}/accept`     | admin    | Accept upstream update                 |
| POST   | `/api/updates/{movie\|tv}/{tmdb_id}/decline`    | admin    | Keep current theme                     |
| GET    | `/api/jobs?status=…&limit=…`                    | read     | Job queue                              |
| GET    | `/api/events?limit=…&component=…&level=…`       | read     | Event log                              |
| GET    | `/api/coverage/plex`                            | read     | Plex library coverage report           |
| GET    | `/api/storage/copies`                           | read     | All copy-mode placements               |
| POST   | `/api/items/{movie\|tv}/{tmdb_id}/redownload`   | admin    | Force re-download                      |
| POST   | `/api/items/{movie\|tv}/{tmdb_id}/override`     | admin    | Manual YouTube URL (form: youtube_url) |
| DELETE | `/api/items/{movie\|tv}/{tmdb_id}/override`     | admin    | Clear override                         |
| POST   | `/api/items/{movie\|tv}/{tmdb_id}/relink`       | admin    | Retry hardlink for one item            |
| POST   | `/api/storage/relink`                           | admin    | Sweep all copies, retry hardlinks      |
| POST   | `/api/sync/now`                                 | admin    | Enqueue immediate sync                 |
| GET    | `/api/tokens`                                   | admin    | List API tokens                        |
| POST   | `/api/tokens`                                   | admin    | Create token (form: name, scope)       |
| DELETE | `/api/tokens/{token_id}`                        | admin    | Revoke token                           |
| POST   | `/api/admin/password`                           | session  | Change admin password                  |
| POST   | `/api/admin/test-cookies`                       | admin    | Validate `/config/cookies.txt` (v1.15.8) |
| POST   | `/api/admin/test-notification`                  | admin    | Synchronously fire a test through every configured Apprise sink (v1.17.0) |
| POST   | `/api/admin/reprobe-plex-themes`                | admin    | Background re-probe of every Plex theme placement (v1.13.40) |
| POST   | `/api/admin/bulk-probe-tdb`                     | admin    | Bulk yt-dlp `--simulate` sweep of every theme's TDB URL (v1.14.29, scope_items v1.15.1) |
| POST   | `/api/admin/reprobe-tdb-failures`               | admin    | Recovery sweep — re-checks every red-pilled row, bypasses 24h cooldown (v1.15.10) |
| POST   | `/api/admin/bulk-let-plex-serve`                | admin    | Server-side composite — probe + unplace for a selection of +P composites (v1.15.28) |
| GET    | `/api/config`                                   | admin    | Current motif.yaml as JSON                |
| PATCH  | `/api/config`                                   | admin    | Partial config update (merges into motif.yaml) |

## Troubleshooting

### Downloads all fail with "Sign in to confirm you're not a bot"

Your cookies are stale or missing. Re-export and replace `/config/cookies.txt`. Watch the logs after; if it persists, your throwaway Google account may need to log in via a real browser session to refresh tokens server-side.

### Themes placed but Plex doesn't show them

1. Check the placement event log entry — does it report `plex_refreshed: true`?
2. If yes but Plex still doesn't show it, your Plex section's "Use local assets" advanced setting may be off. Settings → Library → Edit → Advanced → Use local assets: ON.
3. If motif couldn't resolve a Plex ratingKey, the analyze step is skipped. The theme is still on disk; Plex will pick it up on its own next library scan, just slower.

### "Ambiguous yearless matches" in logs

You have multiple folders with the same title and no year disambiguator. Either rename one to include `(YEAR)` or add an `{edition-...}` tag. motif refuses to place ambiguous matches to avoid putting the wrong theme on the wrong movie.

### The X-Men foot-gun

The normalizer (faithfully ported from your `merge-themes.sh`) treats `x` as Roman numeral 10, so `X-Men` normalizes to `10 men`. Since the same normalizer is applied to both the source title and the destination folder name, matches still succeed — but if you ever switch to a different normalizer, all your X-Men folders will need attention. Documented behavior, not a bug.

### Hardlinks fall back to copy

See "Volume layout & hardlink gotcha" above.

### Container won't start: "could not write session key"

`/config` isn't writable as UID 99. On Unraid, `chown -R 99:100 /mnt/user/appdata/motif`.

## Development

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
mkdir -p /tmp/motif-dev/{config,themes,media/movies,media/tv}
MOTIF_CONFIG_DIR=/tmp/motif-dev/config \
MOTIF_THEMES_DIR=/tmp/motif-dev/themes \
MOTIF_MOVIES_ROOT=/tmp/motif-dev/media/movies \
MOTIF_TV_ROOT=/tmp/motif-dev/media/tv \
MOTIF_PLEX_ENABLED=false \
python -m app.main
```

UI at `http://localhost:5309`.

## License

MIT.
