# motif

**Automated theme orchestration for Plex via [ThemerrDB](https://github.com/LizardByte/ThemerrDB).**

motif syncs ThemerrDB daily, downloads new YouTube themes via your cookies, hardlinks them into your Plex movie/show folders with edition-aware matching, and triggers per-item Plex refreshes — never a full library scan. Everything is surfaced in a CRT-styled web UI on port 5309 with status, search, manual overrides, and Plex coverage reports.

For full documentation (architecture, troubleshooting, advanced topics, contributing), see the **[GitHub repository](https://github.com/Healzangels/motif)**.

---

## Quick start

motif uses a two-volume layout that mirrors what your Plex container sees:

| Container path | Host path (typical) | What it holds |
|---|---|---|
| `/config` | `/mnt/user/appdata/motif` | SQLite DB, motif.yaml, cookies.txt, session key |
| `/data` | `/mnt/user/data` | Single unified mount that mirrors what Plex sees |

After first boot you choose where motif writes themes from the `/settings` page in the UI — typically `/data/media/themes`.

### Option 1 — Docker Compose

```yaml
services:
  motif:
    image: healzangels/motif:latest
    container_name: motif
    restart: unless-stopped
    ports:
      - "192.168.1.10:5309:5309"
    volumes:
      - /mnt/user/appdata/motif:/config
      - /mnt/user/data:/data
    environment:
      MOTIF_PLEX_URL: http://192.168.1.10:32400
      MOTIF_PLEX_TOKEN: YOUR_TOKEN_HERE
    user: "99:100"
```

```bash
mkdir -p /mnt/user/appdata/motif
chown -R 99:100 /mnt/user/appdata/motif
# Drop YouTube cookies (Netscape format)
cp /path/to/cookies.txt /mnt/user/appdata/motif/cookies.txt
chmod 600 /mnt/user/appdata/motif/cookies.txt
docker compose up -d
docker compose logs -f motif
```

### Option 2 — Plain `docker run`

```bash
docker run -d \
  --name motif \
  --restart unless-stopped \
  -p 192.168.1.10:5309:5309 \
  -v /mnt/user/appdata/motif:/config \
  -v /mnt/user/data:/data \
  -e MOTIF_PLEX_URL=http://192.168.1.10:32400 \
  -e MOTIF_PLEX_TOKEN=YOUR_TOKEN_HERE \
  --user 99:100 \
  healzangels/motif:latest
```

### Option 3 — Unraid Community Apps

A template XML is at `unraid/motif.xml` in the GitHub repo. Drop it under `/boot/config/plugins/dockerMan/templates-user/` then add the container from the Unraid Docker UI.

---

## First-time setup

1. Open `http://YOUR_HOST:5309/`. The first load redirects to a one-shot setup page to create the admin username + password.
2. After login, you'll see a red banner: **CONFIGURE THEMES DIRECTORY**. Click into `/settings` → PATHS tab → set `THEMES DIRECTORY` to `/data/media/themes` (or wherever you want motif to write themes inside the `/data` tree).
3. motif starts in **DRY-RUN mode** by default. It'll sync ThemerrDB and analyze what to do, but won't download or place anything until you disable dry-run from the banner. Use this to verify matches before committing.
4. Once dry-run is off and the path is set, motif will start downloading.

---

## Required environment variables

| Variable | Purpose |
|---|---|
| `MOTIF_PLEX_URL` | Plex server URL, e.g. `http://192.168.1.10:32400` |
| `MOTIF_PLEX_TOKEN` | Plex token (Settings → Network → Show Advanced → "X-Plex-Token") |

Optional env vars and full configuration reference live in the [GitHub README](https://github.com/Healzangels/motif#configuration).

---

## Cookies

motif uses `yt-dlp` with your YouTube cookies to download themes. Drop a Netscape-format `cookies.txt` into `/config/`:

```bash
chmod 600 /mnt/user/appdata/motif/cookies.txt
```

The file's owner must match the container user (default `99:100` for Unraid). Use a browser extension like "Get cookies.txt LOCALLY" to export your YouTube session cookies in Netscape format.

Without `cookies.txt` motif will still run but most YouTube downloads will fail (age-restricted / region-locked / login-required content).

---

## Image tags

| Tag | Updates |
|---|---|
| `latest` | Latest stable release |
| `1` | Latest v1.x.y |
| `1.15` | Latest v1.15.x |
| `1.15.0` | Pinned exact version |

---

## Reverse proxy (optional)

motif works behind any reverse proxy. With Authentik forward-auth, set:

```
MOTIF_TRUST_FORWARD_AUTH=1
```

motif will read `X-Authentik-Username` from the proxy and auto-create local accounts on first request. Without it, motif uses its own login flow (bcrypt-hashed sessions).

---

## What motif does NOT do

- **Scan your library**: motif drives off ThemerrDB upstream + targeted per-item Plex refreshes. No full library scans, no metadata refreshes beyond the theme.
- **Modify your media files**: motif only writes to `/themes` and hardlinks into Plex folders. Originals are untouched.
- **Run without Plex**: motif assumes Plex is the consumer; it can't operate standalone.

---

## Support + contributing

- Issues: [GitHub Issues](https://github.com/Healzangels/motif/issues)
- Source: [github.com/Healzangels/motif](https://github.com/Healzangels/motif)
- License: MIT

For the full README (architecture diagrams, troubleshooting, recipe collections, contributor guide), see the GitHub repo.
