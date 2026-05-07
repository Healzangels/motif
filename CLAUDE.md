# motif — context for Claude Code sessions

Single-tenant FastAPI + SQLite + yt-dlp + dulwich service that
automates Plex theme orchestration from ThemerrDB. Single developer
(Connor), homelab scale, deployed via Docker on Unraid behind NPM
+ Authentik. CRT-themed UI on port 5309.

For deep-dive context — recurring bug classes, architecture pivots,
schema migration history, things-that-were-tried-and-removed —
read **`docs/PROJECT_HISTORY.md`** (a structured digest of every
tagged release v1.4.0 → current). When debugging, that file is the
first place to look for the WHY behind a piece of code.

## Stack

| Layer | Detail |
|---|---|
| API + UI | FastAPI on `:5309`; Jinja2 templates in `app/web/templates/` |
| DB | SQLite at `/config/motif.db`; current schema **v38**, migrations in `app/core/db.py` |
| Worker | APScheduler cron (sync) + custom job loop (downloads/place/refresh) |
| Sync transport | tiered: git (dulwich differential) → snapshot (database branch tarball) → remote (per-item HTTP) |
| Download | yt-dlp with `cookies.txt` from `/config` |
| Placement | hardlink-first via `os.link()`, fallback to `shutil.copy2()` if cross-FS |
| Auth | local bcrypt session cookie OR `X-Authentik-Username` forward-auth |

Two-volume container layout: `/config` (appdata) + `/data` (mirrors
Plex's view of the filesystem so hardlinks work).

## SRC letter axis (the row pill across /movies, /tv, /anime)

| Letter | Source kind | Notes |
|---|---|---|
| `T` | ThemerrDB-managed | upstream_source ∈ ('imdb','themoviedb') |
| `A` | Adopted | sidecar matched, motif owns the inode |
| `U` | User URL | manual youtube_url override in user_overrides |
| `M` | Manual sidecar | someone else put theme.mp3 there |
| `P` | Plex-served | themerr-plex embed / Plex Pass cloud |
| `–` | none / dropped | no theme, or `tdb_dropped_at` set |

Same axis renders three places — keep them aligned when changing:
- DB read path: `_SRC_LETTER_SQL` in `app/web/api.py`
- Client logic: `computeSrcLetter` in `app/web/static/app.js`
- Dashboard donut: `renderThemeSourcePie`

## Recurring bug classes (read PROJECT_HISTORY for full detail)

1. **Phantom P after PURGE** — Plex's metadata cache returns 200 to
   `/library/metadata/{rk}/theme` for several seconds after motif
   unlinks the file. Inline HEAD verify must skip rks where motif
   owned the placement (`rk_from_placement` set in `api.py`).
2. **Edition-sibling theme propagation** — `theme=` XML attribute
   propagates between standard/4K editions. Per-section scoping is
   essential; `local_files.section_id` is part of the PK (since v18).
3. **UNIQUE conflicts on orphan promotion** — synthetic-tmdb rows
   getting promoted into a target ID that has stale children. Pattern:
   pre-delete child rows at target then `PRAGMA defer_foreign_keys=ON`
   inside the txn.
4. **innerHTML flicker on poll** — naive tbody.innerHTML rewrites
   blow scroll position. Pattern: `tbody.dataset.lastHash` skip.
5. **Button text race** — don't swap `button.textContent` mid-flight.
   Use adjacent pills (DL/PL) for state. Optimistic placeholder via
   `motifOps.setOptimisticPlaceholder` covers the click→busy gap.
6. **Race between syncWatcher and refreshTopbarStatus** — when both
   own a button's lifecycle. Guard the unlock with `!syncWatcher`.
7. **/api/stats 1s TTL cache** — post-action refreshes need a
   `setTimeout(..., 1100)` to land past the cache.
8. **`database is locked`** — retry only that string in
   `OperationalError`; other OperationalErrors propagate (schema bugs).

## Coding conventions

- **Inline `# vX.Y.Z:` markers** on load-bearing lines, explaining
  WHY the change had to happen. They're searchable archaeology — the
  PROJECT_HISTORY entry tells the full story, the marker is the breadcrumb.
- **Comments lean on WHY**, reference prior bugs/incidents. Skip
  comments that explain WHAT (the code already does that).
- **Single-line comments only.** No multi-line block comments, no
  docstring-style narration. One short line, max.
- **Never delete a `# vX.Y.Z:` marker** unless deleting the line it
  guards.
- **No "fallback" branches or feature flags** for behavior the user
  didn't ask for. No backwards-compat shims for removed features.
- **No premature abstractions.** Three duplicated lines is better
  than a helper that hides intent.

## UI conventions

- All button labels and section headers prefixed with `// `
  (e.g., `// SYNC THEMERRDB`, `// SOURCE BREAKDOWN`).
- Mono font (VT323), green-on-black palette.
- `// SYNCING…` is the canonical busy label (single-word). No
  multi-stage label transitions — they raced and got reverted.
- `✓ DONE` flash for 1.5s on busy → idle transition (dash SYNC,
  library SYNC PLEX). Pattern: `sawBusy` flag.
- Optimistic placeholders bridge click → busy gap. Section labels
  in the placeholder text where applicable (`// SYNCING 4K MOVIES`,
  not generic `// SYNCING PLEX`).

## Commit + release conventions

- Subject: `vX.Y.Z: short summary` under 70 chars.
- Body: section headings + bulleted file-level change notes,
  each note explains WHY (often referencing prior bug or feedback).
- Trailer: `Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>`
- Every meaningful behavior change ships a tag. GitHub Actions
  builds + pushes the Docker image to Docker Hub on `v*.*.*`.
- Schema migrations: forward-only, idempotent. Bump
  `CURRENT_SCHEMA_VERSION` and add `_upgrade_to_vN` block in `db.py`.

## Deploy + branch state

- `main` is the release branch.
- `claude/migrate-to-code-H70WJ` is the active dev branch (work happens here).
- Pushes to `main` from whichever machine is in front of Connor
  (Mac, Windows, or via GitHub web). Tags trigger image builds.
- Image: `healzangels/motif:latest` and `:vX.Y.Z`.
- Production deployment: Unraid box, behind NPM + Authentik forward-auth.

## Things to NEVER do

- `git push --force` to `main` (or any tag-pointed branch).
- Run destructive git ops (reset --hard, clean -fdx, branch -D)
  without explicit ask.
- Add backwards-compat code paths for removed features.
- Write multi-paragraph docstrings or block comments.
- Invent abstractions for hypothetical reuse.
- Commit secrets (Plex token, GitHub PATs, cookies.txt). The events
  log already scrubs `token|secret|password|cookie|auth` patterns;
  don't bypass.

## When debugging

Open `docs/PROJECT_HISTORY.md` § "Recurring Bug Classes" first —
chances are the bug class is already catalogued with its fix
pattern and version markers. Only synthesize a new fix once you've
ruled out a known pattern.

## Session journaling

`docs/SESSION_JOURNAL.md` is an append-only log of every Claude
Code session — what we worked on, why, where we left off. The
SessionStart hook (`.claude/hooks/session-start.sh`) tails the file
and prints it as initial context for the next session, so a fresh
chat (after a crash, after `/compact`, or just on a new day) picks
up where the previous one left off without the user re-pasting.

**Every meaningful task ends with a journal entry.** New entries
go at the **bottom** of the file (newest last, so `tail` surfaces
recent work). When a session ends mid-task, the entry captures
in-flight state under "Open threads" so the next session can
resume cleanly. See the file's header for the entry format.
