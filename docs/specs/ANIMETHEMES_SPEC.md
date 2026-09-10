# `// ANIME THEMES` — design spec (feature brief #2, candidate B)

Status: v1 — 2026-09-09. Tag 1 (resolver + client + harness) shipped as v0.51.314;
tag 2 (the `animethemes` source kind end to end) as v0.51.315; tag 3 (the picker
dialog, §3.5, live-verified) as v0.51.317; tag 3b (song title + artist per
theme, one row per theme) as v0.51.318. §6 decisions 1 and 2 taken 2026-09-09
(dialog; backup default). The "are these real themes?" question is measured in
`animethemes_eval/2026-09-09-tdb-agreement.md` (OP1 = the show's first opening;
human picks differ only by preference among the show's own themes).
Tag 4 (§3.6 review page + sweep + apply-selected) shipped as v0.51.325 — as a
page-scoped job, not an op kind (§6 decision 5). Next: tag 5 (polish).
Candidate A (`// FIND THEME`) is shelved (see FIND_THEME_SPEC.md).

## 1. Problem

ThemerrDB covers roughly a third of the operator's anime section; the rest
is either served by Plex Pass cloud themes (which motif can back up but
cannot re-source) or has no theme at all. Themeing those rows today means a
hand-typed `// SET URL` per show, and YouTube search was measured (A's
spec) to be weakest on exactly this cohort. AnimeThemes.moe is a curated,
API-first catalogue of anime openings/endings with hosted audio, keyed by
AniDB / MAL / AniList ids — a proper source, not a search.

## 2. What the data says (probe 2026-09-09, library snapshot 2026-07-21)

Full table: `docs/specs/animethemes_eval/2026-09-09-baseline.md`.

- 1,280 anime shows. 94 % bridge to an AniDB id from the guids Plex
  already gives us (tvdb 99 %, tmdb 99 % coverage — the old "AniDB rows are
  guid-NULL" belief is stale for this library).
- AnimeThemes has an opening with audio for **87 %** of all anime shows and
  for **300 of the 376 rows that have no theme at all (79 %)**.
- Precision: 95 % of matches are "clean" (season-1 entry, year agrees).
  The 3 % "glance" rows are a bridge fall-through to a later season when
  AnimeThemes lacks season 1 (Bleach → Thousand-Year Blood War) — detectable
  from the bridge's `season` field, so they are flagged, never auto-picked.
- Misses are non-Japanese animation filed in the anime section and
  unreleased titles — outside the catalogue by nature. Name search rescued
  only 14 / 154 unmapped rows, so it is an assist with a human confirm, not
  a fallback the resolver may trust.
- 298 shows map to several AniDB entries (one per season/cour) — the picker
  must group by season; the default is season 1, OP1.
- API: JSON:API, **90 requests / minute**, MIT-licensed server, terms allow
  non-commercial personal use with no anti-automation clause. Two quirks
  that shape the client: `/resource` refuses deep includes (so lookups are
  two steps), and list filters must be **scoped** (`filter[anime][id]=…`)
  or the include comes back empty. Comma lists of up to 50 ids per request
  work; `page[size]=100` works. Whole library ≈ 60 requests.
- Audio: direct `.ogg` (Vorbis) on `a.animethemes.moe`, 2–5 MB, TV-size.
  yt-dlp's generic extractor accepts the link (`direct: True`), so it can
  ride the existing download pipeline (FFmpegExtractAudio → mp3 → the
  loudness conditioner) once the host is allow-listed.

## 3. Design

### 3.1 Sources

| Source | Role | Refresh |
|---|---|---|
| Plex guids (`plex_items.guid_tvdb`, `guid_tmdb`) | the key | every enum |
| Fribb/anime-lists `anime-list-mini.json` | TVDB/TMDB → AniDB bridge (with `season`) | cached on disk under `config_dir/animethemes/`, refreshed weekly with ETag, loaded lazily into an in-memory index |
| AnimeThemes API | anime → OP/ED → audio links | per-request, in-memory cache 24 h |

No boot-time work: nothing is fetched until the first resolver call. A
missing/unfetchable bridge file is a reported state ("bridge unavailable —
retry"), with a log breadcrumb, never a silent empty result (class 9).

### 3.2 Resolver (`app/core/animethemes.py`)

`resolve(row) -> Resolution`:

1. `guid_tvdb` → bridge entries for that TVDB id, ordered season-1 first,
   then by season. Fallback `guid_tmdb` → entries keyed `themoviedb_id.tv`.
2. Collect the AniDB ids; look them up (batched, two-step); keep entries
   that have at least one theme with audio.
3. `confidence`:
   - `clean` — the chosen entry is the bridge's season-1 entry and
     `|AnimeThemes.year − Plex.year| ≤ 1`;
   - `glance` — fell through to a later-season entry, or the year is off
     by > 1 (the Bleach case);
   - `name` — no bridge hit; a name search (`?q=`) returned a title with
     year within 1. Always requires a human confirm; never auto-applied.
4. `Resolution` carries every matched season as a group:
   `seasons: [{season, anidb_id, anime_name, year, themes: [{slug, type,
   sequence, audio: [{link, size, version, source, nc, nsfw}]}]}]`, plus
   `default` = season 1's OP1, preferring version 1 and BD source.

The resolver is pure over its inputs (row + bridge + API client), so the
harness and the endpoints share it verbatim.

### 3.3 Client

`httpx` with a 20 s timeout, `User-Agent: motif/<version>
(+https://github.com/Healzangels/motif)`, a token bucket at 60 req/min
(a third under the documented limit), `Retry-After` honoured on 429, and
at most 50 ids per request. Requests:

- step 1 `GET /resource?filter[site]=aniDB&filter[external_id]=<ids>
  &include=anime&fields[resource]=external_id&fields[anime]=id,name,year,
  season,slug&page[size]=100`
- step 2 `GET /anime?filter[anime][id]=<ids>&include=animethemes.
  animethemeentries.videos.audio&fields[anime]=id,name,slug,year,season
  &fields[animetheme]=type,sequence,slug&fields[animethemeentry]=version,
  nsfw,spoiler&fields[video]=basename,nc,source,resolution
  &fields[audio]=link,size&page[size]=100`
- name search `GET /anime?q=<title>&page[size]=5&include=…` (same fields)

Every non-200 logs a breadcrumb with the status; 429 logs once per burst.

### 3.4 Apply path = SET URL

A pick lands through the existing `POST /api/plex_items/{rk}/manual-url`
with the audio link. Plumbing that needs the new source kind
`animethemes`: `sync.url_source()` and its downloader mirror
`_source_for()`, `_FETCH_ALLOWED_HOSTS` (`animethemes.moe`), the
download-options branch for non-YouTube sources (generic extractor; no JS
runtime), and the JS source-label preview in the SET URL dialog. The
event message `Manual URL set by <user>: <url>` stays byte-identical
(recovery walker); the origin goes into `detail`:
`{"source": "animethemes", "slug": "CowboyBebop-OP1", "anidb": 23,
"confidence": "clean"}`. The row becomes a U-row; revisions, backups and
notifications apply unchanged. No new SRC letter in phases 1–2.

### 3.5 Phase 1 — per-row picker

- Entry point: SOURCE menu `anime-themes` → `// ANIME THEMES` on rows in
  `is_anime` sections (and any row whose guid resolves), group 3 next to
  SET URL; the INFO card's bare copy and `// TRY THIS NEXT` name it.
- Dialog `#anime-themes-dlg` on the canonical shell (as `#manual-url-dlg`).
  Header: title (year) + the AnimeThemes name/year it resolved to + a
  `CLEAN` / `GLANCE` / `NAME MATCH` chip. Body: one `dlg-section` per
  season (`// SEASON 1 — <AnimeThemes name>`), rows `OP1 / OP2 / ED1…` with
  chips (OP/ED, size, BD/WEB, v2 when a later version exists), buttons
  `// PREVIEW` (`.btn-tiny .btn-info`) and `// USE THIS` (`.btn-tiny
  .btn-warn`). GLANCE and NAME resolutions show a one-line warning above
  the list; nothing is pre-selected.
- Preview: `POST …/anime-themes/preview {link}` downloads the `.ogg` and
  transcodes to mp3 into `themes_dir/.edit-candidates/` (the v0.51.281
  candidate pipe: 32-hex id, TTL sweep, range-capable stream route).
  Transcode because Safari does not play Vorbis in `<audio>`; ffmpeg is in
  the container. One preview in flight; close discards.
- `// USE THIS` → manual-url; `download_only` pre-ticked when the row
  already has a theme (lands as a backup revision, never a silent replace).

### 3.6 Phase 2 — review page + sweep (shipped v0.51.325)

- Page `/admin/anime-themes` (linked from the anime tab's hero as
  `// ANIME THEMES ▸` and from Settings › Diagnostics, like ORPHAN SCAN).
  `// RUN SWEEP` resolves every row of the INCLUDED anime sections that has
  no motif file and no user override (edition-scoped, theme_id-or-guid
  linked — `_animethemes_eligible_rows`) as a page-scoped background job
  (`_AT_SWEEP_STATE` + `/api/admin/animethemes-sweep/{start,cancel,status}`,
  the loudness-audit / orphan-scan shape). One `prefetch` then a cache-only
  `resolve` per row, **no name search** — a handful of API calls for the whole
  library. The report is a file (`config_dir/animethemes/sweep.json`, atomic
  replace) served by `GET /api/admin/animethemes-sweep`, which re-runs the
  eligibility query so rows applied since the sweep are flagged `applied`.
- Three buckets: **READY TO APPLY** (CLEAN + an audio default) with a
  checkbox per row, `// SELECT ALL` / `// CLEAR` / `// APPLY SELECTED`;
  **NEEDS A LOOK** (GLANCE — season-1 entry absent or year off; the picker
  per row, never the bulk path); **NOT FOUND** (no bridge entry / no audio;
  the picker's name search per row). Each row shows title (year), the
  AnimeThemes name (year), the pick (OP1 · song · artist · size) and the
  state (no theme / Plex serves → backup).
- `// APPLY SELECTED` walks the selection through each row's own
  `manual-url` (the picker's apply, §3.4) sequentially from the page with a
  running count; Plex-served rows send `download_only` (decision 2). The
  downloads then run through the normal queue + rate limit.

### 3.7 Never

- No apply without a click; no auto-apply of GLANCE or NAME resolutions.
- No background API calls: resolution runs on a click (row or sweep).
- No new SRC letter in phases 1–2; a pick is a user URL.
- No boot-time fetch of the bridge file; no hard dependency on AnimeThemes
  availability anywhere in the app's existing paths.

## 4. Harness — tag 1

`tools/animethemes_eval.py --db <motif.db> [--section-id N] [--out DIR]`:
selects the anime rows the way `/api/library` does (`plex_sections.
is_anime = 1`, `media_type = 'show'`), buckets them (no theme /
Plex-served unlinked / TDB-linked / user-set), runs `resolve()` from
`app.core.animethemes` — the product code, not a re-implementation — and
writes `summary.md` (the table in §2) + `results.json` + the miss list.
Rules: every resolver/bridge change re-runs the harness against the banked
snapshot and may not lower the CLEAN count or raise GLANCE without the
changelog saying why. Offline pytest pins: bridge indexing (tvdb+season,
tmdb tv), season-1-first ordering, the three confidence rules, batch
chunking ≤ 50, the exact two-step query shapes (scoped filter, no deep
include on `/resource`), 429 + Retry-After handling, pacing, caches,
"bridge unavailable" reporting, name-search never auto-applies, version pin.

## 5. Tags

| Tag | Scope | Gate |
|---|---|---|
| 1 | `app/core/animethemes.py` (bridge, client, resolver), `tools/animethemes_eval.py`, banked baseline, offline pins | pytest + ruff; harness reproduces the baseline on the snapshot |
| 2 | source kind `animethemes` end-to-end: url_source/_source_for/allowlist, download branch, SET URL accepts an AnimeThemes link; mirror-drift pins | pytest; live: one row themed from a pasted `.ogg` link |
| 3 | Phase 1 dialog, preview via the candidate pipe, entry points | pytest; live on 5 no-theme rows incl. one GLANCE |
| 4 | Phase 2 review page + page-scoped sweep + apply-selected (v0.51.325) | pytest (offline sweep on the tag-1 fake API; eligibility SQL; endpoints; page); live on the scratch instance |
| 5 | Polish: bridge refresh schedule, README/CLAUDE.md, digest notification for bulk applies | pytest |

## 6. Open decisions

1. ~~Dialog (proposed) vs an in-card section for phase 1.~~ DECIDED 2026-09-09:
   dialog — one surface for both entry paths (like SET URL), the card stays
   short, the modal owns the preview lifecycle (close discards).
2. ~~Phase 2 default for P-rows: backup (proposed) vs replace.~~ DECIDED
   2026-09-09: backup — a row that already has a theme pre-ticks KEEP AS
   BACKUP so a pick lands as a revision, never a silent replace.
3. A dedicated SRC letter for AnimeThemes-sourced rows later (six-site
   cost) — not before phase 2 has run on the real library.
4. Bridge file licence is unstated in its README; attribution line in
   README either way.
5. ~~Sweep as an `op_progress` kind (proposed in §3.6) vs a page-scoped
   job.~~ DECIDED 2026-09-10 (tag 4): page-scoped — the two read-only
   diagnostics that exist (orphan scan, loudness audit) run exactly that
   way, the sweep is watched from its own page, and an op kind costs a
   schema migration + the six-site mirror for a job that never mutates a
   row. Promote to an op kind later only if the operator wants it in the
   ops drawer.
