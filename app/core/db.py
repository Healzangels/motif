"""
SQLite persistence layer.

Schema goals:
- One canonical `themes` table keyed by (media_type, tmdb_id) — the only stable
  upstream identifier (since some entries lack imdb_id).
- Track upstream timestamps so we can diff incremental syncs cheaply.
- Track local file state separately from upstream metadata, so re-syncs don't
  clobber download/placement progress.
- An events table for the live log viewer.
"""
from __future__ import annotations

import logging
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

log = logging.getLogger(__name__)


SCHEMA = """
CREATE TABLE IF NOT EXISTS themes (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    -- v1.18.0 (schema v55): 'collection' added for ThemerrDB-tracked
    -- movie collections (Harry Potter, Spider-Man, etc.). Same row
    -- shape as movies; tmdb_id holds the TMDB collection ID
    -- (disjoint ID space from movies, so no collision).
    media_type           TEXT NOT NULL CHECK (media_type IN ('movie', 'tv', 'collection')),
    tmdb_id              INTEGER NOT NULL,
    tvdb_id              INTEGER,
    imdb_id              TEXT,
    title                TEXT NOT NULL,
    original_title       TEXT,
    year                 TEXT,
    release_date         TEXT,
    youtube_url          TEXT,
    youtube_video_id     TEXT,
    youtube_added_at     TEXT,
    youtube_edited_at    TEXT,
    upstream_source      TEXT NOT NULL CHECK (upstream_source IN ('imdb', 'themoviedb', 'plex_orphan')),
    raw_json             TEXT,
    last_seen_sync_at    TEXT NOT NULL,
    first_seen_sync_at   TEXT NOT NULL,
    failure_kind         TEXT,
    failure_message      TEXT,
    failure_at           TEXT,
    -- v1.10.50: when set, the user has acknowledged the current
    -- failure_kind (or implicitly cleared it via SET URL / UPLOAD /
    -- ADOPT). The TDB pill still paints red so the user knows the
    -- TDB-side URL is broken, but the ! glyph + FAILURES filter
    -- exclude these rows. Cleared automatically when failure_kind
    -- changes to a different value (so a brand-new failure
    -- re-alerts).
    failure_acked_at     TEXT,
    -- v1.10.32: normalized title (normalize_title in app/core/normalize.py)
    -- for the title-fallback library match. Populated by sync.
    title_norm           TEXT,
    -- v1.13.1 (Phase C): set when ThemerrDB stops publishing this
    -- (media_type, tmdb_id) — i.e. the per-item JSON disappeared
    -- from the database branch between syncs. NULL = currently in
    -- TDB. Sync writes the timestamp; the user dismisses via
    -- ACK DROP (clears) or CONVERT TO MANUAL (clears + reclassifies
    -- as U). A re-add by ThemerrDB also clears it automatically.
    tdb_dropped_at       TEXT,
    -- v1.14.28 (schema v45): timestamp of the last yt-dlp probe
    -- against the row's URL. Set by /api/items/{mt}/{id}/probe-tdb
    -- (single-row) or the bulk probe job (v1.14.29). Drives the
    -- bulk-probe cooldown — rows probed within 24h are skipped.
    -- NULL = never probed. Doesn't replace failure_kind/at — those
    -- are set on actual download failures; this is just the probe
    -- timestamp for cooldown bookkeeping.
    last_probed_at       TEXT,
    -- v1.15.81 (schema v51): SHA1 fingerprint of motif-relevant fields
    -- from the upstream TDB JSON record (url, video_id, imdb_id,
    -- title, year, original_title, edited timestamps). Used by sync's
    -- apply loop to skip the full UPDATE when nothing motif reads has
    -- actually changed. Pre-fix the TDB apply phase ran a full upsert
    -- for every "changed" git path even when LizardByte's commits
    -- only touched metadata motif doesn't read — the user's repro had
    -- 9733 paths apply over 2+ minutes producing 0 actual updates.
    -- Fingerprint compare → bump-last-seen-only fast path skips ~90%
    -- of the DB work in the noisy-commit / same-data case. NULL on
    -- legacy rows before v1.15.81 (next sync writes the fingerprint).
    tdb_content_fingerprint TEXT,
    UNIQUE (media_type, tmdb_id)
);

CREATE INDEX IF NOT EXISTS idx_themes_imdb ON themes (imdb_id) WHERE imdb_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_themes_tvdb ON themes (tvdb_id) WHERE tvdb_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_themes_video ON themes (youtube_video_id);
CREATE INDEX IF NOT EXISTS idx_themes_title ON themes (title);
CREATE INDEX IF NOT EXISTS idx_themes_failure ON themes (failure_kind) WHERE failure_kind IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_themes_orphan ON themes (upstream_source) WHERE upstream_source = 'plex_orphan';
-- v1.13.54 (schema v41): dashboard + library SQL filter on
-- `tdb_dropped_at IS NOT NULL` to surface gray TDB◌ rows. Partial
-- index avoids paying for the live (NULL) majority.
CREATE INDEX IF NOT EXISTS idx_themes_dropped ON themes (tdb_dropped_at) WHERE tdb_dropped_at IS NOT NULL;
-- v1.10.32: title_norm holds normalize_title() output ('Twelve Monkeys'
-- → '12 monkeys'). Lets the library JOIN match by normalized title+year
-- as a last-resort fallback when Plex's GUIDs disagree with ThemerrDB.
CREATE INDEX IF NOT EXISTS idx_themes_title_norm
    ON themes (media_type, title_norm, year)
    WHERE title_norm IS NOT NULL AND year IS NOT NULL;

-- v1.11.0: section_id is part of the primary key. An item that lives in
-- multiple Plex sections (e.g. Matilda in standard movies AND 4K movies)
-- now has one local_files row per section, each pointing at a per-section
-- staging path: <themes_dir>/<plex_sections.themes_subdir>/<Title (Year)>/
-- theme.mp3. Files across sibling sections are hardlinked from the same
-- inode (one physical download, N rows) so storage is unchanged.
CREATE TABLE IF NOT EXISTS local_files (
    media_type      TEXT NOT NULL,
    tmdb_id         INTEGER NOT NULL,
    section_id      TEXT NOT NULL,
    theme_id        INTEGER,
    -- v1.18.45: file_path is RELATIVE to settings.themes_dir.
    -- The absolute path is `themes_dir / file_path`. Worker code
    -- joins them at e.g. worker.py:1812 / 2347. Reading file_path
    -- directly + treating it as absolute will silently fail —
    -- v1.18.36 hit exactly this bug in api_unplace_item's
    -- motif_hash compute; v1.18.39 fixed it; this comment is
    -- defense-in-depth so the convention is visible in schema
    -- dumps + `PRAGMA table_info(local_files)`. See CLAUDE.md
    -- "File-path conventions" section for the rationale.
    file_path       TEXT NOT NULL,
    file_sha256     TEXT,
    file_size       INTEGER,
    downloaded_at   TEXT NOT NULL,
    source_video_id TEXT NOT NULL,
    provenance      TEXT NOT NULL DEFAULT 'auto'
                       CHECK (provenance IN ('auto', 'manual')),
    source_kind     TEXT
                       CHECK (source_kind IN ('themerrdb', 'url',
                                              'upload', 'adopt',
                                              'plex_cloud')
                              OR source_kind IS NULL),
    -- v1.11.24: last place worker outcome for this row. The /pending
    -- view reads this to surface the actual reason a download is
    -- still unplaced ('existing_theme:theme.mp3', 'plex_has_theme',
    -- 'no_match', 'placement_error:...') instead of a generic
    -- 'worker should pick it up shortly'. NULL = no place attempt
    -- yet (truly queued).
    last_place_attempt_at      TEXT,
    last_place_attempt_reason  TEXT,
    -- v1.11.99: SET URL / UPLOAD MP3 on a row that already has a
    -- placement now lands the new content in canonical without
    -- touching the placement file. Resolution flows through /pending.
    --   NULL      — no mismatch (default; canonical matches placement)
    --   'pending' — mismatch awaiting user action (PUSH TO PLEX,
    --               ADOPT FROM PLEX, or KEEP MISMATCH)
    --   'acked'   — KEEP MISMATCH chosen; out of /pending but library
    --               row still shows DL=amber + LINK=≠ until a
    --               PUSH TO PLEX or ADOPT FROM PLEX clears it.
    mismatch_state TEXT,
    -- v1.23.37 (schema v67): persist canonical health, mirroring v66's
    -- placements.theme_present. verify_canonical_health stats each row's
    -- themes_dir/file_path and stamps canonical_present (1 present / 0 missing
    -- / NULL unverified) so the library's DL sort can rank a BROKEN canonical
    -- (theme.mp3 gone from motif's storage) in SQL — the red DL dot is
    -- otherwise a per-render filesystem stat the paginated sort can't see.
    canonical_present INTEGER,
    canonical_health_checked_at TEXT,
    -- v0.51.157 (schema v72): read-only loudness audit (Phase 0 of the loudness
    -- feature). ffmpeg's EBU R128 loudnorm analysis (app/core/loudness.py) measures
    -- each local-bytes theme WITHOUT re-encoding; these persist the result so the
    -- report + eventual normalize can rank the spread in SQL. loudness_i = integrated
    -- loudness (LUFS), loudness_tp = true peak (dBTP), loudness_lra = loudness range.
    -- loudness_measured_sha256 pins WHICH bytes were measured — a re-download/replace
    -- changes file_sha256, so a stale measurement is detected (sha mismatch) + re-run.
    -- NULL across all = never measured. Nothing here mutates a file.
    loudness_i        REAL,
    loudness_tp       REAL,
    loudness_lra      REAL,
    loudness_measured_at     TEXT,
    loudness_measured_sha256 TEXT,
    -- v0.51.168 (schema v73): loudness NORMALIZATION state — Phase 1, the first thing
    -- that MUTATES a theme (mp3gain gain, proven bit-exact-reversible by // PROBE
    -- MP3GAIN). norm_state='normalized' when gain is applied (NULL = raw, never
    -- normalized); norm_gain_db = the dB actually applied; norm_target = the LUFS aimed
    -- at; norm_at = when. norm_orig_sha256 is the PRE-normalize file_sha256 — UNDO
    -- (mp3gain -u) compares the restored bytes against it to PROVE the original came
    -- back rather than assume the undo tag worked. Written edition-scoped (full PK).
    norm_state        TEXT,
    norm_gain_db      REAL,
    norm_target       REAL,
    norm_at           TEXT,
    norm_orig_sha256  TEXT,
    -- v0.51.170 (schema v74): the DECODED-PCM hash of the original. norm_orig_sha256 is
    -- the FILE hash, and mp3gain leaves its APE tag behind on undo, so a restored file can
    -- never sha-match the pre-normalize file — v0.51.165's probe measured exactly that
    -- (restored_file_bit_exact=false + restored_diff_is_tag_only=true) and the v0.51.168
    -- undo check was built on the file hash anyway, so a CORRECT restore reported
    -- "not bit-exact". The audio layer is the one that decides: hash the samples before
    -- applying gain, compare after undo. NULL = normalized by an older build (unknown).
    norm_orig_pcm_sha256 TEXT,
    norm_plex_entry_uri TEXT,
    -- v1.21.51 (schema v62): per-edition theme isolation. Movies can
    -- carry multiple editions (Theatrical/Extended/custom) that share
    -- one tmdb_id; edition_key (the normalized {edition-X} folder tag,
    -- '' = standard) lets each edition hold its own theme state. v62
    -- adds the column ('' = today's behavior, exactly); v63 folds it
    -- into the PK. Derived via normalize_edition(parse_folder_name(...)).
    edition_key     TEXT NOT NULL DEFAULT '',
    -- v1.21.52 (schema v63): edition_key folded into the PK (Phase A2).
    PRIMARY KEY (media_type, tmdb_id, section_id, edition_key),
    FOREIGN KEY (media_type, tmdb_id) REFERENCES themes (media_type, tmdb_id) ON DELETE CASCADE,
    FOREIGN KEY (section_id) REFERENCES plex_sections (section_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_local_files_item ON local_files (media_type, tmdb_id);
CREATE INDEX IF NOT EXISTS idx_local_files_section ON local_files (section_id);
CREATE INDEX IF NOT EXISTS idx_local_files_sha ON local_files (file_sha256) WHERE file_sha256 IS NOT NULL;

-- v1.10.57: history of local_files rows that got removed via unmanage
-- or forget. Lets adopt_folder restore the original youtube_url when
-- the sidecar's sha256 matches a previously-tracked file — so the
-- U → unmanage → M → adopt → A path doesn't lose the user's URL.
-- Keyed by content hash so the lookup works even if the new orphan
-- row gets a different synthetic tmdb_id.
CREATE TABLE IF NOT EXISTS local_files_history (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    file_sha256     TEXT NOT NULL,
    media_type      TEXT,
    tmdb_id         INTEGER,
    source_kind     TEXT,
    source_video_id TEXT,
    youtube_url     TEXT,
    saved_at        TEXT NOT NULL,
    saved_reason    TEXT
);
CREATE INDEX IF NOT EXISTS idx_local_files_history_sha
    ON local_files_history (file_sha256, saved_at DESC);

-- v1.11.0: section_id is part of the primary key — a placement lives
-- inside a specific Plex section's location_paths. Combined with
-- media_folder this lets a multi-disk section (multiple location_paths)
-- still hold one placement row per actual folder, while never
-- conflating placements that happen to share folder names across two
-- sections (e.g. /movies/Matilda (2022) vs /movies-4k/Matilda (2022)).
CREATE TABLE IF NOT EXISTS placements (
    media_type      TEXT NOT NULL,
    tmdb_id         INTEGER NOT NULL,
    section_id      TEXT NOT NULL,
    theme_id        INTEGER,
    media_folder    TEXT NOT NULL,
    placed_at       TEXT NOT NULL,
    -- v1.18.0 (schema v55): 'plex_upload' marker for collection
    -- placements that go through POST /library/metadata/{rk}/themes
    -- instead of an on-disk hardlink/copy. Collections have no
    -- media folder so the file-based kinds don't apply.
    placement_kind  TEXT NOT NULL CHECK (placement_kind IN ('hardlink', 'copy', 'symlink', 'plex_upload')),
    plex_rating_key TEXT,
    plex_refreshed  INTEGER NOT NULL DEFAULT 0,
    provenance      TEXT NOT NULL DEFAULT 'auto'
                       CHECK (provenance IN ('auto', 'manual', 'cloud')),
    -- v1.21.51 (schema v62): per-edition theme isolation (see local_files).
    -- Hardlink rows already disambiguate via media_folder; edition_key
    -- additionally disambiguates plex_upload rows (media_folder='').
    -- Backfilled from basename(media_folder) so existing hardlink rows
    -- carry a meaningful key; v63 folds it into the PK.
    edition_key     TEXT NOT NULL DEFAULT '',
    -- v1.23.25 (schema v66): placement health. plex_enum's
    -- verify_placement_health pass stats media_folder/theme.mp3 and stamps
    -- theme_present (1 present / 0 missing / NULL unverified) so the
    -- library's NEEDS WORK + PL sorts can rank a broken placement (theme
    -- deleted from the Plex folder) in SQL — the red PL dot is otherwise a
    -- per-render filesystem stat the paginated server-side sort can't see.
    theme_present   INTEGER,
    placement_health_checked_at TEXT,
    -- v1.21.52 (schema v63): edition_key folded into the PK (Phase A2).
    PRIMARY KEY (media_type, tmdb_id, section_id, media_folder, edition_key),
    FOREIGN KEY (media_type, tmdb_id) REFERENCES themes (media_type, tmdb_id) ON DELETE CASCADE,
    FOREIGN KEY (section_id) REFERENCES plex_sections (section_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_placements_item ON placements (media_type, tmdb_id);
CREATE INDEX IF NOT EXISTS idx_placements_section ON placements (section_id);

-- Download / placement / sync queue. Workers consume from here.
-- v1.11.0: section_id carries the per-section context for download/place/
-- refresh/adopt jobs. NULL for section-agnostic jobs (sync, plex_enum
-- without a section, scan, probe).
-- v1.14.56: dormant asymmetry — the v14 migration adds 'probe' to
-- the jobs.job_type CHECK list (db.py:2276), but the v1.10.45
-- inline probe phase that wrote 'probe' rows was retired by the
-- v1.14.28 single-row /probe-tdb endpoint + v1.14.29 bulk-probe
-- background-thread sweep, neither of which uses the jobs queue.
-- So migrated DBs (pre-v14 → v14+) accept 'probe' job_type; fresh
-- installs reject it. No runtime impact today (zero callers); a
-- future re-introduction must either add 'probe' here OR ship a
-- v45 → v46 migration that drops it from the v14 CHECK. Documented
-- so the inconsistency doesn't surprise the next contributor.
CREATE TABLE IF NOT EXISTS jobs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    job_type        TEXT NOT NULL
                       CHECK (job_type IN ('sync','download','place','refresh','relink','scan','adopt','plex_enum')),
    media_type      TEXT,
    tmdb_id         INTEGER,
    section_id      TEXT,
    payload         TEXT,
    status          TEXT NOT NULL DEFAULT 'pending'
                       CHECK (status IN ('pending', 'running', 'done', 'failed', 'cancelled')),
    attempts        INTEGER NOT NULL DEFAULT 0,
    max_attempts    INTEGER NOT NULL DEFAULT 3,
    last_error      TEXT,
    created_at      TEXT NOT NULL,
    started_at      TEXT,
    finished_at     TEXT,
    next_run_at     TEXT,
    -- v1.11.36: cooperative cancellation flag. UI sets this to 1 via
    -- POST /api/jobs/{id}/cancel; the worker handlers check it at
    -- safe yield points (between sync batches, between plex_enum
    -- sections / pages, etc.) and bail with status='cancelled' on
    -- the next check. Pending jobs flip straight to 'cancelled'
    -- without ever running.
    cancel_requested INTEGER NOT NULL DEFAULT 0,
    -- v1.12.12: non-destructive CLEAR FAILED on the /logs page.
    -- When set, the job stays in status='failed' but renders as
    -- 'ACKNOWLEDGED' (green) and drops out of the topbar failure
    -- count. Lets the user keep failure history for review without
    -- the dot staying red forever.
    acked_at        TEXT
);

CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs (status, next_run_at);
CREATE INDEX IF NOT EXISTS idx_jobs_type ON jobs (job_type, status);
-- v1.10.2: per-item lookup hits this on every library row (job_in_flight
-- correlated subquery). Without it the library page does an N×M scan of
-- jobs per library row and softlocks the API.
CREATE INDEX IF NOT EXISTS idx_jobs_item ON jobs (media_type, tmdb_id);
-- v1.10.2: sync_runs.finished_at and jobs (job_type, status, finished_at)
-- power the library page's plex-scan-stale advisory; cheap to add.
CREATE INDEX IF NOT EXISTS idx_jobs_type_status_finished
    ON jobs (job_type, status, finished_at);

CREATE TABLE IF NOT EXISTS events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          TEXT NOT NULL,
    level       TEXT NOT NULL,
    component   TEXT NOT NULL,
    media_type  TEXT,
    tmdb_id     INTEGER,
    section_id  TEXT,
    message     TEXT NOT NULL,
    detail      TEXT
);

CREATE INDEX IF NOT EXISTS idx_events_ts ON events (ts DESC);
CREATE INDEX IF NOT EXISTS idx_events_item ON events (media_type, tmdb_id, ts DESC);
CREATE INDEX IF NOT EXISTS idx_events_item_section
    ON events (media_type, tmdb_id, section_id, ts DESC);

-- v0.51.147 (schema v71): the in-app notification inbox. A curated subset of
-- notification-worthy events (INBOX_EVENT_KINDS in notify_inbox.py) is recorded
-- here at the notify dispatch chokepoint, UNCONDITIONALLY of the per-event Apprise
-- send-toggle, so the topbar INBOX drawer surfaces auto-added themes even when that
-- kind's Discord/Apprise toggle is off. media_type/tmdb_id/section_id/batch_id are
-- nullable — dispatch() carries only title/body today; a later tag threads item +
-- batch identity for precise click-through + smart grouping.
CREATE TABLE IF NOT EXISTS notifications (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    ts           TEXT NOT NULL,
    event_kind   TEXT NOT NULL,
    severity     TEXT NOT NULL DEFAULT 'info',
    title        TEXT NOT NULL,
    body         TEXT,
    media_type   TEXT,
    tmdb_id      INTEGER,
    section_id   TEXT,
    -- v0.51.220 (schema v78): the exact cut this fired for, so the drawer
    -- click-through opens THAT edition's card instead of the v0.51.218
    -- pick-a-cut prompt. NULL on title-level digests (a batch has no single
    -- edition — those correctly fall back to the picker). '' is the untagged
    -- standard edition, distinct from NULL "unknown".
    edition_key  TEXT,
    batch_id     TEXT,
    seen_at      TEXT,
    dismissed_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_notifications_open
    ON notifications (dismissed_at, ts DESC);

CREATE TABLE IF NOT EXISTS sync_runs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at      TEXT NOT NULL,
    finished_at     TEXT,
    status          TEXT NOT NULL DEFAULT 'running'
                       CHECK (status IN ('running', 'success', 'failed')),
    movies_seen     INTEGER NOT NULL DEFAULT 0,
    tv_seen         INTEGER NOT NULL DEFAULT 0,
    -- v1.22.87 (v65): collections walked this run — the counter
    -- existed since v1.18.0 but was never persisted, so a run
    -- touching only movie_collections/ paths looked empty.
    collections_seen INTEGER NOT NULL DEFAULT 0,
    new_count       INTEGER NOT NULL DEFAULT 0,
    updated_count   INTEGER NOT NULL DEFAULT 0,
    error           TEXT,
    -- v1.13.2 (#1): which transport this run actually used. Stamped
    -- at run_id INSERT from settings.sync_source ('git' / 'database'
    -- / 'remote'). On a fallback cascade the original-requested
    -- transport stays here; fallback_reason explains what happened.
    transport       TEXT,
    -- v1.13.2 (#1): non-NULL when this run cascaded to a slower
    -- tier ('git: <reason>' or 'database: <reason>'). Drives the
    -- dashboard sparkline's amber-tinted bar so the user can see
    -- at a glance which days the fast path failed.
    fallback_reason TEXT,
    -- v1.13.2 (#1): 1 when codeload returned 304 (Phase A.5) or
    -- the git fetch produced no new commits (Phase B). Distinct
    -- from "0 changes after a full walk" so the dashboard chart
    -- can color a sub-second no-op run differently from a
    -- full-walk run with no upstream churn.
    no_changes      INTEGER NOT NULL DEFAULT 0
);

-- v1.12.106: live progress surface for long-running ops (TDB sync,
-- Plex enum). One row per op, upserted by the worker on each
-- natural checkpoint already in the code (per-page index fetch,
-- per-batch flush, per-section enum). The /api/progress endpoint
-- reads running rows; the UI polls 1s while active, 10s idle.
--
-- Finished rows survive for 24h so the ops panel can show a
-- "last N ops" tail in idle state. A periodic sweep (run on
-- /api/progress reads) prunes anything older.
--
-- Cancel protocol: writers set status='cancelling', the worker
-- checks at each checkpoint and exits cleanly, then sets status
-- ='cancelled' + finished_at on the way out.
CREATE TABLE IF NOT EXISTS op_progress (
    op_id           TEXT PRIMARY KEY,
    kind            TEXT NOT NULL
                       -- v1.14.93 (schema v47): widened to include
                       -- 'bulk_probe_tdb'. Pre-fix the bulk-probe-tdb
                       -- worker thread crashed at start_progress with
                       -- IntegrityError on every BULK PROBE TDB click,
                       -- silently — the route returned 200 (the thread
                       -- spawned fine) but the start_progress INSERT
                       -- threw inside the thread, so the user saw "//
                       -- PROBE TDB URLS started" then nothing in LIVE
                       -- OPS and no work happened. Cascaded into the
                       -- LET PLEX SERVE flow stalling at its probe-
                       -- first prompt for the same reason.
                       -- v1.15.56 (schema v49): widened to include
                       -- 'bulk_lps' — v1.15.28 added the bulk LET
                       -- PLEX SERVE server-side composite (route at
                       -- api.py:13905, kind='bulk_lps') but missed
                       -- the schema migration. Every // ADOPT + LET
                       -- PLEX SERVE / // LET PLEX SERVE bulk click
                       -- 500'd at try_acquire's INSERT with
                       -- IntegrityError. Same bug class as v1.15.48
                       -- (status CHECK widening) — code-vs-schema
                       -- drift on a new code value.
                       -- v1.16.0 (schema v52): added the TVDB→TMDB
                       -- bridge background job's kind. Originally
                       -- named 'hama_bridge'; renamed to 'tvdb_bridge'
                       -- in v1.24.93 (schema v68) — the bridge links
                       -- TVDB-only rows (most TV), it was never
                       -- anime/HAMA-specific. try_acquire('tvdb-bridge',
                       -- 'tvdb_bridge') from /api/admin/tvdb-bridge/
                       -- rebuild; without the schema migration every
                       -- // REBUILD BRIDGE click would 500.
                       -- v1.19.45 (schema v59): widened to include
                       -- 'cloud_themes_backup' — the v1.19.42
                       -- walker became a background op (was sync,
                       -- blocked the event loop for 13min on
                       -- single-row scopes that internally walked
                       -- the full ~3,883-row catalog before
                       -- filtering). Same shape as bulk_lps /
                       -- tvdb_bridge: try_acquire('cloud-themes-
                       -- backup', 'cloud_themes_backup') in the
                       -- route → threading.Thread → worker
                       -- thread runs identify_c1_rows +
                       -- backup_cloud_theme with progress + cancel.
                       -- v0.51.195 (schema v76): widened to include
                       -- 'bulk_normalize' — the Phase-2 bulk op that
                       -- mp3gain-levels the eligible library and re-uploads
                       -- each. try_acquire('bulk-normalize',
                       -- 'bulk_normalize') → threading.Thread →
                       -- _bulk_normalize_run (serial, loudest-first,
                       -- per-row _normalize_one_row, cancel + progress).
                       -- Mutates local_files.norm_state → row-refresh kind.
                       -- v0.51.199 (schema v77): 'bulk_normalize_undo' — the
                       -- reverse of bulk_normalize (undo a whole LEVEL run,
                       -- restoring each file + putting Plex back). Same runner
                       -- shape; mutates local_files.norm_state → row-refresh kind.
                       CHECK (kind IN ('tdb_sync', 'plex_enum',
                                       'reprobe_plex_themes',
                                       'bulk_probe_tdb',
                                       'bulk_lps',
                                       'tvdb_bridge',
                                       'cloud_themes_backup',
                                       'bulk_normalize',
                                       'bulk_normalize_undo')),
    status          TEXT NOT NULL DEFAULT 'running'
                       -- v1.15.48 (schema v48): widened to include
                       -- 'pending'. v1.15.37's try_acquire helper
                       -- INSERTs with status='pending' (the route
                       -- claims the slot atomically; the spawned
                       -- thread's start_progress transitions it to
                       -- 'running'). Without 'pending' in the CHECK
                       -- the insert 500'd — the user's logs:
                       -- "sqlite3.IntegrityError: CHECK constraint
                       -- failed: status IN ('running', 'cancelling',
                       -- 'done', 'failed', 'cancelled')" on every
                       -- // REPROBE FAILURES click. v1.15.37 shipped
                       -- the helper but missed the corresponding
                       -- schema migration.
                       CHECK (status IN ('pending', 'running',
                                         'cancelling',
                                         'done', 'failed', 'cancelled')),
    started_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    finished_at     TEXT,
    stage           TEXT,
    stage_label     TEXT,
    stage_current   INTEGER NOT NULL DEFAULT 0,
    stage_total     INTEGER NOT NULL DEFAULT 0,
    processed_total INTEGER NOT NULL DEFAULT 0,
    processed_est   INTEGER NOT NULL DEFAULT 0,
    error_count     INTEGER NOT NULL DEFAULT 0,
    -- detail_json holds: latest activity lines (rolling 5),
    -- per-stage breakdown for the timeline, throughput history
    -- (rolling 30 samples for sparkline), error_message if failed.
    detail_json     TEXT
);
CREATE INDEX IF NOT EXISTS idx_op_progress_status
    ON op_progress (status, updated_at);
CREATE INDEX IF NOT EXISTS idx_op_progress_finished
    ON op_progress (finished_at);

CREATE TABLE IF NOT EXISTS user_overrides (
    media_type      TEXT NOT NULL,
    tmdb_id         INTEGER NOT NULL,
    theme_id        INTEGER,
    youtube_url     TEXT NOT NULL,
    set_at          TEXT NOT NULL,
    set_by          TEXT,
    note            TEXT,
    -- v1.12.72: section_id scopes the override to a specific Plex
    -- section. Empty string ('') means "applies to every section
    -- that doesn't have its own per-section override" (the legacy
    -- global state and the default for callers without section
    -- context). Multiple rows per (media_type, tmdb_id) are now
    -- allowed: one per section + an optional global fallback.
    -- Worker fetch is two-step: try (mt, tmdb, section_id) first,
    -- then fall back to (mt, tmdb, '').
    section_id      TEXT NOT NULL DEFAULT '',
    -- v1.18.77 (schema v56): user's intent for this override.
    --   'replace' (default) — motif's URL should win; force the
    --                          placement past plex_has_theme.
    --   'backup'           — defer to Plex; download as a safety
    --                         net but don't push to Plex.
    -- Disambiguates two states that pre-v1.18.77 looked identical:
    -- (a) "I set this URL as a backup; Plex serves its own theme;
    --      motif's file is the safety net" — intent='backup'
    -- (b) "I set this URL to replace Plex's theme but the
    --      placement got silently skipped because plex_has_theme
    --      blocked it" — intent='replace' + placement missing
    -- Backfilled by the v55→v56 migration based on whether a
    -- placements row exists for the row.
    intent          TEXT NOT NULL DEFAULT 'replace'
                    CHECK (intent IN ('replace', 'backup')),
    -- v1.21.51 (schema v62): per-edition theme isolation (see local_files).
    -- '' = standard edition / today's behavior; v63 folds it into the PK.
    edition_key     TEXT NOT NULL DEFAULT '',
    -- v1.21.52 (schema v63): edition_key folded into the PK (Phase A2).
    PRIMARY KEY (media_type, tmdb_id, section_id, edition_key)
);

-- v1.11.0: plex_sections is treated as append-only at runtime — sections
-- that disappear from Plex are kept with stale last_seen_at so the UI
-- can show them dimmed. NEVER `DELETE FROM plex_sections` directly:
-- local_files and placements FK on section_id ON DELETE CASCADE, so a
-- delete here would silently drop every staged theme + placement for
-- that section. To retire a section, set included=0 instead; once empty
-- of local_files / placements it's effectively retired.
CREATE TABLE IF NOT EXISTS plex_sections (
    section_id      TEXT PRIMARY KEY,
    uuid            TEXT,
    title           TEXT NOT NULL,
    type            TEXT NOT NULL CHECK (type IN ('movie', 'show')),
    agent           TEXT,
    language        TEXT,
    location_paths  TEXT,
    included        INTEGER NOT NULL DEFAULT 1,
    -- v8+: user-applied flags driving the Movies/TV/Anime tab partition.
    -- These are independent: a section can be 4K, anime, both, or neither.
    is_anime        INTEGER NOT NULL DEFAULT 0,
    is_4k           INTEGER NOT NULL DEFAULT 0,
    -- v1.11.0: filesystem-safe slug used as the section's themes subdir.
    -- UNIQUE so collisions are caught by the engine and `_allocate_themes_subdir`
    -- doesn't need its own race-free locking. Computed from
    -- (title, is_4k, is_anime, type) at discovery / flag-toggle time.
    themes_subdir   TEXT NOT NULL DEFAULT '',
    discovered_at   TEXT NOT NULL,
    last_seen_at    TEXT NOT NULL,
    -- v46 (v1.14.74): Plex section-level delta gate. Stores the
    -- `contentChangedAt` value motif observed at the start of the
    -- most recent successful enum for this section. plex_enum
    -- compares the live value (re-read from /library/sections at
    -- enum start) against this; if equal, the full section enum
    -- is skipped (~50ms instead of ~30s-2m). Plex bumps
    -- contentChangedAt only when content actually changes (adds/
    -- removes/edits) — not on no-op rescans — so the gate is a
    -- correct proxy for "is there anything new to fetch." NULL
    -- means "never enumerated"; the gate falls through to a full
    -- enum on first run after the column is added.
    last_enum_content_changed_at TEXT
);
-- v1.11.0: empty themes_subdir is allowed for the brief window before
-- _allocate_themes_subdir runs; the partial unique index excludes it.
CREATE UNIQUE INDEX IF NOT EXISTS idx_plex_sections_themes_subdir
    ON plex_sections (themes_subdir) WHERE themes_subdir != '';

-- v7+: cache of every Plex library item we've discovered. Joined to themes
-- in the unified browse view; not FK'd to plex_sections so a section deletion
-- doesn't cascade-wipe history.
CREATE TABLE IF NOT EXISTS plex_items (
    rating_key       TEXT PRIMARY KEY,
    section_id       TEXT NOT NULL,
    -- v1.18.0 (schema v55): 'collection' added — Plex's own type
    -- string for collection metadata items (matches the value Plex
    -- returns in `<Directory type="collection">` XML responses).
    -- Collection rows in plex_items have folder_path='', file_path
    -- columns NULL, and rating_key pointing at the Plex collection's
    -- own metadata id.
    media_type       TEXT NOT NULL CHECK (media_type IN ('movie', 'show', 'collection')),
    title            TEXT NOT NULL,
    year             TEXT,
    guid_imdb        TEXT,
    guid_tmdb        INTEGER,
    guid_tvdb        INTEGER,
    folder_path      TEXT NOT NULL DEFAULT '',
    -- v1.21.56 (schema v64): normalized {edition-X} tag of folder_path,
    -- '' = standard. Folder-derived (app/core/editions.py) + kept current
    -- by plex_enum. Lets Phase C's library JOIN match placements/local_files
    -- per edition (both also edition_key'd) without a fragile SQL mirror.
    edition_key      TEXT NOT NULL DEFAULT '',
    -- v0.50.17 (schema v69): Plex's editionTitle metadata field, verbatim
    -- ('' when unset). DISPLAY-ONLY — surfaced in the library ED column when the
    -- folder carries NO {edition-X} tag (the user's Alien (1979) case: Plex shows
    -- a metadata "Directors Cut" edition with no folder tag). Captured by
    -- plex_enum from enumerate_section_items. Deliberately NOT folded into
    -- edition_key: per-edition theme scoping (placements/local_files PK) stays
    -- folder-based, so a metadata-only edition never drives placement.
    plex_edition_title TEXT NOT NULL DEFAULT '',
    has_theme        INTEGER NOT NULL DEFAULT 0,
    -- v1.11.26: denormalized themes-row id, populated by plex_enum when
    -- the item is first discovered and refreshed when sync upserts a new
    -- themes row that matches. Pre-fix /api/library re-ran a heavy
    -- correlated subquery (3-OR clauses + NOT EXISTS + ORDER BY + LIMIT
    -- 1) per pi row on every page render, so a 4K-item TV section made
    -- the row query take 20+ seconds. With this column the unified
    -- browse query becomes 'LEFT JOIN themes t ON t.id = pi.theme_id'
    -- — a single PK lookup per row.
    theme_id         INTEGER REFERENCES themes (id) ON DELETE SET NULL,
    -- v9+: result of stat()'ing folder_path/theme.mp3 during plex_enum.
    -- 1 = a sidecar file exists at the Plex folder; combined with motif's
    --     own tracking, this lets the SRC badge differentiate cloud-only
    --     Plex themes (P) from local sidecars motif doesn't track (M).
    local_theme_file INTEGER NOT NULL DEFAULT 0,
    -- v1.10.32: same normalized-title cache as themes.title_norm.
    -- Lets the library JOIN's title-fallback compare apples-to-apples
    -- without re-running normalize_title() on every row. Populated by
    -- plex_enum.
    title_norm       TEXT,
    -- v1.17.9: plex_items.motif_unplaced_at retired. Was added v34
    -- (v1.12.110) as a tombstone for the phantom-P recovery path,
    -- deprecated v1.12.111 once `plex_theme_verified_ok` (HEAD
    -- against /library/metadata/{rk}/theme) replaced it. Dead
    -- column for 5 years of tags → DROP COLUMN in _migrate_v52_to_v53.
    -- v1.12.112: source-of-truth verification of Plex's theme claim.
    -- Plex's API returns `theme="/library/metadata/{rk}/theme/{ver}"`
    -- as a CLAIM that Plex serves a theme; but the underlying source
    -- can be a sidecar (motif's or manual), a themerr-plex embed, a
    -- Plex Pass cloud theme — or a stale metadata cache pointing at
    -- a file that no longer exists. plex_theme_uri stores Plex's
    -- claim verbatim; plex_theme_verified_ok records whether a HEAD
    -- against that URL actually returns 200 (1) or 404 (0). Default
    -- NULL = untested → SRC SQL trusts the claim optimistically.
    -- Verification fires from plex_enum for has_theme=1 rows when
    -- verified_ok IS NULL or the URI changed since last enum.
    plex_theme_uri          TEXT,
    plex_theme_verified_at  TEXT,
    plex_theme_verified_ok  INTEGER,
    -- v1.13.38 (v39): persisted +P composite signal. NULL=unknown,
    -- 0=Plex serves no separate theme, 1=Plex serves a theme not
    -- backed by a local sidecar (cloud / Plex Pass / themerr-plex
    -- embed). Set by plex_enum when local_theme_file=0 (the only
    -- window we can observe Plex's independent state); never
    -- overwritten when local_theme_file=1 (sidecar wins, can't
    -- distinguish). Cleared by PURGE handler after HEAD probe
    -- confirms 404. The +P composite UI dot reads this column
    -- directly instead of recomputing from has_theme +
    -- local_theme_file (which produces transient false positives
    -- post-place).
    plex_independent_theme  INTEGER,
    -- v0.51.128 (v70): consecutive full enums this row has been MISSING
    -- from Plex's section listing. The v1.18.90 reaper increments it per
    -- miss + reaps only at >= _REAP_MISS_THRESHOLD, so a transient Plex
    -- glitch (partial catalog / API hiccup) that drops a row for one enum
    -- can't false-DELETE it + fire a false 💔 Theme lost. Reset to 0 the
    -- moment the row reappears.
    consecutive_missing     INTEGER NOT NULL DEFAULT 0,
    first_seen_at    TEXT NOT NULL,
    last_seen_at     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_plex_items_section ON plex_items (section_id);
CREATE INDEX IF NOT EXISTS idx_plex_items_type    ON plex_items (media_type);
CREATE INDEX IF NOT EXISTS idx_plex_items_imdb    ON plex_items (guid_imdb);
CREATE INDEX IF NOT EXISTS idx_plex_items_tmdb    ON plex_items (guid_tmdb);
CREATE INDEX IF NOT EXISTS idx_plex_items_title   ON plex_items (title COLLATE NOCASE);
-- v1.15.62 (schema v50): theme_id is the canonical join key for
-- the topbar FAIL count subqueries (_FAILURES_SFA_FROM_SQL) and
-- the library main filter. Pre-fix no index → every _topbar_ssr_
-- state SSR call did a full plex_items scan per subquery; page
-- load took 60s+. Catches both TDB-tracked rows and orphans.
CREATE INDEX IF NOT EXISTS idx_plex_items_theme_id
    ON plex_items (theme_id);
-- v1.11.10: composite index for the unified library browse query's hot
-- path. /api/library does INNER JOIN plex_sections ON section_id +
-- WHERE media_type=? + ORDER BY pi.title COLLATE NOCASE LIMIT 50; with
-- 10K+ plex_items the planner was post-filter-sorting which made the
-- first page render take 5-10s under concurrent probe write load.
CREATE INDEX IF NOT EXISTS idx_plex_items_section_title
    ON plex_items (section_id, media_type, title COLLATE NOCASE);
CREATE INDEX IF NOT EXISTS idx_plex_items_title_norm
    ON plex_items (media_type, title_norm, year)
    WHERE title_norm IS NOT NULL AND year IS NOT NULL;
-- v1.13.54 (schema v41): post-place outcome stamps and plex_enum
-- reconcile filter on folder_path; pre-index this scanned ~10K rows.
CREATE INDEX IF NOT EXISTS idx_plex_items_folder_path
    ON plex_items (folder_path)
    WHERE folder_path IS NOT NULL AND folder_path != '';
-- v1.13.54: REPROBE candidate query (`WHERE plex_independent_theme
-- IS NULL AND has_theme = 1 AND local_theme_file = 1`). Partial
-- index narrows the scan to unobserved sidecar-bearing rows.
CREATE INDEX IF NOT EXISTS idx_plex_items_indep_null
    ON plex_items (rating_key)
    WHERE plex_independent_theme IS NULL
      AND has_theme = 1
      AND local_theme_file = 1;

CREATE TABLE IF NOT EXISTS pending_updates (
    media_type        TEXT NOT NULL,
    tmdb_id           INTEGER NOT NULL,
    -- v1.12.99: section_id makes pending_updates per-section so a
    -- KEEP CURRENT decision on the standard library doesn't
    -- silently apply to the 4K library and the topbar UPD count
    -- reflects per-section state. '' (empty) is a fallback for
    -- title-global captures (legacy callers without section context);
    -- the library SQL reads section's own row first, then '' fallback.
    section_id        TEXT NOT NULL DEFAULT '',
    theme_id          INTEGER,
    old_video_id      TEXT,
    new_video_id      TEXT,
    old_youtube_url   TEXT,
    new_youtube_url   TEXT,
    upstream_edited_at TEXT,
    detected_at       TEXT NOT NULL,
    decision          TEXT,
    decision_at       TEXT,
    decision_by       TEXT,
    -- v1.12.34: when ACCEPT UPDATE is invoked on a U-source row
    -- (user_overrides present), motif captures the user's URL
    -- here before deleting user_overrides + downloading the new
    -- TDB URL. REVERT restores the user_overrides row from this
    -- column, giving the user a one-click round-trip back to
    -- their custom URL after they accepted an upstream change.
    replaced_user_url TEXT,
    -- v1.12.54: kind discriminator. 'upstream_changed' = the
    -- ThemerrDB URL actually changed (the original semantic);
    -- 'urls_match' = synthetic row written by the v1.12.53
    -- end-of-sync sweep when override URL exactly matches the
    -- TDB URL — a "convert U → T" prompt, not an upstream
    -- update. The frontend pill render branches on this so the
    -- tooltip and (eventually) color tier can match the actual
    -- semantic instead of misrepresenting both states as a
    -- URL-changed event. Defaults to 'upstream_changed' for
    -- back-compat with rows written before this migration.
    kind              TEXT NOT NULL DEFAULT 'upstream_changed'
                        CHECK (kind IN ('upstream_changed', 'urls_match', 'new_theme_available')),
    -- v1.21.51 (schema v62): per-edition theme isolation (see local_files).
    -- '' = standard edition / today's behavior; v63 folds it into the PK.
    edition_key       TEXT NOT NULL DEFAULT '',
    -- v1.21.52 (schema v63): edition_key folded into the PK (Phase A2).
    PRIMARY KEY (media_type, tmdb_id, section_id, edition_key),
    FOREIGN KEY (media_type, tmdb_id) REFERENCES themes (media_type, tmdb_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_pending_updates_decision
    ON pending_updates (decision, detected_at);
CREATE INDEX IF NOT EXISTS idx_pending_updates_target
    ON pending_updates (media_type, tmdb_id);

-- v1.19.12 (schema v57): clean up orphan urls_match pending_updates
-- when their user_overrides row is deleted. urls_match is by
-- definition a U→T conversion offer; with no user_override left to
-- convert FROM, the prompt is meaningless. Pre-fix, ~12 code paths
-- delete user_overrides (REPLACE TDB, ACCEPT UPDATE, PURGE, sync
-- normalization, recovery walkers, etc.) — adding a follow-up
-- DELETE at each site is tedious + error-prone. The trigger handles
-- it once at the source-of-truth schema level: any future code path
-- that deletes user_overrides automatically cleans the orphan.
-- the user's 2026-05-24 repro: a T+PU row (Cowboys & Aliens) showed
-- the !UPD pill from an orphan kind='urls_match' pending_updates
-- row whose user_overrides counterpart had been deleted by an
-- earlier action (likely REPLACE TDB or ACCEPT UPDATE).
CREATE TRIGGER IF NOT EXISTS trg_user_overrides_cleanup_urls_match
AFTER DELETE ON user_overrides
FOR EACH ROW
BEGIN
    DELETE FROM pending_updates
    WHERE media_type = OLD.media_type
      AND tmdb_id = OLD.tmdb_id
      AND section_id = OLD.section_id
      AND edition_key = OLD.edition_key
      AND kind = 'urls_match';
END;

CREATE TABLE IF NOT EXISTS scan_runs (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at        TEXT NOT NULL,
    finished_at       TEXT,
    status            TEXT NOT NULL DEFAULT 'running'
                        CHECK (status IN ('running','complete','failed','cancelled')),
    sections_scanned  INTEGER NOT NULL DEFAULT 0,
    folders_walked    INTEGER NOT NULL DEFAULT 0,
    themes_found      INTEGER NOT NULL DEFAULT 0,
    findings_count    INTEGER NOT NULL DEFAULT 0,
    initiated_by      TEXT,
    error             TEXT
);
CREATE INDEX IF NOT EXISTS idx_scan_runs_status ON scan_runs (status, started_at);

CREATE TABLE IF NOT EXISTS scan_findings (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    scan_run_id         INTEGER NOT NULL REFERENCES scan_runs(id) ON DELETE CASCADE,
    section_id          TEXT NOT NULL,
    section_type        TEXT NOT NULL CHECK (section_type IN ('movie','show')),
    media_folder        TEXT NOT NULL,
    file_path           TEXT NOT NULL,
    file_size           INTEGER NOT NULL,
    file_mtime          TEXT NOT NULL,
    file_sha256         TEXT NOT NULL,
    finding_kind        TEXT NOT NULL
                          CHECK (finding_kind IN
                            ('exact_match','hash_match','content_mismatch',
                             'orphan_resolvable','orphan_unresolved')),
    theme_id            INTEGER REFERENCES themes(id) ON DELETE SET NULL,
    resolved_metadata   TEXT,
    decision            TEXT NOT NULL DEFAULT 'pending'
                          CHECK (decision IN
                            ('pending','adopt','replace','keep_existing','ignore')),
    decision_at         TEXT,
    decision_by         TEXT,
    adopt_outcome       TEXT,
    adopted_at          TEXT
);
CREATE INDEX IF NOT EXISTS idx_scan_findings_run ON scan_findings (scan_run_id, decision);
CREATE INDEX IF NOT EXISTS idx_scan_findings_kind ON scan_findings (finding_kind, decision);
CREATE INDEX IF NOT EXISTS idx_scan_findings_hash ON scan_findings (file_sha256);

CREATE TABLE IF NOT EXISTS tvdb_lookup_cache (
    cache_key      TEXT PRIMARY KEY,
    response_json  TEXT NOT NULL,
    fetched_at     TEXT NOT NULL,
    expires_at     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS runtime_settings (
    key         TEXT PRIMARY KEY,
    value       TEXT NOT NULL,
    updated_at  TEXT NOT NULL,
    updated_by  TEXT
);

-- v1.21.2 (schema v61): persistent oEmbed cache. Video-title lookups
-- (/api/source/oembed -> YouTube/SoundCloud) were cached ONLY in an
-- in-memory LRU, wiped on every restart/redeploy. the user redeploys per
-- tag, so the cache was perpetually cold -> each INFO card's first open
-- re-fetched its title from YouTube (130-520ms external round-trip),
-- the "slow on first click" symptom. This persists the slim payload so
-- it survives restarts; after the first-ever fetch per URL it's a
-- permanent hit (the in-memory LRU stays as the hot tier in front).
CREATE TABLE IF NOT EXISTS oembed_cache (
    url         TEXT PRIMARY KEY,
    payload     TEXT NOT NULL,
    fetched_at  TEXT NOT NULL
);

-- v1.12.86: per-section one-step REVERT/RESTORE snapshots. Replaces
-- the v1.12.37 themes.previous_youtube_url + previous_youtube_kind
-- columns, which were title-global (one undo slot per title) and
-- caused cross-section bleed: a PURGE in 4K then PURGE in standard
-- overwrote the 4K's snapshot, breaking RESTORE. Now keyed by
-- (media_type, tmdb_id, section_id) so each section gets its own
-- independent slot.
--
-- The empty-string section_id ('') row acts as a fallback for
-- sections that don't have their own snapshot — mirrors the
-- user_overrides pattern. The library main query reads this table
-- via LEFT JOIN (per-section first, then '' fallback) to populate
-- has_previous_url, previous_youtube_kind, and revert_redundant.
CREATE TABLE IF NOT EXISTS previous_urls (
    -- v1.18.0 (schema v55): 'collection' added so PURGE/UNMANAGE
    -- on collection rows can capture RESTORE-able URLs the same
    -- way movies/TV do.
    media_type   TEXT NOT NULL CHECK (media_type IN ('movie', 'tv', 'collection')),
    tmdb_id      INTEGER NOT NULL,
    section_id   TEXT NOT NULL DEFAULT '',
    youtube_url  TEXT NOT NULL,
    kind         TEXT NOT NULL CHECK (kind IN ('user', 'themerrdb')),
    captured_at  TEXT NOT NULL,
    -- v1.12.102: 2-deep undo for PURGE/UNMANAGE → RESTORE → REVERT.
    -- Pre-fix the v1.12.99 user-kind preservation rule meant a PURGE
    -- on a T row with a prior user-kind previous_url couldn't capture
    -- the URL being purged (existing user-kind capture won the
    -- "what's most useful to undo" coin flip). RESTORE then brought
    -- back the older user URL, not the URL the user just purged.
    -- New rule: PURGE/UNMANAGE always overwrites previous_url with
    -- the URL being dropped, but the prior previous_url moves into
    -- hidden_* so REVERT after RESTORE walks the chain back to it.
    -- Non-PURGE captures (SET URL, ACCEPT UPDATE, REPLACE TDB)
    -- clear hidden_* — the chain only survives the round-trip that
    -- created it.
    hidden_url          TEXT,
    hidden_kind         TEXT CHECK (hidden_kind IN ('user', 'themerrdb')
                                    OR hidden_kind IS NULL),
    hidden_captured_at  TEXT,
    PRIMARY KEY (media_type, tmdb_id, section_id),
    -- v1.12.86: when the parent themes row is dropped (PURGE/UNMANAGE
    -- last-section orphan, or api_delete_item full destruction), the
    -- snapshot is meaningless — there's no canonical to revert TO.
    -- ON DELETE CASCADE prevents orphan previous_urls rows from
    -- accumulating across the lifetime of the install. PRAGMA
    -- foreign_keys=ON is set in get_conn().
    FOREIGN KEY (media_type, tmdb_id) REFERENCES themes(media_type, tmdb_id)
        ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_previous_urls_target
    ON previous_urls (media_type, tmdb_id);

-- v1.12.80: append-only provenance log for URL changes, override
-- set/clear, and accept/decline decisions. Distinct from `events`
-- (rotates) — audit_events lives forever so "who changed Willy
-- Wonka and when" is answerable months later.
CREATE TABLE IF NOT EXISTS audit_events (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    occurred_at  TEXT NOT NULL,
    actor        TEXT NOT NULL,
    action       TEXT NOT NULL,
    media_type   TEXT,
    tmdb_id      INTEGER,
    section_id   TEXT,
    details      TEXT
);
CREATE INDEX IF NOT EXISTS idx_audit_events_target
    ON audit_events (media_type, tmdb_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_events_occurred
    ON audit_events (occurred_at DESC);

-- v1.13.11: saved filter presets. Currently scoped to the library page;
-- `scope` lets future pages (queue, history) share the same table.
-- query_json stores the URL query-string fragments verbatim so the UI
-- can apply a saved preset by re-issuing the same fetch params.
CREATE TABLE IF NOT EXISTS saved_filters (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL,
    scope       TEXT NOT NULL DEFAULT 'library',
    query_json  TEXT NOT NULL,
    created_at  TEXT NOT NULL,
    created_by  TEXT
);
CREATE INDEX IF NOT EXISTS idx_saved_filters_scope
    ON saved_filters (scope, name);

-- v1.13.54 (schema v41): per-section failure acknowledgements.
-- themes.failure_acked_at is title-global; the adopt-and-ack /
-- purge-and-ack flows fire a section-scoped action followed by
-- /clear-failure, which used to clear the global flag and bleed
-- the dismissal across every section that owns the title. This
-- table is the new section-scoped sink: write into it when the
-- caller passes section_id, and the library SQL + recovery-options
-- endpoint read it (per-section first, themes.failure_acked_at as
-- a fallback) so a 4K-only adopt no longer dismisses the standard
-- row's failure.
CREATE TABLE IF NOT EXISTS section_failure_acks (
    -- v1.18.0 (schema v55): 'collection' added so failure-ack flows
    -- (TDB URL probe failures, etc.) can apply to collection rows.
    media_type   TEXT NOT NULL CHECK (media_type IN ('movie', 'tv', 'collection')),
    tmdb_id      INTEGER NOT NULL,
    section_id   TEXT NOT NULL,
    acked_at     TEXT NOT NULL,
    acked_by     TEXT,
    PRIMARY KEY (media_type, tmdb_id, section_id)
);
CREATE INDEX IF NOT EXISTS idx_section_failure_acks_lookup
    ON section_failure_acks (media_type, tmdb_id);
"""

CURRENT_SCHEMA_VERSION = 78


def _add_column(conn: sqlite3.Connection, table: str, column: str,
                decl: str) -> None:
    # v1.24.50: idempotent ALTER TABLE ADD COLUMN — skip when the column already
    # exists. The migration runner stamps schema_version per-step but
    # executescript autocommits each statement, so a crash in the window between a
    # column's commit and the version stamp (kill -9 / OOM / power-loss) leaves
    # the column present + the version behind → boot re-runs the migration → bare
    # ADD COLUMN raised "duplicate column name" forever (crash-loop). SQLite has no
    # ADD COLUMN IF NOT EXISTS; PRAGMA table_info is the idiom — the v62→v67
    # migrations already inline this guard, this is the shared form.
    cols = {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}
    if column not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {decl}")


def _migrate_v1_to_v2(conn: sqlite3.Connection) -> None:
    """v2 widens the jobs.job_type CHECK constraint to include 'relink'.
    SQLite can't ALTER CHECK constraints in place, so we recreate the table.
    """
    log.info("Migrating jobs table to schema v2 (add 'relink' job type)")
    conn.executescript("""
        BEGIN;
        CREATE TABLE jobs_new (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            job_type        TEXT NOT NULL CHECK (job_type IN ('sync', 'download', 'place', 'refresh', 'relink')),
            media_type      TEXT,
            tmdb_id         INTEGER,
            payload         TEXT,
            status          TEXT NOT NULL DEFAULT 'pending'
                               CHECK (status IN ('pending', 'running', 'done', 'failed', 'cancelled')),
            attempts        INTEGER NOT NULL DEFAULT 0,
            max_attempts    INTEGER NOT NULL DEFAULT 3,
            last_error      TEXT,
            created_at      TEXT NOT NULL,
            started_at      TEXT,
            finished_at     TEXT,
            next_run_at     TEXT
        );
        INSERT INTO jobs_new SELECT * FROM jobs;
        DROP TABLE jobs;
        ALTER TABLE jobs_new RENAME TO jobs;
        CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs (status, next_run_at);
        CREATE INDEX IF NOT EXISTS idx_jobs_type ON jobs (job_type, status);
        COMMIT;
    """)


def _migrate_v2_to_v3(conn: sqlite3.Connection) -> None:
    """v3 adds the runtime_settings table for UI-toggleable runtime options
    like dry_run."""
    log.info("Migrating to schema v3 (add runtime_settings table)")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS runtime_settings (
            key         TEXT PRIMARY KEY,
            value       TEXT NOT NULL,
            updated_at  TEXT NOT NULL,
            updated_by  TEXT
        );
    """)


def _migrate_v3_to_v4(conn: sqlite3.Connection) -> None:
    """v4 adds:
       - plex_sections cache (auto-discovered libraries with metadata)
       - pending_updates (upstream changes the user hasn't acted on yet)
       - failure_kind columns on themes (download failure classification)
       - provenance column on placements (auto vs manual override vs cloud)
    """
    log.info("Migrating to schema v4 (sections, updates, failures, provenance)")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS plex_sections (
            section_id      TEXT PRIMARY KEY,
            uuid            TEXT,
            title           TEXT NOT NULL,
            type            TEXT NOT NULL CHECK (type IN ('movie', 'show')),
            agent           TEXT,
            language        TEXT,
            location_paths  TEXT,         -- JSON list of file paths
            included        INTEGER NOT NULL DEFAULT 1,  -- 1=managed by motif, 0=excluded
            discovered_at   TEXT NOT NULL,
            last_seen_at    TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS pending_updates (
            media_type        TEXT NOT NULL,
            tmdb_id           INTEGER NOT NULL,
            old_video_id      TEXT,
            new_video_id      TEXT,
            old_youtube_url   TEXT,
            new_youtube_url   TEXT,
            upstream_edited_at TEXT,
            detected_at       TEXT NOT NULL,
            decision          TEXT,        -- 'pending', 'accepted', 'declined'
            decision_at       TEXT,
            decision_by       TEXT,
            PRIMARY KEY (media_type, tmdb_id),
            FOREIGN KEY (media_type, tmdb_id) REFERENCES themes (media_type, tmdb_id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_pending_updates_decision
            ON pending_updates (decision, detected_at);

        ALTER TABLE themes ADD COLUMN failure_kind TEXT;
        ALTER TABLE themes ADD COLUMN failure_message TEXT;
        ALTER TABLE themes ADD COLUMN failure_at TEXT;

        ALTER TABLE placements ADD COLUMN provenance TEXT NOT NULL DEFAULT 'auto'
            CHECK (provenance IN ('auto', 'manual', 'cloud'));

        ALTER TABLE local_files ADD COLUMN provenance TEXT NOT NULL DEFAULT 'auto'
            CHECK (provenance IN ('auto', 'manual'));
    """)


def _migrate_v4_to_v5(conn: sqlite3.Connection) -> None:
    """v5 adds support for filesystem scans (Plex folder adoption) and orphan
    themes (theme.mp3 files that exist in Plex but have no upstream record).

    Changes:
      - themes: add `id INTEGER` (autoincrement, but kept nullable for legacy
        rows; a unique index makes lookups by id efficient). Add `tvdb_id`
        column. Allow negative tmdb_ids by relaxing nothing (the column was
        always INTEGER; the negative-id sentinel is by convention).
      - themes: extend upstream_source CHECK to include 'plex_orphan'.
      - local_files, placements, pending_updates, user_overrides: add
        `theme_id INTEGER` column for future path; existing FK on
        (media_type, tmdb_id) remains the actual relational key.
      - new tables: scan_runs, scan_findings, tvdb_lookup_cache.
      - widen jobs.job_type CHECK to include 'scan' and 'adopt'.
    """
    log.info("Migrating to schema v5 (scan/adopt + orphan themes)")

    # The CHECK constraint on themes.upstream_source needs widening to include
    # 'plex_orphan'. SQLite doesn't allow ALTER ... CHECK, so we recreate the
    # table. Same for jobs.job_type. Existing data is preserved through
    # INSERT INTO ... SELECT ... patterns.
    #
    # CRITICAL: foreign_keys must be OFF during the table-recreate dance,
    # otherwise DROP TABLE themes cascades and wipes all FK'd rows in
    # local_files / placements / pending_updates. We turn FKs off, do the
    # migration, then turn them back on.
    #
    # v1.21.8 (audit HIGH): commit any pending transaction BEFORE the
    # FK toggle. The migration dispatch loop runs steps on one
    # connection; a prior step's writes leave an implicit transaction
    # open, and `PRAGMA foreign_keys = OFF` is a documented no-op
    # INSIDE a transaction → FK stays ON → the `DROP TABLE themes`
    # below cascade-wipes local_files / placements / pending_updates
    # (the v1.18.0 catastrophe). The widen migrations (v55/v58/v60)
    # already guard this via _widen_check_constraint's commit + assert;
    # this hand-rolled v4→v5 rebuild predated that fix and was the lone
    # unprotected table-rebuild. The hard-assert refuses to DROP if the
    # OFF didn't take effect.
    conn.commit()
    conn.execute("PRAGMA foreign_keys = OFF")
    if conn.execute("PRAGMA foreign_keys").fetchone()[0] != 0:
        raise RuntimeError(
            "_migrate_v4_to_v5: `PRAGMA foreign_keys = OFF` did not "
            "take effect (still inside a transaction?). Refusing to "
            "DROP TABLE themes with FK enforcement ON — it would "
            "cascade-wipe local_files / placements. Commit before "
            "toggling FK."
        )
    try:
        conn.executescript("""

        -- ======================================================================
        -- themes: add id, tvdb_id; extend upstream_source CHECK
        -- ======================================================================
        CREATE TABLE themes_new (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            media_type           TEXT NOT NULL CHECK (media_type IN ('movie', 'tv')),
            tmdb_id              INTEGER NOT NULL,
            tvdb_id              INTEGER,
            imdb_id              TEXT,
            title                TEXT NOT NULL,
            original_title       TEXT,
            year                 TEXT,
            release_date         TEXT,
            youtube_url          TEXT,
            youtube_video_id     TEXT,
            youtube_added_at     TEXT,
            youtube_edited_at    TEXT,
            upstream_source      TEXT NOT NULL
                                   CHECK (upstream_source IN ('imdb', 'themoviedb', 'plex_orphan')),
            raw_json             TEXT,
            last_seen_sync_at    TEXT NOT NULL,
            first_seen_sync_at   TEXT NOT NULL,
            failure_kind         TEXT,
            failure_message      TEXT,
            failure_at           TEXT,
            UNIQUE (media_type, tmdb_id)
        );

        INSERT INTO themes_new (
            media_type, tmdb_id, imdb_id, title, original_title, year, release_date,
            youtube_url, youtube_video_id, youtube_added_at, youtube_edited_at,
            upstream_source, raw_json, last_seen_sync_at, first_seen_sync_at,
            failure_kind, failure_message, failure_at
        )
        SELECT
            media_type, tmdb_id, imdb_id, title, original_title, year, release_date,
            youtube_url, youtube_video_id, youtube_added_at, youtube_edited_at,
            upstream_source, raw_json, last_seen_sync_at, first_seen_sync_at,
            failure_kind, failure_message, failure_at
        FROM themes;

        DROP TABLE themes;
        ALTER TABLE themes_new RENAME TO themes;

        CREATE INDEX IF NOT EXISTS idx_themes_imdb ON themes (imdb_id) WHERE imdb_id IS NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_themes_tvdb ON themes (tvdb_id) WHERE tvdb_id IS NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_themes_video ON themes (youtube_video_id);
        CREATE INDEX IF NOT EXISTS idx_themes_title ON themes (title);
        CREATE INDEX IF NOT EXISTS idx_themes_failure ON themes (failure_kind) WHERE failure_kind IS NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_themes_orphan ON themes (upstream_source) WHERE upstream_source = 'plex_orphan';

        -- ======================================================================
        -- jobs: extend job_type to include 'scan' and 'adopt'
        -- ======================================================================
        CREATE TABLE jobs_new (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            job_type        TEXT NOT NULL
                              CHECK (job_type IN ('sync','download','place','refresh','relink','scan','adopt')),
            media_type      TEXT,
            tmdb_id         INTEGER,
            payload         TEXT,
            status          TEXT NOT NULL DEFAULT 'pending'
                              CHECK (status IN ('pending','running','done','failed','cancelled')),
            attempts        INTEGER NOT NULL DEFAULT 0,
            max_attempts    INTEGER NOT NULL DEFAULT 3,
            last_error      TEXT,
            created_at      TEXT NOT NULL,
            started_at      TEXT,
            finished_at     TEXT,
            next_run_at     TEXT
        );
        INSERT INTO jobs_new SELECT * FROM jobs;
        DROP TABLE jobs;
        ALTER TABLE jobs_new RENAME TO jobs;
        CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs (status, next_run_at);
        CREATE INDEX IF NOT EXISTS idx_jobs_type ON jobs (job_type, status);

        -- ======================================================================
        -- Add theme_id column to FK'd tables. Backfill from themes.id via the
        -- (media_type, tmdb_id) join. Existing code keeps using (media_type,
        -- tmdb_id) — the new column is for future code paths.
        -- ======================================================================
        ALTER TABLE local_files     ADD COLUMN theme_id INTEGER;
        ALTER TABLE placements      ADD COLUMN theme_id INTEGER;
        ALTER TABLE pending_updates ADD COLUMN theme_id INTEGER;
        ALTER TABLE user_overrides  ADD COLUMN theme_id INTEGER;

        UPDATE local_files SET theme_id = (
            SELECT id FROM themes
             WHERE themes.media_type = local_files.media_type
               AND themes.tmdb_id = local_files.tmdb_id
        );
        UPDATE placements SET theme_id = (
            SELECT id FROM themes
             WHERE themes.media_type = placements.media_type
               AND themes.tmdb_id = placements.tmdb_id
        );
        UPDATE pending_updates SET theme_id = (
            SELECT id FROM themes
             WHERE themes.media_type = pending_updates.media_type
               AND themes.tmdb_id = pending_updates.tmdb_id
        );
        UPDATE user_overrides SET theme_id = (
            SELECT id FROM themes
             WHERE themes.media_type = user_overrides.media_type
               AND themes.tmdb_id = user_overrides.tmdb_id
        );

        -- ======================================================================
        -- scan_runs: one row per Plex folder scan
        -- ======================================================================
        CREATE TABLE scan_runs (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at        TEXT NOT NULL,
            finished_at       TEXT,
            status            TEXT NOT NULL DEFAULT 'running'
                                CHECK (status IN ('running','complete','failed','cancelled')),
            sections_scanned  INTEGER NOT NULL DEFAULT 0,
            folders_walked    INTEGER NOT NULL DEFAULT 0,
            themes_found      INTEGER NOT NULL DEFAULT 0,
            findings_count    INTEGER NOT NULL DEFAULT 0,
            initiated_by      TEXT,
            error             TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_scan_runs_status ON scan_runs (status, started_at);

        -- ======================================================================
        -- scan_findings: one row per theme.mp3 found during a scan
        -- ======================================================================
        CREATE TABLE scan_findings (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_run_id         INTEGER NOT NULL REFERENCES scan_runs(id) ON DELETE CASCADE,
            section_id          TEXT NOT NULL,                  -- Plex section
            section_type        TEXT NOT NULL CHECK (section_type IN ('movie','show')),
            media_folder        TEXT NOT NULL,                  -- absolute path to the folder
            file_path           TEXT NOT NULL,                  -- absolute path to theme.mp3
            file_size           INTEGER NOT NULL,
            file_mtime          TEXT NOT NULL,
            file_sha256         TEXT NOT NULL,
            -- finding_kind:
            --   exact_match       — file at the canonical themes_dir matches by hash AND inode
            --   hash_match        — same content as a known motif file, but separate inode (adoptable)
            --   content_mismatch  — folder has a theme.mp3 that doesn't match motif's known content for this item
            --   orphan_resolvable — no themes row matches this folder by title+year, but we have nfo/tvdb metadata
            --   orphan_unresolved — no themes row, no metadata: a fallback synthetic id will be allocated
            finding_kind        TEXT NOT NULL
                                  CHECK (finding_kind IN
                                    ('exact_match','hash_match','content_mismatch',
                                     'orphan_resolvable','orphan_unresolved')),
            -- The themes row this finding maps to (if any). For orphans
            -- this is NULL until the user adopts.
            theme_id            INTEGER REFERENCES themes(id) ON DELETE SET NULL,
            -- For orphans: the parsed metadata we discovered, JSON.
            resolved_metadata   TEXT,
            -- decision lifecycle:
            decision            TEXT NOT NULL DEFAULT 'pending'
                                  CHECK (decision IN
                                    ('pending','adopt','replace','keep_existing','ignore')),
            decision_at         TEXT,
            decision_by         TEXT,
            -- adoption result (filled when decision is acted on):
            adopt_outcome       TEXT,                          -- JSON
            adopted_at          TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_scan_findings_run
            ON scan_findings (scan_run_id, decision);
        CREATE INDEX IF NOT EXISTS idx_scan_findings_kind
            ON scan_findings (finding_kind, decision);
        CREATE INDEX IF NOT EXISTS idx_scan_findings_hash
            ON scan_findings (file_sha256);

        -- ======================================================================
        -- tvdb_lookup_cache: stores TVDB API responses to avoid re-querying
        -- ======================================================================
        CREATE TABLE tvdb_lookup_cache (
            cache_key      TEXT PRIMARY KEY,         -- e.g. 'movie:title=Inception:year=2010'
            response_json  TEXT NOT NULL,
            fetched_at     TEXT NOT NULL,
            expires_at     TEXT NOT NULL
        );
    """)
    finally:
        conn.execute("PRAGMA foreign_keys = ON")


def _migrate_v5_to_v6(conn: sqlite3.Connection) -> None:
    """v6 marks the canonical-layout switch from <vid>.mp3 to
    <subdir>/theme.mp3 (mirroring Plex's "Title (Year)" folder shape).

    No schema changes — the version bump pairs with a startup task that
    walks local_files, renames files on disk, and updates file_path. That
    runs in app.main because it needs filesystem access (settings.themes_dir)
    that migrations don't have.
    """
    log.info("Migrating to schema v6 (canonical layout switch — no schema changes)")


def _migrate_v6_to_v7(conn: sqlite3.Connection) -> None:
    """v7 adds plex_items: a cache of every item Plex sees in managed sections.

    motif's themes table is keyed off ThemerrDB; plex_items is keyed off Plex's
    rating_key. The unified browse view in v1.6 LEFT JOINs the two so users
    can see every Plex item with a "has theme support?" badge, even when
    ThemerrDB doesn't cover the title. Manual MP3 upload then targets a
    plex_items row directly.

    Schema is purely additive — existing v6 data is untouched.
    """
    log.info("Migrating to schema v7 (plex_items table)")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS plex_items (
            rating_key      TEXT PRIMARY KEY,
            section_id      TEXT NOT NULL,
            media_type      TEXT NOT NULL CHECK (media_type IN ('movie', 'show')),
            title           TEXT NOT NULL,
            year            TEXT,
            guid_imdb       TEXT,
            guid_tmdb       INTEGER,
            guid_tvdb       INTEGER,
            folder_path     TEXT NOT NULL DEFAULT '',
            has_theme       INTEGER NOT NULL DEFAULT 0,
            first_seen_at   TEXT NOT NULL,
            last_seen_at    TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_plex_items_section ON plex_items (section_id);
        CREATE INDEX IF NOT EXISTS idx_plex_items_type    ON plex_items (media_type);
        CREATE INDEX IF NOT EXISTS idx_plex_items_imdb    ON plex_items (guid_imdb);
        CREATE INDEX IF NOT EXISTS idx_plex_items_tmdb    ON plex_items (guid_tmdb);
        CREATE INDEX IF NOT EXISTS idx_plex_items_title   ON plex_items (title COLLATE NOCASE);
    """)
    # widen jobs.job_type to include 'plex_enum'
    conn.executescript("""
        CREATE TABLE jobs__new (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            job_type        TEXT NOT NULL CHECK (
                job_type IN ('sync', 'download', 'place', 'refresh', 'relink',
                             'scan', 'adopt', 'plex_enum')
            ),
            media_type      TEXT,
            tmdb_id         INTEGER,
            payload         TEXT,
            status          TEXT NOT NULL DEFAULT 'pending'
                               CHECK (status IN ('pending', 'running',
                                                 'success', 'failed', 'cancelled')),
            attempts        INTEGER NOT NULL DEFAULT 0,
            last_error      TEXT,
            created_at      TEXT NOT NULL,
            next_run_at     TEXT NOT NULL,
            started_at      TEXT,
            finished_at     TEXT
        );
        INSERT INTO jobs__new (id, job_type, media_type, tmdb_id, payload, status,
                               attempts, last_error, created_at, next_run_at,
                               started_at, finished_at)
            SELECT id, job_type, media_type, tmdb_id, payload, status,
                   attempts, last_error, created_at, next_run_at,
                   started_at, finished_at FROM jobs;
        DROP TABLE jobs;
        ALTER TABLE jobs__new RENAME TO jobs;
        CREATE INDEX IF NOT EXISTS idx_jobs_status_next ON jobs (status, next_run_at);
        CREATE INDEX IF NOT EXISTS idx_jobs_item       ON jobs (media_type, tmdb_id);
    """)


def _migrate_v8_to_v9(conn: sqlite3.Connection) -> None:
    """v9 adds plex_items.local_theme_file (whether a theme.mp3 sidecar
    exists at the Plex folder). Populated by plex_enum during enumeration;
    used by the SRC badge logic to distinguish Plex cloud themes (P) from
    user-dropped local sidecars (M).
    """
    log.info("Migrating to schema v9 (plex_items.local_theme_file)")
    conn.executescript(
        "ALTER TABLE plex_items ADD COLUMN local_theme_file INTEGER NOT NULL DEFAULT 0;"
    )


def _migrate_v24_to_v25(conn: sqlite3.Connection) -> None:
    """v25 adds themes.previous_youtube_url + previous_youtube_kind
    for the v1.12.37 generalized REVERT.

    Pre-fix (v1.12.34-v1.12.36) only post-accept-update U rows had
    a captured prior URL (pending_updates.replaced_user_url). The
    user wants REVERT to be a one-step undo for any URL-changing
    action — so SET URL replacing an existing override, REPLACE
    TDB on a U row, or ACCEPT UPDATE all populate the previous_*
    columns the same way.

    The v1.12.34 pending_updates.replaced_user_url column is
    superseded but preserved for back-compat with existing
    accepted-update rows. The migration backfills any non-null
    replaced_user_url values into themes.previous_youtube_url
    with kind='user' so previously-accepted rows keep their
    REVERT capability.
    """
    log.info("Migrating to schema v25 (themes.previous_youtube_url)")
    conn.executescript(
        """
        ALTER TABLE themes ADD COLUMN previous_youtube_url TEXT;
        ALTER TABLE themes ADD COLUMN previous_youtube_kind TEXT
            CHECK (previous_youtube_kind IN ('user', 'themerrdb')
                   OR previous_youtube_kind IS NULL);

        UPDATE themes
           SET previous_youtube_url = (
                 SELECT pu.replaced_user_url
                   FROM pending_updates pu
                  WHERE pu.media_type = themes.media_type
                    AND pu.tmdb_id = themes.tmdb_id
                    AND pu.replaced_user_url IS NOT NULL
               ),
               previous_youtube_kind = (
                 SELECT 'user'
                   FROM pending_updates pu
                  WHERE pu.media_type = themes.media_type
                    AND pu.tmdb_id = themes.tmdb_id
                    AND pu.replaced_user_url IS NOT NULL
               )
         WHERE EXISTS (
                 SELECT 1 FROM pending_updates pu
                  WHERE pu.media_type = themes.media_type
                    AND pu.tmdb_id = themes.tmdb_id
                    AND pu.replaced_user_url IS NOT NULL
               );
        """
    )


def _migrate_v25_to_v26(conn: sqlite3.Connection) -> None:
    """v26 adds pending_updates.kind discriminating between
    'upstream_changed' (TDB URL actually changed — original semantic)
    and 'urls_match' (synthetic row written by the v1.12.53
    end-of-sync sweep when override URL exactly matches the TDB URL).

    Frontends can branch on kind so the row pill tooltip reads
    accurately for each case — pre-fix the synthetic case
    inherited the upstream-changed tooltip ("ThemerrDB upstream URL
    changed") which was a lie for those rows. Existing rows
    backfill to 'upstream_changed' since v1.12.53 is the first
    version that wrote the synthetic kind.
    """
    log.info("Migrating to schema v26 (pending_updates.kind)")
    _add_column(conn, "pending_updates", "kind",
                "TEXT NOT NULL DEFAULT 'upstream_changed' "
                "CHECK (kind IN ('upstream_changed', 'urls_match'))")


def _migrate_v26_to_v27(conn: sqlite3.Connection) -> None:
    """v27 adds user_overrides.section_id for per-edition override
    URLs. Previously a single (media_type, tmdb_id) row applied to
    every section that owned the title — users with separately-
    themed editions (4K + standard, anime + plain, director's cut
    + theatrical) couldn't pin different YouTube sources per
    edition without piecemeal hacks.

    New schema: PK becomes (media_type, tmdb_id, section_id).
    Empty string ('') in section_id means "applies to every
    section that doesn't have its own override" — the legacy
    global state and the default for callers without section
    context. Worker fetch is two-step: per-section first, then
    fall back to ''.

    SQLite ALTER TABLE can't change the PK in place, so the
    migration recreates the table:
      1. CREATE the new shape under a temp name
      2. INSERT existing rows with section_id = ''
      3. DROP the old table, rename the new one in
    Rows written before v1.12.72 land with section_id='' so
    behavior is unchanged for unmigrated data — they remain
    "applies globally" until a per-section SET URL writes a
    more-specific row.
    """
    log.info("Migrating to schema v27 (user_overrides.section_id)")
    conn.executescript(
        """
        BEGIN;
        -- v1.22.66: BEGIN/COMMIT added (audit round 2) — executescript
        -- runs statement-by-statement in autocommit without it, so a
        -- container kill between DROP and RENAME stranded the data in
        -- the shadow table and crash-looped every boot (the v1.19.73
        -- incident class). The later rebuilds (v47+) always had this.
        CREATE TABLE user_overrides_v27 (
            media_type      TEXT NOT NULL,
            tmdb_id         INTEGER NOT NULL,
            theme_id        INTEGER,
            youtube_url     TEXT NOT NULL,
            set_at          TEXT NOT NULL,
            set_by          TEXT,
            note            TEXT,
            section_id      TEXT NOT NULL DEFAULT '',
            PRIMARY KEY (media_type, tmdb_id, section_id)
        );
        INSERT INTO user_overrides_v27
            (media_type, tmdb_id, theme_id, youtube_url,
             set_at, set_by, note, section_id)
        SELECT media_type, tmdb_id, theme_id, youtube_url,
               set_at, set_by, note, ''
          FROM user_overrides;
        DROP TABLE user_overrides;
        ALTER TABLE user_overrides_v27 RENAME TO user_overrides;
        COMMIT;
        """
    )


def _migrate_v27_to_v28(conn: sqlite3.Connection) -> None:
    """v28 adds audit_events — an append-only provenance log for
    every URL change, override set/clear, and accept/decline
    decision. Distinct from log_event (which captures generic
    events including transient debug/info ones and gets rotated
    out of the live DB). audit_events is intended to live forever:
    it answers "who changed Willy Wonka's theme URL on 2026-04-12,
    and what was it before?" months later, after the rolling log
    has cycled.

    Indexed by (media_type, tmdb_id, occurred_at DESC) for fast
    per-row lookups in the INFO card; secondary index on
    occurred_at for time-range scans.

    details is a JSON blob — schema varies by action so a fixed
    column shape would either be too narrow or too wide. Each
    action documents its own keys (e.g., set_url stores
    {old_url, new_url, scope}; accept_update stores
    {old_url, new_url, kind}).
    """
    log.info("Migrating to schema v28 (audit_events)")
    # v1.24.50: IF NOT EXISTS on the table + indexes so a crash after the table
    # commit (before the version stamp) doesn't crash-loop on re-run.
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS audit_events (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            occurred_at  TEXT NOT NULL,
            actor        TEXT NOT NULL,
            action       TEXT NOT NULL,
            media_type   TEXT,
            tmdb_id      INTEGER,
            section_id   TEXT,
            details      TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_audit_events_target
            ON audit_events (media_type, tmdb_id, occurred_at DESC);
        CREATE INDEX IF NOT EXISTS idx_audit_events_occurred
            ON audit_events (occurred_at DESC);
        """
    )


def _migrate_v29_to_v30(conn: sqlite3.Connection) -> None:
    """v30 moves the one-step REVERT/RESTORE snapshot from
    themes.previous_youtube_url / previous_youtube_kind (title-global,
    one slot per title) to a new previous_urls table keyed by
    (media_type, tmdb_id, section_id).

    Pre-fix the title-global slot caused cross-section bleed: a PURGE
    in section A then a PURGE in section B overwrote A's snapshot, so
    the user could only RESTORE one section's URL even when both
    sections had been independently configured. The new table gives
    each section its own slot with an '' (empty string) row acting
    as a global fallback for sections that never captured their own.

    Migration steps:
      1. CREATE previous_urls table + index.
      2. Backfill from themes: any non-null previous_youtube_url
         lands in previous_urls with section_id='' (acts as the
         universal fallback under the new read logic).
      3. Drop themes.previous_youtube_url + previous_youtube_kind.
         SQLite ≥ 3.35 supports ALTER TABLE DROP COLUMN; older code
         paths recreate-and-rename. We use DROP COLUMN here since
         motif requires modern SQLite anyway.
    """
    log.info("Migrating to schema v30 (previous_urls per-section)")
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS previous_urls (
            media_type   TEXT NOT NULL CHECK (media_type IN ('movie', 'tv')),
            tmdb_id      INTEGER NOT NULL,
            section_id   TEXT NOT NULL DEFAULT '',
            youtube_url  TEXT NOT NULL,
            kind         TEXT NOT NULL CHECK (kind IN ('user', 'themerrdb')),
            captured_at  TEXT NOT NULL,
            PRIMARY KEY (media_type, tmdb_id, section_id),
            FOREIGN KEY (media_type, tmdb_id) REFERENCES themes(media_type, tmdb_id)
                ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_previous_urls_target
            ON previous_urls (media_type, tmdb_id);
        """
    )
    # v1.24.51: guard the backfill + DROP on the source columns still existing.
    # The whole thing was one executescript (autocommits each statement), so a
    # container kill after a DROP COLUMN committed — version still behind — re-ran
    # the INSERT...SELECT previous_youtube_url against the dropped column → "no
    # such column" crash-loop on every boot. Mirror v52→v53's table_info guard.
    # The backfill is gated on previous_youtube_url (dropped FIRST below), so when
    # it runs both source columns are guaranteed present; INSERT OR IGNORE keeps it
    # safe to re-run. Each DROP is independent so a crash BETWEEN the two still
    # completes (the second column drops on the next boot).
    cols = {r[1] for r in conn.execute("PRAGMA table_info(themes)")}
    if "previous_youtube_url" in cols:
        conn.execute(
            """
            INSERT OR IGNORE INTO previous_urls
                (media_type, tmdb_id, section_id, youtube_url, kind, captured_at)
            SELECT media_type, tmdb_id, '',
                   previous_youtube_url, previous_youtube_kind,
                   COALESCE(youtube_edited_at, last_seen_sync_at)
              FROM themes
             WHERE previous_youtube_url IS NOT NULL
               AND previous_youtube_kind IS NOT NULL
            """
        )
    for _col in ("previous_youtube_url", "previous_youtube_kind"):
        if _col in cols:
            conn.execute(f"ALTER TABLE themes DROP COLUMN {_col}")


def _migrate_v30_to_v31(conn: sqlite3.Connection) -> None:
    """v31 makes pending_updates per-section. Pre-fix the table was
    keyed on (media_type, tmdb_id) — title-global — so a sync that
    detected an upstream URL change wrote one row that lit up the
    blue update pill on EVERY section's instance of the title.
    KEEP CURRENT or ACCEPT UPDATE on standard then silently applied
    to the 4K row too because both rows read the same pending_updates
    record. The topbar UPD count under-reported: each title-with-
    update counted once regardless of how many sections owned it.

    Schema change:
      - Add section_id TEXT NOT NULL DEFAULT '' column.
      - Drop the (media_type, tmdb_id) PK.
      - New PK (media_type, tmdb_id, section_id).
      - SQLite can't ALTER PRIMARY KEY in-place — we recreate the
        table and copy data over (existing rows land at section_id='').
      - The '' row acts as a fallback for legacy callers that don't
        supply section_id; the library SQL reads section's own row
        first, falling back to '' (mirrors user_overrides v27 +
        previous_urls v30 patterns).

    The library SQL, accept/decline endpoints, and the urls_match
    eager-detection paths in api.py / sync.py are updated separately
    to honor the new section_id key. This migration just reshapes
    the storage; reads continue to work via the '' fallback row
    until the call sites are upgraded.
    """
    log.info("Migrating to schema v31 (pending_updates per-section)")
    conn.executescript(
        """
        BEGIN;
        -- v1.22.66: BEGIN/COMMIT added (audit round 2) — executescript
        -- runs statement-by-statement in autocommit without it, so a
        -- container kill between DROP and RENAME stranded the data in
        -- the shadow table and crash-looped every boot (the v1.19.73
        -- incident class). The later rebuilds (v47+) always had this.
        CREATE TABLE pending_updates_v31 (
            media_type        TEXT NOT NULL,
            tmdb_id           INTEGER NOT NULL,
            section_id        TEXT NOT NULL DEFAULT '',
            theme_id          INTEGER,
            old_video_id      TEXT,
            new_video_id      TEXT,
            old_youtube_url   TEXT,
            new_youtube_url   TEXT,
            upstream_edited_at TEXT,
            detected_at       TEXT NOT NULL,
            decision          TEXT,
            decision_at       TEXT,
            decision_by       TEXT,
            replaced_user_url TEXT,
            kind              TEXT NOT NULL DEFAULT 'upstream_changed'
                                CHECK (kind IN ('upstream_changed', 'urls_match')),
            PRIMARY KEY (media_type, tmdb_id, section_id),
            FOREIGN KEY (media_type, tmdb_id) REFERENCES themes (media_type, tmdb_id)
                ON DELETE CASCADE
        );
        INSERT INTO pending_updates_v31
            (media_type, tmdb_id, section_id, theme_id,
             old_video_id, new_video_id, old_youtube_url, new_youtube_url,
             upstream_edited_at, detected_at, decision, decision_at,
             decision_by, replaced_user_url, kind)
        SELECT media_type, tmdb_id, '', theme_id,
               old_video_id, new_video_id, old_youtube_url, new_youtube_url,
               upstream_edited_at, detected_at, decision, decision_at,
               decision_by, replaced_user_url, kind
          FROM pending_updates;
        DROP TABLE pending_updates;
        ALTER TABLE pending_updates_v31 RENAME TO pending_updates;
        COMMIT;
        CREATE INDEX IF NOT EXISTS idx_pending_updates_decision
            ON pending_updates (decision, detected_at);
        CREATE INDEX IF NOT EXISTS idx_pending_updates_target
            ON pending_updates (media_type, tmdb_id);
        """
    )


def _migrate_v31_to_v32(conn: sqlite3.Connection) -> None:
    """v32 adds a hidden URL slot to previous_urls so PURGE/UNMANAGE
    captures don't have to choose between "preserve the older user
    override" and "capture the URL we're about to drop". Now both
    survive: previous_url holds the just-dropped URL (RESTORE target),
    hidden_url holds whatever previous_url was before PURGE (becomes
    the new previous_url after RESTORE so REVERT continues the chain).

    Pre-fix the v1.12.99 user-kind preservation rule kept the older
    user URL but blocked the PURGE-time capture from landing — RESTORE
    then brought back the older user URL, not what was just purged.
    The hidden slot lets us preserve both, in chronological order.

    Schema change is purely additive: three new nullable columns. No
    backfill needed — existing previous_urls rows are still valid as
    single-deep undo, hidden_* defaults to NULL.
    """
    log.info("Migrating to schema v32 (previous_urls hidden slot)")
    _add_column(conn, "previous_urls", "hidden_url", "TEXT")
    _add_column(conn, "previous_urls", "hidden_kind",
                "TEXT CHECK (hidden_kind IN ('user', 'themerrdb') "
                "OR hidden_kind IS NULL)")
    _add_column(conn, "previous_urls", "hidden_captured_at", "TEXT")


def _migrate_v44_to_v45(conn: sqlite3.Connection) -> None:
    """v45 (v1.14.28): add themes.last_probed_at for the URL-probe
    feature.

    Sets the timestamp every time the row's TDB URL is probed via
    yt-dlp --simulate (the new POST /api/items/{mt}/{id}/probe-tdb
    endpoint, plus the bulk probe job in v1.14.29). Drives the
    bulk-probe cooldown — rows probed within 24h are skipped to
    avoid hammering YT/SC unnecessarily.

    Pure column-add. No backfill needed; NULL means "never probed",
    which is the correct initial state for every row.
    """
    log.info("Migrating to schema v45 (themes.last_probed_at)")
    _add_column(conn, "themes", "last_probed_at", "TEXT")


def _migrate_v45_to_v46(conn: sqlite3.Connection) -> None:
    """v46 (v1.14.74): add plex_sections.last_enum_content_changed_at
    for the section-level delta-gate optimization.

    Stores the `contentChangedAt` value Plex reported for the
    section at the start of the most recent successful enum.
    plex_enum compares this against the live value (re-read from
    /library/sections at enum start) and skips the full
    enumerate_section_items call when equal — saving ~30s to
    ~2min per section on stable libraries (the common case for
    daily cron syncs).

    Pure column-add. NULL = "never enumerated" — the gate falls
    through to a full enum on first run after migration, then
    populates the column on success so subsequent runs can skip.
    """
    log.info("Migrating to schema v46 "
             "(plex_sections.last_enum_content_changed_at)")
    _add_column(conn, "plex_sections", "last_enum_content_changed_at", "TEXT")


def _migrate_v46_to_v47(conn: sqlite3.Connection) -> None:
    """v47 (v1.14.93): widen op_progress.kind CHECK to include
    'bulk_probe_tdb'.

    Pre-fix the BULK PROBE TDB worker thread crashed at its very
    first start_progress call with `sqlite3.IntegrityError: CHECK
    constraint failed: kind IN ('tdb_sync', 'plex_enum',
    'reprobe_plex_themes')`. The route returned 200 (the thread
    spawned fine) but the INSERT inside the thread threw, so the
    user saw "// PROBE TDB URLS started" alert then nothing in
    LIVE OPS — silent fail.

    Cascaded into the LET PLEX SERVE bulk action: the v1.14.29
    opt-in pre-flight probe prompt (the user's UX safety: probe TDB
    URLs first so dead URLs don't get LPS'd into oblivion) called
    /api/admin/bulk-probe-tdb, the worker died, the user accepted
    "run probe first" and walked away — no probe, no LPS, no
    error surfaced.

    SQLite can't ALTER CHECK constraints in place, so recreate
    the table. op_progress holds runtime-only state (in-flight
    op tracking; finished rows pruned on read) so a row-copy
    suffices — no concurrent writers risk during migration since
    the worker isn't started until after init_db returns.
    """
    log.info("Migrating to schema v47 "
             "(op_progress.kind CHECK widened for bulk_probe_tdb)")
    conn.executescript("""
        BEGIN;
        CREATE TABLE op_progress_new (
            op_id           TEXT PRIMARY KEY,
            kind            TEXT NOT NULL
                               CHECK (kind IN ('tdb_sync', 'plex_enum',
                                               'reprobe_plex_themes',
                                               'bulk_probe_tdb')),
            status          TEXT NOT NULL DEFAULT 'running'
                               CHECK (status IN ('running', 'cancelling',
                                                 'done', 'failed', 'cancelled')),
            started_at      TEXT NOT NULL,
            updated_at      TEXT NOT NULL,
            finished_at     TEXT,
            stage           TEXT,
            stage_label     TEXT,
            stage_current   INTEGER NOT NULL DEFAULT 0,
            stage_total     INTEGER NOT NULL DEFAULT 0,
            processed_total INTEGER NOT NULL DEFAULT 0,
            processed_est   INTEGER NOT NULL DEFAULT 0,
            error_count     INTEGER NOT NULL DEFAULT 0,
            detail_json     TEXT
        );
        INSERT INTO op_progress_new SELECT * FROM op_progress;
        DROP TABLE op_progress;
        ALTER TABLE op_progress_new RENAME TO op_progress;
        CREATE INDEX IF NOT EXISTS idx_op_progress_status
            ON op_progress (status, updated_at);
        -- v1.15.0: also recreate idx_op_progress_finished — pre-fix
        -- the v46→v47 migration only restored idx_op_progress_status,
        -- so installs that upgraded past v1.14.93 silently lost the
        -- finished-at index. /api/progress polls that filter on
        -- finished_at fell back to table scans, lagging the LIVE OPS
        -- drawer on installs with months of historical ops.
        CREATE INDEX IF NOT EXISTS idx_op_progress_finished
            ON op_progress (finished_at);
        COMMIT;
    """)


def _migrate_v49_to_v50(conn: sqlite3.Connection) -> None:
    # v1.15.62: add idx_plex_items_theme_id. Pre-fix the
    # _FAILURES_SFA_FROM_SQL subqueries joined plex_items via theme_id
    # without an index — every page load did a full scan per subquery
    # (3+ subqueries × ~10K rows) → tab loads took 60s+. Index is
    # CREATE INDEX IF NOT EXISTS so re-running is a no-op.
    log.info("Migrating to schema v50 "
             "(add idx_plex_items_theme_id for FAIL-count perf)")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_plex_items_theme_id "
        "ON plex_items (theme_id)"
    )


def _migrate_v50_to_v51(conn: sqlite3.Connection) -> None:
    # v1.15.81: add themes.tdb_content_fingerprint. Stores a SHA1
    # of the motif-relevant fields from the upstream TDB JSON record
    # so sync's apply loop can skip the full UPDATE when nothing
    # motif reads has actually changed. the user's repro: 9733 git-
    # diff'd paths applied over 2+ minutes resulting in new=0/
    # updated=0 because LizardByte's commits touched fields motif
    # doesn't read. Fingerprint compare → fast-path skip the upsert
    # in that case.
    #
    # Legacy rows get NULL; the next sync writes the fingerprint
    # in the same code path that does the comparison (the absent
    # fingerprint is treated as "different" → full upsert runs +
    # writes the new fingerprint). After one full sync cycle every
    # row has a fingerprint and the fast-path kicks in.
    log.info("Migrating to schema v51 "
             "(add themes.tdb_content_fingerprint for sync apply perf)")
    _add_column(conn, "themes", "tdb_content_fingerprint", "TEXT")


def _migrate_v51_to_v52(conn: sqlite3.Connection) -> None:
    """v52 (v1.16.0): widen op_progress.kind CHECK to include
    'hama_bridge'.

    v1.16.0 added the HAMA-bridge background runner that links
    Plex-themed rows with TVDB-only GUIDs to motif's TMDB-keyed
    themes table via TMDB's external-id /find endpoint. The
    runner calls try_acquire('hama-bridge', 'hama_bridge') —
    without the schema widening, the INSERT 500s with the same
    CHECK-constraint error class as v49 (bulk_lps), v48
    (status=pending), and v47 (bulk_probe_tdb).

    Same table-recreate pattern — SQLite can't ALTER CHECK in
    place. op_progress holds runtime-only state, worker isn't
    started until init_db returns, row-copy safe.
    """
    log.info("Migrating to schema v52 "
             "(op_progress.kind CHECK widened to include 'hama_bridge')")
    conn.executescript("""
        BEGIN;
        CREATE TABLE op_progress_new (
            op_id           TEXT PRIMARY KEY,
            kind            TEXT NOT NULL
                               CHECK (kind IN ('tdb_sync', 'plex_enum',
                                               'reprobe_plex_themes',
                                               'bulk_probe_tdb',
                                               'bulk_lps',
                                               'hama_bridge')),
            status          TEXT NOT NULL DEFAULT 'running'
                               CHECK (status IN ('pending', 'running',
                                                 'cancelling',
                                                 'done', 'failed', 'cancelled')),
            started_at      TEXT NOT NULL,
            updated_at      TEXT NOT NULL,
            finished_at     TEXT,
            stage           TEXT,
            stage_label     TEXT,
            stage_current   INTEGER NOT NULL DEFAULT 0,
            stage_total     INTEGER NOT NULL DEFAULT 0,
            processed_total INTEGER NOT NULL DEFAULT 0,
            processed_est   INTEGER NOT NULL DEFAULT 0,
            error_count     INTEGER NOT NULL DEFAULT 0,
            detail_json     TEXT
        );
        INSERT INTO op_progress_new SELECT * FROM op_progress;
        DROP TABLE op_progress;
        ALTER TABLE op_progress_new RENAME TO op_progress;
        CREATE INDEX IF NOT EXISTS idx_op_progress_status
            ON op_progress (status, updated_at);
        CREATE INDEX IF NOT EXISTS idx_op_progress_finished
            ON op_progress (finished_at);
        COMMIT;
    """)


def _migrate_v53_to_v54(conn: sqlite3.Connection) -> None:
    """v54 (v1.17.17): retroactive kind fix on `previous_urls`.

    Pre-v1.17.17, `_capture_previous_url` (api.py) hardcoded
    `kind = 'themerrdb'` for any themes-row capture, regardless
    of `themes.upstream_source`. For a `plex_orphan` row the
    `themes.youtube_url` field holds the URL captured during
    the original ADOPT — NOT a ThemerrDB URL.

    The mis-labeled kind interacts with the `revert_redundant`
    SQL (api.py:1977-1980) which AND's
    `kind='themerrdb' AND captured_url == themes.youtube_url`
    to suppress RESTORE post-PURGE. Result: orphan rows
    lost their recovery path entirely (the user's screenshot
    showed RE-DOWNLOAD TDB on a NO TDB row as the only
    visible action, but that path was also broken — see the
    parallel `isOrphan` gate fix in app.js for the v1.17.17
    frontend half).

    This migration retroactively flips affected rows so
    the user's existing stranded captures recover RESTORE on
    next boot, not just future PURGEs.

    Idempotent: the UPDATE WHERE clause already constrains to
    the wrong-kind+orphan-source intersection, so re-running
    is a no-op.
    """
    log.info("Migrating to schema v54 "
             "(fix kind='themerrdb' for plex_orphan-sourced "
             "previous_urls captures — v1.17.17 recovery)")
    conn.execute(
        """
        UPDATE previous_urls
           SET kind = 'user'
         WHERE kind = 'themerrdb'
           AND EXISTS (
             SELECT 1 FROM themes t
              WHERE t.media_type = previous_urls.media_type
                AND t.tmdb_id   = previous_urls.tmdb_id
                AND t.upstream_source = 'plex_orphan'
           )
        """
    )


def _migrate_v55_to_v56(conn: sqlite3.Connection) -> None:
    """v56 (v1.18.77): add `user_overrides.intent` to disambiguate
    'replace Plex's theme' from 'backup-only download_only'.

    Pre-v1.18.77 the SET URL dialog's `download_only` checkbox was a
    one-shot worker-payload flag (`auto_place=False`) that left no
    durable record of the user's intent. A row in the "downloaded
    but not placed" state could mean either:

      (a) User intentionally chose download_only as a backup safety
          net while Plex continued serving its own theme.
      (b) User wanted their URL to replace Plex's theme, but the
          placement got silently skipped due to plex_has_theme.

    Both states looked identical post-fact and the UI offered
    "PUSH TO PLEX" as a remediation suggestion for BOTH — wrong
    for case (a).

    v1.18.77 promotes intent to a first-class column on
    user_overrides. New SET URL / UPLOAD MP3 flows persist the
    intent at creation time (from the download_only flag);
    existing rows are backfilled here.

    ## Backfill policy

    Per the user's design choice ("default existing rows to backup"):
      - row has a placements entry for the same (mt, tmdb, section_id)
        → intent='replace' (motif is actively serving the override)
      - else → intent='backup' (safer default for ambiguous rows —
        either intentional backup OR blocked-by-plex-has-theme;
        both default to backup, user can opt-out via the new
        PROMOTE TO ACTIVE button)

    For global overrides (section_id=''), the placement check looks
    at ANY section under the (mt, tmdb) — if motif is serving the
    override in ANY section, treat the global override as 'replace'.

    ## SQLite ALTER TABLE limitations

    SQLite supports `ALTER TABLE ADD COLUMN` with a default value,
    so this migration is simple — no table-rebuild needed. The
    CHECK constraint on the new column is enforced at INSERT/UPDATE
    time (not retroactively on existing rows), which is fine
    because the DEFAULT clause guarantees no row violates the CHECK.
    """
    log.info("Migrating to schema v56 "
             "(add user_overrides.intent for backup-vs-replace)")
    # Add the column with default 'replace' first; backfill overrides
    # the default for rows that should be 'backup' per the policy above.
    _add_column(conn, "user_overrides", "intent",
                "TEXT NOT NULL DEFAULT 'replace' "
                "CHECK (intent IN ('replace', 'backup'))")
    # Backfill: rows WITHOUT a corresponding placement → 'backup'.
    # The match is per-section, except for global overrides
    # (section_id='') which check ANY section under the same
    # (mt, tmdb).
    conn.execute(
        """
        UPDATE user_overrides
           SET intent = 'backup'
         WHERE section_id != ''
           AND NOT EXISTS (
             SELECT 1 FROM placements p
              WHERE p.media_type = user_overrides.media_type
                AND p.tmdb_id   = user_overrides.tmdb_id
                AND p.section_id = user_overrides.section_id
           )
        """
    )
    conn.execute(
        """
        UPDATE user_overrides
           SET intent = 'backup'
         WHERE section_id = ''
           AND NOT EXISTS (
             SELECT 1 FROM placements p
              WHERE p.media_type = user_overrides.media_type
                AND p.tmdb_id   = user_overrides.tmdb_id
           )
        """
    )


def _migrate_v56_to_v57(conn: sqlite3.Connection) -> None:
    """v57 (v1.19.12): add `trg_user_overrides_cleanup_urls_match`
    trigger that auto-cleans orphan `pending_updates kind=
    'urls_match'` rows when their backing user_overrides row is
    deleted.

    the user's 2026-05-24 diagnostic on Cowboys & Aliens (tmdb=49849)
    showed a T+PU row with the !UPD prompt sourced from a
    pending_updates kind='urls_match' row whose user_overrides
    counterpart had been deleted by an earlier action (likely
    REPLACE TDB / ACCEPT UPDATE). urls_match is by definition
    a U→T conversion offer — with no user_override left to
    convert FROM, the prompt is meaningless.

    Pre-fix, ~12 code paths delete user_overrides (REPLACE TDB,
    ACCEPT UPDATE, PURGE, sync normalization, recovery walkers).
    Adding a follow-up DELETE at each site would work but is
    tedious + error-prone (new code paths could forget). The
    trigger handles cleanup once at the source-of-truth schema
    level: any future user_overrides delete automatically cleans
    the orphan.

    The trigger CREATE is also in SCHEMA (the idempotent CREATE
    runs on every init_db), so fresh installs get it without
    needing this migration. This migration handles existing
    installs that pre-date v57 by also one-shot cleaning any
    EXISTING orphans (pending_updates kind='urls_match' rows
    with no matching user_overrides). Defense in depth — the
    read-side gate (api.py _has_user_override_sql in v1.19.12)
    also hides orphans at display time as a safety net.
    """
    log.info("Migrating to schema v57 "
             "(trigger: clean orphan urls_match pending_updates)")
    conn.execute(
        "CREATE TRIGGER IF NOT EXISTS "
        "trg_user_overrides_cleanup_urls_match "
        "AFTER DELETE ON user_overrides "
        "FOR EACH ROW BEGIN "
        "  DELETE FROM pending_updates "
        "   WHERE media_type = OLD.media_type "
        "     AND tmdb_id = OLD.tmdb_id "
        "     AND section_id = OLD.section_id "
        "     AND kind = 'urls_match'; "
        "END"
    )
    # One-shot cleanup for EXISTING orphan urls_match rows on
    # this install. The trigger only fires on future deletes;
    # rows already orphaned by pre-v57 code paths need manual
    # cleanup here. Per the user's 2026-05-24 diagnostic, the
    # orphan count on his install was already 0 (he'd cleared
    # it via UI action) — but other installs might have
    # accumulated orphans over the v1.12.34 → v1.19.12 lifetime.
    cur = conn.execute(
        """
        DELETE FROM pending_updates
         WHERE kind = 'urls_match'
           AND NOT EXISTS (
               SELECT 1 FROM user_overrides uo
                WHERE uo.media_type = pending_updates.media_type
                  AND uo.tmdb_id = pending_updates.tmdb_id
                  AND uo.section_id = pending_updates.section_id
           )
        """
    )
    if cur.rowcount > 0:
        log.info(
            "v57 one-shot cleanup: removed %d orphan "
            "urls_match pending_updates row(s) (no matching "
            "user_overrides exists)",
            cur.rowcount,
        )


def _migrate_v57_to_v58(conn: sqlite3.Connection) -> None:
    """v58 (v1.19.42): widen `local_files.source_kind` CHECK to
    accept `'plex_cloud'` for cloud-themes-backup.

    The new value labels local_files rows written by the v1.19.42
    cloud-backup walker (`app/core/cloud_theme_backup.py`), which
    downloads Plex's INTERNAL `metadata://themes/<sha>` entries
    (served only while Plex Pass is active) and stages them as
    motif-managed backups. Distinct from `'url'` (user-provided
    YouTube/SoundCloud URLs) and `'upload'` (user-uploaded MP3s).

    The plex_cloud row pairs with `last_place_attempt_reason=
    'backup_only'` to reuse the v1.19.21 BK pipe end-to-end
    (BK badge, retry-sweep skip, v1.19.35 PROMOTE TO ACTIVE
    BK-no-override branch). No new column needed — the
    source_kind value IS the cloud-backup marker.

    Mirrors `_migrate_v54_to_v55` shape exactly: wrap the
    `_widen_check_constraint` call in `PRAGMA foreign_keys = OFF`
    per the v1.18.5 CRITICAL lesson (catastrophic FK cascade
    data-loss from v1.18.0's defer_foreign_keys mistake), then
    `PRAGMA foreign_key_check` for insurance, then restore
    `foreign_keys = ON`.

    Pre-flight verified safe on 2026-05-26 against the user's prod
    DB copy: all 5 table counts identical pre/post, foreign_key_
    check empty, positive insert of source_kind='plex_cloud'
    succeeds, negative insert with invalid kind rejected.
    """
    log.info("Migrating to schema v58 "
             "(local_files.source_kind += 'plex_cloud' "
             "for cloud-themes-backup — v1.19.42)")
    # v1.19.78: commit pending implicit txn so `foreign_keys = OFF`
    # is not a no-op on a multi-step upgrade (Opus-4.8 audit HIGH-2).
    conn.commit()
    conn.execute("PRAGMA foreign_keys = OFF")
    try:
        _widen_check_constraint(
            conn,
            "local_files",
            "source_kind",
            ("themerrdb", "url", "upload", "adopt"),
            ("themerrdb", "url", "upload", "adopt", "plex_cloud"),
        )
        violations = conn.execute("PRAGMA foreign_key_check").fetchall()
        if violations:
            raise RuntimeError(
                f"v58 migration: foreign_key_check found "
                f"{len(violations)} violation(s) post-rebuild — "
                f"first 5: {violations[:5]}"
            )
    finally:
        conn.execute("PRAGMA foreign_keys = ON")


def _migrate_v58_to_v59(conn: sqlite3.Connection) -> None:
    """v59 (v1.19.45): widen `op_progress.kind` CHECK to include
    `'cloud_themes_backup'`.

    Pre-fix the v1.19.42 walker ran inline inside an async
    endpoint that contained synchronous Plex API calls + sleeps
    + downloads. The async-def-with-sync-work pattern BLOCKED
    the FastAPI event loop for the full duration of the walk —
    every other request to motif queued. the user's repro: clicked
    BACKUP THIS THEME on a single TV row, browser hit "Failed
    to fetch" + the whole UI locked up. Root cause was an even
    bigger bug: identify_c1_rows ALWAYS walked the full ~3,883
    P-row catalog regardless of rks_scope (filter was applied
    AFTER the walk completed). 13 minutes minimum for one row.

    v1.19.45 converts the cloud-backup endpoint to the same
    acquire-then-spawn-thread pattern bulk_lps / bulk_probe_tdb /
    hama_bridge already use. This migration widens the
    op_progress.kind CHECK so try_acquire('cloud-themes-backup',
    'cloud_themes_backup') succeeds instead of 500-ing on the
    constraint.

    Same shape as _migrate_v51_to_v52 / v55→v56 (hama_bridge add).
    """
    log.info("Migrating to schema v59 "
             "(op_progress.kind CHECK widened to include "
             "'cloud_themes_backup' — v1.19.45)")
    conn.executescript("""
        BEGIN;
        CREATE TABLE op_progress_new (
            op_id           TEXT PRIMARY KEY,
            kind            TEXT NOT NULL
                               CHECK (kind IN ('tdb_sync', 'plex_enum',
                                               'reprobe_plex_themes',
                                               'bulk_probe_tdb',
                                               'bulk_lps',
                                               'hama_bridge',
                                               'cloud_themes_backup')),
            status          TEXT NOT NULL DEFAULT 'running'
                               CHECK (status IN ('pending', 'running',
                                                 'cancelling',
                                                 'done', 'failed', 'cancelled')),
            started_at      TEXT NOT NULL,
            updated_at      TEXT NOT NULL,
            finished_at     TEXT,
            stage           TEXT,
            stage_label     TEXT,
            stage_current   INTEGER NOT NULL DEFAULT 0,
            stage_total     INTEGER NOT NULL DEFAULT 0,
            processed_total INTEGER NOT NULL DEFAULT 0,
            processed_est   INTEGER NOT NULL DEFAULT 0,
            error_count     INTEGER NOT NULL DEFAULT 0,
            detail_json     TEXT
        );
        INSERT INTO op_progress_new SELECT * FROM op_progress;
        DROP TABLE op_progress;
        ALTER TABLE op_progress_new RENAME TO op_progress;
        CREATE INDEX IF NOT EXISTS idx_op_progress_status
            ON op_progress (status, updated_at);
        CREATE INDEX IF NOT EXISTS idx_op_progress_finished
            ON op_progress (finished_at);
        COMMIT;
    """)


def _migrate_v59_to_v60(conn: sqlite3.Connection) -> None:
    """v60 (v1.19.71): widen `pending_updates.kind` CHECK to
    accept `'new_theme_available'` for the v1.19.71 new-theme
    surface.

    Pre-v1.19.71 sync's `is_new` branch wrote pending_updates
    with the default kind='upstream_changed' (or nothing — for
    SRC=— rows the auto-download fired silently). The semantic
    was misleading: 'upstream_changed' implies "TDB rolled an
    existing URL" but is_new is the OPPOSITE — TDB just
    discovered the title.

    v1.19.71 adds a third value 'new_theme_available' and
    widens the apply-loop to write it on three new-theme
    surfaces (in_plex rows only, per v1.19.70 filtering):

      1. Cross-match (motif/Plex has content): kind=
         'new_theme_available' (was 'upstream_changed').
      2. SRC=P case (Plex serves its own theme): write
         pending_updates so the operator can opt-in to motif
         taking over.
      3. SRC=— case (no theme anywhere): EITHER auto-download
         (if new `sync.auto_download_new_themes_for_unthemed_
         rows` setting is enabled) OR write pending_updates
         so the operator can manually decide. Default OFF
         per the user's preference — fewer silent actions.

    Same shape as `_migrate_v57_to_v58` (local_files.source_kind
    widening): wrap `_widen_check_constraint` in foreign_keys
    OFF/check/ON per the v1.18.5 critical lesson.
    """
    log.info("Migrating to schema v60 "
             "(pending_updates.kind += 'new_theme_available' "
             "for v1.19.71 new-theme surfacing)")
    # v1.19.78: commit any pending implicit txn first — PRAGMA
    # foreign_keys is a no-op inside a transaction, and init_db's
    # legacy-isolation connection leaves one open after the prior
    # step's `INSERT INTO schema_version` on multi-step upgrades
    # (Opus-4.8 audit HIGH-2). The helper hard-asserts FK is OFF.
    conn.commit()
    conn.execute("PRAGMA foreign_keys = OFF")
    try:
        _widen_check_constraint(
            conn,
            "pending_updates",
            "kind",
            ("upstream_changed", "urls_match"),
            ("upstream_changed", "urls_match", "new_theme_available"),
        )
        violations = conn.execute("PRAGMA foreign_key_check").fetchall()
        if violations:
            raise RuntimeError(
                f"v60 migration: foreign_key_check found "
                f"{len(violations)} violation(s) post-rebuild — "
                f"first 5: {violations[:5]}"
            )
    finally:
        conn.execute("PRAGMA foreign_keys = ON")


def _migrate_v60_to_v61(conn: sqlite3.Connection) -> None:
    """v61 (v1.21.2): persistent oEmbed cache table so video-title
    lookups survive restarts (the in-memory LRU was cold after every
    redeploy → slow INFO cards). Purely additive new table — the base
    SCHEMA also creates it (IF NOT EXISTS) for fresh installs; this
    migration covers existing v60 installs at boot."""
    conn.execute(
        "CREATE TABLE IF NOT EXISTS oembed_cache ("
        "  url TEXT PRIMARY KEY,"
        "  payload TEXT NOT NULL,"
        "  fetched_at TEXT NOT NULL"
        ")"
    )


def _migrate_v61_to_v62(conn: sqlite3.Connection) -> None:
    """v62 (v1.21.51): per-edition theme isolation — Phase A1 (add column).

    Movies can carry multiple editions (Theatrical/Extended/custom) that
    share one tmdb_id; today motif keys theme state by (media_type,
    tmdb_id, section_id) so all editions collapse onto ONE download /
    override / pending-update / placement-intent (the user's repro: LET PLEX
    SERVE on one edition affected all three + left one stuck). This adds an
    `edition_key` column to the four state tables — the normalized
    {edition-X} folder tag, '' = the standard un-tagged edition.

    Purely additive: '' everywhere reproduces today's behavior EXACTLY.
    v63 (the risky PK rebuild, shipped alone) folds edition_key into the
    PKs; the writer/reader cascade comes after. The base SCHEMA defines the
    column for fresh installs; this covers existing installs at boot.
    Idempotent via PRAGMA table_info."""
    for table in ("local_files", "user_overrides", "pending_updates",
                  "placements"):
        cols = {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}
        if "edition_key" not in cols:
            conn.execute(
                f"ALTER TABLE {table} "
                "ADD COLUMN edition_key TEXT NOT NULL DEFAULT ''"
            )
    # Backfill placements from the hardlink folder's {edition-X} tag, so
    # existing per-edition hardlink rows (which already disambiguate via
    # media_folder) carry a meaningful key. plex_upload rows
    # (media_folder='') stay '' — they collide today; v63 + the writer
    # cascade give them a real key going forward. The other three tables
    # stay '' (no install today holds per-edition state).
    from .normalize import parse_folder_name, normalize_edition
    n = 0
    for rowid, folder in conn.execute(
        "SELECT rowid, media_folder FROM placements "
        "WHERE media_folder != '' AND edition_key = ''"
    ).fetchall():
        ek = normalize_edition(
            parse_folder_name(Path(folder).name).editions_raw)
        if ek:
            conn.execute(
                "UPDATE placements SET edition_key = ? WHERE rowid = ?",
                (ek, rowid))
            n += 1
    if n:
        log.info("v62: backfilled edition_key on %d hardlink "
                 "placement(s) from the folder tag", n)


def _rebuild_pk_add_edition_key(
    conn: sqlite3.Connection, table: str, old_pk: str, new_pk: str,
) -> None:
    """v63 (v1.21.52) helper: fold `edition_key` into `table`'s PRIMARY
    KEY by reading the live CREATE from sqlite_master, string-replacing the
    PK clause, and rebuilding with a column-preserving INSERT. Mirrors
    _widen_check_constraint's swap-dance + hardening (shadow recovery,
    trigger drop, FK-OFF assert before the destructive DROP).

    Caller MUST hold `PRAGMA foreign_keys = OFF` (the v1.18.0 cascade-on-
    DROP lesson). edition_key is already a column (v62); only the PK
    changes, so the column-list INSERT is value-preserving."""
    import re
    real_row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
        (table,)).fetchone()
    shadow_present = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (f"{table}_new",)).fetchone() is not None
    # v1.19.74 recovery: a prior run died after DROP, before RENAME.
    if real_row is None and shadow_present:
        log.warning("v63 recovery: %s missing but %s_new exists — "
                    "renaming shadow back.", table, table)
        _drop_triggers_referencing(conn, table)
        conn.execute(f"ALTER TABLE {table}_new RENAME TO {table}")
        return
    if real_row is None:
        raise RuntimeError(f"v63: table {table!r} not found")
    create_sql = real_row[0]
    # v1.19.74 idempotent no-op: PK already widened.
    if new_pk in create_sql:
        log.info("v63: %s PK already includes edition_key — no-op.", table)
        if shadow_present:
            conn.execute(f"DROP TABLE {table}_new")
        return
    if old_pk not in create_sql:
        raise RuntimeError(
            f"v63: couldn't locate PK clause {old_pk!r} in {table} "
            f"(schema drift?). Live: {create_sql[:300]}")
    new_create = create_sql.replace(old_pk, new_pk, 1)
    new_create = re.sub(
        rf'CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+["`]?{re.escape(table)}["`]?',
        f"CREATE TABLE {table}_new", new_create, count=1)
    cols = [r[1] for r in conn.execute(
        f"PRAGMA table_info({table})").fetchall()]
    col_list = ", ".join(f'"{c}"' for c in cols)
    # v1.19.73: pre-clean a stale shadow from an interrupted run.
    if shadow_present:
        log.warning("v63 recovery: dropping stale '%s_new' shadow.", table)
        conn.execute(f"DROP TABLE {table}_new")
    # v1.19.74: drop triggers referencing the table so SQLite's strict
    # RENAME validation doesn't refuse the swap; SCHEMA recreates them.
    _drop_triggers_referencing(conn, table)
    conn.execute(new_create)
    conn.execute(f"INSERT INTO {table}_new ({col_list}) "
                 f"SELECT {col_list} FROM {table}")
    # v1.19.78: hard-assert FK enforcement is OFF before the DROP (a
    # pending txn silently no-ops the caller's FK-OFF → v1.18.0 cascade).
    if conn.execute("PRAGMA foreign_keys").fetchone()[0] != 0:
        raise RuntimeError(
            f"v63: refusing to DROP {table!r} with foreign_keys ON — a "
            f"pending txn made the caller's FK-OFF a no-op (commit first).")
    conn.execute(f"DROP TABLE {table}")
    conn.execute(f"ALTER TABLE {table}_new RENAME TO {table}")


def _migrate_v62_to_v63(conn: sqlite3.Connection) -> None:
    """v63 (v1.21.52): per-edition theme isolation — Phase A2 (PK widen).

    Fold edition_key into the PKs of local_files / user_overrides /
    pending_updates / placements so each edition holds INDEPENDENT state
    (the goal of the arc). This is the ONE data-loss-risk-class step — a
    PK rebuild (the v1.18.0 catastrophe). Uses the proven foreign_keys=OFF
    recipe (the v26/v30 + v1.19.78-hardened move): rebuild ONLY these 4
    CHILD tables (never the cascade-parent themes/plex_sections), assert
    FK-OFF before each DROP, then foreign_key_check + a zero-row-loss
    assertion. Idempotent + interrupt-recoverable (shadow/trigger handling
    in the helper). '' = today's behavior, so this changes no behavior —
    it only makes per-edition state STORABLE; the writers come in Phase B."""
    log.info("Migrating to schema v63 (edition_key into PKs)")
    pk3 = "PRIMARY KEY (media_type, tmdb_id, section_id)"
    pk3e = "PRIMARY KEY (media_type, tmdb_id, section_id, edition_key)"
    specs = [
        ("local_files", pk3, pk3e),
        ("user_overrides", pk3, pk3e),
        ("pending_updates", pk3, pk3e),
        ("placements",
         "PRIMARY KEY (media_type, tmdb_id, section_id, media_folder)",
         "PRIMARY KEY (media_type, tmdb_id, section_id, media_folder, "
         "edition_key)"),
    ]
    tables = [t for t, _, _ in specs]
    # v1.19.78: commit so foreign_keys=OFF isn't a silent no-op on a
    # multi-step upgrade (the prior schema_version INSERT left a txn open).
    conn.commit()
    before = {t: conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
              for t in tables}
    conn.execute("PRAGMA foreign_keys = OFF")
    try:
        for table, old_pk, new_pk in specs:
            _rebuild_pk_add_edition_key(conn, table, old_pk, new_pk)
        # v1.18.0 data-loss guard: adding a col to a PK can only make keys
        # MORE unique, so every row must survive. Abort loudly if not.
        after = {t: conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                 for t in tables}
        if before != after:
            raise RuntimeError(
                f"v63 ABORT: row count changed during PK rebuild — "
                f"before={before} after={after}")
        violations = conn.execute("PRAGMA foreign_key_check").fetchall()
        if violations:
            raise RuntimeError(
                f"v63: foreign_key_check found {len(violations)} "
                f"violation(s) post-rebuild — first 5: {violations[:5]}")
    finally:
        conn.execute("PRAGMA foreign_keys = ON")


def _migrate_v67_to_v68(conn: sqlite3.Connection) -> None:
    """v68 (v1.24.93): rename the bridge op kind 'hama_bridge' → 'tvdb_bridge'
    (+ op_id 'hama-bridge' → 'tvdb-bridge', + runtime_settings key
    'last_hama_bridge_at' → 'last_tvdb_bridge_at').

    The bridge links TVDB-only plex_items rows (≈ a whole TV library, NOT
    anime-specific) to motif's TMDB-keyed themes — 'hama_bridge' was a misnomer
    (99.7% of stranded rows are TheTVDB-scraper TV, not the HAMA anime agent;
    PROJECT_HISTORY §L). v1.16.2 fixed the USER-FACING label to "TVDB BRIDGE"
    but deliberately kept the internal ids; v1.24.93 finishes the job so no
    'hama' surfaces anywhere operational (op kind, op_id, route, runtime key).

    Rebuild op_progress with the renamed CHECK + CASE-transform the stored
    kind/op_id (mirrors _migrate_v58_to_v59's op_progress rebuild; op_progress
    has no FK relationships so no foreign_keys dance is needed). Then re-key the
    runtime_settings timestamp so the "last rebuild" stat survives. Idempotent:
    a re-run finds no 'hama_bridge'/'hama-bridge'/'last_hama_bridge_at' left, so
    every CASE/WHERE simply matches nothing."""
    log.info("Migrating to schema v68 "
             "(op_progress.kind 'hama_bridge' → 'tvdb_bridge' rename — v1.24.93)")
    conn.executescript("""
        BEGIN;
        CREATE TABLE op_progress_new (
            op_id           TEXT PRIMARY KEY,
            kind            TEXT NOT NULL
                               CHECK (kind IN ('tdb_sync', 'plex_enum',
                                               'reprobe_plex_themes',
                                               'bulk_probe_tdb',
                                               'bulk_lps',
                                               'tvdb_bridge',
                                               'cloud_themes_backup')),
            status          TEXT NOT NULL DEFAULT 'running'
                               CHECK (status IN ('pending', 'running',
                                                 'cancelling',
                                                 'done', 'failed', 'cancelled')),
            started_at      TEXT NOT NULL,
            updated_at      TEXT NOT NULL,
            finished_at     TEXT,
            stage           TEXT,
            stage_label     TEXT,
            stage_current   INTEGER NOT NULL DEFAULT 0,
            stage_total     INTEGER NOT NULL DEFAULT 0,
            processed_total INTEGER NOT NULL DEFAULT 0,
            processed_est   INTEGER NOT NULL DEFAULT 0,
            error_count     INTEGER NOT NULL DEFAULT 0,
            detail_json     TEXT
        );
        INSERT INTO op_progress_new
            (op_id, kind, status, started_at, updated_at, finished_at,
             stage, stage_label, stage_current, stage_total,
             processed_total, processed_est, error_count, detail_json)
        SELECT
            CASE WHEN op_id = 'hama-bridge' THEN 'tvdb-bridge' ELSE op_id END,
            CASE WHEN kind = 'hama_bridge' THEN 'tvdb_bridge' ELSE kind END,
            status, started_at, updated_at, finished_at,
            stage, stage_label, stage_current, stage_total,
            processed_total, processed_est, error_count, detail_json
        FROM op_progress;
        DROP TABLE op_progress;
        ALTER TABLE op_progress_new RENAME TO op_progress;
        CREATE INDEX IF NOT EXISTS idx_op_progress_status
            ON op_progress (status, updated_at);
        CREATE INDEX IF NOT EXISTS idx_op_progress_finished
            ON op_progress (finished_at);
        UPDATE runtime_settings SET key = 'last_tvdb_bridge_at'
            WHERE key = 'last_hama_bridge_at';
        COMMIT;
    """)


def _migrate_v68_to_v69(conn: sqlite3.Connection) -> None:
    """v69 (v0.50.17): add plex_items.plex_edition_title — Plex's editionTitle
    metadata field, captured by plex_enum for DISPLAY in the library ED column
    when the folder carries no {edition-X} tag (the user's Alien (1979) showed a
    metadata "Directors Cut" edition with no folder tag, so the ED column was
    blank). Display-only; NOT folded into edition_key, so folder-based per-edition
    theme scoping is untouched. Idempotent via _add_column; backfills to '' until
    the next enum repopulates each row's editionTitle."""
    log.info("Migrating to schema v69 (plex_items.plex_edition_title — v0.50.17)")
    _add_column(conn, "plex_items", "plex_edition_title", "TEXT NOT NULL DEFAULT ''")


def _migrate_v69_to_v70(conn: sqlite3.Connection) -> None:
    """v70 (v0.51.128): add plex_items.consecutive_missing — the count of
    consecutive full enums this row has been absent from Plex's section listing.
    The v1.18.90 reaper increments it per miss and reaps only at
    >= _REAP_MISS_THRESHOLD (plex_enum.py), so a transient Plex glitch that
    drops a row for a single enum no longer false-DELETEs it + fires a false
    💔 Theme lost. Idempotent via _add_column; existing rows backfill to 0."""
    log.info("Migrating to schema v70 (plex_items.consecutive_missing — v0.51.128)")
    _add_column(conn, "plex_items", "consecutive_missing", "INTEGER NOT NULL DEFAULT 0")


def _migrate_v70_to_v71(conn: sqlite3.Connection) -> None:
    """v71 (v0.51.147): add the `notifications` table — the in-app INBOX store.
    Purely additive new table (no FK, no data touched), idempotent via
    CREATE TABLE IF NOT EXISTS; the end-of-migration SCHEMA run also backfills it.
    Existing installs get an empty inbox that fills from the next dispatch."""
    log.info("Migrating to schema v71 (notifications inbox table — v0.51.147)")
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS notifications (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            ts           TEXT NOT NULL,
            event_kind   TEXT NOT NULL,
            severity     TEXT NOT NULL DEFAULT 'info',
            title        TEXT NOT NULL,
            body         TEXT,
            media_type   TEXT,
            tmdb_id      INTEGER,
            section_id   TEXT,
            batch_id     TEXT,
            seen_at      TEXT,
            dismissed_at TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_notifications_open
            ON notifications (dismissed_at, ts DESC);
        """
    )


def _migrate_v71_to_v72(conn: sqlite3.Connection) -> None:
    """v72 (v0.51.157): add local_files loudness columns — the read-only LOUDNESS
    AUDIT store (Phase 0 of the loudness feature). Five additive columns hold the
    ffmpeg EBU R128 measurement (integrated loudness / true peak / loudness range)
    plus when + which bytes (sha256) it measured, so a stale measurement after a
    re-download is detectable. Purely additive, idempotent via _add_column; existing
    rows read NULL (never measured) until the audit op runs. No file is touched."""
    log.info("Migrating to schema v72 (local_files loudness columns — v0.51.157)")
    _add_column(conn, "local_files", "loudness_i", "REAL")
    _add_column(conn, "local_files", "loudness_tp", "REAL")
    _add_column(conn, "local_files", "loudness_lra", "REAL")
    _add_column(conn, "local_files", "loudness_measured_at", "TEXT")
    _add_column(conn, "local_files", "loudness_measured_sha256", "TEXT")


def _migrate_v72_to_v73(conn: sqlite3.Connection) -> None:
    """v73 (v0.51.168): add local_files loudness-NORMALIZATION state — Phase 1 of the
    loudness feature (the first tag that MUTATES theme bytes, via mp3gain, proven
    bit-exact-reversible by the v0.51.164/165 probe). Five additive columns record a
    per-row normalize so it's visible + fully undoable:

        norm_state         -- 'normalized' when mp3gain gain is applied; NULL = raw
        norm_gain_db       -- the dB actually applied (REAL; for display)
        norm_target        -- the target LUFS used (REAL)
        norm_at            -- when (TEXT ISO)
        norm_orig_sha256   -- the file_sha256 BEFORE normalize, so UNDO can VERIFY it
                              restored the original bytes exactly (the safety assertion)

    Purely additive, idempotent via _add_column; existing rows read NULL (raw, never
    normalized). Edition-scoped writes (keyed on the full local_files PK) come from the
    endpoint, mirroring record_measurement — a normalize never bleeds onto a sibling
    edition. Nothing here touches a file; the migration only widens the row."""
    log.info("Migrating to schema v73 (local_files loudness-normalization state — v0.51.168)")
    _add_column(conn, "local_files", "norm_state", "TEXT")
    _add_column(conn, "local_files", "norm_gain_db", "REAL")
    _add_column(conn, "local_files", "norm_target", "REAL")
    _add_column(conn, "local_files", "norm_at", "TEXT")
    _add_column(conn, "local_files", "norm_orig_sha256", "TEXT")


def _migrate_v73_to_v74(conn: sqlite3.Connection) -> None:
    """v74 (v0.51.170): add local_files.norm_orig_pcm_sha256 — the DECODED-PCM hash of the
    original, captured before gain is applied.

    v73's norm_orig_sha256 is the FILE hash, and undo compared against it. But mp3gain
    leaves its APE tag on the file, so a restored file can NEVER sha-match the pre-normalize
    file — v0.51.165's probe measured precisely that (restored_file_bit_exact=false,
    restored_diff_is_tag_only=true) and the check was built on the file hash regardless, so
    a perfectly correct undo reported "not bit-exact" on the operator's first real audition.
    The audio layer is the one that decides. Additive + idempotent; NULL on rows normalized
    by an older build (undo reports audio_restored=None = unknown, never a false alarm)."""
    log.info("Migrating to schema v74 (local_files.norm_orig_pcm_sha256 — v0.51.170)")
    _add_column(conn, "local_files", "norm_orig_pcm_sha256", "TEXT")


def _migrate_v74_to_v75(conn: sqlite3.Connection) -> None:
    """v75 (v0.51.185): add local_files.norm_plex_entry_uri — the theme entry Plex was
    serving BEFORE motif normalized it.

    Undo has to put Plex back, not just the file. But mp3gain leaves an APE tag, so the
    restored file's sha1 differs from the original's — pushing it would mint a THIRD entry
    (v0.51.176-183: re-upload is the only propagation step, and Plex keys entries by
    content hash). Every normalize/undo cycle would add another. Recording the entry Plex
    served beforehand lets undo re-select THAT one via the v1.18.36 trick — fetch its
    bytes, POST them, content-dedup re-selects the original — so a cycle adds nothing.

    Additive + idempotent; NULL on rows normalized by an older build, where undo falls
    back to pushing the restored file and says so."""
    log.info("Migrating to schema v75 (local_files.norm_plex_entry_uri — v0.51.185)")
    _add_column(conn, "local_files", "norm_plex_entry_uri", "TEXT")


def _migrate_v75_to_v76(conn: sqlite3.Connection) -> None:
    """v76 (v0.51.195): widen op_progress.kind to include 'bulk_normalize' — the Phase-2
    bulk op that mp3gain-levels the eligible library and re-uploads each theme.

    Mirrors _migrate_v58_to_v59's op_progress rebuild (a full-table executescript, NOT
    _widen_check_constraint): op_progress has no FK relationships, so there is no
    foreign_keys dance to do. The CHECK list carries the CURRENT kinds verbatim (v68
    renamed hama_bridge → tvdb_bridge) plus 'bulk_normalize'. Preserves every existing
    row; idempotent on re-run (the CHECK already accepting the value is harmless — a
    second rebuild reproduces the same schema)."""
    log.info("Migrating to schema v76 "
             "(op_progress.kind CHECK widened to include 'bulk_normalize' — v0.51.195)")
    conn.executescript("""
        BEGIN;
        CREATE TABLE op_progress_new (
            op_id           TEXT PRIMARY KEY,
            kind            TEXT NOT NULL
                               CHECK (kind IN ('tdb_sync', 'plex_enum',
                                               'reprobe_plex_themes',
                                               'bulk_probe_tdb',
                                               'bulk_lps',
                                               'tvdb_bridge',
                                               'cloud_themes_backup',
                                               'bulk_normalize')),
            status          TEXT NOT NULL DEFAULT 'running'
                               CHECK (status IN ('pending', 'running',
                                                 'cancelling',
                                                 'done', 'failed', 'cancelled')),
            started_at      TEXT NOT NULL,
            updated_at      TEXT NOT NULL,
            finished_at     TEXT,
            stage           TEXT,
            stage_label     TEXT,
            stage_current   INTEGER NOT NULL DEFAULT 0,
            stage_total     INTEGER NOT NULL DEFAULT 0,
            processed_total INTEGER NOT NULL DEFAULT 0,
            processed_est   INTEGER NOT NULL DEFAULT 0,
            error_count     INTEGER NOT NULL DEFAULT 0,
            detail_json     TEXT
        );
        INSERT INTO op_progress_new SELECT * FROM op_progress;
        DROP TABLE op_progress;
        ALTER TABLE op_progress_new RENAME TO op_progress;
        CREATE INDEX IF NOT EXISTS idx_op_progress_status
            ON op_progress (status, updated_at);
        CREATE INDEX IF NOT EXISTS idx_op_progress_finished
            ON op_progress (finished_at);
        COMMIT;
    """)


def _migrate_v76_to_v77(conn: sqlite3.Connection) -> None:
    """v77 (v0.51.199): widen op_progress.kind to include 'bulk_normalize_undo' — the
    reverse of the Phase-2 bulk op. Same executescript rebuild as _migrate_v75_to_v76
    (op_progress has no FKs, no foreign_keys dance); carries the current kind list +
    'bulk_normalize_undo'. Idempotent on re-run."""
    log.info("Migrating to schema v77 "
             "(op_progress.kind CHECK widened to include 'bulk_normalize_undo' — v0.51.199)")
    conn.executescript("""
        BEGIN;
        CREATE TABLE op_progress_new (
            op_id           TEXT PRIMARY KEY,
            kind            TEXT NOT NULL
                               CHECK (kind IN ('tdb_sync', 'plex_enum',
                                               'reprobe_plex_themes',
                                               'bulk_probe_tdb',
                                               'bulk_lps',
                                               'tvdb_bridge',
                                               'cloud_themes_backup',
                                               'bulk_normalize',
                                               'bulk_normalize_undo')),
            status          TEXT NOT NULL DEFAULT 'running'
                               CHECK (status IN ('pending', 'running',
                                                 'cancelling',
                                                 'done', 'failed', 'cancelled')),
            started_at      TEXT NOT NULL,
            updated_at      TEXT NOT NULL,
            finished_at     TEXT,
            stage           TEXT,
            stage_label     TEXT,
            stage_current   INTEGER NOT NULL DEFAULT 0,
            stage_total     INTEGER NOT NULL DEFAULT 0,
            processed_total INTEGER NOT NULL DEFAULT 0,
            processed_est   INTEGER NOT NULL DEFAULT 0,
            error_count     INTEGER NOT NULL DEFAULT 0,
            detail_json     TEXT
        );
        INSERT INTO op_progress_new SELECT * FROM op_progress;
        DROP TABLE op_progress;
        ALTER TABLE op_progress_new RENAME TO op_progress;
        CREATE INDEX IF NOT EXISTS idx_op_progress_status
            ON op_progress (status, updated_at);
        CREATE INDEX IF NOT EXISTS idx_op_progress_finished
            ON op_progress (finished_at);
        COMMIT;
    """)


def _migrate_v77_to_v78(conn: sqlite3.Connection) -> None:
    """v78 (v0.51.220): add notifications.edition_key so an inbox click-through opens the
    exact cut the notice fired for, not the v0.51.218 pick-a-cut prompt. Nullable + no
    default — NULL means "no single edition" (title-level digests), which correctly
    falls back to the picker; '' is the untagged standard edition. Idempotent ADD COLUMN
    via the shared _add_column guard (crash-between-commit-and-stamp safe)."""
    log.info("Migrating to schema v78 "
             "(notifications.edition_key for edition-exact click-through — v0.51.220)")
    _add_column(conn, "notifications", "edition_key", "TEXT")


def _migrate_v66_to_v67(conn: sqlite3.Connection) -> None:
    """v67 (v1.23.37): persist canonical health on local_files.

    verify_canonical_health stats each row's themes_dir/file_path and stamps
    canonical_present (1 present / 0 missing / NULL unverified) so the
    library's DL sort can rank a BROKEN canonical (theme.mp3 gone from motif's
    canonical storage while the local_files row persists) in SQL. The red DL
    dot is otherwise a per-render filesystem stat the paginated server-side
    sort can't see — the canonical-side mirror of v66's placement health.
    Purely additive (nullable, no default) — existing rows read NULL=unverified
    until the next enum stamps them, behaving exactly as today. Idempotent."""
    cols = {r[1] for r in conn.execute("PRAGMA table_info(local_files)")}
    if "canonical_present" not in cols:
        conn.execute("ALTER TABLE local_files ADD COLUMN canonical_present INTEGER")
    if "canonical_health_checked_at" not in cols:
        conn.execute(
            "ALTER TABLE local_files "
            "ADD COLUMN canonical_health_checked_at TEXT")


def _migrate_v65_to_v66(conn: sqlite3.Connection) -> None:
    """v66 (v1.23.25): persist placement health on placements.

    plex_enum's verify_placement_health pass stats each sidecar placement's
    media_folder/theme.mp3 and stamps theme_present (1 present / 0 missing /
    NULL unverified) so the library's NEEDS WORK + PL sorts can rank a broken
    placement (theme deleted from the Plex folder) in SQL. The red PL dot is
    otherwise a per-render filesystem stat the paginated server-side sort
    can't see. Purely additive (nullable, no default) — existing rows read
    NULL=unverified until the next enum stamps them, behaving exactly as
    today. Idempotent via PRAGMA table_info."""
    cols = {r[1] for r in conn.execute("PRAGMA table_info(placements)")}
    if "theme_present" not in cols:
        conn.execute("ALTER TABLE placements ADD COLUMN theme_present INTEGER")
    if "placement_health_checked_at" not in cols:
        conn.execute(
            "ALTER TABLE placements "
            "ADD COLUMN placement_health_checked_at TEXT")


def _migrate_v64_to_v65(conn: sqlite3.Connection) -> None:
    """v65 (v1.22.87): persist stats.collections_seen on sync_runs.

    The counter existed since v1.18.0 explicitly "so the dashboard
    sparkline can show collection sync activity" but was never written
    — a git-differential run touching only movie_collections/ paths
    recorded movies_seen=0, tv_seen=0 while the done_summary said "N
    items": the run history showed an empty run that did real work.
    Idempotent via PRAGMA table_info."""
    cols = {r[1] for r in conn.execute("PRAGMA table_info(sync_runs)")}
    if "collections_seen" not in cols:
        conn.execute(
            "ALTER TABLE sync_runs "
            "ADD COLUMN collections_seen INTEGER NOT NULL DEFAULT 0")


def _migrate_v63_to_v64(conn: sqlite3.Connection) -> None:
    """v64 (v1.21.56): per-edition theme isolation — Phase B3.

    Adds a folder-derived `edition_key` to plex_items (the Plex view of the
    library). plex_enum keeps it current; this covers existing rows at
    boot. Phase C's library JOIN then matches placements/local_files (both
    edition_key'd since v62/v63) to the right edition row WITHOUT a fragile
    SQL re-implementation of normalize_edition — a stored column is the
    mirror. Purely additive: '' (standard/untagged folder) = today, and
    every install today is standard-edition only, so the backfill is a
    behavior-preserving no-op for all but genuine {edition-X} folders.
    Idempotent via PRAGMA table_info."""
    from .editions import edition_key_for_folder
    cols = {r[1] for r in conn.execute("PRAGMA table_info(plex_items)")}
    if "edition_key" not in cols:
        conn.execute(
            "ALTER TABLE plex_items "
            "ADD COLUMN edition_key TEXT NOT NULL DEFAULT ''")
    n = 0
    for rk, folder in conn.execute(
        "SELECT rating_key, folder_path FROM plex_items "
        "WHERE folder_path != '' AND edition_key = ''"
    ).fetchall():
        ek = edition_key_for_folder(folder)
        if ek:
            conn.execute(
                "UPDATE plex_items SET edition_key = ? WHERE rating_key = ?",
                (ek, rk))
            n += 1
    if n:
        log.info("v64: backfilled edition_key on %d plex_items row(s).", n)


def _migrate_v54_to_v55(conn: sqlite3.Connection) -> None:
    """v55 (v1.18.0): widen CHECK constraints to accept the new
    `media_type='collection'` value (themes / plex_items /
    previous_urls / section_failure_acks) + the new
    `placement_kind='plex_upload'` value (placements).

    v1.18.0 adds Plex Collections support — ThemerrDB's
    `movie_collections/themoviedb/{tmdb_collection_id}.json`
    tree feeds 136 collection theme records (Harry Potter,
    Spider-Man, MCU, etc.), and motif places them onto Plex
    collections via the documented
    `POST /library/metadata/{collectionRatingKey}/themes`
    endpoint (no on-disk sidecar since collections have no
    media folder).

    SQLite can't ALTER CHECK in place. Strategy:
      1. Read the existing `CREATE TABLE` statement from
         `sqlite_master`.
      2. Regex-replace the CHECK clause's allowed values.
      3. Recreate the table from the modified CREATE +
         column-preserving INSERT.

    This is more robust than hardcoding column lists, which
    drift as the schema evolves (we've seen 50+ table edits
    since the relevant tables were created — duplicating those
    columns in the migration would be error-prone).

    `PRAGMA defer_foreign_keys = ON` for the duration of the
    transaction so the FK chain (placements → themes,
    plex_items.theme_id → themes,
    previous_urls → themes) doesn't trip mid-rebuild.

    plex_items.media_type uses Plex's own string values
    (movie/show/collection) — distinct from the
    media_type='movie'/'tv'/'collection' used everywhere else
    in motif (Plex says 'show' for what motif calls 'tv').
    The mapping lives in app/core/plex.py.
    """
    log.info("Migrating to schema v55 "
             "(media_type='collection' + placement_kind='plex_upload' "
             "for Plex Collections support — v1.18.0)")

    # Each entry: (table, column, old_values, new_values).
    widenings = [
        ("themes",                "media_type",     ("movie", "tv"),
                                                    ("movie", "tv", "collection")),
        ("plex_items",            "media_type",     ("movie", "show"),
                                                    ("movie", "show", "collection")),
        ("placements",            "placement_kind",
                                  ("hardlink", "copy", "symlink"),
                                  ("hardlink", "copy", "symlink", "plex_upload")),
        ("previous_urls",         "media_type",     ("movie", "tv"),
                                                    ("movie", "tv", "collection")),
        ("section_failure_acks",  "media_type",     ("movie", "tv"),
                                                    ("movie", "tv", "collection")),
    ]

    # v1.18.5: SQLite's table-rebuild dance requires
    # `foreign_keys = OFF` (NOT `defer_foreign_keys = ON`). The
    # v1.18.0 original used defer_foreign_keys; that pragma only
    # defers FK violation CHECKS to COMMIT time — it does NOT
    # defer cascading actions. When `DROP TABLE themes` ran, the
    # ON DELETE CASCADE on local_files / placements FIRED
    # IMMEDIATELY, wiping both tables for every row, and the
    # ON DELETE SET NULL on plex_items.theme_id nulled the
    # linkage column. Switching to foreign_keys=OFF is the
    # SQLite-recommended pattern (sqlite.org/lang_altertable.html
    # § "Making Other Kinds Of Table Schema Changes"). The
    # closing foreign_keys=ON restores enforcement; a
    # foreign_key_check runs in between to surface any violation
    # the rebuild would have produced (none expected since the
    # CHECK constraint widening is strictly value-additive).
    # v1.19.78: commit pending implicit txn so `foreign_keys = OFF`
    # is not a no-op on a multi-step upgrade (Opus-4.8 audit HIGH-2).
    # This step widens `themes` — the exact cascade-PARENT that caused
    # the v1.18.0 catastrophe — so the FK toggle MUST take effect here.
    conn.commit()
    conn.execute("PRAGMA foreign_keys = OFF")
    try:
        for table, column, old_vals, new_vals in widenings:
            _widen_check_constraint(conn, table, column, old_vals, new_vals)
        # Verify no orphaned references were created by the
        # rebuild. The CHECK widening can't introduce FK
        # violations (we only ADD legal values), but the
        # foreign_key_check is cheap insurance + surfaces any
        # pre-existing corruption that was masked by FKs being
        # off during the rebuild.
        violations = conn.execute("PRAGMA foreign_key_check").fetchall()
        if violations:
            raise RuntimeError(
                f"v55 migration: foreign_key_check found "
                f"{len(violations)} violation(s) post-rebuild — "
                f"first 5: {violations[:5]}"
            )
    finally:
        conn.execute("PRAGMA foreign_keys = ON")


def _widen_check_constraint(
    conn: sqlite3.Connection,
    table: str,
    column: str,
    old_values: tuple[str, ...],
    new_values: tuple[str, ...],
) -> None:
    """v1.18.0 (v55) helper: widen a column's CHECK constraint by
    reading the existing CREATE TABLE statement from
    sqlite_master, regex-replacing the allowed-values tuple,
    and rebuilding the table with column-preserving INSERT.

    Robustness: reads the actual current schema rather than
    duplicating column lists. Survives schema drift between
    table-creation and this migration (50+ tags of evolution
    since some of these tables were created).

    The pattern matches the existing CHECK clause flexibly
    (allows whitespace, comments between values, quote styles).
    Indexes are NOT recreated here — they live in the SCHEMA
    string and `init_db` re-runs that AFTER all migrations, so
    indexes auto-rebuild on the next executescript(SCHEMA).

    v1.18.5 (CRITICAL): caller MUST set `PRAGMA foreign_keys =
    OFF` before invoking this helper. The original v1.18.0
    implementation used `PRAGMA defer_foreign_keys = ON` under
    the assumption it would prevent FK cascades during the DROP
    TABLE step. That was wrong: `defer_foreign_keys` only defers
    VIOLATION CHECKS to COMMIT time — it does NOT defer
    cascading actions (ON DELETE CASCADE / SET NULL fire
    immediately on DROP TABLE in SQLite when foreign_keys is
    ON). The original bug nuked `local_files` and `placements`
    (CASCADE) and nulled `plex_items.theme_id` (SET NULL) for
    every row across every install that ran the v55 migration.
    Recovery from disk lives in `_maybe_recover_post_v55_data_
    loss` below; the prevention is foreign_keys=OFF here."""
    import re

    # v1.19.74: detect prior-failed-migration stuck states + recover
    # before attempting the rebuild. Two states recoverable here:
    #
    # (a) real missing + shadow present — the user's 2026-05-28 v1.19.73
    #     boot: the prior migration completed CREATE+INSERT+DROP but
    #     SQLite's strict ALTER TABLE validation refused the RENAME
    #     because a trigger referencing the table had a dangling
    #     reference (table was just dropped). The shadow has the
    #     widened CHECK + all the data from the INSERT-FROM step;
    #     recovery is: drop referencing triggers, RENAME shadow
    #     back to real, return as a successful widening.
    #
    # (b) real present + CHECK already lists the new values — a
    #     prior manual or partial recovery already widened the
    #     table. Idempotent no-op: drop any leftover shadow,
    #     return.
    real_row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    shadow_row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
        (f"{table}_new",),
    ).fetchone()

    if real_row is None and shadow_row is not None:
        log.warning(
            "v1.19.74 recovery: real table %r is MISSING but shadow "
            "'%s_new' exists. Prior migration failed at the "
            "ALTER TABLE RENAME step (likely SQLite's strict trigger "
            "validation). Dropping any triggers referencing %s + "
            "renaming shadow back to recover.",
            table, table, table,
        )
        _drop_triggers_referencing(conn, table)
        conn.execute(f"ALTER TABLE {table}_new RENAME TO {table}")
        return

    if real_row is None or not real_row[0]:
        raise RuntimeError(
            f"v55 migration: table {table!r} not found in sqlite_master"
        )
    create_sql = real_row[0]

    # v1.19.74 idempotent no-op: if the existing CHECK already lists
    # the new values, treat as already-widened. Pre-fix this would
    # fall through to the regex pattern (looking for OLD values),
    # n=0, raise "couldn't locate CHECK clause" → confusing diagnostic
    # for what's really "already done."
    new_values_check_re = r"\s*,\s*".join(
        rf"['\"]" + re.escape(v) + rf"['\"]" for v in new_values
    )
    already_widened_re = re.compile(
        rf"CHECK\s*\(\s*{re.escape(column)}\s+IN\s*\(\s*"
        rf"{new_values_check_re}\s*\)",
        re.IGNORECASE,
    )
    if already_widened_re.search(create_sql):
        log.warning(
            "v1.19.74 recovery: CHECK on %s.%s already lists %s — "
            "treating as idempotent no-op (likely a prior partial "
            "recovery already widened). %s",
            table, column, new_values,
            ("Dropping leftover shadow." if shadow_row else ""),
        )
        if shadow_row is not None:
            conn.execute(f"DROP TABLE {table}_new")
        return

    # Build a regex that matches `CHECK (column IN ('v1', 'v2', ...))`
    # tolerating whitespace + alternative quoting. We accept either
    # single or double quotes around values, and arbitrary spacing
    # between commas.
    #
    # v1.19.42: capture any trailing content within the CHECK (e.g.
    # ` OR source_kind IS NULL`) so it survives the substitution.
    # Pre-fix this regex required the inner `)` to be immediately
    # followed by the outer `)`, which broke local_files.source_kind
    # (its CHECK has `... IN (...) OR source_kind IS NULL)` and
    # couldn't be located by the original pattern). The `[^)]*`
    # capture is conservative — it only matches content that does
    # NOT contain another `(`, which covers OR/AND-style clauses
    # but stops short of nested parens.
    values_re = r"\s*,\s*".join(
        rf"['\"]" + re.escape(v) + rf"['\"]" for v in old_values
    )
    pattern = re.compile(
        rf"CHECK\s*\(\s*{re.escape(column)}\s+IN\s*\(\s*{values_re}\s*\)([^)]*)\)",
        re.IGNORECASE,
    )

    def _replace(m: "re.Match[str]") -> str:
        tail = m.group(1)  # e.g. "" or " OR source_kind IS NULL"
        return (
            f"CHECK ({column} IN ("
            + ", ".join(f"'{v}'" for v in new_values)
            + f"){tail})"
        )

    new_create_sql, n = pattern.subn(_replace, create_sql, count=1)
    if n == 0:
        raise RuntimeError(
            f"v55 migration: couldn't locate CHECK clause for "
            f"{table}.{column} (old values {old_values}). Schema "
            f"may have drifted since v55 was authored."
        )

    # Replace `CREATE TABLE [IF NOT EXISTS] <table>` with
    # `CREATE TABLE <table>_new` so we can do the swap dance.
    new_create_sql = re.sub(
        rf"CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+[\"`]?{re.escape(table)}[\"`]?",
        f"CREATE TABLE {table}_new",
        new_create_sql,
        count=1,
    )

    # Column list for the positional INSERT (avoids depending
    # on column-order between old and new — SQLite's PRAGMA
    # returns columns in definition order, which the recreated
    # table inherits).
    cols = [r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]
    col_list = ", ".join(f'"{c}"' for c in cols)

    # v1.19.73: pre-clean any leftover `{table}_new` shadow table
    # from a prior interrupted migration attempt. The shadow table
    # is by definition disposable (mid-recipe scratch); pre-dropping
    # is safe and lets re-runs succeed. the user's 2026-05-28 first-
    # failure repro: container restarted mid-migration → shadow
    # persisted → next boot crashed at "table _new already exists".
    # (Sub-pattern of the v1.18.7 cold-path lesson: a recovery code
    # path that early-aborts needs a self-healing entry.)
    if shadow_row is not None:
        log.warning(
            "v1.19.73 recovery: dropping stale '%s_new' shadow table "
            "left behind by a prior interrupted migration attempt. "
            "Re-running the widening recipe cleanly.",
            table,
        )
        conn.execute(f"DROP TABLE {table}_new")

    # v1.19.74: proactively drop triggers referencing the table
    # body so SQLite's strict ALTER TABLE RENAME validation
    # (3.25+) doesn't refuse the RENAME because the trigger's
    # referenced table is missing between DROP and RENAME.
    # Triggers are recreated by init_db's executescript(SCHEMA)
    # at the end of migrations via CREATE TRIGGER IF NOT EXISTS.
    # the user's 2026-05-28 second-failure repro:
    # `trg_user_overrides_cleanup_urls_match` (defined on
    # user_overrides, references pending_updates in body) blocked
    # the RENAME step.
    _drop_triggers_referencing(conn, table)

    conn.execute(new_create_sql)
    conn.execute(
        f"INSERT INTO {table}_new ({col_list}) "
        f"SELECT {col_list} FROM {table}"
    )
    # v1.19.78 (Opus-4.8 audit HIGH-2): hard-assert FK enforcement is
    # actually OFF before the destructive DROP. `PRAGMA foreign_keys`
    # is a SILENT no-op inside an open transaction (SQLite rule), and
    # init_db's connection runs in legacy isolation mode where the
    # prior step's `INSERT INTO schema_version` leaves an implicit txn
    # open on every MULTI-STEP upgrade — so the caller's `PRAGMA
    # foreign_keys = OFF` can silently fail to take effect, re-opening
    # the v1.18.0 cascade-on-DROP data-loss path for cascade-PARENT
    # tables (themes / scan_runs / plex_sections). Convert that silent
    # loss into a loud abort that the recovery branches can re-run.
    if conn.execute("PRAGMA foreign_keys").fetchone()[0] != 0:
        raise RuntimeError(
            f"v55 migration: refusing to DROP {table!r} while foreign_key "
            f"enforcement is still ON — a pending transaction made the "
            f"caller's `PRAGMA foreign_keys = OFF` a no-op. Commit before "
            f"toggling FK (the widen migrations call conn.commit() first)."
        )
    conn.execute(f"DROP TABLE {table}")
    conn.execute(f"ALTER TABLE {table}_new RENAME TO {table}")


def _drop_triggers_referencing(conn: sqlite3.Connection, table: str) -> None:
    """v1.19.74: drop any triggers whose body references `table`.

    SQLite 3.25+ tightened ALTER TABLE RENAME validation: it walks
    the schema looking for views/triggers that reference the table
    being renamed, and refuses if any reference is dangling. During
    the swap-dance (DROP original → RENAME shadow → original), the
    original is briefly missing — so any trigger referencing it
    (e.g. defined on a different table but with the original in its
    body) breaks the RENAME.

    The clean recipe: drop those triggers before the swap-dance,
    rely on init_db's `executescript(SCHEMA)` post-migration to
    recreate them via `CREATE TRIGGER IF NOT EXISTS`. As long as
    the trigger is still defined in SCHEMA, it auto-rebuilds on
    the next init_db pass.

    LIKE-search by table name is conservative — if a trigger
    references a different table whose name contains this one
    (e.g. `pending_updates_history` when widening
    `pending_updates`), it'd false-drop. Schema audit: no such
    namespace collisions currently exist. The trigger would
    auto-rebuild from SCHEMA anyway."""
    rows = conn.execute(
        "SELECT name FROM sqlite_master "
        "WHERE type='trigger' AND sql IS NOT NULL "
        "AND sql LIKE ? "
        "AND name NOT LIKE 'sqlite_%'",
        (f"%{table}%",),
    ).fetchall()
    for (name,) in rows:
        log.info(
            "v1.19.74: dropping trigger %r (references %s in body) "
            "before table rebuild; will be recreated by "
            "executescript(SCHEMA) at end of init_db.",
            name, table,
        )
        conn.execute(f"DROP TRIGGER {name}")


def _migrate_v52_to_v53(conn: sqlite3.Connection) -> None:
    """v53 (v1.17.9): drop plex_items.motif_unplaced_at.

    The column was added at v34 (v1.12.110) as a tombstone for
    phantom-P recovery — "motif just removed its theme from this
    section, suppress Plex's stale cache claim in SRC SQL." It was
    deprecated v1.12.111 once `plex_theme_verified_ok` (HEAD verify
    against /library/metadata/{rk}/theme) replaced it with a more
    accurate signal. No reads, no writes since.

    The v34 migration docstring explicitly anticipated this drop:
      "Future migrations may drop it."

    v1.17.9 hygiene audit (DB sweep finding #3) confirmed zero
    references outside db.py and zero callers in the codebase.
    Safe to drop now — SQLite 3.35+ supports ALTER TABLE DROP
    COLUMN directly (motif's Dockerfile uses python:3.12-slim →
    SQLite 3.45+).

    Idempotent guard: check sqlite_master before the DROP so
    re-running the migration on an already-dropped table is a
    no-op (defends against partial-replay scenarios)."""
    log.info("Migrating to schema v53 "
             "(drop plex_items.motif_unplaced_at — dead since v1.12.111)")
    cols = {
        r[1] for r in conn.execute("PRAGMA table_info(plex_items)").fetchall()
    }
    if "motif_unplaced_at" in cols:
        conn.execute("ALTER TABLE plex_items DROP COLUMN motif_unplaced_at")


def _migrate_v48_to_v49(conn: sqlite3.Connection) -> None:
    """v49 (v1.15.56): widen op_progress.kind CHECK to include
    'bulk_lps'.

    Pre-fix every // ADOPT + LET PLEX SERVE / // LET PLEX SERVE
    bulk click 500'd with `sqlite3.IntegrityError: CHECK constraint
    failed: kind IN ('tdb_sync', 'plex_enum', 'reprobe_plex_themes',
    'bulk_probe_tdb')`. v1.15.28 added the bulk LPS server-side
    composite (route at api.py:13905, calls try_acquire with
    kind='bulk_lps') but missed the schema migration — same bug
    class as v1.15.48's status CHECK widening.

    Same table-recreate pattern as v46→v47 + v47→v48 (SQLite can't
    ALTER CHECK in place). op_progress holds runtime-only state +
    worker isn't started until after init_db returns → row-copy
    safe.

    Lesson: every new try_acquire kind value (and every other code
    constant that lives in a CHECK constraint) needs a behavioral
    test that exercises the INSERT against an init_db()-built
    schema. v1.15.48 added one for try_acquire's status; this tag
    adds the equivalent for its kind.
    """
    log.info("Migrating to schema v49 "
             "(op_progress.kind CHECK widened to include 'bulk_lps')")
    conn.executescript("""
        BEGIN;
        CREATE TABLE op_progress_new (
            op_id           TEXT PRIMARY KEY,
            kind            TEXT NOT NULL
                               CHECK (kind IN ('tdb_sync', 'plex_enum',
                                               'reprobe_plex_themes',
                                               'bulk_probe_tdb',
                                               'bulk_lps')),
            status          TEXT NOT NULL DEFAULT 'running'
                               CHECK (status IN ('pending', 'running',
                                                 'cancelling',
                                                 'done', 'failed', 'cancelled')),
            started_at      TEXT NOT NULL,
            updated_at      TEXT NOT NULL,
            finished_at     TEXT,
            stage           TEXT,
            stage_label     TEXT,
            stage_current   INTEGER NOT NULL DEFAULT 0,
            stage_total     INTEGER NOT NULL DEFAULT 0,
            processed_total INTEGER NOT NULL DEFAULT 0,
            processed_est   INTEGER NOT NULL DEFAULT 0,
            error_count     INTEGER NOT NULL DEFAULT 0,
            detail_json     TEXT
        );
        INSERT INTO op_progress_new SELECT * FROM op_progress;
        DROP TABLE op_progress;
        ALTER TABLE op_progress_new RENAME TO op_progress;
        CREATE INDEX IF NOT EXISTS idx_op_progress_status
            ON op_progress (status, updated_at);
        CREATE INDEX IF NOT EXISTS idx_op_progress_finished
            ON op_progress (finished_at);
        COMMIT;
    """)


def _migrate_v47_to_v48(conn: sqlite3.Connection) -> None:
    """v48 (v1.15.48): widen op_progress.status CHECK to include
    'pending'.

    Pre-fix any call to v1.15.37's `try_acquire` helper 500'd with
    `sqlite3.IntegrityError: CHECK constraint failed: status IN
    ('running', 'cancelling', 'done', 'failed', 'cancelled')` —
    try_acquire INSERTs with status='pending' (the route handler
    claims the op_progress slot atomically; the spawned worker
    thread's start_progress transitions pending → running). The
    helper shipped in v1.15.37 but the corresponding schema
    migration didn't, so every // REPROBE FAILURES /
    // PROBE TDB URLS / // REPROBE PLEX THEMES /
    // BULK LET PLEX SERVE click 500'd. the user's docker logs:
    "sqlite3.IntegrityError: CHECK constraint failed: status IN
    ('running', 'cancelling', 'done', 'failed', 'cancelled')" on
    a v1.15.46 /api/admin/reprobe-tdb-failures POST.

    SQLite can't ALTER CHECK constraints in place; same table-
    recreate pattern as v46→v47. op_progress holds runtime-only
    state — finished rows are pruned on read + the worker isn't
    started until after init_db returns, so the row-copy is safe.
    """
    log.info("Migrating to schema v48 "
             "(op_progress.status CHECK widened to include 'pending')")
    conn.executescript("""
        BEGIN;
        CREATE TABLE op_progress_new (
            op_id           TEXT PRIMARY KEY,
            kind            TEXT NOT NULL
                               CHECK (kind IN ('tdb_sync', 'plex_enum',
                                               'reprobe_plex_themes',
                                               'bulk_probe_tdb')),
            status          TEXT NOT NULL DEFAULT 'running'
                               CHECK (status IN ('pending', 'running',
                                                 'cancelling',
                                                 'done', 'failed', 'cancelled')),
            started_at      TEXT NOT NULL,
            updated_at      TEXT NOT NULL,
            finished_at     TEXT,
            stage           TEXT,
            stage_label     TEXT,
            stage_current   INTEGER NOT NULL DEFAULT 0,
            stage_total     INTEGER NOT NULL DEFAULT 0,
            processed_total INTEGER NOT NULL DEFAULT 0,
            processed_est   INTEGER NOT NULL DEFAULT 0,
            error_count     INTEGER NOT NULL DEFAULT 0,
            detail_json     TEXT
        );
        INSERT INTO op_progress_new SELECT * FROM op_progress;
        DROP TABLE op_progress;
        ALTER TABLE op_progress_new RENAME TO op_progress;
        CREATE INDEX IF NOT EXISTS idx_op_progress_status
            ON op_progress (status, updated_at);
        CREATE INDEX IF NOT EXISTS idx_op_progress_finished
            ON op_progress (finished_at);
        COMMIT;
    """)


def _migrate_v43_to_v44(conn: sqlite3.Connection) -> None:
    """v44 (v1.14.8): backfill section_failure_acks for sections where
    a user-override download has already resolved the per-section
    ATTN axis but the parent themes row's failure_kind is still un-
    acked title-globally.

    Background: pre-v1.14.8 the worker's _record_local_file success
    path didn't write sfa rows on user-override-success (source_kind
    in 'url' / 'upload'). Per the v1.10.50 design intent the title-
    global failure_kind correctly survived (so the red TDB ✗ pill
    stays painted on a dead TDB URL), but the per-section ATTN
    surface — ⚠ glyph in the row, // N FAIL count in the topbar,
    `attn_pills=fail` library filter — kept treating the section as
    failing forever, even though the user had already routed around
    the failure with a working override.

    the user's repro: SET URL → user URL downloads + places → INFO
    dialog reads "✓ RESOLVED — TDB UNAVAILABLE" but the row pill
    still glyphs red ⚠ and the title still counts toward // N FAIL.

    === Backfill criteria ===

    For each (media_type, tmdb_id, section_id) where:
      - local_files row exists with provenance='manual' AND
        source_kind in ('url', 'upload') (= a healthy user-override
        canonical exists for this section), AND
      - themes.failure_kind IS NOT NULL AND themes.failure_acked_at
        IS NULL (= the title still has an un-acked failure title-
        globally), AND
      - no sfa row already exists for this triple

    Insert sfa with acked_by='auto:user_override:backfill' so the
    audit log distinguishes pre-v1.14.8 backfill from v1.14.8+
    write-time acks.

    === Idempotent + safety ===

    Pure INSERT … WHERE NOT EXISTS … so re-running on already-acked
    triples is a no-op. Doesn't touch failure_kind / failure_acked_at
    / failure_message — the title-global failure record is preserved
    so the TDB ✗ pill keeps painting red. Worst case is an over-ack
    (a section that had no user override gets sfa stamped) — guarded
    by the local_files+source_kind requirement, which is the same
    presence-check the SRC=U letter uses.
    """
    log.info("Migrating to schema v44 (backfill sfa for U-resolved sections)")
    cur = conn.execute(
        """
        INSERT INTO section_failure_acks
            (media_type, tmdb_id, section_id, acked_at, acked_by)
        SELECT lf.media_type, lf.tmdb_id, lf.section_id,
               -- v1.14.56: ISO-8601 with tz offset to match the
               -- runtime now_iso() format ('YYYY-MM-DDTHH:MM:SS+00:00').
               -- Pre-fix the migration wrote `datetime('now')` which
               -- emits 'YYYY-MM-DD HH:MM:SS' (no T separator, no tz),
               -- mixing two timestamp formats in section_failure_acks.
               -- Lex-sortable either way but breaks any future date-
               -- range filter or display logic that assumes one shape.
               strftime('%Y-%m-%dT%H:%M:%S+00:00', 'now'), 'auto:user_override:backfill'
        FROM local_files lf
        JOIN themes t
          ON t.media_type = lf.media_type
         AND t.tmdb_id    = lf.tmdb_id
        WHERE lf.provenance = 'manual'
          AND lf.source_kind IN ('url', 'upload')
          AND t.failure_kind IS NOT NULL
          AND t.failure_acked_at IS NULL
          AND NOT EXISTS (
              SELECT 1 FROM section_failure_acks sfa
              WHERE sfa.media_type = lf.media_type
                AND sfa.tmdb_id    = lf.tmdb_id
                AND sfa.section_id = lf.section_id
          )
        """,
    )
    n = cur.rowcount or 0
    if n > 0:
        log.info("v44 backfill: stamped %d section_failure_acks rows", n)
    else:
        log.info("v44 backfill: no sections needed sfa stamping")


def _migrate_v42_to_v43(conn: sqlite3.Connection) -> None:
    """v43 (v1.13.75): backfill themes.failure_kind that pre-v1.13.74
    code wiped from rows whose TDB URL is still demonstrably broken.

    Background: prior to v1.13.74 three code paths blindly cleared
    failure_kind on every successful action — worker._record_local_file
    on every successful download (regardless of source_kind), the
    REVERT API handler unconditionally, and the SET URL API handler
    unconditionally. Net effect: any U row whose user URL had ever
    successfully downloaded after a failed REPLACE-W-TDB attempt got
    its TDB-failure flag wiped, flipping the red `TDB ✗` pill back
    to green even though the TDB URL was still dead. v1.13.74 fixed
    the wipes going forward; this migration repairs the backlog.

    === Reconstruction algorithm ===

    For each themes row currently with failure_kind=NULL, look at the
    events log:

      1. Find the most recent download-failure event for this row
         where the failed URL == the row's CURRENT themes.youtube_url.
         The URL-match guard is critical: if TDB has updated the URL
         since the failure, the old failure no longer applies.

      2. Suppress restoration if any later success event landed for
         the SAME video_id (i.e. a TDB-URL re-attempt actually worked
         in the meantime). User-URL successes have a different
         video_id and don't suppress — they're the exact case that
         caused the wipe.

      3. When (1) fires and (2) doesn't suppress, restore failure_kind,
         failure_message, and failure_at from the failure event's
         detail JSON.

    failure_acked_at is intentionally NOT restored. We don't have
    high-confidence evidence the user previously acked (would require
    audit-event reconstruction of ACK FAILURE clicks). Backfilled
    failures appear as unacked, surfacing in the FAIL counter so the
    user can re-ack at their leisure. The dashboard will show a
    one-time `// BACKFILLED N FAILURES` banner explaining the jump
    (driven by the runtime_settings marker this migration writes).

    === Safety: under-restoration over over-restoration ===

    Every guard fails closed:
      - themes.youtube_url IS NULL → skip (no URL to attribute to)
      - themes.youtube_video_id IS NULL → skip (can't match successes)
      - failure event's detail.kind missing/null → skip
      - failure event's detail.youtube_url != themes.youtube_url → skip
      - any later success matching the URL's video_id → skip

    Worst case is a row that SHOULD be red stays green — same state
    as today, never worse. Idempotent: re-running on rows that
    already have failure_kind set is a no-op (UPDATE filtered by
    `WHERE failure_kind IS NULL`).

    === Why not a manual script ===

    Same reasoning as v42: startup migration runs before workers
    boot, so no in-flight download race. Existing instances self-
    heal on the next motif restart after upgrading to v1.13.75.
    """
    log.info("Migrating to schema v43 (backfill failure_kind from events log)")
    # Restore failure_kind / failure_message / failure_at in one
    # multi-column UPDATE. The correlated subqueries return the same
    # event row (matched by ts ordering), so the three columns stay
    # internally consistent.
    cur = conn.execute(
        """
        UPDATE themes
        SET failure_kind = (
                SELECT json_extract(e.detail, '$.kind')
                FROM events e
                WHERE e.media_type = themes.media_type
                  AND e.tmdb_id   = themes.tmdb_id
                  AND e.level     = 'WARNING'
                  AND e.component = 'download'
                  AND json_extract(e.detail, '$.youtube_url')
                      = themes.youtube_url
                  AND json_extract(e.detail, '$.kind') IS NOT NULL
                ORDER BY e.ts DESC
                LIMIT 1
            ),
            failure_message = (
                SELECT json_extract(e.detail, '$.raw')
                FROM events e
                WHERE e.media_type = themes.media_type
                  AND e.tmdb_id   = themes.tmdb_id
                  AND e.level     = 'WARNING'
                  AND e.component = 'download'
                  AND json_extract(e.detail, '$.youtube_url')
                      = themes.youtube_url
                  AND json_extract(e.detail, '$.kind') IS NOT NULL
                ORDER BY e.ts DESC
                LIMIT 1
            ),
            failure_at = (
                SELECT e.ts
                FROM events e
                WHERE e.media_type = themes.media_type
                  AND e.tmdb_id   = themes.tmdb_id
                  AND e.level     = 'WARNING'
                  AND e.component = 'download'
                  AND json_extract(e.detail, '$.youtube_url')
                      = themes.youtube_url
                  AND json_extract(e.detail, '$.kind') IS NOT NULL
                ORDER BY e.ts DESC
                LIMIT 1
            )
        WHERE failure_kind IS NULL
          AND themes.youtube_url IS NOT NULL
          AND themes.youtube_video_id IS NOT NULL
          AND EXISTS (
              SELECT 1 FROM events e
              WHERE e.media_type = themes.media_type
                AND e.tmdb_id   = themes.tmdb_id
                AND e.level     = 'WARNING'
                AND e.component = 'download'
                AND json_extract(e.detail, '$.youtube_url')
                    = themes.youtube_url
                AND json_extract(e.detail, '$.kind') IS NOT NULL
                AND NOT EXISTS (
                    SELECT 1 FROM events s
                    WHERE s.media_type = e.media_type
                      AND s.tmdb_id   = e.tmdb_id
                      AND s.level     = 'INFO'
                      AND s.component = 'download'
                      AND s.message LIKE 'Downloaded theme for%'
                      AND json_extract(s.detail, '$.video_id')
                          = themes.youtube_video_id
                      AND s.ts > e.ts
                )
          )
        """
    )
    n = cur.rowcount or 0
    if n:
        log.info("v43 backfill restored failure_kind on %d row(s)", n)
        # Banner marker — dashboard reads this once and offers a
        # dismiss button. runtime_settings already exists (created
        # in v3); writing here avoids needing a new table.
        conn.execute(
            """INSERT INTO runtime_settings (key, value, updated_at, updated_by)
               VALUES ('v1_13_75_backfill_count', ?, datetime('now'), 'migration')
               ON CONFLICT(key) DO UPDATE SET
                   value = excluded.value,
                   updated_at = excluded.updated_at""",
            (str(n),),
        )
    else:
        log.info("v43 backfill: no rows needed restoration")


def _migrate_v41_to_v42(conn: sqlite3.Connection) -> None:
    """v42 (v1.13.61): clean stale urls_match pending_updates rows that
    have no canonical.

    Background: v1.12.62 introduced eager urls_match prompts —
    SET URL on a row whose new override URL exactly matches the
    current TDB URL inserted a synthetic pending_updates row to
    surface the "convert U → T via ACCEPT UPDATE" affordance.
    Designed for the case where there's already a working canonical
    to convert.

    The bug: on rows with no canonical (failed downloads / never
    downloaded — e.g., the SET URL retry after a VPN/location
    change to test if a YouTube rights block has cleared), the
    urls_match prompt was actionless — ACCEPT UPDATE re-downloaded
    the same dead URL and looped, KEEP CURRENT works but isn't
    always obvious. The topbar UPD pill clung to a phantom +1.

    v1.13.55 added a canonical-gate to the SET URL endpoint that
    skips the urls_match insert when no canonical exists. This
    migration cleans up the pre-fix rows that landed before the
    gate, so existing instances self-heal on the next motif
    restart.

    Idempotent (DELETE on absent rows is a no-op). Safe to re-run.
    Only touches `kind='urls_match'` rows — `upstream_changed`
    pending_updates from real TDB drift are untouched.

    === Safety analysis: which URLs are preserved? ===
    The migration deletes the urls_match `pending_updates` row only.
    Every URL the user can manipulate via REVERT / RESTORE / SET URL
    lives elsewhere and survives:

      themes.youtube_url        — TDB's current URL. Different table.
      user_overrides.youtube_url — the URL the user typed via SET URL.
                                   Different table.
      previous_urls.youtube_url — REVERT snapshot. Different table.
      local_files.file_path     — canonical on disk. Different table.

    `pending_updates.new_youtube_url` is a MIRROR of themes.youtube_url
    at the moment the urls_match was inserted; deleting it just
    removes the UI prompt, not any URL the user can lose access to.

    The `NOT EXISTS (... local_files lf ... AND lf.file_path IS NOT
    NULL)` guard preserves urls_match rows that have a working
    canonical (real U→T conversion path). Only the actionless
    no-canonical rows are dropped.

    Migration runs at startup before workers boot, so no in-flight
    download race. Worst case for a row mid-download at restart:
    one cycle of missing the urls_match prompt, re-triggerable by
    SET URL on the next click. No data loss either way.
    """
    log.info("Migrating to schema v42 (clean stale urls_match pending_updates)")
    cur = conn.execute(
        """
        DELETE FROM pending_updates
        WHERE kind = 'urls_match'
          AND NOT EXISTS (
              SELECT 1 FROM local_files lf
              WHERE lf.media_type = pending_updates.media_type
                AND lf.tmdb_id    = pending_updates.tmdb_id
                AND lf.section_id = pending_updates.section_id
                AND lf.file_path  IS NOT NULL
          )
        """
    )
    if cur.rowcount:
        log.info("v42 cleanup dropped %d stale urls_match row(s)", cur.rowcount)


def _migrate_v40_to_v41(conn: sqlite3.Connection) -> None:
    """v41 (v1.13.54): performance indexes + per-section failure acks.

    Three additive indexes flagged by the v1.13.50 audit:
    - idx_plex_items_folder_path: post-place outcome stamps and
      plex_enum reconcile do `WHERE folder_path = ?`; pre-index
      this scanned ~10K rows per stamp.
    - idx_plex_items_indep_null: REPROBE candidate query is
      `WHERE plex_independent_theme IS NULL AND has_theme = 1
      AND local_theme_file = 1` — partial index narrows the
      scan to unobserved sidecar-bearing rows.
    - idx_themes_dropped: dashboard + library SQL filter on
      `tdb_dropped_at IS NOT NULL` to surface gray TDB◌ rows;
      partial index avoids paying for the live (NULL) majority.

    Also adds `section_failure_acks` table — `themes.failure_acked_at`
    is title-global, so adopt-and-ack / purge-and-ack flows clearing
    a failure on the 4K row would also dismiss the standard row's
    FAIL count (cross-section bleed). The new table keys
    (media_type, tmdb_id, section_id) with the ack timestamp +
    actor; library SQL and recovery-options endpoints prefer it
    when section_id context exists, falling back to the legacy
    title-global column.
    """
    log.info("Migrating to schema v41 (perf indexes + section_failure_acks)")
    conn.executescript(
        """
        CREATE INDEX IF NOT EXISTS idx_plex_items_folder_path
            ON plex_items (folder_path)
            WHERE folder_path IS NOT NULL AND folder_path != '';

        CREATE INDEX IF NOT EXISTS idx_plex_items_indep_null
            ON plex_items (rating_key)
            WHERE plex_independent_theme IS NULL
              AND has_theme = 1
              AND local_theme_file = 1;

        CREATE INDEX IF NOT EXISTS idx_themes_dropped
            ON themes (tdb_dropped_at)
            WHERE tdb_dropped_at IS NOT NULL;

        CREATE TABLE IF NOT EXISTS section_failure_acks (
            media_type   TEXT NOT NULL CHECK (media_type IN ('movie', 'tv')),
            tmdb_id      INTEGER NOT NULL,
            section_id   TEXT NOT NULL,
            acked_at     TEXT NOT NULL,
            acked_by     TEXT,
            PRIMARY KEY (media_type, tmdb_id, section_id)
        );

        CREATE INDEX IF NOT EXISTS idx_section_failure_acks_lookup
            ON section_failure_acks (media_type, tmdb_id);
        """
    )


def _migrate_v39_to_v40(conn: sqlite3.Connection) -> None:
    """v40 (v1.13.41): widen op_progress.kind CHECK to include
    'reprobe_plex_themes'. v1.13.38 added the worker but didn't
    touch the CHECK, so the very first call to start_progress
    raised IntegrityError on a clean install. SQLite can't ALTER
    a CHECK in place, so rebuild the table preserving rows.
    """
    log.info("Migrating to schema v40 (op_progress.kind widened)")
    conn.executescript(
        """
        BEGIN;
        -- v1.22.66: BEGIN/COMMIT added (audit round 2) — executescript
        -- runs statement-by-statement in autocommit without it, so a
        -- container kill between DROP and RENAME stranded the data in
        -- the shadow table and crash-looped every boot (the v1.19.73
        -- incident class). The later rebuilds (v47+) always had this.
        CREATE TABLE op_progress_new (
            op_id           TEXT PRIMARY KEY,
            kind            TEXT NOT NULL
                               CHECK (kind IN ('tdb_sync', 'plex_enum',
                                               'reprobe_plex_themes')),
            status          TEXT NOT NULL DEFAULT 'running'
                               CHECK (status IN ('running', 'cancelling',
                                                 'done', 'failed', 'cancelled')),
            started_at      TEXT NOT NULL,
            updated_at      TEXT NOT NULL,
            finished_at     TEXT,
            stage           TEXT,
            stage_label     TEXT,
            stage_current   INTEGER NOT NULL DEFAULT 0,
            stage_total     INTEGER NOT NULL DEFAULT 0,
            processed_total INTEGER NOT NULL DEFAULT 0,
            processed_est   INTEGER NOT NULL DEFAULT 0,
            error_count     INTEGER NOT NULL DEFAULT 0,
            detail_json     TEXT
        );
        INSERT INTO op_progress_new SELECT * FROM op_progress;
        DROP TABLE op_progress;
        ALTER TABLE op_progress_new RENAME TO op_progress;
        COMMIT;
        CREATE INDEX IF NOT EXISTS idx_op_progress_status
            ON op_progress (status, updated_at);
        CREATE INDEX IF NOT EXISTS idx_op_progress_finished
            ON op_progress (finished_at);
        """
    )


def _migrate_v38_to_v39(conn: sqlite3.Connection) -> None:
    """v39 (v1.13.38): persist the +P composite signal. Adds
    plex_items.plex_independent_theme — NULL = unknown,
    0 = Plex serves no separate theme, 1 = Plex serves a theme
    not backed by motif's sidecar (cloud / Plex Pass / themerr-
    plex embed). Pre-fix the +P composite condition was recomputed
    on every render from `has_theme=1 AND local_theme_file=0`,
    which only holds in the brief window before plex_enum stamps
    `local_theme_file=1` after a placement — every steady-state
    +P render was a transient false positive. With the persisted
    flag, plex_enum captures the observation when it CAN be made
    (no sidecar at the folder yet) and the value sticks through
    subsequent placements; the user-facing dot reads the column
    directly.

    Schema change is purely additive (one nullable column). Set
    rules in plex_enum (only when local_theme_file=0 we can
    observe), clear rules in PURGE handler after HEAD probe
    confirms 404.
    """
    log.info("Migrating to schema v39 (plex_items.plex_independent_theme)")
    _add_column(conn, "plex_items", "plex_independent_theme", "INTEGER")


def _migrate_v37_to_v38(conn: sqlite3.Connection) -> None:
    """v38 (v1.13.11): add saved_filters table for the library page's
    saved-preset dropdown. Schema is generic — `scope` column lets
    future pages reuse the same table without another migration.
    Idempotent CREATE so a re-run is safe.
    """
    log.info("Migrating to schema v38 (saved_filters)")
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS saved_filters (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT NOT NULL,
            scope       TEXT NOT NULL DEFAULT 'library',
            query_json  TEXT NOT NULL,
            created_at  TEXT NOT NULL,
            created_by  TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_saved_filters_scope
            ON saved_filters (scope, name);
        """
    )


def _migrate_v36_to_v37(conn: sqlite3.Connection) -> None:
    """v37 (#1, v1.13.2) adds telemetry columns to sync_runs so the
    dashboard sparkline + per-transport summary can show actual perf
    in the user's environment. Existing rows backfill to NULL /
    default 0 — no behavior change until the next sync writes.

    Three columns:
      - transport       which sync.source actually executed (git /
                        database / remote). NULL on legacy rows.
      - fallback_reason set when this run cascaded down a tier.
                        NULL = the requested transport completed
                        cleanly.
      - no_changes      1 when the run short-circuited via Phase
                        A.5's 304 or Phase B's no-new-commits path.
                        Distinct from "0 changes after a full walk"
                        so the dashboard chart can render a sub-
                        second no-op differently from a slow run
                        that happened to find no churn.
    """
    log.info("Migrating to schema v37 (sync_runs telemetry)")
    _add_column(conn, "sync_runs", "transport", "TEXT")
    _add_column(conn, "sync_runs", "fallback_reason", "TEXT")
    _add_column(conn, "sync_runs", "no_changes", "INTEGER NOT NULL DEFAULT 0")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_sync_runs_started "
        "ON sync_runs (started_at DESC)")


def _migrate_v35_to_v36(conn: sqlite3.Connection) -> None:
    """v36 (Phase C, v1.13.1) adds themes.tdb_dropped_at — the
    timestamp at which ThemerrDB stopped publishing this
    (media_type, tmdb_id). Pre-fix motif silently kept the row as
    SRC=T forever even though TDB had withdrawn its endorsement;
    now sync stamps the column on detected removals and the UI
    surfaces a gray TDB◌ pill so the user can decide between
    keeping the local theme (ACK DROP), reclassifying it as a
    user-managed override (CONVERT TO MANUAL), or removing it
    (PURGE). Cleared automatically when the item reappears in
    TDB.

    Schema change is purely additive — existing rows have
    tdb_dropped_at=NULL which the SQL treats as "not dropped",
    identical to pre-v36 behavior until the next sync runs.
    """
    log.info("Migrating to schema v36 (themes.tdb_dropped_at)")
    _add_column(conn, "themes", "tdb_dropped_at", "TEXT")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_themes_dropped "
        "ON themes (tdb_dropped_at) WHERE tdb_dropped_at IS NOT NULL")


def _migrate_v34_to_v35(conn: sqlite3.Connection) -> None:
    """v35 adds Plex theme-claim verification on plex_items. Plex's API
    advertises theme URLs that may or may not actually serve content
    — themerr-plex embeds, Plex Pass cloud themes, manual sidecars,
    and stale metadata caches all surface the same way. Now plex_enum
    captures the URI and runs a cheap HEAD against it to verify Plex
    actually serves something, so SRC=P reflects the truth on disk
    (Plex serving a real theme) rather than just Plex's claim.

    Schema change is purely additive: three new nullable columns.
    Existing rows have verified_ok=NULL which the SRC SQL treats as
    "untested → trust the claim optimistically", so behavior is
    identical to pre-v1.12.112 until the first plex_enum runs.
    """
    log.info("Migrating to schema v35 (plex_items theme verification)")
    _add_column(conn, "plex_items", "plex_theme_uri", "TEXT")
    _add_column(conn, "plex_items", "plex_theme_verified_at", "TEXT")
    _add_column(conn, "plex_items", "plex_theme_verified_ok", "INTEGER")


def _migrate_v33_to_v34(conn: sqlite3.Connection) -> None:
    """v34 adds plex_items.motif_unplaced_at — originally a tombstone
    for "motif just removed its theme from this section" so the SRC
    SQL could suppress phantom-P right after PURGE/UNMANAGE.

    DEPRECATED in v1.12.111: the column is no longer read OR written.
    Phantom-P is now handled by `pi.plex_theme_verified_ok` (HEAD
    against /library/metadata/{rk}/theme), which is more accurate
    because it reflects what Plex actually serves rather than what
    motif last did. The tombstone column stays in the schema for
    forward-only migration safety; future migrations may drop it.

    DO NOT add new readers/writers to motif_unplaced_at — every
    place it was used has been replaced by the verified_ok signal.
    Future-self tip: if you're debugging a phantom-P regression,
    grep for `plex_theme_verified_ok`, not `motif_unplaced_at`.

    Schema change here is purely additive (one nullable column);
    the body kept verbatim so re-running this migration on an
    older DB is still idempotent.
    """
    log.info("Migrating to schema v34 (plex_items.motif_unplaced_at)")
    conn.executescript(
        """
        ALTER TABLE plex_items ADD COLUMN motif_unplaced_at TEXT;
        """
    )


def _migrate_v32_to_v33(conn: sqlite3.Connection) -> None:
    """v33 adds the op_progress table: one row per long-running op
    (TDB sync, Plex enum) so the ops side-drawer can render live
    per-stage progress, throughput, ETA, and a cancel button. Pre-fix
    the topbar showed only a spinning dot + "REFRESHING…" text driven
    by /api/stats counts — no per-step visibility into syncs that
    routinely take 10–40 minutes.

    Schema is purely additive: new table, new indexes, no changes
    to existing tables.
    """
    log.info("Migrating to schema v33 (op_progress table)")
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS op_progress (
            op_id           TEXT PRIMARY KEY,
            kind            TEXT NOT NULL
                               CHECK (kind IN ('tdb_sync', 'plex_enum')),
            status          TEXT NOT NULL DEFAULT 'running'
                               CHECK (status IN ('running', 'cancelling',
                                                 'done', 'failed', 'cancelled')),
            started_at      TEXT NOT NULL,
            updated_at      TEXT NOT NULL,
            finished_at     TEXT,
            stage           TEXT,
            stage_label     TEXT,
            stage_current   INTEGER NOT NULL DEFAULT 0,
            stage_total     INTEGER NOT NULL DEFAULT 0,
            processed_total INTEGER NOT NULL DEFAULT 0,
            processed_est   INTEGER NOT NULL DEFAULT 0,
            error_count     INTEGER NOT NULL DEFAULT 0,
            detail_json     TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_op_progress_status
            ON op_progress (status, updated_at);
        CREATE INDEX IF NOT EXISTS idx_op_progress_finished
            ON op_progress (finished_at);
        """
    )


def _migrate_v28_to_v29(conn: sqlite3.Connection) -> None:
    """v29 adds events.section_id + per-(media_type, tmdb_id, section_id)
    index. Previously the events table was title-global — a single
    INFO card opened on a 4K row showed every section's worker /
    sync / API events because the recent_events query had no way to
    scope by section. With section_id on each row, the api_item
    fetch can mirror the audit_events filter (rows for this section
    plus title-global rows where section_id IS NULL), eliminating
    the cross-section HISTORY bleed.

    Existing rows backfill to NULL section_id since we can't recover
    the section retroactively. The api_item filter treats NULL as
    "applies to every section" so legacy data still surfaces — the
    user just won't get section attribution for events captured
    before the migration.
    """
    log.info("Migrating to schema v29 (events.section_id)")
    _add_column(conn, "events", "section_id", "TEXT")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_events_item_section "
        "ON events (media_type, tmdb_id, section_id, ts DESC)")


def _migrate_v23_to_v24(conn: sqlite3.Connection) -> None:
    """v24 adds pending_updates.replaced_user_url for the v1.12.34
    ACCEPT UPDATE / REVERT round-trip on U-source rows.

    Pre-fix, ACCEPT UPDATE on a row that had a manual YouTube URL
    (user_overrides present) was effectively a no-op for the
    canonical file: it flipped pending_updates.decision to
    'accepted' but left user_overrides in place. The download
    worker prefers the override URL, so the "accept" never
    actually pulled the new TDB URL — yet the blue TDB ↑ pill
    disappeared and the source kept reading as U.

    v1.12.34 fixes the flow:
      - On ACCEPT UPDATE: capture user_overrides.youtube_url into
        replaced_user_url here, DELETE user_overrides, then
        re-download. Worker now uses TDB's URL and the row
        classifies as T.
      - On REVERT (post-accept): restore user_overrides from
        replaced_user_url, clear the column, re-download — round
        trip back to U with the user's original URL.

    Soft ALTER — purely additive, NULL default means existing
    rows are correctly classified as 'no replaced URL' without
    backfill.
    """
    log.info("Migrating to schema v24 (pending_updates.replaced_user_url)")
    _add_column(conn, "pending_updates", "replaced_user_url", "TEXT")


def _migrate_v22_to_v23(conn: sqlite3.Connection) -> None:
    """v23 adds jobs.acked_at — non-destructive 'CLEAR FAILED' on the
    /logs page (v1.12.12).

    Pre-fix CLEAR FAILED ran `DELETE FROM jobs WHERE status='failed'`
    which threw away the historical record of failures. Now CLEAR
    FAILED instead stamps jobs.acked_at and the row stays put — it
    renders in green ('ACKNOWLEDGED') instead of red, drops out of
    the "failed" topbar count, but is still filterable / sortable
    so the user can review what failed in the past.
    """
    log.info("Migrating to schema v23 (jobs.acked_at)")
    _add_column(conn, "jobs", "acked_at", "TEXT")


def _migrate_v21_to_v22(conn: sqlite3.Connection) -> None:
    """v22 adds local_files.mismatch_state for the v1.11.99 mismatch
    workflow.

    SET URL / UPLOAD MP3 on a row that already has a placement now
    lands the new download in canonical *without* touching the
    placement file in the Plex folder. The user resolves the
    divergence via /pending or the row's PLACE menu:
      - PUSH TO PLEX    → write canonical to placement
      - ADOPT FROM PLEX → re-adopt placement file as new canonical
                          (discards the user's new download)
      - KEEP MISMATCH   → ack and dismiss from /pending; library row
                          still shows DL=amber + LINK=≠.

    Soft ALTER — purely additive, NULL default means existing rows
    are correctly classified as 'no mismatch' without backfill.
    """
    log.info("Migrating to schema v22 (local_files.mismatch_state)")
    _add_column(conn, "local_files", "mismatch_state", "TEXT")


def _migrate_v20_to_v21(conn: sqlite3.Connection) -> None:
    """v21: dev-mode hard stop.

    v1.11.36 added jobs.cancel_requested. Wipe /config/motif.db.
    """
    raise RuntimeError(
        "v1.11.36 added jobs.cancel_requested. Dev mode: delete "
        "/config/motif.db and restart."
    )


def _migrate_v19_to_v20(conn: sqlite3.Connection) -> None:
    """v20: dev-mode hard stop.

    v1.11.26 added plex_items.theme_id (denormalized themes match cache).
    Wipe /config/motif.db and let plex_enum + sync repopulate.
    """
    raise RuntimeError(
        "v1.11.26 added plex_items.theme_id. Dev mode: delete "
        "/config/motif.db and restart. plex_enum + sync repopulate."
    )


def _migrate_v18_to_v19(conn: sqlite3.Connection) -> None:
    """v19: dev-mode hard stop.

    v1.11.24 added local_files.last_place_attempt_at + reason. Per the
    dev-mode policy, no migration path — wipe /config/motif.db and let
    sync + plex_enum + scan repopulate.
    """
    raise RuntimeError(
        "v1.11.24 changed local_files (added last_place_attempt_at + "
        "last_place_attempt_reason). Dev mode: delete /config/motif.db "
        "and restart. sync + plex_enum + scan will repopulate."
    )


def _migrate_v17_to_v18(conn: sqlite3.Connection) -> None:
    """v18: per-Plex-section themes layout.

    v1.11.0 reshapes local_files, placements, plex_sections, and jobs to
    carry section_id so each Plex section gets its own staging subdir +
    its own per-row tracking. The data shape changes in ways that can't
    be backfilled cleanly (one local_files row per item becomes N rows
    per item-in-N-sections, files have to be re-staged into per-section
    subdirs, etc.).

    v1.11.0 ships as a fresh-start release: existing /config/motif.db
    files must be deleted before first boot. After deletion, motif
    re-runs sync + plex_enum + scan to repopulate everything from
    upstream sources, in the new shape, automatically.
    """
    raise RuntimeError(
        "v1.11.0 is a fresh-start release — the per-section themes layout "
        "is not migratable from v1.10.x. Delete /config/motif.db (and its "
        "-wal / -shm sidecars), then start motif. Sync + plex_enum + scan "
        "will repopulate the database from upstream sources."
    )


def _migrate_v16_to_v17(conn: sqlite3.Connection) -> None:
    """v17: auto-ack any failure_kind that was stamped by a sync probe.

    v1.10.59 fix: probe-detected failures should paint the TDB pill
    red/amber but not increment the topbar's failures badge or require
    user acknowledgement. Going forward _do_probe sets failure_acked_at
    inline; this migration backfills already-flagged rows so the badge
    drops to a sane state immediately after upgrade.
    """
    log.info("Migrating to schema v17 (auto-ack sync-probe failures)")
    conn.execute(
        "UPDATE themes SET failure_acked_at = COALESCE(failure_acked_at, datetime('now')) "
        "WHERE failure_kind IS NOT NULL "
        "  AND failure_acked_at IS NULL "
        "  AND failure_message LIKE 'sync probe:%'"
    )


def _migrate_v15_to_v16(conn: sqlite3.Connection) -> None:
    """v16: add local_files_history table.

    Stores a snapshot of local_files (+ user_overrides.youtube_url) at
    unmanage / forget time, keyed by file_sha256. adopt_folder looks
    this up by sha256 to restore U → M → A loop's original
    youtube_url so the user's metadata isn't silently lost.
    """
    log.info("Migrating to schema v16 (local_files_history)")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS local_files_history (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            file_sha256     TEXT NOT NULL,
            media_type      TEXT,
            tmdb_id         INTEGER,
            source_kind     TEXT,
            source_video_id TEXT,
            youtube_url     TEXT,
            saved_at        TEXT NOT NULL,
            saved_reason    TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_local_files_history_sha
            ON local_files_history (file_sha256, saved_at DESC);
    """)


def _migrate_v14_to_v15(conn: sqlite3.Connection) -> None:
    """v15: add themes.failure_acked_at to split 'TDB URL is broken'
    from 'user has acknowledged the failure'.

    Pre-1.10.50, ACK FAILURE nulled failure_kind which made the TDB
    pill go green even though the upstream URL was still dead. Per
    user feedback the pill should stay red (still failing) while ACK
    only removes the ! glyph + FAILURES-filter membership. Manual
    workarounds (SET URL, UPLOAD, ADOPT) also implicitly ack so the
    alert clears even though TDB's URL remains dead.

    The new column is just a timestamp; failure_kind keeps the kind.
    A new failure_kind value (different from any prior) clears
    failure_acked_at so a different failure re-alerts.
    """
    log.info("Migrating to schema v15 (themes.failure_acked_at)")
    cols = {row[1] for row in conn.execute("PRAGMA table_info(themes)")}
    if "failure_acked_at" not in cols:
        conn.execute("ALTER TABLE themes ADD COLUMN failure_acked_at TEXT")


def _migrate_v13_to_v14(conn: sqlite3.Connection) -> None:
    """v14: add 'probe' to jobs.job_type CHECK constraint.

    SQLite can't ALTER a CHECK in place — rebuild the table preserving
    every column + index. Mirrors the v6 rebuild that added 'plex_enum'.

    Probe is the v1.10.45 background URL-availability job: yt-dlp
    metadata-only check on a single (media_type, tmdb_id) so the
    library's TDB pill can paint dead/cookied URLs ahead of any
    download attempt. Replaces the old 100-cap inline probe phase.
    """
    log.info("Migrating to schema v14 (jobs CHECK + 'probe' type)")
    conn.executescript("""
        CREATE TABLE jobs__v14 (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            job_type        TEXT NOT NULL CHECK (
                job_type IN ('sync', 'download', 'place', 'refresh', 'relink',
                             'scan', 'adopt', 'plex_enum', 'probe')
            ),
            media_type      TEXT,
            tmdb_id         INTEGER,
            payload         TEXT,
            status          TEXT NOT NULL DEFAULT 'pending'
                               CHECK (status IN ('pending', 'running',
                                                 'success', 'failed', 'cancelled',
                                                 'done')),
            attempts        INTEGER NOT NULL DEFAULT 0,
            last_error      TEXT,
            created_at      TEXT NOT NULL,
            next_run_at     TEXT NOT NULL,
            started_at      TEXT,
            finished_at     TEXT
        );
        INSERT INTO jobs__v14 (id, job_type, media_type, tmdb_id, payload, status,
                               attempts, last_error, created_at, next_run_at,
                               started_at, finished_at)
            SELECT id, job_type, media_type, tmdb_id, payload, status,
                   attempts, last_error, created_at, next_run_at,
                   started_at, finished_at FROM jobs;
        DROP TABLE jobs;
        ALTER TABLE jobs__v14 RENAME TO jobs;
        CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs (status, next_run_at);
        CREATE INDEX IF NOT EXISTS idx_jobs_type ON jobs (job_type, status);
        CREATE INDEX IF NOT EXISTS idx_jobs_item ON jobs (media_type, tmdb_id);
        CREATE INDEX IF NOT EXISTS idx_jobs_type_status_finished
            ON jobs (job_type, status, finished_at);
        ANALYZE;
    """)


def _migrate_v12_to_v13(conn: sqlite3.Connection) -> None:
    """v13 adds title_norm columns to themes + plex_items.

    The library JOIN already matches Plex items to ThemerrDB themes by
    (guid_tmdb / tmdb_id) and (guid_imdb / imdb_id). When Plex's GUIDs
    disagree with ThemerrDB (different tmdb assignment, stale Plex
    metadata, localized titles), the row stays untracked even though
    a title-level match exists. Example user report: ThemerrDB has
    'Twelve Monkeys (1995)', Plex has '12 Monkeys (1995)' — same film,
    but the local row reads as untracked.

    Backfill normalized titles via app.core.normalize:normalize_title
    so the library JOIN can fall back to (media_type, title_norm, year)
    when the GUID match misses. Indexes added in the canonical SCHEMA
    backfill on the same path.
    """
    log.info("Migrating to schema v13 (title_norm + indexes)")
    # Column adds (idempotent — guard against repeat application by
    # checking PRAGMA table_info, since SQLite has no IF NOT EXISTS for
    # ADD COLUMN).
    cols = {row[1] for row in conn.execute("PRAGMA table_info(themes)")}
    if "title_norm" not in cols:
        conn.execute("ALTER TABLE themes ADD COLUMN title_norm TEXT")
    cols = {row[1] for row in conn.execute("PRAGMA table_info(plex_items)")}
    if "title_norm" not in cols:
        conn.execute("ALTER TABLE plex_items ADD COLUMN title_norm TEXT")

    # Backfill — import locally to avoid a circular import at module
    # load (normalize.py is a leaf module, but db.py is imported very
    # early in the boot sequence).
    from .normalize import normalize_title
    # v1.20.42: count the lossy .lower() fallbacks so a normalize_title
    # regression during this one-shot backfill leaves a log breadcrumb
    # (class 9 cold-path rule) instead of silently degrading match quality.
    _lossy = 0
    for row in conn.execute(
        "SELECT id, title FROM themes WHERE title_norm IS NULL"
    ).fetchall():
        try:
            tn = normalize_title(row[1] or "")
        except Exception:
            tn = (row[1] or "").lower()
            _lossy += 1
        conn.execute(
            "UPDATE themes SET title_norm = ? WHERE id = ?", (tn, row[0]),
        )
    for row in conn.execute(
        "SELECT rating_key, title FROM plex_items WHERE title_norm IS NULL"
    ).fetchall():
        try:
            tn = normalize_title(row[1] or "")
        except Exception:
            tn = (row[1] or "").lower()
            _lossy += 1
        conn.execute(
            "UPDATE plex_items SET title_norm = ? WHERE rating_key = ?",
            (tn, row[0]),
        )
    if _lossy:
        log.warning("v11 title_norm backfill: %d row(s) fell back to the "
                    "lossy .lower() because normalize_title raised", _lossy)


def _migrate_v11_to_v12(conn: sqlite3.Connection) -> None:
    """v12 re-disambiguates U vs A on legacy rows.

    The v11 backfill couldn't tell apart two cases that look identical in
    local_files alone (provenance='manual', source_video_id is an 11-char
    YouTube id):
      (a) the user provided a manual URL — should be 'url' (U badge)
      (b) the user adopted a sidecar that resolved to a real ThemerrDB
          title (canonical filename uses the upstream youtube id) —
          should be 'adopt' (A badge)
    The disambiguator: case (a) writes a user_overrides row, case (b)
    doesn't. Re-run the classification using that signal.

    User feedback: 'I'm also seeing files from previous tests showing
    as uploaded U instead of A adopted'.
    """
    log.info("Migrating to schema v12 (U vs A disambiguation via user_overrides)")
    conn.executescript("""
        UPDATE local_files
           SET source_kind = 'adopt'
         WHERE source_kind = 'url'
           AND NOT EXISTS (
               SELECT 1 FROM user_overrides uo
                WHERE uo.media_type = local_files.media_type
                  AND uo.tmdb_id = local_files.tmdb_id
           );
    """)


def _migrate_v10_to_v11(conn: sqlite3.Connection) -> None:
    """v11 adds local_files.source_kind so the row badge can show T/U/A
    without guessing.

    Pre-1.10.12 the UI inferred the badge from source_video_id shape:
      ''                   → upload  (U)
      11-char yt-id        → url     (U)
      anything else        → adopt   (A)
    But _do_adopt for an orphan_resolvable that resolved to a real
    ThemerrDB row writes the YouTube video id into source_video_id
    (canonical filename uses it for cache stability). That row got
    mis-classified as U when it should be A.

    Backfill heuristic for existing rows — same shape rules, but
    additionally promote orphan_-prefixed source_video_id (only an
    adopt path produces that pattern) to 'adopt' so we don't lose
    those. Manual rows that share a YouTube-id shape with the URL
    overrides stay tagged 'url' — that's the same ambiguity the
    pre-1.10.12 heuristic had, and there's no data to disambiguate.
    """
    log.info("Migrating to schema v11 (local_files.source_kind)")
    conn.executescript("""
        ALTER TABLE local_files ADD COLUMN source_kind TEXT;
        UPDATE local_files
           SET source_kind = 'themerrdb'
         WHERE provenance = 'auto'
           AND source_video_id NOT LIKE 'orphan_%';
        UPDATE local_files
           SET source_kind = 'adopt'
         WHERE source_video_id LIKE 'orphan_%';
        UPDATE local_files
           SET source_kind = 'upload'
         WHERE provenance = 'manual'
           AND (source_video_id IS NULL OR source_video_id = '');
        UPDATE local_files
           SET source_kind = 'url'
         WHERE provenance = 'manual'
           AND length(source_video_id) = 11
           AND source_video_id GLOB '[A-Za-z0-9_-]*'
           AND source_kind IS NULL;
    """)


def _migrate_v9_to_v10(conn: sqlite3.Connection) -> None:
    """v10: index hygiene + library-page perf.

    Two install paths historically had divergent jobs indexes:
      - fresh install (CREATE TABLE jobs path) got idx_jobs_status +
        idx_jobs_type, but NOT idx_jobs_item.
      - migrated install (v6 _migrate_v5_to_v6 jobs rebuild) got
        idx_jobs_status_next + idx_jobs_item, but NOT idx_jobs_type.

    Result: depending on whether you came in fresh or migrated, the
    library page's per-row job_in_flight correlated subquery either
    used a (media_type, tmdb_id) index or scanned the whole jobs
    table per row — manifesting as a soft-lock on tab navigation
    once jobs grew past a few thousand rows.

    Reconcile by ensuring BOTH (media_type, tmdb_id) and
    (job_type, status) indexes exist, plus the new
    (job_type, status, finished_at) compound used by the
    library page's plex-scan-stale advisory.
    """
    log.info("Migrating to schema v10 (jobs index hygiene)")
    conn.executescript("""
        CREATE INDEX IF NOT EXISTS idx_jobs_item ON jobs (media_type, tmdb_id);
        CREATE INDEX IF NOT EXISTS idx_jobs_type ON jobs (job_type, status);
        CREATE INDEX IF NOT EXISTS idx_jobs_type_status_finished
            ON jobs (job_type, status, finished_at);
        -- ANALYZE updates SQLite's stat tables so the query planner
        -- picks the right index for the library page's correlated
        -- subqueries. Cheap one-shot at migration time.
        ANALYZE;
    """)


def _migrate_v7_to_v8(conn: sqlite3.Connection) -> None:
    """v8 adds is_anime + is_4k flags to plex_sections — user-applied
    classifiers that drive the Movies / TV / Anime tab partition. Both
    default to 0 so existing installs keep their old behaviour until the
    user opts in via Settings → PLEX → LIBRARY SECTIONS.
    """
    log.info("Migrating to schema v8 (plex_sections is_anime + is_4k flags)")
    conn.executescript("""
        ALTER TABLE plex_sections ADD COLUMN is_anime INTEGER NOT NULL DEFAULT 0;
        ALTER TABLE plex_sections ADD COLUMN is_4k    INTEGER NOT NULL DEFAULT 0;
    """)


def init_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        # Pragmas first (safe on every DB state)
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.execute("PRAGMA foreign_keys = ON")

        # Check existing schema version
        existing: int | None = None
        sv_exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='schema_version'"
        ).fetchone() is not None
        if sv_exists:
            row = conn.execute("SELECT MAX(version) FROM schema_version").fetchone()
            existing = row[0]

        if existing is None:
            # Fresh DB — apply the current SCHEMA in full and stamp the version
            conn.executescript(SCHEMA)
            conn.execute(
                "INSERT INTO schema_version (version, applied_at) VALUES (?, datetime('now'))",
                (CURRENT_SCHEMA_VERSION,),
            )
        else:
            # Existing DB — apply migrations one step at a time, then run the
            # idempotent SCHEMA at the end to backfill any new tables/indexes
            current = existing
            while current < CURRENT_SCHEMA_VERSION:
                if current == 1:
                    _migrate_v1_to_v2(conn)
                    current = 2
                elif current == 2:
                    _migrate_v2_to_v3(conn)
                    current = 3
                elif current == 3:
                    _migrate_v3_to_v4(conn)
                    current = 4
                elif current == 4:
                    _migrate_v4_to_v5(conn)
                    current = 5
                elif current == 5:
                    _migrate_v5_to_v6(conn)
                    current = 6
                elif current == 6:
                    _migrate_v6_to_v7(conn)
                    current = 7
                elif current == 7:
                    _migrate_v7_to_v8(conn)
                    current = 8
                elif current == 8:
                    _migrate_v8_to_v9(conn)
                    current = 9
                elif current == 9:
                    _migrate_v9_to_v10(conn)
                    current = 10
                elif current == 10:
                    _migrate_v10_to_v11(conn)
                    current = 11
                elif current == 11:
                    _migrate_v11_to_v12(conn)
                    current = 12
                elif current == 12:
                    _migrate_v12_to_v13(conn)
                    current = 13
                elif current == 13:
                    _migrate_v13_to_v14(conn)
                    current = 14
                elif current == 14:
                    _migrate_v14_to_v15(conn)
                    current = 15
                elif current == 15:
                    _migrate_v15_to_v16(conn)
                    current = 16
                elif current == 16:
                    _migrate_v16_to_v17(conn)
                    current = 17
                elif current == 17:
                    _migrate_v17_to_v18(conn)
                    current = 18
                elif current == 18:
                    _migrate_v18_to_v19(conn)
                    current = 19
                elif current == 19:
                    _migrate_v19_to_v20(conn)
                    current = 20
                elif current == 20:
                    _migrate_v20_to_v21(conn)
                    current = 21
                elif current == 21:
                    _migrate_v21_to_v22(conn)
                    current = 22
                elif current == 22:
                    _migrate_v22_to_v23(conn)
                    current = 23
                elif current == 23:
                    _migrate_v23_to_v24(conn)
                    current = 24
                elif current == 24:
                    _migrate_v24_to_v25(conn)
                    current = 25
                elif current == 25:
                    _migrate_v25_to_v26(conn)
                    current = 26
                elif current == 26:
                    _migrate_v26_to_v27(conn)
                    current = 27
                elif current == 27:
                    _migrate_v27_to_v28(conn)
                    current = 28
                elif current == 28:
                    _migrate_v28_to_v29(conn)
                    current = 29
                elif current == 29:
                    _migrate_v29_to_v30(conn)
                    current = 30
                elif current == 30:
                    _migrate_v30_to_v31(conn)
                    current = 31
                elif current == 31:
                    _migrate_v31_to_v32(conn)
                    current = 32
                elif current == 32:
                    _migrate_v32_to_v33(conn)
                    current = 33
                elif current == 33:
                    _migrate_v33_to_v34(conn)
                    current = 34
                elif current == 34:
                    _migrate_v34_to_v35(conn)
                    current = 35
                elif current == 35:
                    _migrate_v35_to_v36(conn)
                    current = 36
                elif current == 36:
                    _migrate_v36_to_v37(conn)
                    current = 37
                elif current == 37:
                    _migrate_v37_to_v38(conn)
                    current = 38
                elif current == 38:
                    _migrate_v38_to_v39(conn)
                    current = 39
                elif current == 39:
                    _migrate_v39_to_v40(conn)
                    current = 40
                elif current == 40:
                    _migrate_v40_to_v41(conn)
                    current = 41
                elif current == 41:
                    _migrate_v41_to_v42(conn)
                    current = 42
                elif current == 42:
                    _migrate_v42_to_v43(conn)
                    current = 43
                elif current == 43:
                    _migrate_v43_to_v44(conn)
                    current = 44
                elif current == 44:
                    _migrate_v44_to_v45(conn)
                    current = 45
                elif current == 45:
                    _migrate_v45_to_v46(conn)
                    current = 46
                elif current == 46:
                    _migrate_v46_to_v47(conn)
                    current = 47
                elif current == 47:
                    _migrate_v47_to_v48(conn)
                    current = 48
                elif current == 48:
                    _migrate_v48_to_v49(conn)
                    current = 49
                elif current == 49:
                    _migrate_v49_to_v50(conn)
                    current = 50
                elif current == 50:
                    _migrate_v50_to_v51(conn)
                    current = 51
                elif current == 51:
                    _migrate_v51_to_v52(conn)
                    current = 52
                elif current == 52:
                    _migrate_v52_to_v53(conn)
                    current = 53
                elif current == 53:
                    _migrate_v53_to_v54(conn)
                    current = 54
                elif current == 54:
                    _migrate_v54_to_v55(conn)
                    current = 55
                elif current == 55:
                    _migrate_v55_to_v56(conn)
                    current = 56
                elif current == 56:
                    _migrate_v56_to_v57(conn)
                    current = 57
                elif current == 57:
                    _migrate_v57_to_v58(conn)
                    current = 58
                elif current == 58:
                    _migrate_v58_to_v59(conn)
                    current = 59
                elif current == 59:
                    _migrate_v59_to_v60(conn)
                    current = 60
                elif current == 60:
                    _migrate_v60_to_v61(conn)
                    current = 61
                elif current == 61:
                    _migrate_v61_to_v62(conn)
                    current = 62
                elif current == 62:
                    _migrate_v62_to_v63(conn)
                    current = 63
                elif current == 63:
                    _migrate_v63_to_v64(conn)
                    current = 64
                elif current == 64:
                    _migrate_v64_to_v65(conn)
                    current = 65
                elif current == 65:
                    _migrate_v65_to_v66(conn)
                    current = 66
                elif current == 66:
                    _migrate_v66_to_v67(conn)
                    current = 67
                elif current == 67:
                    _migrate_v67_to_v68(conn)
                    current = 68
                elif current == 68:
                    _migrate_v68_to_v69(conn)
                    current = 69
                elif current == 69:
                    _migrate_v69_to_v70(conn)
                    current = 70
                elif current == 70:
                    _migrate_v70_to_v71(conn)
                    current = 71
                elif current == 71:
                    _migrate_v71_to_v72(conn)
                    current = 72
                elif current == 72:
                    _migrate_v72_to_v73(conn)
                    current = 73
                elif current == 73:
                    _migrate_v73_to_v74(conn)
                    current = 74
                elif current == 74:
                    _migrate_v74_to_v75(conn)
                    current = 75
                elif current == 75:
                    _migrate_v75_to_v76(conn)
                    current = 76
                elif current == 76:
                    _migrate_v76_to_v77(conn)
                    current = 77
                elif current == 77:
                    _migrate_v77_to_v78(conn)
                    current = 78
                else:
                    raise RuntimeError(f"No migration from v{current}")
                conn.execute(
                    "INSERT INTO schema_version (version, applied_at) VALUES (?, datetime('now'))",
                    (current,),
                )
            # Now safe to run SCHEMA — all tables have the columns we expect
            conn.executescript(SCHEMA)
        conn.commit()
    log.info("Database initialized at %s", db_path)


@contextmanager
def get_conn(db_path: Path) -> Iterator[sqlite3.Connection]:
    conn = sqlite3.connect(
        db_path,
        timeout=30.0,
        isolation_level=None,  # autocommit; we manage transactions explicitly
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    # v1.13.50: PRAGMA busy_timeout makes every statement (not just
    # the BEGIN IMMEDIATE that transaction() wraps) participate in
    # SQLite's wait-for-lock queue. Pre-fix only the connection-
    # level Python timeout=30.0 covered lock contention, which only
    # kicks in when the C library returns SQLITE_BUSY immediately
    # — leaving plain reads/writes outside an explicit transaction
    # vulnerable to spurious lock failures during heavy sync /
    # plex_enum writer activity. 30000ms matches the Python timeout.
    conn.execute("PRAGMA busy_timeout = 30000")
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Wrap a block in a BEGIN IMMEDIATE...COMMIT.

    v1.11.56: BEGIN IMMEDIATE itself is now retried on
    sqlite3.OperationalError 'database is locked' (the connection's
    own busy_timeout=30s isn't always enough under heavy
    sync/plex_enum write contention — busy_timeout doesn't queue
    fairly, so a particular waiter can lose every race). Up to four
    attempts with backoff (0.5s → 1s → 2s → 4s); after that the
    OperationalError propagates so the caller sees it.

    Once we're past BEGIN IMMEDIATE the transaction body still
    runs to completion or raises the original exception unmodified
    — only the BEGIN itself gets retry semantics. SQLite holds the
    writer lock for the duration of the block; nothing in here
    re-acquires it.
    """
    import time as _t
    delays = (0.5, 1.0, 2.0, 4.0)
    last_err: sqlite3.OperationalError | None = None
    for d in (0.0,) + delays:
        if d:
            _t.sleep(d)
        try:
            conn.execute("BEGIN IMMEDIATE")
            break
        except sqlite3.OperationalError as e:
            if "locked" not in str(e).lower():
                raise
            last_err = e
            continue
    else:
        # Exhausted all retries; re-raise the last lock error so the
        # caller gets a clean OperationalError to handle (worker
        # supervisor / _safe_mark / etc).
        assert last_err is not None
        raise last_err
    try:
        yield conn
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
