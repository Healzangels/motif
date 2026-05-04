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
    UNIQUE (media_type, tmdb_id)
);

CREATE INDEX IF NOT EXISTS idx_themes_imdb ON themes (imdb_id) WHERE imdb_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_themes_tvdb ON themes (tvdb_id) WHERE tvdb_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_themes_video ON themes (youtube_video_id);
CREATE INDEX IF NOT EXISTS idx_themes_title ON themes (title);
CREATE INDEX IF NOT EXISTS idx_themes_failure ON themes (failure_kind) WHERE failure_kind IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_themes_orphan ON themes (upstream_source) WHERE upstream_source = 'plex_orphan';
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
    file_path       TEXT NOT NULL,
    file_sha256     TEXT,
    file_size       INTEGER,
    downloaded_at   TEXT NOT NULL,
    source_video_id TEXT NOT NULL,
    provenance      TEXT NOT NULL DEFAULT 'auto'
                       CHECK (provenance IN ('auto', 'manual')),
    source_kind     TEXT
                       CHECK (source_kind IN ('themerrdb', 'url',
                                              'upload', 'adopt')
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
    PRIMARY KEY (media_type, tmdb_id, section_id),
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
    placement_kind  TEXT NOT NULL CHECK (placement_kind IN ('hardlink', 'copy', 'symlink')),
    plex_rating_key TEXT,
    plex_refreshed  INTEGER NOT NULL DEFAULT 0,
    provenance      TEXT NOT NULL DEFAULT 'auto'
                       CHECK (provenance IN ('auto', 'manual', 'cloud')),
    PRIMARY KEY (media_type, tmdb_id, section_id, media_folder),
    FOREIGN KEY (media_type, tmdb_id) REFERENCES themes (media_type, tmdb_id) ON DELETE CASCADE,
    FOREIGN KEY (section_id) REFERENCES plex_sections (section_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_placements_item ON placements (media_type, tmdb_id);
CREATE INDEX IF NOT EXISTS idx_placements_section ON placements (section_id);

-- Download / placement / sync queue. Workers consume from here.
-- v1.11.0: section_id carries the per-section context for download/place/
-- refresh/adopt jobs. NULL for section-agnostic jobs (sync, plex_enum
-- without a section, scan, probe).
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

CREATE TABLE IF NOT EXISTS sync_runs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at      TEXT NOT NULL,
    finished_at     TEXT,
    status          TEXT NOT NULL DEFAULT 'running'
                       CHECK (status IN ('running', 'success', 'failed')),
    movies_seen     INTEGER NOT NULL DEFAULT 0,
    tv_seen         INTEGER NOT NULL DEFAULT 0,
    new_count       INTEGER NOT NULL DEFAULT 0,
    updated_count   INTEGER NOT NULL DEFAULT 0,
    error           TEXT
);

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
    PRIMARY KEY (media_type, tmdb_id, section_id)
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
    last_seen_at    TEXT NOT NULL
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
    media_type       TEXT NOT NULL CHECK (media_type IN ('movie', 'show')),
    title            TEXT NOT NULL,
    year             TEXT,
    guid_imdb        TEXT,
    guid_tmdb        INTEGER,
    guid_tvdb        INTEGER,
    folder_path      TEXT NOT NULL DEFAULT '',
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
    first_seen_at    TEXT NOT NULL,
    last_seen_at     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_plex_items_section ON plex_items (section_id);
CREATE INDEX IF NOT EXISTS idx_plex_items_type    ON plex_items (media_type);
CREATE INDEX IF NOT EXISTS idx_plex_items_imdb    ON plex_items (guid_imdb);
CREATE INDEX IF NOT EXISTS idx_plex_items_tmdb    ON plex_items (guid_tmdb);
CREATE INDEX IF NOT EXISTS idx_plex_items_title   ON plex_items (title COLLATE NOCASE);
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

CREATE TABLE IF NOT EXISTS pending_updates (
    media_type        TEXT NOT NULL,
    tmdb_id           INTEGER NOT NULL,
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
                        CHECK (kind IN ('upstream_changed', 'urls_match')),
    PRIMARY KEY (media_type, tmdb_id),
    FOREIGN KEY (media_type, tmdb_id) REFERENCES themes (media_type, tmdb_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_pending_updates_decision
    ON pending_updates (decision, detected_at);

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
    media_type   TEXT NOT NULL CHECK (media_type IN ('movie', 'tv')),
    tmdb_id      INTEGER NOT NULL,
    section_id   TEXT NOT NULL DEFAULT '',
    youtube_url  TEXT NOT NULL,
    kind         TEXT NOT NULL CHECK (kind IN ('user', 'themerrdb')),
    captured_at  TEXT NOT NULL,
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
"""

CURRENT_SCHEMA_VERSION = 30


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

    conn.execute("PRAGMA foreign_keys = OFF")
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
    conn.executescript(
        """
        ALTER TABLE pending_updates ADD COLUMN kind TEXT
            NOT NULL DEFAULT 'upstream_changed'
            CHECK (kind IN ('upstream_changed', 'urls_match'));
        """
    )


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
    conn.executescript(
        """
        CREATE TABLE audit_events (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            occurred_at  TEXT NOT NULL,
            actor        TEXT NOT NULL,
            action       TEXT NOT NULL,
            media_type   TEXT,
            tmdb_id      INTEGER,
            section_id   TEXT,
            details      TEXT
        );
        CREATE INDEX idx_audit_events_target
            ON audit_events (media_type, tmdb_id, occurred_at DESC);
        CREATE INDEX idx_audit_events_occurred
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

        INSERT OR IGNORE INTO previous_urls
            (media_type, tmdb_id, section_id, youtube_url, kind, captured_at)
        SELECT media_type, tmdb_id, '',
               previous_youtube_url, previous_youtube_kind,
               COALESCE(youtube_edited_at, last_seen_sync_at)
          FROM themes
         WHERE previous_youtube_url IS NOT NULL
           AND previous_youtube_kind IS NOT NULL;

        ALTER TABLE themes DROP COLUMN previous_youtube_url;
        ALTER TABLE themes DROP COLUMN previous_youtube_kind;
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
    conn.executescript(
        """
        ALTER TABLE events ADD COLUMN section_id TEXT;
        CREATE INDEX IF NOT EXISTS idx_events_item_section
            ON events (media_type, tmdb_id, section_id, ts DESC);
        """
    )


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
    conn.executescript(
        "ALTER TABLE pending_updates ADD COLUMN replaced_user_url TEXT;"
    )


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
    conn.executescript(
        "ALTER TABLE jobs ADD COLUMN acked_at TEXT;"
    )


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
    conn.executescript(
        "ALTER TABLE local_files ADD COLUMN mismatch_state TEXT;"
    )


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
    for row in conn.execute(
        "SELECT id, title FROM themes WHERE title_norm IS NULL"
    ).fetchall():
        try:
            tn = normalize_title(row[1] or "")
        except Exception:
            tn = (row[1] or "").lower()
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
        conn.execute(
            "UPDATE plex_items SET title_norm = ? WHERE rating_key = ?",
            (tn, row[0]),
        )


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
