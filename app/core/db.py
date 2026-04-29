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
    media_type           TEXT NOT NULL CHECK (media_type IN ('movie', 'tv')),
    tmdb_id              INTEGER NOT NULL,
    imdb_id              TEXT,
    title                TEXT NOT NULL,
    original_title       TEXT,
    year                 TEXT,
    release_date         TEXT,
    youtube_url          TEXT,
    youtube_video_id     TEXT,
    youtube_added_at     TEXT,
    youtube_edited_at    TEXT,
    upstream_source      TEXT NOT NULL CHECK (upstream_source IN ('imdb', 'themoviedb')),
    raw_json             TEXT,
    last_seen_sync_at    TEXT NOT NULL,
    first_seen_sync_at   TEXT NOT NULL,
    failure_kind         TEXT,
    failure_message      TEXT,
    failure_at           TEXT,
    PRIMARY KEY (media_type, tmdb_id)
);

CREATE INDEX IF NOT EXISTS idx_themes_imdb ON themes (imdb_id) WHERE imdb_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_themes_video ON themes (youtube_video_id);
CREATE INDEX IF NOT EXISTS idx_themes_title ON themes (title);
CREATE INDEX IF NOT EXISTS idx_themes_failure ON themes (failure_kind) WHERE failure_kind IS NOT NULL;

CREATE TABLE IF NOT EXISTS local_files (
    media_type      TEXT NOT NULL,
    tmdb_id         INTEGER NOT NULL,
    file_path       TEXT NOT NULL,
    file_sha256     TEXT,
    file_size       INTEGER,
    downloaded_at   TEXT NOT NULL,
    source_video_id TEXT NOT NULL,
    provenance      TEXT NOT NULL DEFAULT 'auto'
                       CHECK (provenance IN ('auto', 'manual')),
    PRIMARY KEY (media_type, tmdb_id),
    FOREIGN KEY (media_type, tmdb_id) REFERENCES themes (media_type, tmdb_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS placements (
    media_type      TEXT NOT NULL,
    tmdb_id         INTEGER NOT NULL,
    media_folder    TEXT NOT NULL,
    placed_at       TEXT NOT NULL,
    placement_kind  TEXT NOT NULL CHECK (placement_kind IN ('hardlink', 'copy', 'symlink')),
    plex_rating_key TEXT,
    plex_refreshed  INTEGER NOT NULL DEFAULT 0,
    provenance      TEXT NOT NULL DEFAULT 'auto'
                       CHECK (provenance IN ('auto', 'manual', 'cloud')),
    PRIMARY KEY (media_type, tmdb_id, media_folder),
    FOREIGN KEY (media_type, tmdb_id) REFERENCES themes (media_type, tmdb_id) ON DELETE CASCADE
);

-- Download / placement / sync queue. Workers consume from here.
CREATE TABLE IF NOT EXISTS jobs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    job_type        TEXT NOT NULL CHECK (job_type IN ('sync', 'download', 'place', 'refresh', 'relink')),
    media_type      TEXT,
    tmdb_id         INTEGER,
    payload         TEXT,                    -- JSON
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

CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs (status, next_run_at);
CREATE INDEX IF NOT EXISTS idx_jobs_type ON jobs (job_type, status);

CREATE TABLE IF NOT EXISTS events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          TEXT NOT NULL,
    level       TEXT NOT NULL,
    component   TEXT NOT NULL,
    media_type  TEXT,
    tmdb_id     INTEGER,
    message     TEXT NOT NULL,
    detail      TEXT
);

CREATE INDEX IF NOT EXISTS idx_events_ts ON events (ts DESC);
CREATE INDEX IF NOT EXISTS idx_events_item ON events (media_type, tmdb_id, ts DESC);

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
    youtube_url     TEXT NOT NULL,
    set_at          TEXT NOT NULL,
    set_by          TEXT,
    note            TEXT,
    PRIMARY KEY (media_type, tmdb_id)
);

CREATE TABLE IF NOT EXISTS plex_sections (
    section_id      TEXT PRIMARY KEY,
    uuid            TEXT,
    title           TEXT NOT NULL,
    type            TEXT NOT NULL CHECK (type IN ('movie', 'show')),
    agent           TEXT,
    language        TEXT,
    location_paths  TEXT,
    included        INTEGER NOT NULL DEFAULT 1,
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
    decision          TEXT,
    decision_at       TEXT,
    decision_by       TEXT,
    PRIMARY KEY (media_type, tmdb_id),
    FOREIGN KEY (media_type, tmdb_id) REFERENCES themes (media_type, tmdb_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_pending_updates_decision
    ON pending_updates (decision, detected_at);

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
"""

CURRENT_SCHEMA_VERSION = 4


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
    """Wrap a block in a BEGIN IMMEDIATE...COMMIT."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
