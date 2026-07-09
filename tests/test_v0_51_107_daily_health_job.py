"""v0.51.107 — daily FULL-TABLE health passes, decoupled from the enum.

Code-review findings #2 + #3 of the v0.51.101 section-scoping change: both
verify_*_health passes were moved INSIDE run_plex_enum's per-section scope, so
  - the plex_upload staleness 0-stamp (the RE-PUSH detector) — documented to
    "run unconditionally" — is skipped ENTIRELY on a no-work enum (every section
    delta-gated), so a Plex delete+re-add that destroys an uploaded rating_key
    goes undetected until some section next changes; and
  - with both auto_enum toggles off NO enum fires at all, so canonical +
    placement health never re-stamp and a broken row lags indefinitely.

v0.51.107 adds _daily_health_passes_job — a table-wide (section_ids=None) run of
both passes on a daily 03:25-UTC scheduler slot, regardless of enum config.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from app.core.db import init_db
from app.core.scheduler import _daily_health_passes_job

REPO = Path(__file__).resolve().parent.parent
SCHED_SRC = (REPO / "app" / "core" / "scheduler.py").read_text()
NOW = datetime.now(timezone.utc).isoformat(timespec="seconds")


def _settings(tmp_path, *, themes_dir=None):
    from app.config import Settings
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    if themes_dir is not None:
        s._cfg.paths.themes_dir = str(themes_dir)
    return s


def _theme(conn, tmdb):
    conn.execute(
        "INSERT INTO themes (media_type, tmdb_id, title, upstream_source,"
        " last_seen_sync_at, first_seen_sync_at) VALUES ('movie', ?, ?, 'imdb', ?, ?)",
        (tmdb, f"T{tmdb}", NOW, NOW))


def _section(conn):
    if not conn.execute(
            "SELECT 1 FROM plex_sections WHERE section_id='1'").fetchone():
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime, is_4k,"
            " themes_subdir, included, discovered_at, last_seen_at)"
            " VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))


def test_daily_job_runs_placement_health_table_wide(tmp_path):
    # No enum in play. Seed a STALE plex_upload placement (its plex_rating_key
    # is not a live plex_items row) plus a live plex_items row so the empty-guard
    # passes, and a genuinely-broken sidecar (live folder, no theme.mp3). The
    # daily job must re-stamp both table-wide.
    s = _settings(tmp_path)
    init_db(s.db_path)
    broken_dir = tmp_path / "broken"
    broken_dir.mkdir()  # folder alive, no theme.mp3
    with sqlite3.connect(s.db_path) as conn:
        _section(conn)
        # a LIVE plex item (rk 1) so the plex_upload empty-guard doesn't trip
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type,"
            " guid_tmdb, title, edition_key, has_theme, folder_path,"
            " first_seen_at, last_seen_at)"
            " VALUES ('1','1','movie',1,'Live','',1,?,?,?)",
            (str(tmp_path / "live"), NOW, NOW))
        # STALE plex_upload: uploaded to rk 999 which no longer exists
        _theme(conn, 10)
        conn.execute(
            "INSERT INTO placements (media_type, tmdb_id, section_id, media_folder,"
            " placed_at, placement_kind, provenance, edition_key, plex_rating_key)"
            " VALUES ('movie', 10, '1', '', ?, 'plex_upload', 'auto', '', '999')",
            (NOW,))
        # a genuinely-broken sidecar
        _theme(conn, 20)
        conn.execute(
            "INSERT INTO placements (media_type, tmdb_id, section_id, media_folder,"
            " placed_at, placement_kind, provenance, edition_key)"
            " VALUES ('movie', 20, '1', ?, ?, 'hardlink', 'auto', '')",
            (str(broken_dir), NOW))
        conn.commit()

    _daily_health_passes_job(s)

    with sqlite3.connect(s.db_path) as conn:
        flags = dict(conn.execute(
            "SELECT tmdb_id, theme_present FROM placements").fetchall())
    assert flags[10] == 0, "stale plex_upload rk re-stamped → RE-PUSH (finding #2)"
    assert flags[20] == 0, "broken sidecar re-stamped table-wide (finding #3)"


def test_daily_job_runs_canonical_health_when_themes_dir_set(tmp_path):
    # themes_dir root alive but the canonical theme.mp3 is missing under it →
    # the daily canonical pass must stamp canonical_present=0 (finding #3).
    themes = tmp_path / "themes"
    themes.mkdir()
    s = _settings(tmp_path, themes_dir=themes)
    init_db(s.db_path)
    with sqlite3.connect(s.db_path) as conn:
        _section(conn)
        conn.execute(
            "INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source,"
            " last_seen_sync_at, first_seen_sync_at, youtube_url)"
            " VALUES (1,'movie',30,'X','imdb',?,?,'u')", (NOW, NOW))
        conn.execute(
            "INSERT INTO local_files (media_type, tmdb_id, section_id, theme_id,"
            " file_path, downloaded_at, source_video_id, provenance, source_kind,"
            " edition_key) VALUES ('movie', 30, '1', 1, 'movies/30/theme.mp3', ?,"
            " 'V', 'auto', 'themerrdb', '')", (NOW,))
        conn.commit()

    _daily_health_passes_job(s)

    with sqlite3.connect(s.db_path) as conn:
        cp = conn.execute(
            "SELECT canonical_present FROM local_files WHERE tmdb_id=30"
        ).fetchone()[0]
    assert cp == 0, "missing canonical re-stamped table-wide by the daily job"


def test_daily_job_no_themes_dir_skips_canonical_cleanly(tmp_path):
    # themes_dir unset (first-run) → the canonical branch returns early; the
    # placement pass still runs and the job must not raise.
    s = _settings(tmp_path)  # themes_dir=None
    init_db(s.db_path)
    _daily_health_passes_job(s)  # no rows → clean no-op, no exception


def test_daily_health_job_registered():
    assert "def _daily_health_passes_job(" in SCHED_SRC
    assert "verify_placement_health" in SCHED_SRC
    assert "verify_canonical_health" in SCHED_SRC
    assert 'id="daily_health_passes"' in SCHED_SRC
    # table-wide: the call sites pass NO section_ids (default None → full table).
    assert "verify_placement_health(db_path)" in SCHED_SRC
    assert "verify_canonical_health(db_path, themes_dir)" in SCHED_SRC
    # slotted at 03:25 UTC after the .motif-tmp sweep (03:20).
    assert 'minute="25", hour="3"' in SCHED_SRC