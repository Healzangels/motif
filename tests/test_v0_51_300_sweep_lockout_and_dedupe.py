"""v0.51.300 — holistic round 2, wave 9: sweep lockouts + notify dedupe.

Three confirmed findings:
  1. _restore_lost_placements claimed its gating "mirrors
     _retry_pending_placements" but never mirrored the v1.18.94
     plex_rejected 2-failure/24h lockout — a rejected re-PUSH re-enqueued
     EVERY HOUR forever (fresh unacked failed job + FAIL-dot relight).
  2. The retry sweep's lockout COUNT had no edition scope (the v1.22.87
     dedup did) — one edition's failures locked out its sibling after a
     single failure.
  3. The in-place backup_ready_to_deploy dispatch had NO rate limit
     (the reaper tiers key a 24h dedupe) — every enum re-fired it for
     every staged row; it now dedupes 24h per row and routes through the
     v0.51.288 digest coalescer (a Plex-Pass lapse digests, not fans out).
  Plus: the restore sweep's two silent re-stat skip branches now log
  aggregated INFO (the v1.18.7 cold-path rule).
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
NOW = "2026-08-26T12:00:00+00:00"
SEC = "1"


def _iso_minus_hours(h):
    from datetime import datetime, timedelta, timezone
    return (datetime(2026, 8, 26, 12, tzinfo=timezone.utc)
            - timedelta(hours=h)).isoformat()


def _seed_base(db):
    from app.core.db import get_conn, init_db, transaction
    init_db(db)
    with get_conn(db) as conn, transaction(conn):
        conn.execute(
            """INSERT INTO plex_sections (section_id, title, type, is_anime,
                 is_4k, themes_subdir, included, discovered_at, last_seen_at)
               VALUES (?, 'Movies', 'movie', 0, 0, 'movies', 1, ?, ?)""",
            (SEC, NOW, NOW))


def _failed_job(conn, tmdb_id, edition, hours_ago):
    import json
    conn.execute(
        "INSERT INTO jobs (job_type, status, media_type, tmdb_id, section_id,"
        " payload, created_at, finished_at, last_error)"
        " VALUES ('place', 'failed', 'movie', ?, ?, ?, ?, ?, 'rejected')",
        (tmdb_id, SEC,
         json.dumps({"edition_key": edition}) if edition else "{}",
         _iso_minus_hours(hours_ago), _iso_minus_hours(hours_ago)))


# ── 2. edition-scoped lockout in the retry sweep ─────────────


def test_sibling_edition_failures_do_not_cross_lock(tmp_path):
    from app.core.db import get_conn, transaction
    from app.core.scheduler import _retry_pending_placements
    db = tmp_path / "motif.db"
    _seed_base(db)
    with get_conn(db) as conn, transaction(conn):
        conn.execute(
            """INSERT INTO themes (media_type, tmdb_id, title,
                 upstream_source, last_seen_sync_at, first_seen_sync_at)
               VALUES ('movie', 300001, 'T', 'imdb', ?, ?)""", (NOW, NOW))
        for edn in ("", "extended"):
            conn.execute(
                """INSERT INTO local_files (media_type, tmdb_id, section_id,
                     edition_key, file_path, file_sha256, file_size,
                     downloaded_at, source_video_id, provenance, source_kind,
                     last_place_attempt_at, last_place_attempt_reason)
                   VALUES ('movie', 300001, ?, ?, 'x.mp3', 's', 1, ?, 'v',
                           'auto', 'themerrdb', ?, 'plex_rejected:HTTP_500')""",
                (SEC, edn, NOW, NOW))
        # edition A ('') failed TWICE in 24h -> locked; B once -> retryable.
        _failed_job(conn, 300001, "", 2)
        _failed_job(conn, 300001, "", 4)
        _failed_job(conn, 300001, "extended", 3)
    out = _retry_pending_placements(db, dry_run=True)
    assert out["candidates"] == 1, (
        f"edition B (one failure) must stay auto-retryable — pre-fix A's "
        f"two failures inflated B's count and both locked: {out}")


def test_two_own_failures_still_lock(tmp_path):
    from app.core.db import get_conn, transaction
    from app.core.scheduler import _retry_pending_placements
    db = tmp_path / "motif.db"
    _seed_base(db)
    with get_conn(db) as conn, transaction(conn):
        conn.execute(
            """INSERT INTO themes (media_type, tmdb_id, title,
                 upstream_source, last_seen_sync_at, first_seen_sync_at)
               VALUES ('movie', 300002, 'U', 'imdb', ?, ?)""", (NOW, NOW))
        conn.execute(
            """INSERT INTO local_files (media_type, tmdb_id, section_id,
                 edition_key, file_path, file_sha256, file_size,
                 downloaded_at, source_video_id, provenance, source_kind,
                 last_place_attempt_at, last_place_attempt_reason)
               VALUES ('movie', 300002, ?, '', 'y.mp3', 's', 1, ?, 'v',
                       'auto', 'themerrdb', ?, 'plex_rejected:HTTP_500')""",
            (SEC, NOW, NOW))
        _failed_job(conn, 300002, "", 2)
        _failed_job(conn, 300002, "", 4)
    out = _retry_pending_placements(db, dry_run=True)
    assert out["candidates"] == 0, "the v1.18.94 lockout itself must hold"


# ── 1. the restore sweep honors the same lockout ─────────────


@pytest.fixture
def restore_env(tmp_path, monkeypatch):
    from app.core.db import get_conn, transaction
    import app.config as cfg
    themes = tmp_path / "data" / "themes"
    (themes / "movies" / "T (2020)").mkdir(parents=True)
    media = tmp_path / "media" / "T (2020)"
    media.mkdir(parents=True)
    (tmp_path / "motif.yaml").write_text(f"paths:\n  themes_dir: {themes}\n")
    db = tmp_path / "motif.db"
    _seed_base(db)
    rel = "movies/T (2020)/theme.mp3"
    (themes / rel).write_bytes(b"CANON")
    with get_conn(db) as conn, transaction(conn):
        conn.execute(
            """INSERT INTO themes (media_type, tmdb_id, title,
                 upstream_source, last_seen_sync_at, first_seen_sync_at)
               VALUES ('movie', 300003, 'T', 'imdb', ?, ?)""", (NOW, NOW))
        conn.execute(
            """INSERT INTO local_files (media_type, tmdb_id, section_id,
                 edition_key, file_path, file_sha256, file_size,
                 downloaded_at, source_video_id, provenance, source_kind,
                 canonical_present, last_place_attempt_at,
                 last_place_attempt_reason)
               VALUES ('movie', 300003, ?, '', ?, 's', 5, ?, 'v', 'auto',
                       'themerrdb', 1, ?, 'plex_rejected:HTTP_500')""",
            (SEC, rel, NOW, NOW))
        conn.execute(
            """INSERT INTO placements (media_type, tmdb_id, section_id,
                 edition_key, media_folder, placed_at, placement_kind,
                 theme_present)
               VALUES ('movie', 300003, ?, '', ?, ?, 'hardlink', 0)""",
            (SEC, str(media), NOW))
        conn.execute(
            """INSERT INTO plex_items (rating_key, section_id, media_type,
                 title, guid_tmdb, folder_path, has_theme, first_seen_at,
                 last_seen_at)
               VALUES ('rk-r', ?, 'movie', 'T', 300003, ?, 0, ?, ?)""",
            (SEC, str(media), NOW, NOW))
    real = cfg.Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    monkeypatch.setattr(cfg, "Settings", lambda *a, **kw: real)
    return db, real


def _jobs(db):
    with sqlite3.connect(db) as conn:
        return conn.execute(
            "SELECT COUNT(*) FROM jobs WHERE job_type='place' "
            "AND status='pending'").fetchone()[0]


def test_restore_sweep_honors_the_rejection_lockout(restore_env):
    from app.core.db import get_conn, transaction
    from app.core.scheduler import _restore_lost_placements
    db, settings = restore_env
    with get_conn(db) as conn, transaction(conn):
        _failed_job(conn, 300003, "", 2)
        _failed_job(conn, 300003, "", 4)
    _restore_lost_placements(settings)
    assert _jobs(db) == 0, (
        "two plex_rejected failures in 24h must lock the restore sweep out "
        "— pre-fix it re-enqueued a doomed place job EVERY HOUR forever")


def test_restore_sweep_still_enqueues_below_the_lockout(restore_env):
    from app.core.db import get_conn, transaction
    from app.core.scheduler import _restore_lost_placements
    db, settings = restore_env
    with get_conn(db) as conn, transaction(conn):
        _failed_job(conn, 300003, "", 2)     # ONE failure — retryable
    _restore_lost_placements(settings)
    assert _jobs(db) == 1, "a single failure stays auto-retryable"


# ── 3. backup-ready dedupe + digest routing ──────────────────


def test_backup_ready_transition_dedupes_24h(tmp_path, monkeypatch):
    from app.core.db import get_conn, transaction
    from app.core.plex_enum import _upsert_items
    from app.core.plex import PlexLibraryItem
    import app.config as cfg
    import app.core.notify as notify_mod
    themes = tmp_path / "data" / "themes"
    themes.mkdir(parents=True)
    (tmp_path / "motif.yaml").write_text(f"paths:\n  themes_dir: {themes}\n")
    db = tmp_path / "motif.db"
    _seed_base(db)
    with get_conn(db) as conn, transaction(conn):
        conn.execute(
            """INSERT INTO themes (media_type, tmdb_id, title,
                 upstream_source, last_seen_sync_at, first_seen_sync_at)
               VALUES ('movie', 300004, 'B', 'imdb', ?, ?)""", (NOW, NOW))
        conn.execute(
            """INSERT INTO plex_items (rating_key, section_id, media_type,
                 title, guid_tmdb, folder_path, has_theme, first_seen_at,
                 last_seen_at)
               VALUES ('rk-b', ?, 'movie', 'B', 300004, '/data/movies/B',
                       1, ?, ?)""", (SEC, NOW, NOW))
        conn.execute(
            """INSERT INTO user_overrides (media_type, tmdb_id, section_id,
                 youtube_url, intent, set_at)
               VALUES ('movie', 300004, ?, 'https://y/w', 'backup', ?)""",
            (SEC, NOW))
    real = cfg.Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    monkeypatch.setattr(cfg, "Settings", lambda *a, **kw: real)
    calls: list = []
    monkeypatch.setattr(notify_mod, "dispatch_coalesced",
                        lambda *a, **k: calls.append(k))
    monkeypatch.setattr(notify_mod, "dispatch",
                        lambda *a, **k: calls.append(k))
    item = PlexLibraryItem(
        rating_key="rk-b", section_id=SEC, media_type="movie", title="B",
        year=None, guid_imdb=None, guid_tmdb=300004, guid_tvdb=None,
        folder_path="/data/movies/B", has_theme=False)
    _upsert_items(db, [item], section_id=SEC)          # 1→0 transition
    brd = [c for c in calls
           if c.get("event_kind") == "backup_ready_to_deploy"]
    assert len(brd) == 1, "the transition fires once"
    assert brd[0].get("bulk") is True, (
        "the coalescer route — a mass transition must digest")
    # re-arm has_theme and transition AGAIN inside the 24h window.
    with get_conn(db) as conn, transaction(conn):
        conn.execute("UPDATE plex_items SET has_theme = 1 "
                     "WHERE rating_key = 'rk-b'")
    _upsert_items(db, [item], section_id=SEC)
    brd2 = [c for c in calls
            if c.get("event_kind") == "backup_ready_to_deploy"]
    assert len(brd2) == 1, (
        "pre-fix every enum re-fired the dispatch for every staged row — "
        "the 24h dedupe must swallow the repeat")


def test_v0_51_300_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert "0.51.300: " in init_py
