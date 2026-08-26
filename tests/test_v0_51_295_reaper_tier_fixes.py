"""v0.51.295 — holistic review wave 4: reaper/tier classification fixes.

Five confirmed findings:
  1. still_p suppression tested bare has_theme=1 — a sibling whose theme
     claim motif already HEAD-verified dead (phantom-P) suppressed the
     entire theme-lost pipeline. Now carries the repo-canonical
     COALESCE(plex_theme_verified_ok, 1) = 1 qualifier.
  2. The backup_only tier-1 arm filtered source_kind != 'plex_cloud'
     without COALESCE — a NULL source_kind row matched NO tier-1 arm
     (NULL != x yields no row) while matching tier-3's COALESCE'd arm,
     mis-tiering the loss into other_fallback silence.
  3. _section_enum_overdue read the section-wide MAX(last_seen_at) — a
     partial walk (collections-only / items-only) refreshed the signal
     and indefinitely deferred the 24h bypass the OTHER cohort's stale
     rows need. Now per-cohort: either stale cohort trips it.
  4. resolve_edition_swap carried a STAGED BACKUP to the surviving
     edition and enqueued a place job — auto-deploying a notify-only
     backup and skipping the theme_lost_backup_ready PROMOTE TO ACTIVE
     notification. The resolver now declines backup-intent rows.
  5. enrich_item's local_files lookup ignored its section/edition args
     (bare LIMIT 1, no ORDER BY) — notifications carried an arbitrary
     sibling's source_kind/source_video_id on multi-section titles.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
NOW = "2026-08-26T00:00:00+00:00"
SEC = "1"


@pytest.fixture
def env(tmp_path, monkeypatch):
    from app.core.db import get_conn, init_db, transaction
    themes = tmp_path / "data" / "themes"
    themes.mkdir(parents=True)
    (tmp_path / "motif.yaml").write_text(f"paths:\n  themes_dir: {themes}\n")
    db = tmp_path / "motif.db"
    init_db(db)
    with get_conn(db) as conn, transaction(conn):
        conn.execute(
            """INSERT INTO plex_sections (section_id, title, type, is_anime,
                 is_4k, themes_subdir, included, discovered_at, last_seen_at)
               VALUES (?, 'Movies', 'movie', 0, 0, 'movies', 1, ?, ?)""",
            (SEC, NOW, NOW))
    import app.config as cfg
    real = cfg.Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    monkeypatch.setattr(cfg, "Settings", lambda *a, **kw: real)
    calls: list = []
    import app.core.notify as notify_mod
    monkeypatch.setattr(notify_mod, "dispatch",
                        lambda *a, **k: calls.append(k.get("event_kind")))
    monkeypatch.setattr(notify_mod, "dispatch_coalesced",
                        lambda *a, **k: calls.append(k.get("event_kind")))
    return db, themes, calls


def _seed_title(db, tid, title):
    from app.core.db import get_conn, transaction
    with get_conn(db) as conn, transaction(conn):
        conn.execute(
            """INSERT INTO themes (media_type, tmdb_id, title, year,
                 upstream_source, last_seen_sync_at, first_seen_sync_at)
               VALUES ('movie', ?, ?, '2020', 'themoviedb', ?, ?)""",
            (tid, title, NOW, NOW))


def _seed_item(db, rk, tid, title, *, has_theme=1, verified_ok=None,
               edition="", folder=None):
    from app.core.db import get_conn, transaction
    with get_conn(db) as conn, transaction(conn):
        conn.execute(
            """INSERT INTO plex_items (rating_key, section_id, media_type,
                 title, year, guid_tmdb, edition_key, folder_path, has_theme,
                 plex_theme_verified_ok, first_seen_at, last_seen_at)
               VALUES (?, ?, 'movie', ?, '2020', ?, ?, ?, ?, ?, ?, ?)""",
            (rk, SEC, title, tid, edition,
             folder or f"/data/movies/{title}", has_theme, verified_ok,
             NOW, NOW))


def _live(rk, tid, title, edition="", has_theme=False):
    from app.core.plex import PlexLibraryItem
    return PlexLibraryItem(
        rating_key=rk, section_id=SEC, media_type="movie", title=title,
        year="2020", guid_imdb=None, guid_tmdb=tid, guid_tvdb=None,
        folder_path=f"/data/movies/{title}", has_theme=has_theme)


def _set_verified(db, rk, val):
    from app.core.db import get_conn, transaction
    with get_conn(db) as conn, transaction(conn):
        conn.execute("UPDATE plex_items SET plex_theme_verified_ok = ? "
                     "WHERE rating_key = ?", (val, rk))


def _drive(db, items):
    from app.core.plex_enum import _upsert_items
    _upsert_items(db, items, section_id=SEC)
    _upsert_items(db, items, section_id=SEC)


# ── 1. phantom-P siblings must not suppress the loss ─────────


def test_head_dead_sibling_does_not_suppress_theme_lost(env):
    db, _themes, calls = env
    _seed_title(db, 295001, "Alpha")
    # the row Plex will drop (was serving its own theme)
    _seed_item(db, "rk-lost", 295001, "Alpha", has_theme=1)
    # a sibling that CLAIMS a theme but motif HEAD-verified the claim dead.
    _seed_item(db, "rk-phantom", 295001, "Alpha Phantom", has_theme=1,
               verified_ok=0, folder="/data/movies/Alpha Phantom")
    from app.core.plex_enum import _upsert_items
    sibling = _live("rk-phantom", 295001, "Alpha Phantom", has_theme=True)
    _upsert_items(db, [sibling], section_id=SEC)      # miss #1 (grace)
    _set_verified(db, "rk-phantom", 0)                # HEAD-verified dead
    _upsert_items(db, [sibling], section_id=SEC)      # miss #2 → reap
    assert "plex_theme_lost" in calls, (
        "a phantom-P sibling (verified_ok=0) suppressed the entire "
        "theme-lost pipeline pre-fix")


def test_live_verified_sibling_still_suppresses(env):
    db, _themes, calls = env
    _seed_title(db, 295002, "Beta")
    _seed_item(db, "rk-lost2", 295002, "Beta", has_theme=1)
    _seed_item(db, "rk-live2", 295002, "Beta Live", has_theme=1,
               verified_ok=1, folder="/data/movies/Beta Live")
    _drive(db, [_live("rk-live2", 295002, "Beta Live", has_theme=True)])
    assert "plex_theme_lost" not in calls, (
        "a genuinely-serving sibling keeps the historical suppression")


# ── 2. NULL source_kind backup stamps classify tier-1 ────────


def test_null_source_kind_backup_only_is_backup_ready(env):
    from app.core.db import get_conn, transaction
    db, themes, calls = env
    _seed_title(db, 295003, "Gamma")
    _seed_item(db, "rk-g", 295003, "Gamma", has_theme=1)
    rel = "movies/Gamma/theme.mp3"
    (themes / "movies" / "Gamma").mkdir(parents=True)
    (themes / rel).write_bytes(b"BK")
    with get_conn(db) as conn, transaction(conn):
        conn.execute(
            """INSERT INTO local_files (media_type, tmdb_id, section_id,
                 edition_key, file_path, file_sha256, file_size,
                 downloaded_at, source_video_id, provenance, source_kind,
                 last_place_attempt_reason)
               VALUES ('movie', ?, ?, '', ?, 'sha', 2, ?, 'v', 'auto',
                       NULL, 'backup_only')""",
            (295003, SEC, rel, NOW))
    _seed_title(db, 295999, "Bystander")
    _drive(db, [_live("rk-alive", 295999, "Bystander")])
    assert "theme_lost_backup_ready" in calls, (
        "NULL source_kind failed the un-COALESCE'd tier-1 arm and the loss "
        "mis-tiered to other_fallback silence pre-fix")


# ── 3. per-cohort enum freshness ─────────────────────────────


def test_partial_walk_does_not_mask_the_other_cohorts_staleness(env):
    from app.core.db import get_conn, transaction
    from app.core.plex_enum import _section_enum_overdue
    db, _themes, _calls = env
    stale = "2026-08-20T00:00:00+00:00"
    with get_conn(db) as conn, transaction(conn):
        conn.execute(
            """INSERT INTO plex_items (rating_key, section_id, media_type,
                 title, guid_tmdb, has_theme, first_seen_at, last_seen_at)
               VALUES ('rk-m', ?, 'movie', 'Old Movie', 295500, 0, ?, ?)""",
            (SEC, stale, stale))
        conn.execute(
            """INSERT INTO plex_items (rating_key, section_id, media_type,
                 title, has_theme, first_seen_at, last_seen_at)
               VALUES ('rk-c', ?, 'collection', 'Fresh Coll', 0, ?, ?)""",
            (SEC, NOW, NOW))
    assert _section_enum_overdue(db, SEC, hours=24) is True, (
        "a collections-only walk refreshed the section-wide MAX and "
        "deferred the items cohort's 24h bypass indefinitely pre-fix")


# ── 4. a staged backup never auto-deploys via the swap ───────


def test_edition_swap_declines_a_staged_backup(env):
    from app.core.db import get_conn, transaction
    db, themes, calls = env
    _seed_title(db, 295004, "Delta")
    # lost EXTENDED edition (was serving) + surviving unthemed STANDARD.
    _seed_item(db, "rk-ext", 295004, "Delta", has_theme=1,
               edition="extended",
               folder="/data/movies/Delta (2020) {edition-Extended}")
    rel = "movies/Delta (2020) {edition-extended}/theme.mp3"
    (themes / "movies" / "Delta (2020) {edition-extended}").mkdir(parents=True)
    (themes / rel).write_bytes(b"STAGED")
    with get_conn(db) as conn, transaction(conn):
        conn.execute(
            """INSERT INTO local_files (media_type, tmdb_id, section_id,
                 edition_key, file_path, file_sha256, file_size,
                 downloaded_at, source_video_id, provenance, source_kind,
                 last_place_attempt_reason)
               VALUES ('movie', ?, ?, 'extended', ?, 'sha', 6, ?, 'v',
                       'auto', 'plex_cloud', 'backup_only')""",
            (295004, SEC, rel, NOW))
    survivor = _live("rk-std", 295004, "Delta (2020)")
    _drive(db, [survivor])
    with get_conn(db) as conn:
        lf = conn.execute(
            "SELECT COALESCE(edition_key,'') e FROM local_files "
            " WHERE tmdb_id = 295004").fetchone()
        jobs = conn.execute("SELECT job_type FROM jobs").fetchall()
    assert lf["e"] == "extended", (
        "the staged backup was re-keyed to the survivor pre-fix")
    assert jobs == [], "no place job — a staged backup never auto-deploys"
    assert "theme_lost_backup_ready" in calls, (
        "the tier-1 PROMOTE TO ACTIVE notification is the contract")


# ── 5. enrich_item honors its scope args ─────────────────────


def test_enrich_item_prefers_the_named_section_row(env):
    from app.core.db import get_conn, transaction
    from app.core.notify_content import enrich_item
    db, _themes, _calls = env
    _seed_title(db, 295005, "Epsilon")
    with get_conn(db) as conn, transaction(conn):
        conn.execute(
            """INSERT INTO plex_sections (section_id, title, type, is_anime,
                 is_4k, themes_subdir, included, discovered_at, last_seen_at)
               VALUES ('18', 'Movies 4K', 'movie', 0, 1, 'movies-4k', 1,
                       ?, ?)""", (NOW, NOW))
        for sec, vid in ((SEC, "vidSTANDARD"), ("18", "vid4K0000001")):
            conn.execute(
                """INSERT INTO local_files (media_type, tmdb_id, section_id,
                     edition_key, file_path, file_sha256, file_size,
                     downloaded_at, source_video_id, provenance, source_kind)
                   VALUES ('movie', ?, ?, '', 'p', 's', 1, ?, ?, 'auto',
                           'themerrdb')""",
                (295005, sec, NOW, vid))
    ctx = enrich_item(db, media_type="movie", tmdb_id=295005,
                      section_id="18")
    assert ctx.get("youtube_video_id") == "vid4K0000001", (
        "the bare LIMIT 1 handed notifications an arbitrary sibling's "
        "source_video_id pre-fix")


def test_v0_51_295_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert "0.51.295: " in init_py
