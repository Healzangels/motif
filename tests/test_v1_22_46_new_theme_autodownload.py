"""v1.22.46 — is_new SRC=— auto-download enqueues during the sync.

the user's Tires (2024) repro: AUTO-DOWNLOAD NEW THEMES (UNTHEMED ROWS) was ON,
ThemerrDB synced a NEW theme for an unthemed in-Plex TV show, yet it never
downloaded (motif added = —).

Root cause: _enqueue_download's section-ownership query (the v1.15.142 anime-cohort
rewrite) matched ONLY via the pi.theme_id linkage (`WHERE t.tmdb_id = ?`). But
during a sync's batch processing a BRAND-NEW theme's pi.theme_id is not linked
yet — run_sync calls resolve_theme_ids AFTER the batches — so t.tmdb_id is NULL
on every plex_item, the query returns 0 sections, and the toggle-ON auto-
download silently enqueued nothing. The post-sync enum then missed it too (the
sync's own resolve pre-linked theme_id, so the row wasn't "newly_linked").

Fix: match `pi.guid_tmdb OR the theme_id linkage` — the canonical pattern used
by in_plex / has_sidecar. guid_tmdb works before theme_id is linked (new theme);
theme_id still covers anime (guid_tmdb NULL). This file reproduces the bug
behaviorally and locks the fix.
"""
from __future__ import annotations

import tempfile
from pathlib import Path

from app.core import sync
from app.core.db import get_conn, init_db

_NOW = "2026-06-09T09:00:00"
REPO = Path(__file__).resolve().parent.parent


def _fresh_db():
    d = Path(tempfile.mkdtemp()) / "motif.db"
    init_db(d)
    return d


def _seed_section(conn, sid="s_tv", title="TV Shows", typ="show",
                  anime=0, fourk=0):
    conn.execute(
        "INSERT INTO plex_sections (section_id,title,type,included,is_anime,"
        " is_4k,themes_subdir,discovered_at,last_seen_at) "
        "VALUES (?,?,?,1,?,?,?,?,?)",
        (sid, title, typ, anime, fourk, sid, _NOW, _NOW))


def test_new_tv_theme_autodownloads_when_toggle_on():
    """The exact Tires shape: an unthemed in-Plex TV show (guid_tmdb set, NO
    theme_id link yet) gets a NEW ThemerrDB theme → a download job is enqueued
    when the toggle is ON."""
    db = _fresh_db()
    with get_conn(db) as c:
        _seed_section(c)
        c.execute(
            "INSERT INTO plex_items (rating_key,section_id,media_type,"
            " guid_tmdb,title,year,has_theme,local_theme_file,first_seen_at,"
            " last_seen_at) "
            "VALUES ('rk1','s_tv','show','247522','Tires','2024',0,0,?,?)",
            (_NOW, _NOW))
        c.commit()
    record = {
        "title": "Tires", "first_air_date": "2024-01-01",
        "youtube_theme_url": "https://www.youtube.com/watch?v=WPDshctlAfQ",
        "youtube_theme_edited": _NOW,
    }
    sync._flush_sync_batch(
        db, [("tv", 247522, record, "themoviedb")], sync_ts=_NOW,
        enqueue_downloads=True, auto_place_override=None,
        auto_download_new_themes=True, stats=sync.SyncStats())
    with get_conn(db) as c:
        jobs = c.execute(
            "SELECT job_type, media_type, tmdb_id FROM jobs "
            "WHERE job_type = 'download'").fetchall()
    assert len(jobs) == 1, (
        "v1.22.46: a NEW unthemed in-Plex TV theme must enqueue a download "
        f"when the toggle is ON — got {len(jobs)} download jobs")
    assert jobs[0]["tmdb_id"] == 247522


def test_toggle_off_writes_pending_update_not_download():
    """Regression guard: with the toggle OFF, the same new theme surfaces a
    new_theme_available pending-update (manual prompt) and NO download."""
    db = _fresh_db()
    with get_conn(db) as c:
        _seed_section(c)
        c.execute(
            "INSERT INTO plex_items (rating_key,section_id,media_type,"
            " guid_tmdb,title,year,has_theme,local_theme_file,first_seen_at,"
            " last_seen_at) "
            "VALUES ('rk1','s_tv','show','247522','Tires','2024',0,0,?,?)",
            (_NOW, _NOW))
        c.commit()
    record = {"title": "Tires", "first_air_date": "2024-01-01",
              "youtube_theme_url": "https://youtu.be/WPDshctlAfQ",
              "youtube_theme_edited": _NOW}
    sync._flush_sync_batch(
        db, [("tv", 247522, record, "themoviedb")], sync_ts=_NOW,
        enqueue_downloads=False, auto_place_override=None,
        auto_download_new_themes=False, stats=sync.SyncStats())
    with get_conn(db) as c:
        jobs = c.execute(
            "SELECT 1 FROM jobs WHERE job_type='download'").fetchall()
        pend = c.execute(
            "SELECT kind FROM pending_updates "
            "WHERE media_type='tv' AND tmdb_id=247522").fetchall()
    assert not jobs, "toggle OFF must not auto-download"
    assert any(r["kind"] == "new_theme_available" for r in pend), (
        "toggle OFF must surface a new_theme_available manual prompt")


def test_enqueue_download_still_matches_anime_via_theme_id():
    """No regression to the v1.15.142 anime-cohort fix: a row with guid_tmdb NULL but a
    pi.theme_id linkage still resolves its section + enqueues."""
    db = _fresh_db()
    with get_conn(db) as c:
        _seed_section(c, sid="s_an", title="Anime", anime=1)
        c.execute(
            "INSERT INTO themes (media_type,tmdb_id,title,year,upstream_source,"
            " youtube_url,first_seen_sync_at,last_seen_sync_at) "
            "VALUES ('tv',999,'A','2014','themoviedb','u',?,?)", (_NOW, _NOW))
        tid = c.execute("SELECT id FROM themes WHERE tmdb_id=999").fetchone()[0]
        c.execute(
            "INSERT INTO plex_items (rating_key,section_id,media_type,"
            " guid_tmdb,theme_id,title,year,first_seen_at,last_seen_at) "
            "VALUES ('rk2','s_an','show',NULL,?,'A','2014',?,?)",
            (tid, _NOW, _NOW))
        c.commit()
    with get_conn(db) as c, sync.transaction(c):
        n = sync._enqueue_download(c, media_type="tv", tmdb_id=999,
                                   reason="new")
    assert n == 1, (
        "v1.22.46: HAMA (guid_tmdb NULL, theme_id linked) must still enqueue")


def test_enqueue_download_section_query_uses_guid_or_theme_id():
    src = (REPO / "app" / "core" / "sync.py").read_text()
    idx = src.index("def _enqueue_download")
    body = src[idx:idx + 6000]
    assert "OR (t.tmdb_id = ? AND t.media_type = ?)" in body, (
        "v1.22.46: section query must keep the theme_id (anime cohort) path")
    assert "pi.guid_tmdb = ?" in body, (
        "v1.22.46: section query must ALSO match guid_tmdb (new-theme path, "
        "before resolve_theme_ids links pi.theme_id)")
