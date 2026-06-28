"""v1.22.8 — sync clears stale pending_updates for url-less TDB themes.

Found in the NEEDS-WORK DB dig: anime rows (Witch Hat Atelier, The Beginning
After the End) sat at the top of NEEDS WORK with an undecided 'upstream_changed'
pending whose proposed theme TDB had since removed (themes.youtube_url=NULL).
The v1.20.16 cleanup only fires on TITLE drops (tdb_dropped_at); the
title-present-but-url-less case slips through. _clear_url_less_pending_updates
closes it — while leaving 'urls_match' (the legit U==TDB convert prompt) and
decided rows alone.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from app.core.db import init_db
from app.core.sync import _clear_url_less_pending_updates


NOW = "2026-06-06T00:00:00Z"


def _theme(conn, tid, tmdb, url):
    conn.execute(
        "INSERT INTO themes (id, media_type, tmdb_id, title, youtube_url,"
        " upstream_source, last_seen_sync_at, first_seen_sync_at)"
        " VALUES (?,'tv',?,'X',?,'themoviedb',?,?)", (tid, tmdb, url, NOW, NOW))


def _pending(conn, tmdb, kind, decision):
    conn.execute(
        "INSERT INTO pending_updates (media_type, tmdb_id, section_id,"
        " edition_key, new_youtube_url, detected_at, decision, kind)"
        " VALUES ('tv',?, '', '', 'https://y/new', ?, ?, ?)",
        (tmdb, NOW, decision, kind))


def _count(conn, tmdb):
    return conn.execute(
        "SELECT COUNT(*) FROM pending_updates WHERE tmdb_id=?", (tmdb,)).fetchone()[0]


def test_clears_only_url_less_undecided_tdb_proposals(tmp_path):
    db = tmp_path / "m.db"
    init_db(db)
    with sqlite3.connect(db) as conn:
        # SWEPT: url-less theme + undecided upstream_changed / new_theme_available
        _theme(conn, 1, 100, None);  _pending(conn, 100, "upstream_changed", "pending")
        _theme(conn, 2, 200, None);  _pending(conn, 200, "new_theme_available", "pending")
        # KEPT: theme HAS a url (real proposal) — upstream_changed
        _theme(conn, 3, 300, "https://y/watch?v=abc"); _pending(conn, 300, "upstream_changed", "pending")
        # KEPT: urls_match is the legit U==TDB prompt — never swept (even url-less)
        _theme(conn, 4, 400, None);  _pending(conn, 400, "urls_match", "pending")
        # KEPT: already decided (accepted/declined) — only undecided are swept
        _theme(conn, 5, 500, None);  _pending(conn, 500, "upstream_changed", "accepted")
        _theme(conn, 6, 600, None);  _pending(conn, 600, "new_theme_available", "declined")
        conn.commit()

        n = _clear_url_less_pending_updates(db)
        assert n == 2, "exactly the 2 url-less undecided TDB proposals are swept"

        assert _count(conn, 100) == 0  # swept
        assert _count(conn, 200) == 0  # swept
        assert _count(conn, 300) == 1, "url-having proposal kept"
        assert _count(conn, 400) == 1, "urls_match kept (legit convert prompt)"
        assert _count(conn, 500) == 1, "accepted kept"
        assert _count(conn, 600) == 1, "declined kept"


def test_idempotent_and_noop_when_nothing_stale(tmp_path):
    db = tmp_path / "m.db"
    init_db(db)
    with sqlite3.connect(db) as conn:
        _theme(conn, 1, 100, "https://y/watch?v=abc")
        _pending(conn, 100, "upstream_changed", "pending")
        conn.commit()
    assert _clear_url_less_pending_updates(db) == 0
    assert _clear_url_less_pending_updates(db) == 0  # idempotent


def test_wired_into_run_sync():
    src = (Path(__file__).resolve().parent.parent
           / "app" / "core" / "sync.py").read_text()
    assert "_clear_url_less_pending_updates(db_path)" in src
    # called in run_sync's body (after drop detection), not only defined
    assert src.count("_clear_url_less_pending_updates") >= 2


def test_v1_22_8_version_pin():
    init_py = (Path(__file__).resolve().parent.parent
               / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
