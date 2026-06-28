"""v1.24.71 — REPLACE TDB not offered when ThemerrDB has no theme video.

the user: REPLACE TDB on Daredevil: Born Again (a TDB-tracked title whose
ThemerrDB record has no youtube_url — TDB catalogs the title but has no theme
for it yet) 409'd "ThemerrDB record has no youtube_url". The SOURCE-menu gate
checked isThemerrDb (upstream_source set) but not whether a TDB theme video
actually exists — DOWNLOAD TDB BACKUP already gates on it.youtube_url; REPLACE
TDB was missing the same check.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from app.core.db import init_db

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
NOW = datetime.now(timezone.utc).isoformat(timespec="seconds")


# ── frontend: REPLACE TDB gated on it.youtube_url ────────────────────────────


def test_replace_tdb_gate_requires_youtube_url():
    # the menu item is pushed inside an `if (isThemerrDb && it.youtube_url && …`.
    idx = APP_JS.index("'replace-with-themerrdb', 'REPLACE TDB'")
    guard = APP_JS[APP_JS.rindex("if (isThemerrDb", 0, idx):idx]
    assert "it.youtube_url" in guard, (
        "REPLACE TDB must gate on it.youtube_url (a TDB-tracked title can have "
        "no theme video)")


# ── backend: the endpoint helper still rejects the empty-url case ────────────


def test_replace_with_themerrdb_raises_without_youtube_url(tmp_path):
    from app.core.adopt import replace_with_themerrdb, AdoptError
    db = tmp_path / "m.db"
    init_db(db)
    with sqlite3.connect(db) as c:
        # a TDB-tracked themes row (upstream themoviedb) but NO youtube_url.
        c.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, year, "
            " upstream_source, youtube_url, last_seen_sync_at, first_seen_sync_at)"
            " VALUES ('tv', 202555, 'Daredevil: Born Again', '2025', "
            "         'themoviedb', '', ?, ?)", (NOW, NOW))
        c.commit()
    with pytest.raises(AdoptError) as ei:
        replace_with_themerrdb(
            db, media_type="tv", tmdb_id=202555, decided_by="test")
    assert "no youtube_url" in str(ei.value)
