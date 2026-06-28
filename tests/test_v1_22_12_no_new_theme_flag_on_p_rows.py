"""v1.22.12 — stop flagging new_theme_available on pure SRC=P rows.

the user (after a sync found ~38 new anime themes): a wall of SRC=P rows (Plex
serves its own theme, motif owns nothing) lit up with the blue TDB↑ pill / !UPD
glyph / UPD badge / NEEDS WORK. v1.19.71 deliberately surfaced "TDB just
published a theme" across SRC=—/P/U/A/M so the operator could take over — but
anime is ~100% Plex-served and TDB's catalog keeps growing, so every sync floods
the attention surfaces with rows motif doesn't manage, burying the ones it does.

the user's call: stop flagging P rows (detection + read).
- read (api.py): the `new_theme_available` branch of `_pending_update_actionable_
  sql` now ANDs `_not_p_row` — so a pure-P row's pending is no longer actionable
  (pending_update=0, actionable_update=0, out of UPD/NEEDS WORK/filters). Covers
  rows already detected by prior syncs immediately. SRC=—/U/A/M still pass
  `_not_p_row`, so they keep surfacing (v1.19.71 intent preserved).
- detection (sync.py): an `elif plex_supplies and not has_content: pass` skips
  WRITING new_theme_available for pure-P rows, so they don't accumulate.

The theme stays discoverable; the operator can DOWNLOAD TDB per-row from the
SOURCE menu to take over.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()
SYNC_PY = (REPO / "app" / "core" / "sync.py").read_text()
NOW = "2026-06-06T00:00:00"


# ── Source pins ──────────────────────────────────────────────


def test_actionable_helper_gates_new_theme_on_not_p_row():
    """The actionable gate's new_theme branch must AND _not_p_row, so a pure-P
    row's freshly-discovered theme isn't 'actionable'."""
    i = API_PY.index("def _pending_update_actionable_sql(")
    j = API_PY.index("def _pending_update_detected_sql(", i)
    body = API_PY[i:j]
    ret = body[body.index("return ("):]
    # The new_theme call in the RETURN must be paired with _not_p_row.
    assert "_pending_update_new_theme_kind_sql(t, pi)} AND {_not_p_row_sql(t, pi)}" in ret, (
        "v1.22.12: new_theme_available must be gated on _not_p_row in the "
        "actionable helper"
    )
    assert "v1.22.12" in body


def test_sync_skips_new_theme_for_pure_p_rows():
    """Detection must skip the new_theme_available write for plex_supplies +
    no-content (pure SRC=P) rows."""
    assert "elif plex_supplies and not has_content:" in SYNC_PY, (
        "v1.22.12: sync must skip writing new_theme_available for pure-P rows"
    )
    # The skip must sit between the auto-download branch and the manual-prompt
    # else (so it intercepts only pure-P, not has_content / SRC=—).
    auto = SYNC_PY.index("and enqueue_downloads):")
    skip = SYNC_PY.index("elif plex_supplies and not has_content:")
    els = SYNC_PY.index("# Manual prompt path")
    assert auto < skip < els, "v1.22.12: skip branch misplaced in the chain"


# ── Behavioral ───────────────────────────────────────────────


@pytest.fixture
def admin_client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web.api import create_app
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    db = settings.db_path
    init_db(db)
    init_auth_schema(db)
    create_admin(db, username="testadmin", password="testpassword")
    return TestClient(create_app(settings))


AUTH = {"X-Authentik-Username": "testadmin"}


def _db(tmp_path):
    from app.config import Settings
    return Settings(config_dir=tmp_path, data_dir=tmp_path / "data").db_path


def _section(conn, sid="1"):
    conn.execute(
        "INSERT INTO plex_sections (section_id, title, type, is_anime, is_4k,"
        " themes_subdir, included, discovered_at, last_seen_at)"
        " VALUES (?, 'Movies', 'movie', 0, 0, 'movies', 1, ?, ?)", (sid, NOW, NOW))


def _theme(conn, tid, tmdb, title):
    conn.execute(
        "INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source,"
        " last_seen_sync_at, first_seen_sync_at, youtube_url)"
        " VALUES (?, 'movie', ?, ?, 'imdb', ?, ?, 'https://y/watch?v=NEW')",
        (tid, tmdb, title, NOW, NOW))


def _item(conn, *, rk, tid, tmdb, title, has_theme, verified, sid="1"):
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, media_type, theme_id,"
        " guid_imdb, guid_tmdb, title, year, has_theme, local_theme_file,"
        " folder_path, plex_independent_theme, plex_theme_verified_ok,"
        " first_seen_at, last_seen_at)"
        " VALUES (?, ?, 'movie', ?, ?, ?, ?, 2012, ?, 0, ?, 0, ?, ?, ?)",
        (rk, sid, tid, f"tt{tmdb}", tmdb, title, has_theme,
         f"/data/m/{title}", verified, NOW, NOW))


def _new_theme_pending(conn, tmdb, sid=""):
    conn.execute(
        "INSERT INTO pending_updates (media_type, tmdb_id, section_id,"
        " edition_key, new_video_id, new_youtube_url, detected_at, decision, kind)"
        " VALUES ('movie', ?, ?, '', 'NEW', 'https://y/watch?v=NEW', ?,"
        " 'pending', 'new_theme_available')", (tmdb, sid, NOW))


def _row(client, rk, tab="movies"):
    r = client.get(f"/api/library?tab={tab}", headers=AUTH)
    assert r.status_code == 200, r.text
    m = [it for it in r.json().get("items", []) if it.get("rating_key") == rk]
    assert m, f"row {rk} missing: {[it.get('rating_key') for it in r.json().get('items', [])]}"
    return m[0]


def test_pure_p_row_new_theme_does_not_flag(admin_client, tmp_path):
    """A pure SRC=P row (Plex serves, motif owns nothing) with a freshly-
    discovered new_theme_available must NOT show the blue ↑ / !UPD."""
    db = _db(tmp_path)
    with sqlite3.connect(db) as conn:
        _section(conn)
        # Pure P: has_theme=1, verified=1, NO local_files/override/placement.
        _theme(conn, 1, 100, "Kino's Journey")
        _item(conn, rk="rk-p", tid=1, tmdb=100, title="Kino's Journey",
              has_theme=1, verified=1)
        _new_theme_pending(conn, 100)
        conn.commit()

    row = _row(admin_client, "rk-p")
    assert row.get("pending_update") in (0, None, False), (
        f"v1.22.12: pure-P new_theme must NOT light the blue ↑ pill; got "
        f"{row.get('pending_update')}"
    )
    assert row.get("actionable_update") in (0, None, False), (
        f"v1.22.12: pure-P new_theme must NOT light the !UPD glyph / UPD count; "
        f"got {row.get('actionable_update')}"
    )
    # And it must be absent from the blue tdb_pills=update filter.
    upd = admin_client.get("/api/library?tab=movies&tdb_pills=update", headers=AUTH)
    assert "rk-p" not in [it.get("rating_key") for it in upd.json().get("items", [])]


def test_unthemed_row_new_theme_still_flags(admin_client, tmp_path):
    """The v1.19.71 SRC=— case is preserved: an unthemed row (has_theme=0,
    passes _not_p_row) with new_theme_available still surfaces the blue ↑."""
    db = _db(tmp_path)
    with sqlite3.connect(db) as conn:
        _section(conn)
        # SRC=—: has_theme=0, no content. _not_p_row TRUE → still actionable.
        _theme(conn, 2, 200, "Unthemed Anime")
        _item(conn, rk="rk-dash", tid=2, tmdb=200, title="Unthemed Anime",
              has_theme=0, verified=0)
        _new_theme_pending(conn, 200)
        conn.commit()

    row = _row(admin_client, "rk-dash")
    assert row.get("pending_update") == 1, (
        f"v1.22.12: SRC=— new_theme must STILL flag (v1.19.71 intent); got "
        f"{row.get('pending_update')}"
    )


def test_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
