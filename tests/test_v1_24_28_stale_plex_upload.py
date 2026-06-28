"""v1.24.28 — stale plex_upload detection + honest "RE-PUSH" display.

the user's Avenue Q (continued): a theme deployed via plex_upload (POST to Plex's
metadata store for a rating_key) shows as placed (PL/PU) forever even after a
Plex delete+re-add destroys that rating_key — the theme is gone from Plex, but
verify_placement_health excluded plex_upload from its sidecar stat, so
theme_present stayed NULL and the row read "placed."

Two parts:
  1. Detection: verify_placement_health now stamps plex_upload placements
     theme_present=1 if their plex_rating_key is still a live plex_items row,
     else 0 (stale). Guarded on plex_rating_key IS NOT NULL (legacy rows stay
     optimistically placed).
  2. Display: the library read nulls media_folder + placement_kind for a stale
     plex_upload (so every placed/SRC/PL read treats it as not-placed → SRC='-')
     and sets needs_repush=1 (drives the orange RP badge + keeps it re-pushable).
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core import plex_enum
from app.core.db import get_conn, init_db

NOW = "2026-06-24T00:00:00Z"
AUTH = {"X-Authentik-Username": "testadmin"}
REPO = Path(__file__).resolve().parent.parent


# ── detection: verify_placement_health stamps plex_upload staleness ──

def _theme(conn, tid, tmdb):
    conn.execute(
        "INSERT OR IGNORE INTO plex_sections (section_id, title, type, is_anime,"
        " is_4k, themes_subdir, included, discovered_at, last_seen_at) "
        "VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
    conn.execute(
        "INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source, "
        " last_seen_sync_at, first_seen_sync_at) "
        "VALUES (?, 'movie', ?, 'X', 'plex_orphan', ?, ?)", (tid, tmdb, NOW, NOW))


def _pu_placement(conn, tmdb, rk, theme_present=None):
    conn.execute(
        "INSERT INTO placements (media_type, tmdb_id, section_id, edition_key, "
        " media_folder, placed_at, placement_kind, plex_rating_key, "
        " plex_refreshed, theme_present) "
        "VALUES ('movie', ?, '1', '', '', ?, 'plex_upload', ?, 1, ?)",
        (tmdb, NOW, rk, theme_present))


def _plex_item(conn, rk, tmdb, tid):
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, media_type, theme_id, "
        " guid_tmdb, title, edition_key, has_theme, first_seen_at, last_seen_at) "
        "VALUES (?, '1', 'movie', ?, ?, 'X', '', 0, ?, ?)",
        (rk, tid, tmdb, NOW, NOW))


@pytest.fixture
def db(tmp_path):
    p = tmp_path / "motif.db"
    init_db(p)
    return p


def _present(db, tmdb):
    with get_conn(db) as c:
        return c.execute("SELECT theme_present FROM placements WHERE tmdb_id=?",
                         (tmdb,)).fetchone()[0]


def test_dead_rating_key_stamped_stale(db):
    """A plex_upload whose plex_rating_key has no live plex_items row → stale.
    plex_items is non-empty (the rest of the library enumerated, e.g. the
    re-added item with a fresh rk), so the dead rk's absence is provably real —
    NOT the v1.24.39 empty-table enum-fault case the 0-stamp now skips."""
    with get_conn(db) as c:
        _theme(c, 1, -1)
        _pu_placement(c, -1, "dead-rk")  # no plex_items rk='dead-rk'
        _plex_item(c, "other-live-rk", -1, 1)  # plex_items populated
        c.commit()
    plex_enum.verify_placement_health(db)
    assert _present(db, -1) == 0


def test_live_rating_key_stamped_present(db):
    """A plex_upload whose plex_rating_key IS a live plex_items row → present."""
    with get_conn(db) as c:
        _theme(c, 2, -2)
        _pu_placement(c, -2, "live-rk")
        _plex_item(c, "live-rk", -2, 2)
        c.commit()
    plex_enum.verify_placement_health(db)
    assert _present(db, -2) == 1


def test_null_rating_key_left_untouched(db):
    """A legacy plex_upload with no stored rk stays optimistically placed
    (theme_present preserved as NULL) — never flipped to stale."""
    with get_conn(db) as c:
        _theme(c, 3, -3)
        _pu_placement(c, -3, None)  # plex_rating_key NULL
        c.commit()
    plex_enum.verify_placement_health(db)
    assert _present(db, -3) is None


def test_self_heals_after_rk_becomes_live(db):
    """A previously-stale row (theme_present=0) flips back to 1 once its
    plex_rating_key matches a live plex_items row again (a re-push updates the
    placement's rk). Symmetric stamp = self-healing."""
    with get_conn(db) as c:
        _theme(c, 4, -4)
        _pu_placement(c, -4, "new-rk", theme_present=0)  # was stale
        _plex_item(c, "new-rk", -4, 4)                    # now live again
        c.commit()
    plex_enum.verify_placement_health(db)
    assert _present(db, -4) == 1


def test_returns_stale_count(db):
    with get_conn(db) as c:
        _theme(c, 5, -5)
        _pu_placement(c, -5, "gone")
        _plex_item(c, "live-other", -5, 5)  # plex_items non-empty (v1.24.39)
        c.commit()
    res = plex_enum.verify_placement_health(db)
    assert res["plex_upload_stale"] == 1


# ── display: the library read treats a stale plex_upload as not-placed ──

@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.web.api import create_app
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    return TestClient(create_app(s)), s.db_path


def _seed_pu_row(conn, *, tmdb, rk, theme_present, tid, placement_rk=None):
    # v1.24.40: placement_rk lets a test point the placement at a DEAD rk (one
    # absent from plex_items) while the live plex_items row uses `rk` — modelling
    # a Plex delete+re-add (the item came back under a fresh rk; motif's upload
    # to the OLD rk is genuinely gone). When omitted the placement reuses `rk`
    # (the rk is live), which the tightened RP detection treats as NOT stale.
    _place_rk = placement_rk if placement_rk is not None else rk
    conn.execute(
        "INSERT INTO plex_sections (section_id, title, type, is_anime, is_4k, "
        " themes_subdir, included, discovered_at, last_seen_at) "
        "VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
    conn.execute(
        "INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source, "
        " youtube_url, last_seen_sync_at, first_seen_sync_at) "
        "VALUES (?, 'movie', ?, 'Avenue Q', 'plex_orphan', 'http://y/x', ?, ?)",
        (tid, tmdb, NOW, NOW))
    conn.execute(
        "INSERT INTO local_files (media_type, tmdb_id, section_id, edition_key, "
        " file_path, downloaded_at, source_video_id, provenance, source_kind, "
        " last_place_attempt_reason) "
        "VALUES ('movie', ?, '1', '', ?, ?, 'vid', 'manual', 'url', 'placed')",
        (tmdb, f"movies/x-{tmdb}.mp3", NOW))
    conn.execute(
        "INSERT INTO placements (theme_id, media_type, tmdb_id, section_id, "
        " edition_key, media_folder, placed_at, placement_kind, plex_rating_key, "
        " plex_refreshed, theme_present) "
        "VALUES (?, 'movie', ?, '1', '', '', ?, 'plex_upload', ?, 1, ?)",
        (tid, tmdb, NOW, _place_rk, theme_present))
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, media_type, theme_id, "
        " guid_tmdb, title, edition_key, has_theme, first_seen_at, last_seen_at) "
        "VALUES (?, '1', 'movie', ?, ?, 'Avenue Q', '', 0, ?, ?)",
        (rk, tid, tmdb, NOW, NOW))


def _rows(c):
    r = c.get("/api/library?tab=movies", headers=AUTH)
    assert r.status_code == 200, r.text
    return {it["rating_key"]: it for it in r.json()["items"]}


def test_stale_plex_upload_reads_as_not_placed(client):
    """The Avenue Q repro: a plex_upload row stamped theme_present=0 → the read
    nulls media_folder + placement_kind, sets needs_repush=1, SRC='-'."""
    c, db = client
    with get_conn(db) as conn:
        # v1.24.40: the live plex_items row is rk-live (the re-added item); the
        # placement still points at the destroyed rk-dead → genuinely stale.
        _seed_pu_row(conn, tmdb=-29, rk="rk-live", theme_present=0, tid=90,
                     placement_rk="rk-dead")
        conn.commit()
    row = _rows(c)["rk-live"]
    assert row["needs_repush"] == 1
    # media_folder + placement_kind nulled → every placed/SRC/PL read (incl.
    # computeSrcLetter, separately mirror-tested) treats it as not-placed → SRC='-'.
    assert not row["media_folder"], "stale plex_upload media_folder must null out"
    assert not row["placement_kind"], "stale plex_upload placement_kind must null"
    assert row["file_path"], "the canonical download still shows (DL stays on)"


def test_valid_plex_upload_still_reads_placed(client):
    """A healthy plex_upload (theme_present=1) is unchanged: placement_kind
    stays 'plex_upload', needs_repush=0 — no false RE-PUSH on good rows."""
    c, db = client
    with get_conn(db) as conn:
        _seed_pu_row(conn, tmdb=-30, rk="rk-ok", theme_present=1, tid=91)
        conn.commit()
    row = _rows(c)["rk-ok"]
    assert row["needs_repush"] == 0
    assert row["placement_kind"] == "plex_upload"
    assert row["media_folder"] == "", "valid plex_upload keeps the '' sentinel"


def test_legacy_null_theme_present_still_placed(client):
    """A plex_upload with theme_present NULL (never health-checked / legacy) is
    NOT treated as stale — stays placed, no RE-PUSH badge."""
    c, db = client
    with get_conn(db) as conn:
        _seed_pu_row(conn, tmdb=-31, rk="rk-legacy", theme_present=None, tid=92)
        conn.commit()
    row = _rows(c)["rk-legacy"]
    assert row["needs_repush"] == 0
    assert row["placement_kind"] == "plex_upload"


# ── source pins: detection + badge + chip wired across the stack ────────

def test_detection_pass_present_in_plex_enum():
    src = (REPO / "app" / "core" / "plex_enum.py").read_text()
    assert "placement_kind = 'plex_upload'" in src
    assert "plex_rating_key IS NOT NULL" in src
    assert "plex_upload_stale" in src


def test_repush_badge_and_chip_wired():
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    assert "it.needs_repush" in js
    assert "link-glyph-repush" in js
    css = (REPO / "app" / "web" / "static" / "app.css").read_text()
    assert ".link-glyph-repush" in css and "var(--orange)" in css
    html = (REPO / "app" / "web" / "templates" / "library.html").read_text()
    assert 'data-link-pill="rp"' in html
    api = (REPO / "app" / "web" / "api.py").read_text()
    assert "needs_repush" in api and "_LIB_STALE_PU_SQL" in api
    assert '"rp"' in api  # the link_pills whitelist + branch
