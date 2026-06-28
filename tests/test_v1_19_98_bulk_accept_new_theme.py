"""v1.19.98 — bulk-accept bar fixes for new_theme_available rows.

the user's 2026-05-29 ANIME-page repro (the 5 new themes from sync #95,
all kind='new_theme_available' per v1.19.71):

  ① Accept did nothing — "Bulk-accepted 0 pending updates (0
     eager-flipped, 0 downloads queued)". api_accept_all_updates'
     no-op gate (`new_tdb_url == applied_url`) skipped every row: a
     new_theme_available row has no prior theme, so applied_url falls
     back to themes.youtube_url which EQUALS the synced new_tdb_url.
     The gate exempted urls_match but not new_theme_available.
  ② Bulk-bar count read 3 while the topbar + accept dialog (SQL-side)
     read 5. The 3 JS bulk-bar predicates filtered
     `pending_update && computeSrcLetter(it) !== '-'`, excluding the 2
     SRC=— new-theme rows — v1.19.71 added the SRC=— exception to
     computeSrcLetter + the glyph sites but MISSED these three.
  ③ Selecting rows left the ACCEPT/KEEP count stale (no else to reset
     the label when the computed count was 0).
  ④ (handled by ②) selection-scoped accept already existed
     (_selectedActionableForUpdates → per-row /accept); the predicate
     excluded the rows so it fell to the global path.
  ⑤ The bulk-bar caption overlapped the first button (white-space:
     nowrap + flex-shrink:1 spilled past its box).
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


# ── ① server no-op gate exempts new_theme_available ──────────


def test_noop_gate_exempts_new_theme_available():
    idx = API_PY.index("No-op gate: skip rows where new_url")
    block = API_PY[idx:idx + 1000]
    assert 'kind not in ("urls_match", "new_theme_available")' in block, (
        "v1.19.98: the bulk-accept no-op gate must exempt "
        "new_theme_available (applied==new but the download must "
        "still queue)"
    )


# ── ② the three JS bulk-bar predicates gained the exception ──


def test_three_bulk_bar_predicates_have_new_theme_exception():
    # The exact exception phrase, repeated at the 3 missed sites
    # (header count, button updateCount, _selectedActionableForUpdates).
    needle = "it.pending_update_kind === 'new_theme_available'"
    assert APP_JS.count(needle) >= 5, (
        "v1.19.98: all bulk-bar pending-update predicates must include "
        "the new_theme_available SRC=— exception (3 added here + the "
        f"v1.19.71 sites); found {APP_JS.count(needle)}"
    )
    # The bare (drift-prone) form must be gone from the bulk-bar.
    assert "it.pending_update && computeSrcLetter(it) !== '-'," not in APP_JS, (
        "v1.19.98: no bulk-bar predicate may still use the bare "
        "SRC!=='-' form without the new_theme_available exception"
    )


# ── ③ label reset (no stale count) ───────────────────────────


def test_accept_keep_labels_reset_when_count_zero():
    """v1.19.98 added the else so a 0 count can't leave a stale (N);
    v1.20.1 made the labels selection-aware. Either branch always
    (re)sets the label when shown — pin both the selection gate +
    both no-selection fallbacks so staleness can't return."""
    idx = APP_JS.index("library-accept-all-updates-btn')")
    block = APP_JS[idx:idx + 1400]
    assert "selScoped" in block, "v1.20.1: selection-aware label gate"
    # No-selection fallbacks (the bare ALL labels).
    assert "'// ACCEPT ALL UPDATES'" in block
    assert "'// KEEP ALL CURRENT'" in block
    # Selection branch carries the (N) count.
    assert "(${updateCount})" in block


# ── ⑤ bulk-bar caption can't overlap the buttons ─────────────


def test_bulk_bar_caption_wraps_not_overlaps():
    """v1.20.1: caption stays on a SINGLE line (the user: the v1.19.98
    wrap double-stacked) — flex:1 prevents overlap, nowrap+ellipsis
    truncates instead of wrapping."""
    idx = APP_CSS.index("#library-bulk-bar .missing-banner-text {")
    rule = APP_CSS[idx:idx + 800]
    assert "white-space: nowrap;" in rule
    assert "text-overflow: ellipsis;" in rule
    assert "flex: 1;" in rule
    assert "overflow: hidden;" in rule


# ── ①+④ behavioral: accept-all ACCEPTS a new_theme_available row ─


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
    return TestClient(create_app(settings)), db


AUTH = {"X-Authentik-Username": "testadmin"}


def _seed_new_theme_row(db, *, tmdb_id, title):
    """A pure new_theme_available row: theme + in-Plex item + a
    pending_updates(kind='new_theme_available') whose new URL EQUALS
    themes.youtube_url (the applied==new shape that broke accept).
    No local_files / overrides / placements (SRC=—)."""
    now = "2026-05-29T00:00:00"
    url = f"https://www.youtube.com/watch?v=NEW{tmdb_id}"
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT OR IGNORE INTO plex_sections (section_id, title, type, "
            "  is_anime, is_4k, themes_subdir, included, discovered_at, "
            "  last_seen_at) VALUES ('3','Anime','show',1,0,'anime',1,?,?)",
            (now, now))
        cur = conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, upstream_source, "
            "  last_seen_sync_at, first_seen_sync_at, youtube_url) "
            "VALUES ('tv',?,?,'imdb',?,?,?)",
            (tmdb_id, title, now, now, url))
        theme_id = cur.lastrowid
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type, "
            "  theme_id, guid_tmdb, title, year, has_theme, local_theme_file, "
            "  first_seen_at, last_seen_at) "
            "VALUES (?,'3','show',?,?,?,2024,0,0,?,?)",
            (f"rk{tmdb_id}", theme_id, tmdb_id, title, now, now))
        conn.execute(
            "INSERT INTO pending_updates (media_type, tmdb_id, section_id, "
            "  kind, new_youtube_url, decision, detected_at) "
            "VALUES ('tv',?,'3','new_theme_available',?,'pending',?)",
            (tmdb_id, url, now))
        conn.commit()


def test_bulk_accept_all_accepts_new_theme_rows(admin_client):
    client, db = admin_client
    _seed_new_theme_row(db, tmdb_id=5001, title="Garo The Animation")
    _seed_new_theme_row(db, tmdb_id=5002, title="Jormungand")

    r = client.post("/api/updates/accept-all", headers=AUTH)
    assert r.status_code == 200, r.text
    body = r.json()
    # Pre-fix the no-op gate skipped both → accepted=0. Now both accept.
    assert body["accepted"] >= 2, (
        f"v1.19.98: accept-all must accept new_theme_available rows, "
        f"got {body}"
    )

    # Decisions flipped + a download was queued for each.
    with sqlite3.connect(db) as conn:
        pend = conn.execute(
            "SELECT COUNT(*) FROM pending_updates WHERE decision='pending'"
        ).fetchone()[0]
        dljobs = conn.execute(
            "SELECT COUNT(*) FROM jobs WHERE job_type='download'"
        ).fetchone()[0]
    assert pend == 0, "v1.19.98: both rows must leave the pending state"
    assert dljobs >= 2, "v1.19.98: a download must be queued per accepted row"


def test_updates_count_includes_new_theme_rows(admin_client):
    """The SQL-side count (topbar + accept dialog) already includes
    new_theme_available rows — pin it so the JS bulk-bar count (now
    fixed in ②) and this stay in agreement."""
    client, db = admin_client
    _seed_new_theme_row(db, tmdb_id=5003, title="Kabaneri")
    r = client.get("/api/updates/count", headers=AUTH)
    assert r.status_code == 200, r.text
    assert r.json().get("pending", 0) >= 1


def test_v1_19_98_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
