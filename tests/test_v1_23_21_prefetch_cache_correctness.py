"""v1.23.21 — code-review fixes for the v1.23.19/.20 INFO-card perf work.

Two real defects the review surfaced in the prefetch:
1. mouseover/mouseout BUBBLE on the ⓘ button's inner nodes, so the
   v1.23.20 throttle reset/cancelled its 150ms timer on every sub-pixel
   move and rarely fired. Fixed with a relatedTarget boundary check
   (mouseenter/mouseleave emulation via delegation).
2. The 6s prefetch cache served STALE INFO-card state after a row action
   (hover → CLEAR URL / UNPLACE / … → re-open within 6s showed the
   pre-action card), reintroducing the staleness the app's api()
   cache:'no-store' policy exists to prevent. Fixed by invalidating
   _infoPrefetch on every mutating row action AND on every loadLibrary
   render (which also bounds the otherwise-unbounded Map).

This file also adds the BEHAVIORAL coverage the review flagged missing:
the offloaded api_item (the 540-line run_in_threadpool re-indent) was
only source-text-pinned + one-branch covered. Here we GET /api/items
through the offloaded path and assert the full payload + the 404.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core.auth import create_admin, init_auth_schema
from app.core.db import init_db
from app.core.events import now_iso


REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


# ── prefetch JS fixes (source guards) ────────────────────────


def test_hover_uses_relatedtarget_boundary_check():
    i = APP_JS.index("_lib?.addEventListener('mouseover'")
    block = APP_JS[i:i + 700]
    # both mouseover + mouseout must ignore inner-node churn via
    # btn.contains(e.relatedTarget) — else the 150ms rest never fires.
    assert block.count("btn.contains(e.relatedTarget)") == 2, (
        "both mouseover and mouseout must guard on the button boundary"
    )
    assert "addEventListener('mouseout'" in block


def test_prefetch_cache_invalidated_on_action_and_render():
    # mutating row action clears the cache (no stale post-action card).
    assert "if (act && act !== 'info') _infoPrefetch.clear();" in APP_JS
    # every library render clears it (background mutations + memory bound).
    i = APP_JS.index("async function loadLibrary()")
    body = APP_JS[i:i + 800]
    assert "_infoPrefetch.clear();" in body


# ── offloaded api_item — BEHAVIORAL (the missing coverage) ───


@pytest.fixture
def admin_client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(settings.db_path)
    init_auth_schema(settings.db_path)
    create_admin(settings.db_path, username="testadmin", password="testpassword")
    from app.web.api import create_app
    return TestClient(create_app(settings)), settings.db_path


_H = {"X-Authentik-Username": "testadmin"}


def _seed_full_row(db: Path) -> None:
    now = now_iso()
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, included, "
            "  is_anime, is_4k, themes_subdir, discovered_at, last_seen_at) "
            "VALUES ('1', 'Movies', 'movie', 1, 0, 0, 'movies', ?, ?)",
            (now, now))
        cur = conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, title_norm, year, "
            "  upstream_source, youtube_url, last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('movie', 700, 'Full Row', 'full row', '2020', 'imdb', "
            "  'https://www.youtube.com/watch?v=TDB00000001', ?, ?)", (now, now))
        tid = cur.lastrowid
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type, title, "
            "  title_norm, guid_tmdb, theme_id, has_theme, first_seen_at, last_seen_at) "
            "VALUES ('rk1', '1', 'movie', 'Full Row', 'full row', 700, ?, 1, ?, ?)",
            (tid, now, now))
        conn.execute(
            "INSERT INTO local_files (media_type, tmdb_id, section_id, theme_id, "
            "  file_path, downloaded_at, source_video_id, provenance, source_kind, "
            "  edition_key) VALUES ('movie', 700, '1', ?, 'movies/Full Row/theme.mp3', "
            "  ?, 'TDB00000001', 'auto', 'themerrdb', '')", (tid, now))
        conn.execute(
            "INSERT INTO placements (media_type, tmdb_id, section_id, theme_id, "
            "  media_folder, placed_at, placement_kind, provenance, edition_key) "
            "VALUES ('movie', 700, '1', ?, '/data/movies/Full Row', ?, 'hardlink', "
            "  'auto', '')", (tid, now))
        conn.commit()


def test_offloaded_api_item_returns_full_payload(admin_client):
    client, db = admin_client
    _seed_full_row(db)
    # through the offloaded (run_in_threadpool) handler, with rk + section.
    r = client.get("/api/items/movie/700?section_id=1&rating_key=rk1", headers=_H)
    assert r.status_code == 200, r.text
    data = r.json()
    # the full payload shape survived the 540-line re-indent.
    for key in ("theme", "local_file", "local_files", "placements",
                "override", "overrides", "pending_update", "previous_url",
                "events", "section_context"):
        assert key in data, f"missing key {key!r}"
    assert data["theme"]["tmdb_id"] == 700
    assert data["local_files"] and data["local_files"][0]["source_kind"] == "themerrdb"
    assert data["placements"] and data["placements"][0]["placement_kind"] == "hardlink"


def test_offloaded_api_item_legacy_no_rk_arm(admin_client):
    # the section-only / no-rating_key arm (NOT covered by v1_22_19) still
    # returns the row through the offloaded path.
    client, db = admin_client
    _seed_full_row(db)
    r = client.get("/api/items/movie/700", headers=_H)
    assert r.status_code == 200, r.text
    assert r.json()["theme"]["tmdb_id"] == 700


def test_offloaded_api_item_404_propagates_through_threadpool(admin_client):
    # the raise HTTPException(404) now fires INSIDE run_in_threadpool —
    # confirm it still surfaces as a clean 404, not a 500.
    client, db = admin_client
    r = client.get("/api/items/movie/99999", headers=_H)
    assert r.status_code == 404
