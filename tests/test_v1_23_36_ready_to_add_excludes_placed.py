"""v1.23.36 — "ready to add" excludes rows motif has already placed.

the user (Movies card): "16 ready to add" but he couldn't find 16 themeless
movies. Root cause: ready-to-add was `motif_available && !has_theme`, and
`pi.has_theme` is Plex's metadata flag — it LAGS a fresh placement. 13 of
the 16 had a theme.mp3 already placed in the folder; Plex just hadn't re-
scanned, so has_theme was still 0. Only 3 were genuinely themeless.

Fix: ready-to-add = TDB-available AND Plex-unthemed AND NOT placed by motif
— matching the library's "unthemed" (SRC=—) view. /api/coverage/plex now
returns a `placed` flag per item; the SSR aggregate adds a NOT EXISTS
placements clause.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
NOW = "2026-06-14T00:00:00"
AUTH = {"X-Authentik-Username": "testadmin"}


@pytest.fixture
def admin_client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("MOTIF_PLEX_URL", "http://plex.test:32400")
    monkeypatch.setenv("MOTIF_PLEX_TOKEN", "tok")
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web.api import create_app
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(settings.db_path)
    init_auth_schema(settings.db_path)
    create_admin(settings.db_path, username="testadmin", password="testpassword")
    return TestClient(create_app(settings)), settings.db_path


def _section(conn):
    conn.execute(
        "INSERT INTO plex_sections (section_id, title, type, is_anime, is_4k,"
        " themes_subdir, included, discovered_at, last_seen_at)"
        " VALUES ('1', 'Movies', 'movie', 0, 0, 'movies', 1, ?, ?)", (NOW, NOW))


def _theme(conn, tid, tmdb, title):
    conn.execute(
        "INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source,"
        " last_seen_sync_at, first_seen_sync_at, youtube_url)"
        " VALUES (?, 'movie', ?, ?, 'imdb', ?, ?, 'https://y/watch?v=V')",
        (tid, tmdb, title, NOW, NOW))


def _item(conn, *, rk, tid, tmdb, title, has_theme):
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, media_type, theme_id,"
        " guid_imdb, guid_tmdb, title, year, has_theme, plex_independent_theme,"
        " first_seen_at, last_seen_at)"
        " VALUES (?, '1', 'movie', ?, ?, ?, ?, 2012, ?, 0, ?, ?)",
        (rk, tid, f"tt{tmdb}", tmdb, title, has_theme, NOW, NOW))


def _placement(conn, *, tid, tmdb):
    conn.execute(
        "INSERT INTO placements (media_type, tmdb_id, section_id, theme_id,"
        " media_folder, placed_at, placement_kind, provenance, edition_key)"
        " VALUES ('movie', ?, '1', ?, ?, ?, 'hardlink', 'auto', '')",
        (tmdb, tid, f"/data/m/{tmdb}", NOW))


def _coverage(client):
    r = client.get("/api/coverage/plex", headers=AUTH)
    assert r.status_code == 200, r.text
    return {it["rating_key"]: it for it in r.json()["movies"]}


def test_placed_but_plex_unthemed_row_is_not_ready(admin_client):
    client, db = admin_client
    with sqlite3.connect(db) as conn:
        _section(conn)
        # PLACED but Plex-stale: motif placed a theme (placement row) but
        # pi.has_theme is still 0 (Plex hasn't re-scanned). The bug counted
        # this as "ready to add"; it must NOT be.
        _theme(conn, 1, 101, "Placed Stale")
        _item(conn, rk="rk-stale", tid=1, tmdb=101, title="Placed Stale", has_theme=0)
        _placement(conn, tid=1, tmdb=101)
        # GENUINELY themeless: TDB-available, no placement, Plex unthemed.
        _theme(conn, 2, 102, "Truly Themeless")
        _item(conn, rk="rk-ready", tid=2, tmdb=102, title="Truly Themeless", has_theme=0)
        # THEMED: Plex serves a theme.
        _theme(conn, 3, 103, "Themed")
        _item(conn, rk="rk-themed", tid=3, tmdb=103, title="Themed", has_theme=1)
        conn.commit()

    cov = _coverage(client)
    # the placed-but-stale row is flagged placed=True (the new field).
    assert cov["rk-stale"]["motif_available"] is True
    assert cov["rk-stale"]["has_theme"] is False
    assert cov["rk-stale"]["placed"] is True
    # the genuinely-themeless row is NOT placed.
    assert cov["rk-ready"]["placed"] is False

    # the corrected ready-to-add predicate yields exactly ONE row (rk-ready).
    ready = [rk for rk, it in cov.items()
             if it["motif_available"] and not it["has_theme"] and not it["placed"]]
    assert ready == ["rk-ready"], ready
    # the OLD predicate (no placed check) would have wrongly counted TWO.
    old = [rk for rk, it in cov.items()
           if it["motif_available"] and not it["has_theme"]]
    assert set(old) == {"rk-stale", "rk-ready"}, old


def test_coverage_endpoint_exposes_placed_field(admin_client):
    client, db = admin_client
    with sqlite3.connect(db) as conn:
        _section(conn)
        _theme(conn, 1, 201, "X")
        _item(conn, rk="rk-x", tid=1, tmdb=201, title="X", has_theme=0)
        conn.commit()
    cov = _coverage(client)
    assert "placed" in cov["rk-x"], "to_item must expose the placed flag"


# ── source guards ──


def test_ssr_ready_aggregate_is_placement_aware():
    for alias in ("plex_movies_ready", "plex_tv_ready", "plex_collections_ready"):
        assert f"AS {alias}" in API_PY, f"SSR cov query must compute {alias}"
    assert "NOT EXISTS (SELECT 1 FROM placements p" in API_PY


def test_js_ready_filters_exclude_placed():
    # v1.23.38: the ready predicate is centralized in isAddable (which ANDs
    # !m.placed); all three cards route through it.
    assert "const isAddable = (m) =>" in APP_JS
    i = APP_JS.index("const isAddable = (m) =>")
    assert "!m.placed" in APP_JS[i:i + 320]
    assert "data.movies.filter(isAddable)" in APP_JS
    assert "data.tv.filter(isAddable)" in APP_JS
    assert "animeList.filter(isAddable)" in APP_JS
    assert "collectionsList.filter(isAddable)" in APP_JS
