"""v0.51.95 — dashboard "ready to add" SSR aggregate is per-EDITION.

/api/coverage/plex computes its per-item `placed` flag with
`AND p.edition_key = pi.edition_key` (v1.21.5x edition arc), but the dashboard
SSR "ready to add" aggregate (plex_*_ready) checked placements only by
media_type + tmdb_id + section_id. So for a multi-edition title, a placement on
ONE edition made EVERY sibling edition of the same title+section read
"not ready" → the SSR count undershot the coverage page + the library's
per-row unthemed (SRC=—) view. Mirror-drift bug class: the coverage endpoint
got edition-scoped, the SSR mirror didn't.

Fix: add `AND p.edition_key = pi.edition_key` to all four SSR `_ready`
NOT EXISTS subqueries, matching /api/coverage/plex.
"""
from __future__ import annotations

import re
import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()
NOW = "2026-07-06T00:00:00"
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


def _item(conn, *, rk, tid, tmdb, title, has_theme, edition_key=""):
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, media_type, theme_id,"
        " guid_imdb, guid_tmdb, title, year, has_theme, plex_independent_theme,"
        " edition_key, first_seen_at, last_seen_at)"
        " VALUES (?, '1', 'movie', ?, ?, ?, ?, 2012, ?, 0, ?, ?, ?)",
        (rk, tid, f"tt{tmdb}", tmdb, title, has_theme, edition_key, NOW, NOW))


def _placement(conn, *, tid, tmdb, edition_key=""):
    conn.execute(
        "INSERT INTO placements (media_type, tmdb_id, section_id, theme_id,"
        " media_folder, placed_at, placement_kind, provenance, edition_key)"
        " VALUES ('movie', ?, '1', ?, ?, ?, 'hardlink', 'auto', ?)",
        (tmdb, tid, f"/data/m/{tmdb}", NOW, edition_key))


def _coverage(client):
    r = client.get("/api/coverage/plex", headers=AUTH)
    assert r.status_code == 200, r.text
    return {it["rating_key"]: it for it in r.json()["movies"]}


def _seed_two_editions_one_placed(db):
    """One title, two editions in the same section: the standard edition ('')
    is PLACED, the 4K edition is NOT. Both Plex-unthemed, TDB-available."""
    with sqlite3.connect(db) as conn:
        _section(conn)
        _theme(conn, 1, 301, "Two Editions")
        _item(conn, rk="rk-std", tid=1, tmdb=301, title="Two Editions",
              has_theme=0, edition_key="")
        _item(conn, rk="rk-4k", tid=1, tmdb=301, title="Two Editions",
              has_theme=0, edition_key="{edition-4k}")
        _placement(conn, tid=1, tmdb=301, edition_key="")  # standard only
        conn.commit()


def test_coverage_reference_is_per_edition(admin_client):
    """The reference /api/coverage/plex flags placed per edition: the standard
    edition is placed, the 4K sibling is not."""
    client, db = admin_client
    _seed_two_editions_one_placed(db)
    cov = _coverage(client)
    assert cov["rk-std"]["placed"] is True
    assert cov["rk-4k"]["placed"] is False
    assert cov["rk-4k"]["motif_available"] is True
    assert cov["rk-4k"]["has_theme"] is False


def test_ssr_ready_counts_unplaced_sibling_edition(admin_client):
    """THE FIX: the dashboard SSR "ready to add" count must include the unplaced
    4K sibling (1), not zero — pre-fix the standard edition's placement
    suppressed every sibling of the same title+section."""
    client, db = admin_client
    _seed_two_editions_one_placed(db)
    html = client.get("/", headers=AUTH).text
    m = re.search(r'id="cov-movies-ready"[^>]*>\s*(\d+)\s*<', html)
    assert m, "dashboard must SSR-bake the movies 'ready to add' count"
    assert int(m.group(1)) == 1, (
        f"SSR ready must count the unplaced sibling edition (got {m.group(1)}); "
        "pre-fix the placed standard edition suppressed it via a "
        "tmdb+section-only NOT EXISTS")


def test_single_edition_placed_row_still_not_ready(admin_client):
    """Regression guard: the v1.23.36 single-edition behavior is unchanged — a
    placed row (edition '' == '') is still excluded from ready."""
    client, db = admin_client
    with sqlite3.connect(db) as conn:
        _section(conn)
        _theme(conn, 1, 401, "Placed")
        _item(conn, rk="rk-p", tid=1, tmdb=401, title="Placed", has_theme=0)
        _placement(conn, tid=1, tmdb=401)  # edition '' == pi.edition_key ''
        conn.commit()
    html = client.get("/", headers=AUTH).text
    m = re.search(r'id="cov-movies-ready"[^>]*>\s*(\d+)\s*<', html)
    assert m and int(m.group(1)) == 0, "a placed single-edition row is not ready"


# ── mirror-drift guard: SSR aggregate + coverage/plex agree on edition scope ──


def test_all_ready_subqueries_are_edition_scoped():
    for alias in ("plex_movies_ready", "plex_tv_ready", "plex_anime_ready",
                  "plex_collections_ready"):
        i = API_PY.index(f"AS {alias}")
        block = API_PY[i - 800:i]  # the NOT EXISTS lives just above the alias
        assert "NOT EXISTS (SELECT 1 FROM placements p" in block
        assert "p.edition_key=pi.edition_key" in block, (
            f"SSR {alias} NOT EXISTS must scope by edition_key (mirror "
            "/api/coverage/plex) or multi-edition titles undercount")
    # the reference the SSR mirrors is itself edition-scoped.
    assert "AND p.edition_key = pi.edition_key) AS placed" in API_PY
