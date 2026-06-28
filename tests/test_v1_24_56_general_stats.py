"""v1.24.56 — // GENERAL STATISTICS per-library source-split table.

Adopted from the Missing-Trailer-Downloader dashboard the user asked to mirror
("general Statistics better" → clarified the panel he wanted was the stats
table, not services). Each library (Total / Movies / TV / Anime / Collections)
shows the SRC source split: LOCAL (T/A/U/M, motif owns the theme file) vs PLEX
(P, Plex-Pass served) vs MISSING (–), plus a coverage %. Kept on motif's SRC
axis — no genre-skip column (motif has no genre-skip concept).

The ANIME row needs a TV-vs-anime split that plex_items.media_type can't give
(it's 'show' for both), so v1.24.56 adds ps.is_anime to the /api/stats
theme_sources feed. The 3-up donuts ignore is_anime (they aggregate by letter
across media_type), so the feed change is purely additive.

These tests pin (a) the backend feed carries is_anime + splits anime from TV,
(b) the JS bucketing arithmetic, (c) the template/JS/CSS surfaces.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core.db import get_conn, init_db

REPO = Path(__file__).resolve().parent.parent
DASH_HTML = REPO / "app" / "web" / "templates" / "dashboard.html"
APP_JS = REPO / "app" / "web" / "static" / "app.js"
APP_CSS = REPO / "app" / "web" / "static" / "app.css"
API_PY = REPO / "app" / "web" / "api.py"

NOW = datetime.now(timezone.utc).isoformat(timespec="seconds")
AUTH = {"X-Authentik-Username": "testadmin"}


# ── End-to-end: theme_sources carries is_anime + splits anime from TV ────────


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
    with sqlite3.connect(s.db_path) as c:
        # A movies section, a (non-anime) TV section, an anime section. The
        # anime section is 'show'-typed too — is_anime is the only thing that
        # distinguishes it (the bug this tag's split fixes).
        for sid, title, typ, anime in (
            ("1", "Movies", "movie", 0),
            ("2", "TV", "show", 0),
            ("3", "Anime", "show", 1),
        ):
            c.execute(
                "INSERT INTO plex_sections (section_id, title, type, is_anime,"
                " is_4k, themes_subdir, included, discovered_at, last_seen_at) "
                "VALUES (?,?,?,?,0,?,1,?,?)",
                (sid, title, typ, anime, title.lower(), NOW, NOW),
            )
        c.commit()
    return TestClient(create_app(s)), s.db_path


def _item(c, *, rk, sid, mt):
    # An untheme row (theme_id NULL) lands in the '-' bucket — enough to prove
    # the media_type / is_anime grouping; the SRC-letter logic is pinned
    # elsewhere.
    c.execute(
        "INSERT INTO plex_items (rating_key, section_id, media_type, theme_id, "
        " guid_tmdb, title, edition_key, has_theme, first_seen_at, last_seen_at) "
        "VALUES (?,?,?,NULL,NULL,?, '',0,?,?)",
        (rk, sid, mt, f"item {rk}", NOW, NOW),
    )


def test_theme_sources_rows_carry_is_anime(client):
    c, db = client
    with get_conn(db) as conn:
        _item(conn, rk="m1", sid="1", mt="movie")
        _item(conn, rk="t1", sid="2", mt="show")
        _item(conn, rk="a1", sid="3", mt="show")
        _item(conn, rk="c1", sid="1", mt="collection")
        conn.commit()
    r = c.get("/api/stats", headers=AUTH)
    assert r.status_code == 200, r.text
    src = r.json()["theme_sources"]
    assert src, "expected source rows"
    # Every row carries the is_anime flag (additive feed field).
    assert all("is_anime" in row for row in src), src


def test_anime_splits_from_tv(client):
    c, db = client
    with get_conn(db) as conn:
        _item(conn, rk="t1", sid="2", mt="show")  # TV
        _item(conn, rk="a1", sid="3", mt="show")  # Anime
        conn.commit()
    src = c.get("/api/stats", headers=AUTH).json()["theme_sources"]
    shows = [r for r in src if r["media_type"] == "show"]
    # One 'show' bucket is_anime=0 (TV), one is_anime=1 (Anime) — same
    # media_type, distinguished only by the section flag.
    assert any(r["is_anime"] == 1 for r in shows), shows
    assert any(r["is_anime"] == 0 for r in shows), shows


def test_movie_and_collection_are_not_anime(client):
    c, db = client
    with get_conn(db) as conn:
        _item(conn, rk="m1", sid="1", mt="movie")
        _item(conn, rk="c1", sid="1", mt="collection")
        conn.commit()
    src = c.get("/api/stats", headers=AUTH).json()["theme_sources"]
    for mt in ("movie", "collection"):
        rows = [r for r in src if r["media_type"] == mt]
        assert rows, f"expected a {mt} row"
        assert all(r["is_anime"] == 0 for r in rows), rows


# ── Bucketing arithmetic mirror (the JS _gsBucket logic, in Python) ──────────


def _gs_bucket(rows):
    """Mirror of app.js _gsBucket: LOCAL=T/A/U/M, PLEX=P, MISSING=– / unknown."""
    local = plex = missing = 0
    for r in rows:
        n = r.get("count", 0)
        if r["letter"] == "P":
            plex += n
        elif r["letter"] in {"T", "A", "U", "M"}:
            local += n
        else:
            missing += n
    total = local + plex + missing
    cov = round((local + plex) / total * 100) if total else 0
    return {"local": local, "plex": plex, "missing": missing,
            "total": total, "cov": cov}


def test_bucket_splits_local_plex_missing():
    rows = [
        {"letter": "T", "count": 5},
        {"letter": "A", "count": 1},
        {"letter": "U", "count": 1},
        {"letter": "M", "count": 1},  # local = 8
        {"letter": "P", "count": 2},  # plex  = 2
        {"letter": "-", "count": 10},  # missing = 10
    ]
    b = _gs_bucket(rows)
    assert b["local"] == 8
    assert b["plex"] == 2
    assert b["missing"] == 10
    assert b["total"] == 20
    assert b["cov"] == 50  # (8 + 2) / 20


def test_bucket_coverage_zero_when_empty():
    assert _gs_bucket([])["cov"] == 0


def test_bucket_unknown_letter_folds_into_missing():
    # Defensive: a future/unknown letter must count as missing, never silently
    # vanish (so TOTAL still sums to the library size).
    b = _gs_bucket([{"letter": "?", "count": 3}, {"letter": "T", "count": 1}])
    assert b["missing"] == 3
    assert b["total"] == 4


# ── Backend source pins ──────────────────────────────────────────────────────


def test_api_src_query_selects_and_groups_is_anime():
    src = API_PY.read_text()
    idx = src.index("src_rows = conn.execute")
    block = src[idx:idx + 3400]
    assert "ps.is_anime AS is_anime" in block, "is_anime must be selected"
    assert "GROUP BY letter, plex_media_type, ps.is_anime" in block, (
        "is_anime must be in the GROUP BY or anime/TV collapse together")


def test_api_theme_sources_emits_is_anime():
    src = API_PY.read_text()
    idx = src.index('"theme_sources": [')
    block = src[idx:idx + 400]
    assert '"is_anime"' in block


# ── Template pins ────────────────────────────────────────────────────────────


def test_dashboard_has_general_statistics_section():
    html = DASH_HTML.read_text()
    assert "// GENERAL STATISTICS" in html
    assert 'id="general-stats-block"' in html
    assert 'id="general-stats-body"' in html
    idx = html.index('id="general-stats-table"')
    block = html[idx:idx + 700]
    for col in ("LIBRARY", "TOTAL", "LOCAL", "PLEX", "MISSING", "COVERAGE"):
        assert f">{col}<" in block, f"column header {col} missing"


def test_dashboard_general_stats_has_no_genre_skip_column():
    # motif has no genre-skip concept — the reference's column is intentionally
    # dropped, not mapped to something wrong.
    html = DASH_HTML.read_text()
    idx = html.index('id="general-stats-table"')
    block = html[idx:idx + 700]
    assert "GENRE" not in block.upper()


# ── JS pins ──────────────────────────────────────────────────────────────────


def test_js_renders_general_stats():
    js = APP_JS.read_text()
    assert "function renderGeneralStats(rows)" in js
    assert "function _gsBucket(rows)" in js
    # LOCAL letter set + the is_anime split for TV vs ANIME.
    assert "_GS_LOCAL_LETTERS = new Set(['T', 'A', 'U', 'M'])" in js
    body = js[js.index("function renderGeneralStats(rows)"):][:1400]
    assert "isShow(r) && !r.is_anime" in body, "TV row excludes anime"
    assert "isShow(r) && !!r.is_anime" in body, "ANIME row is the anime split"


def test_js_general_stats_called_on_stats_load():
    js = APP_JS.read_text()
    assert "renderGeneralStats(stats.theme_sources" in js


# ── CSS pin ──────────────────────────────────────────────────────────────────


def test_css_defines_general_stats_classes():
    css = APP_CSS.read_text()
    assert "#general-stats-table .gs-lib" in css
    assert "tr.gs-total" in css
