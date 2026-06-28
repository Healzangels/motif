"""v1.23.41 — PLEX cards show ThemerrDB reach, not a repeat of "themed".

the user (after the v1.23.40 top-card split): "Are the plex cards below now
redundant? I want them to be different but right now think they might be too
similar and just duplicating information."

Root cause: the v1.23.34/40 rework made the TOP row "% themed" coverage cards.
The BOTTOM PLEX cards' foot was "X with theme · W ThemerrDB available" — and the
"X with theme" half restated the exact number the top card already shows. Two
rows of the same four media types, both leading with "themed".

Fix: the PLEX row's foot now carries the ThemerrDB REACH — how much of each
library ThemerrDB's catalogue even covers:

  "W in ThemerrDB · N not in ThemerrDB"   (W = motif_available, N = total − W)

So the top row answers "how much is themed" and the bottom row answers "how much
CAN ThemerrDB ever theme". Distinct cuts, not a duplicate. (On the user's library
the reach is striking — most movies + collections simply aren't in TDB.)

NOTE: W (in-TDB) can be LESS than the top card's themed count — themes can come
from non-TDB sources (U/A/M/P). That's expected and is the whole point; the two
numbers measure different things.
"""
from __future__ import annotations

import re
import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


REPO = Path(__file__).resolve().parent.parent
DASH_HTML = (REPO / "app" / "web" / "templates" / "dashboard.html").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()
NOW = "2026-06-14T00:00:00"
AUTH = {"X-Authentik-Username": "testadmin"}


# ── behavioral: the SSR-baked reach numbers add up ──────────────


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
    init_db(settings.db_path)
    init_auth_schema(settings.db_path)
    create_admin(settings.db_path, username="testadmin", password="testpassword")
    return TestClient(create_app(settings)), settings.db_path


def _theme(conn, tid, tmdb):
    conn.execute(
        "INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source,"
        " last_seen_sync_at, first_seen_sync_at, youtube_url)"
        " VALUES (?, 'movie', ?, ?, 'imdb', ?, ?, 'https://y/watch?v=V')",
        (tid, tmdb, f"M{tmdb}", NOW, NOW))


def _item(conn, *, rk, tid, tmdb, has_theme):
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, media_type, theme_id,"
        " guid_imdb, guid_tmdb, title, year, has_theme, plex_independent_theme,"
        " first_seen_at, last_seen_at)"
        " VALUES (?, '1', 'movie', ?, ?, ?, ?, 2012, ?, 0, ?, ?)",
        (rk, tid, f"tt{tmdb}", tmdb, f"M{tmdb}", has_theme, NOW, NOW))


def _baked(html, stat_id):
    m = re.search(rf'id="{stat_id}">([\d,]+)<', html)
    assert m, f"{stat_id} not baked into the dashboard SSR"
    return int(m.group(1).replace(",", ""))


def test_plex_movies_reach_foot_partitions_the_library(admin_client):
    """not-in-TDB = library total − in-TDB. Seed 3 movies: 2 with a TDB
    theme (in ThemerrDB), 1 with none (not in ThemerrDB)."""
    client, db = admin_client
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime,"
            " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
            " VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        _theme(conn, 1, 101)
        _theme(conn, 2, 102)
        _item(conn, rk="a", tid=1, tmdb=101, has_theme=1)   # in-TDB
        _item(conn, rk="b", tid=2, tmdb=102, has_theme=0)   # in-TDB
        _item(conn, rk="c", tid=None, tmdb=103, has_theme=0)  # not in TDB
        conn.commit()

    html = client.get("/", headers=AUTH).text
    total = _baked(html, "plex-movies-total")
    in_tdb = _baked(html, "plex-movies-motif")
    not_tdb = _baked(html, "plex-movies-not-tdb")
    assert total == 3
    assert in_tdb == 2, "two items resolve to a non-orphan TDB theme"
    assert not_tdb == 1, "not-in-TDB = total − in-TDB"
    assert in_tdb + not_tdb == total, "the reach foot must partition the library"


def test_plex_reach_foot_text_present(admin_client):
    """The rendered PLEX MOVIES card foot reads the reach phrasing, not the
    old 'with theme / ThemerrDB available'."""
    client, db = admin_client
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime,"
            " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
            " VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        _item(conn, rk="a", tid=None, tmdb=101, has_theme=0)
        conn.commit()
    html = client.get("/", headers=AUTH).text
    # v1.24.66: the stat-glyph is now an inline SVG (~470 chars), so widen the
    # window past it to reach the card foot.
    plex_section = html[html.index("// PLEX MOVIES"):html.index("// PLEX MOVIES") + 1500]
    assert "in ThemerrDB" in plex_section
    assert "not in ThemerrDB" in plex_section


# ── source pins: template / SSR / JS all carry the reach shape ──


def test_all_four_plex_cards_use_reach_foot():
    """Every PLEX card (movies/tv/anime/collections) foot uses the reach
    phrasing + a `*-not-tdb` id, and the old foot phrasing is gone from
    the PLEX cards."""
    for slug in ("movies", "tv", "anime", "collections"):
        anchor = DASH_HTML.index(f"// PLEX {slug.upper()}")
        card = DASH_HTML[anchor:DASH_HTML.index("</article>", anchor)]
        assert f'id="plex-{slug}-not-tdb"' in card, slug
        assert "in ThemerrDB" in card and "not in ThemerrDB" in card, slug
        # the bottom-card "with theme" stat moved up to the COVERAGE row.
        assert f'id="plex-{slug}-with-theme"' not in card, slug


def test_ssr_computes_not_in_tdb_as_total_minus_avail():
    """The SSR helper derives not-in-TDB = max(0, total − motif_avail) for
    each of the four card types."""
    for key in ("movies", "tv", "anime", "collections"):
        assert (
            f'state["plex_{key}_not_in_tdb"] = max(' in API_PY
        ), f"missing not_in_tdb derivation for {key}"
    # the field is declared in the safe-default dict too (no None→'—' flash).
    for key in ("movies", "tv", "anime", "collections"):
        assert f'"plex_{key}_not_in_tdb": None,' in API_PY, key


def test_js_hydrates_not_tdb_from_total_minus_motif():
    """renderPlexCoverage + renderPlexAnimeCard write the live not-in-TDB
    count as total − motif_available."""
    assert "$('#plex-movies-not-tdb').textContent = fmt.num(total - motifAvail);" in APP_JS
    assert "$('#plex-tv-not-tdb').textContent = fmt.num(tvTotal - tvMotif);" in APP_JS
    assert "plex-collections-not-tdb" in APP_JS
    assert "collectionsTotal - collectionsMotif" in APP_JS
    # v1.23.90: the anime card moved into renderPlexCoverage, sourced from
    # data.anime (was renderPlexAnimeCard summing /api/sections/coverage).
    assert "plex-anime-not-tdb" in APP_JS
    assert "fmt.num(animeTotal - animeMotif)" in APP_JS
