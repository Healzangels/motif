"""v1.23.90 — anime-section FILMS count on the ANIME card, not MOVIES.

the user: "wondering why the anime numbers don't match, 1,244 vs 1,341." The
dashboard split anime-section films (media_type='movie' in an is_anime=1 section
— anime movies / OVAs) inconsistently with the library tabs. The library is a
clean partition (movies tab = is_anime=0 movies; anime tab = is_anime=1 movie+
show; v1.19.28), but three dashboard sources each split differently: the
// ANIME THEMED card (/api/coverage/plex `anime` array) was show-only (1,244),
the // PLEX ANIME card (/api/sections/coverage section totals) counted EVERY row
in anime sections incl. collections (1,341), and _stats_sync (SSR) was show-only
for anime + un-gated for movies (anime films counted on the MOVIES card).

Fix: every dashboard anime/movies source matches the library partition — movies
aggregates gate is_anime=0, anime aggregates take media_type IN ('show','movie'),
and the PLEX ANIME card is sourced from /api/coverage/plex's anime array (like
its PLEX MOVIES/TV siblings) instead of the collection-contaminated per-section
sum. Anime films move MOVIES→ANIME; the PLEX LIBRARY total is unchanged.
"""
from __future__ import annotations

import re
import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
NOW = "2026-06-19T00:00:00"
AUTH = {"X-Authentik-Username": "testadmin"}


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


def _section(conn, sid, typ, is_anime):
    conn.execute(
        "INSERT INTO plex_sections (section_id, title, type, is_anime, is_4k,"
        " themes_subdir, included, discovered_at, last_seen_at)"
        " VALUES (?, ?, ?, ?, 0, ?, 1, ?, ?)",
        (sid, f"S{sid}", typ, is_anime, f"sub{sid}", NOW, NOW))


def _item(conn, *, rk, sid, mt, tmdb):
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, media_type, theme_id,"
        " guid_imdb, guid_tmdb, title, year, has_theme, plex_independent_theme,"
        " edition_key, first_seen_at, last_seen_at)"
        " VALUES (?, ?, ?, NULL, NULL, ?, ?, 2017, 0, 0, '', ?, ?)",
        (rk, sid, mt, tmdb, f"T{rk}", NOW, NOW))


def _baked(html, stat_id):
    m = re.search(rf'id="{stat_id}">([\d,]+)<', html)
    assert m, f"{stat_id} not baked into the dashboard SSR"
    return int(m.group(1).replace(",", ""))


# ── behavioral: the partition holds at the SSR aggregate ─────────


def test_anime_film_counts_as_anime_not_movies(admin_client):
    """An anime-section film (is_anime=1 movie) lands on the ANIME card, not
    MOVIES — matching the library partition; the anime collection stays on the
    collections card (NOT folded into the anime total)."""
    client, db = admin_client
    with sqlite3.connect(db) as conn:
        _section(conn, "1", "movie", 0)   # regular movies
        _section(conn, "2", "movie", 1)   # anime films
        _section(conn, "3", "show", 1)    # anime shows
        _item(conn, rk="m1", sid="1", mt="movie", tmdb=101)        # regular movie
        _item(conn, rk="a1", sid="2", mt="movie", tmdb=201)        # anime FILM
        _item(conn, rk="a2", sid="3", mt="show", tmdb=202)         # anime show
        _item(conn, rk="c1", sid="2", mt="collection", tmdb=203)   # anime collection
        conn.commit()

    html = client.get("/", headers=AUTH).text
    # MOVIES card = non-anime movies ONLY (the anime film is excluded).
    assert _baked(html, "plex-movies-total") == 1, (
        "v1.23.90: anime-section films must NOT count on the MOVIES card")
    # ANIME card = anime film + anime show (the collection is excluded — matches
    # the ANIME library tab, which is media_type IN ('movie','show')).
    assert _baked(html, "plex-anime-total") == 2, (
        "v1.23.90: the ANIME card counts movie+show in anime sections")
    # the anime collection still lands on the collections card.
    assert _baked(html, "plex-collections-total") == 1


def test_library_total_is_preserved(admin_client):
    """The anime film MOVED from MOVIES to ANIME — the PLEX LIBRARY sum
    (movies+tv+anime+collections) is unchanged, no double-count, no drop."""
    client, db = admin_client
    with sqlite3.connect(db) as conn:
        _section(conn, "1", "movie", 0)
        _section(conn, "2", "movie", 1)
        _item(conn, rk="m1", sid="1", mt="movie", tmdb=101)
        _item(conn, rk="a1", sid="2", mt="movie", tmdb=201)
        conn.commit()
    html = client.get("/", headers=AUTH).text
    movies = _baked(html, "plex-movies-total")
    anime = _baked(html, "plex-anime-total")
    assert movies == 1 and anime == 1, "one regular movie + one anime film"
    assert movies + anime == 2, "no row lost or double-counted in the move"


# ── source pins: all three dashboard sources match the partition ──


def test_coverage_plex_anime_includes_films_movies_excludes_them():
    anime_q = API_PY[API_PY.index("anime_rows = conn.execute("):]
    anime_q = anime_q[:anime_q.index(".fetchall()")]
    assert "pi.media_type IN ('show', 'movie')" in anime_q
    assert "ps.is_anime = 1" in anime_q
    movies_q = API_PY[API_PY.index("movies_rows = conn.execute("):]
    movies_q = movies_q[:movies_q.index(".fetchall()")]
    assert "pi.media_type = 'movie'" in movies_q
    assert "ps.is_anime = 0" in movies_q


def test_stats_sync_anime_aggregates_widened_movies_gated():
    # all four anime aggregates (total/with_theme/motif_avail/ready) take films.
    assert API_PY.count(
        "pi.media_type IN ('show', 'movie') AND ps.is_anime=1") == 4
    # movies total/with_theme gate is_anime=0.
    assert ("SUM(CASE WHEN pi.media_type='movie' AND ps.is_anime=0 THEN 1 ELSE 0 END)"
            in API_PY)
    # the anime "ready" placement check matches movie OR tv per row (anime films
    # place under media_type='movie', shows under 'tv').
    assert "WHERE p.media_type = CASE WHEN pi.media_type='movie'" in API_PY


def test_plex_anime_card_sourced_from_coverage_plex_data():
    # renderPlexAnimeCard (the /api/sections/coverage sum) is retired; the card
    # is now populated in renderPlexCoverage from data.anime.
    assert "function renderPlexAnimeCard(" not in APP_JS
    assert "renderPlexAnimeCard(" not in APP_JS  # no dangling call either
    assert "const animeItems = data.anime" in APP_JS
    assert "const animeNotTdbEl = $('#plex-anime-not-tdb');" in APP_JS
    assert "fmt.num(animeTotal - animeMotif)" in APP_JS


def test_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
