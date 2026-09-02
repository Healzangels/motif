"""v0.51.308 — deep-link card fixes from the operator's first .306 deploy.

  1. A theme-lost notice for an item REMOVED from Plex deep-linked to a
     raw `404: {"detail":"not found"}` card — api_item 404s when the
     themes row is gone, which is exactly the state the notice
     describes. The card renders a designed NOT IN LIBRARY state now.
  2. Anime click-throughs landed on /tv (the v0.51.209 map defaulted
     every show there). list_notifications LEFT JOINs the section for
     is_anime; the drawer row carries data-anime; openNotifRow and the
     /queue REPROBE OPEN ROW handler route on it.
  3. Deep-link opens rendered posterless cards — every producer passes
     rating_key=None and the hero only fell back to a placement's
     plex_rating_key (absent on P-rows). api_item resolves and returns
     plex_rating_key; the hero chain uses it as the final fallback.
"""
from __future__ import annotations

from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
AUTH = {"X-Authentik-Username": "testadmin"}
NOW = "2026-08-30T12:00:00+00:00"


# ── 2. is_anime flows from the section to the drawer row ─────


def test_list_notifications_carries_is_anime(tmp_path):
    from app.core.db import get_conn, init_db, transaction
    from app.core.notify_inbox import list_notifications, record_notification
    db = tmp_path / "motif.db"
    init_db(db)
    with get_conn(db) as conn, transaction(conn):
        conn.execute(
            """INSERT INTO plex_sections (section_id, title, type, is_anime,
                 is_4k, themes_subdir, included, discovered_at, last_seen_at)
               VALUES ('9', 'Anime', 'show', 1, 0, 'anime', 1, ?, ?)""",
            (NOW, NOW))
        conn.execute(
            """INSERT INTO plex_sections (section_id, title, type, is_anime,
                 is_4k, themes_subdir, included, discovered_at, last_seen_at)
               VALUES ('2', 'TV', 'show', 0, 0, 'tv', 1, ?, ?)""",
            (NOW, NOW))
    record_notification(db, event_kind="new_arrival_themed", severity="info",
                        title="anime row", media_type="tv", tmdb_id=308001,
                        section_id="9")
    record_notification(db, event_kind="new_arrival_themed", severity="info",
                        title="tv row", media_type="tv", tmdb_id=308002,
                        section_id="2")
    record_notification(db, event_kind="download_batch", severity="info",
                        title="digest row")  # no section
    by_title = {n["title"]: n for n in list_notifications(db)}
    assert by_title["anime row"]["is_anime"] is True, (
        "the drawer needs the section's is_anime to route the click-through "
        "to /anime — without it every show lands on /tv")
    assert by_title["tv row"]["is_anime"] is False
    assert by_title["digest row"]["is_anime"] is False, (
        "a NULL-section digest must coalesce to False, not explode the JOIN")


# ── 3. api_item resolves the poster rating_key ───────────────


@pytest.fixture
def client(tmp_path, monkeypatch):
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import get_conn, init_db, transaction
    from app.web.api import create_app
    from fastapi.testclient import TestClient
    (tmp_path / "data").mkdir()
    (tmp_path / "motif.yaml").write_text("paths: {}\n")
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    # v0.51.309 (audit r2): sibling-fixture parity — contain any env-derived
    # Settings() a future code path might construct under create_app.
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    with get_conn(s.db_path) as conn, transaction(conn):
        conn.execute(
            """INSERT INTO plex_sections (section_id, title, type, is_anime,
                 is_4k, themes_subdir, included, discovered_at, last_seen_at)
               VALUES ('1', 'Anime', 'show', 1, 0, 'anime', 1, ?, ?)""",
            (NOW, NOW))
        conn.execute(
            """INSERT INTO themes (media_type, tmdb_id, title,
                 upstream_source, last_seen_sync_at, first_seen_sync_at)
               VALUES ('tv', 308101, 'Strike-ish', 'imdb', ?, ?)""",
            (NOW, NOW))
        conn.execute(
            """INSERT INTO plex_items (rating_key, section_id, media_type,
                 title, guid_tmdb, folder_path, edition_key, has_theme,
                 first_seen_at, last_seen_at)
               VALUES ('4321', '1', 'show', 'Strike-ish', 308101,
                       '/data/anime/S', '', 1, ?, ?)""",
            (NOW, NOW))
        conn.execute(
            """INSERT INTO plex_items (rating_key, section_id, media_type,
                 title, guid_tmdb, folder_path, edition_key, has_theme,
                 first_seen_at, last_seen_at)
               VALUES ('9999', '1', 'show', 'Strike-ish',
                       308101, '/data/anime/S {edition-ext}', 'ext', 1, ?, ?)""",
            (NOW, NOW))
    return TestClient(create_app(s))


def test_api_item_resolves_plex_rating_key_for_rk_less_opens(client):
    r = client.get("/api/items/tv/308101?section_id=1", headers=AUTH)
    assert r.status_code == 200
    assert r.json()["plex_rating_key"] == "4321", (
        "every deep-link producer passes rating_key=None — without the "
        "resolved rk the poster hero renders blank on those opens")


def test_api_item_prefers_the_named_cut(client):
    r = client.get("/api/items/tv/308101?section_id=1&edition_key=ext",
                   headers=AUTH)
    assert r.status_code == 200
    assert r.json()["plex_rating_key"] == "9999"


def test_api_item_echoes_a_passed_rating_key(client):
    # v0.51.309 (audit r2): rk '7777' resolves to NOTHING (no plex_items
    # row), so only a real echo returns it — the first draft passed '4321',
    # which the no-rk fallback would also have produced, so an always-resolve
    # mutant stayed green.
    r = client.get("/api/items/tv/308101?section_id=1&rating_key=7777",
                   headers=AUTH)
    assert r.status_code == 200
    assert r.json()["plex_rating_key"] == "7777"


def test_api_item_unknown_title_still_404s(client):
    r = client.get("/api/items/tv/999999", headers=AUTH)
    assert r.status_code == 404


# ── wiring pins (JS) ─────────────────────────────────────────


def test_drawer_row_carries_data_anime():
    blk = APP_JS[APP_JS.index("function rowHtml("):
                 APP_JS.index("function renderEmpty(")]
    assert "n.is_anime" in blk and 'data-anime="1"' in blk


def test_open_notif_row_routes_anime_before_movie():
    i = APP_JS.index("function openNotifRow(")
    blk = APP_JS[i:APP_JS.index("window.location.href", i)]
    assert "row.dataset.anime === '1' ? '/anime'" in blk, (
        "the v0.51.209 map defaulted every show to /tv — anime cards "
        "opened on the wrong page (posterless, no row behind them)")
    # v0.51.309 (audit r2): ORDER is the invariant — /movies excludes
    # is_anime sections while /anime takes movie-typed anime rows, so the
    # movie short-circuit misrouted anime FILMS.
    assert (blk.index("row.dataset.anime === '1'")
            < blk.index("row.dataset.mt === 'movie'")), (
        "anime must be tested before the movie short-circuit")
    # v0.51.311 (review): the /tv DEFAULT is load-bearing too — with only
    # membership + order pinned, a '/movies' tail passed the whole suite.
    assert "row.dataset.mt === 'movie' ? '/movies' : '/tv';" in blk


def test_queue_open_row_routes_anime_before_movie():
    blk = APP_JS[APP_JS.index("const openBtn = e.target.closest"):
                 APP_JS.index("window.location.href = `${tabPath}")]
    assert "openBtn.dataset.anime === '1' ? '/anime'" in blk
    assert (blk.index("openBtn.dataset.anime === '1'")
            < blk.index("mt === 'movie'")), (
        "anime before the movie short-circuit (movie-typed anime sections "
        "live on /anime)")
    # v0.51.311 (review): pin the /tv default tail as well.
    assert "mt === 'movie' ? '/movies' : '/tv';" in blk
    # v0.51.309 (audit r2): the renderer must EMIT the attr the handler
    # reads — the first draft matched the dead `const animeAttr` declaration
    # through a backward fixed window, so deleting the template's
    # ${animeAttr} survived every test.
    j = APP_JS.index("const sectionAttr = det.section_id")
    tmpl = APP_JS[j:APP_JS.index("// OPEN ROW</button>", j)]
    assert "${animeAttr}" in tmpl


def test_info_card_renders_not_in_library_on_404():
    i = APP_JS.index("async function openInfoDialog(")
    blk = APP_JS[i:APP_JS.index("const t = data.theme || {};", i)]
    g = blk.index("e.status === 404")
    seg = blk[g:blk.index("accent-red", g)]
    assert "NOT IN LIBRARY" in seg and "return;" in seg, (
        "a 404 is the removed-item STATE the theme-lost notice describes — "
        "it must render a designed message, not the raw detail JSON")


def test_poster_hero_falls_back_to_the_resolved_rk():
    i = APP_JS.index("const posterRk = String(ratingKey")
    blk = APP_JS[i:APP_JS.index("posterImgHtml", i)]
    assert "data.plex_rating_key" in blk, (
        "rk-less deep-link opens on P-rows have no placements — without the "
        "API-resolved fallback the hero renders blank")


def test_reprobe_event_detail_stamps_is_anime():
    api = (REPO / "app" / "web" / "api.py").read_text()
    assert "ps.is_anime AS section_is_anime" in api
    i = api.index('"is_anime": (')
    assert 'r_orig["section_is_anime"]' in api[i:api.index('"title"', i)]


def test_v0_51_308_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert "0.51.308: " in init_py
