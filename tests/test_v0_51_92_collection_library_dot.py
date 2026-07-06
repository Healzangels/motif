"""v0.51.92 — collections // ALL view: a per-library dot on the row + info card.

the user: in the collections // ALL view several libraries can each own a
collection with the SAME name (e.g. "Action" appears 3×, one per library), and
nothing on the row told them apart. This tag adds a small colored dot keyed to
the owning library's TYPE (movie→amber / show→blue / anime→magenta, reusing the
dashboard per-library accents) with the library name on hover, on the collection
row AND the info card's section chip. Collections only — the movies/tv/anime
tabs are single-type so the dot would be noise.

Behavioral: three same-named collections across a movie / show / anime library
carry distinct section_type + section_is_anime + section_title in the
/api/library response, so the JS can color each dot differently. Source guards
pin the row render gate + color logic, the info-card dot, and the CSS tokens.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()

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
    db = settings.db_path
    init_db(db)
    init_auth_schema(db)
    create_admin(db, username="testadmin", password="testpassword")
    return TestClient(create_app(settings)), db


def _seed_three_action_collections(db: Path) -> None:
    now = "2026-07-05T00:00:00"
    # (section_id, title, type, is_anime)
    sections = [
        ("1", "Movies", "movie", 0),
        ("2", "TV Shows", "show", 0),
        ("3", "Anime", "show", 1),
    ]
    with sqlite3.connect(db) as conn:
        for sid, title, typ, is_anime in sections:
            conn.execute(
                "INSERT INTO plex_sections (section_id, title, type, is_anime, "
                "  is_4k, themes_subdir, included, discovered_at, last_seen_at) "
                "VALUES (?,?,?,?,0,?,1,?,?)",
                (sid, title, typ, is_anime, f"sub{sid}", now, now))
        # A collection named "Action" in EACH library — the user's exact repro.
        for i, (sid, *_rest) in enumerate(sections, start=1):
            tmdb = 8000 + i
            conn.execute(
                "INSERT INTO plex_items (rating_key, section_id, media_type, "
                "  title, year, guid_tmdb, folder_path, first_seen_at, "
                "  last_seen_at) VALUES (?,?, 'collection', 'Action', NULL, ?, "
                "  '', ?, ?)",
                (f"rk{i}", sid, str(tmdb), now, now))
        conn.commit()


def test_library_response_carries_section_type_and_anime_per_collection(
        admin_client):
    client, db = admin_client
    _seed_three_action_collections(db)
    r = client.get("/api/library?tab=collections&per_page=50", headers=AUTH)
    assert r.status_code == 200, r.text
    rows = {it["section_title"]: it for it in r.json()["items"]}
    assert set(rows) == {"Movies", "TV Shows", "Anime"}, (
        "all three same-named 'Action' collections must surface, keyed by "
        f"library: {list(rows)}")
    # Each row exposes the fields the JS needs to color the dot.
    assert rows["Movies"]["section_type"] == "movie"
    assert rows["Movies"]["section_is_anime"] == 0
    assert rows["TV Shows"]["section_type"] == "show"
    assert rows["TV Shows"]["section_is_anime"] == 0
    # Anime is a show library flagged is_anime → the anime accent wins.
    assert rows["Anime"]["section_is_anime"] == 1
    # And they all share the same display title, which is why the dot matters.
    assert all(it["plex_title"] == "Action" for it in rows.values())


def test_select_exposes_section_type_and_anime():
    # The /api/library SELECT must alias the owning library's type + anime flag.
    assert "ps.type AS section_type" in API_PY
    assert "ps.is_anime AS section_is_anime" in API_PY


# ── source guards: row render ────────────────────────────────────────────────


def test_row_libdot_gated_on_collection_with_type_color():
    i = APP_JS.index("const libDot = it.plex_media_type === 'collection'")
    block = APP_JS[i:i + 400]
    # anime wins, then movie, else tv — mirrors the section tab partition.
    assert "it.section_is_anime ? 'lib-dot-anime'" in block
    assert "it.section_type === 'movie' ? 'lib-dot-movies' : 'lib-dot-tv'" in block
    # tooltip carries the library name.
    assert "title=\"${htmlEscape(it.section_title || 'library')}\"" in block
    # non-collection rows get no dot (the ternary's else branch).
    assert "? `<span class=\"lib-dot" in block
    assert ": '';" in block


def test_row_template_renders_libdot():
    assert "${libDot}" in APP_JS


# ── source guards: info card ─────────────────────────────────────────────────


def test_info_card_libdot_for_collections():
    i = APP_JS.index("const infoLibDot = (data.theme && data.theme.media_type "
                     "=== 'collection')")
    block = APP_JS[i:i + 300]
    assert "sc.is_anime ? 'lib-dot-anime'" in block
    assert "sc.type === 'movie' ? 'lib-dot-movies' : 'lib-dot-tv'" in block
    # the dot is injected into the section chip content.
    assert "${infoLibDot}${htmlEscape(sc.section_title)}" in APP_JS


# ── source guards: CSS uses the dashboard per-library accent tokens ──────────


def test_libdot_css_uses_dashboard_accent_tokens():
    assert ".lib-dot-movies { background: var(--dash-movies-color); }" in CSS
    assert ".lib-dot-tv     { background: var(--dash-tv-color); }" in CSS
    assert ".lib-dot-anime  { background: var(--dash-anime-color); }" in CSS
    # the base dot exists and is round.
    base = CSS[CSS.index(".lib-dot {"):CSS.index(".lib-dot {") + 200]
    assert "border-radius: 50%" in base


# ── glossary + legend document the new dot AND the 4K chip (reuse real classes) ─

BASE_HTML = (REPO / "app" / "web" / "templates" / "base.html").read_text()
LIBRARY_HTML = (REPO / "app" / "web" / "templates" / "library.html").read_text()


def test_glossary_documents_4k_and_library_dot():
    # New "// TITLE" section in the // GLOSSARY dialog, reusing the real row
    # classes (v1.23.56 reuse-don't-mirror) — not a hand-mirrored palette.
    i = BASE_HTML.index("// TITLE — badges beside the name")
    sec = BASE_HTML[i:i + 1400]
    assert 'class="tier-badge tier-badge-4k">4K<' in sec, (
        "glossary must decode the 4K library chip via the real tier-badge class")
    for cls in ("lib-dot lib-dot-movies", "lib-dot lib-dot-tv",
                "lib-dot lib-dot-anime"):
        assert f'class="{cls}"' in sec, (
            f"glossary must decode the collections library dot via {cls}")


def test_legend_documents_4k_always_and_dot_on_collections():
    legend = LIBRARY_HTML[LIBRARY_HTML.index("library-legend-body"):
                          LIBRARY_HTML.index("library-legend-foot")]
    # 4K decode is present on every tab (a 4K library can back any tab).
    assert 'class="tier-badge tier-badge-4k">4K<' in legend
    # the library dot rows are gated to the collections tab only.
    ti = legend.index("TITLE — library &amp; variant")
    tail = legend[ti:]
    gate = tail.index('{% if tab == "collections" %}')
    endgate = tail.index("{% endif %}", gate)
    coll_block = tail[gate:endgate]
    for cls in ("lib-dot lib-dot-movies", "lib-dot lib-dot-tv",
                "lib-dot lib-dot-anime"):
        assert f'class="{cls}"' in coll_block, (
            f"collections legend must decode {cls}")
    # 4K sits OUTSIDE the collections gate (shown on all tabs).
    assert 'tier-badge-4k' in tail[:gate]
