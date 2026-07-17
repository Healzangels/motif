"""v0.51.93 — collections // ALL: the owning library NAME trails the collection.

v0.51.92 first shipped this as a type-colored dot; the user didn't like the dot,
so v0.51.93 shows the owning Plex library's NAME after the collection title (a
neutral .lib-tag pill) on the row + keeps the library name on the info card's
section chip. Same goal: a name like "Action" that recurs once per library is
distinguishable. Collections only.

Behavioral: three same-named collections across a movie / show / anime library
each carry their own section_title in the /api/library response, so the tag
labels each row with its real library ("Movies" / "TV Shows" / "Anime", and
"4K Movies" vs "Movies" too — which a type-color couldn't separate). Source
guards pin the row render, the info card, the CSS, the glossary + legend, and
the removal of the dot + its now-dead section_type/section_is_anime columns.
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
BASE_HTML = (REPO / "app" / "web" / "templates" / "base.html").read_text()
LIBRARY_HTML = (REPO / "app" / "web" / "templates" / "library.html").read_text()

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
    now = "2026-07-06T00:00:00"
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
        for i, (sid, *_rest) in enumerate(sections, start=1):
            conn.execute(
                "INSERT INTO plex_items (rating_key, section_id, media_type, "
                "  title, year, guid_tmdb, folder_path, first_seen_at, "
                "  last_seen_at) VALUES (?,?, 'collection', 'Action', NULL, ?, "
                "  '', ?, ?)",
                (f"rk{i}", sid, str(8000 + i), now, now))
        conn.commit()


def test_library_response_labels_each_collection_with_its_library(admin_client):
    client, db = admin_client
    _seed_three_action_collections(db)
    r = client.get("/api/library?tab=collections&per_page=50", headers=AUTH)
    assert r.status_code == 200, r.text
    rows = r.json()["items"]
    # every same-named "Action" collection surfaces, each with its own library.
    by_lib = {it["section_title"]: it for it in rows}
    assert set(by_lib) == {"Movies", "TV Shows", "Anime"}, (
        f"each library's 'Action' collection must carry its section_title: "
        f"{list(by_lib)}")
    assert all(it["plex_title"] == "Action" for it in rows)


def test_select_dropped_the_dead_type_and_anime_aliases():
    # v0.51.93 removed the dot → ps.type/ps.is_anime are no longer selected.
    assert "ps.type AS section_type" not in API_PY
    assert "ps.is_anime AS section_is_anime" not in API_PY


# ── source guards: the dot is gone, the name tag is in ───────────────────────


def test_no_lib_dot_anywhere():
    for src in (APP_JS, CSS, BASE_HTML, LIBRARY_HTML):
        assert "lib-dot" not in src, "the v0.51.92 library dot must be fully removed"


def test_row_renders_library_name_tag_after_the_name():
    i = APP_JS.index("const libTag = (it.plex_media_type === 'collection'")
    block = APP_JS[i:i + 260]
    assert "it.section_title" in block  # gated on having a library name
    assert 'class="lib-tag">${htmlEscape(it.section_title)}</span>' in block
    # rendered AFTER the title-cell name span (trailing the collection title).
    name = APP_JS.index('<span class="title-cell-name">${htmlEscape(it.plex_title)}</span>')
    assert APP_JS.index("${libTag}", name) > name


def test_info_card_section_chip_shows_library_name_no_dot():
    i = APP_JS.index('info-scope-chip info-scope-chip-section')
    block = APP_JS[i:i + 200]
    assert "${htmlEscape(sc.section_title)}</span>" in block
    assert "infoLibDot" not in APP_JS


def test_lib_tag_css_is_a_neutral_pill():
    block = CSS[CSS.index(".lib-tag {"):CSS.index(".lib-tag {") + 260]
    assert "var(--fg-dim)" in block           # muted text
    assert "border: 1px solid var(--line)" in block  # neutral box (not accent)
    assert "border-radius: 50%" not in block  # not a dot


# ── glossary + legend document the name tag (+ still the 4K chip) ────────────


def test_glossary_documents_library_name_tag_and_4k():
    i = BASE_HTML.index("// TITLE — badges beside the name")
    # v0.51.200: widened 1000 → 1800 — the 3-state loudness marker added two rows
    # (outlier + raw, long definitions) between the 4K chip and the lib-tag row.
    sec = BASE_HTML[i:i + 1800]
    assert 'class="tier-badge tier-badge-4k">4K<' in sec
    assert 'class="lib-tag">Movies</span>' in sec


def test_legend_documents_name_tag_on_collections_and_4k_everywhere():
    legend = LIBRARY_HTML[LIBRARY_HTML.index("library-legend-body"):
                          LIBRARY_HTML.index("library-legend-foot")]
    assert 'class="tier-badge tier-badge-4k">4K<' in legend
    ti = legend.index("TITLE — library &amp; variant")
    tail = legend[ti:]
    gate = tail.index('{% if tab == "collections" %}')
    endgate = tail.index("{% endif %}", gate)
    assert 'class="lib-tag">Movies</span>' in tail[gate:endgate]
    assert "tier-badge-4k" in tail[:gate]  # 4K sits outside the collections gate
