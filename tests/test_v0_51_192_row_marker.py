"""v0.51.192 — the library title-cell "leveled" marker + a compact audition note.

Two changes, both on the loudness card surface:

1. A row MARKER (meter glyph beside the title) for norm_state='normalized' rows. Its
   whole existence depends on /api/library carrying norm_state, and the library query
   reads every lf column through the lf_e/lf_g edition-fallback COALESCE pair (api.py
   ~3492). A bare lf_e read would blank the marker for every row served from the ''
   fallback edition — the edition-bleed class this codebase keeps paying for. So the
   load-bearing test drives BOTH the edition-own and the ''-fallback path.

2. The audition note (v0.51.191 preview) is shortened so no state wraps. The operator
   caught the old 53-char stopped message wrapping to a second line while the ~49-char
   playing note did not, so the audition row grew taller only when stopped — a layout
   jump. Verified in a real-stylesheet harness (23px one-line vs 41px wrapped); pinned
   here as the string contract.

Colour discipline: the marker ENCODES meaning, so it's FIXED across themes (theme
SPLIT). Cyan specifically, because --ok and --src-t are the same green and --amber is
the 4K badge in this very cell.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from starlette.testclient import TestClient

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()
BASE_HTML = (REPO / "app" / "web" / "templates" / "base.html").read_text()
LIB_HTML = (REPO / "app" / "web" / "templates" / "library.html").read_text()

AUTH = {"X-Authentik-Username": "testadmin"}
NOW = "2026-07-17T00:00:00"


@pytest.fixture
def admin_client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web.api import create_app
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    with sqlite3.connect(s.db_path) as c:
        c.execute("INSERT INTO plex_sections (section_id, title, type, is_anime, is_4k, "
                  " themes_subdir, included, discovered_at, last_seen_at) "
                  "VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        c.commit()
    return TestClient(create_app(s)), s.db_path


def _seed(db, *, tmdb_id, rk, edition_key, norm_state, lf_edition):
    """A themed row. lf_edition sets the local_files.edition_key, so passing '' while
    plex_items.edition_key is non-empty forces the lf_g '' fallback branch."""
    with sqlite3.connect(db) as c:
        c.execute("INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source, "
                  " last_seen_sync_at, first_seen_sync_at) "
                  "VALUES (?, 'movie', ?, ?, 'imdb', ?, ?)",
                  (tmdb_id, tmdb_id, f"Movie{tmdb_id}", NOW, NOW))
        c.execute("INSERT INTO plex_items (rating_key, section_id, media_type, theme_id, "
                  " guid_tmdb, edition_key, title, year, has_theme, folder_path, "
                  " first_seen_at, last_seen_at) "
                  "VALUES (?, '1', 'movie', ?, ?, ?, ?, 2020, 1, '/data/movies/x', ?, ?)",
                  (rk, tmdb_id, tmdb_id, edition_key, f"Movie{tmdb_id}", NOW, NOW))
        c.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, theme_id, "
                  " edition_key, file_path, downloaded_at, source_video_id, provenance, "
                  " source_kind, norm_state) "
                  "VALUES ('movie', ?, '1', ?, ?, ?, ?, 'vid', 'auto', 'url', ?)",
                  (tmdb_id, tmdb_id, lf_edition, f"movies/{tmdb_id}/theme.mp3", NOW,
                   norm_state))
        c.commit()


def _row(client, tmdb_id):
    r = client.get("/api/library?tab=movies&per_page=50", headers=AUTH)
    assert r.status_code == 200, r.text
    for it in r.json()["items"]:
        if it.get("theme_tmdb") == tmdb_id or it.get("guid_tmdb") == tmdb_id:
            return it
    raise AssertionError(f"tmdb {tmdb_id} not in /api/library")


def test_library_carries_norm_state_for_the_marker(admin_client):
    c, db = admin_client
    _seed(db, tmdb_id=1, rk="r1", edition_key="", norm_state="normalized", lf_edition="")
    _seed(db, tmdb_id=2, rk="r2", edition_key="", norm_state=None, lf_edition="")
    assert _row(c, 1)["norm_state"] == "normalized", "marked row must report normalized"
    assert not _row(c, 2)["norm_state"], "raw row must not claim to be leveled"


def test_norm_state_survives_the_edition_fallback_join(admin_client):
    """The bleed guard: plex_items has a non-'' edition but the local_files row is on the
    '' fallback edition, so norm_state MUST come through lf_g. Reading only lf_e would
    blank the marker for every fallback edition — silently."""
    c, db = admin_client
    _seed(db, tmdb_id=3, rk="r3", edition_key="Directors Cut",
          norm_state="normalized", lf_edition="")
    assert _row(c, 3)["norm_state"] == "normalized", (
        "norm_state lost across the lf_g '' fallback — a bare lf_e read would do this")


def test_query_reads_norm_state_through_the_coalesce_pair():
    """Source guard mirroring the other lf columns: never a bare lf.norm_state."""
    assert "COALESCE(lf_e.norm_state, lf_g.norm_state) AS norm_state" in API_PY


def test_marker_renders_only_for_normalized_and_is_a_trailing_badge():
    assert "it.norm_state === 'normalized'" in APP_JS
    assert "tier-badge tier-badge-lvl" in APP_JS
    # it must be a trailing state badge (sibling of 4K), NOT pushed into titleGlyphs —
    # that slot is the strict one-glyph attention hierarchy (v1.12.106).
    assert "titleGlyphs.push" not in APP_JS.split("const lvlTag")[1].split("return `")[0]


def test_marker_colour_is_fixed_cyan_not_themed_nor_source_green():
    idx = APP_CSS.index(".tier-badge-lvl {")
    rule = APP_CSS[idx:APP_CSS.index("}", idx)]   # this rule only, not the next one
    assert "color: var(--cyan)" in rule, "must be cyan — green would read as SRC=T"
    assert "--amber" not in rule, "amber is the 4K badge in the same cell"
    assert "--accent" not in rule, "must not follow the theme accent — encoded meaning"


def test_glossary_documents_the_marker_on_BOTH_surfaces():
    """Missing either surface is the exact legend drift that cost six tags (v1.23.50-56).
    The in-context legend (library.html) and the full glossary (base.html) both decode
    the row's badges, so both must carry the new one."""
    assert "tier-badge-lvl" in BASE_HTML, "full glossary (base.html) missing the marker"
    assert "tier-badge-lvl" in LIB_HTML, "in-context legend (library.html) missing it"


def test_audition_note_states_are_short_enough_not_to_wrap():
    """The reported bug: the stopped note was longer than the playing note and wrapped,
    growing the audition row only when stopped. All states now sit on one line."""
    assert "stopped · player restored" in APP_JS
    assert "playing at ${loudTarget.toFixed(1)} LUFS · nothing written" in APP_JS
    # the specific 53-char string that wrapped must be gone.
    assert "the player is back to the real file" not in APP_JS
