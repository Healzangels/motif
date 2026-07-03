"""v0.51.21 — the // ALL library chip + a wider RECENTLY ADDED carousel.

the user:
  1. "In all our sections, Movies, TV, Anime, Collections can we add an All
     button in front of standard and 4k if both exist that is both 4k and
     standard combined or in the cases of collections all the different
     collections in one."
  2. "could we extend the range of which recently added is showing by a
     little bit so we see more in the carousel."

Feature 1 — the // ALL chip:
  * movies/tv/anime: a new resolution mode that unions the standard AND 4K
    sections (shown only when BOTH exist — `has_both`). Backend: a new
    `all_res` param on /api/library + `_library_main_query` that SKIPS the
    `ps.is_4k = ?` narrowing so both variants' plex_items render together.
  * collections: ALL = every managed section's collections at once
    (all_res forces section_id='' — the union the backend already supported;
    v1.18.18 only removed the *chip*, which the user is now re-adding).
  * ALL is opt-in: STANDARD stays the movies/tv/anime default; collections
    keep the first-section default. The choice persists per tab.

Feature 2 — the carousel window: _recently_placed_sync LIMIT 24 → 40.
"""
from __future__ import annotations

import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

from starlette.testclient import TestClient


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from app.core.db import init_db  # noqa: E402

NOW = datetime.now(timezone.utc).isoformat(timespec="seconds")
AUTH = {"X-Authentik-Username": "testadmin"}
API_PY = (REPO / "app" / "web" / "api.py").read_text()
LIB_HTML = (REPO / "app" / "web" / "templates" / "library.html").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def _section(c, sid, title, typ, *, is_4k=0, is_anime=0):
    c.execute(
        "INSERT INTO plex_sections (section_id, title, type, is_anime, is_4k,"
        " themes_subdir, included, discovered_at, last_seen_at) "
        "VALUES (?,?,?,?,?,?,1,?,?)",
        (sid, title, typ, is_anime, is_4k, title.lower().replace(" ", "-"),
         NOW, NOW))


def _item(c, *, rk, section, mt, title):
    c.execute(
        "INSERT INTO plex_items (rating_key, section_id, media_type, title,"
        " year, edition_key, has_theme, first_seen_at, last_seen_at) "
        "VALUES (?,?,?,?,'2001','',0,?,?)",
        (rk, section, mt, title, NOW, NOW))


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
        # A standard + a 4K movie section, each with one item.
        _section(c, "1", "Movies", "movie", is_4k=0)
        _section(c, "5", "4K Movies", "movie", is_4k=1)
        _item(c, rk="rk-std", section="1", mt="movie", title="Std Only")
        _item(c, rk="rk-4k", section="5", mt="movie", title="FourK Only")
        # Two sections that each own a collection (for the collections ALL).
        _item(c, rk="rk-col-a", section="1", mt="collection", title="Coll A")
        _item(c, rk="rk-col-b", section="5", mt="collection", title="Coll B")
        c.commit()
    tc = TestClient(create_app(s))
    tc.motif_db = s.db_path  # stash for tests that inspect the DB directly
    return tc


def _rks(resp):
    return {it["rating_key"] for it in resp.json()["items"]}


# ── #1 movies: ALL unions standard + 4K ───────────────────────


def test_movies_standard_shows_only_standard(client):
    resp = client.get("/api/library?tab=movies&fourk=false", headers=AUTH)
    assert resp.status_code == 200
    assert _rks(resp) == {"rk-std"}


def test_movies_fourk_shows_only_fourk(client):
    resp = client.get("/api/library?tab=movies&fourk=true", headers=AUTH)
    assert _rks(resp) == {"rk-4k"}


def test_movies_all_res_unions_both(client):
    resp = client.get("/api/library?tab=movies&all_res=true", headers=AUTH)
    assert resp.status_code == 200
    assert _rks(resp) == {"rk-std", "rk-4k"}, (
        "v0.51.21: // ALL must combine the standard AND 4K sections into one "
        "view")


def test_all_res_ignores_fourk(client):
    # all_res wins even if fourk is also sent — still both.
    resp = client.get("/api/library?tab=movies&all_res=true&fourk=true",
                      headers=AUTH)
    assert _rks(resp) == {"rk-std", "rk-4k"}


# ── #1 collections: ALL unions every section's collections ────


def test_collections_specific_section_scopes(client):
    resp = client.get("/api/library?tab=collections&section_id=1", headers=AUTH)
    assert _rks(resp) == {"rk-col-a"}


def test_collections_all_res_unions_all_sections(client):
    resp = client.get("/api/library?tab=collections&all_res=true", headers=AUTH)
    assert _rks(resp) == {"rk-col-a", "rk-col-b"}, (
        "v0.51.21: collections // ALL = every managed section's collections")


# ── has_both drives the resolution ALL chip's visibility ──────


def test_resolution_state_reports_has_both(client):
    # Both a standard and a 4K movie section exist → has_both True; the SSR
    # ALL chip is shown (not display:none) on /movies.
    html = client.get("/movies", headers=AUTH).text
    i = html.index('data-allres="1"')
    chip = html[i:html.index("</button>", i)]
    assert 'style="display:none"' not in chip, (
        "v0.51.21: the resolution ALL chip must be visible when both "
        "standard and 4K sections exist")


def test_resolution_state_hides_all_when_single_variant(tmp_path, monkeypatch):
    # Only a standard TV section → has_both False → the ALL chip is hidden.
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
        _section(c, "2", "TV Shows", "show", is_4k=0)  # standard only
        c.commit()
    cl = TestClient(create_app(s))
    html = cl.get("/tv", headers=AUTH).text
    i = html.index('data-allres="1"')
    chip = html[i:html.index("</button>", i)]
    assert 'style="display:none"' in chip, (
        "v0.51.21: no 4K TV section → the ALL chip must be hidden")


# ── refresh enumerates BOTH sections in ALL mode ──────────────


def test_refresh_all_res_enqueues_both_sections(client):
    r = client.post("/api/library/refresh",
                    json={"tab": "movies", "all_res": True}, headers=AUTH)
    assert r.status_code == 200
    # Both the standard (1) and 4K (5) movie sections should get a plex_enum.
    body = client.get("/api/library?tab=movies&all_res=true", headers=AUTH)
    assert body.status_code == 200  # sanity: endpoint still serves
    # Inspect the queued jobs directly (db path stashed by the fixture).
    import json
    with sqlite3.connect(client.motif_db) as c:
        c.row_factory = sqlite3.Row
        rows = c.execute(
            "SELECT payload FROM jobs WHERE job_type='plex_enum'").fetchall()
    secs = {json.loads(r["payload"]).get("section_id") for r in rows}
    scopes = {json.loads(r["payload"]).get("scope") for r in rows}
    assert {"1", "5"} <= secs, (
        "v0.51.21: an ALL refresh must enqueue BOTH the standard and 4K "
        "movie sections")
    assert "movies-all" in scopes


# ── #2 carousel window widened 24 → 40 ────────────────────────


def test_recently_placed_limit_widened():
    i = API_PY.index("def _recently_placed_sync(")
    body = API_PY[i:i + 3000]
    assert "LIMIT 40" in body, "v0.51.21: carousel window 24 → 40"
    assert "LIMIT 24" not in body


# ── source pins: chips + JS wiring ────────────────────────────


def test_template_has_all_chips_both_branches():
    assert LIB_HTML.count('data-allres="1"') >= 2  # resolution + collections
    # resolution ALL gated on has_both; collections ALL gated on >1 section.
    assert "_res.has_both" in LIB_HTML
    assert "_sec.sections|length > 1" in LIB_HTML


def test_js_all_res_state_and_handlers():
    assert "allRes: false," in APP_JS  # libraryState field
    assert "document.querySelectorAll('.chips [data-allres]')" in APP_JS
    # loadLibrary sends all_res + omits section_id in ALL mode.
    assert "if (libraryState.allRes) params.set('all_res', 'true');" in APP_JS
    # refresh POST carries all_res.
    assert "all_res: !!libraryState.allRes," in APP_JS
