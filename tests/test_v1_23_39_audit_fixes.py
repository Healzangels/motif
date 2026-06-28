"""v1.23.39 — holistic-audit fixes for two regressions in v1.23.34-38.

F1 (HIGH, app.js): the v1.23.38 `isAddable` predicate (in loadCoverage) read
`TDB_DEAD_FAILURES_GLOBAL`, but that Set was declared inside loadLibrary() — a
DIFFERENT function. On any coverage row that's a ready-candidate with a truthy
failure_kind (exactly the dead-URL rows v1.23.38 targets), the `&&` reached the
out-of-scope reference → ReferenceError → the whole dashboard coverage render
broke. Fix: hoist the Set to module scope (one definition, all consumers see it).

F2 (HIGH, api.py): the v1.23.36 placement-exclusion subqueries keyed on
`p.tmdb_id = pi.guid_tmdb`, but COLLECTIONS carry guid_tmdb=NULL (placements key
on the theme's tmdb_id). So the NOT EXISTS never matched for collections → placed
collections were still counted "ready" and `placed` was always false. Fix: key on
`p.tmdb_id = t.tmdb_id` (the themes id placements actually use, = guid_tmdb for
movies/tv), matching the canonical library JOIN (api.py:3182). Also fixes
guid-less HAMA/legacy movie/tv rows.
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
NOW = "2026-06-14T00:00:00"
AUTH = {"X-Authentik-Username": "testadmin"}


# ── F1: TDB_DEAD_FAILURES_GLOBAL is module-scoped (no ReferenceError) ──


def test_dead_set_defined_once_at_module_scope_before_consumers():
    # exactly one definition, and it's at module (IIFE) scope = 2-space indent,
    # NOT nested in a function (3+ spaces). isAddable + computeTdbPill both read it.
    defs = re.findall(r"^( *)const TDB_DEAD_FAILURES_GLOBAL = new Set\(",
                      APP_JS, re.MULTILINE)
    assert len(defs) == 1, f"expected one TDB_DEAD_FAILURES_GLOBAL def, got {len(defs)}"
    assert defs[0] == "  ", "must be module-scope (2-space indent), not function-nested"
    # the def precedes its first consumer (isAddable in loadCoverage).
    assert (APP_JS.index("const TDB_DEAD_FAILURES_GLOBAL = new Set(")
            < APP_JS.index("const isAddable = (m) =>"))
    # the duplicate local Set (E1) is gone.
    assert "const TDB_DEAD_FAILURES = new Set(" not in APP_JS


# ── F2: collections placement-exclusion uses the right key ──


def test_placement_subqueries_key_on_theme_tmdb_not_guid():
    # collections have guid_tmdb=NULL; placements key on the theme tmdb_id.
    assert "p.tmdb_id=pi.guid_tmdb" not in API_PY
    assert "p.tmdb_id = pi.guid_tmdb" not in API_PY
    assert "p.tmdb_id=t.tmdb_id" in API_PY      # SSR ready aggregates
    assert "p.tmdb_id = t.tmdb_id" in API_PY    # to_item EXISTS


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


def test_placed_collection_is_excluded_from_ready(admin_client):
    # The regression's exact shape: a collection (guid_tmdb=NULL) that motif HAS
    # placed (plex_upload) but Plex hasn't acknowledged (has_theme=0). It must
    # read placed=True and NOT count as ready. The buggy pi.guid_tmdb key never
    # matched the placement (NULL), so it leaked through as ready + placed=False.
    client, db = admin_client
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime, is_4k,"
            " themes_subdir, included, discovered_at, last_seen_at)"
            " VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        # collection theme keyed by a synthetic tmdb_id (like real data: -N).
        conn.execute(
            "INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source,"
            " last_seen_sync_at, first_seen_sync_at, youtube_url)"
            " VALUES (1, 'collection', -100, 'Coll', 'themoviedb', ?, ?, 'u')",
            (NOW, NOW))
        # plex_items collection row: guid_tmdb NULL (the real-world shape),
        # has_theme=0 (Plex stale), theme_id linked.
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type, theme_id,"
            " guid_imdb, guid_tmdb, title, year, has_theme, plex_independent_theme,"
            " first_seen_at, last_seen_at)"
            " VALUES ('rk-coll', '1', 'collection', 1, NULL, NULL, 'Coll', NULL,"
            " 0, 0, ?, ?)", (NOW, NOW))
        # placement keyed by the THEME tmdb_id (-100), plex_upload (media_folder='').
        conn.execute(
            "INSERT INTO placements (media_type, tmdb_id, section_id, theme_id,"
            " media_folder, placed_at, placement_kind, provenance, edition_key)"
            " VALUES ('collection', -100, '1', 1, '', ?, 'plex_upload', 'auto', '')",
            (NOW,))
        conn.commit()

    r = client.get("/api/coverage/plex", headers=AUTH)
    assert r.status_code == 200, r.text
    colls = {it["rating_key"]: it for it in r.json()["collections"]}
    assert "rk-coll" in colls
    assert colls["rk-coll"]["motif_available"] is True
    assert colls["rk-coll"]["has_theme"] is False
    # the whole point: the placement IS detected (placed=True via t.tmdb_id).
    assert colls["rk-coll"]["placed"] is True
