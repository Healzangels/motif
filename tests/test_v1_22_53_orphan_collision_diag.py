"""v1.22.53 — read-only orphan-collision diagnostic.

The de-orphan walker correctly skipped 8 orphans whose imdb resolves to a tmdb
where a theme record already exists (two records for one title). Merging needs a
per-row "which theme wins" decision, so — diagnose-first — this endpoint reports
what each record holds and which one the library row links to.

GET /api/admin/diagnostics/orphan-collisions — admin-gated, read-only.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from app.core.auth import create_admin, init_auth_schema
from app.core.db import get_conn, init_db
from app.web.api import create_app

_NOW = "2026-06-10T09:00:00"

_MAP = {
    "tt_split": {"tmdb_id": 1022789, "kind": "movie"},
    "tt_empty": {"tmdb_id": 555, "kind": "movie"},
}


def _fake_lookup(self, imdb_id):
    return _MAP.get(imdb_id)


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("MOTIF_TMDB_API_KEY", "fakekey")
    from app.config import Settings
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    db = settings.db_path
    init_db(db)
    init_auth_schema(db)
    create_admin(db, username="testadmin", password="testpassword")
    with get_conn(db) as c:
        c.execute(
            "INSERT INTO plex_sections (section_id,title,type,included,"
            " is_anime,is_4k,themes_subdir,discovered_at,last_seen_at) "
            "VALUES ('s','Movies','movie',1,0,0,'m',?,?)", (_NOW, _NOW))
        # SPLIT-TRACKING shape: real TDB theme at 1022789 + orphan dup at -25
        # holding the manual theme's tracking; library row linked to the TARGET.
        c.execute(
            "INSERT INTO themes (media_type,tmdb_id,title,upstream_source,"
            " youtube_url,first_seen_sync_at,last_seen_sync_at) "
            "VALUES ('movie',1022789,'Inside Out 2','themoviedb','u',?,?)",
            (_NOW, _NOW))
        target_id = c.execute(
            "SELECT id FROM themes WHERE tmdb_id=1022789").fetchone()[0]
        c.execute(
            "INSERT INTO themes (media_type,tmdb_id,imdb_id,title,"
            " upstream_source,first_seen_sync_at,last_seen_sync_at) "
            "VALUES ('movie',-25,'tt_split','Inside Out 2','plex_orphan',?,?)",
            (_NOW, _NOW))
        c.execute(
            "INSERT INTO local_files (media_type,tmdb_id,section_id,file_path,"
            " downloaded_at,source_video_id,source_kind) "
            "VALUES ('movie',-25,'s','p/theme.mp3',?,'vid','url')", (_NOW,))
        c.execute(
            "INSERT INTO user_overrides (media_type,tmdb_id,section_id,"
            " youtube_url,set_at) VALUES ('movie',-25,'s','https://u',?)",
            (_NOW,))
        c.execute(
            "INSERT INTO plex_items (rating_key,section_id,media_type,"
            " guid_tmdb,theme_id,title,year,first_seen_at,last_seen_at) "
            "VALUES ('rk1','s','movie','1022789',?,'Inside Out 2','2024',?,?)",
            (target_id, _NOW, _NOW))
        # EMPTY-DUPLICATE shape: real theme at 555 + orphan dup at -3 holding
        # nothing, no library row linked.
        c.execute(
            "INSERT INTO themes (media_type,tmdb_id,title,upstream_source,"
            " youtube_url,first_seen_sync_at,last_seen_sync_at) "
            "VALUES ('movie',555,'Empty','themoviedb','u',?,?)", (_NOW, _NOW))
        c.execute(
            "INSERT INTO themes (media_type,tmdb_id,imdb_id,title,"
            " upstream_source,first_seen_sync_at,last_seen_sync_at) "
            "VALUES ('movie',-3,'tt_empty','Empty','plex_orphan',?,?)",
            (_NOW, _NOW))
        c.commit()
    app = create_app(settings)
    c = TestClient(app)
    c.headers["X-Authentik-Username"] = "testadmin"
    c.db = db  # type: ignore[attr-defined]
    yield c


def test_collision_detail_shapes(client, monkeypatch):
    monkeypatch.setattr(
        "app.core.tmdb.TMDBClient.lookup_by_imdb", _fake_lookup)
    r = client.get("/api/admin/diagnostics/orphan-collisions")
    assert r.status_code == 200
    j = r.json()
    assert j["ok"] is True and j["count"] == 2
    by_imdb = {c["imdb"]: c for c in j["collisions"]}
    split = by_imdb["tt_split"]
    assert split["target_tmdb"] == 1022789
    assert split["target_kind"] == "real themerrdb record"
    assert split["orphan_children"]["local_files"] == 1
    assert split["orphan_children"]["user_overrides"] == 1
    assert split["library_rows_on_target"] == 1
    assert split["library_rows_on_orphan"] == 0
    assert "SPLIT TRACKING" in split["hint"]
    empty = by_imdb["tt_empty"]
    assert sum(empty["orphan_children"].values()) == 0
    assert "safe to delete" in empty["hint"]


def test_read_only_no_mutation(client, monkeypatch):
    monkeypatch.setattr(
        "app.core.tmdb.TMDBClient.lookup_by_imdb", _fake_lookup)
    client.get("/api/admin/diagnostics/orphan-collisions")
    # both orphans still present, untouched
    with get_conn(client.db) as c:
        tmdbs = sorted(r[0] for r in c.execute(
            "SELECT tmdb_id FROM themes").fetchall())
    assert tmdbs == [-25, -3, 555, 1022789]


def test_requires_admin(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(settings.db_path)
    init_auth_schema(settings.db_path)
    app = create_app(settings)
    c = TestClient(app)  # no admin header
    r = c.get("/api/admin/diagnostics/orphan-collisions")
    if r.status_code == 200:
        assert "application/json" not in r.headers.get("content-type", "")
        assert '"collisions"' not in r.text
    else:
        assert r.status_code in (401, 403, 302, 307)
