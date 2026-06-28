"""v1.22.48 — read-only orphan-imdb diagnostic.

the user's question: orphan rows that carry a real imdb_id (e.g. a manually-themed
title like 'M*A*S*H 30th Anniversary Reunion', imdb tt0322649) show as
upstream=plex_orphan / tmdb=orphan. Before building an imdb→tmdb de-orphan
walker (which mutates orphan rows — a data-loss-sensitive area), this diagnostic
quantifies how many such orphans would actually RESOLVE to a real tmdb_id via
TMDB /find vs. titles TMDB genuinely lacks.

GET /api/admin/diagnostics/orphan-imdb — admin-gated, read-only, bounded +
cached TMDB probes off the event loop. No data changes.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from app.core.auth import create_admin, init_auth_schema
from app.core.db import init_db
from app.core.events import now_iso
from app.web.api import create_app

# imdb -> what the (mocked) TMDB /find resolution returns
_TMDB_MAP = {
    "tt_resolves_movie": {"tmdb_id": 12345, "kind": "movie"},   # net-new
    "tt_resolves_existing": {"tmdb_id": 200, "kind": "movie"},  # would merge
    "tt_no_match": None,                                        # TMDB lacks it
    "tt_wrong_type": {"tmdb_id": 999, "kind": "tv"},           # type disagree
}


def _fake_lookup_by_imdb(self, imdb_id):
    return _TMDB_MAP.get(imdb_id)


def _seed(conn: sqlite3.Connection) -> None:
    now = now_iso()
    # a REAL theme at tmdb 200 so the "would merge" orphan collides with it
    conn.execute(
        "INSERT INTO themes (media_type,tmdb_id,imdb_id,title,title_norm,year,"
        " upstream_source,youtube_url,first_seen_sync_at,last_seen_sync_at) "
        "VALUES ('movie',200,'tt_real','Real Movie','real movie','2020',"
        " 'themoviedb','u',?,?)", (now, now))
    # orphans
    rows = [
        (-1, "tt_resolves_movie", "Resolves Net-New"),
        (-2, "tt_resolves_existing", "Resolves Merge"),
        (-3, "tt_no_match", "No TMDB Match"),
        (-4, "tt_wrong_type", "Type Disagree"),
        (-5, None, "No IMDB At All"),
    ]
    for tmdb, imdb, title in rows:
        conn.execute(
            "INSERT INTO themes (media_type,tmdb_id,imdb_id,title,title_norm,"
            " year,upstream_source,first_seen_sync_at,last_seen_sync_at) "
            "VALUES ('movie',?,?,?,?,'2002','plex_orphan',?,?)",
            (tmdb, imdb, title, title.lower(), now, now))
    conn.commit()


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
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        _seed(conn)
    app = create_app(settings)
    c = TestClient(app)
    c.headers["X-Authentik-Username"] = "testadmin"
    yield c


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
    r = c.get("/api/admin/diagnostics/orphan-imdb")
    # The auth middleware may redirect a browser-style GET to a login page
    # (200 HTML) rather than 401 — what matters is the caller never sees the
    # diagnostic JSON (no library data leaks unauthenticated).
    if r.status_code == 200:
        assert "application/json" not in r.headers.get("content-type", ""), (
            "v1.22.48: diagnostic JSON returned to an unauthenticated caller")
        assert "orphans_total" not in r.text
    else:
        assert r.status_code in (401, 403, 302, 307)


def test_classification_counts(client, monkeypatch):
    monkeypatch.setattr(
        "app.core.tmdb.TMDBClient.lookup_by_imdb", _fake_lookup_by_imdb)
    r = client.get("/api/admin/diagnostics/orphan-imdb")
    assert r.status_code == 200
    j = r.json()
    assert j["tmdb_configured"] is True
    assert j["orphans_total"] == 5
    assert j["orphans_with_imdb"] == 4
    assert j["orphans_without_imdb"] == 1
    assert j["probed"] == 4
    assert j["resolves_to_real_tmdb"] == 2          # net-new + merge
    assert j["of_resolved_net_new_identity"] == 1   # tmdb 12345
    assert j["of_resolved_would_merge_existing"] == 1  # tmdb 200 exists
    assert j["no_tmdb_match"] == 2                   # None + wrong-type
    assert j["probe_errors"] == 0
    # the sample carries the resolved tmdb so the operator can eyeball
    by_title = {s["title"]: s for s in j["samples"]}
    assert by_title["Resolves Net-New"]["resolved_tmdb"] == 12345
    assert by_title["Resolves Net-New"]["already_real_theme"] is False
    assert by_title["Resolves Merge"]["already_real_theme"] is True
    assert by_title["No TMDB Match"]["resolved_tmdb"] is None
    assert by_title["Type Disagree"]["resolved_tmdb"] is None


def test_max_probe_bounds_the_work(client, monkeypatch):
    calls = []

    def _counting(self, imdb_id):
        calls.append(imdb_id)
        return _TMDB_MAP.get(imdb_id)

    monkeypatch.setattr(
        "app.core.tmdb.TMDBClient.lookup_by_imdb", _counting)
    r = client.get("/api/admin/diagnostics/orphan-imdb?max_probe=2")
    assert r.status_code == 200
    j = r.json()
    assert j["probed"] == 2
    assert j["not_probed"] == 2  # 4 with imdb - 2 probed
    assert len(calls) == 2       # never probed more than the cap


def test_no_tmdb_key_returns_early(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.delenv("MOTIF_TMDB_API_KEY", raising=False)
    from app.config import Settings
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(settings.db_path)
    init_auth_schema(settings.db_path)
    create_admin(settings.db_path, username="adm", password="testpassword")
    with sqlite3.connect(settings.db_path) as conn:
        conn.row_factory = sqlite3.Row
        _seed(conn)
    app = create_app(settings)
    c = TestClient(app)
    c.headers["X-Authentik-Username"] = "a"
    r = c.get("/api/admin/diagnostics/orphan-imdb")
    assert r.status_code == 200
    j = r.json()
    assert j["tmdb_configured"] is False
    assert j["orphans_with_imdb"] == 4  # counted without probing
    assert j["samples"] == []
