"""v1.20.25 — bulk download (+ probe) include collections.

the user selected 5 TDB-tracked collections and ran bulk download:
the log showed `enqueued: 0, skipped: 5` on every attempt. Root cause:
the bulk download-batch gate was `mt not in ("movie", "tv")`, silently
skipping every collection. The same `("movie", "tv")` exclusion blocked
bulk PROBE TDB SELECTED. _enqueue_download + the place worker
(_do_place_collection) fully support collections — the gates just
never got extended.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()
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


def _seed_collection(db):
    now = "2026-05-30T00:00:00"
    with sqlite3.connect(db) as c:
        c.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime, "
            "  is_4k, themes_subdir, included, discovered_at, last_seen_at) "
            "VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (now, now))
        c.execute(
            "INSERT INTO themes (id, media_type, tmdb_id, title, "
            "  upstream_source, last_seen_sync_at, first_seen_sync_at, "
            "  youtube_url) VALUES (1,'collection',403374,'Jack Reacher "
            "  Collection','themoviedb',?,?,"
            "  'https://www.youtube.com/watch?v=OnprPWbXXYQ')", (now, now))
        # theme_id linkage is required — _enqueue_download matches via
        # pi.theme_id (v1.15.142), and a TDB-tracked collection shows
        # the TDB badge precisely because it IS theme_id-linked.
        c.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type, "
            "  theme_id, guid_tmdb, title, year, has_theme, first_seen_at, "
            "  last_seen_at) VALUES ('c1','1','collection',1,403374,'Jack "
            "  Reacher Collection',NULL,0,?,?)", (now, now))
        c.commit()


def test_bulk_download_enqueues_collection(admin_client):
    client, db = admin_client
    _seed_collection(db)
    r = client.post(
        "/api/library/download-batch",
        json={"items": [{"media_type": "collection", "tmdb_id": 403374,
                         "section_id": "1"}],
              "place": False},
        headers=AUTH)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["enqueued"] >= 1, (
        f"v1.20.25: a TDB-tracked collection must enqueue, not skip "
        f"(got {body})"
    )
    assert body["skipped"] == 0
    # a download job actually landed for the collection.
    with sqlite3.connect(db) as c:
        n = c.execute(
            "SELECT COUNT(*) FROM jobs WHERE job_type='download' "
            "AND media_type='collection' AND tmdb_id=403374").fetchone()[0]
    assert n >= 1


# ── source pins: all three bulk gates include collection ─────


def test_bulk_download_gate_includes_collection():
    idx = API_PY.index('/api/library/download-batch')
    block = API_PY[idx:idx + 4000]
    assert '("movie", "tv", "collection")' in block, (
        "v1.20.25: bulk download-batch must accept collection"
    )


def test_bulk_probe_gates_include_collection():
    # both probe scope-builders accept collection now.
    assert API_PY.count('("movie", "tv", "collection")') >= 3, (
        "v1.20.25: download + both probe scope gates must include "
        "collection"
    )


def test_v1_20_25_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
