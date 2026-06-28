"""v1.21.2 — persistent oEmbed cache (fixes "INFO cards slow on first click").

The INFO card fetches the YouTube/SoundCloud video TITLE for its URL tiles
via /api/source/oembed, which makes an external round-trip
(_fetch_oembed → youtube.com/oembed, 130-520ms cold). That was cached ONLY
in an in-memory LRU (_OEMBED_CACHE), wiped on every restart. the user
redeploys per tag, so the cache was perpetually cold → every card's first
open re-fetched its titles. Diagnosis (this session): api_item itself is
4-8ms — innocent; the oembed external call was the cost.

Fix: a persistent DB tier (oembed_cache table) behind the in-memory LRU.
After the first-ever fetch per URL it's a permanent hit, surviving
redeploys. 30-day TTL so a renamed/deleted video self-heals.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


REPO = Path(__file__).resolve().parent.parent
AUTH = {"X-Authentik-Username": "testadmin"}
YT = "https://www.youtube.com/watch?v=PERSIST1234"


# ── Schema migration ─────────────────────────────────────────────


def test_migrate_v60_to_v61_creates_table():
    from app.core.db import _migrate_v60_to_v61
    conn = sqlite3.connect(":memory:")
    _migrate_v60_to_v61(conn)
    assert conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' "
        "AND name='oembed_cache'").fetchone()
    cols = {r[1] for r in conn.execute("PRAGMA table_info(oembed_cache)")}
    assert cols == {"url", "payload", "fetched_at"}
    # Idempotent (CREATE TABLE IF NOT EXISTS).
    _migrate_v60_to_v61(conn)


def test_fresh_init_creates_oembed_cache_at_v61(tmp_path):
    from app.core.db import init_db, CURRENT_SCHEMA_VERSION
    # oembed_cache landed at v61 and persists onward (v62 added edition_key).
    assert CURRENT_SCHEMA_VERSION >= 61
    db = tmp_path / "m.db"
    init_db(db)
    with sqlite3.connect(db) as conn:
        assert conn.execute(
            "SELECT 1 FROM sqlite_master WHERE name='oembed_cache'").fetchone()


# ── Behavioral: the DB tier serves without hitting the network ────


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
    return TestClient(create_app(settings))


def _settings_db(tmp_path):
    from app.config import Settings
    return Settings(config_dir=tmp_path, data_dir=tmp_path / "data").db_path


def _seed(db, url, payload, *, fetched="datetime('now')"):
    with sqlite3.connect(db) as conn:
        conn.execute(
            f"INSERT INTO oembed_cache (url, payload, fetched_at) "
            f"VALUES (?, ?, {fetched})", (url, json.dumps(payload)))
        conn.commit()


def test_served_from_db_without_network(admin_client, tmp_path, monkeypatch):
    """A DB-cached entry is served WITHOUT an external fetch — make httpx
    explode so any network attempt fails the test, then assert the
    response carries the cached title (the fresh client's in-memory LRU
    is empty, so only the DB tier can answer)."""
    db = _settings_db(tmp_path)
    _seed(db, YT, {"title": "Cached Title", "author_name": "Chan"})

    import httpx

    def _boom(*a, **k):
        raise AssertionError("external oembed fetch must not happen on DB hit")

    monkeypatch.setattr(httpx, "Client", _boom)
    r = admin_client.get(f"/api/source/oembed?url={YT}", headers=AUTH)
    assert r.status_code == 200, r.text
    assert r.json().get("title") == "Cached Title"


def test_fetch_writes_through_to_db(admin_client, tmp_path, monkeypatch):
    """On a cache miss the external result is persisted to oembed_cache so
    the NEXT process (post-redeploy) serves it without a network call."""
    db = _settings_db(tmp_path)
    import httpx

    class _Resp:
        status_code = 200

        def json(self):
            return {"title": "Fetched Title", "author_name": "A",
                    "thumbnail_url": "https://i.ytimg.com/x.jpg"}

    class _Client:
        def __init__(self, *a, **k): pass
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def get(self, *a, **k): return _Resp()

    monkeypatch.setattr(httpx, "Client", _Client)
    url = "https://www.youtube.com/watch?v=WRITETHRU99"
    r = admin_client.get(f"/api/source/oembed?url={url}", headers=AUTH)
    assert r.status_code == 200, r.text
    assert r.json().get("title") == "Fetched Title"
    # It must have been persisted.
    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT payload FROM oembed_cache WHERE url = ?", (url,)).fetchone()
    assert row is not None, "the fetched title must be written through to the DB"
    assert json.loads(row[0])["title"] == "Fetched Title"


def test_stale_db_entry_not_served(admin_client, tmp_path, monkeypatch):
    """An entry older than the 30-day TTL is NOT served from the DB — it
    falls through to the network (mocked to fail → 404), proving the stale
    row didn't satisfy the request."""
    db = _settings_db(tmp_path)
    _seed(db, YT, {"title": "Stale Title"},
          fetched="datetime('now','-40 days')")
    import httpx

    class _Client:
        def __init__(self, *a, **k): pass
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def get(self, *a, **k):
            raise RuntimeError("network down")

    monkeypatch.setattr(httpx, "Client", _Client)
    r = admin_client.get(f"/api/source/oembed?url={YT}", headers=AUTH)
    assert r.status_code == 404, (
        "a stale (>30d) DB entry must not be served; it should fall "
        f"through to the (failing) network → 404. Got {r.status_code}"
    )


def test_v1_21_2_version():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
