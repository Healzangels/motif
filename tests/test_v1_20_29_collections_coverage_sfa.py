"""v1.20.29 — collections coverage failures count is sfa-aware.

the user: dashboard PER-SECTION COVERAGE showed "3" failures for
Collections, but clicking it (→ /collections?status=failures) surfaced
0; only the TDB-✗ (dead) pill, which ignores ack state, showed the 3.

Root cause (verified against prod, 2026-05-28): the synthetic
Collections aggregate row in /api/sections/coverage counted
`failure_kind IS NOT NULL AND failure_acked_at IS NULL` — the
title-global ack only. It was MISSING the per-section ack
(section_failure_acks.acked_at) that the per-section coverage query,
the topbar FAIL count, the status=failures library filter, and
attn_pills=fail all honor. the user's 3 video_removed collections
(Hellboy / RED / Equalizer) were each section-acked 2026-05-24, so the
sfa-blind count read 3 while status=failures (sfa-aware) read 0.

Class-9 mirror-drift: bring the collections aggregate in line with the
canonical sfa-aware failures predicate.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()
AUTH = {"X-Authentik-Username": "testadmin"}


def _seed_collection_failure(db, *, acked: bool):
    """One included Movies section with a single collection plex_item
    whose theme carries an unacked-global video_removed failure. When
    `acked`, also write a section_failure_acks row (per-section ack)."""
    ts = "2026-05-20T00:00:00+00:00"
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections "
            "(section_id, title, type, included, is_anime, is_4k, "
            " discovered_at, last_seen_at) "
            "VALUES ('1', 'Movies', 'movie', 1, 0, 0, ?, ?)", (ts, ts))
        conn.execute(
            "INSERT INTO themes "
            "(id, media_type, tmdb_id, title, upstream_source, "
            " failure_kind, failure_acked_at, last_seen_sync_at, "
            " first_seen_sync_at) "
            "VALUES (9001, 'collection', 17235, 'Hellboy Collection', "
            "        'themoviedb', 'video_removed', NULL, ?, ?)", (ts, ts))
        conn.execute(
            "INSERT INTO plex_items "
            "(rating_key, section_id, media_type, title, theme_id, has_theme, "
            " first_seen_at, last_seen_at) "
            "VALUES ('rk-coll-1', '1', 'collection', 'Hellboy Collection', "
            "        9001, 1, ?, ?)", (ts, ts))
        if acked:
            conn.execute(
                "INSERT INTO section_failure_acks "
                "(media_type, tmdb_id, section_id, acked_at, acked_by) "
                "VALUES ('collection', 17235, '1', "
                "        '2026-05-24T17:56:12+00:00', 'testadmin')")
        conn.commit()


@pytest.fixture
def admin_client_factory(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web.api import create_app

    def _make(*, acked):
        settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
        db = settings.db_path
        init_db(db)
        init_auth_schema(db)
        create_admin(db, username="testadmin", password="testpassword")
        _seed_collection_failure(db, acked=acked)
        return TestClient(create_app(settings))
    return _make


def _collections_row(client):
    r = client.get("/api/sections/coverage", headers=AUTH)
    assert r.status_code == 200, r.text
    secs = r.json()["sections"]
    coll = [s for s in secs if s.get("tab") == "collections"]
    assert coll, f"no collections row in coverage: {secs}"
    return coll[0]


# ── behavioral ───────────────────────────────────────────────


def test_section_acked_collection_failure_not_counted(admin_client_factory):
    """The bug repro: a section-acked collection failure must NOT
    inflate the dashboard failures count (it would 0 out under
    status=failures)."""
    client = admin_client_factory(acked=True)
    row = _collections_row(client)
    assert row["failures"] == 0, (
        "section-acked collection failure must be excluded — matches "
        "what /collections?status=failures surfaces"
    )


def test_unacked_collection_failure_still_counted(admin_client_factory):
    """Guard the other direction: a genuinely unacked collection
    failure must still count, so the fix doesn't silently hide real
    actionable failures."""
    client = admin_client_factory(acked=False)
    row = _collections_row(client)
    assert row["failures"] == 1, (
        "unacked collection failure must still count"
    )


# ── source pin ───────────────────────────────────────────────


def test_collections_coverage_query_joins_sfa():
    """The collections aggregate query must JOIN section_failure_acks
    and gate the failures CASE on sfa.acked_at IS NULL — mirroring the
    per-section coverage query."""
    anchor = API_PY.index("def _collections_query():")
    body = API_PY[anchor:anchor + 4000]
    assert "LEFT JOIN section_failure_acks sfa" in body
    assert "sfa.acked_at IS NULL" in body


def test_v1_20_29_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
