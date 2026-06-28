"""v1.21.36 — code-review fixes on the v1.21.34 import-backup feature.

Three confirmed correctness issues from the xhigh review:
  #2 (primary): the force_place=false import download used reason
     'bulk_import', which the worker's backup_only-stamp whitelist did
     NOT recognize. With auto_place off, the staged backup never got
     last_place_attempt_reason='backup_only' → no UB badge AND the hourly
     retry sweep re-enqueued place jobs forever. v1.21.34 made this the
     DEFAULT for every P-row. Fix: distinct reason 'bulk_import_backup'
     + whitelist it.
  #3: the P-row download_only default fired for collection P-rows (a
     title-collision theme match), which apply rejects (mt not in
     movie/tv) → dead-end. Fix: gate the default on target_mt ∈ (movie,tv).
  #1: the DUPLICATE compare used the RAW CSV url vs the canonical stored
     url, so a non-canonical re-import (youtu.be/VID) re-staged instead of
     reading DUPLICATE. Fix: canonicalize the imported url in preview.
"""
from __future__ import annotations

import csv
import io
import json as _json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core.auth import create_admin, init_auth_schema
from app.core.db import get_conn, init_db, transaction


REPO = Path(__file__).resolve().parent.parent
WORKER_PY = (REPO / "app" / "core" / "worker.py").read_text()


@pytest.fixture
def app_client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    db = settings.db_path
    init_db(db)
    init_auth_schema(db)
    create_admin(db, username="testadmin", password="testpassword")
    from app.web.api import create_app
    return TestClient(create_app(settings)), settings


HDR = {"X-Authentik-Username": "testadmin"}


def _make_csv(rows):
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["Title", "IMDB", "Youtube_URL"])
    for r in rows:
        w.writerow(r)
    return buf.getvalue().encode()


def _seed_themed_p_row(db, *, tmdb, imdb, mt="movie", title="P Themed"):
    with get_conn(db) as conn, transaction(conn):
        conn.execute(
            """INSERT INTO themes
                 (media_type, tmdb_id, imdb_id, title, year,
                  upstream_source, last_seen_sync_at, first_seen_sync_at)
               VALUES (?, ?, ?, ?, 2021, 'themoviedb',
                       '2026-01-01', '2026-01-01')""",
            (mt, tmdb, imdb, title))
        pi_mt = "show" if mt == "tv" else mt
        conn.execute(
            """INSERT INTO plex_items
                 (rating_key, section_id, media_type, title, year,
                  guid_imdb, has_theme, plex_theme_verified_ok, theme_id,
                  first_seen_at, last_seen_at)
               VALUES (?, '1', ?, ?, '2021', ?, 1, 1,
                       (SELECT id FROM themes WHERE tmdb_id = ?),
                       '2026-01-01', '2026-01-01')""",
            (f"rk-{tmdb}", pi_mt, title, imdb, tmdb))


def _preview(client, csv_bytes):
    resp = client.post(
        "/api/import/preview",
        files={"file": ("t.csv", csv_bytes, "text/csv")}, headers=HDR)
    assert resp.status_code == 200, resp.text
    return resp.json()


# ── #2: backup variant uses a whitelisted distinct reason ────


def test_backup_variant_download_uses_bulk_import_backup_reason(app_client):
    client, settings = app_client
    _seed_themed_p_row(settings.db_path, tmdb=36001, imdb="tt36001")
    resp = client.post("/api/import/apply", headers=HDR, json={"decisions": [{
        "theme_media_type": "movie", "theme_tmdb_id": 36001,
        "imported_url": "https://www.youtube.com/watch?v=bkk36001abc",
        "action": "replace", "force_place": False,
    }]})
    assert resp.status_code == 200, resp.text
    with get_conn(settings.db_path) as conn:
        job = conn.execute(
            "SELECT payload FROM jobs WHERE tmdb_id = 36001 "
            "  AND job_type = 'download'").fetchone()
        payload = _json.loads(job["payload"])
        assert payload["reason"] == "bulk_import_backup", (
            "v1.21.36: force_place=false import must use the whitelisted "
            "'bulk_import_backup' reason so the worker stamps backup_only")
        assert payload["force_place"] is False


def test_replace_variant_keeps_bulk_import_reason(app_client):
    client, settings = app_client
    _seed_themed_p_row(settings.db_path, tmdb=36002, imdb="tt36002")
    resp = client.post("/api/import/apply", headers=HDR, json={"decisions": [{
        "theme_media_type": "movie", "theme_tmdb_id": 36002,
        "imported_url": "https://www.youtube.com/watch?v=rep36002abc",
        "action": "replace", "force_place": True,
    }]})
    assert resp.status_code == 200, resp.text
    with get_conn(settings.db_path) as conn:
        job = conn.execute(
            "SELECT payload FROM jobs WHERE tmdb_id = 36002 "
            "  AND job_type = 'download'").fetchone()
        assert _json.loads(job["payload"])["reason"] == "bulk_import"


def test_worker_whitelist_recognizes_bulk_import_backup():
    # The backup_only-stamp whitelist must include the new reason.
    idx = WORKER_PY.index("last_place_attempt_reason = 'backup_only'")
    window = WORKER_PY[max(0, idx - 1500):idx]
    assert '"bulk_import_backup",' in window, (
        "v1.21.36: worker backup_only whitelist must include "
        "'bulk_import_backup'")


# ── #3: collection P-row stays at the safe 'keep' default ────


def test_collection_p_row_does_not_default_to_download_only(app_client):
    """A collection theme matched by title (no imdb/year) is a P-row, but
    apply rejects mt='collection' — so the preview must NOT offer
    download_only (un-appliable dead-end); it stays at 'keep'."""
    client, settings = app_client
    with get_conn(settings.db_path) as conn, transaction(conn):
        conn.execute(
            """INSERT INTO themes
                 (media_type, tmdb_id, title, year, upstream_source,
                  last_seen_sync_at, first_seen_sync_at)
               VALUES ('collection', 36003, 'A24 Films', NULL,
                       'plex_orphan', '2026-01-01', '2026-01-01')""")
        conn.execute(
            """INSERT INTO plex_items
                 (rating_key, section_id, media_type, title, year,
                  has_theme, plex_theme_verified_ok, theme_id,
                  first_seen_at, last_seen_at)
               VALUES ('rk-36003', '1', 'collection', 'A24 Films', NULL,
                       1, 1, (SELECT id FROM themes WHERE tmdb_id = 36003),
                       '2026-01-01', '2026-01-01')""")
    # CSV: title-only match (empty IMDB, no year) → the collection theme.
    data = _preview(client, _make_csv([
        ("A24 Films", "", "https://www.youtube.com/watch?v=coll36003ab")]))
    row = data["rows"][0]
    assert row["current_src"] == "P"
    assert row["default_action"] == "keep", (
        "v1.21.36: a collection P-row must stay 'keep' (apply can't "
        "fulfill download_only for mt='collection')")


# ── #1: non-canonical re-import reads DUPLICATE ──────────────


def test_reimport_youtu_be_form_matches_canonical_backup(app_client):
    client, settings = app_client
    _seed_themed_p_row(settings.db_path, tmdb=36004, imdb="tt36004")
    canon = "https://www.youtube.com/watch?v=ytbe1234567"
    with get_conn(settings.db_path) as conn, transaction(conn):
        conn.execute(
            """INSERT INTO user_overrides
                 (media_type, tmdb_id, youtube_url, set_at, set_by,
                  note, intent, section_id)
               VALUES ('movie', 36004, ?, '2026-01-01', 'seed',
                       'seed', 'backup', '')""",
            (canon,))
    # Re-import the SAME video in youtu.be short form → must dedupe.
    data = _preview(client, _make_csv([
        ("P Themed", "tt36004", "https://youtu.be/ytbe1234567")]))
    row = data["rows"][0]
    assert row["status"] == "duplicate", (
        "v1.21.36: a youtu.be re-import of an already-staged canonical "
        "backup must read DUPLICATE, not re-stage")


def test_v1_21_36_version_pin():
    assert '__version__ = "0.' in (REPO / "app" / "__init__.py").read_text()
