"""v1.21.37 — fold the 3 per-row override queries into 1 + collect ALL
staged backups.

Code-review cleanup: _import_row_current_state ran THREE override queries
per preview row (ovr_bk + ovr_section + ovr_global), and the backup query
was a bare LIMIT 1 with no section scoping — so with MULTIPLE staged
backups (e.g. per-section), it picked an arbitrary one and could miss the
URL that actually matches the import. Now one query partitions in Python
and the duplicate check matches against the SET of all staged backups.
"""
from __future__ import annotations

import csv
import io
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core.auth import create_admin, init_auth_schema
from app.core.db import get_conn, init_db, transaction


REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()


@pytest.fixture
def app_client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(settings.db_path)
    init_auth_schema(settings.db_path)
    create_admin(settings.db_path, username="testadmin", password="testpassword")
    from app.web.api import create_app
    return TestClient(create_app(settings)), settings


HDR = {"X-Authentik-Username": "testadmin"}


def _csv(rows):
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["Title", "IMDB", "Youtube_URL"])
    for r in rows:
        w.writerow(r)
    return buf.getvalue().encode()


def _seed_p_row(db, *, tmdb, imdb):
    with get_conn(db) as conn, transaction(conn):
        conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, imdb_id, title, year, "
            "  upstream_source, last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('movie', ?, ?, 'Multi BK', 2021, 'themoviedb', "
            "        '2026-01-01', '2026-01-01')", (tmdb, imdb))
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type, "
            "  title, year, guid_imdb, has_theme, plex_theme_verified_ok, "
            "  theme_id, first_seen_at, last_seen_at) "
            "VALUES (?, '1', 'movie', 'Multi BK', '2021', ?, 1, 1, "
            "        (SELECT id FROM themes WHERE tmdb_id = ?), "
            "        '2026-01-01', '2026-01-01')",
            (f"rk-{tmdb}", imdb, tmdb))


def _add_backup(db, *, tmdb, section_id, url):
    with get_conn(db) as conn, transaction(conn):
        conn.execute(
            "INSERT INTO user_overrides (media_type, tmdb_id, youtube_url, "
            "  set_at, set_by, note, intent, section_id) "
            "VALUES ('movie', ?, ?, '2026-01-01', 'seed', 'seed', "
            "        'backup', ?)", (tmdb, url, section_id))


def test_import_matches_any_of_multiple_staged_backups(app_client):
    """Two staged backups (different sections, different URLs); the import
    matches the SECOND. Pre-fix the LIMIT-1 backup query could pick the
    first (non-matching) → CONFLICT. Now the set contains both → DUPLICATE."""
    client, settings = app_client
    _seed_p_row(settings.db_path, tmdb=37001, imdb="tt37001")
    url_a = "https://www.youtube.com/watch?v=bksecAaaaaa"
    url_b = "https://www.youtube.com/watch?v=bksecBbbbbb"
    _add_backup(settings.db_path, tmdb=37001, section_id="1", url=url_a)
    _add_backup(settings.db_path, tmdb=37001, section_id="2", url=url_b)
    resp = client.post(
        "/api/import/preview",
        files={"file": ("t.csv", _csv([("Multi BK", "tt37001", url_b)]),
                        "text/csv")}, headers=HDR)
    assert resp.status_code == 200, resp.text
    assert resp.json()["rows"][0]["status"] == "duplicate", (
        "v1.21.37: import URL matching ANY staged backup (any section) "
        "must read DUPLICATE")


def test_single_folded_override_query():
    # The import current-state helper now uses ONE folded override query.
    assert ("SELECT youtube_url, intent, section_id FROM user_overrides"
            in API_PY)
    # The duplicate check matches against the SET of staged backups.
    assert 'for u in cur.get("backup_urls", [])' in API_PY


def test_v1_21_37_version_pin():
    assert '__version__ = "0.' in (REPO / "app" / "__init__.py").read_text()
