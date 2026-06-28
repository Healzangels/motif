"""v1.21.34 — import: P-rows default to "download as backup" + re-import
duplicate detection against a staged backup.

the user's two asks:
  1. On import, a row that already has a theme (Plex-served, P) should
     DEFAULT to "download as backup" instead of "Keep current" — stage
     the imported URL as a UB backup (intent='backup') + download
     without placing, so Plex keeps serving its own theme. the user's
     call: only P-rows flip the default; U/A/M conflict rows keep
     defaulting to 'keep current'.
  2. Re-importing the same CSV onto a row that already has a theme AND a
     staged backup matching the import URL should read DUPLICATE (like
     any other already-present theme), not re-stage the backup.

Mechanism: the apply path derives intent from force_place (false →
'backup'); the preview's current-state helper surfaces a backup URL
SEPARATELY from the active source so (a) a backup doesn't masquerade as
the active 'U' source, and (b) the categorizer can flag the re-import as
duplicate.
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
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


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
    app = create_app(settings)
    return TestClient(app), settings


HDR = {"X-Authentik-Username": "testadmin"}


def _make_csv(rows):
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["Title", "IMDB", "Youtube_URL"])
    for r in rows:
        w.writerow(r)
    return buf.getvalue().encode()


def _seed_p_row(db, *, tmdb, imdb, title="P Themed"):
    """A themed, Plex-serving (P) row: theme + plex_items has_theme=1
    verified, linked via theme_id, NO placement, NO override."""
    with get_conn(db) as conn, transaction(conn):
        conn.execute(
            """INSERT INTO themes
                 (media_type, tmdb_id, imdb_id, title, year,
                  upstream_source, last_seen_sync_at, first_seen_sync_at)
               VALUES ('movie', ?, ?, ?, 2021, 'themoviedb',
                       '2026-01-01', '2026-01-01')""",
            (tmdb, imdb, title))
        conn.execute(
            """INSERT INTO plex_items
                 (rating_key, section_id, media_type, title, year,
                  guid_imdb, has_theme, plex_theme_verified_ok, theme_id,
                  first_seen_at, last_seen_at)
               VALUES (?, '1', 'movie', ?, '2021', ?, 1, 1,
                       (SELECT id FROM themes WHERE tmdb_id = ?),
                       '2026-01-01', '2026-01-01')""",
            (f"rk-{tmdb}", title, imdb, tmdb))


def _add_override(db, *, tmdb, url, intent):
    with get_conn(db) as conn, transaction(conn):
        conn.execute(
            """INSERT INTO user_overrides
                 (media_type, tmdb_id, youtube_url, set_at, set_by,
                  note, intent, section_id)
               VALUES ('movie', ?, ?, '2026-01-01', 'seed', 'seed', ?, '')""",
            (tmdb, url, intent))


def _preview(client, csv_bytes):
    resp = client.post(
        "/api/import/preview",
        files={"file": ("t.csv", csv_bytes, "text/csv")}, headers=HDR)
    assert resp.status_code == 200, resp.text
    return resp.json()


# ── Ask 1: P-rows default to backup ──────────────────────────


def test_preview_p_row_defaults_to_download_backup(app_client):
    client, settings = app_client
    _seed_p_row(settings.db_path, tmdb=34001, imdb="tt34001")
    data = _preview(client, _make_csv([
        ("P Themed", "tt34001", "https://www.youtube.com/watch?v=imp34001abc")]))
    row = data["rows"][0]
    assert row["current_src"] == "P"
    assert row["status"] == "conflict"
    assert row["default_action"] == "download_only", (
        "v1.21.34: P-rows must default to 'download as backup'")
    assert data["counts"]["conflict"] == 1


def test_preview_user_row_still_defaults_to_keep(app_client):
    """the user: only P-rows flip the default. A U-row (active user
    override, different URL) keeps defaulting to 'keep current'."""
    client, settings = app_client
    _seed_p_row(settings.db_path, tmdb=34002, imdb="tt34002")
    _add_override(settings.db_path, tmdb=34002,
                  url="https://www.youtube.com/watch?v=active34002",
                  intent="replace")
    data = _preview(client, _make_csv([
        ("P Themed", "tt34002", "https://www.youtube.com/watch?v=imp34002abc")]))
    row = data["rows"][0]
    assert row["current_src"] == "U", "active 'replace' override → U"
    assert row["status"] == "conflict"
    assert row["default_action"] == "keep", (
        "v1.21.34: U-rows keep defaulting to 'keep current'")


# ── Ask 1 apply: backup variant writes intent='backup' ───────


def test_apply_backup_variant_writes_intent_backup(app_client):
    client, settings = app_client
    _seed_p_row(settings.db_path, tmdb=34003, imdb="tt34003")
    resp = client.post("/api/import/apply", headers=HDR, json={"decisions": [{
        "theme_media_type": "movie", "theme_tmdb_id": 34003,
        "imported_url": "https://www.youtube.com/watch?v=bkk34003abc",
        "action": "replace", "force_place": False,
    }]})
    assert resp.status_code == 200, resp.text
    assert resp.json()["applied"] == 1
    with get_conn(settings.db_path) as conn:
        ov = conn.execute(
            "SELECT youtube_url, intent, note FROM user_overrides "
            "WHERE tmdb_id = 34003 AND section_id = ''").fetchone()
        assert ov is not None
        assert ov["intent"] == "backup", (
            "v1.21.34: force_place=false import must stamp intent='backup'")
        assert "backup" in ov["note"]
        job = conn.execute(
            "SELECT payload FROM jobs WHERE tmdb_id = 34003 "
            "  AND job_type = 'download'").fetchone()
        assert _json.loads(job["payload"])["force_place"] is False


def test_apply_replace_variant_writes_intent_replace(app_client):
    """Counter-test: force_place=true (or omitted) → intent='replace'."""
    client, settings = app_client
    _seed_p_row(settings.db_path, tmdb=34004, imdb="tt34004")
    resp = client.post("/api/import/apply", headers=HDR, json={"decisions": [{
        "theme_media_type": "movie", "theme_tmdb_id": 34004,
        "imported_url": "https://www.youtube.com/watch?v=rep34004abc",
        "action": "replace", "force_place": True,
    }]})
    assert resp.status_code == 200, resp.text
    with get_conn(settings.db_path) as conn:
        ov = conn.execute(
            "SELECT intent FROM user_overrides WHERE tmdb_id = 34004 "
            "  AND section_id = ''").fetchone()
        assert ov["intent"] == "replace"


# ── Ask 2: re-import same CSV → DUPLICATE on a staged backup ──


def test_reimport_with_matching_backup_is_duplicate(app_client):
    client, settings = app_client
    _seed_p_row(settings.db_path, tmdb=34005, imdb="tt34005")
    url = "https://www.youtube.com/watch?v=bkk34005abc"
    _add_override(settings.db_path, tmdb=34005, url=url, intent="backup")
    data = _preview(client, _make_csv([("P Themed", "tt34005", url)]))
    row = data["rows"][0]
    assert row["status"] == "duplicate", (
        "v1.21.34: a row whose staged backup matches the import is a "
        "no-op DUPLICATE")
    assert data["counts"]["duplicate"] == 1


def test_backup_override_does_not_masquerade_as_active_source(app_client):
    """A staged backup (intent='backup') with a DIFFERENT URL than the
    import must NOT flip current_src to U — the active source stays P,
    and the row is still a backup-default conflict (not yet duplicate)."""
    client, settings = app_client
    _seed_p_row(settings.db_path, tmdb=34006, imdb="tt34006")
    _add_override(settings.db_path, tmdb=34006,
                  url="https://www.youtube.com/watch?v=oldbk34006",
                  intent="backup")
    data = _preview(client, _make_csv([
        ("P Themed", "tt34006",
         "https://www.youtube.com/watch?v=newimp34006")]))
    row = data["rows"][0]
    assert row["current_src"] == "P", (
        "intent='backup' override must not be read as the active U source")
    assert row["status"] == "conflict"
    assert row["default_action"] == "download_only"


# ── JS: the new P default pre-selects "Backup only" ──────────


def test_js_download_only_option_has_selected_binding():
    fn_start = APP_JS.index("function bindImportPanel()")
    fn_end = APP_JS.index("// ---- Config form", fn_start)
    body = APP_JS[fn_start:fn_end]
    assert "value=\"download_only\" ${r.default_action === 'download_only' ? 'selected' : ''}" in body, (
        "v1.21.34: the Backup only option must pre-select when the "
        "preview default_action is download_only")


def test_v1_21_34_version_pin():
    assert '__version__ = "0.' in (REPO / "app" / "__init__.py").read_text()
