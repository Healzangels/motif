"""v1.22.31 (audit) — backend hardening: api.py + notify.py silent-failures.

From the full-codebase audit (Tags C / G / D):

- api_adopt_from_plex (Tag G): (1) destroy-then-fail — canon was unlinked
  BEFORE the placement hardlink/copy was secured, so a double-FS-failure left
  the row with no canonical file. Now an atomic temp-then-os.replace. (2)
  fake-success — returned ok:True even when adopted==0 (every row skipped),
  flashing success while the mismatch stayed unresolved. Now 409.
- api_clear_override (Tag G): SELECT+DELETE+audit ran in autocommit; now one
  transaction (a failed audit can't leave a deleted override with no trail).
- api_delete_item (Tag G): empty-parent rmdir swallowed OSError silently; now a
  debug breadcrumb (class-9, auditable).
- api_admin_probe_themes (Tag C): up to 10 SYNCHRONOUS Plex HTTP calls ran on
  the event loop; now run_in_threadpool.
- notify.py (Tag D): _send_embedded / _send_external failure reasons logged at
  DEBUG (the specific HTTP status / exception) while only the aggregate was
  WARNING; bumped to WARNING so a silent channel is diagnosable by default.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core.auth import create_admin, init_auth_schema
from app.core.db import init_db
from app.core.events import now_iso

REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()
NOTIFY_PY = (REPO / "app" / "core" / "notify.py").read_text()

NOW = now_iso()
AUTH = {"X-Authentik-Username": "testadmin"}
TMDB = 770
RK = "9001"


@pytest.fixture
def app_client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    (tmp_path / "themes").mkdir(parents=True, exist_ok=True)
    settings._cfg.paths.themes_dir = str(tmp_path / "themes")
    db = settings.db_path
    init_db(db)
    init_auth_schema(db)
    create_admin(db, username="testadmin", password="testpassword")
    from app.web.api import create_app
    return TestClient(create_app(settings)), db, settings


def _seed_mismatch(db, settings, *, plex_folder: Path, with_placement_file: bool):
    """One mismatch row: local_files.mismatch_state set + a placement whose
    media_folder is `plex_folder`. The canonical file exists (the rejected
    download). The placement theme.mp3 exists iff with_placement_file."""
    themes_dir = settings.themes_dir
    rel = "movies/lotr.mp3"
    canon = themes_dir / rel
    canon.parent.mkdir(parents=True, exist_ok=True)
    canon.write_bytes(b"REJECTED-DOWNLOAD")  # the new content being rejected
    plex_folder.mkdir(parents=True, exist_ok=True)
    if with_placement_file:
        (plex_folder / "theme.mp3").write_bytes(b"PLEX-SERVED-GOOD")
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime,"
            " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
            " VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        cur = conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, year,"
            " upstream_source, last_seen_sync_at, first_seen_sync_at)"
            " VALUES ('movie',?,'LotR','2001','imdb',?,?)", (TMDB, NOW, NOW))
        tid = cur.lastrowid
        conn.execute(
            "INSERT INTO local_files (media_type, tmdb_id, section_id,"
            " edition_key, file_path, downloaded_at, source_video_id,"
            " provenance, source_kind, mismatch_state) VALUES ('movie',?,'1',"
            "'',?,?,'v','manual','upload','pending')", (TMDB, rel, NOW))
        conn.execute(
            "INSERT INTO placements (media_type, tmdb_id, section_id, theme_id,"
            " media_folder, placed_at, placement_kind, plex_refreshed,"
            " provenance, edition_key) VALUES ('movie',?,'1',?,?,?, 'hardlink',"
            "1,'manual','')", (TMDB, tid, str(plex_folder), NOW))
        conn.commit()
    return canon


def test_adopt_zero_sections_returns_409_not_fake_success(app_client, tmp_path):
    # Placement folder has NO theme.mp3 → every row skipped → adopted==0.
    client, db, settings = app_client
    plex_folder = tmp_path / "data" / "Movies" / "LotR (2001)"
    _seed_mismatch(db, settings, plex_folder=plex_folder, with_placement_file=False)

    r = client.post(f"/api/items/movie/{TMDB}/adopt-from-plex", headers=AUTH)
    assert r.status_code == 409, r.text
    assert "no sections could be adopted" in r.text
    # mismatch_state must remain (nothing was resolved).
    with sqlite3.connect(db) as conn:
        ms = conn.execute(
            "SELECT mismatch_state FROM local_files WHERE tmdb_id=?",
            (TMDB,)).fetchone()[0]
    assert ms == "pending", "a fake-success would have left this unresolved silently"


def test_adopt_happy_path_replaces_canon_and_clears_mismatch(app_client, tmp_path):
    client, db, settings = app_client
    plex_folder = tmp_path / "data" / "Movies" / "LotR (2001)"
    canon = _seed_mismatch(db, settings, plex_folder=plex_folder,
                           with_placement_file=True)

    r = client.post(f"/api/items/movie/{TMDB}/adopt-from-plex", headers=AUTH)
    assert r.status_code == 200, r.text
    assert r.json()["sections_adopted"] == 1
    # canon now holds the PLEX-SERVED content (adopted), not the rejected one.
    assert canon.read_bytes() == b"PLEX-SERVED-GOOD"
    # no leftover temp file from the atomic replace.
    assert not (canon.with_name(canon.name + ".adopt.tmp")).exists()
    with sqlite3.connect(db) as conn:
        ms = conn.execute(
            "SELECT mismatch_state FROM local_files WHERE tmdb_id=?",
            (TMDB,)).fetchone()[0]
    assert ms is None, "mismatch must clear on a successful adopt"


# ── source pins ───────────────────────────────────────────────


def test_adopt_uses_atomic_replace():
    fn = API_PY.index("async def api_adopt_from_plex(")
    body = API_PY[fn:fn + 6000]
    # v1.22.69: the temp link/copy + replace moved into the offloaded
    # _stage_canon helper (event-loop class 12); same atomic semantics.
    assert "def _stage_canon(" in body and "os.replace(tc, dst)" in body, (
        "v1.22.31: adopt must link/copy to a temp then atomically replace canon")
    assert "await run_in_threadpool(_stage_canon)" in body
    # The pre-fix unconditional unlink-then-link must be gone.
    assert "if canon_path.exists():\n                    canon_path.unlink()" not in body


def test_probe_themes_runs_off_event_loop():
    fn = API_PY.index("async def api_admin_probe_themes(")
    body = API_PY[fn:fn + 4000]
    assert "await run_in_threadpool(_probe)" in body, (
        "v1.22.31: probe-themes' synchronous Plex calls must run off the loop")


def test_clear_override_wraps_in_transaction():
    fn = API_PY.index("async def api_clear_override(")
    body = API_PY[fn:fn + 2500]
    assert "with get_conn(db) as conn, transaction(conn):" in body, (
        "v1.22.31: clear-override's DELETE + audit must be one transaction")


def test_notify_external_failures_log_warning():
    fn = NOTIFY_PY.index("def _send_external(")
    body = NOTIFY_PY[fn:fn + 2600]
    assert 'log.warning("apprise external API returned' in body
    assert 'log.warning("apprise external API call raised' in body
    assert 'log.debug("apprise external API' not in body, (
        "v1.22.31: external send failure reasons must be WARNING, not DEBUG")


def test_notify_embedded_failure_logs_warning():
    fn = NOTIFY_PY.index("def _send_embedded(")
    body = NOTIFY_PY[fn:fn + 4000]
    assert 'log.warning("apprise embedded notify raised' in body


def test_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
