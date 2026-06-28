"""v1.19.37 — UPLOAD MP3 BK stamp + REVERT confirm dialog accuracy.

Two LIES surfaced by the post-v1.19.36 deep audit:

  1. **UPLOAD MP3 + KEEP AS BACKUP never reaches the BK badge.**
     `api_upload_theme` writes local_files DIRECTLY (api.py
     ~10571-10588) — bypasses worker._record_local_file where
     the v1.19.21/.33 backup_only stamp lives. Result: user
     clicks UPLOAD MP3 + KEEP AS BACKUP, expects to see BK in
     LINK + the v1.18.77 PROMOTE TO ACTIVE banner. Gets
     neither:
       - BK badge gate (`!placed && downloaded &&
         reason==='backup_only'`) fails because reason is NULL.
       - v1.19.35 BK-state synthesis (api_recovery_options
         no-failure branch) gates on the SAME column → no
         override is synthesized → no PROMOTE banner.
       - The v1.19.34 BK tooltip lies — it lists UPLOAD MP3
         as a reach path but the stamp never fires.
       - The upload-dlg KEEP AS BACKUP hint
         (library.html:680-692) sends the user looking for a
         PROMOTE TO ACTIVE button that won't appear.
     Same one-conceptual-surface-multiple-writers shape
     (CLAUDE.md class-9 sub-pattern) the v1.19.33 audit caught
     for SET URL + DOWNLOAD TDB BACKUP — this writer is the
     last one.

  2. **REVERT confirm dialog contradicts the v1.19.34 tooltip.**
     `confirmPlexAgentOverride` (app.js:12642) was gating on
     `act === 'revert'` and prompting "this will replace what
     Plex currently plays with the ThemerrDB version." Since
     v1.19.33, REVERT on a P-row routes through
     auto_place=False — Plex KEEPS serving. The SOURCE-menu
     REVERT tooltip got the v1.19.34 P-branch ("Plex keeps
     serving — row stays SRC=P with a BK badge"); this
     confirm dialog didn't. User hovers tooltip saying one
     thing, clicks, reads dialog saying the opposite.

## Fix

  - **`api_upload_theme`** (api.py): when `download_only=True`,
    stamp `last_place_attempt_reason='backup_only'` +
    `last_place_attempt_at=now()` on the local_files row
    directly. New `elif download_only:` branch mirrors the
    worker's v1.19.21/.33 stamp shape.
  - **`app.js` library click handler**: drop `'revert'` from
    the override-gate list. The remaining acts (redl,
    manual-url, upload-theme, replace-with-themerrdb) still
    override Plex; REVERT (v1.19.33) doesn't.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from fastapi.testclient import TestClient

API_PY = (REPO / "app" / "web" / "api.py").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


# ── Source-text guards ──────────────────────────────────────


def test_upload_theme_stamps_backup_only_on_download_only_branch():
    """`api_upload_theme` must stamp last_place_attempt_reason
    when download_only=True, mirroring worker._record_local_file
    for the upload-theme writer."""
    fn_start = API_PY.index("async def api_upload_theme(")
    fn_end = API_PY.index("@app.post", fn_start + 1)
    body = API_PY[fn_start:fn_end]
    # The `elif download_only:` branch must do the UPDATE.
    assert "elif download_only:" in body, (
        "v1.19.37: api_upload_theme must have an `elif "
        "download_only:` branch alongside the v1.15.75 "
        "place-skip gate"
    )
    assert "last_place_attempt_reason = 'backup_only'" in body, (
        "v1.19.37: download_only branch must stamp "
        "last_place_attempt_reason='backup_only' so the BK badge "
        "fires and the retry sweep skips"
    )
    # Marker for archaeology.
    assert "v1.19.37" in body, (
        "v1.19.37: marker required in api_upload_theme so a "
        "future refactor confronts the writer-coverage history"
    )


def test_revert_removed_from_override_gate():
    """The library click handler's override-confirm gate must
    NOT include `'revert'` anymore. v1.19.33 made REVERT
    backup-only on P-rows; the confirm dialog contradicted the
    v1.19.34 SOURCE-menu tooltip."""
    # Find the override-gate condition near the click handler.
    gate_idx = APP_JS.index("// P-agent override gate")
    chunk = APP_JS[gate_idx:gate_idx + 1200]
    # The remaining acts must be present.
    assert "'redl' || act === 'manual-url'" in chunk, (
        "v1.19.37: regression — redl/manual-url must STILL be "
        "in the override gate (these acts DO override Plex)"
    )
    assert "act === 'replace-with-themerrdb'" in chunk, (
        "v1.19.37: replace-with-themerrdb must STILL be in the "
        "override gate"
    )
    # 'revert' must be GONE from the gate list itself.
    # Slice tighter: the if(...) condition, not surrounding code.
    if_idx = chunk.index("if (act === 'redl'")
    if_end = chunk.index(")", if_idx)
    condition = chunk[if_idx:if_end]
    assert "'revert'" not in condition, (
        "v1.19.37: 'revert' must be removed from the override-"
        "gate condition. v1.19.33 routes REVERT on P-rows through "
        "auto_place=False (Plex keeps serving) — the confirm "
        "dialog directly contradicted the v1.19.34 tooltip"
    )


def test_revert_verb_branch_removed_from_confirm_dialog():
    """Counter-guard: now that revert is out of the gate, the
    `act === 'revert' ? 'revert to ThemerrDB'` ternary branch
    inside the verb assignment must also be gone — otherwise
    it'd be dead code waiting to mislead the next reader."""
    gate_idx = APP_JS.index("// P-agent override gate")
    chunk = APP_JS[gate_idx:gate_idx + 1200]
    assert "'revert to ThemerrDB'" not in chunk, (
        "v1.19.37: dead branch left in the verb ternary — "
        "remove `act === 'revert' ? 'revert to ThemerrDB'` "
        "since the outer gate excludes 'revert'"
    )


# ── End-to-end behavioral ────────────────────────────────────


@pytest.fixture
def admin_client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    # Configure themes_dir via the canonical env override so
    # api_upload_theme passes its `is_paths_ready()` gate. The
    # endpoint computes `media_root = settings.themes_dir /
    # plex_sections.themes_subdir` then writes the upload there.
    themes_dir = tmp_path / "themes"
    themes_dir.mkdir()
    monkeypatch.setenv("MOTIF_THEMES_DIR", str(themes_dir))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web.api import create_app
    settings = Settings(
        config_dir=tmp_path, data_dir=tmp_path / "data",
    )
    db = settings.db_path
    init_db(db)
    init_auth_schema(db)
    create_admin(db, username="testadmin", password="testpassword")
    return TestClient(create_app(settings)), settings


AUTH = {"X-Authentik-Username": "testadmin"}


def _seed_upload_target(conn, *, rk="rk-bk", tmdb_id=100):
    """Seed a P-row (has_theme=1) so api_upload_theme's KEEP AS
    BACKUP path is the canonical case for the v1.19.37 fix."""
    conn.execute(
        "INSERT INTO plex_sections "
        "  (section_id, title, type, is_anime, is_4k, "
        "   themes_subdir, included, discovered_at, last_seen_at) "
        "VALUES ('1','Movies','movie',0,0,'movies',1,"
        "        '2026-05-26T00:00:00','2026-05-26T00:00:00')"
    )
    conn.execute(
        "INSERT INTO plex_items "
        "  (rating_key, section_id, media_type, theme_id, "
        "   guid_imdb, guid_tmdb, title, year, has_theme, "
        "   local_theme_file, folder_path, "
        "   plex_independent_theme, plex_theme_verified_ok, "
        "   first_seen_at, last_seen_at) "
        "VALUES (?, '1', 'movie', NULL, "
        "        'tt100', ?, 'X', 2020, 1, 0, "
        "        '/data/movies/X', 0, 1, "
        "        '2026-05-26', '2026-05-26')",
        (rk, tmdb_id),
    )


def test_upload_theme_with_download_only_stamps_backup_only(
    admin_client, tmp_path,
):
    """End-to-end: POST /api/plex_items/{rk}/upload-theme with
    download_only=1 multipart field. Verify the local_files row
    lands with last_place_attempt_reason='backup_only'."""
    client, settings = admin_client
    db = settings.db_path
    with sqlite3.connect(db) as conn:
        _seed_upload_target(conn, rk="rk-bk", tmdb_id=100)
        conn.commit()

    # POST a minimal valid MP3 body (any non-empty bytes work —
    # api_upload_theme doesn't decode the audio).
    fake_mp3 = b"ID3\x03\x00\x00\x00" + b"\x00" * 64
    r = client.post(
        "/api/plex_items/rk-bk/upload-theme",
        headers=AUTH,
        files={"file": ("theme.mp3", fake_mp3, "audio/mpeg")},
        data={"download_only": "1"},
    )
    assert r.status_code == 200, r.text

    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT last_place_attempt_reason "
            "FROM local_files "
            "WHERE media_type='movie' AND section_id='1'"
        ).fetchone()
    assert row is not None, (
        "v1.19.37: local_files row must exist after upload"
    )
    assert row[0] == "backup_only", (
        f"v1.19.37: download_only upload must stamp "
        f"last_place_attempt_reason='backup_only'; got {row[0]!r}"
    )


def test_upload_theme_without_download_only_does_not_stamp(
    admin_client, tmp_path,
):
    """Counter-guard: normal (non-backup) upload must NOT stamp
    backup_only — that would suppress the place job which IS
    the user's intent on a non-backup upload."""
    client, settings = admin_client
    db = settings.db_path
    with sqlite3.connect(db) as conn:
        _seed_upload_target(conn, rk="rk-replace", tmdb_id=200)
        conn.commit()

    fake_mp3 = b"ID3\x03\x00\x00\x00" + b"\x00" * 64
    r = client.post(
        "/api/plex_items/rk-replace/upload-theme",
        headers=AUTH,
        files={"file": ("theme.mp3", fake_mp3, "audio/mpeg")},
        # No download_only field → defaults to false.
    )
    assert r.status_code == 200, r.text

    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT last_place_attempt_reason "
            "FROM local_files "
            "WHERE media_type='movie' AND section_id='1'"
        ).fetchone()
    assert row is not None
    # Either NULL or anything-but-backup_only is acceptable.
    assert row[0] != "backup_only", (
        f"v1.19.37: non-backup upload must NOT stamp 'backup_only' "
        f"— that would suppress the place job; got {row[0]!r}"
    )
    # AND a place job must have been enqueued. The endpoint
    # allocates a synthetic negative tmdb_id when no real themes
    # row exists for the plex_items row's guid_tmdb (orphan
    # path) — query by section instead so we don't depend on
    # the synthetic ID.
    with sqlite3.connect(db) as conn:
        jobs = conn.execute(
            "SELECT COUNT(*) FROM jobs "
            "WHERE job_type='place' AND media_type='movie' "
            "  AND section_id='1'"
        ).fetchone()
    assert jobs[0] == 1, (
        f"v1.19.37: non-backup upload must enqueue a place job; "
        f"got {jobs[0]}"
    )
