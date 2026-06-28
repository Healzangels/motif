"""v1.15.75 — per-row download-only on SET URL + UPLOAD MP3 dialogs.

the user: "an additional action on P Rows or maybe in the
confirmation box after providing a url or upload file to say
only download and put in the DL location but not place
basically create a backup file in DL but allow plex to continue
to server the theme but gives the option to push that theme
later if wanted and have a local downloaded copy."

Same intent as v1.15.71's bulk-import `download_only` flag —
now wired into the per-row SET URL + UPLOAD MP3 dialogs. When
the user opens either dialog on a P row, a // DOWNLOAD ONLY
checkbox appears. Checked → the enqueued download (manual-url)
or skipped place-job (upload-theme) leaves Plex's theme intact.
The file still lands in local_files so a future PUSH TO PLEX
can promote motif's copy.

The checkbox is hidden on non-P rows (no Plex theme to "keep"
serving — the option is meaningless).
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core.auth import create_admin, init_auth_schema
from app.core.db import get_conn, init_db, transaction


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"
APP_JS = REPO / "app" / "web" / "static" / "app.js"
LIBRARY_HTML = REPO / "app" / "web" / "templates" / "library.html"


# ── Template: checkbox markup present ───────────────────────


def test_manual_url_dialog_has_download_only_checkbox():
    """The // SET URL dialog template must include the download-
    only checkbox + label so the JS can toggle visibility."""
    html = LIBRARY_HTML.read_text()
    dlg_start = html.index('id="manual-url-dlg"')
    dlg_end = html.index("</dialog>", dlg_start)
    dlg = html[dlg_start:dlg_end]
    assert 'id="manual-url-download-only"' in dlg
    assert 'id="manual-url-download-only-row"' in dlg, (
        "v1.15.75: the wrapping label/row needs an id so JS can "
        "hide it on non-P rows"
    )
    assert "DOWNLOAD ONLY" in dlg, (
        "v1.15.75: checkbox label must read 'DOWNLOAD ONLY' so "
        "the user understands the action"
    )


def test_upload_dialog_has_download_only_checkbox():
    """Same checkbox on the // UPLOAD MP3 dialog."""
    html = LIBRARY_HTML.read_text()
    dlg_start = html.index('id="upload-dlg"')
    dlg_end = html.index("</dialog>", dlg_start)
    dlg = html[dlg_start:dlg_end]
    assert 'id="upload-download-only"' in dlg
    assert 'id="upload-download-only-row"' in dlg
    assert "DOWNLOAD ONLY" in dlg


# ── JS: checkbox is gated on srcLetter === 'P' ──────────────


def test_manual_url_dialog_reveals_checkbox_only_on_p_rows():
    """openManualUrlDialog must reveal the row only when srcLetter
    is 'P' (Plex serves a theme already). On non-P rows the
    option is meaningless and the JS must hide it."""
    js = APP_JS.read_text()
    fn_start = js.index("function openManualUrlDialog(")
    fn_end = js.index("function closeManualUrlDialog(", fn_start)
    body = js[fn_start:fn_end]
    assert "manual-url-download-only-row" in body, (
        "v1.15.75: openManualUrlDialog must reference the row id "
        "to toggle visibility"
    )
    # The gate uses srcLetter === 'P'.
    assert "srcLetter === 'P'" in body or "srcLetter == 'P'" in body, (
        "v1.15.75: visibility must be gated on srcLetter === 'P' "
        "— non-P rows hide the option"
    )


def test_upload_dialog_reveals_checkbox_only_on_p_rows():
    """openUploadDialog must accept srcLetter + gate the checkbox
    visibility on it."""
    js = APP_JS.read_text()
    fn_start = js.index("function openUploadDialog(")
    fn_end = js.index("function closeUploadDialog(", fn_start)
    body = js[fn_start:fn_end]
    assert "srcLetter" in body, (
        "v1.15.75: openUploadDialog signature must accept srcLetter "
        "to gate the // DOWNLOAD ONLY checkbox"
    )
    assert "upload-download-only-row" in body
    assert "srcLetter === 'P'" in body or "srcLetter == 'P'" in body


def test_upload_theme_dispatch_passes_src_letter():
    """The library-row 'upload-theme' click dispatcher (the
    `else if (act === 'upload-theme') { openUploadDialog({...}) }`
    branch — not the isPlexAgentRow guard or the action-menu
    button labels) must pass btn.dataset.srcLetter through to
    openUploadDialog. Without this the checkbox gating is dead
    code."""
    js = APP_JS.read_text()
    # Anchor on the dispatch branch specifically — it's the one
    # that immediately invokes openUploadDialog({.
    needle = "else if (act === 'upload-theme') {"
    idx = js.index(needle)
    chunk = js[idx:idx + 400]
    assert "openUploadDialog(" in chunk, "test anchor stale"
    assert "srcLetter" in chunk, (
        "v1.15.75: the upload-theme dispatch branch must pass "
        "btn.dataset.srcLetter to openUploadDialog so the dialog "
        "can gate the // DOWNLOAD ONLY checkbox"
    )


# ── JS submit passes the flag ───────────────────────────────


def test_manual_url_submit_passes_download_only_body_field():
    """When the checkbox is checked, the POST body must include
    `download_only: true`. Without this the backend has no signal."""
    js = APP_JS.read_text()
    fn_start = js.index("function bindManualUrlDialog(")
    fn_end = js.index("// ---- Manual upload", fn_start)
    body = js[fn_start:fn_end]
    assert "download_only" in body, (
        "v1.15.75: manual-url submit must include download_only "
        "in the POST body when the checkbox is checked"
    )


def test_upload_submit_passes_download_only_form_field():
    """Upload form must append `download_only=1` to the
    multipart body when the checkbox is checked."""
    js = APP_JS.read_text()
    fn_start = js.index("function bindUploadDialog(")
    fn_end = js.index("\n  function ", fn_start + 1)
    body = js[fn_start:fn_end]
    assert "fd.append('download_only'" in body, (
        "v1.15.75: upload submit must append download_only to the "
        "FormData when the checkbox is checked"
    )


# ── Backend honors the flag ─────────────────────────────────


def test_api_manual_url_reads_download_only_from_body():
    """The manual-url endpoint must read body.download_only.

    v1.16.4 update: the payload representation changed from
    `force_place=not download_only` to a branch:
      - download_only=True  → payload["auto_place"] = False
      - download_only=False → payload["force_place"] = True
    The worker's post-download gate reads auto_place (falls back
    to settings.auto_place_default) AND force_place
    INDEPENDENTLY — `force_place=False` doesn't suppress place
    enqueueing, only `auto_place=False` does. Setting force_place
    to False (the pre-v1.16.4 representation) still enqueued a
    non-forcing place job that ran, skipped on plex_has_theme,
    but painted the amber PL pulse mid-flight.
    """
    src = API_PY.read_text()
    fn_start = src.index('@app.post("/api/plex_items/{rating_key}/manual-url"')
    fn_end = src.index("@app.post", fn_start + 1)
    body = src[fn_start:fn_end]
    assert 'body or {}).get("download_only")' in body, (
        "v1.15.75: manual-url must read download_only from the "
        "request body"
    )
    # v1.16.4: download_only=True must set auto_place=False (not
    # force_place=False — the worker honors auto_place to skip
    # the post-download place-enqueue branch).
    assert 'payload["auto_place"] = False' in body, (
        "v1.16.4: download_only=True must set auto_place=False "
        "in the payload — that's what the worker reads to skip "
        "enqueueing the place job. The pre-v1.16.4 representation "
        "(force_place=False) didn't suppress the place job, so "
        "the row's PL pip briefly pulsed amber before the job "
        "skipped at runtime with plex_has_theme."
    )


def test_api_upload_theme_reads_download_only_from_form():
    """The upload-theme endpoint must read form.download_only and
    skip the place-job enqueue when it's set."""
    src = API_PY.read_text()
    fn_start = src.index('@app.post("/api/plex_items/{rating_key}/upload-theme"')
    fn_end = src.index("@app.post", fn_start + 1)
    body = src[fn_start:fn_end]
    assert 'form.get("download_only")' in body, (
        "v1.15.75: upload-theme must read download_only from the "
        "multipart form"
    )
    assert "not download_only" in body, (
        "v1.15.75: place enqueue must be gated on `not download_only` "
        "so the backup-only path skips the placement step"
    )


# ── End-to-end: download_only sets force_place=false ────────


@pytest.fixture
def app_client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    settings = Settings(config_dir=tmp_path,
                        data_dir=tmp_path / "data")
    db = settings.db_path
    init_db(db)
    init_auth_schema(db)
    create_admin(db, username="testadmin", password="testpassword")
    from app.web.api import create_app
    app = create_app(settings)
    client = TestClient(app)
    return client, settings


def test_manual_url_download_only_enqueues_auto_place_false(app_client):
    """End-to-end: SET URL with download_only=true → download job's
    payload carries auto_place=False (NOT force_place=False — see
    v1.16.4 rationale in test_api_manual_url_reads_download_only_
    from_body)."""
    client, settings = app_client
    with get_conn(settings.db_path) as conn, transaction(conn):
        conn.execute(
            """INSERT INTO themes
                 (media_type, tmdb_id, imdb_id, title, year,
                  upstream_source, last_seen_sync_at,
                  first_seen_sync_at)
               VALUES ('movie', 75001, 'tt75001', 'P Row', 2020,
                       'themoviedb', '2026-01-01', '2026-01-01')"""
        )
        conn.execute(
            """INSERT INTO plex_items
                 (rating_key, section_id, media_type, title, year,
                  guid_imdb, has_theme, theme_id, first_seen_at,
                  last_seen_at)
               VALUES ('rk-75001', '1', 'movie', 'P Row', '2020',
                       'tt75001', 1,
                       (SELECT id FROM themes WHERE tmdb_id = 75001),
                       '2026-01-01', '2026-01-01')"""
        )
    resp = client.post(
        "/api/plex_items/rk-75001/manual-url",
        json={
            "youtube_url": "https://www.youtube.com/watch?v=backup00001",
            "download_only": True,
        },
        headers={"X-Authentik-Username": "testadmin"},
    )
    assert resp.status_code == 200, resp.text
    with get_conn(settings.db_path) as conn:
        rows = conn.execute(
            "SELECT payload FROM jobs WHERE job_type = 'download' "
            "  AND tmdb_id = 75001 AND status = 'pending'"
        ).fetchall()
        assert rows, "v1.15.75: download job must still be enqueued"
        payload = json.loads(rows[0]["payload"])
        # v1.16.4: new contract — download_only=True sets
        # auto_place=False (not force_place=False).
        assert payload.get("auto_place") is False, (
            "v1.16.4: download_only=true must set auto_place=False "
            f"so the worker skips the place-enqueue branch; got "
            f"payload={payload!r}"
        )
        assert "force_place" not in payload, (
            "v1.16.4: force_place must NOT be set on the "
            "download_only path — its presence with True would "
            "flip auto_place back to True via the worker's gate."
        )


def test_manual_url_default_force_place_is_true(app_client):
    """Counter-test: SET URL without download_only (the existing
    behavior every v1.11-v1.15.74 caller used) must still default
    to force_place=true."""
    client, settings = app_client
    with get_conn(settings.db_path) as conn, transaction(conn):
        conn.execute(
            """INSERT INTO themes
                 (media_type, tmdb_id, imdb_id, title, year,
                  upstream_source, last_seen_sync_at,
                  first_seen_sync_at)
               VALUES ('movie', 75002, 'tt75002', 'Default', 2020,
                       'themoviedb', '2026-01-01', '2026-01-01')"""
        )
        conn.execute(
            """INSERT INTO plex_items
                 (rating_key, section_id, media_type, title, year,
                  guid_imdb, has_theme, theme_id, first_seen_at,
                  last_seen_at)
               VALUES ('rk-75002', '1', 'movie', 'Default', '2020',
                       'tt75002', 1,
                       (SELECT id FROM themes WHERE tmdb_id = 75002),
                       '2026-01-01', '2026-01-01')"""
        )
    resp = client.post(
        "/api/plex_items/rk-75002/manual-url",
        json={"youtube_url": "https://www.youtube.com/watch?v=default0001"},
        headers={"X-Authentik-Username": "testadmin"},
    )
    assert resp.status_code == 200
    with get_conn(settings.db_path) as conn:
        rows = conn.execute(
            "SELECT payload FROM jobs WHERE job_type = 'download' "
            "  AND tmdb_id = 75002"
        ).fetchall()
        payload = json.loads(rows[0]["payload"])
        assert payload.get("force_place") is True, (
            "v1.15.75: missing download_only must default to "
            "force_place=true (v1.11-v1.15.74 behavior)"
        )
