"""v0.51.201 — Phase 2 Tag 6: per-theme LEVEL LOUDNESS on UPLOAD MP3 + SET URL.

A checkbox on each dialog (default UNCHECKED = raw, per the operator's D2) that
conditions THIS theme toward the target before it's placed — the cheap half (Plex
only ever ingests the leveled copy). Two paths, one engine:

  - UPLOAD MP3 is synchronous, so the endpoint conditions the file inline and stamps
    the loudness/normalize columns via the SAME worker._cond_columns mapping (no drift).
    A RAW upload must also CLEAR any stale norm_state left by a prior normalized upload,
    or // UNDO would try to restore the wrong original over the new bytes.
  - SET URL downloads via the worker, so the endpoint threads a `normalize` field into
    the download job payload; the worker honors it OVER the global normalize_on_download
    toggle (force one theme leveled with the global off, or raw with it on). Absent →
    the global setting stands (backward compat for TDB auto-downloads).
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core.auth import create_admin, init_auth_schema
from app.core.db import init_db
from app.core.events import now_iso

NOW = now_iso()
AUTH = {"X-Authentik-Username": "testadmin"}
MP3 = b"ID3" + b"\x00" * 64   # passes _looks_like_audio
RK = "9500"
TMDB = 500
REPO = Path(__file__).resolve().parent.parent
WORKER_PY = (REPO / "app" / "core" / "worker.py").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
LIB_HTML = (REPO / "app" / "web" / "templates" / "library.html").read_text()


@pytest.fixture
def app_client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.delenv("MOTIF_NORMALIZE_ON_DOWNLOAD", raising=False)
    # v0.51.201: another test leaks MOTIF_LOUDNESS_TARGET via os.environ (persists past
    # its run); without clearing it, settings.loudness_target_lufs clamps to the floor and
    # norm_target lands at -31 not the -18 default. Same env-leak class as v0.51.197.
    monkeypatch.delenv("MOTIF_LOUDNESS_TARGET", raising=False)
    from app.config import Settings
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    settings._cfg.paths.themes_dir = str(tmp_path / "themes")
    db = settings.db_path
    init_db(db)
    init_auth_schema(db)
    create_admin(db, username="testadmin", password="testpassword")
    from app.web.api import create_app
    return TestClient(create_app(settings)), db, monkeypatch


def _seed(db, *, has_theme=1, norm_state=None):
    """One movie row with a standard ('') edition. norm_state pre-seeds a leveled
    local_files row (for the raw-clears-stale test)."""
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime, is_4k,"
            " themes_subdir, included, discovered_at, last_seen_at)"
            " VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        cur = conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, year, upstream_source,"
            " last_seen_sync_at, first_seen_sync_at, youtube_url)"
            " VALUES ('movie',?,'M','2001','imdb',?,?,'u')", (TMDB, NOW, NOW))
        tid = cur.lastrowid
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type, theme_id,"
            " guid_tmdb, title, year, edition_key, folder_path, has_theme,"
            " first_seen_at, last_seen_at) VALUES (?,'1','movie',?,?,'M','2001','',"
            "'/data/Movies/M (2001)',?,?,?)", (RK, tid, TMDB, has_theme, NOW, NOW))
        if norm_state is not None:
            conn.execute(
                "INSERT INTO local_files (media_type, tmdb_id, section_id, edition_key,"
                " file_path, file_sha256, downloaded_at, source_video_id, provenance,"
                " source_kind, norm_state, norm_gain_db, norm_target, norm_at,"
                " norm_orig_sha256, norm_orig_pcm_sha256, loudness_i)"
                " VALUES ('movie',?,'1','','movies/500.mp3','oldsha',?,'v','manual',"
                "'upload',?, -6.0, -18.0, ?, 'oldorig', 'oldpcm', -18.0)",
                (TMDB, NOW, norm_state, NOW))
        conn.commit()
    return tid


def _lf(db):
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        return conn.execute(
            "SELECT * FROM local_files WHERE tmdb_id=? AND section_id='1'"
            " AND edition_key=''", (TMDB,)).fetchone()


def _fake_condition(*, called):
    def _fn(theme, *, target_lufs):
        called.append(target_lufs)
        theme.write_bytes(b"ID3LEVELED" + b"\x00" * 40)   # bytes change → new sha
        return {"ok": True, "changed": True, "note": "leveled",
                "applied_db": -6.0, "target": target_lufs,
                "before_i": -6.0, "before_tp": -1.0,
                "loudness_i": -18.0, "true_peak": -3.0, "lra": 5.0,
                "file_sha256": "leveledsha", "orig_sha256": "origsha",
                "orig_pcm_sha256": "origpcm"}
    return _fn


# ── UPLOAD MP3: inline conditioning ──────────────────────────────────────────


def test_upload_normalize_conditions_and_stamps_norm_columns(app_client):
    client, db, mp = app_client
    _seed(db)
    called = []
    mp.setattr("app.core.loudness_apply.condition_new_download",
               _fake_condition(called=called))
    r = client.post(f"/api/plex_items/{RK}/upload-theme",
                    files={"file": ("theme.mp3", MP3, "audio/mpeg")},
                    data={"normalize": "1"}, headers=AUTH)
    assert r.status_code == 200, r.text
    assert called, "condition_new_download must run when normalize is checked"
    row = _lf(db)
    assert row["norm_state"] == "normalized"
    assert row["norm_gain_db"] == -6.0
    assert row["norm_target"] == -18.0
    assert row["norm_orig_sha256"] == "origsha"
    assert row["norm_orig_pcm_sha256"] == "origpcm"
    assert row["loudness_i"] == -18.0
    # the recorded sha/size describe the POST-gain file, not the raw upload.
    assert row["file_sha256"] == "leveledsha"


def test_upload_raw_leaves_norm_null_and_does_not_condition(app_client):
    client, db, mp = app_client
    _seed(db)
    called = []
    mp.setattr("app.core.loudness_apply.condition_new_download",
               _fake_condition(called=called))
    r = client.post(f"/api/plex_items/{RK}/upload-theme",
                    files={"file": ("theme.mp3", MP3, "audio/mpeg")},
                    headers=AUTH)   # no normalize field → raw
    assert r.status_code == 200, r.text
    assert not called, "a raw upload must NOT condition"
    row = _lf(db)
    assert row["norm_state"] is None
    assert row["norm_gain_db"] is None


def test_upload_raw_clears_stale_norm_state(app_client):
    """A raw re-upload over a previously-normalized row must NULL the norm columns —
    else // UNDO would try to restore 'oldorig' over the fresh raw bytes."""
    client, db, mp = app_client
    _seed(db, norm_state="normalized")
    # (no monkeypatch needed — raw path doesn't call condition)
    r = client.post(f"/api/plex_items/{RK}/upload-theme",
                    files={"file": ("theme.mp3", MP3, "audio/mpeg")},
                    headers=AUTH)
    assert r.status_code == 200, r.text
    row = _lf(db)
    assert row["norm_state"] is None, "stale norm_state must be cleared on a raw upload"
    assert row["norm_orig_sha256"] is None
    assert row["norm_orig_pcm_sha256"] is None


# ── SET URL: normalize threaded into the download job payload ─────────────────


def _download_payload(db):
    with sqlite3.connect(db) as conn:
        row = conn.execute("SELECT payload FROM jobs WHERE job_type='download'"
                           " ORDER BY id DESC LIMIT 1").fetchone()
    return json.loads(row[0]) if row else None


def test_manual_url_threads_normalize_true(app_client):
    client, db, _ = app_client
    _seed(db, has_theme=0)
    r = client.post(f"/api/plex_items/{RK}/manual-url",
                    json={"youtube_url": "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
                          "normalize": True}, headers=AUTH)
    assert r.status_code == 200, r.text
    assert _download_payload(db).get("normalize") is True


def test_manual_url_threads_normalize_false(app_client):
    client, db, _ = app_client
    _seed(db, has_theme=0)
    r = client.post(f"/api/plex_items/{RK}/manual-url",
                    json={"youtube_url": "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
                          "normalize": False}, headers=AUTH)
    assert r.status_code == 200, r.text
    # explicit False forces raw — must be present so the worker overrides the global.
    assert _download_payload(db).get("normalize") is False


def test_manual_url_string_false_does_not_enable_leveling(app_client):
    """v0.51.202 (review #3): a JSON string "false" is truthy under bool(), so an API
    caller could accidentally level. The endpoint must coerce falsey strings to False."""
    client, db, _ = app_client
    _seed(db, has_theme=0)
    r = client.post(f"/api/plex_items/{RK}/manual-url",
                    json={"youtube_url": "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
                          "normalize": "false"}, headers=AUTH)
    assert r.status_code == 200, r.text
    assert _download_payload(db).get("normalize") is False


def test_manual_url_absent_normalize_leaves_no_key(app_client):
    """An older client / API caller that omits normalize → no key → the worker
    falls back to the global normalize_on_download setting."""
    client, db, _ = app_client
    _seed(db, has_theme=0)
    r = client.post(f"/api/plex_items/{RK}/manual-url",
                    json={"youtube_url": "https://www.youtube.com/watch?v=dQw4w9WgXcQ"},
                    headers=AUTH)
    assert r.status_code == 200, r.text
    assert "normalize" not in _download_payload(db)


# ── worker gate honors the payload override ──────────────────────────────────


def test_worker_download_gate_honors_payload_normalize():
    """The conditioning gate must read the per-job override + fall back to the global,
    not the old bare `if self.settings.normalize_on_download:`."""
    assert 'payload.get("normalize")' in WORKER_PY
    # v0.51.203: the gate decision moved into _should_condition_download (the explicit
    # per-job override still wins, then auto-added vs manual — see the v0.51.203 gate tests).
    assert "_should_condition_download(self.settings, payload)" in WORKER_PY
    assert "def _should_condition_download(" in WORKER_PY
    # the pre-Tag-6 unconditional gate (global only) must be gone.
    assert ("if self.settings.normalize_on_download:\n"
            "            from .loudness_apply import condition_new_download"
            not in WORKER_PY)


# ── UI wiring ────────────────────────────────────────────────────────────────


def test_both_dialogs_have_the_checkbox():
    assert 'id="upload-normalize"' in LIB_HTML
    assert 'id="manual-url-normalize"' in LIB_HTML
    assert LIB_HTML.count("LEVEL LOUDNESS") >= 2


def test_both_handlers_send_normalize_and_reset_on_open():
    # upload: FormData append + reset-on-open
    assert "fd.append('normalize', '1')" in APP_JS
    # set url: explicit boolean in the body
    assert "body.normalize = !!normInput.checked" in APP_JS
    # both dialogs reset the checkbox to unchecked (raw default) each open
    assert APP_JS.count("normInput.checked = false") == 2


def test_endpoint_and_upload_share_the_cond_columns_mapping():
    """UPLOAD MP3's norm-column write reuses worker._cond_columns (no drift with the
    download path) and condition_new_download for the conditioning itself."""
    assert "from ..core.worker import _cond_columns" in API_PY
    assert "from ..core.loudness_apply import condition_new_download" in API_PY
