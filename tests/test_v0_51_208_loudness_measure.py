"""v0.51.208 — the per-item loudness probe (// RE-MEASURE / // MEASURE NOW).

The loudness analogue of // PROBE TDB URL: POST /api/admin/loudness/measure-one re-reads a
theme's CURRENT bytes, stamps loudness_i/tp/lra + measured_sha256 + file_sha256 for the
edition-scoped PK, and returns the fresh measurement + derived marker. It is READ-ONLY
against the audio — it never rewrites the theme or touches Plex.

measure_loudness is monkeypatched (no ffmpeg in CI); the file bytes are real so the sha the
endpoint stamps is verifiable.
"""
from __future__ import annotations

import hashlib
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest
from starlette.testclient import TestClient

from app.core.db import init_db, get_conn

NOW = datetime.now(timezone.utc).isoformat(timespec="seconds")
AUTH = {"X-Authentik-Username": "testadmin"}
REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()
THEME_BYTES = b"a-real-theme-file-body"
THEME_SHA = hashlib.sha256(THEME_BYTES).hexdigest()


@pytest.fixture
def client_and_db(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    # v0.51.208: pin the loudness target to its -18 default — the marker assertions depend
    # on the outlier threshold, and other loudness tests leak MOTIF_LOUDNESS_TARGET (the
    # v0.51.201 leak). Without this a leaked floor (-31) makes -20 read as an outlier.
    monkeypatch.delenv("MOTIF_LOUDNESS_TARGET", raising=False)
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.web.api import create_app
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    themes = tmp_path / "themes"
    (themes / "movies" / "1").mkdir(parents=True, exist_ok=True)
    (themes / "movies" / "1" / "theme.mp3").write_bytes(THEME_BYTES)
    monkeypatch.setattr(Settings, "themes_dir", property(lambda self: themes))
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    with sqlite3.connect(s.db_path) as c:
        c.execute("INSERT INTO plex_sections (section_id, title, type, is_anime,"
                  " is_4k, themes_subdir, included, discovered_at, last_seen_at) "
                  "VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        c.commit()
    return TestClient(create_app(s)), s.db_path


def _seed(db, *, normalized=False, loudness_i=None, file_path="movies/1/theme.mp3"):
    with sqlite3.connect(db) as c:
        c.execute("PRAGMA foreign_keys = OFF")
        c.execute("INSERT OR IGNORE INTO themes (id, media_type, tmdb_id, title, year, "
                  " upstream_source, last_seen_sync_at, first_seen_sync_at) "
                  "VALUES (1,'movie',1,'T','2000','imdb',?,?)", (NOW, NOW))
        cols = ("media_type, tmdb_id, section_id, edition_key, file_path, file_sha256, "
                "downloaded_at, source_video_id, file_size, loudness_i, loudness_measured_sha256")
        vals = ["movie", 1, "1", "", file_path, "OLD_SHA", NOW, "vid", 1000, loudness_i, "OLD_SHA"]
        if normalized:
            cols += ", norm_state, norm_gain_db, norm_target, norm_at"
            vals += ["normalized", -13.5, -18.0, NOW]
        c.execute(f"INSERT INTO local_files ({cols}) VALUES ({','.join('?'*len(vals))})", vals)
        c.commit()


def _mock_measure(monkeypatch, i=-20.0, tp=-3.0, lra=5.0):
    monkeypatch.setattr("app.core.loudness.measure_loudness",
                        lambda *a, **k: {"loudness_i": i, "true_peak": tp, "lra": lra})


def _post(c, **over):
    body = {"media_type": "movie", "tmdb_id": 1, "section_id": "1", "edition_key": ""}
    body.update(over)
    return c.post("/api/admin/loudness/measure-one", json=body, headers=AUTH)


def _row(db):
    with get_conn(db) as c:
        return c.execute("SELECT loudness_i, loudness_tp, loudness_lra, loudness_measured_sha256, "
                         "file_sha256, norm_state FROM local_files WHERE tmdb_id=1").fetchone()


def test_remeasure_stamps_the_current_bytes(client_and_db, monkeypatch):
    c, db = client_and_db
    _seed(db, loudness_i=None)          # never measured
    _mock_measure(monkeypatch)
    r = _post(c)
    assert r.status_code == 200
    j = r.json()
    assert j["ok"] is True
    assert j["loudness_i"] == -20.0 and j["true_peak"] == -3.0
    assert j["loudness_marker"] == "raw"      # -20 < target(-18)+margin(6) = -12
    row = _row(db)
    assert row["loudness_i"] == -20.0 and row["loudness_tp"] == -3.0 and row["loudness_lra"] == 5.0
    # measured_sha256 == file_sha256 == the ACTUAL file bytes' sha → measurement is "current".
    assert row["loudness_measured_sha256"] == THEME_SHA
    assert row["file_sha256"] == THEME_SHA


def test_remeasure_is_read_only_no_norm_state_change(client_and_db, monkeypatch):
    """Re-measuring a LEVELED row refreshes the number but must not un-level it or push Plex."""
    c, db = client_and_db
    _seed(db, normalized=True, loudness_i=-18.0)
    _mock_measure(monkeypatch, i=-18.2, tp=-9.0)
    # tripwire: the read-only probe must never push to Plex.
    from app.web import api
    monkeypatch.setattr(api, "_push_theme_to_plex",
                        lambda *a, **k: pytest.fail("re-measure must not touch Plex"))
    j = _post(c).json()
    assert j["ok"] is True and j["loudness_marker"] == "leveled"
    assert _row(db)["norm_state"] == "normalized"


def test_silent_theme_does_not_ship_negative_infinity(client_and_db, monkeypatch):
    c, db = client_and_db
    _seed(db, loudness_i=None)
    _mock_measure(monkeypatch, i=float("-inf"), tp=float("-inf"))
    j = _post(c).json()
    assert j["ok"] is True
    assert j["loudness_i"] is None and j["true_peak"] is None   # never a raw -inf on the wire
    assert j["clipping"] is False


def test_missing_file_is_a_clean_error_not_a_500(client_and_db, monkeypatch):
    c, db = client_and_db
    _seed(db, loudness_i=-15.0, file_path="movies/1/GONE.mp3")
    _mock_measure(monkeypatch)
    r = _post(c)
    assert r.status_code == 200
    assert r.json()["ok"] is False and "missing on disk" in r.json()["error"]


def test_missing_identity_is_a_400(client_and_db, monkeypatch):
    c, db = client_and_db
    _seed(db, loudness_i=-15.0)
    r = c.post("/api/admin/loudness/measure-one", json={"section_id": "1"}, headers=AUTH)
    assert r.status_code == 400


# ── source-shape guards ──────────────────────────────────────────────────────

def test_endpoint_is_wired_and_read_only():
    assert '@app.post("/api/admin/loudness/measure-one")' in API_PY
    # the endpoint body derives the marker via the shared helper + never pushes to Plex.
    start = API_PY.index('@app.post("/api/admin/loudness/measure-one")')
    end = API_PY.index('@app.post("/api/admin/loudness/bulk-normalize")', start)
    body = API_PY[start:end]
    assert "_loudness_marker(" in body
    assert "_push_theme_to_plex" not in body, "the probe must be read-only against Plex"


def test_button_and_handler_are_wired_in_js():
    assert 'data-act="loud-measure"' in APP_JS
    assert "// RE-MEASURE" in APP_JS and "// MEASURE NOW" in APP_JS
    assert "/api/admin/loudness/measure-one" in APP_JS
    # on success it re-opens the card so the stepper base re-seeds (not a stale inline patch).
    h = APP_JS[APP_JS.index('data-act="loud-measure"]\')?.addEventListener'):]
    assert "openInfoDialog(mediaType, tmdbId, sectionId, ratingKey)" in h[:2000]
