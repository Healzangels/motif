"""v0.51.212 — data-integrity hardening for the v0.51.208 per-item loudness probe.

The probe was the only writer of file_sha256 that (a) hashed AFTER measuring, (b) wrote the
sha without its file_size, and (c) wrote blind on the PK with no re-check of the state it
read. Together those let a LEVEL landing during the ~1s ffmpeg window be silently overwritten
with pre-level numbers that the audit's `measured_sha256 == file_sha256` skip then made
PERMANENT — a row stuck reading "leveled" while reporting raw loudness forever.

Every test here drives the real endpoint against real bytes on disk; measure_loudness is the
only mock, and it doubles as the injection point for the concurrent-writer races.
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
API_PY = (REPO / "app" / "web" / "api.py").read_text()
THEME_BYTES = b"a-real-theme-file-body"
THEME_SHA = hashlib.sha256(THEME_BYTES).hexdigest()


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.delenv("MOTIF_LOUDNESS_TARGET", raising=False)   # v0.51.201 env leak
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.web.api import create_app
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    themes = tmp_path / "themes"
    (themes / "movies" / "1").mkdir(parents=True, exist_ok=True)
    theme = themes / "movies" / "1" / "theme.mp3"
    theme.write_bytes(THEME_BYTES)
    monkeypatch.setattr(Settings, "themes_dir", property(lambda self: themes))
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    with sqlite3.connect(s.db_path) as c:
        c.execute("INSERT INTO plex_sections (section_id, title, type, is_anime,"
                  " is_4k, themes_subdir, included, discovered_at, last_seen_at) "
                  "VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        c.commit()
    return TestClient(create_app(s)), s.db_path, theme


def _seed(db, *, normalized=False, loudness_i=None, sha="OLD_SHA", file_size=999999):
    with sqlite3.connect(db) as c:
        c.execute("PRAGMA foreign_keys = OFF")
        c.execute("INSERT OR IGNORE INTO themes (id, media_type, tmdb_id, title, year, "
                  " upstream_source, last_seen_sync_at, first_seen_sync_at) "
                  "VALUES (1,'movie',1,'T','2000','imdb',?,?)", (NOW, NOW))
        cols = ("media_type, tmdb_id, section_id, edition_key, file_path, file_sha256, "
                "downloaded_at, source_video_id, file_size, loudness_i, "
                "loudness_measured_sha256")
        vals = ["movie", 1, "1", "", "movies/1/theme.mp3", sha, NOW, "vid", file_size,
                loudness_i, sha]
        if normalized:
            cols += ", norm_state, norm_gain_db, norm_target, norm_at, norm_orig_sha256"
            vals += ["normalized", -13.5, -18.0, NOW, "PRE_LEVEL_SHA"]
        c.execute(f"INSERT INTO local_files ({cols}) VALUES ({','.join('?'*len(vals))})", vals)
        c.commit()


def _measure(monkeypatch, i=-20.0, tp=-3.0, lra=5.0, side_effect=None):
    def _m(*a, **k):
        if side_effect is not None:
            side_effect()
        return {"loudness_i": i, "true_peak": tp, "lra": lra}
    monkeypatch.setattr("app.core.loudness.measure_loudness", _m)


def _post(c):
    return c.post("/api/admin/loudness/measure-one",
                  json={"media_type": "movie", "tmdb_id": 1, "section_id": "1",
                        "edition_key": ""}, headers=AUTH)


def _row(db):
    with get_conn(db) as c:
        return c.execute("SELECT loudness_i, loudness_tp, file_sha256, file_size, norm_state, "
                         "loudness_measured_sha256 FROM local_files WHERE tmdb_id=1").fetchone()


# ── the three integrity holes ────────────────────────────────────────────────

def test_file_size_is_refreshed_with_the_sha(env, monkeypatch):
    """The v0.51.177 Plex-ceiling gate reads file_size beside file_sha256. Re-stamping the
    sha alone marked an over-ceiling row's measurement 'current' while its size still
    described the OLD bytes — which re-qualified it for a LEVEL whose ~12MB re-upload
    Plex answers with a 500."""
    c, db, _ = env
    _seed(db, file_size=999999)          # stale: claims ~1MB, the real file is 22 bytes
    _measure(monkeypatch)
    assert _post(c).json()["ok"] is True
    row = _row(db)
    assert row["file_sha256"] == THEME_SHA
    assert row["file_size"] == len(THEME_BYTES), "file_size must ride with file_sha256"


def test_a_level_landing_mid_measure_is_not_clobbered(env, monkeypatch):
    """The headline race. A blind PK write put PRE-level loudness onto a row another writer
    had just marked normalized — and because measured_sha256 then equalled file_sha256, the
    audit's staleness skip made that wrong number permanent."""
    c, db, _ = env
    _seed(db, loudness_i=None)           # raw: norm_state NULL, sha OLD_SHA
    levelled_sha = hashlib.sha256(b"post-level-bytes").hexdigest()

    def _level_lands():
        with sqlite3.connect(db) as x:
            x.execute("UPDATE local_files SET norm_state='normalized', file_sha256=?, "
                      "loudness_i=-18.0, norm_gain_db=-13.5 WHERE tmdb_id=1", (levelled_sha,))
            x.commit()

    _measure(monkeypatch, i=-5.0, side_effect=_level_lands)   # -5.0 = the PRE-level number
    j = _post(c).json()
    assert j["ok"] is False and "landed while measuring" in j["error"]
    row = _row(db)
    assert row["norm_state"] == "normalized", "the concurrent LEVEL must survive"
    assert row["loudness_i"] == -18.0, "pre-level loudness must NOT overwrite the leveled row"
    assert row["file_sha256"] == levelled_sha


def test_bytes_replaced_mid_measure_are_refused(env, monkeypatch):
    """TOCTOU: measure-then-hash paired OLD-bytes loudness with NEW-bytes sha — a
    self-consistent lie that _normalize_one_row's staleness gate and loudness_apply's
    expect_sha re-hash both accept, so mp3gain would apply a gain computed from audio
    that is no longer on disk."""
    c, db, theme = env
    _seed(db, loudness_i=-15.0)
    _measure(monkeypatch, i=-5.0,
             side_effect=lambda: theme.write_bytes(b"completely-different-audio"))
    j = _post(c).json()
    assert j["ok"] is False and "changed while it was being measured" in j["error"]
    row = _row(db)
    assert row["loudness_i"] == -15.0 and row["file_sha256"] == "OLD_SHA", "nothing written"


def test_out_of_band_replacement_of_a_leveled_file_is_surfaced(env, monkeypatch):
    """On a normalized row a stored sha that disagrees with disk is the ONLY record that the
    leveled file was swapped out from under motif — i.e. that norm_orig_pcm_sha256 anchors
    bytes that no longer exist and UNDO can never restore them. Re-stamping it silently
    erased the evidence."""
    c, db, _ = env
    _seed(db, normalized=True, loudness_i=-18.0, sha="STALE_SHA")
    seen = []
    from app.web import api
    monkeypatch.setattr(api, "log_event", lambda *a, **k: seen.append(k))
    _measure(monkeypatch, i=-18.4)
    assert _post(c).json()["ok"] is True          # it still measures — it just tells you
    assert any(e.get("level") == "WARNING" and "UNDO" in e.get("message", "") for e in seen), \
        "a leveled file replaced out-of-band must leave a breadcrumb"


def test_a_raw_row_with_new_bytes_is_not_flagged(env, monkeypatch):
    """The complement: on an unleveled row a changed sha is the NORMAL case (a re-download),
    so it must not cry wolf — the warning is specifically about dead undo anchors."""
    c, db, _ = env
    _seed(db, loudness_i=-9.0, sha="STALE_SHA")   # raw, norm_state NULL
    seen = []
    from app.web import api
    monkeypatch.setattr(api, "log_event", lambda *a, **k: seen.append(k))
    _measure(monkeypatch)
    assert _post(c).json()["ok"] is True
    assert not [e for e in seen if e.get("level") == "WARNING"]


# ── source-shape guard ───────────────────────────────────────────────────────

def test_the_write_is_a_compare_and_set():
    """Pin the shape, not just the behavior: the UPDATE must re-assert the (norm_state,
    file_sha256) pair it read, or the race above silently returns."""
    start = API_PY.index('@app.post("/api/admin/loudness/measure-one")')
    body = API_PY[start:API_PY.index('@app.post("/api/admin/loudness/bulk-normalize")', start)]
    assert "AND norm_state IS ? AND file_sha256 IS ?" in body
    assert "file_size=?" in body, "file_size must be written with file_sha256"
    assert body.index("sha = _sha256(theme)") < body.index("m = measure_loudness(theme)"), \
        "hash BEFORE measuring — measure-then-hash is the TOCTOU"
