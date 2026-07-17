"""v0.51.194 — Phase 2 groundwork: the per-row normalize chokepoint.

normalize-one's per-row work (guards → over-ceiling → rk → entry_before → mp3gain →
DB write → Plex re-upload) is extracted VERBATIM into module-level `_normalize_one_row`
so // NORMALIZE (one row) and the coming bulk op call ONE implementation. A second copy
in the bulk op would be the mirror-drift class this codebase keeps paying for.

The endpoint's own behavioral tests (test_v0_51_169) prove the endpoint still works;
this pins the chokepoint DIRECTLY, because the bulk op calls it directly (not via HTTP),
and its guards are what keep a bulk run from mutating an ineligible row.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from app.core.db import init_db, get_conn

REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()
NOW = "2026-07-17T00:00:00"


def test_chokepoint_is_module_level_and_the_endpoint_delegates():
    from app.web.api import _normalize_one_row  # module-level import must resolve
    assert callable(_normalize_one_row)
    # normalize-one's _run must call it, not re-implement the mutation
    assert "return _normalize_one_row(db, settings, row, target)" in API_PY
    # and the mutation must live in the chokepoint, not the endpoint
    choke = API_PY[API_PY.index("def _normalize_one_row("):API_PY.index("def create_app(")]
    assert "norm_state='normalized'" in choke
    assert "_push_theme_to_plex(settings" in choke


@pytest.fixture
def db_and_settings(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    themes = tmp_path / "themes"
    themes.mkdir(exist_ok=True)
    monkeypatch.setattr(Settings, "themes_dir", property(lambda self: themes))
    # plex OFF so the entry_before snapshot is skipped; _push is monkeypatched per-test
    monkeypatch.setattr(Settings, "plex_url", property(lambda self: None))
    monkeypatch.setattr(Settings, "plex_token", property(lambda self: None))
    init_db(s.db_path)
    with sqlite3.connect(s.db_path) as c:
        c.execute("PRAGMA foreign_keys = OFF")
        c.execute("INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source, "
                  " last_seen_sync_at, first_seen_sync_at) "
                  "VALUES (5, 'movie', 5, 'Loud', 'imdb', ?, ?)", (NOW, NOW))
        c.execute("INSERT INTO plex_items (rating_key, media_type, section_id, guid_tmdb, "
                  " edition_key, title, has_theme, first_seen_at, last_seen_at) "
                  "VALUES ('9005', 'movie', '1', 5, '', 'Loud', 1, ?, ?)", (NOW, NOW))
        c.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, edition_key, "
                  " file_path, file_sha256, loudness_measured_sha256, downloaded_at, "
                  " source_video_id, loudness_i, loudness_tp, norm_state) "
                  "VALUES ('movie', 5, '1', '', 'movies/5/theme.mp3', 'shaX', 'shaX', ?, "
                  " 'vid', -5.2, 2.9, NULL)", (NOW,))
        c.commit()
    return s


def _row(db):
    with get_conn(db) as conn:
        return conn.execute(
            "SELECT lf.media_type, lf.tmdb_id, lf.section_id, lf.edition_key, "
            " lf.file_path, lf.file_sha256, lf.loudness_measured_sha256, lf.loudness_i, "
            " lf.loudness_tp, lf.norm_state, t.title, t.year "
            "FROM local_files lf LEFT JOIN themes t "
            " ON t.media_type=lf.media_type AND t.tmdb_id=lf.tmdb_id "
            "WHERE lf.tmdb_id=5").fetchone()


def _fake_normalize(monkeypatch):
    def _fn(path, target, measured_i, true_peak, *, expect_sha=None):
        return {"ok": True, "changed": True, "steps": -9, "applied_db": -13.5,
                "note": "leveled", "old_sha": "shaX", "new_sha": "shaY",
                "old_pcm_sha": "pcmX", "new_i": -18.7, "new_tp": -10.6, "new_lra": 5.0}
    monkeypatch.setattr("app.core.loudness_apply.normalize_file", _fn)


def _fake_push(monkeypatch):
    monkeypatch.setattr("app.web.api._push_theme_to_plex",
                        lambda settings, **kw: {"ok": True, "serving_normalized": True})


def test_chokepoint_normalizes_an_eligible_row(db_and_settings, monkeypatch):
    s = db_and_settings
    _fake_normalize(monkeypatch)
    _fake_push(monkeypatch)
    from app.web.api import _normalize_one_row
    res = _normalize_one_row(s.db_path, s, _row(s.db_path), -18.0)
    assert res["ok"] is True and res["changed"] is True
    assert res["applied_db"] == -13.5
    assert res["plex_is_serving_it"] is True
    with get_conn(s.db_path) as conn:
        got = conn.execute("SELECT norm_state, norm_gain_db, norm_target FROM local_files "
                           "WHERE tmdb_id=5").fetchone()
    assert got["norm_state"] == "normalized"
    assert got["norm_gain_db"] == -13.5 and got["norm_target"] == -18.0


def test_chokepoint_refuses_an_already_normalized_row(db_and_settings, monkeypatch):
    s = db_and_settings
    with sqlite3.connect(s.db_path) as c:
        c.execute("UPDATE local_files SET norm_state='normalized' WHERE tmdb_id=5")
        c.commit()
    from app.web.api import _normalize_one_row
    res = _normalize_one_row(s.db_path, s, _row(s.db_path), -18.0)
    assert res["ok"] is False and "already normalized" in res["error"]


def test_chokepoint_refuses_a_stale_measurement(db_and_settings, monkeypatch):
    s = db_and_settings
    with sqlite3.connect(s.db_path) as c:
        # measurement no longer matches the bytes → the gain would be wrong
        c.execute("UPDATE local_files SET loudness_measured_sha256='different' WHERE tmdb_id=5")
        c.commit()
    from app.web.api import _normalize_one_row
    res = _normalize_one_row(s.db_path, s, _row(s.db_path), -18.0)
    assert res["ok"] is False and "stale" in res["error"]
