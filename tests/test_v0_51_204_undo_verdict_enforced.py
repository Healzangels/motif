"""v0.51.204 (audit H1) — the UNDO path must ENFORCE the audio-restored verdict.

Before this tag, undo_file returned ok=True whenever mp3gain -u merely ran, even when the
decoded PCM did NOT match the pre-normalize hash (audio_restored=False — a deep attenuation
clamped a frame's global_gain and -u over-restored). _undo_one_row gated only on ok, then
flipped the row to raw, WIPED its recovery hashes, and re-pushed the degraded bytes to Plex —
leaving a theme permanently louder than its original, marked "raw", with no recovery reference.

Now: undo_file's ok gates on the verdict; _undo_one_row's degraded branch keeps norm_state +
the recovery refs, re-stamps only the measurement, and does NOT touch Plex.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

import app.core.loudness_apply as la
from app.core.db import init_db, get_conn

NOW = "2026-07-18T00:00:00"

# the SELECT _bulk_normalize_undo_run / the undo-one endpoint feed into _undo_one_row.
_ROW_SQL = (
    "SELECT lf.media_type, lf.tmdb_id, lf.section_id, lf.edition_key, "
    " lf.file_path, lf.norm_state, lf.norm_orig_sha256, "
    " lf.norm_plex_entry_uri, lf.norm_orig_pcm_sha256, t.title "
    "FROM local_files lf "
    "LEFT JOIN themes t ON t.media_type=lf.media_type AND t.tmdb_id=lf.tmdb_id "
    "WHERE lf.norm_state='normalized' LIMIT 1"
)


@pytest.fixture
def bench(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    # ABSOLUTE file_path so _undo_one_row uses it directly (themes_dir isn't configured here).
    theme = tmp_path / "theme.mp3"
    theme.write_bytes(b"degraded-bytes")
    with sqlite3.connect(s.db_path) as c:
        c.execute("PRAGMA foreign_keys=OFF")
        c.execute("INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source, "
                  " last_seen_sync_at, first_seen_sync_at) "
                  "VALUES (1,'movie',1,'M1','imdb',?,?)", (NOW, NOW))
        # a LEVELED row with a real recovery reference (norm_orig_pcm_sha256).
        c.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, edition_key, "
                  " file_path, file_sha256, loudness_i, loudness_tp, loudness_measured_sha256, "
                  " downloaded_at, source_video_id, norm_state, norm_gain_db, norm_target, "
                  " norm_at, norm_orig_sha256, norm_orig_pcm_sha256, norm_plex_entry_uri) "
                  "VALUES ('movie',1,'1','', ?, 'LEVELED_SHA', -18.0, -2.0, 'LEVELED_SHA', "
                  " ?, 'v', 'normalized', -13.5, -18.0, ?, 'ORIG_FILE_SHA', 'ORIG_PCM', "
                  " 'entry://x')",
                  (str(theme), NOW, NOW))
        c.commit()
    return s


def _fetch_row(db):
    with get_conn(db) as c:
        return c.execute(_ROW_SQL).fetchone()


def _norm_row(db):
    with get_conn(db) as c:
        return c.execute("SELECT norm_state, norm_orig_pcm_sha256, norm_orig_sha256, "
                         " norm_plex_entry_uri, loudness_i, file_sha256 "
                         "FROM local_files WHERE tmdb_id=1").fetchone()


def test_over_restore_keeps_the_row_leveled_and_does_not_touch_plex(bench, monkeypatch):
    from app.web import api
    # undo_file: -u ran, but the AUDIO did not come back (deep-attenuation clamp).
    monkeypatch.setattr(la, "undo_file", lambda *a, **k: {
        "ok": False, "audio_restored": False, "file_bit_exact": False,
        "new_sha": "DEGRADED_SHA", "new_i": -4.0, "new_tp": -1.0, "new_lra": 5.0,
        "error": "undo did not restore the original audio",
    })
    # Plex "configured" so the v0.51.205 L1 gate (refuse a pushed row without Plex) doesn't
    # intercept — this scenario is a pushed row being undone WITH Plex available.
    from app.config import Settings
    monkeypatch.setattr(Settings, "plex_url", property(lambda self: "http://x"))
    monkeypatch.setattr(Settings, "plex_token", property(lambda self: "t"))
    # a tripwire: Plex must NOT be touched on the degraded path.
    monkeypatch.setattr(api, "_push_theme_to_plex",
                        lambda *a, **k: pytest.fail("Plex must not be pushed on over-restore"))

    row = _fetch_row(bench.db_path)
    res = api._undo_one_row(bench.db_path, bench, row)

    assert res["ok"] is False
    assert res["audio_restored"] is False
    assert res["error"]
    r = _norm_row(bench.db_path)
    # the row stays LEVELED with its recovery references intact — NOT flipped to raw.
    assert r["norm_state"] == "normalized"
    assert r["norm_orig_pcm_sha256"] == "ORIG_PCM"
    assert r["norm_orig_sha256"] == "ORIG_FILE_SHA"
    assert r["norm_plex_entry_uri"] == "entry://x"
    # but the measurement is re-stamped to the degraded file so the audit isn't stale.
    assert r["loudness_i"] == -4.0
    assert r["file_sha256"] == "DEGRADED_SHA"


def test_bulk_undo_does_not_count_an_over_restore_as_undone(bench, monkeypatch):
    """The bulk runner must not report a degraded over-restore in n_undone."""
    import json
    from app.web import api
    monkeypatch.setattr(la, "undo_file", lambda *a, **k: {
        "ok": False, "audio_restored": False, "file_bit_exact": False,
        "new_sha": "DEGRADED_SHA", "new_i": -4.0, "new_tp": -1.0, "new_lra": 5.0,
        "error": "undo did not restore the original audio",
    })
    from app.config import Settings
    monkeypatch.setattr(Settings, "plex_url", property(lambda self: "http://x"))
    monkeypatch.setattr(Settings, "plex_token", property(lambda self: "t"))
    monkeypatch.setattr(api, "_push_theme_to_plex", lambda *a, **k: {})
    monkeypatch.setattr("app.core.notify.dispatch", lambda *a, **k: None)

    api._bulk_normalize_undo_run(bench.db_path, bench)
    with get_conn(bench.db_path) as c:
        st, detail = c.execute("SELECT status, detail_json FROM op_progress "
                               "WHERE op_id='bulk-normalize-undo'").fetchone()
    assert st == "done"
    ds = {d["l"]: d["v"] for d in json.loads(detail)["done_summary"]}
    assert ds.get("undone", 0) == 0   # the over-restored row is NOT counted undone


def test_undo_file_ok_enforces_the_verdict():
    """Source-contract: undo_file's ok is gated on the audio verdict, not just '-u ran'."""
    src = Path("app/core/loudness_apply.py").read_text()
    assert 'out["ok"] = out["audio_restored"] is not False' in src
