"""v0.51.338 — a levelled theme must not read CHANGED on CANONICAL HEALTH.

changed_canonicals flags a present canonical whose on-disk size differs from
local_files.file_size. motif's own loudness writers rewrite the canonical in
place (mp3gain appends an APEv2 undo tag), and the normalize / undo / over-restore
UPDATEs stamped the new file_sha256 but not the new size, so every levelled theme
became a permanent false CHANGED row.

  (i)  the three writers stamp file_size from a post-write stat;
  (ii) verify_canonical_health heals rows already stamped stale — only when the
       bytes still hash to file_sha256; a genuine change stays listed.

mp3gain / ffmpeg are never run: normalize_file / undo_file are replaced by fakes
that append bytes to the file and return the shapes the real ones return.
"""
from __future__ import annotations

import hashlib
import logging
import sqlite3

import pytest

import app.core.adopt as adopt
import app.core.loudness_apply as la
from app.core import plex_enum
from app.core.canonical_health import changed_canonicals
from app.core.db import get_conn, init_db

NOW = "2026-07-17T00:00:00"
ORIGINAL = b"ID3\x03" + b"\xff\xfb" * 600
APE_TAG = b"APETAGEX" + b"\x00" * 120


def _sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


@pytest.fixture
def bench(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    themes = tmp_path / "themes"
    themes.mkdir()
    monkeypatch.setattr(Settings, "themes_dir", property(lambda self: themes))
    # Plex off: no entry_before snapshot, no put-back; the push is faked below.
    monkeypatch.setattr(Settings, "plex_url", property(lambda self: None))
    monkeypatch.setattr(Settings, "plex_token", property(lambda self: None))
    from app.web import api
    monkeypatch.setattr(api, "_push_theme_to_plex",
                        lambda settings, **kw: {"ok": True, "serving_normalized": True})
    monkeypatch.setattr(api, "log_event", lambda *a, **k: None)
    init_db(s.db_path)
    return s, themes


def _seed(db, themes, *, tmdb, disk: bytes, sha: str, size, leveled=False):
    rel = f"movies/{tmdb}/theme.mp3"
    (themes / rel).parent.mkdir(parents=True, exist_ok=True)
    (themes / rel).write_bytes(disk)
    with sqlite3.connect(db) as c:
        c.execute("PRAGMA foreign_keys = OFF")
        c.execute("INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source, "
                  " last_seen_sync_at, first_seen_sync_at) "
                  "VALUES (?, 'movie', ?, 'Loud', 'imdb', ?, ?)", (tmdb, tmdb, NOW, NOW))
        c.execute("INSERT INTO plex_items (rating_key, media_type, section_id, guid_tmdb, "
                  " edition_key, title, has_theme, first_seen_at, last_seen_at) "
                  "VALUES (?, 'movie', '1', ?, '', 'Loud', 1, ?, ?)",
                  (str(9000 + tmdb), tmdb, NOW, NOW))
        c.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, edition_key, "
                  " file_path, file_sha256, file_size, loudness_measured_sha256, "
                  " downloaded_at, source_video_id, source_kind, loudness_i, loudness_tp, "
                  " canonical_present, norm_state, norm_gain_db, norm_target, norm_at, "
                  " norm_orig_sha256, norm_orig_pcm_sha256) "
                  "VALUES ('movie', ?, '1', '', ?, ?, ?, ?, ?, 'vid', 'themerrdb', "
                  " -5.2, 2.9, 1, ?, ?, ?, ?, ?, ?)",
                  (tmdb, rel, sha, size, sha, NOW,
                   "normalized" if leveled else None,
                   -13.5 if leveled else None, -18.0 if leveled else None,
                   NOW if leveled else None,
                   "orig-sha" if leveled else None, "orig-pcm" if leveled else None))
        c.commit()
    return themes / rel


def _row(db, tmdb):
    with get_conn(db) as conn:
        return conn.execute(
            "SELECT lf.*, t.title, t.year FROM local_files lf "
            "LEFT JOIN themes t ON t.media_type=lf.media_type AND t.tmdb_id=lf.tmdb_id "
            "WHERE lf.tmdb_id=?", (tmdb,)).fetchone()


def _changed(db, themes):
    with get_conn(db) as conn:
        return changed_canonicals(conn, themes)


def _append_and_report(path, tail: bytes) -> str:
    with open(path, "ab") as f:
        f.write(tail)
    return _sha(path.read_bytes())


# ── (i) the writers stamp the rewritten size ─────────────────

def test_normalize_one_stamps_the_rewritten_size(bench, monkeypatch):
    s, themes = bench
    theme = _seed(s.db_path, themes, tmdb=5, disk=ORIGINAL, sha=_sha(ORIGINAL),
                  size=len(ORIGINAL))
    assert _changed(s.db_path, themes) == []

    def fake_normalize(path, target, measured_i, true_peak, *, expect_sha=None):
        new_sha = _append_and_report(path, APE_TAG)   # mp3gain appends its undo tag
        return {"ok": True, "changed": True, "steps": -9, "applied_db": -13.5,
                "note": None, "error": None, "old_sha": expect_sha, "new_sha": new_sha,
                "old_pcm_sha": "orig-pcm", "new_i": -18.7, "new_tp": -10.6, "new_lra": 5.0}
    monkeypatch.setattr(la, "normalize_file", fake_normalize)

    from app.web.api import _normalize_one_row
    res = _normalize_one_row(s.db_path, s, _row(s.db_path, 5), -18.0)
    assert res["ok"] is True and res["changed"] is True

    got = _row(s.db_path, 5)
    assert got["norm_state"] == "normalized"
    assert got["file_sha256"] == _sha(theme.read_bytes())
    assert got["file_size"] == theme.stat().st_size
    assert _changed(s.db_path, themes) == [], "a levelled theme is not CHANGED"


def test_normalize_one_keeps_the_prior_size_when_the_file_cannot_be_stated(bench, monkeypatch):
    s, themes = bench
    _seed(s.db_path, themes, tmdb=6, disk=ORIGINAL, sha=_sha(ORIGINAL), size=len(ORIGINAL))

    def fake_normalize(path, target, measured_i, true_peak, *, expect_sha=None):
        new_sha = _append_and_report(path, APE_TAG)
        path.unlink()   # gone before the stamp: the size is unknowable, not zero
        return {"ok": True, "changed": True, "steps": -9, "applied_db": -13.5,
                "note": None, "error": None, "old_sha": expect_sha, "new_sha": new_sha,
                "old_pcm_sha": "orig-pcm", "new_i": -18.7, "new_tp": -10.6, "new_lra": 5.0}
    monkeypatch.setattr(la, "normalize_file", fake_normalize)

    from app.web.api import _normalize_one_row
    _normalize_one_row(s.db_path, s, _row(s.db_path, 6), -18.0)
    assert _row(s.db_path, 6)["file_size"] == len(ORIGINAL)


def test_undo_one_stamps_the_restored_size(bench, monkeypatch):
    s, themes = bench
    leveled = ORIGINAL + APE_TAG
    theme = _seed(s.db_path, themes, tmdb=7, disk=leveled, sha=_sha(leveled),
                  size=len(leveled), leveled=True)
    assert _changed(s.db_path, themes) == []

    def fake_undo(path, expect_sha=None, expect_pcm_sha=None):
        new_sha = _append_and_report(path, b"MP3GAIN_UNDO-cleared")
        return {"ok": True, "audio_restored": True, "file_bit_exact": False,
                "new_sha": new_sha, "new_i": -5.2, "new_tp": 2.9, "new_lra": 5.0,
                "error": None}
    monkeypatch.setattr(la, "undo_file", fake_undo)

    from app.web.api import _undo_one_row
    res = _undo_one_row(s.db_path, s, _row(s.db_path, 7))
    assert res["ok"] is True

    got = _row(s.db_path, 7)
    assert got["norm_state"] is None
    assert got["file_sha256"] == _sha(theme.read_bytes())
    assert got["file_size"] == theme.stat().st_size
    assert _changed(s.db_path, themes) == [], "an undone theme is not CHANGED"


def test_over_restored_undo_stamps_the_degraded_size(bench, monkeypatch):
    s, themes = bench
    leveled = ORIGINAL + APE_TAG
    theme = _seed(s.db_path, themes, tmdb=8, disk=leveled, sha=_sha(leveled),
                  size=len(leveled), leveled=True)

    def fake_undo(path, expect_sha=None, expect_pcm_sha=None):
        new_sha = _append_and_report(path, b"clamped-frames")
        return {"ok": False, "audio_restored": False, "file_bit_exact": False,
                "new_sha": new_sha, "new_i": -4.0, "new_tp": -1.0, "new_lra": 5.0,
                "error": "undo did not restore the original audio"}
    monkeypatch.setattr(la, "undo_file", fake_undo)

    from app.web.api import _undo_one_row
    res = _undo_one_row(s.db_path, s, _row(s.db_path, 8))
    assert res["ok"] is False and res["audio_restored"] is False

    got = _row(s.db_path, 8)
    assert got["norm_state"] == "normalized"   # still flagged for inspection
    assert got["file_sha256"] == _sha(theme.read_bytes())
    assert got["file_size"] == theme.stat().st_size
    assert _changed(s.db_path, themes) == []


# ── (ii) verify_canonical_health heals stale stamps, not real changes ──

def test_verify_heals_a_size_stale_row_and_keeps_a_real_change(bench, monkeypatch, caplog):
    s, themes = bench
    db = s.db_path
    leveled = ORIGINAL + APE_TAG
    # a pre-.338 level: the recorded sha IS these bytes, only the size is the old one.
    stale = _seed(db, themes, tmdb=11, disk=leveled, sha=_sha(leveled), size=len(ORIGINAL))
    # a genuine out-of-band edit: neither size nor sha describe the bytes on disk.
    swapped = b"ID3\x04" + b"\x00" * 900
    _seed(db, themes, tmdb=12, disk=swapped, sha=_sha(ORIGINAL), size=len(ORIGINAL))
    # a healthy row: nothing to hash.
    _seed(db, themes, tmdb=13, disk=ORIGINAL, sha=_sha(ORIGINAL), size=len(ORIGINAL))
    before = {r["tmdb_id"] for r in _changed(db, themes)}
    assert before == {11, 12}

    hashed = []
    real_hash = adopt._hash_file

    def counting_hash(path):
        hashed.append(path)
        return real_hash(path)
    monkeypatch.setattr(adopt, "_hash_file", counting_hash)

    with caplog.at_level(logging.INFO, logger=plex_enum.log.name):
        res = plex_enum.verify_canonical_health(db, themes)
    assert res["checked"] == 3 and res["missing"] == 0 and res["skipped"] == 0

    assert _row(db, 11)["file_size"] == stale.stat().st_size
    assert _row(db, 12)["file_size"] == len(ORIGINAL)      # left for CHANGED to report
    after = _changed(db, themes)
    assert [r["tmdb_id"] for r in after] == [12]
    assert after[0]["on_disk"] == len(swapped)
    # only the size-mismatched rows cost a hash; a healthy row never does.
    assert len(hashed) == 2
    assert any(rec.levelno == logging.INFO and "healed" in rec.getMessage()
               for rec in caplog.records)

    # a second pass has nothing left to heal and hashes only the genuine change.
    hashed.clear()
    plex_enum.verify_canonical_health(db, themes)
    assert len(hashed) == 1

def test_verify_heal_loses_to_a_writer_that_restamped_the_row_meanwhile(bench, monkeypatch):
    s, themes = bench
    db = s.db_path
    leveled = ORIGINAL + APE_TAG
    _seed(db, themes, tmdb=21, disk=leveled, sha=_sha(leveled), size=len(ORIGINAL))
    real_hash = adopt._hash_file

    def hash_while_a_writer_lands(path):
        # a re-download or restore re-stamps the row between verify's read and its heal.
        with sqlite3.connect(db) as c:
            c.execute("UPDATE local_files SET file_size = 777, file_sha256 = ? WHERE tmdb_id = 21",
                      ("f" * 64,))
            c.commit()
        return real_hash(path)
    monkeypatch.setattr(adopt, "_hash_file", hash_while_a_writer_lands)
    plex_enum.verify_canonical_health(db, themes)
    row = _row(db, 21)
    assert (row["file_size"], row["file_sha256"]) == (777, "f" * 64), (
        "the heal must be compare-and-set: it may only correct the size it read, never "
        "overwrite a stamp another writer made after the read")
