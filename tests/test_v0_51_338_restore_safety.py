"""v0.51.338: restore safety for // RESTORE FROM PLEX (the v0.51.337 themes check).

  1a. A present canonical is never overwritten from Plex's store: the store path
      refuses before any Plex call, a sidecar "already present" is terminal for the
      row, and the download worker stamps canonical_present=1 so a fresh REPAIR ALL
      download is not still flagged broken until the next daily verify.
  1b. The sidecar restore links the way placement does: any OSError from os.link
      (EPERM/ENOTSUP on SMB/CIFS/FUSE, not only EXDEV) falls back to a copy, staged
      through .motif-tmp so a copy that dies mid-way leaves no partial canonical.
  1c. Restored bytes with a new sha clear the loudness/norm anchors (the
      revisions.py rule), so // UNDO cannot run mp3gain -u on never-gained bytes.
"""
from __future__ import annotations

import errno
import hashlib
import logging
import shutil
import sqlite3
import threading
from pathlib import Path

from app.core import canonical_health as ch
from app.core.db import get_conn, init_db

NOW = "2026-09-12T00:00:00"

_NORM_COLS = ("loudness_i", "loudness_tp", "loudness_lra", "loudness_measured_at",
              "loudness_measured_sha256", "norm_state", "norm_gain_db", "norm_target",
              "norm_at", "norm_orig_sha256", "norm_orig_pcm_sha256", "norm_plex_entry_uri")


# ── seed helpers ──────────────────────────────────────────────────────

def _section(conn, section_id="1"):
    conn.execute(
        "INSERT INTO plex_sections (section_id, title, type, is_anime, is_4k,"
        " themes_subdir, included, discovered_at, last_seen_at)"
        " VALUES (?, 'M', 'movie', 0, 0, 'movies', 1, ?, ?)"
        " ON CONFLICT(section_id) DO NOTHING", (section_id, NOW, NOW))


def _theme(conn, *, tmdb):
    conn.execute(
        "INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source,"
        " last_seen_sync_at, first_seen_sync_at, youtube_url)"
        " VALUES (?, 'movie', ?, ?, 'plex_orphan', ?, ?, NULL)",
        (tmdb, tmdb, f"T{tmdb}", NOW, NOW))


def _lf(conn, *, tmdb, canonical_present=0, file_size=None, file_sha256=None, extra=None):
    cols = {"media_type": "movie", "tmdb_id": tmdb, "section_id": "1", "theme_id": tmdb,
            "file_path": f"movies/{tmdb}/theme.mp3", "file_size": file_size,
            "file_sha256": file_sha256, "downloaded_at": NOW, "source_video_id": "",
            "provenance": "manual", "source_kind": "upload",
            "canonical_present": canonical_present, "edition_key": ""}
    cols.update(extra or {})
    conn.execute(f"INSERT INTO local_files ({','.join(cols)}) VALUES ({','.join('?' * len(cols))})",
                 tuple(cols.values()))


def _placement(conn, *, tmdb, media_folder, kind="hardlink", rk=None):
    conn.execute(
        "INSERT INTO placements (media_type, tmdb_id, section_id, media_folder, placement_kind,"
        " provenance, placed_at, plex_rating_key, theme_present, edition_key)"
        " VALUES ('movie', ?, '1', ?, ?, 'manual', ?, ?, 1, '')",
        (tmdb, media_folder, kind, NOW, rk))


def _dirs(tmp_path: Path):
    db = tmp_path / "m.db"
    init_db(db)
    return db, tmp_path / "themes", tmp_path / "plex"


def _row(db, tmdb):
    with get_conn(db) as conn:
        return next(x for x in ch._broken_rows_with_placement(conn) if x["tmdb_id"] == tmdb)


def _lf_cols(db, tmdb, cols):
    with sqlite3.connect(db) as conn:
        return conn.execute(f"SELECT {', '.join(cols)} FROM local_files WHERE tmdb_id = ?",
                            (tmdb,)).fetchone()


class FakePlex:
    """The two calls the store path makes; records every one."""
    def __init__(self, body=b"plex-stored-20-bytes"):
        self.body = body
        self.calls: list[tuple] = []

    def get_themes(self, *, rating_key):
        self.calls.append(("themes", rating_key))
        return {"ok": True, "http_status": 200, "error": None,
                "body": {"MediaContainer": {"Metadata": [{"ratingKey": "upload://themes/abc",
                                                          "selected": True}]}}}

    def fetch_theme_bytes(self, *, item_rating_key, entry_uri):
        self.calls.append(("fetch", item_rating_key, entry_uri))
        return {"ok": True, "http_status": 200, "bytes": self.body}


def _seed_fresh_store_row(tmp_path, *, tmdb=201, media_folder="", sidecar=False):
    """A plex_upload row whose canonical was just re-downloaded (non-empty on disk)
    but still carries canonical_present=0 from before the download."""
    db, themes, plexdir = _dirs(tmp_path)
    fresh = b"fresh-levelled-canonical-bytes-36by"
    with sqlite3.connect(db) as conn:
        _section(conn)
        _theme(conn, tmdb=tmdb)
        _lf(conn, tmdb=tmdb, canonical_present=0, file_size=len(fresh),
            file_sha256=hashlib.sha256(fresh).hexdigest())
        folder = media_folder
        if sidecar:
            (plexdir / str(tmdb)).mkdir(parents=True)
            (plexdir / str(tmdb) / "theme.mp3").write_bytes(b"sidecar-bytes")
            folder = str(plexdir / str(tmdb))
        _placement(conn, tmdb=tmdb, media_folder=folder, kind="plex_upload", rk=f"9{tmdb}")
        conn.commit()
    canonical = themes / "movies" / str(tmdb) / "theme.mp3"
    canonical.parent.mkdir(parents=True)
    canonical.write_bytes(fresh)
    return db, themes, canonical, fresh


# ── 1a: a present canonical is never overwritten from Plex ───────────

def test_store_refetch_refuses_a_present_canonical_before_asking_plex(tmp_path):
    db, themes, canonical, fresh = _seed_fresh_store_row(tmp_path)
    before = _lf_cols(db, 201, ("file_size", "file_sha256"))
    plex = FakePlex()
    res = ch.refetch_from_plex_store(db, themes, plex, _row(db, 201))
    assert res == {"ok": False, "reason": "canonical_already_present"}
    assert canonical.read_bytes() == fresh, "the fresh canonical must survive"
    assert plex.calls == [], "Plex must not even be asked for the bytes"
    assert _lf_cols(db, 201, ("file_size", "file_sha256")) == before, "no re-stamp"


def test_bulk_leaves_a_freshly_downloaded_canonical_alone(tmp_path):
    db, themes, canonical, fresh = _seed_fresh_store_row(tmp_path)
    plex = FakePlex()
    res = ch.restore_from_plex(db, themes, plex)
    assert res["restored"] == 0 and res["restored_store"] == 0
    assert [(s["tmdb_id"], s["reason"]) for s in res["skipped"]] == [(201, "canonical_already_present")]
    assert canonical.read_bytes() == fresh
    assert plex.calls == []


def test_sidecar_already_present_is_terminal_for_the_row(tmp_path, monkeypatch):
    """A plex_upload row that ALSO has a folder: the sidecar path's refusal must end
    the row — falling through hands a present canonical to the store overwrite."""
    db, themes, canonical, fresh = _seed_fresh_store_row(tmp_path, tmdb=202, sidecar=True)
    store_calls = []
    monkeypatch.setattr(ch, "refetch_from_plex_store",
                        lambda *a, **k: store_calls.append(a) or {"ok": True, "bytes": 0, "entry_uri": "x"})
    res = ch.restore_from_plex(db, themes, FakePlex())
    assert store_calls == [], "canonical_already_present must not fall through to the store"
    assert res["restored"] == 0
    assert [(s["tmdb_id"], s["reason"]) for s in res["skipped"]] == [(202, "canonical_already_present")]
    assert canonical.read_bytes() == fresh


def _settings(tmp_path):
    from app.config import Settings
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    return s


def test_worker_record_stamps_canonical_present_on_insert_and_on_conflict(tmp_path, monkeypatch):
    from app.core import events, worker as worker_mod
    monkeypatch.setattr(events, "log_event", lambda *a, **k: None)
    monkeypatch.setattr(worker_mod, "log_event", lambda *a, **k: None, raising=False)
    s = _settings(tmp_path)
    with sqlite3.connect(s.db_path) as conn:
        _section(conn)
        _theme(conn, tmdb=300)
        _theme(conn, tmdb=301)
        # a row verify stamped broken, before REPAIR ALL re-downloads it
        _lf(conn, tmdb=301, canonical_present=0, file_size=5, file_sha256="0" * 64)
        conn.commit()
    w = worker_mod.Worker(settings=s, stop_event=threading.Event(),
                          bucket=worker_mod.TokenBucket(60, 60))
    for tmdb in (300, 301):
        w._record_local_file(
            media_type="movie", tmdb_id=tmdb, section_id="1",
            rel_path=f"movies/{tmdb}/theme.mp3", sha256="a" * 64, size=123,
            video_id="vid", provenance="auto", source_kind="themerrdb", job_payload="{}")
    assert _lf_cols(s.db_path, 300, ("canonical_present",)) == (1,), "insert"
    assert _lf_cols(s.db_path, 301, ("canonical_present", "file_sha256")) == (1, "a" * 64), \
        "conflict-update over a row stamped 0"


# ── 1b: the sidecar link falls back like placement, atomically ───────

def _seed_sidecar_row(tmp_path, *, tmdb=101, sidecar=b"sidecar-bytes-101", extra=None,
                      file_sha256=None, kind="hardlink"):
    db, themes, plexdir = _dirs(tmp_path)
    with sqlite3.connect(db) as conn:
        _section(conn)
        _theme(conn, tmdb=tmdb)
        _lf(conn, tmdb=tmdb, canonical_present=0, file_size=5, file_sha256=file_sha256, extra=extra)
        (plexdir / str(tmdb)).mkdir(parents=True)
        (plexdir / str(tmdb) / "theme.mp3").write_bytes(sidecar)
        _placement(conn, tmdb=tmdb, media_folder=str(plexdir / str(tmdb)), kind=kind)
        conn.commit()
    return db, themes, themes / "movies" / str(tmdb) / "theme.mp3"


def test_link_refused_on_a_network_share_still_restores_by_copy(tmp_path, monkeypatch):
    db, themes, canonical = _seed_sidecar_row(tmp_path)
    r = _row(db, 101)

    def eperm(src, dst, *a, **k):
        raise OSError(errno.EPERM, "Operation not permitted")
    monkeypatch.setattr("os.link", eperm)
    res = ch.restore_from_placement(db, themes, r)
    assert res == {"ok": True, "kind": "copy"}, "EPERM is not a reason to refuse the row"
    assert canonical.read_bytes() == b"sidecar-bytes-101"
    assert not list(themes.rglob("*.motif-tmp"))
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT placement_kind FROM placements WHERE tmdb_id = 101").fetchone()[0] == "copy"
    assert _lf_cols(db, 101, ("canonical_present",)) == (1,)


def test_a_copy_dying_midway_leaves_no_partial_and_the_retry_restores(tmp_path, monkeypatch):
    db, themes, canonical = _seed_sidecar_row(tmp_path)
    r = _row(db, 101)
    real_copy2 = shutil.copy2

    def exdev(src, dst, *a, **k):
        raise OSError(errno.EXDEV, "Invalid cross-device link")

    def enospc(src, dst, *a, **k):
        Path(dst).write_bytes(Path(src).read_bytes()[:4])
        raise OSError(errno.ENOSPC, "No space left on device")
    monkeypatch.setattr("os.link", exdev)
    monkeypatch.setattr(shutil, "copy2", enospc)
    res = ch.restore_from_placement(db, themes, r)
    assert not res["ok"] and res["reason"].startswith("link_failed:")
    assert not canonical.exists(), "a partial canonical would read as present forever"
    assert not list(themes.rglob("*.motif-tmp")), "the staging file must be cleaned up"
    assert _lf_cols(db, 101, ("canonical_present",)) == (0,)
    monkeypatch.setattr(shutil, "copy2", real_copy2)
    res2 = ch.restore_from_placement(db, themes, r)
    assert res2 == {"ok": True, "kind": "copy"}
    assert canonical.read_bytes() == b"sidecar-bytes-101"


def test_a_guard_stat_error_is_logged_not_swallowed(tmp_path, monkeypatch, caplog):
    db, themes, canonical = _seed_sidecar_row(tmp_path)
    r = _row(db, 101)
    real_stat = Path.stat
    fired = []

    def stat(self, *, follow_symlinks=True):
        if self == canonical and not fired:
            fired.append(1)
            raise PermissionError(errno.EACCES, "Permission denied", str(self))
        return real_stat(self, follow_symlinks=follow_symlinks)
    monkeypatch.setattr(Path, "stat", stat)
    with caplog.at_level(logging.WARNING, logger="motif.canonical_health"):
        res = ch.restore_from_placement(db, themes, r)
    assert fired, "premise: the guard stat raised"
    assert res["ok"], "an unreadable guard stat still attempts the restore"
    assert any(str(canonical) in rec.getMessage() and "Permission denied" in rec.getMessage()
               for rec in caplog.records), "the swallowed stat error needs a breadcrumb"


# ── 1c: new bytes void the loudness/norm anchors; identical bytes keep them ──

_LEVELLED = {"loudness_i": -18.1, "loudness_tp": -1.2, "loudness_lra": 6.0,
             "loudness_measured_at": NOW, "loudness_measured_sha256": "0" * 64,
             "norm_state": "normalized", "norm_gain_db": -3.0, "norm_target": -18.0,
             "norm_at": NOW, "norm_orig_sha256": "a" * 64, "norm_orig_pcm_sha256": "b" * 64,
             "norm_plex_entry_uri": "upload://themes/old"}


def test_a_sha_changing_restore_clears_every_norm_anchor(tmp_path):
    db, themes, canonical = _seed_sidecar_row(tmp_path, file_sha256="0" * 64, extra=_LEVELLED)
    assert ch.restore_from_placement(db, themes, _row(db, 101))["ok"]
    new_sha = hashlib.sha256(b"sidecar-bytes-101").hexdigest()
    assert _lf_cols(db, 101, ("file_sha256",)) == (new_sha,)
    got = dict(zip(_NORM_COLS, _lf_cols(db, 101, _NORM_COLS)))
    assert got == dict.fromkeys(_NORM_COLS), (
        "restored bytes are not the levelled bytes — a stale norm_state makes // UNDO "
        "run mp3gain -u on audio that was never gained")


def test_a_same_sha_restore_keeps_the_norm_anchors(tmp_path):
    same = hashlib.sha256(b"sidecar-bytes-101").hexdigest()
    db, themes, canonical = _seed_sidecar_row(tmp_path, file_sha256=same, extra=_LEVELLED)
    assert ch.restore_from_placement(db, themes, _row(db, 101))["ok"]
    got = dict(zip(_NORM_COLS, _lf_cols(db, 101, _NORM_COLS)))
    assert got == _LEVELLED, "identical bytes are still exactly the file those anchors describe"


def test_an_unreadable_rehash_after_a_restore_still_clears_the_norm_anchors(tmp_path, monkeypatch, caplog):
    same = hashlib.sha256(b"sidecar-bytes-101").hexdigest()
    db, themes, canonical = _seed_sidecar_row(tmp_path, file_sha256=same, extra=_LEVELLED)
    real_open = Path.open
    fired = []

    def open_(self, mode="r", *a, **k):
        if self == canonical and "rb" in mode:
            fired.append(1)
            raise PermissionError(errno.EACCES, "Permission denied", str(self))
        return real_open(self, mode, *a, **k)
    monkeypatch.setattr(Path, "open", open_)
    with caplog.at_level(logging.WARNING, logger="motif.canonical_health"):
        assert ch.restore_from_placement(db, themes, _row(db, 101))["ok"]
    assert fired, "premise: the post-restore re-hash could not read the canonical"
    got = dict(zip(_NORM_COLS, _lf_cols(db, 101, _NORM_COLS)))
    assert got == dict.fromkeys(_NORM_COLS), (
        "bytes were written but could not be hashed — the anchors describe a file we "
        "can no longer vouch for, so they must clear, not survive on the prior sha")


# ── a 0-byte stub is broken, not present: both guards still restore it ──

def test_a_zero_byte_stub_is_restored_from_plex_store(tmp_path):
    """A failed download leaves a 0-byte stub that verify stamps canonical_present=0 —
    exactly what RESTORE FROM PLEX targets, so the present-file guard must not refuse it."""
    plex_body = b"plex-stored-20-bytes"
    db, themes, canonical, _fresh = _seed_fresh_store_row(tmp_path / "one")
    canonical.write_bytes(b"")
    plex = FakePlex(plex_body)
    res = ch.refetch_from_plex_store(db, themes, plex, _row(db, 201))
    assert res["ok"], res
    assert canonical.read_bytes() == plex_body, "the stub must be replaced by Plex's bytes"
    assert plex.calls, "a stub is not a present canonical — Plex must be asked"
    assert _lf_cols(db, 201, ("canonical_present", "file_size")) == (1, len(plex_body))

    db2, themes2, canonical2, _ = _seed_fresh_store_row(tmp_path / "bulk")
    canonical2.write_bytes(b"")
    bulk = ch.restore_from_plex(db2, themes2, FakePlex(plex_body))
    assert bulk["restored_store"] == 1 and bulk["skipped"] == [], bulk
    assert canonical2.read_bytes() == plex_body


def test_a_zero_byte_stub_is_restored_from_the_sidecar(tmp_path):
    db, themes, canonical = _seed_sidecar_row(tmp_path)
    canonical.parent.mkdir(parents=True)
    canonical.write_bytes(b"")
    res = ch.restore_from_placement(db, themes, _row(db, 101))
    assert res["ok"], res
    assert canonical.read_bytes() == b"sidecar-bytes-101", "the stub must be replaced, not kept"
    assert _lf_cols(db, 101, ("canonical_present",)) == (1,)
    assert not list(themes.rglob("*.motif-tmp"))
