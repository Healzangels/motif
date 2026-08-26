"""v0.51.292 — holistic review: revision restore/rotation data-loss fixes.

Three confirmed findings in app/core/revisions.py:
  1. restore_revision captured the outgoing canonical BEFORE copying the
     restore-target — the capture's keep-last-2 rotation made the older of
     two retained revisions 3rd-newest, NULLed it, and unlinked its binary,
     so restoring it always crashed (FileNotFoundError) AND destroyed the
     bytes it was asked to restore. The copy now happens first.
  2. The sha-keyed dedupe lets several rows share one retained file
     (content recurrence A→B→A′); rotation unlinked by the rotated row's
     path, destroying a NEWER revision's binary while it still reported
     restorable=1. Rotation now skips paths any row still references.
  3. restore_revision is a byte-replacement writer but never cleared the 11
     loudness/norm columns (or norm_plex_entry_uri) — stale anchors let
     // UNDO LEVELING run mp3gain -u against the wrong bytes. It now
     mirrors save_edit's _cond_columns(None, sha) clear, and save_edit
     gained the 12th (norm_plex_entry_uri) clear too.
"""
from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
NOW = "2026-08-26T00:00:00+00:00"
MT, TID, SEC = "movie", 292001, "1"
REL = "movies/Twilight (2008)/theme.mp3"


@pytest.fixture
def env(tmp_path):
    from app.core.db import get_conn, init_db, transaction
    db = tmp_path / "t.db"
    themes = tmp_path / "themes"
    (themes / "movies" / "Twilight (2008)").mkdir(parents=True)
    init_db(db)
    with get_conn(db) as conn, transaction(conn):
        conn.execute(
            """INSERT INTO plex_sections (section_id, title, type, is_anime,
                 is_4k, themes_subdir, included, discovered_at, last_seen_at)
               VALUES (?, 'Movies', 'movie', 0, 0, 'movies', 1, ?, ?)""",
            (SEC, NOW, NOW))
        conn.execute(
            """INSERT INTO themes (media_type, tmdb_id, title, year,
                 upstream_source, last_seen_sync_at, first_seen_sync_at)
               VALUES (?, ?, 'Twilight', '2008', 'themoviedb', ?, ?)""",
            (MT, TID, NOW, NOW))
    return db, themes


def _seed_lf(db, themes, content, vid):
    from app.core.db import get_conn, transaction
    (themes / REL).write_bytes(content)
    sha = hashlib.sha256(content).hexdigest()
    with get_conn(db) as conn, transaction(conn):
        conn.execute(
            """INSERT INTO local_files (media_type, tmdb_id, section_id,
                 edition_key, file_path, file_sha256, file_size, downloaded_at,
                 source_video_id, provenance, source_kind)
               VALUES (?, ?, ?, '', ?, ?, ?, ?, ?, 'auto', 'themerrdb')
               ON CONFLICT(media_type, tmdb_id, section_id, edition_key)
               DO UPDATE SET file_sha256=excluded.file_sha256,
                             file_size=excluded.file_size,
                             source_video_id=excluded.source_video_id""",
            (MT, TID, SEC, REL, sha, len(content), NOW, vid))
    return sha


def _capture_one(db, themes, content, vid):
    from app.core.revisions import capture_revision
    _seed_lf(db, themes, content, vid)
    stash = themes / f"{vid}.stale"
    (themes / REL).replace(stash)
    return capture_revision(db, themes, media_type=MT, tmdb_id=TID,
                            section_id=SEC, edition_key="",
                            reason="replaced_by_download", stashed_file=stash)


def _revs(db):
    from app.core.db import get_conn
    with get_conn(db) as conn:
        return [dict(r) for r in conn.execute(
            "SELECT id, reason, content_sha256, retained_path "
            "  FROM theme_revisions ORDER BY id")]


# ── 1. restoring the OLDER retained revision must not destroy it ──


def test_restore_of_the_older_retained_revision_survives_rotation(env):
    from app.core.revisions import restore_revision
    db, themes = env
    rev_a = _capture_one(db, themes, b"AAA-oldest", "vidA0000001")
    _capture_one(db, themes, b"BBB-middle", "vidB0000001")
    _seed_lf(db, themes, b"CCC-current", "vidC0000001")
    # steady state: rev_a (older, retained) + rev_b (newer, retained) + CCC live.
    out = restore_revision(db, themes, revision_id=rev_a)
    # pre-fix this raised FileNotFoundError (the internal capture rotated
    # rev_a out and unlinked its binary before the copy) — and the bytes died.
    assert (themes / REL).read_bytes() == b"AAA-oldest"
    assert out["restored_sha256"] == hashlib.sha256(b"AAA-oldest").hexdigest()
    reasons = [r["reason"] for r in _revs(db)]
    assert "replaced_by_restore" in reasons, "the outgoing CCC was captured"
    from app.core.db import get_conn
    with get_conn(db) as conn:
        jobs = conn.execute("SELECT job_type, status FROM jobs").fetchall()
    assert [(j["job_type"], j["status"]) for j in jobs] == [("place", "pending")]


def test_failed_capture_leaves_no_tmp_and_reraises(env):
    from app.core import revisions
    db, themes = env
    rev_a = _capture_one(db, themes, b"AAA-oldest", "vidA0000001")
    _seed_lf(db, themes, b"CCC-current", "vidC0000001")
    orig = revisions.capture_revision
    def _boom(*a, **kw):
        raise RuntimeError("disk full")
    revisions.capture_revision = _boom
    try:
        with pytest.raises(RuntimeError):
            revisions.restore_revision(db, themes, revision_id=rev_a)
    finally:
        revisions.capture_revision = orig
    assert not (themes / REL).with_suffix(".rev-restore-tmp").exists()
    assert (themes / REL).read_bytes() == b"CCC-current", "canonical untouched"


# ── 2. shared retained files survive rotation ────────────────


def test_recurrent_content_shares_a_file_that_rotation_must_not_unlink(env):
    db, themes = env
    _capture_one(db, themes, b"XXX-recurs", "vidX0000001")
    _capture_one(db, themes, b"YYY-between", "vidY0000001")
    rev3 = _capture_one(db, themes, b"XXX-recurs", "vidX0000002")
    revs = _revs(db)
    assert len(revs) == 3
    r1, r2, r3 = revs
    assert r1["retained_path"] is None, "oldest rotated to metadata-only"
    assert r3["retained_path"] is not None and rev3 == r3["id"]
    # the sha-keyed dedupe gave rev1 and rev3 the SAME path — rotation NULLed
    # rev1 but pre-fix also unlinked the file rev3 had just been recorded
    # against, while rev3 still reported restorable=1.
    assert (themes / r3["retained_path"]).read_bytes() == b"XXX-recurs"


# ── 3. restore clears the loudness/norm anchors ──────────────


def test_restore_clears_all_norm_anchors(env):
    from app.core.db import get_conn, transaction
    from app.core.revisions import restore_revision
    db, themes = env
    rev_a = _capture_one(db, themes, b"AAA-oldest", "vidA0000001")
    _seed_lf(db, themes, b"CCC-current", "vidC0000001")
    with get_conn(db) as conn, transaction(conn):
        conn.execute(
            "UPDATE local_files SET loudness_i=-10.5, loudness_tp=1.2, "
            "  loudness_lra=6.0, loudness_measured_at=?, "
            "  loudness_measured_sha256='deadbeef', norm_state='normalized', "
            "  norm_gain_db=-4.5, norm_target=-18.0, norm_at=?, "
            "  norm_orig_sha256='cafe', norm_orig_pcm_sha256='f00d', "
            "  norm_plex_entry_uri='upload://themes/abc' "
            " WHERE media_type=? AND tmdb_id=?", (NOW, NOW, MT, TID))
    restore_revision(db, themes, revision_id=rev_a)
    with get_conn(db) as conn:
        row = dict(conn.execute(
            "SELECT loudness_i, loudness_tp, loudness_lra, norm_state, "
            "       norm_gain_db, norm_orig_sha256, norm_orig_pcm_sha256, "
            "       norm_plex_entry_uri, loudness_measured_sha256 "
            "  FROM local_files WHERE media_type=? AND tmdb_id=?",
            (MT, TID)).fetchone())
    assert all(v is None for v in row.values()), (
        f"stale anchors survive the byte replacement: {row} — // UNDO "
        f"LEVELING would run mp3gain -u against the restored bytes")


def test_save_edit_clears_the_plex_entry_uri_too():
    src = (REPO / "app" / "core" / "audio_edit.py").read_text()
    i = src.index("norm_orig_sha256=?, norm_orig_pcm_sha256=?, ")
    blk = src[i:src.index("WHERE", i)]
    assert "norm_plex_entry_uri = NULL" in blk, (
        "save_edit is the sibling byte-replacement writer — same 12th clear")


def test_v0_51_292_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert "0.51.292: " in init_py
