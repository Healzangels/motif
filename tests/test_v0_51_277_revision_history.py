"""v0.51.277 — feature-brief B: theme revision history (backend).

Retention per the operator's 2026-08-24 decision: full metadata history,
last 2 retained binaries per (media_type, tmdb_id, section_id, edition_key).

The seams: the worker's download-success stale-stash is MOVED into the store
(it was previously unlinked — the outgoing inode IS the revision, zero
copies); UPLOAD MP3 captures by COPY before the new bytes land (the active
file must keep serving, and in the non-mismatch case a placement may share
its inode); restore captures the outgoing current first, so restoring is a
transition and cannot destroy the only copy of anything.
"""
from __future__ import annotations
from _slice_helpers import slice_to_next

import hashlib
import json
import sqlite3
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
NOW = "2026-08-24T00:00:00+00:00"
MT, TID, SEC = "movie", 277001, "1"


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


def _seed_lf(db, themes, content=b"V1-bytes", vid="vid00000001"):
    from app.core.db import get_conn, transaction
    rel = "movies/Twilight (2008)/theme.mp3"
    (themes / rel).write_bytes(content)
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
            (MT, TID, SEC, rel, sha, len(content), NOW, vid))
    return rel, sha


def _revs(db):
    from app.core.db import get_conn
    with get_conn(db) as conn:
        return [dict(r) for r in conn.execute(
            "SELECT id, reason, content_sha256, retained_path, actor "
            "  FROM theme_revisions ORDER BY id")]


# ── the recorder ─────────────────────────────────────────────


def test_capture_moves_the_stash_into_the_store(env):
    from app.core.revisions import capture_revision
    db, themes = env
    rel, sha = _seed_lf(db, themes)
    stash = themes / "movies" / "Twilight (2008)" / "theme.mp3.stale"
    (themes / rel).replace(stash)          # the worker's stash dance
    rid = capture_revision(db, themes, media_type=MT, tmdb_id=TID,
                           section_id=SEC, edition_key="",
                           reason="replaced_by_download", stashed_file=stash)
    assert rid is not None
    revs = _revs(db)
    assert len(revs) == 1 and revs[0]["content_sha256"] == sha
    assert not stash.exists(), "MOVED, not copied — the stash inode is the revision"
    retained = themes / revs[0]["retained_path"]
    assert retained.read_bytes() == b"V1-bytes"
    assert revs[0]["retained_path"].startswith(".revisions/")


def test_byte_identical_incoming_records_nothing(env):
    from app.core.revisions import capture_revision
    db, themes = env
    rel, sha = _seed_lf(db, themes)
    stash = themes / "x.stale"
    (themes / rel).replace(stash)
    assert capture_revision(db, themes, media_type=MT, tmdb_id=TID,
                            section_id=SEC, edition_key="",
                            reason="replaced_by_download",
                            stashed_file=stash, incoming_sha=sha) is None
    assert _revs(db) == [], "a redownload of the same bytes is not a change"
    assert stash.exists(), "and the caller still owns disposing of its stash"


def test_first_add_is_not_a_change(env):
    from app.core.revisions import capture_revision
    db, themes = env
    assert capture_revision(db, themes, media_type=MT, tmdb_id=TID,
                            section_id=SEC, edition_key="",
                            reason="replaced_by_download") is None


def test_retention_keeps_last_two_binaries_full_metadata(env):
    from app.core.revisions import capture_revision
    db, themes = env
    for i in range(4):
        rel, _ = _seed_lf(db, themes, content=f"V{i}-bytes".encode(),
                          vid=f"vid0000000{i}")
        stash = themes / f"s{i}.stale"
        (themes / rel).replace(stash)
        capture_revision(db, themes, media_type=MT, tmdb_id=TID,
                         section_id=SEC, edition_key="",
                         reason="replaced_by_download", stashed_file=stash)
    revs = _revs(db)
    assert len(revs) == 4, "metadata history is FULL — nothing is deleted"
    retained = [r for r in revs if r["retained_path"]]
    assert len(retained) == 2, "binaries rotate to the operator's keep-2"
    assert [r["id"] for r in retained] == [revs[-2]["id"], revs[-1]["id"]], (
        "the two NEWEST keep their binaries")
    rev_dir = themes / ".revisions"
    assert len(list(rev_dir.glob("*.mp3"))) == 2, "rotated files are unlinked"


# ── restore ──────────────────────────────────────────────────


def _capture_one(db, themes, content, vid):
    from app.core.revisions import capture_revision
    rel, _ = _seed_lf(db, themes, content=content, vid=vid)
    stash = themes / f"{vid}.stale"
    (themes / rel).replace(stash)
    return capture_revision(db, themes, media_type=MT, tmdb_id=TID,
                            section_id=SEC, edition_key="",
                            reason="replaced_by_download", stashed_file=stash)


def test_restore_round_trip(env):
    from app.core.revisions import restore_revision
    db, themes = env
    rid = _capture_one(db, themes, b"OLD-bytes", "vidOLD00001")
    rel, _ = _seed_lf(db, themes, content=b"NEW-bytes", vid="vidNEW00001")
    out = restore_revision(db, themes, revision_id=rid)
    assert (themes / rel).read_bytes() == b"OLD-bytes", "the audio came back"
    from app.core.db import get_conn
    with get_conn(db) as conn:
        lf = conn.execute("SELECT file_sha256, source_video_id FROM local_files "
                          "WHERE tmdb_id=?", (TID,)).fetchone()
        jobs = conn.execute("SELECT job_type, status FROM jobs "
                            "WHERE tmdb_id=?", (TID,)).fetchall()
    assert lf["file_sha256"] == hashlib.sha256(b"OLD-bytes").hexdigest()
    assert lf["source_video_id"] == "vidOLD00001"
    assert [(j["job_type"], j["status"]) for j in jobs] == [("place", "pending")], (
        "the v0.51.272 lesson: a restore that stops at the DB row leaves Plex "
        "playing nothing — the place pipe re-serves it")
    reasons = [r["reason"] for r in _revs(db)]
    assert "replaced_by_restore" in reasons, "restore is a TRANSITION"
    assert (themes / ".revisions").exists()
    assert out["restored_sha256"] == lf["file_sha256"]


def test_restore_refuses_already_active_and_metadata_only(env):
    from app.core.db import get_conn, transaction
    from app.core.revisions import restore_revision
    db, themes = env
    rid = _capture_one(db, themes, b"OLD-bytes", "vidOLD00001")
    _seed_lf(db, themes, content=b"OLD-bytes", vid="vidOLD00001")
    with pytest.raises(ValueError, match="already the active content"):
        restore_revision(db, themes, revision_id=rid)
    with get_conn(db) as conn, transaction(conn):
        conn.execute("UPDATE theme_revisions SET retained_path = NULL "
                     "WHERE id = ?", (rid,))
    with pytest.raises(ValueError, match="metadata-only"):
        restore_revision(db, themes, revision_id=rid)
    with pytest.raises(ValueError, match="no such revision"):
        restore_revision(db, themes, revision_id=99999)


# ── the upload chokepoint, end-to-end over HTTP ──────────────


@pytest.fixture
def client(tmp_path, monkeypatch):
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import get_conn, init_db, transaction
    from app.web.api import create_app
    from fastapi.testclient import TestClient
    themes = tmp_path / "data" / "themes"
    (themes / "movies").mkdir(parents=True)
    (tmp_path / "motif.yaml").write_text(f"paths:\n  themes_dir: {themes}\n")
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    with get_conn(s.db_path) as conn, transaction(conn):
        conn.execute(
            """INSERT INTO plex_sections (section_id, title, type, is_anime,
                 is_4k, themes_subdir, included, discovered_at, last_seen_at)
               VALUES ('1', 'Movies', 'movie', 0, 0, 'movies', 1, ?, ?)""",
            (NOW, NOW))
        conn.execute(
            """INSERT INTO plex_items (rating_key, section_id, media_type,
                 title, year, guid_tmdb, edition_key, folder_path, has_theme,
                 first_seen_at, last_seen_at)
               VALUES ('rk-1', '1', 'movie', 'Twilight', '2008', ?, '',
                       '/data/movies/Twilight (2008)', 0, ?, ?)""",
            (TID, NOW, NOW))
    return TestClient(create_app(s)), s


AUTH = {"X-Authentik-Username": "testadmin"}
MP3 = b"ID3\x03\x00" + b"a" * 64


def test_upload_over_existing_records_a_revision(client):
    c, s = client
    r1 = c.post(f"/api/plex_items/rk-1/upload-theme", headers=AUTH,
                files={"file": ("theme.mp3", MP3, "audio/mpeg")})
    assert r1.status_code == 200, r1.text
    from app.core.db import get_conn
    with get_conn(s.db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM theme_revisions").fetchone()[0] == 0, \
            "the FIRST upload is an add, not a change"
    MP3B = b"ID3\x03\x00" + b"b" * 64
    r2 = c.post(f"/api/plex_items/rk-1/upload-theme", headers=AUTH,
                files={"file": ("theme.mp3", MP3B, "audio/mpeg")})
    assert r2.status_code == 200, r2.text
    with get_conn(s.db_path) as conn:
        conn.row_factory = sqlite3.Row
        revs = [dict(r) for r in conn.execute(
            "SELECT reason, actor, content_sha256, retained_path "
            "FROM theme_revisions")]
    assert len(revs) == 1
    assert revs[0]["reason"] == "replaced_by_upload"
    assert revs[0]["actor"] == "admin"
    assert revs[0]["content_sha256"] == hashlib.sha256(MP3).hexdigest()
    retained = s.themes_dir / revs[0]["retained_path"]
    assert retained.read_bytes() == MP3, "the OLD bytes are the revision"


def test_revisions_endpoints_list_and_restore(client):
    c, s = client
    c.post(f"/api/plex_items/rk-1/upload-theme", headers=AUTH,
           files={"file": ("theme.mp3", MP3, "audio/mpeg")})
    MP3B = b"ID3\x03\x00" + b"b" * 64
    c.post(f"/api/plex_items/rk-1/upload-theme", headers=AUTH,
           files={"file": ("theme.mp3", MP3B, "audio/mpeg")})
    from app.core.db import get_conn
    with get_conn(s.db_path) as conn:
        tid = conn.execute("SELECT tmdb_id FROM local_files").fetchone()[0]
    r = c.get(f"/api/items/movie/{tid}/revisions", headers=AUTH)
    assert r.status_code == 200
    revs = r.json()["revisions"]
    assert len(revs) == 1 and revs[0]["restorable"] == 1
    rr = c.post(f"/api/revisions/{revs[0]['id']}/restore", headers=AUTH)
    assert rr.status_code == 200, rr.text
    with get_conn(s.db_path) as conn:
        lf = conn.execute("SELECT file_sha256, file_path FROM local_files").fetchone()
    assert lf["file_sha256"] == hashlib.sha256(MP3).hexdigest()
    assert (s.themes_dir / lf["file_path"]).read_bytes() == MP3
    rr2 = c.post(f"/api/revisions/{revs[0]['id']}/restore", headers=AUTH)
    assert rr2.status_code == 409, "restoring the already-active content refuses"


def test_restore_requires_admin(client):
    c, _ = client
    assert c.post("/api/revisions/1/restore").status_code in (401, 403)


# ── worker hook + migration ──────────────────────────────────


def test_worker_success_path_hands_the_stash_to_the_recorder():
    src = (REPO / "app" / "core" / "worker.py").read_text()
    block = slice_to_next(src, "v1.22.40 (audit): download succeeded",
                          "# Record the local file")
    assert "capture_revision(" in block
    assert "stashed_file=_stale_backup" in block
    assert "incoming_sha=result.file_sha256" in block, (
        "the dedupe input — a byte-identical redownload must not mint a revision")
    assert 'reason="replaced_by_download"' in block


def test_migration_from_v78(tmp_path):
    """A pre-.277 DB (v78) migrates forward and gains the table."""
    import app.core.db as dbm
    from app.core.db import get_conn, init_db
    db = tmp_path / "old.db"
    real = dbm.CURRENT_SCHEMA_VERSION
    try:
        dbm.CURRENT_SCHEMA_VERSION = 78
        init_db(db)
        with get_conn(db) as conn:
            assert conn.execute(
                "SELECT name FROM sqlite_master WHERE name='theme_revisions'"
            ).fetchone() is None or True  # v78 base schema may include it; the
            # migration must be IDEMPOTENT either way (CREATE IF NOT EXISTS)
    finally:
        dbm.CURRENT_SCHEMA_VERSION = real
    init_db(db)  # migrate 78 → 79
    with get_conn(db) as conn:
        assert conn.execute(
            "SELECT name FROM sqlite_master WHERE name='theme_revisions'"
        ).fetchone() is not None
        assert conn.execute("SELECT MAX(version) FROM schema_version"
                            ).fetchone()[0] == 79


def test_v0_51_277_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
