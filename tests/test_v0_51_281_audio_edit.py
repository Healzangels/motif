"""v0.51.281 — feature-brief C: trim/fade editing (backend).

The parked design blocker dissolved by our own construction: a trim is a
lossy ffmpeg re-encode, which used to mean destroying the only copy — but
revision history (v0.51.277) retains the pre-edit bytes as a restorable
revision, so an edit is non-destructive the same way every replacement is.

Deviations from the brief's §7.2, deliberate and recorded: no loudness
normalization in the editor (motif's mp3gain pipeline is lossless + undoable;
a second re-encode here would break its undo anchors — use // LEVEL LOUDNESS)
and silence-detection deferred (the brief marks it optional).

save_edit needs NO ffmpeg, so the save/lock/revision tests run everywhere;
only the render tests skip when ffmpeg is absent.
"""
from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
HAS_FFMPEG = shutil.which("ffmpeg") is not None and shutil.which("ffprobe") is not None
NOW = "2026-08-26T00:00:00+00:00"
MT, TID, SEC = "movie", 281001, "1"


def _make_fixture_mp3(dest: Path, seconds: float = 3.0) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        ["ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
         "-f", "lavfi", "-i", f"sine=frequency=440:duration={seconds}",
         "-codec:a", "libmp3lame", "-q:a", "9", str(dest)],
        check=True, timeout=60)


@pytest.fixture
def env(tmp_path):
    from app.core.db import get_conn, init_db, transaction
    db = tmp_path / "t.db"
    themes = tmp_path / "themes"
    (themes / "movies" / "T (2020)").mkdir(parents=True)
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
               VALUES (?, ?, 'T', '2020', 'themoviedb', ?, ?)""",
            (MT, TID, NOW, NOW))
    return db, themes


def _seed_lf(db, themes, content=b"CANON", *, norm=False):
    from app.core.db import get_conn, transaction
    rel = "movies/T (2020)/theme.mp3"
    (themes / rel).write_bytes(content)
    sha = hashlib.sha256(content).hexdigest()
    with get_conn(db) as conn, transaction(conn):
        conn.execute(
            """INSERT INTO local_files (media_type, tmdb_id, section_id,
                 edition_key, file_path, file_sha256, file_size, downloaded_at,
                 source_video_id, provenance, source_kind, norm_state)
               VALUES (?, ?, ?, '', ?, ?, ?, ?, '', 'auto', 'themerrdb', ?)
               ON CONFLICT(media_type, tmdb_id, section_id, edition_key)
               DO UPDATE SET file_sha256=excluded.file_sha256,
                             norm_state=excluded.norm_state""",
            (MT, TID, SEC, rel, sha, len(content), NOW,
             "normalized" if norm else None))
    return rel, sha


def _mk_candidate(themes, content=b"EDITED-bytes"):
    from app.core.audio_edit import _CAND_DIR
    import secrets
    cid = secrets.token_hex(16)
    d = themes / _CAND_DIR
    d.mkdir(parents=True, exist_ok=True)
    (d / f"{cid}.mp3").write_bytes(content)
    return cid


# ── render (needs ffmpeg) ────────────────────────────────────


@pytest.mark.skipif(not HAS_FFMPEG, reason="ffmpeg/ffprobe not installed")
def test_render_trims_to_the_requested_window(env, tmp_path):
    from app.core.audio_edit import probe_duration, render_candidate
    _, themes = env
    _make_fixture_mp3(themes / "movies" / "T (2020)" / "theme.mp3", 3.0)
    out = render_candidate(themes, "movies/T (2020)/theme.mp3",
                           trim_start=0.5, trim_end=2.0)
    assert 1.2 <= out["duration_s"] <= 1.8, "≈1.5s window"
    assert out["file_size"] > 0 and len(out["sha256"]) == 64
    cand = themes / ".edit-candidates" / f"{out['candidate_id']}.mp3"
    assert cand.exists()
    assert probe_duration(cand) is not None, "the candidate is valid audio"


@pytest.mark.skipif(not HAS_FFMPEG, reason="ffmpeg/ffprobe not installed")
def test_render_with_fades_produces_valid_audio(env):
    from app.core.audio_edit import render_candidate
    _, themes = env
    _make_fixture_mp3(themes / "movies" / "T (2020)" / "theme.mp3", 3.0)
    out = render_candidate(themes, "movies/T (2020)/theme.mp3",
                           trim_start=0.0, trim_end=3.0,
                           fade_in=0.5, fade_out=0.5)
    assert out["duration_s"] > 2.5


@pytest.mark.skipif(not HAS_FFMPEG, reason="ffmpeg/ffprobe not installed")
def test_render_refuses_bad_bounds(env):
    from app.core.audio_edit import EditError, render_candidate
    _, themes = env
    _make_fixture_mp3(themes / "movies" / "T (2020)" / "theme.mp3", 3.0)
    rel = "movies/T (2020)/theme.mp3"
    with pytest.raises(EditError, match="start < end"):
        render_candidate(themes, rel, trim_start=2.0, trim_end=1.0)
    with pytest.raises(EditError, match="past the file"):
        render_candidate(themes, rel, trim_start=0.0, trim_end=99.0)
    with pytest.raises(EditError, match="fades cannot overlap"):
        render_candidate(themes, rel, trim_start=0.0, trim_end=1.0,
                         fade_in=0.8, fade_out=0.8)


# ── save (no ffmpeg needed) ──────────────────────────────────


def test_save_promotes_candidate_records_revision_and_replaces(env):
    from app.core.audio_edit import save_edit
    db, themes = env
    rel, sha = _seed_lf(db, themes, b"CANON-bytes", norm=True)
    cid = _mk_candidate(themes, b"EDITED-bytes")
    out = save_edit(db, themes, media_type=MT, tmdb_id=TID, section_id=SEC,
                    edition_key="", candidate_id=cid, base_sha=sha)
    assert (themes / rel).read_bytes() == b"EDITED-bytes"
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        lf = conn.execute("SELECT file_sha256, norm_state FROM local_files "
                          "WHERE tmdb_id=?", (TID,)).fetchone()
        revs = [dict(r) for r in conn.execute(
            "SELECT reason, retained_path FROM theme_revisions")]
        jobs = conn.execute("SELECT job_type, status FROM jobs "
                            "WHERE tmdb_id=?", (TID,)).fetchall()
    assert lf["file_sha256"] == hashlib.sha256(b"EDITED-bytes").hexdigest()
    assert lf["norm_state"] is None, (
        "new bytes — the mp3gain undo anchors are invalid and must clear")
    assert [r["reason"] for r in revs] == ["replaced_by_edit"]
    retained = themes / revs[0]["retained_path"]
    assert retained.read_bytes() == b"CANON-bytes", (
        "the pre-edit audio is a restorable revision — the lossy-re-encode "
        "objection is dissolved by construction")
    assert [(j["job_type"], j["status"]) for j in jobs] == [("place", "pending")]
    assert not (themes / ".edit-candidates" / f"{cid}.mp3").exists(), (
        "the candidate was MOVED into place, not copied")
    assert out["sha256"] == lf["file_sha256"]


def test_save_refuses_a_stale_base_sha(env):
    """The brief's concurrent-edit guard, as an optimistic lock."""
    from app.core.audio_edit import EditError, save_edit
    db, themes = env
    rel, _ = _seed_lf(db, themes, b"CANON-bytes")
    cid = _mk_candidate(themes)
    with pytest.raises(EditError, match="changed while you were editing"):
        save_edit(db, themes, media_type=MT, tmdb_id=TID, section_id=SEC,
                  edition_key="", candidate_id=cid,
                  base_sha="deadbeef" * 8)
    assert (themes / rel).read_bytes() == b"CANON-bytes", "nothing replaced"
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM theme_revisions"
                            ).fetchone()[0] == 0, "and no revision was burned"


def test_save_refuses_expired_candidate(env):
    from app.core.audio_edit import EditError, save_edit
    db, themes = env
    _seed_lf(db, themes)
    with pytest.raises(EditError, match="expired or already used"):
        save_edit(db, themes, media_type=MT, tmdb_id=TID, section_id=SEC,
                  edition_key="", candidate_id="ab" * 16, base_sha="")


def test_candidate_id_is_traversal_safe(env):
    from app.core.audio_edit import EditError, candidate_path
    _, themes = env
    for bad in ("../../etc/passwd", "..%2f..", "x" * 32, "AB" * 16, "short"):
        with pytest.raises(EditError, match="invalid candidate id"):
            candidate_path(themes, bad)


def test_discard_deletes_the_candidate(env):
    from app.core.audio_edit import discard_candidate
    _, themes = env
    cid = _mk_candidate(themes)
    discard_candidate(themes, cid)
    assert not (themes / ".edit-candidates" / f"{cid}.mp3").exists()
    discard_candidate(themes, cid)  # idempotent


# ── endpoints ────────────────────────────────────────────────


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import get_conn, init_db, transaction
    from app.web.api import create_app
    from fastapi.testclient import TestClient
    themes = tmp_path / "data" / "themes"
    (themes / "movies" / "T (2020)").mkdir(parents=True)
    (tmp_path / "motif.yaml").write_text(f"paths:\n  themes_dir: {themes}\n")
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    with get_conn(s.db_path) as conn, transaction(conn):
        conn.execute(
            """INSERT INTO plex_sections (section_id, title, type, is_anime,
                 is_4k, themes_subdir, included, discovered_at, last_seen_at)
               VALUES (?, 'Movies', 'movie', 0, 0, 'movies', 1, ?, ?)""",
            (SEC, NOW, NOW))
        conn.execute(
            """INSERT INTO themes (media_type, tmdb_id, title, year,
                 upstream_source, last_seen_sync_at, first_seen_sync_at)
               VALUES (?, ?, 'T', '2020', 'themoviedb', ?, ?)""",
            (MT, TID, NOW, NOW))
    return TestClient(create_app(s)), s


AUTH = {"X-Authentik-Username": "testadmin"}


def test_save_endpoint_end_to_end(client):
    c, s = client
    from app.core.db import get_conn, transaction
    rel = "movies/T (2020)/theme.mp3"
    (s.themes_dir / rel).write_bytes(b"CANON-bytes")
    sha = hashlib.sha256(b"CANON-bytes").hexdigest()
    with get_conn(s.db_path) as conn, transaction(conn):
        conn.execute(
            """INSERT INTO local_files (media_type, tmdb_id, section_id,
                 edition_key, file_path, file_sha256, file_size, downloaded_at,
                 source_video_id, provenance, source_kind)
               VALUES (?, ?, ?, '', ?, ?, 11, ?, '', 'auto', 'themerrdb')""",
            (MT, TID, SEC, rel, sha, NOW))
    cid = _mk_candidate(s.themes_dir, b"EDITED-bytes")
    r = c.post(f"/api/items/{MT}/{TID}/edit-theme/save", headers=AUTH,
               json={"candidate_id": cid, "base_sha": sha,
                     "section_id": SEC, "edition_key": ""})
    assert r.status_code == 200, r.text
    assert (s.themes_dir / rel).read_bytes() == b"EDITED-bytes"
    stale = c.post(f"/api/items/{MT}/{TID}/edit-theme/save", headers=AUTH,
                   json={"candidate_id": cid, "base_sha": sha,
                         "section_id": SEC, "edition_key": ""})
    assert stale.status_code == 409, "candidate consumed + sha moved — 409s"


def test_candidate_stream_is_admin_gated_and_validates_id(client):
    c, s = client
    assert c.get(f"/api/items/{MT}/{TID}/edit-candidate/{'ab'*16}.mp3"
                 ).status_code in (401, 403)
    r = c.get(f"/api/items/{MT}/{TID}/edit-candidate/{'zz'*16}.mp3",
              headers=AUTH)
    assert r.status_code == 400, "non-hex id refused"
    r = c.get(f"/api/items/{MT}/{TID}/edit-candidate/{'ab'*16}.mp3",
              headers=AUTH)
    assert r.status_code == 404, "valid id, no file → expired"
    cid = _mk_candidate(s.themes_dir, b"AUDIO")
    r = c.get(f"/api/items/{MT}/{TID}/edit-candidate/{cid}.mp3", headers=AUTH)
    assert r.status_code == 200 and r.content == b"AUDIO"


def test_preview_endpoint_404s_without_a_canonical(client):
    c, _ = client
    r = c.post(f"/api/items/{MT}/{TID}/edit-theme", headers=AUTH,
               json={"section_id": SEC, "trim_start": 0, "trim_end": 1})
    assert r.status_code == 404


def test_v0_51_281_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
