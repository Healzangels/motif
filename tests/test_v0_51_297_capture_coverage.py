"""v0.51.297 — holistic review wave 6: capture-revision writer coverage.

The v0.51.277 revision surface catalogued its writers — the review found
three canonical-replacement paths missing from it:
  1. api_adopt_from_plex swapped bytes without clearing the norm anchors
     (behavioral test rides tests/test_v1_21_72_adopt_restore_edition.py::
     test_adopt_clears_norm_anchors).
  2. backup_cloud_theme (force-capture / allow_existing_local) replaced an
     existing canonical with os.replace and NO capture — the previous
     audio was silently destroyed.
  3. The worker's sibling-hardlink short-circuit atomically replaced the
     section's theme.mp3 with NO capture (the direct download path
     stashes + captures).
"""
from __future__ import annotations

import sqlite3
from pathlib import Path
from unittest.mock import MagicMock

import pytest

REPO = Path(__file__).resolve().parent.parent
NOW = "2026-08-26T00:00:00+00:00"


def _seed(conn, *, rk, tmdb_id):
    conn.execute(
        "INSERT INTO plex_sections (section_id, title, type, is_anime, is_4k,"
        " themes_subdir, included, discovered_at, last_seen_at)"
        " VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, media_type, title,"
        " year, guid_tmdb, has_theme, first_seen_at, last_seen_at)"
        " VALUES (?, '1', 'movie', 'T', '2020', ?, 1, ?, ?)",
        (rk, tmdb_id, NOW, NOW))
    conn.execute(
        "INSERT INTO themes (media_type, tmdb_id, title, year,"
        " upstream_source, last_seen_sync_at, first_seen_sync_at)"
        " VALUES ('movie', ?, 'T', '2020', 'themoviedb', ?, ?)",
        (tmdb_id, NOW, NOW))


def test_cloud_backup_captures_the_outgoing_canonical(tmp_path):
    import hashlib
    from app.core.cloud_theme_backup import backup_cloud_theme
    from app.core.db import init_db
    db_path = tmp_path / "test.db"
    themes_dir = tmp_path / "themes"
    (themes_dir / "movies" / "T (2020)").mkdir(parents=True)
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        _seed(conn, rk="rk-cap", tmdb_id=297001)
        rel = "movies/T (2020)/theme.mp3"
        (themes_dir / rel).write_bytes(b"OLD-canonical")
        conn.execute(
            "INSERT INTO local_files (media_type, tmdb_id, section_id,"
            " edition_key, file_path, file_sha256, file_size, downloaded_at,"
            " source_video_id, provenance, source_kind)"
            " VALUES ('movie', 297001, '1', '', ?, ?, 13, ?, 'v', 'auto',"
            " 'themerrdb')",
            (rel, hashlib.sha256(b"OLD-canonical").hexdigest(), NOW))
        conn.commit()
        plex = MagicMock()
        plex._rk_path.return_value = "/library/metadata/rk-cap/file"
        plex._headers = {"X-Plex-Token": "fake"}
        resp = MagicMock()
        resp.status_code = 200
        resp.content = b"\x49\x44\x33" + b"NEW" * 400
        resp.text = ""
        plex._client.get.return_value = resp
        target = {
            "rating_key": "rk-cap", "guid_tmdb": 297001,
            "media_type": "movie", "section_id": "1", "title": "T",
            "year": "2020", "entry_uri": "metadata://themes/" + "a" * 40,
            "sha1": "a" * 40,
        }
        result = backup_cloud_theme(conn, target, themes_dir, plex)
        assert result["ok"] is True
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        revs = [dict(r) for r in conn.execute(
            "SELECT reason, retained_path FROM theme_revisions"
            " WHERE tmdb_id = 297001")]
    assert len(revs) == 1 and revs[0]["reason"] == "replaced_by_cloud_backup", (
        f"the swap must capture the outgoing canonical first: {revs}")
    assert revs[0]["retained_path"], "binary retained, not metadata-only"
    assert (themes_dir / revs[0]["retained_path"]).read_bytes() == (
        b"OLD-canonical"), "the retained bytes are the PREVIOUS audio"


def test_cloud_backup_first_add_still_captures_nothing(tmp_path):
    from app.core.cloud_theme_backup import backup_cloud_theme
    from app.core.db import init_db
    db_path = tmp_path / "test.db"
    themes_dir = tmp_path / "themes"
    themes_dir.mkdir()
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        _seed(conn, rk="rk-new", tmdb_id=297002)
        conn.commit()
        plex = MagicMock()
        plex._rk_path.return_value = "/library/metadata/rk-new/file"
        plex._headers = {"X-Plex-Token": "fake"}
        resp = MagicMock()
        resp.status_code = 200
        resp.content = b"\x49\x44\x33" + b"\x00" * 128
        resp.text = ""
        plex._client.get.return_value = resp
        target = {
            "rating_key": "rk-new", "guid_tmdb": 297002,
            "media_type": "movie", "section_id": "1", "title": "N",
            "year": "2020", "entry_uri": "metadata://themes/" + "b" * 40,
            "sha1": "b" * 40,
        }
        assert backup_cloud_theme(conn, target, themes_dir, plex)["ok"]
    with sqlite3.connect(db_path) as conn:
        n = conn.execute("SELECT COUNT(*) FROM theme_revisions").fetchone()[0]
    assert n == 0, "a first add is not a change — no revision minted"


def test_sibling_hardlink_branch_captures_before_replace():
    # the sibling short-circuit is deep in the worker's download flow; the
    # capture seam itself is behaviorally covered by the revisions suite —
    # this pins the WIRING: capture (COPY mode) before the atomic replace.
    src = (REPO / "app" / "core" / "worker.py").read_text()
    i = src.index("if not _same_inode:")
    blk = src[i:src.index("os.replace(_sib_tmp, target_mp3)", i)]
    assert "capture_revision(" in blk, (
        "the short-circuit replaces this section's canonical — the outgoing "
        "audio must get a revision entry like the direct download path")
    assert 'reason="replaced_by_download"' in blk
    assert 'incoming_sha=sibling["file_sha256"]' in blk, (
        "byte-identical relinks must not mint a meaningless revision")


def test_v0_51_297_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert "0.51.297: " in init_py
