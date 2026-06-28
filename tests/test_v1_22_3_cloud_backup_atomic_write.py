"""v1.22.3 — DOWNLOAD PLEX BACKUP must write atomically (temp + os.replace).

the user's Watchmen Midnight (over-ceiling, 15.3MB themerrdb canonical): DOWNLOAD
PLEX BACKUP reported "1 error" and the UI blamed Plex ("Plex wouldn't serve
those bytes"). The in-container repro proved Plex served the bytes fine
(http 200, 2.9MB); docker logs showed the truth:
    backup_cloud_theme: rk=417813 disk write failed: PermissionError(13, ...)
backup_cloud_theme used `abs_path.write_bytes()`, which open('wb') TRUNCATES the
existing theme.mp3 in place — needs WRITE on that file. On the user's Unraid the
themerrdb canonical isn't writable by the container user, so the overwrite
EACCES'd, while the worker's download (temp + rename) wrote the same path fine.
v1.22.3 writes a temp sibling + os.replace (needs only DIRECTORY write), the
same atomic pattern placement.py uses.
"""
from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from app.core.canonical import canonical_theme_subdir
from app.core.db import init_db


NOW = "2026-06-05T00:00:00Z"
OLD = b"OLD-CANONICAL-BYTES-themerrdb-15mb-stand-in"
NEW = b"\x49\x44\x33" + b"NEW-CLOUD-BYTES" * 64  # ID3-ish, distinct from OLD


def _section(conn):
    conn.execute(
        "INSERT INTO plex_sections (section_id, title, type, is_anime, is_4k,"
        " themes_subdir, included, discovered_at, last_seen_at)"
        " VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))


def _mock_plex(rk):
    plex = MagicMock()
    plex._rk_path.return_value = f"/library/metadata/{rk}/file"
    plex._headers = {}
    resp = MagicMock()
    resp.status_code = 200
    resp.content = NEW
    resp.text = ""
    resp.headers = {}  # no content-length → truncation check is skipped
    plex._client.get.return_value = resp
    return plex


def _target(rk, tmdb, ek):
    return {
        "rating_key": rk, "guid_tmdb": tmdb, "media_type": "movie",
        "section_id": "1", "title": "RO Test", "year": "2009",
        "edition_key": ek,
        "entry_uri": "upload://themes/" + "a" * 40, "sha1": "a" * 40,
    }


def test_backup_overwrites_readonly_canonical_via_atomic_replace(tmp_path):
    """THE regression: a non-writable existing theme.mp3 must NOT block the
    backup. os.replace swaps it via the (writable) directory."""
    from app.core.cloud_theme_backup import backup_cloud_theme
    db = tmp_path / "m.db"
    themes = tmp_path / "themes"
    themes.mkdir()
    init_db(db)

    # Pre-create the canonical for edition 'midnight' with OLD bytes, 0444.
    folder = themes / "movies" / canonical_theme_subdir("RO Test", "2009", "midnight")
    folder.mkdir(parents=True)
    canonical = folder / "theme.mp3"
    canonical.write_bytes(OLD)
    os.chmod(canonical, 0o444)

    # Self-validating: confirm the file is genuinely non-writable in-place
    # here. If the env ignores perms (running as root), the regression can't
    # be exercised — skip rather than pass vacuously.
    try:
        fh = open(canonical, "wb")
        fh.close()
        pytest.skip("env ignores 0444 (root?) — atomic-replace not exercised")
    except PermissionError:
        pass

    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    try:
        _section(conn)
        conn.commit()
        result = backup_cloud_theme(conn, _target("rk-ro", 700, "midnight"),
                                    themes, _mock_plex("rk-ro"))
    finally:
        conn.close()

    assert result["ok"] is True, result
    assert result["bytes_written"] == len(NEW)
    # The read-only canonical was atomically replaced with the new bytes.
    assert canonical.read_bytes() == NEW
    # No temp file left behind.
    assert not (folder / "theme.mp3.backup-tmp").exists()


def test_backup_cleans_up_temp_on_write_failure(tmp_path):
    """If the disk write genuinely fails, no .backup-tmp is orphaned."""
    from app.core.cloud_theme_backup import backup_cloud_theme
    db = tmp_path / "m.db"
    themes = tmp_path / "themes"
    themes.mkdir()
    init_db(db)
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    try:
        _section(conn)
        conn.commit()
        plex = _mock_plex("rk-x")
        # Make the destination DIRECTORY unwritable so even os.replace fails.
        folder = themes / "movies" / canonical_theme_subdir("RO Test", "2009", "")
        folder.mkdir(parents=True)
        if os.geteuid() == 0:
            pytest.skip("root ignores dir perms")
        os.chmod(folder, 0o555)
        try:
            result = backup_cloud_theme(conn, _target("rk-x", 701, ""),
                                        themes, plex)
            assert result["ok"] is False
            assert "disk" in (result.get("error") or "")
            assert not (folder / "theme.mp3.backup-tmp").exists()
        finally:
            os.chmod(folder, 0o755)
    finally:
        conn.close()


def test_atomic_pattern_source_pin():
    """The in-place write_bytes(audio_bytes) is gone; os.replace + a temp
    sibling are used instead."""
    src = (Path(__file__).resolve().parent.parent
           / "app" / "core" / "cloud_theme_backup.py").read_text()
    fn = src.index("def backup_cloud_theme(")
    body = src[fn:fn + 12000]
    assert "os.replace(tmp_path, abs_path)" in body
    assert ".backup-tmp" in body
    assert "abs_path.write_bytes(audio_bytes)" not in body, (
        "in-place write_bytes must be replaced by the atomic temp + replace")


def test_dedup_query_is_edition_scoped():
    """The sha256 dedup guard must filter on edition_key (was sibling-blind)."""
    src = (Path(__file__).resolve().parent.parent
           / "app" / "core" / "cloud_theme_backup.py").read_text()
    fn = src.index("def backup_cloud_theme(")
    body = src[fn:fn + 12000]
    dedup = body[body.index("SELECT file_sha256 FROM local_files"):]
    head = dedup[:300]
    assert "AND edition_key = ?" in head, (
        "dedup guard must be edition-scoped or it compares a sibling edition")


def test_v1_22_3_version_pin():
    init_py = (Path(__file__).resolve().parent.parent
               / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
