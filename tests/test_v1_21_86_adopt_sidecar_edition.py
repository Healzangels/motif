"""v1.21.86 — ADOPT (sidecar) respects editions.

Audit found _do_adopt (adopt.py) staged into canonical_theme_subdir(title,
year) with NO edition arg and INSERTed local_files / placements with the
edition_key column omitted -> defaulted '' (standard). So adopting the
Extended edition's theme.mp3 sidecar wrote the row onto the STANDARD edition
and staged into the standard folder.

Now _do_adopt derives the edition from the sidecar's media_folder
({edition-X} tag) and keys the canonical subdir + both INSERTs to it.
"""
from __future__ import annotations

import sqlite3

import pytest

from app.core.db import init_db, get_conn
from app.core.events import now_iso


NOW = now_iso()
TMDB = 120
EXT_FOLDER_BASENAME = "LotR (2001) {edition-Sam Takes a Step}"


@pytest.fixture
def settings(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    (tmp_path / "themes").mkdir(parents=True, exist_ok=True)
    s._cfg.paths.themes_dir = str(tmp_path / "themes")
    init_db(s.db_path)
    return s


def test_do_adopt_writes_rows_at_the_sidecars_edition(settings, tmp_path):
    s = settings
    db = s.db_path
    # Source sidecar lives in the Sam Takes a Step edition's Plex folder.
    media_folder = tmp_path / "Movies" / EXT_FOLDER_BASENAME
    media_folder.mkdir(parents=True, exist_ok=True)
    src = media_folder / "theme.mp3"
    src.write_bytes(b"ID3audio!")

    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime,"
            " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
            " VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        cur = conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, year,"
            " upstream_source, last_seen_sync_at, first_seen_sync_at)"
            " VALUES ('movie',?,'LotR','2001','imdb',?,?)", (TMDB, NOW, NOW))
        tid = cur.lastrowid
        conn.commit()

    finding = {
        "section_id": "1",
        "section_type": "movie",
        "finding_kind": "content_mismatch",
        "theme_id": tid,
        "file_path": str(src),
        "file_sha256": "deadbeef" * 8,
        "file_size": src.stat().st_size,
        "media_folder": str(media_folder),
    }

    from app.core.adopt import _do_adopt
    _do_adopt(db, finding, s, decided_by="testadmin")

    with get_conn(db) as conn:
        lf = conn.execute(
            "SELECT edition_key, file_path FROM local_files"
            " WHERE media_type='movie' AND tmdb_id=?", (TMDB,)).fetchall()
        pl = conn.execute(
            "SELECT edition_key FROM placements"
            " WHERE media_type='movie' AND tmdb_id=?", (TMDB,)).fetchall()

    assert len(lf) == 1 and lf[0]["edition_key"] == "sam takes a step", (
        "local_files must be keyed to the sidecar's edition, not ''", [dict(r) for r in lf])
    assert len(pl) == 1 and pl[0]["edition_key"] == "sam takes a step", (
        "placement must be keyed to the sidecar's edition, not ''", [dict(r) for r in pl])
    # The canonical staged into the edition-tagged subdir.
    assert "{edition-" in lf[0]["file_path"], lf[0]["file_path"]


def test_v1_21_86_version_pin():
    from pathlib import Path
    init_py = (Path(__file__).resolve().parent.parent / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
