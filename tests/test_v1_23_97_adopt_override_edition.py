"""v1.23.97 (audit) — adopt's URL-history restore wrote the override at the WRONG
edition.

The twin of v1.22.63: that tag scoped `_maybe_restore_url_history`'s local_files
FLIP to the adopting edition, but the adopt-as-U branch's user_overrides INSERT
still hardcoded edition_key='' (column default). The worker's override resolution
(worker.py:1522-1534) is edition-keyed — it tries (section, edition) then
('', edition), with NO edition='' fallback (v1.21.92). So restoring an Extended
adopt wrote the override at edition='' while flipping Extended's local_files row to
source_kind='url' → the worker found no resolvable override for the Extended
edition's download (U-row with no backing URL). Fix: write the override at the
adopting edition (or '' when the adopt wasn't edition-scoped), matching the flip.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from app.core.adopt import _maybe_restore_url_history
from app.core.db import init_db

REPO = Path(__file__).resolve().parent.parent
ADOPT_PY = (REPO / "app" / "core" / "adopt.py").read_text()
TS = "2026-06-19T00:00:00+00:00"
SHA = "cd" * 32


def _seed(db, *, theme_url=""):
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime,"
            " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
            " VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (TS, TS))
        conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, upstream_source,"
            " last_seen_sync_at, first_seen_sync_at, youtube_url)"
            " VALUES ('movie', 500, 'LotR', 'themoviedb', ?, ?, ?)",
            (TS, TS, theme_url))
        for ek in ("", "Extended"):
            conn.execute(
                "INSERT INTO local_files (media_type, tmdb_id, section_id,"
                " edition_key, file_path, downloaded_at, source_video_id,"
                " source_kind) VALUES ('movie', 500, '1', ?, ?, ?,"
                " 'adoptvid', 'adopt')",
                (ek, f"movies/LotR{ek}/theme.mp3", TS))
        conn.execute(
            "INSERT INTO local_files_history (file_sha256, media_type,"
            " tmdb_id, source_kind, source_video_id, youtube_url, saved_at)"
            " VALUES (?, 'movie', 500, 'url', 'VID123',"
            " 'https://yt/old', ?)", (SHA, TS))
        conn.commit()


def _overrides(db) -> dict:
    with sqlite3.connect(db) as conn:
        return {r[0]: r[1] for r in conn.execute(
            "SELECT edition_key, youtube_url FROM user_overrides"
            " WHERE tmdb_id = 500")}


def test_adopt_as_u_writes_override_at_adopting_edition(tmp_path):
    """The restored override lands at the ADOPTING edition (Extended), not '' —
    so the worker's edition-keyed resolution can actually find it."""
    db = tmp_path / "motif.db"
    init_db(db)
    _seed(db)  # theme_url='' ≠ history url → adopt-as-U (override branch)
    out = _maybe_restore_url_history(
        db, media_type="movie", tmdb_id=500, sha256=SHA,
        decided_by="test", section_id="1", edition_key="Extended")
    assert out and out["source_kind"] == "url"
    ovr = _overrides(db)
    assert ovr.get("Extended") == "https://yt/old", (
        "v1.23.97: override must land on the adopting (Extended) edition")
    assert "" not in ovr, (
        "the override must NOT land on the standard '' edition — the worker "
        "can't resolve a '' override for an Extended-edition download")


def test_adopt_as_u_legacy_none_edition_writes_standard(tmp_path):
    """edition_key=None (legacy callers) → override at '' (column default),
    matching the section-wide legacy local_files flip."""
    db = tmp_path / "motif.db"
    init_db(db)
    _seed(db)
    _maybe_restore_url_history(
        db, media_type="movie", tmdb_id=500, sha256=SHA,
        decided_by="test", section_id="1")  # no edition_key → None
    assert _overrides(db).get("") == "https://yt/old"


def test_override_insert_carries_edition_key():
    assert "edition_key or ''" in ADOPT_PY
    # the adopt-as-U override INSERT now lists edition_key in its column tuple.
    i = ADOPT_PY.index("'restored from local_files_history on adopt'")
    block = ADOPT_PY[max(0, i - 300):i + 200]
    assert "edition_key" in block


def test_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
