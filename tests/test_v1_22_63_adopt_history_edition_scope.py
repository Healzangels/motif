"""v1.22.63 (audit round 2, Batch A #5) — adopt's URL-history restore
bled to sibling editions.

`_maybe_restore_url_history` (the v1.10.57 A→U promotion on adopt)
flipped `local_files.source_kind`/`source_video_id` with a
section-only WHERE — the one edition straggler the v1.21.66→v1.22.25
edition-scope campaign missed (`_do_adopt` itself was made
edition-keyed in v1.21.86, but its sibling helper wasn't). Post-v63
PK a title legitimately holds '' + 'Extended' local_files rows in one
section, so adopting the Extended sidecar rewrote the standard
edition's tracking too (the v1.22.51 corruption class).

Fix: edition_key threaded from the adopt call site (same
edition_key_for_folder derivation _do_adopt uses) into both UPDATE
branches.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from app.core.adopt import _maybe_restore_url_history
from app.core.db import init_db

REPO = Path(__file__).resolve().parent.parent
ADOPT_PY = (REPO / "app" / "core" / "adopt.py").read_text()
TS = "2026-06-11T00:00:00+00:00"
SHA = "ab" * 32


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
        # Two edition rows in the SAME section — both adopt-sourced.
        for ek in ("", "Extended"):
            conn.execute(
                "INSERT INTO local_files (media_type, tmdb_id, section_id,"
                " edition_key, file_path, downloaded_at, source_video_id,"
                " source_kind) VALUES ('movie', 500, '1', ?, ?, ?,"
                " 'adoptvid', 'adopt')",
                (ek, f"movies/LotR{ek}/theme.mp3", TS))
        # History row matching the adopted bytes (URL-sourced).
        conn.execute(
            "INSERT INTO local_files_history (file_sha256, media_type,"
            " tmdb_id, source_kind, source_video_id, youtube_url, saved_at)"
            " VALUES (?, 'movie', 500, 'url', 'VID123',"
            " 'https://yt/old', ?)", (SHA, TS))
        conn.commit()


def _kinds(db) -> dict:
    with sqlite3.connect(db) as conn:
        return {r[0]: (r[1], r[2]) for r in conn.execute(
            "SELECT edition_key, source_kind, source_video_id"
            " FROM local_files WHERE tmdb_id = 500")}


def test_restore_flips_only_the_adopting_edition(tmp_path):
    """Adopt-as-U path: the Extended adopt flips Extended's row only —
    the standard edition's tracking is untouched (pre-fix: both
    rewritten)."""
    db = tmp_path / "motif.db"
    init_db(db)
    _seed(db)
    out = _maybe_restore_url_history(
        db, media_type="movie", tmdb_id=500, sha256=SHA,
        decided_by="test", section_id="1", edition_key="Extended")
    assert out and out["source_kind"] == "url"
    kinds = _kinds(db)
    assert kinds["Extended"] == ("url", "VID123")
    assert kinds[""] == ("adopt", "adoptvid"), (
        "v1.22.63: the standard edition's row must NOT be rewritten by "
        "the Extended adopt's history restore"
    )


def test_restore_as_t_flips_only_the_adopting_edition(tmp_path):
    """Adopt-as-T path (history URL == TDB URL): same edition scoping."""
    db = tmp_path / "motif.db"
    init_db(db)
    _seed(db, theme_url="https://yt/old")
    out = _maybe_restore_url_history(
        db, media_type="movie", tmdb_id=500, sha256=SHA,
        decided_by="test", section_id="1", edition_key="Extended")
    assert out and out["source_kind"] == "themerrdb"
    kinds = _kinds(db)
    assert kinds["Extended"] == ("themerrdb", "VID123")
    assert kinds[""] == ("adopt", "adoptvid")


def test_legacy_none_edition_keeps_section_wide_behavior(tmp_path):
    """edition_key=None (legacy callers without edition context) keeps
    the pre-fix section-wide flip — the narrowing is opt-in by the
    caller that KNOWS the edition."""
    db = tmp_path / "motif.db"
    init_db(db)
    _seed(db)
    _maybe_restore_url_history(
        db, media_type="movie", tmdb_id=500, sha256=SHA,
        decided_by="test", section_id="1")
    kinds = _kinds(db)
    assert kinds["Extended"][0] == "url" and kinds[""][0] == "url"


def test_adopt_call_site_threads_the_edition():
    """The adopt_finding call site derives the edition from the
    finding's media_folder — the same derivation _do_adopt uses."""
    assert ('edition_key=edition_key_for_folder(finding["media_folder"])'
            in ADOPT_PY)
