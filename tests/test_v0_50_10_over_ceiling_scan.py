"""v0.50.10 — ORPHAN SCAN: over-ceiling re-encoded uploads aren't false-flagged.

the user's prod: '10' (24MB theme) and '10 Cloverfield Lane' (10.7MB) were
re-encoded to ~8.5MB to fit Plex's ~10MB upload ceiling and uploaded — the probe
showed the re-encoded `upload://` entry is selected:true (Plex IS serving
motif's theme). But the scanner compared motif's CANONICAL hash, which Plex
never holds (the re-encoded blob has a different hash by design — v1.24.47), so
it perpetually flagged motif_entry_missing and RE-PUSH could never clear it.

Fix (scanner-only, read-only diagnostic — verify_placement_health is rk-based
and excludes plex_upload, so the library was already correct): when motif's
canonical is OVER the ceiling AND Plex has a SELECTED upload:// entry, classify
the row OK (Plex serves the re-encoded upload), not motif_entry_missing.
"""
from __future__ import annotations

import hashlib
import sqlite3
from pathlib import Path
from unittest.mock import MagicMock

from test_v1_14_59_recovery_options_behavioral import (  # noqa: F401
    _seed_plex_item, _seed_section, _seed_theme)

from app.core.db import init_db
from app.core.orphan_scan import scan_plex_upload_placements

TS = "2026-06-27T00:00:00+00:00"
# NOT the canonical's hash — mirrors Plex holding the re-encoded blob.
REENCODED_SHA = hashlib.sha1(b"smaller-reencoded-bytes").hexdigest()
OVER = 11 * 1024 * 1024   # > Plex's ~10MB ceiling
UNDER = 2 * 1024 * 1024


def _seed(conn, themes: Path, tid: int, size_bytes: int):
    _seed_section(conn, "1")
    _seed_theme(conn, tmdb_id=tid, title=f"Big {tid}")
    pk = conn.execute(
        "SELECT id FROM themes WHERE tmdb_id=?", (tid,)).fetchone()[0]
    rk = f"rk{tid}"
    _seed_plex_item(conn, rating_key=rk, section_id="1", tmdb_id=tid)
    conn.execute("UPDATE plex_items SET theme_id=?, edition_key='' "
                 "WHERE rating_key=?", (pk, rk))
    rel = f"movie-{tid}/theme.mp3"
    p = themes / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(b"\0" * size_bytes)
    conn.execute(
        "INSERT INTO local_files (media_type, tmdb_id, section_id, edition_key,"
        " file_path, downloaded_at, source_video_id, source_kind)"
        " VALUES ('movie',?,'1','',?,?, 'vid', 'upload')", (tid, rel, TS))
    conn.execute(
        "INSERT INTO placements (media_type, tmdb_id, section_id, media_folder,"
        " edition_key, placed_at, placement_kind, plex_rating_key)"
        " VALUES ('movie',?,'1','','', ?, 'plex_upload', ?)", (tid, TS, rk))


def _themes(plex, selected_scheme: str):
    """One selected entry under `selected_scheme`, hash != canonical."""
    plex.get_themes.return_value = {"ok": True, "body": {"MediaContainer": {
        "size": 1, "Metadata": [
            {"ratingKey": f"{selected_scheme}://themes/{REENCODED_SHA}",
             "selected": True}]}}}


def _scan(tmp_path, tid, size, scheme):
    db = tmp_path / "m.db"
    themes = tmp_path / "themes"
    themes.mkdir()
    init_db(db)
    with sqlite3.connect(db) as conn:
        _seed(conn, themes, tid, size)
        conn.commit()
    plex = MagicMock()
    _themes(plex, scheme)
    return scan_plex_upload_placements(db, plex, themes_dir=themes)[0]


def test_over_ceiling_selected_upload_reads_ok(tmp_path):
    f = _scan(tmp_path, 9051, OVER, "upload")
    assert f["drift_type"] == "ok", f"{f['drift_type']}: {f['details']}"


def test_under_ceiling_diff_hash_still_missing(tmp_path):
    """The heuristic is gated on over-ceiling — a normal theme whose hash isn't
    present is still genuinely missing."""
    f = _scan(tmp_path, 9052, UNDER, "upload")
    assert f["drift_type"] == "motif_entry_missing", f["drift_type"]


def test_over_ceiling_but_metadata_selected_still_missing(tmp_path):
    """Over-ceiling but Plex selected its OWN metadata:// theme (no selected
    upload) → motif's theme really isn't serving → still flagged."""
    f = _scan(tmp_path, 9053, OVER, "metadata")
    assert f["drift_type"] == "motif_entry_missing", f["drift_type"]


def test_helper_and_branch_present():
    src = (Path(__file__).resolve().parent.parent
           / "app" / "core" / "orphan_scan.py").read_text()
    assert "def _over_ceiling(" in src
    assert "selected_upload_present" in src
