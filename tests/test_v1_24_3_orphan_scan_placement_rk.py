"""v1.24.3 — orphan scan probes the placement's own plex_rating_key.

the user's prod orphan scan flagged 7 plex_upload rows (motif_entry_missing /
no_plex_entries / rk_lookup_failed) that RE-PUSH never cleared. Root cause: the
titles are MULTI-EDITION (Amadeus +Director's Cut, Star Wars ×4 cuts, Avatar
+Extended, LotR/Hobbit Theatrical+Extended) and the plex_upload placement is keyed
edition_key='' while its plex_rating_key targets a NAMED edition's rk (verified
serving, ok=1). scan_one_placement RE-RESOLVED the rk from plex_items by the
placement's '' edition_key — probing the wrong edition, or none in that section
(→ rk_lookup_failed) — instead of the rk the placement actually used.

Fix: probe placements.plex_rating_key directly; the plex_items resolution stays as
the folder_path source + the legacy fallback for placements with no stored rk.
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

REPO = Path(__file__).resolve().parent.parent
ORPHAN_PY = (REPO / "app" / "core" / "orphan_scan.py").read_text()
TS = "2026-06-20T00:00:00+00:00"
CANON_BYTES = b"ID3-motif-canonical"
CANON_SHA = hashlib.sha1(CANON_BYTES).hexdigest()


def _themed_get(plex):
    """plex.get_themes mock: one selected entry carrying motif's sha (mirrors
    Plex's upload://themes/<sha1> content-dedup)."""
    plex.get_themes.return_value = {
        "ok": True,
        "body": {"MediaContainer": {"size": 1, "Metadata": [
            {"ratingKey": f"upload://themes/{CANON_SHA}", "selected": True}]}},
    }


def _seed_canonical(conn, themes_dir: Path, mt, tid, sec, ek):
    rel = f"{mt}-{tid}/theme.mp3"
    p = themes_dir / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(CANON_BYTES)
    conn.execute(
        "INSERT INTO local_files (media_type, tmdb_id, section_id, edition_key,"
        " file_path, downloaded_at, source_video_id, source_kind)"
        " VALUES (?,?,?,?,?,?, 'vid', 'upload')",
        (mt, tid, sec, ek, rel, TS))


def test_placement_rk_probed_not_reresolved_edition(tmp_path):
    """The '' placement targets the Director's-Cut rk; the scan must probe THAT
    (where motif uploaded), not re-resolve the literal '' plex_items row."""
    db = tmp_path / "m.db"
    themes = tmp_path / "themes"; themes.mkdir()
    init_db(db)
    with sqlite3.connect(db) as conn:
        _seed_section(conn, "1")
        _seed_theme(conn, tmdb_id=279, title="Amadeus")
        tid_pk = conn.execute(
            "SELECT id FROM themes WHERE tmdb_id=279").fetchone()[0]
        for ek, rk in (("", "rk-std"), ("director s cut", "rk-dc")):
            _seed_plex_item(conn, rating_key=rk, section_id="1", tmdb_id=279)
            conn.execute(
                "UPDATE plex_items SET theme_id=?, edition_key=? "
                "WHERE rating_key=?", (tid_pk, ek, rk))
        _seed_canonical(conn, themes, "movie", 279, "1", "")
        # the mis-keyed placement: ed='' but rk points at the Director's Cut.
        conn.execute(
            "INSERT INTO placements (media_type, tmdb_id, section_id,"
            " media_folder, edition_key, placed_at, placement_kind,"
            " plex_rating_key)"
            " VALUES ('movie',279,'1','','', ?, 'plex_upload', 'rk-dc')", (TS,))
        conn.commit()
    plex = MagicMock(); _themed_get(plex)
    findings = scan_plex_upload_placements(db, plex, themes_dir=themes)
    assert len(findings) == 1
    f = findings[0]
    assert f["rk"] == "rk-dc", (
        "must probe the placement's stored rk, not the '' edition")
    assert f["drift_type"] == "ok", (
        f"expected ok, got {f['drift_type']}: {f['details']}")
    # it probed rk-dc and ONLY rk-dc (never the re-resolved rk-std).
    called = {c.kwargs.get("rating_key") for c in plex.get_themes.call_args_list}
    assert called == {"rk-dc"}


def test_rk_lookup_failed_rescued_when_placement_has_rk(tmp_path):
    """LotR/Hobbit case: no '' edition in this section, but the placement stores
    the Theatrical rk → probe it instead of returning rk_lookup_failed."""
    db = tmp_path / "m.db"
    themes = tmp_path / "themes"; themes.mkdir()
    init_db(db)
    with sqlite3.connect(db) as conn:
        _seed_section(conn, "1")
        _seed_theme(conn, tmdb_id=121, title="LotR Two Towers")
        tid_pk = conn.execute(
            "SELECT id FROM themes WHERE tmdb_id=121").fetchone()[0]
        # only named editions exist in section 1 — NO '' row.
        for ek, rk in (("theatrical", "rk-theat"), ("extended edition", "rk-ext")):
            _seed_plex_item(conn, rating_key=rk, section_id="1", tmdb_id=121)
            conn.execute(
                "UPDATE plex_items SET theme_id=?, edition_key=? "
                "WHERE rating_key=?", (tid_pk, ek, rk))
        _seed_canonical(conn, themes, "movie", 121, "1", "")
        conn.execute(
            "INSERT INTO placements (media_type, tmdb_id, section_id,"
            " media_folder, edition_key, placed_at, placement_kind,"
            " plex_rating_key)"
            " VALUES ('movie',121,'1','','', ?, 'plex_upload', 'rk-theat')", (TS,))
        conn.commit()
    plex = MagicMock(); _themed_get(plex)
    f = scan_plex_upload_placements(db, plex, themes_dir=themes)[0]
    assert f["rk"] == "rk-theat"
    assert f["drift_type"] == "ok", f"got {f['drift_type']}: {f['details']}"


def test_genuine_rk_lookup_failed_still_fires(tmp_path):
    """No stored rk AND no resolvable plex_items row → still rk_lookup_failed
    (guard against over-correction)."""
    db = tmp_path / "m.db"
    init_db(db)
    with sqlite3.connect(db) as conn:
        _seed_section(conn, "1")
        _seed_theme(conn, tmdb_id=999, title="Ghost")
        conn.execute(
            "INSERT INTO placements (media_type, tmdb_id, section_id,"
            " media_folder, edition_key, placed_at, placement_kind)"
            " VALUES ('movie',999,'1','','', ?, 'plex_upload')", (TS,))
        conn.commit()
    plex = MagicMock()
    f = scan_plex_upload_placements(db, plex)[0]
    assert f["drift_type"] == "rk_lookup_failed"
    plex.get_themes.assert_not_called()


def test_scan_reads_placement_rating_key():
    # v1.24.5 widened the lookup to a LEFT JOIN that also pulls the rk's folder.
    assert "SELECT p.plex_rating_key, pi.folder_path AS prk_folder" in ORPHAN_PY
    assert "rk = placement_rk or rk_row[\"rating_key\"]" in ORPHAN_PY
    assert "v1.24.3" in ORPHAN_PY
