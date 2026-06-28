"""v1.24.5 — code-review follow-ups on the v1.24.3/v1.24.4 orphan-scan arc.

#1 scan_one_placement: folder_path now comes from the SAME rk we probe
   (placement_rk's own plex_items folder), not the edition-resolved rk_row. For a
   MISLABELED placement (edition_key='' but plex_rating_key points at a named
   edition) the two diverge — pre-fix the orphan_sidecar check + DELETE SIDECAR
   target stated the wrong-edition folder (the live Amadeus symptom).

#2 rekey_mislabeled_placements: dedup intra-batch target PKs into conflicts so two
   re-keys landing on the same final PK can't collide on the second UPDATE and
   abort the whole transaction.
"""
from __future__ import annotations

import hashlib
import sqlite3
from pathlib import Path
from unittest.mock import MagicMock

from test_v1_14_59_recovery_options_behavioral import (  # noqa: F401
    _seed_plex_item, _seed_section, _seed_theme)

from app.core.db import init_db
from app.core.orphan_scan import (
    rekey_mislabeled_placements, scan_plex_upload_placements)

TS = "2026-06-20T00:00:00+00:00"
CANON = b"ID3-motif-canonical"
CANON_SHA = hashlib.sha1(CANON).hexdigest()


def _themed_get(plex):
    plex.get_themes.return_value = {
        "ok": True,
        "body": {"MediaContainer": {"size": 1, "Metadata": [
            {"ratingKey": f"upload://themes/{CANON_SHA}", "selected": True}]}},
    }


def _seed_canonical(conn, themes_dir: Path, mt, tid, sec, ek):
    rel = f"{mt}-{tid}/theme.mp3"
    p = themes_dir / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(CANON)
    conn.execute(
        "INSERT INTO local_files (media_type, tmdb_id, section_id, edition_key,"
        " file_path, downloaded_at, source_video_id, source_kind)"
        " VALUES (?,?,?,?,?,?, 'vid', 'upload')",
        (mt, tid, sec, ek, rel, TS))


def _placement(conn, tmdb, ek, rk):
    conn.execute(
        "INSERT INTO placements (media_type, tmdb_id, section_id, media_folder,"
        " edition_key, placed_at, placement_kind, plex_rating_key)"
        " VALUES ('movie', ?, '1', '', ?, ?, 'plex_upload', ?)",
        (tmdb, ek, TS, rk))


# ── #1: orphan_sidecar check follows the placement's rk, not the '' edition ──

def test_sidecar_check_uses_placement_rk_folder_not_resolved_edition(tmp_path):
    db = tmp_path / "m.db"
    themes = tmp_path / "themes"; themes.mkdir()
    std_folder = tmp_path / "Amadeus (1984)"
    dc_folder = tmp_path / "Amadeus (1984) {edition-Director's Cut}"
    std_folder.mkdir(); dc_folder.mkdir()
    # the standard edition's OWN legit sidecar lives in its folder; the Director's
    # Cut folder (the placement's real rk) has none.
    (std_folder / "theme.mp3").write_bytes(b"standard sidecar")
    init_db(db)
    with sqlite3.connect(db) as conn:
        _seed_section(conn, "1")
        _seed_theme(conn, tmdb_id=279, title="Amadeus")
        tid_pk = conn.execute(
            "SELECT id FROM themes WHERE tmdb_id=279").fetchone()[0]
        for ek, rk, fld in (("", "rk-std", std_folder),
                            ("director s cut", "rk-dc", dc_folder)):
            _seed_plex_item(conn, rating_key=rk, section_id="1", tmdb_id=279)
            conn.execute(
                "UPDATE plex_items SET theme_id=?, edition_key=?, folder_path=? "
                "WHERE rating_key=?", (tid_pk, ek, str(fld), rk))
        _seed_canonical(conn, themes, "movie", 279, "1", "")
        # MISLABELED: placement keyed '' but plex_rating_key is the Director's Cut.
        _placement(conn, 279, "", "rk-dc")
        conn.commit()
    plex = MagicMock(); _themed_get(plex)
    f = scan_plex_upload_placements(db, plex, themes_dir=themes)[0]
    assert f["rk"] == "rk-dc"
    # pre-fix folder_path resolved from the '' edition (std folder) → it found the
    # standard's sidecar → orphan_sidecar_on_disk. Post-fix it checks the DC
    # folder (the rk we probe), which has none → ok.
    assert f["drift_type"] == "ok", (
        f"sidecar check must follow placement_rk's folder; got "
        f"{f['drift_type']}: {f.get('details')}")


# ── #2: intra-batch target-PK collision is deduped into conflicts ──

def test_intra_batch_target_collision_deduped(tmp_path):
    db = tmp_path / "m.db"; init_db(db)
    with sqlite3.connect(db) as conn:
        _seed_section(conn, "1")
        _seed_theme(conn, tmdb_id=300, title="Z")
        tid_pk = conn.execute(
            "SELECT id FROM themes WHERE tmdb_id=300").fetchone()[0]
        # two plex_items rows BOTH tagged edition 'c' (different rks).
        for rk in ("rk-A", "rk-B"):
            _seed_plex_item(conn, rating_key=rk, section_id="1", tmdb_id=300)
            conn.execute(
                "UPDATE plex_items SET theme_id=?, edition_key='c' "
                "WHERE rating_key=?", (tid_pk, rk))
        # two placements (editions 'a','b') whose rks both resolve to 'c' →
        # both want to re-key to the SAME final PK (movie,300,'1','','c').
        _placement(conn, 300, "a", "rk-A")
        _placement(conn, 300, "b", "rk-B")
        conn.commit()
    res = rekey_mislabeled_placements(db, dry_run=False)
    assert res["rekeyed_count"] == 1, "only one re-key can land on the target PK"
    assert res["conflict_count"] == 1, "the colliding sibling → conflict"
    with sqlite3.connect(db) as conn:
        eks = sorted(r[0] for r in conn.execute(
            "SELECT edition_key FROM placements WHERE tmdb_id=300"))
    assert eks.count("c") == 1 and len(eks) == 2, (
        f"exactly one re-keyed to 'c', the other untouched; got {eks}")


def test_review_fix_markers_present():
    src = (Path(__file__).resolve().parent.parent
           / "app" / "core" / "orphan_scan.py").read_text()
    api = (Path(__file__).resolve().parent.parent
           / "app" / "web" / "api.py").read_text()
    # #1 folder from placement_rk; #5 media_folder invariant; #2 dedup; #3 log after txn
    assert "placement_rk_folder" in src
    assert "AND p.media_folder = ''" in src
    assert "seen_targets" in src
    # #4 endpoint error handling
    assert "placement PK collision during apply" in api
