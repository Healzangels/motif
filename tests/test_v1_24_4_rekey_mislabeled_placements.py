"""v1.24.4 — backfill: re-key mislabeled plex_upload placements to their rk's edition.

Companion to v1.24.3. The ''-as-falsy place-path bug wrote plex_upload placements
keyed edition_key='' whose plex_rating_key targets a NAMED edition (Amadeus Director's
Cut etc.). rekey_mislabeled_placements re-labels each to the edition its rk genuinely
belongs to (per plex_items) — tracking-only, dry-run by default, PK-safe.
"""
from __future__ import annotations
from _slice_helpers import slice_to_next

import sqlite3
from pathlib import Path

from test_v1_14_59_recovery_options_behavioral import (  # noqa: F401
    _seed_plex_item, _seed_section, _seed_theme)

from app.core.db import init_db
from app.core.orphan_scan import rekey_mislabeled_placements

REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()
TS = "2026-06-20T00:00:00+00:00"


def _placement(conn, tmdb, ek, rk, *, sec="1", folder=""):
    conn.execute(
        "INSERT INTO placements (media_type, tmdb_id, section_id, media_folder,"
        " edition_key, placed_at, placement_kind, plex_rating_key)"
        " VALUES ('movie', ?, ?, ?, ?, ?, 'plex_upload', ?)",
        (tmdb, sec, folder, ek, TS, rk))


def _seed_two_editions(conn, tmdb):
    _seed_section(conn, "1")
    _seed_theme(conn, tmdb_id=tmdb, title="X")
    tid_pk = conn.execute(
        "SELECT id FROM themes WHERE tmdb_id=?", (tmdb,)).fetchone()[0]
    for ek, rk in (("", "rk-std"), ("director s cut", "rk-dc")):
        _seed_plex_item(conn, rating_key=rk, section_id="1", tmdb_id=tmdb)
        conn.execute(
            "UPDATE plex_items SET theme_id=?, edition_key=? WHERE rating_key=?",
            (tid_pk, ek, rk))


def test_mislabeled_placement_rekeyed(tmp_path):
    db = tmp_path / "m.db"; init_db(db)
    with sqlite3.connect(db) as conn:
        _seed_two_editions(conn, 279)
        _placement(conn, 279, "", "rk-dc")  # ed='' but rk is the Director's Cut
        conn.commit()
    # dry-run: plan only, no write.
    plan = rekey_mislabeled_placements(db, dry_run=True)
    assert plan["rekeyed_count"] == 1
    assert plan["rekeyed"][0]["from_edition"] == ""
    assert plan["rekeyed"][0]["to_edition"] == "director s cut"
    with sqlite3.connect(db) as conn:
        assert conn.execute(
            "SELECT edition_key FROM placements WHERE tmdb_id=279"
        ).fetchone()[0] == "", "dry-run must not write"
    # apply: the placement is re-keyed.
    res = rekey_mislabeled_placements(db, dry_run=False)
    assert res["rekeyed_count"] == 1
    with sqlite3.connect(db) as conn:
        assert conn.execute(
            "SELECT edition_key FROM placements WHERE tmdb_id=279"
        ).fetchone()[0] == "director s cut"


def test_correctly_labeled_unchanged(tmp_path):
    db = tmp_path / "m.db"; init_db(db)
    with sqlite3.connect(db) as conn:
        _seed_two_editions(conn, 279)
        _placement(conn, 279, "director s cut", "rk-dc")  # already correct
        conn.commit()
    res = rekey_mislabeled_placements(db, dry_run=False)
    assert res["rekeyed_count"] == 0 and res["conflict_count"] == 0


def test_pk_conflict_skipped(tmp_path):
    # both the '' placement (rk-dc) AND a real 'director s cut' placement exist;
    # re-keying the '' one would collide → reported as conflict, NOT applied.
    db = tmp_path / "m.db"; init_db(db)
    with sqlite3.connect(db) as conn:
        _seed_two_editions(conn, 279)
        _placement(conn, 279, "", "rk-dc")                # mislabeled
        _placement(conn, 279, "director s cut", "rk-dc")  # target slot taken
        conn.commit()
    res = rekey_mislabeled_placements(db, dry_run=False)
    assert res["rekeyed_count"] == 0
    assert res["conflict_count"] == 1
    with sqlite3.connect(db) as conn:
        eks = {r[0] for r in conn.execute(
            "SELECT edition_key FROM placements WHERE tmdb_id=279")}
    assert eks == {"", "director s cut"}, "untouched on conflict"


def test_rk_not_in_plex_items_unresolved(tmp_path):
    db = tmp_path / "m.db"; init_db(db)
    with sqlite3.connect(db) as conn:
        _seed_section(conn, "1")
        _seed_theme(conn, tmdb_id=500, title="Y")
        _placement(conn, 500, "", "rk-ghost")  # rk not in plex_items
        conn.commit()
    res = rekey_mislabeled_placements(db, dry_run=False)
    assert res["rekeyed_count"] == 0
    assert res["rk_unresolved_count"] == 1
    with sqlite3.connect(db) as conn:
        assert conn.execute(
            "SELECT edition_key FROM placements WHERE tmdb_id=500"
        ).fetchone()[0] == "", "unresolvable rk must not be touched"


def test_endpoint_wired_dry_run_default():
    blk = slice_to_next(API_PY, '@app.post("/api/admin/placements/rekey-mislabeled")',
                       "@app.post(", "@app.get(", "@app.put(", "@app.delete(")
    assert "apply: bool = Query(False)" in blk
    assert "rekey_mislabeled_placements(db, dry_run=not apply)" in blk
    assert "run_in_threadpool" in blk
