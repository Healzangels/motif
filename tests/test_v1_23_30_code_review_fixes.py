"""v1.23.30 — code-review fixes for the v1.23.23–.29 session diff.

#1 (HIGH): verify_placement_health mass-stamped theme_present=0 on a /data
   mount outage — Path.is_file() returns False (NOT OSError) on ENOENT/ENOTDIR,
   so the per-row OSError skip never fired and the v1.23.26 cap guarded only the
   prune DELETE, leaving the 0-stamp UPDATE unbounded. Now the cap bounds the
   missing-stamps AND the prune together; confirmed-present stamps still apply.
#2: the no-change sync title's catalog "(N checked)" fallback now gates on
   sync_stats.errors==0 (an errored empty sync no longer claims a clean verify).
#3: the attention sort's broken branch gates on plex_independent_theme=0 (an LPS
   row with a stale missing-sidecar no longer hoists to the top of NEEDS WORK).
#5: the worker superseded-sidecar DELETE is scoped to the folder it just
   unlinked (media_folder = folder_path), so it can't drop a record for a file
   it didn't remove.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from app.core.db import init_db
from app.core.events import now_iso
from app.core.plex_enum import verify_placement_health


REPO = Path(__file__).resolve().parent.parent
WORKER = (REPO / "app" / "core" / "worker.py").read_text()
API = (REPO / "app" / "web" / "api.py").read_text()


def _seed_missing_hardlinks(db: Path, tmp: Path, n: int, start: int = 0):
    """n hardlink placements whose media_folder exists but has NO theme.mp3
    (reads 'missing'), with NO plex_upload sibling → each would stamp 0."""
    now = now_iso()
    with sqlite3.connect(db) as conn:
        for i in range(start, start + n):
            d = tmp / f"miss{i}"
            d.mkdir(parents=True, exist_ok=True)
            conn.execute(
                "INSERT OR IGNORE INTO themes (media_type, tmdb_id, title, "
                "  title_norm, year, upstream_source, last_seen_sync_at, "
                "  first_seen_sync_at) VALUES ('movie', ?, 'T','t','2020','imdb',?,?)",
                (i, now, now))
            conn.execute(
                "INSERT INTO placements (media_type, tmdb_id, section_id, "
                "  edition_key, media_folder, placed_at, placement_kind, "
                "  provenance) VALUES ('movie', ?, '1', '', ?, ?, 'hardlink', 'auto')",
                (i, str(d), now))
        conn.commit()


def _present(db: Path, tmp: Path, tid: int):
    now = now_iso()
    d = tmp / f"ok{tid}"
    d.mkdir(parents=True, exist_ok=True)
    (d / "theme.mp3").write_bytes(b"x")
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT OR IGNORE INTO themes (media_type, tmdb_id, title, title_norm, "
            "  year, upstream_source, last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('movie', ?, 'T','t','2020','imdb',?,?)", (tid, now, now))
        conn.execute(
            "INSERT INTO placements (media_type, tmdb_id, section_id, edition_key, "
            "  media_folder, placed_at, placement_kind, provenance) "
            "VALUES ('movie', ?, '1', '', ?, ?, 'hardlink', 'auto')",
            (tid, str(d), now))
        conn.commit()


def _theme_present_values(db: Path):
    with sqlite3.connect(db) as conn:
        return [r[0] for r in conn.execute("SELECT theme_present FROM placements")]


def test_small_missing_set_stamps_broken_normally(tmp_path):
    db = tmp_path / "m.db"
    init_db(db)
    _seed_missing_hardlinks(db, tmp_path, 10)  # 10 < cap(50)
    res = verify_placement_health(db)
    assert res["missing"] == 10
    assert all(v == 0 for v in _theme_present_values(db))  # genuinely stamped


def test_mass_missing_skips_zero_stamps_and_prune(tmp_path):
    # #1: a mount-fault-shaped mass-missing read must NOT flip everything broken.
    db = tmp_path / "m.db"
    init_db(db)
    _seed_missing_hardlinks(db, tmp_path, 60)  # 60 > cap(50); suspect=60
    res = verify_placement_health(db)
    assert res["missing"] == 0, "mass-missing must be treated as a mount fault"
    # nothing stamped theme_present=0 — all preserved (NULL).
    assert all(v is None for v in _theme_present_values(db))


def test_present_stamps_apply_even_under_mass_missing(tmp_path):
    # confirmed-present rows are always safe to stamp, even when the cap trips.
    db = tmp_path / "m.db"
    init_db(db)
    _seed_missing_hardlinks(db, tmp_path, 60)
    _present(db, tmp_path, 9001)
    res = verify_placement_health(db)
    assert res["missing"] == 0
    with sqlite3.connect(db) as conn:
        tp = conn.execute(
            "SELECT theme_present FROM placements WHERE tmdb_id=9001").fetchone()[0]
    assert tp == 1, "a confirmed-present sidecar must still stamp 1"


# ── source-pins (matching each area's existing convention) ───────


def test_no_change_title_skips_checked_count_on_errors():
    i = WORKER.index('if new_count == 0 and upd_count == 0:')
    block = WORKER[i:WORKER.index('event_kind="sync_completed"', i)]
    # v1.23.79: the catalog count now always applies on a no-change sync (no
    # more total_seen two-step), but it still skips on errors.
    assert "if not sync_stats.errors:" in block


def test_attention_broken_branch_gates_on_lps():
    i = API.index('"attention": (')
    # v1.24.41: 1200→1800 — the bucket-0 broken branch gained a multi-line
    # rk-liveness gate comment, pushing the WHEN further into the CASE.
    block = API[i:i + 1800]
    j = block.index("COALESCE(p_e.theme_present, p_g.theme_present) = 0")
    # the indep gate must accompany the broken branch (within the same WHEN).
    assert "plex_independent_theme" in block[j:j + 160]


def test_worker_superseded_delete_scoped_to_folder():
    # anchor on the unique except-log just below the scoped DELETE block.
    # v1.23.31: the DELETE moved into delete_superseded_sidecar_placement; the
    # folder scoping is now the folder_path arg passed as media_folder.
    j = WORKER.index("superseded-record cleanup")
    block = WORKER[j - 1600:j]
    assert "if folder_path:" in block                       # gated on a folder
    assert "folder_path, _coll_edition_key)" in block       # scoped to it
