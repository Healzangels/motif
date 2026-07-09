"""v0.51.106 — folder-liveness mount-fault signal on verify_placement_health.

Code-review finding #1 (of the v0.51.101 section-scoping change): the mount-fault
cap is an ABSOLUTE floor — `cap = max(50, total // 4)`. v0.51.101 made the pass
runnable against a SINGLE scoped section. A /data blip that reads every sidecar of
a small (say 30-row) section missing has suspect=30 < the 50 floor, so the guard
never fires and the whole section false-stamps broken (red PL / NEEDS WORK).

A count/ratio gate can't fix this: a small section that all-reads-missing on a
mount blip is IDENTICAL by count to a small library where those themes are
genuinely broken — and v1.23.30's contract is that genuine breakage MUST surface
(test_small_missing_set_stamps_broken_normally). The distinguisher is folder
liveness: a mount fault takes the CONTAINING folder down too (is_dir()→False),
whereas a genuine theme.mp3 deletion leaves the folder alive. v0.51.106 trips the
mount-fault guard (even below the absolute floor) only when ~all of the examined
set read missing-AND-folder-gone. verify_canonical_health is untouched — its
themes_dir root probe is already section-count-independent.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from app.core.db import init_db
from app.core.plex_enum import verify_placement_health

NOW = "2026-07-09T00:00:00"


def _seed(conn, *, tmdb, media_folder, kind="hardlink"):
    if not conn.execute(
            "SELECT 1 FROM plex_sections WHERE section_id='1'").fetchone():
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime, is_4k,"
            " themes_subdir, included, discovered_at, last_seen_at)"
            " VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
    conn.execute(
        "INSERT INTO themes (media_type, tmdb_id, title, upstream_source,"
        " last_seen_sync_at, first_seen_sync_at) VALUES ('movie', ?, ?, 'imdb', ?, ?)",
        (tmdb, f"T{tmdb}", NOW, NOW))
    conn.execute(
        "INSERT INTO placements (media_type, tmdb_id, section_id, media_folder,"
        " placed_at, placement_kind, provenance, edition_key)"
        " VALUES ('movie', ?, '1', ?, ?, ?, 'auto', '')",
        (tmdb, media_folder, NOW, kind))


def _present_values(db):
    with sqlite3.connect(db) as conn:
        return dict(conn.execute(
            "SELECT tmdb_id, theme_present FROM placements").fetchall())


def test_small_section_dead_folders_treated_as_mount_fault(tmp_path):
    # 6 sidecar placements whose media_folder does NOT exist (the /data mount is
    # gone → is_file()=False AND is_dir()=False). 6 < the absolute cap floor
    # (max(50, 6//4)=50), so ONLY the folder-liveness signal can catch it.
    db = tmp_path / "m.db"
    init_db(db)
    with sqlite3.connect(db) as conn:
        for i in range(6):
            # never created on disk → folder is gone
            _seed(conn, tmdb=i + 1, media_folder=str(tmp_path / "gone" / f"m{i}"))
        conn.commit()

    res = verify_placement_health(db)
    assert res["missing"] == 0, "dead-folder mass-missing = mount fault, skip stamps"
    assert all(v is None for v in _present_values(db).values())


def test_small_section_live_folders_missing_file_still_stamps_broken(tmp_path):
    # Same small size (6), but the folders EXIST with no theme.mp3 — a genuine
    # per-file breakage, not a mount fault. The folder-liveness clause must NOT
    # fire (folder_gone=0), so v1.23.30's stamp-broken contract still holds even
    # at this small, fraction-relevant size.
    db = tmp_path / "m.db"
    init_db(db)
    with sqlite3.connect(db) as conn:
        for i in range(6):
            d = tmp_path / f"live{i}"
            d.mkdir()  # folder exists; no theme.mp3 inside
            _seed(conn, tmdb=i + 1, media_folder=str(d))
        conn.commit()

    res = verify_placement_health(db)
    assert res["missing"] == 6, "live-folder missing files are genuine breakage"
    assert all(v == 0 for v in _present_values(db).values())


def test_few_dead_folders_among_healthy_still_surface(tmp_path):
    # A HEALTHY library (folders alive, themes present) with a couple of genuine
    # folder removals (dead folders). folder_gone is a small fraction of the
    # examined set → the mount-fault clause must NOT fire → the removed rows
    # still stamp broken (feeds the v1.24.26 auto-restore).
    db = tmp_path / "m.db"
    init_db(db)
    with sqlite3.connect(db) as conn:
        for i in range(8):  # present, folders alive
            d = tmp_path / f"ok{i}"
            d.mkdir()
            (d / "theme.mp3").write_bytes(b"x")
            _seed(conn, tmdb=i + 1, media_folder=str(d))
        # 2 genuine folder removals (dead folders)
        _seed(conn, tmdb=101, media_folder=str(tmp_path / "removed" / "a"))
        _seed(conn, tmdb=102, media_folder=str(tmp_path / "removed" / "b"))
        conn.commit()

    res = verify_placement_health(db)
    assert res["missing"] == 2, "a few dead folders in a healthy library are real"
    vals = _present_values(db)
    assert vals[101] == 0 and vals[102] == 0     # removed, stamped broken
    assert vals[1] == 1                          # healthy, stamped present
