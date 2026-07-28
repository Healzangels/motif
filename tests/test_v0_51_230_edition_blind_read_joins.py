"""v0.51.230 — audit wave 3: the edition-blind placements→local_files read joins.

Five sites joined `placements` to `local_files` on (media_type, tmdb_id, section_id) only.
Both tables carry `edition_key` in their PRIMARY KEY (verified by PRAGMA, added by schema
v63), so without it EVERY placement fans across EVERY edition's local_files row — SUM and
COUNT multiply. This re-opens the v1.14.36 cartesian bug on the edition axis; the inline
comment on those lines still documented only the v1.14.36 SECTION-axis fix.

Measured on a 2-edition title with 2 copy placements of 100B each:
    edition-blind join -> 400 bytes / 4 rows      (2x inflated)
    edition-scoped     -> 200 bytes / 2 rows      (truth)

Affected surfaces: the dashboard STORAGE 'copies' KPI, /api/public/stats (the Homepage
widget), /api/storage/copies (whose whole purpose is telling the operator how many bytes
reorganising the share would reclaim — so the number they act on was wrong), plus two more.

Also: sql_missing_count joined local_files title-wide while the DOWNLOAD MISSING action
query it mirrors is section+edition scoped, so a title downloaded in the standard section
reported 0 missing on the 4K tab while the action would enqueue it.
"""
from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path

from app.core.db import init_db

REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()
NOW = "2026-07-27T00:00:00"


def test_both_tables_really_are_edition_keyed():
    """The premise. If edition_key ever leaves either PK the joins below are moot — and
    a future reader should be told that by a failing test, not by silence."""
    db = Path(tempfile.mkdtemp()) / "m.db"
    init_db(db)
    c = sqlite3.connect(db)
    for t in ("local_files", "placements"):
        pk = [r[1] for r in c.execute(f"PRAGMA table_info({t})") if r[5]]
        assert "edition_key" in pk, f"{t} PK lost edition_key: {pk}"


def test_every_placements_to_local_files_join_is_edition_scoped():
    """A bare (media_type, tmdb_id, section_id) join is one-to-MANY once a title has a
    second edition. Every such join must carry the edition predicate."""
    blind = API_PY.count("AND lf.section_id = p.section_id")
    scoped = API_PY.count("AND lf.edition_key = p.edition_key")
    assert blind == scoped, (
        f"{blind} placements->local_files joins but only {scoped} carry "
        "`AND lf.edition_key = p.edition_key` — the unscoped ones row-multiply")
    assert scoped >= 5, "expected at least the 5 known sites"


def test_the_multiplication_is_actually_gone():
    """Behavioral: reproduce the fan-out on a real DB and assert the scoped join returns
    the truth. A source-pin alone would not prove the predicate is doing anything."""
    db = Path(tempfile.mkdtemp()) / "m.db"
    init_db(db)
    c = sqlite3.connect(db)
    c.execute("PRAGMA foreign_keys=OFF")
    c.execute("INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source,"
              " last_seen_sync_at, first_seen_sync_at) "
              "VALUES (1,'movie',120,'Two Cuts','imdb',?,?)", (NOW, NOW))
    for edn in ("theatrical", "extended"):
        c.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, edition_key,"
                  " file_path, file_sha256, downloaded_at, source_video_id, file_size) "
                  "VALUES ('movie',120,'1',?,?,?,?,'v',100)",
                  (edn, f"m/{edn}.mp3", f"sha{edn}", NOW))
        c.execute("INSERT INTO placements (media_type, tmdb_id, section_id, media_folder,"
                  " edition_key, placement_kind, placed_at) "
                  "VALUES ('movie',120,'1',?,?,'copy',?)", (f"/data/{edn}", edn, NOW))
    c.commit()

    base = ("SELECT COALESCE(SUM(lf.file_size),0), COUNT(*) FROM placements p "
            "JOIN local_files lf ON lf.media_type=p.media_type "
            "AND lf.tmdb_id=p.tmdb_id AND lf.section_id=p.section_id ")
    blind_sum, blind_rows = c.execute(base + "WHERE p.placement_kind='copy'").fetchone()
    scoped_sum, scoped_rows = c.execute(
        base + "AND lf.edition_key=p.edition_key WHERE p.placement_kind='copy'").fetchone()

    assert (blind_sum, blind_rows) == (400, 4), "the fan-out premise must hold"
    assert (scoped_sum, scoped_rows) == (200, 2), (
        "the edition-scoped join must report the 2 real copies and their 200 real bytes")


def test_missing_count_matches_the_action_it_describes():
    """sql_missing_count is supposed to mirror DOWNLOAD MISSING (section+edition scoped
    since v1.11.0 / v1.23.65). Title-wide made the 4K tab read 0 missing for a title that
    was only downloaded in the standard section — while the button would enqueue it."""
    i = API_PY.index("sql_missing_count = f\"\"\"")
    block = API_PY[i:API_PY.index('"""', i + 30)]
    assert "AND lf.section_id = pi.section_id" in block
    assert "AND lf.edition_key = pi.edition_key" in block
