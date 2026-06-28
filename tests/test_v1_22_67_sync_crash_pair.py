"""v1.22.67 (audit round 2, Batch B #3) — two sync crash bugs.

(1) The failure handler could crash ITSELF. run_sync's outer
`except Exception as e:` contains an inner defensive
`except Exception as e:` (the fallback_reason detail_json read,
v1.17.1). Python DELETES the except variable when its clause exits —
so when the inner read failed (correlated with the same DB duress
that failed the sync), the later `isinstance(e, _JobCancelled)`
raised NameError: the REAL error was masked AND finish_progress never
ran, leaving the tdb_sync op_progress row 'running' forever (phantom
op pinning anyMutatingOpActive until container restart). Fix: the
inner variable is `_fr_e`.

(2) urls_match seeding crashed every sync under dual-edition
overrides. The synthetic INSERT...SELECT joined user_overrides where
two same-section rows differing only in edition_key both matched the
TDB URL → two IDENTICAL output rows (the INSERT writes
edition_key='') → PK IntegrityError → the whole sync failed,
repeatably on every run. Fix: SELECT DISTINCT + ON CONFLICT DO
NOTHING (which also makes the long-standing comment true).
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from app.core.db import init_db

REPO = Path(__file__).resolve().parent.parent
SYNC_PY = (REPO / "app" / "core" / "sync.py").read_text()
TS = "2026-06-11T00:00:00+00:00"


# ── (1) except-variable shadowing ────────────────────────────


def test_inner_except_no_longer_shadows_outer_e():
    """The inner defensive handler must not rebind `e` — Python's
    implicit `del` at clause exit unbound the outer exception."""
    i = SYNC_PY.index("fallback_reason detail_json read failed")
    region = SYNC_PY[i - 1200:i + 200]
    assert "except Exception as _fr_e:" in region, (
        "v1.22.67: the fallback-reason read guard must bind a private "
        "name; `as e` unbinds the outer handler's exception"
    )
    # The outer failure path still references e afterwards.
    after = SYNC_PY[i:i + 3000]
    assert "isinstance(e, _JobCancelled)" in after


def test_shadowing_semantics_demo():
    """Executable proof of the mechanism this guards against: an inner
    `except ... as e` deletes `e` from the scope when it fires."""
    def repro():
        try:
            raise ValueError("outer")
        except Exception as e:  # noqa: F841
            try:
                raise KeyError("inner")
            except Exception as e:  # noqa: F811
                pass
            return isinstance(e, ValueError)  # NameError pre-fix shape
    try:
        repro()
        raised = False
    except NameError:
        raised = True
    assert raised, "Python must delete the rebound except variable"


# ── (2) urls_match dual-edition PK collision ─────────────────


def _seed_dual_edition_matching_overrides(db):
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime,"
            " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
            " VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (TS, TS))
        conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title,"
            " upstream_source, last_seen_sync_at, first_seen_sync_at,"
            " youtube_url, youtube_video_id)"
            " VALUES ('movie', 600, 'W', 'themoviedb', ?, ?,"
            " 'https://yt/match', 'MATCH')", (TS, TS))
        for ek in ("", "Extended"):
            conn.execute(
                "INSERT INTO user_overrides (media_type, tmdb_id,"
                " youtube_url, set_at, section_id, edition_key)"
                " VALUES ('movie', 600, 'https://yt/match', ?, '1', ?)",
                (TS, ek))
        conn.commit()


def _run_synthetic_insert(db):
    """Execute the exact INSERT from sync.py source against the seeded
    DB (the statement is inline in run_sync; this extracts it verbatim
    so the test exercises the REAL SQL)."""
    anchor = SYNC_PY.index(
        "SELECT DISTINCT t.media_type, t.tmdb_id, uo.section_id,")
    i = SYNC_PY.rindex("INSERT INTO pending_updates (", 0, anchor)
    j = SYNC_PY.index("DO NOTHING", anchor) + len("DO NOTHING")
    sql = SYNC_PY[i:j]
    with sqlite3.connect(db) as conn:
        cur = conn.execute(sql, (TS,))
        conn.commit()
        return cur.rowcount


def test_dual_edition_matching_overrides_no_longer_crash(tmp_path):
    """The repro: '' + 'Extended' overrides in one section, both equal
    to the TDB URL → pre-fix IntegrityError (PK), now exactly ONE
    synthetic urls_match row."""
    db = tmp_path / "motif.db"
    init_db(db)
    _seed_dual_edition_matching_overrides(db)
    inserted = _run_synthetic_insert(db)
    assert inserted == 1
    with sqlite3.connect(db) as conn:
        rows = conn.execute(
            "SELECT section_id, edition_key, kind FROM pending_updates"
            " WHERE tmdb_id = 600").fetchall()
    assert rows == [("1", "", "urls_match")]


def test_rerun_is_idempotent(tmp_path):
    """A second sync's seeding pass must not crash or duplicate (the
    NOT EXISTS guard + ON CONFLICT both protect)."""
    db = tmp_path / "motif.db"
    init_db(db)
    _seed_dual_edition_matching_overrides(db)
    _run_synthetic_insert(db)
    again = _run_synthetic_insert(db)
    assert again == 0
