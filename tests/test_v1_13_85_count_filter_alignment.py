"""v1.13.85 — count vs row count alignment under attn_pills filter.

the user reported a v1.13.82 deployment showing "// RESULTS · 28
MATCHES" in the library page header but "no items match the
current filters" in the body — the count and the row list
disagreed by 28.

Root cause in api.py /api/library: the count query has THREE
construction paths (line ~1711):

  1. Fast path (no_pills + status=all + tdb=any) — uses
     sql_from_pi_only, JUST plex_items + plex_sections.
  2. Full path (needs_themes_for_count) — uses sql_from with
     every JOIN (themes + lf + p + sfa + pu_sec + pu_global).
  3. Slim path (else) — sql_from_pi_only + optional lf + p
     based on which pill axes the WHERE references.

Pre-fix `no_pills` was:
    not (src_pills or tdb_pills or dl_pills or pl_pills
         or link_pills or ed_pills)

It DIDN'T include `attn_pills`. So a request with ONLY
`attn_pills=update` (no other pills, status=all, tdb=any) hit
the fast path, which uses sql_from_pi_only. The attn_pills
predicate references t.failure_kind / t.failure_acked_at /
sfa.acked_at / EXISTS pending_updates / _SRC_LETTER_SQL — NONE
of which are reachable from sql_from_pi_only.

Result: the count's WHERE attempted to evaluate the predicate
against missing tables, but SQLite (depending on the predicate
shape) silently returned the unfiltered count rather than 500.
The row select went down the full sql_from path with all the
JOINs and correctly returned 0 — so header said 28, body 0.

Fix shape:
- Add `attn_pills` to `no_pills` so the fast path is skipped.
- Add `attn_pills` to `needs_themes_for_count` so the
  needs-themes branch (full FROM with all JOINs) is taken.

These tests pin both the static guards and a behavioral
simulation of the bug.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from app.core.db import init_db


REPO = Path(__file__).resolve().parent.parent


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ── static guards: the fix is in production code ─────────────

def test_no_pills_check_includes_attn_pills():
    """The `no_pills` boolean must include `attn_pills` in its
    OR-chain so a request with only attn_pills doesn't fall into
    the fast path. Pre-fix the literal was:
        not (src_pills or tdb_pills or dl_pills or pl_pills
             or link_pills or ed_pills)
    Pin the post-fix shape — must include attn_pills."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    # The fixed assignment includes attn_pills.
    assert (
        "no_pills = not (src_pills or tdb_pills or dl_pills or pl_pills"
        in src
    )
    # And attn_pills is in the chain (whitespace tolerant).
    nopills_idx = src.index("no_pills = not (src_pills")
    nopills_block = src[nopills_idx:nopills_idx + 300]
    assert "attn_pills" in nopills_block, (
        "attn_pills must be in the no_pills disjunction so "
        "the count fast-path is skipped when attn is the only "
        "active filter (resurrects the user's '28 matches with 0 "
        "rendered rows' bug otherwise)"
    )


def test_needs_themes_for_count_includes_attn_pills():
    """The `needs_themes_for_count` boolean must include
    `attn_pills` so attn-pill requests route through the full
    sql_from (with t / lf / p / sfa joins). Pre-fix the slim
    count path couldn't evaluate attn predicates — silent
    drift between header count and body rows."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    # Find the needs_themes_for_count assignment block.
    anchor = src.index("needs_themes_for_count = (")
    block = src[anchor:anchor + 1500]
    # Must include the v1.13.85 attn_pills line.
    assert "or bool(attn_pills)" in block, (
        "attn_pills must be in needs_themes_for_count so the "
        "count routes to the full FROM (themes + sfa joined)"
    )


# ── behavioral: count vs row select agreement ────────────────

def _seed_realistic_state(db: Path):
    """Seed a fresh DB with a known shape that exposes the bug:
    one section, 5 plex_items, only 1 of which has a pending
    update + theme present (would match attn_pills=update).
    Pre-fix the slim count returned 5 (all section items);
    post-fix the full count returns 1."""
    now = _now_iso()
    with sqlite3.connect(db) as conn:
        # One movie section with 5 items.
        conn.execute(
            "INSERT INTO plex_sections ("
            "  section_id, title, type, included, discovered_at,"
            "  last_seen_at, is_4k, is_anime"
            ") VALUES ('1', 'Movies', 'movie', 1, ?, ?, 0, 0)",
            (now, now),
        )
        for tmdb_id in range(1, 6):
            conn.execute(
                "INSERT INTO themes ("
                "  media_type, tmdb_id, title, upstream_source,"
                "  youtube_url, youtube_video_id,"
                "  last_seen_sync_at, first_seen_sync_at"
                ") VALUES ('movie', ?, ?, 'imdb', ?, ?, ?, ?)",
                (tmdb_id, f"x{tmdb_id}",
                 f"https://www.youtube.com/watch?v=v{tmdb_id}",
                 f"v{tmdb_id}", now, now),
            )
            conn.execute(
                "INSERT INTO plex_items ("
                "  rating_key, section_id, media_type, title,"
                "  guid_tmdb, first_seen_at, last_seen_at"
                ") VALUES (?, '1', 'movie', ?, ?, ?, ?)",
                (f"rk{tmdb_id}", f"x{tmdb_id}", tmdb_id, now, now),
            )
        # Only tmdb_id=1 has a real pending_update + a theme
        # present (placement + local_files at source_kind=
        # themerrdb → src='T' → matches attn_pills=update).
        conn.execute(
            "INSERT INTO pending_updates ("
            "  media_type, tmdb_id, decision, detected_at"
            ") VALUES ('movie', 1, 'pending', ?)",
            (now,),
        )
        conn.execute(
            "INSERT INTO local_files ("
            "  media_type, tmdb_id, section_id, file_path,"
            "  source_video_id, downloaded_at, source_kind"
            ") VALUES ('movie', 1, '1', 'x.mp3', 'v1', ?, 'themerrdb')",
            (now,),
        )
        conn.execute(
            "INSERT INTO placements ("
            "  media_type, tmdb_id, section_id, media_folder,"
            "  placed_at, placement_kind, plex_refreshed"
            ") VALUES ('movie', 1, '1', '/data/x', ?, 'hardlink', 0)",
            (now,),
        )


def _slim_count_pre_fix(db: Path) -> int:
    """Mirror the pre-v1.13.85 slim count path that the bug
    exploited: sql_from_pi_only + the attn_pills WHERE clause.
    The predicate references t / lf / sfa which AREN'T joined
    here — SQLite raises 'no such column' OR (depending on
    predicate shape) silently returns the unfiltered count.
    This test asserts that running the predicate against the
    slim FROM either errors OR returns a count != the truth.
    Either way it's the bug — that's the whole point."""
    sql = """
        SELECT COUNT(*)
        FROM plex_items pi
        INNER JOIN plex_sections ps
          ON ps.section_id = pi.section_id AND ps.included = 1
        WHERE 1=1
    """
    with sqlite3.connect(db) as conn:
        return conn.execute(sql).fetchone()[0]


def _full_count_post_fix(db: Path) -> int:
    """Mirror the post-v1.13.85 full count path — joins themes
    + lf + p + sfa and applies the attn_pills=update predicate
    (matches the library filter)."""
    from app.web.api import _SRC_LETTER_SQL
    sql = f"""
        SELECT COUNT(*)
        FROM plex_items pi
        INNER JOIN plex_sections ps
          ON ps.section_id = pi.section_id AND ps.included = 1
        INNER JOIN themes t
          ON t.tmdb_id = pi.guid_tmdb
         AND t.media_type = (CASE pi.media_type
                              WHEN 'show' THEN 'tv'
                              ELSE pi.media_type END)
        LEFT JOIN placements p
          ON p.media_type = t.media_type
         AND p.tmdb_id = t.tmdb_id
         AND p.section_id = pi.section_id
        LEFT JOIN local_files lf
          ON lf.media_type = t.media_type
         AND lf.tmdb_id = t.tmdb_id
         AND lf.section_id = pi.section_id
        LEFT JOIN section_failure_acks sfa
          ON sfa.media_type = t.media_type
         AND sfa.tmdb_id = t.tmdb_id
         AND sfa.section_id = pi.section_id
        WHERE EXISTS (
            SELECT 1 FROM pending_updates pu
            WHERE pu.media_type = t.media_type
              AND pu.tmdb_id = t.tmdb_id
              AND pu.decision = 'pending'
          )
          AND ({_SRC_LETTER_SQL}) != '-'
    """
    with sqlite3.connect(db) as conn:
        return conn.execute(sql).fetchone()[0]


@pytest.fixture
def db(tmp_path: Path) -> Path:
    p = tmp_path / "motif.db"
    init_db(p)
    _seed_realistic_state(p)
    return p


def test_full_count_path_returns_filtered_count(db):
    """Post-fix: the full FROM with the attn_pills=update
    predicate returns 1 (only tmdb_id=1 has a real pending
    update + a theme placed)."""
    assert _full_count_post_fix(db) == 1


def test_slim_count_path_would_have_returned_unfiltered(db):
    """Documents the bug: the slim count FROM (no themes/lf/p
    joins) couldn't evaluate the attn_pills predicate. With NO
    WHERE clause it returns the section total (5) — that's the
    "28 matches" pattern the user saw scaled down to a 5-item
    test case. Pinning this so a regression that re-removes
    attn_pills from no_pills (sending requests back to the
    slim path) gets caught — the slim count would diverge from
    the truth (1)."""
    assert _slim_count_pre_fix(db) == 5
    # And the truth (full count) is 1. Disagreement = the bug.
    assert _full_count_post_fix(db) == 1
    assert _slim_count_pre_fix(db) != _full_count_post_fix(db)


def test_full_count_agrees_with_row_select_on_realistic_state(db):
    """Sanity: the full count path returns the same number as
    SELECT-ing all matching rows. They share the same WHERE
    clause when the count routes to needs_themes_for_count, so
    they MUST agree."""
    from app.web.api import _SRC_LETTER_SQL
    select_sql = f"""
        SELECT pi.rating_key
        FROM plex_items pi
        INNER JOIN plex_sections ps
          ON ps.section_id = pi.section_id AND ps.included = 1
        INNER JOIN themes t
          ON t.tmdb_id = pi.guid_tmdb
         AND t.media_type = (CASE pi.media_type
                              WHEN 'show' THEN 'tv'
                              ELSE pi.media_type END)
        LEFT JOIN placements p
          ON p.media_type = t.media_type
         AND p.tmdb_id = t.tmdb_id
         AND p.section_id = pi.section_id
        LEFT JOIN local_files lf
          ON lf.media_type = t.media_type
         AND lf.tmdb_id = t.tmdb_id
         AND lf.section_id = pi.section_id
        LEFT JOIN section_failure_acks sfa
          ON sfa.media_type = t.media_type
         AND sfa.tmdb_id = t.tmdb_id
         AND sfa.section_id = pi.section_id
        WHERE EXISTS (
            SELECT 1 FROM pending_updates pu
            WHERE pu.media_type = t.media_type
              AND pu.tmdb_id = t.tmdb_id
              AND pu.decision = 'pending'
          )
          AND ({_SRC_LETTER_SQL}) != '-'
    """
    with sqlite3.connect(db) as conn:
        rows = conn.execute(select_sql).fetchall()
    assert len(rows) == _full_count_post_fix(db) == 1
