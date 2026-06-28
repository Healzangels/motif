"""v1.13.84 — topbar pill breakdown SQL aligned with library filter.

the user reported: clicking the topbar // UPD pill (1 UPD) lands
on /movies?fourk=true&attn_pills=update with `0 matches`. The
badge cycle says the update is in 4K Movies but the library page
shows nothing.

Root cause: the v1.13.78 update_tab_breakdown_rows query had only
`pu.decision='pending'` as its predicate. The library filter for
`attn_pills=update` (api.py:1154-1167) has TWO predicates: the
EXISTS pending_updates AND `({_SRC_LETTER_SQL}) != '-'` — which
excludes rows whose SRC computes to `-` (TDB-dropped, no theme).

So a row with src=`-` counted in the breakdown (badge cycles to it)
but was hidden by the filter (page shows nothing).

Same drift class on the FAIL pill: the breakdown filtered on 4
specific failure kinds (`video_private`, `video_removed`,
`video_age_restricted`, `geo_blocked`) while the library filter +
the failures_total count both use ANY `failure_kind IS NOT NULL`.
A row with kind=`cookies_expired` / `network_error` / `unknown`
counted in the badge + showed in the filter but was missing from
the breakdown — cycle had no entry to land on.

v1.13.84 rebuilt both breakdown queries to mirror their respective
library-filter predicates exactly.

These tests pin the alignment by direct SQL exercise. The pattern:
seed a row that the LIBRARY FILTER would match → assert the
BREAKDOWN counts it. Conversely seed a row the filter would NOT
match → assert the breakdown excludes it.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from app.core.db import init_db


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@pytest.fixture
def db(tmp_path: Path) -> Path:
    p = tmp_path / "motif.db"
    init_db(p)
    return p


def _seed_section(conn, *, section_id: str, type_: str = "movie",
                  is_anime: int = 0, is_4k: int = 0):
    now = _now_iso()
    conn.execute(
        "INSERT INTO plex_sections ("
        "  section_id, title, type, included, discovered_at, last_seen_at,"
        "  is_4k, is_anime"
        ") VALUES (?, ?, ?, 1, ?, ?, ?, ?)",
        (section_id, f"sec-{section_id}", type_, now, now,
         is_4k, is_anime),
    )


def _seed_theme(conn, *, tmdb_id: int, failure_kind: str | None = None):
    now = _now_iso()
    conn.execute(
        "INSERT INTO themes ("
        "  media_type, tmdb_id, title, upstream_source,"
        "  youtube_url, youtube_video_id,"
        "  last_seen_sync_at, first_seen_sync_at,"
        "  failure_kind, failure_acked_at"
        ") VALUES ('movie', ?, ?, 'imdb', ?, ?, ?, ?, ?, NULL)",
        (tmdb_id, f"x{tmdb_id}",
         "https://www.youtube.com/watch?v=vid", "vid",
         now, now, failure_kind),
    )


def _seed_plex_item(conn, *, tmdb_id: int, section_id: str = "1"):
    now = _now_iso()
    conn.execute(
        "INSERT INTO plex_items ("
        "  rating_key, section_id, media_type, title, guid_tmdb,"
        "  first_seen_at, last_seen_at"
        ") VALUES (?, ?, 'movie', ?, ?, ?, ?)",
        (f"rk{tmdb_id}-{section_id}", section_id, f"x{tmdb_id}",
         tmdb_id, now, now),
    )


def _seed_pending_update(conn, *, tmdb_id: int):
    conn.execute(
        "INSERT INTO pending_updates ("
        "  media_type, tmdb_id, decision, detected_at"
        ") VALUES ('movie', ?, 'pending', ?)",
        (tmdb_id, _now_iso()),
    )


def _seed_local_file(conn, *, tmdb_id: int, section_id: str = "1",
                      source_kind: str = "themerrdb"):
    """Adding a local_file gives the row a SRC letter (T / U / A
    depending on source_kind + placement). Without local_files +
    placements, SRC computes to '-' (no theme)."""
    now = _now_iso()
    conn.execute(
        "INSERT INTO local_files ("
        "  media_type, tmdb_id, section_id, file_path, source_video_id,"
        "  downloaded_at, source_kind"
        ") VALUES ('movie', ?, ?, 'x.mp3', 'vid', ?, ?)",
        (tmdb_id, section_id, now, source_kind),
    )


def _seed_placement(conn, *, tmdb_id: int, section_id: str = "1"):
    """Adding a placement is the OTHER condition for SRC != '-'.
    Together with local_files at source_kind='themerrdb' yields
    SRC='T' (the most common pending-update target)."""
    conn.execute(
        "INSERT INTO placements ("
        "  media_type, tmdb_id, section_id, media_folder,"
        "  placed_at, placement_kind, plex_refreshed"
        ") VALUES ('movie', ?, ?, '/data/x', ?, 'hardlink', 0)",
        (tmdb_id, section_id, _now_iso()),
    )


# ── Helpers that mirror the production queries ───────────────

def _upd_breakdown(db: Path):
    """Mirror the v1.13.84 update_tab_breakdown_rows query so the
    SQL stays pinned. Same JOIN + WHERE shape as production."""
    # Inline-import _SRC_LETTER_SQL so a refactor doesn't drift.
    from app.web.api import _SRC_LETTER_SQL
    sql = f"""
        SELECT ps.type, ps.is_anime, ps.is_4k, COUNT(*) AS n
        FROM themes t
        JOIN plex_items pi
          ON pi.guid_tmdb = t.tmdb_id
         AND pi.media_type = (CASE t.media_type WHEN 'tv' THEN 'show' ELSE t.media_type END)
        JOIN plex_sections ps
          ON ps.section_id = pi.section_id AND ps.included = 1
        LEFT JOIN placements p
          ON p.media_type = t.media_type
         AND p.tmdb_id = t.tmdb_id
         AND p.section_id = pi.section_id
        LEFT JOIN local_files lf
          ON lf.media_type = t.media_type
         AND lf.tmdb_id = t.tmdb_id
         AND lf.section_id = pi.section_id
        WHERE EXISTS (
            SELECT 1 FROM pending_updates pu
            WHERE pu.media_type = t.media_type
              AND pu.tmdb_id = t.tmdb_id
              AND pu.decision = 'pending'
          )
          AND ({_SRC_LETTER_SQL}) != '-'
        GROUP BY ps.type, ps.is_anime, ps.is_4k
        ORDER BY ps.is_anime, ps.type, ps.is_4k
    """
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(sql).fetchall()
    return [(r["type"], bool(r["is_anime"]), bool(r["is_4k"]), r["n"])
            for r in rows]


def _fail_breakdown(db: Path):
    """Mirror v1.13.84 failure_tab_breakdown_rows."""
    sql = """
        SELECT ps.type, ps.is_anime, ps.is_4k, COUNT(*) AS n
        FROM themes t
        JOIN plex_items pi
          ON pi.guid_tmdb = t.tmdb_id
         AND pi.media_type = (CASE t.media_type WHEN 'tv' THEN 'show' ELSE t.media_type END)
        JOIN plex_sections ps
          ON ps.section_id = pi.section_id AND ps.included = 1
        LEFT JOIN section_failure_acks sfa
          ON sfa.media_type = t.media_type
         AND sfa.tmdb_id = t.tmdb_id
         AND sfa.section_id = pi.section_id
        WHERE t.failure_kind IS NOT NULL
          AND t.failure_acked_at IS NULL
          AND sfa.acked_at IS NULL
        GROUP BY ps.type, ps.is_anime, ps.is_4k
        ORDER BY ps.is_anime, ps.type, ps.is_4k
    """
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(sql).fetchall()
    return [(r["type"], bool(r["is_anime"]), bool(r["is_4k"]), r["n"])
            for r in rows]


# ── UPD breakdown ↔ filter alignment ─────────────────────────

def test_upd_breakdown_excludes_src_dash_row(db):
    """the user's exact bug: a pending_update exists for a row that
    has a plex_items entry in 4K Movies but NO local_files /
    placements / sidecar there → SRC computes to `-` → library
    filter excludes it → breakdown must too. Pre-fix the
    breakdown returned (movie, False, True, 1); post-fix it
    returns no row (the one source the filter wouldn't show)."""
    with sqlite3.connect(db) as conn:
        _seed_section(conn, section_id="2", is_4k=1)
        _seed_theme(conn, tmdb_id=1)
        _seed_plex_item(conn, tmdb_id=1, section_id="2")
        _seed_pending_update(conn, tmdb_id=1)
        # Critically: NO local_files, NO placements, NO sidecar
        # → SRC computes to `-` for this (tmdb_id, section_id).
    assert _upd_breakdown(db) == []


def test_upd_breakdown_includes_src_T_row(db):
    """Inverse: row with placement + local_files of source_kind=
    'themerrdb' computes SRC='T'. Library filter includes it →
    breakdown must too."""
    with sqlite3.connect(db) as conn:
        _seed_section(conn, section_id="1", is_4k=0)
        _seed_theme(conn, tmdb_id=1)
        _seed_plex_item(conn, tmdb_id=1, section_id="1")
        _seed_pending_update(conn, tmdb_id=1)
        _seed_local_file(conn, tmdb_id=1, section_id="1",
                         source_kind="themerrdb")
        _seed_placement(conn, tmdb_id=1, section_id="1")
    rows = _upd_breakdown(db)
    assert rows == [("movie", False, False, 1)]


def test_upd_breakdown_partitions_by_section_src_state(db):
    """The same tmdb_id appears in BOTH 4K (no theme → SRC=`-`)
    and standard (has theme → SRC='T'). The pending_update is
    title-global. Breakdown should count ONLY the standard
    section — that's the only one the filter would surface."""
    with sqlite3.connect(db) as conn:
        _seed_section(conn, section_id="1", is_4k=0)  # standard
        _seed_section(conn, section_id="2", is_4k=1)  # 4K
        _seed_theme(conn, tmdb_id=1)
        _seed_plex_item(conn, tmdb_id=1, section_id="1")
        _seed_plex_item(conn, tmdb_id=1, section_id="2")
        _seed_pending_update(conn, tmdb_id=1)
        # Only standard has theme.
        _seed_local_file(conn, tmdb_id=1, section_id="1",
                         source_kind="themerrdb")
        _seed_placement(conn, tmdb_id=1, section_id="1")
    rows = _upd_breakdown(db)
    assert rows == [("movie", False, False, 1)]


def test_upd_breakdown_excludes_decided_pending_update(db):
    """Sanity: decision='accepted' / 'declined' must not surface
    even when SRC is good. Carried over from v1.13.78."""
    with sqlite3.connect(db) as conn:
        _seed_section(conn, section_id="1")
        _seed_theme(conn, tmdb_id=1)
        _seed_plex_item(conn, tmdb_id=1, section_id="1")
        _seed_local_file(conn, tmdb_id=1, section_id="1")
        _seed_placement(conn, tmdb_id=1, section_id="1")
        _seed_pending_update(conn, tmdb_id=1)
        conn.execute(
            "UPDATE pending_updates SET decision='accepted' "
            "WHERE tmdb_id=1",
        )
    assert _upd_breakdown(db) == []


# ── FAIL breakdown ↔ filter alignment ────────────────────────

def test_fail_breakdown_includes_cookies_expired(db):
    """Pre-fix the breakdown filtered on 4 specific kinds and
    excluded cookies_expired — but failures_total counts it AND
    the library filter shows it. v1.13.84: any non-null
    failure_kind, unacked, counts."""
    with sqlite3.connect(db) as conn:
        _seed_section(conn, section_id="1")
        _seed_theme(conn, tmdb_id=1, failure_kind="cookies_expired")
        _seed_plex_item(conn, tmdb_id=1, section_id="1")
    rows = _fail_breakdown(db)
    assert rows == [("movie", False, False, 1)]


def test_fail_breakdown_includes_network_error(db):
    """Same drift for network_error."""
    with sqlite3.connect(db) as conn:
        _seed_section(conn, section_id="1")
        _seed_theme(conn, tmdb_id=1, failure_kind="network_error")
        _seed_plex_item(conn, tmdb_id=1, section_id="1")
    rows = _fail_breakdown(db)
    assert rows == [("movie", False, False, 1)]


def test_fail_breakdown_includes_unknown(db):
    """Same drift for unknown."""
    with sqlite3.connect(db) as conn:
        _seed_section(conn, section_id="1")
        _seed_theme(conn, tmdb_id=1, failure_kind="unknown")
        _seed_plex_item(conn, tmdb_id=1, section_id="1")
    rows = _fail_breakdown(db)
    assert rows == [("movie", False, False, 1)]


def test_fail_breakdown_still_includes_dead_kinds(db):
    """Regression guard: video_removed (the most common dead kind)
    must still surface. The widening shouldn't drop the existing
    coverage."""
    with sqlite3.connect(db) as conn:
        _seed_section(conn, section_id="1")
        _seed_theme(conn, tmdb_id=1, failure_kind="video_removed")
        _seed_plex_item(conn, tmdb_id=1, section_id="1")
    rows = _fail_breakdown(db)
    assert rows == [("movie", False, False, 1)]


def test_fail_breakdown_excludes_acked_via_themes_global(db):
    """Title-global ack still excludes (matches the
    failure_acked_at IS NULL filter)."""
    with sqlite3.connect(db) as conn:
        _seed_section(conn, section_id="1")
        _seed_theme(conn, tmdb_id=1, failure_kind="video_removed")
        _seed_plex_item(conn, tmdb_id=1, section_id="1")
        conn.execute(
            "UPDATE themes SET failure_acked_at = ? WHERE tmdb_id=1",
            (_now_iso(),),
        )
    assert _fail_breakdown(db) == []


def test_fail_breakdown_excludes_per_section_acked(db):
    """Per-section ack via section_failure_acks excludes the
    section from the breakdown — same as the library filter's
    `sfa.acked_at IS NULL` predicate. Threaded through the v1.13.81
    sfa lifecycle work."""
    with sqlite3.connect(db) as conn:
        _seed_section(conn, section_id="1")
        _seed_theme(conn, tmdb_id=1, failure_kind="video_removed")
        _seed_plex_item(conn, tmdb_id=1, section_id="1")
        conn.execute(
            "INSERT INTO section_failure_acks "
            "(media_type, tmdb_id, section_id, acked_at, acked_by) "
            "VALUES ('movie', 1, '1', ?, 'admin')",
            (_now_iso(),),
        )
    assert _fail_breakdown(db) == []


# ── static guards: production SQL has the alignment ─────────

def test_production_upd_breakdown_includes_src_filter():
    """Pin the v1.13.84 SQL: the production UPD breakdown query
    must include `({_SRC_LETTER_SQL}) != '-'`. Static guard
    against a regression that drops the filter."""
    api_py = (Path(__file__).resolve().parent.parent
              / "app" / "web" / "api.py").read_text()
    # Find the update_tab_breakdown_rows assignment.
    anchor = api_py.index("update_tab_breakdown_rows = conn.execute")
    # v1.22.83: the query moved to the edition-aware two-tier joins,
    # so the SRC fragment is the _LIB_ variant (COALESCE p_e/p_g)
    # and the window widened past the quartet.
    # v1.23.87: widened 4500→6000 — the edition '' fallback gate added ~11 lines
    # to the p_g/lf_g joins, pushing the asserted SRC fragment past the window.
    block = api_py[anchor:anchor + 6000]
    assert "({_LIB_SRC_LETTER_SQL}) != '-'" in block
    assert "EXISTS (" in block  # the pending_updates EXISTS clause


def test_production_fail_breakdown_uses_any_failure_kind():
    """Pin the v1.13.84 SQL widening for FAIL breakdown: must use
    `failure_kind IS NOT NULL`, not the older 4-kind whitelist."""
    api_py = (Path(__file__).resolve().parent.parent
              / "app" / "web" / "api.py").read_text()
    anchor = api_py.index("failure_tab_breakdown_rows = conn.execute")
    block = api_py[anchor:anchor + 2000]
    assert "t.failure_kind IS NOT NULL" in block
    # The pre-fix 4-kind whitelist literal must not reappear.
    assert ("t.failure_kind IN ('video_private','video_removed',"
            "'video_age_restricted','geo_blocked')") not in block
