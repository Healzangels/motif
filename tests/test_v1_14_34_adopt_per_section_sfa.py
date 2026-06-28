"""v1.14.34 — _do_adopt writes per-section sfa (was title-global).

Closes the third + final "user resolves locally" cross-section
bleed site (CLAUDE.md class K, audit Theme 1). v1.14.24 closed
SET URL (api_manual_url) + UPLOAD MP3 (api_upload_theme) by
switching their implicit-ack writes from title-global
`themes.failure_acked_at` to per-section
`section_failure_acks`. Adopt was the matching gap on the
"user adopts an existing sidecar" path.

## Pre-fix bug

When a user adopts a sidecar in section A on a multi-section
title, `_do_adopt` (adopt.py:697) ran:

    UPDATE themes SET failure_acked_at = ?
    WHERE media_type = ? AND tmdb_id = ?
      AND failure_kind IS NOT NULL

That's title-global. Section B's row also lost its `! FAIL`
glyph + counted out of the topbar `N FAIL` count, even though
the user did nothing to resolve B's failing TDB URL.

## Fix

Convert to the v1.14.24 sfa pattern: INSERT into
section_failure_acks scoped to the row's section_id, with the
EXISTS gate so we don't write a stale ack on a row with no
failure. ON CONFLICT DO NOTHING preserves any earlier explicit
ack the user may have set (acked_by + acked_at distinguish
'auto:adopt' from a deliberate / earlier ack).

The audit recommends this as a class-fix candidate (Theme 1
"section_id contract"); v1.14.34 is the surgical version that
finishes the user-resolves-locally cluster (SET URL + UPLOAD +
ADOPT all per-section now). The class-level ACK paths
(api_clear_failure) were already per-section since v1.13.54.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── Static-text guards on the fix ────────────────────────────


def test_do_adopt_no_longer_writes_title_global_failure_acked_at():
    """The pre-fix UPDATE on themes.failure_acked_at must be
    gone from _do_adopt. Pin the substring so a refactor can't
    revert to the title-global form."""
    src = (REPO / "app" / "core" / "adopt.py").read_text()
    fn_anchor = src.index("def _do_adopt(")
    body = src[fn_anchor:fn_anchor + 13000]  # v1.24.1: was 10000 (ON CONFLICT grew _do_adopt)
    # The pre-fix shape — anchored to the SET clause + the
    # bare-WHERE form (no section_id in the WHERE).
    bare_pattern = (
        '"UPDATE themes SET failure_acked_at = ? "\n'
        '            "WHERE media_type = ? AND tmdb_id = ? '
        'AND failure_kind IS NOT NULL"'
    )
    assert bare_pattern not in body, (
        "_do_adopt still writes title-global failure_acked_at — "
        "this bleeds the dismissal across every section that "
        "owns the title. Convert to the section_failure_acks "
        "INSERT pattern (see v1.14.34 marker)."
    )


def test_do_adopt_writes_per_section_sfa():
    """The new INSERT must target section_failure_acks scoped to
    the row's section_id. Pin every structural piece so a
    refactor can't silently weaken the predicate (e.g. dropping
    the EXISTS gate would write a stale ack on rows with no
    failure_kind)."""
    src = (REPO / "app" / "core" / "adopt.py").read_text()
    fn_anchor = src.index("def _do_adopt(")
    body = src[fn_anchor:fn_anchor + 13000]  # v1.24.1: was 10000 (ON CONFLICT grew _do_adopt)
    assert "INSERT INTO section_failure_acks" in body
    assert "auto:adopt" in body
    # The EXISTS gate prevents writing on a no-failure row.
    sfa_anchor = body.index("INSERT INTO section_failure_acks")
    sfa_block = body[sfa_anchor:sfa_anchor + 1500]
    assert "WHERE EXISTS (" in sfa_block
    assert "t.failure_kind IS NOT NULL" in sfa_block
    # Idempotent over re-adopt — preserves earlier acks.
    assert "ON CONFLICT(media_type, tmdb_id, section_id) DO NOTHING" in sfa_block


def test_do_adopt_v1_14_34_marker_present():
    """The archaeology comment captures the why — load-bearing in
    case a future refactor decides to "simplify" the per-section
    INSERT back into a UPDATE."""
    src = (REPO / "app" / "core" / "adopt.py").read_text()
    assert "v1.14.34: per-section sfa write" in src
    # Cross-references the v1.14.24 SET URL + UPLOAD MP3 fixes
    # so the reader can trace the full bleed-class history.
    assert "v1.14.24" in src


def test_adopt_section_id_required_in_scope():
    """`section_id = finding[\"section_id\"]` is the row-scoped
    write target; without it the sfa write is a no-op (the
    AdoptError raises early). Pin so a refactor can't move the
    section_id resolution past the sfa write site."""
    src = (REPO / "app" / "core" / "adopt.py").read_text()
    fn_anchor = src.index("def _do_adopt(")
    body = src[fn_anchor:fn_anchor + 13000]  # v1.24.1: was 10000 (ON CONFLICT grew _do_adopt)
    # The section_id pull comes BEFORE the sfa write.
    section_pull = body.index('section_id = finding["section_id"]')
    sfa_write = body.index("INSERT INTO section_failure_acks")
    assert section_pull < sfa_write
    # And the AdoptError gate exists right after the pull.
    assert 'raise AdoptError("scan finding missing section_id")' in body


# ── Behavioral: extract the actual SQL + run on a fixture ────


def test_sfa_insert_runs_against_fixture(tmp_path):
    """End-to-end: extract the actual sfa INSERT from adopt.py
    at runtime, run it against a fixture seeded with a failing
    theme + section, assert exactly one section_failure_acks row
    lands with the right (media_type, tmdb_id, section_id, by).
    Sister section's ack stays NULL — pinning the bleed fix."""
    import re
    import sqlite3
    from app.core.db import init_db
    from app.core.events import now_iso

    db_path = tmp_path / "motif.db"
    init_db(db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    now = now_iso()
    # Seed: one theme with a failure_kind set (so EXISTS gate
    # fires), two sections owning it (sec1 = adopted, sec2 =
    # unaffected — must NOT receive an sfa row).
    for sid, title, subdir, is_4k in (
        ("sec1", "Movies", "movies", 0),
        ("sec2", "Movies 4K", "movies-4k", 1),
    ):
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, "
            "  included, is_anime, is_4k, themes_subdir, "
            "  discovered_at, last_seen_at) "
            "VALUES (?, ?, 'movie', 1, 0, ?, ?, ?, ?)",
            (sid, title, is_4k, subdir, now, now),
        )
    conn.execute(
        "INSERT INTO themes (media_type, tmdb_id, title, year, "
        "                    upstream_source, first_seen_sync_at, "
        "                    last_seen_sync_at, failure_kind, "
        "                    failure_message, failure_at) "
        "VALUES ('movie', 5555, 'Multi-Section Movie', 2020, "
        "        'imdb', ?, ?, 'video_removed', 'gone', ?)",
        (now, now, now),
    )
    conn.commit()
    # Extract the adopt.py sfa INSERT.
    src = (REPO / "app" / "core" / "adopt.py").read_text()
    fn_anchor = src.index("def _do_adopt(")
    body = src[fn_anchor:fn_anchor + 13000]  # v1.24.1: was 10000 (ON CONFLICT grew _do_adopt)
    # Locate the conn.execute("""...""" containing the sfa
    # INSERT and pull the SQL out.
    m = re.search(
        r'"""(INSERT INTO section_failure_acks.+?)"""',
        body, re.DOTALL,
    )
    assert m, "couldn't extract the sfa INSERT triple-quoted SQL"
    sql = m.group(1)
    # Run it for sec1 ONLY — the bleed-fix means sec2 must NOT
    # get an sfa row.
    conn.execute(
        sql,
        ("movie", 5555, "sec1", now_iso(),
         "movie", 5555),
    )
    conn.commit()
    rows = conn.execute(
        "SELECT section_id, acked_by FROM section_failure_acks "
        "WHERE media_type = 'movie' AND tmdb_id = 5555 "
        "ORDER BY section_id"
    ).fetchall()
    conn.close()
    # Exactly one row, for sec1, with auto:adopt as the actor.
    assert len(rows) == 1, (
        f"Expected 1 sfa row (sec1 only), got {len(rows)}: "
        f"{[(r['section_id'], r['acked_by']) for r in rows]}. "
        "If sec2 also got a row, the per-section scope leaked."
    )
    assert rows[0]["section_id"] == "sec1"
    assert rows[0]["acked_by"] == "auto:adopt"


def test_sfa_insert_no_op_when_no_failure_kind(tmp_path):
    """The EXISTS gate must short-circuit when the row has no
    failure_kind — adopting a healthy row mustn't strand a
    stale sfa entry."""
    import re
    import sqlite3
    from app.core.db import init_db
    from app.core.events import now_iso

    db_path = tmp_path / "motif.db"
    init_db(db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    now = now_iso()
    conn.execute(
        "INSERT INTO plex_sections (section_id, title, type, "
        "  included, is_anime, is_4k, themes_subdir, "
        "  discovered_at, last_seen_at) "
        "VALUES ('sec1', 'Movies', 'movie', 1, 0, 0, "
        "        'movies', ?, ?)",
        (now, now),
    )
    # Healthy theme — no failure_kind set.
    conn.execute(
        "INSERT INTO themes (media_type, tmdb_id, title, year, "
        "                    upstream_source, first_seen_sync_at, "
        "                    last_seen_sync_at) "
        "VALUES ('movie', 6666, 'Healthy Movie', 2021, "
        "        'imdb', ?, ?)",
        (now, now),
    )
    conn.commit()
    src = (REPO / "app" / "core" / "adopt.py").read_text()
    fn_anchor = src.index("def _do_adopt(")
    # v1.24.1: widened 10000→13000 — the ON CONFLICT clauses on the
    # local_files/placements upserts grew _do_adopt, pushing the sfa INSERT
    # (~9608 chars in) past the old window so `.+?` couldn't complete.
    body = src[fn_anchor:fn_anchor + 13000]
    m = re.search(
        r'"""(INSERT INTO section_failure_acks.+?)"""',
        body, re.DOTALL,
    )
    assert m, "couldn't extract the sfa INSERT triple-quoted SQL"
    sql = m.group(1)
    conn.execute(
        sql,
        ("movie", 6666, "sec1", now_iso(),
         "movie", 6666),
    )
    conn.commit()
    n = conn.execute(
        "SELECT COUNT(*) FROM section_failure_acks "
        "WHERE media_type = 'movie' AND tmdb_id = 6666"
    ).fetchone()[0]
    conn.close()
    assert n == 0, (
        "EXISTS gate didn't fire — a healthy row got an sfa "
        "entry it shouldn't have. The gate is the only thing "
        "keeping no-failure rows out of section_failure_acks."
    )


# ── Reuse pin: the v1.14.24 SET URL + UPLOAD MP3 fixes ───────


def test_v14_24_sister_fixes_still_in_place():
    """The matching v1.14.24 conversions on api_manual_url +
    api_upload_theme MUST stay in place. v1.14.34's fix is the
    third member of the trio — if either of the v1.14.24 sites
    regresses, the same bleed comes back through SET URL or
    UPLOAD MP3 instead."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    # Both site markers are present + reference per-section sfa.
    assert "v1.14.24: per-section sfa write" in src
    # Both sites use 'auto:set_url' / 'auto:upload' (or grep-able
    # equivalents) so the actor field tells the audit log what
    # happened.
    assert "auto:set_url" in src
    assert "auto:upload" in src
