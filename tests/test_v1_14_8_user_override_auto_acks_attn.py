"""v1.14.8 — user-override-success auto-acks the per-section ATTN axis.

Pre-fix: a row with src=U (user override active, working canonical
placed) kept rendering the red ⚠ ATTN glyph and counting toward
// N FAIL forever. The INFO dialog correctly read "✓ RESOLVED —
TDB UNAVAILABLE" but the per-row glyph + topbar count + library
filter all kept treating the section as failing. The user had to
click ACK FAILURE manually after providing their working
override.

the user's repro: "row with a U are showing red ! which shouldn't
be possible since a U would ack the fail". Screenshot: 13
Assassins, src=U (purple badge), red ⚠ glyph, INFO dialog reads
"// RESOLVED VIA URL — Playing your url copy. TDB URL is dead
— if it revives you'll see a TDB↑ pill."

Root cause: pre-v1.13.74 the worker's _record_local_file blindly
cleared themes.failure_kind on every successful download. v1.13.74
narrowed that to source_kind='themerrdb' (so the red TDB ✗ pill
correctly survives a user-URL success — TDB-side URL is still
broken). But the per-section ATTN axis (sfa table) wasn't
addressed: nothing wrote sfa on user-override success, so the
section stayed un-acked indefinitely and the ⚠ glyph + FAIL count
+ filter kept lighting up.

v1.14.8 fix in three layers:

  1. **Worker write path** (worker.py): on user-override success
     (source_kind in 'url'/'upload'), INSERT sfa with
     `acked_by='auto:user_override'`. Mirrors v1.10.50's design —
     title-global failure_kind survives (TDB ✗ pill stays red),
     only the per-section ATTN ack lands.

  2. **Schema v44 backfill migration** (db.py): for users
     upgrading from <v1.14.8, stamp sfa rows for sections where a
     healthy U override already exists but the parent themes row
     has un-acked failure. acked_by='auto:user_override:backfill'
     so the audit log distinguishes pre-v1.14.8 backfill from
     v1.14.8+ write-time acks.

  3. **failures_total query** (api.py): rewritten from a title-
     global COUNT against themes to a per-(title, section) count
     joining sfa. Same shape as the existing
     failure_tab_breakdown_rows query so count + breakdown +
     library filter all agree (mirror principle from v1.13.84-88).
     failure_tab_row also gains the sfa join + filter so its
     fallback-href tab choice agrees with the others.

JS already reads `failure_acked_at` as COALESCE(sfa.acked_at,
themes.failure_acked_at) (v1.13.54), so the per-row ⚠ glyph
naturally suppresses once sfa is written — no JS change needed.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from app.core.db import init_db, CURRENT_SCHEMA_VERSION


REPO = Path(__file__).resolve().parent.parent


# ── Schema v44 ────────────────────────────────────────────────


def test_schema_version_bumped_to_44():
    """v1.14.8 ships schema v44. v1.14.28 added v45. The contract
    that matters here: schema is AT LEAST v44 (the v1.14.8 sfa
    backfill migration ran). Future migrations bump the floor."""
    assert CURRENT_SCHEMA_VERSION >= 44


def _seed_db(tmp_path):
    db_path = tmp_path / "motif.db"
    init_db(db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return db_path, conn


def _seed_failing_themed_movie_with_user_override(
    conn, *, tmdb_id=12345, section_id="sec1",
):
    """Set up: themes row with un-acked failure, plex_items in a
    section, user_overrides record + local_files row marking that
    the user override has been successfully downloaded
    (provenance='manual', source_kind='url'). This is the exact
    state the v1.14.8 fixes target."""
    conn.execute(
        "INSERT INTO plex_sections (section_id, title, type, included, "
        "                           is_anime, is_4k, themes_subdir, "
        "                           discovered_at, last_seen_at) "
        "VALUES (?, 'Movies', 'movie', 1, 0, 0, 'movies', "
        "        datetime('now'), datetime('now'))",
        (section_id,),
    )
    conn.execute(
        "INSERT INTO themes (media_type, tmdb_id, title, upstream_source, "
        "                    youtube_url, youtube_video_id, "
        "                    failure_kind, failure_message, "
        "                    failure_at, failure_acked_at, "
        "                    last_seen_sync_at, first_seen_sync_at) "
        "VALUES ('movie', ?, '13 Assassins', 'imdb', ?, ?, "
        "        'video_removed', 'Video unavailable', "
        "        datetime('now'), NULL, "
        "        datetime('now'), datetime('now'))",
        (tmdb_id, "https://www.youtube.com/watch?v=DEAD12345AB", "DEAD12345AB"),
    )
    conn.execute(
        "INSERT INTO plex_items (rating_key, title, year, media_type, section_id, "
        "                        guid_tmdb, first_seen_at, last_seen_at) "
        "VALUES ('rk1', '13 Assassins', 2010, 'movie', ?, ?, "
        "        datetime('now'), datetime('now'))",
        (section_id, str(tmdb_id)),
    )
    conn.execute(
        "INSERT INTO user_overrides (media_type, tmdb_id, section_id, "
        "                            youtube_url, set_at, set_by) "
        "VALUES ('movie', ?, ?, ?, datetime('now'), 'admin')",
        (tmdb_id, section_id,
         "https://www.youtube.com/watch?v=USER123ABCD"),
    )
    conn.execute(
        "INSERT INTO local_files (media_type, tmdb_id, section_id, file_path, "
        "                         file_sha256, file_size, downloaded_at, "
        "                         source_video_id, provenance, source_kind) "
        "VALUES ('movie', ?, ?, '/themes/movies/13 Assassins (2010)/theme.mp3', "
        "        'aaa', 4042796, datetime('now'), 'USER123ABCD', "
        "        'manual', 'url')",
        (tmdb_id, section_id),
    )
    conn.commit()


# ── Migration backfill ────────────────────────────────────────


def test_v44_migration_backfills_sfa_for_existing_user_overrides(tmp_path):
    """A fresh DB at the current schema includes the v44 backfill
    as part of init. To exercise the migration directly we need a
    DB that started at v43 — but that's complex to construct
    without an explicit test fixture. Instead, test the backfill's
    SHAPE: insert a U row WITHOUT sfa, run the same SQL the
    migration runs, verify it acks exactly the right row."""
    db_path, conn = _seed_db(tmp_path)
    _seed_failing_themed_movie_with_user_override(conn)

    # The migration-shaped SQL (idempotent — won't double-stamp).
    cur = conn.execute("""
        INSERT INTO section_failure_acks
            (media_type, tmdb_id, section_id, acked_at, acked_by)
        SELECT lf.media_type, lf.tmdb_id, lf.section_id,
               datetime('now'), 'auto:user_override:backfill'
        FROM local_files lf
        JOIN themes t
          ON t.media_type = lf.media_type
         AND t.tmdb_id    = lf.tmdb_id
        WHERE lf.provenance = 'manual'
          AND lf.source_kind IN ('url', 'upload')
          AND t.failure_kind IS NOT NULL
          AND t.failure_acked_at IS NULL
          AND NOT EXISTS (
              SELECT 1 FROM section_failure_acks sfa
              WHERE sfa.media_type = lf.media_type
                AND sfa.tmdb_id    = lf.tmdb_id
                AND sfa.section_id = lf.section_id
          )
    """)
    conn.commit()
    assert cur.rowcount == 1, "exactly one section should be back-stamped"

    # Verify: sfa row exists for our (movie, 12345, sec1) triple
    # with acked_by reflecting the backfill source.
    sfa = conn.execute(
        "SELECT acked_by FROM section_failure_acks "
        "WHERE media_type='movie' AND tmdb_id=12345 AND section_id='sec1'"
    ).fetchone()
    assert sfa is not None
    assert sfa["acked_by"] == "auto:user_override:backfill"


def test_v44_migration_idempotent_no_double_stamp(tmp_path):
    """Re-running the backfill SQL on an already-stamped row must
    no-op. The NOT EXISTS guard is the safety here."""
    db_path, conn = _seed_db(tmp_path)
    _seed_failing_themed_movie_with_user_override(conn)

    backfill_sql = """
        INSERT INTO section_failure_acks
            (media_type, tmdb_id, section_id, acked_at, acked_by)
        SELECT lf.media_type, lf.tmdb_id, lf.section_id,
               datetime('now'), 'auto:user_override:backfill'
        FROM local_files lf
        JOIN themes t
          ON t.media_type = lf.media_type
         AND t.tmdb_id    = lf.tmdb_id
        WHERE lf.provenance = 'manual'
          AND lf.source_kind IN ('url', 'upload')
          AND t.failure_kind IS NOT NULL
          AND t.failure_acked_at IS NULL
          AND NOT EXISTS (
              SELECT 1 FROM section_failure_acks sfa
              WHERE sfa.media_type = lf.media_type
                AND sfa.tmdb_id    = lf.tmdb_id
                AND sfa.section_id = lf.section_id
          )
    """
    conn.execute(backfill_sql)
    conn.commit()
    # Re-run.
    cur = conn.execute(backfill_sql)
    conn.commit()
    assert cur.rowcount == 0, "second run must be a no-op"
    # And sfa still has exactly one row for our triple.
    n = conn.execute(
        "SELECT COUNT(*) FROM section_failure_acks "
        "WHERE media_type='movie' AND tmdb_id=12345 AND section_id='sec1'"
    ).fetchone()[0]
    assert n == 1


def test_v44_migration_skips_themerrdb_sourced_local_files(tmp_path):
    """A themerrdb-sourced local_files row (the canonical TDB
    download path) MUST NOT trigger an auto-ack. The TDB-success
    path has its own clear-failure-kind logic (worker.py:1162);
    the backfill targets only U-resolved sections."""
    db_path, conn = _seed_db(tmp_path)
    # Same seed but source_kind='themerrdb' instead of 'url'.
    conn.execute(
        "INSERT INTO plex_sections (section_id, title, type, included, "
        "                           is_anime, is_4k, themes_subdir, "
        "                           discovered_at, last_seen_at) "
        "VALUES ('sec1', 'Movies', 'movie', 1, 0, 0, 'movies', "
        "        datetime('now'), datetime('now'))"
    )
    conn.execute(
        "INSERT INTO themes (media_type, tmdb_id, title, upstream_source, "
        "                    youtube_url, youtube_video_id, "
        "                    failure_kind, failure_acked_at, "
        "                    last_seen_sync_at, first_seen_sync_at) "
        "VALUES ('movie', 99, 'TDB Movie', 'imdb', ?, 'XYZ12345678', "
        "        'video_removed', NULL, "
        "        datetime('now'), datetime('now'))",
        ("https://www.youtube.com/watch?v=XYZ12345678",),
    )
    conn.execute(
        "INSERT INTO local_files (media_type, tmdb_id, section_id, file_path, "
        "                         file_sha256, file_size, downloaded_at, "
        "                         source_video_id, provenance, source_kind) "
        "VALUES ('movie', 99, 'sec1', '/themes/x.mp3', 'a', 100, "
        "        datetime('now'), 'XYZ12345678', 'auto', 'themerrdb')"
    )
    conn.commit()
    cur = conn.execute("""
        INSERT INTO section_failure_acks
            (media_type, tmdb_id, section_id, acked_at, acked_by)
        SELECT lf.media_type, lf.tmdb_id, lf.section_id,
               datetime('now'), 'auto:user_override:backfill'
        FROM local_files lf
        JOIN themes t
          ON t.media_type = lf.media_type
         AND t.tmdb_id    = lf.tmdb_id
        WHERE lf.provenance = 'manual'
          AND lf.source_kind IN ('url', 'upload')
          AND t.failure_kind IS NOT NULL
          AND t.failure_acked_at IS NULL
          AND NOT EXISTS (
              SELECT 1 FROM section_failure_acks sfa
              WHERE sfa.media_type = lf.media_type
                AND sfa.tmdb_id    = lf.tmdb_id
                AND sfa.section_id = lf.section_id
          )
    """)
    conn.commit()
    assert cur.rowcount == 0, (
        "TDB-sourced local_files must not back-stamp sfa — only "
        "user-override (url/upload) sources"
    )


def test_v44_migration_skips_already_acked_themes(tmp_path):
    """If themes.failure_acked_at is already set (user clicked ACK
    manually), the backfill must skip — there's no per-section
    surface still un-acked to address."""
    db_path, conn = _seed_db(tmp_path)
    _seed_failing_themed_movie_with_user_override(conn)
    # Pre-ack the title globally.
    conn.execute(
        "UPDATE themes SET failure_acked_at = datetime('now') "
        "WHERE media_type='movie' AND tmdb_id=12345"
    )
    conn.commit()
    cur = conn.execute("""
        INSERT INTO section_failure_acks
            (media_type, tmdb_id, section_id, acked_at, acked_by)
        SELECT lf.media_type, lf.tmdb_id, lf.section_id,
               datetime('now'), 'auto:user_override:backfill'
        FROM local_files lf
        JOIN themes t
          ON t.media_type = lf.media_type
         AND t.tmdb_id    = lf.tmdb_id
        WHERE lf.provenance = 'manual'
          AND lf.source_kind IN ('url', 'upload')
          AND t.failure_kind IS NOT NULL
          AND t.failure_acked_at IS NULL
          AND NOT EXISTS (
              SELECT 1 FROM section_failure_acks sfa
              WHERE sfa.media_type = lf.media_type
                AND sfa.tmdb_id    = lf.tmdb_id
                AND sfa.section_id = lf.section_id
          )
    """)
    conn.commit()
    assert cur.rowcount == 0


def test_v44_migration_skips_clean_themes_no_failure(tmp_path):
    """A row with no failure_kind set obviously needs no ack."""
    db_path, conn = _seed_db(tmp_path)
    conn.execute(
        "INSERT INTO plex_sections (section_id, title, type, included, "
        "                           is_anime, is_4k, themes_subdir, "
        "                           discovered_at, last_seen_at) "
        "VALUES ('sec1', 'Movies', 'movie', 1, 0, 0, 'movies', "
        "        datetime('now'), datetime('now'))"
    )
    conn.execute(
        "INSERT INTO themes (media_type, tmdb_id, title, upstream_source, "
        "                    youtube_url, youtube_video_id, failure_kind, "
        "                    last_seen_sync_at, first_seen_sync_at) "
        "VALUES ('movie', 77, 'Clean Movie', 'imdb', ?, "
        "        'CLEAN567890', NULL, "
        "        datetime('now'), datetime('now'))",
        ("https://www.youtube.com/watch?v=CLEAN567890",),
    )
    conn.execute(
        "INSERT INTO local_files (media_type, tmdb_id, section_id, file_path, "
        "                         file_sha256, file_size, downloaded_at, "
        "                         source_video_id, provenance, source_kind) "
        "VALUES ('movie', 77, 'sec1', '/themes/x.mp3', 'a', 100, "
        "        datetime('now'), 'CLEAN567890', 'manual', 'url')"
    )
    conn.commit()
    cur = conn.execute("""
        INSERT INTO section_failure_acks
            (media_type, tmdb_id, section_id, acked_at, acked_by)
        SELECT lf.media_type, lf.tmdb_id, lf.section_id,
               datetime('now'), 'auto:user_override:backfill'
        FROM local_files lf
        JOIN themes t
          ON t.media_type = lf.media_type
         AND t.tmdb_id    = lf.tmdb_id
        WHERE lf.provenance = 'manual'
          AND lf.source_kind IN ('url', 'upload')
          AND t.failure_kind IS NOT NULL
          AND t.failure_acked_at IS NULL
          AND NOT EXISTS (
              SELECT 1 FROM section_failure_acks sfa
              WHERE sfa.media_type = lf.media_type
                AND sfa.tmdb_id    = lf.tmdb_id
                AND sfa.section_id = lf.section_id
          )
    """)
    conn.commit()
    assert cur.rowcount == 0


# ── Worker write path (static guard) ──────────────────────────


def test_worker_writes_sfa_on_user_override_success():
    """Pin the worker._record_local_file branch: when source_kind
    is in ('url', 'upload') AND the parent theme has unacked
    failure, INSERT sfa for the section. acked_by must be the
    synthetic 'auto:user_override' string so manual vs auto acks
    are distinguishable in the audit log."""
    src = (REPO / "app" / "core" / "worker.py").read_text()
    # Find the elif block for url/upload.
    assert 'elif source_kind in ("url", "upload"):' in src
    anchor = src.index('elif source_kind in ("url", "upload"):')
    body = src[anchor:anchor + 2500]
    # The INSERT references section_failure_acks + the synthetic
    # acked_by string + the EXISTS-failure_kind guard.
    assert "INSERT INTO section_failure_acks" in body
    assert "'auto:user_override'" in body
    # The EXISTS guard ensures we don't ack a section whose theme
    # has no failure pending.
    assert "WHERE EXISTS (" in body
    assert "t.failure_kind IS NOT NULL" in body
    assert "t.failure_acked_at IS NULL" in body
    # ON CONFLICT DO NOTHING — re-running shouldn't replace a
    # (presumably more recent / manual) ack already in place.
    assert "ON CONFLICT(media_type, tmdb_id, section_id) DO NOTHING" in body


def test_worker_themerrdb_branch_unchanged():
    """v1.14.8 added the elif for url/upload. The pre-existing
    themerrdb branch (clear failure_kind + delete sfa) must
    survive verbatim — that's the v1.13.81 P0 audit fix that
    keeps the TDB clear path working."""
    src = (REPO / "app" / "core" / "worker.py").read_text()
    assert 'if source_kind == "themerrdb":' in src
    anchor = src.index('if source_kind == "themerrdb":')
    body = src[anchor:anchor + 1500]
    assert "UPDATE themes SET failure_kind = NULL" in body
    assert "DELETE FROM section_failure_acks" in body


# ── failures_total query is sfa-aware ─────────────────────────


def test_failures_total_query_joins_sfa():
    """Pre-fix `failures_total` was a title-global COUNT against
    themes; the breakdown was per-section + sfa-aware. They could
    only agree by coincidence. v1.14.8 rewrote failures_total to
    mirror the breakdown exactly so count + breakdown + filter are
    structurally equal.

    v1.14.30: extracted the sfa-aware FROM/JOIN into module-level
    `_FAILURES_SFA_FROM_SQL` so the FOUR consumers (failures_total,
    failures_unavailable, failures_cookies, public-stats, dashboard
    insights chart) share one source-of-truth predicate. The
    contract this test pins ("failures_total is sfa-aware") still
    holds — just check the canonical constant has the joins AND
    the failures_total subquery references it."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    # The v1.14.8 marker survives — it's the load-bearing
    # archaeology comment for the rewrite.
    marker = "v1.14.8: rewrote from a title-global COUNT"
    assert marker in src, "v1.14.8 marker comment must be present"
    # The canonical sfa-aware FROM/JOIN constant carries the joins
    # the v1.14.8 fix guaranteed.
    constant_anchor = src.index("_FAILURES_SFA_FROM_SQL = (")
    # v1.15.61 widened the constant with explanatory comment;
    # bumped slice 800 → 2500 to keep the JOIN clauses in scope.
    constant = src[constant_anchor:constant_anchor + 2500]
    assert "JOIN plex_items pi" in constant
    assert "JOIN plex_sections ps" in constant
    assert "LEFT JOIN section_failure_acks sfa" in constant
    where_anchor = src.index("_FAILURES_SFA_WHERE_SQL = (")
    where = src[where_anchor:where_anchor + 400]
    assert "sfa.acked_at IS NULL" in where
    # The failures_total subquery wires both constants in.
    block_start = src.index(marker)
    block_end = src.index("AS failures_total", block_start)
    block = src[block_start:block_end]
    assert "_FAILURES_SFA_FROM_SQL" in block
    assert "_FAILURES_SFA_WHERE_SQL" in block


def test_failure_tab_row_now_joins_sfa():
    """The static fallback href for the topbar FAIL pill
    (failure_tab_row) needs the same sfa awareness so its tab
    pick agrees with the breakdown + filter. Pre-fix it could
    point at a tab whose only failures were already U-resolved."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    anchor = src.index("failure_tab_row = conn.execute(")
    body = src[anchor:anchor + 2000]
    assert "LEFT JOIN section_failure_acks sfa" in body
    assert "AND sfa.acked_at IS NULL" in body


# ── End-to-end: count agreement ───────────────────────────────


def test_failures_total_agrees_with_breakdown_count_after_sfa(tmp_path):
    """The mirror principle: SUM(breakdown.count) MUST equal
    failures_total. Test fixture: seed two sections — one U-resolved
    (sfa stamped), one still failing (no sfa). The U-resolved
    section must be excluded from BOTH counts."""
    db_path, conn = _seed_db(tmp_path)
    # Section sec1: U-resolved (sfa exists).
    _seed_failing_themed_movie_with_user_override(
        conn, tmdb_id=111, section_id="sec1",
    )
    # sec2: same title in another (4K) section, NO user override,
    # still failing — should count.
    conn.execute(
        "INSERT INTO plex_sections (section_id, title, type, included, "
        "                           is_anime, is_4k, themes_subdir, "
        "                           discovered_at, last_seen_at) "
        "VALUES ('sec2', '4K Movies', 'movie', 1, 0, 1, '4k_movies', "
        "        datetime('now'), datetime('now'))"
    )
    conn.execute(
        "INSERT INTO plex_items (rating_key, title, year, media_type, "
        "                        section_id, guid_tmdb, "
        "                        first_seen_at, last_seen_at) "
        "VALUES ('rk2', '13 Assassins', 2010, 'movie', 'sec2', '111', "
        "        datetime('now'), datetime('now'))"
    )
    # Stamp sfa for sec1 (simulating the v1.14.8 worker write).
    conn.execute(
        "INSERT INTO section_failure_acks "
        "(media_type, tmdb_id, section_id, acked_at, acked_by) "
        "VALUES ('movie', 111, 'sec1', datetime('now'), "
        "        'auto:user_override')"
    )
    conn.commit()

    # failures_total — v1.14.8 shape.
    failures_total = conn.execute("""
        SELECT COUNT(*) FROM themes t
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
    """).fetchone()[0]

    # failure_tab_breakdown_rows — same shape, GROUP BY.
    breakdown = conn.execute("""
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
    """).fetchall()
    breakdown_sum = sum(r["n"] for r in breakdown)

    assert failures_total == 1, (
        "the U-resolved sec1 must be excluded; only sec2 (no override) counts"
    )
    assert failures_total == breakdown_sum, (
        f"mirror principle: failures_total ({failures_total}) must equal "
        f"SUM(breakdown.n) ({breakdown_sum})"
    )
    # And the breakdown points to the 4K-movies tab (sec2's variant).
    assert len(breakdown) == 1
    assert breakdown[0]["is_4k"] == 1


def test_per_row_failure_acked_at_reflects_sfa_after_write(tmp_path):
    """The /api/library row select COALESCE's sfa.acked_at into
    failure_acked_at (v1.13.54). After v1.14.8's worker write,
    the row's failure_acked_at field must come back non-null,
    which is what the JS glyph render gates on."""
    db_path, conn = _seed_db(tmp_path)
    _seed_failing_themed_movie_with_user_override(conn)
    # Stamp sfa (simulating v1.14.8 worker behavior).
    conn.execute(
        "INSERT INTO section_failure_acks "
        "(media_type, tmdb_id, section_id, acked_at, acked_by) "
        "VALUES ('movie', 12345, 'sec1', datetime('now'), "
        "        'auto:user_override')"
    )
    conn.commit()
    # COALESCE the per-row select uses.
    row = conn.execute("""
        SELECT t.failure_kind,
               COALESCE(sfa.acked_at, t.failure_acked_at) AS failure_acked_at
        FROM themes t
        LEFT JOIN section_failure_acks sfa
          ON sfa.media_type = t.media_type
         AND sfa.tmdb_id = t.tmdb_id
         AND sfa.section_id = 'sec1'
        WHERE t.media_type = 'movie' AND t.tmdb_id = 12345
    """).fetchone()
    # failure_kind survives (TDB ✗ pill keeps painting).
    assert row["failure_kind"] == "video_removed"
    # failure_acked_at reflects the sfa stamp → JS glyph suppresses.
    assert row["failure_acked_at"] is not None


# ── Migration registered in step ladder ───────────────────────


def test_v43_to_v44_migration_step_registered():
    """The migration step must be wired in the elif ladder so a
    DB at v43 picks up v44 on the next motif start."""
    src = (REPO / "app" / "core" / "db.py").read_text()
    assert "elif current == 43:" in src
    # The block must call the v44 migration function.
    anchor = src.index("elif current == 43:")
    block = src[anchor:anchor + 200]
    assert "_migrate_v43_to_v44(conn)" in block
    assert "current = 44" in block


def test_v44_migration_function_exists_with_correct_target_table():
    """The migration body must INSERT into section_failure_acks
    via a SELECT that joins local_files + themes with the
    user-override criteria."""
    src = (REPO / "app" / "core" / "db.py").read_text()
    assert "def _migrate_v43_to_v44(conn: sqlite3.Connection) -> None:" in src
    anchor = src.index("def _migrate_v43_to_v44(")
    body = src[anchor:anchor + 4000]
    assert "INSERT INTO section_failure_acks" in body
    assert "FROM local_files lf" in body
    assert "JOIN themes t" in body
    assert "lf.provenance = 'manual'" in body
    assert "lf.source_kind IN ('url', 'upload')" in body
    assert "t.failure_kind IS NOT NULL" in body
    assert "t.failure_acked_at IS NULL" in body
    assert "'auto:user_override:backfill'" in body
