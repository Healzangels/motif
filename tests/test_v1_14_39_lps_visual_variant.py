"""v1.14.39 — LPS (Let-Plex-Serve) visual variant.

the user repro: post-v1.14.27 LET PLEX SERVE leaves rows with
motif's canonical preserved (DL=present) but no placement file
at the Plex folder (PL=absent). That state matched the existing
`await` predicate (DL+!PL → "needs placement"), polluting
// NEEDS WORK with rows that are intentional, not broken.

## Fix shape (Option C — original blue variant)

Detect LPS state via discriminator `plex_independent_theme=1`:
the row has no placement BECAUSE Plex is serving its own theme,
which is the entire intent of LET PLEX SERVE. Render it as a
distinct BLUE chip + blue title glyph so the user sees at a
glance "this is intentional, not awaiting placement". Amber
`await` semantics stay for ACTUAL placement-needed rows.

Filter behavior: LPS rows EXCLUDED from `attn_pills="await"` /
`pl_pills="await"` / `pl_pills="broken"` so the // NEEDS WORK
count + the !P chip filter no longer over-flag them.

## Color choice

`--blue` (the existing palette token, used by Pp / +P composite
indicator / TDB↑ chip / pending-update title glyph). Shares
informational-class semantics with those uses. Cyan was the
adopt-A-chip color (rejected — collision); a brand-new color
would have grown motif's palette without need.

## Predicate

The LPS state in three places (mirror principle):
  • Server SQL `attn_pills="await"`: `+ AND COALESCE(pi.plex_
    independent_theme, 0) = 0` (excludes LPS from the filter)
  • Server JS post-stat `_row_matches_pl`: `is_lps` derivation
    + exclusion in `await`/`broken` branches
  • JS render `lpsState`: `!!file_path && !media_folder &&
    plex_independent_theme === 1`

## Edge cases addressed

  • Plex stops serving its independent theme later → LPS flag
    drops to 0 → row gracefully reverts to amber await (correct:
    NOW the user really does need to act)
  • Probe staleness → minor visual lag during the probe-stale
    window; same pattern as other plex_independent_theme uses
  • Title glyph priority: LPS slots between mismatch and await
    (more specific than await; never collides with mismatch
    since mismatch requires media_folder != NULL)
  • Filter+chip mirror: filter exclusion keeps the row out of
    NEEDS WORK; the blue chip stays visible so the row is still
    discoverable by browsing
"""
from __future__ import annotations

import re
import sqlite3
from pathlib import Path

from app.core.db import init_db
from app.core.events import now_iso


REPO = Path(__file__).resolve().parent.parent


# ── Server SQL: attn_pills="await" excludes LPS ──────────────


def test_attn_pills_await_predicate_excludes_lps_state():
    """The attn_pills="await" SQL must include the
    `plex_independent_theme = 0` exclusion. Pre-fix LPS rows
    counted in // NEEDS WORK + matched the !P filter. Pin the
    exact predicate shape so a refactor can't silently drop it."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    # Anchor on the v1.14.39 marker comment.
    assert "v1.14.39: exclude LPS state" in src
    # v1.24.43: the await predicate was extracted to the shared _LIB_AWAIT_SQL
    # constant (reused by the attn_pills=await filter AND the AWAIT badge count,
    # so they can't drift). The LPS exclusion must still live there.
    anchor = src.index('_LIB_AWAIT_SQL = (')
    block = src[anchor:anchor + 500]
    assert "COALESCE(lf_e.file_path, lf_g.file_path) IS NOT NULL" in block
    assert "COALESCE(p_e.media_folder, p_g.media_folder) IS NULL" in block
    assert "COALESCE(pi.plex_independent_theme, 0) = 0" in block
    # and the filter branch references the shared constant (no inline copy).
    assert "attn_branches.append(_LIB_AWAIT_SQL)" in src


# ── Server JS post-stat: _row_matches_pl excludes LPS ────────


def test_row_matches_pl_uses_lps_discriminator():
    """`_row_matches_pl` must derive `is_lps` and exclude it from
    `await`/`broken` branches. Mirror of the SQL exclusion above
    (different code path — handles the post-stat pagination
    triggered when 'on' or 'broken' is in the pill set)."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("def _row_matches_pl(it, pills):")
    # v1.18.25: widened slice — the 'pushed' branch + the
    # mutual-exclusivity rationale comments pushed await/broken
    # past the prior 2000-char boundary.
    body = src[fn_anchor:fn_anchor + 3500]
    # The is_lps derivation matches the JS lpsState predicate.
    assert "is_lps = (" in body
    assert 'it.get("file_path")' in body
    assert 'not it.get("media_folder")' in body
    assert 'it.get("plex_independent_theme") == 1' in body
    # Both await + broken branches gate on `not is_lps`.
    assert 'p == "await"' in body
    assert 'p == "broken"' in body
    # Count `not is_lps` occurrences in the function — should be
    # at least 2 (one per await/broken branch).
    n = body.count("not is_lps")
    assert n >= 2, f"Expected ≥2 `not is_lps` gates, found {n}"


# ── JS: lpsState derivation + chip + glyph render ────────────


def test_js_derives_lps_state_with_correct_predicate():
    """The JS lpsState const must use the exact discriminator
    pattern. Pin the substring so a refactor that "simplifies"
    the predicate (e.g. dropping the file_path check) gets
    caught — without file_path, a row with NO canonical AND
    plex_independent_theme=1 would match (wrong: that's a pure-P
    row, not LPS)."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    assert "const lpsState = !!it.file_path" in js
    # All three pieces of the predicate must be present.
    lps_anchor = js.index("const lpsState = !!it.file_path")
    block = js[lps_anchor:lps_anchor + 500]
    assert "!it.media_folder" in block
    assert "it.plex_independent_theme === 1" in block


def test_js_awaiting_approval_excludes_lps():
    """awaitingApproval must subtract lpsState — otherwise the
    row would render as amber await (chip + title glyph) AND as
    blue lps simultaneously, which is incoherent. Pin the
    `&& !lpsState` clause."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    aa_anchor = js.index("const awaitingApproval = !it.job_in_flight")
    block = js[aa_anchor:aa_anchor + 300]
    assert "&& !lpsState" in block


# v1.14.40 NOTE: the v1.14.39 visual variants (blue PL chip,
# blue title glyph, .state-pill.lps, .title-glyph-lps CSS) were
# REVERTED in v1.14.40 — see test_v1_14_40_lps_link_chip.py for
# the new shape (PS chip in LINK column instead). The four tests
# that pinned the reverted blue-chip behavior are removed; the
# tests below pin the SURVIVING contracts from v1.14.39
# (filter exclusion, awaitingApproval predicate). The visual
# story of LPS now lives in test_v1_14_40_lps_link_chip.py.


def test_css_state_pill_await_still_amber():
    """Sanity: the v1.14.39 fix is ADDITIVE — `.state-pill.await`
    still uses --amber. Pin so a refactor that "consolidates"
    color classes doesn't accidentally repaint await rows."""
    css = (REPO / "app" / "web" / "static" / "app.css").read_text()
    await_anchor = css.index(".state-pill.await {")
    block = css[await_anchor:await_anchor + 200]
    assert "var(--amber)" in block


# ── Behavioral: extracted SQL filter excludes LPS ────────────


def test_attn_await_sql_excludes_lps_against_fixture(tmp_path):
    """End-to-end: seed a fixture with one LPS row and one
    awaiting-placement row. Run the v1.14.39 await predicate
    against both. LPS should NOT match; await SHOULD match."""
    db_path = tmp_path / "motif.db"
    init_db(db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    now = now_iso()
    # Seed plex_sections + 2 themes.
    conn.execute(
        "INSERT INTO plex_sections (section_id, title, type, included, "
        "  is_anime, is_4k, themes_subdir, discovered_at, last_seen_at) "
        "VALUES ('1', 'Movies', 'movie', 1, 0, 0, 'movies', ?, ?)",
        (now, now),
    )
    for tid, title in ((100, "LPS Row"), (101, "Awaiting Row")):
        conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, year, "
            "  upstream_source, first_seen_sync_at, last_seen_sync_at) "
            "VALUES ('movie', ?, ?, 2020, 'imdb', ?, ?)",
            (tid, title, now, now),
        )
        conn.execute(
            "INSERT INTO local_files (media_type, tmdb_id, section_id, "
            "  file_path, file_size, downloaded_at, source_video_id, "
            "  source_kind, provenance) "
            "VALUES ('movie', ?, '1', ?, 100, ?, "
            "        'aaa11111111', 'themerrdb', 'auto')",
            (tid, f"theme-{tid}.mp3", now),
        )
        # Both have plex_items rows — LPS one has plex_independent_
        # theme=1, awaiting one has plex_independent_theme=0.
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type, "
            "  guid_tmdb, title, year, has_theme, local_theme_file, "
            "  folder_path, plex_independent_theme, "
            "  first_seen_at, last_seen_at) "
            "VALUES (?, '1', 'movie', ?, ?, 2020, 1, 0, ?, ?, ?, ?)",
            (str(tid), str(tid), title, f"/data/movies/{title}",
             1 if tid == 100 else 0, now, now),
        )
    conn.commit()
    # Build the v1.14.39 await predicate as a runnable query.
    # Same shape as the SQL fragment we pin above.
    sql = """
        SELECT t.media_type, t.tmdb_id
        FROM themes t
        JOIN plex_items pi
          ON pi.guid_tmdb = t.tmdb_id
         AND pi.media_type = (CASE t.media_type WHEN 'tv' THEN 'show' ELSE t.media_type END)
        LEFT JOIN local_files lf
          ON lf.media_type = t.media_type
         AND lf.tmdb_id = t.tmdb_id
         AND lf.section_id = pi.section_id
        LEFT JOIN placements p
          ON p.media_type = t.media_type
         AND p.tmdb_id = t.tmdb_id
         AND p.section_id = pi.section_id
        WHERE lf.file_path IS NOT NULL
          AND p.media_folder IS NULL
          AND COALESCE(pi.plex_independent_theme, 0) = 0
        ORDER BY t.tmdb_id
    """
    rows = conn.execute(sql).fetchall()
    conn.close()
    tmdb_ids = [r["tmdb_id"] for r in rows]
    assert tmdb_ids == [101], (
        f"Expected only the awaiting row (tmdb=101) to match the "
        f"v1.14.39 await predicate; got {tmdb_ids}. If 100 (LPS) "
        "also matched, the plex_independent_theme exclusion "
        "regressed."
    )


# ── _row_matches_pl behavioral: LPS excluded from await/broken ──


def test_row_matches_pl_excludes_lps_from_await():
    """End-to-end: run the v1.14.39 _row_matches_pl logic
    against a synthetic LPS dict — must NOT match `await`."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("def _row_matches_pl(it, pills):")
    fn_end = src.index("\n    # ", fn_anchor + 100)
    fn_src = src[fn_anchor:fn_end]
    # Compile + exec the function in a sandbox.
    ns: dict = {}
    exec(fn_src, ns)
    _row_matches_pl = ns["_row_matches_pl"]
    # An LPS row.
    lps_row = {
        "file_path": "movies/x.mp3",
        "media_folder": None,
        "plex_independent_theme": 1,
        "placement_missing": False,
    }
    assert _row_matches_pl(lps_row, {"await"}) is False, (
        "LPS row matched pl_pills='await' — exclusion failed."
    )
    # Sanity: an actual awaiting row (no plex_independent_theme)
    # SHOULD match.
    awaiting_row = {
        "file_path": "movies/y.mp3",
        "media_folder": None,
        "plex_independent_theme": 0,
        "placement_missing": False,
    }
    assert _row_matches_pl(awaiting_row, {"await"}) is True


def test_row_matches_pl_excludes_lps_from_broken():
    """Same idea for `broken`: a row with placement_missing=True
    AND plex_independent_theme=1 is in transition (placement file
    gone but Plex is serving — same recovery path as LPS). Must
    not show in pl_pills='broken' either."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("def _row_matches_pl(it, pills):")
    fn_end = src.index("\n    # ", fn_anchor + 100)
    fn_src = src[fn_anchor:fn_end]
    ns: dict = {}
    exec(fn_src, ns)
    _row_matches_pl = ns["_row_matches_pl"]
    lps_broken = {
        "file_path": "movies/x.mp3",
        "media_folder": None,
        "plex_independent_theme": 1,
        "placement_missing": True,
    }
    assert _row_matches_pl(lps_broken, {"broken"}) is False
    actually_broken = {
        "file_path": "movies/y.mp3",
        "media_folder": "/data/movies/Y",
        "plex_independent_theme": 0,
        "placement_missing": True,
    }
    assert _row_matches_pl(actually_broken, {"broken"}) is True
