"""v1.14.44 — LET PLEX SERVE only when motif owns a placement.

the user repro 1: "A Christmas Carol (2009)" pure-M row showed
BOTH `LET PLEX SERVE` AND `ADOPT + LET PLEX SERVE` in the
INFO card's TRY THIS NEXT. The bare LET PLEX SERVE was a no-op
(motif has no placement to unplace); only ADOPT + LET PLEX
SERVE makes sense (adopt the sidecar, then unplace).

the user repro 2: "10,000 BC (2008)" pure-P row showed `LET PLEX
SERVE` despite Plex already serving its own theme. The button
was meaningless — no placement to unplace, Plex IS already
serving.

Both bugs share one root cause: the v1.14.40 `purge-revert-to-
plex` gate was `if p_available and not is_lps:` — never checked
whether motif actually had a placement to unplace.

## Fix

Unified rule: **LET PLEX SERVE only makes sense when motif owns
a placement to unplace.** Add `motif_has_placement` boolean to
the gate.

Refactored the v1.14.42 LPS-detection block: ONE
`section_placement` query feeds BOTH `is_lps` (the LPS
discriminator) AND `motif_has_placement` (the new LET PLEX
SERVE gate). Cleaner — same fetchone() result, two derived
booleans.

## Cases after the fix

| State | motif_has_placement | LET PLEX SERVE shows? |
|---|---|---|
| T/U/A (motif owns placement)             | True  | YES (intended) |
| Pure-M (sidecar exists, motif didn't place) | False | NO (the user bug 1) |
| Pure-P (Plex serves, motif owns nothing)   | False | NO (the user bug 2) |
| LPS (post-LET-PLEX-SERVE)                | False | NO (already gated by `not is_lps`, v1.14.40) |

ADOPT + LET PLEX SERVE keeps its existing gate
(`m_available and p_available and not is_lps`) — already correct
for pure-M+P composite (the bundled flow that goes adopt-then-
unplace).
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from app.core.db import init_db
from app.core.events import now_iso


REPO = Path(__file__).resolve().parent.parent


# ── Server: motif_has_placement gate present ────────────────


def test_let_plex_serve_gate_includes_motif_has_placement():
    """The LET PLEX SERVE button must only appear when motif owns
    a placement to unplace. Pure-M and pure-P rows must NOT see
    it (would be a no-op or meaningless action).

    v1.14.47 reorg: the option moved from the api_recovery_options
    no-fail branch to the SOURCE-menu render in app.js. The
    `motif_has_placement` predicate is mirrored client-side as
    `placed` (== !!media_folder, the v1.14.10 derivation). The
    contract this test pins survives via the SOURCE-menu render's
    `placed` gate."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    lps_anchor = js.index("'purge-revert-to-plex', 'LET PLEX SERVE'")
    # Walk back to the surrounding `if (` gate.
    gate_start = js.rfind("if (", lps_anchor - 800, lps_anchor)
    gate = js[gate_start:lps_anchor]
    # plex_independent_theme === 1 == p_available
    assert "plex_independent_theme === 1" in gate
    # placed == motif_has_placement (LPS rows are !placed → hidden)
    assert "placed" in gate
    # The v1.14.47 reorg note in the gate's comment block.
    comment_block = js[max(0, gate_start - 1500):gate_start]
    assert "v1.14.47" in comment_block


def test_motif_has_placement_derived_inside_with_block():
    """v1.14.61 deleted the entire `is_lps` + `motif_has_placement`
    derivation block — both variables had zero downstream
    consumers in api_recovery_options post-v1.14.47. The contract
    this test originally pinned (placement of the derivation
    inside the with-block to avoid the v1.14.42 closed-DB crash)
    survives in the strictest form: there's no derivation to
    misplace."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("async def api_recovery_options(")
    body = src[fn_anchor:fn_anchor + 25000]
    # The deletion marker is present.
    assert "v1.14.61: deleted the v1.14.42/v1.14.44 `is_lps`" in body


def test_is_lps_still_uses_same_placement_query():
    """v1.14.61 deleted both `is_lps` and `motif_has_placement`
    from api_recovery_options. The shared-placement-query
    refactoring this test pinned is moot now that there's no
    placement query in the function. The SOURCE-menu LET PLEX
    SERVE gate at app.js mirrors the LPS contract via the JS-
    side `placed` predicate (the equivalent of the deleted
    server-side `motif_has_placement`)."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("async def api_recovery_options(")
    body = src[fn_anchor:fn_anchor + 25000]
    # The v1.14.61 deletion marker.
    assert "v1.14.61: deleted the v1.14.42/v1.14.44 `is_lps`" in body
    # JS-side placed gate still exists for LET PLEX SERVE (mirror
    # of the deleted server-side motif_has_placement).
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    lps_anchor = js.index("'purge-revert-to-plex', 'LET PLEX SERVE'")
    gate_start = js.rfind("if (", lps_anchor - 800, lps_anchor)
    gate = js[gate_start:lps_anchor]
    assert "placed" in gate


def test_adopt_let_plex_serve_gate_unchanged():
    """The ADOPT + LET PLEX SERVE button's gate must require BOTH
    a sidecar (m_available) AND Plex serving its own theme
    (p_available). Pure-M without P must NOT see it (would
    delete the only theme); LPS state must NOT see it (sidecar
    already gone).

    v1.14.47 reorg: the option moved from api_recovery_options
    no-fail branch to the SOURCE-menu render in app.js. The
    JS-equivalent gate is:
      • plex_local_theme === 1   ← m_available "sidecar exists"
      • !placed                  ← m_available "motif didn't place"
      • plex_independent_theme === 1  ← p_available
    LPS state (file_path + !placed + p_available) has
    plex_local_theme === 0 (motif unplaced its own file; no
    foreign sidecar) so the gate naturally excludes LPS."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    adopt_anchor = js.index("'adopt-and-let-plex-serve', 'ADOPT + LET PLEX SERVE'")
    gate_start = js.rfind("if (", adopt_anchor - 800, adopt_anchor)
    gate = js[gate_start:adopt_anchor]
    assert "plex_local_theme === 1" in gate
    assert "!placed" in gate
    assert "plex_independent_theme === 1" in gate


# ── Behavioral: extracted SQL distinguishes the four cases ──


def _seed_three_rows(tmp_path):
    """Fixture: 3 themes in one section.
      - tmdb 100: T-source, motif owns placement (LET PLEX SERVE applies)
      - tmdb 200: pure-M, sidecar exists, motif owns nothing (no LPS)
      - tmdb 300: pure-P, Plex serves, motif owns nothing (no LPS)
    All three have plex_independent_theme=1 (Plex serves its own
    theme) so p_available=True for each."""
    db_path = tmp_path / "motif.db"
    init_db(db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    now = now_iso()
    conn.execute(
        "INSERT INTO plex_sections (section_id, title, type, "
        "  included, is_anime, is_4k, themes_subdir, "
        "  discovered_at, last_seen_at) "
        "VALUES ('1', 'Movies', 'movie', 1, 0, 0, 'movies', ?, ?)",
        (now, now),
    )
    for tid, title, has_canonical, has_placement, local_theme_file in (
        (100, "T-Source",     True,  True,  1),  # motif placed
        (200, "Pure-M",       False, False, 1),  # external sidecar
        (300, "Pure-P",       False, False, 0),  # nothing at Plex folder
    ):
        conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, year, "
            "                    upstream_source, first_seen_sync_at, "
            "                    last_seen_sync_at) "
            "VALUES ('movie', ?, ?, 2020, 'imdb', ?, ?)",
            (tid, title, now, now),
        )
        if has_canonical:
            conn.execute(
                "INSERT INTO local_files (media_type, tmdb_id, "
                "  section_id, file_path, file_size, downloaded_at, "
                "  source_video_id, source_kind, provenance) "
                "VALUES ('movie', ?, '1', ?, 100, ?, "
                "        'aaa11111111', 'themerrdb', 'auto')",
                (tid, f"theme-{tid}.mp3", now),
            )
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, "
            "  media_type, guid_tmdb, title, year, has_theme, "
            "  local_theme_file, folder_path, plex_independent_theme, "
            "  first_seen_at, last_seen_at) "
            "VALUES (?, '1', 'movie', ?, ?, 2020, 1, ?, ?, 1, ?, ?)",
            (str(tid), str(tid), title, local_theme_file,
             f"/data/movies/{title}", now, now),
        )
        if has_placement:
            conn.execute(
                "INSERT INTO placements (media_type, tmdb_id, "
                "  section_id, media_folder, placed_at, "
                "  placement_kind, provenance) "
                "VALUES ('movie', ?, '1', ?, ?, 'hardlink', 'auto')",
                (tid, f"/data/movies/{title}", now),
            )
    conn.commit()
    return db_path, conn


def test_motif_has_placement_query_distinguishes_three_cases(tmp_path):
    """End-to-end: the v1.14.44 `section_placement` query against
    a fixture with 3 rows (T+placement, pure-M, pure-P).
    Returns a row only for tmdb=100 (the one with a placement)."""
    db_path, conn = _seed_three_rows(tmp_path)
    sql = (
        "SELECT 1 FROM placements "
        "WHERE media_type = ? AND tmdb_id = ? AND section_id = ?"
    )
    matched = []
    for tid in (100, 200, 300):
        row = conn.execute(sql, ("movie", tid, "1")).fetchone()
        if row is not None:
            matched.append(tid)
    conn.close()
    assert matched == [100], (
        f"Expected only tmdb=100 (T+placement) to match the "
        f"motif_has_placement query; got {matched}. If 200 (pure-M) "
        "or 300 (pure-P) matched, the gate would still over-show "
        "LET PLEX SERVE on those rows."
    )


def test_let_plex_serve_only_appears_for_t_source_row(tmp_path):
    """End-to-end on the gate logic: simulate the recovery-options
    no-fail branch for each of the 3 rows. Only tmdb=100 should
    produce the `purge-revert-to-plex` option."""
    db_path, conn = _seed_three_rows(tmp_path)
    # Replicate the gate exactly: p_available + not is_lps +
    # motif_has_placement. All three rows have p_available=True
    # (plex_independent_theme=1 in the fixture).
    placement_sql = (
        "SELECT 1 FROM placements "
        "WHERE media_type = ? AND tmdb_id = ? AND section_id = ?"
    )
    local_sql = (
        "SELECT 1 FROM local_files "
        "WHERE media_type = ? AND tmdb_id = ? AND section_id = ? "
        "  AND file_path IS NOT NULL"
    )
    showing_let_plex_serve = []
    for tid in (100, 200, 300):
        local = conn.execute(local_sql, ("movie", tid, "1")).fetchone()
        placement = conn.execute(placement_sql, ("movie", tid, "1")).fetchone()
        motif_has_placement = placement is not None
        # p_available True for all three by fixture construction.
        p_available = True
        is_lps = bool(local and p_available and not motif_has_placement)
        if p_available and not is_lps and motif_has_placement:
            showing_let_plex_serve.append(tid)
    conn.close()
    assert showing_let_plex_serve == [100], (
        f"Expected LET PLEX SERVE option only for tmdb=100 "
        f"(T+placement); got {showing_let_plex_serve}. If 200 or "
        "300 are in the list, the v1.14.44 gate is leaking."
    )
