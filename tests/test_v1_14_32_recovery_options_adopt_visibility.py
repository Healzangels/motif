"""v1.14.32 — ADOPT + LET PLEX SERVE option hides on already-adopted rows.

the user repro: the *batteries not included row was already adopted
(SRC=A, history shows "Inline adopt of sidecar at /data/media/
movies/-batteries not included (1987)") yet the INFO card's
// TRY THIS NEXT section showed BOTH:
  • LET PLEX SERVE
  • ADOPT + LET PLEX SERVE  ← nonsense, nothing left to adopt

## Root cause

`api_recovery_options` (api.py ≈ line 10125) decides
`m_available` ("there's a sidecar at the Plex folder that motif
didn't place") via:

    placement = conn.execute(
        "SELECT 1 FROM placements WHERE plex_rating_key = ?",
        (rating_key,),
    ).fetchone()
    m_available = placement is None

But `placements.plex_rating_key` is set ONLY by the place worker
(worker.py:1554, via OUTCOME object). The adopt code path
(adopt.py:686) inserts the placements row WITHOUT setting
plex_rating_key — so it stays NULL on every adopted row.

Net effect: the query never finds the placements row for an
adopted item, so `m_available` stays True, so ADOPT + LET PLEX
SERVE keeps appearing as a recovery option even after adoption
already happened. Confusing UX + the action would no-op or
double-process.

## Fix

Switch the placement-existence query from `WHERE plex_rating_key
= ?` to the natural placements PK
`(media_type, tmdb_id, section_id)`. The section_id is in the
plex_items row we already SELECT'd one query earlier — extend
that SELECT to include `pi.section_id` and use it in the
follow-up query.

Doesn't depend on adopt.py also setting plex_rating_key (a
worthwhile follow-up but separable). This fix is the right shape
even if plex_rating_key gets backfilled later — it answers the
actual question ("is there a placement at this section") instead
of the proxy ("did the place worker stamp the rk").
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


def _extract_concat_sql(block: str, anchor_substr: str) -> str:
    """Pull a multi-string-literal SQL out of a `conn.execute(...)`
    call where the SQL is split across N adjacent `"..."` chunks.
    Locates the conn.execute line whose SQL contains
    `anchor_substr`, then concatenates every quoted chunk inside
    that call's parens. Returns the joined SQL string."""
    import re
    # Find the conn.execute( call whose body contains the anchor.
    # Slice from the conn.execute( marker through the matching
    # `).fetchone()` (the surrounding pattern in this codebase).
    exec_anchors = [m.start() for m in re.finditer(
        r'conn\.execute\(', block)]
    for start in exec_anchors:
        end = block.find(").fetchone()", start)
        if end == -1:
            continue
        chunk = block[start:end]
        if anchor_substr not in chunk:
            continue
        # Pull every quoted literal inside this chunk and join.
        parts = re.findall(r'"([^"]*)"', chunk)
        if not parts:
            continue
        return "".join(parts)
    raise AssertionError(
        f"Couldn't find a conn.execute call containing "
        f"{anchor_substr!r} — the v1.14.32 fix may have been "
        "refactored away."
    )


# ── Static-text guards on the fix ────────────────────────────


def test_recovery_options_uses_placements_pk_not_rating_key():
    """The placement-existence query must key on (media_type,
    tmdb_id, section_id) — the natural placements PK — not on
    plex_rating_key. Pin so a refactor can't revert to the
    rk-keyed form (which silently misses adopted rows)."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("async def api_recovery_options(")
    # v1.22.71: slice to the function's actual end — the fixed 12000
    # window went stale when the rk-threading block grew the body.
    body = src[fn_anchor:src.index("\n    @app.", fn_anchor)]
    # The v1.14.32 marker anchors the m_available block.
    block_anchor = body.index("v1.14.32: switched from")
    block = body[block_anchor:block_anchor + 1500]
    # Must use the placements PK shape (joined across the line-
    # continued string literals — substring still works because
    # Python concatenates adjacent string lits at compile time
    # and the source still contains each literal).
    assert "WHERE media_type = ? AND tmdb_id = ?" in block
    assert "AND section_id = ?" in block
    # Must NOT use the bare plex_rating_key form (the bug). Walk
    # the whole api_recovery_options body — neither the rk form
    # query nor a lingering placements-keyed-on-rk pattern should
    # survive.
    assert "FROM placements\n" not in block
    assert "WHERE plex_rating_key = ?" not in body, (
        "Reverted to the rk-keyed query — this misses "
        "already-adopted rows because adopt.py never sets "
        "placements.plex_rating_key. See v1.14.32 marker."
    )


def test_recovery_options_selects_section_id_from_plex_items():
    """The state SELECT must pull `pi.section_id` so the follow-
    up placements query has the natural PK column to filter on.
    Without this the section_id has to be re-resolved (extra
    query) or the lookup falls back to the buggy rk form."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("async def api_recovery_options(")
    # v1.22.71: slice to the function's actual end — the fixed 12000
    # window went stale when the rk-threading block grew the body.
    body = src[fn_anchor:src.index("\n    @app.", fn_anchor)]
    state_anchor = body.index(
        "SELECT pi.local_theme_file, pi.plex_independent_theme",
    )
    state_block = body[state_anchor:state_anchor + 400]
    assert "pi.section_id" in state_block


def test_recovery_options_marker_explains_why():
    """The v1.14.32 archaeology comment must explain WHY the
    rk-keyed form was wrong (load-bearing in case a future
    refactor decides plex_rating_key is the cleaner key without
    realizing the gap)."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    assert "v1.14.32: switched from `WHERE plex_rating_key" in src
    assert "stays NULL on rows" in src or "stays NULL" in src
    # Reference the underlying gap (adopt code never sets it).
    assert "/adopt-sidecar" in src or "adopt.py" in src


# ── Behavioral: extract the actual SQL + run on fixture ─────


def test_placements_pk_query_finds_adopted_row(tmp_path):
    """End-to-end: extract the actual placements query from
    api.py at runtime, run it against a fixture seeded with an
    adopted row (placements row exists with plex_rating_key=NULL,
    matching what adopt.py writes), and assert the query finds
    the row → m_available evaluates to False → ADOPT + LET PLEX
    SERVE option correctly hides."""
    import re
    import sqlite3
    from app.core.db import init_db
    from app.core.events import now_iso

    db_path = tmp_path / "motif.db"
    init_db(db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    # Seed: a section, a theme, a local_files row (the canonical),
    # and a placements row matching what adopt.py inserts (no
    # plex_rating_key — the gap that makes the rk query fail).
    now = now_iso()
    conn.execute(
        "INSERT INTO plex_sections (section_id, title, type, included, "
        "                           is_anime, is_4k, themes_subdir, "
        "                           discovered_at, last_seen_at) "
        "VALUES (?, ?, ?, 1, 0, 0, ?, ?, ?)",
        ("1", "Movies", "movie", "movies", now, now),
    )
    conn.execute(
        "INSERT INTO themes (media_type, tmdb_id, title, year, "
        "                    upstream_source, first_seen_sync_at, "
        "                    last_seen_sync_at) "
        "VALUES ('movie', 11548, 'Adopted Movie', 1987, "
        "        'imdb', ?, ?)",
        (now, now),
    )
    conn.execute(
        "INSERT INTO local_files (media_type, tmdb_id, section_id, "
        "                         file_path, file_size, downloaded_at, "
        "                         source_video_id, source_kind, provenance) "
        "VALUES ('movie', 11548, '1', 'adopted/theme.mp3', 100, ?, "
        "        'adopthashabc', 'adopt', 'manual')",
        (now,),
    )
    # The adopted placements row — matches adopt.py:686 INSERT
    # exactly (no plex_rating_key).
    conn.execute(
        "INSERT INTO placements (media_type, tmdb_id, section_id, "
        "                        media_folder, placed_at, "
        "                        placement_kind, plex_refreshed, "
        "                        provenance) "
        "VALUES ('movie', 11548, '1', '/data/movies/Adopted', "
        "        ?, 'hardlink', 0, 'manual')",
        (now,),
    )
    conn.commit()
    # Extract the actual SQL the v1.14.32 fix writes. The query
    # is split across 3 adjacent string literals — find the
    # conn.execute( call and concatenate them.
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("async def api_recovery_options(")
    # v1.22.71: slice to the function's actual end — the fixed 12000
    # window went stale when the rk-threading block grew the body.
    body = src[fn_anchor:src.index("\n    @app.", fn_anchor)]
    m_block_anchor = body.index("v1.14.32: switched from")
    m_block = body[m_block_anchor:m_block_anchor + 2000]
    sql = _extract_concat_sql(m_block, "FROM placements")
    # Behavioral: the query MUST find the adopted row.
    found = conn.execute(sql, ("movie", 11548, "1")).fetchone()
    conn.close()
    assert found is not None, (
        "The placements PK query didn't find the adopted row "
        "even though the placements row exists — m_available "
        "would still be True → ADOPT + LET PLEX SERVE would "
        "incorrectly appear. v1.14.32 regressed."
    )


def test_placements_pk_query_returns_none_when_no_placement(tmp_path):
    """Inverse: a row with NO placements entry (pure-M state —
    sidecar at Plex folder but motif didn't place) must STILL
    return None from the query → m_available correctly evaluates
    to True → ADOPT + LET PLEX SERVE option correctly appears."""
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
        "INSERT INTO plex_sections (section_id, title, type, included, "
        "                           is_anime, is_4k, themes_subdir, "
        "                           discovered_at, last_seen_at) "
        "VALUES (?, ?, ?, 1, 0, 0, ?, ?, ?)",
        ("1", "Movies", "movie", "movies", now, now),
    )
    conn.execute(
        "INSERT INTO themes (media_type, tmdb_id, title, year, "
        "                    upstream_source, first_seen_sync_at, "
        "                    last_seen_sync_at) "
        "VALUES ('movie', 22222, 'Pure-M Movie', 1990, "
        "        'imdb', ?, ?)",
        (now, now),
    )
    # NO local_files, NO placements — pure-M state.
    conn.commit()
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("async def api_recovery_options(")
    # v1.22.71: slice to the function's actual end — the fixed 12000
    # window went stale when the rk-threading block grew the body.
    body = src[fn_anchor:src.index("\n    @app.", fn_anchor)]
    m_block_anchor = body.index("v1.14.32: switched from")
    m_block = body[m_block_anchor:m_block_anchor + 2000]
    sql = _extract_concat_sql(m_block, "FROM placements")
    found = conn.execute(sql, ("movie", 22222, "1")).fetchone()
    conn.close()
    # No placement → query returns None → m_available stays True →
    # ADOPT option correctly surfaces. Confirms the fix doesn't
    # over-correct (we still want the option for actual pure-M).
    assert found is None
