"""v1.13.78 — topbar UPD pill: fourk-aware route + cycle.

Two related bugs:

  - Wrong landing: clicking the topbar UPD pill went to
    /<tab>?tdb_pills=update with no fourk dimension. The library
    page's variant came from localStorage, which often loaded the
    wrong one (e.g. 4K with 0 matches when the actual pending
    update lived in standard).

  - No rotation: the v1.13.69 FAIL pill cycles through every
    (tab, fourk) pair with un-acked failures on successive clicks.
    UPD didn't have the same affordance — first click landed
    somewhere reasonable, second click did nothing new.

Both fix shapes mirror v1.13.69 for FAIL exactly:
  1. Server: stats.updates.tabs = [{tab, fourk, count}, ...]
     (mirrors stats.failures.tabs)
  2. Client: bindUpdatesBadgeCycle() rotates through the array
     on each click, wraps from last → first.

These tests pin the SQL shape + the JS wiring via static-text
guards (consistent with the v1.13.77 pattern).
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


def _insert_section(conn, *, section_id: str, type_: str,
                    is_anime: int = 0, is_4k: int = 0):
    """Plex section seed for the breakdown JOIN."""
    now = _now_iso()
    conn.execute(
        "INSERT INTO plex_sections ("
        "  section_id, title, type, included, discovered_at, last_seen_at,"
        "  is_4k, is_anime"
        ") VALUES (?, ?, ?, 1, ?, ?, ?, ?)",
        (section_id, f"{type_}-{section_id}", type_, now, now,
         is_4k, is_anime),
    )


def _insert_pending_update(conn, *, tmdb_id: int, media_type: str,
                           section_id: str, plex_media_type: str):
    """Theme + plex_items + pending_updates row that the breakdown
    query joins through. plex_media_type follows the schema's
    'show'/'movie' (vs themes.media_type which is 'tv'/'movie')."""
    now = _now_iso()
    conn.execute(
        "INSERT INTO themes ("
        "  media_type, tmdb_id, title, upstream_source,"
        "  last_seen_sync_at, first_seen_sync_at"
        ") VALUES (?, ?, ?, 'imdb', ?, ?)",
        (media_type, tmdb_id, f"x{tmdb_id}", now, now),
    )
    conn.execute(
        "INSERT INTO plex_items ("
        "  rating_key, section_id, media_type, title, guid_tmdb,"
        "  first_seen_at, last_seen_at"
        ") VALUES (?, ?, ?, ?, ?, ?, ?)",
        (f"rk{tmdb_id}", section_id, plex_media_type,
         f"x{tmdb_id}", tmdb_id, now, now),
    )
    conn.execute(
        "INSERT INTO pending_updates ("
        "  media_type, tmdb_id, decision, detected_at"
        ") VALUES (?, ?, 'pending', ?)",
        (media_type, tmdb_id, now),
    )


def _breakdown(db: Path):
    """Mirror the v1.13.78 update_tab_breakdown_rows query so the
    SQL stays pinned by the test."""
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute("""
            SELECT ps.type, ps.is_anime, ps.is_4k, COUNT(*) AS n
            FROM pending_updates pu
            JOIN plex_items pi
              ON pi.guid_tmdb = pu.tmdb_id
             AND pi.media_type = (CASE pu.media_type WHEN 'tv' THEN 'show' ELSE pu.media_type END)
            JOIN plex_sections ps
              ON ps.section_id = pi.section_id AND ps.included = 1
            WHERE pu.decision = 'pending'
            GROUP BY ps.type, ps.is_anime, ps.is_4k
            ORDER BY ps.is_anime, ps.type, ps.is_4k
        """).fetchall()
    return [{"type": r["type"], "is_anime": bool(r["is_anime"]),
             "is_4k": bool(r["is_4k"]), "n": r["n"]} for r in rows]


# ── breakdown SQL ────────────────────────────────────────────

def test_breakdown_partitions_standard_vs_fourk(db):
    """Same tab, two variants — should produce two rows."""
    with sqlite3.connect(db) as conn:
        _insert_section(conn, section_id="1", type_="movie", is_4k=0)
        _insert_section(conn, section_id="2", type_="movie", is_4k=1)
        _insert_pending_update(conn, tmdb_id=1, media_type="movie",
                               section_id="1", plex_media_type="movie")
        _insert_pending_update(conn, tmdb_id=2, media_type="movie",
                               section_id="2", plex_media_type="movie")
    rows = _breakdown(db)
    assert len(rows) == 2
    standard = next(r for r in rows if not r["is_4k"])
    fourk = next(r for r in rows if r["is_4k"])
    assert standard["n"] == 1
    assert fourk["n"] == 1


def test_breakdown_excludes_decided_updates(db):
    """Only decision='pending' counts — accepted / declined are
    historical and shouldn't surface in the badge."""
    with sqlite3.connect(db) as conn:
        _insert_section(conn, section_id="1", type_="movie", is_4k=0)
        _insert_pending_update(conn, tmdb_id=1, media_type="movie",
                               section_id="1", plex_media_type="movie")
        # Mark accepted so the row drops out of the count.
        conn.execute(
            "UPDATE pending_updates SET decision='accepted' "
            "WHERE tmdb_id = 1",
        )
    assert _breakdown(db) == []


def test_breakdown_excludes_unmanaged_sections(db):
    """included=0 sections must not contribute to the count —
    matches the WHERE ps.included=1 filter."""
    with sqlite3.connect(db) as conn:
        # Insert section as included=0 directly (bypasses helper).
        now = _now_iso()
        conn.execute(
            "INSERT INTO plex_sections ("
            "  section_id, title, type, included, discovered_at,"
            "  last_seen_at, is_4k, is_anime"
            ") VALUES ('1', 'x', 'movie', 0, ?, ?, 0, 0)",
            (now, now),
        )
        _insert_pending_update(conn, tmdb_id=1, media_type="movie",
                               section_id="1", plex_media_type="movie")
    assert _breakdown(db) == []


def test_breakdown_handles_tv_and_anime_split(db):
    """TV and anime live on different sections (is_anime=1) but
    both have plex_media_type='show'. Breakdown should partition
    on is_anime so the JS picks 'tv' vs 'anime' correctly."""
    with sqlite3.connect(db) as conn:
        _insert_section(conn, section_id="1", type_="show", is_anime=0)
        _insert_section(conn, section_id="2", type_="show", is_anime=1)
        _insert_pending_update(conn, tmdb_id=10, media_type="tv",
                               section_id="1", plex_media_type="show")
        _insert_pending_update(conn, tmdb_id=20, media_type="tv",
                               section_id="2", plex_media_type="show")
    rows = _breakdown(db)
    tv = [r for r in rows if not r["is_anime"]]
    anime = [r for r in rows if r["is_anime"]]
    assert len(tv) == 1 and tv[0]["n"] == 1
    assert len(anime) == 1 and anime[0]["n"] == 1


def test_breakdown_orders_by_anime_type_4k(db):
    """ORDER BY ps.is_anime, ps.type, ps.is_4k pins the click-cycle
    sequence so users see Movies → 4K Movies → TV → 4K TV →
    Anime → 4K Anime in a predictable order each time."""
    with sqlite3.connect(db) as conn:
        # Insert in deliberately scrambled order to verify ORDER BY
        # is what produces the deterministic sequence (not insertion).
        _insert_section(conn, section_id="6", type_="show", is_anime=1, is_4k=1)
        _insert_section(conn, section_id="1", type_="movie")
        _insert_section(conn, section_id="3", type_="show")
        _insert_section(conn, section_id="2", type_="movie", is_4k=1)
        _insert_section(conn, section_id="5", type_="show", is_anime=1)
        _insert_section(conn, section_id="4", type_="show", is_4k=1)
        for tid, sid, mt, pmt in [
            (1, "1", "movie", "movie"),
            (2, "2", "movie", "movie"),
            (3, "3", "tv", "show"),
            (4, "4", "tv", "show"),
            (5, "5", "tv", "show"),
            (6, "6", "tv", "show"),
        ]:
            _insert_pending_update(conn, tmdb_id=tid, media_type=mt,
                                   section_id=sid, plex_media_type=pmt)
    rows = _breakdown(db)
    sequence = [(r["type"], r["is_anime"], r["is_4k"]) for r in rows]
    assert sequence == [
        ("movie", False, False),
        ("movie", False, True),
        ("show",  False, False),
        ("show",  False, True),
        ("show",  True,  False),
        ("show",  True,  True),
    ]


# ── /api/stats response shape ────────────────────────────────

def test_api_stats_response_includes_updates_tabs():
    """The /api/stats response must surface stats.updates.tabs so
    the JS layer can read it. Static guard against a refactor that
    drops the field."""
    api_py = (Path(__file__).resolve().parent.parent
              / "app" / "web" / "api.py").read_text()
    # Pin the SQL alias rename + the response-field assembly.
    assert "update_tab_breakdown_rows" in api_py
    # The new tabs key must appear inside the "updates": {...} block.
    updates_block_start = api_py.index('"updates": {')
    next_block = api_py.index('"failures":', updates_block_start)
    updates_block = api_py[updates_block_start:next_block]
    assert '"tabs":' in updates_block, (
        "v1.13.78 added a tabs array to stats.updates so the topbar "
        "UPD pill can cycle and pick the right fourk variant — "
        "removing it would resurrect the wrong-landing bug"
    )


# ── JS wiring ────────────────────────────────────────────────

def test_upd_badge_href_includes_fourk_param():
    """Pre-fix the href was `/${tabHint}?tdb_pills=update` — no
    fourk. Post-fix it includes the fourk param so the library
    page doesn't fall back to localStorage's variant.

    v1.13.79: deep-link param migrated tdb_pills=update →
    attn_pills=update (FAIL pill made the same migration in
    v1.13.68; UPD missed it). The fourk-included pattern
    survives; only the chip-axis param changed."""
    js = (Path(__file__).resolve().parent.parent
          / "app" / "web" / "static" / "app.js").read_text()
    # Old broken pattern (tab + ANY *_pills=update only, no fourk)
    # must NOT reappear.
    assert "/${tabHint}?tdb_pills=update`" not in js
    assert "/${tabHint}?attn_pills=update`" not in js
    # New pattern: fourk-included AND attn-axis (v1.13.79).
    assert "/${firstUpdTab}?fourk=${firstUpdFourk}&attn_pills=update" in js


def test_upd_badge_data_tabs_attribute_is_set():
    """The cycle handler reads dataset.updTabs. Static guard that
    the data-* write happens during refreshTopbarStatus."""
    js = (Path(__file__).resolve().parent.parent
          / "app" / "web" / "static" / "app.js").read_text()
    assert "updBadge.dataset.updTabs = JSON.stringify(updBreakdown)" in js


def test_bind_updates_badge_cycle_exists_and_is_wired():
    """v1.24.48: the UPD cycle converged onto the shared bindBadgeCycle (was its
    own bindUpdatesBadgeCycle copy). It must be wired from the init block with the
    UPD badge id + dataset key + the attn_pills=update deep-link."""
    js = (Path(__file__).resolve().parent.parent
          / "app" / "web" / "static" / "app.js").read_text()
    assert "function bindBadgeCycle(" in js
    assert "bindBadgeCycle('topbar-updates-badge', 'updTabs', 'attn_pills=update');" in js


def test_upd_cycle_handler_targets_attn_pills_update():
    """The cycle handler's navigation URL must use the deep-link that lights up
    the blue ! ATTN chip on landing. v1.13.79: was tdb_pills=update; migrated to
    attn_pills=update. v1.24.48: the deep-link is the `query` arg passed to the
    shared bindBadgeCycle at the UPD init site; the cycle body builds &${query}."""
    js = (Path(__file__).resolve().parent.parent
          / "app" / "web" / "static" / "app.js").read_text()
    assert "bindBadgeCycle('topbar-updates-badge', 'updTabs', 'attn_pills=update')" in js
    cycle_start = js.index("function bindBadgeCycle(")
    cycle_body = js[cycle_start:cycle_start + 1500]
    assert "fourk=${next.fourk ? '1' : '0'}" in cycle_body
    assert "&${query}`" in cycle_body
