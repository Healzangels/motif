"""v1.19.5 — topbar FAIL/UPD pill cycle data routes collection items.

the user's 2026-05-24 follow-up after v1.19.1 deployed: topbar
showed "3 FAIL" but clicking the pill never navigated to the
collections tab. Diagnostic confirmed all 3 failures lived in
collection items.

## Root cause

The Python expressions mapping `ps.type` to a tab name in
/api/stats's `failures.tabs` / `failures.tab_hint` /
`updates.tabs` / `updates.tab_hint` grouped purely by SECTION
type. But plex_sections.type CHECK is constrained to
('movie', 'show') — there is no 'collection' SECTION type.

Collections are ITEMS: `plex_items.media_type = 'collection'`
rows living INSIDE movie/show sections. The /collections route
filters on the ITEM's media_type, not section type. The pill
cycle needs to do the same routing or the cycle silently
misroutes collection-item failures to the wrong tab → 0
visible matches on click.

## Fix

Two parts to each of the 4 breakdown/tab_hint queries:

  1. SQL: SELECT (and GROUP BY for breakdowns) include
     `pi.media_type AS item_media_type`.
  2. Python mapping: check `item_media_type == 'collection'`
     FIRST. If true → 'collections' tab regardless of section
     type. Otherwise fall through to the existing section-type
     mapping.

## What's pinned

- All 4 queries SELECT `pi.media_type AS item_media_type`
- All 4 Python mappings route 'collection' items first
- Markers reference v1.19.5 + the user's 2026-05-24 repro
- End-to-end: insert a failing collection ITEM inside a
  movie SECTION; failures.tabs includes a 'collections' entry
  and failures.tab_hint == 'collections'
- failures.total still equals sum of failures.tabs counts
  (no double-counting or drops)

## Out of scope (deferred)

The plex_enum_active / plex_enum_pending dicts at
api.py:6731+/6753+ are keyed on {movies, tv, anime} ONLY —
no 'collections' key. That's a data-structure change, not a
ternary fix. Flagged for follow-up.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"


# ── Source pins ──────────────────────────────────────────────


def test_failure_breakdown_selects_item_media_type():
    """failure_tab_breakdown_rows must SELECT (and GROUP BY)
    `pi.media_type` so collection items get their own entry."""
    src = API_PY.read_text()
    idx = src.index("failure_tab_breakdown_rows = conn.execute(")
    block = src[idx:idx + 2000]
    assert "pi.media_type AS item_media_type" in block, (
        "v1.19.5: breakdown must SELECT pi.media_type as "
        "item_media_type"
    )
    assert "GROUP BY ps.type, ps.is_anime, ps.is_4k,\n" in block \
        and "pi.media_type" in block, (
        "v1.19.5: GROUP BY must include pi.media_type so each "
        "(section_type, item_type) combo gets its own row"
    )


def test_failure_tab_row_selects_item_media_type():
    """failure_tab_row must also pull item_media_type so the
    tab_hint Python ternary can check it."""
    src = API_PY.read_text()
    idx = src.index("failure_tab_row = conn.execute(")
    block = src[idx:idx + 2000]
    assert "pi.media_type AS item_media_type" in block


def test_update_breakdown_selects_item_media_type():
    """Same SQL change for the UPD pill's breakdown query."""
    src = API_PY.read_text()
    idx = src.index("update_tab_breakdown_rows = conn.execute(")
    block = src[idx:idx + 2500]
    assert "pi.media_type AS item_media_type" in block


def test_update_tab_row_selects_item_media_type():
    """Same SQL change for the UPD pill's tab_hint query."""
    src = API_PY.read_text()
    idx = src.index("update_tab_row = conn.execute(")
    block = src[idx:idx + 2500]
    assert "pi.media_type AS item_media_type" in block


def test_failure_tabs_python_routes_collection_first():
    """The failures.tabs list-comp must check
    item_media_type=='collection' FIRST so collection items
    route to /collections regardless of which section they
    live in."""
    src = API_PY.read_text()
    # Scope tight to the failure_tab_breakdown_rows
    # list-comprehension only — wider lookback catches the
    # earlier updates.tabs block (which has its own
    # 'movies' / 'collections' references).
    idx = src.index("for r in failure_tab_breakdown_rows")
    # Walk back to the opening "{ in the dict literal.
    block_start = src.rfind('"tab":', 0, idx)
    block = src[block_start:idx]
    assert '"collections"' in block, (
        "v1.19.5: tabs mapping must produce 'collections' tab"
    )
    assert "item_media_type" in block and "collection" in block
    # The collection check must come BEFORE the section-type checks
    # (otherwise is_anime / type=='movie' would win).
    coll_pos = block.index("collections")
    movies_pos = block.index('"movies"')
    assert coll_pos < movies_pos, (
        "v1.19.5: 'collections' branch must come BEFORE 'movies' "
        "branch so collection items in movie sections route "
        "correctly"
    )


def test_update_tabs_python_routes_collection_first():
    """Same routing-priority pin for the UPD pill's tabs list."""
    src = API_PY.read_text()
    idx = src.index("for r in update_tab_breakdown_rows")
    block_start = src.rfind('"tab":', 0, idx)
    block = src[block_start:idx]
    coll_pos = block.index("collections")
    movies_pos = block.index('"movies"')
    assert coll_pos < movies_pos


def test_failure_tab_hint_routes_collection_first():
    """failure tab_hint ternary must check item_media_type first."""
    src = API_PY.read_text()
    # Find the failures tab_hint block.
    idx = src.index(
        '"tab_hint": (\n                    "collections" '
        'if failure_tab_row'
    )
    block = src[idx:idx + 600]
    assert "item_media_type" in block and "collection" in block


def test_update_tab_hint_routes_collection_first():
    """Same priority pin for the UPD tab_hint."""
    src = API_PY.read_text()
    idx = src.index(
        '"tab_hint": (\n                    "collections" '
        'if update_tab_row'
    )
    block = src[idx:idx + 600]
    assert "item_media_type" in block and "collection" in block


def test_scheduler_sweep_uses_julianday_for_time_compare():
    """v1.19.5 bonus fix: scheduler's plex_rejected lockout
    filter used `j2.finished_at > datetime('now', '-24 hours')`
    which compares two DIFFERENTLY-FORMATTED strings:
      - finished_at: ISO-T with timezone (`2026-05-24T...+00:00`)
      - datetime(): space-separated, no tz (`2026-05-23 ...`)
    Lexicographic comparison: T (0x54) > space (0x20), so any
    ISO-T timestamp on the same date as the boundary always
    compares "greater" regardless of actual time. v1.18.94's
    lockout silently miscounted failures by time-of-day.
    julianday() converts both sides to numeric Julian day,
    format-agnostic + correct."""
    src = (REPO / "app" / "core" / "scheduler.py").read_text()
    # Find the v1.18.94 plex_rejected lockout filter.
    idx = src.index("lf.last_place_attempt_reason LIKE 'plex_rejected:%'")
    block = src[idx:idx + 2500]
    assert "julianday" in block, (
        "v1.19.5: lockout filter must use julianday() for "
        "finished_at vs boundary comparison (was lex-string "
        "datetime() which is format-sensitive)"
    )
    # Specifically must compare via julianday on BOTH sides.
    assert "julianday(j2.finished_at)" in block
    assert "julianday('now'" in block


def test_marker_references_repro_context():
    """v1.19.5 markers must explain the WHY (collection items
    live inside movie/show sections, route by item type)."""
    src = API_PY.read_text()
    pos = 0
    blocks = []
    while True:
        try:
            i = src.index("v1.19.5", pos)
        except ValueError:
            break
        blocks.append(src[i:i + 800])
        pos = i + 7
    combined = "\n".join(blocks)
    assert "2026-05-24" in combined or "the user" in combined, (
        "v1.19.5: at least one marker must reference the repro"
    )
    assert "collection" in combined.lower()
    # Must mention the item-vs-section distinction.
    assert "section" in combined.lower()


# ── End-to-end ───────────────────────────────────────────────


@pytest.fixture
def admin_client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web.api import create_app
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    db = settings.db_path
    init_db(db)
    init_auth_schema(db)
    create_admin(db, username="testadmin", password="testpassword")
    return TestClient(create_app(settings))


AUTH = {"X-Authentik-Username": "testadmin"}


def _settings_db(tmp_path):
    from app.config import Settings
    return Settings(
        config_dir=tmp_path, data_dir=tmp_path / "data",
    ).db_path


def _seed_collection_failure(conn, *, theme_id, tmdb_id, rk,
                              section_id="1"):
    """Theme + plex_items row for a collection ITEM living
    inside a movie-typed section. Matches the user's repro
    shape exactly."""
    # v1.19.5 test note: use 'video_removed' so this row
    # satisfies BOTH failure_tab_row's narrow filter
    # (failure_kind IN 4 specific kinds) AND
    # failure_tab_breakdown_rows's broad filter (IS NOT NULL).
    # Pre-existing v1.13.84-era drift between the two queries;
    # see audit follow-up note in journal entry.
    conn.execute(
        "INSERT INTO themes "
        "  (id, media_type, tmdb_id, title, upstream_source, "
        "   last_seen_sync_at, first_seen_sync_at, "
        "   failure_kind, failure_acked_at) "
        "VALUES (?, 'collection', ?, ?, 'imdb', "
        "        '2026-05-24T00:00:00', '2026-05-24T00:00:00', "
        "        'video_removed', NULL)",
        (theme_id, tmdb_id, f"Coll-{tmdb_id}"),
    )
    conn.execute(
        "INSERT INTO plex_items "
        "  (rating_key, section_id, media_type, theme_id, "
        "   guid_imdb, guid_tmdb, title, year, has_theme, "
        "   local_theme_file, folder_path, "
        "   plex_independent_theme, first_seen_at, "
        "   last_seen_at) "
        "VALUES (?, ?, 'collection', ?, "
        "        ?, ?, ?, 2020, 0, 0, "
        "        '', 0, "
        "        '2026-05-24T00:00:00', "
        "        '2026-05-24T00:00:00')",
        (rk, section_id, theme_id,
         f"tt{tmdb_id}", tmdb_id, f"Coll-{tmdb_id}"),
    )


def test_collection_failure_appears_in_failures_tabs(
    admin_client, tmp_path,
):
    """the user's exact repro. Insert a collection ITEM with a
    failure inside a movie-typed section. /api/stats
    failures.tabs must include a 'collections' entry."""
    db = _settings_db(tmp_path)
    with sqlite3.connect(db) as conn:
        # Movie SECTION (the only allowed section type per
        # the CHECK constraint — no 'collection' section type).
        conn.execute(
            "INSERT INTO plex_sections "
            "  (section_id, title, type, is_anime, is_4k, "
            "   themes_subdir, included, "
            "   discovered_at, last_seen_at) "
            "VALUES ('1', 'Movies', 'movie', 0, 0, "
            "        'movies', 1, "
            "        '2026-05-24T00:00:00', "
            "        '2026-05-24T00:00:00')"
        )
        # Collection ITEM (media_type='collection') inside it.
        _seed_collection_failure(
            conn, theme_id=88, tmdb_id=500, rk='coll-1',
        )
        conn.commit()

    r = admin_client.get("/api/stats", headers=AUTH)
    assert r.status_code == 200, r.text
    failures = r.json().get("failures", {})

    # Sanity: canonical count picked it up.
    assert failures.get("total", 0) >= 1, (
        f"v1.19.5 sanity: canonical FAIL count must see the "
        f"failure; got total={failures.get('total')}"
    )

    # Bug: tabs[] must include a 'collections' entry.
    tabs = failures.get("tabs", [])
    coll_entries = [
        t for t in tabs if t.get("tab") == "collections"
    ]
    assert coll_entries, (
        f"v1.19.5: collection-item failure must surface in "
        f"failures.tabs as tab='collections' even though it "
        f"lives in a movie SECTION; got tabs={tabs}"
    )
    assert coll_entries[0]["count"] >= 1


def test_collection_failure_drives_tab_hint(
    admin_client, tmp_path,
):
    """When the ONLY failure is a collection item, tab_hint
    must be 'collections'. Pre-fix it would have been 'movies'
    (the section type) and the static fallback href would
    have pointed to /movies → 0 matches."""
    db = _settings_db(tmp_path)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections "
            "  (section_id, title, type, is_anime, is_4k, "
            "   themes_subdir, included, "
            "   discovered_at, last_seen_at) "
            "VALUES ('1', 'Movies', 'movie', 0, 0, "
            "        'movies', 1, "
            "        '2026-05-24T00:00:00', "
            "        '2026-05-24T00:00:00')"
        )
        _seed_collection_failure(
            conn, theme_id=88, tmdb_id=500, rk='coll-1',
        )
        conn.commit()

    r = admin_client.get("/api/stats", headers=AUTH)
    failures = r.json()["failures"]
    assert failures.get("tab_hint") == "collections", (
        f"v1.19.5: tab_hint must be 'collections' when the only "
        f"failure is a collection ITEM (even if its section "
        f"type='movie'); got {failures.get('tab_hint')!r}"
    )


def test_failures_total_matches_sum_with_collections(
    admin_client, tmp_path,
):
    """failures.total == sum of failures.tabs[*].count. The
    v1.19.5 fix adds GROUP BY pi.media_type but must not
    double-count or drop."""
    db = _settings_db(tmp_path)
    with sqlite3.connect(db) as conn:
        # Movie section.
        conn.execute(
            "INSERT INTO plex_sections "
            "  (section_id, title, type, is_anime, is_4k, "
            "   themes_subdir, included, "
            "   discovered_at, last_seen_at) "
            "VALUES ('1', 'Movies', 'movie', 0, 0, "
            "        'movies', 1, "
            "        '2026-05-24T00:00:00', "
            "        '2026-05-24T00:00:00')"
        )
        # 2 collection-item failures.
        _seed_collection_failure(
            conn, theme_id=88, tmdb_id=500, rk='c1',
        )
        _seed_collection_failure(
            conn, theme_id=89, tmdb_id=501, rk='c2',
        )
        # 1 movie-item failure in the same section.
        conn.execute(
            "INSERT INTO themes "
            "  (id, media_type, tmdb_id, title, upstream_source, "
            "   last_seen_sync_at, first_seen_sync_at, "
            "   failure_kind) "
            "VALUES (90, 'movie', 502, 'Movie X', 'imdb', "
            "        '2026-05-24T00:00:00', "
            "        '2026-05-24T00:00:00', "
            "        'unavailable')"
        )
        conn.execute(
            "INSERT INTO plex_items "
            "  (rating_key, section_id, media_type, theme_id, "
            "   guid_imdb, guid_tmdb, title, year, has_theme, "
            "   local_theme_file, folder_path, "
            "   plex_independent_theme, first_seen_at, "
            "   last_seen_at) "
            "VALUES ('mv-1', '1', 'movie', 90, "
            "        'tt502', 502, 'Movie X', 2020, 0, 0, "
            "        '/data/movies/Movie X', 0, "
            "        '2026-05-24T00:00:00', "
            "        '2026-05-24T00:00:00')"
        )
        conn.commit()

    r = admin_client.get("/api/stats", headers=AUTH)
    failures = r.json()["failures"]
    total = failures["total"]
    breakdown_sum = sum(
        t.get("count", 0) for t in failures.get("tabs", [])
    )
    assert total == breakdown_sum, (
        f"v1.19.5: failures.total ({total}) must equal sum of "
        f"failures.tabs counts ({breakdown_sum}). If they "
        f"diverge, the item_media_type grouping is adding or "
        f"dropping rows incorrectly."
    )
    # Specifically: collections=2, movies=1.
    coll = [t for t in failures["tabs"] if t.get("tab") == "collections"]
    assert coll and coll[0]["count"] == 2, (
        f"v1.19.5: expected collections count=2; got {coll}"
    )
    mov = [t for t in failures["tabs"] if t.get("tab") == "movies"]
    assert mov and mov[0]["count"] == 1, (
        f"v1.19.5: expected movies count=1 (the non-collection "
        f"item); got {mov}"
    )
