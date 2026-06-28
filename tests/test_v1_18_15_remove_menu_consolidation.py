"""v1.18.15 — REMOVE menu consolidation: drop DELETE, wire CLEAR URL auto-evict.

the user's audit on v1.18.13 logs:

  > "I funder Del, Unmanage Purge, but not sure what our extra
  > Delete is. Reading what it does makes sense but I'm not a
  > fan of the double delete, I think the Clear URL might have
  > been better or made more sense."

The v1.18.8 wire-up added a separate `delete-orphan` (DELETE)
button alongside PURGE on orphan rows so the user had a path to
fully clear a zombie orphan + its captured URL chain. That made
the chain reachable but introduced a confusing two-destructive-
button state on active orphans where PURGE and DELETE both fired
the same way modulo the RESTORE preservation.

## v1.18.15 design

Drop DELETE entirely. PURGE and CLEAR URL compose to achieve the
same end state:

  PURGE on an active orphan
    → captures user_youtube_url into previous_urls
    → drops user_overrides + local_files + placements
    → NULLs themes.youtube_url + youtube_video_id (zombie state)
    → row stays so RESTORE keeps working

  CLEAR URL on the zombie orphan (v1.18.15)
    → drops the targeted previous_urls row
    → checks if the orphan has ANY remaining state across
      (user_overrides, local_files, placements, previous_urls)
    → if NONE remain: drops the themes row + FK CASCADE clears
      the children + plex_items.theme_id SET NULL

Each button is one surgical primitive. The composition (PURGE →
CLEAR URL) replaces the v1.18.8 one-step DELETE. The
`api_delete_item` endpoint stays in place as a power-user API
path but is no longer surfaced in the UI.

## Tests

  * **Frontend pins:** DELETE menu entry gone, `delete-orphan`
    handler gone, `deleteOrphan()` helper gone, CLEAR URL
    tooltip mentions the zombie auto-evict.
  * **Backend pins:** `_maybe_evict_empty_orphan` helper exists
    and the four guards are checked, `api_clear_url_override`
    returns `zombie_evicted` flag, `api_delete_item` endpoint
    still defined.
  * **End-to-end:** seed a zombie orphan with a previous_urls
    row, call CLEAR URL, assert themes row is gone + cascade
    cleared children. Seed a non-zombie orphan (still has
    placement), call CLEAR URL, assert row survives. Seed a
    TDB row (upstream='themoviedb'), call CLEAR URL, assert
    row survives regardless.
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

APP_JS = REPO / "app" / "web" / "static" / "app.js"
API_PY = REPO / "app" / "web" / "api.py"


# ── Frontend: DELETE entry + helper removed ───────────────────


def test_delete_menu_entry_removed():
    """The DELETE menu push must be gone from app.js."""
    js = APP_JS.read_text()
    assert "menuItemHtml('delete-orphan'" not in js
    assert "'delete-orphan', 'DELETE'" not in js


def test_delete_orphan_handler_removed():
    """The `else if (act === 'delete-orphan')` action handler
    must be gone — once the menu entry is removed it's dead code
    that calls a removed helper."""
    js = APP_JS.read_text()
    assert "act === 'delete-orphan'" not in js


def test_delete_orphan_helper_removed():
    """The `deleteOrphan()` function definition must be gone."""
    js = APP_JS.read_text()
    assert "async function deleteOrphan(" not in js
    assert "function deleteOrphan(" not in js


def test_v1_18_15_marker_present():
    """A `v1.18.15` marker comment must anchor the removal in
    app.js so future grep for the version finds the change."""
    js = APP_JS.read_text()
    assert "v1.18.15:" in js


# ── Frontend: CLEAR URL tooltip mentions zombie auto-evict ────


def test_clear_url_tooltip_mentions_zombie_evict():
    """The CLEAR URL tooltip must inform the user that on a
    zombie orphan, the empty row will be tidied away. Without
    this the post-PURGE → CLEAR URL composition's row-removal
    side effect is invisible UX."""
    js = APP_JS.read_text()
    # The new tooltip branch for zombie orphans.
    assert "isZombieOrphan" in js, (
        "v1.18.15: tooltip must branch on isZombieOrphan state "
        "to surface the auto-evict outcome"
    )
    assert "tidied away" in js, (
        "v1.18.15: tooltip must tell user the empty row is "
        "removed on CLEAR URL of a zombie orphan"
    )


def test_clear_url_zombie_orphan_gate_definition():
    """The isZombieOrphan computation must check ALL four state
    fields: not downloaded, not placed, no user URL, no TDB URL.
    A row with ANY of those isn't a zombie and the regular
    tooltip should apply."""
    js = APP_JS.read_text()
    idx = js.index("const isZombieOrphan")
    block = js[idx:idx + 400]
    assert "!downloaded" in block
    assert "!placed" in block
    assert "!it.user_youtube_url" in block
    assert "!it.youtube_url" in block


# ── Backend: _maybe_evict_empty_orphan helper ─────────────────


def test_evict_helper_exists():
    """`_maybe_evict_empty_orphan` must be defined in api.py."""
    src = API_PY.read_text()
    assert "def _maybe_evict_empty_orphan(" in src


def test_evict_helper_checks_all_four_tables():
    """The eviction guard must check all four child tables —
    user_overrides, local_files, placements, previous_urls —
    before dropping the themes row. Missing any one would
    orphan rows in that table after eviction."""
    src = API_PY.read_text()
    fn_idx = src.index("def _maybe_evict_empty_orphan(")
    # Bound by next top-level def to keep the slice clean.
    next_def = src.index("\ndef ", fn_idx + 1)
    body = src[fn_idx:next_def]
    for table in (
        '"user_overrides"', '"local_files"', '"placements"',
        '"previous_urls"',
    ):
        assert table in body, (
            f"v1.18.15: _maybe_evict_empty_orphan must check "
            f"{table} for residual state before dropping the row"
        )


def test_evict_helper_only_targets_plex_orphan():
    """Eviction must short-circuit for non-orphan rows — real
    TDB rows would just be re-created on the next sync, and
    silently dropping them would surprise the user."""
    src = API_PY.read_text()
    fn_idx = src.index("def _maybe_evict_empty_orphan(")
    next_def = src.index("\ndef ", fn_idx + 1)
    body = src[fn_idx:next_def]
    assert 'upstream_source"] != "plex_orphan"' in body, (
        "v1.18.15: _maybe_evict_empty_orphan must return False "
        "early for non-orphan rows"
    )


def test_clear_url_endpoint_wires_evict():
    """`api_clear_url_override` must call _maybe_evict_empty_orphan
    after the previous_url delete, inside the same transaction."""
    src = API_PY.read_text()
    fn_idx = src.index("async def api_clear_url_override(")
    body = src[fn_idx:fn_idx + 6000]
    assert "_maybe_evict_empty_orphan(" in body, (
        "v1.18.15: clear-url endpoint must invoke the evict helper"
    )
    assert "zombie_evicted" in body, (
        "v1.18.15: clear-url response must surface the evict "
        "outcome so the frontend / audit log can react"
    )


def test_api_delete_item_endpoint_still_exists():
    """The api_delete_item endpoint must remain in place as a
    power-user API path even though the UI no longer surfaces
    it. Removing the endpoint would break programmatic users
    + cron scripts that may depend on it."""
    src = API_PY.read_text()
    assert "async def api_delete_item(" in src


# ── End-to-end: CLEAR URL eviction behavior ───────────────────


@pytest.fixture
def zombie_orphan_fixture(tmp_path: Path):
    """Seed an orphan in zombie state: themes row + previous_urls
    row only. No user_overrides, local_files, or placements.
    This mirrors the post-PURGE state the user's M*A*S*H row would
    enter after a PURGE click in the v1.18.15 model."""
    db_path = tmp_path / "motif.db"
    from app.core.db import init_db
    init_db(db_path)
    ts = "2026-05-20T14:00:00"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO plex_sections "
            "  (section_id, title, type, is_anime, is_4k, "
            "   themes_subdir, included, discovered_at, last_seen_at) "
            "VALUES ('1', 'Movies', 'movie', 0, 0, "
            "        'movies', 1, ?, ?)",
            (ts, ts),
        )
        conn.execute(
            "INSERT INTO themes "
            "  (media_type, tmdb_id, title, title_norm, year, "
            "   upstream_source, "
            "   last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('movie', -42, 'Zombie Orphan', "
            "        'zombie orphan', '2020', 'plex_orphan', ?, ?)",
            (ts, ts),
        )
        # Post-PURGE captured URL.
        conn.execute(
            "INSERT INTO previous_urls "
            "  (media_type, tmdb_id, section_id, youtube_url, "
            "   kind, captured_at) "
            "VALUES ('movie', -42, '1', "
            "        'https://youtube.com/watch?v=AAAAAAAAAAA', "
            "        'user', ?)",
            (ts,),
        )
        conn.commit()
    return db_path


@pytest.fixture
def active_orphan_with_previous_fixture(tmp_path: Path):
    """Seed an orphan that's still active (has a placement) but
    has a captured previous_url from an earlier SET URL change.
    CLEAR URL on this row should drop only the previous_urls row
    — the themes row must survive because the orphan has live
    state."""
    db_path = tmp_path / "motif.db"
    from app.core.db import init_db
    init_db(db_path)
    ts = "2026-05-20T14:00:00"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO plex_sections "
            "  (section_id, title, type, is_anime, is_4k, "
            "   themes_subdir, included, discovered_at, last_seen_at) "
            "VALUES ('1', 'Movies', 'movie', 0, 0, "
            "        'movies', 1, ?, ?)",
            (ts, ts),
        )
        conn.execute(
            "INSERT INTO themes "
            "  (media_type, tmdb_id, title, title_norm, year, "
            "   upstream_source, "
            "   last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('movie', -43, 'Active Orphan', "
            "        'active orphan', '2020', 'plex_orphan', ?, ?)",
            (ts, ts),
        )
        # Active state: a placement still exists.
        conn.execute(
            "INSERT INTO placements "
            "  (media_type, tmdb_id, section_id, media_folder, "
            "   placement_kind, placed_at, plex_refreshed) "
            "VALUES ('movie', -43, '1', "
            "        '/data/media/movies/Active Orphan (2020)', "
            "        'hardlink', ?, 0)",
            (ts,),
        )
        conn.execute(
            "INSERT INTO previous_urls "
            "  (media_type, tmdb_id, section_id, youtube_url, "
            "   kind, captured_at) "
            "VALUES ('movie', -43, '1', "
            "        'https://youtube.com/watch?v=PREVIOUSAAA', "
            "        'user', ?)",
            (ts,),
        )
        conn.commit()
    return db_path


@pytest.fixture
def tdb_row_with_previous_fixture(tmp_path: Path):
    """Seed a real TDB-tracked row with a captured previous URL.
    CLEAR URL must NEVER evict TDB rows — the next sync would
    just re-create them."""
    db_path = tmp_path / "motif.db"
    from app.core.db import init_db
    init_db(db_path)
    ts = "2026-05-20T14:00:00"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO plex_sections "
            "  (section_id, title, type, is_anime, is_4k, "
            "   themes_subdir, included, discovered_at, last_seen_at) "
            "VALUES ('1', 'Movies', 'movie', 0, 0, "
            "        'movies', 1, ?, ?)",
            (ts, ts),
        )
        conn.execute(
            "INSERT INTO themes "
            "  (media_type, tmdb_id, title, title_norm, year, "
            "   upstream_source, "
            "   last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('movie', 99999, 'Real TDB Movie', "
            "        'real tdb movie', '2020', 'themoviedb', ?, ?)",
            (ts, ts),
        )
        conn.execute(
            "INSERT INTO previous_urls "
            "  (media_type, tmdb_id, section_id, youtube_url, "
            "   kind, captured_at) "
            "VALUES ('movie', 99999, '1', "
            "        'https://youtube.com/watch?v=PREVTDBAAAA', "
            "        'user', ?)",
            (ts,),
        )
        conn.commit()
    return db_path


def test_evict_drops_zombie_orphan(zombie_orphan_fixture):
    """End-to-end: _maybe_evict_empty_orphan on a zombie orphan
    drops the themes row. Cascade clears any FK'd children
    (none in this fixture — pure zombie)."""
    from app.web.api import (
        _maybe_evict_empty_orphan, _clear_previous_url,
    )
    db_path = zombie_orphan_fixture
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        # Mirror the api_clear_url_override flow: first drop the
        # previous_urls row, then run the evict helper.
        _clear_previous_url(conn, "movie", -42, section_id="1")
        evicted = _maybe_evict_empty_orphan(conn, "movie", -42)
        conn.commit()
    assert evicted is True
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT 1 FROM themes "
            "WHERE media_type='movie' AND tmdb_id=-42"
        ).fetchone()
    assert row is None, (
        "v1.18.15: zombie orphan themes row must be evicted"
    )


def test_evict_preserves_active_orphan(active_orphan_with_previous_fixture):
    """End-to-end: an orphan that still owns a placement must
    survive CLEAR URL. Only the previous_urls row gets dropped."""
    from app.web.api import (
        _maybe_evict_empty_orphan, _clear_previous_url,
    )
    db_path = active_orphan_with_previous_fixture
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        _clear_previous_url(conn, "movie", -43, section_id="1")
        evicted = _maybe_evict_empty_orphan(conn, "movie", -43)
        conn.commit()
    assert evicted is False, (
        "v1.18.15: orphan with surviving placement must NOT "
        "be evicted by CLEAR URL"
    )
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT 1 FROM themes "
            "WHERE media_type='movie' AND tmdb_id=-43"
        ).fetchone()
        assert row is not None
        # The placement also survives (we only dropped previous_urls).
        pl = conn.execute(
            "SELECT 1 FROM placements "
            "WHERE media_type='movie' AND tmdb_id=-43"
        ).fetchone()
        assert pl is not None


def test_evict_never_drops_tdb_row(tdb_row_with_previous_fixture):
    """End-to-end: a TDB row must NEVER be evicted regardless of
    its child state. _maybe_evict_empty_orphan gates on
    upstream_source='plex_orphan'."""
    from app.web.api import (
        _maybe_evict_empty_orphan, _clear_previous_url,
    )
    db_path = tdb_row_with_previous_fixture
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        _clear_previous_url(conn, "movie", 99999, section_id="1")
        evicted = _maybe_evict_empty_orphan(conn, "movie", 99999)
        conn.commit()
    assert evicted is False, (
        "v1.18.15: TDB rows must never be evicted — next sync "
        "would re-create them"
    )
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT 1 FROM themes "
            "WHERE media_type='movie' AND tmdb_id=99999"
        ).fetchone()
    assert row is not None


def test_evict_handles_missing_themes_row(tmp_path: Path):
    """Guard: calling _maybe_evict_empty_orphan on a non-existent
    themes row must return False, not raise. Defensive — the
    api_clear_url_override flow could theoretically race a
    concurrent operation."""
    from app.core.db import init_db
    from app.web.api import _maybe_evict_empty_orphan
    db_path = tmp_path / "motif.db"
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        evicted = _maybe_evict_empty_orphan(conn, "movie", -99)
    assert evicted is False
