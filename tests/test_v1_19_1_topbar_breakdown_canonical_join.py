"""v1.19.1 — topbar FAIL/UPD breakdown queries use canonical join.

the user's 2026-05-23 repro: topbar showed "3 FAIL" but clicking
landed on a /movies page with 0 matches. Filter chip lit up
correctly (STATUS ▲ red triangle) but the library was empty.

## Root cause

Four breakdown queries in /api/stats — the data driving the
topbar pill click-cycle target — were using the OLD join shape
`pi.guid_tmdb = t.tmdb_id + media_type` that v1.15.62 retired
for the canonical FAIL count.

Canonical (`_FAILURES_SFA_FROM_SQL`):
  FROM themes t JOIN plex_items pi ON pi.theme_id = t.id

Breakdowns (pre-fix):
  FROM themes t JOIN plex_items pi
    ON pi.guid_tmdb = t.tmdb_id
   AND pi.media_type = (CASE t.media_type WHEN 'tv' THEN 'show'
                                          ELSE t.media_type END)

These return different row sets. Orphan-adopted themes (where
pi.theme_id is back-stamped but pi.guid_tmdb is NULL) appear
in the canonical count but were INVISIBLE to the breakdowns.

Result:
  - Topbar count: 3 (canonical query — sees orphan failures)
  - Breakdown: [] (broken — misses orphans entirely)
  - failBadge.dataset.failTabs = [] → empty cycle data
  - JS falls back to default `tab_hint` (also broken — uses
    the same broken join)
  - Click lands on /movies?attn_pills=fail
  - Library FROM uses canonical pi.theme_id = t.id (correct!)
    but the tab_where filter (movies-only) hides failures
    that live in other sections

Net visible: count says 3, click lands on wrong tab, page
shows 0 matches. Classic contract-drift bug.

## What was missed in v1.19.0 audit

Track 2 of the v1.19.0 audit reported "SRC letter axis aligned
across all 5 sites" + "placement-kind alignment intact" but did
NOT enumerate the topbar breakdown queries. The drift had been
sitting there since v1.15.62 (~80 tags). The audit needs a
mirror-principle guard test to fail loud when a future query
diverges from the canonical join shape.

## Fix

Migrate all 4 breakdown queries to the canonical join:
  - failure_tab_row (tab_hint for FAIL pill)
  - failure_tab_breakdown_rows (per-tab cycle data for FAIL)
  - update_tab_row (tab_hint for UPD pill)
  - update_tab_breakdown_rows (per-tab cycle data for UPD)

## What's pinned

- All 4 queries use pi.theme_id = t.id (not pi.guid_tmdb)
- No remaining pi.guid_tmdb = t.tmdb_id joins in the breakdown
  region of api.py (audit guard)
- v1.19.1 markers reference the v1.15.62 canonical migration
- End-to-end: seed an orphan-adopted failing theme, verify
  /api/stats failures.tabs has an entry for it
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"


# ── Source-level pins ────────────────────────────────────────


def test_failure_breakdown_queries_use_canonical_join():
    """Both failure-axis breakdown queries (tab_hint +
    per-tab cycle) must use the v1.15.62 canonical
    `pi.theme_id = t.id` join, NOT the retired
    `pi.guid_tmdb = t.tmdb_id` shape."""
    src = API_PY.read_text()
    # Locate the failure_tab_row query.
    f_row_idx = src.index("failure_tab_row = conn.execute(")
    f_row_block = src[f_row_idx:f_row_idx + 1500]
    assert "pi.theme_id = t.id" in f_row_block, (
        "v1.19.1: failure_tab_row must use canonical theme_id join"
    )
    assert "pi.guid_tmdb = t.tmdb_id" not in f_row_block, (
        "v1.19.1: failure_tab_row must NOT use the retired "
        "guid_tmdb join — drifts from _FAILURES_SFA_FROM_SQL"
    )
    # Same for failure_tab_breakdown_rows.
    f_bd_idx = src.index("failure_tab_breakdown_rows = conn.execute(")
    f_bd_block = src[f_bd_idx:f_bd_idx + 1500]
    assert "pi.theme_id = t.id" in f_bd_block
    assert "pi.guid_tmdb = t.tmdb_id" not in f_bd_block


def test_update_breakdown_queries_use_canonical_join():
    """Both update-axis breakdown queries (tab_hint +
    per-tab cycle) must use the same canonical join. Same
    drift class — UPD pill cycle data uses the same shape
    as FAIL pill, so the fix applies symmetrically."""
    src = API_PY.read_text()
    u_row_idx = src.index("update_tab_row = conn.execute(")
    u_row_block = src[u_row_idx:u_row_idx + 2500]
    assert "pi.theme_id = t.id" in u_row_block
    assert "pi.guid_tmdb = t.tmdb_id" not in u_row_block

    u_bd_idx = src.index("update_tab_breakdown_rows = conn.execute(")
    u_bd_block = src[u_bd_idx:u_bd_idx + 2500]
    assert "pi.theme_id = t.id" in u_bd_block
    assert "pi.guid_tmdb = t.tmdb_id" not in u_bd_block


def test_breakdown_queries_marker_references_v1_15_62():
    """The v1.19.1 marker must reference the v1.15.62 canonical
    migration so future archaeologists understand the lineage —
    we're not introducing a new join shape, we're closing a
    pre-existing drift to the EXISTING canonical shape."""
    src = API_PY.read_text()
    # Anchor on the v1.19.1 canonical-join fix block specifically
    # (avoid false-matching v1.19.12 / v1.19.13 / v1.19.60 helpers
    # whose markers also start with "v1.19.1" as a substring).
    idx = src.index("v1.19.1: canonical-join")
    block = src[idx:idx + 2500]
    assert "v1.15.62" in block, (
        "v1.19.1: marker must reference the v1.15.62 canonical "
        "migration that the breakdown queries had drifted from"
    )
    assert "_FAILURES_SFA_FROM_SQL" in block, (
        "v1.19.1: marker should name the canonical SQL constant "
        "so a grep finds the relationship"
    )


def test_no_remaining_drift_in_breakdown_region():
    """Audit guard: scan the /api/stats breakdown region (lines
    ~6240-6500) for any remaining `pi.guid_tmdb = t.tmdb_id`
    joins. If a future patch reintroduces the drift in a new
    breakdown query, this catches it."""
    src = API_PY.read_text()
    # The breakdown region starts at failure_tab_row and ends
    # before the next major section. Use the failure_tab_row
    # anchor as the start; cap at +6000 chars to cover all 4
    # queries + their detail builders.
    region_start = src.index("failure_tab_row = conn.execute(")
    region_end = region_start + 6000
    region = src[region_start:region_end]
    # No drifted joins should remain.
    assert "pi.guid_tmdb = t.tmdb_id" not in region, (
        "v1.19.1: breakdown region must not contain any "
        "guid_tmdb-based joins (use pi.theme_id = t.id, the "
        "canonical shape from _FAILURES_SFA_FROM_SQL)"
    )


# ── End-to-end: orphan failure appears in breakdown ──────────


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


def test_orphan_failure_appears_in_failures_tabs_breakdown(
    admin_client, tmp_path,
):
    """End-to-end repro of the user's 2026-05-23 bug. Seed an
    orphan-adopted failing theme (pi.theme_id back-stamped but
    pi.guid_tmdb NULL — the case the OLD guid_tmdb join missed).
    /api/stats failures.tabs must include the entry, otherwise
    the click-cycle has no target and lands on the wrong tab
    via fallback. Pre-fix this test would have returned tabs=[]
    despite total=1."""
    db = _settings_db(tmp_path)
    with sqlite3.connect(db) as conn:
        # 1. Section.
        conn.execute(
            "INSERT INTO plex_sections "
            "  (section_id, title, type, is_anime, is_4k, "
            "   themes_subdir, included, discovered_at, last_seen_at) "
            "VALUES ('1', 'Movies', 'movie', 0, 0, "
            "        'movies', 1, '2026-05-23T00:00:00', "
            "        '2026-05-23T00:00:00')"
        )
        # 2. Theme with a failure (un-acked).
        conn.execute(
            "INSERT INTO themes "
            "  (id, media_type, tmdb_id, title, upstream_source, "
            "   last_seen_sync_at, first_seen_sync_at, "
            "   failure_kind, failure_acked_at) "
            "VALUES (42, 'movie', 99999, 'Orphan Fail', 'imdb', "
            "        '2026-05-23T00:00:00', '2026-05-23T00:00:00', "
            "        'unavailable', NULL)"
        )
        # 3. plex_items WITH theme_id set (orphan-adopted shape)
        # but pi.guid_tmdb NULL — the case the OLD guid_tmdb join
        # silently missed.
        conn.execute(
            "INSERT INTO plex_items "
            "  (rating_key, section_id, media_type, theme_id, "
            "   guid_imdb, guid_tmdb, title, year, has_theme, "
            "   local_theme_file, folder_path, "
            "   plex_independent_theme, first_seen_at, "
            "   last_seen_at) "
            "VALUES ('orphan-1', '1', 'movie', 42, "
            "        'tt99999', NULL, 'Orphan Fail', 2020, 0, 0, "
            "        '/data/movies/Orphan Fail', 0, "
            "        '2026-05-23T00:00:00', "
            "        '2026-05-23T00:00:00')"
        )
        conn.commit()

    # /api/stats has a 1s cache TTL; the fresh admin_client
    # hasn't fetched yet so first hit is fine.
    r = admin_client.get("/api/stats", headers=AUTH)
    assert r.status_code == 200
    failures = r.json().get("failures", {})

    # The canonical count must see the failure (sanity check).
    assert failures.get("total", 0) >= 1, (
        f"v1.19.1 sanity: canonical FAIL count must see orphan "
        f"failure; got total={failures.get('total')}"
    )

    # AND the breakdown must include a movies entry.
    # Pre-fix this assertion would have failed — tabs=[] despite
    # total=1.
    tabs = failures.get("tabs", [])
    movies_entries = [
        t for t in tabs
        if t.get("tab") == "movies" and t.get("fourk") is False
    ]
    assert movies_entries, (
        f"v1.19.1: orphan failure must surface in failures.tabs "
        f"so the click-cycle has a valid target; got tabs={tabs}"
    )
    assert movies_entries[0]["count"] >= 1


def test_breakdown_count_agrees_with_canonical_count(
    admin_client, tmp_path,
):
    """Sum of failures.tabs[*].count must equal failures.total.
    If they diverge, the click-cycle is missing entries OR
    counting some failures twice. Cross-axis sanity check that
    applies regardless of orphan-vs-tracked shape."""
    db = _settings_db(tmp_path)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections "
            "  (section_id, title, type, is_anime, is_4k, "
            "   themes_subdir, included, discovered_at, last_seen_at) "
            "VALUES ('1', 'Movies', 'movie', 0, 0, "
            "        'movies', 1, '2026-05-23T00:00:00', "
            "        '2026-05-23T00:00:00')"
        )
        # Seed 3 failing themes with different orphan/tracked
        # shapes to stress the join.
        for tid, (rk, guid) in enumerate(
            [("rk1", 11111), ("rk2", None), ("rk3", 33333)],
            start=1,
        ):
            conn.execute(
                "INSERT INTO themes (id, media_type, tmdb_id, "
                "  title, upstream_source, last_seen_sync_at, "
                "  first_seen_sync_at, failure_kind) "
                "VALUES (?, 'movie', ?, ?, 'imdb', "
                "        '2026-05-23T00:00:00', "
                "        '2026-05-23T00:00:00', 'unavailable')",
                (tid, guid or (99000 + tid),
                 f"Fail {tid}"),
            )
            conn.execute(
                "INSERT INTO plex_items "
                "  (rating_key, section_id, media_type, theme_id, "
                "   guid_imdb, guid_tmdb, title, year, has_theme, "
                "   local_theme_file, folder_path, "
                "   plex_independent_theme, first_seen_at, "
                "   last_seen_at) "
                "VALUES (?, '1', 'movie', ?, ?, ?, ?, "
                "        2020, 0, 0, ?, 0, "
                "        '2026-05-23T00:00:00', "
                "        '2026-05-23T00:00:00')",
                (rk, tid, f"tt{tid}", guid,
                 f"Fail {tid}",
                 f"/data/movies/Fail {tid}"),
            )
        conn.commit()

    r = admin_client.get("/api/stats", headers=AUTH)
    failures = r.json()["failures"]
    total = failures["total"]
    breakdown_sum = sum(t.get("count", 0) for t in failures.get("tabs", []))
    assert total == breakdown_sum, (
        f"v1.19.1: failures.total ({total}) must equal sum of "
        f"failures.tabs counts ({breakdown_sum}). If they "
        f"diverge, the topbar pill click-cycle has a different "
        f"row population than the badge count — exactly the "
        f"drift that bit the user 2026-05-23."
    )
