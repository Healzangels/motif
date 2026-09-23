"""v1.14.91 — THEMED + ↺ (broken) attn pill no longer no-ops.

the user: "filtering on the conical missing filter is showing
all results"

Repro: /movies, click THEMED chip, click ↺ (canonical missing)
attn pill. Expected: just rows where motif's local file was
deleted but the Plex copy is intact. Actual (pre-fix): all
3,295 themed rows.

## Root cause

The route handler at api.py:6857-6868 has special routing for
the broken-only pill case:

    if dl_set == {"broken"} and status == "all":
        effective_status = "dl_missing"
        dl_set = set()
    if attn_set == {"broken"} and status == "all":
        effective_status = "dl_missing"
        attn_set = set()

The override translates `attn_pills={broken}` into a
`status=dl_missing` query (which has the SQL pre-filter +
post-stat narrowing the broken pill needs). But the override
ONLY fires when `status == "all"`. With THEMED active
(status='has_theme'), the override skipped.

Once the override skipped, attn_pills={broken} fell through
to the SQL loop. The loop has branches for fail / update /
mismatch / await — but NO branch for "broken" (the comment
acknowledges this: "Mixed-with-broken would need post-SQL
union; out of scope for v1.13.68"). So `attn_branches` came
out empty → no SQL filter for broken → result: every themed
row, the broken pill silently dropped.

## Fix

Expand the routing condition to include `status='has_theme'`.
This is semantically safe because dl_missing → has_theme:
a tracked-but-missing canonical means a theme source IS
present, so the row is themed by definition. The override
narrows the result correctly.

Same fix for dl_pills broken (same shape, same blind spot).

Mixed-with-broken (e.g. attn_pills={broken, fail}) is still
out of scope per the v1.13.68 comment — that needs a SQL UNION
or a post-stat OR-narrowing pass. the user's report is the solo
case so this fix is targeted.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"
AUTH = {"X-Authentik-Username": "testadmin"}
# v0.51.346: a run clock, never a fixed date (a seeded date ages into every recency window)
NOW = datetime.now(timezone.utc).isoformat(timespec="seconds")
# (rating_key, canonical on disk?, placed?, plex_independent_theme, reason); "u-none" has no theme at all.
ROWS = [("t-ok", True, True, 0, None), ("t-gone-placed", False, True, 0, None),
        ("t-gone-backup", False, False, 1, "backup_only"), ("u-none", None, False, 0, None)]


# v0.51.346: no route rewrite to status=dl_missing any more, so the v1.14.91 routing literals became behaviour.
@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web import api
    monkeypatch.setattr(api, "log_event", lambda *a, **k: None)
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    s._cfg.paths.themes_dir = str(tmp_path / "themes")
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    with sqlite3.connect(s.db_path) as c:
        c.execute("INSERT INTO plex_sections (section_id, title, type, is_anime, is_4k, themes_subdir, included,"
                  " discovered_at, last_seen_at) VALUES ('1', 'Movies', 'movie', 0, 0, 'movies', 1, ?, ?)", (NOW, NOW))
        for n, (rk, on_disk, placed, lps, reason) in enumerate(ROWS, start=1):
            if on_disk is not None:
                c.execute("INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source, last_seen_sync_at,"
                          " first_seen_sync_at) VALUES (?, 'movie', ?, ?, 'imdb', ?, ?)", (n, 500 + n, rk, NOW, NOW))
                c.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, file_path, downloaded_at,"
                          " source_video_id, last_place_attempt_reason) VALUES ('movie', ?, '1', ?, ?, 'v', ?)",
                          (500 + n, f"movies/{rk}.mp3", NOW, reason))
                if on_disk:
                    (tmp_path / "themes" / "movies").mkdir(parents=True, exist_ok=True)
                    (tmp_path / "themes" / "movies" / f"{rk}.mp3").write_bytes(b"theme")
                if placed:
                    c.execute("INSERT INTO placements (media_type, tmdb_id, section_id, media_folder, placed_at,"
                              " placement_kind) VALUES ('movie', ?, '1', ?, ?, 'hardlink')",
                              (500 + n, str(tmp_path / "media" / rk), NOW))
            c.execute("INSERT INTO plex_items (rating_key, section_id, media_type, theme_id, title,"
                      " plex_independent_theme, first_seen_at, last_seen_at) VALUES (?, '1', 'movie', ?, ?, ?, ?, ?)",
                      (rk, n if on_disk is not None else None, rk, lps, NOW, NOW))
    return TestClient(api.create_app(s))


def _rks(client, **params):
    r = client.get("/api/library", params={"tab": "movies", "per_page": 50, **params}, headers=AUTH)
    assert r.status_code == 200, r.text
    assert r.json()["total"] == len(r.json()["items"])
    return {it["rating_key"] for it in r.json()["items"]}


@pytest.mark.parametrize("axis", ["dl_pills", "attn_pills"])
def test_themed_plus_broken_narrows_to_the_canonical_missing_rows(client, axis):
    """THEMED + ↺ must not show every themed row (the v1.14.91 report). Since v0.51.346 it also keeps a
    canonical-missing row with no placement — its DL dot paints red too."""
    broken = {"t-gone-placed", "t-gone-backup"}
    assert _rks(client, status="has_theme", **{axis: "broken"}) == broken
    assert _rks(client, **{axis: "broken"}) == broken


@pytest.mark.parametrize("axis", ["dl_pills", "attn_pills"])
def test_unthemed_plus_broken_matches_nothing(client, axis):
    """UNTHEMED rows have no canonical, so broken beside the untracked status is empty — never every untracked row."""
    assert _rks(client, status="untracked") == {"u-none"}
    assert _rks(client, status="untracked", **{axis: "broken"}) == set()


def test_attn_pills_broken_in_sql_loop_remains_unhandled():
    """The SQL loop in _library_main_query (api.py ~1196-1305)
    intentionally has no branch for `p == "broken"` — the
    routing override consumes it. This test pins the v1.13.68
    architectural decision: solo broken is route-overridden;
    mixed broken is documented as out-of-scope.

    If a future change adds a SQL branch for broken, the
    routing override should also be re-evaluated (might no
    longer be needed)."""
    src = API_PY.read_text()
    # Slice the attn_pills loop body.
    loop_anchor = src.index(
        "v1.13.68: ATTN (attention) pill axis"
    )
    # The loop ends where the next major block (dl_pills handling)
    # begins.
    loop_end = src.index(
        "v1.12.81: when 'broken' is in dl_pills or pl_pills",
        loop_anchor,
    )
    loop_block = src[loop_anchor:loop_end]
    # The branches present.
    assert 'p == "fail"' in loop_block
    assert 'p == "update"' in loop_block
    assert 'p == "mismatch"' in loop_block
    assert 'p == "await"' in loop_block
    # Broken intentionally absent — handled at route level.
    assert 'p == "broken"' not in loop_block, (
        "If a SQL branch for broken got added, re-check whether "
        "the route-level override is still needed."
    )
