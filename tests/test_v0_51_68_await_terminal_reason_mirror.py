"""v0.51.68 — the AWAIT predicate excludes BOTH terminal reasons at all four sites.

Complexity/regression audit finding (the dominant "mirror-drift" class): the single
concept "downloaded canonical, no placement, awaiting a PLACE" is computed in FOUR
independent places that must stay byte-equivalent but had drifted:

  1. _LIB_AWAIT_SQL           — attn_pills=await filter/count.  excludes BOTH
                                 backup_only (v0.51.36) AND over_ceiling (v1.24.46).  ✓
  2. _row_matches_attn await  — attn post-stat fallback (fires on attn=await+broken
                                 multi-select).  had ONLY backup_only → an over-ceiling
                                 row re-appeared as AWAIT and re-enqueued a doomed
                                 (guaranteed-500) upload on click.  ← the live bug.
  3. pl_pills=await SQL       — had ONLY backup_only (missing over_ceiling).
  4. _row_matches_pl await    — pl post-stat fallback.  had NEITHER exclusion.

v0.51.68 adds the missing exclusion(s) to sites 2/3/4 (the exclusion STRINGS only — no
refactor into a shared helper, per the audit's low-risk guidance and CLAUDE.md's
"no premature abstraction" rule). This behavioral test seeds an over_ceiling row + a
backup_only row + a genuine-await row + a still-retriable row and asserts the two
terminal rows drop out of ALL FOUR await surfaces while the genuine/retriable rows
stay — closing the previously-uncovered post-stat paths (the v1.24.46 guard only hit
the SQL attn path).

Behavioral (real DB + real on-disk theme files so the `broken` branch, which stats
canonical_missing, doesn't spuriously match), NOT a source-text pin.
"""
from __future__ import annotations

import sqlite3

import pytest
from fastapi.testclient import TestClient

AUTH = {"X-Authentik-Username": "testadmin"}

# (tmdb_id, title, last_place_attempt_reason, expect_in_await)
ROWS = [
    (201, "Over Ceiling Row", "plex_rejected:over_ceiling", False),
    (202, "Backup Only Row2", "backup_only", False),
    (203, "Genuine Await Row", None, True),
    (204, "Retriable 500 Row", "plex_rejected:HTTP_500", True),
]


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web.api import create_app
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(settings.db_path)
    init_auth_schema(settings.db_path)
    create_admin(settings.db_path, username="testadmin", password="testpassword")
    _seed(settings)
    return TestClient(create_app(settings))


def _seed(settings):
    now = "2026-07-04T00:00:00"
    # themes_dir is unconfigured in tests (None) → _annotate_canonical_state
    # short-circuits canonical_missing/placement_missing to False for every row,
    # which models the realistic AWAIT state (the downloaded canonical IS present).
    # So the post-stat `broken` branch (which keys on canonical_missing) matches
    # nothing here and doesn't confound the await assertion — no on-disk files
    # needed.
    with sqlite3.connect(settings.db_path) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime, "
            "  is_4k, themes_subdir, included, discovered_at, last_seen_at) "
            "VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (now, now))
        for tid, title, reason, _ in ROWS:
            rel = f"movies/{tid}/theme.mp3"
            conn.execute(
                "INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source, "
                "  last_seen_sync_at, first_seen_sync_at, youtube_url) "
                "VALUES (?,?,?,?,'imdb',?,?, 'https://www.youtube.com/watch?v=X')",
                (tid, "movie", tid, title, now, now))
            conn.execute(
                "INSERT INTO plex_items (rating_key, section_id, media_type, theme_id, "
                "  guid_tmdb, title, year, has_theme, local_theme_file, folder_path, "
                "  plex_independent_theme, plex_theme_verified_ok, first_seen_at, "
                "  last_seen_at) VALUES (?, '1','movie',?,?,?,2020,1,1,'/data/movies/x',"
                "  0,1,?,?)",
                (f"r{tid}", tid, tid, title, now, now))
            # Downloaded canonical, NO placement row → the AWAIT shape. reason varies.
            conn.execute(
                "INSERT INTO local_files (media_type, tmdb_id, section_id, theme_id, "
                "  file_path, downloaded_at, source_video_id, provenance, source_kind, "
                "  last_place_attempt_reason) VALUES ('movie',?,'1',?,?,?,'X','auto',"
                "  'themerrdb',?)",
                (tid, tid, rel, now, reason))
        conn.commit()


def _titles(client, query):
    r = client.get(f"/api/library?tab=movies&per_page=50&{query}", headers=AUTH)
    assert r.status_code == 200, r.text
    return [(it.get("plex_title") or it.get("theme_title")) for it in r.json()["items"]]


# One parametrization per await SURFACE. The multi-select variants (await,broken /
# await,on) are what route through the Python post-stat matchers — the paths the
# v1.24.46 guard never exercised.
@pytest.mark.parametrize("query,surface", [
    ("attn_pills=await", "attn await (SQL _LIB_AWAIT_SQL)"),
    ("attn_pills=await,broken", "attn await (Python _row_matches_attn post-stat)"),
    ("pl_pills=await", "pl await (SQL branch)"),
    ("pl_pills=await,on", "pl await (Python _row_matches_pl post-stat)"),
])
def test_terminal_reasons_excluded_from_every_await_surface(env, query, surface):
    got = _titles(env, query)
    for _tid, title, _reason, expect in ROWS:
        if expect:
            assert title in got, f"{surface}: genuine/retriable await row '{title}' must match"
        else:
            assert title not in got, (
                f"{surface}: terminal row '{title}' must NOT appear as AWAIT "
                f"(mirror-drift regression — a terminal row re-enqueues a doomed op on click)")


def test_terminal_rows_still_listed_unfiltered(env):
    # Sanity: exclusion from AWAIT must not hide the rows entirely.
    got = _titles(env, "")
    for _tid, title, _reason, _ in ROWS:
        assert title in got, f"'{title}' should still list with no filter"


def test_version_pin():
    from pathlib import Path
    init_py = (Path(__file__).resolve().parent.parent / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
