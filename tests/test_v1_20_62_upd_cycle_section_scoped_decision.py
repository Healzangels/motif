"""v1.20.62 — UPD pill cycle lands on a tab with 0 matches (section drift).

the user's repro (2026-05-31): the "3 UPD" badge cycle visited the first
two tabs correctly, then landed on /anime?attn_pills=update with 0
matches, then wrapped back to a real tab.

## Root cause — class-9 mirror-drift (v1.18.67)

The badge COUNT (`updates_pending`, api.py:6724) and the library filter
(`attn_pills=update`, api.py:2197) both check the pending decision
SECTION-SCOPED: `COALESCE((decision WHERE section_id=pi.section_id),
(decision WHERE section_id='')) = 'pending'`. v1.18.67 made them
section-scoped so a KEEP-CURRENT on one section doesn't keep the row
"pending" on another.

But the two queries that drive the UPD pill CYCLE — `update_tab_row`
(7051, the first-href / tab_hint) and `update_tab_breakdown_rows` (7106,
the rotation list) — were left on the OLD title-wide
`EXISTS (SELECT 1 FROM pending_updates pu WHERE media_type/tmdb_id AND
decision='pending')`. That matches a pending in ANY section of the
title. So a title present in both a non-anime section (with a per-section
pending) AND an anime section leaked the pending into the anime tab's
breakdown — the cycle routed there, but the section-scoped library
filter showed 0. The count was right ("3 UPD"); only the cycle
over-routed.

## Fix

Both cycle queries now use the section-scoped COALESCE decision check
(byte-identical to the count + filter). They also gain the
`OR new_theme_kind` SRC-gate exception the library filter got in v1.19.72
(so the cycle also reaches net-new TDB themes on SRC=— rows).
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"

NOW = "2026-05-31T00:00:00+00:00"
AUTH = {"X-Authentik-Username": "testadmin"}


# ── Source pins (guard the shape; behavioral test below proves it) ──


def _query_block(name: str) -> str:
    src = API_PY.read_text()
    idx = src.index(f"{name} = conn.execute(")
    # v1.23.87: widened 4000→5500 — the edition '' fallback gate added lines to
    # the p_g/lf_g joins, pushing the asserted SRC-letter gate past the window.
    return src[idx:idx + 5500]


@pytest.mark.parametrize("query", ["update_tab_row", "update_tab_breakdown_rows"])
def test_cycle_query_uses_section_scoped_decision(query):
    """Both cycle queries must check the decision section-scoped (mirror
    of the library filter + count), NOT the title-wide EXISTS."""
    block = _query_block(query)
    # The section-scoped COALESCE form must be present.
    # v1.21.81: the decision read is ALSO edition-scoped (filters to the
    # row's edition_key) so a per-edition accept/decline can't bleed.
    assert "AND pu.section_id = pi.section_id AND pu.edition_key = pi.edition_key)" in block, (
        f"{query}: decision check must be section + edition scoped"
    )
    assert ") = 'pending'" in block
    # The old title-wide decision EXISTS must be gone.
    assert "pu.decision = 'pending'" not in block, (
        f"{query}: the title-wide `EXISTS(... pu.decision='pending')` "
        f"decision check must be replaced by the section-scoped COALESCE"
    )


@pytest.mark.parametrize("query", ["update_tab_row", "update_tab_breakdown_rows"])
def test_cycle_query_has_new_theme_src_exception(query):
    """Both cycle queries must mirror the library filter's
    `(SRC != '-' OR new_theme_kind)` gate (v1.19.72) — the new_theme
    exception must immediately follow the SRC-letter gate."""
    block = _query_block(query)
    # v1.22.83: the cycle queries moved to the edition-aware two-tier
    # joins, so the gate reads the _LIB_ SRC variant.
    gate = "({_LIB_SRC_LETTER_SQL}) != '-'"
    assert gate in block, f"{query}: SRC-letter gate missing"
    after = block[block.index(gate):block.index(gate) + 120]
    assert "_pending_update_new_theme_kind_sql('t', 'pi')" in after, (
        f"{query}: SRC gate must be `(SRC != '-' OR new_theme_kind)`"
    )


# ── Behavioral (the discriminator — phantom-guard avoidance) ─────────


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


def _settings_db(tmp_path):
    from app.config import Settings
    return Settings(config_dir=tmp_path, data_dir=tmp_path / "data").db_path


def _seed_section(conn, *, section_id, is_anime):
    conn.execute(
        "INSERT INTO plex_sections (section_id, title, type, is_anime,"
        " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
        " VALUES (?, ?, 'movie', ?, 0, ?, 1, ?, ?)",
        (section_id, f"sec{section_id}", is_anime, f"sub{section_id}", NOW, NOW),
    )


def _seed_eligible_row(conn, *, theme_id, tmdb_id, section_id, rk):
    """A row that PASSES the update predicate on its own merits: an
    'upload' local_file + placement (SRC='U', non-url local content so
    the URL-diff clause passes regardless of the pending row)."""
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, media_type,"
        " theme_id, guid_tmdb, title, year, has_theme, local_theme_file,"
        " folder_path, plex_independent_theme, first_seen_at, last_seen_at)"
        " VALUES (?, ?, 'movie', ?, ?, ?, 2020, 0, 0, '/data/x', 0, ?, ?)",
        (rk, section_id, theme_id, tmdb_id, f"x{tmdb_id}", NOW, NOW),
    )
    conn.execute(
        "INSERT INTO local_files (media_type, tmdb_id, section_id, file_path,"
        " source_video_id, downloaded_at, source_kind)"
        " VALUES ('movie', ?, ?, 'x.mp3', 'vid', ?, 'upload')",
        (tmdb_id, section_id, NOW),
    )
    conn.execute(
        "INSERT INTO placements (media_type, tmdb_id, section_id, media_folder,"
        " placed_at, placement_kind, plex_refreshed)"
        " VALUES ('movie', ?, ?, ?, ?, 'hardlink', 0)",
        (tmdb_id, section_id, f"/data/{section_id}", NOW),
    )


def test_per_section_pending_does_not_leak_into_anime_cycle(admin_client, tmp_path):
    """A title themed in BOTH a non-anime section (with a per-section
    pending) AND an anime section (NO pending) must produce ONLY a
    'movies' cycle entry — not a phantom 'anime' one."""
    db = _settings_db(tmp_path)
    with sqlite3.connect(db) as conn:
        _seed_section(conn, section_id="1", is_anime=0)   # /movies
        _seed_section(conn, section_id="2", is_anime=1)   # /anime
        conn.execute(
            "INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source,"
            " youtube_url, youtube_video_id, last_seen_sync_at, first_seen_sync_at)"
            " VALUES (90, 'movie', 500, 'x500', 'imdb',"
            " 'https://www.youtube.com/watch?v=tdb', 'tdb', ?, ?)",
            (NOW, NOW),
        )
        _seed_eligible_row(conn, theme_id=90, tmdb_id=500, section_id="1", rk="rk-s1")
        _seed_eligible_row(conn, theme_id=90, tmdb_id=500, section_id="2", rk="rk-s2")
        # Per-section pending in the NON-anime section ONLY. No '' global,
        # no anime-section row.
        conn.execute(
            "INSERT INTO pending_updates (media_type, tmdb_id, section_id,"
            " decision, detected_at, new_youtube_url, kind)"
            " VALUES ('movie', 500, '1', 'pending', ?,"
            " 'https://www.youtube.com/watch?v=new', 'upstream_changed')",
            (NOW,),
        )
        conn.commit()

    r = admin_client.get("/api/stats", headers=AUTH)
    assert r.status_code == 200, r.text
    updates = r.json()["updates"]
    tabs = updates.get("tabs", [])

    anime = [t for t in tabs if t.get("tab") == "anime"]
    movies = [t for t in tabs if t.get("tab") == "movies"]
    assert not anime, (
        f"v1.20.62: a per-section pending in the NON-anime section must "
        f"NOT leak into the anime cycle entry; got tabs={tabs}"
    )
    assert movies and movies[0]["count"] == 1, (
        f"v1.20.62: the real movies entry must still be present; got {tabs}"
    )
    # The badge COUNT (already section-scoped) sees only section 1 → 1.
    assert updates["pending"] == 1, updates


def test_tab_hint_routes_to_real_section_not_anime(admin_client, tmp_path):
    """tab_hint (the static first-href) must point at the section that
    actually has the pending, not the leaked anime one."""
    db = _settings_db(tmp_path)
    with sqlite3.connect(db) as conn:
        _seed_section(conn, section_id="1", is_anime=0)
        _seed_section(conn, section_id="2", is_anime=1)
        conn.execute(
            "INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source,"
            " youtube_url, youtube_video_id, last_seen_sync_at, first_seen_sync_at)"
            " VALUES (90, 'movie', 500, 'x500', 'imdb',"
            " 'https://www.youtube.com/watch?v=tdb', 'tdb', ?, ?)",
            (NOW, NOW),
        )
        _seed_eligible_row(conn, theme_id=90, tmdb_id=500, section_id="1", rk="rk-s1")
        _seed_eligible_row(conn, theme_id=90, tmdb_id=500, section_id="2", rk="rk-s2")
        conn.execute(
            "INSERT INTO pending_updates (media_type, tmdb_id, section_id,"
            " decision, detected_at, new_youtube_url, kind)"
            " VALUES ('movie', 500, '1', 'pending', ?,"
            " 'https://www.youtube.com/watch?v=new', 'upstream_changed')",
            (NOW,),
        )
        conn.commit()

    r = admin_client.get("/api/stats", headers=AUTH)
    updates = r.json()["updates"]
    assert updates.get("tab_hint") == "movies", (
        f"v1.20.62: tab_hint must route to the section with the pending "
        f"(movies), not the leaked anime; got {updates.get('tab_hint')!r}"
    )


def test_v1_20_62_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
