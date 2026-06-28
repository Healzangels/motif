"""v1.23.24 — sorting by PL groups rows by the rendered dot color.

the user's report (TV Shows, sort by PL): cyan (plex_upload / pushed) and green
(sidecar / placed) dots were interleaved instead of grouped. Cause: the SQL
`pl` sort key was `media_folder IS NOT NULL THEN 0 ELSE 1`, but a plex_upload
placement carries media_folder='' (a non-NULL empty-string sentinel), so BOTH
cyan and green fell into bucket 0 — no separation.

Fix: the `pl` sort key now mirrors the JS dot derivation — one bucket per
rendered state: green (real sidecar folder) → cyan (plex_upload) → amber
(await: canonical, no placement, not LPS) → gray (off + LPS). The red 'broken'
state is a post-SQL stat and is handled by v1.23.25's stored health flag.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
NOW = "2026-06-13T00:00:00"


# ── B: the broken dot is a plain red dot (no slash) ──────────


def test_broken_dot_has_no_slash_pseudo_element():
    # the ::after diagonal slash is gone — broken is a solid red dot now.
    i = APP_CSS.index(".state-pill.broken {")
    block = APP_CSS[i:i + 400]
    assert ".state-pill.broken::after" not in block, (
        "v1.23.24: the broken dot must be a plain red dot (no ::after slash)"
    )


# ── C: PL sort groups by dot color (behavioral) ──────────────


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
    init_db(settings.db_path)
    init_auth_schema(settings.db_path)
    create_admin(settings.db_path, username="testadmin", password="testpassword")
    return TestClient(create_app(settings)), settings.db_path


AUTH = {"X-Authentik-Username": "testadmin"}


def _section(conn, sid="1"):
    conn.execute(
        "INSERT INTO plex_sections (section_id, title, type, is_anime, is_4k,"
        " themes_subdir, included, discovered_at, last_seen_at)"
        " VALUES (?, 'Movies', 'movie', 0, 0, 'movies', 1, ?, ?)", (sid, NOW, NOW))


def _theme(conn, tid, tmdb, title):
    conn.execute(
        "INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source,"
        " last_seen_sync_at, first_seen_sync_at, youtube_url)"
        " VALUES (?, 'movie', ?, ?, 'imdb', ?, ?, 'https://y/watch?v=V')",
        (tid, tmdb, title, NOW, NOW))


def _item(conn, *, rk, tid, tmdb, title, has_theme):
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, media_type, theme_id,"
        " guid_imdb, guid_tmdb, title, year, has_theme, plex_independent_theme,"
        " first_seen_at, last_seen_at)"
        " VALUES (?, '1', 'movie', ?, ?, ?, ?, 2012, ?, 0, ?, ?)",
        (rk, tid, f"tt{tmdb}", tmdb, title, has_theme, NOW, NOW))


def _local_file(conn, *, tid, tmdb):
    conn.execute(
        "INSERT INTO local_files (media_type, tmdb_id, section_id, theme_id,"
        " file_path, downloaded_at, source_video_id, provenance, source_kind,"
        " edition_key) VALUES ('movie', ?, '1', ?, ?, ?, 'V', 'auto',"
        " 'themerrdb', '')", (tmdb, tid, f"movies/{tmdb}/theme.mp3", NOW))


def _placement(conn, *, tid, tmdb, media_folder, kind):
    conn.execute(
        "INSERT INTO placements (media_type, tmdb_id, section_id, theme_id,"
        " media_folder, placed_at, placement_kind, provenance, edition_key)"
        " VALUES ('movie', ?, '1', ?, ?, ?, ?, 'auto', '')",
        (tmdb, tid, media_folder, NOW, kind))


def test_pl_sort_groups_green_cyan_amber_gray(admin_client):
    client, db = admin_client
    with sqlite3.connect(db) as conn:
        _section(conn)
        # GREEN (on): real sidecar folder, hardlink. Title sorts LAST so the
        # only way it leads is the PL bucket, not the title tiebreaker.
        _theme(conn, 1, 101, "Z Green")
        _item(conn, rk="rk-green", tid=1, tmdb=101, title="Z Green", has_theme=1)
        _local_file(conn, tid=1, tmdb=101)
        _placement(conn, tid=1, tmdb=101,
                   media_folder="/data/m/Z Green", kind="hardlink")
        # CYAN (pushed): plex_upload, media_folder='' (the sentinel that fooled
        # the old sort into bucket 0 alongside green).
        _theme(conn, 2, 102, "Y Cyan")
        _item(conn, rk="rk-cyan", tid=2, tmdb=102, title="Y Cyan", has_theme=1)
        _local_file(conn, tid=2, tmdb=102)
        _placement(conn, tid=2, tmdb=102, media_folder="", kind="plex_upload")
        # AMBER (await): canonical present, no placement.
        _theme(conn, 3, 103, "X Await")
        _item(conn, rk="rk-await", tid=3, tmdb=103, title="X Await", has_theme=0)
        _local_file(conn, tid=3, tmdb=103)
        # GRAY (off): nothing placed, no canonical.
        _theme(conn, 4, 104, "W Off")
        _item(conn, rk="rk-off", tid=4, tmdb=104, title="W Off", has_theme=0)
        conn.commit()

    r = client.get("/api/library?tab=movies&sort=pl&sort_dir=asc", headers=AUTH)
    assert r.status_code == 200, r.text
    order = [it["rating_key"] for it in r.json()["items"]]
    for rk in ("rk-green", "rk-cyan", "rk-await", "rk-off"):
        assert rk in order, f"{rk} missing from {order}"
    # the WHOLE point: cyan and green are SEPARATED, not interleaved — green
    # (bucket 0) precedes cyan (bucket 1) even though "Z Green" sorts after
    # "Y Cyan" alphabetically. Then amber, then gray.
    assert (order.index("rk-green") < order.index("rk-cyan")
            < order.index("rk-await") < order.index("rk-off")), order


def test_pl_sort_key_distinguishes_plex_upload():
    # source guard: the pl sort key must branch on placement_kind='plex_upload'
    # (the green/cyan separation) — not the old flat media_folder 0/1.
    i = API_PY.index('"pl": (')
    # v1.23.25 prepended the broken branch (theme_present=0) to the CASE, and
    # v1.24.36 prepended the stale-plex_upload→await branch (+ its comment), so
    # the plex_upload/await branches sit further down — widen the window.
    block = API_PY[i:i + 1700]
    assert "placement_kind, p_g.placement_kind) = 'plex_upload'" in block
    assert "plex_independent_theme" in block
