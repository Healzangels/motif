"""v1.23.35 — sorting by LINK groups rows by the rendered LINK chip.

the user (Movies, sort by LINK): PU and "—" rows were interleaved instead of
grouped. Cause: the SQL `link` sort key was only
`placement_kind WHEN 'hardlink' THEN 0 WHEN 'copy' THEN 1 ELSE 2`, so PU
(plex_upload), the four backups (PB/TB/AB/UB), M (mismatch) and "—" ALL
collapsed to rank 2 — the title tiebreak then interleaved them.

Fix: the `link` sort key now mirrors the renderLibraryRow chip precedence
(app.js): PB → TB → AB → UB → M → HL → C → PU → —, one rank per chip, using
the same field expressions as the link_pills filter branches so filter and
sort agree.
"""
from __future__ import annotations
from _slice_helpers import slice_to_next

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()
NOW = "2026-06-13T00:00:00"
AUTH = {"X-Authentik-Username": "testadmin"}


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


def _local_file(conn, *, tid, tmdb, source_kind="themerrdb", reason=None,
                mismatch=None):
    conn.execute(
        "INSERT INTO local_files (media_type, tmdb_id, section_id, theme_id,"
        " file_path, downloaded_at, source_video_id, provenance, source_kind,"
        " last_place_attempt_reason, mismatch_state, edition_key)"
        " VALUES ('movie', ?, '1', ?, ?, ?, 'V', 'auto', ?, ?, ?, '')",
        (tmdb, tid, f"movies/{tmdb}/theme.mp3", NOW, source_kind, reason, mismatch))


def _placement(conn, *, tid, tmdb, media_folder, kind):
    conn.execute(
        "INSERT INTO placements (media_type, tmdb_id, section_id, theme_id,"
        " media_folder, placed_at, placement_kind, provenance, edition_key)"
        " VALUES ('movie', ?, '1', ?, ?, ?, ?, 'auto', '')",
        (tmdb, tid, media_folder, NOW, kind))


def test_link_sort_groups_by_chip_in_precedence_order(admin_client):
    client, db = admin_client
    # Each row paints a distinct LINK chip. Titles run REVERSE-alphabetical
    # against the rank (Z..R), so a title-only sort would invert the rank
    # order — the only way the rows come out PB→…→— is the LINK sort key.
    rows = [
        # rk,        tmdb, title,    rank, seed
        ("rk-pb",    101, "Z PB",    0, dict(source_kind="plex_cloud", reason="backup_only")),
        ("rk-tb",    102, "Y TB",    1, dict(source_kind="themerrdb", reason="backup_only")),
        ("rk-ab",    103, "X AB",    2, dict(source_kind="adopt", reason="backup_only")),
        ("rk-ub",    104, "W UB",    3, dict(source_kind=None, reason="backup_only")),
        ("rk-m",     105, "V M",     4, dict(mismatch="content", placement=("hardlink", "/data/m/VM"))),
        ("rk-hl",    106, "U HL",    5, dict(placement=("hardlink", "/data/m/UHL"))),
        ("rk-c",     107, "T C",     6, dict(placement=("copy", "/data/m/TC"))),
        ("rk-pu",    108, "S PU",    7, dict(placement=("plex_upload", ""))),
        ("rk-none",  109, "R none",  8, dict(bare=True)),
    ]
    with sqlite3.connect(db) as conn:
        _section(conn)
        for rk, tmdb, title, _rank, seed in rows:
            tid = tmdb
            _theme(conn, tid, tmdb, title)
            placed = "placement" in seed
            _item(conn, rk=rk, tid=tid, tmdb=tmdb, title=title,
                  has_theme=1 if placed else 0)
            if seed.get("bare"):
                continue
            _local_file(conn, tid=tid, tmdb=tmdb,
                        source_kind=seed.get("source_kind", "themerrdb"),
                        reason=seed.get("reason"),
                        mismatch=seed.get("mismatch"))
            if placed:
                kind, folder = seed["placement"]
                _placement(conn, tid=tid, tmdb=tmdb, media_folder=folder, kind=kind)
        conn.commit()

    r = client.get("/api/library?tab=movies&sort=link&sort_dir=asc", headers=AUTH)
    assert r.status_code == 200, r.text
    order = [it["rating_key"] for it in r.json()["items"]]
    expected = ["rk-pb", "rk-tb", "rk-ab", "rk-ub", "rk-m",
                "rk-hl", "rk-c", "rk-pu", "rk-none"]
    for rk in expected:
        assert rk in order, f"{rk} missing from {order}"
    got = [rk for rk in order if rk in expected]
    # The whole point: grouped by chip in precedence order — NOT interleaved
    # by the title tiebreak (which would have produced the reverse).
    assert got == expected, got


def test_pu_and_none_are_separated(admin_client):
    # The exact pair from the user's report: PU and "—" must not interleave.
    client, db = admin_client
    with sqlite3.connect(db) as conn:
        _section(conn)
        # "A none" sorts FIRST alphabetically but is rank 8 (— / last).
        _theme(conn, 1, 201, "A none")
        _item(conn, rk="rk-none", tid=1, tmdb=201, title="A none", has_theme=0)
        # "Z pu" sorts LAST alphabetically but is rank 7 (PU, before —).
        _theme(conn, 2, 202, "Z pu")
        _item(conn, rk="rk-pu", tid=2, tmdb=202, title="Z pu", has_theme=1)
        _local_file(conn, tid=2, tmdb=202)
        _placement(conn, tid=2, tmdb=202, media_folder="", kind="plex_upload")
        conn.commit()
    r = client.get("/api/library?tab=movies&sort=link&sort_dir=asc", headers=AUTH)
    assert r.status_code == 200, r.text
    order = [it["rating_key"] for it in r.json()["items"]]
    # PU (rank 7) precedes — (rank 8) despite "Z pu" > "A none" alphabetically.
    assert order.index("rk-pu") < order.index("rk-none"), order


def test_link_sort_key_ranks_every_chip(admin_client=None):
    # Source guard: the link sort key must branch on the backup reason +
    # source_kind + mismatch + each placement_kind — not the old flat
    # hardlink/copy/else CASE.
    block = slice_to_next(API_PY, '"link": (', "'attention'")
    assert "last_place_attempt_reason, lf_g.last_place_attempt_reason) = 'backup_only'" in block
    for sk in ("'plex_cloud'", "'themerrdb'", "'adopt'"):
        assert sk in block, f"link sort must rank backup source {sk}"
    assert "mismatch_state, lf_g.mismatch_state) IS NOT NULL" in block
    assert "placement_kind, p_g.placement_kind) = 'plex_upload'" in block
    # The reverted flat CASE must be gone.
    assert "WHEN 'hardlink' THEN 0 WHEN 'copy' THEN 1 ELSE 2 END" not in block
