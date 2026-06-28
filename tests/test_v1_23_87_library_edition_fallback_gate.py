"""v1.23.87 — library row stops bleeding the '' theme onto UNPLACED sibling editions.

the user's repro (1408 "Director's Cut"): a distinct-folder edition with NO theme
of its own rendered SRC=T / green DL / green PL / HL, while its INFO card
correctly said "no theme staged" (prod DB: the DC edition has zero placements/
local_files of its own; the only theme lives at edition_key='').

Root cause: the row read joins placements/local_files two ways (p_e/lf_e at the
row's own edition, p_g/lf_g at '') and reads COALESCE(p_e, p_g). The '' fallback
was ungated, so an unplaced multi-edition sibling inherited the standard's theme.

v1.23.87 gates the '' fallback the way the INFO card does (v1.21.95), but
nuanced so a legit case survives:
  - PLACEMENT fallback (p_g): suppressed on any multi-edition title.
  - DOWNLOAD fallback (lf_g): kept when THIS edition has its own placement
    (a placed edition sharing a '' download — the v1.21.58 transitional shape),
    suppressed only for an unplaced multi-edition sibling.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

NOW = "2026-06-19T00:00:00Z"
AUTH = {"X-Authentik-Username": "testadmin"}


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web.api import create_app
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    return TestClient(create_app(s)), s.db_path


def _seed(conn):
    conn.execute(
        "INSERT INTO plex_sections (section_id, title, type, is_anime,"
        " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
        " VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
    conn.execute(
        "INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source,"
        " last_seen_sync_at, first_seen_sync_at) VALUES (80,'movie',800,'1408',"
        "'imdb',?,?)", (NOW, NOW))


def _plex_item(conn, rk, ek, folder, has_theme, ltf):
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, media_type, theme_id,"
        " guid_tmdb, title, edition_key, folder_path, has_theme, local_theme_file,"
        " first_seen_at, last_seen_at) VALUES (?, '1','movie',80,800,'1408',?,?,?,?,?,?)",
        (rk, ek, folder, has_theme, ltf, NOW, NOW))


def _placement(conn, ek, folder):
    conn.execute(
        "INSERT INTO placements (theme_id, media_type, tmdb_id, section_id,"
        " edition_key, media_folder, placed_at, placement_kind, plex_refreshed)"
        " VALUES (80,'movie',800,'1',?,?,?,'hardlink',1)", (ek, folder, NOW))


def _local_file(conn, ek):
    conn.execute(
        "INSERT INTO local_files (media_type, tmdb_id, section_id, edition_key,"
        " file_path, downloaded_at, source_video_id, provenance, source_kind,"
        " last_place_attempt_reason) VALUES ('movie',800,'1',?,?,?, 'v','auto',"
        "'themerrdb','placed')", (ek, f"movies/1408-{ek or 'std'}.mp3", NOW))


def _rows(c):
    r = c.get("/api/library?tab=movies", headers=AUTH)
    assert r.status_code == 200, r.text
    return {it["rating_key"]: it for it in r.json()["items"]}


# ── THE bug: unplaced sibling must not inherit the standard '' theme ──

def test_unplaced_sibling_does_not_inherit_standard_theme(client):
    c, db = client
    with sqlite3.connect(db) as conn:
        _seed(conn)
        _plex_item(conn, "rk-std", "", "/data/Movies/1408 (2007)", 1, 1)
        _plex_item(conn, "rk-dc", "directors cut",
                   "/data/Movies/1408 (2007) {edition-Directors Cut}", 0, 0)
        _placement(conn, "", "/data/Movies/1408 (2007)")
        _local_file(conn, "")
        conn.commit()
    rows = _rows(c)
    std, dc = rows["rk-std"], rows["rk-dc"]
    assert std["media_folder"] and std["placement_kind"] == "hardlink"
    assert std["source_kind"] == "themerrdb"
    # the DC has NO placement and NO download of its own → fully un-themed
    assert not dc["media_folder"], "DC must not inherit the '' placement"
    assert not dc["placement_kind"]
    assert not dc["source_kind"]
    assert not dc["file_path"], "DC must not inherit the '' download either"


# ── nuance: a PLACED sibling sharing the '' download still shows it ──

def test_placed_sibling_keeps_shared_empty_download(client):
    """The v1.21.58 transitional shape: a tagged edition with its OWN placement
    but the download record still at ''. It IS themed, so the row must keep
    showing its download via the gated '' fallback (p_e present escape)."""
    c, db = client
    with sqlite3.connect(db) as conn:
        _seed(conn)
        _plex_item(conn, "rk-std", "", "/data/Movies/1408 (2007)", 0, 0)
        _plex_item(conn, "rk-ext", "extended",
                   "/data/Movies/1408 (2007) {edition-Extended}", 0, 0)
        # the EXTENDED edition has its OWN placement; the download is shared at ''
        _placement(conn, "extended", "/data/Movies/1408 (2007) {edition-Extended}")
        _local_file(conn, "")
        conn.commit()
    ext = _rows(c)["rk-ext"]
    assert ext["placement_kind"] == "hardlink", "extended has its own placement"
    assert ext["file_path"], "placed edition keeps the shared '' download"


# ── regression: a single (mis-keyed) edition still gets its '' theme ──

def test_single_miskeyed_edition_keeps_empty_fallback(client):
    c, db = client
    with sqlite3.connect(db) as conn:
        _seed(conn)
        _plex_item(conn, "rk-only", "directors cut",
                   "/data/Movies/1408 (2007) {edition-Directors Cut}", 1, 0)
        _placement(conn, "", "/data/Movies/1408 (2007)")
        _local_file(conn, "")
        conn.commit()
    only = _rows(c)["rk-only"]
    assert only["media_folder"] and only["placement_kind"] == "hardlink"
    assert only["file_path"]


# ── the filter-count (slim) + stats queries agree with the rows ──

def test_placed_filter_excludes_unplaced_sibling(client):
    """End-to-end through the slim filter-count + main-query WHERE: a 'placed'
    filter must not include the unplaced DC sibling."""
    c, db = client
    with sqlite3.connect(db) as conn:
        _seed(conn)
        _plex_item(conn, "rk-std", "", "/data/Movies/1408 (2007)", 1, 1)
        _plex_item(conn, "rk-dc", "directors cut",
                   "/data/Movies/1408 (2007) {edition-Directors Cut}", 0, 0)
        _placement(conn, "", "/data/Movies/1408 (2007)")
        _local_file(conn, "")
        conn.commit()
    r = c.get("/api/library?tab=movies&status=placed", headers=AUTH)
    assert r.status_code == 200, r.text
    rks = {it["rating_key"] for it in r.json()["items"]}
    assert "rk-std" in rks and "rk-dc" not in rks


def test_stats_endpoint_ok_with_gated_query(client):
    """Smoke the gated stats/donut query (3 blocks) — a typo there 500s here."""
    c, db = client
    with sqlite3.connect(db) as conn:
        _seed(conn)
        _plex_item(conn, "rk-std", "", "/data/Movies/1408 (2007)", 1, 1)
        _plex_item(conn, "rk-dc", "directors cut",
                   "/data/Movies/1408 (2007) {edition-Directors Cut}", 0, 0)
        _placement(conn, "", "/data/Movies/1408 (2007)")
        _local_file(conn, "")
        conn.commit()
    assert c.get("/api/stats", headers=AUTH).status_code == 200


def test_all_read_sites_gate_the_fallback():
    src = (Path(__file__).resolve().parent.parent / "app" / "web" / "api.py").read_text()
    gate = "SELECT COUNT(DISTINCT _ec.edition_key) FROM plex_items _ec"
    # main browse + slim filter-count + 3 stats blocks, each gating p_g AND lf_g.
    # v1.24.41: +1 — the RE-PUSH count mini-render (_REPUSH_COUNT_FROM) gates its
    # own p_g '' fallback the same way, so the count matches the rendered RP rows.
    # v1.24.43: +1 — the AWAIT count mini-render (_AWAIT_COUNT_FROM) gated its lf_g
    # '' fallback the same way. v0.50.34: −1 — the AWAIT badge + its count machinery
    # were removed (the attn_pills=await FILTER, which rides the main browse gate,
    # stays), so that dedicated gate is gone.
    assert src.count(gate) == 11, f"expected 11 '' fallback gates, got {src.count(gate)}"


def test_v1_23_87_version_pin():
    init_py = (Path(__file__).resolve().parent.parent
               / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
