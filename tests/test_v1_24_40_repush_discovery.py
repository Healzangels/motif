"""v1.24.40 — RP (re-push needed) discoverability + rk-liveness accuracy.

Three parts, from the user's review of the live MOVIES page (his Two Towers row
read RP even though its uploaded rating_key was live + serving):

  (a) Accuracy — _LIB_STALE_PU_SQL now requires the uploaded rk is genuinely
      DEAD (no live plex_items row). A re-linked / self-healed placement (stale
      theme_present=0 stamp, but the rk is back) no longer false-reads RP; it
      self-corrects at READ time instead of waiting for the next enum.
  (b) ⟳ ATTN filter chip — attn_pills=repush, reusing the SAME _LIB_STALE_PU_SQL
      predicate as link_pills=rp (one source of truth, two entry points).
  (c) Topbar RE-PUSH count badge — _REPUSH_COUNT_SQL counts genuinely-dead RP so
      the badge tracks the rendered chips (mirrors FAIL/UPD/DROP).
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core.db import get_conn, init_db

NOW = datetime.now(timezone.utc).isoformat(timespec="seconds")
AUTH = {"X-Authentik-Username": "testadmin"}
REPO = Path(__file__).resolve().parent.parent


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.web.api import create_app
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    with sqlite3.connect(s.db_path) as c:
        c.execute("INSERT INTO plex_sections (section_id, title, type, is_anime,"
                  " is_4k, themes_subdir, included, discovered_at, last_seen_at) "
                  "VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        c.commit()
    return TestClient(create_app(s)), s.db_path


def _pu(c, *, tmdb, tid, title, live_rk, placement_rk):
    # A plex_upload placement (theme_present=0) whose live plex_items row is
    # live_rk; the placement points at placement_rk. When placement_rk == live_rk
    # the rk is alive (re-linked / self-healed → NOT genuinely RP); when it's a
    # different, never-enumerated rk it's genuinely dead → RP.
    c.execute("INSERT INTO themes (id, media_type, tmdb_id, title, year, "
              " upstream_source, last_seen_sync_at, first_seen_sync_at) "
              "VALUES (?, 'movie', ?, ?, '2002', 'imdb', ?, ?)",
              (tid, tmdb, title, NOW, NOW))
    c.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, "
              " edition_key, theme_id, file_path, source_kind, source_video_id, "
              " downloaded_at, canonical_present) "
              "VALUES ('movie', ?, '1', '', ?, 'f.mp3','themerrdb','v',?,1)",
              (tmdb, tid, NOW))
    c.execute("INSERT INTO placements (theme_id, media_type, tmdb_id, section_id,"
              " edition_key, media_folder, placed_at, placement_kind, "
              " plex_rating_key, plex_refreshed, theme_present) "
              "VALUES (?, 'movie', ?, '1', '', '', ?, 'plex_upload', ?, 1, 0)",
              (tid, tmdb, NOW, placement_rk))
    c.execute("INSERT INTO plex_items (rating_key, section_id, media_type, "
              " theme_id, guid_tmdb, title, edition_key, has_theme, "
              " first_seen_at, last_seen_at) "
              "VALUES (?, '1','movie',?,?,?,'',1,?,?)",
              (live_rk, tid, tmdb, title, NOW, NOW))


def _rows(c, qs=""):
    r = c.get(f"/api/library?tab=movies{qs}", headers=AUTH)
    assert r.status_code == 200, r.text
    return {it["rating_key"]: it for it in r.json()["items"]}


# ── (a) rk-liveness accuracy ────────────────────────────────────────────

def test_live_rk_stale_stamp_no_longer_reads_rp(client):
    # the user's Two Towers: placement rk is LIVE (== the plex_items rk) but the
    # theme_present=0 stamp is stale → must NOT read RP.
    c, db = client
    with get_conn(db) as conn:
        _pu(conn, tmdb=-1, tid=10, title="Two Towers",
            live_rk="rk-live", placement_rk="rk-live")
        conn.commit()
    row = _rows(c)["rk-live"]
    assert row["needs_repush"] == 0, "a live rk must self-correct out of RP"
    assert row["placement_kind"] == "plex_upload", "it's still placed"


def test_dead_rk_reads_rp(client):
    # A genuine re-add: the placement points at a destroyed rk (absent from
    # plex_items) → RP.
    c, db = client
    with get_conn(db) as conn:
        _pu(conn, tmdb=-2, tid=11, title="Avenue Q",
            live_rk="rk-new", placement_rk="rk-dead")
        conn.commit()
    row = _rows(c)["rk-new"]
    assert row["needs_repush"] == 1, "a destroyed upload rk must read RP"
    assert not row["placement_kind"], "stale plex_upload reads not-placed"


# ── (b) ATTN repush filter == link rp filter (same rows) ────────────────

def test_attn_repush_filter_matches_only_genuine_rp(client):
    c, db = client
    with get_conn(db) as conn:
        _pu(conn, tmdb=-1, tid=10, title="Live", live_rk="L", placement_rk="L")
        _pu(conn, tmdb=-2, tid=11, title="Dead", live_rk="N", placement_rk="dead")
        conn.commit()
    attn = _rows(c, "&attn_pills=repush")
    link = _rows(c, "&link_pills=rp")
    assert set(attn) == {"N"}, "ATTN repush surfaces only the genuinely-dead RP"
    assert set(link) == {"N"}, "LINK rp surfaces the identical row set"


# ── (c) badge count tracks genuinely-dead RP ────────────────────────────

def test_repush_count_sql_excludes_live_rk(client):
    from app.web.api import _REPUSH_COUNT_SQL
    c, db = client
    with get_conn(db) as conn:
        _pu(conn, tmdb=-1, tid=10, title="Live", live_rk="L", placement_rk="L")
        _pu(conn, tmdb=-2, tid=11, title="Dead", live_rk="N", placement_rk="dead")
        conn.commit()
        n = conn.execute(_REPUSH_COUNT_SQL).fetchone()[0]
    assert n == 1, "badge counts only the genuinely-dead RP (live rk excluded)"


# ── source pins: every surface present ──────────────────────────────────

def test_surfaces_present():
    api = (REPO / "app" / "web" / "api.py").read_text()
    assert '"repush"' in api                       # attn_pills valid set token
    # the ATTN branch reuses the shared predicate (no parallel implementation)
    assert 'elif p == "repush":' in api
    assert "attn_branches.append(f\"({_LIB_STALE_PU_SQL})\")" in api
    assert "_REPUSH_COUNT_SQL" in api
    base = (REPO / "app" / "web" / "templates" / "base.html").read_text()
    assert "topbar-repush-badge" in base and "attn_pills=repush" in base
    lib = (REPO / "app" / "web" / "templates" / "library.html").read_text()
    assert 'data-attn-pill="repush"' in lib       # filter chip
    assert "gg-repush" in lib                      # legend FLAGS entry
    ops = (REPO / "app" / "web" / "static" / "ops.css").read_text()
    assert "op-tone-repush" in ops
