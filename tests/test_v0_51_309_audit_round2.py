"""v0.51.309 — build audit round 2: five fixes over the .307/.308 delta.

  1. Anime MOVIES misrouted: both routing ternaries tested mt==='movie'
     before data-anime, but /movies excludes is_anime sections while
     /anime takes movie-typed anime rows — anime films landed on a tab
     that filters them out. Anime is tested FIRST now (order pins live
     beside the .308 tests).
  2. The .307 double-activation absorb was click-path only — after the
     dot's focus park, a fast second Enter (or key-repeat) activated
     .notif-main through the KEYDOWN delegate and navigated. Same
     400ms dotReadTs bail there now.
  3. plex_rating_key resolution matched guid_tmdb only — AniDB anime
     and most collections have guid_tmdb NULL and bond via theme_id,
     so the fix's own target rows stayed posterless. Two-arm match
     (guid_tmdb OR theme_id — the v1.22.17 class), driven below.
  4. The .307 focus park lands on .notif-main, but the focus-visible
     ring was scoped to .notif-clickable rows — keyboard focus visibly
     vanished on digest rows. The ring covers every row's main now.
  5. The 404 NOT IN LIBRARY card rendered .dlg-section as the dialog's
     only child — a primitive styled as a mid-card divider, drawing a
     stray rule + dead space. A leading-section neutralizer fixes it
     (:first-child since v0.51.311 — :only-child broke on a 2nd element).
"""
from __future__ import annotations

from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
OPS_CSS = (REPO / "app" / "web" / "static" / "ops.css").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
AUTH = {"X-Authentik-Username": "testadmin"}
NOW = "2026-08-30T12:00:00+00:00"


# ── 2. the keydown-path absorb ───────────────────────────────


def test_keydown_row_branch_absorbs_the_dot_read_tail():
    i = APP_JS.index("listEl.addEventListener('keydown'")
    end = APP_JS.index("openNotifRow(row);", i) + len("openNotifRow(row);")
    blk = APP_JS[i:end]
    seg = blk[blk.index("closest('.notif-row.notif-clickable')"):]
    assert "dotReadTs" in seg and "return" in seg, (
        "the click-path absorb alone left a keyboard escape: the focus park "
        "puts .notif-main under a second Enter / key-repeat, and the keydown "
        "delegate navigated with no bail")
    assert seg.index("dotReadTs") < seg.index("openNotifRow(row);")


# ── 3. two-arm poster resolution, driven ─────────────────────


@pytest.fixture
def client(tmp_path, monkeypatch):
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import get_conn, init_db, transaction
    from app.web.api import create_app
    from fastapi.testclient import TestClient
    (tmp_path / "data").mkdir()
    (tmp_path / "motif.yaml").write_text("paths: {}\n")
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    with get_conn(s.db_path) as conn, transaction(conn):
        conn.execute(
            """INSERT INTO plex_sections (section_id, title, type, is_anime,
                 is_4k, themes_subdir, included, discovered_at, last_seen_at)
               VALUES ('1', 'Anime', 'show', 1, 0, 'anime', 1, ?, ?)""",
            (NOW, NOW))
        cur = conn.execute(
            """INSERT INTO themes (media_type, tmdb_id, title,
                 upstream_source, last_seen_sync_at, first_seen_sync_at)
               VALUES ('tv', 309001, 'AniDB-ish', 'imdb', ?, ?)""",
            (NOW, NOW))
        # the AniDB shape: guid_tmdb NULL, bonded via theme_id only.
        conn.execute(
            """INSERT INTO plex_items (rating_key, section_id, media_type,
                 title, guid_tmdb, theme_id, folder_path, edition_key,
                 has_theme, first_seen_at, last_seen_at)
               VALUES ('5555', '1', 'show', 'AniDB-ish', NULL, ?,
                       '/data/anime/A', '', 1, ?, ?)""",
            (cur.lastrowid, NOW, NOW))
    return TestClient(create_app(s))


def test_api_item_resolves_guidless_rows_via_theme_id(client):
    r = client.get("/api/items/tv/309001?section_id=1", headers=AUTH)
    assert r.status_code == 200
    assert r.json()["plex_rating_key"] == "5555", (
        "AniDB anime / collections have guid_tmdb NULL and bond via "
        "theme_id — the guid-only arm left the fix's own target rows "
        "(anime P-rows) posterless")


# ── 4 + 5. CSS: the ring covers every row; the lone section ──


def test_focus_ring_covers_every_rows_main():
    i = OPS_CSS.index(".notif-row .notif-main:focus-visible")
    rule = OPS_CSS[OPS_CSS.index("{", i):OPS_CSS.index("}", i)]
    assert "outline" in rule, (
        "every .notif-main is a control (v0.51.274) and the .307 focus park "
        "lands there — a .notif-clickable-scoped ring left digest rows "
        "with invisible focus")
    assert ".notif-row.notif-clickable .notif-main:focus-visible" not in OPS_CSS


def test_lone_dlg_section_sheds_its_divider():
    i = APP_CSS.index("#info-dlg-body > .dlg-section:first-child")
    rule = APP_CSS[APP_CSS.index("{", i):APP_CSS.index("}", i)]
    for prop in ("margin-top: 0", "padding-top: 0", "border-top: 0"):
        assert prop in rule, (
            ".dlg-section is styled as a mid-card divider — as the 404 "
            "card's only child it drew a stray rule + dead space")


def test_v0_51_309_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert "0.51.309: " in init_py
