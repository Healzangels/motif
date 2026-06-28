"""v1.22.22 — section-keyed-without-edition bleeds (edition audit).

Three writes were keyed by (…, section_id) but not edition, so they bled across
sibling editions of a multi-edition title:

#3 api_unplace_item (LET PLEX SERVE / UNPLACE): the plex_items flag flip
   (local_theme_file=0, plex_theme_verified_ok=NULL) used the section-wide
   pi_where while the placements DELETE + local_files UPDATE were already
   edition-scoped — so LPS on one edition dropped a sibling's SRC pill (M/A→P/–)
   until the next plex_enum. Now scoped to the clicked rating_key (the
   plex_items PK).

#1 api_manual_url (SET URL) urls_match path: cleared + re-inserted
   pending_updates for the whole section, wiping a sibling edition's per-edition
   accepted/declined decision (v1.21.81). Now edition-scoped.

#4 scanner._classify_and_record: the `theme_id = ?` disjunct is title-wide
   (themes keyed by media_type+tmdb_id), so a sidecar found in ONE edition's
   folder flipped local_theme_file=1 on EVERY edition's row. Now the theme_id
   branch is edition-scoped to the scanned folder's edition.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()
SCANNER_PY = (REPO / "app" / "core" / "scanner.py").read_text()
NOW = "2026-06-06T00:00:00"
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
    db = settings.db_path
    init_db(db)
    init_auth_schema(db)
    create_admin(db, username="testadmin", password="testpassword")
    return TestClient(create_app(settings)), db


def _two_edition_title(conn, *, tmdb=500, theme_url="https://www.youtube.com/watch?v=TDB"):
    conn.execute(
        "INSERT OR IGNORE INTO plex_sections (section_id, title, type, is_anime,"
        " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
        " VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
    conn.execute(
        "INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source,"
        " last_seen_sync_at, first_seen_sync_at, youtube_url,"
        " youtube_video_id) VALUES (1,'movie',?,'Multi','imdb',?,?,?, 'TDB')",
        (tmdb, NOW, NOW, theme_url))
    for rk, ed, folder, theme in (
        ("rk-std", "", "/data/m/Multi", 1),
        ("rk-ext", "extended", "/data/m/Multi {edition-Extended}", 1),
    ):
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type,"
            " theme_id, guid_imdb, guid_tmdb, title, year, has_theme,"
            " local_theme_file, folder_path, plex_independent_theme,"
            " plex_theme_verified_ok, edition_key, first_seen_at, last_seen_at)"
            " VALUES (?, '1','movie',1,?,?,'Multi',2012,1,?,?,0,1,?,?,?)",
            (rk, f"tt{tmdb}", tmdb, theme, folder, ed, NOW, NOW))


def _ltf(db, rk):
    with sqlite3.connect(db) as conn:
        return conn.execute(
            "SELECT local_theme_file FROM plex_items WHERE rating_key=?",
            (rk,)).fetchone()[0]


# ── #3 UNPLACE plex_items flag flip is rk-scoped ─────────────


def test_unplace_does_not_drop_sibling_edition_local_theme_flag(admin_client):
    client, db = admin_client
    with sqlite3.connect(db) as conn:
        _two_edition_title(conn)
        # Each edition has its own placement (distinct folders).
        for ed, folder in (("", "/data/m/Multi"),
                           ("extended", "/data/m/Multi {edition-Extended}")):
            conn.execute(
                "INSERT INTO placements (media_type, tmdb_id, section_id,"
                " edition_key, media_folder, placed_at, placement_kind,"
                " plex_rating_key, plex_refreshed, provenance)"
                " VALUES ('movie',500,'1',?,?,?, 'hardlink', NULL, 1, 'auto')",
                (ed, folder, NOW))
        conn.commit()

    r = client.post("/api/items/movie/500/unplace?section_id=1&rating_key=rk-std",
                    headers=AUTH)
    assert r.status_code == 200, r.text

    assert _ltf(db, "rk-std") == 0, "the unplaced edition's flag clears"
    assert _ltf(db, "rk-ext") == 1, (
        "v1.22.22 #3: the sibling Extended edition's local_theme_file must "
        "stay 1 — LPS on Standard must not drop its SRC pill")


# ── #1 SET URL urls_match clear is edition-scoped ────────────


def test_set_url_urls_match_spares_sibling_pending_decision(admin_client):
    client, db = admin_client
    with sqlite3.connect(db) as conn:
        _two_edition_title(conn, theme_url="https://www.youtube.com/watch?v=dQw4w9WgXcQ")
        # Standard edition has a DECLINED per-edition decision that must survive.
        conn.execute(
            "INSERT INTO pending_updates (media_type, tmdb_id, section_id,"
            " edition_key, kind, new_youtube_url, decision, detected_at)"
            " VALUES ('movie',500,'1','','upstream_changed',"
            " 'https://www.youtube.com/watch?v=OTHER','declined',?)", (NOW,))
        conn.commit()

    # SET URL on the Extended edition with the URL TDB serves → urls_match path.
    r = client.post("/api/plex_items/rk-ext/manual-url", headers=AUTH,
                    json={"youtube_url": "https://www.youtube.com/watch?v=dQw4w9WgXcQ"})
    assert r.status_code == 200, r.text

    with sqlite3.connect(db) as conn:
        std = conn.execute(
            "SELECT decision FROM pending_updates WHERE tmdb_id=500"
            " AND section_id='1' AND edition_key=''").fetchone()
    assert std is not None and std[0] == "declined", (
        "v1.22.22 #1: the Standard edition's declined decision must survive a "
        "SET URL on the Extended edition (pre-fix the section-wide DELETE "
        "wiped it)")


# ── Source pins ──────────────────────────────────────────────


def test_unplace_pi_where_scoped_to_rating_key():
    i = API_PY.index("when the click carried a rating_key")
    block = API_PY[i:i + 1000]
    assert 'pi_where = "rating_key = ?"' in block
    assert "pi_where_args = [rating_key]" in block


def test_set_url_pending_clear_and_insert_carry_edition():
    i = API_PY.index("scope the clear + re-insert to the")
    block = API_PY[i:i + 2400]
    assert "AND edition_key = ?" in block, "the DELETE must filter edition_key"
    assert "section_id, edition_key," in block, (
        "the urls_match re-INSERT must include the edition_key column")


def test_scanner_theme_id_branch_is_edition_scoped():
    i = SCANNER_PY.index("edition_key_for_folder(folder_str)")
    block = SCANNER_PY[i - 200:i + 400]
    assert "theme_id = ? AND edition_key = ?" in block, (
        "v1.22.22 #4: the title-wide theme_id disjunct must be edition-scoped")


def test_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
