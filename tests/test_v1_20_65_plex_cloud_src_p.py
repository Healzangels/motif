"""v1.20.65 — a PROMOTED cloud-backup row renders SRC=P, not T.

PROMOTE TO ACTIVE on a cloud-backup (source_kind='plex_cloud') row
re-uploads Plex's own cloud theme back to Plex as a plex_upload
placement (media_folder='', provenance defaults to 'auto'). It rendered
SRC=T via the provenance='auto' placed-fallback — affirmatively wrong
(no ThemerrDB involved; T also makes it eligible for TDB-update prompts).
the user's call: render P (the letter it showed pre-PROMOTE — PROMOTE
changes placement ownership, not content source).

New branch in BOTH _SRC_LETTER_SQL (api.py) and computeSrcLetter (app.js),
placed ABOVE the provenance='auto'→T fallback. Per the CLAUDE.md SRC-axis
discipline: a direct SQL exercise + a JS mirror-drift guard.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from app.core.db import init_db
from app.web.api import _SRC_LETTER_SQL


REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
NOW = "2026-06-01T00:00:00+00:00"


@pytest.fixture
def db(tmp_path):
    p = tmp_path / "motif.db"
    init_db(p)
    return p


def _seed(conn, *, theme_id, tmdb_id, rk, source_kind=None,
          placement_kind=None, media_folder=None, provenance="auto",
          has_theme=0, verified_ok=None):
    conn.execute(
        "INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source,"
        " last_seen_sync_at, first_seen_sync_at)"
        " VALUES (?, 'movie', ?, ?, 'imdb', ?, ?)",
        (theme_id, tmdb_id, f"x{tmdb_id}", NOW, NOW),
    )
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, media_type, theme_id,"
        " guid_tmdb, title, has_theme, plex_theme_verified_ok, first_seen_at,"
        " last_seen_at) VALUES (?, '1', 'movie', ?, ?, ?, ?, ?, ?, ?)",
        (rk, theme_id, tmdb_id, f"x{tmdb_id}", has_theme, verified_ok, NOW, NOW),
    )
    if source_kind is not None:
        conn.execute(
            "INSERT INTO local_files (media_type, tmdb_id, section_id, file_path,"
            " source_video_id, downloaded_at, source_kind)"
            " VALUES ('movie', ?, '1', 'x.mp3', 'vid', ?, ?)",
            (tmdb_id, NOW, source_kind),
        )
    if placement_kind is not None:
        conn.execute(
            "INSERT INTO placements (theme_id, media_type, tmdb_id, section_id,"
            " media_folder, placed_at, placement_kind, plex_refreshed, provenance)"
            " VALUES (?, 'movie', ?, '1', ?, ?, ?, 1, ?)",
            (theme_id, tmdb_id, media_folder, NOW, placement_kind, provenance),
        )


def _letter(db, rk):
    sql = f"""
        SELECT ({_SRC_LETTER_SQL}) AS letter
        FROM plex_items pi
        LEFT JOIN themes t ON t.id = pi.theme_id
        LEFT JOIN placements p ON p.media_type = t.media_type
             AND p.tmdb_id = t.tmdb_id AND p.section_id = pi.section_id
        LEFT JOIN local_files lf ON lf.media_type = t.media_type
             AND lf.tmdb_id = t.tmdb_id AND lf.section_id = pi.section_id
        WHERE pi.rating_key = ?
    """
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        return conn.execute(sql, (rk,)).fetchone()["letter"]


def test_promoted_plex_cloud_row_is_P(db):
    """The fix: plex_upload placement (media_folder='') + source_kind=
    'plex_cloud' + provenance='auto' → P, NOT T."""
    with sqlite3.connect(db) as conn:
        _seed(conn, theme_id=1, tmdb_id=1, rk="rk-cloud",
              source_kind="plex_cloud", placement_kind="plex_upload",
              media_folder="", provenance="auto", has_theme=1, verified_ok=1)
        conn.commit()
    assert _letter(db, "rk-cloud") == "P"


def test_staged_unplaced_plex_cloud_still_P(db):
    """A staged (not-yet-promoted) cloud backup has no placement +
    has_theme=1 → P via the existing media_folder IS NULL branch."""
    with sqlite3.connect(db) as conn:
        _seed(conn, theme_id=2, tmdb_id=2, rk="rk-staged",
              source_kind="plex_cloud", placement_kind=None,
              has_theme=1, verified_ok=1)
        conn.commit()
    assert _letter(db, "rk-staged") == "P"


def test_placed_themerrdb_still_T(db):
    """Control: a normal placed ThemerrDB row is unaffected."""
    with sqlite3.connect(db) as conn:
        _seed(conn, theme_id=3, tmdb_id=3, rk="rk-tdb",
              source_kind="themerrdb", placement_kind="hardlink",
              media_folder="/data/x", provenance="auto")
        conn.commit()
    assert _letter(db, "rk-tdb") == "T"


def test_placed_auto_provenance_non_cloud_still_T(db):
    """Control: the provenance='auto' fallback still yields T for a
    placed row that ISN'T plex_cloud (no source_kind match) — the new
    branch must not steal it."""
    with sqlite3.connect(db) as conn:
        _seed(conn, theme_id=4, tmdb_id=4, rk="rk-auto",
              source_kind=None, placement_kind="plex_upload",
              media_folder="", provenance="auto")
        conn.commit()
    assert _letter(db, "rk-auto") == "T"


def test_js_compute_src_letter_mirrors_sql():
    """Mirror-drift guard: computeSrcLetter must have the plex_cloud→P
    branch, placed BEFORE the placedProv==='auto'→T fallback."""
    start = APP_JS.index("function computeSrcLetter(")
    body = APP_JS[start:start + 1600]
    cloud = body.index("sourceKind === 'plex_cloud') return 'P'")
    auto = body.index("placedProv === 'auto') return 'T'")
    assert cloud < auto, (
        "plex_cloud→P must precede the provenance='auto'→T fallback, "
        "mirroring _SRC_LETTER_SQL"
    )


def test_js_inline_src_chip_mirrors_sql():
    """v1.21.8 mirror-drift fix: the renderLibraryRow inline-SRC chip
    (the visible row pill) must ALSO carry the plex_cloud→P branch
    before its placedProv==='auto'→T fallback. v1.20.65 added the
    branch to computeSrcLetter + _SRC_LETTER_SQL but missed this THIRD
    placed-render site — so a PROMOTED plex_cloud row rendered T in the
    chip while the src filter / sort / dashboard donut all said P (the
    v1.18.0→v1.18.24 inline-lag class). This guard closes the gap the
    original test_js_compute_src_letter_mirrors_sql left open (it only
    windowed computeSrcLetter)."""
    cloud = APP_JS.index(
        "else if (placed && sourceKind === 'plex_cloud') {")
    auto = APP_JS.index(
        "else if (placed && placedProv === 'auto') {")
    assert cloud < auto, (
        "v1.21.8: inline-SRC plex_cloud→P branch must precede the "
        "placedProv==='auto'→T fallback"
    )
    seg = APP_JS[cloud:auto]
    assert "link-badge-cloud" in seg and ">P<" in seg


def test_v1_20_65_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
