"""v1.18.39 — Resolve local_files.file_path against themes_dir.

the user's v1.18.38 100% Wolf re-test surfaced this bug. Docker
log showed:

  unplace[plex_upload]: motif_hash unavailable for movie/520946
    section_id=1 — reason: file missing:
    movies/100% Wolf (2020)/theme.mp3.

That's a RELATIVE path. But motif's info modal shows the
canonical lives at `/data/media/themes/movies/100% Wolf
(2020)/theme.mp3` and is present (3,895,172 bytes). Motif
stores `local_files.file_path` RELATIVE to `settings.themes_dir`
(per worker.py:1812, 2347 — `themes_dir / local["file_path"]`).

v1.18.36's motif_hash compute missed the join and treated the
relative path as absolute. Hash failed on every plex_upload row.
The orphan scan from v1.18.37 confirmed the breadth:
`{"motif_hash_unknown": 10}` across the user's install.

The fallback heuristic happened to pick the correct themerr-plex
entry on 100% Wolf only because it was first in Plex's
enumeration. For some other row where motif's own entry happens
to be first, the picker would select motif's theme and the
re-upload trick would just re-select motif's own theme —
defeating the LPS intent.

## Fix

Both `api_unplace_item` and `orphan_scan.scan_plex_upload_placements`
now join `settings.themes_dir / file_path` before hashing. Absolute
paths (legacy rows) pass through unchanged.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from app.core.db import get_conn, init_db, transaction


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"


# ── orphan_scan._resolve_canonical helper ────────────────────


def test_resolve_canonical_joins_relative_with_themes_dir(tmp_path):
    """Relative file_path + themes_dir → joined absolute path."""
    from app.core.orphan_scan import _resolve_canonical
    themes_dir = tmp_path / "themes"
    out = _resolve_canonical(
        themes_dir, "movies/100% Wolf (2020)/theme.mp3",
    )
    assert out == themes_dir / "movies/100% Wolf (2020)/theme.mp3"


def test_resolve_canonical_passes_absolute_through(tmp_path):
    """Absolute paths (legacy rows) pass through unchanged."""
    from app.core.orphan_scan import _resolve_canonical
    themes_dir = tmp_path / "themes"
    out = _resolve_canonical(themes_dir, "/data/already/absolute.mp3")
    assert out == Path("/data/already/absolute.mp3")


def test_resolve_canonical_no_themes_dir_returns_as_is():
    """themes_dir=None → return Path(rel_or_abs) unchanged (test
    fixtures that pass absolute paths don't need themes_dir)."""
    from app.core.orphan_scan import _resolve_canonical
    out = _resolve_canonical(None, "/abs/path.mp3")
    assert out == Path("/abs/path.mp3")


# ── orphan_scan honors themes_dir kwarg ──────────────────────


def _seed_placement(db, *, media_type, tmdb_id, section_id):
    ts = "2026-05-21T00:00:00+00:00"
    with get_conn(db) as conn, transaction(conn):
        conn.execute(
            "INSERT OR IGNORE INTO themes "
            "(media_type, tmdb_id, title, youtube_url, "
            " upstream_source, last_seen_sync_at, "
            " first_seen_sync_at) "
            "VALUES (?, ?, ?, ?, 'themoviedb', ?, ?)",
            (media_type, tmdb_id, f"Test {tmdb_id}", "", ts, ts),
        )
        conn.execute(
            "INSERT OR IGNORE INTO plex_sections "
            "(section_id, title, type, included, "
            " discovered_at, last_seen_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (section_id, "Test Section", "movie", 1, ts, ts),
        )
        conn.execute(
            "INSERT OR IGNORE INTO plex_items "
            "(rating_key, section_id, media_type, title, year, "
            " guid_tmdb, has_theme, first_seen_at, last_seen_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (f"rk-{tmdb_id}", section_id, "movie",
             f"Test {tmdb_id}", 2024, str(tmdb_id), 1, ts, ts),
        )
        conn.execute(
            "INSERT OR REPLACE INTO placements "
            "(media_type, tmdb_id, section_id, placement_kind, "
            " media_folder, placed_at) "
            "VALUES (?, ?, ?, 'plex_upload', '', ?)",
            (media_type, tmdb_id, section_id, ts),
        )


def test_scan_uses_themes_dir_to_find_canonical(tmp_path):
    """When themes_dir is passed + local_files.file_path is
    RELATIVE, the scanner correctly joins them and hashes the
    file. Pre-v1.18.39 this returned motif_hash_unknown."""
    from app.core.orphan_scan import scan_plex_upload_placements
    db = tmp_path / "motif.db"
    init_db(db)
    _seed_placement(db, media_type="movie", tmdb_id=200,
                     section_id="1")
    # Put a fake canonical at themes_dir/movies/Test 200/theme.mp3
    themes_dir = tmp_path / "themes"
    canonical_dir = themes_dir / "movies" / "Test 200"
    canonical_dir.mkdir(parents=True)
    canonical = canonical_dir / "theme.mp3"
    canonical.write_bytes(b"theme audio test bytes")
    # local_files row stores the RELATIVE path.
    ts = "2026-05-21T00:00:00+00:00"
    with get_conn(db) as conn, transaction(conn):
        conn.execute(
            "INSERT OR REPLACE INTO local_files "
            "(media_type, tmdb_id, section_id, file_path, "
            " downloaded_at, source_video_id) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("movie", 200, "1",
             "movies/Test 200/theme.mp3",  # RELATIVE
             ts, "test_video"),
        )
    # Plex has a single entry matching motif's hash → drift_type=ok.
    expected_hash = hashlib.sha1(canonical.read_bytes()).hexdigest()
    plex = MagicMock()
    plex.get_themes.return_value = {
        "ok": True, "http_status": 200, "error": None,
        "body": {"MediaContainer": {"size": 1, "Metadata": [
            {"ratingKey": f"upload://themes/{expected_hash}",
             "selected": True},
        ]}},
    }
    findings = scan_plex_upload_placements(
        db, plex, themes_dir=themes_dir,
    )
    assert len(findings) == 1
    assert findings[0]["drift_type"] == "ok", (
        f"v1.18.39 expected hash match; got "
        f"{findings[0]['drift_type']}: {findings[0]['details']}"
    )
    # motif_canonical reports the RESOLVED absolute path.
    assert "movies/Test 200/theme.mp3" in findings[0]["motif_canonical"]
    assert str(themes_dir) in findings[0]["motif_canonical"]


def test_scan_without_themes_dir_treats_path_as_absolute(tmp_path):
    """Backward compat: omit themes_dir, paths in local_files.
    file_path are treated as already-absolute. Used by older
    test fixtures."""
    from app.core.orphan_scan import scan_plex_upload_placements
    db = tmp_path / "motif.db"
    init_db(db)
    _seed_placement(db, media_type="movie", tmdb_id=201,
                     section_id="1")
    # Put canonical at an absolute path; local_files stores absolute.
    canonical = tmp_path / "abs_theme.mp3"
    canonical.write_bytes(b"absolute theme bytes")
    ts = "2026-05-21T00:00:00+00:00"
    with get_conn(db) as conn, transaction(conn):
        conn.execute(
            "INSERT OR REPLACE INTO local_files "
            "(media_type, tmdb_id, section_id, file_path, "
            " downloaded_at, source_video_id) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("movie", 201, "1", str(canonical),
             ts, "test_video"),
        )
    expected_hash = hashlib.sha1(canonical.read_bytes()).hexdigest()
    plex = MagicMock()
    plex.get_themes.return_value = {
        "ok": True, "http_status": 200, "error": None,
        "body": {"MediaContainer": {"size": 1, "Metadata": [
            {"ratingKey": f"upload://themes/{expected_hash}",
             "selected": True},
        ]}},
    }
    findings = scan_plex_upload_placements(db, plex)
    assert findings[0]["drift_type"] == "ok"


# ── api_unplace_item joins themes_dir ────────────────────────


def test_api_unplace_uses_themes_dir_for_motif_hash():
    """The plex_upload branch in api_unplace_item must join
    settings.themes_dir / file_path before hashing. Pin the
    source so a refactor doesn't accidentally lose it."""
    src = API_PY.read_text()
    fn_idx = src.index("async def api_unplace_item(")
    body = src[fn_idx:fn_idx + 28000]
    # The fix uses settings.themes_dir / rel.
    assert "settings.themes_dir / rel" in body, (
        "v1.18.39: motif_hash compute must join themes_dir with "
        "the relative file_path from local_files"
    )
    # The absolute-path passthrough must still be present.
    assert "Path(rel).is_absolute()" in body


def test_api_unplace_diagnostic_log_message_explains_path():
    """The diagnostic log line for missing files must include
    the RESOLVED path (not the raw relative one). Otherwise
    operators see paths that don't exist on disk and can't
    confirm whether motif's resolution is correct."""
    src = API_PY.read_text()
    fn_idx = src.index("async def api_unplace_item(")
    body = src[fn_idx:fn_idx + 28000]
    # canonical variable holds the resolved path; log references it.
    assert 'f"file missing: {canonical}"' in body
