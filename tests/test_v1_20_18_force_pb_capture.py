"""v1.20.18 — force-PB capture on non-C1 rows.

the user: "lets build it" — on a row where DOWNLOAD PLEX BACKUP finds no
cloud (metadata://) theme to auto-back-up, offer a confirm-retry that
captures WHATEVER Plex is currently serving (incl an upload:// themerr-
plex embed) and stages it as a Plex Backup (PB), swapping any existing
TB/UB. Reversible (ThemerrDB linkage lives on themes.youtube_url). A
sha256 dedup guard makes it a no-op when Plex is echoing motif's own
uploaded bytes back.

Covers:
- _classify_themes_response_force: returns the SELECTED entry whatever
  the scheme (where strict returns None for upload://-selected rows).
- identify_c1_rows(force=True): captures an upload-selected row that
  strict skips.
- backup_cloud_theme dedup guard: byte-identical served bytes → skip
  the swap (no relabel); different bytes → REPLACE to plex_cloud.
- endpoint + worker thread the `force` flag; worker tallies
  skipped_identical separately.
- app.js confirm-retry + force POST + PB tooltip retune.
"""
from __future__ import annotations

import hashlib
import sqlite3
from pathlib import Path
from unittest.mock import MagicMock


REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()
LIB_HTML = (REPO / "app" / "web" / "templates" / "library.html").read_text()
CTB_PY = (REPO / "app" / "core" / "cloud_theme_backup.py").read_text()

H40 = "a" * 40
G40 = "b" * 40


# ── force classifier ─────────────────────────────────────────


def test_force_classifier_captures_selected_upload():
    """Strict skips an upload://-selected row; force captures it."""
    from app.core.cloud_theme_backup import (
        _classify_themes_response, _classify_themes_response_force,
    )
    body = {"MediaContainer": {"Metadata": [
        {"ratingKey": "metadata://themes/" + H40, "selected": False},
        {"ratingKey": "upload://themes/" + G40, "selected": True},
    ]}}
    assert _classify_themes_response(body) is None, (
        "strict must skip an upload-selected row"
    )
    pick = _classify_themes_response_force(body)
    assert pick is not None, "force must capture the selected upload"
    assert pick["entry_uri"] == "upload://themes/" + G40
    assert pick["sha1"] == G40


def test_force_classifier_captures_selected_metadata():
    from app.core.cloud_theme_backup import _classify_themes_response_force
    body = {"MediaContainer": {"Metadata": [
        {"ratingKey": "metadata://themes/" + H40, "selected": True},
    ]}}
    pick = _classify_themes_response_force(body)
    assert pick and pick["entry_uri"] == "metadata://themes/" + H40


def test_force_classifier_none_when_nothing_selected():
    from app.core.cloud_theme_backup import _classify_themes_response_force
    body = {"MediaContainer": {"Metadata": [
        {"ratingKey": "metadata://themes/" + H40, "selected": False},
        {"ratingKey": "upload://themes/" + G40, "selected": False},
    ]}}
    assert _classify_themes_response_force(body) is None


def test_force_classifier_legacy_no_flag_defers_to_strict():
    """No `selected` flag at all → can't tell what's serving →
    fall back to the strict single-metadata heuristic."""
    from app.core.cloud_theme_backup import (
        _classify_themes_response, _classify_themes_response_force,
    )
    body = {"MediaContainer": {"Metadata": [
        {"ratingKey": "metadata://themes/" + H40},
    ]}}
    assert (
        _classify_themes_response_force(body)
        == _classify_themes_response(body)
    )


# ── identify_c1_rows(force=True) ─────────────────────────────


def _seed_pure_p_row(conn, *, rk, tmdb_id, section_id="1"):
    conn.execute(
        "INSERT OR IGNORE INTO plex_sections "
        "  (section_id, title, type, is_anime, is_4k, themes_subdir, "
        "   included, discovered_at, last_seen_at) "
        "VALUES (?, 'Movies', 'movie', 0, 0, 'movies', 1, "
        "        '2026-05-29', '2026-05-29')", (section_id,))
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, media_type, "
        "  guid_tmdb, title, year, has_theme, first_seen_at, last_seen_at) "
        "VALUES (?, ?, 'movie', ?, 'Force Row', 2020, 1, "
        "        '2026-05-29', '2026-05-29')", (rk, section_id, tmdb_id))


def test_identify_force_captures_upload_selected_row(tmp_path):
    from app.core.db import init_db
    from app.core.cloud_theme_backup import identify_c1_rows
    db = tmp_path / "t.db"
    init_db(db)
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        _seed_pure_p_row(conn, rk="rk-up", tmdb_id=900)
        conn.commit()
        plex = MagicMock()

        def _get_themes(*, rating_key):
            return {"ok": True, "http_status": 200, "error": None,
                    "body": {"MediaContainer": {"Metadata": [
                        {"ratingKey": "upload://themes/" + G40,
                         "selected": True},
                    ]}}}
        plex.get_themes.side_effect = _get_themes

        strict = identify_c1_rows(
            conn, plex, inter_call_sleep_s=0, use_cursor=False,
            rks_scope=["rk-up"])
        assert strict == [], "strict must skip the upload-selected row"

        forced = identify_c1_rows(
            conn, plex, inter_call_sleep_s=0, use_cursor=False,
            rks_scope=["rk-up"], force=True)
        assert len(forced) == 1, "force must capture the served upload"
        assert forced[0]["entry_uri"] == "upload://themes/" + G40


# ── backup_cloud_theme dedup guard ───────────────────────────


def _seed_existing_canonical(conn, *, tmdb_id, sha256, source_kind,
                             section_id="1"):
    conn.execute(
        "INSERT OR IGNORE INTO plex_sections "
        "  (section_id, title, type, is_anime, is_4k, themes_subdir, "
        "   included, discovered_at, last_seen_at) "
        "VALUES (?, 'Movies', 'movie', 0, 0, 'movies', 1, "
        "        '2026-05-29', '2026-05-29')", (section_id,))
    conn.execute(
        "INSERT INTO local_files (media_type, tmdb_id, section_id, "
        "  file_path, file_sha256, file_size, downloaded_at, "
        "  source_video_id, provenance, source_kind, "
        "  last_place_attempt_reason) "
        "VALUES ('movie', ?, ?, 'movies/x/theme.mp3', ?, 10, "
        "        '2026-05-29', 'vid', 'auto', ?, 'backup_only')",
        (tmdb_id, section_id, sha256, source_kind))


def _mock_fetch(plex, body_bytes):
    plex._rk_path.return_value = "/library/metadata/rk/file"
    plex._headers = {}
    resp = MagicMock()
    resp.status_code = 200
    resp.content = body_bytes
    resp.text = ""
    resp.headers = {}
    plex._client.get.return_value = resp


def test_backup_dedup_skips_identical_bytes(tmp_path):
    """Force capture whose bytes match the existing canonical → skip
    the swap (no relabel of source_kind)."""
    from app.core.db import init_db
    from app.core.cloud_theme_backup import backup_cloud_theme
    db = tmp_path / "t.db"
    themes_dir = tmp_path / "themes"
    themes_dir.mkdir()
    init_db(db)
    served = b"\x49\x44\x33identical-bytes"
    sha = hashlib.sha256(served).hexdigest()
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        _seed_existing_canonical(
            conn, tmdb_id=910, sha256=sha, source_kind="themerrdb")
        conn.commit()
        plex = MagicMock()
        _mock_fetch(plex, served)
        target = {
            "rating_key": "rk", "guid_tmdb": 910, "media_type": "movie",
            "section_id": "1", "title": "Dedup", "year": "2020",
            "entry_uri": "upload://themes/" + G40, "sha1": G40,
        }
        result = backup_cloud_theme(conn, target, themes_dir, plex)
        assert result["ok"] is True
        assert result.get("skipped_identical") is True, (
            "byte-identical bytes must skip the swap"
        )
        # source_kind must NOT be relabeled to plex_cloud.
        row = conn.execute(
            "SELECT source_kind FROM local_files WHERE tmdb_id=910"
        ).fetchone()
        assert row["source_kind"] == "themerrdb", (
            "v1.20.18: identical-bytes capture must not relabel the "
            "existing canonical"
        )


def test_backup_swaps_when_bytes_differ(tmp_path):
    """Force capture whose bytes differ → REPLACE to plex_cloud."""
    from app.core.db import init_db
    from app.core.cloud_theme_backup import backup_cloud_theme
    db = tmp_path / "t.db"
    themes_dir = tmp_path / "themes"
    themes_dir.mkdir()
    init_db(db)
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        _seed_existing_canonical(
            conn, tmdb_id=911, sha256="deadbeef" * 8,
            source_kind="themerrdb")
        # FK: local_files references themes — seed the themes row so
        # the REPLACE path's ON CONFLICT lands cleanly.
        conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, "
            "  upstream_source, last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('movie', 911, 'Swap', 'imdb', "
            "        '2026-05-29', '2026-05-29')")
        conn.commit()
        plex = MagicMock()
        _mock_fetch(plex, b"\x49\x44\x33totally-different-bytes")
        target = {
            "rating_key": "rk", "guid_tmdb": 911, "media_type": "movie",
            "section_id": "1", "title": "Swap", "year": "2020",
            "entry_uri": "upload://themes/" + G40, "sha1": G40,
        }
        result = backup_cloud_theme(conn, target, themes_dir, plex)
        assert result["ok"] is True
        assert not result.get("skipped_identical")
        row = conn.execute(
            "SELECT source_kind, last_place_attempt_reason "
            "FROM local_files WHERE tmdb_id=911"
        ).fetchone()
        assert row["source_kind"] == "plex_cloud", (
            "v1.20.18: a genuine swap must relabel to plex_cloud"
        )
        assert row["last_place_attempt_reason"] == "backup_only"


# ── endpoint + worker source pins ────────────────────────────


def test_endpoint_parses_force_and_threads_it():
    assert 'force = bool((body or {}).get("force", False))' in API_PY
    # passed into the worker thread kwargs.
    assert '"force": force,' in API_PY


def test_worker_threads_force_and_tallies_skipped():
    assert "force=force," in API_PY  # into identify_c1_rows
    assert "skipped_identical: list[dict] = []" in API_PY
    assert 'if result.get("skipped_identical"):' in API_PY
    assert "unchanged (identical)" in API_PY  # summary line


def test_identify_signature_and_force_implies_allow_existing():
    assert "force: bool = False," in CTB_PY
    # force implies allow_existing_local in the walker.
    idx = CTB_PY.index("if force:\n        allow_existing_local = True")
    assert idx > 0, "force must imply allow_existing_local"


# ── app.js confirm-retry + tooltip ───────────────────────────


def test_js_force_capture_helper_and_wiring():
    # v1.21.35: signature gained a hasTdb arg (TDB-revert copy branch).
    assert "function cloudBackupForceCapture(" in APP_JS
    # v0.51.51: invoked straight from the per-row handler (was the strict-run
    # 0-result fallback).
    assert "cloudBackupForceCapture(rk, hasTdb, allowExistingLocal)" in APP_JS
    # the POST forces + allows existing local.
    assert "force: true, allow_existing_local: true" in APP_JS


def test_js_pb_tooltip_generalized():
    """PB tooltip must no longer claim 'cloud theme' (false for a
    force-captured upload) — generalized to 'theme Plex was serving'."""
    assert "the theme Plex was serving for this row" in APP_JS
    assert "the theme Plex was serving for this row" in LIB_HTML


def test_v1_20_18_version_pin():
    # Loose pin (the canonical exact pin lives in test_v1_13_79) so
    # this sibling test survives every subsequent version bump.
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
