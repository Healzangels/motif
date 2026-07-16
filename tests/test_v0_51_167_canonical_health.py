"""v0.51.167 — CANONICAL HEALTH: find + repair themes missing from motif's storage.

The loudness audit (v0.51.163) surfaced ~14 library items whose canonical
theme.mp3 was gone from disk — ffmpeg measure returned rc=254 "No such file or
directory". A vanished / 0-byte canonical is a storage-health problem separate
from loudness. This tag:

  1. Extends verify_canonical_health (+ the per-render _annotate_canonical_state)
     to treat a 0-byte theme.mp3 as missing — a failed download that left a stub
     is functionally gone, and the downloader itself removes + re-downloads one
     (downloader.py:589). Both stat paths must agree (CLAUDE.md).

  2. Adds app/core/canonical_health.py: broken_canonical_report classifies each
     canonical_present=0 row as RE-DOWNLOADABLE (recorded ThemerrDB / user URL →
     one-click re-fetch) or CANONICAL MISSING (uploaded / adopted / cloud-backed →
     manual re-place). enqueue_canonical_repairs re-downloads the former.

  3. Surfaces both at /admin/canonical-health (a Diagnostics dashboard mirroring
     LOUDNESS AUDIT) with RUN CHECK + REPAIR ALL.

Everything is edition-scoped (local_files PK carries edition_key).
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core import plex_enum
from app.core.canonical_health import (
    broken_canonical_report,
    classify_repair,
    enqueue_canonical_repairs,
)
from app.core.db import get_conn, init_db


REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
SETTINGS_HTML = (REPO / "app" / "web" / "templates" / "settings.html").read_text()
NOW = "2026-07-16T00:00:00"
AUTH = {"X-Authentik-Username": "testadmin"}


# ── seed helpers ─────────────────────────────────────────────

def _section(conn, section_id="1"):
    conn.execute(
        "INSERT INTO plex_sections (section_id, title, type, is_anime, is_4k,"
        " themes_subdir, included, discovered_at, last_seen_at)"
        " VALUES (?, 'M', 'movie', 0, 0, 'movies', 1, ?, ?)"
        " ON CONFLICT(section_id) DO NOTHING", (section_id, NOW, NOW))


def _theme(conn, *, tid, tmdb, youtube_url="https://y/w", upstream="imdb"):
    conn.execute(
        "INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source,"
        " last_seen_sync_at, first_seen_sync_at, youtube_url)"
        " VALUES (?, 'movie', ?, ?, ?, ?, ?, ?)",
        (tid, tmdb, f"T{tmdb}", upstream, NOW, NOW, youtube_url))


def _lf(conn, *, tid, tmdb, source_kind="themerrdb", canonical_present=0,
        edition_key="", section_id="1", file_path=None):
    conn.execute(
        "INSERT INTO local_files (media_type, tmdb_id, section_id, theme_id,"
        " file_path, downloaded_at, source_video_id, provenance, source_kind,"
        " canonical_present, edition_key)"
        " VALUES ('movie', ?, ?, ?, ?, ?, 'V', 'auto', ?, ?, ?)",
        (tmdb, section_id, tid, file_path or f"movies/{tmdb}/theme.mp3",
         NOW, source_kind, canonical_present, edition_key))


def _override(conn, *, tmdb, url="https://y/override", section_id="", edition_key=""):
    conn.execute(
        "INSERT INTO user_overrides (media_type, tmdb_id, youtube_url, set_at,"
        " section_id, edition_key) VALUES ('movie', ?, ?, ?, ?, ?)",
        (tmdb, url, NOW, section_id, edition_key))


def _plex_item(conn, *, rk, tid, tmdb, section_id="1"):
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, media_type, theme_id,"
        " guid_imdb, guid_tmdb, title, year, has_theme, plex_independent_theme,"
        " first_seen_at, last_seen_at)"
        " VALUES (?, ?, 'movie', ?, ?, ?, ?, 2012, 1, 0, ?, ?)",
        (rk, section_id, tid, f"tt{tmdb}", tmdb, f"T{tmdb}", NOW, NOW))


# ── 1. zero-byte detection (verify_canonical_health) ─────────

def test_verify_flags_zero_byte_as_missing(tmp_path):
    """A 0-byte theme.mp3 exists on disk (is_file()==True) but is a corrupt /
    failed download — the loudness audit's rc=254 cohort. v0.51.167 stamps it
    canonical_present=0, not 1."""
    db = tmp_path / "m.db"
    init_db(db)
    themes = tmp_path / "themes"
    with sqlite3.connect(db) as conn:
        _section(conn)
        _theme(conn, tid=1, tmdb=101)
        _theme(conn, tid=2, tmdb=102)
        _theme(conn, tid=3, tmdb=103)
        _lf(conn, tid=1, tmdb=101, canonical_present=None)  # present, non-empty
        _lf(conn, tid=2, tmdb=102, canonical_present=None)  # present, 0-byte
        _lf(conn, tid=3, tmdb=103, canonical_present=None)  # absent
        conn.commit()
    (themes / "movies/101").mkdir(parents=True)
    (themes / "movies/101/theme.mp3").write_bytes(b"real-audio-bytes")
    (themes / "movies/102").mkdir(parents=True)
    (themes / "movies/102/theme.mp3").write_bytes(b"")       # 0-byte stub

    res = plex_enum.verify_canonical_health(db, themes)
    assert res["missing"] == 2, res   # 102 (0-byte) + 103 (absent)
    flags = dict(sqlite3.connect(db).execute(
        "SELECT tmdb_id, canonical_present FROM local_files").fetchall())
    assert flags[101] == 1   # non-empty file → present
    assert flags[102] == 0   # 0-byte → verified missing (the new behavior)
    assert flags[103] == 0   # absent → verified missing


# ── 2. per-render canonical_missing agrees (library API) ─────

@pytest.fixture
def admin_client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.web.api import create_app
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    settings._cfg.paths.themes_dir = str(tmp_path / "themes")
    init_db(settings.db_path)
    init_auth_schema(settings.db_path)
    create_admin(settings.db_path, username="testadmin", password="testpassword")
    return TestClient(create_app(settings)), settings.db_path, Path(tmp_path / "themes")


def test_zero_byte_renders_dl_broken(admin_client):
    """The live red DL dot + dl_pills=broken filter derive from the per-render
    _annotate_canonical_state stat, NOT the stored flag. It must also treat a
    0-byte canonical as missing, else the dot disagrees with verify_canonical_health."""
    client, db, themes = admin_client
    with sqlite3.connect(db) as conn:
        _section(conn)
        _theme(conn, tid=1, tmdb=101)
        _plex_item(conn, rk="rk-1", tid=1, tmdb=101)
        _lf(conn, tid=1, tmdb=101, canonical_present=1)  # stored says present…
        # dl_pills=broken narrows to rows that ALSO have a placement (api.py:2384) —
        # a placement-less broken canonical is invisible there, which is exactly why
        # the /admin/canonical-health report exists. Give this row one so we can
        # prove the 0-byte fix flows through the existing broken-row filter too.
        conn.execute(
            "INSERT INTO placements (media_type, tmdb_id, section_id, theme_id,"
            " media_folder, placed_at, placement_kind, edition_key)"
            " VALUES ('movie', 101, '1', 1, '/movies/T101', ?, 'hardlink', '')", (NOW,))
        conn.commit()
    (themes / "movies/101").mkdir(parents=True)
    (themes / "movies/101/theme.mp3").write_bytes(b"")   # …but it's 0-byte on disk

    r = client.get("/api/library?tab=movies", headers=AUTH)
    assert r.status_code == 200, r.text
    row = next(it for it in r.json()["items"] if it["rating_key"] == "rk-1")
    assert row["canonical_missing"] is True, "0-byte canonical must render broken"
    # and the broken filter isolates it (placement-backed, so it clears the narrowing).
    r2 = client.get("/api/library?tab=movies&dl_pills=broken", headers=AUTH)
    assert "rk-1" in [it["rating_key"] for it in r2.json()["items"]]


# ── 3. repair classification ─────────────────────────────────

def test_classify_redownloadable_and_missing(tmp_path):
    db = tmp_path / "m.db"
    init_db(db)
    with get_conn(db) as conn:
        _section(conn)
        # themerrdb + TDB url → redownload
        _theme(conn, tid=1, tmdb=101, youtube_url="https://y/a")
        _lf(conn, tid=1, tmdb=101, source_kind="themerrdb")
        # themerrdb + NO tdb url → surface
        _theme(conn, tid=2, tmdb=102, youtube_url=None)
        _lf(conn, tid=2, tmdb=102, source_kind="themerrdb")
        # url source + override present → redownload
        _theme(conn, tid=3, tmdb=103, youtube_url=None)
        _lf(conn, tid=3, tmdb=103, source_kind="url")
        _override(conn, tmdb=103)
        # upload / adopt / plex_cloud → surface EVEN with a TDB url on the theme
        _theme(conn, tid=4, tmdb=104, youtube_url="https://y/tdb")
        _lf(conn, tid=4, tmdb=104, source_kind="upload")
        _theme(conn, tid=5, tmdb=105, youtube_url="https://y/tdb")
        _lf(conn, tid=5, tmdb=105, source_kind="adopt")
        _theme(conn, tid=6, tmdb=106, youtube_url="https://y/tdb")
        _lf(conn, tid=6, tmdb=106, source_kind="plex_cloud")
        # plex_orphan (no real TDB backing) even with a url → surface
        _theme(conn, tid=7, tmdb=107, youtube_url="https://y/x", upstream="plex_orphan")
        _lf(conn, tid=7, tmdb=107, source_kind="themerrdb")
        conn.commit()

        def cls(tmdb):
            r = conn.execute(
                "SELECT lf.media_type, lf.tmdb_id, lf.section_id, lf.edition_key,"
                " lf.source_kind, t.youtube_url AS tdb_url, t.upstream_source"
                " FROM local_files lf JOIN themes t"
                "   ON t.media_type=lf.media_type AND t.tmdb_id=lf.tmdb_id"
                " WHERE lf.tmdb_id=?", (tmdb,)).fetchone()
            return classify_repair(conn, r)

        assert cls(101) == "redownload"
        assert cls(102) == "canonical_missing"
        assert cls(103) == "redownload"
        assert cls(104) == "canonical_missing"
        assert cls(105) == "canonical_missing"
        assert cls(106) == "canonical_missing"
        assert cls(107) == "canonical_missing"


# ── 4. report shape + split ──────────────────────────────────

def test_report_splits_broken_rows(tmp_path):
    db = tmp_path / "m.db"
    init_db(db)
    with get_conn(db) as conn:
        _section(conn)
        # 2 broken redownloadable, 1 broken manual, 1 HEALTHY (present) — excluded.
        _theme(conn, tid=1, tmdb=101, youtube_url="https://y/a")
        _lf(conn, tid=1, tmdb=101, source_kind="themerrdb", canonical_present=0)
        _theme(conn, tid=2, tmdb=102, youtube_url=None)
        _lf(conn, tid=2, tmdb=102, source_kind="url", canonical_present=0)
        _override(conn, tmdb=102)
        _theme(conn, tid=3, tmdb=103, youtube_url=None)
        _lf(conn, tid=3, tmdb=103, source_kind="upload", canonical_present=0)
        _theme(conn, tid=4, tmdb=104, youtube_url="https://y/b")
        _lf(conn, tid=4, tmdb=104, source_kind="themerrdb", canonical_present=1)
        # NULL (never verified) must also be excluded.
        _theme(conn, tid=5, tmdb=105, youtube_url="https://y/c")
        _lf(conn, tid=5, tmdb=105, source_kind="themerrdb", canonical_present=None)
        conn.commit()
        rep = broken_canonical_report(conn)

    assert rep["counts"] == {"broken": 3, "redownloadable": 2, "canonical_missing": 1}
    assert {e["tmdb_id"] for e in rep["redownloadable"]} == {101, 102}
    assert [e["tmdb_id"] for e in rep["canonical_missing"]] == [103]
    # the manual entry carries the RESTORE-FROM-PLEX hint + INFO deep-link fields.
    m = rep["canonical_missing"][0]
    assert "has_live_placement" in m
    assert m["media_type"] == "movie" and m["section_id"] == "1"


def test_report_marks_live_placement_hint(tmp_path):
    db = tmp_path / "m.db"
    init_db(db)
    with get_conn(db) as conn:
        _section(conn)
        _theme(conn, tid=1, tmdb=101, youtube_url=None)
        _lf(conn, tid=1, tmdb=101, source_kind="adopt", canonical_present=0)
        # a surviving Plex-folder copy (verify_placement_health stamped present).
        conn.execute(
            "INSERT INTO placements (media_type, tmdb_id, section_id, theme_id,"
            " media_folder, placed_at, placement_kind, edition_key, theme_present)"
            " VALUES ('movie', 101, '1', 1, '/movies/T101', ?, 'hardlink', '', 1)",
            (NOW,))
        conn.commit()
        rep = broken_canonical_report(conn)
    assert rep["canonical_missing"][0]["has_live_placement"] is True


# ── 5. repair enqueues re-downloads, surfaces the rest ───────

def test_repair_enqueues_only_redownloadable(tmp_path):
    db = tmp_path / "m.db"
    init_db(db)
    with get_conn(db) as conn:
        _section(conn)
        # redownloadable broken row — needs a plex_items row so _enqueue_download
        # finds an included section to queue into.
        _theme(conn, tid=1, tmdb=101, youtube_url="https://y/a")
        _lf(conn, tid=1, tmdb=101, source_kind="themerrdb", canonical_present=0)
        _plex_item(conn, rk="rk-1", tid=1, tmdb=101)
        # manual (upload) broken row — must be surfaced, never enqueued.
        _theme(conn, tid=2, tmdb=102, youtube_url="https://y/b")
        _lf(conn, tid=2, tmdb=102, source_kind="upload", canonical_present=0)
        _plex_item(conn, rk="rk-2", tid=2, tmdb=102)
        conn.commit()
        summary = enqueue_canonical_repairs(conn)
        conn.commit()

    assert summary["repaired_rows"] == 1
    assert summary["enqueued_sections"] >= 1
    assert summary["surfaced"] == 1
    jobs = dict(sqlite3.connect(db).execute(
        "SELECT tmdb_id, COUNT(*) FROM jobs WHERE job_type='download'"
        " GROUP BY tmdb_id").fetchall())
    assert jobs.get(101, 0) >= 1, "redownloadable row enqueued"
    assert 102 not in jobs, "upload row must NOT be re-downloaded (would swap content)"


# ── 6. endpoints ─────────────────────────────────────────────

def test_page_and_report_endpoints(admin_client):
    client, db, themes = admin_client
    r = client.get("/admin/canonical-health", headers=AUTH)
    assert r.status_code == 200
    assert "// CANONICAL HEALTH" in r.text

    with sqlite3.connect(db) as conn:
        _section(conn)
        _theme(conn, tid=1, tmdb=101, youtube_url="https://y/a")
        _lf(conn, tid=1, tmdb=101, source_kind="themerrdb", canonical_present=0)
        conn.commit()

    rep = client.get("/api/admin/canonical-health/report", headers=AUTH).json()
    assert rep["counts"]["redownloadable"] == 1
    assert rep["redownloadable"][0]["tmdb_id"] == 101


def test_check_endpoint_restamps_zero_byte(admin_client):
    """POST /check re-stats fresh: a 0-byte canonical the stored flag calls
    present (=1) is re-stamped broken and appears in the report."""
    client, db, themes = admin_client
    with sqlite3.connect(db) as conn:
        _section(conn)
        _theme(conn, tid=1, tmdb=101, youtube_url="https://y/a")
        _lf(conn, tid=1, tmdb=101, source_kind="themerrdb", canonical_present=1)
        conn.commit()
    (themes / "movies/101").mkdir(parents=True)
    (themes / "movies/101/theme.mp3").write_bytes(b"")   # 0-byte on disk

    rep = client.post("/api/admin/canonical-health/check", headers=AUTH).json()
    assert rep["counts"]["broken"] == 1
    assert rep["redownloadable"][0]["tmdb_id"] == 101
    # the fresh stamp was persisted.
    flag = sqlite3.connect(db).execute(
        "SELECT canonical_present FROM local_files WHERE tmdb_id=101").fetchone()[0]
    assert flag == 0


def test_repair_endpoint_enqueues(admin_client):
    client, db, themes = admin_client
    with sqlite3.connect(db) as conn:
        _section(conn)
        _theme(conn, tid=1, tmdb=101, youtube_url="https://y/a")
        _lf(conn, tid=1, tmdb=101, source_kind="themerrdb", canonical_present=0)
        _plex_item(conn, rk="rk-1", tid=1, tmdb=101)
        conn.commit()
    res = client.post("/api/admin/canonical-health/repair", headers=AUTH).json()
    assert res["ok"] is True
    assert res["repaired_rows"] == 1
    n = sqlite3.connect(db).execute(
        "SELECT COUNT(*) FROM jobs WHERE job_type='download' AND tmdb_id=101"
    ).fetchone()[0]
    assert n >= 1


def test_endpoints_require_admin(admin_client):
    client, db, themes = admin_client
    # no auth header → 401 (forward-auth trust is on but no username supplied).
    assert client.get("/api/admin/canonical-health/report").status_code == 401
    assert client.post("/api/admin/canonical-health/repair").status_code == 401


# ── 7. UI wiring ─────────────────────────────────────────────

def test_bind_canonical_health_wired():
    assert "function bindCanonicalHealth()" in APP_JS
    assert "bindCanonicalHealth();" in APP_JS   # called in init
    assert "/api/admin/canonical-health/check" in APP_JS
    assert "/api/admin/canonical-health/repair" in APP_JS


def test_diagnostics_card_links_to_page():
    assert 'href="/admin/canonical-health"' in SETTINGS_HTML
    assert "// CANONICAL HEALTH" in SETTINGS_HTML
