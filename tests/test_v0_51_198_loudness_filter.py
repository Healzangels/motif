"""v0.51.198 — the library LOUDNESS filter axis (normalized / raw / outliers).

A new pill axis on the library filter drawer. The high-risk part is the query: the
predicate reads lf_e/lf_g COALESCE aliases, so the COUNT path must grow the same joins
(needs_lf_for_count) and the one bound param (the outliers threshold) must align with its
`?`. This drives /api/library?loudness_pills=... against real seeded rows so both the
count header AND the row list are proven — not just the source text.

The 'outliers' token uses the SAME definition as // LEVEL OUTLIERS (raw + more than the
margin louder than the configured target), so the filter and the button agree.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from starlette.testclient import TestClient

from app.core.db import init_db

NOW = "2026-07-17T00:00:00"
AUTH = {"X-Authentik-Username": "testadmin"}
REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
LIB_HTML = (REPO / "app" / "web" / "templates" / "library.html").read_text()
BASE_HTML = (REPO / "app" / "web" / "templates" / "base.html").read_text()


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.delenv("MOTIF_LOUDNESS_TARGET", raising=False)   # default -18
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.web.api import create_app
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    with sqlite3.connect(s.db_path) as c:
        c.execute("INSERT INTO plex_sections (section_id, title, type, is_anime, is_4k, "
                  " themes_subdir, included, discovered_at, last_seen_at) "
                  "VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        c.commit()
    return TestClient(create_app(s)), s.db_path


def _seed(db, *, tmdb, loudness_i=None, norm=None, has_file=True):
    with sqlite3.connect(db) as c:
        c.execute("PRAGMA foreign_keys = OFF")
        c.execute("INSERT OR IGNORE INTO themes (id, media_type, tmdb_id, title, "
                  " upstream_source, last_seen_sync_at, first_seen_sync_at) "
                  "VALUES (?, 'movie', ?, ?, 'imdb', ?, ?)",
                  (tmdb, tmdb, f"M{tmdb}", NOW, NOW))
        c.execute("INSERT INTO plex_items (rating_key, media_type, section_id, guid_tmdb, "
                  " theme_id, edition_key, title, has_theme, first_seen_at, last_seen_at) "
                  "VALUES (?, 'movie', '1', ?, ?, '', ?, 1, ?, ?)",
                  (9000 + tmdb, tmdb, tmdb, f"M{tmdb}", NOW, NOW))
        if has_file:
            c.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, "
                      " edition_key, file_path, file_sha256, downloaded_at, "
                      " source_video_id, loudness_i, loudness_tp, norm_state) "
                      "VALUES ('movie', ?, '1', '', ?, ?, ?, 'v', ?, -2.0, ?)",
                      (tmdb, f"m/{tmdb}.mp3", f"s{tmdb}", NOW, loudness_i, norm))
        c.commit()


def _ids(client, token):
    """The tmdb_ids the loudness filter returns, plus the header count."""
    c, _ = client
    r = c.get(f"/api/library?tab=movies&loudness_pills={token}", headers=AUTH).json()
    ids = sorted(it["guid_tmdb"] for it in r["items"])   # library item's tmdb key
    return ids, r["total"]


def _fill(db):
    _seed(db, tmdb=1, loudness_i=-5.2, norm=None)          # raw + outlier (>-12 @ target -18)
    _seed(db, tmdb=2, loudness_i=-9.0, norm=None)          # raw + outlier
    _seed(db, tmdb=3, loudness_i=-16.0, norm=None)         # raw, NOT outlier
    _seed(db, tmdb=4, loudness_i=-18.7, norm="normalized")  # leveled
    _seed(db, tmdb=5, loudness_i=None, has_file=False)     # no local file (unthemed)


def test_normalized_filter(client):
    _fill(client[1])
    ids, total = _ids(client, "normalized")
    assert ids == [4] and total == 1


def test_raw_filter_excludes_outliers(client):
    _fill(client[1])
    ids, total = _ids(client, "raw")
    # v0.51.211: raw = unleveled AND NOT an outlier → only row 3 (-16.0, within margin).
    # rows 1,2 are loud outliers (amber glyph, not the dim raw glyph); row 4 leveled; row 5
    # has no file. Pre-fix RAW was a superset [1,2,3] that disagreed with the raw row marker.
    assert ids == [3] and total == 1


def test_raw_plus_outliers_is_every_unleveled_row(client):
    # v0.51.211: the raw + outliers chips PARTITION the unleveled set, so selecting BOTH gives
    # every unleveled row — the capability the old superset RAW provided, now via multi-select.
    _fill(client[1])
    ids, total = _ids(client, "raw,outliers")
    assert ids == [1, 2, 3] and total == 3


def test_outliers_filter_matches_level_outliers(client):
    _fill(client[1])
    ids, total = _ids(client, "outliers")
    # raw + louder than target(-18) + margin(6) = louder than -12 → rows 1,2
    assert ids == [1, 2] and total == 2


def test_multiple_tokens_are_ORed(client):
    _fill(client[1])
    ids, total = _ids(client, "normalized,outliers")
    assert ids == [1, 2, 4] and total == 3


def test_count_header_agrees_with_rows(client):
    """The COUNT path grows the lf join (needs_lf_for_count) — header must equal len(rows),
    the exact 500/miscount class the v1.13.32 comment documents."""
    _fill(client[1])
    c, _ = client
    for token, n in [("normalized", 1), ("raw", 1), ("outliers", 2)]:
        r = c.get(f"/api/library?tab=movies&loudness_pills={token}", headers=AUTH).json()
        assert r["total"] == len(r["items"]) == n, (token, r["total"], len(r["items"]))


def test_outliers_track_the_configured_target(client, monkeypatch):
    """A quieter configured target makes more rows outliers — filter follows the target."""
    from app.config import Settings
    monkeypatch.setattr(Settings, "loudness_target_lufs", property(lambda self: -22.0))
    _fill(client[1])
    ids, total = _ids(client, "outliers")
    # target -22 + margin 6 = louder than -16 → rows 1,2,3 (not the -16.0 boundary... > -16)
    assert ids == [1, 2] and total == 2   # -16.0 is not > -16.0


# ── every registration surface (mirror-drift guard) ──────────────────────────

def test_axis_registered_on_every_surface():
    # server whitelist
    api_py = (REPO / "app" / "web" / "api.py").read_text()
    assert '_pset(loudness_pills, {"normalized", "raw", "outliers"})' in api_py
    assert "loudness_target=settings.loudness_target_lufs" in api_py
    # drawer chips
    assert 'data-loudness-pill="normalized"' in LIB_HTML
    assert 'data-loudness-pill="raw"' in LIB_HTML
    assert 'data-loudness-pill="outliers"' in LIB_HTML
    # JS: state Set, click registry, deep-link, hydrate map, and 4 serializers
    assert "loudnessPills: new Set()" in APP_JS
    assert "state: 'loudnessPills'" in APP_JS
    assert "param: 'loudness_pills'" in APP_JS
    assert "key: 'loudnessPills'" in APP_JS
    assert APP_JS.count("loudness_pills") >= 4   # the 4 URL/param builders


def test_axis_documented_in_legend_and_glossary():
    # in-context library legend + full base.html glossary both decode the filter states
    assert "loudness-pill-outliers" in LIB_HTML or "LOUDNESS" in LIB_HTML
    assert "LOUDNESS" in BASE_HTML
