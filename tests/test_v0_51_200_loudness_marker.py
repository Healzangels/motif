"""v0.51.200 — Phase 2 Tag 5: the 3-state title-cell loudness marker.

Extends the v0.51.192 leveled-only marker to three states: leveled / outlier / raw.
The state is derived SERVER-SIDE in _library_main_query (it.loudness_marker) so the
marker and the v0.51.198 LOUDNESS filter chip share ONE 3-state rule — both key off
the SAME _OUTLIER_MARGIN_DB threshold, so they can't drift.

Drives /api/library against real seeded rows (the marker must survive the SELECT →
Python derivation → JSON round-trip) and pins that the marker classification agrees
with the filter for the same rows. Plus the UI-wiring surfaces (glyph map, CSS,
legend on both templates).
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
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
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


def _markers(client, *, target_query=""):
    """{tmdb_id: loudness_marker} for the whole movies tab."""
    c, _ = client
    r = c.get(f"/api/library?tab=movies&per_page=100{target_query}", headers=AUTH).json()
    return {it["guid_tmdb"]: it.get("loudness_marker") for it in r["items"]}


def _fill(db):
    _seed(db, tmdb=1, loudness_i=-5.2, norm=None)           # raw + outlier (> -12 @ target -18)
    _seed(db, tmdb=2, loudness_i=-9.0, norm=None)           # raw + outlier
    _seed(db, tmdb=3, loudness_i=-16.0, norm=None)          # raw, NOT outlier
    _seed(db, tmdb=4, loudness_i=-18.7, norm="normalized")   # leveled
    _seed(db, tmdb=5, loudness_i=None, has_file=False)      # no local file → no marker
    _seed(db, tmdb=6, loudness_i=None, has_file=True)       # local file, un-audited → raw


def test_marker_three_states(client):
    _fill(client[1])
    m = _markers(client)
    assert m[1] == "outlier"
    assert m[2] == "outlier"
    assert m[3] == "raw"
    assert m[4] == "leveled"
    assert m[5] is None          # no local file → nothing to mark
    assert m[6] == "raw"         # has a file but never audited → raw, not outlier


def test_loudness_i_is_not_leaked_into_library_rows(client):
    """v0.51.202 (review #2): loudness_i is SELECTed only to derive loudness_marker
    server-side; nothing client-side reads it, so it must be popped from each row."""
    _fill(client[1])
    c, _ = client
    rows = c.get("/api/library?tab=movies&per_page=100", headers=AUTH).json()["items"]
    assert rows, "seeded rows must be present"
    assert all("loudness_marker" in it for it in rows)
    assert all("loudness_i" not in it for it in rows), "loudness_i must not ride in the JSON"


def test_marker_agrees_with_outliers_filter(client):
    """Drift guard: the rows the marker calls 'outlier' are EXACTLY the rows the
    ?loudness_pills=outliers filter returns — one _OUTLIER_MARGIN_DB threshold."""
    _fill(client[1])
    c, _ = client
    marked_outliers = {t for t, mk in _markers(client).items() if mk == "outlier"}
    r = c.get("/api/library?tab=movies&loudness_pills=outliers", headers=AUTH).json()
    filtered_outliers = {it["guid_tmdb"] for it in r["items"]}
    assert marked_outliers == filtered_outliers == {1, 2}


def test_marker_agrees_with_raw_and_leveled_filters(client):
    _fill(client[1])
    c, _ = client
    m = _markers(client)
    # v0.51.211: the raw FILTER now equals the raw MARKER exactly (excludes outliers), so the
    # filter and the dim raw glyph agree. Pre-fix the raw filter was a SUPERSET that also
    # matched amber-outlier rows — the inconsistency the user caught.
    raw_rows = {it["guid_tmdb"] for it in
                c.get("/api/library?tab=movies&loudness_pills=raw", headers=AUTH).json()["items"]}
    assert raw_rows == {t for t, mk in m.items() if mk == "raw"} == {3, 6}
    # raw + outliers together == every unleveled marker (the old superset, now via multi-select).
    both = {it["guid_tmdb"] for it in
            c.get("/api/library?tab=movies&loudness_pills=raw,outliers", headers=AUTH).json()["items"]}
    assert both == {t for t, mk in m.items() if mk in ("raw", "outlier")} == {1, 2, 3, 6}
    lev_rows = {it["guid_tmdb"] for it in
                c.get("/api/library?tab=movies&loudness_pills=normalized", headers=AUTH).json()["items"]}
    assert lev_rows == {t for t, mk in m.items() if mk == "leveled"} == {4}


def test_marker_tracks_the_configured_target(client, monkeypatch):
    """A quieter target makes more raw rows outliers — the marker follows the target,
    same as the filter (both bind target + _OUTLIER_MARGIN_DB)."""
    from app.config import Settings
    monkeypatch.setattr(Settings, "loudness_target_lufs", property(lambda self: -22.0))
    _fill(client[1])
    m = _markers(client)
    # target -22 + margin 6 = louder than -16 → rows 1,2 outliers (row 3 == -16.0 is not > -16)
    assert m[1] == "outlier" and m[2] == "outlier"
    assert m[3] == "raw"     # -16.0 is exactly the boundary, not > -16.0


# ── UI wiring (mirror-drift + reuse-real-classes guards) ─────────────────────


def test_marker_glyph_map_has_all_three_states():
    assert "LOUD_MARK = {" in APP_JS
    for cls in ("tier-badge-lvl", "tier-badge-loud", "tier-badge-raw"):
        assert cls in APP_JS, cls
    assert "it.loudness_marker" in APP_JS


def test_marker_css_defines_the_two_new_states():
    assert ".tier-badge-loud {" in APP_CSS
    assert ".tier-badge-raw {" in APP_CSS


def test_new_marker_rules_reference_only_defined_tokens():
    """Every var(--X) in the two new rules must resolve to a `--X:` definition. Caught a
    real bug at build time: .tier-badge-raw first used var(--muted), which is NOT a defined
    token — it would have fallen back to currentColor (the bright row text), the exact
    OPPOSITE of the muted baseline the raw state needs. A var() typo renders silently."""
    import re
    defined = set(re.findall(r"(--[a-z0-9-]+)\s*:", APP_CSS))
    for sel in (".tier-badge-loud {", ".tier-badge-raw {"):
        i = APP_CSS.index(sel)
        rule = APP_CSS[i:APP_CSS.index("}", i)]
        used = set(re.findall(r"var\((--[a-z0-9-]+)\)", rule))
        missing = used - defined
        assert not missing, f"{sel} references undefined token(s): {missing}"


def test_legend_decodes_all_three_on_both_templates():
    # every marker state must be decodable in the on-page legend AND the glossary,
    # or it's the drift the v0.51.192/193 leveled-legend gating already bit us on.
    for html in (LIB_HTML, BASE_HTML):
        assert "tier-badge-loud" in html
        assert "tier-badge-raw" in html


def test_select_carries_loudness_i():
    """The server derivation needs loudness_i per row — it must be in the SELECT."""
    api_py = (REPO / "app" / "web" / "api.py").read_text()
    assert "COALESCE(lf_e.loudness_i, lf_g.loudness_i) AS loudness_i" in api_py


def test_filter_and_marker_share_one_margin_constant():
    """Every outlier-threshold consumer imports _OUTLIER_MARGIN_DB (was a literal 6.0) so
    the margin is single-sourced: the filter predicate, the library-row marker, the INFO
    card chip (v0.51.207), the // RE-MEASURE probe (v0.51.208)... `>=` not `==` so a new
    consumer that correctly imports the constant doesn't force a count bump every tag; the
    no-literal assert below is the real guard against a hardcoded margin sneaking back."""
    api_py = (REPO / "app" / "web" / "api.py").read_text()
    assert api_py.count("_OUTLIER_MARGIN_DB as _loud_margin") >= 3
    # the old literal is gone from the filter param bind.
    assert "params.append(loudness_target + 6.0)" not in api_py
