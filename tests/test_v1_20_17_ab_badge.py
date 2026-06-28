"""v1.20.17 — AB (Adopted Backup) LINK chip split out of UB.

the user: "similar to how we split the TB out from UB maybe we should
also have an AB for adopted backup which similar is a shade of blue
similar to the adopted chip and this would be for when the backed up
file is an adopted source."

Backup rows (backup_only stamp, no placement) now partition by
source_kind into FOUR chips:
  PB (.link-glyph-b,  amber-bright)  source_kind='plex_cloud'
  TB (.link-glyph-tb, green-pale)    source_kind='themerrdb'
  AB (.link-glyph-ab, cyan-pale)     source_kind='adopt'    ← NEW
  UB (.link-glyph-bk, violet-bright) source_kind ∈ url/upload/NULL

The AB cyan is a new --cyan-pale token (#a6e4ff) — a lighter tint of
the SRC=A cyan, paralleling PB/TB/UB being lighter tints of P/T/U,
but distinct from the SRC=A pill it sits beside. Pre-v1.20.17 adopted
backups were silently bucketed into UB (violet).
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()
LIB_HTML = (REPO / "app" / "web" / "templates" / "library.html").read_text()


# ── tokens + chip CSS ────────────────────────────────────────


def test_cyan_pale_token_defined():
    assert "--cyan-pale: #a6e4ff;" in APP_CSS
    assert "--cyan-pale-rgb: 166, 228, 255;" in APP_CSS


def test_link_glyph_ab_uses_cyan_pale():
    idx = APP_CSS.index(".link-glyph-ab {")
    rule = APP_CSS[idx:idx + 260]
    assert "color: var(--cyan-pale)" in rule
    assert "rgba(var(--cyan-pale-rgb), 0.5)" in rule


def test_promote_ab_button_tone_cyan():
    assert ".btn-promote-ab { color: var(--cyan-pale);" in APP_CSS


# ── render + filter wiring ───────────────────────────────────


def test_render_has_adopt_backup_branch_before_ub():
    assert "const isAdoptBackup = isBackupOnly" in APP_JS
    assert "it.source_kind === 'adopt'" in APP_JS
    assert "link-glyph-ab" in APP_JS and ">AB</span>" in APP_JS
    # AB branch must sit before the generic backup_only (UB) branch.
    ab_idx = APP_JS.index("link-glyph-ab")
    ub_idx = APP_JS.index("link-glyph-bk")
    assert ab_idx < ub_idx, "AB branch must precede the UB branch"
    # ...and AFTER the TB branch (PB > TB > AB > UB order).
    tb_idx = APP_JS.index("link-glyph-tb")
    assert tb_idx < ab_idx, "AB branch must follow the TB branch"


def test_ab_filter_chip_and_allowlists():
    assert 'data-link-pill="ab"' in LIB_HTML
    assert "link-glyph-ab" in LIB_HTML
    # both JS allowlists (deep-link parser + // ALL chip list) carry 'ab'
    assert "'b','bk','tb','ab'" in APP_JS
    assert "'b', 'bk', 'tb', 'ab'" in APP_JS
    # api.py _pset whitelist
    assert '"bk", "tb", "ab"' in API_PY


def test_promote_tone_branches_on_adopt():
    assert "promoteSourceKind === 'adopt'" in APP_JS
    assert "'btn-promote-ab'" in APP_JS


def test_playback_label_has_ab_branch():
    assert "(AB badge · backup-only" in APP_JS


# ── SQL: ab matches adopt; bk excludes it ────────────────────


def test_sql_ab_branch_and_whitelist():
    assert 'elif p == "ab":' in API_PY
    ab_idx = API_PY.index('elif p == "ab":')
    ab_block = API_PY[ab_idx:ab_idx + 900]
    # v1.21.59: lf.* reads are now COALESCE(lf_e.x, lf_g.x) (per-edition).
    assert "COALESCE(lf_e.source_kind, lf_g.source_kind) = 'adopt'" in ab_block
    assert "backup_only" in ab_block
    # BK now excludes adopt (and still themerrdb + plex_cloud).
    bk_idx = API_PY.index('elif p == "bk":')
    bk_block = API_PY[bk_idx:bk_idx + 1700]
    assert "NOT IN" in bk_block
    assert "'plex_cloud', 'themerrdb', 'adopt'" in bk_block


# ── behavioral: an adopt-backup row filters as AB, not UB ─────


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
    return TestClient(create_app(settings))


AUTH = {"X-Authentik-Username": "testadmin"}


def _db(tmp_path):
    from app.config import Settings
    return Settings(config_dir=tmp_path, data_dir=tmp_path / "data").db_path


def test_adopt_backup_row_filters_as_ab_not_ub(admin_client, tmp_path):
    from app.core.db import init_db
    db = _db(tmp_path)
    init_db(db)
    now = "2026-05-29T00:00:00"
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, "
            "  is_anime, is_4k, themes_subdir, included, discovered_at, "
            "  last_seen_at) VALUES ('1','Movies','movie',0,0,'movies',1,"
            "  ?, ?)", (now, now))
        conn.execute(
            "INSERT INTO themes (id, media_type, tmdb_id, title, "
            "  upstream_source, last_seen_sync_at, first_seen_sync_at, "
            "  youtube_url) VALUES (1,'movie',201,'Adopt Backup Row','imdb',"
            "  ?, ?, 'https://www.youtube.com/watch?v=Y')", (now, now))
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type, "
            "  theme_id, guid_imdb, guid_tmdb, title, year, has_theme, "
            "  local_theme_file, folder_path, plex_independent_theme, "
            "  plex_theme_verified_ok, first_seen_at, last_seen_at) "
            "VALUES ('r2','1','movie',1,NULL,201,'Adopt Backup Row',2020,1,"
            "  0,'/data/movies/y',1,1,?,?)", (now, now))
        # adopt-sourced backup_only canonical, no placement.
        conn.execute(
            "INSERT INTO local_files (media_type, tmdb_id, section_id, "
            "  theme_id, file_path, downloaded_at, source_video_id, "
            "  provenance, source_kind, last_place_attempt_reason) "
            "VALUES ('movie',201,'1',1,'movies/y/theme.mp3',?,'Y','manual',"
            "  'adopt','backup_only')", (now,))
        conn.commit()

    def titles(link):
        r = admin_client.get(
            f"/api/library?tab=movies&link_pills={link}&per_page=50",
            headers=AUTH)
        assert r.status_code == 200, r.text
        return [(it.get("plex_title") or it.get("theme_title"))
                for it in r.json()["items"]]

    # AB matches the row; UB (bk) does NOT (it's now adopt-excluded).
    assert "Adopt Backup Row" in titles("ab"), "adopt backup must match AB"
    assert "Adopt Backup Row" not in titles("bk"), (
        "v1.20.17: an adopt backup must NOT match UB anymore"
    )


def test_v1_20_17_version_pin():
    # Loose pin (the canonical exact pin lives in test_v1_13_79) so
    # this sibling test survives every subsequent version bump.
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
