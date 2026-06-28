"""v1.19.90 — TB (ThemerrDB Backup) LINK chip split out of UB.

the user: "rather than lump TDB backups into UB we make TB which is
themerrdb backed up rows using a lighter green chip."

Backup rows (backup_only stamp, no placement) now partition by
source_kind into three chips:
  PB (.link-glyph-b,  amber-bright)   source_kind='plex_cloud'
  TB (.link-glyph-tb, green-pale)     source_kind='themerrdb'   ← NEW
  UB (.link-glyph-bk, violet-bright)  source_kind ∈ url/upload/adopt/NULL

The TB green is a new --green-pale token (#c2ffe0) — a lighter tint
of the SRC=T green-bright, paralleling PB/UB being lighter tints of
P/U, but distinct from the SRC=T pill it sits beside.
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


def test_green_pale_token_defined():
    assert "--green-pale: #c2ffe0;" in APP_CSS
    assert "--green-pale-rgb: 194, 255, 224;" in APP_CSS


def test_link_glyph_tb_uses_green_pale():
    idx = APP_CSS.index(".link-glyph-tb {")
    rule = APP_CSS[idx:idx + 260]
    assert "color: var(--green-pale)" in rule
    assert "rgba(var(--green-pale-rgb), 0.5)" in rule


def test_promote_tb_button_tone_green():
    assert ".btn-promote-tb { color: var(--green-pale);" in APP_CSS


# ── render + filter wiring ───────────────────────────────────


def test_render_has_tdb_backup_branch_before_ub():
    assert "const isTdbBackup = isBackupOnly" in APP_JS
    assert "it.source_kind === 'themerrdb'" in APP_JS
    assert "link-glyph-tb" in APP_JS and ">TB</span>" in APP_JS
    # TB branch must sit before the generic backup_only (UB) branch.
    tb_idx = APP_JS.index("link-glyph-tb")
    ub_idx = APP_JS.index("link-glyph-bk")
    assert tb_idx < ub_idx, "TB branch must precede the UB branch"


def test_tb_filter_chip_and_lists():
    assert 'data-link-pill="tb"' in LIB_HTML
    assert "link-glyph-tb" in LIB_HTML
    assert "'hl','c','m','none','ps','pu','rp','b','bk','tb'" in APP_JS
    assert "'hl', 'c', 'm', 'none', 'ps', 'pu', 'rp', 'b', 'bk', 'tb'" in APP_JS


def test_promote_tone_branches_on_themerrdb():
    assert "promoteSourceKind === 'themerrdb'" in APP_JS
    assert "'btn-promote-tb'" in APP_JS


def test_playback_label_has_tb_branch():
    assert "(TB badge · backup-only" in APP_JS


# ── SQL: tb matches themerrdb; bk excludes it ────────────────


def test_sql_tb_branch_and_whitelist():
    assert 'elif p == "tb":' in API_PY
    tb_idx = API_PY.index('elif p == "tb":')
    tb_block = API_PY[tb_idx:tb_idx + 900]
    # v1.21.59: lf.* reads are now COALESCE(lf_e.x, lf_g.x) (per-edition).
    assert "COALESCE(lf_e.source_kind, lf_g.source_kind) = 'themerrdb'" in tb_block
    assert "backup_only" in tb_block
    assert '"tb"' in API_PY  # whitelist
    # BK now excludes themerrdb.
    bk_idx = API_PY.index('elif p == "bk":')
    bk_block = API_PY[bk_idx:bk_idx + 1600]
    assert "NOT IN" in bk_block and "themerrdb" in bk_block


# ── behavioral: a themerrdb-backup row filters as TB, not UB ──


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


def test_themerrdb_backup_row_filters_as_tb_not_ub(admin_client, tmp_path):
    from app.core.db import init_db
    db = _db(tmp_path)
    init_db(db)
    now = "2026-05-26T00:00:00"
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, "
            "  is_anime, is_4k, themes_subdir, included, discovered_at, "
            "  last_seen_at) VALUES ('1','Movies','movie',0,0,'movies',1,"
            "  ?, ?)", (now, now))
        conn.execute(
            "INSERT INTO themes (id, media_type, tmdb_id, title, "
            "  upstream_source, last_seen_sync_at, first_seen_sync_at, "
            "  youtube_url) VALUES (1,'movie',101,'TDB Backup Row','imdb',"
            "  ?, ?, 'https://www.youtube.com/watch?v=X')", (now, now))
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type, "
            "  theme_id, guid_imdb, guid_tmdb, title, year, has_theme, "
            "  local_theme_file, folder_path, plex_independent_theme, "
            "  plex_theme_verified_ok, first_seen_at, last_seen_at) "
            "VALUES ('r1','1','movie',1,NULL,101,'TDB Backup Row',2020,1,"
            "  0,'/data/movies/x',1,1,?,?)", (now, now))
        # themerrdb-sourced backup_only canonical, no placement.
        conn.execute(
            "INSERT INTO local_files (media_type, tmdb_id, section_id, "
            "  theme_id, file_path, downloaded_at, source_video_id, "
            "  provenance, source_kind, last_place_attempt_reason) "
            "VALUES ('movie',101,'1',1,'movies/x/theme.mp3',?,'X','auto',"
            "  'themerrdb','backup_only')", (now,))
        conn.commit()

    def titles(link):
        r = admin_client.get(
            f"/api/library?tab=movies&link_pills={link}&per_page=50",
            headers=AUTH)
        assert r.status_code == 200, r.text
        return [(it.get("plex_title") or it.get("theme_title"))
                for it in r.json()["items"]]

    # TB matches the row; UB (bk) does NOT (it's now themerrdb-excluded).
    assert "TDB Backup Row" in titles("tb"), "themerrdb backup must match TB"
    assert "TDB Backup Row" not in titles("bk"), (
        "v1.19.90: a themerrdb backup must NOT match UB anymore"
    )


def test_v1_19_90_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
