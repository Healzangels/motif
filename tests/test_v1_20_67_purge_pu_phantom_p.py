"""v1.20.67 — fix phantom-P on a PU row after PURGE (api_forget_item).

Same class as v1.20.66 (DEL), other handler. A PU row's rk can't enter
the folder-based `rk_from_placement` skip (its `folder_path` is real, the
placement sentinel is ''), so `_rks_we_actually_touched = rk_clear &
rk_from_placement` excludes it → the transaction's rk_zero/rk_keep_p
split never touches it (has_theme stays 1), and the inline HEAD-verify
hits Plex's stale-200 cache → phantom SRC=P even though PURGE destroyed
everything.

PURGE's teardown surfaces no per-rk fallback signal (unlike DEL's restore
loop), so the fix is the approved pessimistic zero: resolve the PU rks,
force them into rk_zero (so the transaction clears has_theme=0/verified_ok
=0 even when Plex is DISABLED — no inline-verify), and skip them in the
verify loop so a stale-200 probe can't re-raise them. If Plex genuinely
still serves its own theme, the next plex_enum re-detects it → P.

Behavioral coverage: the transaction-zero half runs WITHOUT Plex, so it's
tested end-to-end via the endpoint with Plex disabled (pre-fix the PU row
stayed has_theme=1). The verify-loop skip (Plex-enabled) needs a live-Plex
stale window → source-pinned.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()
NOW = "2026-06-01T00:00:00+00:00"
AUTH = {"X-Authentik-Username": "testadmin"}


# ── Behavioral: PURGE zeroes a PU row even with Plex disabled ─────


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


def _settings_db(tmp_path):
    from app.config import Settings
    return Settings(config_dir=tmp_path, data_dir=tmp_path / "data").db_path


def _seed_pu_row(db):
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime,"
            " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
            " VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        conn.execute(
            "INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source,"
            " last_seen_sync_at, first_seen_sync_at)"
            " VALUES (50,'movie',500,'x','imdb',?,?)", (NOW, NOW))
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type, theme_id,"
            " guid_tmdb, title, has_theme, plex_theme_verified_ok, local_theme_file,"
            " folder_path, first_seen_at, last_seen_at)"
            " VALUES ('rk-pu','1','movie',50,500,'x',1,1,0,'/data/movies/x',?,?)",
            (NOW, NOW))
        conn.execute(
            "INSERT INTO local_files (media_type, tmdb_id, section_id, file_path,"
            " source_video_id, downloaded_at, source_kind)"
            " VALUES ('movie',500,'1','x.mp3','vid',?,'plex_cloud')", (NOW,))
        # plex_upload placement, media_folder='' (the PU sentinel).
        conn.execute(
            "INSERT INTO placements (theme_id, media_type, tmdb_id, section_id,"
            " media_folder, placed_at, placement_kind, plex_refreshed)"
            " VALUES (50,'movie',500,'1','',?,'plex_upload',1)", (NOW,))
        conn.commit()


def test_purge_zeroes_pu_row_with_plex_disabled(admin_client, tmp_path):
    """With Plex disabled the inline-verify is skipped, so the
    transaction's rk_zero UPDATE is the ONLY thing that can clear the PU
    row. Pre-fix the PU rk wasn't in rk_zero (excluded by
    _rks_we_actually_touched) → has_theme stayed 1 → phantom P. Post-fix
    it's forced into rk_zero → has_theme=0."""
    db = _settings_db(tmp_path)
    _seed_pu_row(db)
    r = admin_client.post("/api/items/movie/500/forget", headers=AUTH)
    assert r.status_code == 200, r.text
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT has_theme, plex_theme_verified_ok FROM plex_items "
            "WHERE rating_key='rk-pu'").fetchone()
        # The PU placement + local_files are gone (PURGE destroyed them).
        pl = conn.execute(
            "SELECT COUNT(*) n FROM placements WHERE tmdb_id=500").fetchone()
        lf = conn.execute(
            "SELECT COUNT(*) n FROM local_files WHERE tmdb_id=500").fetchone()
    assert row["has_theme"] == 0, (
        "PURGE must zero a PU row's has_theme even with Plex disabled "
        "(was phantom-P: the rk_zero split missed it)"
    )
    assert row["plex_theme_verified_ok"] == 0
    assert pl["n"] == 0 and lf["n"] == 0, "PURGE still destroys the row"


# ── Source pins for the Plex-gated verify-loop skip ──────────────


def _fn() -> str:
    start = API_PY.index("def api_forget_item(")
    end = API_PY.index("\n    @app.", start + 100)
    return API_PY[start:end]


def test_plex_upload_rks_resolved_from_pu_placements():
    fn = _fn()
    assert "plex_upload_rks: set[str] = set()" in fn
    anchor = fn.index("pu_sections = [")
    block = fn[anchor:anchor + 200]
    assert "(pr[\"placement_kind\"] or \"\") == \"plex_upload\"" in block


def test_pu_rks_forced_into_rk_zero():
    fn = _fn()
    anchor = fn.index("for _pu_rk in plex_upload_rks:")
    block = fn[anchor:anchor + 160]
    assert "rk_zero.append(_pu_rk)" in block


def test_verify_loop_skips_pu_rks_before_probe():
    fn = _fn()
    skip_idx = fn.index("if rk in plex_upload_rks:")
    owned_idx = fn.index("if rk in rk_from_placement:")
    # v1.22.58: the probe is offloaded via run_in_threadpool (event-loop
    # lint) — anchor on the assignment, which still marks the probe site.
    probe_idx = fn.index("result = await run_in_threadpool(")
    assert skip_idx < owned_idx < probe_idx


def test_v1_20_67_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
