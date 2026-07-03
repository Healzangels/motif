"""v0.51.42 — api_item resolves plex_independent_theme + has_theme independently.

Code-review finding on v0.51.37: the fallback chain gated the section/global
`MAX(plex_independent_theme)` fallback on `_pi_independent is None AND
_pi_has_theme is None`. Because plex_items.has_theme is `NOT NULL`, a tier-1
rating_key hit ALWAYS set `_pi_has_theme`, so the conjunct short-circuited the
fallback — a row whose own plex_independent_theme is NULL (a sidecar row;
plex_enum writes None) never resolved the flag off its siblings. The info-card
+P label regressed from 1 → null for a multi-edition title.

Fix: run each tier if EITHER flag is unresolved, and fill only the column still
None — so independent resolves off siblings again while a tier-1 rk has_theme is
never clobbered.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()
AUTH = {"X-Authentik-Username": "testadmin"}


# ── source guard: the coupled gate is gone ───────────────────────────

def test_fallback_gate_is_decoupled():
    # the coupling that caused the regression must not come back.
    assert "_pi_independent is None and _pi_has_theme is None" not in API_PY
    # each tier now runs on an OR of the two, and fills only what's still None.
    assert "(_pi_independent is None or _pi_has_theme is None) and section_id" in API_PY


# ── behavioral: NULL-independent rk resolves the flag off a sibling ───

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
    init_db(settings.db_path)
    init_auth_schema(settings.db_path)
    create_admin(settings.db_path, username="testadmin", password="testpassword")
    return TestClient(create_app(settings))


def _seed(db, *, r1_indep, r2_indep):
    now = "2026-07-03T00:00:00"
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime, is_4k, "
            "  themes_subdir, included, discovered_at, last_seen_at) "
            "VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (now, now))
        conn.execute(
            "INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source, "
            "  last_seen_sync_at, first_seen_sync_at, youtube_url) "
            "VALUES (1,'movie',5199,'Two-Edition Title','imdb',?,?, "
            "  'https://www.youtube.com/watch?v=X')", (now, now))
        # r1 — the queried edition: its OWN plex_independent_theme is NULL
        # (sidecar row), has_theme=1 (Plex serving).
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type, theme_id, "
            "  guid_tmdb, title, year, has_theme, local_theme_file, folder_path, "
            "  plex_independent_theme, plex_theme_verified_ok, first_seen_at, "
            "  last_seen_at) VALUES ('r1','1','movie',1,5199,'Two-Edition Title',"
            "  1999,1,0,'/data/movies/x {edition-Theatrical}',?,1,?,?)",
            (r1_indep, now, now))
        # r2 — a sibling edition (same guid+section) with the flag set.
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type, theme_id, "
            "  guid_tmdb, title, year, has_theme, local_theme_file, folder_path, "
            "  plex_independent_theme, plex_theme_verified_ok, first_seen_at, "
            "  last_seen_at) VALUES ('r2','1','movie',1,5199,'Two-Edition Title',"
            "  1999,1,0,'/data/movies/x {edition-Extended}',?,1,?,?)",
            (r2_indep, now, now))
        conn.commit()


def test_null_independent_rk_resolves_from_sibling(admin_client, tmp_path):
    from app.config import Settings
    db = Settings(config_dir=tmp_path, data_dir=tmp_path / "data").db_path
    _seed(db, r1_indep=None, r2_indep=1)   # queried rk NULL, sibling independent
    r = admin_client.get("/api/items/movie/5199?rating_key=r1&section_id=1", headers=AUTH)
    assert r.status_code == 200, r.text
    data = r.json()
    # regression was null here; the sibling's 1 must resolve through the fallback.
    assert data.get("plex_independent_theme") == 1
    assert data.get("plex_has_theme") == 1


def test_resolved_rk_flag_wins_over_sibling(admin_client, tmp_path):
    # when the queried rk's OWN flag is resolved (0), the fallback must NOT run —
    # its per-edition value wins, not a sibling's 1 (no edition conflation).
    from app.config import Settings
    db = Settings(config_dir=tmp_path, data_dir=tmp_path / "data").db_path
    _seed(db, r1_indep=0, r2_indep=1)
    r = admin_client.get("/api/items/movie/5199?rating_key=r1&section_id=1", headers=AUTH)
    assert r.status_code == 200, r.text
    assert r.json().get("plex_independent_theme") == 0
