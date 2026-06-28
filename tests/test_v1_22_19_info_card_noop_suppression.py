"""v1.22.19 — INFO-card no-op pending suppression in the edition-first branch.

Audit MED #7 (contract-drift class, like v1.17.10/.17). v1.12.119 added a no-op
suppression to api_item's pending-update resolution: when a pending's
new_youtube_url already equals the row's currently-applied URL, accepting would
just re-download the same URL, so the card hides it (the row pill / actionable
gate hides it too). v1.21.68 later added an EDITION-FIRST resolution branch
(`if has_presence and _info_edition is not None:`) that fetches the pending by
(section, edition) → (section, '') → global — but it never carried the
v1.12.119 suppression. So for any edition-resolved row (rating_key sent, the
common path), a no-op pending surfaced a PROPOSED CHANGE in the INFO card that
the row pill suppresses — a card-vs-pill disagreement.

v1.22.19 mirrors the suppression into the edition-first branch. 'urls_match'
stays exempt (the U→T classification flip is meaningful even when URLs match).
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()
NOW = "2026-06-06T00:00:00"
APPLIED = "https://y/watch?v=APPLIED"
AUTH = {"X-Authentik-Username": "testadmin"}


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


def _db(tmp_path):
    from app.config import Settings
    return Settings(config_dir=tmp_path, data_dir=tmp_path / "data").db_path


def _seed(tmp_path, *, tmdb, new_url, rk):
    """A movie with a user override (the applied URL) + an upstream_changed
    pending whose new URL is `new_url`. Untagged folder → edition ''."""
    db = _db(tmp_path)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT OR IGNORE INTO plex_sections (section_id, title, type,"
            " is_anime, is_4k, themes_subdir, included, discovered_at,"
            " last_seen_at) VALUES ('1','Movies','movie',0,0,'movies',1,?,?)",
            (NOW, NOW))
        conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, upstream_source,"
            " last_seen_sync_at, first_seen_sync_at, youtube_url)"
            " VALUES ('movie',?,?, 'imdb',?,?,'https://y/watch?v=TDB')",
            (tmdb, f"Movie {tmdb}", NOW, NOW))
        tid = conn.execute(
            "SELECT id FROM themes WHERE media_type='movie' AND tmdb_id=?",
            (tmdb,)).fetchone()[0]
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type,"
            " theme_id, guid_imdb, guid_tmdb, title, year, has_theme,"
            " local_theme_file, folder_path, plex_independent_theme,"
            " plex_theme_verified_ok, edition_key, first_seen_at, last_seen_at)"
            " VALUES (?, '1','movie',?,?,?,?,2012,1,0,?,0,1,'',?,?)",
            (rk, tid, f"tt{tmdb}", tmdb, f"Movie {tmdb}",
             f"/data/m/Movie {tmdb}", NOW, NOW))
        # The applied URL (override@section) → also makes has_presence True.
        conn.execute(
            "INSERT INTO user_overrides (media_type, tmdb_id, section_id,"
            " edition_key, youtube_url, intent, set_at, set_by)"
            " VALUES ('movie',?, '1','',?, 'replace', ?, 'admin')",
            (tmdb, APPLIED, NOW))
        # Pending upstream_changed at (section '1', edition '').
        conn.execute(
            "INSERT INTO pending_updates (media_type, tmdb_id, section_id,"
            " edition_key, old_video_id, new_video_id, old_youtube_url,"
            " new_youtube_url, detected_at, decision, kind)"
            " VALUES ('movie',?, '1','','OLD','NEW','https://y/watch?v=OLD',"
            " ?, ?, 'pending','upstream_changed')",
            (tmdb, new_url, NOW))
        conn.commit()


def _info(client, tmdb, rk):
    r = client.get(f"/api/items/movie/{tmdb}?section_id=1&rating_key={rk}",
                   headers=AUTH)
    assert r.status_code == 200, r.text
    return r.json()


def test_noop_pending_suppressed_in_edition_first_branch(admin_client, tmp_path):
    """The pending's new URL == the applied override URL → accepting is a
    no-op → the INFO card must NOT surface it (matches the row pill)."""
    _seed(tmp_path, tmdb=700, new_url=APPLIED, rk="rk1")
    body = _info(admin_client, 700, "rk1")
    assert body.get("pending_update") is None, (
        "v1.22.19: a no-op pending (new==applied) must be suppressed in the "
        f"edition-first INFO branch; got {body.get('pending_update')}"
    )


def test_real_diff_pending_still_surfaces(admin_client, tmp_path):
    """Control: a genuine change (new != applied) still surfaces, so the
    suppression doesn't over-reach."""
    _seed(tmp_path, tmdb=701, new_url="https://y/watch?v=DIFFERENT", rk="rk2")
    body = _info(admin_client, 701, "rk2")
    pu = body.get("pending_update")
    assert pu is not None and pu["new_youtube_url"] == "https://y/watch?v=DIFFERENT", (
        "v1.22.19: a real-diff pending must still surface in the INFO card"
    )


def test_urls_match_kind_exempt_from_suppression(admin_client, tmp_path):
    """'urls_match' is the meaningful-even-when-equal case (U→T flip) and must
    survive the suppression even though new == applied."""
    _seed(tmp_path, tmdb=702, new_url=APPLIED, rk="rk3")
    db = _db(tmp_path)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "UPDATE pending_updates SET kind='urls_match' WHERE tmdb_id=702")
        conn.commit()
    body = _info(admin_client, 702, "rk3")
    assert body.get("pending_update") is not None, (
        "v1.22.19: 'urls_match' must stay exempt from the no-op suppression"
    )


# ── Source pin: suppression present in the edition-first branch ──


def test_edition_first_branch_has_noop_suppression():
    i = API_PY.index("audit MED #7")
    block = API_PY[i:i + 1900]
    assert 'pu["kind"] != "urls_match"' in block
    assert 'pu["new_youtube_url"] == applied_url' in block
    assert "pu = None" in block
    # It sits before the section-branch elif (so it's in the edition-first arm).
    assert "elif has_presence:" in block


def test_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
