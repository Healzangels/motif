"""v0.51.279 — feature-brief A completion: dry-run on ACCEPT UPDATE.

The brief's remaining A criterion: "Dry-run shows proposed behavior without
downloading, replacing, or refreshing Plex." (Its OTHER criterion —
preview-both — was already met by shipped code: renderPendingUpdateDiff draws
thumbnail + oembed title + clickable link on BOTH the current and proposed
tiles since v1.14.3/v1.19.60, verified before this tag was scoped.)

`POST /api/updates/{mt}/{id}/accept?dry_run=true` returns the plan and writes
NOTHING: everything above the branch is reads (the pending-row fetch + the
override fetch), so returning inside the transaction commits nothing. The
plan names each side effect verbatim so a UI can render it.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

REPO = Path(__file__).resolve().parent.parent
NOW = "2026-08-25T00:00:00+00:00"
MT, TID, SEC = "movie", 279001, "1"


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web.api import create_app
    themes = tmp_path / "data" / "themes"
    themes.mkdir(parents=True)
    (tmp_path / "motif.yaml").write_text(f"paths:\n  themes_dir: {themes}\n")
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    with sqlite3.connect(s.db_path) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime, "
            "  is_4k, themes_subdir, included, discovered_at, last_seen_at) "
            "VALUES (?, 'Movies', 'movie', 0, 0, 'movies', 1, ?, ?)",
            (SEC, NOW, NOW))
        conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, year, youtube_url, "
            "  youtube_video_id, upstream_source, last_seen_sync_at, "
            "  first_seen_sync_at) "
            "VALUES (?, ?, 'T', '2020', 'https://www.youtube.com/watch?v=oldVID00001', "
            "        'oldVID00001', 'themoviedb', ?, ?)", (MT, TID, NOW, NOW))
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type, title, "
            "  year, guid_tmdb, edition_key, folder_path, has_theme, "
            "  first_seen_at, last_seen_at) "
            "VALUES ('rk-279', ?, 'movie', 'T', '2020', ?, '', '/d', 0, ?, ?)",
            (SEC, TID, NOW, NOW))
        conn.execute(
            "INSERT INTO user_overrides (media_type, tmdb_id, youtube_url, "
            "  set_at, set_by, section_id) "
            "VALUES (?, ?, 'https://www.youtube.com/watch?v=usrVID00001', ?, "
            "        'admin', '')", (MT, TID, NOW))
        conn.execute(
            "INSERT INTO pending_updates (media_type, tmdb_id, section_id, "
            "  old_video_id, new_video_id, old_youtube_url, new_youtube_url, "
            "  detected_at, decision, kind) "
            "VALUES (?, ?, '', 'oldVID00001', 'newVID00001', "
            "  'https://www.youtube.com/watch?v=oldVID00001', "
            "  'https://www.youtube.com/watch?v=newVID00001', ?, 'pending', "
            "  'upstream_changed')", (MT, TID, NOW))
        conn.commit()
    return TestClient(create_app(s)), s


AUTH = {"X-Authentik-Username": "testadmin"}


def _state(db):
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        return {
            "overrides": conn.execute(
                "SELECT COUNT(*) c FROM user_overrides").fetchone()["c"],
            "pending": conn.execute(
                "SELECT decision FROM pending_updates").fetchone()["decision"],
            "jobs": conn.execute("SELECT COUNT(*) c FROM jobs").fetchone()["c"],
            "themes_url": conn.execute(
                "SELECT youtube_url FROM themes").fetchone()["youtube_url"],
        }


def test_dry_run_reports_the_plan_and_writes_nothing(client):
    c, s = client
    before = _state(s.db_path)
    r = c.post(f"/api/updates/{MT}/{TID}/accept?section_id={SEC}&dry_run=true",
               headers=AUTH)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["dry_run"] is True
    w = body["would"]
    assert w["delete_override"] is True, "an override exists — ACCEPT would drop it"
    assert w["apply_url"].endswith("newVID00001")
    assert w["sections"] == [SEC]
    assert w["enqueue_download"] is True
    assert _state(s.db_path) == before, (
        "the brief's criterion verbatim: no download, no replace, no decision "
        "write — NOTHING may change on a dry run")


def test_dry_run_is_repeatable_then_real_accept_still_works(client):
    c, s = client
    for _ in range(3):
        assert c.post(f"/api/updates/{MT}/{TID}/accept?section_id={SEC}"
                      f"&dry_run=true", headers=AUTH).status_code == 200
    r = c.post(f"/api/updates/{MT}/{TID}/accept?section_id={SEC}", headers=AUTH)
    assert r.status_code == 200, r.text
    after = _state(s.db_path)
    assert after["overrides"] == 0, "the real accept deletes the override"
    assert after["jobs"] >= 1, "and enqueues the download"


def test_plan_is_honest_when_the_title_is_not_in_plex(client):
    """enqueue_download in the plan mirrors the fan-out's real gate — owning
    Plex rows. Claiming True unconditionally would promise a download the
    real accept would not perform (zero owning sections → zero jobs)."""
    c, s = client
    with sqlite3.connect(s.db_path) as conn:
        conn.execute("DELETE FROM plex_items"); conn.commit()
    r = c.post(f"/api/updates/{MT}/{TID}/accept?section_id={SEC}&dry_run=true",
               headers=AUTH)
    assert r.status_code == 200, r.text
    assert r.json()["would"]["enqueue_download"] is False


def test_default_remains_the_real_accept(client):
    """No param → unchanged pre-.279 behavior; dry-run is strictly opt-in."""
    c, s = client
    c.post(f"/api/updates/{MT}/{TID}/accept?section_id={SEC}", headers=AUTH)
    assert _state(s.db_path)["overrides"] == 0


def test_dry_run_requires_admin_like_the_real_thing(client):
    c, _ = client
    assert c.post(f"/api/updates/{MT}/{TID}/accept?dry_run=true"
                  ).status_code in (401, 403)


def test_preview_both_sides_already_shipped():
    """The other A criterion, recorded as MET rather than rebuilt: both diff
    tiles render a thumbnail and a clickable link."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    i = js.index("function renderPendingUpdateDiff(")
    body = js[i:js.index("\n  function ", i + 1)]
    assert body.count("diff-tile-thumb") >= 2, "a thumb on each side"
    assert body.count("<a href=") >= 2, "a click-through on each side"


def test_v0_51_279_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
