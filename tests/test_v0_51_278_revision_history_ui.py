"""v0.51.278 — feature-brief B, UI: the INFO card's revisions section.

Backend shipped in .277; this surfaces it. Revisions ride api_item's single
fetch (the card is ONE request by design — v1.23.19 caches one promise), the
section renders only when history exists, RESTORE reuses the MEASURE NOW
button shape, and a refusal (already-active / metadata-only) renders the
endpoint's operator-readable 409 detail inline instead of a dead generic
error. The v1.18.81 rule is the point here: the backend→frontend pipe is
proven at the ENDPOINT (api_item really carries the rows), not just pinned in
JS.
"""
from __future__ import annotations

import hashlib
import sqlite3
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
NOW = "2026-08-25T00:00:00+00:00"
MT, TID = "movie", 278001


@pytest.fixture
def client(tmp_path, monkeypatch):
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import get_conn, init_db, transaction
    from app.web.api import create_app
    from fastapi.testclient import TestClient
    themes = tmp_path / "data" / "themes"
    (themes / "movies" / "T (2020)").mkdir(parents=True)
    (tmp_path / "motif.yaml").write_text(f"paths:\n  themes_dir: {themes}\n")
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    with get_conn(s.db_path) as conn, transaction(conn):
        conn.execute(
            """INSERT INTO plex_sections (section_id, title, type, is_anime,
                 is_4k, themes_subdir, included, discovered_at, last_seen_at)
               VALUES ('1', 'Movies', 'movie', 0, 0, 'movies', 1, ?, ?)""",
            (NOW, NOW))
        conn.execute(
            """INSERT INTO themes (media_type, tmdb_id, title, year,
                 upstream_source, last_seen_sync_at, first_seen_sync_at)
               VALUES (?, ?, 'T', '2020', 'themoviedb', ?, ?)""",
            (MT, TID, NOW, NOW))
        rel = "movies/T (2020)/theme.mp3"
        (themes / rel).write_bytes(b"CURRENT")
        conn.execute(
            """INSERT INTO local_files (media_type, tmdb_id, section_id,
                 edition_key, file_path, file_sha256, file_size, downloaded_at,
                 source_video_id, provenance, source_kind)
               VALUES (?, ?, '1', '', ?, ?, 7, ?, 'vidCUR00001', 'auto', 'themerrdb')""",
            (MT, TID, rel, hashlib.sha256(b"CURRENT").hexdigest(), NOW))
    return TestClient(create_app(s)), s


AUTH = {"X-Authentik-Username": "testadmin"}


def _seed_rev(s, retained=True):
    from app.core.db import get_conn, transaction
    rel = ".revisions/movie-278001-s1-std-abc.mp3"
    if retained:
        (s.themes_dir / ".revisions").mkdir(exist_ok=True)
        (s.themes_dir / rel).write_bytes(b"OLD")
    with get_conn(s.db_path) as conn, transaction(conn):
        conn.execute(
            """INSERT INTO theme_revisions (media_type, tmdb_id, section_id,
                 edition_key, created_at, source_kind, source_video_id,
                 content_sha256, file_size, reason, actor, retained_path)
               VALUES (?, ?, '1', '', ?, 'themerrdb', 'vidOLD00001', ?, 3,
                       'replaced_by_download', 'system', ?)""",
            (MT, TID, NOW, hashlib.sha256(b"OLD").hexdigest(),
             rel if retained else None))


# ── the pipe: api_item actually carries the rows (v1.18.81) ──


def test_api_item_embeds_revisions(client):
    c, s = client
    _seed_rev(s)
    r = c.get(f"/api/items/{MT}/{TID}", headers=AUTH)
    assert r.status_code == 200, r.text
    revs = r.json().get("revisions")
    assert revs and len(revs) == 1, (
        "v1.18.81: the JS conditional is worthless unless the endpoint "
        "actually returns the field it keys on")
    assert revs[0]["restorable"] == 1
    assert revs[0]["source_video_id"] == "vidOLD00001"


def test_api_item_with_no_history_returns_empty_list(client):
    c, _ = client
    r = c.get(f"/api/items/{MT}/{TID}", headers=AUTH)
    assert r.status_code == 200
    assert r.json().get("revisions") == [], (
        "empty list, not a missing key — the JS gate is `revs.length`")


def test_restore_through_the_ui_contract(client):
    """The exact call the button makes, end to end."""
    c, s = client
    _seed_rev(s)
    from app.core.db import get_conn
    with get_conn(s.db_path) as conn:
        rid = conn.execute("SELECT id FROM theme_revisions").fetchone()[0]
    rr = c.post(f"/api/revisions/{rid}/restore", headers=AUTH)
    assert rr.status_code == 200, rr.text
    r2 = c.get(f"/api/items/{MT}/{TID}", headers=AUTH).json()
    reasons = [x["reason"] for x in r2["revisions"]]
    assert "replaced_by_restore" in reasons, "the transition row reaches the card"
    rr2 = c.post(f"/api/revisions/{rid}/restore", headers=AUTH)
    assert rr2.status_code == 409
    assert "already the active content" in rr2.json()["detail"], (
        "the refusal detail is what the button renders inline")


# ── the card render + binding (structural, anchored) ─────────


def test_card_renders_revisions_between_loudness_and_disk():
    i = APP_JS.index("${_grp('loudness', _loudnessRows)}")
    j = APP_JS.index("${_grp('file & placement', _onDiskRows)}")
    section = APP_JS[i:j]
    assert "data.revisions" in section
    assert "<h4>revisions</h4>" in section
    assert "if (!revs.length) return '';" in section, (
        "a fresh row gets no empty shell")
    assert 'data-act="rev-restore"' in section
    assert "metadata only" in section, (
        "a rotated revision says so instead of offering a dead button")


def test_restore_button_binding_and_conventions():
    i = APP_JS.index('button[data-act="rev-restore"]')
    block = APP_JS[APP_JS.rindex("// v0.51.278", 0, i):APP_JS.index("loud-measure", i)]
    assert "setTimeout(refreshTopbarStatus, 1100)" in block, "bug class #7"
    assert "b.disabled = true" in block, "no double-restore from a double-click"
    assert "e.message" in block, "the 409 refusal reason renders inline"


def test_v0_51_278_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
