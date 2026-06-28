"""v1.20.12 — ACCEPT UPDATE always re-pends on terminal download failure.

Silent-bug audit HIGH-1: ACCEPT UPDATE (per-row + bulk accept-all) flips
pending_updates.decision='accepted' at click, clearing the pill, but
stamped a rollback ONLY when there was an override URL to restore. An
accept with NO override — a SRC=— new_theme_available row, or a pure-T
upstream change with no user URL — got rollback=None, so a terminal
download failure left the row stuck decision='accepted' with no theme on
disk and no recovery surface (sync only re-emits new_theme on is_new).

Fix (chosen: rollback-on-failure, keeps the instant-accept UX):
- api.py: ACCEPT UPDATE + bulk accept-all ALWAYS stamp a baseline
  rollback {"kind": "accept_update"} when they flip a decision; the
  override fields are added only when there's an override to restore.
- worker.py _run_rollback_safe: the decision re-pend now runs
  UNCONDITIONALLY for kind='accept_update' (was nested inside
  `if replaced`); the override restore is the additive, override-only
  half.
"""
from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()
WORKER_PY = (REPO / "app" / "core" / "worker.py").read_text()
NOW = "2026-05-29T00:00:00"


def _settings(tmp_path):
    from app.config import Settings
    from app.core.db import init_db
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    return s


def _worker(settings):
    from app.core.worker import Worker, TokenBucket
    return Worker(settings=settings, stop_event=threading.Event(),
                  bucket=TokenBucket(60, 60))


def _seed_accepted_new_theme(db, *, tmdb_id, section_id="3"):
    """A new_theme row already flipped to decision='accepted' (the
    post-click state), as if ACCEPT UPDATE just ran."""
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, upstream_source, "
            "last_seen_sync_at, first_seen_sync_at, youtube_url) "
            "VALUES ('tv',?,'X','imdb',?,?,'u')", (tmdb_id, NOW, NOW))
        conn.execute(
            "INSERT INTO pending_updates (media_type, tmdb_id, section_id, "
            "kind, new_youtube_url, decision, decision_at, decision_by, "
            "detected_at) VALUES ('tv',?,?,'new_theme_available','u',"
            "'accepted',?,'admin',?)", (tmdb_id, section_id, NOW, NOW))
        conn.commit()


def _fake_job(db, *, tmdb_id, section_id, rollback):
    """Insert + fetch a failed download job carrying the rollback recipe."""
    payload = json.dumps({"rollback": rollback})
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO jobs (job_type, media_type, tmdb_id, section_id, "
            "payload, status, created_at) "
            "VALUES ('download','tv',?,?,?,'failed',?)",
            (tmdb_id, section_id, payload, NOW))
        conn.commit()
    c = sqlite3.connect(db)
    c.row_factory = sqlite3.Row
    return c.execute("SELECT * FROM jobs WHERE tmdb_id=?", (tmdb_id,)).fetchone()


def _decision(db, tmdb_id, section_id="3"):
    with sqlite3.connect(db) as conn:
        r = conn.execute(
            "SELECT decision FROM pending_updates WHERE tmdb_id=? "
            "AND section_id=?", (tmdb_id, section_id)).fetchone()
    return r[0] if r else None


# ── core fix: worker re-pends even with NO override ──────────


def test_worker_repends_accept_failure_without_override(tmp_path):
    """HIGH-1: a no-override accept rollback must still re-pend the
    decision (pre-fix the re-pend was trapped inside `if replaced`)."""
    settings = _settings(tmp_path)
    db = settings.db_path
    _seed_accepted_new_theme(db, tmdb_id=9001)
    job = _fake_job(db, tmdb_id=9001, section_id="3",
                    rollback={"kind": "accept_update"})  # no replaced_user_url
    _worker(settings)._run_rollback_safe(job, "download failed: dead video")
    assert _decision(db, 9001) == "pending", (
        "v1.20.12: a failed accept with no override must re-pend the row "
        "so the pill returns + the user can choose again"
    )


def test_worker_repends_and_restores_override(tmp_path):
    """Counter-guard: the override-restore half still works (now in the
    conditional branch) — decision re-pends AND the override is back."""
    settings = _settings(tmp_path)
    db = settings.db_path
    _seed_accepted_new_theme(db, tmdb_id=9002)
    job = _fake_job(db, tmdb_id=9002, section_id="3", rollback={
        "kind": "accept_update",
        "replaced_user_url": "https://yt/mine",
        "prior_intent": "backup",
    })
    _worker(settings)._run_rollback_safe(job, "download failed")
    assert _decision(db, 9002) == "pending"
    with sqlite3.connect(db) as conn:
        ovr = conn.execute(
            "SELECT youtube_url, intent FROM user_overrides WHERE tmdb_id=9002 "
            "AND section_id='3'").fetchone()
    assert ovr is not None and ovr[0] == "https://yt/mine"
    assert ovr[1] == "backup", "v1.19.36 intent preservation must survive"


def test_worker_repend_is_unconditional_source_pin():
    """Structural guard: the decision re-pend UPDATE must NOT be nested
    inside `if replaced` (that's the bug). The `if replaced:` must come
    AFTER the pending_updates re-pend in the accept_update branch."""
    start = WORKER_PY.index('if kind == "accept_update":')
    nxt = WORKER_PY.index('elif kind == "revert":', start)
    branch = WORKER_PY[start:nxt]
    repend = branch.index("UPDATE pending_updates SET")
    if_replaced = branch.index("if replaced:")
    assert repend < if_replaced, (
        "v1.20.12: the decision re-pend must run unconditionally — it must "
        "appear BEFORE the `if replaced:` override-restore branch"
    )


# ── endpoints stamp a rollback even with no override ─────────


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
    return TestClient(create_app(settings)), db


AUTH = {"X-Authentik-Username": "testadmin"}


def _seed_new_theme_with_item(db, tmdb_id):
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT OR IGNORE INTO plex_sections (section_id, title, type, "
            "is_anime, is_4k, themes_subdir, included, discovered_at, "
            "last_seen_at) VALUES ('3','Anime','show',1,0,'anime',1,?,?)",
            (NOW, NOW))
        cur = conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, upstream_source, "
            "last_seen_sync_at, first_seen_sync_at, youtube_url) "
            "VALUES ('tv',?,'X','imdb',?,?,'https://yt/new')", (tmdb_id, NOW, NOW))
        tid = cur.lastrowid
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type, "
            "theme_id, guid_tmdb, title, year, has_theme, first_seen_at, "
            "last_seen_at) VALUES (?,'3','show',?,?,'X',2024,0,?,?)",
            (f"rk{tmdb_id}", tid, tmdb_id, NOW, NOW))
        conn.execute(
            "INSERT INTO pending_updates (media_type, tmdb_id, section_id, "
            "kind, new_youtube_url, decision, detected_at) "
            "VALUES ('tv',?,'3','new_theme_available','https://yt/new',"
            "'pending',?)", (tmdb_id, NOW))
        conn.commit()


def _job_rollback_kind(db, tmdb_id):
    with sqlite3.connect(db) as conn:
        r = conn.execute(
            "SELECT payload FROM jobs WHERE job_type='download' AND tmdb_id=?",
            (tmdb_id,)).fetchone()
    if not r:
        return None
    return (json.loads(r[0]).get("rollback") or {}).get("kind")


def test_accept_update_stamps_rollback_for_no_override_row(admin_client):
    client, db = admin_client
    _seed_new_theme_with_item(db, 9101)
    r = client.post("/api/updates/tv/9101/accept?section_id=3", headers=AUTH)
    assert r.status_code == 200, r.text
    assert _job_rollback_kind(db, 9101) == "accept_update", (
        "v1.20.12: per-row ACCEPT UPDATE must stamp a rollback even with "
        "no override, so a failed download re-pends the row"
    )


def test_bulk_accept_all_stamps_rollback_for_no_override_row(admin_client):
    client, db = admin_client
    _seed_new_theme_with_item(db, 9102)
    r = client.post("/api/updates/accept-all", headers=AUTH)
    assert r.status_code == 200, r.text
    assert _job_rollback_kind(db, 9102) == "accept_update", (
        "v1.20.12: bulk accept-all must stamp a rollback per row too"
    )


def test_v1_20_12_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
