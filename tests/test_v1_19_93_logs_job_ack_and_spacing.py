"""v1.19.93 — LOGS JOBS table: ack theme-less failures + NOTE spacing.

Two issues the user hit after the v1.19.71/.91 _flush_sync_batch
sync crash put a failed SYNC job in the LOGS FAILED list:

1. **No // ACK button on the failed sync job.** The `ackableFail`
   predicate (app.js) required `j.media_type && j.tmdb_id` — but a
   global SYNC/enum job has neither, so the button never rendered
   and the crashed sync run was undismissable from LOGS. The
   endpoint it would call, POST /api/jobs/{id}/ack-failure, ALREADY
   handles theme-less jobs (api.py:18352 — stamps jobs.acked_at,
   skips the theme ack). So the backend was ready; only the button
   predicate blocked it. Fix: relax the predicate to
   `!isOpProgress && status==='failed' && !acked_at` (op-progress
   synthetic rows stay excluded — their string ids can't hit the
   int-typed endpoint).

2. **Cramped spacing** between the NOTE (error message) column and
   the ACTION button — the grid `gap` (12px) alone read as tight
   once a button was present. Fix: a `.jobs-note-cell` class on the
   NOTE span + `padding-right: var(--gap-4)` so the ellipsis-
   truncated error never butts against the button. Internal
   padding → column track unchanged → header stays aligned.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


# ── 1. predicate relaxed (source pins) ───────────────────────


def test_ackable_predicate_drops_media_type_requirement():
    p = APP_JS.index("const ackableFail =")
    pred = APP_JS[p:p + 160]
    assert "!isOpProgress" in pred
    assert "j.status === 'failed'" in pred
    assert "!j.acked_at" in pred
    assert "j.media_type" not in pred, (
        "v1.19.93: ackableFail must not require media_type — "
        "theme-less sync/enum failures are ackable too"
    )
    assert "j.tmdb_id" not in pred


# ── 2. NOTE-cell spacing (source pins) ───────────────────────


def test_note_cell_class_on_render():
    assert 'class="muted jobs-note-cell"' in APP_JS, (
        "v1.19.93: the NOTE span must carry .jobs-note-cell so the "
        "spacing rule can target it"
    )


def test_note_cell_has_right_padding_css():
    idx = APP_CSS.index(".jobs-grid-row > .jobs-note-cell")
    rule = APP_CSS[idx:idx + 600]
    assert "padding-right: var(--gap-4)" in rule, (
        "v1.19.93: NOTE cell must reserve right-padding so the "
        "error text doesn't butt against the ACTION button"
    )


# ── 3. behavioral: a theme-less failed SYNC job is ackable ───


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


def test_failed_sync_job_without_theme_is_ackable(admin_client):
    client, db = admin_client
    now = "2026-05-29T00:00:00"
    with sqlite3.connect(db) as conn:
        cur = conn.execute(
            "INSERT INTO jobs (job_type, media_type, tmdb_id, status, "
            "  last_error, created_at) "
            "VALUES ('sync', NULL, NULL, 'failed', "
            "  '_flush_sync_batch() missing 1 required keyword-only "
            "argument', ?)",
            (now,),
        )
        job_id = cur.lastrowid
        conn.commit()

    # The theme-less sync job acks cleanly: jobs.acked_at stamped,
    # theme_acked False (nothing to ack), HTTP 200.
    r = client.post(f"/api/jobs/{job_id}/ack-failure", headers=AUTH)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["ok"] is True
    assert body["theme_acked"] is False, (
        "v1.19.93: a sync job has no theme binding — theme_acked "
        "must be False, but the job itself still acks"
    )

    with sqlite3.connect(db) as conn:
        acked = conn.execute(
            "SELECT acked_at FROM jobs WHERE id = ?", (job_id,)
        ).fetchone()[0]
    assert acked is not None, (
        "v1.19.93: jobs.acked_at must be stamped so the LOGS row "
        "flips to the green 'failed (acknowledged)' tone"
    )


def test_acking_nonfailed_job_409s(admin_client):
    """Guard the endpoint contract the button relies on: only
    failed jobs ack (pending/running/done 409)."""
    client, db = admin_client
    now = "2026-05-29T00:00:00"
    with sqlite3.connect(db) as conn:
        cur = conn.execute(
            "INSERT INTO jobs (job_type, status, created_at) "
            "VALUES ('sync', 'running', ?)", (now,))
        job_id = cur.lastrowid
        conn.commit()
    r = client.post(f"/api/jobs/{job_id}/ack-failure", headers=AUTH)
    assert r.status_code == 409, r.text


# ── version pin ──────────────────────────────────────────────


def test_v1_19_93_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
