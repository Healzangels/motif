"""v1.18.95 — surface op_progress kinds in the /logs JOBS panel.

the user's repro: BULK PROBE TDB ran for 16+ minutes (visible in
the LIVE OPS drawer as 330/5,332 = 6%, ELAPSED 16m 42s) but the
/logs JOBS panel said "no jobs in the queue" the entire time.
The EVENT STREAM only had the single START event from 21:48:31
with nothing for the next 16 minutes.

## Root cause

bulk_probe_tdb writes to `op_progress`, not `jobs`. The /api/jobs
endpoint (which feeds the JOBS panel) queries `jobs` only.
Same gap for the other op_progress-only kinds:

  - bulk_lps           (bulk LET PLEX SERVE)
  - tvdb_bridge        (TVDB→TMDB bridge rebuild)
  - reprobe_plex_themes

(tdb_sync + plex_enum also have op_progress rows, but those ALSO
create `jobs` rows so they show in the panel by that path.)

## Fix

Extend `/api/jobs` to synthesize op_progress rows into the
response as job-like dicts. Synthetic rows:

  - id = 'op:<op_id>' (string, distinguishable from int job ids)
  - job_type = op_progress.kind
  - status = op_progress.status (with 'cancelling' → 'running'
    for filter consistency — operationally still running)
  - created_at = op_progress.started_at
  - last_error = synthesised progress summary (stage_label +
    current/total + latest activity from detail_json)
  - is_op_progress = True (frontend flag)

Synthetic rows sort FIRST so live activity is most visible.
Status filter (`?status=running`) includes op_progress rows
whose status matches (running + cancelling).

Frontend (app.js): when `is_op_progress` is true, skip the
CANCEL button — /api/jobs/{id}/cancel expects int, and CANCEL
already works via the LIVE OPS drawer.

## What's pinned

- /api/jobs synthesizes a row for each running op_progress kind
- Status filter maps correctly (status=running includes
  op_progress.status in {running, cancelling})
- Synthetic rows sort before real jobs
- Combined response respects the limit cap
- Empty op_progress + real jobs returns just real jobs (no
  regression on the existing path)
- Frontend skips CANCEL button when is_op_progress=true
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"
APP_JS = REPO / "app" / "web" / "static" / "app.js"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ── Source-level pins: API + frontend wiring ─────────────────


def test_api_jobs_unions_op_progress():
    """/api/jobs must query op_progress + synthesize rows into
    the response. Pin the structural markers so a future refactor
    can't quietly drop the union."""
    src = API_PY.read_text()
    # The endpoint must mention op_progress (the union source).
    # Scope to within the api_jobs function.
    fn_idx = src.index("async def api_jobs(")
    fn_end = src.index("@app.post(\"/api/jobs/", fn_idx)
    body = src[fn_idx:fn_end]
    assert "FROM op_progress" in body, (
        "v1.18.95: /api/jobs must SELECT FROM op_progress"
    )
    assert "is_op_progress" in body, (
        "v1.18.95: synthesized rows must carry is_op_progress flag"
    )
    assert "op:" in body, (
        "v1.18.95: synthetic id format must use 'op:' prefix"
    )


def test_api_jobs_handles_cancelling_under_running_filter():
    """When status='running' filter is active, op_progress rows
    in 'cancelling' state must also appear — operationally
    they're still running (worker walking to next safe yield)."""
    src = API_PY.read_text()
    fn_idx = src.index("async def api_jobs(")
    fn_end = src.index("@app.post(\"/api/jobs/", fn_idx)
    body = src[fn_idx:fn_end]
    assert "cancelling" in body, (
        "v1.18.95: 'cancelling' op_progress status must be "
        "surfaced under the 'running' filter"
    )


def test_frontend_skips_cancel_for_op_progress_rows():
    """app.js's actionCell must check is_op_progress and
    suppress the CANCEL button. /api/jobs/{id}/cancel expects
    int job_id; synthetic rows carry a string id so the
    endpoint would 422."""
    src = APP_JS.read_text()
    assert "is_op_progress" in src, (
        "v1.18.95: app.js must consume the is_op_progress flag"
    )
    # The check must gate the cancellable computation.
    idx = src.index("is_op_progress")
    block = src[idx:idx + 500]
    assert "cancellable" in block, (
        "v1.18.95: is_op_progress must gate the cancellable check"
    )


# ── End-to-end /api/jobs behavior ────────────────────────────


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


def _insert_op_progress(db, *, op_id, kind, status="running",
                         stage_label="Doing things",
                         stage_current=0, stage_total=0,
                         activity_messages=None):
    now = _now_iso()
    detail = {}
    if activity_messages:
        detail["activity"] = [
            {"message": m, "ts": now} for m in activity_messages
        ]
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO op_progress "
            "  (op_id, kind, status, started_at, updated_at, "
            "   stage, stage_label, stage_current, stage_total, "
            "   processed_total, error_count, detail_json) "
            "VALUES (?, ?, ?, ?, ?, 'main', ?, ?, ?, ?, 0, ?)",
            (op_id, kind, status, now, now,
             stage_label, stage_current, stage_total,
             stage_current,
             json.dumps(detail) if detail else None),
        )
        conn.commit()


def _insert_job(db, *, job_type="download", status="running"):
    now = _now_iso()
    with sqlite3.connect(db) as conn:
        cur = conn.execute(
            "INSERT INTO jobs (job_type, status, media_type, "
            "                   tmdb_id, created_at) "
            "VALUES (?, ?, 'movie', 100, ?)",
            (job_type, status, now),
        )
        conn.commit()
        return cur.lastrowid


def _settings_db(tmp_path):
    from app.config import Settings
    return Settings(
        config_dir=tmp_path, data_dir=tmp_path / "data",
    ).db_path


def test_api_jobs_includes_running_op_progress(
    admin_client, tmp_path,
):
    """The repro: a running bulk_probe_tdb must appear in the
    /api/jobs response so the /logs JOBS panel can render it."""
    db = _settings_db(tmp_path)
    _insert_op_progress(
        db, op_id="bulk-probe-tdb", kind="bulk_probe_tdb",
        status="running",
        stage_label="Probing TDB URLs (all rows)",
        stage_current=330, stage_total=5332,
        activity_messages=[
            "alive=313 (cleared=0), dead=8, indeterminate=0, other=9",
        ],
    )
    r = admin_client.get("/api/jobs", headers=AUTH)
    assert r.status_code == 200
    jobs = r.json()["jobs"]
    assert any(j["id"] == "op:bulk-probe-tdb" for j in jobs), (
        f"v1.18.95: bulk_probe_tdb must appear; got {jobs}"
    )
    op_job = next(j for j in jobs if j["id"] == "op:bulk-probe-tdb")
    assert op_job["job_type"] == "bulk_probe_tdb"
    assert op_job["status"] == "running"
    assert op_job["is_op_progress"] is True
    # The activity summary must include the progress numbers + the
    # latest detail_json activity line so the user knows what's
    # happening at a glance.
    assert "330" in op_job["last_error"]
    assert "5,332" in op_job["last_error"]
    assert "alive=313" in op_job["last_error"]


def test_api_jobs_status_running_filter_includes_op_progress(
    admin_client, tmp_path,
):
    """?status=running filter must include op_progress rows
    whose status is 'running' or 'cancelling'."""
    db = _settings_db(tmp_path)
    _insert_op_progress(db, op_id="op-running", kind="bulk_lps",
                        status="running")
    _insert_op_progress(db, op_id="op-cancelling",
                        kind="tvdb_bridge", status="cancelling")
    _insert_op_progress(db, op_id="op-done",
                        kind="reprobe_plex_themes", status="done")

    r = admin_client.get("/api/jobs?status=running", headers=AUTH)
    assert r.status_code == 200
    ids = {j["id"] for j in r.json()["jobs"]}
    assert "op:op-running" in ids
    assert "op:op-cancelling" in ids, (
        "v1.18.95: 'cancelling' must surface under the 'running' "
        "filter — operationally still running"
    )
    assert "op:op-done" not in ids


def test_api_jobs_status_done_filter_excludes_running_op(
    admin_client, tmp_path,
):
    """?status=done filter must NOT include op_progress rows in
    'running' state. Each status maps cleanly."""
    db = _settings_db(tmp_path)
    _insert_op_progress(db, op_id="op-running",
                        kind="bulk_probe_tdb", status="running")
    _insert_op_progress(db, op_id="op-done",
                        kind="bulk_probe_tdb", status="done")
    r = admin_client.get("/api/jobs?status=done", headers=AUTH)
    ids = {j["id"] for j in r.json()["jobs"]}
    assert "op:op-running" not in ids
    assert "op:op-done" in ids


def test_api_jobs_status_all_includes_everything(
    admin_client, tmp_path,
):
    """?status=all (default) includes op_progress rows in every
    state alongside real jobs."""
    db = _settings_db(tmp_path)
    _insert_op_progress(db, op_id="op1", kind="bulk_probe_tdb",
                        status="running")
    _insert_op_progress(db, op_id="op2", kind="bulk_lps",
                        status="failed")
    real_id = _insert_job(db, job_type="download", status="running")

    r = admin_client.get("/api/jobs", headers=AUTH)
    jobs = r.json()["jobs"]
    ids = {j["id"] for j in jobs}
    assert "op:op1" in ids
    assert "op:op2" in ids
    assert real_id in ids, (
        f"real job id={real_id} must still appear; got {ids}"
    )


def test_api_jobs_synthetic_rows_sort_first(
    admin_client, tmp_path,
):
    """Live activity (op_progress) is most relevant — sort
    synthetic rows ahead of historical real jobs so the user
    sees what's happening at the top of the list."""
    db = _settings_db(tmp_path)
    # Insert several real jobs first.
    real_ids = [
        _insert_job(db, job_type="download", status="done")
        for _ in range(3)
    ]
    _insert_op_progress(db, op_id="live-op",
                        kind="bulk_probe_tdb", status="running")
    r = admin_client.get("/api/jobs", headers=AUTH)
    jobs = r.json()["jobs"]
    # First entry must be the op_progress row.
    assert jobs[0]["id"] == "op:live-op", (
        f"v1.18.95: op_progress rows must sort first; got "
        f"first={jobs[0]['id']}, full={[j['id'] for j in jobs]}"
    )


def test_api_jobs_limit_respected_with_synthesized_rows(
    admin_client, tmp_path,
):
    """A runaway op_progress backlog can't push real jobs off
    the page — combined response is capped at `limit`. Pre-fix
    a naive concat would have returned synthesized + limit real
    rows = more than `limit` total."""
    db = _settings_db(tmp_path)
    for i in range(5):
        _insert_op_progress(
            db, op_id=f"op{i}", kind="bulk_probe_tdb",
            status="running",
        )
    for _ in range(10):
        _insert_job(db, job_type="download", status="done")
    r = admin_client.get("/api/jobs?limit=7", headers=AUTH)
    jobs = r.json()["jobs"]
    assert len(jobs) <= 7, (
        f"v1.18.95: limit=7 must cap combined response; got "
        f"{len(jobs)} rows"
    )


def test_api_jobs_empty_op_progress_is_no_regression(
    admin_client, tmp_path,
):
    """With zero op_progress rows, /api/jobs returns the same
    shape as pre-v1.18.95 — just the real jobs."""
    db = _settings_db(tmp_path)
    real_id = _insert_job(db, job_type="download", status="running")
    r = admin_client.get("/api/jobs", headers=AUTH)
    jobs = r.json()["jobs"]
    assert len(jobs) == 1
    assert jobs[0]["id"] == real_id
    assert "is_op_progress" not in jobs[0] \
        or not jobs[0].get("is_op_progress")


def test_api_jobs_progress_summary_format(
    admin_client, tmp_path,
):
    """The activity summary in last_error must combine
    stage_label + counters + latest detail_json activity so
    the user gets a single-line snapshot of what's happening."""
    db = _settings_db(tmp_path)
    _insert_op_progress(
        db, op_id="op", kind="bulk_probe_tdb", status="running",
        stage_label="Probing TDB URLs (all rows)",
        stage_current=100, stage_total=1000,
        activity_messages=["alive=80, dead=2"],
    )
    r = admin_client.get("/api/jobs", headers=AUTH)
    job = r.json()["jobs"][0]
    summary = job["last_error"]
    # All three pieces present.
    assert "Probing TDB URLs (all rows)" in summary
    assert "100" in summary and "1,000" in summary
    assert "10%" in summary  # 100/1000
    assert "alive=80, dead=2" in summary


def test_api_jobs_no_total_renders_processed_only(
    admin_client, tmp_path,
):
    """Some ops don't have a known stage_total upfront (e.g.,
    early in a scan). The summary shows 'N processed' instead
    of '0/0 (0%)' to avoid the dividing-by-zero ugliness."""
    db = _settings_db(tmp_path)
    _insert_op_progress(
        db, op_id="op", kind="bulk_lps", status="running",
        stage_label="Scanning", stage_current=42, stage_total=0,
    )
    r = admin_client.get("/api/jobs", headers=AUTH)
    summary = r.json()["jobs"][0]["last_error"]
    assert "42 processed" in summary
    assert "0%" not in summary
    assert "0/0" not in summary


def test_api_jobs_cancelling_op_status_mapped_to_running(
    admin_client, tmp_path,
):
    """op_progress.status='cancelling' is op_progress-only;
    the synthesized status must map to 'running' so the
    frontend's stateLevel switch (which only knows the
    jobs.status vocab) renders consistently."""
    db = _settings_db(tmp_path)
    _insert_op_progress(db, op_id="op", kind="bulk_probe_tdb",
                        status="cancelling")
    r = admin_client.get("/api/jobs", headers=AUTH)
    job = r.json()["jobs"][0]
    assert job["status"] == "running"
    # The cancel_requested flag is set so the frontend has the
    # raw signal if it ever wants to display "CANCELLING…".
    assert job["cancel_requested"] == 1


def test_api_jobs_handles_missing_detail_json(
    admin_client, tmp_path,
):
    """detail_json can be NULL on freshly-started ops before
    the first update_progress call lands. Summary must still
    render — just without the activity tail."""
    db = _settings_db(tmp_path)
    with sqlite3.connect(_settings_db(tmp_path)) as conn:
        conn.execute(
            "INSERT INTO op_progress "
            "  (op_id, kind, status, started_at, updated_at, "
            "   stage_label, stage_current, stage_total, "
            "   processed_total, error_count) "
            "VALUES ('op', 'bulk_probe_tdb', 'running', ?, ?, "
            "        'Starting', 0, 0, 0, 0)",
            (_now_iso(), _now_iso()),
        )
        conn.commit()
    r = admin_client.get("/api/jobs", headers=AUTH)
    assert r.status_code == 200, r.text
    job = r.json()["jobs"][0]
    assert "Starting" in job["last_error"]
