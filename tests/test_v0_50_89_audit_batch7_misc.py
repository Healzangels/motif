"""v0.50.89 — holistic-audit Batch 7: remaining API/JS + misc LOW findings.

Of the 8 originally-listed findings, 4 were actionable, 1 was a moot
dead-code surface (removed), and 2 were dropped as non-bugs:

  ACTIONED:
  #2 app.js bulk-ADOPT click-handler predicate used a bare `!it.media_folder`
     that treated a plex_upload row (media_folder='') as unplaced, pulling it
     into the adopt candidate set. Widened to mirror the adoptOnlyCount bucket.
  #4 api_decide_finding / api_decide_findings_bulk parsed request.json() with
     no guard → a malformed body raised JSONDecodeError → HTTP 500. Now 400.
  #8 scheduler jobs had no misfire_grace_time (APScheduler default 1s), so a
     cron straddled by a container restart was silently skipped. job_defaults
     now sets misfire_grace_time=3600 + coalesce=True for every job.

  MOOT → REMOVED:
  #3 the /scans-page client JS (scansState/loadScansList/loadFindings/
     bindScans …) was orphaned dead code — no template hosts the #scan-*
     elements and there's no /scans route, so bindScans() always
     early-returned. Removed the whole surface (the live /api/scans*
     endpoints stay).

  DROPPED (verified non-bugs):
  #1 _import_row_current_state already classifies plex_upload rows correctly
     (they carry a placements row, so land in the placed T/A branch).
  #5 api_admin_test_cookies' Path.exists/stat/read_text are sub-ms local FS
     ops, not the network round-trips the class-12 lint targets.
  #6 downloader truncation guard scope + #7 tmdb confidence: documented as
     deliberate tradeoffs in-code (comment-only), no behavioral change.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()
SCHED_PY = (REPO / "app" / "core" / "scheduler.py").read_text()


# ── #2: bulk-ADOPT click-handler mirrors the widened `placed` predicate ──


def test_bulk_adopt_candidate_filter_uses_widened_placed():
    """The ADOPT scan loop must compute the same widened `placed` the
    adoptOnlyCount bucket uses (plex_upload counts as placed), not a bare
    `!it.media_folder` that mis-includes plex_upload rows."""
    anchor = APP_JS.index("// sidecar-only state: no placement + a sidecar exists.")
    block = APP_JS[anchor:anchor + 600]
    assert "it.placement_kind === 'plex_upload'" in block, (
        "v0.50.89: the adopt candidate filter must treat plex_upload as placed"
    )
    # the bare un-widened form must no longer gate the push
    assert "if (!it.media_folder && !!it.plex_local_theme)" not in APP_JS, (
        "the un-widened predicate must be gone"
    )


# ── #3: orphaned scans client surface removed ───────────────────────────


def test_scans_client_surface_removed():
    for sym in ("function loadScansList", "function bindScans",
                "function loadFindings", "let scansState"):
        assert sym not in APP_JS, f"dead scans symbol still present: {sym}"
    assert "bindScans();" not in APP_JS, "dead bindScans() call still wired"


def test_scans_server_endpoints_retained():
    """Removal was client-only — the live JSON endpoints must remain."""
    assert "/api/scans" in API_PY


# ── #4: decide-finding handlers return 400 (not 500) on bad JSON ────────


def test_decide_finding_guards_json_parse():
    for fn in ("async def api_decide_finding(",
               "async def api_decide_findings_bulk("):
        i = API_PY.index(fn)
        body = API_PY[i:i + 900]
        assert "await request.json()" in body
        assert "except Exception as e:" in body, (
            f"{fn} must guard the JSON parse"
        )
        assert "status_code=400" in body


def test_decide_finding_bad_json_yields_400(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web.api import create_app
    from fastapi.testclient import TestClient

    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(settings.db_path)
    init_auth_schema(settings.db_path)
    create_admin(settings.db_path, username="a", password="pw12345678")
    client = TestClient(create_app(settings))
    hdr = {"X-Authentik-Username": "a"}

    r = client.post("/api/scans/findings/1/decision",
                    content=b"{not valid json", headers=hdr)
    assert r.status_code == 400, f"expected 400, got {r.status_code}: {r.text}"

    r2 = client.post("/api/scans/findings/decisions/bulk",
                     content=b"@@@", headers=hdr)
    assert r2.status_code == 400, f"expected 400, got {r2.status_code}: {r2.text}"


# ── #8: scheduler sets misfire_grace_time + coalesce for all jobs ───────


def test_scheduler_sets_misfire_grace_and_coalesce():
    i = SCHED_PY.index("BackgroundScheduler(")
    block = SCHED_PY[i:i + 400]
    assert "job_defaults=" in block
    assert "misfire_grace_time" in block
    assert "coalesce" in block


def test_scheduler_job_defaults_applied_at_construction(tmp_path, monkeypatch):
    """Constructing the scheduler must actually carry the grace/coalesce
    defaults through to APScheduler's job_defaults (not just appear in
    source)."""
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.db import init_db
    from app.core.scheduler import start_scheduler

    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(settings.db_path)
    sched = start_scheduler(settings)
    try:
        jd = sched._job_defaults
        assert jd.get("misfire_grace_time") == 3600
        assert jd.get("coalesce") is True
    finally:
        sched.shutdown(wait=False)
