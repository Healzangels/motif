"""v1.21.20 — orphan scan (+ cloud-backup dry-run) must not freeze the UI.

the user: "running an orphan scan locked up motif." Root cause: the
GET /api/admin/orphan-scan handler is `async def` but called
scan_plex_upload_placements() INLINE — and that function does synchronous
per-row Plex /themes HTTP + per-row DB work. Awaiting it inline blocked
the single uvicorn event loop for the entire (per-row-network) scan, so
every other request stalled → the whole UI appeared frozen.

Fix: offload the blocking scan to the threadpool (run_in_threadpool). The
sibling POST /api/admin/cloud-themes-backup-dry-run had the identical
shape (identify_c1_rows walks has_theme=1 rows hitting /themes per row,
holding a DB conn the whole walk) and got the same fix.

Behavioral discriminator: a function offloaded to the threadpool runs in
a worker thread with NO running asyncio loop, so asyncio.get_running_loop()
raises there. If it had run inline on the event loop, that call would
succeed. We assert the scan ran OFF the loop.
"""
from __future__ import annotations

import asyncio
from pathlib import Path

from fastapi.testclient import TestClient

from app.core.auth import create_admin, init_auth_schema
from app.core.db import init_db


REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()
FWD_HDR = {"X-Authentik-Username": "testadmin"}


def _ran_on_event_loop() -> bool:
    """True iff called from a thread with a running asyncio loop (i.e. it
    ran INLINE on the event loop, not offloaded to the threadpool)."""
    try:
        asyncio.get_running_loop()
        return True
    except RuntimeError:
        return False


def _make_app(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    # both endpoints 503 unless Plex is enabled + a token is set
    monkeypatch.setenv("MOTIF_PLEX_ENABLED", "true")
    monkeypatch.setenv("MOTIF_PLEX_TOKEN", "testtoken")
    monkeypatch.setenv("MOTIF_PLEX_URL", "http://plex.test:32400")
    themes = tmp_path / "themes"
    themes.mkdir(exist_ok=True)
    monkeypatch.setenv("MOTIF_THEMES_DIR", str(themes))
    from app.config import Settings
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(settings.db_path)
    init_auth_schema(settings.db_path)
    create_admin(settings.db_path, username="testadmin", password="testpassword")
    from app.web.api import create_app
    return TestClient(create_app(settings)), settings


# NOTE: the orphan-scan-specific off-loop test was removed in v1.21.24 —
# the synchronous GET it exercised was replaced by a background-thread op
# (POST /orphan-scan/start), so the off-the-event-loop property is now
# structural rather than a run_in_threadpool offload. See
# test_v1_21_24_orphan_scan_background_op.py for the new flow's coverage.


def test_cloud_backup_dry_run_runs_off_the_event_loop(tmp_path, monkeypatch):
    captured = {}

    def fake_identify(*a, **k):
        captured["on_loop"] = _ran_on_event_loop()
        return []

    import app.core.cloud_theme_backup as ctb
    monkeypatch.setattr(ctb, "identify_c1_rows", fake_identify)

    client, _ = _make_app(tmp_path, monkeypatch)
    r = client.post("/api/admin/cloud-themes-backup-dry-run", json={},
                    headers=FWD_HDR)
    assert r.status_code == 200, r.text
    assert captured.get("on_loop") is False


def test_cloud_backup_dry_run_offloads_via_threadpool_sourcepin():
    # Guard against a future regression that drops the offload. (The orphan
    # scan's own offload became a background thread in v1.21.24.)
    assert "targets = await run_in_threadpool(_run_dry_run)" in API_PY


def test_version_bumped():
    assert '__version__ = "0.' in (REPO / "app" / "__init__.py").read_text()
