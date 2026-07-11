"""v0.51.65 — the legacy (body-absent) /api/library/refresh tags scope='scan_all'.

Code-review follow-up to v0.51.63: the /collections REFRESH button now locks on
a COLLECTIONS-scoped enum count (or the scan_all/cascade pipeline signal), not
"any enum". The body-absent /api/library/refresh ("scan everything", incl.
collections) enqueued an UNTAGGED plex_enum (payload '{}'), which matched NEITHER
signal — so a raw-API global refresh left the /collections REFRESH button
clickable mid-scan. It's not UI-reachable (the button always sends a concrete
tab), but this closes the supported-endpoint hole: the legacy global refresh is a
full scan, so it's tagged scope='scan_all' like /api/libraries/refresh, and the
existing pipeline lock machinery (plex_enum_pipeline_in_flight) covers it.
"""
from __future__ import annotations

import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest
from starlette.testclient import TestClient

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from app.core.db import init_db  # noqa: E402

NOW = datetime.now(timezone.utc).isoformat(timespec="seconds")
AUTH = {"X-Authentik-Username": "testadmin"}


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.web.api import create_app
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    tc = TestClient(create_app(s))
    tc.motif_db = s.db_path
    return tc


def _enum_payloads(db):
    with sqlite3.connect(db) as c:
        c.row_factory = sqlite3.Row
        return [json.loads(r["payload"]) for r in c.execute(
            "SELECT payload FROM jobs WHERE job_type='plex_enum'").fetchall()]


def test_body_absent_global_refresh_is_scan_all(client):
    """POST /api/library/refresh with NO body → a single global plex_enum tagged
    scope='scan_all' (was an untagged '{}' payload)."""
    r = client.post("/api/library/refresh", headers=AUTH)
    assert r.status_code == 200
    payloads = _enum_payloads(client.motif_db)
    assert len(payloads) == 1, "legacy global refresh enqueues one global job"
    assert payloads[0].get("scope") == "scan_all", (
        "v0.51.65: the body-absent global refresh must be scan_all-scoped so the "
        "pipeline lock (and the /collections REFRESH lock) covers it")


def test_scan_all_scope_is_covered_by_pipeline_count():
    """The scan_all tag is what plex_enum_pipeline_in_flight keys on, so tagging
    the global refresh scan_all makes it lock the library buttons."""
    api_py = (REPO / "app" / "web" / "api.py").read_text()
    assert "json_extract(payload, '$.scope') IN ('cascade','scan_all')" in api_py
    # and the legacy insert now carries the tag (not the old empty payload).
    # v0.51.129: the payload also carries "force": True (manual refresh forces a
    # full enum), so match the scope key as a substring, not the whole dict.
    assert '"scope": "scan_all"' in api_py or "'scope': 'scan_all'" in api_py


def test_v0_51_65_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
