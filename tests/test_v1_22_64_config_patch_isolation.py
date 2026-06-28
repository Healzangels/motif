"""v1.22.64 (audit round 2, Batch A #6) — a rejected config PATCH
poisoned the live config.

PATCH /api/config did `cfg = settings.cfg` (the LIVE object — Settings
returns its internal _cfg by reference) and `_apply_partial_config`
mutates in place, raising mid-loop on the first bad field. A rejected
PATCH (unknown field / type mismatch / critical validation) therefore
left every field applied BEFORE the failure live in the running
process despite the 400 — and the next unrelated successful save
persisted those rejected values to motif.yaml permanently.

Fix: the partial is applied to a copy.deepcopy of the config; the
live object changes only via save()+reload() on the success path.
"""
from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()
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
    init_db(settings.db_path)
    init_auth_schema(settings.db_path)
    create_admin(settings.db_path, username="testadmin",
                 password="testpassword")
    return TestClient(create_app(settings)), settings


def test_rejected_patch_leaves_live_config_untouched(admin_client):
    """A body whose FIRST field is valid and SECOND is unknown must 400
    AND leave the first field's live value unchanged (pre-fix the valid
    field was already applied to the live object when the loop raised)."""
    client, settings = admin_client
    before = settings.cfg.downloads.concurrency
    r = client.patch("/api/config", headers=AUTH, json={
        "downloads": {"concurrency": before + 3, "bogus_field": 1},
    })
    assert r.status_code == 400
    assert settings.cfg.downloads.concurrency == before, (
        "v1.22.64: a rejected PATCH must not mutate the live config — "
        f"concurrency leaked to {settings.cfg.downloads.concurrency}"
    )
    # And the next read endpoint agrees.
    j = client.get("/api/config", headers=AUTH).json()
    assert j["config"]["downloads"]["concurrency"] == before


def test_rejected_patch_never_persists_via_later_save(admin_client):
    """The poisoned-then-persisted shape: rejected PATCH followed by an
    unrelated successful PATCH must not write the rejected value to
    disk."""
    client, settings = admin_client
    before = settings.cfg.downloads.concurrency
    client.patch("/api/config", headers=AUTH, json={
        "downloads": {"concurrency": before + 3, "bogus_field": 1},
    })
    r2 = client.patch("/api/config", headers=AUTH, json={
        "matching": {},
    })
    assert r2.status_code == 200, r2.text
    # Reload from disk — the rejected concurrency must not have ridden
    # along with the unrelated save.
    settings.reload()
    assert settings.cfg.downloads.concurrency == before, (
        "v1.22.64: the rejected value must not be persisted by a later "
        "unrelated save"
    )


def test_successful_patch_still_applies_and_persists(admin_client):
    """Regression lock: the success path still works end-to-end."""
    client, settings = admin_client
    before = settings.cfg.downloads.concurrency
    new = before + 1
    r = client.patch("/api/config", headers=AUTH, json={
        "downloads": {"concurrency": new},
    })
    assert r.status_code == 200, r.text
    assert settings.cfg.downloads.concurrency == new
    settings.reload()
    assert settings.cfg.downloads.concurrency == new


def test_patch_handler_uses_deepcopy():
    """Source pin: the handler applies to a deep copy, not settings.cfg
    directly."""
    i = API_PY.index("async def api_patch_config(")
    body = API_PY[i:API_PY.index("\n    @app.", i + 1)]
    assert "cfg = copy.deepcopy(settings.cfg)" in body
    assert "cfg = settings.cfg\n" not in body
