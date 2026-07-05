"""v0.51.72 — the sync-probe endpoint no longer NameErrors on httpx.

Found while wiring the CI lint gate (ruff F821). api_sync_probe's nested
_probe_remote / _probe_database call httpx.Client(...), but httpx was never
imported in that function or at module level (it's imported LOCALLY inside 6
OTHER functions in api.py). So the settings-page "test connection" for the
remote + database sync transports raised NameError, swallowed by the call-site
`except Exception` into a bogus `{"ok": false, "error": "NameError: name
'httpx' is not defined"}` — the probe reported the transport as unreachable
even when it was fine (CLAUDE.md silent-defensive-catch bug class). The git
transport was unaffected (it imports dulwich locally).

Behavioral: probe against a dead local port. Before the fix the error is a
NameError; after, it's an ordinary connection error. The discriminator is
"NameError / not defined must NOT appear in the probe error".
"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

AUTH = {"X-Authentik-Username": "testadmin"}
# discard/closed port → httpx connects-refused fast, no real network needed.
DEAD = "http://127.0.0.1:9/"


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web.api import create_app
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    return TestClient(create_app(s))


@pytest.mark.parametrize("source", ["remote", "database"])
def test_probe_does_not_nameerror_on_httpx(client, source):
    r = client.post(f"/api/sync/probe?source={source}&url={DEAD}", headers=AUTH)
    assert r.status_code == 200, r.text
    body = r.json()
    # The probe will FAIL (dead port) — that's fine. What must NOT happen is a
    # NameError from the missing httpx import.
    err = (body.get("error") or "") + (body.get("detail") or "")
    assert "NameError" not in err and "not defined" not in err, (
        f"v0.51.72: {source} probe NameError'd on the missing httpx import: {body!r}")


def test_git_probe_unaffected(client):
    # sanity: the git transport (dulwich, imported locally) was never broken.
    r = client.post(f"/api/sync/probe?source=git&url={DEAD}", headers=AUTH)
    assert r.status_code == 200
    assert "NameError" not in (r.json().get("error") or "")
