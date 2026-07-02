"""v0.51.13 — round-4 audit Batch C (API robustness).

#10: cleanup-dead-rk deletes only on a DEFINITIVE 404 — a transport error /
  5xx from Plex (get_themes ok=False, http_status None) must refuse, not fall
  through to the placement DELETE (a scan during a Plex restart flags LIVE
  plex_upload placements as plex_fetch_failed).
#11: api_upload_theme reads the multipart body in capped chunks so an
  oversized upload is rejected before it's fully resident (the v1.23.18
  OOM-before-cap class, missed at this endpoint).
#12: the PURGE/UNPLACE/UNMANAGE/DELETE sidecar/canonical unlink loops are
  offloaded via run_in_threadpool — they hit the /data + themes mounts and
  froze the event loop on disk spin-up (class 12, invisible to the v1.22.58
  AST lint which covers network/subprocess only).
#27: PUT /api/dashboard/layout guards its JSON parse (400, not raw 500) —
  the last bare `await request.json()` in the file.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core.db import init_db

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
    from app.web.api import create_app
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    db = settings.db_path
    init_db(db)
    init_auth_schema(db)
    create_admin(db, username="testadmin", password="testpassword")
    return TestClient(create_app(settings)), db


# ── #27: malformed layout body → clean 400 ──────────────────────────────

def test_layout_put_malformed_body_is_400(admin_client):
    client, _ = admin_client
    r = client.put("/api/dashboard/layout", content=b"{not json",
                   headers={**AUTH, "Content-Type": "application/json"})
    assert r.status_code == 400, r.text
    assert "invalid JSON body" in r.json()["detail"]


def test_layout_put_valid_body_still_works(admin_client):
    client, _ = admin_client
    r = client.put("/api/dashboard/layout",
                   json={"sections": [{"id": "coverage", "hidden": False}]},
                   headers=AUTH)
    assert r.status_code == 200, r.text
    assert r.json()["ok"] is True


# ── #10: cleanup-dead-rk needs a definitive 404 ─────────────────────────

def test_cleanup_dead_rk_requires_definitive_404():
    i = API_PY.index("async def api_admin_orphan_cleanup_dead_rk")
    body = API_PY[i:i + 5000]
    assert 'resp.get("http_status") != 404' in body, (
        "transport errors (http_status=None) / 5xx must refuse the DELETE")
    # ordering: the indeterminate refusal sits BEFORE the placement DELETE.
    assert body.index('http_status") != 404') < body.index("DELETE FROM placements")


# ── #11: upload reads in capped chunks ──────────────────────────────────

def test_upload_theme_reads_in_capped_chunks():
    i = API_PY.index("async def api_upload_theme")
    body = API_PY[i:i + 4000]
    assert "await upload.read(4 * 1024 * 1024)" in body, (
        "chunked read with a running cap (v1.23.18 pattern)")
    assert "if len(_buf) > _CAP:" in body
    # the old unbounded read must be gone from this handler.
    assert "data = await upload.read()\n" not in body


def test_no_unbounded_upload_read_anywhere():
    # repo-wide: every multipart read is chunked now (this + the v1.23.18
    # restore endpoint were the only two upload.read() sites).
    assert not re.search(r"await upload\.read\(\)\s*\n", API_PY), (
        "an unbounded `await upload.read()` re-appeared — use the chunked "
        "running-cap pattern (v1.23.18 / v0.51.13)")


# ── #12: destructive-handler unlink loops are offloaded ─────────────────

@pytest.mark.parametrize("handler,fn_name", [
    ("async def api_unplace_item", "_unlink_sidecars"),
    ("async def api_forget_item", "_unlink_files"),
    ("async def api_unmanage_item", "_unlink_canonicals"),
    ("async def api_delete_item", "_unlink_files"),
])
def test_destructive_fs_loops_offloaded(handler, fn_name):
    i = API_PY.index(handler)
    # slice to the next handler declaration.
    j = API_PY.index("\n    @app.", i + 10)
    body = API_PY[i:j]
    assert f"def {fn_name}(" in body, f"{handler}: nested offload target missing"
    assert f"await run_in_threadpool({fn_name})" in body, (
        f"{handler}: the unlink loop must run off the event loop (class 12)")
    # no direct-body unlink loop remains: every p.unlink() in the handler
    # must live inside the nested def (i.e. indented deeper than 8 spaces)
    # — except none should be at handler depth.
    for m in re.finditer(r"\n(\s+)\S[^\n]*\.unlink\(\)", body):
        assert len(m.group(1)) > 8, (
            f"{handler}: a handler-depth unlink remains at offset {m.start()}")
