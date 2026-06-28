"""v1.22.59 — orphans page: per-item re-probe instead of full rescan.

the user: "The Orphan Scan reprobe action makes it do a full rescan; is it
possible that reprobe only checks the specific item being reprobed."

Pre-fix the per-row // PROBE button (and the post-action refresh after
RE-PUSH / LET PLEX SERVE / PURGE / DELETE SIDECAR) called runScan() —
re-walking EVERY plex_upload placement with a Plex /themes round-trip
each, so every row action cost a full-library sweep.

Fix, three layers:
  * core: the per-placement classification body extracted to
    scan_one_placement(); scan_plex_upload_placements loops over it —
    one classification implementation, two entry points.
  * api: POST /api/admin/orphan-scan/item re-probes one placement off
    the event loop (class 12), patches the cached full-scan state in
    place, and returns placement_gone=True when the placement no longer
    exists (PURGE / LPS removed it) so the page drops the row.
  * orphans.html: PROBE + every post-action refresh call reprobeRow()
    (per-item endpoint + in-place row patch + client-side chip
    recompute) instead of runScan().
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from app.core.auth import create_admin, init_auth_schema
from app.core.db import get_conn, init_db, transaction
import app.web.api as apimod

from test_v1_18_37_orphan_scan import _seed_placement, _seed_local_file

REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()
ORPHANS_HTML = (REPO / "app" / "web" / "templates" / "orphans.html").read_text()
FWD_HDR = {"X-Authentik-Username": "testadmin"}


@pytest.fixture(autouse=True)
def _reset_orphan_state():
    apimod._ORPHAN_SCAN_STATE.clear()
    apimod._ORPHAN_SCAN_STATE.update(status="idle")
    yield
    apimod._ORPHAN_SCAN_STATE.clear()
    apimod._ORPHAN_SCAN_STATE.update(status="idle")


# ── core: extraction equivalence ─────────────────────────────


def test_full_scan_delegates_to_scan_one_placement(tmp_path):
    """The full scan and the single-item entry point produce the SAME
    finding for the same placement — one classification implementation."""
    from app.core.orphan_scan import (
        scan_one_placement, scan_plex_upload_placements,
    )
    db = tmp_path / "motif.db"
    init_db(db)
    _seed_placement(db, media_type="movie", tmdb_id=77, section_id="1")
    # rk_lookup_failed path needs no Plex mock behavior at all.
    with get_conn(db) as conn, transaction(conn):
        conn.execute("DELETE FROM plex_items WHERE rating_key = ?",
                     ("rk-77",))
    plex = MagicMock()
    full = scan_plex_upload_placements(db, plex)
    single = scan_one_placement(db, plex, "movie", 77, "1")
    assert len(full) == 1
    assert single == full[0]
    assert single["drift_type"] == "rk_lookup_failed"


def test_scan_one_placement_ok_path(tmp_path):
    """Single-item probe classifies a healthy row `ok` — exercising the
    Plex /themes + hash-match path through the extracted function."""
    import hashlib
    from app.core.orphan_scan import scan_one_placement
    db = tmp_path / "motif.db"
    init_db(db)
    _seed_placement(db, media_type="movie", tmdb_id=88, section_id="1")
    audio = tmp_path / "theme.mp3"
    audio.write_bytes(b"\x00\x01\x02" * 1000)
    _seed_local_file(db, media_type="movie", tmdb_id=88, section_id="1",
                     file_path=audio)
    expected = hashlib.sha1(audio.read_bytes()).hexdigest()
    plex = MagicMock()
    plex.get_themes.return_value = {
        "ok": True, "http_status": 200, "error": None,
        "body": {"MediaContainer": {"size": 1, "Metadata": [
            {"ratingKey": f"upload://themes/{expected}", "selected": True},
        ]}},
    }
    finding = scan_one_placement(db, plex, "movie", 88, "1")
    assert finding["drift_type"] == "ok"
    plex.get_themes.assert_called_once_with(rating_key="rk-88")


# ── endpoint behavior ────────────────────────────────────────


def _make_app(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("MOTIF_PLEX_ENABLED", "true")
    monkeypatch.setenv("MOTIF_PLEX_TOKEN", "testtoken")
    monkeypatch.setenv("MOTIF_PLEX_URL", "http://plex.test:32400")
    from app.config import Settings
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(settings.db_path)
    init_auth_schema(settings.db_path)
    create_admin(settings.db_path, username="testadmin",
                 password="testpassword")
    return TestClient(apimod.create_app(settings)), settings


def _item_url(mt="movie", tid=77, sec="1"):
    return (f"/api/admin/orphan-scan/item?media_type={mt}"
            f"&tmdb_id={tid}&section_id={sec}")


def test_item_endpoint_placement_gone(tmp_path, monkeypatch):
    """No matching plex_upload placement → placement_gone (the page
    drops the row). Reached without any Plex traffic."""
    client, _ = _make_app(tmp_path, monkeypatch)
    r = client.post(_item_url(), headers=FWD_HDR)
    assert r.status_code == 200
    j = r.json()
    assert j["ok"] is True
    assert j["finding"] is None
    assert j["placement_gone"] is True


def test_item_endpoint_returns_single_finding(tmp_path, monkeypatch):
    """Seeded placement → the endpoint returns ITS finding (the canned
    scan_one_placement result) — no full-library walk."""
    calls = []

    def fake_scan_one(db, plex, mt, tid, sec, edition_key=None, themes_dir=None):
        calls.append((mt, tid, sec))
        return {"media_type": mt, "tmdb_id": tid, "section_id": sec,
                "title": "Test", "year": 2024, "rk": f"rk-{tid}",
                "drift_type": "ok", "details": "stub", "entries_count": 1,
                "motif_hash": None, "motif_canonical": None,
                "motif_entry_present": True, "motif_entry_selected": True,
                "any_selected": True, "orphan_sidecar_path": None,
                "plex_folder_path": None}

    import app.core.orphan_scan as oscan
    monkeypatch.setattr(oscan, "scan_one_placement", fake_scan_one)
    client, settings = _make_app(tmp_path, monkeypatch)
    _seed_placement(settings.db_path, media_type="movie", tmdb_id=77,
                    section_id="1")
    r = client.post(_item_url(), headers=FWD_HDR)
    assert r.status_code == 200
    j = r.json()
    assert j["placement_gone"] is False
    assert j["finding"]["drift_type"] == "ok"
    assert calls == [("movie", 77, "1")]


def test_item_endpoint_patches_cached_scan_state(tmp_path, monkeypatch):
    """A completed full-scan's cached findings get the re-probed row
    patched in place + the summary recomputed — so a page reload shows
    the fresh state without a new full scan."""
    def fake_scan_one(db, plex, mt, tid, sec, edition_key=None, themes_dir=None):
        return {"media_type": mt, "tmdb_id": tid, "section_id": sec,
                "title": "Test", "year": 2024, "rk": f"rk-{tid}",
                "drift_type": "ok", "details": "now healthy",
                "entries_count": 1, "motif_hash": None,
                "motif_canonical": None, "motif_entry_present": True,
                "motif_entry_selected": True, "any_selected": True,
                "orphan_sidecar_path": None, "plex_folder_path": None}

    import app.core.orphan_scan as oscan
    monkeypatch.setattr(oscan, "scan_one_placement", fake_scan_one)
    client, settings = _make_app(tmp_path, monkeypatch)
    _seed_placement(settings.db_path, media_type="movie", tmdb_id=77,
                    section_id="1")
    stale = {"media_type": "movie", "tmdb_id": 77, "section_id": "1",
             "drift_type": "nothing_selected"}
    other = {"media_type": "tv", "tmdb_id": 5, "section_id": "2",
             "drift_type": "ok"}
    apimod._ORPHAN_SCAN_STATE.update(
        status="done", findings=[stale, other],
        summary={"nothing_selected": 1, "ok": 1}, total=2, done=2,
    )
    r = client.post(_item_url(), headers=FWD_HDR)
    assert r.status_code == 200
    st = apimod._ORPHAN_SCAN_STATE
    assert len(st["findings"]) == 2
    patched = [f for f in st["findings"] if f["tmdb_id"] == 77][0]
    assert patched["drift_type"] == "ok"
    assert st["summary"] == {"ok": 2}


def test_item_endpoint_gone_drops_cached_row(tmp_path, monkeypatch):
    """placement_gone also removes the row from the cached findings +
    recomputes summary/total (the PURGE / LPS afterglow)."""
    client, _ = _make_app(tmp_path, monkeypatch)
    stale = {"media_type": "movie", "tmdb_id": 77, "section_id": "1",
             "drift_type": "rk_lookup_failed"}
    apimod._ORPHAN_SCAN_STATE.update(
        status="done", findings=[stale],
        summary={"rk_lookup_failed": 1}, total=1, done=1,
    )
    r = client.post(_item_url(), headers=FWD_HDR)
    assert r.json()["placement_gone"] is True
    st = apimod._ORPHAN_SCAN_STATE
    assert st["findings"] == []
    assert st["summary"] == {}
    assert st["total"] == 0


def test_item_endpoint_503_when_plex_disabled(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("MOTIF_PLEX_ENABLED", "false")
    from app.config import Settings
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(settings.db_path)
    init_auth_schema(settings.db_path)
    create_admin(settings.db_path, username="testadmin",
                 password="testpassword")
    client = TestClient(apimod.create_app(settings))
    r = client.post(_item_url(), headers=FWD_HDR)
    assert r.status_code == 503


def test_item_endpoint_runs_off_event_loop():
    """The Plex round-trip is offloaded (class 12) — also enforced
    globally by test_v1_22_58_async_no_blocking_calls."""
    idx = API_PY.index("async def api_admin_orphan_scan_item(")
    body = API_PY[idx:API_PY.index("@app.", idx + 1)]
    assert "await run_in_threadpool(_probe_one)" in body


# ── template wiring ──────────────────────────────────────────


def test_probe_button_carries_item_key():
    """The PROBE button needs mt/id/sec (the per-item endpoint's key) —
    the old data-rk-only shape forced the full-rescan fallback."""
    idx = ORPHANS_HTML.index('data-act="probe"')
    tag = ORPHANS_HTML[ORPHANS_HTML.rfind("<button", 0, idx):idx + 200]
    assert 'data-mt="' in tag and 'data-id="' in tag and 'data-sec="' in tag


def test_repush_button_carries_section():
    """RE-PUSH must carry data-sec too — without it the post-action
    re-probe keys on section_id='' and wrongly drops the row as
    placement_gone."""
    idx = ORPHANS_HTML.index('data-act="repush"')
    tag = ORPHANS_HTML[ORPHANS_HTML.rfind("<button", 0, idx):idx + 250]
    assert 'data-sec="' in tag


def test_probe_and_post_actions_use_reprobe_row():
    """PROBE + every post-action refresh go through reprobeRow (the
    per-item path); the full runScan survives ONLY on the RUN SCAN
    button + load-restore."""
    assert "async function reprobeRow(" in ORPHANS_HTML
    assert "/api/admin/orphan-scan/item" in ORPHANS_HTML
    # v1.22.81: reprobe threads the row edition.
    assert "await reprobeRow(mt, id, sec, ek)" in ORPHANS_HTML
    # The old full-rescan-after-action shape must be gone.
    assert "setTimeout(runScan, 1500)" not in ORPHANS_HTML
    # placement_gone drops the row client-side.
    assert "placement_gone" in ORPHANS_HTML
