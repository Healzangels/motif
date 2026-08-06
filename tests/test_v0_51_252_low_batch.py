"""v0.51.252 — cosmetic/efficiency LOW batch (review backlog closeout).

Five independent items, one tag:

1. /api/services WAN-probe TTL cache (300s) — the 30s dashboard poll
   re-probed ThemerrDB's git host + TMDB every tick (~5,760 outbound
   requests/day per open dashboard) for status that doesn't change
   per-tick. Plex stays fresh every call: it's LAN and the panel's
   headline signal.
2. loadDashboard's five GETs fire concurrently — first paint paid the
   SUM of five serial round-trips instead of the slowest one.
3. Boot Plex section discovery moved to a daemon thread — a slow/down
   Plex held the whole boot (UI included) while nothing at startup
   reads the refreshed rows synchronously.
4. renderMissing + the #movies-missing-body/#tv-missing-body writes
   removed — the elements left the templates long ago, so every write
   was behind an `if (el)` that never fired.
5. setBulkLabel guard — bulk-bar labels no longer ping-pong: a click
   handler owns its button while disabled (// PUSHING i/N + result
   flash), and the selection updater used to stamp the resting count
   label straight over it (bug-class 5).
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core.db import init_db

AUTH = {"X-Authentik-Username": "testadmin"}
REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
MAIN_PY = (REPO / "app" / "main.py").read_text()


# ── 1. WAN-probe TTL cache (behavioral) ──────────────────────────────────

class _FakeResp:
    status_code = 200

    def json(self):
        return {}


class _FakeHttpxClient:
    urls: list = []

    def __init__(self, *a, **k):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def get(self, url, *a, **k):
        _FakeHttpxClient.urls.append(url)
        return _FakeResp()


def test_wan_probes_cached_plex_stays_fresh(tmp_path, monkeypatch):
    """Two /api/services calls: the ThemerrDB probe fires ONCE (cached),
    the Plex probe fires BOTH times (fresh). Pre-fix the second call
    re-probed the WAN host too — this is the discriminator."""
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("MOTIF_PLEX_URL", "http://plex.test:32400")
    monkeypatch.setenv("MOTIF_PLEX_TOKEN", "fake-token")
    monkeypatch.setenv("MOTIF_DB_GIT_URL", "https://tdb.test/repo")
    monkeypatch.setattr("httpx.Client", _FakeHttpxClient)
    _FakeHttpxClient.urls = []
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.web.api import create_app
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    c = TestClient(create_app(s))

    first = c.get("/api/services", headers=AUTH).json()
    urls_after_first = list(_FakeHttpxClient.urls)
    second = c.get("/api/services", headers=AUTH).json()
    urls_after_second = list(_FakeHttpxClient.urls)

    assert first["themerrdb"]["online"] is True
    tdb_probes = [u for u in urls_after_second if "tdb.test" in u]
    plex_probes = [u for u in urls_after_second if "plex.test" in u]
    assert len(tdb_probes) == 1, (
        f"v0.51.252: the WAN probe must be served from the 300s cache on the "
        f"second call — got {len(tdb_probes)} probes ({urls_after_first} → "
        f"{urls_after_second})")
    assert len(plex_probes) == 2, (
        "Plex must be probed FRESH on every call — it's the panel's headline "
        f"signal; got {len(plex_probes)} probes")
    # The cached payload is the same answer, not a degraded shape.
    assert second["themerrdb"] == first["themerrdb"]
    assert second["tmdb"] == first["tmdb"]


# ── 2. dashboard GETs concurrent ─────────────────────────────────────────

def _load_dashboard_body() -> str:
    i = APP_JS.index("async function loadDashboard()")
    return APP_JS[i:APP_JS.index("\n  function ", i)]


def test_dashboard_gets_fire_concurrently():
    body = _load_dashboard_body()
    starts = [body.index(f"const {v} = api('GET', ")
              for v in ("_statsP", "_histP", "_secP", "_insP", "_evsP")]
    first_await = body.index("await _statsP")
    assert max(starts) < first_await, (
        "v0.51.252: all five GETs must be dispatched before the first await "
        "— a promise created after an await is serial again")
    # The old inline serial awaits are gone.
    for path in ("'/api/sync/history", "'/api/sections/coverage'",
                 "'/api/dashboard/insights'", "'/api/events?limit=20'"):
        assert f"await api('GET', {path}" not in body, (
            f"serial await re-introduced for {path}")
    # Each response still renders behind its own seq-token guard (the
    # v1.15.109 contract) — count stays at the five-await level.
    assert body.count("if (loadDashboard._seq !== _myToken) return;") >= 5


# ── 3. boot section discovery backgrounded ───────────────────────────────

def test_boot_section_discovery_runs_on_a_daemon_thread():
    i = MAIN_PY.index("def _discover_sections_at_boot()")
    seg = MAIN_PY[i:i + 2000]
    assert "refresh_sections(" in seg, (
        "the discovery body must live inside the nested thread target")
    assert re.search(
        r"threading\.Thread\(target=_discover_sections_at_boot,\s*daemon=True",
        MAIN_PY), (
        "v0.51.252: boot section discovery must run on a daemon thread — "
        "inline it blocks the whole boot on a slow/down Plex")


# ── 4. renderMissing dead code gone ──────────────────────────────────────

def test_render_missing_and_its_targets_are_gone():
    live = "\n".join(ln for ln in APP_JS.splitlines()
                     if not ln.lstrip().startswith("//"))
    assert "renderMissing" not in live, "dead renderMissing resurrected"
    assert "missing-body" not in live, (
        "a #*-missing-body write is back — no template renders the element")


# ── 5. bulk-bar labels route through the disabled guard ──────────────────

def test_bulk_labels_all_route_through_set_bulk_label():
    assert "function setBulkLabel(btn, text)" in APP_JS
    i = APP_JS.index("function setBulkLabel(btn, text)")
    assert "if (!btn.disabled)" in APP_JS[i:i + 200], (
        "the guard IS the fix: a disabled bulk button is handler-owned "
        "(// PUSHING i/N / result flash) — don't stamp the count label on it")
    fn = APP_JS.index("function updateLibrarySelectionUi()")
    body = APP_JS[fn:APP_JS.index("\n  function ", fn + 10)]
    direct = re.findall(r"\w+Btn\.textContent\s*=", body)
    assert not direct, (
        f"v0.51.252: bulk-bar label writes must go through setBulkLabel "
        f"(ping-pong guard) — direct writes found: {direct}")
    assert body.count("setBulkLabel(") >= 18, (
        "the guard helper lost call sites — labels are being written some "
        "other way or buttons were removed without updating this pin")


def test_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
