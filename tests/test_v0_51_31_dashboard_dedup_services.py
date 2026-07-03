"""v0.51.31 — dashboard de-dup (// COVERAGE COMPARISON removed) + SERVICES
enrichment (ThemerrDB + TMDB).

the user: "reducing some of the charts on the dashboard, seems a lot of them
now have a lot of duplicate information ... COVERAGE COMPARISON ... any other
services worth tracking that motif is using or built off of lets add that to
// SERVICES".

Part 1 — the // COVERAGE COMPARISON block (renderCoverageComparison) is gone.
It fed the SAME /api/sections/coverage per-section themed/unthemed split the
// PER-SECTION COVERAGE table (renderSectionCoverage) already renders, so it
was a pure duplicate. Removed the template section, the JS fn + its call, and
the now-dead .coverage-row / .coverage-bar CSS (their sole builder).

Part 2 — SERVICES gained ThemerrDB (always shown — the upstream catalogue) and
TMDB (only carded when a key is configured — optional orphan resolution). Both
mirror the Plex short-timeout reachability probe, run off the event loop, and
keep credentials server-side.
"""
from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core.db import init_db

AUTH = {"X-Authentik-Username": "testadmin"}
REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()
DASH = (REPO / "app" / "web" / "templates" / "dashboard.html").read_text()


# ── Part 1: // COVERAGE COMPARISON fully removed ────────────────────────

def test_coverage_comparison_template_section_removed():
    assert 'id="coverage-comparison-block"' not in DASH
    assert 'id="coverage-comparison-body"' not in DASH
    # the rendered heading element is gone (the removal-note comment may still
    # name the block — assert on the <h2 block-title>, not the bare phrase).
    assert "<h2 class=\"block-title\">// COVERAGE COMPARISON</h2>" not in DASH


def test_coverage_comparison_js_removed():
    # the render fn, its call site, and the cache-key var are all gone.
    assert "function renderCoverageComparison" not in APP_JS
    assert "renderCoverageComparison(" not in APP_JS
    assert "_lastCoverageComparisonKey" not in APP_JS
    assert "coverage-comparison-body" not in APP_JS


def test_coverage_comparison_dead_css_removed():
    # .coverage-row* / .coverage-bar* were built ONLY by the removed fn.
    assert ".coverage-comparison-body {" not in APP_CSS
    assert ".coverage-row {" not in APP_CSS
    assert ".coverage-bar {" not in APP_CSS
    assert ".coverage-bar-seg" not in APP_CSS


def test_per_section_coverage_table_survives():
    # the de-dup keeps the // PER-SECTION COVERAGE table (its own classes).
    assert "function renderSectionCoverage" in APP_JS
    assert ".section-coverage-row {" in APP_CSS
    assert 'id="section-coverage-body"' in DASH


# ── Part 2: SERVICES backend adds ThemerrDB + TMDB ──────────────────────

def test_backend_probes_themerrdb_and_tmdb():
    # both new dicts are built + returned, mirroring the Plex probe shape.
    assert '"themerrdb": tdb' in API_PY and '"tmdb": tmdb' in API_PY
    # ThemerrDB reachability = the git source's smart-HTTP refs endpoint.
    assert "settings.sync_git_url" in API_PY
    assert "git-upload-pack" in API_PY
    assert '"source": settings.sync_source' in API_PY
    # TMDB is only probed when a key is configured (no key → no network hit).
    assert "settings.tmdb_api_key" in API_PY
    assert "api.themoviedb.org/3/configuration" in API_PY


def test_js_renders_themerrdb_and_tmdb_cards():
    idx = APP_JS.index("async function loadServices()")
    body = APP_JS[idx:idx + 2500]
    assert "data.themerrdb" in body and "'ThemerrDB'" in body
    # TMDB card is gated on `configured` so an absent key isn't a red chip.
    assert "data.tmdb" in body and "'TMDB'" in body
    assert "if (tmdb.configured)" in body


# ── Part 2: behavioral — endpoint shape, no network ─────────────────────

@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    # Empty the ThemerrDB git URL so the reachability probe is skipped —
    # keeps the test network-free (like the plex-unconfigured v1.24.53 test).
    monkeypatch.setenv("MOTIF_DB_GIT_URL", "")
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.web.api import create_app
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    return TestClient(create_app(s))


def test_services_reports_themerrdb_and_tmdb_keys(client):
    data = client.get("/api/services", headers=AUTH).json()
    tdb = data["themerrdb"]
    # git_url emptied → not configured, probe skipped, no network / latency.
    assert tdb["configured"] is False
    assert tdb["online"] is False
    assert tdb["latency_ms"] is None
    assert isinstance(tdb["source"], str)  # active transport still reported
    tmdb = data["tmdb"]
    # no api key in the test settings → not configured, no probe.
    assert tmdb["configured"] is False
    assert tmdb["online"] is False
    assert tmdb["latency_ms"] is None
