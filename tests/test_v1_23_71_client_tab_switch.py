"""v1.23.71 — client-side library tab switching (kills the full-page-reload lag).

The nav tab links are <a href> full navigations; each switch re-parsed the
~18k-line app.js + re-ran all init before loadLibrary fired (the ~1s lag; the
/api/library query itself is ≤160ms, measured v1.23.70). switchLibraryTab swaps
the tab in place: fetch the new tab's server-rendered HTML, swap only the per-tab
fragments (toolbar chips + legend), re-bind the swapped chips, re-hydrate
libraryState, loadLibrary(). Progressive enhancement — the <a href> still works
and ANY error falls back to a full navigation, so it can never regress nav.

No browser harness here, so these are source pins (the switcher + its safety net)
+ a behavioral check that the server renders the swap targets for every tab.
"""
from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
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
    create_admin(settings.db_path, username="testadmin", password="testpassword")
    return TestClient(create_app(settings))


# ── source pins: the switcher + re-callable chip binder exist ──


def test_switcher_functions_exist():
    assert "async function switchLibraryTab(" in APP_JS
    assert "function bindLibraryToolbarChips(" in APP_JS
    assert "function hydrateLibraryStateForTab(" in APP_JS
    # the chips are re-bound after the swap (they're direct-bound, not delegated).
    i = APP_JS.index("async function switchLibraryTab(")
    body = APP_JS[i:i + 4000]  # v0.50.92: widened for the pre-swap variant block
    assert "bindLibraryToolbarChips();" in body
    assert "replaceWith(newChips)" in body


def test_progressive_enhancement_fallback():
    # switchLibraryTab bails to a full nav for a non-library path.
    assert "window.location.href = href; return;" in APP_JS
    # the nav interceptor: library-page-only + plain-left-click-only, and a
    # switch failure falls back to a full navigation.
    i = APP_JS.index("closest('.nav a[data-nav]')")
    nav = APP_JS[i - 120:i + 1000]
    assert "getElementById('library-tab')" in nav, "only intercept on a library page"
    assert "e.metaKey" in nav and "e.button !== 0" in nav, "modified clicks pass through"
    assert "window.location.href = a.href" in nav, "switch failure → full nav"


def test_popstate_handles_back_forward():
    i = APP_JS.index("addEventListener('popstate'")
    body = APP_JS[i:i + 1000]
    assert "switchLibraryTab(location.pathname, false)" in body
    assert "location.reload()" in body, "deep-link target → full reload for fidelity"


# ── behavioral: the server renders the swap targets for every tab ──


def test_every_tab_exposes_the_swap_targets(admin_client):
    for tab in ("movies", "tv", "anime", "collections"):
        html = admin_client.get("/" + tab, headers=AUTH).text
        assert 'class="chips" role="tablist"' in html, f"{tab}: toolbar chips group"
        assert 'class="library-legend-body"' in html, f"{tab}: legend body"
        assert 'id="library-subtitle"' in html, f"{tab}: subtitle"
        assert 'id="library-tab"' in html, f"{tab}: hidden tab input"


# ── v1.23.73 code-review fixes ──


def test_highlightnav_clears_prior_active():
    # the in-place switch reuses the nav DOM, so highlightNav must remove the
    # prior tab's .active before adding (else every visited tab stays lit).
    i = APP_JS.index("function highlightNav(")
    body = APP_JS[i:i + 1300]
    assert "a.active').forEach" in body and "remove('active')" in body


def test_switch_has_inflight_seq_guard():
    i = APP_JS.index("async function switchLibraryTab(")
    body = APP_JS[i:i + 2600]
    assert "switchLibraryTab._seq" in body
    assert "if (switchLibraryTab._seq !== myseq) return" in body, (
        "a newer switch must supersede an older in-flight one"
    )
    # the swap selector targets the toolbar tablist (has aria-label), not the
    # filter-status tablist.
    assert '.chips[role="tablist"][aria-label]' in body


def test_same_tab_click_is_noop():
    i = APP_JS.index("closest('.nav a[data-nav]')")
    body = APP_JS[i:i + 700]
    assert "tab === tabEl.value" in body, "re-clicking the active tab must no-op"
