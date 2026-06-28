"""v1.22.95 — collapsible filter drawer + select-all change-event fix.

the user: (1) "improve our space usage of the filter chip section ...
by default the filter chips are hidden but there was a filter button
... that would reveal the filters" — the seven pill axes (TDB / SRC /
ATTN / DL / PL / LINK / ED) now live in a hidden-by-default drawer
behind a // FILTERS toggle, split into two semantic columns
(theme-axis | file-axis — the grouping the old divider line encoded).
An active-count badge keeps hidden filters discoverable; the drawer
auto-opens when a deep-link lands with axis filters applied.

(2) "clicking [select all] shows it as still unchecked and after a
bit of time it will check itself" — the header checkbox handler used
'click' + preventDefault; browsers toggle a checkbox BEFORE
dispatching click and REVERT it after a prevented handler returns,
clobbering the handler's own .checked write until the next async
poll re-render. Now a 'change' handler (post-native-toggle, nothing
to revert) — the canonical-state writes stick immediately.
"""
from __future__ import annotations

import re
import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core.auth import create_admin, init_auth_schema
from app.core.db import init_db
from app.core.events import now_iso


REPO = Path(__file__).resolve().parent.parent
LIB_HTML = (REPO / "app" / "web" / "templates" / "library.html").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()

# Live-code variants — explanatory comments legitimately mention the
# retired class names; absence checks run against code only.
CSS_CODE = re.sub(r"/\*.*?\*/", "", APP_CSS, flags=re.S)
HTML_CODE = re.sub(r"<!--.*?-->", "",
                   re.sub(r"\{#.*?#\}", "", LIB_HTML, flags=re.S),
                   flags=re.S)


# ── drawer structure ─────────────────────────────────────────


def test_template_has_toggle_and_hidden_drawer():
    assert 'id="library-filter-toggle"' in LIB_HTML
    assert 'aria-controls="library-filter-drawer"' in LIB_HTML
    assert 'id="library-filter-count"' in LIB_HTML
    assert ('<div class="pill-filter-drawer" id="library-filter-drawer" '
            'hidden>') in LIB_HTML


def test_two_semantic_columns():
    i = LIB_HTML.index('id="library-filter-drawer"')
    drawer = LIB_HTML[i:]
    theme = drawer.index('aria-label="theme-axis filters"')
    file_ = drawer.index('aria-label="file-axis filters"')
    assert theme < file_
    # theme column holds TDB/SRC/ATTN; file column DL/PL/LINK/ED.
    theme_block = drawer[theme:file_]
    assert 'aria-label="TDB pill filter"' in theme_block
    assert 'aria-label="SRC pill filter"' in theme_block
    assert 'aria-label="ATTN pill filter"' in theme_block
    file_block = drawer[file_:]
    for ax in ("DL", "PL", "LINK", "EDITION"):
        assert f'aria-label="{ax} pill filter"' in file_block


def test_divider_class_retired():
    assert "pill-filter-row-divider" not in HTML_CODE
    assert ".pill-filter-row-divider" not in CSS_CODE, (
        "the column split replaced the divider rule"
    )


def test_drawer_css_is_two_column_grid():
    assert ".pill-filter-drawer {" in APP_CSS
    i = APP_CSS.index(".pill-filter-drawer {")
    block = APP_CSS[i:i + 500]
    assert "grid-template-columns: repeat(2, minmax(0, 1fr));" in block
    assert ".library-filter-toggle {" in APP_CSS
    assert ".library-filter-count {" in APP_CSS


# ── JS wiring ────────────────────────────────────────────────


def test_js_toggle_badge_and_auto_open():
    assert "function _setFilterDrawerOpen(" in APP_JS
    assert "function updateFilterDrawerUi(" in APP_JS
    assert "function _activeAxisFilterCount(" in APP_JS
    # the badge updates at the loadLibrary chokepoint (covers pill
    # clicks, CLEAR ALL, presets, deep-links).
    i = APP_JS.index("async function loadLibrary()")
    head = APP_JS[i:i + 1200]
    assert "updateFilterDrawerUi();" in head
    # auto-open after hydration when axis filters are active.
    assert ("if (_drawerWasOpen || _activeAxisFilterCount() > 0)"
            in APP_JS)
    assert "localStorage.getItem('motifFilterDrawerOpen')" in APP_JS


def test_select_all_uses_change_event():
    assert ("library-select-all')?.addEventListener('change'"
            in APP_JS)
    # the click+preventDefault shape is gone (the revert-after-
    # handler race was the visible lag).
    assert ("library-select-all')?.addEventListener('click'"
            not in APP_JS)
    i = APP_JS.index("library-select-all')?.addEventListener('change'")
    body = APP_JS[i:i + 5000]
    assert "e.preventDefault();" not in body[:300], (
        "change handlers must not preventDefault — there is nothing "
        "to prevent and it re-introduces revert semantics on some "
        "browsers"
    )
    # canonical-state writes survive (the v1.15.61/16.11 contracts).
    assert "headerCb.indeterminate = false;" in body
    assert "headerCb.checked = turnOn;" in body


# ── behavioral: both library variants render the drawer ──────


@pytest.fixture
def app_client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    db = settings.db_path
    init_db(db)
    init_auth_schema(db)
    create_admin(db, username="testadmin", password="testpassword")
    from app.web.api import create_app
    return TestClient(create_app(settings)), db


_H = {"X-Authentik-Username": "testadmin"}


def test_movies_page_renders_drawer(app_client):
    client, _db = app_client
    r = client.get("/movies", headers=_H)
    assert r.status_code == 200
    assert 'id="library-filter-drawer" hidden' in r.text
    assert 'id="library-filter-toggle"' in r.text
    assert 'aria-label="EDITION pill filter"' in r.text


def test_collections_page_renders_drawer_without_ed(app_client):
    client, _db = app_client
    r = client.get("/collections", headers=_H)
    assert r.status_code == 200
    assert 'id="library-filter-drawer" hidden' in r.text
    # ED axis is movies/tv-only (v1.18.50 wider-scope rule); the
    # file-axis column must still close cleanly without it.
    assert 'aria-label="EDITION pill filter"' not in r.text
    assert 'aria-label="file-axis filters"' in r.text
