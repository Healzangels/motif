"""v1.18.47 — DIAGNOSTICS tab in settings with orphan-scan link.

the user on v1.18.46 close: "let's add the nav link as well in
settings Diagnostics". The /admin/orphans dashboard shipped in
v1.18.40 was reachable only via direct URL — no nav surface,
required a bookmark.

v1.18.47 adds a DIAGNOSTICS tab to /settings with an //
ORPHAN SCAN block linking out to /admin/orphans. Read-only
operator dashboards group here (future: TVDB bridge stats,
sync delta reports, etc. as additional blocks in the same
panel).

## Placement

Tabs order: PATHS / PLEX / DOWNLOADS / MATCHING / SCHEDULE /
IMPORT / RUNTIME / **DIAGNOSTICS** / NOTIFICATIONS / TOKENS /
PASSWORD / HOMEPAGE. Sits between RUNTIME and NOTIFICATIONS
— both are admin-ops tabs, but DIAGNOSTICS specifically
points at read-only inspection while NOTIFICATIONS configures
write-out behavior.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core.auth import create_admin, init_auth_schema
from app.core.db import init_db


REPO = Path(__file__).resolve().parent.parent
SETTINGS_HTML = REPO / "app" / "web" / "templates" / "settings.html"


# ── Tab nav ──────────────────────────────────────────────────


def test_diagnostics_tab_added_to_settings_nav():
    """The settings page's tab nav must include a DIAGNOSTICS
    button with data-tab='diagnostics'."""
    src = SETTINGS_HTML.read_text()
    assert 'data-tab="diagnostics"' in src, (
        "v1.18.47: settings.html must add a DIAGNOSTICS tab "
        "in the tabs nav"
    )
    assert ">DIAGNOSTICS<" in src


def test_diagnostics_tab_placed_between_runtime_and_notifications():
    """Specific placement: DIAGNOSTICS sits between RUNTIME
    and NOTIFICATIONS in the tabs nav. Both are admin-ops
    tabs but DIAGNOSTICS is for read-only inspection."""
    src = SETTINGS_HTML.read_text()
    runtime_idx = src.index('data-tab="runtime"')
    diag_idx = src.index('data-tab="diagnostics"')
    notif_idx = src.index('data-tab="notifications"')
    assert runtime_idx < diag_idx < notif_idx, (
        "v1.18.47: DIAGNOSTICS must sit between RUNTIME and "
        "NOTIFICATIONS in the tab order"
    )


# ── Panel body ───────────────────────────────────────────────


def test_diagnostics_panel_exists_with_orphan_scan_block():
    """A <section data-panel='diagnostics'> must exist with a
    // ORPHAN SCAN block + link to /admin/orphans."""
    src = SETTINGS_HTML.read_text()
    assert 'data-panel="diagnostics"' in src
    panel_idx = src.index('data-panel="diagnostics"')
    panel_block = src[panel_idx:panel_idx + 3000]
    assert "// ORPHAN SCAN" in panel_block
    assert 'href="/admin/orphans"' in panel_block


def test_diagnostics_panel_explains_what_orphan_scan_does():
    """The block intro must explain what the dashboard does so
    operators don't have to click in to understand the
    feature. Same convention as other settings blocks."""
    src = SETTINGS_HTML.read_text()
    panel_idx = src.index('data-panel="diagnostics"')
    panel_block = src[panel_idx:panel_idx + 3000]
    # The intro must reference the key concepts.
    assert "drift" in panel_block.lower()
    assert "plex_upload" in panel_block
    # Must mention it's read-only.
    assert "no automatic mutations" in panel_block.lower() \
        or "read-only" in panel_block.lower()


def test_diagnostics_panel_starts_hidden_like_other_tabs():
    """Following the existing tab-panel convention, the
    DIAGNOSTICS panel must start hidden (display:none) and
    become visible only when the DIAGNOSTICS tab is active."""
    src = SETTINGS_HTML.read_text()
    panel_idx = src.index('data-panel="diagnostics"')
    # The section opening tag should include the style.
    section_tag = src[panel_idx:panel_idx + 200]
    assert "display:none" in section_tag


# ── Settings page still loads ────────────────────────────────


@pytest.fixture
def admin_client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    settings = Settings(
        config_dir=tmp_path, data_dir=tmp_path / "data",
    )
    db = settings.db_path
    init_db(db)
    init_auth_schema(db)
    create_admin(db, username="testadmin", password="testpassword")
    from app.web.api import create_app
    app = create_app(settings)
    return TestClient(app)


AUTH = {"X-Authentik-Username": "testadmin"}


def test_settings_page_still_renders_with_diagnostics_tab(admin_client):
    """The /settings page must still render with the new tab
    added. Catches any syntactic Jinja error or template
    parsing issue."""
    r = admin_client.get("/settings", headers=AUTH)
    assert r.status_code == 200
    # The DIAGNOSTICS tab + panel must be in the rendered HTML.
    assert "DIAGNOSTICS" in r.text
    assert "// ORPHAN SCAN" in r.text
    assert 'href="/admin/orphans"' in r.text
