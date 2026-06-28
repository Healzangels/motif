"""v1.23.53 — the chip LEGEND is always available + renders as a real chip.

the user (3rd deploy report): the // LEGEND toggle rendered as an unstyled native
white box and only appeared in help mode. Two fixes:
  1. the toggle carries the long-standing .chip class, so it renders as a proper
     outlined chip even from a stale cached app.css that lacks the
     .library-legend-pill rule (the cause of the white box);
  2. it's no longer help-mode gated — the chip legend shows on every library
     page, help on or off (the user: "should display if help is pressed or isn't").
"""
from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient


REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
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


def test_legend_toggle_renders_as_a_chip(admin_client):
    html = admin_client.get("/anime", headers=AUTH).text
    # always in the markup (no server-side help gate), and reusing .chip.
    assert 'class="chip library-legend-pill"' in html
    assert "// LEGEND" in html
    assert 'id="library-legend"' in html  # the decode panel


def test_legend_not_css_gated_on_help_mode():
    # the toggle + the panel are no longer scoped under body.help-mode.
    assert "body.help-mode .library-legend-pill" not in APP_CSS
    assert "body.help-mode .library-legend-panel" not in APP_CSS
    assert ".library-legend-panel.open { display: block; }" in APP_CSS


def test_legend_wired_regardless_of_help_mode():
    # the legend click handler lives at the top level of initHelpMode (which runs
    # unconditionally on DOMContentLoaded), not inside an `if (help on)` branch.
    i = APP_JS.index("function initHelpMode()")
    body = APP_JS[i:i + 3000]
    assert "library-legend-toggle" in body
    assert "legendToggle.addEventListener('click'" in body
