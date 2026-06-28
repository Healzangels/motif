"""v1.23.44 — // HELP toggle + glossary modal (help feature, Tag 2 of 3).

The spine of the user's help/discovery layer: a topbar // HELP toggle that flips
"help mode" (a persisted user preference) and a // GLOSSARY reference modal that
decodes the entire chip vocabulary in one place. Off by default so the normal
chrome is unchanged; the Unraid model adapted to motif.

- toggle: body.help-mode class + localStorage('motif:help_mode'), survives
  across tabs (a preference, not a per-tab filter snapshot — cf. v1.18.84).
- help-mode bar: appears only in help mode, points at the glossary.
- glossary: a .dlg modal with SRC / LINK / DL-PL / FLAGS / TOPBAR sections,
  chip colors mirroring the row chips via the shared palette tokens.

Tag 3 (the inline library LEGEND strip) hangs off the same body.help-mode class.
"""
from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient


REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
BASE_HTML = (REPO / "app" / "web" / "templates" / "base.html").read_text()
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


# ── behavioral: every authenticated page carries the help surfaces ──


def test_help_surfaces_render_on_every_page(admin_client):
    """base.html renders the toggle, the (hidden) help bar, and the glossary
    dialog — so the help layer is reachable from anywhere."""
    html = admin_client.get("/", headers=AUTH).text
    assert 'id="help-toggle"' in html
    assert 'aria-pressed="false"' in html
    assert 'id="help-mode-bar"' in html
    assert 'id="help-glossary"' in html
    assert 'id="help-glossary-open"' in html


def test_glossary_decodes_every_axis(admin_client):
    """The glossary covers every chip axis + a sample of each axis's values."""
    html = admin_client.get("/", headers=AUTH).text
    for section in ("// SRC", "// LINK", "// DL &amp; PL", "// FLAGS", "// TOPBAR"):
        assert section in html, f"glossary missing {section} section"
    # a representative chip from each of the trickiest axes is decoded.
    assert "ThemerrDB-managed" in html        # SRC T
    assert "Pushed" in html and "via its API" in html   # LINK PU
    assert "Plex Backup" in html              # LINK PB
    assert "PROMOTE TO ACTIVE to deploy" in html  # LINK TB
    # v1.23.56: SRC/LINK swatches reuse the real row classes; dots/flags stay gc-.
    for cls in ("link-badge-themerrdb", "link-badge-user", "link-glyph-pu",
                "link-glyph-b", "gd-on", "gd-await", "gg-fail"):
        assert cls in html, f"glossary missing chip class {cls}"


# ── source pins: toggle state machine + persistence ──


def test_help_mode_persists_in_localstorage_not_session():
    """Help mode is a cross-tab USER PREFERENCE → localStorage (motif:* key),
    NOT the sessionStorage scope used for per-tab filter snapshots (v1.18.84)."""
    assert "const HELP_MODE_KEY = 'motif:help_mode';" in APP_JS
    i = APP_JS.index("function setHelpMode(")
    body = APP_JS[i:i + 260]
    assert "localStorage.setItem(HELP_MODE_KEY" in body
    assert "sessionStorage" not in body


def test_apply_help_mode_toggles_body_class_and_aria():
    assert "function applyHelpMode(on)" in APP_JS
    i = APP_JS.index("function applyHelpMode(on)")
    body = APP_JS[i:i + 500]
    assert "classList.toggle('help-mode', on)" in body
    assert "aria-pressed" in body
    # init is wired into the DOMContentLoaded boot sequence.
    di = APP_JS.index("document.addEventListener('DOMContentLoaded'")
    assert "initHelpMode();" in APP_JS[di:di + 200]


def test_glossary_dialog_closes_via_data_close_and_backdrop():
    i = APP_JS.index("function initHelpMode()")
    # v0.50.19: 1200 → 1600 — the GLOSSARY caret open/close wiring added a few
    # lines before the backdrop handler. Same three close mechanisms, further apart.
    body = APP_JS[i:i + 1600]
    assert "showModalNoFocusRing(dlg)" in body, "// GLOSSARY opens the dialog"
    assert "[data-close]" in body
    assert "e.target === dlg" in body, "backdrop click closes"


# ── CSS pins: active state + glossary chip palette ──


def test_help_toggle_has_active_state_and_glossary_palette():
    assert '.help-toggle[aria-pressed="true"]' in APP_CSS
    assert ".help-mode-bar {" in APP_CSS
    # v1.23.56: the glossary SRC/LINK chips reuse the real row classes directly
    # (no parallel gc-* palette to drift); the reuse is guarded by test_v1_23_50.
    assert 'class="link-badge link-badge-themerrdb"' in BASE_HTML
    assert 'class="link-badge link-badge-user"' in BASE_HTML
    # v1.23.48: the help bar is CSS-gated on body.help-mode (the prior `hidden`
    # attribute was overridden by display:flex, so it showed regardless).
    assert 'id="help-mode-bar"' in BASE_HTML
    assert "body.help-mode .help-mode-bar { display: flex; }" in APP_CSS
    i = APP_CSS.index(".help-mode-bar {")
    assert "display: none" in APP_CSS[i:i + 160]
