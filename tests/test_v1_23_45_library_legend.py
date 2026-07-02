"""v1.23.45 — in-context library LEGEND (help feature, Tag 3 of 3).

The library-first payoff: when help mode is on, a collapsible LEGEND strip sits
above the rows decoding every chip on THAT tab, so a new user never has to guess
what T / HL / PU / the dots mean — and the full reference is one click away in
the // GLOSSARY. Hangs off the same body.help-mode class as the v1.23.44 toggle;
chip colors reuse the glossary palette (gc-*/gd-*/gg-*).

Collections drops the chips it physically can't have — A/M (no folder to adopt a
sidecar from) and HL (collection themes are plex_upload, not folder sidecars).
"""
from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient


REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
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


def _legend(html):
    """Slice out just the LEGEND panel (#library-legend) — the glossary in
    base.html also carries the gc-* classes, so chip assertions must be scoped
    to the legend. v1.23.49: the panel is a <div>, not a <details>."""
    i = html.index('id="library-legend"')
    # v1.23.68: widened 4000 -> 6000 for the new TDB axis section.
    return html[i:i + 6000]


# ── behavioral: the legend renders + decodes this tab's chips ──


def test_legend_renders_above_the_table(admin_client):
    html = admin_client.get("/movies", headers=AUTH).text
    # v1.23.49: the toggle PILL sits in the results header (next to NEEDS WORK);
    # the decode PANEL drops below. Both precede the table.
    assert 'id="library-legend-toggle"' in html
    assert "// LEGEND" in html
    assert html.index('id="library-legend-toggle"') < html.index('id="library-table"')
    assert 'id="library-legend"' in html
    assert html.index('id="library-legend"') < html.index('id="library-table"')
    leg = _legend(html)
    # v1.23.68: the TDB axis is decoded too (added to glossary + legend).
    for axis in ("TDB", "SRC", "LINK", "DL / PL", "FLAGS"):
        assert axis in leg, f"legend missing {axis}"
    # the full reference is reachable from the legend foot.
    assert 'id="library-legend-gloss"' in leg


def test_movies_legend_has_sidecar_chips(admin_client):
    leg = _legend(admin_client.get("/movies", headers=AUTH).text)
    # v1.23.56: SRC/LINK chips reuse the real row classes; dots/flags stay gc-.
    # v1.23.68: the TDB axis reuses the real .tdb-pill-* row classes too.
    for chip in ("tdb-pill-yes", "tdb-pill-update", "tdb-pill-no",
                 "link-badge-themerrdb", "link-badge-user", "link-badge-adopt",
                 "link-badge-manual", "link-badge-cloud", "link-glyph-hardlink",
                 "link-glyph-pu", "gd-on", "gg-fail"):
        assert chip in leg, f"movies legend missing {chip}"


def test_collections_legend_drops_sidecar_only_chips(admin_client):
    leg = _legend(admin_client.get("/collections", headers=AUTH).text)
    # collections still have these.
    assert ("link-badge-themerrdb" in leg and "link-badge-user" in leg
            and "link-badge-cloud" in leg and "link-glyph-pu" in leg)
    # but NOT the folder-sidecar chips it can't have.
    assert "link-badge-adopt" not in leg, "collections can't adopt a folder sidecar"
    assert "link-glyph-hardlink" not in leg, "collection themes are plex_upload, not hardlinks"


# ── source pins: gated on help mode + collapse persisted ──


def test_legend_always_visible_not_help_gated():
    # v1.23.53: the toggle reuses .chip and is no longer help-mode gated — it
    # shows on every library page; the panel still drops below only when opened.
    assert ".library-legend-pill {" in APP_CSS
    i = APP_CSS.index(".library-legend-pill {")
    pill = APP_CSS[i:APP_CSS.index("}", i)]
    assert "display: none" not in pill, "toggle no longer hidden by default"
    assert "body.help-mode .library-legend-pill" not in APP_CSS, "no help-mode gate"
    assert ".library-legend-panel { display: none; }" in APP_CSS  # collapsed default
    assert ".library-legend-panel.open { display: block; }" in APP_CSS
    assert "body.help-mode .library-legend-panel.open" not in APP_CSS, "panel un-gated"


def test_legend_collapse_state_persisted():
    # v1.23.49: the legend is a header toggle + a panel (was a <details>).
    i = APP_JS.index("const legendToggle = document.getElementById('library-legend-toggle')")
    # slice the whole legend-init block (ends at initSettingsHelp) rather than a
    # fixed length — v0.50.96 delegated the gloss link, growing the block.
    body = APP_JS[i:APP_JS.index("initSettingsHelp();", i)]
    assert "motif:help_legend_open" in body
    assert "legendToggle.addEventListener('click'" in body
    assert "legendPanel.classList.toggle('open'" in body
    # the legend's // GLOSSARY link opens the same reference dialog.
    assert "library-legend-gloss" in body
    assert "showModalNoFocusRing(dlg)" in body
