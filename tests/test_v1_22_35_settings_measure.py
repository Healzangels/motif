"""v1.22.35 — settings uniform reading measure.

the user's review: "inconsistency with full page text and half page text and
spacing between buttons and sections ... find a uniform style across the board."

Root cause: inside a settings section, the intro paragraph (.block-intro) and
the save-action row (.form-actions) ran the FULL block width while the form
fields (.form-grid) capped at 720px — a stair-step of measures. This pass gives
every standard section ONE 720px column via a --measure-form token shared by
.block-intro, .form-grid, and the save .form-actions; wide-content sections
(.block-body-flush tables) opt out. Two inline margin overrides were folded
into shared rules; nested-in-form save rows lose their double-gap.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
SETTINGS_HTML = (REPO / "app" / "web" / "templates" / "settings.html").read_text()
DESIGN_SYSTEM = (REPO / "docs" / "DESIGN_SYSTEM.md").read_text()


# ── token + the rules that share it ───────────────────────────


def test_measure_token_defined():
    assert re.search(r"--measure-form:\s*720px", APP_CSS), (
        "v1.22.35: --measure-form token (720px) must exist in :root")


def test_form_grid_uses_measure_token():
    i = APP_CSS.index(".form-grid {")
    block = APP_CSS[i:i + 200]
    assert "max-width: var(--measure-form)" in block, (
        "v1.22.35: .form-grid must reference --measure-form, not a literal 720px")


def test_block_intro_capped_to_measure():
    i = APP_CSS.index(".block-intro { margin: 0;")
    block = APP_CSS[i:i + 120]
    assert "max-width: var(--measure-form)" in block, (
        "v1.22.35: .block-intro must cap to the measure so intro text lines up "
        "with the form")


def test_settings_save_row_capped_to_measure():
    # Standard (non-flush) settings save rows share the measure; flush sections
    # (LIBRARY SECTIONS / API TOKENS tables) are exempted.
    assert ".tab-panel .block-body:not(.block-body-flush) > *" in APP_CSS, (
        "v1.22.35: every direct child of a standard settings block-body "
        "(intro, form, divider, save row) must cap to the measure")
    assert ".tab-panel .form-grid > .form-actions:last-child" in APP_CSS, (
        "v1.22.35: nested-in-form save rows must drop the double-gap")


# ── inline overrides folded into shared rules ─────────────────


def test_no_inline_margin_override_on_cookies_or_events():
    # The two standard-section inline margin overrides are gone (folded into
    # .form-hint + .form-actions and .form-actions + .form-hint).
    assert 'class="form-actions" style="margin-top:var(--gap-2)"' not in SETTINGS_HTML, (
        "v1.22.35: cookies TEST form-actions inline margin must be removed")
    assert 'class="form-hint" style="margin-top: var(--gap-3)"' not in SETTINGS_HTML, (
        "v1.22.35: the EVENTS footnote inline margin must be removed")
    assert ".form-actions + .form-hint" in APP_CSS, (
        "v1.22.35: the footnote-after-actions spacing must be a shared rule")


def test_inline_margin_overrides_only_remain_in_import_flow():
    # Any surviving inline margin override must be inside the bespoke IMPORT
    # preview-then-apply flow (a documented special screen), not a standard
    # form section.
    overrides = re.findall(r'style="margin-top:\s*var\(--gap-\d\)"', SETTINGS_HTML)
    # Exactly the two IMPORT preview overrides (PREVIEW RESULTS header + APPLY).
    assert len(overrides) <= 2, (
        f"v1.22.35: expected <=2 inline margin overrides (both in the IMPORT "
        f"preview flow), found {len(overrides)}")


# ── docs ──────────────────────────────────────────────────────


def test_design_system_documents_the_measure():
    flat = " ".join(DESIGN_SYSTEM.split())
    assert "--measure-form" in flat
    assert "test_v1_22_35_settings_measure" in flat, (
        "v1.22.35: DESIGN_SYSTEM.md must reference this guard test")


# ── end-to-end smoke render ───────────────────────────────────


@pytest.fixture
def admin_client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web.api import create_app
    from fastapi.testclient import TestClient
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    db = settings.db_path
    init_db(db)
    init_auth_schema(db)
    create_admin(db, username="testadmin", password="testpassword")
    return TestClient(create_app(settings))


def test_settings_page_still_renders(admin_client):
    r = admin_client.get("/settings", headers={"X-Authentik-Username": "testadmin"})
    assert r.status_code == 200
    assert "set the tempo for YouTube downloads" in r.text  # DOWNLOAD TUNING
