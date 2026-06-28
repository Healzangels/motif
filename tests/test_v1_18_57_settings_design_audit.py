"""v1.18.57 — settings design audit + spacing fix.

the user's review of /settings:

> "Can we review our setting for downloads, would like to fix the
>  spacing between the sections as there is a big extra white
>  space gap. Can we also take an in general design review of
>  all settings sections to make sure they are consistent design
>  styles and choices."

He marked a red X on the screenshot — between the // PROBE TDB
URLS button and the dashed `form-hint-divider` line above the
// REPROBE FAILURES section.

## Fix A — tighten the form-hint-divider spacing

`.form-hint-divider` had `margin: 18px 0 10px`. The 18px top
margin created a ~35px dead zone above the dashed line, which
visually disconnected the divider from the button-row it was
separating from. the user's red X landed exactly there.

Tightened to `margin: 10px 0 6px` — divider still reads as a
section separator (dashed line + opacity), but no longer floats
orphaned in dead space.

## Fix B — add the missing subtitle on DEFAULT PLACEMENT METHOD

Audit found 2 of 18 settings sections lacked the standard
`<span class="muted small">` subtitle:

  - `// DEFAULT PLACEMENT METHOD` — no subtitle, no block-intro
    paragraph. The header rendered bare. Added a subtitle:
    "hardlink to disk or upload via the Plex API".
  - `// API TOKENS` — replaces the subtitle slot with a
    `// NEW TOKEN` action button. Acceptable secondary pattern
    (header has a primary action surface). Kept as-is.

## Fix C — document the settings section header pattern

Added a "Settings section header (v1.18.57)" subsection to
`docs/DESIGN_SYSTEM.md` § "Form layout". Captures the three-
part header shape (`// PREFIX` title + muted subtitle +
optional block-intro) + the audit guard reference, so any
future settings addition mirrors the established pattern.

This test is the audit guard — it walks every top-level
`<section class="block tab-panel">` in settings.html and pins
the design rules.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
SETTINGS_HTML = REPO / "app" / "web" / "templates" / "settings.html"
APP_CSS = REPO / "app" / "web" / "static" / "app.css"
DESIGN_SYSTEM = REPO / "docs" / "DESIGN_SYSTEM.md"


# Sections where the subtitle slot is intentionally replaced by
# something else (e.g., a primary action button). Documented
# secondary pattern. Add new exceptions here with a comment
# explaining why the subtitle isn't applicable.
SUBTITLE_EXCEPTIONS = {
    # API TOKENS replaces subtitle with a // NEW TOKEN button —
    # the section's primary action is creating a token, so the
    # button surface lives in the header row in lieu of the
    # contextual subtitle.
    "// API TOKENS",
}


def _enumerate_top_level_settings_sections() -> list[tuple[str, str]]:
    """Return [(title, header_block), ...] for every top-level
    .block.tab-panel section in settings.html. Excludes nested
    headers (e.g., the // EVENTS subsection under NOTIFICATIONS
    or // PREVIEW RESULTS under IMPORT)."""
    src = SETTINGS_HTML.read_text()
    # Match each top-level <section class="block tab-panel" ...>
    # through to the matching </section>. The naive approach
    # (no XML parsing) works because settings.html doesn't nest
    # <section class="block tab-panel"> inside another one.
    sections = re.findall(
        r'<section class="block tab-panel"[^>]*>(.*?)</section>',
        src,
        re.DOTALL,
    )
    out = []
    for section_body in sections:
        # Extract the first <header class="block-head"> block
        # (the section's primary header — nested .block-head
        # blocks for EVENTS / PREVIEW are inside, not at the
        # section's top level).
        header_match = re.search(
            r'<header class="block-head[^"]*">(.*?)</header>',
            section_body,
            re.DOTALL,
        )
        if not header_match:
            continue
        header_block = header_match.group(1)
        title_match = re.search(
            r'<h2 class="block-title">([^<]+)</h2>',
            header_block,
        )
        if not title_match:
            continue
        title = title_match.group(1).strip()
        out.append((title, header_block))
    return out


# ── Audit: every section has a subtitle or documented exception ─


def test_every_section_has_subtitle_or_exception():
    """Every top-level settings section must carry either a
    `<span class="muted small">` subtitle in the header OR be
    listed in SUBTITLE_EXCEPTIONS with a documented reason.
    Audit guard for the v1.18.57 consistency pass."""
    sections = _enumerate_top_level_settings_sections()
    assert len(sections) >= 17, (
        f"Expected at least 17 top-level sections, found {len(sections)}. "
        f"Template structure may have changed."
    )
    missing = []
    for title, header_block in sections:
        if title in SUBTITLE_EXCEPTIONS:
            continue
        if 'class="muted small"' not in header_block:
            missing.append(title)
    assert not missing, (
        f"v1.18.57: settings sections missing the muted-small "
        f"subtitle: {missing}. Either add a subtitle to the "
        f"section header or document the exception in "
        f"SUBTITLE_EXCEPTIONS with a comment."
    )


def test_every_section_uses_motif_voice_prefix():
    """Every section title MUST start with `// ` (motif voice
    convention, CLAUDE.md UI conventions). Pin so a future
    section can't drift to a plain heading."""
    sections = _enumerate_top_level_settings_sections()
    bad = [t for t, _ in sections if not t.startswith("// ")]
    assert not bad, (
        f"v1.18.57: settings sections lacking // prefix: {bad}"
    )


def test_default_placement_method_has_subtitle():
    """Specific pin for v1.18.57's fix — DEFAULT PLACEMENT
    METHOD was the bare-header section that motivated the
    audit. Make sure the subtitle survives future refactors."""
    src = SETTINGS_HTML.read_text()
    # Anchor on the title, walk forward looking for muted small
    # within the same header block.
    idx = src.index("// DEFAULT PLACEMENT METHOD")
    after = src[idx:idx + 800]
    # The subtitle must appear before the closing </header>.
    end_header = after.index("</header>")
    header_only = after[:end_header]
    assert 'class="muted small"' in header_only, (
        "v1.18.57: // DEFAULT PLACEMENT METHOD must have a "
        "muted-small subtitle in its header"
    )


def test_api_tokens_exception_has_new_token_button():
    """// API TOKENS is the documented exception — verify the
    header row's action button (// NEW TOKEN) is present so
    the exception remains valid."""
    src = SETTINGS_HTML.read_text()
    idx = src.index("// API TOKENS")
    after = src[idx:idx + 800]
    end_header = after.index("</header>")
    header_only = after[:end_header]
    assert "// NEW TOKEN" in header_only, (
        "v1.18.57: // API TOKENS exception requires the // NEW "
        "TOKEN action button in the header (replaces the "
        "subtitle slot). If the button moves, add a subtitle "
        "instead and drop the SUBTITLE_EXCEPTIONS entry."
    )


# ── Fix A: form-hint-divider spacing tightened ──────────────


def test_form_hint_divider_margins_tightened():
    """The .form-hint-divider CSS must use tightened margins
    (~10px top, ~6px bottom) per v1.18.57. Pre-fix margins
    (18px 0 10px) created the dead zone the user X'd on the
    DOWNLOADS tab screenshot."""
    src = APP_CSS.read_text()
    # Anchor on the rule, slice to the closing brace.
    # v1.18.59: widened 1000 → 2000 chars. v1.18.59 added the
    # white-space:normal override + ~700 chars of inline
    # comment explaining it, pushing the closing brace past
    # the original 1000-char window.
    idx = src.index(".form-hint-divider {")
    block = src[idx:idx + 2000]
    end = block.index("}")
    rule = block[:end]
    # The new margin must be present.
    assert "margin: 10px 0 6px" in rule, (
        "v1.18.57: .form-hint-divider margin must be tightened "
        "to `10px 0 6px` (was `18px 0 10px`)"
    )
    # The pre-fix margin must NOT survive.
    assert "margin: 18px 0 10px" not in rule, (
        "v1.18.57: the pre-fix .form-hint-divider margin "
        "would re-introduce the dead-zone gap"
    )


def test_form_hint_divider_only_used_once():
    """The divider's tighter margins are tuned for the
    DOWNLOADS tab's PROBE/REPROBE button-row split — the only
    site that uses it. If a future tag adds more sites, this
    test fires loud so the spacing tuning gets re-evaluated
    in the new context."""
    src = SETTINGS_HTML.read_text()
    count = src.count("form-hint-divider")
    assert count == 1, (
        f"v1.18.57: expected exactly 1 form-hint-divider usage, "
        f"found {count}. If a new usage landed, verify the "
        f"`10px 0 6px` margins still work in the new context "
        f"or generalize the spacing tuning."
    )


# ── Fix C: design pattern documented ─────────────────────────


def test_design_system_documents_settings_header_pattern():
    """docs/DESIGN_SYSTEM.md must include a 'Settings section
    header' subsection capturing the three-part shape (//
    PREFIX title + muted subtitle + optional block-intro)."""
    src = DESIGN_SYSTEM.read_text()
    assert "Settings section header" in src, (
        "v1.18.57: DESIGN_SYSTEM.md must document the settings "
        "section header pattern"
    )
    # Key reference points the docs should mention.
    flat = " ".join(src.split())
    assert "muted small" in flat
    assert "block-intro" in flat
    assert "API TOKENS" in flat, (
        "v1.18.57: docs must mention the API TOKENS exception "
        "explicitly so future readers don't try to add a "
        "subtitle there"
    )


def test_design_system_references_this_audit_test():
    """The docs must reference this audit test by file name so
    future readers know where the rule is enforced."""
    src = DESIGN_SYSTEM.read_text()
    assert "test_v1_18_57_settings_design_audit" in src, (
        "v1.18.57: DESIGN_SYSTEM.md must reference the audit "
        "test file so the rule + the enforcement link up"
    )


# ── End-to-end render ───────────────────────────────────────


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


AUTH = {"X-Authentik-Username": "testadmin"}


def test_settings_page_renders_with_new_subtitle(admin_client):
    """/settings page must still render after the v1.18.57
    changes. End-to-end smoke test."""
    r = admin_client.get("/settings", headers=AUTH)
    assert r.status_code == 200
    # The new DEFAULT PLACEMENT METHOD subtitle must reach the
    # rendered HTML.
    assert "hardlink to disk or upload via the Plex API" in r.text, (
        "v1.18.57: the new subtitle must render"
    )
    # And the original sections still render their subtitles.
    assert "set the tempo for YouTube downloads" in r.text  # DOWNLOAD TUNING
    assert "verify recovery URLs are alive" in r.text  # PROBE TDB URLS
