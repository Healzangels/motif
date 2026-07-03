"""v1.13.79 — link/version cleanups bundled in one tag.

Three small fixes:

  1. v1.13.75 backfill banner's // SHOW IN LOGS link pointed at
     /logs which 404s. The actual route is /queue (the LOGS nav
     label was renamed in v1.13.66 but the URL stayed for
     deep-link stability).

  2. The topbar // UPD pill click landed on ?tdb_pills=update
     (blue ↑ TDB chip). v1.13.68 migrated FAIL from status=failures
     to attn_pills=fail; UPD missed the same migration. Both
     pills now share the ATTN axis as their canonical filter
     surface.

  3. app/__init__.py:__version__ silently drifted from v1.13.73
     through v1.13.78 — the topbar brand showed v1.13.73 even
     when the deployed image was v1.13.77. Bump-before-tag
     protocol added to CLAUDE.md.

Static-text guards (consistent with v1.13.77, v1.13.78 patterns).
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── Bug 1: banner link points at /queue not /logs ────────────

def test_backfill_banner_link_targets_queue_not_logs():
    """The v1.13.75 banner had `href="/logs?..."` which 404s.
    v1.13.79 routes it to /queue (the actual LOGS-tab URL)."""
    html = (REPO / "app" / "web" / "templates" / "dashboard.html").read_text()
    # Find the banner block (anchored on the BACKFILLED string).
    block_start = html.index("BACKFILLED")
    banner = html[block_start:block_start + 2000]
    # The new link must be /queue, NOT /logs.
    assert 'href="/queue?level=WARNING' in banner, (
        "v1.13.79 routes the banner to /queue (LOGS tab's actual URL)"
    )
    assert 'href="/logs?' not in banner, (
        "Pre-fix /logs link 404s — re-adding it would resurrect "
        "the dead-link bug"
    )


# ── Bug 2: UPD pill uses ATTN axis ───────────────────────────

def test_upd_pill_static_href_uses_attn_axis():
    """base.html's static fallback href must use attn_pills=update,
    not the older tdb_pills=update. Affects right-click → open in
    new tab and pre-JS-paint behavior."""
    html = (REPO / "app" / "web" / "templates" / "base.html").read_text()
    badge_start = html.index('id="topbar-updates-badge"')
    # Walk back to find the <a> tag this id belongs to.
    anchor_start = html.rfind("<a ", 0, badge_start)
    anchor = html[anchor_start:badge_start + 200]
    assert 'href="/movies?attn_pills=update"' in anchor
    assert 'href="/movies?tdb_pills=update"' not in anchor


def test_upd_pill_dynamic_href_uses_attn_axis():
    """app.js's per-tick href update + the cycle handler must both
    use attn_pills=update."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # The dynamic href (refreshTopbarStatus) must include the new
    # deep-link.
    assert "/${firstUpdTab}?fourk=${firstUpdFourk}&attn_pills=update" in js
    # The cycle handler must too. v1.24.48: the UPD cycle converged onto the
    # shared bindBadgeCycle; its deep-link is the `query` arg at the init site.
    assert "bindBadgeCycle('topbar-updates-badge', 'updTabs', 'attn_pills=update')" in js
    # Old tdb_pills=update pattern should NOT appear in either path.
    # (Allow it in comments — restrict the check to URL-template
    #  string literals.)
    assert "fourk=${firstUpdFourk}&tdb_pills=update" not in js
    assert "fourk=${next.fourk ? '1' : '0'}&tdb_pills=update" not in js


# ── Bug 3: __version__ kept current ──────────────────────────

def test_version_string_matches_current_release():
    """The topbar brand reads __version__ from app/__init__.py.
    This file must be bumped BEFORE creating each git tag — the
    UI silently lies otherwise (image is v1.13.X but UI shows
    the prior version). v1.13.79 added the protocol to CLAUDE.md.

    This test pins __version__ to the v1.13.79 value to catch
    a regression where a future tag forgets the bump. When the
    next tag lands, this constant must be updated alongside
    app/__init__.py."""
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.51.41"' in init_py, (
        "Bump app/__init__.py __version__ to match the next tag — "
        "see CLAUDE.md § release conventions"
    )


def test_claude_md_documents_version_bump_protocol():
    """The bump-before-tag protocol must be in CLAUDE.md so a
    future session reading it before tagging sees the requirement.

    Whitespace-tolerant — the markdown line-wraps the phrase, so
    we collapse whitespace before the substring check."""
    raw = (REPO / "CLAUDE.md").read_text()
    flat = " ".join(raw.split())
    assert "Bump `app/__init__.py` `__version__`" in flat
    assert "BEFORE creating each git tag" in flat
