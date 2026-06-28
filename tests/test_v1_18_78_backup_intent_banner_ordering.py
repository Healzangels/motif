"""v1.18.78 — backup-intent banner ordering + PROMOTE TO ACTIVE visibility.

the user's repro on v1.18.77 deploy: checked 'KEEP AS BACKUP' at
SET URL time on the 1941 row, migration ran, intent='backup'
correctly persisted. But the info card still read
'✓ RESOLVED VIA URL' and no // PROMOTE TO ACTIVE button.

## Root cause

v1.18.77's banner branch ordering was:

  if data.resolved → 'RESOLVED VIA URL'
  elif data.plex_resolved && intent==backup → 'BACKUP READY'
  elif data.plex_resolved → 'PLEX SERVES — OPTIONAL UPGRADES'
  ...

For a backup-intent row where motif has the file on disk:
  - locally_resolved = true (motif has local_files)
  - plex_resolved = false (mutually exclusive: p_available AND
    NOT locally_resolved)

So data.resolved=true wins first, banner reads "RESOLVED VIA
URL" before the intent check fires. Same root cause hides the
PROMOTE TO ACTIVE button (gated on data.plex_resolved which
is false).

## Fix

Branch on `overrideIntent === 'backup'` BEFORE data.resolved.
If the user explicitly chose backup, that governs the banner
regardless of whether motif's file is on disk (it WILL be —
that's the whole point of backup).

PROMOTE TO ACTIVE: visibility now gated on intent='backup'
alone (always meaningful — deploy the backup).

MARK AS BACKUP: still gated on intent='replace' AND
data.plex_resolved — demoting a RESOLVED intent=replace row
would un-deploy motif's URL with no fallback theme unless
Plex has its own to serve.
"""
from __future__ import annotations

import re
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


def _app_js() -> str:
    return (REPO / "app" / "web" / "static" / "app.js").read_text()


# ── Banner: backup-intent check precedes data.resolved ──────


def test_backup_intent_branch_precedes_resolved_branch():
    """The `if (overrideIntent === 'backup')` branch must come
    BEFORE `if (data.resolved)`. Pre-v1.18.78 the resolved
    branch fired first for backup-intent rows where motif's
    file is on disk (locally_resolved=true), so the banner
    never reached the backup branch."""
    js = _app_js()
    # Find the banner-selection block.
    block_start = js.index("let sectionTitleText;")
    # Walk to the end of the block (the closing brace).
    block_end = js.index("section.innerHTML", block_start)
    block = js[block_start:block_end]
    backup_idx = block.index("overrideIntent === 'backup'")
    resolved_idx = block.index("data.resolved")
    assert backup_idx < resolved_idx, (
        "v1.18.78: backup-intent branch must precede the "
        "data.resolved branch. Pre-fix `locally_resolved=true` "
        "on a backup-intent row routed the banner to "
        "'RESOLVED VIA URL' instead of 'BACKUP READY — "
        "DEFERRING TO PLEX'."
    )


def test_backup_branch_no_longer_gated_on_plex_resolved():
    """The backup-intent branch must NOT require data.plex_resolved
    anymore. The v1.18.77 gate `data.plex_resolved && intent==
    'backup'` was never true for a row where motif had a local
    file."""
    js = _app_js()
    block_start = js.index("let sectionTitleText;")
    block_end = js.index("section.innerHTML", block_start)
    block = js[block_start:block_end]
    # The new shape is a bare `overrideIntent === 'backup'`
    # branch — no `data.plex_resolved &&` prefix.
    assert "overrideIntent === 'backup') {" in block, (
        "v1.18.78: backup branch should be a single-condition "
        "check on overrideIntent alone"
    )
    # The old combined check must be gone.
    assert "data.plex_resolved && overrideIntent === 'backup'" not in block


def test_backup_banner_text_unchanged():
    """The banner text itself stays 'BACKUP READY — DEFERRING
    TO PLEX' — only the branch ordering changed."""
    js = _app_js()
    assert "BACKUP READY — DEFERRING TO PLEX" in js


# ── PROMOTE TO ACTIVE visibility ────────────────────────────


def test_promote_button_visible_whenever_intent_is_backup():
    """The PROMOTE TO ACTIVE button must show whenever the row
    has intent='backup'. Pre-v1.18.78 the outer guard was
    `if (overrideIntent && data.plex_resolved)` — for a
    locally_resolved backup row, plex_resolved=false → button
    hidden."""
    js = _app_js()
    # Find the intent-flip block.
    fbi = js.index("intentFlipBtnsHtml = '';")
    fbe = js.index("section.innerHTML", fbi)
    block = js[fbi:fbe]
    # Outer guard: just `overrideIntent` (truthy when 'backup'
    # OR 'replace'). The branch INSIDE then chooses which button.
    assert "if (overrideIntent)" in block, (
        "v1.18.78: outer guard for intent-flip buttons must "
        "be `if (overrideIntent)` — backup-intent rows should "
        "always see PROMOTE TO ACTIVE"
    )
    # And the inner backup branch is unconditional (no extra
    # plex_resolved gate).
    # v1.19.39: widened from 800 → 2200 chars. The v1.19.39
    # synthetic-override tooltip branch added a comment block
    # + isSynthetic/promoteTip variable declarations between
    # the `if (overrideIntent === 'backup') {` line and the
    # promote-to-active button — the original 800-char window
    # no longer reaches back to the if.
    # v1.19.86: widened 2200 → 2900 — the PROMOTE tone-class
    # (promoteSourceKind / promoteToneClass) comment + decls added
    # another ~450 chars before the button.
    promote_idx = block.index("data-act=\"promote-to-active\"")
    pre_promote = block[max(0, promote_idx - 2900):promote_idx]
    # The promote branch is `if (overrideIntent === 'backup')` —
    # no additional gate.
    assert "overrideIntent === 'backup'" in pre_promote


def test_mark_as_backup_still_gated_on_plex_resolved():
    """MARK AS BACKUP must STILL be gated on data.plex_resolved.
    Demoting a RESOLVED intent=replace row (where motif's URL is
    actively serving) would leave the row themeless unless Plex
    has its own theme to fall back to. The button only makes
    sense when Plex has something to serve."""
    js = _app_js()
    fbi = js.index("intentFlipBtnsHtml = '';")
    fbe = js.index("section.innerHTML", fbi)
    block = js[fbi:fbe]
    # The replace-branch inner condition must include plex_resolved.
    assert (
        "overrideIntent === 'replace' && data.plex_resolved"
        in block
    ), (
        "v1.18.78: MARK AS BACKUP must remain gated on "
        "data.plex_resolved — demoting a RESOLVED row would "
        "leave it themeless without Plex's fallback"
    )


# ── v1.18.78 markers + cross-references ─────────────────────


def test_v1_18_78_marker_explains_ordering_fix():
    """The banner block must reference v1.18.78 + the v1.18.77
    branch-ordering bug so future readers tracing 'why does
    backup intent win over locally_resolved' find the marker."""
    js = _app_js()
    block_start = js.index("let sectionTitleText;")
    block_end = js.index("section.innerHTML", block_start)
    pre_block = js[max(0, block_start - 3000):block_start]
    assert "v1.18.78" in pre_block, (
        "v1.18.78: marker required near the banner ordering"
    )
    flat = " ".join(pre_block.split())
    # The marker should reference the v1.18.77 bug shape.
    assert ("locally_resolved" in flat
            or "mutually exclusive" in flat
            or "swallowed" in flat.lower()), (
        "v1.18.78: marker should explain WHY the ordering "
        "matters (locally_resolved + plex_resolved mutual "
        "exclusion)"
    )


def test_v1_18_78_marker_on_button_visibility_fix():
    """The intent-flip button visibility block must reference
    v1.18.78 + explain why PROMOTE TO ACTIVE was hidden in
    v1.18.77."""
    js = _app_js()
    fbi = js.index("intentFlipBtnsHtml = '';")
    pre_block = js[max(0, fbi - 2000):fbi]
    assert "v1.18.78" in pre_block
