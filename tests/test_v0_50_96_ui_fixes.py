"""v0.50.96 — two on-device UI bugs (the user).

1. LEGEND: the "full reference in // GLOSSARY" button inside the library legend
   went dead after any client-side library-tab switch. switchLibraryTab
   innerHTML-swaps `.library-legend-body` (v1.23.71), which replaces the
   #library-legend-gloss node and drops the direct click listener initHelpMode
   attached once. Fixed via event delegation on the STABLE #library-legend panel
   (the panel persists across the body swap, so the listener survives).

2. LOGIN: on a narrow mobile viewport the username/password fields spilled past
   the round vinyl-label oval. The global `.input { min-width: 240px }` floor
   forced them wider than the trimmed `.auth-card-inner` (64% ≈ 217px at 368px);
   scoped `.auth-card .input { min-width: 0 }` into the ≤560px media query so the
   fields shrink to the inner (measured: input 240px → 198px, no overflow).
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


# ── 1. legend gloss link is delegated on the stable panel ────────────────

def test_gloss_link_wired_by_delegation_on_stable_panel():
    """The click must be delegated on #library-legend (the panel, which
    survives the body swap), not bound directly to #library-legend-gloss
    (which switchLibraryTab replaces)."""
    anchor = APP_JS.index("const legendToggle = document.getElementById('library-legend-toggle');")
    end = APP_JS.index("initSettingsHelp();", anchor)
    block = APP_JS[anchor:end]
    assert "legendPanel.addEventListener('click'" in block, (
        "gloss click must be delegated on the stable legendPanel"
    )
    assert "closest('#library-legend-gloss')" in block, (
        "delegation must match clicks on the #library-legend-gloss button"
    )
    # the fragile direct binding must be gone.
    assert "glossLink.addEventListener('click'" not in block, (
        "a direct listener on the gloss button dies when switchLibraryTab "
        "innerHTML-swaps the legend body"
    )


def test_switch_library_tab_still_swaps_legend_body():
    """The delegation fix is load-bearing precisely because switchLibraryTab
    innerHTML-replaces the legend body. If this swap ever goes away, the WHY
    behind delegation changes — pin it so the two stay coupled."""
    assert "curLeg.innerHTML = newLeg.innerHTML;" in APP_JS, (
        "switchLibraryTab replaces the legend body — the reason the gloss "
        "listener must be delegated, not direct"
    )


# ── 2. login fields no longer spill past the oval on mobile ──────────────

def _media_600_or_560_block(css: str, width: str) -> str:
    marker = f"@media (max-width: {width}) {{"
    i = css.index(marker)
    depth = 0
    j = i
    while True:
        if css[j] == "{":
            depth += 1
        elif css[j] == "}":
            depth -= 1
            if depth == 0:
                return css[i:j + 1]
        j += 1


def test_auth_input_min_width_dropped_on_narrow_screens():
    block = _media_600_or_560_block(APP_CSS, "560px")
    assert ".auth-card .input { min-width: 0; }" in block, (
        "auth fields must be allowed to shrink to .auth-card-inner on mobile"
    )


def test_global_input_floor_unchanged_on_desktop():
    """The 240px floor still applies to .input generally — the fix only
    releases it inside the auth card on narrow viewports."""
    # anchor on the TOP-LEVEL .input rule (line-start), not the indented
    # `.auth-card .input` override the fix added earlier in the file.
    idx = APP_CSS.index("\n.input {")
    end = APP_CSS.index("}", idx)
    assert "min-width: 240px;" in APP_CSS[idx:end]
