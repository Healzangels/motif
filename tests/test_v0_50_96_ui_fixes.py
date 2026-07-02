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

def test_input_floor_self_limits_to_container():
    """v0.51.1 (code-review altitude): the login spill is fixed at the root — the
    base .input floor is now min(240px, 100%), which caps at the container's own
    width so it can never overflow a narrower parent (the auth oval, a tight cell).
    The 240px floor still holds wherever there's room; the per-container
    .auth-card min-width:0 escape (v0.50.96) is retired."""
    idx = APP_CSS.index("\n.input {")
    end = APP_CSS.index("}", idx)
    assert "min-width: min(240px, 100%);" in APP_CSS[idx:end], "floor must self-limit to the container"
    # the scoped escape is gone — the global cap covers the auth card now.
    assert ".auth-card .input { min-width: 0; }" not in APP_CSS
