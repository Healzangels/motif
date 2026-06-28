"""v1.23.49 — UI polish from the user's deploy review.

  1. legend toggle moved INLINE next to NEEDS WORK (was its own full row); the
     decode panel drops below only when opened.
  2. // HELP toggle grouped at the right with the logout control (was floating
     at the left of the status cluster, crammed against IDLE) + sized to match
     the op-pill cluster height.
  3. LIVE OPS drawer × goes RED on hover (matching the info-card / glossary ×).
  4. an expanded op-card's outer outline uses the card's OWN tone (--ot-rgb)
     instead of a hardcoded green — an amber PLEX card had a green outline.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
BASE_HTML = (REPO / "app" / "web" / "templates" / "base.html").read_text()
LIBRARY_HTML = (REPO / "app" / "web" / "templates" / "library.html").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
OPS_CSS = (REPO / "app" / "web" / "static" / "ops.css").read_text()


def test_legend_pill_sits_next_to_needs_work():
    """The legend toggle is inside .block-head-actions, before NEEDS WORK."""
    i = LIBRARY_HTML.index('class="block-head-actions"')
    j = LIBRARY_HTML.index("</div>", LIBRARY_HTML.index("</header>", i) - 200)
    block = LIBRARY_HTML[i:LIBRARY_HTML.index("</header>", i)]
    assert 'id="library-legend-toggle"' in block, "legend pill must be in the header"
    assert (block.index('id="library-legend-toggle"')
            < block.index('id="library-sort-attention-btn"')), "pill before NEEDS WORK"


def test_help_toggle_grouped_with_logout():
    """// HELP sits immediately before the logout control and is sized to the
    cluster height. v1.23.52: the ad-hoc margin-left was dropped — IDLE / HELP /
    logout are spaced by the single .topbar-status gap now."""
    hi = BASE_HTML.index('id="help-toggle"')
    li = BASE_HTML.index('class="topbar-logout"')
    assert hi < li, "// HELP must precede the logout link"
    # the help button's </button> is immediately followed by the logout <a>.
    bend = BASE_HTML.index("</button>", hi) + len("</button>")
    assert BASE_HTML[bend:li].strip().startswith('<a href="/logout"'), (
        "// HELP must sit directly next to the logout control")
    css = APP_CSS[APP_CSS.index(".help-toggle {"):]
    css = css[:css.index("}")]
    assert "height: 22px" in css and "align-items: center" in css


def test_drawer_close_is_red_on_hover():
    i = OPS_CSS.index(".ops-drawer-close:hover")
    assert "var(--red)" in OPS_CSS[i:i + 80]
    assert "var(--green)" not in OPS_CSS[i:i + 80]


def test_expanded_card_outline_uses_card_tone():
    i = OPS_CSS.index(".op-card-expanded {")
    block = OPS_CSS[i:OPS_CSS.index("}", i)]
    assert "var(--ot-rgb" in block, "outline must inherit the card's tone"
    # the hardcoded green is gone (a fallback inside var(--ot-rgb, ...) is fine).
    assert "outline: 1px solid rgba(var(--green-rgb), 0.35)" not in block
