"""v0.51.79 — CSS+login audit CSS pass: focus-visible a11y + dead-CSS removal.

- Accessibility (WCAG 2.4.7): the universal :focus{outline:none} strips focus rings;
  the :focus-visible allow-list repaints them, but the primary nav links, settings
  checkboxes, and topbar help/logout were missed → keyboard focus invisible. Added.
  The glyph-only logout link also got an aria-label (title alone is unreliable).
- Dead CSS: 4 rename/redesign-residue selector clusters with ZERO live emitters
  (.attn-pill-cookies removed v1.19.67; .chip-info never emitted [blue lives on
  .btn-info]; #import-preview-table .url-cell replaced by .url-link v1.15.77;
  .src-key-clear renamed to .pill-filter-clear) deleted.
- ops.css tokenization (separate concern, same tag): exact-match hardcoded sizes/radii
  swapped to the app.css :root tokens (zero visual change).

Static guards so the a11y rings can't silently vanish and the dead CSS can't creep back.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
OPS_CSS = (REPO / "app" / "web" / "static" / "ops.css").read_text()
BASE_HTML = (REPO / "app" / "web" / "templates" / "base.html").read_text()


def test_focus_visible_rings_added_for_missed_controls():
    for sel in (".nav a:focus-visible", ".help-toggle:focus-visible",
                ".topbar-logout:focus-visible", 'input[type="checkbox"]:focus-visible'):
        assert sel in APP_CSS, f"missing keyboard focus ring: {sel}"


def test_logout_link_has_aria_label():
    assert 'class="topbar-logout"' in BASE_HTML
    logout = BASE_HTML[BASE_HTML.index('class="topbar-logout"') - 60:
                       BASE_HTML.index('class="topbar-logout"') + 120]
    assert 'aria-label="Sign out"' in logout


def test_dead_css_clusters_stay_removed():
    # zero live emitters — deleting them is safe, re-adding is dead weight.
    for dead in (".attn-pill-cookies", ".chip-info", ".url-cell", ".src-key-clear"):
        assert dead not in APP_CSS, f"dead selector crept back into app.css: {dead}"
    # and the dead JS selector-list entry stays gone.
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    assert "'.src-key-clear'" not in js


def test_ops_css_tokenized_exact_matches():
    # the exact-token-match values now reference vars (zero visual change); a
    # regression re-hardcoding them would silently skip the ops surface on a scale change.
    assert "var(--t-tiny)" in OPS_CSS and "var(--radius)" in OPS_CSS
    # off-scale literals with no token are intentionally left (spot-check one).
    assert "border-radius: 4px" in OPS_CSS
