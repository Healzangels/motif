"""v0.51.138 — CSS-audit T5: remove provably-dead library CSS.

Four rules the app can never reach (grep + render-path verified), removed:

  * `.form-label-row` — the v1.22.55 settings redesign retired the label-row
    wrapper (scalar fields use .field-row now). No live markup references it —
    test_v1_22_55 + test_v1_17_8 already assert its ABSENCE in the markup.
  * `.state-pill.warn`, `.state-pill.bad`, `.state-pill.mismatch` — the DL/PL
    dot suffix (renderLibraryRow) only ever computes on/broken/pushed/await/''
    (dl = broken|on|'' ; pl = broken|pushed|on|await|''). 'mismatch' was retired
    as a dot state in v1.12.81; 'warn'/'bad' were never stamped. So all three
    styled nothing.

The .state-pill.mismatch removal also corrected test_v1_15_43, which pinned its
colour and mislabelled it a LIVE surface. The genuinely-live mismatch surfaces —
`.link-glyph-mismatch` (LINK-column M glyph) and `.attn-pill-mismatch` (!M filter
chip) — are untouched and still guarded there.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
# strip comments — removal breadcrumbs still name the dead selectors in prose.
APP_CSS = re.sub(r"/\*.*?\*/", "", (REPO / "app" / "web" / "static" / "app.css").read_text(), flags=re.S)


def test_dead_rules_are_gone():
    for sel in (".form-label-row {", ".state-pill.warn ", ".state-pill.bad ",
                ".state-pill.mismatch {"):
        assert sel not in APP_CSS, f"dead rule {sel!r} must stay removed (CSS-audit T5)"


def test_live_state_pill_variants_survive():
    # the states the dot suffix DOES compute must keep their rules.
    for sel in (".state-pill.on ", ".state-pill.broken ", ".state-pill.pushed ",
                ".state-pill.await "):
        assert sel in APP_CSS, f"live state-pill variant {sel!r} must NOT be removed"


def test_live_mismatch_surfaces_untouched():
    # the real M surfaces (LINK glyph + filter chip) stay defined.
    assert ".link-glyph-mismatch {" in APP_CSS
    assert ".attn-pill-mismatch {" in APP_CSS


def test_form_label_text_sibling_kept():
    # only .form-label-row went; its sibling .form-label-text is unrelated.
    assert ".form-label-text {" in APP_CSS
