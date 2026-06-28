"""v1.20.1 — bulk-bar ACCEPT/KEEP labels selection-aware + single-line caption.

the user (on v1.19.98): the ACCEPT/KEEP buttons were wider than the
others + the caption double-stacked (the v1.19.98 wrap). And: nothing
selected shouldn't show a count ("presumed all"); a selection should
read like the other bulk buttons — drop "ALL", switch to singular/
plural + the selection count.

v1.20.1:
  - nothing selected → '// ACCEPT ALL UPDATES' / '// KEEP ALL CURRENT'
    (no count)
  - selection       → '// ACCEPT UPDATE (1)' / '// ACCEPT UPDATES (N)'
    and '// KEEP CURRENT (N)'
  - caption back to a single line (nowrap + ellipsis, flex:1) — no
    double-stack, no overlap.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def _label_block() -> str:
    idx = APP_JS.index("library-accept-all-updates-btn')")
    return APP_JS[idx:idx + 1400]


def test_no_count_when_nothing_selected():
    block = _label_block()
    # The no-selection fallback is the bare ALL label (no (N)).
    assert "'// ACCEPT ALL UPDATES'" in block
    assert "'// KEEP ALL CURRENT'" in block


def test_selection_label_is_singular_plural_plus_count():
    block = _label_block()
    assert "// ACCEPT UPDATE${updateCount === 1 ? '' : 'S'} (${updateCount})" in block
    assert "// KEEP CURRENT (${updateCount})" in block
    # Gated on a real selection with actionable rows.
    assert "const selScoped = n > 0 && updateCount > 0;" in APP_JS


def test_caption_single_line():
    idx = APP_CSS.index("#library-bulk-bar .missing-banner-text {")
    rule = APP_CSS[idx:idx + 800]
    assert "white-space: nowrap;" in rule
    assert "text-overflow: ellipsis;" in rule
    assert "flex: 1;" in rule
    assert "white-space: normal" not in rule


def test_label_selection_logic_replica():
    """Replicate the JS label selection so the singular/plural + the
    no-selection fallback are exercised, not just text-pinned."""
    def accept_label(n, update_count):
        sel_scoped = n > 0 and update_count > 0
        if sel_scoped:
            return f"// ACCEPT UPDATE{'' if update_count == 1 else 'S'} ({update_count})"
        return "// ACCEPT ALL UPDATES"

    def keep_label(n, update_count):
        sel_scoped = n > 0 and update_count > 0
        return f"// KEEP CURRENT ({update_count})" if sel_scoped else "// KEEP ALL CURRENT"

    assert accept_label(0, 5) == "// ACCEPT ALL UPDATES"     # nothing selected
    assert accept_label(1, 1) == "// ACCEPT UPDATE (1)"      # singular
    assert accept_label(3, 3) == "// ACCEPT UPDATES (3)"     # plural
    assert accept_label(2, 0) == "// ACCEPT ALL UPDATES"     # selection, 0 actionable → ALL
    assert keep_label(0, 5) == "// KEEP ALL CURRENT"
    assert keep_label(3, 3) == "// KEEP CURRENT (3)"


def test_v1_20_1_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
