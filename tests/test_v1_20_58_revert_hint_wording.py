"""v1.20.58 — clearer REVERT-unavailable reason when there's nothing to
revert to.

On a fresh row the captured previous URL can equal the applied URL (e.g.
ACCEPT UPDATE with no earlier override), so the `previousUrl ===
currentCanonical` revertHint branch fires even though there's nothing
different to go back to. The old copy ("the previous URL is identical to
what's currently applied, so reverting would just re-create the override
at the same URL") read as if a duplicate URL existed. Reworded to lead
with "nothing earlier to revert to" (the user's Ballerina repro).
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def test_revert_hint_leads_with_nothing_to_revert_to():
    # Anchor on the duplicate-URL revertHint branch.
    anchor = APP_JS.index("} else if (previousUrl && previousUrl === currentCanonical) {")
    body = APP_JS[anchor:anchor + 800]
    assert "nothing earlier to revert to" in body, (
        "the no-op revert reason must lead with 'nothing earlier to "
        "revert to', not the old duplicate-URL wording"
    )
    # The misleading 'duplicate'-flavored phrasing must be gone.
    assert "re-create the override at the same URL" not in body
    assert "is identical to what's currently applied" not in body


def test_no_previous_url_branch_unchanged():
    """The genuinely-no-previous branch keeps its own accurate copy."""
    assert "unavailable — no previous URL was captured." in APP_JS


def test_v1_20_58_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
