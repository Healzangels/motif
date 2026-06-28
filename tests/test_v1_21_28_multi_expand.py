"""v1.21.28 — multiple LIVE OPS cards can be unfurled at the same time.

the user: "make it so both can be unfurled at the same time." The drawer
tracked a single expandedOpId (expanding one collapsed the previous);
it's now a Set so any number of cards can be open independently.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
OPS_JS = (REPO / "app" / "web" / "static" / "ops.js").read_text()


def test_state_is_a_set():
    assert "expandedOpIds: new Set()," in OPS_JS
    # the singular id is fully gone
    assert "expandedOpId:" not in OPS_JS
    assert "state.expandedOpId " not in OPS_JS
    assert "state.expandedOpId=" not in OPS_JS


def test_toggle_uses_set_add_delete():
    idx = OPS_JS.index("function toggleExpand(")
    body = OPS_JS[idx:idx + 400]
    assert "state.expandedOpIds.add(opId)" in body
    assert "state.expandedOpIds.delete(opId)" in body


def test_membership_checks_use_has():
    # render + in-place + fetch all gate on set membership
    assert "state.expandedOpIds.has(op.op_id)" in OPS_JS
    assert "state.expandedOpIds.has(opId)" in OPS_JS


def test_aged_out_and_poll_iterate_the_set():
    # reconcile drops only aged-out ids (keeps other open cards)
    assert "state.expandedOpIds.forEach((id)" in OPS_JS
    # ESC collapses ALL open cards then closes
    assert "state.expandedOpIds.clear()" in OPS_JS


def test_version_bumped():
    assert '__version__ = "0.' in (REPO / "app" / "__init__.py").read_text()
