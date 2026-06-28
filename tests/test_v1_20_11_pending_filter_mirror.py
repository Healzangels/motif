"""v1.20.11 — PENDING_EXISTS filter mirrors the pill-render presence gate.

Silent-bug audit HIGH-2: the `PENDING_EXISTS` filter expression (drives
the blue TDB↑ `update` filter chip, and the green `tdb` chip via NOT
PENDING_EXISTS) carried the v1.19.71 new_theme_available escape ONLY in
its KIND gate — not in its PRESENCE gate. So a SRC=— new_theme row (no
local_files / override / placement / sidecar) fell OUT of the blue
`update` filter AND wrongly INTO the green `tdb` filter — even though the
row's pill renders, the topbar UPD badge counts it, and the
attn_pills=update filter surfaces it (all of which DO carry the escape in
both gates). A v1.19.72 mirror-drift miss; the prior count-floor guard
(`>= 9` matches) didn't pin this specific site.

Fix: add the escape to PENDING_EXISTS's presence gate, matching the
canonical pill-render expression (the `pending_update` / actionable
columns), so the filter and the rendered pill agree.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()


def test_pending_exists_carries_new_theme_escape_in_both_gates():
    """PENDING_EXISTS must include _pending_update_new_theme_kind_sql in
    BOTH its presence gate AND its kind gate (count >= 2), matching the
    pill-render. Pre-fix it had the escape only once (kind gate)."""
    start = API_PY.index("PENDING_EXISTS = (")
    end = API_PY.index("SRC_NOT_DASH = (", start)
    block = API_PY[start:end]
    # v1.22.10: the KIND gate is now the shared _pending_update_actionable_sql
    # helper (which composes the new_theme escape); the PRESENCE gate keeps its
    # inline escape. So both gates still carry it — one literal, one via helper.
    assert "_pending_update_new_theme_kind_sql" in block, (
        "v1.20.11: PENDING_EXISTS presence gate must keep the inline "
        "new_theme escape"
    )
    assert "_pending_update_actionable_sql" in block, (
        "v1.22.10: the kind gate is now the actionable helper (carries the "
        "new_theme escape) — so SRC=— new_theme rows filter consistently "
        "with how their pill renders"
    )


def test_pending_exists_presence_gate_matches_pill_render():
    """Tie the filter to the source of truth: the pill-render
    `pending_update` column's presence gate ends with `OR
    pi.local_theme_file = 1` immediately followed by the escape. The
    PENDING_EXISTS presence gate (string-concat form) must do the same —
    `"  OR pi.local_theme_file = 1"` followed (within the gate) by the
    new_theme escape."""
    start = API_PY.index("PENDING_EXISTS = (")
    end = API_PY.index("SRC_NOT_DASH = (", start)
    block = API_PY[start:end]
    anchor = block.index('"  OR pi.local_theme_file = 1"')
    # The escape must appear after the presence-gate local_theme_file
    # line, before the kind gate's COALESCE opens.
    after = block[anchor:anchor + 800]
    kind_gate = after.index("COALESCE(") if "COALESCE(" in after else len(after)
    presence_tail = after[:kind_gate]
    assert "_pending_update_new_theme_kind_sql" in presence_tail, (
        "v1.20.11: the PENDING_EXISTS PRESENCE gate must carry the "
        "new_theme escape (it was missing — the kind gate had it but the "
        "presence gate didn't, so SRC=— new_theme rows were excluded)"
    )


def test_v1_20_11_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
