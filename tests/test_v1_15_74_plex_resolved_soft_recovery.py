"""v1.15.74 — soften // TRY THIS NEXT on P / +P rows.

the user: "on rows that have failed tbd red pill but have a theme
being provided by plex or +P Let's change the try this in the
info card since if the row is P but a failed red pill tdb you
don't really have to try anything next it would be optional."

A row with `failure_kind` set + Plex serving an independent
theme (pi.plex_independent_theme == 1) has a working theme
without motif's help. The TRY THIS NEXT actions (SET URL, FIX
COOKIES, etc.) are OPTIONAL upgrades — the user can take them
if they want a motif-managed theme, but the row isn't broken
in the user-visible sense.

Pre-v1.15.74 the recovery section title read "// TRY THIS NEXT"
in default (red) tone, the same as a fully-broken row. v1.15.74
adds a `plex_resolved` flag to /api/items/{mt}/{id}/recovery-
options that the frontend checks to swap the title to
"✓ PLEX SERVES — OPTIONAL UPGRADES" + apply the green-banner
recovery-section-resolved styling. Options stay available so
the user can still pick a motif-managed theme.

Distinct from `resolved` (which means motif has a local source)
— a P row has no local source, so locally_resolved is False;
the new `plex_resolved` carries the "Plex is covering it"
signal independently.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"
APP_JS = REPO / "app" / "web" / "static" / "app.js"


def test_api_recovery_options_returns_plex_resolved_flag():
    """The recovery-options endpoint must return a `plex_resolved`
    boolean in its response. Without it the frontend has no way
    to detect the P-row-with-failure case for soft titling."""
    src = API_PY.read_text()
    fn_start = src.index("async def api_recovery_options(")
    # Anchor on the next top-level @app.get/@app.post so we capture
    # the full function body.
    fn_end = src.index("    @app.", fn_start + 1)
    body = src[fn_start:fn_end]
    assert '"plex_resolved":' in body, (
        "v1.15.74: api_recovery_options must include plex_resolved "
        "in its JSON response so the frontend can soft-title the "
        "TRY THIS NEXT section on P-row-with-failure rows"
    )


def test_plex_resolved_is_gated_on_p_available_and_not_locally_resolved():
    """plex_resolved must be True ONLY when Plex serves an
    independent theme AND motif has no local source. If motif
    has a local source, the existing `resolved` flag handles
    the case (more specific signal — keeps the existing
    RESOLVED VIA banner intact)."""
    src = API_PY.read_text()
    fn_start = src.index("async def api_recovery_options(")
    fn_end = src.index("    @app.", fn_start + 1)
    body = src[fn_start:fn_end]
    # Either of these compositions works.
    has_correct_gate = (
        "plex_resolved = bool(p_available and not locally_resolved)" in body
        or "p_available and not locally_resolved" in body
    )
    assert has_correct_gate, (
        "v1.15.74: plex_resolved must be `p_available AND NOT "
        "locally_resolved` so motif-locally-resolved rows still "
        "use the RESOLVED VIA banner (more specific signal)"
    )


def test_recovery_section_title_uses_plex_resolved_branch():
    """The frontend section-title selector must branch on
    data.plex_resolved and render the OPTIONAL UPGRADES wording.
    Without this branch the soft title never appears."""
    js = APP_JS.read_text()
    fn_start = js.index("async function hydrateRecoveryOptions(")
    fn_end = js.index("\n  async function ", fn_start + 1)
    body = js[fn_start:fn_end]
    assert "data.plex_resolved" in body, (
        "v1.15.74: hydrateRecoveryOptions must check "
        "data.plex_resolved to soft-title the section header"
    )
    assert "OPTIONAL UPGRADES" in body or "PLEX SERVES" in body, (
        "v1.15.74: the plex_resolved title text must read along "
        "the lines of 'PLEX SERVES — OPTIONAL UPGRADES' so the "
        "user knows the actions below are optional"
    )


def test_recovery_section_resolved_styling_extends_to_plex_resolved():
    """The .recovery-section-resolved class (green banner tone)
    must apply for plex_resolved rows too — same visual signal as
    locally-resolved rows so the user reads "this is fine,
    upgrade if you want" rather than "you need to act"."""
    js = APP_JS.read_text()
    fn_start = js.index("async function hydrateRecoveryOptions(")
    fn_end = js.index("\n  async function ", fn_start + 1)
    body = js[fn_start:fn_end]
    # The `if (data.resolved || ackedOnly)` gate must include
    # data.plex_resolved as a 3rd OR-branch.
    assert "data.plex_resolved" in body
    # Find the classList.add gate.
    gate_idx = body.index("recovery-section-resolved")
    gate_chunk = body[max(0, gate_idx - 200):gate_idx]
    assert "plex_resolved" in gate_chunk, (
        "v1.15.74: the recovery-section-resolved class must apply "
        "when data.plex_resolved is true, not only on data.resolved "
        "/ ackedOnly — extends the green-banner treatment to the "
        "Plex-serves case"
    )
