"""v1.19.96 — DL/PL pending pulse amber → cyan (match status-bar queue tone).

the user: "the DL and PL blinking amber, since now we are using cyan
in the download text, these don't line up any longer."

v1.19.88 set the status-bar queue tone (download/place/scan ops) to
cyan (`--tone-queue: var(--cyan)`), splitting it away from Plex's
amber. But the row's DL/PL in-flight pulse (`.state-pill-pending`,
from v1.13.13) stayed amber — so the SAME "motif queue work in
flight" concept showed cyan in the status bar and amber on the row.

v1.19.96 flips `.state-pill-pending` to cyan so cyan is the single
"motif queue work in flight" signal everywhere. The pulse animation
distinguishes 'working' (pulsing cyan) from the steady-cyan
PL=pushed state. Amber stays the Plex / warn / await tone.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
OPS_CSS = (REPO / "app" / "web" / "static" / "ops.css").read_text()


def _rule(css: str, selector: str) -> str:
    idx = css.index(selector)
    return css[idx:css.index("}", idx) + 1]


def test_pending_pulse_is_cyan_not_amber():
    rule = _rule(APP_CSS, ".state-pill-pending {")
    assert "background: var(--cyan) !important;" in rule, (
        "v1.19.96: the DL/PL in-flight pulse must be cyan"
    )
    assert "box-shadow: 0 0 6px var(--cyan);" in rule
    assert "--amber" not in rule, (
        "v1.19.96: the pending pulse must no longer use amber — that "
        "was the mismatch with the cyan status-bar queue tone"
    )


def test_pending_pulse_keeps_its_animation():
    """The transient cue is the PULSE — it must survive the recolor
    (it's what distinguishes 'working' from the steady-cyan
    PL=pushed state now that both are cyan)."""
    rule = _rule(APP_CSS, ".state-pill-pending {")
    assert "animation: state-pill-pulse" in rule


def test_pending_pulse_matches_status_bar_queue_tone():
    """The alignment guard: the row pending pulse and the status-bar
    queue tone must resolve to the SAME color so a download/place in
    flight reads identically across both surfaces. Both are
    var(--cyan)."""
    pending = _rule(APP_CSS, ".state-pill-pending {")
    queue_tone = _rule(OPS_CSS, "--tone-queue:")
    assert "var(--cyan)" in pending
    assert "var(--cyan)" in queue_tone, (
        "v1.19.88/v1.19.96: status-bar --tone-queue must stay cyan — "
        "the row pending pulse now matches it"
    )


def test_pushed_pl_state_still_cyan_steady():
    """PL=pushed (plex_upload) stays steady cyan — pulsing-cyan
    (placing) → steady-cyan (pushed) is the intended continuity, not
    a regression. Pin it so the recolor didn't disturb it."""
    rule = _rule(APP_CSS, ".state-pill.pushed {")
    assert "background: var(--cyan);" in rule


def test_await_state_stays_amber():
    """The 'await' PL state (downloaded, awaiting placement) is a
    needs-action signal, NOT in-flight work — it stays amber. Only
    the in-flight PULSE moved to cyan."""
    rule = _rule(APP_CSS, ".state-pill.await {")
    assert "background: var(--amber);" in rule, (
        "v1.19.96: 'await' is a needs-action amber state — the "
        "recolor only touched the in-flight pulse, not await"
    )


def test_v1_19_96_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
