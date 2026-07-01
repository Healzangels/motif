"""v0.50.68 — hero-wave: instant start + queue-scaled intensity.

Two follow-ups from the user on the v0.50.67 reactive hero wave:

1. INSTANT START: the wave reacted ~1.1s late — it waited for refreshTopbarStatus
   to see the enqueued job past the /api/stats 1s cache. Now ops.js flips
   motif-busy the instant of the click (in setOptimisticPlaceholder), and
   refreshTopbarStatus unions hasOptimistic() so it can't strip it back off in the
   gap before the real op lands.

2. INTENSITY BY QUEUE DEPTH: stacking work (tdb sync + plex refresh + downloads…)
   now escalates the wave via data-busy-level (1..3), CAPPED at 3 so it never
   looks messy. Score = distinct active op-kinds + total queued per-row jobs.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
OPS_JS = (REPO / "app" / "web" / "static" / "ops.js").read_text()


def _rule(css: str, sel: str) -> str:
    i = css.index(sel)
    return css[i:css.index("}", i) + 1]


# ── 1. Instant start (ops.js kicks + hasOptimistic; app.js unions it) ──

def test_optimistic_click_flips_the_wave_immediately():
    """setOptimisticPlaceholder (fired on the click, before any poll) adds
    motif-busy so the wave reacts with zero latency."""
    fn = OPS_JS[OPS_JS.index("function setOptimisticPlaceholder("):]
    fn = fn[:fn.index("\n  }") + 1]
    assert "document.documentElement.classList.add('motif-busy')" in fn


def test_ops_exposes_has_optimistic():
    assert "function hasOptimistic()" in OPS_JS
    assert "_optimisticOp.expiresAt > Date.now()" in OPS_JS
    # exported so app.js can union it into the busy calc.
    assert re.search(r"window\.motifOps\s*=\s*\{[^}]*\bhasOptimistic\b", OPS_JS, re.S)


def test_app_unions_optimistic_so_busy_isnt_stripped_in_the_gap():
    """refreshTopbarStatus keeps motif-busy on while a click-time optimistic op is
    live, so the ~1.1s /api/stats gap doesn't flicker the wave off."""
    assert "window.motifOps.hasOptimistic()" in APP_JS
    assert "const heroBusy = anyMutatingOpActive || _optimisticBusy;" in APP_JS
    assert "_root.classList.toggle('motif-busy', heroBusy);" in APP_JS


# ── 2. Intensity level scales with queue depth, capped (v0.50.71: at 4) ──

def test_busy_score_counts_op_kinds_plus_queued_jobs():
    # distinct active op-kinds + the per-row job SUM (pending+running).
    assert ("const _busyScore = (themerrdbBusy ? 1 : 0) + (plexEnumBusy ? 1 : 0)\n"
            "        + (opProgressRunning ? 1 : 0) + perJobSum;" in APP_JS)
    # perJobSum is a real sum now (not just the boolean), so a deep download queue
    # pushes the level up.
    assert "const perJobSum = (" in APP_JS
    assert "const perJobBusy = perJobSum > 0;" in APP_JS


def test_busy_level_is_capped_at_4():
    # v0.50.71: 4 tiers now (>=6 → 4 for sync + refresh + a queue), still capped.
    assert ("const _busyLevel = _busyScore >= 6 ? 4\n"
            "        : _busyScore >= 4 ? 3 : _busyScore >= 2 ? 2 : 1;" in APP_JS)
    # the level is published as a data attribute the CSS keys off.
    assert "_root.setAttribute('data-busy-level', String(_busyLevel));" in APP_JS
    assert "_root.removeAttribute('data-busy-level');" in APP_JS


def test_css_levels_escalate_monotonically_and_cap():
    """L1 (base) < L2 < L3 < L4 on brightness (opacity up) + height (scaleY up).
    v0.50.71 added L4 for heavy stacked activity; L4 is the ceiling — no level 5."""
    def vals(sel):
        r = _rule(APP_CSS, sel)
        op = float(re.search(r"opacity:\s*([\d.]+)", r).group(1))
        dur = float(re.search(r"animation-duration:\s*([\d.]+)s", r).group(1))
        sy = float(re.search(r"scaleY\(([\d.]+)\)", r).group(1))
        return op, dur, sy
    l1 = vals("html.motif-busy .hero::after {")
    l2 = vals('html.motif-busy[data-busy-level="2"] .hero::after {')
    l3 = vals('html.motif-busy[data-busy-level="3"] .hero::after {')
    l4 = vals('html.motif-busy[data-busy-level="4"] .hero::after {')
    # opacity (brightness) rises across all four
    assert l1[0] < l2[0] < l3[0] < l4[0]
    # duration (speed) falls monotonically — never SLOWER at a higher level
    assert l1[1] > l2[1] > l3[1] > l4[1]
    # scaleY (height) rises
    assert l1[2] < l2[2] < l3[2] < l4[2]
    # capped: no level 5
    assert '[data-busy-level="5"]' not in APP_CSS
    # v0.50.71: L4's richness comes largely from the SECOND layer swelling — its
    # ::before opacity jumps more than the primary's, so the cross-weave reads
    # fuller instead of just faster/messier.
    b3 = vals('html.motif-busy[data-busy-level="3"] .hero::before {')
    b4 = vals('html.motif-busy[data-busy-level="4"] .hero::before {')
    assert b4[0] - b3[0] >= 0.10


def test_top_level_stays_inside_the_reserved_band():
    """The tallest level's scaleY must not exceed the clearance the 38px band
    affords (~1.35 max before the scaled wave would reach the subtitle)."""
    r = _rule(APP_CSS, 'html.motif-busy[data-busy-level="4"] .hero::after {')
    sy = float(re.search(r"scaleY\(([\d.]+)\)", r).group(1))
    assert sy <= 1.35
