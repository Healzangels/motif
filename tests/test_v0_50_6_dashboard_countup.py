"""v0.50.6 — dashboard coverage-% count-up (glitch-free, flavor pass 3/4 cont.).

The 4 coverage % numbers (cov-{movies,tv,anime,collections}-pct) are SSR-baked,
so a naive count-up flashes the real value then resets to 0 and climbs. The
glitch-free path the user chose:

  1. dashboard.html runs a PARSER-BLOCKING inline <script> (before first paint)
     that stashes each % in data-countup + resets the text to 0%;
  2. app.js dashCountUp() climbs 0→value (easeOutCubic) and clears data-countup;
  3. setCov defers its pct write while data-countup is pending, so the 1s
     coverage poll can't clobber the climb.

reduced-motion users skip the inline reset (keep the static SSR value).
"""
from __future__ import annotations
from _slice_helpers import slice_to_next

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
DASH = (REPO / "app" / "web" / "templates" / "dashboard.html").read_text()


def test_inline_prepaint_reset_in_template():
    """The pre-paint reset stashes SSR value + zeroes the text. v0.50.57: the
    count-up animates for everyone now — no reduced-motion bail (full CRT motion)."""
    assert "el.dataset.countup = el.textContent" in DASH
    assert "el.textContent = '0%'" in DASH
    assert "prefers-reduced-motion: reduce" not in DASH


def test_dashcountup_climbs_and_clears():
    assert "function dashCountUp()" in DASH or "function dashCountUp()" in APP_JS
    assert "function dashCountUp()" in APP_JS
    # v0.50.46: widened 900→1500 — the bar-fill lockstep drive added lines before
    # the removeAttribute gate-clear.
    # v0.51.224 (ultra-review #4): the 2-space end-anchors NEVER matched — dashCountUp is
    # 4-space-nested and the last function of its kind, so slice_to_next fell through to
    # EOF (7685 chars, whole rest of file). The four asserts passed only because their
    # strings are file-unique — a gutted guard. Bound on the invocation that immediately
    # follows the definition (4-space `dashCountUp();`), with loadDashboard( as a backstop.
    body = slice_to_next(APP_JS, "function dashCountUp()",
                        "\n    dashCountUp();", "\n    loadDashboard(")
    assert "data-countup" in body or "dataset.countup" in body
    assert "requestAnimationFrame" in body
    # rounds each displayed frame (no float artifacts) + clears the gate at the end.
    assert "Math.round(target * eased)" in body
    assert "removeAttribute('data-countup')" in body
    # actually invoked at dashboard init.
    assert "dashCountUp();" in APP_JS


def test_setcov_defers_while_counting():
    """The live coverage poll must not clobber the climb in progress."""
    assert "if (pctEl && !pctEl.dataset.countup)" in APP_JS
