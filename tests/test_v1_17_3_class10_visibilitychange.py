"""v1.17.3 — class-10 visibilitychange sweep + Apprise URLs UX hint.

PROJECT_HISTORY § 12.Q canonized class 10:

  > long-running `setInterval` polls with tight ceilings die in
  > inactive tabs under Chromium's ~1/min throttle. Need ~1 tick/
  > min ceiling headroom + `visibilitychange` listener to re-arm
  > on tab return.

v1.16.7 closed the ONE motif site where the ceiling was tight
enough to kill the interval (`libraryRapidPoll`, ceiling 60s →
300s + an existing visibilitychange handler that triggered a
library refresh on tab return). v1.17.0 audit pass surfaced
Task #10 — "audit remaining setInterval sites for the same
pattern."

Audit findings (see PROJECT_HISTORY § 14.A):

| Site | Cadence | Has TIMEOUT | Vulnerable to ceiling-kill | Vulnerable to stall |
|---|---|---|---|---|
| syncWatcher (lines 2172/2215) | 2s | No (state-based) | No | Yes — sync detection lags up to 60s |
| _watchOpForCompletion (5162) | inside, 2s | 30 min wall-clock | No | Slight — watcher still terminates correctly |
| _updatePresetActiveState (6630) | 600ms | No | No | Not at sub-second cadences |
| libraryRapidTimer (8090) | inside | 5 min wall-clock | Fixed v1.16.7 | Already covered |
| finishWatcher (10065) | inside, 2s | 30 min wall-clock | No | Slight |
| refreshTopbarStatus (13464) | 10s | No | No | Yes — topbar pills go stale |
| loadDashboard (13650) | 30s | No | No | Yes — dashboard stat cards lag |
| loadQueue (13651) | 10s | No | No | Yes — /queue rows stop refreshing |
| loadLibrary (13657) | 30s | No | No | Yes — covered by v1.16.7 handler |

The "ceiling-kill" pattern was unique to libraryRapidTimer.
Other sites are throttled-but-functional in inactive tabs, but
the user sees stale state on tab return until the next throttled
tick fires (up to ~60s later).

v1.17.3 fix: extend the existing v1.16.7 visibilitychange handler
to also wake up every polled surface — topbar, ops state,
dashboard (if on /), queue (if on /queue). loadLibrary remains
gated on the library-body element so it doesn't fire on settings
/ dashboard / queue pages.

Net effect: tab return collapses any visible-state-lag to
milliseconds across every page, not just the library tabs.

Plus L5 from the v1.17.0 audit — a cosmetic UX hint on the
Apprise URLs textarea explaining that blank lines + leading/
trailing whitespace are stripped on save (the textarea reloads
in normalized form, which some users misread as data loss).
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_INIT = REPO / "app" / "__init__.py"
APP_JS = REPO / "app" / "web" / "static" / "app.js"
SETTINGS_HTML = REPO / "app" / "web" / "templates" / "settings.html"


# ── Version pin ──────────────────────────────────────────────


def test_version_at_least_v1_17_3():
    import re
    src = APP_INIT.read_text()
    m = re.search(r'__version__\s*=\s*"(\d+)\.(\d+)\.(\d+)"', src)
    assert m
    major, minor, patch = int(m.group(1)), int(m.group(2)), int(m.group(3))
    assert (major, minor, patch) >= (0, 17, 3)


# ── visibilitychange handler covers every polled surface ──────


def _visibilitychange_body() -> str:
    """Extract the visibilitychange listener body (the function
    passed to addEventListener). Bounded by the next non-indented
    block in the file. Returns the listener body for substring
    assertions."""
    src = APP_JS.read_text()
    handler_idx = src.index(
        "document.addEventListener('visibilitychange', () => {")
    end_idx = src.index("\n  });\n", handler_idx)
    return src[handler_idx:end_idx + 7]


def test_visibilitychange_refreshes_topbar_on_tab_return():
    """Every motif page renders the topbar. On tab return the
    topbar pills must re-fetch immediately so stale FAIL/UPD
    counts don't linger."""
    body = _visibilitychange_body()
    assert "refreshTopbarStatus" in body, (
        "v1.17.3: visibilitychange handler must trigger "
        "refreshTopbarStatus on tab return — every page has a "
        "topbar to refresh."
    )


def test_visibilitychange_refreshes_ops_state_on_tab_return():
    """The ops drawer + topbar mini-bar drive off the
    /api/progress poll. Tab-throttle stalls that poll; tab return
    must re-fire to catch up to current worker state."""
    body = _visibilitychange_body()
    assert "motifOps" in body
    assert ".refresh" in body, (
        "v1.17.3: visibilitychange must call motifOps.refresh() "
        "to re-stat any running background ops on tab return."
    )


def test_visibilitychange_refreshes_library_when_present():
    """The pre-existing v1.16.7 library refresh on tab return
    must still fire — gated on the library-body element so
    non-library pages don't pay the cost."""
    body = _visibilitychange_body()
    assert "library-body" in body
    assert "loadLibrary" in body


def test_visibilitychange_refreshes_dashboard_on_root_path():
    """/ shows dashboard cards on a 30s background poll. Tab
    return must collapse the stale-card gap."""
    body = _visibilitychange_body()
    assert "loadDashboard" in body
    assert "path === '/'" in body, (
        "v1.17.3: dashboard refresh must be gated on the / path "
        "so it doesn't fire when the user is on a different page."
    )


def test_visibilitychange_refreshes_queue_on_queue_path():
    """/queue polls jobs + events every 10s. Tab return must
    refresh both to avoid showing the user a stale snapshot."""
    body = _visibilitychange_body()
    assert "loadQueue" in body
    assert "path === '/queue'" in body


def test_visibilitychange_typeof_guards_each_function():
    """Each refresh call must be guarded by `typeof X ===
    'function'` (or equivalent existence check) so a future
    bundle that ships a subset of functions doesn't crash the
    handler. Defensive — the handler runs on EVERY tab return,
    a crash here is user-visible."""
    body = _visibilitychange_body()
    # Anchor on each refresh call's surrounding context.
    assert "typeof refreshTopbarStatus === 'function'" in body
    assert "typeof window.motifOps.refresh === 'function'" in body
    assert "typeof loadLibrary === 'function'" in body
    assert "typeof loadDashboard === 'function'" in body
    assert "typeof loadQueue === 'function'" in body


# ── L5: Apprise URLs whitespace-stripping UX hint ────────────


def test_apprise_urls_textarea_has_whitespace_strip_hint():
    """The textarea for Apprise URLs strips blank lines + leading/
    trailing whitespace on save (the round-trip silently
    normalizes). The hint text under the field calls that out
    so users don't misread the post-save state as data loss."""
    html = SETTINGS_HTML.read_text()
    # Anchor on the APPRISE URLS label.
    anchor = html.index("APPRISE URLS")
    block = html[anchor:anchor + 1500]
    assert "blank lines" in block.lower() and "stripped" in block.lower(), (
        "v1.17.3 L5: the form-hint under APPRISE URLS must mention "
        "that blank lines + whitespace are stripped on save."
    )
