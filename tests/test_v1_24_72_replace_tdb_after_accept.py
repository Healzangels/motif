"""v1.24.72 — REPLACE TDB hidden after a stale accepted_update.

the user: The Super Mario Galaxy Movie (SRC=U — a user URL applied) offered no
REPLACE TDB in the SOURCE menu even though ThemerrDB has a theme video for it
(themerrdb url set). Root cause: the row accepted a TDB upstream update long ago
(accepted_update=1), then the user overrode it with a user URL. accepted_update
is a STICKY historical flag, so the gate's bare `!it.accepted_update` kept
suppressing REPLACE TDB even though the active theme is no longer the TDB one.

The "recent accept already pulled the current TDB URL (no-op)" concern the flag
guards only holds while the accepted TDB is STILL active (SRC=T) — and the
placement clause below the gate already excludes T. Relax to
`(!it.accepted_update || srcLetter !== 'T')`.

The behavioral counterpart lives in tests/js/test_menu_actions.js (the node
harness exercises computeMenuActions on a U + accepted_update=1 row).
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
LIB_JS = (REPO / "app" / "web" / "static" / "lib" / "menu-actions.js").read_text()


def _replace_tdb_gate(src: str) -> str:
    """The conjunction guarding the REPLACE TDB menu push."""
    idx = src.index("'replace-with-themerrdb', 'REPLACE TDB'")
    return src[src.rindex("if (isThemerrDb", 0, idx):idx]


# ── live gate (app.js) ───────────────────────────────────────────────────────


def test_app_js_gate_relaxes_accepted_update_on_non_t_rows():
    gate = _replace_tdb_gate(APP_JS)
    # the bare suppression is gone …
    assert "&& !it.accepted_update &&" not in gate, (
        "bare !it.accepted_update wrongly hides REPLACE TDB on overridden rows")
    # … replaced by the srcLetter-qualified form.
    assert "(!it.accepted_update || srcLetter !== 'T')" in gate


def test_app_js_gate_keeps_youtube_url_and_lps_guards():
    # the v1.24.71 + v1.14.46 guards must survive the relaxation.
    gate = _replace_tdb_gate(APP_JS)
    assert "it.youtube_url" in gate
    assert "!lpsHasCanonical" in gate
    assert "sidecarOnly || isManualPlacement || isPlexAgent" in gate


# ── test-only mirror (lib/menu-actions.js) stays in sync on this dimension ────


def test_menu_actions_mirror_relaxes_accepted_update():
    # menu-actions.js is the node-harness mirror; keep the dimension we touched
    # consistent with the live gate so the harness reflects real behavior.
    assert '(!it.accepted_update || srcLetter !== "T")' in LIB_JS
