"""v1.15.107 — silent-failure audit fixes.

Three independent log-visibility / breadcrumb fixes bundled from
a fresh silent-failure audit pass.

## What changed

**PlexClient._put log.debug → log.warning** — `_put` backs
`refresh()` and `analyze()`, the side-effecting PUTs that tell
Plex to re-scan a folder after motif's PURGE/DEL/UNMANAGE. When
those PUTs failed silently, the user's "PURGE didn't make my
row drop to '—'" had no breadcrumb in the default log. Bumped
to log.warning to mirror the existing `_get` log level.

**`_trigger_plex_item_refresh` rk-level failure log.debug →
log.warning** — same reasoning at a higher tier. The function
runs immediately after a destructive action specifically to
flush Plex's stale theme cache, so per-rk failures are not the
"noisy polling" kind — each one is "this destructive action
didn't fully land in Plex's view."

**`scanner.py` multi-candidate UPDATE breadcrumb** — when the
adopt-time UPDATE plex_items matches 0 rows (Unraid host-vs-
container path-domain mismatch we don't yet handle), log a
debug breadcrumb so `--log-level=debug` operators can diagnose
without instrumenting from outside. Sibling to v1.15.90/.91/.92/
.106 phantom-M class.
"""

from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
PLEX_PY = REPO / "app" / "core" / "plex.py"
API_PY = REPO / "app" / "web" / "api.py"
SCANNER_PY = REPO / "app" / "core" / "scanner.py"


# ── PlexClient._put log level ────────────────────────────────

def test_plex_put_logs_at_warning_level():
    """`_put` failures must surface at WARNING. Pre-fix DEBUG
    hid the systemic Plex-refresh-PUT failure case in
    `_trigger_plex_item_refresh` post-PURGE."""
    src = PLEX_PY.read_text()
    fn_start = src.index("def _put(")
    fn_end = src.index("\n    def ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    assert "log.warning(" in fn_body, (
        "v1.15.107: _put's except handler must log at WARNING — "
        "_put is the back-end of PlexClient.refresh / .analyze, "
        "the side-effecting calls used by _trigger_plex_item_refresh."
    )
    # Path should be part of the message so the operator can
    # correlate against the calling endpoint.
    assert "%s" in fn_body
    # The pre-fix log.debug call must be gone.
    assert 'log.debug("Plex PUT failed' not in fn_body


# ── _trigger_plex_item_refresh rk-level log level ────────────

def test_trigger_plex_item_refresh_rk_failure_at_warning():
    """Per-rk refresh failures must surface at WARNING — they
    indicate Plex's metadata cache wasn't flushed for that
    rating_key, which is the only path that clears phantom-P
    without waiting for the next plex_enum cycle."""
    import re
    src = API_PY.read_text()
    fn_start = src.index("def _trigger_plex_item_refresh(")
    # Scan for the next sibling function — handles both `def` and
    # `async def` at the same 4-space indentation level. The
    # function is nested inside a FastAPI route registration
    # closure, so siblings are also indented by 4.
    sibling = re.search(
        r"\n    (?:async )?def [a-z_]", src[fn_start + 1:])
    assert sibling, "No sibling function found after _trigger_plex_item_refresh"
    fn_end = fn_start + 1 + sibling.start()
    fn_body = src[fn_start:fn_end]
    assert "plex refresh failed for rk=" in fn_body, (
        "Expected the rk-level failure log line to remain."
    )
    # The post-fix line uses log.warning. Strip the pre-fix
    # log.debug from acceptable forms by asserting WARNING.
    rk_line_idx = fn_body.index("plex refresh failed for rk=")
    # Walk back to the log.X( call within ~80 chars.
    head = fn_body[max(0, rk_line_idx - 80):rk_line_idx]
    assert "log.warning(" in head, (
        "v1.15.107: per-rk refresh failure must log at WARNING."
    )


# ── scanner multi-candidate UPDATE breadcrumb ────────────────

def test_scanner_zero_rowcount_breadcrumb():
    """The multi-candidate UPDATE plex_items in scanner.py must
    log a debug breadcrumb when rowcount == 0 — that's the
    Unraid path-domain mismatch signature."""
    src = SCANNER_PY.read_text()
    # Anchor on the multi-candidate UPDATE block.
    anchor = src.index("UPDATE plex_items SET local_theme_file = 1")
    # Window covers both branches + the rowcount check.
    block = src[anchor:anchor + 2500]
    assert "cur.rowcount == 0" in block
    assert "log.debug(" in block
    # The breadcrumb should reference Unraid / path-domain — the
    # operator-facing context that makes the line actionable.
    assert "path-domain mismatch" in block or "Unraid" in block


def test_scanner_update_captures_cursor():
    """Both branches of the multi-candidate UPDATE must capture
    the cursor so cur.rowcount is reachable for the breadcrumb."""
    src = SCANNER_PY.read_text()
    anchor = src.index("UPDATE plex_items SET local_theme_file = 1")
    block = src[anchor - 200:anchor + 1500]
    # The capture pattern is `cur = conn.execute(`.
    assert block.count("cur = conn.execute(") >= 2, (
        "Both multi-candidate UPDATE branches must capture cursor."
    )
