"""v1.15.23 — cookies filter fix + REPROBE FAILURES post-flush
activity update + axis-token cross-reference test.

## Bugs

**Bug 2 (the user)**: "neither the new yellow tdb pill or the
yellow lock filter properly, we should make a test to catch
this in the future since this happens frequently"

Clicking the cookies STATUS pill (⚿) in the library STATUS
filter row didn't filter results. Root cause: api.py:7301
defines `_pset(attn_pills, {"fail", "update", "mismatch",
"await", "broken"})` — the valid set was missing "cookies".
v1.15.17 added cookies to:
  - JS pillAxes values list (click handler)
  - JS deep-link parser values
  - Server SQL branch (`elif p == "cookies":`)
  - library.html template button
But missed THIS server-side parse validation set. `_pset`
silently drops any value not in the valid set, so
`?attn_pills=cookies` parsed to an empty set, no filter
ran, all rows returned.

Same mirror-principle leak class the v1.14.11 comment at
api.py:~7295 documented for the `Pp` token. The cross-
reference test added here catches any future axis-token
addition that misses one surface.

**Bug 1 (the user)**: "wouldn't cleared be also 10 if i was
doing a reprobe of failed so the 10 alive were most likely
also failed before?"

After cancelling a REPROBE FAILURES run with 10 probed +
10 alive, LIVE OPS showed `cleared=0`. Root cause: the
in-loop activity update fires at done % 10, but
_flush_batch only runs every BATCH_SIZE=50 results. So
at done=10 the activity message reflects an unflushed
state where n_alive_cleared is still 0. After cancel,
_flush_batch fires + writes the alive-clears, but no
follow-up activity update reflects the new counts.
The DB ends up correct; the LIVE OPS card is stale.

## Fix

1. **Cookies token**: add "cookies" to the
   `_pset(attn_pills, ...)` valid set.

2. **Cross-reference test**: every JS pillAxes axis value
   must be in the corresponding server _pset valid set.
   Catches the mirror-principle leak at static-test time.

3. **Post-flush activity helper**: new `_post_terminal_activity()`
   inside `_bulk_probe_tdb_run` that posts the current
   counters reflecting the post-flush state. Called after
   `_flush_batch()` in:
     - Cancel path (with "Cancelled at" prefix)
     - Natural-end path (no prefix)
     - Bail (also routes through natural-end after break;
       prefix becomes "Bailed at" + "wait ~1h" suffix via
       the `rate_limit_bail` flag)
"""
from __future__ import annotations

import re
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"
APP_JS = REPO / "app" / "web" / "static" / "app.js"


# ── Bug 2: cookies token in server _pset ──────────────────────


def test_attn_pills_pset_includes_cookies():
    """Pin the v1.15.23 fix: server-side _pset(attn_pills, ...)
    valid set must include "cookies". Pre-fix the cookies STATUS
    filter pill silently no-op'd because _pset dropped the value
    as invalid."""
    src = API_PY.read_text()
    # Find the attn_set parse line.
    parse_anchor = src.index("attn_set = _pset(attn_pills,")
    parse_block = src[parse_anchor:parse_anchor + 400]
    assert '"cookies"' in parse_block, (
        "v1.15.23: 'cookies' must be in the attn_pills valid set "
        "(server-side _pset call) — pre-fix the cookies STATUS "
        "filter pill silently no-op'd because _pset dropped the "
        "unknown token"
    )
    # All five pre-existing tokens must still be present.
    for tok in ('"fail"', '"update"', '"mismatch"', '"await"', '"broken"'):
        assert tok in parse_block, (
            f"v1.15.23 fix must not have dropped {tok} from the "
            f"attn_pills valid set"
        )


# ── Cross-reference test (the REGRESSION GUARD the user asked for) ─


def _extract_pill_axes_values(axis_attr: str) -> set[str]:
    """Pull the values list for a pillAxes entry by attr name.
    Mirrors the v1.14.58 cross-reference test extractor."""
    src = APP_JS.read_text()
    # Find the pillAxes block.
    block_anchor = src.index("const pillAxes = [")
    block_end = src.index("];", block_anchor)
    block = src[block_anchor:block_end]
    # Find the entry by attr name.
    needle = f"attr: '{axis_attr}',"
    idx = block.find(needle)
    if idx == -1:
        return set()
    sub = block[idx:idx + 800]
    m = re.search(r"values:\s*\[\s*([^\]]+?)\s*\]", sub, flags=re.DOTALL)
    if not m:
        return set()
    raw = m.group(1)
    # Tokens are quoted strings.
    return set(re.findall(r"'([^']+)'", raw))


def _extract_server_pset_values(pset_var: str) -> set[str]:
    """Pull the server-side _pset(VAR, {...}) valid set by var
    name. Returns the literal token set."""
    src = API_PY.read_text()
    # Find the _pset call assignment.
    needle = f"{pset_var} = _pset("
    idx = src.find(needle)
    if idx == -1:
        return set()
    # Walk forward to the closing brace of the literal set.
    sub = src[idx:idx + 1200]
    # Pattern: _pset(attn_pills, {"fail", "cookies", ...})
    m = re.search(r"_pset\([^,]+,\s*\{([^}]+)\}\)", sub, flags=re.DOTALL)
    if not m:
        return set()
    raw = m.group(1)
    return set(re.findall(r'"([^"]+)"', raw))


def test_attn_pill_tokens_agree_across_client_and_server():
    """the user: 'we should make a test to catch this in the
    future since this happens frequently'.

    Every value in the JS pillAxes 'attnPill' values list MUST
    appear in the server-side _pset valid set for attn_pills.
    Otherwise the server silently drops the token (mirror-
    principle leak — same class as the v1.14.11 'Pp' bug noted
    in api.py:~7295).

    This test sits next to the v1.14.58 cross-reference suite
    but operates on a different layer: v1.14.58 checks
    template ↔ JS axis ↔ JS deep-link parser, this test checks
    JS axis ↔ server parse validation. Both axes are needed for
    a token addition to actually filter."""
    js_tokens = _extract_pill_axes_values("attnPill")
    server_tokens = _extract_server_pset_values("attn_set")
    assert js_tokens, "Failed to extract JS attnPill values"
    assert server_tokens, "Failed to extract server attn_set values"
    missing_on_server = js_tokens - server_tokens
    extra_on_server = server_tokens - js_tokens
    assert not missing_on_server, (
        f"v1.15.23: JS attnPill axis has tokens the server "
        f"_pset doesn't recognize → those filter pills silently "
        f"no-op. Missing on server: {sorted(missing_on_server)}"
    )
    assert not extra_on_server, (
        f"v1.15.23: server _pset accepts tokens the JS axis "
        f"doesn't expose → operator can't reach those server "
        f"branches via the UI. Extra on server: "
        f"{sorted(extra_on_server)}"
    )


# ── Bug 1: post-flush activity update ────────────────────────


def test_post_terminal_activity_helper_defined():
    """The helper must exist inside _bulk_probe_tdb_run so it
    closes over the current counters. Pin the function name so
    a future refactor can't silently rename it."""
    src = API_PY.read_text()
    fn_start = src.index("def _bulk_probe_tdb_run(")
    fn_end = src.index("\ndef ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    assert "def _post_terminal_activity(" in fn_body, (
        "v1.15.23: _post_terminal_activity helper must live inside "
        "_bulk_probe_tdb_run (closes over local counters)"
    )


def test_post_terminal_activity_called_in_cancel_path():
    """Cancel path must call _post_terminal_activity AFTER
    _flush_batch so the LIVE OPS card reflects the post-flush
    cleared count. Pre-fix the cancel path was: pool.shutdown
    → _flush_batch → finish_progress (no activity update)."""
    src = API_PY.read_text()
    fn_start = src.index("def _bulk_probe_tdb_run(")
    fn_end = src.index("\ndef ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    cancel_anchor = fn_body.index("if _cancel():")
    cancel_block = fn_body[cancel_anchor:cancel_anchor + 1500]
    # Both _flush_batch + _post_terminal_activity must be present
    # AND _flush_batch must precede _post_terminal_activity.
    flush_idx = cancel_block.index("_flush_batch()")
    post_idx = cancel_block.index("_post_terminal_activity()")
    finish_idx = cancel_block.index("op_progress.finish_progress(")
    assert flush_idx < post_idx < finish_idx, (
        "v1.15.23: cancel path order must be _flush_batch → "
        "_post_terminal_activity → finish_progress"
    )


def test_post_terminal_activity_called_after_natural_end_flush():
    """Natural-end path (after the for loop) must call
    _post_terminal_activity AFTER the final _flush_batch. This
    also covers the bail break-out which falls through here."""
    src = API_PY.read_text()
    fn_start = src.index("def _bulk_probe_tdb_run(")
    fn_end = src.index("\ndef ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    # The natural-end _flush_batch is the one OUTSIDE the for
    # loop but still inside the with-pool block (pre-fix it was
    # the only flush after the loop ended).
    # Find the v1.15.23 marker comment.
    marker_anchor = fn_body.index("# v1.15.23: post-flush activity update so")
    block = fn_body[marker_anchor - 200:marker_anchor + 800]
    assert "_flush_batch()" in block
    assert "_post_terminal_activity()" in block


def test_post_terminal_activity_includes_bail_context_when_bail():
    """When rate_limit_bail is True, the activity message must
    include the 'Bailed at X/N — ... wait ~1h' context. the user's
    v1.15.14 bail UX needs to survive the post-flush update."""
    src = API_PY.read_text()
    fn_start = src.index("def _bulk_probe_tdb_run(")
    fn_end = src.index("\ndef ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    helper_anchor = fn_body.index("def _post_terminal_activity(")
    helper_end = fn_body.index("with ThreadPoolExecutor", helper_anchor)
    helper_body = fn_body[helper_anchor:helper_end]
    assert "rate_limit_bail" in helper_body
    assert "Bailed at" in helper_body
    assert "wait ~1h" in helper_body


def test_post_terminal_activity_includes_cancel_context_when_cancelled():
    """When cancelled_early is True, the activity message must
    include the 'Cancelled at X/N' context so the operator
    sees the cancellation distinguished from a natural end."""
    src = API_PY.read_text()
    fn_start = src.index("def _bulk_probe_tdb_run(")
    fn_end = src.index("\ndef ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    helper_anchor = fn_body.index("def _post_terminal_activity(")
    helper_end = fn_body.index("with ThreadPoolExecutor", helper_anchor)
    helper_body = fn_body[helper_anchor:helper_end]
    assert "cancelled_early" in helper_body
    assert "Cancelled at" in helper_body
