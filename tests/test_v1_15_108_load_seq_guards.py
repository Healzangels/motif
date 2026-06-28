"""v1.15.108 — race-condition fixes for loadQueue + loadLibraries.

Second silent-failure audit pass surfaced two polling-race patterns
analogous to v1.13.28's loadLibrary seq-guard fix.

## loadQueue

`/queue` polls every 10s (app.js:12639). Eight click handlers
(cancel, ack, force-cancel, filter chip, retry, force-stop, etc.)
also kick `loadQueue()` to surface their action's effect. A click
landing 1-2s before the next setInterval tick could race — the
older response's `$('#jobs-body').innerHTML = ...` write at app.js
line 3240 (now 3250+ after the guard) would clobber the newer one.

Pre-fix the symptom: a job that just transitioned to 'cancelled'
flashes back to 'cancelling…' briefly when the stale poll lands
late.

## loadLibraries

Section-include toggle (3764) + libraries save (3892) both kick
`loadLibraries()` via setTimeout fallbacks. Rapid toggle sequences
(user toggling sections quickly) could fire concurrent loads;
older response wins on tbody innerHTML write.

## refreshTopbarStatus unawaited

Three click handlers (app.js:1232, 3890, 4020) called
`refreshTopbarStatus()` without `await` or `.catch()` inside an
outer try/catch — the unawaited promise rejection became an
unhandled rejection (console-only, not user-visible). Each
gained `.catch(() => {})` to be explicit about silent intent.

## Tests

Static guards on the new shape of each load function — they
must set a `._seq` counter on entry, capture the token, and
check the token before every DOM write.
"""

from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = REPO / "app" / "web" / "static" / "app.js"


def _fn_body(name: str) -> str:
    """Slice the body of a top-level `async function NAME(...)`
    block. Returns content from the opener line through the
    closing brace at the same indentation."""
    src = APP_JS.read_text()
    needle = f"async function {name}("
    start = src.index(needle)
    # Walk forward looking for the next `  async function ` or
    # `  function ` at the same 2-space indent. Sufficient since
    # the file is single-IIFE wrapped, so all helpers live at
    # 2-space indent.
    import re
    m = re.search(r"\n  (?:async )?function [a-zA-Z_]", src[start + 1:])
    if not m:
        return src[start:]
    return src[start:start + 1 + m.start()]


# ── loadQueue seq guard ─────────────────────────────────────

def test_load_queue_assigns_seq_token():
    body = _fn_body("loadQueue")
    assert "loadQueue._seq" in body
    assert "_myToken = loadQueue._seq" in body


def test_load_queue_checks_token_before_tbody_write():
    """The token check must precede the `$('#jobs-body').innerHTML
    = ...` write so a stale response can't clobber fresh state.
    v1.22.79: the function gained guards AFTER the events fetch too,
    so rindex (last occurrence) broke — anchor the guard that follows
    the JOBS fetch instead."""
    body = _fn_body("loadQueue")
    fetch_idx = body.index("data = await api('GET', path);")
    guard_idx = body.index(
        "if (loadQueue._seq !== _myToken) return;", fetch_idx)
    # v1.20.54: the render is captured into a const for the hash-skip;
    # the seq guard must still precede the (now const) render.
    tbody_idx = body.index("const _jobsHtml = data.jobs")
    assert fetch_idx < guard_idx < tbody_idx, (
        "loadQueue's seq token check must run between the jobs fetch "
        "and the tbody render — otherwise a stale response can clobber"
    )


# ── loadLibraries seq guard ──────────────────────────────────

def test_load_libraries_assigns_seq_token():
    body = _fn_body("loadLibraries")
    assert "loadLibraries._seq" in body
    assert "_myToken = loadLibraries._seq" in body


def test_load_libraries_checks_token_after_fetch():
    body = _fn_body("loadLibraries")
    # The token check must follow the api('GET', '/api/libraries')
    # call so a superseded fetch bails before tbody writes.
    fetch_idx = body.index("/api/libraries")
    guard_idx = body.index("if (loadLibraries._seq !== _myToken) return;", fetch_idx)
    assert guard_idx > fetch_idx


# ── refreshTopbarStatus unawaited fixes ──────────────────────

def test_refresh_topbar_status_unawaited_calls_have_catch():
    """The three click-handler sites (dry-run on, dry-run off,
    libraries save) must each chain `.catch(() => {})` so an
    unawaited rejection doesn't become an unhandled promise."""
    src = APP_JS.read_text()
    # Walk every `refreshTopbarStatus()` call site; each must be
    # followed (within the same statement) by `.catch(`.
    import re
    bare_calls = re.findall(
        r"refreshTopbarStatus\(\)\s*;",  # bare ; (no chain)
        src,
    )
    assert bare_calls == [], (
        f"v1.15.108: found {len(bare_calls)} bare "
        "refreshTopbarStatus() call(s) — each must chain "
        ".catch(() => {}) so unawaited rejections are explicit."
    )
