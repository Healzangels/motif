"""v1.14.42 — HOTFIX: closed-database crash + probe indeterminate widening.

## Bug 1 (CRITICAL): closed-database crash on api_recovery_options

the user repro: clicking INFO on Rubble & Crew triggered a 500
in api_recovery_options:

    sqlite3.ProgrammingError: Cannot operate on a closed database.

Root cause: v1.14.40 added `placement_for_lps = conn.execute(...)`
inside the `if not kind:` no-fail branch — but that branch lives
OUTSIDE the `with get_conn(db) as conn:` context manager. By the
time the LPS-detection query ran, conn was already closed.

Fix: move the `is_lps` derivation INSIDE the `with get_conn`
block (alongside m_available, p_available, revert_to_user_
available which all get computed there). The no-fail branch
post-block then just reads the pre-computed bool.

## Bug 2 (UX): probe shows red ✗ for ambiguous failures

the user repro: probe of `KWXXC228g24` returned `x Unknown error`
in red. The yt-dlp message was "This video is not available" —
which `classify_yt_dlp_error` deliberately maps to UNKNOWN (per
v1.12.88 reasoning: too ambiguous to declare VIDEO_REMOVED;
could be transient anti-bot, cookies needed, IP throttling, or
parser quirks).

But the probe response only flagged COOKIES_EXPIRED as
indeterminate. UNKNOWN and NETWORK_ERROR rendered as hard red
✗ — same visual signal as a definitively dead URL. Misleading.

Fix: widen the indeterminate set to include UNKNOWN and
NETWORK_ERROR. JS handler hint text becomes kind-aware (was
cookies-only). `needs_manual_override` still discriminates the
"definitively dead" set (video_private/removed/age/geo) — those
get red ✗ + failure_kind written as before.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── Bug 1: LPS detection inside with-block ──────────────────


def test_recovery_options_lps_detection_inside_conn_block():
    """v1.14.61 deleted the v1.14.42/v1.14.44 LPS-detection block
    entirely (zero downstream consumers post-v1.14.47; pure dead
    SQL roundtrip per /recovery-options call). The original
    v1.14.42 hotfix contract — "any LPS detection code must live
    INSIDE the with-block to avoid the closed-DB crash" — survives
    in the strictest form: there's no LPS detection code in the
    function at all to misplace.

    Pin: the v1.14.61 deletion marker is present + no `is_lps`
    derivation appears anywhere inside api_recovery_options."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("async def api_recovery_options(")
    # v1.22.71: slice to the function's actual end — the fixed 25000
    # window went stale when the rk-threading block grew the body.
    body = src[fn_anchor:src.index("\n    @app.", fn_anchor)]
    assert "v1.14.61: deleted the v1.14.42/v1.14.44 `is_lps`" in body
    # Strip comments so the marker mentioning is_lps doesn't trip.
    body_no_comments = "\n".join(
        line for line in body.splitlines()
        if not line.lstrip().startswith("#")
    )
    assert "is_lps = " not in body_no_comments
    assert "motif_has_placement = " not in body_no_comments


def test_no_fail_branch_no_longer_runs_conn_execute():
    """Sanity: the no-fail branch (`if not kind:`) must not
    execute `conn.execute(...)` directly. Pre-v1.14.42-hotfix
    a query lived in the no-fail branch outside the with-block.

    v1.14.47 emptied the no-fail branch entirely (returns
    options=[]). v1.14.61 deleted the v1.14.42 hotfix marker
    along with the LPS-detection block it guarded — the
    function no longer needs the marker because there's no
    in-with-block LPS derivation to point at. Contract still
    holds: zero `conn.execute` calls in the no-fail branch."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("async def api_recovery_options(")
    # v1.22.71: slice to the function's actual end — the fixed 25000
    # window went stale when the rk-threading block grew the body.
    body = src[fn_anchor:src.index("\n    @app.", fn_anchor)]
    branch_anchor = body.index("if not kind:")
    recipes_anchor = body.index("recipes: dict[str, list[dict]] = {", branch_anchor)
    branch_block = body[branch_anchor:recipes_anchor]
    # v1.19.35: the contract is "don't use the CLOSED outer `conn`,"
    # not "no conn-shaped substring anywhere." The v1.19.35 BK-state
    # probe opens a FRESH `bk_conn` via its own `with get_conn(db) as
    # bk_conn:` block — safe because the connection is alive for the
    # duration. Match `\bconn.execute` (word-boundary on conn) so the
    # `bk_conn.execute` calls don't trip the closed-conn guard.
    import re
    closed_conn_uses = re.findall(r'\bconn\.execute\b', branch_block)
    assert not closed_conn_uses, (
        f"no-fail branch still references the CLOSED outer `conn` "
        f"({len(closed_conn_uses)} occurrence(s)) — any such call "
        f"hits ProgrammingError. Open a fresh `with get_conn(db) as "
        f"bk_conn:` block as v1.19.35 did for BK-state detection."
    )


def test_is_lps_referenced_in_no_fail_branch():
    """v1.14.61 deleted the LPS-detection block in
    api_recovery_options. The contract this test originally pinned
    (LPS-aware no-fail branch) was already gone in v1.14.47 (the
    no-fail branch returns empty options); v1.14.55 M5 verified
    no downstream consumer; v1.14.61 deleted the dead producer.

    LPS detection still happens client-side via the JS
    `lpsState` predicate — pin that survives so the SOURCE-menu
    `placed` gate (the mirror-principle equivalent of
    `not is_lps`) keeps working."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # JS-side LPS detection survives.
    assert "lpsState" in js
    # SOURCE-menu LET PLEX SERVE gate uses `placed` (LPS rows
    # are !placed → button hides correctly without the server
    # needing to compute is_lps).
    lps_anchor = js.index("'purge-revert-to-plex', 'LET PLEX SERVE'")
    gate_start = js.rfind("if (", lps_anchor - 800, lps_anchor)
    gate = js[gate_start:lps_anchor]
    assert "placed" in gate


# ── Bug 2: probe indeterminate set widened ──────────────────


def test_probe_response_indeterminate_includes_unknown_and_network():
    """The probe endpoint must mark UNKNOWN and NETWORK_ERROR as
    indeterminate (alongside COOKIES_EXPIRED). Pre-fix only
    COOKIES_EXPIRED was marked, so UNKNOWN and NETWORK_ERROR
    rendered as hard red ✗."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("async def api_probe_tdb(")
    body = src[fn_anchor:fn_anchor + 15000]
    # The widened set.
    assert "FailureKind.COOKIES_EXPIRED" in body
    assert "FailureKind.NETWORK_ERROR" in body
    assert "FailureKind.UNKNOWN" in body
    # The new flag derivation.
    assert '"indeterminate": result in indeterminate_set' in body
    # The pre-fix narrow form must NOT survive.
    assert (
        '"indeterminate": result == FailureKind.COOKIES_EXPIRED'
        not in body
    )


def test_probe_response_v1_14_42_marker_present():
    """Marker comment captures the rationale (v1.12.88
    cross-reference for the UNKNOWN ambiguity)."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    assert "v1.14.42: indeterminate set widened" in src


# ── Bug 2: JS hint text kind-aware ──────────────────────────


def test_js_probe_handler_hint_text_kind_aware():
    """The single-row probe click handler's amber-? text must
    branch on res.kind — not assume cookies. Pre-fix the text
    was hardcoded to "(cookies needed for conclusive probe)"
    which lied for UNKNOWN/NETWORK_ERROR."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # The kind-aware branching for the inline result.
    inline_anchor = js.index('slot.textContent = `? ${res.message}${hint}`')
    block = js[inline_anchor - 1500:inline_anchor + 100]
    assert "res.kind === 'cookies_expired'" in block
    assert "res.kind === 'network_error'" in block
    assert "v1.14.42" in block
    # Pre-fix wording must NOT survive.
    pre_fix = '? ${res.message} (cookies needed for conclusive probe)'
    assert pre_fix not in js


def test_js_probe_helper_hint_text_kind_aware():
    """The LET PLEX SERVE probe-then-confirm helper's amber-?
    text must also be kind-aware. Same pre-fix issue."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    fn_anchor = js.index(
        "async function _probeAndConfirmLetPlexServe",
    )
    block = js[fn_anchor:fn_anchor + 15000]
    assert "v1.14.42" in block
    assert "res.kind === 'cookies_expired'" in block
    assert "res.kind === 'network_error'" in block
    # The pre-fix UNCONDITIONAL wording is gone — wording for
    # cookies_expired survives as one of the kind-aware branch
    # values (intentional). What must NOT survive is the old
    # template that always appended this text regardless of kind.
    pre_fix_template = (
        '? TDB URL probe inconclusive: ${res.message} '
        '(cookies needed for definitive answer)'
    )
    assert pre_fix_template not in block


# ── Reuse pin: needs_manual_override still discriminates ────


def test_probe_still_writes_failure_kind_for_manual_override_set():
    """The `needs_manual_override` discriminator (video_private/
    removed/age/geo) is what gates failure_kind writes — must
    stay unchanged. Indeterminate is a SEPARATE concept (does
    the user need more info before acting); needs_manual_override
    is "this row is definitively dead, write failure_kind"."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("async def api_probe_tdb(")
    body = src[fn_anchor:fn_anchor + 15000]
    # The gate stays.
    assert "result.needs_manual_override" in body
    # And the failure_kind write block is still there.
    assert "UPDATE themes SET failure_kind = ?" in body
