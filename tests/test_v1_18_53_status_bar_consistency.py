"""v1.18.53 — comprehensive status bar consistency audit.

the user's review of v1.18.52:

> "Can we the same sort of check making sure all aspects are
>  working as quickly as possible, we're getting the proper text,
>  getting % numbers when we should for different types of status
>  bar text and N queued items. Want them to all feel consistent
>  across the board."

v1.18.51 audited KIND_LABEL + TONE_BY_KIND for op_progress kinds.
v1.18.53 widens to:

  1. ALL kinds rendered in the topbar/drawer — both real
     op_progress kinds (db.py CHECK) AND synth queue/pending
     kinds emitted by progress.py.
  2. ALL three JS-side maps:
       - KIND_LABEL (drawer card title)
       - TONE_BY_KIND (color identity)
       - OP_MINI_PRIORITY (picker contention rank)
  3. The poll cadence: 1s when ANY op is running OR pending,
     10s idle. The poll detection uses `state.ops.some` over
     the full ops array (no kind filter), so any kind
     automatically gets the fast cadence — pin that.
  4. Documentation: CLAUDE.md has the Status bar consistency
     section so future sessions reading conventions see the
     rule.

Pre-v1.18.53 missing entries:

  - OP_MINI_PRIORITY had no entry for tvdb_bridge → fell to
    FALLBACK=99 → only won the slot when nothing else was
    running. Same gap for place_queue / scan_queue /
    refresh_queue / relink_queue / adopt_queue.

v1.18.53 fills the gaps + writes the audit guard.
"""

from __future__ import annotations

import re
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
OPS_JS = REPO / "app" / "web" / "static" / "ops.js"
DB_PY = REPO / "app" / "core" / "db.py"
PROGRESS_PY = REPO / "app" / "core" / "progress.py"
CLAUDE_MD = REPO / "CLAUDE.md"


# ── Helpers — enumerate every kind from canonical sources ────


def _real_op_progress_kinds() -> set[str]:
    """Real op_progress kinds from db.py's CREATE TABLE CHECK."""
    src = DB_PY.read_text()
    match = re.search(
        r"CREATE TABLE IF NOT EXISTS op_progress.*?"
        r"CHECK \(kind IN \(([^)]+)\)\)",
        src,
        re.DOTALL,
    )
    assert match, "Could not locate op_progress.kind CHECK in db.py"
    return set(re.findall(r"'([a-z_]+)'", match.group(1)))


# Synth kinds: progress.py emits these via _synthesize_queue_ops
# (for each job_type IN ('download','place','scan','refresh',
# 'relink','adopt')) + the pending-card synths.
SYNTH_QUEUE_JOB_TYPES = (
    "download", "place", "scan", "refresh", "relink", "adopt",
)
SYNTH_PENDING_KINDS = ("tdb_sync_pending", "plex_enum_pending")


def _all_synth_kinds() -> set[str]:
    return {
        f"{jt}_queue" for jt in SYNTH_QUEUE_JOB_TYPES
    } | set(SYNTH_PENDING_KINDS)


def _all_topbar_kinds() -> set[str]:
    """Every kind a user might see in the topbar mini-bar or drawer
    card — real ops + synths."""
    return _real_op_progress_kinds() | _all_synth_kinds()


def _extract_map_keys(map_name: str) -> set[str]:
    """Parse a JS object literal from ops.js and return its keys."""
    src = OPS_JS.read_text()
    start_marker = f"const {map_name} = {{"
    start = src.index(start_marker)
    # Find the matching closing }; ignoring nested objects (none of
    # these maps have nested object values).
    end = src.index("};", start)
    body = src[start:end]
    return set(re.findall(r"^\s*([a-z_]+)\s*:", body, re.MULTILINE))


# ── Audit: every kind appears in all three maps ──────────────


def test_every_kind_has_kind_label():
    """KIND_LABEL must cover every kind a user might see. Without
    it, the drawer card renders raw `// bulk_lps` instead of
    `// BULK LET PLEX SERVE`."""
    expected = _all_topbar_kinds()
    actual = _extract_map_keys("KIND_LABEL")
    missing = expected - actual
    assert not missing, (
        f"v1.18.53: KIND_LABEL missing entries for kind(s): {missing}"
    )


def test_every_kind_has_tone():
    """TONE_BY_KIND must cover every kind. Without it the drawer
    card has no tone class — looks generic, no color identity."""
    expected = _all_topbar_kinds()
    actual = _extract_map_keys("TONE_BY_KIND")
    missing = expected - actual
    assert not missing, (
        f"v1.18.53: TONE_BY_KIND missing entries for kind(s): {missing}"
    )


def test_every_kind_has_mini_priority():
    """OP_MINI_PRIORITY must cover every kind. Without an explicit
    entry the kind inherits FALLBACK=99 (lowest priority) and
    only wins the mini-bar slot when nothing else is running.
    Explicit entries document intent + prevent silent gaps."""
    expected = _all_topbar_kinds()
    actual = _extract_map_keys("OP_MINI_PRIORITY")
    missing = expected - actual
    assert not missing, (
        f"v1.18.53: OP_MINI_PRIORITY missing entries for kind(s): "
        f"{missing}. Without explicit priorities these kinds always "
        f"lose the contended mini-bar slot — pick a priority "
        f"rank that reflects user attention (download=1 highest, "
        f"queue synths=6 lowest)."
    )


# ── Audit: tvdb_bridge specifically (v1.18.53 fix) ──────────


def test_tvdb_bridge_has_priority():
    """tvdb_bridge specifically — pre-v1.18.53 it was a real
    op_progress kind that mutated row state but lacked an
    explicit priority. Pin so a future cleanup doesn't drop it."""
    src = OPS_JS.read_text()
    pri_start = src.index("const OP_MINI_PRIORITY = {")
    pri_end = src.index("};", pri_start)
    pri_body = src[pri_start:pri_end]
    assert re.search(r"tvdb_bridge:\s+\d+,", pri_body), (
        "v1.18.53: tvdb_bridge must have an explicit OP_MINI_PRIORITY "
        "entry — it's a real op_progress kind that mutates row state"
    )


def test_queue_synth_kinds_have_priorities():
    """All 6 synth queue kinds (download_queue + 5 others) must
    have explicit priorities. download_queue was already at 1;
    the 5 others were left at FALLBACK pre-v1.18.53. Pin all 6
    so future refactors can't silently drop them."""
    src = OPS_JS.read_text()
    pri_start = src.index("const OP_MINI_PRIORITY = {")
    pri_end = src.index("};", pri_start)
    pri_body = src[pri_start:pri_end]
    for jt in SYNTH_QUEUE_JOB_TYPES:
        kind = f"{jt}_queue"
        assert re.search(rf"{kind}:\s+\d+,", pri_body), (
            f"v1.18.53: {kind} must have an explicit "
            f"OP_MINI_PRIORITY entry"
        )


# ── Audit: poll cadence covers all kinds ─────────────────────


def test_poll_cadence_kind_agnostic():
    """The ops.js `poll()` cadence-bump must check ALL ops via
    `state.ops.some` without filtering on kind, so every new
    kind automatically inherits the 1s cadence when active."""
    src = OPS_JS.read_text()
    # Find the cadence-decision block.
    poll_idx = src.index("async function poll()")
    # v0.51.17 (audit #13): widened 3000→4000 — the anti-fork entry block
    # (pending-timer clear + pollInFlight latch) lands ahead of the cadence.
    body = src[poll_idx:poll_idx + 4000]
    # The `running` and `pending` flags must read across the whole
    # ops array, not filter on specific kinds.
    assert "state.ops.some" in body, (
        "v1.18.53: poll cadence must use state.ops.some (no kind filter)"
    )
    # The interval transition collapses to 1000ms while any op is
    # running/pending OR the drawer is open (v1.21.30 added drawerOpen so
    # an op started just after opening the drawer shows up within 1s).
    assert "(running || pending || state.drawerOpen) ? 1000 : 10000" in body, (
        "v1.18.53/v1.21.30: poll interval must collapse to 1000ms while "
        "any op is running/pending or the drawer is open"
    )


def test_drawer_open_forces_fast_poll():
    """Opening the drawer drops the poll cadence to 1s even when
    idle — so a user inspecting the drawer sees fresh data.
    Mirrors the running-ops cadence; pin so a refactor can't
    silently widen it."""
    src = OPS_JS.read_text()
    fn_idx = src.index("function openDrawer()")
    body = src[fn_idx:fn_idx + 1100]
    assert "state.pollInterval = 1000" in body, (
        "v1.18.53: openDrawer must drop pollInterval to 1000ms"
    )


# ── Audit: drawer counter % shown for real-bar ops ──────────


def test_drawer_counter_pct_renders_when_useRealBar():
    """v1.18.51's drawer counter % chip must remain — the
    `showPct = useRealBar && pct != null` rule is the single
    source of truth for "should the drawer show a numerical
    %?". Pin so a future cleanup of the conditional doesn't
    silently drop the % display."""
    src = OPS_JS.read_text()
    assert "showPct" in src
    assert "useRealBar" in src
    assert "op-card-counter-pct" in src


def test_topbar_mini_pct_renders_when_not_indeterminate():
    """The topbar mini-bar shows the same % when the bar isn't
    indeterminate. Pin so the dual-surface % consistency
    the user asked about in v1.18.51 stays intact."""
    src = OPS_JS.read_text()
    # The mini-bar HTML includes the pct toFixed(0) + '%' branch.
    assert "pct.toFixed(0) + '%'" in src


# ── Audit: overflow `+N QUEUED` covers all queueable kinds ───


def test_overflow_chip_surfaces_plex_enum_pending():
    """Pin the existing v1.14.84 plex_enum_pending overflow
    surfacing — most common case (user clicks REFRESH on
    multiple tabs)."""
    src = OPS_JS.read_text()
    assert "kind === 'plex_enum_pending'" in src


def test_overflow_chip_surfaces_tdb_sync_pending():
    """Pin v1.14.90 — when a sync is queued behind a running
    plex_enum the badge shows SYNC QUEUED."""
    src = OPS_JS.read_text()
    assert "kind === 'tdb_sync_pending'" in src


def test_overflow_chip_surfaces_download_queue_depth():
    """Pin v1.15.5 — when downloads are queued behind a running
    one the badge shows the queue depth."""
    src = OPS_JS.read_text()
    assert "kind === 'download_queue'" in src


def test_overflow_chip_surfaces_live_probes():
    """Pin v1.15.30 — probes running in the background while
    another op holds the mini-bar slot get a suffix label."""
    src = OPS_JS.read_text()
    # The probeKinds Set lists the three probe-ish kinds.
    assert "probeKinds" in src
    for k in ("bulk_probe_tdb", "reprobe_plex_themes", "bulk_lps"):
        # Each must appear inside the Set definition.
        assert f"'{k}'" in src


# ── Audit: documentation landed in CLAUDE.md ────────────────


def test_claude_md_documents_status_bar_consistency():
    """The Status bar consistency section must exist in CLAUDE.md
    so future sessions reading the conventions see the rule."""
    src = CLAUDE_MD.read_text()
    flat = " ".join(src.split())
    assert "Status bar consistency" in flat, (
        "v1.18.53: CLAUDE.md must include Status bar consistency section"
    )
    # Key concepts the section must reference.
    assert "KIND_LABEL" in flat
    assert "TONE_BY_KIND" in flat
    assert "OP_MINI_PRIORITY" in flat
    # Audit guard reference so the rule and the test link up.
    assert "test_v1_18_53_status_bar_consistency" in flat


# ── Audit: the audit-of-the-audit ────────────────────────────


def test_audit_kinds_match_canonical_sources():
    """Sanity check the audit's own assumption — that
    SYNTH_QUEUE_JOB_TYPES matches what progress.py actually
    emits. If a new job_type lands in progress.py's queue
    synthesis, this audit needs updating."""
    src = PROGRESS_PY.read_text()
    # The job_type filter list lives in _synthesize_queue_ops.
    match = re.search(
        r"job_type IN \(([^)]+)\)",
        src,
    )
    assert match, "Could not locate job_type filter in progress.py"
    declared = set(re.findall(r"'([a-z_]+)'", match.group(1)))
    audit_set = set(SYNTH_QUEUE_JOB_TYPES)
    missing = declared - audit_set
    assert not missing, (
        f"v1.18.53 audit drift: progress.py queue synthesis emits "
        f"job_types {missing} that aren't in this test's "
        f"SYNTH_QUEUE_JOB_TYPES. Update the constant."
    )


def test_version_marker_present():
    """v1.18.53 marker landed on the changed sites."""
    assert "v1.18.53" in OPS_JS.read_text()
    assert "v1.18.53" in CLAUDE_MD.read_text()
