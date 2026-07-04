"""v1.15.82 — kill the op-mini bar flip on concurrent ops.

the user: "When a download is ongoing and a refresh is queued at
the same time it flips between showing the download progress
and the refresh. Let's have this treated similar to others
where the extra Pill shows the notification of the other task
on going. Let's have download take prio followed by refresh,
sync themerrdb, prob url, probe plex sidecar."

Root cause: both ops.js (client-side picker) and api.py's
_topbar_ssr_state sorted concurrent running ops by
updated_at DESC. Two real-running ops both ticking their
updated_at means whichever ticks last that frame wins the
single mini-bar slot — visible flip every poll.

v1.15.82 replaces the sort with a stable priority order
(downloads > refresh > sync > probe_tdb > probe_plex_themes).
The lower-priority op falls into the existing
#op-mini-overflow "+N ops" pill (v1.12.109 wiring already in
place). updated_at remains as a tiebreaker within the same
kind for deterministic ordering.

Both client + SSR must use the same priority — otherwise the
SSR-baked op label flashes briefly on nav before JS reconciles
to a different op.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
OPS_JS = REPO / "app" / "web" / "static" / "ops.js"
API_PY = REPO / "app" / "web" / "api.py"


# ── 1. ops.js client picker ─────────────────────────────────


def test_ops_js_declares_priority_map():
    """ops.js must declare an OP_MINI_PRIORITY map covering
    the user's order: download > refresh > sync > probe tdb >
    probe plex sidecar."""
    js = OPS_JS.read_text()
    # Map must exist.
    assert "OP_MINI_PRIORITY" in js, (
        "v1.15.82: ops.js must declare OP_MINI_PRIORITY map for "
        "the priority-ordered mini-bar picker"
    )
    # Find the declaration block.
    decl_start = js.index("const OP_MINI_PRIORITY")
    decl_end = js.index("}", decl_start)
    decl = js[decl_start:decl_end]
    # Required entries with the user's order.
    # v0.51.46 (the user): tdb_sync now outranks plex_enum (was 2 vs 3) so
    # SYNC THEMERRDB holds the contended slot over a concurrent REFRESH PLEX.
    pairs = [
        ("download_queue", 1),
        ("tdb_sync", 2),
        ("plex_enum", 3),
        ("bulk_probe_tdb", 4),
        ("reprobe_plex_themes", 5),
    ]
    import re
    for kind, want_prio in pairs:
        m = re.search(rf"{kind}:\s*(\d+)", decl)
        assert m, f"v1.15.82: OP_MINI_PRIORITY missing entry for {kind}"
        got = int(m.group(1))
        assert got == want_prio, (
            f"v1.15.82: OP_MINI_PRIORITY[{kind}] = {got}, want "
            f"{want_prio} (the user's order: download > refresh > "
            "sync > probe tdb > probe plex sidecar)"
        )


def test_ops_js_picker_uses_priority_sort():
    """The mini-bar picker must sort by OP_MINI_PRIORITY (not
    updated_at) so concurrent ops don't flip the displayed bar.
    updated_at is allowed as a SECONDARY tiebreaker."""
    js = OPS_JS.read_text()
    fn_start = js.index("function renderTopbar(")
    fn_end = js.index("function ", fn_start + 1)
    body = js[fn_start:fn_end]
    # The picker must reference the priority map.
    assert "OP_MINI_PRIORITY[a.kind]" in body, (
        "v1.15.82: picker must look up each candidate's priority "
        "from OP_MINI_PRIORITY[a.kind] in its sort callback"
    )


def test_ops_js_pending_companions_share_real_op_priority():
    """plex_enum_pending + tdb_sync_pending are JS-side synth
    cards for queued ops. They must share their REAL parent's
    priority — otherwise a queued sync card could preempt a
    real download in the slot competition (v1.14.65 ghost-flip
    pattern, different kind)."""
    js = OPS_JS.read_text()
    decl_start = js.index("const OP_MINI_PRIORITY")
    decl_end = js.index("}", decl_start)
    decl = js[decl_start:decl_end]
    # plex_enum and plex_enum_pending must have the same priority.
    import re
    real_pe = int(re.search(r"plex_enum:\s*(\d+)", decl).group(1))
    pending_pe = int(re.search(r"plex_enum_pending:\s*(\d+)", decl).group(1))
    assert real_pe == pending_pe, (
        "v1.15.82: plex_enum_pending must share plex_enum's "
        "priority so a queued companion doesn't compete with the "
        "real running parent"
    )
    real_sync = int(re.search(r"tdb_sync:\s*(\d+)", decl).group(1))
    pending_sync = int(re.search(r"tdb_sync_pending:\s*(\d+)", decl).group(1))
    assert real_sync == pending_sync


# ── 2. SSR picker in api.py mirrors the priority ────────────


def test_topbar_ssr_query_uses_priority_case():
    """_topbar_ssr_state's op_progress query must ORDER BY a
    priority CASE before updated_at — otherwise SSR + JS pick
    different ops on a nav with concurrent running ops, and the
    bar flashes the SSR pick before JS reconciles."""
    src = API_PY.read_text()
    fn_start = src.index("def _topbar_ssr_state(")
    fn_end = src.index("def _dashboard_ssr_state(", fn_start)
    body = src[fn_start:fn_end]
    # Must include a CASE-WHEN ordering on kind.
    assert "ORDER BY" in body and "CASE" in body, (
        "v1.15.82: _topbar_ssr_state must ORDER BY a CASE kind "
        "expression so SSR uses the same priority order as ops.js"
    )
    # The same five kinds we wrap on the client must appear in
    # the CASE (downloads aren't queryable here — they're a JS
    # synth — but the rest should match).
    for kind in ("plex_enum", "tdb_sync", "bulk_probe_tdb",
                 "reprobe_plex_themes"):
        assert f"'{kind}'" in body, (
            f"v1.15.82: SSR CASE must include {kind} so its "
            "priority matches the client-side picker"
        )


def test_topbar_ssr_priority_matches_client_order():
    """The SSR CASE order must agree with ops.js's
    OP_MINI_PRIORITY for the kinds the SSR can see. plex_enum
    < tdb_sync < bulk_probe_tdb < reprobe_plex_themes."""
    import re
    src = API_PY.read_text()
    fn_start = src.index("def _topbar_ssr_state(")
    fn_end = src.index("def _dashboard_ssr_state(", fn_start)
    body = src[fn_start:fn_end]
    # Parse out the CASE → priority value mapping.
    case_start = body.index("ORDER BY")
    case_end = body.index("updated_at DESC", case_start)
    case_block = body[case_start:case_end]
    def _prio(kind: str) -> int:
        m = re.search(rf"WHEN\s+'{kind}'\s+THEN\s+(\d+)", case_block)
        assert m, f"v1.15.82: SSR CASE missing WHEN '{kind}'"
        return int(m.group(1))
    # v0.51.46 (the user): tdb_sync (sync themerrdb) now ranks ABOVE plex_enum
    # (refresh) so SYNC THEMERRDB holds the slot over a concurrent REFRESH PLEX.
    assert _prio("tdb_sync") < _prio("plex_enum"), (
        "v0.51.46: SSR priority must rank tdb_sync (sync) above plex_enum "
        "(refresh) so the sync holds the slot when both run"
    )
    assert _prio("plex_enum") < _prio("bulk_probe_tdb")
    assert _prio("bulk_probe_tdb") < _prio("reprobe_plex_themes")
