"""v0.51.120 — write the button-lock stashes BEFORE refreshTopbarStatus's
supersession bail (root-cause fix for the fast-switch flash).

refreshTopbarStatus bails at `if (refreshTopbarStatus._seq !== _myToken) return`
when a newer call supersedes it. The window stashes the library REFRESH lock
reads (__motif_global_enum_pipeline, __motif_enum_active/pending, collections)
used to be written only AFTER that bail, so continuous fast section-switching —
which supersedes every poll — starved them stale. v0.51.120 extracts a pure
`_deriveEnumStashes(q)` and writes the stashes BEFORE the bail, so even a
superseded poll keeps them fresh; the v0.51.119 freshness gate becomes an
always-fresh backstop.

The helper's globalEnumPipeline formula MUST stay identical to the main block's
inline one (they both drive the same button lock) — the drift guard here pins
that, since the two now live apart.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def _helper_src() -> str:
    start = JS.index("  function _deriveEnumStashes(q) {")
    end = JS.index("\n  async function refreshTopbarStatus", start)
    return JS[start:end]


def _run(q: dict) -> dict:
    quickjs = pytest.importorskip("quickjs")
    ctx = quickjs.Context()
    return json.loads(ctx.eval(
        _helper_src() + f"\nJSON.stringify(_deriveEnumStashes({json.dumps(q)}));"))


# ── the stashes are written BEFORE the supersession bail ─────


def test_stashes_written_before_the_supersession_bail():
    bail = "if (refreshTopbarStatus._seq !== _myToken) return;"
    before = JS[:JS.index(bail)]  # everything up to the FIRST (supersession) bail
    for stash in ("window.__motif_enum_active =",
                  "window.__motif_enum_pending =",
                  "window.__motif_plex_enum_collections_busy =",
                  "window.__motif_global_enum_pipeline =",
                  "window.__motif_enum_stash_ts = Date.now()"):
        assert stash in before, f"{stash} must be written before the supersession bail"
    # and the early write goes through the shared helper.
    assert "_deriveEnumStashes(stats.queue" in before


# ── drift guard: helper formula == main block's inline formula ──


def _global_pipeline_rhs(block: str) -> str:
    m = re.search(r"const globalEnumPipeline =\s*(.+?);", block, re.DOTALL)
    assert m, "globalEnumPipeline assignment not found"
    return re.sub(r"\s+", " ", m.group(1)).strip()


def test_helper_pipeline_formula_matches_the_main_block():
    helper_rhs = _global_pipeline_rhs(_helper_src())
    main_block = JS[JS.index("async function refreshTopbarStatus"):]
    main_rhs = _global_pipeline_rhs(main_block)
    assert helper_rhs == main_rhs, (
        "the _deriveEnumStashes globalEnumPipeline formula drifted from the main "
        "refreshTopbarStatus block — they must stay identical or the stash and the "
        "button's own lock diverge")


# ── behavioral: _deriveEnumStashes derives the right globalEnumPipeline ──


def test_pipeline_true_on_scan_all_cascade_in_flight():
    assert _run({"plex_enum_pipeline_in_flight": 2})["globalEnumPipeline"] is True


def test_pipeline_true_on_tdb_sync_with_auto_enum():
    assert _run({"themerrdb_sync_in_flight": 1})["globalEnumPipeline"] is True


def test_pipeline_false_on_tdb_sync_without_auto_enum():
    out = _run({"themerrdb_sync_in_flight": 1, "auto_enum_after_sync": False})
    assert out["globalEnumPipeline"] is False


def test_pipeline_true_when_two_tabs_enumerate():
    out = _run({"plex_enum_active": {"movies": {"standard": True},
                                     "tv": {"standard": True}}})
    assert out["globalEnumPipeline"] is True


def test_single_tab_enum_is_not_a_pipeline():
    # a plain per-tab refresh (one tab) must NOT light the global pipeline.
    out = _run({"plex_enum_active": {"tv": {"standard": True}}})
    assert out["globalEnumPipeline"] is False
    assert out["enumActive"] == {"tv": {"standard": True}}  # passthrough


def test_idle_queue_is_all_clear():
    out = _run({})
    assert out == {"enumActive": {}, "enumPending": {},
                   "collectionsEnumBusy": False, "globalEnumPipeline": False}
