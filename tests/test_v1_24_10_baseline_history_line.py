"""v1.24.10 — synthesize a baseline "how it got here" HISTORY line.

The INFO card's HISTORY section is built from the per-row `events` table, which
the scheduler prunes after 30 days (`_prune_events`). So a row that was themed
automatically and never touched since loses its HISTORY section entirely once
its download/place events age out — the card shows no provenance timeline at
all. the user's "A Knight of the Seven Kingdoms" diagnostic: themed ~May 20, events
gone by ~June 21.

Fix (consistency, not a bug): when there are no live events, render a single
durable origin line synthesized from the row's STORED provenance — source_kind
(via the existing `_humanSourceKind`), provenance (auto/manual), and the
download timestamp (`local_files.downloaded_at`, now surfaced by api_item,
falling back to `motif_added_at`). None of those are pruned, so every row motif
owns a local file for keeps a one-line "how it got here" forever. The history
degrades gracefully: detailed event timeline → one-line origin after the prune.

`renderRowHistory` gained a `baseline` param:
  - events present              → real event timeline + CLEAR button (unchanged)
  - no events, baseline present → synthesized ORIGIN row, NO CLEAR (nothing
                                  stored to clear)
  - no events, no baseline      → '' (pure-P / SRC=— rows with no motif file
                                  stay sectionless, as before)

Backup-only rows tag the line "· backup (deferring to Plex)" so it doesn't read
as an applied theme (mirrors v1.24.9).

This file mixes static-source guards with a quickjs harness that actually
EXECUTES renderRowHistory across all three branches (the function depends only
on a `htmlEscape` global + JS builtins, so it runs standalone).
"""
from __future__ import annotations

from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


# ── Server contract: api_item surfaces the durable download timestamp ──

def test_api_item_returns_downloaded_at_via_select_star():
    """The synthesized line's date anchor. api_item builds data.local_file
    from `SELECT * FROM local_files` (api.py ~14413), so downloaded_at — a
    native local_files column — is already on the payload. No explicit column
    is needed; if this SELECT ever narrows to named columns, the baseline date
    (and every other lf field the INFO card reads) silently disappears."""
    assert "SELECT * FROM local_files" in API_PY


# ── Static guards: the wiring in openInfoDialog + renderRowHistory ──

def test_openinfodialog_builds_baseline_from_provenance():
    """baselineHistory is derived from the local_files provenance fields and
    passed as the 4th arg to renderRowHistory."""
    assert "const baselineHistory = lf" in APP_JS
    assert "label: _humanSourceKind(lf.source_kind || '')," in APP_JS
    # v1.24.11: the date is pre-formatted via the card-wide fmt.timeAuto so
    # the ORIGIN row matches the other card timestamps (not bare toLocaleString).
    assert "at: fmt.timeAuto(lf.downloaded_at || data.motif_added_at || '')," in APP_JS
    assert "backupOnly: lfIsBackupOnly," in APP_JS
    assert "renderRowHistory(\n      events, t.media_type, t.tmdb_id, baselineHistory)" in APP_JS


def test_render_row_history_takes_baseline_param():
    assert "function renderRowHistory(events, mediaType, tmdbId, baseline) {" in APP_JS
    # empty only when neither events nor a baseline exist
    assert "if (!hasEvents && !baseline) return '';" in APP_JS


# ── Behavioral: run renderRowHistory in quickjs across all branches ──

def _render_fn_src() -> str:
    start = APP_JS.index("function renderRowHistory(")
    end = APP_JS.index("function renderPendingUpdateDiff(", start)
    return APP_JS[start:end]


def _run(call: str) -> str:
    quickjs = pytest.importorskip("quickjs")
    ctx = quickjs.Context()
    harness = (
        "var htmlEscape = function(s){return String(s===undefined||s===null?'':s)"
        ".replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')"
        ".replace(/\"/g,'&quot;').replace(/'/g,'&#39;');};\n"
        + _render_fn_src() + "\n"
        + call
    )
    return ctx.eval(harness)


# v1.24.11: openInfoDialog pre-formats `at` via fmt.timeAuto, so the harness
# passes a card-formatted display string (renderRowHistory just escapes it).
_BASELINE = ("{label:'Downloaded from ThemerrDB URL',provenance:'auto',"
             "at:'May 20, 2026, 12:00 PM',backupOnly:false}")


def test_baseline_row_renders_when_events_empty():
    out = _run(f"renderRowHistory([], 'tv', 387, {_BASELINE});")
    assert "// HISTORY" in out
    assert "ORIGIN" in out
    assert "Downloaded from ThemerrDB URL" in out
    assert "auto" in out
    assert "origin" in out  # the summary note
    # v1.24.11: the pre-formatted date is rendered verbatim (escaped)
    assert "May 20, 2026, 12:00 PM" in out
    # synthesized rows have nothing stored to clear
    assert 'data-clear="events"' not in out
    assert "history-row-synthetic" in out


def test_no_section_when_no_events_and_no_baseline():
    out = _run("renderRowHistory([], 'tv', 387, null);")
    assert out == "", "pure-P / no-file rows must stay sectionless"


def test_backup_only_baseline_is_flagged_not_applied():
    bk = _BASELINE.replace("backupOnly:false", "backupOnly:true")
    out = _run(f"renderRowHistory([], 'tv', 387, {bk});")
    assert "backup (deferring to Plex)" in out


def test_real_events_still_win_over_baseline():
    """When events exist, the live timeline + CLEAR render — the baseline is
    only a fallback, never a replacement."""
    ev = ("[{ts:'2026-06-20T00:00:00Z',level:'INFO',component:'download',"
          "message:'Downloaded theme for Foo',detail:null}]")
    out = _run(f"renderRowHistory({ev}, 'tv', 387, {_BASELINE});")
    assert "Downloaded theme for Foo" in out
    assert "1 event · click to expand" in out  # full summary note, not a loose substring
    assert 'data-clear="events"' in out  # real events ARE clearable
    assert "history-row-synthetic" not in out
    assert "ORIGIN" not in out


# ── CSS: the synthetic accent uses tokens, not a raw colour ──

def test_synthetic_row_css_uses_tokens():
    assert ".history-row-synthetic { border-left-color: rgba(var(--grey-rgb), 0.5); }" in APP_CSS
    assert ".history-synthetic-note {" in APP_CSS


# ── Whole-file parse guard ──

def test_app_js_still_parses():
    quickjs = pytest.importorskip("quickjs")
    ctx = quickjs.Context()
    try:
        ctx.eval(APP_JS)
    except quickjs.JSException as e:
        assert "SyntaxError" not in str(e), f"app.js failed to parse: {e}"
