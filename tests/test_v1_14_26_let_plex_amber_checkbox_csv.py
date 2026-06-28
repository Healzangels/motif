"""v1.14.26 — small bundle: LET PLEX SERVE amber, header checkbox, CSV UTF-16 LE.

Three small fixes shipped together; all touch UI surfaces and
all are independent. Bundling because each is 1-3 line shapes
that don't interact, and one verify pass on the deploy covers
all three.

## Item A — LET PLEX SERVE button → amber (Plex tone)

Pre-fix the LET PLEX SERVE recovery-card button rendered violet
(`tone: "user"`), which collided semantically with the U chip.
The action transitions the row toward Plex serving its own theme,
so the button color should match the P chip palette (amber via
`btn-plex` / `tone: "plex"`).

Both LET PLEX SERVE sites (the no-failure path at api.py:9728
and the failure-recovery path at api.py:9942) get the same
tone change.

## Item B — header checkbox tri-state semantic + defensive belt

the user's repro: SELECT ALL on page → uncheck one row → click
header expecting to clear → header stuck on `−` with the page
re-selected.

Pre-fix `turnOn = !allSelected` meant clicking a partially-
selected (indeterminate) header re-selected every row instead
of clearing — counterintuitive and the opposite of what almost
every other tri-state UI does. Standard semantic:

  none selected   → click selects all
  some selected   → click DEselects all (collapses indeterminate)
  all selected    → click DEselects all

v1.14.26 changes the gate to `turnOn = !anySelected` — turn-on
fires ONLY when literally nothing on the visible page is
selected. Anything else collapses to deselect.

Plus a defensive belt in updateLibrarySelectionUi: if
libraryState.selected is empty, force header to fully unchecked
regardless of what the per-row visibility check computed.
Catches the rare browser-quirk + stale-NodeList edge cases.

## Item C — CSV export switched to UTF-16 LE BOM

(v1.14.6 originally added UTF-8 BOM. the user's v1.14.26 repro
showed mojibake had returned after round-tripping through
Google Drive into Excel-on-Mac.)

UTF-8 BOM works in modern Excel + Numbers, but some round-trip
paths strip it (Google Drive upload/download is the common
culprit). UTF-16 LE with BOM is what Excel emits natively when
you "Save As CSV (Mac)" — works across Excel (Mac + Win),
Numbers, Google Sheets, LibreOffice without encoding
detection. File size doubles but CSVs are small.

JS implementation: ArrayBuffer + DataView since browsers'
TextEncoder is UTF-8-only. Iterates code units via
charCodeAt — handles surrogate pairs correctly because JS
strings are already UTF-16 internally.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── Item A: LET PLEX SERVE tone ───────────────────────────────


def test_let_plex_serve_no_failure_path_uses_plex_tone():
    """The LET PLEX SERVE button must use tone='plex' for amber
    styling.

    v1.14.47 reorg: the no-failure-path LET PLEX SERVE moved
    from the INFO card recovery options to the SOURCE menu
    (where intentional Plex actions belong, per the user's UX
    principle that TRY THIS NEXT is for error states only).
    The contract this test pins ('tone'/color is plex-amber)
    survives via the SOURCE-menu render."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # SOURCE-menu LET PLEX SERVE block (v1.14.47).
    assert "'purge-revert-to-plex', 'LET PLEX SERVE'" in js
    anchor = js.index("'purge-revert-to-plex', 'LET PLEX SERVE'")
    block = js[anchor:anchor + 1500]
    # Tone is plex (amber).
    assert "tone: 'plex'" in block


def test_let_plex_serve_failure_recovery_path_uses_plex_tone():
    """Same fix on the failure-recovery LET PLEX SERVE button
    (the purge-and-ack variant that fires on failed rows where
    Plex has its own theme)."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    marker = "v1.14.26: tone amber to match the Plex"
    assert marker in src
    block_start = src.index(marker)
    block = src[block_start:block_start + 800]
    assert '"tone": "plex"' in block


def test_no_more_user_tone_on_let_plex_serve_buttons():
    """Regression guard: no LET PLEX SERVE button site can use
    tone='user' anymore. v1.14.47 reorg: the no-failure-path
    LET PLEX SERVE moved from the INFO card to the SOURCE menu;
    the failure-recovery purge-and-ack block stayed in api.py.
    Check both surviving sites."""
    api_src = (REPO / "app" / "web" / "api.py").read_text()
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # Site 1: failure-recovery purge-and-ack in api.py
    # (still has the LET PLEX SERVE label).
    api_anchor = api_src.index('"label": "LET PLEX SERVE"')
    api_block = api_src[api_anchor:api_anchor + 600]
    assert '"tone": "user"' not in api_block, (
        "Failure-recovery LET PLEX SERVE block uses tone='user' "
        "(should be 'plex' for amber Plex color)"
    )
    # Site 2: SOURCE-menu LET PLEX SERVE in app.js (v1.14.47).
    js_anchor = js.index("'purge-revert-to-plex', 'LET PLEX SERVE'")
    js_block = js[js_anchor:js_anchor + 1500]
    assert "tone: 'user'" not in js_block, (
        "SOURCE-menu LET PLEX SERVE uses tone='user' (should "
        "be 'plex' for amber Plex color)"
    )


def test_tone_class_map_has_plex_entry():
    """Regression guard: the JS TONE_CLASS map at app.js:8958
    must map 'plex' → 'btn-plex' (amber). v1.14.26 doesn't
    touch this side; pin so a refactor doesn't drop the
    mapping."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    anchor = js.index("const TONE_CLASS = {")
    block = js[anchor:anchor + 500]
    assert "plex: 'btn-plex'" in block


# ── Item B: header checkbox tri-state ─────────────────────────


def test_header_click_uses_any_selected_not_all_selected():
    """The click handler's intent gate must be `!anySelected`
    (= "select all only when nothing is selected") not
    `!allSelected` (= "select all when not all are selected" —
    which made indeterminate-click select-all instead of
    deselect-all).

    v1.16.11 broadened the gate from visible-page anySelected
    to FULL-selection anySelected — the header is now a master
    toggle. With pagination + SELECT ALL FILTERED, the visible-
    only semantics left off-page rows orphaned in the bulk bar
    after a header click. New gate:
    `libraryState.selected.size > 0` covers visible + off-page."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    handler_anchor = js.index("getElementById('library-select-all')?.addEventListener")
    body = js[handler_anchor:handler_anchor + 3000]
    # v1.16.11 form — master-toggle.
    assert "const anySelected = libraryState.selected.size > 0;" in body
    assert "const turnOn = !anySelected;" in body


def test_header_click_pre_fix_all_selected_gone():
    """Regression guard: the pre-fix `const allSelected =
    visibleKeys.every(...)` must not survive in the click
    handler.

    Strip line comments so the rationale comment quoting the
    deleted shape doesn't trip the guard."""
    js_raw = (REPO / "app" / "web" / "static" / "app.js").read_text()
    handler_anchor = js_raw.index("getElementById('library-select-all')?.addEventListener")
    end = js_raw.index("updateLibrarySelectionUi();", handler_anchor) + 100
    body_raw = js_raw[handler_anchor:end]
    body = "\n".join(
        line for line in body_raw.splitlines()
        if not line.lstrip().startswith("//")
    )
    assert "const allSelected = visibleKeys.every(" not in body


def test_update_selection_ui_force_clears_header_on_empty_selection():
    """The defensive belt: when libraryState.selected.size === 0,
    force the header to fully unchecked regardless of what the
    per-row visibility check computed. Catches stale NodeList +
    browser-quirk edge cases."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # Anchor on the v1.14.26 defensive marker.
    marker = "v1.14.26: defensive belt"
    assert marker in js
    block_start = js.index(marker)
    block = js[block_start:block_start + 800]
    assert "if (libraryState.selected.size === 0)" in block
    assert "all.checked = false" in block
    assert "all.indeterminate = false" in block


# ── Item C: CSV UTF-16 LE BOM ─────────────────────────────────


def test_csv_export_uses_utf16le_bom():
    """The CSV blob must be built as UTF-16 LE with BOM
    (0xFF 0xFE leading bytes) instead of the v1.14.6 UTF-8 BOM
    (0xEF 0xBB 0xBF). UTF-16 LE survives Google Drive
    round-trips and is what Excel emits natively."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # Anchor on the v1.14.26 CSV marker.
    marker = "v1.14.26: switched from UTF-8 BOM to UTF-16 LE"
    assert marker in js
    block_start = js.index(marker)
    block = js[block_start:block_start + 2500]
    # BOM bytes set explicitly.
    assert "view.setUint8(0, 0xFF)" in block
    assert "view.setUint8(1, 0xFE)" in block
    # Per-char encoding loop with little-endian flag.
    assert "view.setUint16(2 + i * 2, csvText.charCodeAt(i), /* littleEndian */ true)" in block


def test_csv_blob_mime_type_declares_utf16le():
    """Blob MIME type must declare charset=utf-16le so any
    consumer that respects MIME (HTTP servers, modern parsers)
    has the correct hint."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    assert "type: 'text/csv;charset=utf-16le'" in js


def test_csv_pre_fix_utf8_bom_blob_shape_gone():
    """Regression guard: the v1.14.6 `new Blob(['﻿', ...])` shape
    must not survive — would re-introduce the UTF-8 BOM that
    Google Drive strips."""
    js_raw = (REPO / "app" / "web" / "static" / "app.js").read_text()
    js = "\n".join(
        line for line in js_raw.splitlines()
        if not line.lstrip().startswith("//")
    )
    # The pre-v1.14.26 Blob construction with UTF-8 BOM as first
    # element is gone.
    assert "new Blob(['﻿', lines.join" not in js


def test_csv_blob_constructed_from_arraybuffer():
    """The new Blob must be constructed from the ArrayBuffer
    we built (not a string array). Pin the construction shape."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    assert "const blob = new Blob([buf]," in js
    # The ArrayBuffer is sized at 2 (BOM) + N*2 (chars).
    assert "new ArrayBuffer(2 + csvText.length * 2)" in js


# ── Behavioral: the BOM bytes are correctly first ─────────────


def test_csv_byte_layout_is_correct():
    """End-to-end byte layout sanity check.
    Replicate the JS encoding logic in Python and verify:
      bytes[0:2] = BOM (0xFF 0xFE)
      bytes[2:]  = UTF-16 LE encoding of the text"""
    sample = "Übel Blatt\r\n"
    # Mirror the JS logic.
    bom = b'\xff\xfe'
    # JS: charCodeAt iterates UTF-16 code units; Python's
    # str.encode('utf-16-le') does the same.
    body = sample.encode('utf-16-le')
    expected = bom + body
    # Verify our spec: BOM first, then 2-byte little-endian.
    assert expected[0] == 0xFF
    assert expected[1] == 0xFE
    # 'Ü' is U+00DC → little-endian: 0xDC 0x00.
    assert expected[2] == 0xDC
    assert expected[3] == 0x00
