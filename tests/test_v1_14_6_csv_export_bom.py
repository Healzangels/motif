"""v1.14.6 — CSV export prefixed with UTF-8 BOM.

Pre-fix: the // EXPORT CSV button built a Blob with `type:
'text/csv;charset=utf-8'` but no UTF-8 BOM in the body. Excel
(Mac + Windows) and Numbers ignore the MIME charset declaration
and sniff the encoding by BOM. Without the BOM, Excel falls
back to the system locale encoding — MacRoman on macOS,
Windows-1252 on Windows — and decodes UTF-8 multi-byte
characters as garbage:

    Pokémon Evolutions  →  Pok√©mon Evolutions   (MacRoman)
    Pokémon Evolutions  →  PokÃ©mon Evolutions   (Windows-1252)

the user's repro: exported a selection containing "Pokémon
Evolutions"; the file opened in Excel/Numbers showed the
mojibake form.

Fix: prepend `\\ufeff` (the UTF-8 BOM) to the Blob content. The
charset=utf-8 in the MIME type stays — relevant for HTTP-served
CSVs and any consumer that respects the declared charset — but
the BOM is what actually fixes Excel.

Tests pin via static-text guard on the export-csv handler.
Behavioral coverage of the actual download would need a browser
harness the project hasn't adopted (deferring per CLAUDE.md
test conventions).
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


def test_csv_blob_uses_a_bom_at_byte_offset_zero():
    """The CSV blob must start with a BOM so spreadsheet apps
    decode the file with the right encoding instead of falling
    back to system locale (MacRoman / Windows-1252) and
    rendering UTF-8 / UTF-16 bytes as garbage.

    v1.14.6 used UTF-8 BOM (0xEF 0xBB 0xBF). v1.14.26 superseded
    that with UTF-16 LE BOM (0xFF 0xFE) because UTF-8 BOM was
    getting stripped on Google-Drive round-trips. Either BOM
    satisfies the contract — this test accepts either shape so
    a future tag can switch back without thrashing the test."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # v1.14.26 form: BOM bytes set explicitly via DataView.
    has_utf16le_bom = (
        "view.setUint8(0, 0xFF)" in js
        and "view.setUint8(1, 0xFE)" in js
    )
    # v1.14.6 form (pre-v1.14.26): literal UTF-8 BOM as first
    # Blob array element.
    has_utf8_bom = "new Blob(['﻿', lines.join" in js
    assert has_utf16le_bom or has_utf8_bom, (
        "CSV export must prefix a BOM (UTF-8 or UTF-16 LE) so "
        "spreadsheet apps decode the file correctly"
    )


def test_csv_blob_mime_type_declares_charset():
    """The MIME type must declare a charset (utf-8 or utf-16le)
    so any consumer that respects MIME (HTTP middleware,
    server-side CSV parsers) has the correct hint. Either
    charset satisfies the contract."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    assert (
        "type: 'text/csv;charset=utf-8'" in js
        or "type: 'text/csv;charset=utf-16le'" in js
    )


def test_csv_blob_no_longer_starts_at_lines_join():
    """Regression guard against reverting to the original (pre-
    BOM) shape:

        new Blob([lines.join('\\r\\n') + '\\r\\n'], ...)

    A revert that drops the BOM entirely would re-introduce
    mojibake on every encoding-detection app."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    assert "new Blob([lines.join('\\r\\n') + '\\r\\n']" not in js


def test_app_js_csv_export_handler_bom_is_real_byte_sequence():
    """Sanity check: the BOM in the source file must be actual
    bytes, not a JS \\u escape (which Blob would treat as a
    multi-character string). Accepts either v1.14.26's
    explicit DataView write OR v1.14.6's UTF-8 BOM-as-string
    pattern."""
    raw = (REPO / "app" / "web" / "static" / "app.js").read_bytes()
    # v1.14.26 form: explicit BOM bytes set via DataView.setUint8.
    has_utf16_form = (
        b"view.setUint8(0, 0xFF)" in raw
        and b"view.setUint8(1, 0xFE)" in raw
    )
    # v1.14.6 form: literal UTF-8 BOM bytes inside the Blob array.
    has_utf8_form = b"new Blob(['\xef\xbb\xbf', lines.join" in raw
    assert has_utf16_form or has_utf8_form, (
        "BOM in source file must be actual bytes (UTF-16 LE via "
        "DataView write, or UTF-8 as literal char in Blob array)"
    )
