"""v0.51.84 — modal dialogs have accessible names (UI audit a11y).

A native `<dialog>` opened with `showModal()` does NOT derive its accessible
name from a descendant `<h2>`, so a screen reader announced "#info-dlg" and
"#help-glossary" as unnamed dialogs on open. Fix: `aria-labelledby` on each
dialog pointing at its (now id'd) title heading.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
BASE_HTML = (REPO / "app" / "web" / "templates" / "base.html").read_text()


def _dialog_has_accessible_name(dialog_id: str) -> None:
    # the <dialog ...> open tag (attrs may wrap across lines).
    m = re.search(rf'<dialog\b[^>]*\bid="{re.escape(dialog_id)}"[^>]*>',
                  BASE_HTML, re.DOTALL)
    if not m:
        # id might precede other attrs — match the whole tag then check id present.
        m = re.search(r'<dialog\b[^>]*>', BASE_HTML, re.DOTALL)
    assert m, f"dialog #{dialog_id} tag not found"
    tag = m.group(0)
    assert f'id="{dialog_id}"' in tag
    m2 = re.search(r'aria-labelledby="([^"]+)"', tag)
    assert m2, f"#{dialog_id} must have aria-labelledby (else showModal announces an unnamed dialog)"
    target_id = m2.group(1)
    # the referenced id must exist on an element that carries visible text.
    assert f'id="{target_id}"' in BASE_HTML, (
        f"#{dialog_id} aria-labelledby points at id={target_id!r} which doesn't exist")


def test_info_dialog_has_accessible_name():
    _dialog_has_accessible_name("info-dlg")
    # specifically the MOTIF INFO title.
    assert 'id="info-dlg-title"' in BASE_HTML
    assert re.search(r'id="info-dlg-title"[^>]*>//\s*MOTIF INFO', BASE_HTML)


def test_help_glossary_dialog_has_accessible_name():
    _dialog_has_accessible_name("help-glossary")
    assert 'id="help-glossary-title"' in BASE_HTML
