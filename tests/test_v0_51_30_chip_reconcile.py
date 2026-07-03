"""v0.51.30 — collections // ALL + a section could both be .chip-active at once
(the user). bindLibrary's full-load hydration now reconciles the chip group once
from the resolved state so exactly ONE chip is active — SSR marks ALL active by
default (server can't see the client's persisted pick), and the old piecemeal
per-axis toggles never cleared that SSR-ALL when the client resolved to a section.
"""
from __future__ import annotations
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def _bind_library() -> str:
    i = APP_JS.index("function bindLibrary()")
    return APP_JS[i:i + 20000]


def test_bindlibrary_reconciles_chips_from_resolved_state():
    body = _bind_library()
    assert "v0.51.30: authoritative chip reconcile" in body
    # ALL chip active iff allRes.
    assert "x.classList.toggle('chip-active', libraryState.allRes)" in body
    # fourk + section chips active ONLY when NOT allRes (mutual exclusion w/ ALL).
    assert body.count("!libraryState.allRes") >= 2, (
        "v0.51.30: the fourk + section reconcile toggles must both gate on "
        "!libraryState.allRes so ALL is mutually exclusive with them")
    assert "x.dataset.fourk === (libraryState.fourk ? '1' : '0')" in body
    assert "(x.dataset.sectionId || '') === libraryState.section_id" in body
