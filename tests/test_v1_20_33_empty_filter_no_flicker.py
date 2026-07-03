"""v1.20.33 — no loading⇄no-items flicker on a 0-result filter.

the user (on a filter with 0 matches, while a bulk download was running so
the row-refresh poll fires every couple seconds): "when there are no
results on a filter will continuously see it say briefly loading the no
results then loading again back and forth."

Root cause: loadLibrary's empty-state branch wrote the "no items…"
message then `delete tbody.dataset.lastHash` (v1.13.21, to force a render
on the N→0→N transition). With the hash deleted, the NEXT poll's
`if (tbody.dataset.lastHash == null)` repainted the "loading…"
placeholder — so each background poll flickered loading… ⇄ no-items.

Fix: hash-guard the empty branch with the same lastHash mechanism as the
populated branch, using the full empty-row HTML as the hash. The empty
HTML never hash-matches a row render, so the N→0→N safety is preserved,
but lastHash is no longer null between polls so the placeholder stays put.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def _loadlibrary_body():
    start = APP_JS.index("async function loadLibrary()")
    end = APP_JS.index("async function loadLibraryPresets()")
    return APP_JS[start:end]


def test_empty_branch_no_longer_deletes_lasthash():
    """Deleting lastHash on 0 results is what caused the placeholder to
    repaint every poll — it must be gone from loadLibrary."""
    body = _loadlibrary_body()
    assert "delete tbody.dataset.lastHash" not in body, (
        "v1.20.33: the empty-state branch must not delete lastHash "
        "(that re-armed the 'loading…' placeholder → flicker)"
    )


def test_empty_branch_hash_guarded():
    """The empty branch now sets lastHash to the empty HTML and only
    writes when it differs — same pattern as the populated branch."""
    body = _loadlibrary_body()
    assert "const emptyHtml =" in body
    assert "tbody.dataset.lastHash = emptyHtml" in body
    # the write is guarded on a hash mismatch (no unconditional rewrite).
    assert "if (tbody.dataset.lastHash !== emptyHtml)" in body


def test_loading_placeholder_still_first_load_only():
    """The 'loading…' placeholder must still be gated on lastHash==null
    so it only paints on a genuine first load — not every poll."""
    body = _loadlibrary_body()
    assert "if (tbody.dataset.lastHash == null)" in body
    idx = body.index("if (tbody.dataset.lastHash == null)")
    # v0.51.41: window widened 200→400 — the placeholder is now the branded
    # recordLoaderHtml('loading…') spinner (with a lead comment) rather than a
    # bare inline string, so the "loading…" caption sits further from the guard.
    assert "loading…" in body[idx:idx + 400]


def test_v1_20_33_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
