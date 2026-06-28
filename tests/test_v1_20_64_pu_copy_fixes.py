"""v1.20.64 — two PU-related copy fixes from the deferred audit.

Both were verified misleading in current code (the other two items in the
bundle — LET PLEX SERVE confirms + UPLOAD MP3 hint — were verified
ACCURATE and dropped).

1. **failure-acknowledged badge** (hydrateRecoveryOptions): the gate
   admits `data.resolved || ackedOnly || data.plex_resolved`, but the
   badge ternary had only TWO outcomes. A plex_resolved-but-not-acked
   row (P-row, TDB sync failed, Plex still serving its own theme) fell
   to "(unavailable — failure acknowledged)" even though the user never
   acked anything — contradicting the "PLEX SERVES" section header.

2. **mismatch PUSH TO PLEX tooltip**: flat "Overwrite the Plex-folder
   file with the new download." — but a PU/collection row has no
   Plex-folder file (media_folder=''); the push re-uploads via the API.
   The action already branches on `mismatchKind` ('api' vs 'file'); now
   the tooltip does too.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def test_failure_badge_has_third_plex_serving_branch():
    anchor = APP_JS.index("tdb-unavailable-badge muted small")
    block = APP_JS[anchor:anchor + 900]
    # The new 3rd branch + its discriminator.
    assert "Plex is serving its own theme" in block, (
        "plex_resolved-not-acked rows must get their own label, not "
        "the misleading 'failure acknowledged'"
    )
    assert "data.acked" in block, "the 3-way must discriminate on data.acked"
    # The two original labels stay.
    assert "using local source" in block
    assert "failure acknowledged" in block


def test_mismatch_push_tooltip_is_pu_aware():
    start = APP_JS.index("if (themed && isMismatch && downloaded && placed) {")
    block = APP_JS[start:start + 1200]
    assert "'replace', 'PUSH TO PLEX'" in block
    # Branches on the kind already computed for the action.
    assert "mismatchKind === 'api'" in block
    assert "Re-upload the new download to Plex via the API" in block
    # The sidecar wording stays for file-kind rows.
    assert "Overwrite the Plex-folder file with the new download." in block


def test_v1_20_64_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
