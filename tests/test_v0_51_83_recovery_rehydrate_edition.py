"""v0.51.83 — INFO-drawer recovery re-hydrate stays edition-scoped.

Found by the status/info/library CSS audit pass. hydrateRecoveryOptions takes a
5th `rowRk` arg (v1.22.71) that pins the server's /recovery-options resolution
to the clicked row's edition — otherwise the server falls back to an unscoped
`LIMIT 1` representative rk (an arbitrary sibling edition). The initial call
from openInfoDialog passed it, but the two IN-PLACE re-hydrations after
mark-alive and clear-failure (ACK FAILURE) dropped it, so on a multi-edition
title (LotR / Watchmen) ACKing the 4K card could redraw the resolved/acked
annotation from the standard edition's state — the exact edition-blind-read
class the rk-threading effort closed.

Source guard (the drift is a dropped call-site arg; the server-side rk scoping
is already proven behaviorally by the v1.22.71 tests).
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def test_signature_still_takes_row_rk():
    # if the 5th param is ever renamed/removed, the guards below go stale.
    assert "async function hydrateRecoveryOptions(root, mediaType, tmdbId, sectionId," in APP_JS
    assert "rowRk) {" in APP_JS
    # and it is what pins the server fetch to the edition.
    assert "if (rowRk) params.set('rating_key', rowRk);" in APP_JS


def test_rehydrate_calls_pass_row_rk_not_bare():
    # the bug shape — a 4-arg re-hydrate — must not reappear.
    assert "hydrateRecoveryOptions(root, mt, id, sectionId)" not in APP_JS, (
        "v0.51.83: a 4-arg re-hydrate drops rowRk → server LIMIT 1 → arbitrary "
        "sibling edition's annotation")
    # both in-place re-hydrations (mark-alive + clear-failure) carry rowRk.
    assert APP_JS.count("hydrateRecoveryOptions(root, mt, id, sectionId, rowRk)") == 2, (
        "both recovery re-hydrations must pass rowRk to stay edition-scoped")
