"""v1.23.22 — INFO card surfaces the proposed TDB URL for urls_match rows.

the user's repro (Daemons of the Shadow Realm): a bulk-imported U row whose
themes.youtube_url is empty got a `urls_match` pending update (ThemerrDB
now publishes the SAME video the override already uses). The card's
THEMERRDB MATCH panel said "ThemerrDB now publishes the same video", but
the `themerrdb url` field right above showed `—`.

Cause: the v1.18.63 `_pendingTdbUrl` derivation excluded `kind ===
'urls_match'`, so the proposed URL (pending_update.new_youtube_url) never
reached the field, which then fell back to the empty themes.youtube_url.
The exclusion is removed — new_youtube_url is what TDB publishes for ANY
pending kind, so the field shows it (with tdbUrlIsPending only flagging
"(pending)" when it differs from the committed value).
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def test_displayed_tdb_url_no_longer_excludes_urls_match():
    # the DISPLAY value (_pendingTdbUrl) must surface new_youtube_url for
    # ANY pending kind — the urls_match exclusion that blanked the
    # themerrdb url field to `—` is gone from THIS assignment.
    i = APP_JS.index("const _pendingTdbUrl = (data.pending_update")
    assign = APP_JS[i:APP_JS.index("    const tdbUrl =", i)]
    assert "kind !== 'urls_match'" not in assign, (
        "urls_match rows must surface the proposed TDB URL in the "
        "themerrdb url field, not fall through to `—`"
    )
    assert "data.pending_update.decision === 'pending'" in assign
    assert "data.pending_update.new_youtube_url" in assign


def test_pending_flag_still_excludes_urls_match():
    # the "(pending)" CHANGE flag (tdbUrlIsPending) must STILL exclude
    # urls_match (v1.18.63: same-video format rewrite, not a real change)
    # AND compare against the committed value.
    i = APP_JS.index("const tdbUrlIsPending")
    block = APP_JS[i:APP_JS.index(";", i)]
    assert "data.pending_update.kind !== 'urls_match'" in block
    assert "_pendingTdbUrl !== _committedTdbUrl" in block
