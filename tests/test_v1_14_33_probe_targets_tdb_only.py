"""v1.14.33 — PROBE TDB URL targets the TDB URL only (not the override).

the user repro: 13 Assassins
  • TDB URL `2GdEDKC8X2Q` — opened in browser, "Video unavailable"
  • Override URL `ESsp3MMMVUM user` — alive
  • INFO card shows: probe `// PROBE TDB URL  ✓ alive`
  • Row also shows red `TDB ✗` pill + last failure
    `video_removed · ERROR: [youtube] 2GdEDKC8X2Q: Video unavailable`

The button label says "PROBE TDB URL". The implementation was
probing the override URL when one existed (via the COALESCE
`(override or TDB)` pattern from v1.14.28). For 13 Assassins the
override is alive, so the probe correctly returned alive — but
the user's mental model + the safety-net design intent is that
the probe answers "is the TDB URL alive (so I can revert to it)".

The whole point per the user's original concern: "I'm a bit worried
about doing this when there is a chance the url that themerrdb
could be providing could be a dead url ... we wouldn't be able to
revert back to themerrdb url because of a hidden failing url".
That concern is ABOUT the TDB URL specifically. Probing the
override misses the entire failure mode.

## Scope

1. /api/items/{mt}/{id}/probe-tdb — `target_url = row["youtube_url"]`
   (the TDB URL). The override lookup is dropped entirely. 409 with
   "no TDB URL to probe" when the row has no themerrdb URL.
2. _bulk_probe_tdb_run — SQL filter selects `t.youtube_url`
   directly (no COALESCE with overrides). Rows with no TDB URL
   are skipped via `NULLIF(t.youtube_url, '') IS NOT NULL`.
3. INFO card probe button — gate on `tdbUrl` only (was
   `currentUrl || tdbUrl`). A row with an override but no TDB
   URL shouldn't show the button at all (nothing TDB-side to
   probe).
4. Tooltip text updated so the user knows overrides are NOT
   probed by this button.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── Single-row endpoint targets TDB URL only ─────────────────


def test_probe_tdb_endpoint_no_longer_reads_user_overrides():
    """The endpoint must NOT read the user_overrides table at
    all — the probe targets the TDB URL specifically. Pre-fix it
    fetched override.youtube_url and used (override or TDB) as
    target_url. Pin the regression by asserting the override
    lookup is gone from the function body."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("async def api_probe_tdb(")
    body = src[fn_anchor:fn_anchor + 15000]
    # The pre-fix override lookup query.
    assert "FROM user_overrides" not in body, (
        "Endpoint still reads user_overrides — the probe should "
        "target the TDB URL ONLY, not (override or TDB). the user's "
        "13 Assassins repro: TDB dead, override alive, probe "
        "incorrectly returned alive."
    )
    # And the pre-fix COALESCE expression.
    assert "(ovr and ovr[" not in body
    assert 'or row["youtube_url"]' not in body


def test_probe_tdb_endpoint_targets_themes_youtube_url_only():
    """`target_url` must come from the themes table — not from
    user_overrides. v1.18.63 added a pending_updates dispatch:
    target = pending_url || committed_url, where committed_url
    is row["youtube_url"] (themes table). The override-exclusion
    invariant from v1.14.33 holds; the dispatch only changes
    WHICH themes-side URL gets picked (committed vs pending)."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("async def api_probe_tdb(")
    fn_end = src.index("@app.post", fn_anchor + 1)
    body = src[fn_anchor:fn_end]
    # The committed-side source is still themes.youtube_url.
    assert "SELECT youtube_url FROM themes" in body, (
        "v1.14.33: committed URL must come from themes table"
    )
    # The dispatch through pending → committed.
    assert "target_url = pending_url or committed_url" in body, (
        "v1.18.63 dispatch must reach target_url"
    )
    # Override lookup still absent.
    assert "FROM user_overrides" not in body


def test_probe_tdb_endpoint_409s_when_no_tdb_url():
    """Plex_orphan rows (no TDB URL) should 409 with a message
    that explicitly says "no TDB URL to probe" — pre-fix the
    409 said "no URL to probe (orphan row with no override)"
    which leaked the COALESCE assumption."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("async def api_probe_tdb(")
    fn_end = src.index("@app.post", fn_anchor + 1)
    body = src[fn_anchor:fn_end]
    assert "no TDB URL to probe" in body
    # The pre-fix copy is gone.
    assert "no URL to probe (orphan row with no override)" not in body


def test_probe_tdb_v1_14_33_marker_present():
    """The v1.14.33 archaeology comment captures WHY the change
    happened (the user's 13 Assassins repro — TDB dead, override
    alive, button label said PROBE TDB URL). Pin so a future
    refactor that "simplifies" by re-introducing the COALESCE
    has to confront the marker."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    assert "v1.14.33: targets the TDB URL specifically" in src
    assert "13 Assassins" in src


# ── Bulk runner targets TDB URL only ────────────────────────


def test_bulk_probe_runner_no_longer_joins_user_overrides():
    """The bulk runner's SELECT was `LEFT JOIN user_overrides`
    + `COALESCE(override_url, tdb_url)`. Both must go — the
    runner targets `t.youtube_url` directly, no COALESCE, no
    user_overrides JOIN."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("def _bulk_probe_tdb_run(")
    body = src[fn_anchor:fn_anchor + 25000]
    # The SQL filter block.
    sql_anchor = body.index("SELECT t.media_type AS media_type")
    sql_end = body.index(".fetchall()", sql_anchor)
    sql = body[sql_anchor:sql_end]
    assert "user_overrides" not in sql
    assert "COALESCE(NULLIF(ovr.youtube_url" not in sql
    # And selects t.youtube_url directly.
    assert "t.youtube_url AS target_url" in sql


def test_bulk_probe_runner_skips_rows_with_no_tdb_url():
    """Rows with no TDB URL (plex_orphan rows that ended up in
    themes) are skipped via NULLIF(t.youtube_url, '') IS NOT NULL
    — the bulk runner has no TDB URL to probe for them."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("def _bulk_probe_tdb_run(")
    body = src[fn_anchor:fn_anchor + 25000]
    sql_anchor = body.index("SELECT t.media_type AS media_type")
    sql_end = body.index(".fetchall()", sql_anchor)
    sql = body[sql_anchor:sql_end]
    assert "NULLIF(t.youtube_url, '') IS NOT NULL" in sql


def test_bulk_probe_v1_14_33_marker_present():
    """Bulk runner has its own v1.14.33 marker explaining the
    same fix shape applied to the bulk path — sibling to the
    single-row endpoint."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    assert "v1.14.33: targets the TDB URL only" in src


# ── INFO card button gates on tdbUrl only ───────────────────


def test_info_card_probe_button_gated_on_tdb_url_only():
    """The probe button must hide when there's no TDB URL —
    even if the row has an override URL. Pre-fix the gate was
    `currentUrl || tdbUrl` so a plex_orphan + override row
    showed the button → click 409'd."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    assert "const probeBtnHtml = tdbUrl" in js
    # The pre-fix gate must NOT survive.
    assert "const probeBtnHtml = probeTargetUrl" not in js
    # And the obsolete probeTargetUrl const should be gone.
    assert "const probeTargetUrl = currentUrl || tdbUrl;" not in js


def test_info_card_probe_button_tooltip_clarifies_tdb_only():
    """Tooltip must say overrides are NOT probed — the user's
    mental model of the button needs to match what it does.
    Pin the substring so a future copy edit can't silently
    drop the disclaimer."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    btn_anchor = js.index('data-act="probe-tdb"')
    btn_block = js[btn_anchor:btn_anchor + 1500]
    assert "Override URLs are NOT probed" in btn_block


# ── Probe-on-confirm helper still works (string contract) ───


def test_let_plex_serve_probe_helper_uses_tdb_url_messaging():
    """The probe-then-confirm helper was already worded as
    'TDB URL' for all three branches (alive/inconclusive/dead).
    v1.14.33 doesn't change the helper — but pin the messaging
    so a future refactor doesn't accidentally make it ambiguous
    again given the underlying endpoint now actually probes the
    TDB URL (so the labels are now true)."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    fn_anchor = js.index("async function _probeAndConfirmLetPlexServe")
    body = js[fn_anchor:fn_anchor + 15000]
    assert "TDB URL probed: alive" in body
    assert "TDB URL probe inconclusive" in body
    assert "TDB URL probed: DEAD" in body
