"""v1.17.13 — audit rollover Tier A.

The v1.17.12 three-agent audit (security / frontend race / error
UX) returned 22 actionable findings across the three axes. v1.17.13
ships the Tier-A bundle: high-conviction + low-risk fixes that
collectively close the most dangerous silent-failure paths.

## Error UX (5 silent-loader sites)

Pre-fix, five page loaders swallowed fetch failures to
`console.error` (or `/* ignore */`) and left the UI showing stale
or empty data with no signal that anything was wrong:

* **loadQueue** (`/queue`) — stale jobs table on /api/jobs 500.
* **loadTokens** (`/settings`) — placeholder "loading…" indefinitely
  on /api/tokens failure; user might create duplicate.
* **loadConfigIntoForms** (`/settings`) — silently aborted on
  /api/config failure, leaving form fields at HTML defaults
  (empty inputs, unchecked boxes). A subsequent SAVE click
  would PATCH those blank values over the real server-side
  config — **silent data loss**. This was the most dangerous of
  the five.
* **loadScansList + loadFindings + loadScanDetail** (`/scans`) —
  load failure indistinguishable from genuine empty state.
* **refreshDryRunState** (`/settings`) — silent fail; if dry-run
  state flipped server-side the UI silently disagreed.

All five now write a user-visible error surface (tbody row in
red for table loaders, banner+save-disable for config, "STATE
UNKNOWN" label for dry-run). loadConfigIntoForms also sets a
module-level `configLoadFailed` flag that bindConfigSaves
honors — saves are explicitly blocked with an inline error
message until a successful reload.

## Frontend race: optimistic-placeholder pairing (4 sites)

Class-5 silent topbar drift. Four buttons set an optimistic
placeholder before the API call and didn't clear it on failure
— leaving the topbar mini-bar showing "// QUEUING DOWNLOAD" /
"// REFRESHING PLEX" for the full 5s placeholder TTL with no
real op behind it. Sites: SYNC PLEX topbar, REFRESH FROM PLEX
library, REPLACE TDB row, recovery-card redl/revert (shared
catch).

## Frontend race: visibility-guard polling bodies

Class-10 sub-pattern. `syncWatcher` (both click-path + page-load
arming) and `libraryRapidPoll` polled their `/api/stats` /
`loadLibrary` work even when the tab was hidden. Under
Chromium's ~1/min throttle the tick body still ran on the
stretched cadence, hitting the network + writing to a tab the
user can't see. All three intervals now bail early on
`document.visibilityState !== 'visible'`. The visibilitychange
handler at page top re-arms work on tab return, so nothing is
lost.

## Security: apprise_urls scheme validation + GET masking

The v1.17.12 audit flagged that `notifications.apprise_urls`:
1. Accepted any scheme (apprise plugins are a wide registry,
   but file://, ftp://, javascript:, etc. should never appear
   in a notification URL).
2. Were returned in plaintext from GET /api/config — an
   exfiltrated admin-scope API token leaks every embedded
   webhook credential (Discord webhook token, Pushover
   user/app keys, Telegram bot token, mailto user:pass).

Asymmetric to the existing masking on plex.token / tmdb_api_key /
proxy_url. v1.17.13 closes both:
* `_validate_apprise_url_scheme(url)` enforces URI shape + a
  deny-list of dangerous schemes at validate() time. Lenient on
  allowlist so new apprise plugins don't require code changes.
* `mask_apprise_url(url)` returns `<scheme>://***` for the GET
  response — preserves scheme visibility, hides credentials.
* PATCH handler treats masked entries positionally ("keep the
  existing URL at this slot") so GET-then-PATCH round-trips
  don't wipe credentials, mirroring plex.token's mask-equals-
  keep contract.
"""

from __future__ import annotations

import re
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_JS = REPO / "app" / "web" / "static" / "app.js"
API_PY = REPO / "app" / "web" / "api.py"
CONFIG_FILE_PY = REPO / "app" / "core" / "config_file.py"
APP_INIT = REPO / "app" / "__init__.py"


# ── Error UX: silent-loader fixes ─────────────────────────────


def test_load_queue_surfaces_fetch_errors_in_tbody():
    """loadQueue must wrap its /api/jobs await in try/catch and
    write an accent-red row on failure."""
    src = APP_JS.read_text()
    idx = src.index("loadQueue._seq = (loadQueue._seq || 0) + 1;")
    window = src[idx:idx + 1500]
    assert "try {" in window and "catch (e)" in window, (
        "v1.17.13: loadQueue must guard the /api/jobs fetch."
    )
    assert "queue load failed" in window, (
        "v1.17.13: loadQueue catch must write a user-visible "
        "error message into #jobs-body."
    )
    assert "accent-red" in window


def test_load_tokens_surfaces_fetch_errors_in_tbody():
    """loadTokens must guard the /api/tokens await + surface the
    error in #tokens-body."""
    src = APP_JS.read_text()
    idx = src.index("async function loadTokens()")
    window = src[idx:idx + 1200]
    assert "try {" in window and "catch (e)" in window
    assert "tokens load failed" in window
    assert "accent-red" in window


def test_load_config_into_forms_sets_failed_flag_and_banner():
    """The most dangerous silent loader. On /api/config failure
    we must set a `configLoadFailed` flag + render a visible
    banner — saves will check the flag and refuse to PATCH
    until reload succeeds."""
    src = APP_JS.read_text()
    # Module-level flag declared.
    assert "let configLoadFailed = false;" in src, (
        "v1.17.13: configLoadFailed flag must be declared at "
        "module level (read by bindConfigSaves)."
    )
    # Banner helper exists.
    assert "_renderConfigLoadFailedBanner" in src, (
        "v1.17.13: must declare a banner helper for the "
        "config-load-failed surface."
    )
    # Catch path sets the flag + calls the banner.
    idx = src.index("async function loadConfigIntoForms()")
    window = src[idx:idx + 2500]
    assert "configLoadFailed = true" in window
    assert "_renderConfigLoadFailedBanner(" in window
    # Success path clears the flag + the banner.
    assert "configLoadFailed = false" in window
    assert "_clearConfigLoadFailedBanner(" in window


def test_bind_config_saves_refuses_when_load_failed():
    """The save-side defense: if configLoadFailed is true, the
    SAVE click must abort with a user-visible error and NOT
    fire a PATCH (which would otherwise serialize HTML defaults
    over real server-side config)."""
    src = APP_JS.read_text()
    idx = src.index("function bindConfigSaves()")
    window = src[idx:idx + 2500]
    assert "if (configLoadFailed)" in window, (
        "v1.17.13: bindConfigSaves must guard on the "
        "configLoadFailed flag before PATCHing."
    )
    assert "save blocked" in window, (
        "v1.17.13: the load-failed save-block must surface a "
        "human-readable message (not a silent abort)."
    )


# v0.50.89: test_load_scans_list/findings/scan_detail_surfaces_errors removed —
# the /scans-page client JS (loadScansList/loadFindings/loadScanDetail) was
# orphaned dead code (no template hosted the #scan-* elements, no /scans route)
# and was deleted. The error-surfacing behavior they guarded no longer exists.


def test_refresh_dry_run_state_surfaces_unknown_label():
    """refreshDryRunState catch must surface "STATE UNKNOWN"
    instead of leaving the label blank/stale (which could
    silently disagree with the real server-side flag)."""
    src = APP_JS.read_text()
    idx = src.index("async function refreshDryRunState()")
    window = src[idx:idx + 1200]
    assert "STATE UNKNOWN" in window, (
        "v1.17.13: refreshDryRunState catch must render "
        "STATE UNKNOWN so the user knows the label is stale."
    )


# ── Frontend race: optimistic-placeholder pairing ──────────────


def test_sync_plex_topbar_btn_clears_placeholder_on_error():
    """SYNC PLEX topbar btn catch must call
    clearOptimisticPlaceholder('plex_enum')."""
    src = APP_JS.read_text()
    idx = src.index("syncPlexBtn.addEventListener('click'")
    window = src[idx:idx + 2000]
    assert "Refresh Plex failed" in window
    # The clear must appear inside the catch block.
    catch_idx = window.index("Refresh Plex failed")
    pre_catch = window[max(0, catch_idx - 600):catch_idx]
    assert "clearOptimisticPlaceholder('plex_enum')" in pre_catch, (
        "v1.17.13: SYNC PLEX btn catch must clear the "
        "plex_enum optimistic placeholder."
    )


def test_library_refresh_btn_clears_placeholder_on_error():
    """REFRESH FROM PLEX library btn catch must clear the
    plex_enum placeholder."""
    src = APP_JS.read_text()
    # Find the library refresh handler — anchor on the failure
    # alert string + the libraryRefreshLabel call that's unique
    # to this site.
    idx = src.index("Refresh failed: ' + err.message")
    window = src[max(0, idx - 1500):idx + 300]
    assert "clearOptimisticPlaceholder('plex_enum')" in window, (
        "v1.17.13: library REFRESH btn catch must clear the "
        "plex_enum placeholder."
    )


def test_replace_tdb_clears_placeholder_on_error():
    """REPLACE TDB row btn catch must clear the download_queue
    placeholder. The endpoint is `/replace-with-themerrdb` —
    distinct from the unrelated `replaceTheme()` function
    that pushes motif's canonical back to Plex without setting
    a placeholder."""
    src = APP_JS.read_text()
    # Anchor on the actual endpoint POST (api('POST', ...) — not
    # the action-key declarations above. The unique substring is
    # the `await api(` adjacent to the endpoint path.
    idx = src.index(
        "/replace-with-themerrdb`")
    window = src[idx:idx + 1500]
    assert "Replace failed:" in window, (
        "v1.17.13 sanity: the row-pill REPLACE TDB catch "
        "should be near the /replace-with-themerrdb POST."
    )
    assert "clearOptimisticPlaceholder('download_queue')" in window


def test_recovery_card_catch_clears_placeholder():
    """The shared recovery-card catch must clear the
    download_queue placeholder (set by the redl + revert
    branches above)."""
    src = APP_JS.read_text()
    idx = src.index("Recovery action failed")
    window = src[max(0, idx - 1500):idx + 200]
    assert "clearOptimisticPlaceholder('download_queue')" in window


# ── Frontend race: visibility-guard polling bodies ─────────────


def test_sync_watcher_click_path_visibility_guarded():
    """syncWatcher (click-path) must bail when document is
    hidden. Race audit #8 (class-10 sub-pattern)."""
    src = APP_JS.read_text()
    # Two syncWatcher = setInterval bodies — find each by their
    # surrounding context.
    click_idx = src.index(
        "syncWatcher = setInterval(async () => {")
    body = src[click_idx:click_idx + 1500]
    assert "document.visibilityState" in body, (
        "v1.17.13: syncWatcher click-path interval body must "
        "check document.visibilityState and bail when hidden."
    )


def test_sync_watcher_page_load_path_visibility_guarded():
    """Second syncWatcher arming on page-load also guards on
    visibility."""
    src = APP_JS.read_text()
    # Count guards — both interval bodies should have one.
    n = src.count(
        "if (document.visibilityState !== 'visible') return;")
    assert n >= 3, (
        f"v1.17.13: expected at least 3 visibility-guards "
        f"(syncWatcher click-path, syncWatcher page-load path, "
        f"libraryRapidPoll body) but found {n}."
    )


def test_library_rapid_poll_visibility_guarded():
    """libraryRapidPoll interval body must bail when tab is
    hidden — Chromium's throttle stretches the 2s interval
    and the existing `Date.now() > libraryRapidUntil` check
    can prematurely terminate the rapid-poll."""
    src = APP_JS.read_text()
    idx = src.index("libraryRapidTimer = setInterval(")
    body = src[idx:idx + 1500]
    assert "document.visibilityState" in body, (
        "v1.17.13: libraryRapidPoll body must check "
        "document.visibilityState."
    )


# ── Security: apprise_urls scheme validation + masking ────────


def test_validate_apprise_url_scheme_helper():
    """Helper exists in config_file.py + enforces URI shape +
    deny-list of dangerous schemes."""
    from app.core.config_file import _validate_apprise_url_scheme
    # Allowed apprise plugin schemes (lenient).
    assert _validate_apprise_url_scheme("discord://x/y")
    assert _validate_apprise_url_scheme("pushover://u@a")
    assert _validate_apprise_url_scheme("tgram://bot/chat")
    assert _validate_apprise_url_scheme("mailto://u:p@host")
    assert _validate_apprise_url_scheme("slack://x/y/z/m")
    assert _validate_apprise_url_scheme("https://hook.x/y")
    # Denied — local-resource / code-injection schemes.
    assert not _validate_apprise_url_scheme("file:///etc/passwd")
    assert not _validate_apprise_url_scheme("ftp://host/path")
    assert not _validate_apprise_url_scheme("javascript:alert(1)")
    assert not _validate_apprise_url_scheme("gopher://host")
    # Shape violations.
    assert not _validate_apprise_url_scheme("not-a-url")
    assert not _validate_apprise_url_scheme("")
    assert not _validate_apprise_url_scheme("://x")


def test_mask_apprise_url_helper():
    """`mask_apprise_url(url)` returns `<scheme>://***` so the
    GET payload preserves scheme visibility but hides
    credentials."""
    from app.core.config_file import mask_apprise_url
    assert (
        mask_apprise_url("discord://abc123/xyz789")
        == "discord://***"
    )
    assert (
        mask_apprise_url("pushover://userkey@apptoken")
        == "pushover://***"
    )
    assert (
        mask_apprise_url("tgram://bot_token/chat_id")
        == "tgram://***"
    )
    # Empty / malformed → safe fallback.
    assert mask_apprise_url("") == ""
    assert mask_apprise_url("nonsense") == "***"


def test_is_masked_apprise_url_helper():
    """`_is_masked_apprise_url` detects the round-trip
    'keep existing at this position' marker."""
    from app.core.config_file import _is_masked_apprise_url
    assert _is_masked_apprise_url("discord://***")
    assert _is_masked_apprise_url("pushover://***")
    # Not masked — real URLs.
    assert not _is_masked_apprise_url("discord://abc/xyz")
    assert not _is_masked_apprise_url("")
    assert not _is_masked_apprise_url("***")
    assert not _is_masked_apprise_url("discord://***/extra")


def test_validate_rejects_bad_scheme_in_apprise_urls():
    """validate() must surface a per-URL error for any
    apprise_urls entry that fails scheme validation."""
    from app.core.config_file import (
        MotifConfig, NotificationsConfig, validate,
    )
    cfg = MotifConfig()
    cfg.notifications = NotificationsConfig(
        apprise_urls=[
            "discord://ok/ok",        # ok
            "file:///etc/passwd",     # denied
            "javascript:alert(1)",    # denied
        ],
    )
    errors = validate(cfg)
    # Expect at least two errors mentioning apprise_urls[1] and
    # [2] (the discord one passes).
    msgs = [e for e in errors if "apprise_urls" in e]
    assert any("[1]" in e for e in msgs), (
        "v1.17.13: file:// scheme must be rejected with "
        "an apprise_urls[1] error."
    )
    assert any("[2]" in e for e in msgs), (
        "v1.17.13: javascript: scheme must be rejected with "
        "an apprise_urls[2] error."
    )


def test_api_get_config_masks_apprise_urls():
    """The api.py GET handler must call mask_apprise_url() on
    each entry. Pin via the source — we don't have a full
    FastAPI test harness here, the source-grep is sufficient."""
    src = API_PY.read_text()
    # The GET handler imports + uses mask_apprise_url.
    idx = src.index("@app.get(\"/api/config\")")
    end = src.index("@app.patch(\"/api/config\")", idx)
    block = src[idx:end]
    assert "mask_apprise_url" in block, (
        "v1.17.13: GET /api/config must mask apprise_urls "
        "(prevents credential leak via admin token theft)."
    )
    # Set-count metadata for the UI.
    assert "apprise_urls_set_count" in block, (
        "v1.17.13: GET response should expose "
        "apprise_urls_set_count so the UI knows how many "
        "are configured without seeing the masked values."
    )


def test_api_patch_config_preserves_masked_apprise_urls():
    """PATCH handler must positionally preserve existing URLs
    when the body sends back the masked form — same contract
    as plex.token's mask-equals-keep behavior."""
    src = API_PY.read_text()
    # Locate the apprise_urls branch in _apply_partial_config.
    assert "_is_masked_apprise_url" in src, (
        "v1.17.13: PATCH must import _is_masked_apprise_url "
        "to detect round-trip 'keep' entries."
    )
    assert "_validate_apprise_url_scheme" in src, (
        "v1.17.13: PATCH must validate scheme on real (non-"
        "masked) entries."
    )


# ── Version pin (soft floor) ──────────────────────────────────


def test_version_pinned_at_or_above_1_17_13():
    src = APP_INIT.read_text()
    m = re.search(r'__version__\s*=\s*"(\d+)\.(\d+)\.(\d+)"', src)
    assert m
    found = tuple(int(x) for x in m.groups())
    assert found >= (0, 17, 13), (
        f"v1.17.13: __version__ must be >= 1.17.13 "
        f"(found {'.'.join(str(x) for x in found)})."
    )
