"""v1.17.5 — design audit rollover.

the user's three-part design audit ask:

  1. "the settings download Prode TDB URL section the spacing
     seems strange with lots of extra spacing"
  2. "let's do a across the board check of our buttons and the
     size they are. I want to make sure there is consistency with
     buttons and the size they are relative to the task."
  3. "let's check all the confirmation text from clicking a
     button and make sure they all have the same dismiss after a
     bit built in"

Audit (one foreground + one background agent, see
DESIGN_AUDIT_v1.17.5_DRAFT.md before deletion) surfaced:

* PROBE TDB URLs `.form-actions` has 42px (gap-6 + 18px padding)
  + dashed top border — calibrated for "follows a form-grid"
  (visual separator from form fields). When it follows a
  `.form-hint` paragraph the paragraph IS the separator;
  the dashed border + 42px compounding gap reads as "two
  separate forms" rather than "explanation + action."
  Fix: `.form-hint + .form-actions` + `.form-hint-divider +
  .form-hint + .form-actions` selectors tighten the spacing
  for that specific shape without affecting other usages.

* 5 button size/tone mismatches in templates:
  - `// RE-LINK ALL` (dashboard) was tiny → promoted to full
    `.btn-warn` to match its REPROBE / PROBE peer cluster.
  - `// REBUILD BRIDGE` was default → promoted to `.btn-warn`
    so it visually aligns with the other mutating server jobs.
  - `// APPLY IMPORT` was default → promoted to `.btn-warn`
    (mutates user_overrides + enqueues N downloads).
  - DRY-RUN ENABLE + DISABLE were split tone (warn / default)
    despite being symmetric mutating toggles. Both → `.btn-warn`.
  - `+ NEW TOKEN` had wrong prefix → `// NEW TOKEN` per § 3
    convention.

* 5 JS-rendered buttons missing `// ` prefix per § 3:
  - `RELINK` (storage waste row in dashboard)
  - `DOWNLOAD` (dashboard missing-themes)
  - `REFRESH` (libraries per-section)
  - `REVOKE` (tokens)
  - `VIEW` (scans)

* 6 of 19 status-text sites missing or partial auto-dismiss:
  - `bindConfigSaves` setTimeout was inside success branch only;
    moved into finally with split duration (2500ms ok / 4000ms
    err) so every SAVE button on /settings dismisses both
    paths cleanly.
  - `bindReprobePlexThemes` / `bindBulkProbeTdb` /
    `bindReprobeTdbFailures` catch blocks now call
    `_autoDismissOpStatus(8000)` so the start-path error doesn't
    linger (success path already cleaned up by the completion
    watcher).
  - `bindTestCookies` finally now calls `_autoDismissOpStatus`
    with 6s so the summary line clears (the `<ul>` per-check
    results below remain for inspection).
  - `bindSyncProbe` finally now auto-dismisses at 6s — covers
    both the click-path and the auto-probe-on-load path.
  - Password update form finally adds a 5s clear so both
    `✓ password updated` and `✗ <error>` dismiss after a beat.
  - Info-card PROBE TDB URL slot now auto-clears at 6s so
    stale text doesn't persist across rebound probes in the
    same dialog session.

Dialog-submit errors (override / manual-url / upload) were
intentionally NOT auto-dismissed: the dialog stays open on
failure as the user's signal; auto-dismissing the explanation
would leave them with no cue what failed.

Design-system § 6 "per-site override" smells (inline `style=
margin-top:var(--gap-N)` etc. on .block-head / .form-hint /
.form-actions) flagged for a future v1.17.6 cleanup — they use
tokens so § 5.6 isn't violated, just the "promote stable
overrides into primitives" hygiene.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_INIT = REPO / "app" / "__init__.py"
APP_CSS = REPO / "app" / "web" / "static" / "app.css"
APP_JS = REPO / "app" / "web" / "static" / "app.js"
DASHBOARD_HTML = REPO / "app" / "web" / "templates" / "dashboard.html"
SETTINGS_HTML = REPO / "app" / "web" / "templates" / "settings.html"


# ── Version ──────────────────────────────────────────────────


def test_version_at_least_v1_17_5():
    import re
    src = APP_INIT.read_text()
    m = re.search(r'__version__\s*=\s*"(\d+)\.(\d+)\.(\d+)"', src)
    assert m
    major, minor, patch_ = int(m.group(1)), int(m.group(2)), int(m.group(3))
    assert (major, minor, patch_) >= (0, 17, 5)


# ── Form-actions spacing fix (the user's specific ask) ─────────


def test_form_actions_after_hint_has_tightened_spacing():
    """`.form-hint + .form-actions` selector must trim the
    margin-top + drop the dashed border so PROBE TDB URLs
    section doesn't render as two separate forms."""
    css = APP_CSS.read_text()
    assert ".form-hint + .form-actions" in css
    assert ".form-hint-divider + .form-hint + .form-actions" in css
    # The tightened rule must drop the top border.
    fix_idx = css.index(".form-hint + .form-actions")
    fix_end = css.index("}", fix_idx)
    block = css[fix_idx:fix_end]
    assert "border-top: none" in block
    assert "var(--gap-3)" in block or "var(--gap-2)" in block, (
        "v1.17.5: the tightened spacing should use the spacing "
        "scale tokens, not raw px."
    )


# ── Button size/tone fixes ────────────────────────────────────


def test_relink_all_promoted_to_full_warn():
    """// RE-LINK ALL was tiny — peers (REPROBE PLEX THEMES /
    PROBE TDB URLS) are full warn. Promote to match."""
    html = DASHBOARD_HTML.read_text()
    assert (
        '<button class="btn btn-warn" id="relink-all-btn">// RE-LINK ALL</button>'
        in html
    )
    assert 'btn-tiny btn-warn" id="relink-all-btn"' not in html


def test_rebuild_bridge_promoted_to_warn():
    """// REBUILD BRIDGE is a mutating server job — should be
    btn-warn like REPROBE / PROBE peers."""
    html = SETTINGS_HTML.read_text()
    assert 'class="btn btn-tiny btn-warn" id="tvdb-bridge-rebuild-btn"' in html


def test_dry_run_pair_both_btn_warn():
    """DRY-RUN ENABLE + DISABLE are symmetric mutating toggles —
    both should be .btn-warn (pre-fix DISABLE was default-tone)."""
    html = SETTINGS_HTML.read_text()
    assert (
        '<button class="btn btn-warn" id="dry-run-on-btn"' in html
        and '<button class="btn btn-warn" id="dry-run-off-btn"' in html
    )


def test_new_token_button_uses_slash_prefix():
    """// NEW TOKEN — was `+ NEW TOKEN`, violated the // prefix
    convention from § 3."""
    html = SETTINGS_HTML.read_text()
    assert ">// NEW TOKEN</button>" in html
    assert ">+ NEW TOKEN</button>" not in html


def test_apply_import_promoted_to_warn():
    """// APPLY IMPORT mutates user_overrides + enqueues
    downloads — peer to SAVE forms which are all btn-warn."""
    html = SETTINGS_HTML.read_text()
    assert (
        'class="btn btn-tiny btn-warn" id="import-apply-btn"'
        in html
    )


# ── JS-rendered button // prefixes ────────────────────────────


def test_js_rendered_buttons_use_slash_prefix():
    """5 JS-rendered buttons that were missing the // prefix per
    § 3 must now carry it."""
    js = APP_JS.read_text()
    # RELINK (storage waste row)
    assert '>// RELINK</button>' in js
    # DOWNLOAD (dashboard missing-themes)
    assert '>// DOWNLOAD</button>' in js
    # REFRESH (libraries per-section)
    assert '>// REFRESH</button>' in js
    # REVOKE (tokens)
    assert '>// REVOKE</button>' in js
    # v0.50.89: the VIEW (scans) button assertion was dropped — the /scans
    # client surface that rendered it was orphaned dead code, now removed.


# ── Status-text auto-dismiss ─────────────────────────────────


def test_bindConfigSaves_dismiss_lives_in_finally():
    """The setTimeout that clears `.form-status` text after a
    SAVE must live in a finally block so both ok + err paths
    auto-clear. Pre-fix the timeout was inside the success
    branch only; error messages lingered indefinitely."""
    js = APP_JS.read_text()
    fn_anchor = js.index("function bindConfigSaves()")
    # v0.51.90: the end-anchor comment was renamed // Clear-token button →
    # // Clear-secret buttons in v0.51.89 (generalized clear handler).
    fn_end = js.index("\n    // Clear-secret buttons", fn_anchor)
    body = js[fn_anchor:fn_end]
    # Must be inside a finally block.
    assert "} finally {" in body
    finally_idx = body.index("} finally {")
    finally_block = body[finally_idx:]
    assert "setTimeout(" in finally_block
    # Split duration: 2500ms ok, 4000ms err.
    assert "2500" in finally_block and "4000" in finally_block
    # Removes both the ok + err classes on clear.
    assert "s.classList.remove('ok', 'err')" in finally_block


def test_bindReprobePlexThemes_start_path_error_auto_dismisses():
    """The start-path catch (no completion watcher launched) must
    auto-dismiss so `✗ failed` doesn't linger."""
    js = APP_JS.read_text()
    fn_anchor = js.index("function bindReprobePlexThemes()")
    fn_end = js.index("\n  function bindBulkProbeTdb()", fn_anchor)
    body = js[fn_anchor:fn_end]
    catch_idx = body.index("} catch (e) {")
    catch_block = body[catch_idx:]
    assert "_autoDismissOpStatus" in catch_block


def test_bindBulkProbeTdb_start_path_error_auto_dismisses():
    js = APP_JS.read_text()
    fn_anchor = js.index("function bindBulkProbeTdb()")
    fn_end = js.index("\n  // v1.15.10: REPROBE FAILURES", fn_anchor)
    body = js[fn_anchor:fn_end]
    catch_idx = body.index("} catch (e) {")
    catch_block = body[catch_idx:]
    assert "_autoDismissOpStatus" in catch_block


def test_bindReprobeTdbFailures_start_path_error_auto_dismisses():
    js = APP_JS.read_text()
    fn_anchor = js.index("function bindReprobeTdbFailures()")
    fn_end = js.index("\n  // v1.15.8: TEST COOKIES", fn_anchor)
    body = js[fn_anchor:fn_end]
    catch_idx = body.index("} catch (e) {")
    catch_block = body[catch_idx:]
    assert "_autoDismissOpStatus" in catch_block


def test_bindTestCookies_finally_auto_dismisses():
    js = APP_JS.read_text()
    fn_anchor = js.index("function bindTestCookies()")
    fn_end = js.index("\n  // v1.17.0: TEST NOTIFICATION", fn_anchor)
    body = js[fn_anchor:fn_end]
    finally_idx = body.index("} finally {")
    finally_block = body[finally_idx:]
    assert "_autoDismissOpStatus" in finally_block


def test_bindSyncProbe_finally_auto_dismisses():
    js = APP_JS.read_text()
    fn_anchor = js.index("function bindSyncProbe()")
    # bindSyncProbe wraps an inner runProbe() — find its finally.
    body = js[fn_anchor:fn_anchor + 3000]
    finally_idx = body.index("} finally {")
    finally_block = body[finally_idx:finally_idx + 800]
    assert "_autoDismissOpStatus" in finally_block


def test_password_form_finally_auto_dismisses():
    js = APP_JS.read_text()
    # The password-form submit handler lives in the settings init.
    anchor = js.index("$('#password-form')")
    body = js[anchor:anchor + 1500]
    # Must have a finally block (pre-fix only had try/catch).
    assert "} finally {" in body
    finally_idx = body.index("} finally {")
    finally_block = body[finally_idx:]
    assert "setTimeout(" in finally_block
    # 5s duration for the password update flash.
    assert "5000" in finally_block


def test_info_card_probe_tdb_url_slot_auto_dismisses():
    """The PROBE TDB URL button inside the info card sets
    #probe-result text on every branch (alive / indeterminate /
    dead / error). Without auto-clear it persists across re-clicks
    in the same dialog session — confusing."""
    js = APP_JS.read_text()
    anchor = js.index('button[data-act="probe-tdb"]')
    body = js[anchor:anchor + 4500]
    finally_idx = body.index("} finally {")
    finally_block = body[finally_idx:finally_idx + 1200]
    assert "setTimeout(" in finally_block
    # Slot uses raw textContent + color, not the form-status classes.
    assert "slot.textContent = ''" in finally_block
