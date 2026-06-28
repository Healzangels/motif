"""v1.15.24 — MARK ALIVE recovery + Settings REPROBE FAILURES
layout polish + Library Scope auto-hide.

## Bundled fixes

### 1. MARK ALIVE recovery action

the user's KWXXC228g24 case: row stuck as `failure_kind=video_removed`
from a prior probe that misclassified an anti-bot response. Current
probes return UNKNOWN/indeterminate (correctly — yt-dlp can't
confirm alive against anti-bot variability), so REPROBE FAILURES
never clears the failure_kind. The URL plays fine in a browser.
Operator had no way to clear the stuck flag.

New endpoint `POST /api/items/{mt}/{id}/mark-alive` is the explicit
"trust me, I checked" override:
- Mirrors the alive-clear write block from /probe-tdb (themes-level
  failure_kind clear + DELETE from section_failure_acks)
- Different from /clear-failure (ACK FAILURE) which acknowledges
  the failure but keeps failure_kind set + the red TDB ✗ pill;
  MARK ALIVE asserts the URL works and clears failure_kind
  entirely
- Recorded in audit log as `action="mark_alive"` so the operator-
  override is auditable

New recovery option `_MARK_ALIVE_OPT` (action="mark-alive", priority
8, tone="cookies" yellow) added to recipes where misclassification
is plausible: `video_private`, `video_removed`, `network_error`,
`unknown`. Skipped for `cookies_expired` (already has FIX COOKIES),
`video_age_restricted`, `geo_blocked` (operator can't easily verify
alive-from-their-IP).

JS dispatcher: new `mark-alive` branch with a confirm dialog before
the POST. Same close-and-reload shape as clear-failure (re-hydrate
recovery + refresh topbar + reload library).

### 2. Settings REPROBE FAILURES layout

the user (planning conversation): "fix the layout of the themerdb
reprobe it looks a bit off". Pre-fix the // PROBE TDB URLS hint
sat above its button, the // COOLDOWN paragraph followed, then
the // REPROBE FAILURES hint sat ALSO above the // PROBE TDB URLS
button — read like another generic note rather than a button-
specific explanation. Fix: regroup so the REPROBE FAILURES hint
sits directly above its button, with a dashed `.form-hint-divider`
between the two button groups.

### 3. Library Scope auto-hide

the user: "whats the point to these since they're set in the
libraries section below?". For interactive operators who've
enumerated sections + toggled MGD per-section, the
SECTION INCLUDE / SECTION EXCLUDE text inputs are dead UI
(sections.py:177 notes the env-var rules apply only on FIRST
DISCOVERY). Auto-hide unless `MOTIF_PLEX_SECTION_INCLUDE` or
`_EXCLUDE` is in env_overrides() — preserves visibility for
headless/Compose-driven deploys, removes noise for everyone else.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"
APP_JS = REPO / "app" / "web" / "static" / "app.js"
APP_CSS = REPO / "app" / "web" / "static" / "app.css"
SETTINGS_HTML = REPO / "app" / "web" / "templates" / "settings.html"


# ── 1. MARK ALIVE endpoint ─────────────────────────────────────


def test_mark_alive_endpoint_route_defined():
    """The new endpoint must exist with admin auth + the
    documented path shape."""
    src = API_PY.read_text()
    assert '@app.post("/api/items/{media_type}/{tmdb_id}/mark-alive")' in src
    assert "async def api_mark_alive(" in src
    fn_start = src.index("async def api_mark_alive(")
    fn_end = src.index("\n    @app.", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    assert "_require_admin(request)" in fn_body


def test_mark_alive_clears_failure_kind_and_sfa():
    """The endpoint must mirror the alive-clear write block from
    /probe-tdb: title-global failure_kind clear + DELETE per-
    section sfa rows. Pin both writes so a future refactor can't
    silently drop the sfa cleanup (which would leave the FAIL
    pill suppressed-but-stale)."""
    src = API_PY.read_text()
    fn_start = src.index("async def api_mark_alive(")
    fn_end = src.index("\n    @app.", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    assert "UPDATE themes" in fn_body
    assert "SET failure_kind = NULL" in fn_body
    assert "DELETE FROM section_failure_acks" in fn_body


def test_mark_alive_records_audit_entry():
    """Operator overrides must be auditable. Pin the audit
    entry's action name so a future refactor can't silently
    drop the audit trail."""
    src = API_PY.read_text()
    fn_start = src.index("async def api_mark_alive(")
    fn_end = src.index("\n    @app.", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    assert "_record_audit(" in fn_body
    assert 'action="mark_alive"' in fn_body


def test_mark_alive_handles_no_op_for_clean_row():
    """A row with no failure_kind set must return a no-op
    response without firing the audit/log_event side-effects.
    Otherwise spurious clicks would pollute the log."""
    src = API_PY.read_text()
    fn_start = src.index("async def api_mark_alive(")
    fn_end = src.index("\n    @app.", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    # The no-op branch returns early with note.
    assert '"no_op": True' in fn_body
    assert "no failure to clear" in fn_body


# ── 2. MARK ALIVE recovery option in recipes ──────────────────


def test_mark_alive_constant_defined():
    """The shared `_MARK_ALIVE_OPT` constant must define the
    action keyword + tone + interactive=True so all recipes
    consume the same shape. Pre-fix duplicated entries drifted
    out of sync (different priorities, tooltips); the constant
    prevents that class.
    v1.15.34: tone changed from "cookies" (yellow) to
    "magenta" — separating MARK ALIVE (operator override) from
    FIX COOKIES (config repair) so muscle-memory clicks don't
    fire the wrong action."""
    src = API_PY.read_text()
    assert "_MARK_ALIVE_OPT = {" in src
    const_anchor = src.index("_MARK_ALIVE_OPT = {")
    const_block = src[const_anchor:const_anchor + 1200]
    assert '"action": "mark-alive"' in const_block
    assert '"tone": "magenta"' in const_block
    assert '"tone": "cookies"' not in const_block, (
        "v1.15.34: MARK ALIVE tone moved from cookies → magenta"
    )
    assert '"interactive": True' in const_block
    assert '"priority": 8' in const_block


def test_mark_alive_added_to_video_removed_recipe():
    """the user's specific case (KWXXC228g24 stuck as video_removed)
    must surface MARK ALIVE."""
    src = API_PY.read_text()
    recipe_anchor = src.index('"video_removed": [')
    recipe_block = src[recipe_anchor:recipe_anchor + 1200]
    assert "_MARK_ALIVE_OPT" in recipe_block, (
        "v1.15.24: video_removed recipe must include MARK ALIVE — "
        "the user's KWXXC228g24 case explicitly motivates this fix"
    )


def test_mark_alive_added_to_unknown_and_network_error_recipes():
    """The other two common-misclassification kinds. UNKNOWN is
    where bare 'This video is not available' lands (per v1.12.88
    classifier comment); NETWORK_ERROR can stick if a single
    transient probe wrote it. Both should offer MARK ALIVE."""
    src = API_PY.read_text()
    for kind in ("unknown", "network_error"):
        recipe_anchor = src.index(f'"{kind}": [')
        recipe_block = src[recipe_anchor:recipe_anchor + 1500]
        assert "_MARK_ALIVE_OPT" in recipe_block, (
            f"v1.15.24: '{kind}' recipe must include MARK ALIVE"
        )


# ── 3. JS dispatcher ──────────────────────────────────────────


def test_js_dispatcher_handles_mark_alive_action():
    """Recovery click handler must include a `mark-alive` branch
    that POSTs to the new endpoint + re-hydrates the dialog."""
    src = APP_JS.read_text()
    assert "if (act === 'mark-alive')" in src
    handler_anchor = src.index("if (act === 'mark-alive')")
    handler_block = src[handler_anchor:handler_anchor + 1500]
    assert "/mark-alive" in handler_block
    # Same close-and-reload shape as clear-failure.
    assert "hydrateRecoveryOptions" in handler_block
    assert "refreshTopbarStatus" in handler_block
    assert "loadLibrary" in handler_block


def test_js_dispatcher_confirms_before_marking_alive():
    """MARK ALIVE is operator-asserted override — must require
    explicit confirmation so a misclick can't accidentally
    clear failure_kind."""
    src = APP_JS.read_text()
    handler_anchor = src.index("if (act === 'mark-alive')")
    handler_block = src[handler_anchor:handler_anchor + 1500]
    assert "confirm(" in handler_block


# ── 4. Settings REPROBE FAILURES layout ────────────────────────


def test_settings_reprobe_hint_grouped_with_its_button():
    """The REPROBE FAILURES hint paragraph must sit BETWEEN the
    PROBE TDB URLS button group and the REPROBE FAILURES button
    (separated by .form-hint-divider) so the operator's eye
    groups each hint with the button it describes. Pre-fix the
    hint sat above BOTH buttons and read like a generic note."""
    src = SETTINGS_HTML.read_text()
    # The divider class must exist + be positioned between
    # bulk-probe-tdb-btn and reprobe-tdb-failures-btn.
    assert "form-hint-divider" in src
    bulk_btn = src.index('id="bulk-probe-tdb-btn"')
    reprobe_btn = src.index('id="reprobe-tdb-failures-btn"')
    divider = src.index("form-hint-divider")
    assert bulk_btn < divider < reprobe_btn, (
        "v1.15.24: divider must sit BETWEEN the two button groups"
    )
    # The REPROBE FAILURES hint paragraph must follow the divider
    # (not precede the PROBE TDB URLS button).
    reprobe_hint = src.index("REPROBE FAILURES</strong>")
    assert divider < reprobe_hint < reprobe_btn


def test_form_hint_divider_css_defined():
    """The .form-hint-divider class must be styled so it actually
    renders as a visible separator (dashed line). Pre-fix the
    class wasn't defined and rendered as the browser default
    `<hr>` (jarring solid line)."""
    src = APP_CSS.read_text()
    assert ".form-hint-divider" in src
    rule_anchor = src.index(".form-hint-divider")
    rule_block = src[rule_anchor:rule_anchor + 300]
    assert "border-top:" in rule_block
    assert "dashed" in rule_block


# ── 5. Library Scope auto-hide ─────────────────────────────────


def test_library_scope_jinja_global_registered():
    """The visibility helper must be registered as a Jinja env
    global so the settings template can call it."""
    src = API_PY.read_text()
    assert "def _library_scope_inputs_visible(" in src
    assert 'templates.env.globals["library_scope_inputs_visible"]' in src


def test_library_scope_helper_checks_env_overrides():
    """The helper must consult settings.env_overrides() and
    return True only when one of the relevant env keys is
    present. Pin both keys so a future refactor can't silently
    drop one."""
    src = API_PY.read_text()
    fn_anchor = src.index("def _library_scope_inputs_visible(")
    fn_end = src.index("templates.env.globals", fn_anchor)
    fn_body = src[fn_anchor:fn_end]
    assert "env_overrides()" in fn_body
    assert '"plex.section_include"' in fn_body
    assert '"plex.section_exclude"' in fn_body


def test_settings_template_wraps_library_scope_in_visibility_check():
    """The LIBRARY SCOPE block must be wrapped in
    `{% if library_scope_inputs_visible() %} ... {% endif %}`.
    Without the wrap the inputs always show — defeats the
    whole point of the auto-hide.

    Anchors on the rendered `<p class="form-subhead">// LIBRARY
    SCOPE</p>` element specifically — there's also an HTML
    comment earlier in the file that mentions "// LIBRARY SCOPE"
    in passing (PLEX section header), so a bare substring search
    would land on the wrong location."""
    src = SETTINGS_HTML.read_text()
    scope_anchor = src.index('class="form-subhead">// LIBRARY SCOPE')
    # Walk back ~800 chars to find the {% if ... %}.
    pre_block = src[max(0, scope_anchor - 1500):scope_anchor]
    assert "library_scope_inputs_visible()" in pre_block
    assert "{% if" in pre_block
    # And there must be a matching {% endif %} after the
    # SECTION EXCLUDE label closes.
    exclude_anchor = src.index("data-cfg-field-list=\"plex.section_exclude\"")
    post_block = src[exclude_anchor:exclude_anchor + 1500]
    assert "{% endif %}" in post_block
