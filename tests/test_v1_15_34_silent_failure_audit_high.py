"""v1.15.34 — silent-failure audit (HIGH-severity batch) +
MARK ALIVE tone change yellow → magenta.

the user: "lets do a holistic silent failure check. Also wondering
if the manual override for the failure in the info card should
be a different color then yellow"

Four parallel Explore agents audited the codebase by domain
(workers/threads, FastAPI routes, frontend JS, core libs);
v1.15.34 ships the 7 HIGH-severity findings + the MARK ALIVE
color change. The MED findings are deferred to v1.15.35.

## Seven HIGH-severity fixes

### 1. download-missing + bulk-download JSON coercion (api.py)
Pre-fix `try: body = await request.json() except Exception:
body = {}` silently coerced malformed bodies to `{}`. The
download-missing route then enqueued jobs against tab=movies
+ fourk=false defaults, returning 200 success the caller
couldn't distinguish from "no missing themes on this tab."
v1.15.34 raises HTTPException(400) on parse failure.

### 2. bulk-probe cookies-snapshot fallback race (api.py)
Pre-fix `try: snapshot ... except Exception: per_call_cookies
= cookies` re-introduced the v1.15.11 race condition
(multiple workers reading the same cookies file concurrently)
silently. Operator saw "probe error" results that were
actually thread-safety artifacts. v1.15.34 logs + skips the
row instead of falling back to the shared file. Also logs
unlink failures so /tmp snapshot leaks become visible.

### 3. mirror-compaction silent rmtree (sync.py)
Pre-fix `shutil.rmtree(self.repo_path, ignore_errors=True)`
silently absorbed all rmtree failures. "Compaction succeeded"
was logged but the old mirror dir stayed on disk — silent
storage bloat. v1.15.34 drops `ignore_errors`, wraps in
try/except + log.warning.

### 4. plex.py enumerate_section_items silent JSON parse
Pre-fix `return []` on HTTP/JSON failure was indistinguishable
from a legitimately empty section. plex_enum's caller treated
[] as success → stamped `last_enum_content_changed_at` →
short-circuited future enums until coverage drift was visible.
v1.15.34 defines `PlexParseError` + raises on both paths;
the existing `try/except` in plex_enum counts the failure
correctly and skips the post-success bookkeeping.

### 5. _topbar_ssr_state silent except (api.py)
Pre-fix bare `except Exception: pass` silently hid the
FAIL/COOKIES/DISK LOW banners on every page-load when the DB
hiccupped. Operator only noticed when something was wrong
and the warning banner was missing. v1.15.34 logs the
exception at WARNING.

### 6. app.js refreshTopbarStatus null deref
Pre-fix `idle.querySelector('.op-pill-label').textContent =
'IDLE'` would null-deref on missing inner span and burn the
whole tick. v1.15.34 guards the result.

### 7. app.js openOverrideDialog getElementById chain
Pre-fix unguarded chain — first missing element burned the
SET URL flow silently. v1.15.34 bulk-validates all 5 children
up front + console.warn on missing elements.

## MARK ALIVE tone yellow → magenta

the user's exploratory question: yellow grouped MARK ALIVE
visually with FIX COOKIES, but the two have very different
semantics — FIX COOKIES is a config repair, MARK ALIVE is an
operator override that can mask a real dead URL. Magenta
visually separates them so muscle-memory clicks on one don't
fire the other.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"
APP_JS = REPO / "app" / "web" / "static" / "app.js"
APP_CSS = REPO / "app" / "web" / "static" / "app.css"
SYNC_PY = REPO / "app" / "core" / "sync.py"
PLEX_PY = REPO / "app" / "core" / "plex.py"


# ── #1: download-missing + bulk-download raise on JSON parse ──


def test_bulk_download_raises_on_invalid_json_body():
    """The bulk-download route must raise HTTPException(400) on
    malformed JSON, not silently coerce to {}. Pre-fix the
    silent coerce let a malformed body fall through to the
    items-validation 400 — operator couldn't distinguish bad
    JSON from a legit empty items list."""
    src = API_PY.read_text()
    # Anchor on the v1.15.34 marker block in the bulk-download
    # route handler (the v1.15.4 backup-mode docstring).
    anchor = src.index(
        "v1.15.34: raise on JSON parse failure rather than coercing"
    )
    block = src[anchor:anchor + 800]
    assert 'raise HTTPException(' in block
    assert "status_code=400" in block
    assert "invalid JSON body" in block


def test_download_missing_raises_on_invalid_json_body():
    """Same fix shape as bulk-download — pre-fix a malformed
    body silently defaulted to tab=movies + fourk=false,
    enqueuing jobs against the wrong scope."""
    src = API_PY.read_text()
    # Anchor on the download-missing-specific marker (it
    # references the wrong-tab/fourk concern).
    anchor = src.index(
        "v1.15.34: raise on JSON parse failure rather than coercing\n"
        "        # to {} — pre-fix a malformed body would silently default to\n"
        "        # tab=movies + fourk=false"
    )
    block = src[anchor:anchor + 800]
    assert "raise HTTPException(" in block
    assert "status_code=400" in block


# ── #2: bulk-probe cookies-snapshot fallback race ─────────────


def test_cookies_snapshot_failure_skips_row_no_shared_fallback():
    """The v1.15.11 race fix used a per-worker /tmp snapshot of
    cookies.txt; pre-v1.15.34 a snapshot-failure fallback to
    the shared file silently re-introduced exactly that race.
    v1.15.34 must log + return an error tuple instead of using
    the shared file."""
    src = API_PY.read_text()
    # Anchor on the v1.15.34 marker inside the snapshot except
    # branch.
    anchor = src.index(
        "v1.15.34: log + skip the row instead of"
    )
    block = src[anchor:anchor + 1500]
    # Must log the failure.
    assert "log.warning(" in block
    # Must return an error tuple (5-element shape: mt, tid,
    # url, None, error string).
    assert "cookies snapshot failed:" in block
    # Must NOT silently fall back to the shared file.
    assert "per_call_cookies = cookies" not in block, (
        "v1.15.34: snapshot-failure must NOT fall back to the "
        "shared cookies file (re-introduces v1.15.11 race)"
    )


def test_cookies_snapshot_unlink_failure_logged_not_swallowed():
    """The /tmp snapshot cleanup unlink was previously a bare
    `except Exception: pass`. v1.15.34 logs at WARNING so disk
    pressure becomes visible before /tmp fills."""
    src = API_PY.read_text()
    # Anchor on the v1.15.34 marker inside the unlink except.
    anchor = src.index(
        "v1.15.34: log unlink failures so /tmp"
    )
    block = src[anchor:anchor + 600]
    assert "log.warning(" in block


# ── #3: sync.py mirror-compaction silent rmtree ───────────────


def test_mirror_compaction_rmtree_drops_ignore_errors():
    """Pre-fix `shutil.rmtree(..., ignore_errors=True)`
    silently absorbed all failures. v1.15.34 wraps in
    try/except + log.warning so operator sees compaction
    cleanup failures."""
    src = SYNC_PY.read_text()
    # Locate the compaction block (anchored on the v1.15.34
    # marker comment).
    anchor = src.index(
        "v1.15.34: drop ignore_errors=True so rmtree failures"
    )
    block = src[anchor:anchor + 1200]
    # Must wrap in try/except.
    assert "try:" in block
    assert "shutil.rmtree(self.repo_path)" in block
    # ignore_errors=True must be GONE from this call.
    assert "shutil.rmtree(self.repo_path, ignore_errors=True)" not in block
    assert "except OSError" in block
    assert "log.warning(" in block


# ── #4: plex.py PlexParseError + raise on JSON/HTTP failure ───


def test_plex_parse_error_class_defined():
    """The new PlexParseError exception class must exist + be
    documented (its docstring explains why it's raised — to
    distinguish parse failures from legitimately empty
    responses, since the silent-empty path stamped
    last_enum_content_changed_at and short-circuited future
    enums)."""
    src = PLEX_PY.read_text()
    assert "class PlexParseError(Exception):" in src
    # Documented (docstring explains the v1.15.34 rationale).
    anchor = src.index("class PlexParseError(Exception):")
    docstring_block = src[anchor:anchor + 1500]
    assert "v1.15.34" in docstring_block
    assert "last_enum_content_changed_at" in docstring_block, (
        "PlexParseError docstring must call out the silent-empty "
        "→ enum-gate-stamp consequence so future maintainers "
        "understand why we raise rather than return [] / {}"
    )


def test_enumerate_section_items_raises_on_http_failure():
    """The HTTP-failure branch (status != 200 or no response)
    must raise PlexParseError instead of returning []."""
    src = PLEX_PY.read_text()
    # Anchor on the v1.15.34 marker inside the HTTP-failure
    # branch.
    anchor = src.index(
        "v1.15.34: raise PlexParseError instead of returning"
    )
    block = src[anchor:anchor + 1200]
    assert "raise PlexParseError(" in block, (
        "v1.15.34: HTTP-failure branch must raise, not return []"
    )


def test_enumerate_section_items_raises_on_json_parse_failure():
    """Same supersession on the JSON-parse branch."""
    src = PLEX_PY.read_text()
    anchor = src.index(
        "v1.15.34: raise on JSON parse failure"
    )
    block = src[anchor:anchor + 800]
    assert "raise PlexParseError(" in block


# ── #5: _topbar_ssr_state silent except ───────────────────────


def test_topbar_ssr_state_logs_exceptions():
    """Pre-fix bare `except Exception: pass` silently hid the
    FAIL/COOKIES/DISK LOW banners when the DB hiccupped.
    v1.15.34 logs at WARNING so the operator can correlate
    missing-banner reports with DB state."""
    src = API_PY.read_text()
    # Find the _topbar_ssr_state function.
    fn_anchor = src.index("def _topbar_ssr_state(")
    fn_end = src.index("templates.env.globals[\"topbar_ssr_state\"]", fn_anchor)
    fn_body = src[fn_anchor:fn_end]
    assert "log.warning(" in fn_body
    assert "topbar_ssr_state failed" in fn_body
    # The bare-pass must be GONE.
    # (The line after `except Exception as e:` should NOT be
    # just `pass` immediately.)
    assert "except Exception as e:\n            # All defaults remain" in fn_body


# ── #6: app.js refreshTopbarStatus null deref guard ───────────


def test_refresh_topbar_status_guards_op_pill_label():
    """The idle.querySelector('.op-pill-label') result must be
    null-checked before .textContent assignment. Pre-fix a
    missing inner span burned the entire refreshTopbarStatus
    tick."""
    src = APP_JS.read_text()
    # Anchor on the v1.15.34 marker.
    anchor = src.index(
        "v1.15.34: guard the .op-pill-label dereference"
    )
    block = src[anchor:anchor + 600]
    assert "querySelector('.op-pill-label')" in block
    assert "if (idleLabel)" in block
    # The unguarded `.textContent` chain must not appear in
    # this block (the guarded version assigns to idleLabel
    # only after a null-check).
    assert ".querySelector('.op-pill-label').textContent" not in block, (
        "v1.15.34: the unguarded chained access must be replaced"
    )


# ── #7: app.js openOverrideDialog children validation ─────────
# v1.19.87: test_open_override_dialog_bulk_validates_children removed
# — openOverrideDialog (the failure-recovery override-dlg) was deleted
# as dead code, so the silent-failure concern this guarded is moot.


# ── MARK ALIVE tone change ────────────────────────────────────


def test_mark_alive_tone_is_magenta_not_cookies():
    """The _MARK_ALIVE_OPT constant in api.py must use
    `tone: "magenta"` (was `"cookies"` in v1.15.24). the user
    wanted the operator-override semantics distinguished from
    config-repair (FIX COOKIES) so muscle-memory clicks don't
    fire the wrong action."""
    src = API_PY.read_text()
    const_anchor = src.index("_MARK_ALIVE_OPT = {")
    const_block = src[const_anchor:const_anchor + 1500]
    assert '"tone": "magenta"' in const_block
    assert '"tone": "cookies"' not in const_block, (
        "v1.15.34: MARK ALIVE tone moved from cookies → magenta"
    )


def test_js_tone_class_map_includes_magenta():
    """The JS TONE_CLASS map must map `magenta` → `btn-magenta`
    so the new tone resolves to a real CSS class. Otherwise
    MARK ALIVE buttons render with no tone-specific styling."""
    src = APP_JS.read_text()
    map_anchor = src.index("const TONE_CLASS = {")
    map_end = src.index("};", map_anchor)
    map_block = src[map_anchor:map_end]
    assert "magenta: 'btn-magenta'" in map_block


def test_btn_magenta_css_class_defined():
    """The .btn-magenta CSS rule must exist with a var(--magenta)
    color + border-color, plus a hover state. Mirror of the
    .btn-cookies pattern from v1.15.17."""
    css = APP_CSS.read_text()
    assert ".btn-magenta {" in css
    anchor = css.index(".btn-magenta {")
    block = css[anchor:anchor + 400]
    assert "var(--magenta)" in block
    # Hover state.
    assert ".btn-magenta:hover" in css


def test_btn_cookies_unchanged_for_actual_cookies_actions():
    """Defensive guard: the v1.15.34 magenta-split must not
    accidentally drop the .btn-cookies class — FIX COOKIES +
    cookies_expired ACK FAILURE still consume it."""
    css = APP_CSS.read_text()
    assert ".btn-cookies {" in css
    js = APP_JS.read_text()
    assert "cookies: 'btn-cookies'" in js
