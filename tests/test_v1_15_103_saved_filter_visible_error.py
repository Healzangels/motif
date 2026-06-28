"""v1.15.103 — saved-filter loader surfaces fetch failure (AUDIT_FRONTEND M7).

The May 13 AUDIT_FRONTEND.md M7 finding: `loadLibraryPresets`
catches fetch errors with `console.error` + `return`, leaving
the // PRESETS popover in its initial "none yet" state. A
transient 500 (or auth expiry, or any backend error) hides
the user's saved presets entirely with NO user-visible
signal — they'd think their presets vanished.

Pre-v1.15.103 the catch block was:

    } catch (e) {
        console.error('saved-filters load failed:', e);
        return;
    }

The `_renderPresetsList()` call below didn't run, so the
list element kept whatever HTML it had at popup open time —
which is the empty-state "none yet" message.

## Fix

Render a muted-error `<li>` directly in the catch block
before returning. The user sees "preset load failed — see
console" instead of "none yet". Same `library-presets-popup-empty
muted small` styling so it slots into the popover layout
without a new CSS rule.

## Why not retry / silent re-poll

The audit's recommendation was to make the failure VISIBLE,
not to retry. Retries hide root-causes (auth expiry,
backend down) — a visible error tells the user "something's
wrong, look at console" so they can investigate. Aligns with
the v1.15.94 lesson: silent log/error swallows hide real
state-consistency issues. Make failures observable.

## Tests

Static guard that the loader's catch block renders the
error message into the list element.
"""

from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = REPO / "app" / "web" / "static" / "app.js"


def test_load_library_presets_renders_error_on_failure():
    """The catch block in `loadLibraryPresets` must write a
    visible error message into `library-presets-list` so the
    popover surfaces the fetch failure to the user."""
    src = APP_JS.read_text()
    fn_start = src.index("async function loadLibraryPresets()")
    # The function is short; 60 lines is more than enough.
    block = src[fn_start:fn_start + 3000]
    # The catch block must include a list.innerHTML assignment
    # with an error message.
    catch_idx = block.index("catch (e)")
    catch_block = block[catch_idx:catch_idx + 1000]
    assert "list.innerHTML" in catch_block, (
        "v1.15.103: loadLibraryPresets's catch block must "
        "render an error message into the list element. Pre-fix "
        "the catch only console.error'd and returned, leaving "
        "the popover claiming the user has no presets. See "
        "AUDIT_FRONTEND.md M7."
    )
    assert "preset load failed" in catch_block, (
        "v1.15.103: the error message must include the literal "
        "'preset load failed' (so users searching their console "
        "for the matching log line can correlate)."
    )


def test_load_library_presets_error_uses_design_token_classes():
    """The error <li> must reuse the existing
    `library-presets-popup-empty muted small` classes so it
    visually slots into the popover. Don't introduce new ad-hoc
    inline styles."""
    src = APP_JS.read_text()
    fn_start = src.index("async function loadLibraryPresets()")
    block = src[fn_start:fn_start + 3000]
    catch_idx = block.index("catch (e)")
    catch_block = block[catch_idx:catch_idx + 1000]
    assert "library-presets-popup-empty" in catch_block, (
        "v1.15.103: error <li> must use the existing "
        "`library-presets-popup-empty` class (same as the "
        "'none yet' empty state) so the popover layout "
        "stays consistent."
    )
    assert "muted" in catch_block and "small" in catch_block, (
        "v1.15.103: error message should be muted + small "
        "(non-alarming) — matches the 'none yet' empty state's "
        "visual weight."
    )
