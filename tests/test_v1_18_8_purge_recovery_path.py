"""v1.18.8 — recovery path after PURGE on zombie-orphan rows.

## What broke

the user reported the post-PURGE state across all libraries:

  1. SOURCE menu showed no RESTORE button — even though the
     captured URL was in `previous_urls`.
  2. REMOVE menu still showed `× PURGE` — but clicking it ran an
     idempotent no-op (the orphan was already zombified).
  3. No DELETE button to fully drop the row + captured URL.

Three coordinated bugs:

  * **SQL `has_previous_url` NULL trap.** The library query
    computed `has_previous_url` via
      COALESCE(pv_sec, pv_global) IS NOT NULL
        AND COALESCE(pv_sec, pv_global) != COALESCE(override, t.youtube_url)
    Post-PURGE on a zombie orphan the inner COALESCE returns
    NULL (no override row, themes.youtube_url nulled). SQL's
    `'value' != NULL` evaluates as UNKNOWN, which CASE treats
    as FALSE → has_previous_url=0 → JS gate at the SOURCE-menu
    RESTORE branch returns false → RESTORE never rendered.

  * **PURGE button stayed visible on zombie orphans.** The
    JS gate `if (downloaded || isOrphan)` ignored that the
    orphan could BE in zombie state (URLs nulled, no canonical,
    no placement). Clicking PURGE on a zombie hit api_forget_
    item which idempotently UPDATEd nothing — the user's "PURGE
    again does nothing."

  * **DELETE not surfaced.** The `delete-orphan` action handler
    was wired (app.js:11795) but no menu item ever rendered it.
    Dead code waiting for UI. So the user had no path to fully
    clear a zombie orphan + its captured URL chain.

## Fix

  * `_library_main_query` `has_previous_url` switches from `!=`
    to `IS NOT` for the inner inequality. SQLite's `IS NOT` is
    NULL-safe: `'a' IS NOT NULL` → TRUE; `NULL IS NOT NULL` →
    FALSE. Post-PURGE on zombie: captured-URL IS NOT NULL → TRUE
    → has_previous_url=1 → RESTORE renders.

  * `renderLibraryRow` PURGE gate splits on a new helper
    `orphanHasPurgeableState` — `isOrphan && (user_youtube_url
    || youtube_url || downloaded || placed)`. PURGE shows only
    while there's something to purge; zombie state hides it.

  * `renderLibraryRow` adds a DELETE menu item to the REMOVE
    menu for every orphan row (zombie or not). Uses the existing
    `delete-orphan` action; tooltip explains the cleanup target
    (with vs without RESTORE-capable state).
"""

from __future__ import annotations

import sys
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

API_PY = REPO / "app" / "web" / "api.py"
APP_JS = REPO / "app" / "web" / "static" / "app.js"


# ── SQL has_previous_url NULL-safe comparison ─────────────────


def test_has_previous_url_uses_is_not_for_null_safe_inequality():
    """The inner inequality in the has_previous_url CASE must use
    SQLite's `IS NOT` (NULL-safe) rather than `!=`. Without the
    fix, post-PURGE on a zombie orphan returns has_previous_url=0
    because `captured_url != NULL` evaluates as UNKNOWN → CASE
    treats as FALSE → no RESTORE button."""
    src = API_PY.read_text()
    # v1.23.10: the CASE body moved into the shared constant
    # _PREV_URL_DIFFERS_SQL (the restore ATTN pill interpolates the
    # same text). Pin the constant's body AND that the CASE still
    # routes through it, so the NULL-safe guarantee covers both
    # consumers.
    assert "(CASE WHEN {_PREV_URL_DIFFERS_SQL}" in src
    idx = src.index("_PREV_URL_DIFFERS_SQL = ")
    case_block = src[idx:src.index('"""', idx + 60) + 3]
    assert "IS NOT COALESCE(" in case_block, (
        "v1.18.8: has_previous_url's inner inequality must use "
        "`IS NOT COALESCE(...)` for NULL-safe comparison. The "
        "v1.17.24 zombie-orphan path produces NULL on the right "
        "side; `!=` evaluates UNKNOWN → CASE returns 0 → "
        "RESTORE never appears."
    )


def test_has_previous_url_inequality_not_plain_neq():
    """Counter-pin: the buggy `!= COALESCE(` form must be GONE
    from the has_previous_url branch. The same `!=` pattern is
    fine elsewhere — but this specific branch needs the NULL-
    safe operator."""
    src = API_PY.read_text()
    # v1.23.10: anchor moved to the shared constant (see test above).
    idx = src.index("_PREV_URL_DIFFERS_SQL = ")
    case_block = src[idx:src.index('"""', idx + 60) + 3]
    # The buggy form was:
    #   COALESCE(pv_sec.youtube_url, pv_global.youtube_url) != COALESCE(
    # Pin that THAT specific shape is gone (the surrounding `IS NOT NULL`
    # at the start of the CASE is fine — different comparison).
    assert "youtube_url) != COALESCE(" not in case_block, (
        "v1.18.8: has_previous_url must not use `!=` on the "
        "inner COALESCE comparison — that's the NULL trap"
    )


# ── JS: PURGE button gated on purgeable state ─────────────────


def test_purge_button_hides_on_zombie_orphan():
    """The PURGE-button gate must include an `orphanHasPurgeableState`
    check so zombie orphans (URLs nulled, no canonical, no
    placement) hide PURGE entirely — clicking it on a zombie
    is a no-op that confuses the user."""
    js = APP_JS.read_text()
    # Pin the new gate variable.
    assert "orphanHasPurgeableState" in js, (
        "v1.18.8: PURGE gate must use orphanHasPurgeableState"
    )
    # Pin the gate's truthy condition: any of url / downloaded /
    # placed must hold.
    idx = js.index("const orphanHasPurgeableState")
    block = js[idx:idx + 400]
    assert "user_youtube_url" in block
    assert "youtube_url" in block
    assert "downloaded" in block
    assert "placed" in block
    # And the outer `if` must use the gate.
    assert "if (downloaded || orphanHasPurgeableState)" in js, (
        "v1.18.8: PURGE button visibility must be "
        "`if (downloaded || orphanHasPurgeableState)`"
    )


# ── JS: DELETE menu item REMOVED in v1.18.15 ──────────────────
# The original v1.18.8 wire-up of `delete-orphan` was reverted in
# v1.18.15 per the user's UX feedback ("not a fan of the double
# delete"). The PURGE → CLEAR URL composition replaces it: PURGE
# drops files + override (orphan zombies); CLEAR URL drops the
# captured chain AND auto-evicts the now-empty themes row. See
# test_v1_18_15_remove_menu_consolidation.py for the new pins.


def test_delete_orphan_menu_item_removed_in_v1_18_15():
    """The `delete-orphan` menu entry must NOT be in the REMOVE
    menu anymore. Pre-v1.18.15 this surfaced a second destructive
    button alongside PURGE; v1.18.15 dropped it in favor of the
    CLEAR URL auto-evict path."""
    js = APP_JS.read_text()
    assert "menuItemHtml('delete-orphan'" not in js, (
        "v1.18.15: delete-orphan menu entry must be removed "
        "(replaced by PURGE → CLEAR URL composition)"
    )
    # Multi-line shape too (the v1.18.8 form).
    assert "'delete-orphan', 'DELETE'" not in js, (
        "v1.18.15: delete-orphan menu wire-up must be removed"
    )
