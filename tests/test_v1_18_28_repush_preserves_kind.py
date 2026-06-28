"""v1.18.28 — RE-PUSH preserves the row's current placement_kind.

the user's repro on v1.18.27: RE-PUSH on a plex_upload row landed
the same effect as SWITCH TO SIDECAR — the row demoted to a
sidecar instead of re-uploading via the Plex collection-theme
API. Same bug shape for the mismatch PUSH TO PLEX item: a
mismatch on a plex_upload row would silently demote on confirm.

## Root cause

Three sites in app.js fired action='replace' without passing
a `kind`:

  1. Collection PUSH TO PLEX (line ~8383)
  2. Movie/TV RE-PUSH        (line ~8407)
  3. Mismatch PUSH TO PLEX   (line ~8437)

Both the JS `replaceTheme(mediaType, tmdbId, btn, kind)` helper
(v1.18.23) and the backend `api_replace_item` `/replace` endpoint
(v1.18.23) already accepted an optional `kind` body param. When
`kind` was absent, the worker dispatched via
`settings.default_placement_method` — which for a user with
`placement.default_method='file'` (the v1.18.23 default) silently
demoted plex_upload rows to sidecars on every RE-PUSH.

## Fix

  - menuItemHtml gains a `kind` extras key → `data-kind="..."`
    dataset attribute.
  - action='replace' dispatcher reads `btn.dataset.kind` and
    forwards it to `replaceTheme(..., kind)`.
  - The three sites compute a `kind` per row:
      collection row             → 'api'  (no folder option)
      placement_kind=='plex_upload' → 'api'
      otherwise                  → 'file'

The explicit replace-file / replace-api actions (v1.18.23 PUSH
TO PLEX (FILE)/(API) on awaiting-approval rows) are unchanged —
they hardcode their kind at dispatch and don't go through this
path.
"""

from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_JS = REPO / "app" / "web" / "static" / "app.js"


# ── menuItemHtml emits data-kind ─────────────────────────────


def test_menu_item_html_serialises_kind_extras_to_dataset():
    """The shared `menuItemHtml` builder must emit a
    `data-kind="..."` attribute when `extras.kind` is set, so
    the action='replace' dispatcher can read it back off
    `btn.dataset.kind`."""
    js = APP_JS.read_text()
    idx = js.index("function menuItemHtml(")
    body = js[idx:idx + 4500]
    assert "extras.kind !== undefined" in body, (
        "v1.18.28: menuItemHtml must forward the kind extras key "
        "as a data-kind attribute"
    )
    assert 'data-kind="${htmlEscape(extras.kind)}"' in body


# ── action='replace' reads btn.dataset.kind ──────────────────


def test_replace_action_dispatcher_forwards_btn_dataset_kind():
    """The action='replace' dispatcher must forward
    `btn.dataset.kind` to `replaceTheme` as its 4th arg so the
    row's CURRENT placement_kind reaches the backend instead of
    falling through to settings.default_placement_method."""
    js = APP_JS.read_text()
    # Find the dispatcher block for act === 'replace'
    idx = js.index("else if (act === 'replace')")
    block = js[idx:idx + 800]
    # The replaceTheme call must include btn.dataset.kind.
    assert "btn.dataset.kind" in block, (
        "v1.18.28: replace dispatcher must forward "
        "btn.dataset.kind to replaceTheme"
    )
    assert "replaceTheme(btn.dataset.mt, btn.dataset.id, btn," in block


# ── Collection PUSH passes kind='api' ────────────────────────


def test_collection_push_to_plex_passes_kind_api():
    """The collection PUSH TO PLEX (single-button) must include
    `kind: 'api'` so the worker never falls through to
    default_placement_method (which could be 'file' and would
    error since collections have no folder)."""
    js = APP_JS.read_text()
    # Anchor on the collection-branch comment so the slice is stable.
    idx = js.index("// v1.18.28: pass kind='api' explicitly")
    block = js[idx:idx + 1000]
    assert "'replace', 'PUSH TO PLEX'" in block
    assert "kind: 'api'" in block


# ── Movie/TV RE-PUSH passes current kind ─────────────────────


def test_movie_tv_repush_preserves_current_placement_kind():
    """RE-PUSH must compute a kind based on the row's CURRENT
    placement_kind — 'api' for plex_upload rows, 'file' for
    sidecar rows — instead of letting the backend default fire."""
    js = APP_JS.read_text()
    # v1.18.46: anchor-based slicing. Pre-fix this was a fixed
    # 1200-char window (widened to 2200 in v1.18.29). Now slices
    # from the v1.18.28 anchor up to the next adjacent v1.18.X
    # marker (the next placement-block comment or the SWITCH
    # PLACEMENT comment that closes the RE-PUSH block).
    from _slice_helpers import slice_to_next
    block = slice_to_next(
        js,
        "// v1.18.28: RE-PUSH must preserve the row's CURRENT",
        "// v1.18.23: SWITCH PLACEMENT",
        "// v1.18.28: same kind-preservation rule",
    )
    # The kind discriminator must be present.
    assert "it.placement_kind === 'plex_upload' ? 'api' : 'file'" in block
    # And it must be passed through to the menu item.
    assert "kind: repushKind" in block


# ── Mismatch PUSH passes current kind ────────────────────────


def test_mismatch_push_to_plex_preserves_current_placement_kind():
    """Mismatch PUSH TO PLEX is structurally a RE-PUSH that
    overwrites the existing placement — it must preserve the
    row's current placement_kind for the same reason."""
    js = APP_JS.read_text()
    idx = js.index(
        "// v1.18.28: same kind-preservation rule as RE-PUSH"
    )
    # v1.20.64: widened 1000→1500 — the PU-aware tooltip ternary now
    # sits between the label and the options object, pushing kind:/tone:
    # further from the anchor.
    block = js[idx:idx + 1500]
    assert "it.placement_kind === 'plex_upload' ? 'api' : 'file'" in block
    assert "kind: mismatchKind" in block


# ── replaceTheme already forwards kind (v1.18.23 regression guard)


def test_replace_theme_helper_still_forwards_kind_body():
    """Regression guard for the v1.18.23 helper signature —
    replaceTheme must still accept a `kind` arg and post it as
    JSON body. v1.18.28 builds on this contract."""
    js = APP_JS.read_text()
    idx = js.index("async function replaceTheme(")
    body = js[idx:idx + 1500]
    assert "async function replaceTheme(mediaType, tmdbId, btn, kind)" in body
    assert "JSON.stringify({ kind })" in body
