"""v1.18.29 — PLACE menu items color-code by placement_kind.

Follow-on to v1.18.28: now that RE-PUSH preserves the row's
current `placement_kind` instead of silently demoting via
`settings.default_placement_method`, the BUTTON also needs to
visually telegraph which kind it will produce. Pre-v1.18.29
every placement action wore the same amber `warn` tint, so a
RE-PUSH on a plex_upload row looked identical to a RE-PUSH on
a sidecar row even though the new v1.18.28 behavior was
correct — the user couldn't tell which placement kind was about
to fire without inspecting the LINK chip on the row.

## Color vocab

Mirrors the LINK column chips so the row + button colors pair
visually:

  - PUSH / RE-PUSH / SWITCH targeting `placement_kind='file'`
    (hardlink sidecar) → green (`tone: 'place_file'`,
    `.btn.lib-source-place_file`) — same hue family as the HL
    LINK chip.

  - PUSH / RE-PUSH / SWITCH targeting `placement_kind='plex_upload'`
    (API upload) → cyan (`tone: 'place_api'`,
    `.btn.lib-source-place_api`) — same hue family as the PU
    LINK chip.

SWITCH buttons take the TARGET kind's color (where the
placement is going), not the current kind's color — the color
foreshadows the row's new LINK chip after the click.

RE-PUSH on movie/TV rows also gains a `(FILE)` / `(API)` suffix
in the label so the choice is unambiguous in monochrome contexts
(accessibility — color-blind users + screenshots).

## Files

  - app/web/static/app.css: new `.btn.lib-source-place_file` +
    `.btn.lib-source-place_api` rules (with `:hover` variants).
  - app/web/static/app.js: tone wired into 6 menu items —
    collection PUSH, movie/TV PUSH (FILE)/(API), movie/TV
    RE-PUSH, SWITCH TO SIDECAR/API, mismatch PUSH. Vocab comment
    block in `menuItemHtml` updated.

The v1.14.50 cross-reference test already enforces that every
JS `tone:'X'` literal has a matching `.btn.lib-source-X` CSS
rule (and vice-versa) — adding the two tones plus their CSS
rules satisfies it without changes.
"""

from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_JS = REPO / "app" / "web" / "static" / "app.js"
APP_CSS = REPO / "app" / "web" / "static" / "app.css"


# ── CSS: new tone classes exist ──────────────────────────────


def test_css_has_place_file_tone_rule():
    """`.btn.lib-source-place_file` must declare a green color
    matching the HL LINK chip. v0.51.116: uses the FIXED --ok-bright /
    --ok-deep tokens (the HL chip's tone), NOT the themeable --green
    alias — the action color encodes the placement kind, so it must
    stay green on every theme to keep matching the fixed HL chip."""
    css = APP_CSS.read_text()
    assert ".btn.lib-source-place_file {" in css
    # Find the rule block.
    idx = css.index(".btn.lib-source-place_file {")
    block = css[idx:idx + 400]
    assert "var(--ok-bright)" in block, (
        "v0.51.116: place_file tone uses the FIXED --ok-bright to match "
        "the HL link chip, not the themeable --green alias"
    )
    assert "var(--ok-deep)" in block
    # must NOT ride the themeable green/accent aliases.
    assert "var(--green" not in block and "var(--accent" not in block


def test_css_has_place_api_tone_rule():
    """`.btn.lib-source-place_api` must declare a cyan color
    matching the PU LINK chip (uses --cyan, same pair as
    `.btn.lib-source-adopt`)."""
    css = APP_CSS.read_text()
    assert ".btn.lib-source-place_api {" in css
    idx = css.index(".btn.lib-source-place_api {")
    block = css[idx:idx + 400]
    assert "var(--cyan)" in block, (
        "v1.18.29: place_api tone uses --cyan to match PU link "
        "chip color family"
    )


def test_css_place_tones_have_hover_variants():
    """Every other lib-source tone has a `:hover:not(:disabled)`
    rule. Pin the same shape for place_file / place_api so
    hover-state feedback stays consistent across the SOURCE +
    PLACE menus."""
    css = APP_CSS.read_text()
    assert ".btn.lib-source-place_file:hover:not(:disabled) {" in css
    assert ".btn.lib-source-place_api:hover:not(:disabled) {" in css


# ── JS: tone wired into placement menu items ─────────────────


def test_collection_push_uses_place_api_tone():
    """Collection PUSH TO PLEX is always API (no folder option)
    so it must take the cyan place_api tone."""
    js = APP_JS.read_text()
    # Anchor on the v1.18.28 comment block that gates the
    # collection branch.
    idx = js.index("// v1.18.28: pass kind='api' explicitly")
    block = js[idx:idx + 1000]
    assert "tone: 'place_api'" in block, (
        "v1.18.29: collection PUSH must use place_api tone "
        "(always cyan — always an API upload)"
    )


def test_movie_tv_push_file_uses_place_file_tone():
    """PUSH TO PLEX (FILE) must take the green place_file tone."""
    js = APP_JS.read_text()
    idx = js.index("'replace-file', 'PUSH TO PLEX (FILE)'")
    block = js[idx:idx + 500]
    assert "tone: 'place_file'" in block, (
        "v1.18.29: PUSH TO PLEX (FILE) must use place_file tone"
    )


def test_movie_tv_push_api_uses_place_api_tone():
    """PUSH TO PLEX (API) must take the cyan place_api tone."""
    js = APP_JS.read_text()
    idx = js.index("'replace-api', 'PUSH TO PLEX (API)'")
    block = js[idx:idx + 500]
    assert "tone: 'place_api'" in block, (
        "v1.18.29: PUSH TO PLEX (API) must use place_api tone"
    )


def test_repush_label_branches_by_kind_on_movie_tv():
    """RE-PUSH on movie/TV must surface its kind in the label
    so the choice is unambiguous in monochrome contexts.
    Collections stay plain `RE-PUSH` (no kind ambiguity)."""
    js = APP_JS.read_text()
    # Anchor on the v1.18.29 RE-PUSH label comment.
    idx = js.index(
        "// v1.18.29: surface the placement_kind in the label"
    )
    block = js[idx:idx + 1500]
    # Movie/TV branch produces the (FILE)/(API) suffix labels.
    assert "'RE-PUSH (API)'" in block
    assert "'RE-PUSH (FILE)'" in block
    # Collections branch keeps the plain label.
    assert "'RE-PUSH'" in block
    # The branch must key on isCollectionRow.
    assert "isCollectionRow" in block


def test_repush_tone_matches_kind():
    """RE-PUSH's tone must mirror the same kind discriminator
    used for the label — repushKind='api' → place_api,
    otherwise place_file."""
    js = APP_JS.read_text()
    idx = js.index(
        "// v1.18.29: surface the placement_kind in the label"
    )
    block = js[idx:idx + 1500]
    assert (
        "repushKind === 'api'\n        ? 'place_api' : 'place_file'"
        in block
    ), (
        "v1.18.29: RE-PUSH tone must branch on repushKind to "
        "match the (FILE)/(API) label"
    )
    # Pass-through to the menu item.
    assert "tone: repushTone" in block


def test_switch_buttons_tone_matches_target_kind():
    """SWITCH TO SIDECAR / SWITCH TO API tones reflect the
    TARGET kind (where the placement is going). isPlexUpload
    true → switching TO sidecar → place_file; isPlexUpload
    false → switching TO api → place_api."""
    js = APP_JS.read_text()
    idx = js.index("// v1.18.29: SWITCH buttons color-code by the TARGET")
    block = js[idx:idx + 1000]
    assert (
        "isPlexUpload ? 'place_file' : 'place_api'" in block
    ), (
        "v1.18.29: SWITCH tone must invert isPlexUpload — the "
        "TARGET kind drives the color, not the current kind"
    )
    assert "tone: switchTone" in block


def test_mismatch_push_tone_matches_kind():
    """Mismatch PUSH TO PLEX must color-code by the kind it
    will overwrite the placement with (same rule as RE-PUSH)."""
    js = APP_JS.read_text()
    idx = js.index("// v1.18.29: same tone-by-kind rule as RE-PUSH")
    # v1.20.64: widened 600→1100 — the PU-aware tooltip ternary now sits
    # between the label and the options object.
    block = js[idx:idx + 1100]
    assert (
        "mismatchKind === 'api'\n        ? 'place_api' : 'place_file'"
        in block
    )
    assert "tone: mismatchTone" in block


# ── menuItemHtml tone vocab comment documents the new tones ──


def test_menu_item_html_vocab_comment_documents_place_tones():
    """The aligned vocab comment block at the top of
    menuItemHtml must list the new tones so a contributor
    reading the function sees the full vocab in one place.
    Also enforced by the v1.14.50 cross-reference test, but
    pin it here too so a regression in either direction
    surfaces in the v1.18.29 test file."""
    js = APP_JS.read_text()
    fn_idx = js.index(
        "function menuItemHtml(act, label, tip, extras = {})"
    )
    body = js[fn_idx:fn_idx + 5700]
    assert "//   place_file" in body
    assert "//   place_api" in body
