"""v1.14.50 — static cross-reference: every JS tone literal has a matching CSS class.

the user's testing-strategy ask after the v1.14.48 incident:

> "How can we test in the future to make sure that doesn't happen?"

The bug class: JS code passes `tone: 'X'` to a render helper that
emits a CSS class name like `lib-source-X` or routes through a
JS-side `TONE_CLASS` map to a `btn-X` class. If the matching CSS
declaration doesn't exist, the button silently falls back to plain
styling — no console error, no test failure under the existing
static-text guards. v1.14.42 (closed-DB hotfix) was a runtime
crash version of the same "static guards passed; runtime broke"
shape; v1.14.48 was the visual-only twin.

This test catches the v1.14.48 shape exactly: extract every tone
literal from each surface, derive the CSS class it would emit,
assert each class exists in app.css.

## Surfaces audited

1. **SOURCE-menu axis** (app.js → CSS)
   `menuItemHtml(..., { tone: '<X>' })` emits `lib-source-<X>`.
   Each `<X>` must have a `.btn.lib-source-<X> {` rule in app.css.

2. **Recovery-card axis** (api.py → JS TONE_CLASS map → CSS)
   api.py options carry `"tone": "<X>"`. The JS-side
   `TONE_CLASS` map (app.js:9548) routes `<X>` to a CSS class
   name. Each api.py tone must:
   - Have a key in `TONE_CLASS`
   - Have its mapped CSS class declared in app.css

A new tone added to either axis without the matching CSS rule
fails this test immediately.
"""
from __future__ import annotations

import re
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
JS = (REPO / "app" / "web" / "static" / "app.js")
CSS = (REPO / "app" / "web" / "static" / "app.css")
API = (REPO / "app" / "web" / "api.py")


# ── SOURCE-menu axis: tone in extras → lib-source-{tone} class ─


def _source_menu_tones() -> set[str]:
    """Extract every `tone: '<X>'` literal from app.js. These are
    the SOURCE-menu menuItemHtml extras — single-quoted JS string
    literals."""
    return set(re.findall(r"\btone:\s*'([a-z_]+)'", JS.read_text()))


def _css_lib_source_classes() -> set[str]:
    """Extract every `.btn.lib-source-<X> {` rule declaration
    from app.css. The trailing `{` is required so a marker
    comment that mentions the class name doesn't false-positive."""
    return set(re.findall(r"\.btn\.lib-source-([a-z_]+)\s*\{", CSS.read_text()))


def test_source_menu_tones_all_have_matching_css_class():
    """Every JS `tone: '<X>'` literal in app.js must have a
    matching `.btn.lib-source-<X>` rule in app.css.

    Pre-v1.14.48 this would have failed with `tone: 'plex'` ∉
    {themerrdb, user, adopt, manual, cloud}. v1.14.48 added the
    `.btn.lib-source-plex` rule (renamed from the unused
    `lib-source-cloud`)."""
    js_tones = _source_menu_tones()
    css_tones = _css_lib_source_classes()
    missing = js_tones - css_tones
    assert not missing, (
        f"SOURCE-menu tone(s) {sorted(missing)} passed in JS but no "
        f".btn.lib-source-<tone> rule exists in app.css. Without the "
        f"matching CSS rule the button falls back to plain styling. "
        f"Add the CSS class or remove the JS callsite. "
        f"(JS tones: {sorted(js_tones)}; CSS tones: {sorted(css_tones)})"
    )


def test_source_menu_no_unused_lib_source_classes():
    """Inverse guard: every `.btn.lib-source-<X>` CSS rule must
    have at least one JS caller passing `tone: '<X>'`. Catches the
    pre-v1.14.48 dead-code state where `.btn.lib-source-cloud`
    sat in CSS for ~3 years with zero callers — invisible drift
    that delayed surfacing v1.14.48 (an unused-class sweep would
    have flagged it).

    Tolerates the case where the JS lookup is stripped of comments
    so a marker mentioning a stale tone name doesn't keep the
    class alive artificially."""
    js_raw = JS.read_text()
    js_no_comments = "\n".join(
        line for line in js_raw.splitlines()
        if not line.lstrip().startswith("//")
    )
    js_tones = set(re.findall(r"\btone:\s*'([a-z_]+)'", js_no_comments))
    css_tones = _css_lib_source_classes()
    unused = css_tones - js_tones
    assert not unused, (
        f"`.btn.lib-source-{{tone}}` CSS rule(s) exist for tones "
        f"{sorted(unused)} but no JS caller passes `tone: '<tone>'`. "
        f"Either delete the dead CSS or wire up the caller. "
        f"(unused-class drift was the v1.14.48 root cause)."
    )


# ── Recovery-card axis: api.py tone → TONE_CLASS map → CSS ────


def _api_recovery_tones() -> set[str]:
    """Extract every `\"tone\": \"<X>\"` literal from api.py.
    These are the recovery-option dicts returned by
    api_recovery_options."""
    return set(re.findall(r'"tone":\s*"([a-z_]+)"', API.read_text()))


def _js_tone_class_map() -> dict[str, str]:
    """Parse the `TONE_CLASS = { ... }` literal in app.js into a
    Python dict {tone: css_class_name}."""
    js = JS.read_text()
    anchor = js.index("const TONE_CLASS = {")
    block_end = js.index("};", anchor)
    block = js[anchor:block_end]
    # Each entry is `key: 'value',` (or last entry without trailing
    # comma). Capture both bare-identifier keys and quoted keys.
    entries = re.findall(r"(\w+)\s*:\s*'([a-zA-Z0-9_-]+)'", block)
    return dict(entries)


def _css_classes() -> set[str]:
    """Extract every CSS class declaration from app.css. Returns
    the bare class names (no leading dot)."""
    return set(re.findall(r"\.([a-zA-Z0-9_-]+)\s*\{", CSS.read_text()))


def test_recovery_card_tones_all_in_tone_class_map():
    """Every `"tone": "<X>"` literal in api.py must have a
    corresponding key in the JS `TONE_CLASS` map. An api.py tone
    with no map entry would render as plain styling (the JS
    fallback is `TONE_CLASS[opt.tone] || ''`)."""
    api_tones = _api_recovery_tones()
    map_keys = set(_js_tone_class_map().keys())
    missing = api_tones - map_keys
    assert not missing, (
        f"api.py recovery options use tone(s) {sorted(missing)} "
        f"but the JS TONE_CLASS map (app.js:9548) has no entry. "
        f"Add the map entry or change the api.py tone string. "
        f"(api.py tones: {sorted(api_tones)}; map keys: {sorted(map_keys)})"
    )


def test_recovery_card_tone_class_values_all_in_css():
    """Every value in the JS `TONE_CLASS` map must be a CSS class
    that actually exists in app.css. Catches the case where a map
    entry points at a class that was renamed / deleted out from
    under it (the inverse of v1.14.48 — JS thinks the class
    exists, but it doesn't)."""
    tone_map = _js_tone_class_map()
    css_classes = _css_classes()
    # Each mapped value is either `lib-source-<X>` or `btn-<X>`.
    # Both forms appear in app.css as bare class names.
    missing = {
        tone: cls for tone, cls in tone_map.items()
        if cls not in css_classes
    }
    assert not missing, (
        f"JS TONE_CLASS map points at CSS class(es) that don't "
        f"exist in app.css: {missing}. Either add the CSS rule or "
        f"fix the map value."
    )


def test_recovery_card_no_unused_tone_class_entries():
    """Inverse guard: every key in the JS `TONE_CLASS` map must
    be referenced by at least one api.py `"tone": "<X>"` literal.
    Catches drift where a tone gets retired from api.py but the
    map entry survives + ages into a foot-gun."""
    api_tones = _api_recovery_tones()
    map_keys = set(_js_tone_class_map().keys())
    unused = map_keys - api_tones
    assert not unused, (
        f"JS TONE_CLASS map has key(s) {sorted(unused)} with no "
        f"api.py caller passing that tone. Either remove the map "
        f"entry or wire up the caller (or document the planned "
        f"caller in a marker comment)."
    )


# ── Documentation pin ────────────────────────────────────────


def test_menu_item_html_tone_vocab_documents_all_source_menu_tones():
    """The menuItemHtml() comment block (app.js, ~line 5500-5520)
    documents the valid SOURCE-menu tones for future contributors.
    It must list every tone that has a `.btn.lib-source-<tone>`
    CSS rule — otherwise a contributor reading the comment + the
    CSS in isolation can pick a tone the comment doesn't mention
    (or worse, miss that the comment list is the wrong source of
    truth).

    Pin: every CSS-declared tone appears as `<tone>     = ` (the
    aligned-table format the comment uses) in the comment block."""
    js = JS.read_text()
    fn_anchor = js.index("function menuItemHtml(act, label, tip, extras = {}) {")
    # The vocab comment sits ~50 lines into the function body.
    body = js[fn_anchor:fn_anchor + 5700]
    css_tones = _css_lib_source_classes()
    for tone in sorted(css_tones):
        assert f"//   {tone}" in body, (
            f"menuItemHtml() tone vocab comment doesn't mention "
            f"`{tone}` (CSS rule exists but contributor docs lag). "
            f"Update the comment block at the top of menuItemHtml."
        )
