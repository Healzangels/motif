"""v1.15.116 — CSS duplicate-rule lint + 3 inline-style → primitive migrations.

Two related cleanups bundled.

## CSS duplicate-rule lint

v1.15.115 just shipped a fix for two `.dlg-close` rules that
silently cascade-fought each other. The lint prevents the
class of bug — duplicate top-level selectors with overlapping
properties — from re-appearing.

Scan: every top-level rule block `SELECTOR { ... }` in app.css
+ ops.css. If the same SELECTOR_LIST string appears more than
once, fail with a useful message.

Allowlist: empty for now. If a future case needs two rules
(e.g. progressive enhancement / media query overlap), add an
entry with the reason.

## 3 inline-style → primitive migrations

Inline-style audit surfaced three JS template-literal rows that
re-implement existing `.mono` + `.small` + `.muted` primitives
inline:

  - app.js:2981 (storage-copies media_folder cell)
  - app.js:3765 (libraries table .lib-locations cell)
  - app.js:3979 (api-tokens token_prefix cell)

All three had `font-family:var(--font-mono);font-size:var
(--t-tiny)` inline. Replaced with `class="mono small"` (and a
color inline kept where the original color was `--fg-dim`,
which doesn't match `.muted`'s `--fg-mute`).

This is a DESIGN_SYSTEM hygiene win: when primitives exist,
JS template literals should USE them rather than re-implement
them inline.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = REPO / "app" / "web" / "static" / "app.css"
OPS_CSS = REPO / "app" / "web" / "static" / "ops.css"
APP_JS = REPO / "app" / "web" / "static" / "app.js"


# ── CSS duplicate-rule lint ─────────────────────────────────

# Selectors known to legitimately appear twice. Keep tight —
# adding entries should always come with a comment explaining
# why the duplicate isn't a bug.
DUP_ALLOWLIST: set[str] = set()


def _top_level_selectors(src: str) -> list[str]:
    """Return the list of top-level selector strings in CSS
    source — i.e. the SELECTOR_LIST text immediately preceding
    each `{` at brace-depth 0. Comments stripped first."""
    src = re.sub(r"/\*.*?\*/", "", src, flags=re.DOTALL)
    sels: list[str] = []
    depth = 0
    buf: list[str] = []
    for c in src:
        if c == "{":
            if depth == 0:
                sel = "".join(buf).strip()
                if sel and not sel.startswith("@"):
                    sels.append(re.sub(r"\s+", " ", sel))
                buf = []
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                buf = []
        elif depth == 0:
            buf.append(c)
    return sels


def test_no_duplicate_top_level_selectors():
    """Same SELECTOR_LIST defined more than once at the top level
    is a cascade-fight class waiting to surface. Pre-fix v1.15.115:
    two `.dlg-close` rules silently overrode each other, shrinking
    the × close button from 22px to 13px on 4 of 5 dialogs."""
    from collections import Counter
    for path in (APP_CSS, OPS_CSS):
        src = path.read_text()
        sels = _top_level_selectors(src)
        counts = Counter(sels)
        dups = [s for s, n in counts.items()
                if n > 1 and s not in DUP_ALLOWLIST]
        assert not dups, (
            f"v1.15.116: duplicate top-level rule(s) in {path.name}. "
            "Same selector + same specificity → later rule wins on "
            "overlapping properties, silently overriding the earlier. "
            "Consolidate them or use a more-specific selector (e.g. "
            "descendant combinator) for the variant. "
            f"Duplicates: {dups}"
        )


def test_allowlist_entries_are_still_duplicated():
    """If an allowlist entry no longer matches a duplicate, drop
    the entry — same hygiene as the v1.15.89 / .111 lints."""
    if not DUP_ALLOWLIST:
        return
    for path in (APP_CSS, OPS_CSS):
        src = path.read_text()
        sels = _top_level_selectors(src)
        for entry in list(DUP_ALLOWLIST):
            count = sels.count(entry)
            if count >= 2:
                DUP_ALLOWLIST.discard(entry)
                break
    assert not DUP_ALLOWLIST, (
        f"Stale allowlist entries: {DUP_ALLOWLIST}"
    )


# ── inline-style → primitive migrations ──────────────────────

def test_mono_small_primitives_replace_inline_font_styles():
    """The three known JS template-literal sites that previously
    inlined `font-family:var(--font-mono);font-size:var(--t-tiny)`
    must now use `class="mono small"` (sometimes with extra
    classes / color inline)."""
    src = APP_JS.read_text()
    # The pre-fix inline must not appear anymore. Catches a
    # regression where someone re-inlines the font/size pair
    # instead of using the primitives.
    bad = re.findall(
        r'style="[^"]*font-family:var\(--font-mono\);'
        r'font-size:var\(--t-tiny\)[^"]*"',
        src,
    )
    assert not bad, (
        "v1.15.116: inline font-mono/t-tiny pattern reintroduced. "
        "Use `class=\"mono small\"` instead — the primitives exist "
        f"for exactly this purpose. Found: {bad}"
    )


def test_lib_locations_uses_mono_small_class():
    """Sanity: the libraries-table location cell uses the
    primitives now (was inline-styled pre-v1.15.116)."""
    src = APP_JS.read_text()
    assert 'class="lib-locations mono small"' in src
