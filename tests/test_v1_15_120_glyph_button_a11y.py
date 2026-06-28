"""v1.15.120 — accessible names on glyph-only buttons.

## The gap

Glyph-only buttons (`<button>×</button>`, `<button>1H</button>`,
etc.) have no human-readable label inside. Without `title=` or
`aria-label=`, both screen-reader users AND sighted users who
hover get no context for what the button does.

Initial scan (2026-05-18) found 8 unlabeled glyph buttons:

  5 dialog × close buttons:
    - base.html (info-dlg-close)
    - library.html × 3 (upload / override / manual-url)
    - settings.html (new-token-dlg-close)
  3 event-filter time-range chips in queue.html (1H/24H/7D)

## Fix

All 8 buttons got both `aria-label=` (for assistive tech) and
`title=` (for sighted-hover tooltip). The label values are
descriptive ("Close dialog", "Events from the last hour", etc.)
so the affordance is clear without context.

The 'ALL' time chip also got `title="Show all events"` for
consistency — its text is alphabetic so it wasn't strictly a
gap, but the sibling row deserves uniform tooltip coverage.

## Forward-looking lint

Scans every `<button>...</button>` in `app/web/templates/`. If
the inner content is a short glyph-only string (≤4 chars, less
than half alphabetic) AND the opening tag has neither
`title=` nor `aria-label=` nor `aria-labelledby=`, fail.

Allowlist: empty initially. Entries take the form
`(filename, line_glyph)` with a reason comment.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
TEMPLATE_DIR = REPO / "app" / "web" / "templates"


# Entries: (filename, glyph_text) → reason. Empty initially.
ALLOWLIST: dict[tuple[str, str], str] = {}


def _scan_templates() -> list[tuple[str, int, str, str]]:
    """Return list of (filename, line, glyph, attrs) for each
    glyph-only <button> without an accessible name."""
    gaps: list[tuple[str, int, str, str]] = []
    for f in TEMPLATE_DIR.glob("*.html"):
        src = f.read_text()
        # Strip HTML and Jinja comments so commented-out buttons
        # don't false-positive.
        src = re.sub(r"<!--.*?-->", "", src, flags=re.DOTALL)
        src = re.sub(r"\{#.*?#\}", "", src, flags=re.DOTALL)
        for m in re.finditer(
            r"<button\b([^>]*)>([^<]+)</button>", src,
        ):
            attrs = m.group(1)
            content = m.group(2).strip()
            # Skip non-glyph buttons (longer than 4 chars).
            if len(content) > 4 or not content:
                continue
            alpha = sum(1 for c in content if c.isalpha())
            # All-alphabetic short words (e.g. "ALL", "OK") might
            # still need labels but they're contextual; skip to
            # focus on actual symbol-only buttons.
            if alpha == len(content):
                continue
            # If less than half is alphabetic, treat as glyph.
            if alpha > len(content) / 2:
                continue
            has_title = "title=" in attrs
            has_aria_label = "aria-label=" in attrs
            has_aria_labelledby = "aria-labelledby=" in attrs
            if has_title or has_aria_label or has_aria_labelledby:
                continue
            line = src[: m.start()].count("\n") + 1
            gaps.append((f.name, line, content, attrs.strip()))
    return gaps


def test_no_unlabeled_glyph_only_buttons():
    """Every glyph-only `<button>` must carry `title=` or
    `aria-label=` (or `aria-labelledby=`). Pre-v1.15.120 there
    were 8 unlabeled glyph buttons — 5 × close dialogs + 3
    time-range chips (1H/24H/7D). All 8 got `aria-label=` +
    `title=` in this commit."""
    gaps = _scan_templates()
    findings = [
        f"{name}:{line}  content={glyph!r}  attrs={attrs[:80]}"
        for name, line, glyph, attrs in gaps
        if (name, glyph) not in ALLOWLIST
    ]
    assert not findings, (
        "v1.15.120: glyph-only buttons must have an accessible "
        "name via `title=`, `aria-label=`, or `aria-labelledby=`. "
        "Screen-reader users get no context otherwise. Add an "
        "appropriate label or allowlist with reason.\n  - "
        + "\n  - ".join(findings)
    )


def test_close_buttons_have_aria_label():
    """The × close button is the most common a11y gap. Every
    `<button class="dlg-close">×` template instance must carry
    `aria-label=` so assistive tech announces the affordance."""
    pattern_with_label = re.compile(
        r"<button[^>]*class=\"dlg-close\"[^>]*aria-label=",
    )
    pattern_any = re.compile(
        r"<button[^>]*class=\"dlg-close\"[^>]*>",
    )
    for f in TEMPLATE_DIR.glob("*.html"):
        src = f.read_text()
        all_close = pattern_any.findall(src)
        labeled = pattern_with_label.findall(src)
        assert len(labeled) == len(all_close), (
            f"v1.15.120: in {f.name}, {len(all_close) - len(labeled)} "
            f"`.dlg-close` button(s) missing aria-label. Every × "
            "close affordance must be labeled for screen-reader users."
        )


def test_allowlist_entries_remain_relevant():
    """Stale allowlist entries get dropped — same hygiene as
    v1.15.89 / .111 / .116 lints."""
    if not ALLOWLIST:
        return
    gaps = _scan_templates()
    actual = {(name, glyph) for name, _, glyph, _ in gaps}
    stale = [entry for entry in ALLOWLIST if entry not in actual]
    assert not stale, f"Stale allowlist entries: {stale}"
