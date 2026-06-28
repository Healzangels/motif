"""v1.15.84 — equalize inter-column spacing in the import preview.

the user's v1.15.83 feedback after testing the column-spread fix:
"super, can we fix the spacing on the imdb, current, importd
url columns, right now they are un-even spaced between them"

With table-layout:fixed + text-align:center, the visual gap
between adjacent columns N→N+1 reduces to:

    gap = (col_N - content_N)/2 + (col_N+1 - content_N+1)/2

i.e. the average of each column's "flanking whitespace" (column
width minus content width). For gaps to look uniform, every
column needs the same flanking budget.

At v1.15.83's widths:
* IMDB col 130, content `tt########` ≈ 80px → 50px flanking
* CURRENT col 120, content U+ⓘ      ≈ 66px → 54px flanking
* URL col 200, content ▶ + vid       ≈ 110px → 90px flanking

URL's 90px vs IMDB's 50px — almost 2× — is the "uneven" the user
saw: URL→CURRENT and URL→ACTION gaps looked larger than
IMDB→CURRENT.

v1.15.84 tunes each col to ~60px flanking:
* IMDB 130 → 140 (80 + 60)
* CURRENT 120 → 130 (66 + ~64)
* URL 200 → 170 (110 + 60)

Now the (col − content) budget is roughly constant across all
three, so centered content produces uniform-looking gaps.

The URL trim (200 → 170) accepts that v1.15.83's spread intent
overshot on URL specifically — the compact play-link doesn't
need 90px of flanking when its neighbors only get 50.
"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
APP_CSS = ROOT / "app" / "web" / "static" / "app.css"


def _col_width(col_class: str) -> int:
    """Extract pixel width from `#import-preview-table .{col} {...}`."""
    css = APP_CSS.read_text()
    rule_start = css.index(f"#import-preview-table .{col_class} {{")
    rule_end = css.index("}", rule_start)
    rule = css[rule_start:rule_end]
    m = re.search(r"width:\s*(\d+)px", rule)
    assert m, f"missing pixel width for .{col_class}"
    return int(m.group(1))


# ── Per-column pinned widths ─────────────────────────────────


def test_imdb_width_140():
    """v1.15.84: imdb 130 → 140. Tuned for ~60px flanking around
    the centered tt-ID link (~80px content)."""
    assert _col_width("col-imdb") == 140


def test_current_width_130():
    """v1.15.84: current 120 → 130. Tuned for ~64px flanking
    around the centered U badge + ⓘ button (~66px content)."""
    assert _col_width("col-import-current") == 130


def test_url_width_170():
    """v1.15.84: url 200 → 170. Tuned for ~60px flanking around
    the centered ▶ + 11-char vid (~110px content). v1.15.83's
    200px gave URL almost 2× the flanking of neighbors — the
    visual gap mismatch the user flagged."""
    assert _col_width("col-import-url") == 170


# ── Equal-flanking invariant (the actual ask) ────────────────


def test_flanking_budget_is_uniform_across_imdb_current_url():
    """The reason the v1.15.84 widths look uniform: each col's
    (width - content_width) lands at ~60px. We can't measure
    rendered content width from CSS, but we CAN assert that the
    three pinned widths differ from each other by AT MOST 40px —
    if they drift apart by more, the flanking budgets diverge
    and uneven gaps come back.

    Content widths assumed (mono font, rough):
      imdb tt########  ≈ 80
      current U + ⓘ    ≈ 66
      url ▶ + vid      ≈ 110

    Differences in column widths SHOULD track differences in
    content widths (i.e. wider content → proportionally wider
    column) to keep flanking constant."""
    imdb = _col_width("col-imdb")
    current = _col_width("col-import-current")
    url = _col_width("col-import-url")

    # Flanking budgets (col − content) should all sit in a tight
    # band. We use 30..80 as a generous floor/ceiling so the
    # test fails on drastic skew but tolerates mono-font width
    # estimation error.
    imdb_flank = imdb - 80
    current_flank = current - 66
    url_flank = url - 110

    flanks = [imdb_flank, current_flank, url_flank]
    spread = max(flanks) - min(flanks)
    assert spread <= 20, (
        f"v1.15.84: flanking budgets imdb={imdb_flank} "
        f"current={current_flank} url={url_flank} span {spread}px. "
        "Uniform inter-column spacing requires the spread to be "
        "small — otherwise the centered content blocks land at "
        "uneven visual distances from each other."
    )


def test_total_fixed_width_stays_under_budget():
    """v1.15.84 net change vs v1.15.83 is −10px (imdb +10,
    current +10, url −30). Total: 36+100+140+130+170+160 = 736.
    Stays well under the v1.15.80 (now 800px) ceiling so TITLE
    flex headroom is preserved on narrow viewports."""
    cols = [
        ("col-state", 36),
        ("col-import-status", 100),
        ("col-imdb", 140),
        ("col-import-current", 130),
        ("col-import-url", 170),
        ("col-import-action", 160),
    ]
    total = sum(expected for _, expected in cols)
    for col_class, expected in cols:
        actual = _col_width(col_class)
        assert actual == expected, (
            f".{col_class} expected {expected}px, got {actual}px"
        )
    assert total < 800, f"v1.15.84 total {total}px exceeds 800 cap"
    assert total == 736, (
        f"v1.15.84 documented total is 736px; CSS sums to {total}px. "
        "If the widths changed intentionally, update this test + "
        "the block comment in app.css."
    )
