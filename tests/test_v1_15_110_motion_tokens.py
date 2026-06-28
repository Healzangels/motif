"""v1.15.110 — motion duration tokens.

DESIGN_SYSTEM.md § 6 flagged the missing motion token vocabulary:
transition durations were written as raw `0.12s` / `0.15s` / `80ms`
ad-hoc with no consistency. v1.15.110 adds three motion tokens:

  --motion-fast: 80ms    — button press / transform feedback
  --motion-normal: 120ms — hover / color / border transitions
  --motion-slow: 400ms   — bar fills, layout shifts

The 0.12s and 0.15s clusters (13 sites total) collapse onto
--motion-normal — visually indistinguishable. The lone 0.6s
coverage-bar width keeps raw (different design intent).

## Tests

Static guards that the tokens are defined + that no raw
duration values remain in transition rules (except the
documented outlier).
"""

from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = REPO / "app" / "web" / "static" / "app.css"


def test_motion_tokens_defined_in_root():
    """Three motion tokens must be defined in `:root`."""
    src = APP_CSS.read_text()
    root_end = src.index("\n}", src.index(":root"))
    root_block = src[: root_end]
    assert "--motion-fast:" in root_block
    assert "--motion-normal:" in root_block
    assert "--motion-slow:" in root_block


def test_motion_tokens_have_expected_values():
    src = APP_CSS.read_text()
    assert "--motion-fast: 80ms" in src
    assert "--motion-normal: 120ms" in src
    assert "--motion-slow: 400ms" in src


def test_no_raw_duration_in_transition_rules():
    """No `transition: ... 0.12s ...` etc. remain. The only
    permitted raw values are:
      - 0.6s width transition on coverage bars (deliberate
        slow contrast vs the 0.4s fill cluster)
      - 0.2s opacity inside the .pulsed-pill rule (too
        narrow to justify a fourth token)"""
    src = APP_CSS.read_text()
    # Strip comments — the :root doc comment intentionally
    # references the pre-fix raw values.
    code = re.sub(r"/\*.*?\*/", "", src, flags=re.DOTALL)
    # Strip every well-formed motion-token reference so what
    # remains in each transition body is only the truly-raw
    # duration substrings. Then any `Xms` or `0.Xs` literal
    # still inside a `transition:` rule is a non-tokenized site.
    stripped = re.sub(r"var\(--motion-(?:fast|normal|slow)\)",
                      "<<MOTION>>", code)
    bad = []
    for m in re.finditer(r"transition:[^;}]+[;}]", stripped):
        body = m.group(0)
        # Allow-listed outliers (kept raw on purpose):
        if "0.6s" in body and "width" in body:
            continue
        if "stroke-dasharray" in body and "opacity 0.2s" in body:
            continue
        if re.search(r"\b(?:0?\.\d+s|\d+ms)\b", body):
            bad.append(body)
    assert not bad, (
        "v1.15.110: every transition must use --motion-fast / "
        "--motion-normal / --motion-slow tokens. Outliers (0.6s "
        "coverage width, 0.2s pulsed-pill opacity) are explicit. "
        f"Found raw durations in: {bad}"
    )


def test_documented_outlier_preserved():
    """The 0.6s coverage-bar width is the documented outlier —
    its deliberate slowness distinguishes it from the 0.4s fill
    cluster. Must remain raw."""
    src = APP_CSS.read_text()
    assert "transition: width 0.6s ease;" in src, (
        "The 0.6s coverage-bar width outlier must remain — "
        "its slower cadence is design intent."
    )
