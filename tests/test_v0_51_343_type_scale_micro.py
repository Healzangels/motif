"""v0.51.343: the 9px step has a name, --t-micro, so a type-scale change carries its pills and captions."""
from __future__ import annotations

import re
from pathlib import Path

from _slice_helpers import slice_between

REPO = Path(__file__).resolve().parent.parent
STATIC = REPO / "app" / "web" / "static"
APP_CSS = (STATIC / "app.css").read_text()
OPS_CSS = (STATIC / "ops.css").read_text()

# v0.51.343: the dialog close glyph's 22px pre-dates this lint and only coincides with --t-large
NOT_THE_SCALE = {(".dlg-close", "22px")}


def _scale() -> dict[str, float]:
    root = slice_between(APP_CSS, "\n:root {", "\n}")
    return {name: float(px) for name, px in re.findall(r"^\s*(--t-[\w-]+):\s*(\d+(?:\.\d+)?)px;", root, re.M)}


def _font_sizes(css: str):
    body = re.sub(r"/\*.*?\*/", "", css, flags=re.S)
    for m in re.finditer(r"([^{}]+)\{([^{}]*)\}", body):
        selector = " ".join(m.group(1).split())
        if selector.startswith(":root"):
            continue
        for value in re.findall(r"(?:^|[;\s])font-size:\s*([^;}]+)", m.group(2)):
            # v0.51.344: `9px !important` still retypes the 9px step (PB-083)
            yield selector, re.sub(r"\s*!important$", "", value.strip())
        for value in re.findall(r"(?:^|[;\s])font:\s*([^;}]+)", m.group(2)):
            # v0.51.344: a font: shorthand's size (the token a /line-height may follow) is a font-size too
            size = re.search(r"(?:^|\s)(\d*\.?\d+px)(?=/|\s|$)", value.strip())
            if size:
                yield selector, size.group(1)


def test_the_size_reader_sees_important_and_the_font_shorthand():
    css = ".a { font-size: 9px !important; }\n.b { font: 700 11px/14px var(--font-mono); }\n.c { font: inherit; }"
    assert list(_font_sizes(css)) == [(".a", "9px"), (".b", "11px")]


def test_the_smallest_step_is_named():
    scale = _scale()
    assert "--t-micro" in scale and scale["--t-micro"] < min(v for k, v in scale.items() if k != "--t-micro"), scale


def test_no_font_size_retypes_a_scale_step():
    steps = {f"{px:g}px" for px in _scale().values()}
    assert len(steps) >= 7, steps
    sizes = [pair for css in (APP_CSS, OPS_CSS) for pair in _font_sizes(css)]
    assert len(sizes) > 100, len(sizes)
    offenders = {(selector, value) for selector, value in sizes if value in steps} - NOT_THE_SCALE
    assert not offenders, f"read the type-scale token instead of retyping its size: {sorted(offenders)}"
