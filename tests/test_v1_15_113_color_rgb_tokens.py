"""v1.15.113 — `--<color>-rgb` triple tokens for rgba composition.

Closes the largest open DESIGN_SYSTEM.md § 6 gap: 151 sites
across app.css + ops.css hardcoded brand-color RGB triplets
inline (e.g. `background: rgba(109,255,181, 0.08)`). A palette
adjustment had to touch every site individually.

## Tokens added

11 `--<color>-rgb` triple tokens in `:root`, each mapping to the
RGB components of an existing hex color token:

  --green-rgb   109, 255, 181 (matches --green)
  --amber-rgb   255, 184, 74  (matches --amber)
  --orange-rgb  255, 122, 58  (matches --orange)
  --red-rgb     255, 107, 107 (matches --red)
  --cyan-rgb    109, 211, 255 (matches --cyan)
  --blue-rgb    109, 143, 255 (matches --blue)
  --violet-rgb  196, 109, 255 (matches --violet)
  --magenta-rgb 255, 122, 214 (matches --magenta)
  --brown-rgb   192, 133, 82  (matches --brown)
  --white-rgb   255, 255, 255 (overlay / shadow utility)
  --black-rgb   0, 0, 0       (overlay / shadow utility)

## Migration

185 of the 201 raw `rgba(R, G, B, A)` calls migrated to
`rgba(var(--<color>-rgb), A)`. Remaining 20 raw calls are one-off
color variants with no token equivalent (gray 180/180/180,
light-amber 255/184/108, pink-anime 255/122/184 + 255/92/168,
bg-elev variant 10/13/12, alt-green 143/228/150, light-amber-2
255/212/122). Leaving raw documents the variant intent; future
tokens can absorb them as they get reused.

## Tests

Static guards on the new tokens + sample migration shapes.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = REPO / "app" / "web" / "static" / "app.css"
OPS_CSS = REPO / "app" / "web" / "static" / "ops.css"


def test_color_rgb_tokens_defined():
    """All 11 `--<color>-rgb` tokens must be defined in :root."""
    src = APP_CSS.read_text()
    root_end = src.index("\n}", src.index(":root"))
    root_block = src[:root_end]
    # v1.15.121: --brown-rgb renamed to --lemon-rgb (and value
    # changed from biscuit brown to lemon gold). The hex sibling
    # --brown likewise renamed to --lemon. Pre-fix the cookies
    # tone was muddy brown; the new lemon gold reads as yellow
    # while still being distinct from amber.
    expected = [
        "--green-rgb", "--amber-rgb", "--orange-rgb", "--red-rgb",
        "--cyan-rgb", "--blue-rgb", "--violet-rgb", "--magenta-rgb",
        "--lemon-rgb", "--white-rgb", "--black-rgb",
    ]
    missing = [t for t in expected if t not in root_block]
    assert not missing, f"v1.15.113: missing tokens in :root: {missing}"


def test_color_rgb_values_match_hex_tokens():
    """Each --<color>-rgb must encode the same RGB components as
    its --<color> hex token. Drift would silently break the
    composition pattern."""
    src = APP_CSS.read_text()
    # v1.15.121: brown → lemon (cookies tone shift; see app.css
    # :root --lemon definition for full color history).
    pairs = {
        "accent":  ("#6dffb5", "109, 255, 181"),  # v0.51.108: green now aliases accent
        "amber":   ("#ffb84a", "255, 184, 74"),
        "orange":  ("#ff7a3a", "255, 122, 58"),
        "red":     ("#ff6b6b", "255, 107, 107"),
        "cyan":    ("#6dd3ff", "109, 211, 255"),
        "blue":    ("#6d8fff", "109, 143, 255"),
        "violet":  ("#c46dff", "196, 109, 255"),
        "magenta": ("#ff7ad6", "255, 122, 214"),
        "lemon":   ("#f5dd2b", "245, 221, 43"),
    }
    for name, (hex_v, rgb_v) in pairs.items():
        assert f"--{name}: {hex_v}" in src, (
            f"Sanity: --{name} hex token must exist before "
            "the --rgb sibling is meaningful."
        )
        assert f"--{name}-rgb:" in src
        # The values line is `  --green-rgb:   109, 255, 181;` —
        # match the components allowing extra whitespace.
        m = re.search(rf"--{name}-rgb:\s*([\d,\s]+);", src)
        assert m, f"--{name}-rgb not parseable"
        normalized = re.sub(r"\s+", " ", m.group(1)).strip()
        assert normalized == rgb_v, (
            f"v1.15.113: --{name}-rgb has {normalized!r}, expected "
            f"{rgb_v!r} — drift from --{name} ({hex_v}) would "
            "break the composition pattern."
        )


def test_brand_palette_rgba_migrated_to_tokens():
    """No brand-palette RGB triplets remain in raw `rgba(R,G,B,A)`
    form. The 9 named brand colors (green/amber/orange/red/cyan/
    blue/violet/magenta/brown) plus white + black must all use
    `var(--<color>-rgb)` form after migration."""
    # v1.15.121: (192,133,82) brown → (245,221,43) lemon.
    brand_triplets = {
        (109, 255, 181), (255, 184, 74), (255, 122, 58),
        (255, 107, 107), (109, 211, 255), (109, 143, 255),
        (196, 109, 255), (255, 122, 214), (245, 221, 43),
        (255, 255, 255), (0, 0, 0),
    }
    for path in (APP_CSS, OPS_CSS):
        src = path.read_text()
        # Strip /* ... */ comments — the v1.15.113 :root comment
        # block intentionally references the pre-fix raw triplets
        # as examples.
        code = re.sub(r"/\*.*?\*/", "", src, flags=re.DOTALL)
        for m in re.finditer(
            r"rgba\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*,",
            code,
        ):
            r, g, b = int(m[1]), int(m[2]), int(m[3])
            assert (r, g, b) not in brand_triplets, (
                f"v1.15.113: raw rgba({r},{g},{b},...) in "
                f"{path.name} — must use rgba(var(--<color>-rgb), ...)"
            )


def test_token_form_widely_used():
    """Sanity: the `rgba(var(--*-rgb), ...)` shape must appear in
    both app.css and ops.css. Catches a regression where someone
    reverts the migration script's edits."""
    for path in (APP_CSS, OPS_CSS):
        src = path.read_text()
        n = len(re.findall(r"rgba\(var\(--\w+-rgb\)", src))
        assert n >= 30, (
            f"v1.15.113: only {n} token-form rgba calls in "
            f"{path.name} — migration may have been partially "
            "reverted (expected >= 30)."
        )
