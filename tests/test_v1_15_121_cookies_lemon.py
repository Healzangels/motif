"""v1.15.121 — cookies tone: biscuit brown → lemon gold.

the user on v1.15.116:

> Can we take a quick pass at the color of our filter buttons and
> related chips and pills. I don't like the brown tdb cookies pill
> or brown lock filter. I think yellow made more sense and looked
> better but its looks so similar to the plex color.

Color history of the cookies/auth tone:

  pre-v1.15.17 — amber (clustered with Plex)
  v1.15.17     — --yellow #ffe066 (still too close to amber)
  v1.15.43     — --brown #c08552 (the user: muddy, "don't like brown")
  v1.15.121    — --lemon #f5dd2b (current — yellow that's
                   distinct from amber's orange-warmth)

Why #f5dd2b works where #ffe066 didn't:

  --amber:  RGB(255, 184, 74)   R high, G mid, B mid (orange-warm)
  v1.15.17: RGB(255, 224, 102)  same R, G slightly higher, similar B
                                  → still reads as "warm yellow"
                                  → clusters perceptually with amber
  --lemon:  RGB(245, 221, 43)   R lower, G higher, B lower
                                  → reads as "yellow not amber"
                                  → does not cluster with amber

The token was renamed from --brown to --lemon so future
maintainers don't read the old name and get confused by the new
value. The four cookies surfaces (.btn-cookies, .tdb-pill-cookies,
.attn-pill-cookies, --tone-cookies in ops.css) all use --lemon.
"""

from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_CSS = REPO / "app" / "web" / "static" / "app.css"
OPS_CSS = REPO / "app" / "web" / "static" / "ops.css"


def test_lemon_token_replaces_brown_in_root():
    src = APP_CSS.read_text()
    assert "--lemon: #f5dd2b;" in src
    assert "--lemon-bright: #f8e664;" in src
    assert "--lemon-rgb:" in src
    # The old token must not coexist with the new — leaving it
    # invites future edits to reach for the wrong token.
    # Strip comments (history references --brown by name).
    import re
    root_anchor = src.index(":root {")
    # Find the first closing `}` at column 0 (end of :root block).
    root_end = src.index("\n}", root_anchor)
    root_block = src[root_anchor:root_end]
    code = re.sub(r"/\*.*?\*/", "", root_block, flags=re.DOTALL)
    assert "--brown:" not in code, (
        "v1.15.121: --brown token must be fully removed from :root, "
        "not left as a dead synonym. Rename consumers to --lemon."
    )
    assert "--brown-rgb:" not in code
    assert "--brown-bright:" not in code


def test_lemon_distinct_from_amber():
    """The RGB components must be visibly distinct from amber so
    cookies surfaces don't cluster with Plex amber surfaces."""
    src = APP_CSS.read_text()
    assert "--amber: #ffb84a" in src, "Sanity: --amber unchanged"
    assert "--lemon: #f5dd2b" in src
    # Distinct enough to read as different colors. The eye perceives
    # the cookies tone as "yellow" not "amber" when R drops below
    # 255 and G crosses ~200.
    import re
    m = re.search(r"--lemon-rgb:\s*(\d+),\s*(\d+),\s*(\d+)", src)
    assert m, "--lemon-rgb missing"
    r, g, b = int(m[1]), int(m[2]), int(m[3])
    assert r < 250, (
        f"v1.15.121: --lemon R should be <250 to drop the orange "
        "tint that made v1.15.17's #ffe066 too close to amber. "
        f"Got R={r}."
    )
    assert g > 210, (
        f"v1.15.121: --lemon G should be >210 to push the perception "
        "from amber/orange to yellow. Got G={g}."
    )
    assert b < 60, (
        f"v1.15.121: --lemon B should be <60 to keep the warmth "
        "without drifting into amber's orange-warmth. Got B={b}."
    )


def test_no_brown_var_references_in_code():
    """No `var(--brown)` / `var(--brown-rgb)` / `var(--brown-
    bright)` references remain in CSS — every cookies surface was
    migrated to --lemon."""
    import re
    for path in (APP_CSS, OPS_CSS):
        src = path.read_text()
        # Strip comments — history references --brown by name.
        code = re.sub(r"/\*.*?\*/", "", src, flags=re.DOTALL)
        for tok in ("var(--brown)", "var(--brown-rgb)",
                    "var(--brown-bright)"):
            assert tok not in code, (
                f"v1.15.121: stale {tok} reference in {path.name}. "
                "Migrate to --lemon."
            )


def test_cookies_family_surfaces_use_lemon():
    """The three live canonical cookies surfaces all reference --lemon
    (row pill, action button, semantic alias). v0.51.79 (CSS audit): the
    .attn-pill-cookies filter chip was dropped from this list — the STATUS
    pill it styled was removed v1.19.67 (dead CSS, deleted v0.51.79)."""
    css = APP_CSS.read_text()
    ops = OPS_CSS.read_text()

    for selector in (".btn-cookies {", ".tdb-pill-cookies {"):
        idx = css.index(selector)
        block = css[idx:idx + 300]
        assert "var(--lemon)" in block, (
            f"v1.15.121: {selector.rstrip(' {')} must use --lemon"
        )
    assert "--tone-cookies: var(--lemon);" in ops, (
        "v1.15.121: ops.css --tone-cookies alias must use --lemon"
    )
