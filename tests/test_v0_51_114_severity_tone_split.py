"""v0.51.114 — the "Split" theming decision + a fixed severity tone.

The user chose SPLIT: the row-table SRC/LINK pills, chips and status dots stay
FIXED (green T, amber P, …) so the dense data grid is scannable on every theme,
while the peripheral SOURCE chrome (source-menu action buttons, URL links,
success messages) FOLLOWS the theme accent — the glossary + library legend
anchor what each color means, and every one of those elements also carries a
text label, so the color is a secondary cue there.

The one carve-out is the SEVERITY scale (import CONFLICT + orphan-drift warn):
there the color is a SAFETY signal, not a source label. A CONFLICT import that
renders the same green as a CLEAN one could be applied over an existing theme
unnoticed, and green already means "safe" — so it must stay fixed amber.

  * .btn-tone-warn — NEW fixed-amber severity tone (the warn tier of the
    btn-tone-* family). import-preview `conflict` + the orphan-drift warn chips
    ride it; the ok tier rides the existing fixed-green .btn-tone-ok.
  * .btn-warn stays the THEMED action tone (SAVE / TEST / CLEAR → accent, from
    v0.51.111) — the split keeps the two apart.

Plus a preset hex==rgb drift guard: every preset hand-writes --bg/--line/
--accent as BOTH a hex and a decimal triplet; if they drift, rgba(var(--*-rgb))
glows/borders render a stale color that no other test would catch.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP = (REPO / "app" / "web" / "static" / "app.css").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
ORPHANS = (REPO / "app" / "web" / "templates" / "orphans.html").read_text()
BASE = (REPO / "app" / "web" / "templates" / "base.html").read_text()

THEMES_BLK = BASE[BASE.index("window.MOTIF_THEMES = {"):
                  BASE.index("};", BASE.index("window.MOTIF_THEMES = {")) + 2]
PRESETS = ("plex", "dracula", "nord", "gruvbox", "tokyonight", "synthwave", "mono")


def _rule(css: str, selector: str) -> str:
    i = css.index(selector)
    return css[i:css.index("}", i) + 1]


def _preset(name: str) -> str:
    i = THEMES_BLK.index(name + ": {")
    return THEMES_BLK[i:THEMES_BLK.index("}", i)]


# ── the fixed-amber severity tone ────────────────────────────


def test_btn_tone_warn_is_fixed_amber():
    rule = _rule(APP, ".btn-tone-warn {")
    assert "color: var(--amber);" in rule
    assert "border-color: var(--amber);" in rule
    # fixed — never the themeable accent/green alias.
    assert "--accent" not in rule and "--green" not in rule


def test_import_conflict_uses_fixed_amber_tone():
    # the import-preview status map: CONFLICT rides the fixed-amber severity
    # tone, not the themed .btn-warn action tone (or it reads like a CLEAN row).
    assert "conflict: 'btn-tone-warn'," in APP_JS
    assert "conflict: 'btn-warn'," not in APP_JS
    assert "clean: 'btn-tone-ok'," in APP_JS  # ok tier already fixed green


def test_orphan_drift_severity_scale_is_theme_independent():
    # the DRIFT_TONE map: ok=fixed green, warn tier=fixed amber, danger=red —
    # none of which alias the accent, so the scale never collapses on a theme.
    blk = ORPHANS[ORPHANS.index("const DRIFT_TONE = {"):]
    blk = blk[:blk.index("};") + 2]
    assert "'ok': 'btn-tone-ok'," in blk
    for warn in ("no_plex_entries", "motif_hash_unknown",
                 "motif_entry_missing", "orphan_sidecar_on_disk"):
        assert f"'{warn}': 'btn-tone-warn'," in blk, f"{warn} must be fixed amber"
    # no themed tone leaks into the severity map.
    assert "'btn-warn'" not in blk
    assert "lib-source-themerrdb" not in blk  # ok tier no longer themed green


def test_btn_warn_stays_the_themed_action_tone():
    # the SPLIT: .btn-warn keeps following the accent (SAVE / TEST / CLEAR);
    # only the SEMANTIC warn-severity uses moved off it onto .btn-tone-warn.
    assert ".btn-warn { color: var(--accent-bright); border-color: var(--accent); }" in APP


# ── preset hex == rgb drift guard ────────────────────────────


def _hex_to_rgb(h: str) -> tuple:
    h = h.lstrip("#")
    return tuple(int(h[i:i + 2], 16) for i in (0, 2, 4))


def test_preset_rgb_triplets_match_their_hex():
    # every preset writes --bg/--line/--accent as BOTH a hex and a decimal
    # triplet (the rgba(var(--*-rgb)) glow/border form). A drift renders a
    # stale color on solid chrome vs its glow — invisible to every other test.
    for name in PRESETS:
        blk = _preset(name)
        for tok in ("--bg", "--line", "--accent"):
            hx = re.search(rf"'{tok}':\s*'(#[0-9a-fA-F]{{6}})'", blk)
            rgb = re.search(rf"'{tok}-rgb':\s*'(\d+),\s*(\d+),\s*(\d+)'", blk)
            assert hx, f"{name} missing {tok} hex"
            assert rgb, f"{name} missing {tok}-rgb"
            want = _hex_to_rgb(hx.group(1))
            got = tuple(int(g) for g in rgb.groups())
            assert want == got, (
                f"{name} {tok}: hex {hx.group(1)} = {want} but "
                f"{tok}-rgb = {got} (drift — update the triplet)")
