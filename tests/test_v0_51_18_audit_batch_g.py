"""v0.51.18 — round-4 audit Batch G: recent-delta CSS + test hygiene.

Findings fixed here (all CONFIRMED by the adversarial verify pass):

  #22 (app.css) — the v0.50.88/91 running-job topbar shrink protections
    (.topbar-status min-width:0 + flex-wrap; the #op-mini full-width
    strip) were scoped to the ≤600px block only. At 601-780px
    (half-snapped 1440px desktop = 720px, iPad portrait 768px) the base
    auto/1fr/auto topbar grid still sized the status column at
    max-content, so any running job pushed the ?/logout controls past
    the right edge and grew a page horizontal scrollbar for the job's
    duration. A new disjoint 601-780px block applies the same two
    protections without the ≤600px block's two-row re-layout.

  #33 (app.css) — the v0.51.10 shutter reveal-edge box-shadows bled
    back into the viewport at a plain translateY(±100%): the glow
    (5px offset + 22px blur ≈ 27px) projected ~27px past each parked
    shutter, leaving a static green band at both screen poles at full
    opacity for ~110ms before the veil's 62ms fade. The shutters now
    park PAST the poles (calc(±100% ± 30px)) so the glow exits with
    them along the same ease. The user explicitly opted to fix this.

  #32 (tests/test_v0_51_6_mobile_tap_targets.py) — the desktop-density
    guard used bare substring asserts that matched four unrelated
    rules ("height: 22px;" on .state-pill-btn/.ed-pill-btn/.link-glyph;
    "height: 20px;" substring-matching .pill-filter-spacer's
    min-height), so dropping either pinned density shipped green — the
    v1.18.81 phantom-guard class. Now scoped rule-body extraction.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
TAP_TEST = (REPO / "tests" / "test_v0_51_6_mobile_tap_targets.py").read_text()


def _block(css: str, start_marker: str) -> str:
    i = css.index(start_marker)
    depth = 0
    j = i
    while True:
        if css[j] == "{":
            depth += 1
        elif css[j] == "}":
            depth -= 1
            if depth == 0:
                return css[i:j + 1]
        j += 1


# ── #22: tablet-range topbar shrink protections ───────────────


TABLET = _block(CSS, "@media (min-width: 601px) and (max-width: 780px) {")


def test_tablet_block_exists_and_is_disjoint():
    # disjoint from the phone block — the ≤600px two-row topbar
    # re-layout must NOT extend to tablets.
    assert "grid-template-areas" not in TABLET
    assert "grid-area" not in TABLET


def test_tablet_status_column_can_shrink():
    assert (".topbar-status { min-width: 0; flex-wrap: wrap; "
            "justify-content: flex-end; }") in TABLET, (
        "v0.51.18 #22: without a shrink floor the auto status column "
        "reports max-content and overflows the viewport under a "
        "running job — the v0.50.88 mechanism, one breakpoint up")


def test_tablet_op_mini_is_full_width_strip():
    assert ".topbar:has(#op-mini:not([hidden])) { padding-bottom: 30px; }" in TABLET
    assert "#op-mini:not([hidden])" in TABLET
    mini = TABLET[TABLET.index("#op-mini:not([hidden]) {"):]
    mini = mini[:mini.index("}")]
    assert "position: absolute;" in mini
    assert "left: 0; right: 0; bottom: 0;" in mini


def test_phone_block_protections_still_in_place():
    """The ≤600px twins must survive — the tablet block is an addition,
    not a move (test_v0_50_88/91 pin them in place)."""
    phone = _block(CSS, "@media (max-width: 600px) {")
    assert (".topbar-status { grid-area: status; min-width: 0; "
            "flex-wrap: wrap; justify-content: flex-end; }") in phone
    assert "#op-mini .op-mini-bar { width: 90px; flex: 0 0 auto; }" in phone


# ── #33: superseded by v0.51.20 (shutters removed entirely) ───


def test_shutter_glow_finding_now_moot():
    """v0.51.18 #33 parked the shutters past the poles so their reveal-edge
    glow exited the viewport. v0.51.20 removed the shutter mechanism outright
    (power-on now mirrors power-off — a veil + beam, no shutters), so the
    glow-band artifact is structurally impossible. Guard the removal so the
    shutters can't creep back."""
    assert "@keyframes crt-power-on-shutter-top" not in CSS
    assert "@keyframes crt-power-on-shutter-bot" not in CSS
    assert ".crt-power-on::before" not in CSS and ".crt-power-on::after" not in CSS
    # see test_v0_51_20_crt_power_on_mirror for the replacement effect's pins.


# ── #32: the tap-target desktop guard is scoped, not substring ──


def test_desktop_density_guard_extracts_rule_bodies():
    assert ".help-toggle {" in TAP_TEST, (
        "v0.51.18 #32: the 22px assert must extract the .help-toggle "
        "rule body (bare substring matched 4 unrelated rules)")
    assert ".pill-filter-row .pill-filter-clear," in TAP_TEST, (
        "the 20px assert must anchor on the v1.12.25 shared filter-pill "
        "density rule (bare substring matched .pill-filter-spacer's "
        "min-height)")
    # the phantom shape — a bare assert against the whole pre slice —
    # must be gone.
    assert 'assert "height: 22px;" in pre' not in TAP_TEST
    assert 'assert "height: 20px;" in pre' not in TAP_TEST
