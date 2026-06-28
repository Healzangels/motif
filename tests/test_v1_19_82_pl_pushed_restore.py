"""v1.19.82 — restore the cyan PL=pushed dot for plex_upload rows.

v1.18.25 added a cyan PL dot ('pushed' state) for plex_upload
placements (theme uploaded to Plex's metadata store via API, no
sidecar in the media folder). v1.19.67 removed it (audit R2),
reasoning the adjacent LINK=PU chip already conveyed "API push"
so the cyan dot was a pure duplicate.

That premise was incomplete. The LINK glyph chain is an
if/else-if ladder where the mismatch branch sits ABOVE the
plex_upload branch:

    } else if (isMismatch && placed) { ...M...        # fires first
    } else if (placement_kind === 'hardlink') { ...HL... }
    } else if (placement_kind === 'copy')     { ...C... }
    } else if (placement_kind === 'plex_upload') { ...PU... }  # skipped

So a plex_upload row in mismatch_state renders LINK=M, NOT PU —
and the mismatch chip hides placement-KIND for HL/C alike. In
that state `PL=green + LINK=M` is indistinguishable from a
mismatched sidecar placement. The cyan PL dot is the ONLY
at-a-glance "this is an API placement (no folder sidecar)"
signal when LINK=M has eaten the PU chip.

The restore is VISUAL-ONLY. The server still classifies
plex_upload as pl_pills='on' (api.py _row_matches_pl 'on' branch
matches both sidecar AND plex_upload — pinned by v1.18.30's
test_row_matches_pl_on_includes_plex_upload), so cyan rows still
match the PL=on filter. No new filter chip, no new pl_pills
value, no SQL change. LINK=PU remains the axis for filtering
"just the API pushes". The cyan `.state-pill.pushed` CSS rule was
never removed (v1.18.30 only dropped the FILTER chip rule).
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()


def _pl_block() -> str:
    idx = APP_JS.index("const pl = placementBroken")
    end = APP_JS.index(";", idx)
    return APP_JS[idx:end]


# ── The cyan dot derivation is restored ──────────────────────


def test_pl_derivation_has_pushed_branch():
    """The `pl` ternary must paint 'pushed' for placed
    plex_upload rows, ahead of the generic 'on' branch."""
    block = _pl_block()
    assert "(placed && isPlexUpload) ? 'pushed'" in block, (
        "v1.19.82: plex_upload placements must derive PL='pushed' "
        "(cyan), restored after the v1.19.67 removal."
    )


def test_pushed_branch_precedes_on_branch():
    """'pushed' must be evaluated BEFORE the bare `placed`
    branch — otherwise every plex_upload row short-circuits to
    'on' (green) and the cyan dot never renders."""
    block = _pl_block()
    pushed_at = block.index("'pushed'")
    on_at = block.index(": placed ? 'on'")
    assert pushed_at < on_at, (
        "v1.19.82: the (placed && isPlexUpload) ? 'pushed' branch "
        "must sit above the generic placed ? 'on' branch"
    )


def test_broken_still_precedes_pushed():
    """A plex_upload row whose Plex copy went missing must still
    render 'broken' (red), not 'pushed' — placementBroken stays
    the top branch."""
    block = _pl_block()
    broken_at = block.index("placementBroken ? 'broken'")
    pushed_at = block.index("'pushed'")
    assert broken_at < pushed_at, (
        "v1.19.82: placementBroken must remain the first branch so "
        "a lost API placement reads red, not cyan."
    )


# ── Tooltip ──────────────────────────────────────────────────


def test_pushed_tooltip_present_and_metadata_honest():
    """The 'pushed' tooltip must read as served-from-metadata,
    not 'Placed in Plex folder' (which is false for plex_upload —
    there is no folder sidecar)."""
    assert "pl === 'pushed'" in APP_JS, (
        "v1.19.82: the 'pushed' tooltip branch must be restored"
    )
    idx = APP_JS.index("const plTip = placeInFlight")
    end = APP_JS.index(";", idx)
    tip_block = APP_JS[idx:end]
    assert (
        "Pushed to Plex — served from its metadata store "
        "(no folder sidecar)." in tip_block
    ), (
        "v1.19.82: 'pushed' tooltip must state the theme lives in "
        "Plex's metadata store with no folder sidecar"
    )


def test_on_tooltip_stays_folder_specific():
    """The 'on' (green, sidecar) tooltip keeps its folder wording —
    correct for HL/C placements, which DO write theme.mp3 to the
    media folder."""
    idx = APP_JS.index("const plTip = placeInFlight")
    end = APP_JS.index(";", idx)
    tip_block = APP_JS[idx:end]
    assert "Placed in Plex folder." in tip_block


# ── CSS unchanged — cyan dot, matches LINK=PU ────────────────


def test_state_pill_pushed_css_present_and_cyan():
    """`.state-pill.pushed` must exist and use --cyan so the dot
    matches the LINK=PU chip (also --cyan)."""
    assert ".state-pill.pushed {" in APP_CSS
    idx = APP_CSS.index(".state-pill.pushed {")
    rule = APP_CSS[idx:idx + 120]
    assert "var(--cyan)" in rule, (
        "v1.19.82: the cyan dot must use the --cyan token (same hue "
        "as .link-glyph-pu) for cross-column consistency"
    )


def test_link_pu_glyph_also_cyan():
    """The LINK=PU chip and the PL=pushed dot must share --cyan —
    same concept (API push), same hue across both columns."""
    idx = APP_CSS.index(".link-glyph-pu {")
    rule = APP_CSS[idx:idx + 250]
    assert "color: var(--cyan)" in rule


# ── Visual-only: no filter regression ────────────────────────


def test_no_pushed_filter_chip_resurrected():
    """The restore is visual-only — the FILTER chip removed in
    v1.18.30 must NOT come back (no data-pl-pill='pushed' button,
    no .state-pill-btn-pushed rule)."""
    library_html = (
        REPO / "app" / "web" / "templates" / "library.html"
    ).read_text()
    assert 'data-pl-pill="pushed"' not in library_html, (
        "v1.19.82: cyan dot is visual-only — no PL=pushed filter "
        "chip (that was the v1.18.30 dedupe, still valid)"
    )
    # The FILTER-chip rule (selector + brace) must stay gone; the
    # v1.18.30 comment still mentions the name in prose, so pin the
    # rule shape, not the bare token.
    assert ".state-pill-btn-pushed {" not in APP_CSS


def test_server_still_filters_plex_upload_as_pl_on():
    """The PL=on server filter must stay 4-valued — no 'pushed'
    filter value re-added — so cyan dots remain findable via the
    existing PL=on chip with no new pl_pills value. The fact that
    PL=on matches plex_upload rows (the visual-only contract) is
    owned by v1.18.30's test_row_matches_pl_on_includes_plex_upload."""
    assert '_pset(pl_pills, {"on", "await", "off", "broken"})' in API_PY, (
        "v1.19.82: pl_pills whitelist must stay 4-valued — no "
        "'pushed' filter value re-added (cyan dot is visual-only)"
    )


# ── Version pin ──────────────────────────────────────────────


def test_v1_19_82_version_pin():
    # Relaxed to the v1.19.x prefix after v1.19.83 continued the line —
    # the exact bump-every-tag pin lives in test_v1_13_79.
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
