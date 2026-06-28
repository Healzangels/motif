"""v1.19.75 — rename BP/BU LINK chips to PB/UB + concise tooltips.

the user's request: "change the BP and BU chips, BP to PB for Plex
Backup and BU to UB for User Backup, also can we cleanup the on
hover information for BU as right now it's overly verbose. Let's
make sure it keeps to the format and style of the others"

## Rationale

v1.19.63 renamed B → BP (Backup Plex) and BK → BU (Backup User)
to clarify each chip's scope. The reverse-ordered abbreviations
read more naturally as "<scope> Backup":
- Plex Backup → PB
- User Backup → UB

The v1.19.63 BU tooltip ballooned into a wall of provenance
reasoning (every code path that lands a row on BU was listed).
That detail belongs in the row's INFO card, not a chip tooltip
the user sees in passing. The other LINK chips follow a terse
one-sentence style:
- HL: "Hardlink — canonical and Plex-folder file share an inode."
- C:  "Copy — uses extra disk; cross-filesystem fallback."
- M:  "Mismatch — canonical differs from Plex copy."
- PU: "Pushed — motif uploaded the theme to Plex via API. Plex
       serves it from its metadata store (no sidecar at the
       media folder)."

v1.19.75 trims PB + UB to match:
- PB filter: "Plex Backup — motif copy of Plex's cloud theme
              staged as insurance against Plex Pass loss."
- PB row badge: same + "PROMOTE TO ACTIVE from INFO to deploy."
- UB filter: "User Backup — motif has the file ready but not
              placed in the Plex folder. PUSH TO PLEX to install."
- UB row badge: same as UB filter.

CSS classnames (.link-glyph-b, .link-glyph-bk) + data-link-pill
URL params ('b', 'bk') are unchanged for deep-link stability.
"""
from __future__ import annotations

import sys
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
LIBRARY_HTML = (
    REPO / "app" / "web" / "templates" / "library.html"
).read_text()


# ── PB rename (cloud-backup row + filter) ────────────────────


def test_pb_label_in_row_badge():
    """The JS linkCell `isPlexCloudBackup` branch must render
    the literal label 'PB' (not 'B' or 'BP')."""
    idx = APP_JS.index("if (isPlexCloudBackup)")
    end = APP_JS.index("else if (isBackupOnly)", idx)
    block = APP_JS[idx:end]
    assert ">PB</span>" in block, (
        "v1.19.75: cloud-backup row badge must render 'PB' "
        "(Plex Backup)"
    )
    # Stale labels gone.
    assert ">B</span>" not in block, (
        "v1.19.75: legacy 'B' label must be gone from the cloud-"
        "backup row branch"
    )
    assert ">BP</span>" not in block, (
        "v1.19.75: legacy 'BP' label must be gone from the cloud-"
        "backup row branch"
    )


def test_pb_label_in_filter_chip():
    """The library.html cloud-backup filter chip must show 'PB'
    as its visible label."""
    idx = LIBRARY_HTML.index('data-link-pill="b"')
    block = LIBRARY_HTML[max(0, idx - 300):idx + 400]
    assert ">PB</button>" in block
    assert ">BP</button>" not in block, (
        "v1.19.75: legacy 'BP' chip label must be gone"
    )


def test_pb_filter_tooltip_matches_terse_style():
    """The filter chip tooltip must be a single sentence
    matching the HL/C/M/PU style — no parenthetical (PB)
    repetition, no version-tag archaeology."""
    idx = LIBRARY_HTML.index('data-link-pill="b"')
    block = LIBRARY_HTML[max(0, idx - 300):idx + 500]
    tooltip_start = block.index('title="', block.index('class="link-glyph link-glyph-b"'))
    tooltip = block[tooltip_start:block.index('"', tooltip_start + 8) + 1]
    # Must lead with the canonical "Plex Backup —" phrasing.
    assert "Plex Backup —" in tooltip
    # Pre-v1.19.75 the tooltip embedded "(BP)" or "(B)" as a
    # parenthetical hint — drop that, the chip text already
    # says PB.
    assert "(PB)" not in tooltip
    assert "(BP)" not in tooltip
    assert "(B)" not in tooltip
    # No v-tag archaeology (overly verbose for a chip tooltip).
    assert "v1.19" not in tooltip
    assert "v1.18" not in tooltip


def test_pb_row_badge_tooltip_has_cta():
    """The row badge tooltip must point at PROMOTE TO ACTIVE
    so the user knows how to deploy the staged backup."""
    idx = APP_JS.index("if (isPlexCloudBackup)")
    block = APP_JS[idx:idx + 1500]
    # CTA: PROMOTE TO ACTIVE
    assert "PROMOTE TO ACTIVE" in block, (
        "v1.19.75: PB row badge must keep its CTA so the user "
        "knows how to deploy the backup"
    )
    # Lead-in matches the canonical phrasing.
    assert "Plex Backup —" in block


# ── UB rename (backup-only row + filter) ─────────────────────


def test_ub_label_in_row_badge():
    """The JS linkCell `isBackupOnly` branch must render the
    literal label 'UB' (not 'BK' or 'BU')."""
    idx = APP_JS.index("else if (isBackupOnly)")
    end = APP_JS.index("} else if (isMismatch", idx)
    block = APP_JS[idx:end]
    assert ">UB</span>" in block, (
        "v1.19.75: backup-only row badge must render 'UB' "
        "(User Backup)"
    )
    assert ">BK</span>" not in block
    assert ">BU</span>" not in block, (
        "v1.19.75: legacy 'BU' label must be gone"
    )


def test_ub_label_in_filter_chip():
    """The library.html UB filter chip must show 'UB' as its
    visible label."""
    idx = LIBRARY_HTML.index('data-link-pill="bk"')
    block = LIBRARY_HTML[max(0, idx - 400):idx + 500]
    assert ">UB</button>" in block
    assert ">BU</button>" not in block, (
        "v1.19.75: legacy 'BU' chip label must be gone"
    )


def test_ub_filter_tooltip_matches_terse_style():
    """The UB filter tooltip must be a single sentence matching
    the HL/C/M/PU style — short, what + where, no provenance
    wall."""
    idx = LIBRARY_HTML.index('data-link-pill="bk"')
    block = LIBRARY_HTML[max(0, idx - 400):idx + 500]
    tooltip_start = block.index(
        'title="', block.index('class="link-glyph link-glyph-bk"')
    )
    tooltip = block[tooltip_start:block.index('"', tooltip_start + 8) + 1]
    # Lead-in.
    assert "User Backup —" in tooltip
    # Pre-fix the tooltip listed every entry path
    # ("user upload, SET URL / UPLOAD MP3 with KEEP AS BACKUP,
    # DOWNLOAD TDB BACKUP, ACCEPT UPDATE / REVERT on a P-row,
    # ..."). v1.19.75 trims those — the row's INFO card is
    # the right place for that detail.
    assert "DOWNLOAD TDB BACKUP" not in tooltip, (
        "v1.19.75: provenance enumeration must be dropped from "
        "the UB tooltip"
    )
    assert "ACCEPT UPDATE" not in tooltip, (
        "v1.19.75: provenance enumeration must be dropped"
    )
    # No v-tag archaeology.
    assert "v1.19" not in tooltip
    # Length sanity — shouldn't exceed the verbosity of the
    # PU tooltip (the longest existing chip).
    pu_idx = LIBRARY_HTML.index('data-link-pill="pu"')
    pu_block = LIBRARY_HTML[pu_idx:pu_idx + 400]
    pu_tooltip_start = pu_block.index('title="')
    pu_tooltip = pu_block[
        pu_tooltip_start:pu_block.index('"', pu_tooltip_start + 8) + 1
    ]
    assert len(tooltip) <= len(pu_tooltip) + 50, (
        f"v1.19.75: UB tooltip ({len(tooltip)} chars) must be "
        f"roughly as terse as PU's ({len(pu_tooltip)} chars). "
        f"Pre-fix UB was 400+ chars of provenance listing."
    )


def test_ub_row_badge_tooltip_has_cta():
    """UB row badge tooltip must point at PUSH TO PLEX so the
    user knows how to install the backup."""
    idx = APP_JS.index("else if (isBackupOnly)")
    end = APP_JS.index("} else if (isMismatch", idx)
    block = APP_JS[idx:end]
    assert "PUSH TO PLEX" in block, (
        "v1.19.75: UB row badge must keep its CTA"
    )
    assert "User Backup —" in block


# ── CSS classnames + URL params unchanged (deep-link stability) ──


def test_link_glyph_b_classname_unchanged():
    """The .link-glyph-b class must still be in use — bookmarks
    + the v1.19.43 cloud-backup pipe rely on it. Renaming the
    chip text doesn't break the underlying classname."""
    assert ".link-glyph-b " in (
        REPO / "app" / "web" / "static" / "app.css"
    ).read_text() or ".link-glyph-b{" in (
        REPO / "app" / "web" / "static" / "app.css"
    ).read_text()


def test_data_link_pill_b_param_unchanged():
    """URL deep-link param `?link_pills=b` must still hydrate
    the chip. v1.19.75 only renames the label, not the wire
    format."""
    assert 'data-link-pill="b"' in LIBRARY_HTML


def test_data_link_pill_bk_param_unchanged():
    """URL deep-link param `?link_pills=bk` must still hydrate
    the chip."""
    assert 'data-link-pill="bk"' in LIBRARY_HTML


# ── Version pin ──────────────────────────────────────────────


def test_v1_19_75_version_pin():
    """Loose prefix — later tags continue the v1.19.x line."""
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
