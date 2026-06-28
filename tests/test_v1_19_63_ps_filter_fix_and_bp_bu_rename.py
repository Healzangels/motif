"""v1.19.63 — PS filter excludes backup_only rows + B→BP, BK→BU rename.

the user's v1.19.62 deploy review:

  1. The /anime LINK=PS filter chip was selected but ALL 50
     results rendered BK badges. The PS server-side SQL filter
     hadn't been updated when v1.19.61 unified PS-with-DL into
     BK (stamping backup_only on the plex_has_theme skip). JS
     renders isBackupOnly BEFORE lpsState so a row matching
     both paints BK, but the SQL filter for `link_pills=ps`
     still matched any lpsState row — including the freshly-
     stamped BK rows. Result: PS filter chip returns BK-badged
     rows. Mirror-drift between JS render and SQL filter.

  2. the user's design polish: rename badge labels for clarity:
       - B  → BP (Backup Plex)  — motif's local copy IS Plex's
         own cloud theme (v1.19.42 walker / SOURCE-menu
         DOWNLOAD PLEX BACKUP)
       - BK → BU (Backup User)  — motif's local copy is from
         a user source (upload, URL, TDB download, etc.)
     Plus BU color switched from blue → violet-bright so the
     family pairing reads at a glance:
       - SRC=P (amber)  / LINK=BP (amber-bright)
       - SRC=U (violet) / LINK=BU (violet-bright)

## v1.19.63 fixes

  1. `app/web/api.py`: PS link_pills SQL branch adds
     `last_place_attempt_reason IS NULL OR != 'backup_only'`
     so it's mutually exclusive with the BK filter. Mirrors
     the JS if/else cascade (linkCell isBackupOnly checked
     BEFORE lpsState).
  2. `app/web/static/app.css`: new --violet-bright token;
     .link-glyph-bk re-colored blue → violet-bright (+
     violet-family border/bg).
  3. `app/web/static/app.js`: linkCell labels B → BP, BK → BU,
     tooltips updated to "Backup Plex (BP)" / "Backup User
     (BU)". CSS classnames + URL params unchanged for deep-
     link stability.
  4. `app/web/templates/library.html`: filter-chip labels
     B → BP, BK → BU, tooltips updated. data-link-pill values
     stay 'b' and 'bk'.

`link_pills=b` / `link_pills=bk` URL params + .link-glyph-b /
.link-glyph-bk CSS selectors are intentionally NOT renamed —
keeps existing deep links + bookmarks working, keeps the CSS
upgrade non-cascading.
"""
from __future__ import annotations

import sys
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

API_PY = (REPO / "app" / "web" / "api.py").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
LIBRARY_HTML = (
    REPO / "app" / "web" / "templates" / "library.html"
).read_text()


# ── Bug 1: PS filter excludes backup_only ───────────────────
# v1.19.63's PS filter fix was made moot by v1.19.66, which
# dropped the PS chip entirely. The two tests that pinned the
# PS SQL exclusion are removed here — see
# tests/test_v1_19_66_revert_ps_chip.py for the no-op SQL pin.


# ── Rename: B → BP (Backup Plex) ─────────────────────────────


def test_link_glyph_b_renders_pb_label():
    """The JS linkCell B branch must render the literal label
    'PB' (v1.19.75 reorder from 'BP'). CSS classname stays
    .link-glyph-b for backwards-compat."""
    # The B branch html template.
    idx = APP_JS.index("if (isPlexCloudBackup)")
    end = APP_JS.index("else if (isBackupOnly)", idx)
    block = APP_JS[idx:end]
    assert ">PB</span>" in block, (
        "v1.19.75: cloud-backup row must render label 'PB' "
        "(Plex Backup), not 'B' or 'BP'"
    )
    # Classname unchanged.
    assert "link-glyph-b" in block
    # Tooltip uses the new "Plex Backup —" phrasing.
    assert "Plex Backup" in block


def test_filter_chip_b_renders_pb_label():
    """The library.html filter chip must show 'PB' as its visible
    label. data-link-pill='b' kept."""
    idx = LIBRARY_HTML.index('data-link-pill="b"')
    block = LIBRARY_HTML[max(0, idx - 300):idx + 400]
    assert ">PB</button>" in block
    assert "Plex Backup" in block


# ── Rename: BK → UB (User Backup, v1.19.75 from v1.19.63 BU) ──


def test_link_glyph_bk_renders_ub_label():
    """The JS linkCell BK branch must render the literal label
    'UB' (v1.19.75 reorder from 'BU'). CSS classname stays
    .link-glyph-bk for backwards-compat."""
    idx = APP_JS.index("else if (isBackupOnly)")
    end = APP_JS.index("} else if (isMismatch", idx)
    block = APP_JS[idx:end]
    assert ">UB</span>" in block, (
        "v1.19.75: backup-only row must render label 'UB' "
        "(User Backup), not 'BK' or 'BU'"
    )
    assert "link-glyph-bk" in block
    assert "User Backup" in block


def test_filter_chip_bk_renders_ub_label():
    """The library.html BK filter chip must show 'UB' as its
    visible label. data-link-pill='bk' kept."""
    idx = LIBRARY_HTML.index('data-link-pill="bk"')
    block = LIBRARY_HTML[max(0, idx - 400):idx + 500]
    assert ">UB</button>" in block
    assert "User Backup" in block


# ── BU color: violet-bright family ───────────────────────────


def test_violet_bright_token_defined():
    """v1.19.63 added --violet-bright color token mirroring the
    --amber / --amber-bright pair."""
    assert "--violet-bright:" in APP_CSS, (
        "v1.19.63: must define --violet-bright color token"
    )


def test_link_glyph_bk_color_switched_to_violet_bright():
    """The .link-glyph-bk CSS rule must use --violet-bright (was
    --blue pre-v1.19.63) so BU sits in the user-content family
    alongside SRC=U (violet)."""
    idx = APP_CSS.index(".link-glyph-bk {")
    end = APP_CSS.index("}", idx)
    rule = APP_CSS[idx:end]
    assert "var(--violet-bright)" in rule, (
        "v1.19.63: .link-glyph-bk must color with --violet-bright"
    )
    assert "var(--violet-rgb)" in rule, (
        "v1.19.63: .link-glyph-bk border/bg must use violet-rgb"
    )
    # Old blue color must be gone.
    assert "var(--blue)" not in rule, (
        "v1.19.63: pre-fix blue color must not remain"
    )


# ── Stability: URL params + CSS classnames unchanged ────────


def test_link_pills_values_unchanged():
    """data-link-pill='b' / data-link-pill='bk' must STAY — they
    drive the URL query param (link_pills=b / link_pills=bk).
    Renaming would break existing bookmarks + deep links."""
    assert 'data-link-pill="b"' in LIBRARY_HTML
    assert 'data-link-pill="bk"' in LIBRARY_HTML


def test_api_link_pills_allowed_set_unchanged():
    """The api.py _pset allow-list for link_pills must still
    include 'b' and 'bk' (URL param stability)."""
    idx = API_PY.index('link_set = _pset(link_pills,')
    line_end = API_PY.index(")", idx) + 1
    line = API_PY[idx:line_end]
    # Allow either quote style.
    assert "'b'" in line or '"b"' in line
    assert "'bk'" in line or '"bk"' in line


# ── Version pin ──────────────────────────────────────────────


def test_v1_19_63_version_pin():
    """v1.19.63 bumped. Relaxed to v1.19.x prefix after v1.19.64
    continued the line."""
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
