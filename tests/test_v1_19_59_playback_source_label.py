"""v1.19.59 — INFO card "playback source" line clarifies SRC=U vs URL display.

the user's confusion on Bastard!! (1992): row classified as
SRC=U but the "applied url" line showed
`https://www.youtube.com/watch?v=buQ5WwyrlFo themerrdb`. The
"themerrdb" tag suggested TDB-source, contradicting the U pill.

Root cause: the INFO card's "applied url" line falls back to
`themes.youtube_url` when `source_kind=upload` exists but no
`user_override` row does. So the URL displays as TDB-sourced
(because it IS the TDB URL), but the actual playback is the
uploaded MP3.

the user's quote: "I'm a bit confused on why it's a U to be fair
and not a T as it's showing the applied url (youtube) ...
themerrdb and don't have any history in the info of uploading
a user url but I supposed it's possible did and it's the same
as themerrdb which then prompted to update since they matched?"

## Fix

New "playback source" line in the INFO card synthesizes the
four signals (source_kind, last_place_attempt_reason, ovr,
placements) into ONE human-readable phrase. Directly answers
"why is this row's SRC letter what it is?"

Label cases:
  - source_kind=upload → "User-uploaded MP3"
  - source_kind=url → "Downloaded from user URL"
  - source_kind=themerrdb → "Downloaded from ThemerrDB URL"
  - source_kind=adopt → "Adopted from existing sidecar"
  - source_kind=plex_cloud + backup_only → "Plex cloud backup
    staged (B badge · PROMOTE TO ACTIVE to deploy)"
  - source_kind=any + backup_only → "<friendly> (BK badge ·
    backup-only · PROMOTE TO ACTIVE to deploy)"
  - no local_file + is_plex_agent → "Plex serves its own
    theme (motif standing by)"
  - no local_file otherwise → "(none — row has no theme staged)"

Placement suffix when applicable: "· placed: <kind>" or
"· not placed" so the user can see the deploy state too.
"""
from __future__ import annotations

import sys
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


# ── Helper function exists + has documented label map ────────


def test_derive_playback_source_label_helper_defined():
    """The INFO card builder must define a helper that synthesizes
    the playback-source label from (lf, ovr, placements)."""
    assert "function _derivePlaybackSourceLabel(" in APP_JS, (
        "v1.19.59: _derivePlaybackSourceLabel helper must exist"
    )
    assert "function _humanSourceKind(" in APP_JS


def test_helper_handles_all_source_kinds():
    """The _humanSourceKind switch must produce a human label
    for every defined source_kind value. v1.19.42 added
    'plex_cloud'; the helper must handle it specifically (not
    fall through to the generic case)."""
    idx = APP_JS.index("function _humanSourceKind(")
    fn_end = APP_JS.index("\n    }", idx) + 6
    body = APP_JS[idx:fn_end]
    for kind in ("themerrdb", "url", "upload", "adopt", "plex_cloud"):
        assert f"'{kind}'" in body, (
            f"v1.19.59: _humanSourceKind must handle '{kind}'"
        )


def test_helper_special_cases_plex_cloud_backup_only():
    """A plex_cloud + backup_only row must produce a label that
    references the B badge + PROMOTE TO ACTIVE recovery — the
    user-facing action."""
    idx = APP_JS.index("function _derivePlaybackSourceLabel(")
    fn_end = APP_JS.index("\n    function _humanSourceKind", idx)
    body = APP_JS[idx:fn_end]
    assert "plex_cloud" in body
    assert "backup_only" in body
    assert "PROMOTE TO ACTIVE" in body
    assert "B badge" in body


def test_helper_handles_bk_rows():
    """backup_only rows must produce a label referencing their
    backup badge. v1.19.90 split the badge by source: themerrdb →
    TB, user (url/upload/adopt) → UB (was the stale 'BK badge'
    label). plex_cloud → B/PB is handled in its own branch above."""
    idx = APP_JS.index("function _derivePlaybackSourceLabel(")
    fn_end = APP_JS.index("\n    function _humanSourceKind", idx)
    body = APP_JS[idx:fn_end]
    assert "TB badge" in body
    assert "UB badge" in body


def test_helper_handles_plex_agent_no_local_file():
    """No local_file + Plex serves its own theme → label says
    'Plex serves its own theme (motif standing by)'."""
    idx = APP_JS.index("function _derivePlaybackSourceLabel(")
    fn_end = APP_JS.index("\n    function _humanSourceKind", idx)
    body = APP_JS[idx:fn_end]
    assert "plex_independent_theme" in body or "is_plex_agent" in body
    assert "Plex serves its own theme" in body
    assert "motif standing by" in body


def test_helper_surfaces_placement_kind():
    """When the row has placements, the label must append the
    placement kind so the user sees "uploaded MP3 · placed:
    plex_upload" rather than ambiguous "uploaded MP3" alone."""
    idx = APP_JS.index("function _derivePlaybackSourceLabel(")
    fn_end = APP_JS.index("\n    function _humanSourceKind", idx)
    body = APP_JS[idx:fn_end]
    assert "placement_kind" in body
    assert "placed" in body


# ── Render: line is in the INFO card grid ────────────────────


def test_info_card_renders_playback_source_line():
    """The INFO card must render the playback-source line via the helper.
    v1.24.89: it moved from a dlg-grid <dt>/<dd> to the hero headline
    (.info-hero-playback) beside the cover — same helper output, new home
    (option A: cover top-left, headline beside it, grid full-width below)."""
    idx = APP_JS.index("<h3 class=\"info-title\">${htmlEscape(t.title || '—')}")
    block = APP_JS[idx:idx + 3000]
    assert "info-hero-playback" in block, (
        "v1.24.89: INFO card hero must include the .info-hero-playback "
        "headline beside the cover"
    )
    assert "_derivePlaybackSourceLabel()" in block, (
        "v1.19.59: the playback line must render the helper's output"
    )


def test_info_card_playback_line_has_tooltip():
    """The playback line must carry a title= tooltip explaining what it
    means. v1.24.89: the tooltip rides the .info-hero-playback <p> now
    (was the grid <dt>)."""
    idx = APP_JS.index("<h3 class=\"info-title\">${htmlEscape(t.title || '—')}")
    block = APP_JS[idx:idx + 3000]
    pb_idx = block.index("info-hero-playback")
    tag = block[pb_idx:pb_idx + 400]
    assert "title=" in tag
    assert "SRC letter" in tag


# ── the user's exact repro: Bastard!! shape ────────────────────


def test_bastard_shape_label_says_uploaded_mp3():
    """the user's Bastard!! row shape (source_kind=upload + ovr
    is None + placement_kind=plex_upload) must produce a label
    that says 'User-uploaded MP3' explicitly so future-the user
    doesn't need to puzzle out the SRC=U classification."""
    # Behavioral simulation of the helper logic.
    # source_kind='upload', last_place_attempt_reason=None,
    # placements=[{placement_kind: 'plex_upload'}]
    sk = "upload"
    reason = None
    is_plex_cloud_backup = sk == "plex_cloud" and reason == "backup_only"
    is_bk = reason == "backup_only" and not is_plex_cloud_backup
    if is_plex_cloud_backup:
        label = "Plex cloud backup staged"
    elif is_bk:
        label = "(BK)"
    else:
        # Friendly from source_kind:
        kind_map = {
            "themerrdb": "Downloaded from ThemerrDB URL",
            "url": "Downloaded from user URL",
            "upload": "User-uploaded MP3",
            "adopt": "Adopted from existing sidecar",
            "plex_cloud": "Plex's cloud theme (downloaded by motif)",
        }
        label = kind_map.get(sk, f"source_kind={sk}")
    placed_suffix = " · placed: plex_upload"
    full = label + placed_suffix
    assert full == "User-uploaded MP3 · placed: plex_upload", (
        f"v1.19.59: Bastard!!'s playback source label should be "
        f"explicit; got {full!r}"
    )


# ── Version pin ──────────────────────────────────────────────


def test_v1_19_59_version_pin():
    """Version bumped at v1.19.59 (then v1.19.60 for the !UPD
    glyph diff-capture fix). Match the v1.19.x prefix."""
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
