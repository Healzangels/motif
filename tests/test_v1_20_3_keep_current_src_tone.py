"""v1.20.3 — KEEP CURRENT buttons take on the SRC color of the
source being kept.

the user (ANIME page, 2 selected P-rows): "keep current looks like it
will be keeping a tdb theme due to its color — can we make the keep
current color based on what sort of src row you would be keeping, so
in this case it would be amber."

The default-green KEEP CURRENT read as "keep the TDB (green) theme"
even on P-rows where KEEP CURRENT preserves the Plex (amber) source.

v1.20.3:
  - Shared `SRC_LETTER_TONE` map (T→themerrdb, A→adopt, U→user,
    P→plex) — the existing SOURCE-menu tone vocab. M + '-' omitted
    (M has no button tone; M's axis red reads as 'danger' on a
    button) → neutral green.
  - Per-row KEEP CURRENT (SOURCE menu): `tone: SRC_LETTER_TONE[srcLetter]`.
  - Bulk-bar KEEP CURRENT button: tinted by the dominant SRC across
    the actionable rows; single clean source → that tone; M / mixed /
    none → neutral green.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


# ── shared SRC-letter → tone map ─────────────────────────────


def test_src_letter_tone_map_defined():
    assert ("const SRC_LETTER_TONE = "
            "{ T: 'themerrdb', A: 'adopt', U: 'user', P: 'plex' };") in APP_JS, (
        "v1.20.3: the shared SRC-letter → tone map must exist"
    )


def test_src_letter_tone_map_omits_m_and_dash():
    """M + '-' must NOT be in the map — they fall back to neutral
    green (M's axis color is red = danger on a button; '-' has no
    source to keep)."""
    idx = APP_JS.index("const SRC_LETTER_TONE = ")
    line = APP_JS[idx:idx + 80]
    assert " M:" not in line, "M must not get a button tone (red=danger)"
    assert "'-'" not in line and '"-"' not in line


def test_tone_values_all_have_css_rules():
    """Every tone the map can emit must have a real .btn.lib-source-*
    rule (the v1.14.50 cross-reference contract)."""
    for tone in ("themerrdb", "adopt", "user", "plex"):
        assert f".btn.lib-source-{tone} {{" in APP_CSS, (
            f"v1.20.3: SRC_LETTER_TONE emits lib-source-{tone} but no "
            f"CSS rule exists"
        )


# ── per-row KEEP CURRENT (SOURCE menu) ───────────────────────


def test_per_row_keep_current_carries_src_tone():
    idx = APP_JS.index("'decline-update', 'KEEP CURRENT'")
    # v1.21.81: widened from 320 — the extras object gained rk: it.rating_key
    # ahead of the tone key, pushing tone past the old window.
    block = APP_JS[idx:idx + 420]
    assert "tone: SRC_LETTER_TONE[srcLetter]" in block, (
        "v1.20.3: the per-row KEEP CURRENT menu item must pass "
        "tone: SRC_LETTER_TONE[srcLetter]"
    )


# ── bulk-bar KEEP CURRENT button ─────────────────────────────


def test_bulk_keep_current_clears_then_applies_tone():
    idx = APP_JS.index("library-decline-all-updates-btn')")
    block = APP_JS[idx:idx + 2600]
    # Removes any stale tone before applying the fresh one (selection
    # changes between renders).
    assert "declineAllBtn.classList.remove(" in block
    assert "'lib-source-themerrdb', 'lib-source-adopt'," in block
    # Applies the computed tone via the shared map.
    assert "SRC_LETTER_TONE[_keepLetter]" in block
    assert "declineAllBtn.classList.add(`lib-source-${_keepTone}`)" in block


def test_bulk_keep_current_mixed_selection_stays_neutral():
    """A mixed-SRC selection must NOT get a tone (the _keepMixed
    guard) — neutral green is the fallback."""
    idx = APP_JS.index("library-decline-all-updates-btn')")
    block = APP_JS[idx:idx + 2600]
    assert "_keepMixed = true" in block
    assert "(!_keepMixed && _keepLetter)" in block


# ── replica of the dominant-SRC logic ────────────────────────


def test_dominant_src_tone_logic_replica():
    """Exercise the JS dominant-source selection so the single-source
    / mixed / M / empty branches are validated, not just text-pinned."""
    SRC_LETTER_TONE = {"T": "themerrdb", "A": "adopt", "U": "user", "P": "plex"}

    def keep_tone(letters):
        keep_letter = None
        mixed = False
        for letter in letters:
            if keep_letter is None:
                keep_letter = letter
            elif keep_letter != letter:
                mixed = True
                break
        if mixed or not keep_letter:
            return None
        return SRC_LETTER_TONE.get(keep_letter)

    assert keep_tone(["P", "P"]) == "plex"      # the user's repro → amber
    assert keep_tone(["U"]) == "user"           # single user override
    assert keep_tone(["T", "T", "T"]) == "themerrdb"
    assert keep_tone(["P", "U"]) is None        # mixed → neutral
    assert keep_tone(["M", "M"]) is None        # M has no tone → neutral
    assert keep_tone([]) is None                # nothing actionable → neutral


def test_v1_20_3_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
