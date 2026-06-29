"""v0.50.55 — the legend + glossary decode every chip the row actually renders.

Audit (the user: "make sure we've got everything included") found gaps where a
row-visible chip had no decode in either surface:
  - FLAGS / ATTN axis: the row has a 7-chip attention axis (⚠ ! !M !P ↺ ↩ ⟳) but
    both surfaces decoded only 4 — mismatch (!M), awaiting-placement (!P) and the
    restorable-snapshot (↩) had no decode anywhere; ⊘ too-big was title-only.
  - SRC: the "Plex also serves" composite corner-dot (link-badge-also-plex) was
    undecoded.
  - LINK (in-context legend only): C (copy), M (mismatch), AB (adopted backup)
    were missing though the legend claims "every chip on THIS tab".

This pins those decodes present + ties the glossary FLAGS section to the real
ATTN: filter row, so a future attention chip can't ship without a decode.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
BASE_HTML = (REPO / "app" / "web" / "templates" / "library.html").read_text()  # legend
GLOSS_HTML = (REPO / "app" / "web" / "templates" / "base.html").read_text()    # glossary


def _attn_filter_states() -> set[str]:
    """The attention states the ATTN: filter row actually exposes (source of truth).
    'cookies' is honored as a no-op for bookmark compat (moved to the TDB axis), so
    it carries no data-attn-pill button — naturally excluded."""
    return set(re.findall(r'data-attn-pill="([a-z]+)"', BASE_HTML))


def test_glossary_decodes_every_attn_filter_state():
    states = _attn_filter_states()
    assert {"fail", "update", "mismatch", "await", "broken", "restore", "repush"} <= states
    for st in states:
        assert f"gg-{st}" in GLOSS_HTML, f"// GLOSSARY FLAGS missing a decode for ATTN '{st}' (gg-{st})"


def test_glossary_decodes_the_title_only_toobig_flag():
    # ⊘ too-big has no ATTN filter chip (title glyph only) but is still row-visible.
    assert "gg-toobig" in GLOSS_HTML


def test_glossary_decodes_the_plex_also_serves_dot():
    assert "link-badge link-badge-themerrdb link-badge-also-plex" in GLOSS_HTML


def test_legend_adds_common_link_and_attn_chips():
    # lean set the user chose: LINK C/M/AB + the !M/!P attention chips.
    for cls in ("link-glyph-copy", "link-glyph-mismatch", "link-glyph-ab",
                "gg-mismatch", "gg-await"):
        assert cls in BASE_HTML, f"in-context legend missing {cls}"


def test_new_attn_labels_match_the_filter_chip_labels():
    # the decode shows !M / !P (the ATTN chip labels), not a bare ! — so the three
    # !-family flags are distinguishable in the list.
    for html in (GLOSS_HTML, BASE_HTML):
        assert 'gg-mismatch">!M<' in html
        assert 'gg-await">!P<' in html
