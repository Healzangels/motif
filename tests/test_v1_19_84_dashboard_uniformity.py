"""v1.19.84 — dashboard design-uniformity audit follow-ups.

A holistic style review of the dashboard (follow-on to the v1.19.81
settings pass) surfaced five drifts. the user approved all five:

  D1 (HIGH, voice) — the `//` section-header prefix was on all 7
     big stat cards (COVERAGE + PLEX LIBRARY) but missing from all
     8 mini cards (OPERATIONS / ACTIVITY / STORAGE). Same
     `.stat-label` primitive, inconsistent motif voice. Added `//`
     to every mini-card label so all 15 cards read uniformly.

  D2 (MED, header) — // STORAGE WASTE jammed a description into its
     <h2> ("— placements that fell back to copy") while every other
     block keeps a short // TITLE + a sibling subtitle. The desc was
     also redundant with the block's body help-text paragraph.
     Trimmed the h2 to // STORAGE WASTE.

  D3 (LOW, decoration) — the decorative stat-glyph (▷ ◇ ◈) sat only
     on the 3 TDB cards, not the 4 PLEX cards (same big-card tier).
     Added glyphs to the PLEX cards, reusing each TDB counterpart's
     glyph so they rhyme (PLEX MOVIES ▷, TV ◇, COLLECTIONS ◈) + a
     filled ◆ for PLEX ANIME.

  D4 (LOW, dead code) — `.stat-primary` (green) was the v1.13.48
     ThemerrDB-card tone, superseded by `.stat-tdb-primary`
     (--green-bright) in v1.15.72. No template/JS referenced it.
     Removed the orphaned rule.

  D5 (LOW, voice) — the RECENT ACTIVITY "all events →" link was the
     one header-slot action missing the `//` prefix. Added it.
"""
from __future__ import annotations

import re
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
DASH_HTML = (REPO / "app" / "web" / "templates" / "dashboard.html").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


# ── D1 — every stat-card label carries the // prefix ─────────


def test_all_stat_labels_carry_slash_prefix():
    """Every `.stat-label` in the dashboard must use motif's `//`
    header voice — guards against a future card landing bare."""
    labels = re.findall(r'<span class="stat-label">([^<]*)</span>', DASH_HTML)
    assert labels, "no stat-labels found — selector drifted?"
    bare = [l for l in labels if not l.startswith("// ")]
    assert not bare, (
        f"v1.19.84: these stat-labels lack the // prefix: {bare}"
    )


def test_mini_card_labels_now_prefixed():
    """The 8 mini-card labels that were bare pre-fix must now read
    // <LABEL>."""
    for label in (
        "// QUEUE PENDING", "// RUNNING NOW", "// FAILURES",
        "// ADDED TODAY", "// ADDED THIS WEEK",
        "// HARDLINKS", "// COPIES", "// USER PROVIDED",
    ):
        assert f'<span class="stat-label">{label}</span>' in DASH_HTML, (
            f"v1.19.84: mini-card label {label!r} must carry the // prefix"
        )


# ── D2 — STORAGE WASTE title trimmed ─────────────────────────


def test_storage_waste_title_is_short():
    assert '<h2 class="block-title">// STORAGE WASTE</h2>' in DASH_HTML, (
        "v1.19.84: // STORAGE WASTE h2 must be the short title form"
    )
    assert "// STORAGE WASTE — placements that fell back to copy" not in DASH_HTML, (
        "v1.19.84: the description must no longer live in the h2 "
        "(it's redundant with the body help-text)"
    )


# ── D3 — PLEX cards gained their glyphs ──────────────────────


def test_plex_cards_have_rhyming_glyphs():
    """Each PLEX big card carries a media_glyph; movies/tv/collections reuse
    their TDB counterpart's icon, anime gets its own. v1.24.66: the glyphs are
    Feather-style SVG via the media_glyph() macro (was the ▶/▭/✦/▦ set), but the
    rhyming-with-TDB contract holds — each PLEX card's label is followed by the
    matching media_glyph() call."""
    for label, kind in (
        ("// MOVIES THEMED", "movies"),
        ("// TV THEMED", "tv"),
        ("// ANIME THEMED", "anime"),
        ("// COLLECTIONS THEMED", "collections"),
    ):
        assert (
            f'<span class="stat-label">{label}</span>'
            f"{{{{ media_glyph('{kind}') }}}}" in DASH_HTML
        ), f"v1.24.66: {label} must carry the {kind} media_glyph"


def test_tdb_card_glyphs_unchanged():
    """Regression baseline — the TDB glyphs rhyme with their PLEX counterparts.
    v1.24.66: each media type's media_glyph() appears twice (TDB + PLEX)."""
    for kind in ("movies", "tv", "anime", "collections"):
        assert DASH_HTML.count(f"media_glyph('{kind}')") == 2


# ── D4 — dead .stat-primary rule removed ─────────────────────


def test_stat_primary_rule_removed():
    assert ".stat-primary {" not in APP_CSS, (
        "v1.19.84: the dead .stat-primary rule must be gone "
        "(superseded by .stat-tdb-primary in v1.15.72)"
    )


def test_live_stat_tone_rules_preserved():
    """The fix must NOT have removed the live tone rules — only the
    orphaned .stat-primary."""
    assert ".stat-tdb-primary {" in APP_CSS
    assert ".stat-plex-primary {" in APP_CSS
    assert ".stat-plex-tv" in APP_CSS
    assert ".stat-plex-anime" in APP_CSS
    assert ".stat-plex-collections {" in APP_CSS


# ── D5 — RECENT ACTIVITY link prefixed ───────────────────────


def test_all_events_link_prefixed():
    # v0.50.72: → the Events Log view, not the JOBS default (?view=events).
    assert ('<a href="/queue?view=events" class="link-tiny">// all events →</a>'
            in DASH_HTML), (
        "v1.19.84: the RECENT ACTIVITY link must carry the // prefix + hit events"
    )
    assert '>all events →</a>' not in DASH_HTML, (
        "v1.19.84: the bare (un-prefixed) all-events link must be gone"
    )
