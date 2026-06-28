"""v1.14.10 — composite-+P SRC pill (the yellow-dot filter).

Adds a new SRC pill so the user can filter to exactly the rows
that render the yellow-dot indicator on their SRC chip — i.e.
rows where the primary SRC letter (T/U/A/M) is set AND Plex
also serves its own theme via cloud / embed / Pass. Pre-fix the
yellow dot was visually distinct on individual rows but had no
filter axis, so a user couldn't quickly see "show me everything
with the dot".

The dot fires when (per app.js:5056):

    plex_independent_theme === 1
    AND primaryLetter !== 'P'
    AND primaryLetter !== '-'

The new pill's filter clause mirrors that predicate exactly via
_SRC_LETTER_SQL. Mirror principle: the pill's filter MUST agree
with the visual indicator on the rows.

=== URL-encoding choice ===

The wire token is `Pp` (lowercase p suffix), not `+P`. URL form-
encoding turns `+` into a space, which would break the round-
trip via URLSearchParams. The button's visible label keeps `+P`
since that's how the indicator reads in the row chip's tooltip
(v1.13.34 + v1.13.59 design). Audit-log values + URL params see
`Pp` consistently.

=== Why a separate pill from `P` ===

The existing `P` pill (v1.13.43) matches both pure-P rows AND
composite-+P rows — i.e. it's an OR. The new `Pp` pill matches
ONLY composite-+P rows — i.e. it excludes pure-P. They're
distinct surfaces:
  - `P` answers "which rows does Plex have a theme for at all"
  - `Pp` answers "which rows have a motif-managed theme that's
    also being shadowed by a Plex-served theme"
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── Pill button exists in the SRC row ─────────────────────────


def test_composite_plex_pill_button_in_library_html():
    """The button must be in the SRC pill row, between P and the
    TDB-only T (so the visual order reads T U A M P +P T —)."""
    html = (REPO / "app" / "web" / "templates" / "library.html").read_text()
    # Locate the SRC pill row anchor.
    src_row_start = html.index('aria-label="SRC pill filter"')
    src_row_end = html.index("</div>", src_row_start)
    row = html[src_row_start:src_row_end]
    # The new button must exist + use the wire token `Pp`.
    assert 'data-src-filter="Pp"' in row
    # Visible label is `+P` (matches the row chip indicator copy).
    assert ">+P</button>" in row
    # v1.23.12: chip tone demoted to the copy chip's muted grey —
    # two amber chips side by side read as one unit (the user). The
    # also-plex class still supplies the amber corner dot, keeping
    # parity with the row indicator.
    assert "link-badge link-badge-copy link-badge-also-plex" in row


def test_pill_button_position_between_p_and_tdb_only():
    """Visual ordering: T U A M P +P T —. The +P button sits between
    the existing P and the faded TDB-only T."""
    html = (REPO / "app" / "web" / "templates" / "library.html").read_text()
    src_row_start = html.index('aria-label="SRC pill filter"')
    src_row_end = html.index("</div>", src_row_start)
    row = html[src_row_start:src_row_end]
    p_pos = row.index('data-src-filter="P"')
    pp_pos = row.index('data-src-filter="Pp"')
    tdb_only_pos = row.index('data-tdb-only="1"')
    assert p_pos < pp_pos < tdb_only_pos, (
        "v1.14.10: +P pill must sit between P and the TDB-only T"
    )


def test_pill_tooltip_explains_composite_semantic():
    """Tooltip must convey that the pill matches the dot indicator
    AND restricts to T/U/A/M primary letters (i.e. excludes pure-P
    and src='-' which the dot never decorates)."""
    html = (REPO / "app" / "web" / "templates" / "library.html").read_text()
    src_row_start = html.index('aria-label="SRC pill filter"')
    src_row_end = html.index("</div>", src_row_start)
    row = html[src_row_start:src_row_end]
    # The tooltip references the matching primary letters explicitly
    # so the user understands what's IN vs OUT.
    assert "T/U/A/M" in row
    assert "yellow-dot" in row.lower()


# ── Server-side filter routing ────────────────────────────────


def test_pp_token_in_valid_tokens_set():
    """The api.py src_pills handler must accept 'Pp' as a valid
    token alongside the letters."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    assert 'valid_tokens = valid_letters | {"Pp"}' in src


def test_pp_token_emits_dot_indicator_predicate():
    """The composite-only branch must filter on the exact predicate
    the JS uses to render the dot:

        plex_independent_theme = 1 AND src letter NOT IN ('P', '-')

    Mirror principle: same expression on both sides, so the count +
    rows + dot all agree."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    # Anchor on the new branch marker.
    marker = "v1.14.10: composite-+P only"
    assert marker in src, "v1.14.10 marker comment must be present"
    block = src[src.index(marker):src.index(marker) + 1500]
    assert 'include_plus_p_only' in block or 'pi.plex_independent_theme = 1' in block
    # The clause itself.
    assert 'pi.plex_independent_theme = 1' in block
    assert "NOT IN ('P', '-')" in block


def test_pp_does_not_overlap_with_plain_p_pill():
    """The plain `P` pill keeps its v1.13.43 OR semantic (pure-P OR
    composite). The `Pp` pill is the strict subset. They're parsed
    distinctly so a user can pick `+P` alone without being forced to
    also include pure-P matches."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    # `non_p` excludes both 'P' and 'Pp' so neither short-circuits
    # the other.
    assert 'non_p = [w for w in wanted if w not in ("P", "Pp")]' in src
    # Two separate flags drive the two clauses.
    assert 'include_p_flag = "P" in wanted' in src
    assert 'include_plus_p_only = "Pp" in wanted' in src


# ── JS ALL button picks up the new token ──────────────────────


def test_all_letters_includes_pp_token():
    """When the user clicks the SRC ALL button, the inverse-filter
    pattern adds every chip — the new `Pp` chip must be in that set
    or ALL would silently exclude composite-+P rows."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    assert "const allLetters = ['T', 'U', 'A', 'M', 'P', 'Pp', '-'];" in js


# ── Behavioral: filter SQL agrees with the dot predicate ──────


def test_composite_p_filter_sql_matches_js_dot_render_gate():
    """End-to-end: the JS row-render gate at app.js:5056-5058 is

        plex_independent_theme === 1
        AND primaryLetter !== 'P'
        AND primaryLetter !== '-'

    The server-side Pp-pill clause must mirror that. This test
    pins the JS gate is unchanged AND the server clause is in
    place — so a future drift on either side fails loudly."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # JS render gate (v1.13.34/38 area).
    assert "it.plex_independent_theme === 1" in js
    assert "_primaryLetter !== 'P'" in js
    assert "_primaryLetter !== '-'" in js
    # Server-side Pp pill clause.
    src = (REPO / "app" / "web" / "api.py").read_text()
    assert "pi.plex_independent_theme = 1" in src
    assert "NOT IN ('P', '-')" in src
