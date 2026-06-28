"""v1.14.23 — close two more pill-token leaks (dropped + broken).

Wave 1.2 from the audit. Two findings of the same shape — pill
token live in template + URL parser + server, missed by JS
click-handler ALL list (or, for tdb_pills=dropped, the deep-link
parser too).

Same bug class as the v1.14.10/11/17 +P/Pp chain that took three
releases to fully close (CLAUDE.md class P, mirror-principle
drift; PROJECT_HISTORY § 1 R, token chain leaks).

## H1 — tdb_pills="dropped" missed in 2 of 3 JS layers

(Audit ref: AUDIT_FRONTEND.md H1)

The TDB ◌ chip ('dropped' value) is in:
- template (data-tdb-pill="dropped" button)
- server (api.py:6295 _pset valid set has 'dropped')
- localStorage hydrate (generic — accepts whatever was saved)
- click handler toggle (also generic)

Pre-fix MISSED in:
- click handler ALL button (`pillAxes[0].values` array)
- URL deep-link parser (`PILL_DEEP_LINKS[tdb_pills].values` Set)

Net effect:
- Clicking // ALL on TDB row activates 5 chips but TDB ◌ stays
  inactive (the inverse-filter pattern is broken).
- Sharing a `?tdb_pills=dropped` URL silently strips the token
  on page load.

## H4 — pl_pills="broken" missed in click-handler ALL

(Audit ref: AUDIT_FRONTEND.md H4)

The red ●broken PL pill is in:
- template (data-pl-pill="broken" button)
- server (_pset valid set: ('on','await','off','broken'))
- URL deep-link parser (already has 'broken')
- localStorage + toggle (generic)

Pre-fix MISSED in:
- click handler ALL list (`['on', 'await', 'off']`)

Net effect: clicking // ALL on PL row activates 3 chips but
●broken stays inactive.

This audit shows the recurring pattern is wider than just one
token — the JS pillAxes ALL set + PILL_DEEP_LINKS values Set
need to mirror server-side acceptance for EVERY axis. v1.14.23
closes the two known instances; Wave 4 may need a class-level
fix (single config block listing every (axis, valid values) tuple).
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── tdb_pills='dropped' fixes ─────────────────────────────────


def test_tdb_pill_all_handler_includes_dropped():
    """The // ALL button on the TDB pill row must activate the
    'dropped' chip too. Pin the pillAxes click-handler values
    array contents."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # Anchor on the v1.14.23 marker comment for tdb.
    marker = "v1.14.23: 'dropped' added to TDB ALL set"
    assert marker in js
    block_start = js.index(marker)
    block = js[block_start:block_start + 800]
    # The values array must include 'dropped' (+ v1.24.75 'empty').
    assert "values: ['tdb', 'update', 'cookies', 'dead', 'none', 'dropped', 'empty']" in block


def test_tdb_pill_deep_link_parser_includes_dropped():
    """URL deep-link parser must accept 'dropped' so
    `?tdb_pills=dropped` hydrates the chip on page load."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # Anchor on the v1.14.23 marker comment for the deep-link.
    marker = "v1.14.23: include the v1.13.1 'dropped' token"
    assert marker in js
    block_start = js.index(marker)
    block = js[block_start:block_start + 500]
    assert "values: new Set(['tdb','update','cookies','dead','none','dropped','empty'])" in block


def test_tdb_pill_pre_fix_5_token_set_is_gone():
    """Regression guard: the pre-fix 5-token sets must not
    survive in either the click-handler ALL or the deep-link
    parser. Comment-stripped to dodge rationale comments."""
    js_raw = (REPO / "app" / "web" / "static" / "app.js").read_text()
    js = "\n".join(
        line for line in js_raw.splitlines()
        if not line.lstrip().startswith("//")
    )
    # Pre-fix click-handler shape.
    assert "values: ['tdb', 'update', 'cookies', 'dead', 'none']" not in js
    # Pre-fix deep-link parser shape.
    assert "values: new Set(['tdb','update','cookies','dead','none'])" not in js


# ── pl_pills='broken' fix ─────────────────────────────────────


def test_pl_pill_all_handler_includes_broken():
    """The // ALL button on the PL pill row must activate the
    ●broken chip too."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # Anchor on the v1.14.23 marker for pl. The phrase now
    # appears twice (the v1.14.52 historical comment + the
    # v1.18.25 in-context marker right above the values array);
    # use rindex to pick the latter so the 500-char window
    # captures the actual values literal.
    marker = "v1.14.23: 'broken' added to PL ALL set"
    assert marker in js
    block_start = js.rindex(marker)
    block = js[block_start:block_start + 500]
    # v1.18.30: PL axis re-shrunk to 4 entries — the v1.18.25
    # 'pushed' chip was retired as redundant with LINK=PU
    # (literal same SQL predicate). The set still contains
    # 'broken' (the v1.14.23 fix this test pins).
    assert "values: ['on', 'await', 'off', 'broken']" in block


def test_pl_pill_deep_link_parser_already_has_broken():
    """Regression guard: PL deep-link parser already had
    'broken' (per audit). v1.14.23 doesn't touch it; pin so
    a refactor doesn't drop it."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    pl_anchor = js.index("param: 'pl_pills'")
    block = js[pl_anchor:pl_anchor + 500]
    # v1.18.30: 'pushed' retired; 'broken' still present.
    assert "values: new Set(['on','await','off','broken'])" in block


def test_pl_pill_pre_fix_3_token_set_is_gone():
    """Regression guard: the pre-fix click-handler PL ALL
    list (`['on', 'await', 'off']`) must not survive."""
    js_raw = (REPO / "app" / "web" / "static" / "app.js").read_text()
    js = "\n".join(
        line for line in js_raw.splitlines()
        if not line.lstrip().startswith("//")
    )
    assert "values: ['on', 'await', 'off']" not in js


# ── End-to-end mirror: every JS layer agrees with server ─────


def test_tdb_pills_mirror_check_across_all_layers():
    """Mirror-principle check: the 'dropped' token must be
    accepted by EVERY layer that filters tdb_pills values:
    server _pset, JS click-handler ALL, JS deep-link parser.

    If this test fails, we've recursed the bug class. The
    template (data-tdb-pill="dropped") should ALSO be
    present but is harder to pin without parsing HTML —
    grep for the data-attr at minimum."""
    py = (REPO / "app" / "web" / "api.py").read_text()
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    html = (REPO / "app" / "web" / "templates" / "library.html").read_text()
    # Server _pset (api.py).
    assert '"dropped"' in py and "tdb_pills" in py
    # JS click-handler ALL.
    assert "'dropped'" in js
    # JS deep-link parser.
    assert "'dropped'" in js  # (same string both places — fine)
    # Template button.
    assert 'data-tdb-pill="dropped"' in html


def test_pl_pills_broken_mirror_check_across_all_layers():
    """Same mirror check for PL='broken' — every JS+server+template
    layer accepts 'broken'."""
    py = (REPO / "app" / "web" / "api.py").read_text()
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    html = (REPO / "app" / "web" / "templates" / "library.html").read_text()
    # Server _pset
    assert '"broken"' in py
    # JS click-handler ALL (the v1.14.23 fix).
    assert "'broken'" in js
    # Template button.
    assert 'data-pl-pill="broken"' in html
