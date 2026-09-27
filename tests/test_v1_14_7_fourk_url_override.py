"""v1.14.7 — `?fourk=0` URL param must beat localStorage-saved variant.

Pre-fix the library page's variant resolution had this shape:

    if (sp.get('fourk') === 'true' || sp.get('fourk') === '1') {
        libraryState.fourk = true;
    } else {
        // fall back to localStorage motif:variant:<tab>
    }

The gate only treated `?fourk=1` / `?fourk=true` as an explicit
override. `?fourk=0` (deep-link to standard) silently fell into
the localStorage-fallback branch — and if localStorage had
'fourk' saved from a prior 4K visit on that tab, the page would
render 4K despite the URL explicitly saying standard.

the user's repro: topbar shows `1 UPD` and `7 FAIL`; both pills
have hrefs that build with the actual breakdown's fourk dimension
(see v1.13.78 + v1.13.69 — the breakdown SQL knows the update /
failure is in standard movies, so the href is
`/movies?fourk=0&attn_pills=update`). But localStorage has
`motif:variant:movies='fourk'` from a previous 4K browsing
session. Click → page lands on 4K with 0 matches.

v1.14.7 fix: any presence of `?fourk=` in the URL is an explicit
override — both =1 (force 4K) AND =0 (force standard) win over
localStorage. Only ABSENCE of the param falls back.

Tests pin the new gate via static-text guard. Behavioral coverage
of the actual page-load + localStorage interaction would need a
browser harness the project hasn't adopted (deferring per
CLAUDE.md test conventions).
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── New gate keys on `sp.has('fourk')` ────────────────────────


def test_fourk_param_presence_is_the_gate():
    """The new gate must check `sp.has('fourk')` — presence rather
    than truthy-value matching. That's how `?fourk=0` becomes an
    explicit override."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    assert "const sawFourkParam = sp.has('fourk');" in js
    assert "if (sawFourkParam) {" in js


def test_fourk_param_value_drives_libraryState():
    """Inside the explicit-override branch, the value of the param
    decides the variant (true/1 → 4K, anything else → standard).
    libraryState.fourk must be set unconditionally — not only on
    truthy values, otherwise =0 would leave it at its previous
    value (whatever localStorage / earlier code set)."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # The branch flips libraryState.fourk based on the parsed value.
    assert "const want4k = (v === 'true' || v === '1');" in js
    assert "libraryState.fourk = want4k;" in js


def test_chip_state_reflects_explicit_override():
    """The 4K/STD chip pair must visually match the override too —
    pre-fix on the (broken) `=0` path the chip stayed wherever it
    was even when libraryState.fourk eventually got set elsewhere,
    so user saw 4K active in the chip while standard rendered (or
    vice versa)."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # The class-toggle uses the parsed want4k value (not a hardcoded
    # === '1' check) so the STD chip lights up on ?fourk=0 too.
    assert "x.dataset.fourk === (want4k ? '1' : '0')" in js


# ── Old gate must be gone ─────────────────────────────────────


def test_old_truthy_only_gate_is_removed():
    """Regression guard: the pre-fix line

        if (sp.get('fourk') === 'true' || sp.get('fourk') === '1') {

    must not survive. A revert that re-narrowed the gate to only
    truthy values would silently re-break the topbar pill routing.

    Strip line comments before the check so the rationale comment
    documenting the removal doesn't trip the guard."""
    js_raw = (REPO / "app" / "web" / "static" / "app.js").read_text()
    js = "\n".join(
        line for line in js_raw.splitlines()
        if not line.lstrip().startswith("//")
    )
    assert (
        "if (sp.get('fourk') === 'true' || sp.get('fourk') === '1')"
        not in js
    ), (
        "v1.14.7: the old truthy-only gate must not reappear — "
        "?fourk=0 must beat localStorage too"
    )


def test_localStorage_fallback_only_fires_when_param_absent():
    """The localStorage-fallback branch must live under `else` of
    the `if (sawFourkParam)` gate. Pin the position so a refactor
    can't accidentally promote the fallback above the explicit
    override."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    gate_pos = js.index("if (sawFourkParam) {")
    fallback_pos = js.index("`motif:variant:${tabKey}`", gate_pos)
    # The fallback must live INSIDE the `else` that follows the gate
    # (i.e. after the gate, before the next unrelated structure).
    # Also assert the `else {` is between them.
    else_pos = js.index("} else {", gate_pos)
    assert gate_pos < else_pos < fallback_pos, (
        "v1.14.7: localStorage fallback must be inside the else of "
        "the sawFourkParam gate — promoting it above would re-break "
        "the explicit-override semantic"
    )


# ── Topbar pill href shape unchanged ──────────────────────────


# v0.51.354: these two pinned the href ASSIGNMENT's exact text, which broke when the route moved through a
# named const on its way into the cache. What they are actually guarding is the emitted SHAPE — a standard
# (non-4K) tab must send an explicit `fourk=0`, because the receiving page's v1.14.7 override treats a missing
# param as "whatever localStorage last chose". Read that off the real poll instead of off the source.
def test_upd_pill_href_still_emits_fourk_zero_when_standard(tmp_path):
    """The UPD pill href construction is unchanged by v1.14.7 (the fix is on the receiving side): the href must
    still emit `fourk=0` for a standard update so the override semantics receive a 0 to act on."""
    from test_v0_51_354_badge_route_cache import POLL_STATS, _poll
    out = _poll(tmp_path, POLL_STATS)  # updates live on standard tv
    assert out["cache"]["routes"]["upd"]["href"] == "/tv?fourk=0&attn_pills=update", out["cache"]


def test_upd_pill_href_emits_fourk_one_for_a_4k_tab(tmp_path):
    """The other half of the same shape — v1.13.78's reason for carrying fourk at all."""
    from test_v0_51_354_badge_route_cache import POLL_STATS, _poll
    stats = dict(POLL_STATS, updates={"pending": 2, "tab_hint": "movies",
                                      "tabs": [{"tab": "movies", "fourk": True, "count": 2}]})
    out = _poll(tmp_path, stats)
    assert out["cache"]["routes"]["upd"]["href"] == "/movies?fourk=1&attn_pills=update", out["cache"]


def test_fail_pill_href_still_emits_fourk_zero_when_standard(tmp_path):
    """Same shape for the FAIL pill — v1.13.69 wired the breakdown cycle, v1.14.7 makes the receiving page
    actually honor the standard variant."""
    from test_v0_51_354_badge_route_cache import POLL_STATS, _poll
    out = _poll(tmp_path, POLL_STATS)  # failures live on standard anime
    assert out["cache"]["routes"]["fail"]["href"] == "/anime?fourk=0&attn_pills=fail", out["cache"]
