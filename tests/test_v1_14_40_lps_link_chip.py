"""v1.14.40 — LPS state moves to LINK column + LPS-aware recovery card.

v1.14.39 added a blue PL chip + blue title `!` glyph for LPS
state. the user's design feedback: amber is "Plex" in his mental
model, blue PL collided semantically with blue pending-update
title glyph, and PL chip should honestly read "no placement"
not carry intent.

Better fit: a NEW value in the LINK column (`PS` = Plex-
Serving). LINK column is "what's the relationship between
motif's canonical and Plex's serving file" — LPS IS a
relationship type ("Plex serves its own; motif's canonical
preserved"). Slots cleanly alongside HL/C/M/—.

## Scope

1. **LINK chip**: new `PS` value (amber, matching motif's "Plex"
   color vocabulary). Renders when `lpsState` is true.
2. **Revert v1.14.39 PL/title-glyph blue variants**: PL chip
   falls back to gray `''` (honest "no placement"). Title `!`
   glyph no longer fires for LPS rows (the awaitingApproval
   `&& !lpsState` exclusion stays so genuine attention cases
   like Radarr/Sonarr deleting a sidecar externally still
   surface).
3. **Recovery card LPS-aware**: detect LPS in `api_recovery_
   options` no-fail branch. Hide LET PLEX SERVE + ADOPT + LET
   PLEX SERVE (already done). Surface PUSH MOTIF'S THEME
   (action `push-to-plex` → /replace), REVERT TO USER URL (if
   previous_url with kind='user'), RE-DOWNLOAD FROM TDB (if
   non-orphan + youtube_url set).
4. **JS dispatcher**: new `act === 'push-to-plex'` branch wires
   the recovery card button to /api/items/{mt}/{id}/replace.
5. **SQL filter exclusion stays** (from v1.14.39).
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── LINK chip: PS branch ────────────────────────────────────


# v1.14.40's PS render tests removed in v1.19.66 — the chip
# was dropped entirely (vestigial post-v1.19.61 PS→BK
# unification, replaced by SRC=P + LINK=— combo for the
# backup-candidate filter workflow). See
# tests/test_v1_19_66_revert_ps_chip.py for the negative
# assertions (chip MUST NOT exist).


# ── PL chip: revert v1.14.39 blue variant ───────────────────


def test_pl_ternary_no_longer_has_lps_branch():
    """The pl ternary must NOT include the v1.14.39 'lps' branch.
    The PL chip should read as gray (`''` state) for LPS rows —
    it answers "is there a placement?" (true: no), not "what's
    the row's intent?". The LINK PS chip carries the LPS signal."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    pl_anchor = js.index("const pl = placementBroken")
    block = js[pl_anchor:pl_anchor + 500]
    # The v1.14.39 LPS branch must NOT survive.
    assert ": lpsState ? 'lps'" not in block


def test_pl_tooltip_no_longer_has_lps_branch():
    """The plTip must NOT include the LPS-specific branch — the
    PL chip's tooltip should match its actual state ("Not placed
    in Plex folder" for LPS rows). The LPS-specific tooltip lives
    on the new PS LINK chip."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    plTip_anchor = js.index("const plTip = placeInFlight")
    block = js[plTip_anchor:plTip_anchor + 1000]
    assert "pl === 'lps'" not in block
    # The "Plex serves" string should NOT appear in the plTip
    # block (it's now exclusive to the linkCell tooltip).
    assert "Plex serves its own theme" not in block


# ── Title-cell glyph: LPS branch reverted ───────────────────


def test_title_glyph_chain_no_longer_has_lps_branch():
    """The title-cell glyph chain must NOT include the v1.14.39
    blue `!` LPS branch. The amber `!` `awaitingApproval` branch
    correctly subtracts lpsState so it doesn't fire on LPS rows
    either — net effect: no title glyph at all on LPS rows. The
    PS LINK chip is the single source of LPS truth."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    chain_anchor = js.index("} else if (it.mismatch_state === 'pending') {")
    block = js[chain_anchor:chain_anchor + 3000]
    assert "} else if (lpsState) {" not in block
    # Marker comment explains why the branch was removed.
    assert "v1.14.40: removed the v1.14.39 LPS title-glyph branch" in block


def test_awaiting_approval_still_excludes_lps():
    """The `awaitingApproval && !lpsState` exclusion must STAY
    so the amber title `!` doesn't fire on LPS rows. Genuine
    "theme went missing externally" cases (Radarr/Sonarr
    deletes the placement file) DO fire because they have
    plex_independent_theme=0 — they're not LPS, they're broken
    + need attention."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    aa_anchor = js.index("const awaitingApproval = !it.job_in_flight")
    block = js[aa_anchor:aa_anchor + 300]
    assert "&& !lpsState" in block


# ── CSS: new link-glyph-ps + removed v1.14.39 classes ───────


def test_css_link_glyph_ps_class_defined():
    """v1.20.47 (CSS audit): `.link-glyph-ps` was REMOVED as dead CSS.
    The PS row badge was deprecated — the LPS signal folded into the
    SRC=P pill, and `link_pills=ps` survives only as an api.py no-op
    (library.html:394), so nothing ever rendered the class (no PS button
    in the LINK filter row, no PS branch in renderLibraryRow). This now
    guards the REMOVAL so the orphan can't creep back."""
    css = (REPO / "app" / "web" / "static" / "app.css").read_text()
    assert ".link-glyph-ps {" not in css


def test_css_v1_14_39_lps_classes_removed():
    """The v1.14.39 `.state-pill.lps` and `.title-glyph-lps`
    classes must NOT survive — their declarations were removed
    in v1.14.40 (replaced with marker comments explaining why).
    Pin the absence of the actual rule blocks."""
    css = (REPO / "app" / "web" / "static" / "app.css").read_text()
    assert ".state-pill.lps {" not in css
    # title-glyph-lps as a class declaration (with brace).
    assert ".title-glyph-lps " not in css or ".title-glyph-lps  {" not in css
    # The marker comments confirming the cleanup.
    assert "v1.14.40: removed `.state-pill.lps`" in css
    assert "v1.14.40: removed `.title-glyph-lps`" in css


def test_css_state_pill_await_still_amber():
    """Sanity: the v1.14.40 fix is purely additive on the LINK
    column + revertive on the PL/title-glyph. `.state-pill.await`
    still uses --amber (unchanged from before v1.14.39)."""
    css = (REPO / "app" / "web" / "static" / "app.css").read_text()
    await_anchor = css.index(".state-pill.await {")
    block = css[await_anchor:await_anchor + 200]
    assert "var(--amber)" in block


# ── api_recovery_options no-fail branch: LPS-aware ──────────


def test_api_recovery_options_detects_lps_state():
    """v1.14.61 deleted the LPS-detection block in
    api_recovery_options entirely. Both `is_lps` and
    `motif_has_placement` had zero downstream consumers
    inside the function post-v1.14.47 (verified by v1.14.55 M5
    dropped finding); the SQL roundtrip was pure dead provision.
    The contract this test originally pinned (LPS detection
    still happens somewhere) survives via the JS-side `lpsState`
    predicate at app.js (computeSrcLetter / link-cell render)
    + the per-row `is_lps` in `_row_matches_pl` at api.py:2028."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    # The v1.14.61 deletion marker is in api_recovery_options.
    fn_anchor = src.index("async def api_recovery_options(")
    body = src[fn_anchor:fn_anchor + 25000]
    assert "v1.14.61: deleted the v1.14.42/v1.14.44 `is_lps`" in body
    # The independent _row_matches_pl is_lps survives.
    assert "is_lps = (" in src
    # JS-side LPS predicate also survives (sanity: not all LPS
    # detection got nuked).
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    assert "lpsState" in js


def test_api_recovery_options_hides_let_plex_serve_when_lps():
    """When the row is in LPS state, neither LET PLEX SERVE nor
    ADOPT + LET PLEX SERVE may render — both already happened
    (LPS == "motif unplaced; Plex serves on its own").

    v1.14.47 reorg: the no-fail branch was emptied (TRY THIS
    NEXT is failure-only now); the LET PLEX SERVE / ADOPT + LET
    PLEX SERVE actions moved to the SOURCE menu. The "hide on
    LPS" contract survives via the SOURCE-menu render gates,
    which require `placed` (= motif_has_placement) — LPS rows
    are by definition !placed, so neither button appears."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # LET PLEX SERVE gate: requires `placed`.
    lps_anchor = js.index("'purge-revert-to-plex', 'LET PLEX SERVE'")
    # Walk back to the surrounding `if (`.
    gate_start = js.rfind("if (", lps_anchor - 800, lps_anchor)
    lps_gate = js[gate_start:lps_anchor]
    assert "placed" in lps_gate, (
        "v1.14.47 SOURCE-menu LET PLEX SERVE gate must require "
        "placed (LPS rows are !placed → button hides)"
    )
    # ADOPT + LET PLEX SERVE gate: requires `!placed` AND
    # plex_local_theme === 1 (sidecar exists). LPS state has
    # !placed but plex_local_theme === 0 (motif unplaced its own
    # file; no foreign sidecar to adopt) → button hides.
    adopt_anchor = js.index("'adopt-and-let-plex-serve', 'ADOPT + LET PLEX SERVE'")
    adopt_gate_start = js.rfind("if (", adopt_anchor - 800, adopt_anchor)
    adopt_gate = js[adopt_gate_start:adopt_anchor]
    assert "plex_local_theme === 1" in adopt_gate
    assert "!placed" in adopt_gate


def test_api_recovery_options_surfaces_push_to_plex_when_lps():
    """When the row is in LPS state, the user must have a one-
    click way to undo (re-place motif's canonical at the Plex
    folder).

    v1.14.47 reorg: the dedicated `push-to-plex` recovery action
    was removed in favor of the existing PLACE-menu PUSH TO PLEX
    button — its gate (themed && downloaded && !placed &&
    !dlBroken) IS the LPS state, so PUSH TO PLEX naturally
    surfaces on every LPS row without needing a separate action.

    Pin the marker on the no-fail branch explaining the move,
    plus the PLACE-menu PUSH TO PLEX render in app.js (the
    surviving entry point)."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("async def api_recovery_options(")
    body = src[fn_anchor:fn_anchor + 25000]
    # The v1.14.47 marker explains the move.
    assert "PUSH MOTIF'S THEME → PLACE menu's PUSH TO PLEX" in body
    # The PLACE-menu PUSH TO PLEX entry point survives in app.js.
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    assert "PUSH TO PLEX" in js


def test_api_recovery_options_surfaces_revert_when_lps_with_user_url():
    """LPS rows with a captured previous user URL must have a
    one-click REVERT path.

    v1.14.47 reorg: the dedicated REVERT TO USER URL recovery
    option was removed; the existing SOURCE-menu REVERT button
    (UNDO section) handles this generically — it surfaces on
    every row with a captured previous_url snapshot of
    kind='user' regardless of LPS state.

    Pin the v1.14.47 marker explaining the move + the SOURCE-
    menu REVERT entry point's existence."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("async def api_recovery_options(")
    body = src[fn_anchor:fn_anchor + 25000]
    assert "REVERT TO USER URL → SOURCE menu UNDO section" in body
    # The SOURCE-menu REVERT button still exists in app.js.
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # The UNDO section header marker is stable across versions.
    assert "// ── 6. UNDO" in js


def test_api_recovery_options_surfaces_redl_when_lps_non_orphan():
    """LPS rows with TDB coverage must have a one-click way to
    pull the canonical back from TDB.

    v1.14.47 reorg: the dedicated RE-DOWNLOAD FROM TDB recovery
    option was removed; the SOURCE-menu REDL button handles this
    generically. The v1.14.47 marker notes the SOURCE-menu gate
    was relaxed to allow LPS rows (was previously gated to
    require a placement)."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("async def api_recovery_options(")
    body = src[fn_anchor:fn_anchor + 25000]
    assert "RE-DOWNLOAD FROM TDB → SOURCE menu" in body
    # The SOURCE-menu REDL dispatcher branch survives.
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    assert "act === 'redl'" in js


# ── JS dispatcher: push-to-plex branch ──────────────────────


def test_js_dispatcher_handles_push_to_plex():
    """LPS rows must have a click path that POSTs to /api/items/
    {mt}/{id}/replace (re-places motif's canonical).

    v1.14.47 reorg: the `act === 'push-to-plex'` recovery-card
    branch was removed; PLACE-menu PUSH TO PLEX (which routes to
    the same /replace endpoint) is the surviving path. Its gate
    (themed && downloaded && !placed && !dlBroken) IS the LPS
    state, so PUSH TO PLEX surfaces on every LPS row.

    Pin the /replace endpoint stays callable from app.js + the
    v1.14.47 marker on the no-fail branch documenting the move."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # The PLACE-menu PUSH TO PLEX dispatcher branch routes to
    # `replaceTheme()` which POSTs /api/items/{mt}/{id}/replace.
    assert "act === 'replace'" in js
    assert "/api/items/${mediaType}/${tmdbId}/replace" in js
    src = (REPO / "app" / "web" / "api.py").read_text()
    assert "PUSH MOTIF'S THEME → PLACE menu's PUSH TO PLEX" in src


# ── Reuse pin: v1.14.39 SQL filter exclusion stays ──────────


def test_v1_14_39_sql_filter_exclusion_still_active():
    """The v1.14.39 attn_pills='await' SQL exclusion (and the
    _row_matches_pl mirror) MUST stay — they're the actual
    NEEDS WORK / !P filter fix. v1.14.40 only changes the visual
    surface (PL chip + LINK chip + title glyph + recovery card),
    not the filter behavior."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    # The attn_pills="await" predicate marker.
    assert "v1.14.39: exclude LPS state" in src
    assert "COALESCE(pi.plex_independent_theme, 0) = 0" in src
    # The _row_matches_pl mirror.
    assert "is_lps = (" in src
