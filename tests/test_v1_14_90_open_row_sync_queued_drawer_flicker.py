"""v1.14.90 — three drawer/queue clarity fixes from the user's
v1.14.89 retest.

## A. OPEN ROW from /queue lands on correct fourk + prefilled search

the user: "It opened the info card still which is fine, it also
changed to the proper library but it went to the 4k section
(the last section I was in that library) instead of the
standard library it actually exists within. It would be great
if we could make it fill in the search bar with the name of
the movie so it truely would result in bring up just that row."

Pre-fix the OPEN ROW button only passed info_open + info_mt +
info_section. The page hydrated `?fourk=` from localStorage's
last-visited variant per tab, so a row living in standard
movies would land on /movies?fourk=1 if the user was last on
4K — empty results. And even on the right variant, the row
sat in a section's worth of others.

Fix:
1. api.py REPROBE log_event detail now includes is_4k from
   plex_sections JOIN.
2. The OPEN ROW button stamps data-fourk + data-title.
3. The handler appends `?fourk=` + `?q=` to the URL.
4. The library URL parser hydrates `?q=` into libraryState.q
   AND the visible search input.

## B. Queued THEMERRDB SYNC visible in drawer + topbar +N badge

the user: "when a themerrdb sync is pressed while active plex
refreshes are going on other than the button changing to
locked and syncing themerrdb there is no way to tell that's
its queued up. Could we add that into N Queue tracking and
into the drawer."

Pre-fix the sync job sat as 'pending' in the jobs table while
plex_enum/scan held the long-worker. No synth row, no drawer
card, no topbar signal — only the dash button's lock + label
hinted anything was queued.

Fix:
1. progress.py emits a `tdb_sync_pending` synth row (mirroring
   plex_enum_pending) when sync is pending AND another long-
   worker is running.
2. ops.js TONE_BY_KIND + KIND_LABEL gain `tdb_sync_pending` →
   tdb tone + 'THEMERRDB SYNC (QUEUED)' label so the drawer
   renders it as a sibling to the running THEMERRDB SYNC card.
3. The topbar +N QUEUED badge surfaces sync pending: shows
   "+N QUEUED · SYNC" when both plex queue + sync queued, or
   "SYNC QUEUED" (tdb tone) when only sync is queued.

## C. Drawer text flicker during active sync/refresh

the user: "Lastly I still see the same flicking on the text in
the drawer while active refresh or sync are running"

v1.14.81's per-card hash-skip prevented full body rebuild but
the active card itself still re-rendered every poll because
elapsed/counter/rate/pct change every tick → hash differs →
DOM replace → repaint → flicker.

Fix: structural-hash + in-place updater.
1. _structuralHash(op) captures only fields that determine
   STRUCTURE (kind, status, stage, presence flags, badges).
   Excludes elapsed, counter values, rate, sparkline data,
   activity items.
2. _updateCardInPlace(el, op) twiddles dynamic textContent +
   style.width on known selectors (.op-card-stage, counter
   spans, bar fill, .op-card-meta-item[data-meta-key=...]'s
   .op-card-meta-value, timeline cell classes, sparkline +
   activity sub-elements).
3. The diff loop has a fast path: if existing children match
   desired by key in the same order, do per-slot in-place
   updates and skip the body.replaceChildren call entirely.
   Only structural shape changes (added/removed/reordered
   cards) trigger the full fragment rebuild.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
JS = REPO / "app" / "web" / "static" / "app.js"
OPS_JS = REPO / "app" / "web" / "static" / "ops.js"
API_PY = REPO / "app" / "web" / "api.py"
PROGRESS_PY = REPO / "app" / "core" / "progress.py"


# ── A. OPEN ROW correct fourk + prefilled search ───────────────


def test_reprobe_row_select_pulls_section_is_4k():
    """The REPROBE row enrichment SQL must JOIN plex_sections to
    pick up is_4k. Without it the event detail can't carry the
    field the OPEN ROW button needs to deep-link the right variant."""
    src = API_PY.read_text()
    assert (
        "ps.is_4k AS section_is_4k" in src
    ), "REPROBE row SELECT must include section_is_4k for fourk routing"
    assert (
        "LEFT JOIN plex_sections ps" in src
    ), "REPROBE row SELECT must LEFT JOIN plex_sections (is_4k source)"


def test_reprobe_log_event_detail_includes_is_4k():
    """The log_event detail dict must include the is_4k field so
    the /queue render can stamp data-fourk on the OPEN ROW button."""
    src = API_PY.read_text()
    # Anchor on the REPROBE log_event call; the detail dict is
    # right below.
    anchor = src.index('REPROBE error rk={rk}')
    block = src[anchor:anchor + 2500]
    assert '"is_4k"' in block, (
        "REPROBE log_event detail must include is_4k for OPEN ROW "
        "fourk routing"
    )
    assert "section_is_4k" in block


def test_open_row_button_stamps_fourk_and_title():
    """The /queue events render's OPEN ROW button must emit
    data-fourk and data-title attributes so the click handler can
    deep-link to the correct library variant + prefill search."""
    src = JS.read_text()
    # Anchor on the OPEN ROW button rendering.
    # v0.51.308: structural block (the attr builders through the button's
    # close) — the fixed -500 window lost det.is_4k when animeAttr landed.
    a = src.index("const sectionAttr = det.section_id")
    block = src[a:src.index("// OPEN ROW</button>", a)]
    assert "data-fourk" in block, (
        "OPEN ROW button must stamp data-fourk for variant routing"
    )
    assert "data-title" in block, (
        "OPEN ROW button must stamp data-title for search prefill"
    )
    # The fourk attribute reads from det.is_4k.
    assert "det.is_4k" in block


def test_open_row_handler_appends_fourk_and_q_url_params():
    """The OPEN ROW click handler must append ?fourk= + ?q= to
    the URL so the library page lands on the right variant with
    the search pre-filtered."""
    src = JS.read_text()
    # Anchor on the v1.14.90 marker inside the handler body — the
    # button-render also references reprobe-open-row, so we need
    # to land in the handler specifically.
    anchor = src.index(
        "v1.14.90: also pass ?fourk= (from the section's"
    )
    block = src[anchor:anchor + 2500]
    assert "params.set('fourk'" in block, (
        "OPEN ROW handler must include ?fourk= in the URL"
    )
    assert "params.set('q'" in block, (
        "OPEN ROW handler must include ?q= in the URL"
    )


def test_library_url_parser_hydrates_q_search_param():
    """The library URL parser must hydrate ?q= into libraryState.q
    AND the visible search input. Pre-fix only chip filters
    hydrated, so the OPEN ROW deep-link's ?q= was silently ignored."""
    src = JS.read_text()
    # Anchor on the v1.14.90 q-hydration block.
    anchor = src.index("v1.14.90: ?q= search-term hydration")
    block = src[anchor:anchor + 1500]
    assert "sp.get('q')" in block, "Parser must read ?q= from URL"
    assert "libraryState.q = wantQ" in block
    assert "library-search" in block, (
        "Parser must also set the visible <input> value so the "
        "user sees the active search"
    )


# ── B. Queued THEMERRDB SYNC visible in drawer + topbar ────────


def test_progress_emits_tdb_sync_pending_synth_row():
    """progress.py must emit a synth tdb_sync_pending row when sync
    is pending AND another long-worker job is running, mirroring
    the plex_enum_pending pattern."""
    src = PROGRESS_PY.read_text()
    assert "tdb_sync_pending" in src, (
        "progress.py must define a tdb_sync_pending synth row"
    )
    # The synth must gate on other long-worker activity (sync
    # alone in pending = brief click→pickup window, not a real
    # queue) — same shape as v1.14.77's plex_enum_pending gate.
    # v1.15.2: anchor on the synth-row op_id literal to skip
    # past v1.14.92's earlier comment that mentions
    # tdb_sync_pending in the plex_enum_pending block.
    anchor = src.index('"queue:tdb_sync_pending"')
    block = src[max(0, anchor - 2500):anchor + 1500]
    assert "sync_is_queued" in block, (
        "tdb_sync_pending must be gated on a real-queue condition"
    )
    # SQL must check for plex_enum/scan running as the blocking
    # cases (plus another sync running).
    assert "'plex_enum'" in block or "plex_enum" in block
    assert "'scan'" in block or "scan" in block


def test_tdb_sync_pending_synth_includes_queue_depth_detail():
    """The synth row's detail dict must carry queue_depth so the
    topbar badge / drawer can render the count consistently with
    the plex_enum_pending shape."""
    src = PROGRESS_PY.read_text()
    anchor = src.index("tdb_sync_pending")
    block = src[anchor:anchor + 2500]
    assert '"queue_depth"' in block


def test_ops_js_tone_and_label_for_tdb_sync_pending():
    """ops.js TONE_BY_KIND + KIND_LABEL must include
    tdb_sync_pending → 'tdb' tone + 'THEMERRDB SYNC (QUEUED)'
    label so the drawer renders it as a sibling to the running
    THEMERRDB SYNC card."""
    src = OPS_JS.read_text()
    # TONE_BY_KIND entry.
    tone_anchor = src.index("const TONE_BY_KIND")
    tone_block = src[tone_anchor:tone_anchor + 1500]
    assert "tdb_sync_pending:" in tone_block
    assert "'tdb'" in tone_block.split("tdb_sync_pending")[1].split("\n")[0]
    # KIND_LABEL entry.
    label_anchor = src.index("const KIND_LABEL")
    label_block = src[label_anchor:label_anchor + 2500]
    assert "tdb_sync_pending:" in label_block
    assert "THEMERRDB SYNC (QUEUED)" in label_block


def test_topbar_overflow_pill_surfaces_sync_pending():
    """The #op-mini-overflow pill must render a SYNC-flavored
    badge when tdb_sync_pending is in the ops list. Two cases:
      - both plex queue + sync queued → '+N QUEUED · SYNC'
      - only sync queued → 'SYNC QUEUED' (tdb tone)

    Slice widened in v1.15.5 — the overflow pill block grew
    to add a download_queue branch after the sync branch,
    pushing SYNC QUEUED past the original 4000-char window."""
    src = OPS_JS.read_text()
    anchor = src.index("v1.14.84: repurpose the v1.13.45-hidden")
    # v1.15.30 expanded the overflow block (probe-suffix
    # composition) — widen the slice further to include the
    # full cascade.
    block = src[anchor:anchor + 9500]
    assert "tdb_sync_pending" in block, (
        "Overflow pill must check for tdb_sync_pending"
    )
    # Mixed-queue label. v1.15.30 appends an optional
    # ${probeSuffix} after "SYNC" — pin the literal "QUEUED · SYNC"
    # head, the suffix is a separate concern.
    assert "QUEUED · SYNC" in block, (
        "Mixed plex+sync queue must label the badge with · SYNC"
    )
    # Sync-only label + tdb tone. v1.15.30: the sync-only
    # branch's label is now `SYNC QUEUED${probeSuffix}` — the
    # leading literal is still "SYNC QUEUED".
    assert "SYNC QUEUED" in block
    assert "op-tone-tdb" in block, (
        "Sync-only badge must use tdb tone (orange) to differentiate "
        "from the plex-tone (green) refresh-queue badge"
    )


# ── C. Drawer text flicker — structural hash + in-place updater ─


def test_meta_items_carry_data_meta_key_attribute():
    """The renderCard meta-items must each carry a data-meta-key
    attribute (rate / eta / elapsed / errors) so the in-place
    updater can find them by selector."""
    src = OPS_JS.read_text()
    # Anchor on renderCard's meta block.
    anchor = src.index('class="op-card-meta">')
    block = src[anchor:anchor + 3000]
    assert 'data-meta-key="rate"' in block
    assert 'data-meta-key="eta"' in block
    assert 'data-meta-key="elapsed"' in block
    assert 'data-meta-key="errors"' in block


def test_meta_items_have_value_class_for_in_place_updates():
    """Each meta-item's value span must carry op-card-meta-value
    class so the updater can find it via a stable selector
    instead of relying on positional :last-child."""
    src = OPS_JS.read_text()
    anchor = src.index('class="op-card-meta">')
    block = src[anchor:anchor + 3000]
    # Should appear at least 4 times (one per meta item).
    assert block.count('op-card-meta-value') >= 4


def test_structural_hash_function_exists():
    """ops.js must define _structuralHash(op) — captures only
    the fields whose change requires a full DOM replace.

    The hash output (the JSON blob) must include only structural
    flags, NOT raw counter / timing values. stage_current may be
    REFERENCED to derive a presence flag (e.g. isStuckPending),
    but the value itself must not appear in the keys/values of
    the hash."""
    src = OPS_JS.read_text()
    assert "function _structuralHash(op)" in src
    anchor = src.index("function _structuralHash(op)")
    block = src[anchor:anchor + 2500]
    # The JSON.stringify call's object literal lives between the
    # `return JSON.stringify({` and the matching `});`. Slice that
    # out and check no high-frequency dynamic value gets embedded
    # as-is.
    json_start = block.index("return JSON.stringify({")
    json_end = block.index("});", json_start)
    json_block = block[json_start:json_end]
    # Counter values and elapsed seconds must NOT be inlined into
    # the hash blob — only their presence flags belong there.
    assert "stage_current" not in json_block, (
        "_structuralHash JSON blob must not embed stage_current "
        "(dynamic — would defeat in-place update)"
    )
    assert "processed_total" not in json_block, (
        "_structuralHash JSON blob must not embed processed_total "
        "(dynamic)"
    )


def test_update_card_in_place_function_exists():
    """ops.js must define _updateCardInPlace(el, op) — twiddles
    dynamic textContent + style on known selectors."""
    src = OPS_JS.read_text()
    assert "function _updateCardInPlace(el, op)" in src
    anchor = src.index("function _updateCardInPlace(el, op)")
    block = src[anchor:anchor + 5000]
    # Must update the headline.
    assert ".op-card-stage" in block
    # Must update counter target (for tickCounters smoothing).
    assert "data-op-counter-target" in block
    # Must update bar fill width.
    assert ".op-card-bar-fill" in block
    # Must update meta values via data-meta-key selector.
    assert "data-meta-key" in block
    # Must update timeline cell classes.
    assert ".op-card-timeline-step" in block


def test_render_drawer_body_uses_structural_hash_fast_path():
    """renderDrawerBody must check for sameStructure first and
    use _updateCardInPlace when structural hash matches, falling
    back to the fragment rebuild only on shape changes."""
    src = OPS_JS.read_text()
    anchor = src.index("function renderDrawerBody(ops)")
    block = src[anchor:anchor + 8000]
    # Fast-path marker.
    assert "sameStructure" in block, (
        "renderDrawerBody must compute sameStructure to decide "
        "fast-path vs fragment rebuild"
    )
    # Fast path uses _updateCardInPlace.
    assert "_updateCardInPlace" in block
    # Must still record the structural hash on cards.
    assert "_structuralHash" in block
    assert "data-card-skel" in block or "cardSkel" in block


def test_in_place_update_only_repaints_counter_when_target_changes():
    """The counter must update only when stage_current actually
    changed — otherwise an idle poll would re-paint the counter
    on every tick and defeat the v1.14.90 flicker fix.

    v1.15.13 amended the original contract: the textContent
    assignment IS allowed (and required) when target changes,
    because the prior approach (rely on tickCounters() to
    advance textContent via smoothing) silently broke.
    tickCounters' fallback `current = +attr || target` makes
    current === target on first read whenever
    data-op-counter-current is unset (which it always is post-
    render — the inline template literal sets textContent but
    not the attr) → early return → textContent never updates.
    the user v1.15.12 repro: drawer big counter stuck on initial
    render value while the rest of the card kept ticking. The
    v1.15.13 fix snaps the counter to its new value when the
    target changes.

    The flicker invariant the original v1.14.90 test guarded is
    preserved by the if-guard around the assignment — when
    target is unchanged, no write happens, no repaint."""
    src = OPS_JS.read_text()
    anchor = src.index("function _updateCardInPlace(el, op)")
    block = src[anchor:anchor + 5000]
    cur_anchor = block.index(".op-card-counter-current")
    cur_block = block[cur_anchor:cur_anchor + 800]
    # The if-guard around the writes is the load-bearing part for
    # the flicker invariant.
    assert "if (cur.getAttribute('data-op-counter-target') !== target)" in cur_block, (
        "Counter writes must be gated on target-actually-changed "
        "to preserve the v1.14.90 no-flicker contract"
    )
    # Both attributes get written (target + current) plus textContent.
    assert "data-op-counter-target" in cur_block
    assert "cur.textContent = fmtNum" in cur_block, (
        "v1.15.13: counter MUST update textContent directly when "
        "target changes (tickCounters' fallback prevents animation "
        "from advancing it)"
    )
