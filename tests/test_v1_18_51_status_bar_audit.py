"""v1.18.51 — status bar audit pass + library row refresh during enum.

the user's review of v1.18.50:

> "I did see a bit of a disconnect between the amber dl dot and
>  the row updating on the collections section. I saw the status
>  bar but the row didn't update right away once completed."
> "Can we also add in the drawer any section that displays a %
>  in the status bar a similar % in the drawer view as when you
>  open the drawer you get the N out N and the bar but not a %
>  like you did just viewing the status bar."
> "Can we do a general audit as well of the status bars to make
>  sure they still make sense, apply when needed and show the
>  correct response during all different event types or source
>  type or placement options."

## Bug A — library row stale during plex_enum cascade

`libraryRapidPoll` auto-stops after 2 consecutive empty polls
where no row has `job_in_flight`. plex_enum (esp. the cascade
fired by SYNC → AUTO-REFRESH PLEX AFTER SYNC) mutates rows
without per-row job markers — `plex_items.has_theme` flips,
`folder_path` drift triggers relink, new placements get queued
+ completed. With no per-row signal, rapid-poll stops while
state is still changing; the row UI lags the 30s background
tick.

Fix: refreshTopbarStatus stashes `myTabEnumBusy` +
`globalEnumActive` on `window.__motif_queue`, detects
transitions (false→true: kick rapid-poll; true→false:
one-shot loadLibrary). rapid-poll's auto-stop heuristic also
treats these signals as "still busy" so it survives the
enum window even if no per-row job is in flight.

## Bug B — drawer counter missing the `%`

Topbar mini-bar shows `<label> <bar> XX%`. Drawer card showed
N / N + a bar but no % textually. Added `.op-card-counter-pct`
chip next to the N/N pair so the two surfaces agree. Hidden
when the bar is indeterminate (no real % to show). Live-
updated on every render tick same as the counter.

## Bug C — TONE_BY_KIND / KIND_LABEL gaps

Audited the op_progress.kind CHECK constraint (db.py) against
ops.js's KIND_LABEL + TONE_BY_KIND maps. Two kinds were emitted
by api.py but missing from the JS-side maps:

  - `bulk_probe_tdb` — emitted by api.py:2930 (bulk-probe TDB
    URLs). Had a priority entry but no label/tone — drawer
    rendered "// bulk_probe_tdb" with no tone class.
  - `bulk_lps` — emitted by api.py:3492 (bulk LET PLEX SERVE,
    v1.15.28). Missing from all three maps. Drawer rendered
    raw kind, no tone, no mini-bar priority.

Added both to TONE_BY_KIND (tdb / plex respectively), KIND_LABEL
(BULK PROBE TDB / BULK LET PLEX SERVE), and OP_MINI_PRIORITY
(both at level 4 alongside bulk operations).
"""

from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_JS = REPO / "app" / "web" / "static" / "app.js"
OPS_JS = REPO / "app" / "web" / "static" / "ops.js"
OPS_CSS = REPO / "app" / "web" / "static" / "ops.css"
DB_PY = REPO / "app" / "core" / "db.py"


# ── Bug A: library refresh on plex_enum transitions ──────────


def test_refresh_topbar_stashes_my_tab_enum_busy():
    """refreshTopbarStatus must stash myTabBusy on
    window.__motif_queue so libraryRapidPoll's auto-stop check
    can see it. Without the stash the rapid-poll stops while
    plex_enum is still mutating rows."""
    src = APP_JS.read_text()
    # Stash key name.
    assert "__motif_queue.myTabEnumBusy" in src, (
        "v1.18.51: refreshTopbarStatus must stash "
        "myTabBusy as myTabEnumBusy on window.__motif_queue"
    )


def test_refresh_topbar_stashes_global_enum_active():
    """Mirror stash for globalEnumPipeline so settings SCAN ALL
    + sync→enum cascade keep the library refreshing."""
    src = APP_JS.read_text()
    assert "__motif_queue.globalEnumActive" in src


def test_refresh_topbar_kicks_rapid_poll_on_op_start():
    """On false→true transition of anyMutatingOpActive, kick
    libraryRapidPoll so the row UI starts refreshing
    immediately. v1.18.52 widened from myTabBusy-only to
    cover ALL row-mutating backend ops."""
    src = APP_JS.read_text()
    # The kick site lives next to the prevAnyMutatingActive comparison.
    assert "prevAnyMutatingActive" in src, (
        "v1.18.52: must track prevAnyMutatingActive across ticks"
    )
    idx = src.index("prevAnyMutatingActive")
    block = src[idx:idx + 1200]
    assert "libraryRapidPoll()" in block, (
        "v1.18.52: false→true transition must kick rapid-poll"
    )


def test_refresh_topbar_one_shot_load_on_op_end():
    """On true→false transition of anyMutatingOpActive, fire a
    one-shot loadLibrary so the post-op state shows up immediately
    (don't wait for the 30s background tick)."""
    src = APP_JS.read_text()
    idx = src.index("prevAnyMutatingActive")
    block = src[idx:idx + 1200]
    assert "loadLibrary()" in block, (
        "v1.18.52: true→false transition must one-shot loadLibrary"
    )


def test_rapid_poll_auto_stop_treats_global_op_as_busy():
    """libraryRapidPoll's auto-stop check must read
    window.__motif_queue.anyMutatingOpActive so the timer
    doesn't terminate while ANY row-mutating op is in flight.
    v1.18.52 unified myTabEnumBusy + globalEnumActive into the
    single anyMutatingOpActive signal that also covers
    tdb_sync + op_progress-only ops."""
    src = APP_JS.read_text()
    fn_idx = src.index("function libraryRapidPoll(")
    body = src[fn_idx:fn_idx + 3500]
    assert "anyMutatingOpActive" in body, (
        "v1.18.52: rapid-poll auto-stop must check anyMutatingOpActive"
    )


# ── Bug B: drawer counter shows % ────────────────────────────


def test_drawer_counter_html_contains_pct_chip():
    """The op-card counter HTML must include the
    `op-card-counter-pct` chip when a real-bar is in play."""
    src = OPS_JS.read_text()
    assert "op-card-counter-pct" in src, (
        "v1.18.51: drawer counter must render the % chip"
    )
    # The chip uses pct.toFixed(0) + '%' for consistency with
    # the topbar mini-bar.
    assert "pct.toFixed(0)" in src


def test_drawer_pct_chip_hidden_for_indeterminate_bars():
    """Indeterminate bars (no real % available) must NOT show
    a stale or zero % chip — the chip is conditional on
    useRealBar + pct != null."""
    src = OPS_JS.read_text()
    # Find the showPct definition.
    assert "const showPct" in src or "showPct" in src
    # The conditional ties to useRealBar + pct != null.
    idx = src.index("showPct")
    block = src[idx:idx + 200]
    assert "useRealBar" in block
    assert "pct != null" in block


def test_drawer_pct_chip_updated_on_tick():
    """The per-tick render must also update the % chip text so
    a long-running op's drawer card reflects the current
    percentage. Without the tick update, the % would stay
    stuck at first-render value."""
    src = OPS_JS.read_text()
    # The live-update site uses the [data-op-counter-pct] attr.
    assert "data-op-counter-pct" in src


def test_drawer_pct_chip_css_rule_exists():
    """The `.op-card-counter-pct` CSS rule must exist so the
    chip renders with proper alignment + tone color."""
    src = OPS_CSS.read_text()
    assert ".op-card-counter-pct" in src, (
        "v1.18.51: ops.css must define .op-card-counter-pct"
    )
    # The chip carries the card's tone color. v1.19.88 centralized
    # the per-tone cyan/green overrides into the card's --ot custom
    # property, so the pct chip now reads `color: var(--ot, …)`
    # (same as .op-card-counter-current) instead of per-tone selectors.
    pct_anchor = src.index(".op-card-counter-pct {")
    pct_block = src[pct_anchor:pct_anchor + 250]
    assert "color: var(--ot" in pct_block, (
        "v1.18.51 + v1.19.88: the pct chip must take the card's tone "
        "color via --ot"
    )


# ── Bug C: KIND_LABEL / TONE_BY_KIND audit gaps ──────────────


def test_all_op_progress_kinds_have_drawer_labels():
    """Every kind accepted by the op_progress.kind CHECK
    constraint must have a KIND_LABEL entry so the drawer card
    header reads a friendly string instead of the raw
    snake-case kind."""
    db_src = DB_PY.read_text()
    # The CHECK list is multi-line — extract the kinds.
    # Pattern: CHECK (kind IN ('tdb_sync', 'plex_enum', ...))
    import re
    match = re.search(
        r"CHECK \(kind IN \(([^)]+)\)\)\s*,?\s*\n\s*status",
        db_src,
    )
    assert match, "Could not find op_progress.kind CHECK in db.py"
    kinds = re.findall(r"'([a-z_]+)'", match.group(1))
    assert len(kinds) >= 5, f"Expected several kinds, got {kinds}"

    ops_src = OPS_JS.read_text()
    # Locate KIND_LABEL object.
    kl_idx = ops_src.index("const KIND_LABEL = {")
    kl_end = ops_src.index("};", kl_idx)
    kl_body = ops_src[kl_idx:kl_end]

    missing = []
    for k in kinds:
        if f"{k}:" not in kl_body and f"{k} :" not in kl_body:
            missing.append(k)
    assert not missing, (
        f"v1.18.51: KIND_LABEL missing entries for op_progress "
        f"kinds: {missing}"
    )


def test_all_op_progress_kinds_have_tones():
    """Same audit but for TONE_BY_KIND — without a tone the
    drawer card loses its color identity."""
    db_src = DB_PY.read_text()
    import re
    match = re.search(
        r"CHECK \(kind IN \(([^)]+)\)\)\s*,?\s*\n\s*status",
        db_src,
    )
    assert match
    kinds = re.findall(r"'([a-z_]+)'", match.group(1))

    ops_src = OPS_JS.read_text()
    tone_idx = ops_src.index("const TONE_BY_KIND = {")
    tone_end = ops_src.index("};", tone_idx)
    tone_body = ops_src[tone_idx:tone_end]

    missing = []
    for k in kinds:
        if f"{k}:" not in tone_body and f"{k} :" not in tone_body:
            missing.append(k)
    assert not missing, (
        f"v1.18.51: TONE_BY_KIND missing entries for op_progress "
        f"kinds: {missing}"
    )


def test_bulk_probe_tdb_label_added():
    """bulk_probe_tdb specifically — pre-fix it had a priority
    but no label/tone."""
    src = OPS_JS.read_text()
    assert "bulk_probe_tdb:    'BULK PROBE TDB'" in src \
        or "bulk_probe_tdb: 'BULK PROBE TDB'" in src


def test_bulk_lps_label_added():
    """bulk_lps specifically — pre-fix it was missing from
    all three maps."""
    src = OPS_JS.read_text()
    assert "bulk_lps:       'BULK LET PLEX SERVE'" in src \
        or "bulk_lps: 'BULK LET PLEX SERVE'" in src


def test_bulk_lps_priority_added():
    """bulk_lps needs an OP_MINI_PRIORITY entry so it surfaces
    in the topbar mini-bar when a user fires the bulk action."""
    src = OPS_JS.read_text()
    pri_idx = src.index("const OP_MINI_PRIORITY = {")
    pri_end = src.index("};", pri_idx)
    pri_body = src[pri_idx:pri_end]
    assert "bulk_lps:" in pri_body, (
        "v1.18.51: bulk_lps must be in OP_MINI_PRIORITY"
    )


# ── version marker pin ───────────────────────────────────────


def test_app_js_carries_v1_18_51_marker():
    src = APP_JS.read_text()
    assert "v1.18.51" in src


def test_ops_js_carries_v1_18_51_marker():
    src = OPS_JS.read_text()
    assert "v1.18.51" in src
