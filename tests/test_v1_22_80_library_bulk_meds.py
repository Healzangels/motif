"""v1.22.80 (audit round 2, Batch C #9) — library bulk-action MEDs.

(1) Bulk REVERT MISMATCH collected edition_key into its targets but
only ever sent section_id — /revert's rating_key param (v1.21.73, the
per-row path always threads it) was never used, so on a multi-edition
section the backend's legacy un-scoped path restored/dropped the
override and reset pending-update state across SIBLING editions while
the summary read "1 REVERTED". Targets now carry the row's rk and the
URL threads both params.

(2) findItemForButton's single find() with an OR matched whichever
row came earlier in render order — a sibling edition/section with the
same (mt, tmdb) could beat the actual clicked row, mis-driving the
catalogued isPlexAgentRow confirm gate (the "Plex is already
supplying…" warning silently skipped where designed to fire, or a
spurious prompt) and the adopt-sidecar confirm copy. Now rk-exact
match first, (mt, id) fallback only after.

(3) 7th SRC-axis mirror-drift site (the exact v1.19.38 class): the
bulk LET PLEX SERVE handler's M-sidecar gate used bare
`!it.media_folder` while the lpsOnlyCount bucket uses the widened
placed predicate — `!''` is TRUE for plex_upload rows, so the handler
skipped rows the bucket counted: the button said (N), the click did
fewer (or "Nothing to LET PLEX SERVE"). Now both use
`!!media_folder || placement_kind === 'plex_upload'`. CLAUDE.md site
table gained the row.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


# ── (1) bulk REVERT MISMATCH edition scope ───────────────────


def test_bulk_revert_targets_carry_and_send_rating_key():
    i = APP_JS.index("v1.13.68: bulk REVERT MISMATCH")
    handler = APP_JS[i:i + 5000]
    assert "rating_key: it.rating_key || ''" in handler
    assert "_rvq.set('rating_key', t.rating_key)" in handler
    assert "_rvq.set('section_id', t.section_id)" in handler
    # The dead edition_key-collected-but-never-sent shape is gone.
    assert "edition_key: it.edition_key || ''" not in handler


# ── (2) findItemForButton rk-first ───────────────────────────


def test_find_item_for_button_prefers_rk_exact():
    i = APP_JS.index("function findItemForButton(btn)")
    body = APP_JS[i:i + 1400]
    rk_idx = body.index("items.find((it) => it.rating_key === rk)")
    fallback_idx = body.index("it.theme_media_type === mt")
    assert rk_idx < fallback_idx, (
        "v1.22.80: the rk-exact match must run BEFORE the (mt, id) "
        "fallback — render order must not pick a sibling edition"
    )
    # The old OR-combined single find must be gone.
    assert "(rk && it.rating_key === rk) ||" not in body


# ── (3) 7th SRC-axis site uses the widened placed predicate ──


def test_bulk_lps_msidecar_gate_mirrors_bucket():
    i = APP_JS.index("const _lpsPlaced = !!it.media_folder")
    block = APP_JS[i:i + 400]
    assert "it.placement_kind === 'plex_upload'" in block
    assert "it.plex_local_theme === 1 && !_lpsPlaced" in block
    # The bare-media_folder form must be gone from this gate.
    assert ("it.plex_local_theme === 1 && !it.media_folder"
            not in APP_JS), (
        "v1.22.80: bare !media_folder evaluates !'' as TRUE for "
        "plex_upload rows (the v1.19.38 class)"
    )


def test_claude_md_site_table_gained_the_seventh_row():
    md = (REPO / "CLAUDE.md").read_text()
    assert "Bulk LPS M-sidecar gate" in md
