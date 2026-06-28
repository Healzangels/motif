"""v1.14.70 — dashboard rework: split rows by concept + add PLEX ANIME + rename ORPHANS.

the user v1.14.67 follow-up:

> "I think the dashboard could use some work. The Queue
>  pending, Running Now, Failures don't really make sense in
>  the row where they are as they lack context. Also it would
>  be good to see all the plex libraries instead of just Plex
>  Movies and Plex TV. Also Orphans...sound weird in the
>  context, I think it would make more sense if it was
>  something like user provided or something similar since
>  thats what they really are once they have a theme,
>  adopted, user provided it is themed and oprhaned doesn't
>  really make that clear and sound like it could be
>  something that needs to be fixed."

The dashboard's pre-fix layout collapsed two unrelated
concepts (theme coverage + queue operations) into the top
row, and the PLEX COVERAGE row mixed Plex-side coverage with
filesystem-realization stats. v1.14.70 splits each concept
into its own labeled section:

  1. COVERAGE  (TDB-side: MOVIES TRACKED, TV SERIES TRACKED)
  2. OPERATIONS (queue/running/failures, mini cards)
  3. PLEX LIBRARY (PLEX MOVIES, PLEX TV, PLEX ANIME — new
     anime card aggregates from /api/sections/coverage)
  4. STORAGE (HARDLINKS, COPIES, USER PROVIDED — last card
     renamed from ORPHANS to convey "themed by user, not in
     TDB" instead of the alarming "orphan" framing).

Element ids stay stable so existing JS write paths and
dashboard-customize state keep working.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
DASH = REPO / "app" / "web" / "templates" / "dashboard.html"
JS = REPO / "app" / "web" / "static" / "app.js"
CSS = REPO / "app" / "web" / "static" / "app.css"


# ── Section reorg: 4 labeled sections ─────────────────────────


def _dash_html() -> str:
    return DASH.read_text()


def test_top_stats_section_relabelled_to_coverage():
    """v1.14.70 narrowed the original "TOP STATS" catch-all label
    to "COVERAGE". v1.15.27 dropped the TDB MOVIES/TV cards
    entirely and replaced the section with ACTIVITY (themes-added
    + a single TDB CATALOG reference card). The remaining
    historical guard: the pre-v1.14.70 "TOP STATS" name must
    stay gone."""
    html = _dash_html()
    assert "TOP STATS" not in html
    # Note: data-dash-label="COVERAGE" is no longer asserted —
    # v1.15.27 retired the COVERAGE section in favor of ACTIVITY.


def test_operations_section_exists_with_three_mini_stats():
    """QUEUE PENDING / RUNNING NOW / FAILURES moved into a
    dedicated OPERATIONS section. Each is a `stat-mini` card
    powered by /api/stats queue counts (data-stat hooks
    unchanged so the JS write path doesn't need updating)."""
    html = _dash_html()
    assert 'data-dash-section="operations"' in html
    assert 'data-dash-label="OPERATIONS"' in html
    # All three mini cards are inside the OPERATIONS section.
    ops_start = html.index('data-dash-section="operations"')
    next_section = html.index('data-dash-section="', ops_start + 1)
    ops_block = html[ops_start:next_section]
    assert 'data-stat="queue.pending"' in ops_block
    assert 'data-stat="queue.running"' in ops_block
    assert 'data-stat="queue.failed"' in ops_block


def test_top_stats_no_longer_contains_queue_stats():
    """Regression guard: the queue mini-cards (PENDING / RUNNING /
    FAILURES) must live in their own OPERATIONS section, not
    bundled with the leading dashboard row. v1.14.70 split them
    out; v1.15.27 replaced the leading row with ACTIVITY but the
    invariant — queue stats live in OPERATIONS — is unchanged."""
    html = _dash_html()
    activity_start = html.index('data-dash-section="activity"')
    activity_end = html.index('</section>', activity_start)
    activity_block = html[activity_start:activity_end]
    assert 'data-stat="queue.pending"' not in activity_block
    assert 'data-stat="queue.running"' not in activity_block
    assert 'data-stat="queue.failed"' not in activity_block


def test_plex_coverage_section_relabelled_to_plex_library():
    """The pre-fix label was PLEX COVERAGE; v1.14.70 renames
    it PLEX LIBRARY because (a) it's the "Plex's view of your
    library" concept, and (b) the COVERAGE label is now owned
    by the top section (TDB-side coverage). Keeps the labels
    distinct in the customize toolbar."""
    html = _dash_html()
    assert 'data-dash-section="plex-coverage"' in html
    assert 'data-dash-label="PLEX LIBRARY"' in html


def test_storage_section_exists_separately():
    """v1.14.70: HARDLINKS / COPIES / USER PROVIDED moved out
    of the PLEX LIBRARY row into their own STORAGE section.
    They're filesystem-realization stats — separate concept
    from Plex's view of the library."""
    html = _dash_html()
    assert 'data-dash-section="storage"' in html
    assert 'data-dash-label="STORAGE"' in html
    storage_start = html.index('data-dash-section="storage"')
    storage_end = html.index('</section>', storage_start)
    storage_block = html[storage_start:storage_end]
    assert 'id="storage-hardlinks"' in storage_block
    assert 'id="storage-copies"' in storage_block
    assert 'id="orphan-count"' in storage_block  # id stable; label changed


# ── PLEX ANIME card ───────────────────────────────────────────


def test_plex_anime_card_present_and_initially_hidden():
    """The PLEX ANIME card is added to the PLEX LIBRARY section
    next to PLEX MOVIES + PLEX TV. Initially hidden so single-
    purpose installs (no anime sections) don't see an empty
    0/0 card; JS reveals it when /api/sections/coverage
    returns at least one is_anime=1 row."""
    html = _dash_html()
    assert 'id="plex-anime-card"' in html
    # The placeholder cells follow the same shape as PLEX TV.
    # v1.15.27 renamed plex-anime-total → plex-anime-total-chip
    # for its coverage-% layout; v1.15.31 reverted to the pre-
    # v1.15.27 layout (total as big number) and the original
    # `plex-anime-total` ID is back.
    assert 'id="plex-anime-total"' in html
    # v1.23.41: foot reframed to ThemerrDB reach (in-TDB / not-in-TDB);
    # the with-theme stat moved up to the COVERAGE row.
    assert 'id="plex-anime-not-tdb"' in html
    # Initially hidden — JS reveals when anime sections exist.
    # v1.15.32: a new data-dash-card attr can sit between the id
    # and style attrs, so anchor them as separate substrings on
    # the same article element rather than as a single literal.
    anime_anchor = html.index('id="plex-anime-card"')
    anime_tag_end = html.index('>', anime_anchor)
    anime_tag = html[anime_anchor:anime_tag_end]
    assert 'style="display:none"' in anime_tag, (
        "PLEX ANIME card must default to style=\"display:none\""
    )


def test_render_plex_anime_card_function_aggregates_anime_sections():
    """v1.23.90: the PLEX ANIME card is populated in renderPlexCoverage from
    /api/coverage/plex's `anime` array (movie+show in anime sections, matching
    the ANIME tab) — the per-section renderPlexAnimeCard sum is retired. It still
    writes total + motif_available + the not-in-TDB reach and hides on 0."""
    js = JS.read_text()
    assert "function renderPlexAnimeCard(" not in js
    anchor = js.index("const animeItems = data.anime")
    block = js[anchor:anchor + 900]
    assert "animeItems.length" in block
    assert "animeItems.filter((m) => m.motif_available).length" in block
    assert "$('#plex-anime-total')" in block
    assert "$('#plex-anime-not-tdb')" in block
    # hide-when-empty / show-when-present, in one ternary now.
    assert "animeCard.style.display = animeTotal > 0 ? '' : 'none'" in block


def test_render_plex_anime_card_called_from_load_dashboard():
    """v1.23.90: the PLEX ANIME card no longer needs a dedicated call — it's
    populated alongside PLEX MOVIES/TV inside renderPlexCoverage (fed by
    /api/coverage/plex), so the retired renderPlexAnimeCard call is gone."""
    js = JS.read_text()
    assert "renderPlexAnimeCard(" not in js  # no def, no dangling call
    assert "const animeItems = data.anime" in js


# ── ORPHANS → USER PROVIDED rename ────────────────────────────


def test_orphans_card_label_renamed_user_provided():
    """The visible label changed from ORPHANS to USER PROVIDED.
    Element id (`orphan-count`) stays so the JS write path is
    untouched. The new label is paired with a muted footer
    explaining "items themed by you, not in TDB" so the user
    sees what the count means."""
    html = _dash_html()
    # Find the storage section and inspect the orphan-count card.
    storage_start = html.index('data-dash-section="storage"')
    storage_end = html.index('</section>', storage_start)
    storage_block = html[storage_start:storage_end]
    # New label.
    assert "USER PROVIDED" in storage_block
    # Old label must not survive on this card. (Other places in
    # the codebase like the JS comment "Orphans count" or the
    # SQL alias `orphans_total` use the technical term — fine.)
    assert "ORPHANS" not in storage_block
    # Explanatory footer.
    assert "items themed by you, not in TDB" in storage_block
    # Element id stable.
    assert 'id="orphan-count"' in storage_block


# ── Customize-mode integration: each new section is moveable ──


def test_each_new_section_carries_data_dash_attributes():
    """The dashboard-customize JS keys on `data-dash-section`
    (stable layout id) + `data-dash-label` (display name).
    Both new sections (operations, storage) must carry both
    attributes so they show up in the customize toolbar with
    the right label and remember their position."""
    html = _dash_html()
    for sid, label in [
        ("operations", "OPERATIONS"),
        ("storage", "STORAGE"),
    ]:
        assert f'data-dash-section="{sid}"' in html
        assert f'data-dash-label="{label}"' in html


# ── Grid CSS now auto-fits each row's card count ───────────────


def test_grid_stats_uses_auto_fit_columns():
    """The pre-fix `.grid-stats` was hard-coded to
    `2fr 2fr 1fr 1fr 1fr` — assumed 5-card 2-large+3-mini per
    row. The new rows have varying card counts (2/3/3/3) so
    the grid uses `repeat(auto-fit, minmax(...))` to flow N
    cards naturally instead of forcing 5."""
    css = CSS.read_text()
    assert "repeat(auto-fit, minmax(200px, 1fr))" in css
    # The hard-coded 2fr 2fr 1fr 1fr 1fr line must not survive.
    assert "grid-template-columns: 2fr 2fr 1fr 1fr 1fr;" not in css
