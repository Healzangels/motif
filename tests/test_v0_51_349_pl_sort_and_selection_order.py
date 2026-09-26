"""v0.51.349 — sort=pl ranks by the dot the row paints, and SELECT ALL FILTERED takes the view's order.

Two leftovers the v0.51.346 reviewers logged, neither blocking then:

* `sort=pl`'s amber rank took any row with a canonical and no placement, so a backup_only or over-ceiling row sorted
  among the amber "awaiting" rows while its own dot is gray and PL=off lists it (v0.51.346 / v0.51.347 settled that a
  terminal reason is not awaiting). The v1.23.24/.25 invariant is "rank by the dot you paint".
* `buildLibraryFilterParams` never sent sort / sort_dir, so the one selection request answered in title order whatever
  the list showed. For a title held twice the stored row was then the wrong one, and EXPORT CSV wrote its own order.

The PL ranking is checked against renderLibraryRow's own `pl` derivation run under node, so the sort cannot drift from
the dot again; the selection order is checked against the paged walk of the same view.
"""
from __future__ import annotations

import pytest

from test_v0_51_346_library_selection import _drive, _get, _rebuilt, _selection, lib, needs_node  # noqa: F401


def _walk_rks(lib, **params):
    rows, page = [], 1
    while True:
        body = _get(lib, page=page, per_page=5, **params)
        if not body["items"]:
            return rows
        rows += [it["rating_key"] for it in body["items"]]
        page += 1


@needs_node
@pytest.mark.parametrize("sort_dir", ["asc", "desc"])
def test_pl_sort_groups_every_row_with_the_dot_its_own_row_paints(lib, tmp_path, sort_dir):
    from test_v0_51_346_library_semantics import _row_display_states  # the real renderLibraryRow derivations

    rows = []
    page = 1
    while True:
        body = _get(lib, tab="movies", page=page, per_page=50, sort="pl", sort_dir=sort_dir)
        if not body["items"]:
            break
        rows += body["items"]
        page += 1
    assert len(rows) > 5
    shown = _row_display_states(rows)
    # What this tag fixes: the amber "awaiting placement" rows are ONE block, and the rows whose dot is gray because
    # their placement ended for good (a backup, an over-ceiling theme) or because Plex serves its own (a stale upload
    # on an LPS row) are not inside it. Rows are otherwise left alone: a dead upload with no canonical ranks in the
    # broken group, and a row with a job in flight hides its own await until the job ends — the sort would need the
    # per-row job subquery inside ORDER BY to see that, which is the cost v0.51.345 spent the tag removing.
    dots = [shown[it["rating_key"]]["pl"] for it in rows]
    amber = [i for i, d in enumerate(dots) if d == "await"]
    assert amber, "premise: the seed paints amber PL dots"
    assert amber == list(range(amber[0], amber[-1] + 1)), \
        f"the amber rows must not be split by other states ({sort_dir}): {dots}"
    block = {rows[i]["rating_key"] for i in amber}
    terminal = {it["rating_key"] for it in rows
                if it["last_place_attempt_reason"] in ("backup_only", "plex_rejected:over_ceiling")
                and not it["media_folder"] and it["placement_kind"] != "plex_upload"}
    lps_stale = {it["rating_key"] for it in rows if it.get("needs_repush") and it["plex_independent_theme"] == 1}
    assert terminal and lps_stale, "premise: the seed has both kinds this tag moves"
    assert not (terminal & block), sorted(terminal & block)
    assert not (lps_stale & block), sorted(lps_stale & block)
    assert {shown[rk]["pl"] for rk in terminal | lps_stale} == {""}, \
        {rk: shown[rk]["pl"] for rk in terminal | lps_stale}


def _stale_pu_lps_row(lib, rk, title, tmdb):
    """A stale plex_upload on an LPS row: RE-PUSH is stale, but Plex serves its own, so the dot is gray."""
    import contextlib
    import sqlite3
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    rel = f"movies/{rk}.mp3"
    (lib.themes / "movies").mkdir(parents=True, exist_ok=True)
    (lib.themes / rel).write_bytes(b"canonical")
    with contextlib.closing(sqlite3.connect(lib.db)) as c, c:
        c.execute("INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source, last_seen_sync_at,"
                  " first_seen_sync_at, youtube_url) VALUES (?, 'movie', ?, ?, 'imdb', ?, ?, ?)",
                  (tmdb, tmdb, title, now, now, "https://www.youtube.com/watch?v=stale"))
        c.execute("INSERT INTO plex_items (rating_key, section_id, media_type, theme_id, guid_tmdb, title,"
                  " edition_key, folder_path, has_theme, local_theme_file, plex_independent_theme,"
                  " plex_theme_verified_ok, first_seen_at, last_seen_at)"
                  " VALUES (?, '1', 'movie', ?, ?, ?, '', ?, 1, 0, 1, 1, ?, ?)",
                  (rk, tmdb, tmdb, title, f"/nonexistent/{rk}", now, now))
        c.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, edition_key, file_path, downloaded_at,"
                  " source_video_id, provenance, source_kind) VALUES ('movie', ?, '1', '', ?, ?, ?, 'auto', 'themerrdb')",
                  (tmdb, rel, now, f"vid{tmdb}"))
        # theme_present=0 with a stored rating_key nothing live carries = the v1.24.40 "genuinely dead upload" shape
        c.execute("INSERT INTO placements (media_type, tmdb_id, section_id, edition_key, media_folder, placed_at,"
                  " placement_kind, plex_rating_key, plex_refreshed, theme_present)"
                  " VALUES ('movie', ?, '1', '', '', ?, 'plex_upload', ?, 1, 0)", (tmdb, now, f"dead-{rk}"))


@needs_node
def test_a_stale_upload_that_plex_serves_itself_sorts_with_the_gray_rows(lib, tmp_path):
    # its title sits between two amber rows on purpose: ranked amber it lands INSIDE their block, which is what the
    # v1.24.36 stale-upload pin did to an LPS row until this tag added the plex_independent_theme guard
    _stale_pu_lps_row(lib, "z1", "Title 08a", 7708)
    rows, page = [], 1
    while True:
        body = _get(lib, tab="movies", page=page, per_page=50, sort="pl", sort_dir="asc")
        if not body["items"]:
            break
        rows += body["items"]
        page += 1
    from test_v0_51_346_library_semantics import _row_display_states
    shown = _row_display_states(rows)
    assert shown["z1"]["pl"] == "", "premise: Plex serves its own, so the row paints gray"
    order = [it["rating_key"] for it in rows]
    amber = [i for i, rk in enumerate(order) if shown[rk]["pl"] == "await"]
    assert amber, "premise: the seed paints amber dots"
    assert not (amber[0] < order.index("z1") < amber[-1]), \
        f"a gray stale-upload row sorted inside the amber block: {[(rk, shown[rk]['pl']) for rk in order]}"


@needs_node
@pytest.mark.parametrize("sort,sort_dir", [("pl", "asc"), ("year", "desc"), ("attention", "asc"), ("title", "desc")])
def test_select_all_filtered_takes_the_views_own_order(lib, tmp_path, sort, sort_dir):
    state = {"tab": "movies", "sort": sort, "sortDir": sort_dir}
    steps = [{"op": "state", "tab": "movies", "set": state},
             {"op": "click", "id": "library-select-all-filtered-btn"}]
    (clicked,) = _drive(tmp_path, "movies", steps)["steps"][1:]
    (url,) = [q["url"] for q in clicked["requests"] if q["url"].startswith("/api/library?")]
    assert f"sort={sort}" in url or sort == "title", url
    assert f"sort_dir={sort_dir}" in url or sort_dir == "asc", url
    body = _selection(lib, tab="movies", sort=sort, sort_dir=sort_dir)
    assert [it["rating_key"] for it in _rebuilt(body)] == _walk_rks(lib, tab="movies", sort=sort, sort_dir=sort_dir)


@needs_node
def test_the_default_view_still_sends_no_sort(lib, tmp_path):
    # title ascending is the default on both sides; sending it would be noise in every request
    steps = [{"op": "state", "tab": "movies", "set": {"tab": "movies", "sort": "title", "sortDir": "asc"}},
             {"op": "click", "id": "library-select-all-filtered-btn"}]
    (clicked,) = _drive(tmp_path, "movies", steps)["steps"][1:]
    (url,) = [q["url"] for q in clicked["requests"] if q["url"].startswith("/api/library?")]
    assert "sort=" not in url and "sort_dir=" not in url, url
