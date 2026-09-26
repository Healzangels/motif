"""v0.51.348 — bulk PUSH stops offering rows Plex will refuse.

A row's own PL dot has excluded the two TERMINAL place reasons for a while: backup_only since v0.51.36 (a backup
defers to Plex on purpose) and plex_rejected:over_ceiling since v0.51.347 (Plex 500s over ~10MB — CLAUDE.md class 11).
The three bulk-PUSH predicates never read the reason at all, so the // PUSH TO PLEX (N) badge counted those rows and
the click scooped them into its candidates: a push that cannot land, reported per item as a failure.

Driven through the real app.js under node (the v0.51.346 page harness): the bulk bar's count, the button's (N) badge
and the click's own requests, for rows selected on the page AND for rows that arrived through one SELECT ALL request.
"""
from __future__ import annotations

import pytest

from test_v0_51_346_library_selection import (  # the real page harness: app.js in a vm with fake DOM + fetch
    _drive, _rebuilt, _selection, lib, needs_node,  # noqa: F401  (lib is a fixture)
)

PUSH_BTN = "library-push-selected-btn"

# one row per state, all themed (upstream_source imdb, theme_tmdb set) and all "downloaded, not placed"
BASE = {"section_id": "1", "plex_media_type": "movie", "folder_path": "/m", "edition_key": "", "plex_has_theme": 0,
        "plex_local_theme": 0, "plex_theme_verified_ok": 1, "plex_independent_theme": 0, "theme_media_type": "movie",
        "youtube_url": "https://www.youtube.com/watch?v=x", "failure_kind": None, "failure_acked_at": None,
        "upstream_source": "imdb", "tdb_dropped_at": None, "file_path": "movies/x.mp3", "source_video_id": "x",
        "source_kind": "themerrdb", "mismatch_state": None, "media_folder": None, "placement_kind": None,
        "placement_provenance": None, "job_in_flight": None, "pending_update": 0, "pending_update_kind": None,
        "canonical_missing": False}


def _row(rk, tmdb, reason):
    return dict(BASE, rating_key=rk, plex_title=f"Row {rk}", theme_tmdb=tmdb, last_place_attempt_reason=reason)


ROWS = [_row("p1", 101, None),                             # plain awaiting — the only pushable row
        _row("p2", 102, "link_failed: no space left"),      # a RETRYABLE failure is still awaiting
        _row("t1", 201, "backup_only"),                     # terminal: deferring to Plex on purpose
        _row("t2", 202, "plex_rejected:over_ceiling")]      # terminal: Plex refuses the size


def _bar(tmp_path, rows):
    steps = [{"op": "state", "set": {"tab": "movies"}}, {"op": "select", "rows": "rows"}, {"op": "ui"},
             {"op": "select", "rows": "rows"}, {"op": "click", "id": PUSH_BTN}]
    out = _drive(tmp_path, "movies", steps, rowsets={"rows": rows})["steps"]
    bar = out[2]["bar"][PUSH_BTN]
    pushed = sorted(q["url"].split("/")[4] for q in out[4]["requests"] if "/replace" in q["url"])
    return bar, pushed, out[4]["confirms"]


@needs_node
def test_the_push_badge_and_the_click_skip_both_terminal_reasons(tmp_path):
    bar, pushed, confirms = _bar(tmp_path, ROWS)
    assert "(2)" in bar["text"], f"the badge must count only the two awaiting rows, not the terminal ones: {bar['text']}"
    assert pushed == ["101", "102"], f"the click must push only the awaiting rows: {pushed}"
    assert "201" not in pushed and "202" not in pushed
    # and the confirm says WHY two were left out, in words that fit a backup / an over-ceiling theme
    assert confirms and "2 skipped" in confirms[0] and "backup" in confirms[0] and "too large" in confirms[0], confirms


@needs_node
def test_a_selection_of_only_terminal_rows_offers_no_push(tmp_path):
    bar, pushed, _ = _bar(tmp_path, [r for r in ROWS if r["rating_key"].startswith("t")])
    assert bar["display"] == "none" or bar["disabled"] or "(0)" in bar["text"], bar
    assert pushed == [], f"nothing to push, so nothing may be sent: {pushed}"


@needs_node
def test_the_rows_one_select_all_request_returns_carry_the_reason(lib, tmp_path):
    # the server side of the same rule: a selected off-page row must carry last_place_attempt_reason, or the bulk
    # predicates read undefined and offer the push again
    body = _selection(lib, tab="movies")   # the harness fixture seeds the library itself
    assert "last_place_attempt_reason" in body["columns"]
    rows = _rebuilt(body)
    terminal = {it["rating_key"] for it in rows if it["last_place_attempt_reason"] in
                ("backup_only", "plex_rejected:over_ceiling")}
    assert terminal, "premise: the seeded library has terminal rows"
    steps = [{"op": "state", "set": {"tab": "movies"}}, {"op": "select", "rows": "rows"},
             {"op": "click", "id": PUSH_BTN}]
    out = _drive(tmp_path, "movies", steps, rowsets={"rows": rows})["steps"]
    pushed = {q["url"] for q in out[2]["requests"] if "/replace" in q["url"]}
    by_tmdb = {str(it["theme_tmdb"]): it["rating_key"] for it in rows}
    pushed_rks = {by_tmdb.get(u.split("/")[4]) for u in pushed}
    assert not (pushed_rks & terminal), sorted(pushed_rks & terminal)


@needs_node
@pytest.mark.parametrize("reason", ["backup_only", "plex_rejected:over_ceiling"])
def test_the_row_and_the_bulk_bar_agree_on_what_is_awaiting(tmp_path, reason):
    # the drift this tag closes: the row's own dot said "not awaiting" while the bulk bar still counted the row
    bar, pushed, _ = _bar(tmp_path, [_row("only", 900, reason)])
    assert pushed == [] and "(1)" not in bar["text"], (reason, bar["text"], pushed)
