"""v0.51.237 — an index entry with no `id` must not cause a FALSE tdb drop.

sync has three signals for "this run's catalog is untrustworthy":

  index_incomplete   a whole index page failed  -> skip the WHOLE sweep
  errored_by_mt      per-item fetch failed, id known -> exclude just those ids
  (nothing)          entry present but has no `id`   <- the gap

An id-less entry took a bare `continue`: no stats.errors (so the errors gate
passed) and no errored_by_mt entry (so nothing was excluded), while the item's
last_seen_sync_at was never refreshed. The full-walk sweep keys on exactly that
column, so a LIVE title got stamped tdb_dropped_at — SRC flips to – with the
gray TDB◌ pill, and its pending_updates row is deleted. The v1.21.38 false-drop
class through the one door v1.21.44's per-id exclusion cannot cover: with no id
there is nothing to exclude, so the only safe move is to skip detection (what
git_unresolved already does on the baseline-reset path).
"""
from __future__ import annotations

import inspect

import pytest

from app.core import sync
from app.core.db import get_conn, init_db


SRC = inspect.getsource(sync.run_sync)


# ── behavioural: drive the real fetch loop (harness mirrors v1.21.44's) ───

@pytest.fixture
def db_path(tmp_path):
    db = tmp_path / "m.db"
    init_db(db)
    return db


def _seed_theme(db, *, tmdb_id, last_seen, mt="movie"):
    with get_conn(db) as conn:
        conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, upstream_source, "
            "  last_seen_sync_at, first_seen_sync_at) "
            "VALUES (?, ?, ?, 'themoviedb', ?, ?)",
            (mt, tmdb_id, f"T{tmdb_id}", last_seen, last_seen))


def _drop(db, tmdb_id, mt="movie"):
    with get_conn(db) as conn:
        r = conn.execute(
            "SELECT tdb_dropped_at FROM themes WHERE media_type=? AND tmdb_id=?",
            (mt, tmdb_id)).fetchone()
    return r["tdb_dropped_at"] if r else "MISSING"


def test_an_idless_index_entry_does_not_false_drop_a_live_title(db_path, monkeypatch):
    """The whole point. An index entry arrives with no `id`. Pre-fix that was a
    bare continue — no error counted, nothing excluded — so the sweep ran and
    stamped every row whose last_seen was stale, including LIVE titles."""
    _seed_theme(db_path, tmdb_id=100, last_seen="2020-01-01T00:00:00Z")
    _seed_theme(db_path, tmdb_id=200, last_seen="2020-01-01T00:00:00Z")

    def fake_index(client, base_url, media_path):
        if media_path == "movies":
            # a malformed entry: present in the catalog, but unmappable
            return ([{"id": None, "imdb_id": None, "title": "T100"}], 0)
        return ([], 0)

    monkeypatch.setattr(sync, "_fetch_index", fake_index)
    monkeypatch.setattr(sync, "_fetch_item", lambda *a, **k: None)
    sync.run_sync(db_path, "http://fake.invalid", source="remote",
                  enqueue_downloads=False)

    assert _drop(db_path, 100) is None, (
        "a live title must not be stamped tdb_dropped_at because ONE index "
        "entry lost its id")
    # Deliberate and conservative: with an unmappable entry we cannot tell
    # which rows the run actually covered, so the WHOLE sweep stands down —
    # a real removal simply waits for the next clean run. Same trade-off
    # index_incomplete already makes; under-stamping is recoverable,
    # over-stamping destroys pending_updates.
    assert _drop(db_path, 200) is None, (
        "the sweep must stand down entirely, not stamp on partial knowledge")


def test_a_clean_run_still_stamps_a_real_drop(db_path, monkeypatch):
    """The suppression must be conditional — otherwise this fix would silently
    disable drop detection forever, which is the v1.21.38 starvation bug."""
    _seed_theme(db_path, tmdb_id=100, last_seen="2020-01-01T00:00:00Z")
    _seed_theme(db_path, tmdb_id=200, last_seen="2020-01-01T00:00:00Z")

    def fake_index(client, base_url, media_path):
        if media_path == "movies":
            return ([{"id": 100, "imdb_id": None}], 0)   # every entry mappable
        return ([], 0)

    monkeypatch.setattr(sync, "_fetch_index", fake_index)
    monkeypatch.setattr(
        sync, "_fetch_item",
        lambda *a, **k: {"id": 100, "youtube_theme_url": "https://youtu.be/x"})
    sync.run_sync(db_path, "http://fake.invalid", source="remote",
                  enqueue_downloads=False)

    assert _drop(db_path, 200) is not None, (
        "with no unmappable entries the sweep must still run and stamp a "
        "genuinely-removed title")


def test_idless_entry_is_counted_not_silently_skipped():
    i = SRC.index("if result is None:")
    j = SRC.index('if result[0] == "error":', i)
    block = SRC[i:j]
    assert "unmappable_entries += 1" in block, (
        "a bare `continue` leaves the entry invisible to BOTH drop-sweep guards")


def test_unmappable_entries_suppresses_the_drop_sweep():
    """It must gate the same branch index_incomplete does — per-id exclusion is
    impossible without an id, so the whole sweep has to stand down."""
    assert "elif index_incomplete or unmappable_entries:" in SRC, (
        "an unmappable entry must suppress drop detection like an incomplete "
        "index does")


def test_the_full_walk_sweep_is_the_one_being_guarded():
    """Anchor the guard to the branch that actually stamps drops off
    last_seen_sync_at, so this can't drift onto the git changeset detector
    (which keys on explicit deletions and is deliberately ungated)."""
    i = SRC.index("elif index_incomplete or unmappable_entries:")
    j = SRC.index("detection_ok = True", i)
    branch = SRC[i:j]
    assert "_detect_and_stamp_drops_full_walk" in branch, (
        "the suppressed branch must be the last_seen-based full-walk sweep")


def test_operator_can_tell_the_two_skip_reasons_apart():
    """Cold paths need the MORE explicit breadcrumb (v1.18.5/v1.18.7). Reusing
    the 'an index page failed' wording for an id-less entry would send the
    operator looking at the wrong upstream fault."""
    i = SRC.index("skipping drop detection")
    msg = SRC[i:i + 700]
    assert "index_incomplete else" in msg, "the message must branch on the cause"
    assert "no `id`" in msg


def test_a_fetch_error_still_uses_per_item_exclusion_not_the_big_hammer():
    """Regression lock on v1.21.44: a per-item error has a known id, so it must
    keep excluding just that id — never escalate to suppressing the sweep, which
    is what v1.21.38 did and what starved detection."""
    i = SRC.index('if result[0] == "error":')
    j = SRC.index("batch.append(result)", i)
    block = SRC[i:j]
    assert "errored_by_mt.setdefault(" in block
    assert "unmappable_entries" not in block, (
        "an error with a known id must NOT suppress the whole sweep")


def test_do_fetch_returns_none_only_for_the_idless_case():
    """The counter's premise. If _do_fetch grows another None return, the count
    stops meaning 'unmappable' and would suppress drop detection for unrelated
    reasons."""
    i = SRC.index("def _do_fetch(entry):")
    j = SRC.index("fetch_stage = ", i)
    body = SRC[i:j]
    assert body.count("return None") == 1, (
        "another `return None` would silently widen what suppresses the sweep")
    k = body.index("return None")
    assert "tmdb_id is None" in body[:k], "the only None return is the id-less guard"
