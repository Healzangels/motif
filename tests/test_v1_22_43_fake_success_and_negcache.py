"""v1.22.43 (holistic audit) — fake-success + transient-error-poisons-cache.

A4 api_override (SET URL via the override dialog): dropped _enqueue_download's
count and returned ok:True unconditionally, so SET URL on an item no included
Plex section owns saved the override but queued NO download behind a green
toast (fake-success — mirror of the v1.22.16 ACCEPT UPDATE #5 fix).

N1 tmdb._lookup_by_tvdb / _lookup_by_imdb returned None on a transient non-200
(5xx / 429). _cached_or_fetch then cached that None as a 7-day NEGATIVE result,
so a momentary TMDB outage silently killed AniDB resolution for a week. Now
they RAISE TMDBError (like _search) — caught → None, NOT cached.

N2 notify.dispatch_coalesced: on the leading edge it set send_single, then armed
the timer INSIDE the lock; if _arm_coalesce_timer raised, the except reset the
window and returned early — dark-holing the user's first-of-burst notification.
Now it falls through to dispatch the leading-edge single.
"""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from app.core.db import init_db
from app.core.tmdb import TMDBClient, TMDBError

REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()
TMDB_PY = (REPO / "app" / "core" / "tmdb.py").read_text()


# ── N1: transient TMDB error must not poison the negative cache ──


def _match_response():
    r = MagicMock(status_code=200)
    r.json.return_value = {
        "movie_results": [], "tv_results": [
            {"id": 777, "name": "X", "first_air_date": "2001-01-01"}],
        "person_results": [], "tv_episode_results": [],
        "tv_season_results": [],
    }
    return r


def test_lookup_by_tvdb_raises_on_transient_500():
    assert "def _lookup_by_tvdb" in TMDB_PY
    idx = TMDB_PY.index("def _lookup_by_tvdb")
    body = TMDB_PY[idx:idx + 1400]
    assert "raise TMDBError(f\"tvdb find returned HTTP" in body, (
        "v1.22.43: _lookup_by_tvdb must RAISE on non-200 (not return None) so "
        "a transient error isn't cached as a 7-day negative")
    assert "return None" not in body[body.index("status_code != 200"):
                                     body.index("status_code != 200") + 120]


def test_lookup_by_imdb_raises_on_transient_500():
    idx = TMDB_PY.index("def _lookup_by_imdb")
    body = TMDB_PY[idx:idx + 1200]
    assert "raise TMDBError(f\"imdb find returned HTTP" in body, (
        "v1.22.43: _lookup_by_imdb must RAISE on non-200")


def test_transient_tvdb_error_not_cached_negative(tmp_path):
    """A 500 → None (caught) but NOT cached; the very next call re-fetches and,
    on a 200 with a match, succeeds. Pre-fix the None was cached for 7 days."""
    db = tmp_path / "motif.db"
    init_db(db)
    client = TMDBClient(api_key="k", db_path=db)
    err = MagicMock(status_code=500)
    with patch("app.core.tmdb.httpx.get",
               side_effect=[err, _match_response()]) as mock_get:
        first = client.lookup_by_tvdb(424242, "tv")   # transient 500
        second = client.lookup_by_tvdb(424242, "tv")  # must RE-fetch
    assert first is None
    assert second is not None and second["tmdb_id"] == 777
    assert mock_get.call_count == 2, (
        "v1.22.43: a transient 500 must NOT poison the cache — the 2nd call "
        f"must re-hit the API; got {mock_get.call_count} calls")


def test_raw_lookup_by_tvdb_raises(tmp_path):
    db = tmp_path / "motif.db"
    init_db(db)
    client = TMDBClient(api_key="k", db_path=db)
    with patch("app.core.tmdb.httpx.get",
               return_value=MagicMock(status_code=503)):
        with pytest.raises(TMDBError):
            client._lookup_by_tvdb(1, "tv")
        with pytest.raises(TMDBError):
            client._lookup_by_imdb("tt1")


# ── N2: leading-edge single survives an arm-timer failure ───────


def test_bulk_item_buffered_even_if_arm_raises():
    """v1.23.46: if the trailing-timer arm fails on a BULK item, the item is
    still BUFFERED (not dark-holed) — the next bulk event re-arms + flushes.
    The single-action path never arms a timer, so it's immune by construction."""
    from app.core import notify as n
    n._COALESCE_BUF.clear()
    n._COALESCE_TIMERS.clear()
    n._COALESCE_CFG.clear()  # v0.51.257: replaced the write-only ACTIVE flag
    sent = []

    def _capture(db, cfg, *, event_kind, title, body, body_format="text", **kw):
        sent.append(title)

    cfg = SimpleNamespace(
        events={"theme_pushed": True}, apprise_urls=["discord://x/y"],
        apprise_external_url="")
    with patch.object(n, "dispatch", _capture), \
            patch.object(n, "_arm_coalesce_timer",
                         side_effect=RuntimeError("Timer.start failed")):
        n.dispatch_coalesced(
            Path("/x"), cfg, event_kind="theme_pushed", item_label="T1",
            single_title="📤 pushed T1", single_body="b",
            batch_title_fn=lambda nn: "batch",
            batch_body_fn=lambda labels: "body",
            body_format="markdown", window_seconds=3600, bulk=True)
    # Buffered, not lost — the arm failure was logged but didn't pop the buffer.
    assert sent == [], "bulk items don't send immediately"
    buf = n._COALESCE_BUF.get("theme_pushed")
    assert buf and buf[0]["title"] == "📤 pushed T1", (
        f"the bulk item must survive an arm failure in the buffer — got {buf!r}")


def test_single_action_dispatches_immediately_no_arm():
    """v1.23.46: a SINGLE action dispatches the rich single and returns BEFORE
    the timer-arm path — so an arm failure cannot affect it (no leading-edge
    dark-hole class remains)."""
    from app.core import notify as n
    src = (REPO / "app" / "core" / "notify.py").read_text()
    # the not-bulk branch dispatches then returns, ahead of any arm.
    i = src.index("if not bulk:")
    block = src[i:i + 400]
    assert "dispatch(db_path, notifications" in block
    assert "\n        return\n" in block, "single path returns before arming"
    assert block.index("dispatch(db_path") < block.index("\n        return\n")


# ── A4: api_override SET URL surfaces a 0-enqueue ───────────────


# v0.51.251: test_api_override_captures_enqueue_count_and_warns_on_zero
# removed with its subject — the dead api_override endpoint is gone. The
# live SET URL flow's 0-enqueue guard is pinned by the manual-url tests.
