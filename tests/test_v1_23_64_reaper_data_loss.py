"""v1.23.64 — holistic-audit cluster F: reaper / enumeration data-loss.

Three independent silent-data-loss / dropped-signal paths in the Plex
enumeration + theme-loss reaper:

  #2/#3 (plex.py) — an EMPTY Metadata page returned mid-section (transient
    under load / mid-rescan) while totalSize indicates more items broke the
    pagination walk "successfully" with a TRUNCATED item list and NO error.
    The v1.18.89 reaper then treated the short set as authoritative and DELETED
    the unseen plex_items rows (the v1.22.29 short-page class through the
    empty-page door). Now raises PlexParseError so plex_enum counts a section
    error + skips the reaper instead of reaping live rows.

  #12 (plex_enum.py) — the still_p survivor check that SUPPRESSES a theme-lost
    notification queried ALL plex_items with no plex_sections.included gate, so
    a stale has_theme=1 row in a DISABLED/removed section masked a genuine loss
    in a managed section. Now joins plex_sections AND included=1.

  #11 (plex_enum.py) — a transient Apprise dispatch failure on a theme-lost
    notification was unrecoverable (the reaper already DELETED the source row,
    so no future enum re-detects the loss to retry). Now also persists a durable
    events row so the loss is surfaced in the LOGS UI even when the push fails.

(#14 snapshot-tarball truncation is deferred to its own tag — see the
v1.23.64 note in app/__init__.py.)
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from app.core.plex import PlexClient, PlexConfig, PlexParseError

REPO = Path(__file__).resolve().parent.parent
PLEX_PY = (REPO / "app" / "core" / "plex.py").read_text()
ENUM_PY = (REPO / "app" / "core" / "plex_enum.py").read_text()


# ── #2/#3: empty mid-section page raises instead of truncating ──


def test_empty_page_mid_section_raises_not_truncates():
    """Behavioral: page 1 returns 5 of totalSize=10, then page 2 comes back
    EMPTY (size=0, Metadata []) while offset(5) < totalSize(10). Pre-fix the
    walk broke 'successfully' with a TRUNCATED 5-item list → the reaper deleted
    the 5 unseen rows. Now it must raise PlexParseError so the caller fails the
    section instead of reaping live rows."""
    items = ",".join(
        '{"ratingKey": "%d", "type": "movie", "title": "T%d",'
        ' "year": 2020, "Guid": []}' % (i, i)
        for i in range(5)
    )
    pages = [
        '{"MediaContainer": {"totalSize": 10, "size": 5, "Metadata": ['
        + items + ']}}',
        # page 2: EMPTY but totalSize still advertises 10 → truncation.
        '{"MediaContainer": {"totalSize": 10, "size": 0, "Metadata": []}}',
    ]
    calls: list = []

    class _Resp:
        status_code = 200

        def __init__(self, text: str) -> None:
            self.text = text

    def _capture_get(self, path, **kwargs):
        idx = len(calls)
        calls.append(path)
        assert idx < len(pages), "walk fetched more pages than seeded"
        return _Resp(pages[idx])

    cfg = PlexConfig(url="http://x", token="t",
                     movie_section="1", tv_section="2")
    with patch.object(PlexClient, "_get", _capture_get):
        client = PlexClient(cfg)
        with pytest.raises(PlexParseError):
            client.enumerate_section_items(section_id="1", media_type="movie")
    assert len(calls) == 2, (
        f"must fetch page 1 then the empty page 2, then raise "
        f"(made {len(calls)} calls)"
    )


def test_legit_complete_walk_still_succeeds():
    """Control: a clean two-page walk that REACHES totalSize must NOT raise —
    the empty-page raise only fires when items genuinely remain."""
    def _page(start: int, n: int) -> str:
        items = ",".join(
            '{"ratingKey": "%d", "type": "movie", "title": "T%d",'
            ' "year": 2020, "Guid": []}' % (i, i)
            for i in range(start, start + n)
        )
        return ('{"MediaContainer": {"totalSize": 10, "size": %d,'
                ' "Metadata": [%s]}}' % (n, items))
    pages = [_page(0, 5), _page(5, 5)]
    calls: list = []

    class _Resp:
        status_code = 200

        def __init__(self, text: str) -> None:
            self.text = text

    def _capture_get(self, path, **kwargs):
        idx = len(calls)
        calls.append(path)
        return _Resp(pages[min(idx, len(pages) - 1)])

    cfg = PlexConfig(url="http://x", token="t",
                     movie_section="1", tv_section="2")
    with patch.object(PlexClient, "_get", _capture_get):
        client = PlexClient(cfg)
        out = client.enumerate_section_items(section_id="1",
                                             media_type="movie")
    assert len(out) == 10
    assert len(calls) == 2


# ── #12: still_p survivor check is gated on an INCLUDED section ──


def test_still_p_survivor_check_gates_on_included_section():
    i = ENUM_PY.index("still_p = conn.execute(")
    block = ENUM_PY[i:i + 700]
    assert "JOIN plex_sections ps" in block, (
        "v1.23.64: the still_p survivor check must join plex_sections"
    )
    assert "ps.included = 1" in block, (
        "v1.23.64: a survivor in a DISABLED section must not suppress the "
        "theme-lost notification"
    )
    # still matches via theme link OR direct guid (the v1.22.32 superset).
    assert "pi.guid_tmdb = ?" in block


# ── #11: durable events row when theme-lost dispatch fails ──


def test_theme_lost_dispatch_failure_writes_durable_event():
    i = ENUM_PY.index("dispatch failed for %s/%s")
    # window covers the except block + the new durable log_event.
    block = ENUM_PY[i:i + 2300]
    assert 'log_event(' in block, (
        "v1.23.64: a failed theme-lost dispatch must persist a durable events "
        "row (the reaper already deleted the source row — no retry is possible)"
    )
    assert 'component="plex_enum"' in block
    assert "already reaped" in block
