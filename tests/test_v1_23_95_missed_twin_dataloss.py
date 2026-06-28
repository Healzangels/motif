"""v1.23.95 (audit) — two "missed twin" data-loss fixes from a silent-failure sweep.

The codebase had prior sweeps that closed a class on ONE code path but left its
sibling open:

(1) plex.enumerate_collections_for_section was missing the v1.23.64 empty-page
    guard that its twin enumerate_section_items got. A transient EMPTY Metadata
    page mid-walk (totalSize says more remain) fell through to a silent `break`,
    returning a TRUNCATED collection list → the v1.18.89 reaper deleted the unseen
    live collection plex_items rows (under its 20%/50-row abort guard). Now it
    raises PlexParseError like the items walk, so plex_enum counts a section error
    + skips the reaper.

(2) downloader.download_theme's FRESH return path had no 0-byte guard — only the
    pre-existing-file branch (v1.22.30) checked size>0. A fresh yt-dlp run whose
    ffmpeg postproc emitted an empty theme.mp3 without raising returned a
    SUCCESSFUL DownloadResult (file_size=0) → the worker hardlinked an empty theme
    into Plex behind a green check (`if not expected_mp3.exists()` only catches a
    MISSING file, not a present-but-empty one). Now the fresh path raises
    DownloadError on a 0-byte result so the retry ladder re-runs.
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from app.core.plex import PlexClient, PlexConfig, PlexParseError

REPO = Path(__file__).resolve().parent.parent
DOWNLOADER_PY = (REPO / "app" / "core" / "downloader.py").read_text()


# ── (1) collections enum: empty mid-walk page raises, not truncates ──


def _coll_page(total: int, size: int, start: int) -> str:
    items = ",".join(
        '{"ratingKey": "%d", "type": "collection", "title": "C%d", "Guid": []}'
        % (i, i)
        for i in range(start, start + size)
    )
    return ('{"MediaContainer": {"totalSize": %d, "size": %d, "Metadata": [%s]}}'
            % (total, size, items))


class _Resp:
    status_code = 200

    def __init__(self, text: str) -> None:
        self.text = text


def _client():
    return PlexClient(PlexConfig(url="http://x", token="t",
                                 movie_section="1", tv_section="2"))


def test_collections_empty_page_mid_section_raises_not_truncates():
    pages = [
        _coll_page(total=10, size=5, start=0),
        # page 2: EMPTY while totalSize still advertises 10 → truncation.
        '{"MediaContainer": {"totalSize": 10, "size": 0, "Metadata": []}}',
    ]
    calls: list = []

    def _capture_get(self, path, **kwargs):
        idx = len(calls)
        calls.append(path)
        assert idx < len(pages), "walk fetched more pages than seeded"
        return _Resp(pages[idx])

    with patch.object(PlexClient, "_get", _capture_get):
        with pytest.raises(PlexParseError):
            _client().enumerate_collections_for_section(section_id="1")
    assert len(calls) == 2, (
        f"must fetch page 1 then the empty page 2, then raise "
        f"(made {len(calls)} calls)")


def test_collections_complete_walk_still_succeeds():
    """Control: a clean two-page walk reaching totalSize must NOT raise."""
    pages = [_coll_page(10, 5, 0), _coll_page(10, 5, 5)]
    calls: list = []

    def _capture_get(self, path, **kwargs):
        idx = len(calls)
        calls.append(path)
        return _Resp(pages[min(idx, len(pages) - 1)])

    with patch.object(PlexClient, "_get", _capture_get):
        out = _client().enumerate_collections_for_section(section_id="1")
    assert len(out) == 10
    assert len(calls) == 2


# ── (2) downloader fresh-path 0-byte guard ──


def test_downloader_fresh_zero_byte_guard():
    """The FRESH return path must reject a 0-byte produced file (raise
    DownloadError), distinct from the v1.22.30 pre-existing 'stale 0-byte' guard."""
    fn = DOWNLOADER_PY.index("def download_theme(")
    body = DOWNLOADER_PY[fn:]
    # the fresh return now checks the produced size and raises on 0.
    assert "_fresh_size = expected_mp3.stat().st_size" in body
    assert "if _fresh_size == 0:" in body
    assert 'raise DownloadError(' in body
    # the final DownloadResult uses the validated size, not a re-stat.
    assert "file_size=_fresh_size," in body


def test_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
