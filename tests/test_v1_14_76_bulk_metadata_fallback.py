"""v1.14.76 — bulk /library/metadata fetch for the show-folder fallback (perf #5).

the user v1.14.74 logs showed the show-folder fallback firing on
every show section enum (his Plex doesn't honor
includeLocations=1):

    section 23 — 1 show(s) missing folder_path
    section 3  — 1232 show(s) missing folder_path  (~10s)
    section 2  — 4126 show(s) missing folder_path  (~68s)

The pre-fix path used a 16-thread pool of per-rk
/library/metadata GETs. v1.14.76 switches to Plex's batch
endpoint — comma-separated rating_keys in one URL, one
MediaContainer back with N elements. 50 ids per batch, 4
concurrent batches.

For the user's 4126-item TV section that's:
  pre:  4126 HTTP GETs across 16 threads (~68s wall)
  post: ~83 batches × 50 ids → ~21 wall-clock batches with
        4-way concurrency (~single-digit seconds)

This test file pins the bulk method's behavior + the fallback
loop's switch to it.
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from app.core.plex import PlexClient, PlexConfig  # noqa: E402


# ── New bulk method exists + has the right shape ──────────────


def test_get_item_paths_bulk_method_exists():
    assert hasattr(PlexClient, "get_item_paths_bulk")


def test_get_item_paths_bulk_returns_empty_dict_for_empty_input():
    cfg = PlexConfig(url="http://x", token="t",
                     movie_section="1", tv_section="2")
    with patch.object(PlexClient, "_get") as get_mock:
        client = PlexClient(cfg)
        out = client.get_item_paths_bulk([])
    assert out == {}
    # Empty input: no HTTP call wasted.
    assert get_mock.call_count == 0


def test_get_item_paths_bulk_parses_per_rk_locations():
    """Plex's /library/metadata/{rk1,rk2,…} returns one item per
    id under MediaContainer.Metadata in a single response. The
    bulk method must keep the per-rk Location mapping intact.
    v1.14.78: parser switched XML → JSON; mock body uses the
    Plex JSON shape (Metadata array, per-item Location array)."""
    class _Resp:
        status_code = 200
        text = (
            '{"MediaContainer": {"Metadata": ['
            '{"ratingKey": "100", "Location": [{"path": "/data/tv/Show A"}]},'
            '{"ratingKey": "200", "Location": [{"path": "/data/tv/Show B"}]},'
            '{"ratingKey": "300", "Location": [{"path": "/data/tv/Show C"}]}'
            ']}}'
        )

    cfg = PlexConfig(url="http://x", token="t",
                     movie_section="1", tv_section="2")
    with patch.object(PlexClient, "_get", return_value=_Resp()):
        client = PlexClient(cfg)
        out = client.get_item_paths_bulk(
            ["100", "200", "300"], max_concurrent_batches=1)
    assert out == {
        "100": "/data/tv/Show A",
        "200": "/data/tv/Show B",
        "300": "/data/tv/Show C",
    }


def test_get_item_paths_bulk_omits_rks_with_no_path():
    """When Plex returns an item with no Location AND no
    Media[].Part[], the rk gets omitted from the result. Callers
    must treat absence as 'no path' (matches the per-item
    fallback's `paths[0] if paths else ''` semantic).
    v1.14.78: JSON mock body."""
    class _Resp:
        status_code = 200
        text = (
            '{"MediaContainer": {"Metadata": ['
            '{"ratingKey": "100", "Location": [{"path": "/data/tv/Show A"}]},'
            '{"ratingKey": "200"}'
            ']}}'
        )

    cfg = PlexConfig(url="http://x", token="t",
                     movie_section="1", tv_section="2")
    with patch.object(PlexClient, "_get", return_value=_Resp()):
        client = PlexClient(cfg)
        out = client.get_item_paths_bulk(
            ["100", "200"], max_concurrent_batches=1)
    assert "100" in out
    assert "200" not in out


def test_get_item_paths_bulk_batches_input_at_size_limit():
    """120 input rks at batch_size=50 → 3 HTTP calls (50/50/20).
    Each call's URL contains a comma-separated id list.
    v1.14.78: empty MediaContainer JSON body."""
    captured_ids: list[str] = []

    class _Resp:
        status_code = 200
        text = '{"MediaContainer": {}}'

    def _capture_get(self, path, **kwargs):
        # Extract the id list from the path
        # `/library/metadata/<ids>`.
        captured_ids.append(path.rsplit("/", 1)[-1])
        return _Resp()

    cfg = PlexConfig(url="http://x", token="t",
                     movie_section="1", tv_section="2")
    rks = [str(i) for i in range(120)]
    with patch.object(PlexClient, "_get", _capture_get):
        client = PlexClient(cfg)
        client.get_item_paths_bulk(
            rks, batch_size=50, max_concurrent_batches=1)
    assert len(captured_ids) == 3
    # First batch has 50 commas-separated ids; last has 20.
    assert captured_ids[0].count(",") == 49
    assert captured_ids[-1].count(",") == 19


def test_get_item_paths_bulk_url_keeps_commas_literal():
    """`_rk_path` URL-encodes via quote(safe=''), which would
    percent-encode the commas separating rating_keys (Plex's
    parser expects literal commas). The bulk method must use
    quote(safe=',') to keep them intact.
    v1.14.78: empty MediaContainer JSON body."""
    captured_paths: list[str] = []

    class _Resp:
        status_code = 200
        text = '{"MediaContainer": {}}'

    def _capture_get(self, path, **kwargs):
        captured_paths.append(path)
        return _Resp()

    cfg = PlexConfig(url="http://x", token="t",
                     movie_section="1", tv_section="2")
    with patch.object(PlexClient, "_get", _capture_get):
        client = PlexClient(cfg)
        client.get_item_paths_bulk(
            ["100", "200", "300"], max_concurrent_batches=1)
    assert len(captured_paths) == 1
    # Literal commas, not %2C.
    assert "100,200,300" in captured_paths[0]
    assert "%2C" not in captured_paths[0]


def test_get_item_paths_bulk_invokes_on_batch_complete_callback():
    """The cumulative-resolved callback fires after each batch
    (so the existing fallback progress bar can advance without
    per-item ticks).
    v1.14.78: JSON mock body with the resolved item so the
    callback's resolved-count argument is 1, not 0."""
    callback_calls: list[int] = []

    class _Resp:
        status_code = 200
        text = (
            '{"MediaContainer": {"Metadata": ['
            '{"ratingKey": "1", "Location": [{"path": "/x"}]}'
            ']}}'
        )

    cfg = PlexConfig(url="http://x", token="t",
                     movie_section="1", tv_section="2")
    with patch.object(PlexClient, "_get", return_value=_Resp()):
        client = PlexClient(cfg)
        client.get_item_paths_bulk(
            ["1"], on_batch_complete=callback_calls.append,
            max_concurrent_batches=1)
    # One batch fired one callback.
    assert callback_calls == [1]


def test_get_item_paths_bulk_callback_exceptions_are_swallowed():
    """Progress callback is decorative — must never break the
    fallback if the callback raises.
    v1.14.78: empty MediaContainer JSON body."""
    class _Resp:
        status_code = 200
        text = '{"MediaContainer": {}}'

    def _raises(_n):
        raise RuntimeError("boom")

    cfg = PlexConfig(url="http://x", token="t",
                     movie_section="1", tv_section="2")
    with patch.object(PlexClient, "_get", return_value=_Resp()):
        client = PlexClient(cfg)
        # Should not raise.
        client.get_item_paths_bulk(
            ["1"], on_batch_complete=_raises,
            max_concurrent_batches=1)


# ── enumerate_section_items fallback uses the new method ──────


def test_enumerate_section_fallback_calls_bulk_not_per_item():
    """The show-folder fallback inside enumerate_section_items
    must invoke get_item_paths_bulk, NOT loop get_item_paths
    per rk (which was the pre-v1.14.76 ThreadPool shape)."""
    src = (REPO / "app" / "core" / "plex.py").read_text()
    fn_start = src.index("def enumerate_section_items(")
    # End at the next top-level method.
    next_def = src.index("\n    def ", fn_start + 10)
    body = src[fn_start:next_def]
    assert "get_item_paths_bulk(" in body
    # The pre-fix per-item ThreadPoolExecutor wrap must NOT
    # survive in the fallback block. The block can still use
    # ThreadPool internally inside get_item_paths_bulk — that's
    # a different call site. Restrict the regex to "_fill" /
    # ThreadPoolExecutor INSIDE the function body.
    assert "def _fill(" not in body
    assert "max_workers=16" not in body  # the pre-fix threadpool size


def test_v1_14_76_marker_at_fallback_callsite():
    """A v1.14.76 marker must explain the bulk-fallback switch
    so a future audit of the fallback can grep here for the
    rationale + the per-item → bulk migration."""
    src = (REPO / "app" / "core" / "plex.py").read_text()
    fn_start = src.index("def enumerate_section_items(")
    next_def = src.index("\n    def ", fn_start + 10)
    body = src[fn_start:next_def]
    assert "v1.14.76: switched from per-item ThreadPool" in body


# ── Cross-ref: get_item_paths (single-rk) is preserved ────────


def test_single_rk_get_item_paths_still_exists():
    """v1.14.76 added the bulk variant ALONGSIDE the single-rk
    method — get_item_paths is still public and unchanged so
    any external caller (or future motif callsite that wants
    single-rk semantics) keeps working."""
    assert hasattr(PlexClient, "get_item_paths")
    # And it still has the per-rk path-list shape.
    src = (REPO / "app" / "core" / "plex.py").read_text()
    fn_start = src.index("def get_item_paths(self, rating_key:")
    body = src[fn_start:fn_start + 1000]
    assert "self._rk_path(rating_key)" in body
