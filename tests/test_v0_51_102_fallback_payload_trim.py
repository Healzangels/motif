"""v0.51.102 — trim the show folder-path fallback payload + raise its concurrency.

The per-show bulk /library/metadata fallback (get_item_paths_bulk) reads ONLY
Location.path (with a Part.file fallback), but it fetched the FULL metadata tree
(Genre/Role/Writer/…) per item — and it fires on every enum on Plex builds that
ignore includeLocations=1 at section level. v0.51.102 sends the same
excludeElements trim the /all listing uses (~5-10× smaller batches) and bumps
max_concurrent_batches 4 → 8. Location/Media/Part are NEVER excluded, so folder
discovery is unaffected.
"""
from __future__ import annotations

import inspect
import sys
from pathlib import Path
from unittest.mock import patch

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from app.core.plex import PlexClient, PlexConfig  # noqa: E402


class _Resp:
    status_code = 200
    text = ('{"MediaContainer": {"Metadata": ['
            '{"ratingKey": "100", "Location": [{"path": "/data/tv/Show A"}]}'
            ']}}')


def test_fallback_sends_excludeelements_and_keeps_folder_carrying_elements():
    cfg = PlexConfig(url="http://x", token="t",
                     movie_section="1", tv_section="2")
    with patch.object(PlexClient, "_get", return_value=_Resp()) as gm:
        client = PlexClient(cfg)
        out = client.get_item_paths_bulk(["100"])
    # folder discovery still works with the trim applied.
    assert out == {"100": "/data/tv/Show A"}
    # the trim param was actually sent on the wire...
    params = gm.call_args.kwargs.get("params") or {}
    excl = params.get("excludeElements", "")
    assert excl, "fallback must send an excludeElements trim (v0.51.102)"
    # ...and it must NOT drop the elements that carry the folder path, or
    # folder discovery silently breaks on every show.
    for keep in ("Location", "Media", "Part"):
        assert keep not in excl, (
            f"{keep} carries the show folder path — excluding it would break "
            "folder discovery")


def test_bulk_concurrency_default_raised_to_8():
    sig = inspect.signature(PlexClient.get_item_paths_bulk)
    assert sig.parameters["max_concurrent_batches"].default == 8
