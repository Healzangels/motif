"""v1.23.77 — library-tab refresh is items-only (skip collections).

the user: a movies/tv/anime "REFRESH FROM PLEX" should re-enumerate only that
library's items, NOT also its collections. Collections stay refreshable from
the /collections tab (the v1.18.4 collections_only mode), and the full "scan
everything" refresh + the nightly cron cascade still walk both.

`run_plex_enum` gains a `skip_collections` kwarg — symmetric to v1.18.4's
`collections_only`. Under it the per-section collections walk
(enumerate_collections_for_section + upsert) is skipped, and — like a failed
collections fetch — the contentChangedAt stamp is NOT written, so an items-only
pass doesn't delta-suppress the next collections refresh of that section.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from test_v1_14_74_content_changed_at_delta_gate import (  # noqa: F401
    _FakeClient, _seed_db)

from app.core import plex_enum
from app.core.plex import PlexConfig, PlexLibraryItem

REPO = Path(__file__).resolve().parent.parent
ENUM_PY = (REPO / "app" / "core" / "plex_enum.py").read_text()
WORKER_PY = (REPO / "app" / "core" / "worker.py").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()


class _TrackingClient(_FakeClient):
    """Records whether the collections walk fired + returns a collection row
    so a non-skipped walk leaves a `media_type='collection'` plex_items row."""

    def __init__(self, live_cca: str):
        super().__init__(live_cca)
        self.collections_called = False

    def enumerate_collections_for_section(self, **kwargs):
        self.collections_called = True
        return [PlexLibraryItem(
            rating_key="900", section_id="1", media_type="collection",
            title="A Collection", year=None,
            guid_imdb=None, guid_tmdb=None, guid_tvdb=None,
            folder_path="", has_theme=False, plex_theme_uri="",
        )]


def _collection_rows(db_file: Path) -> int:
    with sqlite3.connect(db_file) as conn:
        return conn.execute(
            "SELECT count(*) FROM plex_items WHERE media_type = 'collection'"
        ).fetchone()[0]


def _stamp(db_file: Path) -> str | None:
    with sqlite3.connect(db_file) as conn:
        return conn.execute(
            "SELECT last_enum_content_changed_at FROM plex_sections "
            "WHERE section_id = '1'").fetchone()[0]


# ── behavioral: skip_collections does items, not collections ──


def test_skip_collections_walks_items_only(tmp_path, monkeypatch):
    db_file = _seed_db(tmp_path, stored_cca=None)
    fake = _TrackingClient(live_cca="1700000005")
    monkeypatch.setattr(plex_enum, "PlexClient", lambda cfg: fake)
    cfg = PlexConfig(url="http://x", token="t",
                     movie_section="1", tv_section="2")
    plex_enum.run_plex_enum(db_file, cfg, skip_collections=True)

    assert fake.enumerate_called is True, "items still enumerated"
    assert fake.collections_called is False, "collections walk skipped"
    assert _collection_rows(db_file) == 0, "no collection rows upserted"
    # The stamp stays unset so the next full enum / cron still does collections
    # (an items-only pass must not arm the delta gate).
    assert _stamp(db_file) is None, (
        "items-only pass must NOT stamp contentChangedAt — else the delta "
        "gate would let the next full enum skip the section and starve "
        "collections of their refresh"
    )


def test_default_refresh_still_walks_both(tmp_path, monkeypatch):
    db_file = _seed_db(tmp_path, stored_cca=None)
    fake = _TrackingClient(live_cca="1700000005")
    monkeypatch.setattr(plex_enum, "PlexClient", lambda cfg: fake)
    cfg = PlexConfig(url="http://x", token="t",
                     movie_section="1", tv_section="2")
    plex_enum.run_plex_enum(db_file, cfg)  # default = full

    assert fake.enumerate_called is True and fake.collections_called is True
    assert _collection_rows(db_file) == 1, "collection row upserted on full"
    assert _stamp(db_file) == "1700000005", "full pass stamps the gate"


# ── source pins ──


def test_run_plex_enum_signature_has_skip_collections():
    assert "skip_collections: bool = False" in ENUM_PY


def test_collections_walk_gated_on_skip_collections():
    assert "if not skip_collections:" in ENUM_PY
    gi = ENUM_PY.index("if not skip_collections:")
    assert "enumerate_collections_for_section(" in ENUM_PY[gi:gi + 400]


def test_stamp_gated_on_skip_collections():
    # the contentChangedAt stamp must skip an items-only pass too.
    assert "not collections_only and not skip_collections" in ENUM_PY


def test_worker_reads_and_forwards_skip_collections():
    i = WORKER_PY.index("def _do_plex_enum(self, job:")
    # v0.51.129: widened from 2200 — the force-enum plumbing lengthened the fn
    # body, pushing the run_plex_enum call (with skip_collections=) past 2200.
    body = WORKER_PY[i:i + 2600]
    assert 'payload.get("skip_collections")' in body
    assert "skip_collections=skip_collections," in body


def test_api_sets_skip_collections_for_library_tabs_only():
    # movies/tv/anime get skip_collections; collections keeps collections_only.
    assert 'elif tab in ("movies", "tv", "anime"):' in API_PY
    si = API_PY.index('elif tab in ("movies", "tv", "anime"):')
    assert 'job_payload["skip_collections"] = True' in API_PY[si:si + 200]
    # the collections tab must NOT get skip_collections.
    ci = API_PY.index('if tab == "collections":\n                        '
                      'job_payload["collections_only"] = True')
    assert "skip_collections" not in API_PY[ci:ci + 120]
