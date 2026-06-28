"""v1.22.85 (audit round 2, Batch D #2) — pagination no-progress guard
+ bulk-LPS parameter-cap chunking.

(1) Both Plex pagination loops (enumerate_section_items + the
collections walk) had no no-progress guard: a malformed page with
totalSize present, size missing/0, and NON-empty Metadata slipped
every break — `offset += 0` re-fetched the same page forever, `out`
growing until OOM and the job never finishing (the v1.22.29 reorder
removed the short-page break that incidentally caught this). Both
loops now abort loudly when a page advertises size<=0 with items.

(2) _bulk_lps_run built its (media_type, tmdb_id) IN-tuple list
UNBOUNDED — a huge select-all hit SQLite's parameter cap ("too many
SQL variables") and the whole op aborted. Its sibling
_bulk_probe_tdb_run caps at 499 pairs; this one now CHUNKS at 400
pairs per query so coverage stays full instead of degrading.
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from app.core.plex import PlexClient, PlexConfig

REPO = Path(__file__).resolve().parent.parent
PLEX_PY = (REPO / "app" / "core" / "plex.py").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()


def test_both_pagination_loops_have_no_progress_guard():
    assert PLEX_PY.count("if container_size <= 0:") == 2, (
        "v1.22.85: both walk loops need the size<=0 abort"
    )
    assert "aborting walk to avoid an" in PLEX_PY


def test_malformed_page_terminates_instead_of_looping():
    """Behavioral: totalSize=100, size=0, non-empty Metadata — the
    pre-fix loop never advanced offset and never broke."""
    calls = []

    class _Resp:
        status_code = 200
        text = (
            '{"MediaContainer": {"totalSize": 100, "size": 0,'
            ' "Metadata": ['
            '{"ratingKey": "1", "type": "movie", "title": "X",'
            ' "year": 2020, "Guid": []}'
            ']}}'
        )

    def _capture_get(self, path, **kwargs):
        calls.append(path)
        assert len(calls) < 50, "pagination loop failed to terminate"
        return _Resp()

    cfg = PlexConfig(url="http://x", token="t",
                     movie_section="1", tv_section="2")
    with patch.object(PlexClient, "_get", _capture_get):
        client = PlexClient(cfg)
        client.enumerate_section_items(section_id="1",
                                       media_type="movie")
    assert len(calls) == 1, (
        f"v1.22.85: the walk must abort on the FIRST size=0 page "
        f"(made {len(calls)} requests)"
    )


def test_bulk_lps_in_tuples_chunked():
    i = API_PY.index("def _bulk_lps_run(")
    body = API_PY[i:API_PY.index("\ndef ", i + 1)]
    assert "for _ck in range(0, len(target_keys), 400):" in body, (
        "v1.22.85: the IN-tuple list must be chunked under SQLite's "
        "parameter cap"
    )
    assert "rows.extend(conn.execute(" in body
