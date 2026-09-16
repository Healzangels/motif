"""v0.51.343 R1-F5: library // RESTORE FROM PLEX says a row whose download is in flight waits; it never reads as FAILED."""
from __future__ import annotations

import pytest

from test_v0_51_339_canonical_health_restore import _NODE
from test_v0_51_342_canonical_round3 import REPAIR, URL, _cols, _item, _run_library_loop, _seed
from test_v0_51_342_restore_from_plex_job import AUTH, env  # noqa: F401 — env is the endpoints' fixture

pytestmark = pytest.mark.skipif(not _NODE, reason="node not installed")


def _row(tmdb):
    return {"canonical_missing": True, "file_path": f"movies/{tmdb}/theme.mp3", "theme_media_type": "movie",
            "theme_tmdb": tmdb, "plex_title": f"T{tmdb}"}


def _in_flight_answer(client, settings, tmp_path, tmdb):
    _seed(settings.db_path, tmp_path / "plex", tmdb, source_kind="themerrdb", tdb_url=URL, plex_item=True)
    assert client.post(REPAIR, headers=AUTH).json()["repaired_rows"] == 1, "premise: REPAIR queued the row's download"
    answer = client.post(_item(tmdb), headers=AUTH).json()
    assert (answer["restored"], [s["reason"] for s in answer["skipped"]]) == (0, ["download_in_flight"]), \
        "premise: the endpoint leaves a row whose download is queued untouched"
    return answer


def test_a_row_whose_download_is_in_flight_reads_as_waiting_not_failed(env):  # noqa: F811 — the imported job-endpoint fixture
    client, settings, tmp_path, _events = env
    answer = _in_flight_answer(client, settings, tmp_path, 1904)
    out = _run_library_loop(tmp_path / "library-loop", [_row(1904)], [answer])
    assert (out["calls"], out["left"]) == ([f"POST {_item(1904)}"], 0)
    assert out["text"] == "// 0 RESTORED · 1 WAITING ON DOWNLOAD", \
        "a row the server deliberately left alone read as FAILED, or its wait went unsaid"
    assert _cols(settings.db_path, 1904, ("canonical_present",)) == (0,), "premise: the row was not touched"


def test_restored_waiting_and_failed_are_each_counted_on_their_own(env):  # noqa: F811 — the imported job-endpoint fixture
    client, settings, tmp_path, _events = env
    _seed(settings.db_path, tmp_path / "plex", 1905)
    in_flight = _in_flight_answer(client, settings, tmp_path, 1904)
    restored = client.post(_item(1905), headers=AUTH).json()
    assert (restored["restored"], restored["skipped"]) == (1, []), "premise: a row with its Plex-folder copy restores"
    answers = [
        restored,
        in_flight,
        {"ok": True, "restored": 0, "skipped": [{"section_id": "2", "reason": "download_in_flight"},
                                                {"section_id": "1", "reason": "canonical_already_present"}]},
        {"ok": True, "restored": 0, "skipped": [{"section_id": "1", "reason": "link_failed:[Errno 2]"},
                                                {"section_id": "2", "reason": "write_failed:[Errno 28]"}]},
        {"__throw": {"status": 409}},
    ]
    out = _run_library_loop(tmp_path / "library-loop", [_row(t) for t in (1905, 1904, 3, 4, 5)], answers)
    assert (len(out["calls"]), out["left"]) == (5, 0)
    assert out["text"] == "// 1 RESTORED · 2 WAITING ON DOWNLOAD · 3 FAILED", \
        "an in-flight skip counted as FAILED, a section already present counted at all, or a real failure went unsaid"
