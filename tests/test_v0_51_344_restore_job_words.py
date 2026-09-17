"""v0.51.344: RESTORE FROM PLEX's marker warnings say what a restart will show, and its run stamps tell two runs in one second apart."""
from __future__ import annotations

import json
import logging
import os
import threading
from datetime import datetime, timezone
from pathlib import Path

import pytest

from app.core import canonical_health as ch
from test_v0_51_339_canonical_health_restore import _NODE, _report
from test_v0_51_342_canonical_round3 import _BTN, _PAGE, _STATUS, _WORDS, _page
from test_v0_51_342_restore_from_plex_job import (  # noqa: F401 — env is a fixture
    AUTH, START, HeldRestore, _ago, _finish, _marker, _reset_job, _status, env,
)

_FILE = "restore_from_plex.json"


def _earlier_run(settings):
    marker = {"status": "done", "started_at": _ago(days=2, minutes=1), "finished_at": _ago(days=2),
              "actor": "testadmin", "broken": 3, "restored": 3, "restored_sidecar": 3, "restored_store": 0,
              "skipped": [], "skipped_count": 0, "not_attempted": 0, "cancelled": False, "plex_unreachable": False}
    _marker(settings).parent.mkdir(parents=True, exist_ok=True)
    _marker(settings).write_text(json.dumps(marker))
    return marker


def _marker_writes(monkeypatch, *, refuse, remove_fails=False):
    """The marker's os.replace: `refuse(status)` fails that write; returns the statuses that landed."""
    real_replace, real_unlink = os.replace, os.unlink
    landed: list[str] = []

    def replace(src, dst, *a, **k):
        if str(dst).endswith(_FILE):
            status = json.loads(Path(src).read_text())["status"]
            if refuse(status):
                raise OSError(28, "No space left on device")
            landed.append(status)
        return real_replace(src, dst, *a, **k)

    def unlink(p, *a, **k):
        if remove_fails and str(p).endswith(_FILE):
            raise OSError(13, "Permission denied")
        return real_unlink(p, *a, **k)
    monkeypatch.setattr(os, "replace", replace)
    monkeypatch.setattr(os, "unlink", unlink)
    return landed


def _restart_shows(client, prediction, earlier):
    """What the warning says a restart will show, against what one shows."""
    _reset_job()
    st = _status(client)
    if "shows no last run" in prediction:
        assert st == {"status": "idle"}, (prediction, st)
    elif "earlier run left as the last run" in prediction:
        assert (st["status"], st.get("started_at")) == (earlier["status"], earlier["started_at"]), (prediction, st)
    elif "cut off by a restart" in prediction:
        assert st["status"] == "interrupted", (prediction, st)
    else:
        raise AssertionError(f"no restart prediction in: {prediction}")


@pytest.mark.parametrize("remove_fails", [False, True], ids=["removed", "remove-fails"])
@pytest.mark.parametrize("start_lands", [True, False], ids=["start-marker-landed", "start-marker-failed"])
def test_a_final_marker_that_cannot_be_written_says_what_a_restart_will_show(env, monkeypatch, caplog, start_lands,
                                                                            remove_fails):
    client, settings, tmp_path, events = env
    earlier = _earlier_run(settings)
    landed = _marker_writes(monkeypatch, refuse=lambda s: not (start_lands and s == "running"),
                            remove_fails=remove_fails)
    monkeypatch.setattr(ch, "restore_from_plex", HeldRestore(released=True))
    with caplog.at_level(logging.WARNING):
        assert client.post(START, headers=AUTH).json()["started"] is True
        assert _finish(client)["status"] == "done", "a marker failure relabelled the run"
    assert landed == (["running"] if start_lands else []), "premise: which of this run's markers landed"
    warned = [r.getMessage() for r in caplog.records
              if r.levelno == logging.WARNING and r.getMessage().startswith("canonical restore:")]
    (removing,) = [m for m in warned if "could not write" in m and "removing" in m]
    # v0.51.344: it named a running marker the run never wrote — the file it removes is then an earlier run's
    assert ("removing the running marker" in removing) == start_lands, removing
    _restart_shows(client, [m for m in warned if "after a restart" in m][-1], earlier)


@pytest.mark.parametrize("start_lands", [True, False], ids=["start-marker-landed", "start-marker-failed"])
def test_a_cut_off_run_says_whether_the_next_start_can_report_it(env, monkeypatch, caplog, start_lands):
    client, settings, tmp_path, events = env
    from app.web import api as api_mod
    monkeypatch.setattr(api_mod, "_CANON_RESTORE_SHUTDOWN", threading.Event())
    monkeypatch.setattr(api_mod, "_CANON_RESTORE_THREAD", None)
    earlier = _earlier_run(settings)
    landed = _marker_writes(monkeypatch, refuse=lambda s: not start_lands)
    held = HeldRestore()
    monkeypatch.setattr(ch, "restore_from_plex", held)
    assert client.post(START, headers=AUTH).json()["started"] is True
    assert held.entered.wait(10)
    with caplog.at_level(logging.WARNING):
        job = api_mod.canon_restore_shutdown()
        job.join(10)
    assert not job.is_alive()
    # v0.51.344: the cut-off rewrites its running marker with the run's counts — every landed write is still 'running'
    assert set(landed) == ({"running"} if start_lands else set()), "premise: whether this run's running marker landed"
    (said,) = [r.getMessage() for r in caplog.records if "cut off by a motif shutdown" in r.getMessage()]
    _reset_job()
    st = _status(client)
    # v0.51.344: the promise of a cut-off report holds only when a running marker is there to give it
    assert ("next start reports it cut off" in said) == (st["status"] == "interrupted"), (said, st)
    if not start_lands:
        assert (st["status"], st["started_at"]) == (earlier["status"], earlier["started_at"])


@pytest.mark.skipif(not _NODE, reason="node not installed")
def test_a_run_started_in_the_second_the_page_last_saw_reads_as_this_starts_run(env, monkeypatch):
    client, settings, tmp_path, events = env
    from app.core import events as events_mod
    second = datetime.now(timezone.utc).replace(microsecond=0)
    clock = {"now": second}

    class OneSecond(datetime):
        @classmethod
        def now(cls, tz=None):
            return clock["now"]
    monkeypatch.setattr(events_mod, "datetime", OneSecond)
    runs = []
    for ms in (100, 600):
        clock["now"] = second.replace(microsecond=ms * 1000)
        monkeypatch.setattr(ch, "restore_from_plex", HeldRestore(released=True))
        assert client.post(START, headers=AUTH).json()["started"] is True
        runs.append(_finish(client))
    first, later = runs
    _s0, s1 = _page(tmp_path / "page", [_PAGE, first, {"__throw": {"status": 502}}, later, _report()], [_BTN])
    status = s1[_STATUS]
    # v0.51.344: stamped to the second, both runs read as one and the page said the start never reached motif
    assert (status["text"], status["className"]) == (_WORDS + " · 1 skipped (1 no Plex copy)",
                                                     "form-status form-status-ok"), status
    assert datetime.fromisoformat(later["started_at"]) > datetime.fromisoformat(first["started_at"])
