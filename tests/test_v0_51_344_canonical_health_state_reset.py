"""v0.51.344: a test that closes publishing, pins the memo, holds the publish lock or signals exit must not reach the tests after it."""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from app.core import canonical_health as ch
from app.web import api as api_mod
from test_v0_51_342_restore_pool import FakePlex, _lf_cols, _seed_store

REPO = Path(__file__).resolve().parents[1]
CONFTEST = REPO / "tests" / "conftest.py"
_HELD: list = []  # v0.51.344: the lock a leaker holds on purpose — let go of last, so a broken reset reads red instead of hanging the files after this one


@pytest.fixture(scope="module", autouse=True)
def _let_go_of_the_held_lock():
    yield
    for lock in _HELD:
        if lock.locked():
            lock.release()


def _publish_one(root: Path) -> tuple[dict, Path, Path]:
    root.mkdir()
    db, themes = _seed_store(root, n=1)
    return ch.restore_from_plex(db, themes, FakePlex()), db, themes


def _read(tmp_path: Path) -> None:
    assert ch._IN_FLIGHT_DOWNLOADS == {}, "the memo still remembers another test's run"
    # read before publishing: a lock an earlier test still holds must read red here, never hang the publish below
    assert ch._PUBLISH_LOCK.acquire(blocking=False), "a write from an earlier test still holds the publish lock"
    ch._PUBLISH_LOCK.release()
    assert not api_mod._CANON_RESTORE_SHUTDOWN.is_set(), "an earlier test's exit still tells the restore job motif is shutting down"
    res, db, themes = _publish_one(tmp_path / "read")
    assert ([s["reason"] for s in res["skipped"]], res["restored"]) == ([], 1), "an earlier test's exit still closes publishing"
    assert (themes / "movies" / "601" / "theme.mp3").read_bytes() == b"store-9601"
    assert _lf_cols(db, 601, ("canonical_present",)) == (1,)


def test_a_leaker_closes_publishing_and_pins_the_memo(tmp_path):
    res, _db, _themes = _publish_one(tmp_path / "leak")
    assert res["restored"] == 1, "premise: a store publish ran"
    assert "1" in ch._IN_FLIGHT_DOWNLOADS, "premise: the publish remembered this run's queued downloads"
    assert ch.close_publishing(0.0) is True
    assert api_mod.canon_restore_shutdown() is None
    assert ch._PUBLISH_CLOSED.is_set() and api_mod._CANON_RESTORE_SHUTDOWN.is_set(), "premise: both exit latches are set"


def test_the_reader_after_it_publishes_with_a_fresh_memo(tmp_path):
    _read(tmp_path)


class TestInsideOneClass:
    def test_a_leaker_inside_the_class_also_holds_the_publish_lock(self, tmp_path):
        _publish_one(tmp_path / "leak")
        assert "1" in ch._IN_FLIGHT_DOWNLOADS, "premise: a publish remembered a run's queued downloads"
        assert ch.close_publishing(0.0) is True
        assert api_mod.canon_restore_shutdown() is None
        assert ch._PUBLISH_LOCK.acquire(blocking=False), "premise: nothing held the publish lock before this test"
        _HELD.append(ch._PUBLISH_LOCK)  # a write the interpreter froze
        assert ch._PUBLISH_CLOSED.is_set() and ch._PUBLISH_LOCK.locked(), "premise: the gate is closed and the lock held"

    def test_the_reader_inside_the_class_publishes_too(self, tmp_path):
        _read(tmp_path)


LEAKER_IDS = ("test_a_leaker.py::test_a_leaker_closes_publishing_and_pins_the_memo",
              "test_a_leaker.py::TestInsideOneClass::test_a_leaker_inside_the_class_also_holds_the_publish_lock")
READER_IDS = ("test_a_leaker.py::test_the_reader_after_it_publishes_with_a_fresh_memo",
              "test_a_leaker.py::TestInsideOneClass::test_the_reader_inside_the_class_publishes_too")


def _run_in_a_child_session(root: Path, with_conftest: bool) -> str:
    root.mkdir()
    if with_conftest:
        shutil.copy(CONFTEST, root / "conftest.py")
    shutil.copy(Path(__file__), root / "test_a_leaker.py")  # the four tests above, under the copied conftest or none
    env = {k: v for k, v in os.environ.items() if k != "PYTEST_ADDOPTS"}
    env["PYTHONPATH"] = os.pathsep.join((str(REPO), str(REPO / "tests")))
    proc = subprocess.run(
        [sys.executable, "-m", "pytest", "-v", "--color=no", "-p", "no:cacheprovider",
         f"--rootdir={root}", f"--confcutdir={root}", "-k", "not in_a_child_session", "test_a_leaker.py"],
        cwd=root, env=env, capture_output=True, text=True, timeout=180,
    )
    return proc.stdout + proc.stderr


def test_in_a_child_session_the_conftest_puts_canonical_health_back_before_the_next_test(tmp_path):
    out = _run_in_a_child_session(tmp_path / "with_conftest", with_conftest=True)
    for test_id in LEAKER_IDS + READER_IDS:
        assert f"{test_id} PASSED" in out, out


def test_in_a_child_session_the_readers_go_red_when_nothing_puts_canonical_health_back(tmp_path):
    out = _run_in_a_child_session(tmp_path / "without_conftest", with_conftest=False)
    for test_id in LEAKER_IDS:
        assert f"{test_id} PASSED" in out, out
    for test_id in READER_IDS:
        assert f"{test_id} FAILED" in out, out
