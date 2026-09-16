"""v0.51.344: a test module that writes os.environ bare must not reach the modules after it."""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
CONFTEST = REPO / "tests" / "conftest.py"

LEAKER = '''import os


def test_writes_the_environment_bare():
    os.environ["MOTIF_ZZ_ENV_ADDED"] = "leaked"
    os.environ["MOTIF_ZZ_ENV_CHANGED"] = "leaked"
    del os.environ["MOTIF_ZZ_ENV_DELETED"]
'''

READER = '''import os

import pytest


@pytest.fixture(scope="module")
def phase_log():
    yield
    with open(os.environ["MOTIF_ZZ_PHASE_LOG"], "w") as f:
        f.write(os.environ.get("PYTEST_CURRENT_TEST", ""))


def test_sees_the_environment_the_session_started_with(phase_log):
    assert "MOTIF_ZZ_ENV_ADDED" not in os.environ
    assert os.environ.get("MOTIF_ZZ_ENV_CHANGED") == "preset"
    assert os.environ.get("MOTIF_ZZ_ENV_DELETED") == "preset"
'''

LEAKER_ID = "test_a_leaker.py::test_writes_the_environment_bare"
READER_ID = "test_b_reader.py::test_sees_the_environment_the_session_started_with"


def _run_leaker_then_reader(root: Path, with_conftest: bool) -> tuple[str, str]:
    root.mkdir()
    if with_conftest:
        shutil.copy(CONFTEST, root / "conftest.py")
    (root / "test_a_leaker.py").write_text(LEAKER)
    (root / "test_b_reader.py").write_text(READER)
    env = {k: v for k, v in os.environ.items() if k != "PYTEST_ADDOPTS"}
    env.update(
        PYTHONPATH=str(REPO),
        MOTIF_ZZ_ENV_CHANGED="preset",
        MOTIF_ZZ_ENV_DELETED="preset",
        MOTIF_ZZ_PHASE_LOG=str(root / "phase.txt"),
    )
    proc = subprocess.run(
        [sys.executable, "-m", "pytest", "-v", "--color=no", "-p", "no:cacheprovider",  # v0.51.344: FORCE_COLOR/PY_COLORS would wrap PASSED in ANSI codes
         f"--rootdir={root}", f"--confcutdir={root}", "test_a_leaker.py", "test_b_reader.py"],
        cwd=root, env=env, capture_output=True, text=True, timeout=180,
    )
    phase = (root / "phase.txt").read_text() if (root / "phase.txt").exists() else ""
    return proc.stdout + proc.stderr, phase


def test_the_conftest_puts_the_environment_back_before_the_next_module(tmp_path):
    out, phase = _run_leaker_then_reader(tmp_path / "with_conftest", with_conftest=True)
    assert f"{LEAKER_ID} PASSED" in out, out
    assert f"{READER_ID} PASSED" in out, out
    assert phase.endswith("(teardown)"), f"the restore rewound pytest's own phase variable: {phase!r}"


def test_the_reader_goes_red_when_nothing_puts_the_environment_back(tmp_path):
    out, _ = _run_leaker_then_reader(tmp_path / "without_conftest", with_conftest=False)
    assert f"{LEAKER_ID} PASSED" in out, out
    assert f"{READER_ID} FAILED" in out, out
