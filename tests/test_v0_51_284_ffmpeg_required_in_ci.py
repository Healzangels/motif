"""v0.51.284 — the render tests cannot silently skip in CI.

v0.51.281 shipped the trim/fade render path with tests that skip when ffmpeg
is absent, on the assumption ubuntu runners ship it. Verified on the tag's
own CI run: 4 skipped — the SAME skips as the dev Mac. The render path had
never executed anywhere, and nothing would ever have said so: a skip hides
inside a green run, which is the phantom class this repo keeps re-learning.

Two-part fix, each half guarding the other:
  * both workflows install ffmpeg before pytest;
  * both set MOTIF_REQUIRE_FFMPEG=1, and the test module raises at IMPORT
    when the flag is set and ffmpeg is missing — so if the install step is
    ever dropped, CI goes red instead of quietly skipping again.
"""
from __future__ import annotations

from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parent.parent
CI = (REPO / ".github" / "workflows" / "ci.yml").read_text()
REL = (REPO / ".github" / "workflows" / "release.yml").read_text()
EDIT_TESTS = (REPO / "tests" / "test_v0_51_281_audio_edit.py").read_text()


def _pytest_runs(text: str) -> list[str]:
    wf = yaml.safe_load(text)
    return [s.get("run", "") for j in wf["jobs"].values()
            for s in j.get("steps", []) if "pytest" in s.get("run", "")]


def test_both_workflows_install_ffmpeg_before_pytest():
    for name, text in (("ci.yml", CI), ("release.yml", REL)):
        wf = yaml.safe_load(text)
        for job in wf["jobs"].values():
            runs = [s.get("run", "") for s in job.get("steps", [])]
            pytest_idx = [i for i, r in enumerate(runs) if "pytest" in r]
            if not pytest_idx:
                continue
            ffmpeg_idx = [i for i, r in enumerate(runs) if "install -y" in r
                          and "ffmpeg" in r]
            assert ffmpeg_idx and ffmpeg_idx[0] < pytest_idx[0], (
                f"{name}: ffmpeg must install before pytest — the runner "
                f"image does NOT ship it (verified on the v0.51.281 run)")


def test_both_pytest_invocations_carry_the_require_flag():
    for name, text in (("ci.yml", CI), ("release.yml", REL)):
        for run in _pytest_runs(text):
            assert "MOTIF_REQUIRE_FFMPEG=1" in run, (
                f"{name}: without the flag, dropping the install step would "
                f"return to silent skipping")


def test_the_flag_turns_a_missing_ffmpeg_into_a_hard_failure():
    assert 'os.environ.get("MOTIF_REQUIRE_FFMPEG")' in EDIT_TESTS
    assert "raise RuntimeError" in EDIT_TESTS, (
        "the module must refuse to collect-and-skip under the flag")


def test_v0_51_284_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
