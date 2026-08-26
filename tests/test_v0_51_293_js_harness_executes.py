"""v0.51.293 — holistic review: the JS menu-actions harness actually runs.

tests/js/test_menu_actions.js (641 lines, 38 behavioral subtests for the
SOURCE/PLACE/REMOVE menu gating) was executed by NOTHING — no CI step, no
pytest wrapper — while its header claimed regressions "fail CI", and two
pytest files (v1_24_72, v1_24_74) explicitly deferred behavioral coverage
to it. Its documented invocation (`node --test tests/js/`) even errored on
Node 22: the filename matches none of node's default test patterns, so the
directory arg was treated as a module entry point (MODULE_NOT_FOUND).

This wrapper runs the harness inside the pytest gate. Without node it
SKIPS locally but hard-fails under MOTIF_REQUIRE_NODE=1, which both CI
workflows now set (the MOTIF_REQUIRE_FFMPEG precedent — runners ship node).
"""
from __future__ import annotations

import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
HARNESS = REPO / "tests" / "js" / "test_menu_actions.js"

_NODE = shutil.which("node")
if os.environ.get("MOTIF_REQUIRE_NODE") and not _NODE:
    raise RuntimeError(
        "MOTIF_REQUIRE_NODE=1 but node is not on PATH — the JS menu-actions "
        "harness would silently not run (the exact gap v0.51.294 closes)")


@pytest.mark.skipif(not _NODE, reason="node not installed")
def test_menu_actions_harness_passes():
    r = subprocess.run(
        [_NODE, "--test", str(HARNESS)],
        capture_output=True, text=True, timeout=120, cwd=REPO)
    assert r.returncode == 0, (
        f"JS harness failed:\n{r.stdout[-3000:]}\n{r.stderr[-1500:]}")
    m = re.search(r"# pass (\d+)", r.stdout)
    assert m and int(m.group(1)) >= 30, (
        f"harness ran but executed suspiciously few subtests "
        f"({m.group(1) if m else 'none parsed'}) — collection regressed")
    assert "# fail 0" in r.stdout


def test_ci_requires_node_on_both_workflows():
    for wf in ("ci.yml", "release.yml"):
        src = (REPO / ".github" / "workflows" / wf).read_text()
        assert "MOTIF_REQUIRE_NODE=1" in src, (
            f"{wf}: without the flag, a runner image dropping node would "
            f"silently skip the harness again")


def test_harness_header_documents_the_working_invocation():
    head = HARNESS.read_text()[:800]
    assert "node --test tests/js/test_menu_actions.js" in head, (
        "the bare-directory form errors on Node 22 (filename matches no "
        "default test pattern) — the header must show the form that works")


def test_v0_51_293_version_pin_harness():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert "0.51.293: " in init_py
