"""v0.51.73 — the CI quality gate is wired and keeps its two blocking checks.

Professional-bar follow-up ("would a staff engineer approve"): before this, the
only GitHub Actions workflow (release.yml) just built the Docker image on tag —
the 7,000+ tests, ruff, mypy, and pip-audit ran NOWHERE automatically, so the
gate was "the dev remembered to run pytest". v0.51.73 adds .github/workflows/
ci.yml running on every push/PR, with two BLOCKING gates (pytest + ruff's
pyflakes-correctness subset) and three report-only signals (full ruff, mypy,
pip-audit).

This guard pins the structure so a future edit can't silently gut the gate
(e.g. drop the pytest step or turn the blocking ruff gate into `|| true`).
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
CI = (REPO / ".github" / "workflows" / "ci.yml").read_text()
DEV = (REPO / "requirements-dev.txt").read_text()


def test_ci_workflow_exists_and_runs_on_push_and_pr():
    assert (REPO / ".github" / "workflows" / "ci.yml").is_file()
    assert "on:" in CI and "push:" in CI and "pull_request:" in CI


def test_ci_runs_the_full_pytest_suite_blocking():
    # The pytest step must exist and must NOT be neutered with `|| true`.
    assert "pytest -q -p no:cacheprovider" in CI
    # crude but effective: the pytest line itself isn't suffixed with || true.
    pytest_line = next(l for l in CI.splitlines() if "pytest -q" in l)
    assert "|| true" not in pytest_line, "the pytest gate must stay BLOCKING"


def test_ci_ruff_correctness_gate_is_blocking():
    # the pyflakes-correctness gate (the family that caught the v0.51.72 httpx
    # NameError) must run and stay blocking.
    assert "ruff check app/ tests/ --select F --ignore F401,F811,F841,F541" in CI
    ruff_gate = next(l for l in CI.splitlines()
                     if "--select F --ignore F401,F811,F841,F541" in l)
    assert "|| true" not in ruff_gate, "the ruff correctness gate must stay BLOCKING"


def test_ci_has_the_report_only_signals():
    # report-only tier present (non-fatal via || true is expected HERE).
    assert "mypy" in CI and "pip-audit" in CI


def test_dev_requirements_lists_the_tooling():
    for pkg in ("pytest", "quickjs", "ruff", "mypy", "pip-audit"):
        assert pkg in DEV, f"requirements-dev.txt must pin {pkg} for CI"


def test_ci_targets_the_runtime_python():
    # must test on the Dockerfile's Python (3.12), not just whatever's newest.
    assert 'python-version: "3.12"' in CI
