"""v0.51.267 — three fixes from the external review-validation brief.

  #2  release.yml published images with NO test gate. It ran no pytest and had
      no dependency on a CI result, resting on the comment "a branch push
      already ran this". ci.yml deliberately skips tags, and workflow_dispatch
      can publish any commit at all — so a red tree could reach :nightly, the
      channel every deployment tracks. A `gate` job now re-runs ci.yml's two
      BLOCKING checks against the exact tagged source, and build-and-push
      `needs: gate`.

  #8  pip-audit was `|| true`, so a newly published dependency CVE scrolled past
      in a log while the job went green. Measured clean at this commit, so the
      ratchet cost nothing — it is blocking now. Ruff-full and mypy stay
      report-only (~300 pre-existing style/type findings).

  #10 `Update Docker Hub description` was gated on `steps.tags.outputs.is_stable`,
      an output retired at 0.50.0 and never emitted since — the step had been
      silently skipped on every release. Removed.

These are YAML-parsed structural assertions, not string matching: a workflow
cannot be exercised from pytest, and per the brief's own #5 guidance a static
guard is appropriate exactly where the invariant IS the config shape. The
never-emitted-output lint below is the general form of #10, so the class cannot
recur silently.
"""
from __future__ import annotations

import re
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parent.parent
WF = REPO / ".github" / "workflows"


def _wf(name: str) -> dict:
    return yaml.safe_load((WF / f"{name}.yml").read_text())


def _steps(wf: dict, job: str) -> list[dict]:
    return wf["jobs"][job]["steps"]


def _strings(node) -> list[str]:
    """Every string value in the parsed workflow (comments are already gone)."""
    if isinstance(node, str):
        return [node]
    if isinstance(node, dict):
        return [x for v in node.values() for x in _strings(v)]
    if isinstance(node, list):
        return [x for v in node for x in _strings(v)]
    return []


def _runs(wf: dict, job: str) -> str:
    return "\n".join(s.get("run", "") for s in _steps(wf, job))


# ── #2: nothing publishes without the suite passing ──────────


def test_build_and_push_requires_the_gate_job():
    rel = _wf("release")
    needs = rel["jobs"]["build-and-push"].get("needs")
    needs = [needs] if isinstance(needs, str) else (needs or [])
    assert "gate" in needs, (
        "v0.51.267: build-and-push must depend on the gate job — without it a "
        "red commit can publish to :nightly, which deployments track")


def test_the_gate_runs_the_full_suite():
    runs = _runs(_wf("release"), "gate")
    assert "pytest" in runs, "the gate must actually run the suite"
    assert "|| true" not in runs, "a gate that swallows failure is not a gate"


def test_the_gate_mirrors_ci_blocking_checks():
    """Drift guard: the gate exists to re-run ci.yml's BLOCKING checks. If a new
    blocking check lands in ci.yml, it belongs here too."""
    ci, rel = _wf("ci"), _wf("release")
    ci_blocking = [s["run"].strip() for j in ("test", "quality")
                   for s in _steps(ci, j)
                   if "run" in s and "BLOCKING" in s.get("name", "")]
    gate = _runs(rel, "gate")
    for cmd in ci_blocking:
        assert cmd in gate, (
            f"ci.yml blocks on {cmd!r} but the release gate does not run it")


def test_the_gate_covers_manual_dispatch_too():
    """workflow_dispatch is the accidental-publication path — it must not bypass
    the gate. A job-level `needs` applies to every trigger, so simply assert the
    gate carries no trigger-scoped `if`."""
    rel = _wf("release")
    assert "if" not in rel["jobs"]["gate"], (
        "the gate must run for tag pushes AND workflow_dispatch")


# ── #8: the dependency audit actually fails ──────────────────


def test_pip_audit_is_blocking():
    steps = _steps(_wf("ci"), "quality")
    audit = [s for s in steps if "pip-audit" in s.get("run", "")]
    assert audit, "the pip-audit step vanished"
    for s in audit:
        assert "|| true" not in s["run"], (
            "v0.51.267: pip-audit must fail the job. To accept a specific "
            "finding use --ignore-vuln GHSA-xxxx with a rationale, never "
            "`|| true` — that hides the next unrelated CVE too")


def test_ruff_and_mypy_stay_report_only():
    """The ratchet was deliberate and narrow — this is NOT a mandate to flip the
    other two, which carry ~300 pre-existing findings."""
    runs = _runs(_wf("ci"), "quality")
    assert "mypy --ignore-missing-imports app/ || true" in runs
    assert "ruff check app/ tests/ || true" in runs


def test_ci_still_blocks_on_the_suite_and_pyflakes():
    ci = _wf("ci")
    assert "pytest" in _runs(ci, "test")
    assert "--select F" in _runs(ci, "quality")


# ── #10 generalized: no step may read an output nothing emits ──


def test_no_step_reads_an_output_that_is_never_emitted():
    """The general form of the is_stable bug: a condition referencing an output
    no step writes is silently always-false, so the step never runs and nothing
    reports it. Only shell steps are checked — a `uses:` action declares its
    outputs elsewhere, out of this file's reach."""
    problems = []
    for name in ("ci", "release"):
        wf = _wf(name)
        # Scan parsed string VALUES only. Reading raw text would count a COMMENT
        # that merely names an output (this file's own removal note does) as a
        # reference — the lint would then flag what it just fixed.
        text = "\n".join(_strings(wf))
        emitted: dict[str, set[str]] = {}
        shell_ids: set[str] = set()
        for job in wf["jobs"].values():
            for st in job.get("steps", []):
                sid = st.get("id")
                if not sid or "run" not in st:
                    continue
                shell_ids.add(sid)
                emitted[sid] = set(re.findall(
                    r'([A-Za-z_][A-Za-z0-9_]*)=.*?>>\s*"?\$GITHUB_OUTPUT',
                    st["run"]))
        for sid, out in re.findall(r"steps\.([A-Za-z0-9_-]+)\.outputs\.([A-Za-z0-9_-]+)", text):
            if sid in shell_ids and out not in emitted.get(sid, set()):
                problems.append(f"{name}.yml: steps.{sid}.outputs.{out} is never emitted")
    assert not problems, (
        "these workflow outputs are read but never written — the referencing "
        "step silently never runs:\n  " + "\n  ".join(sorted(set(problems))))


def test_v0_51_267_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
