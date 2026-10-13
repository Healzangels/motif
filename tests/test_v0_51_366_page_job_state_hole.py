"""v0.51.366 — a page-scoped job that dies before its try leaves the page "running".

From the 2026-10-07 code review, filed as latent and low-probability. Four jobs — the
AnimeThemes sweep, the orphan scan, the loudness audit and RESTORE FROM PLEX — have their
state set to `running` by the ENDPOINT and cleared only inside the thread's `try`. Each
began with a local import outside that try. An import that raises there kills the thread
with the page still reading "running", and these are deliberately NOT op kinds
(ANIMETHEMES_SPEC §6 decision 5), so `progress.sweep_stuck`'s 90-minute reaper never sees
them: nothing short of a restart clears the page. v0.51.344 closed the sibling hole where
`t.start()` itself raised; this is the same hole one statement later.

The imports moved inside. What may still precede the try is a `def` (its body does not run)
and a plain assignment the `except` needs bound — `t0` and `marked` in the restore.

The guard DISCOVERS these jobs rather than listing them: every `threading.Thread(target=…)`
in api.py whose target carries its own `_*_STATE` dict instead of an op_progress row. A new
page-scoped job is held to the same rule without anyone remembering to add it.
"""
from __future__ import annotations

import ast
import sys
import types
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
API = REPO / "app" / "web" / "api.py"
TREE = ast.parse(API.read_text())


def _thread_targets() -> set:
    out = set()
    for node in ast.walk(TREE):
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                and node.func.attr == "Thread"):
            for kw in node.keywords:
                if kw.arg == "target" and isinstance(kw.value, ast.Name):
                    out.add(kw.value.id)
    return out


def _functions() -> dict:
    return {n.name: n for n in ast.walk(TREE)
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}


def _page_scoped() -> dict:
    """Thread targets whose state is their own `_*_STATE` dict, not an op_progress row."""
    fns, out = _functions(), {}
    for name in _thread_targets():
        fn = fns.get(name)
        if fn is None:
            continue
        names = {n.id for n in ast.walk(fn) if isinstance(n, ast.Name)}
        state = {s for s in names if s.startswith("_") and s.endswith("_STATE")}
        reaped = any(isinstance(n, ast.Attribute) and isinstance(n.value, ast.Name)
                     and n.value.id == "op_progress" for n in ast.walk(fn))
        if state and not reaped:
            out[name] = sorted(state)
    return out


def test_the_page_scoped_jobs_are_found():
    """Non-vacuous: if the discovery stops seeing them, the rule below guards nothing."""
    found = _page_scoped()
    assert set(found) == {
        "_orphan_scan_run", "_loudness_audit_run",
        "_animethemes_sweep_run", "_canon_restore_run",
    }, (
        f"v0.51.366: the page-scoped job set changed — {found}. A NEW one must obey the "
        f"rule below; one that moved to op_progress is reaped by sweep_stuck and can leave."
    )


# What may be bound before the try because the `except` reads it: a literal, or a
# monotonic clock read (the restore's `t0`). v0.51.368: this was "any Assign", so a
# path join — `cache_dir = settings.config_dir / "animethemes"`, an attribute chain and
# a `/` that can both raise — moved back in front of the try and the test stayed green,
# weaker than the rule CLAUDE.md states for it. Found by the cross-tag review's mutation.
_SAFE_CALLS = {("time", "monotonic")}


def _cannot_raise(node) -> bool:
    if node is None or isinstance(node, ast.Constant):
        return True
    if (isinstance(node, ast.Call) and not node.args and not node.keywords
            and isinstance(node.func, ast.Attribute) and isinstance(node.func.value, ast.Name)):
        return (node.func.value.id, node.func.attr) in _SAFE_CALLS
    return False


def test_nothing_that_can_raise_runs_before_the_try():
    for name, states in _page_scoped().items():
        fn = _functions()[name]
        body = fn.body
        if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
            body = body[1:]
        pre = []
        for st in body:
            if isinstance(st, ast.Try):
                break
            pre.append(st)
        else:
            raise AssertionError(
                f"v0.51.366: {name} has no try — nothing moves {states} out of 'running'")
        for st in pre:
            if isinstance(st, ast.FunctionDef):
                continue                       # a def's body does not run here
            assert isinstance(st, (ast.Assign, ast.AnnAssign)) and _cannot_raise(st.value), (
                f"v0.51.366: {name} runs `{ast.unparse(st)[:60]}` (line {st.lineno}) before its "
                f"try. If it raises, {states} stays 'running' and no reaper covers these — "
                f"move it inside."
            )


# ── and the behaviour, for the two that are reachable from a test ──

@pytest.mark.parametrize("fn_name,module,attr,state_name", [
    ("_loudness_audit_run", "app.core.loudness_audit", "run_loudness_audit",
     "_LOUDNESS_AUDIT_STATE"),
    ("_orphan_scan_run", "app.core.orphan_scan", "scan_plex_upload_placements",
     "_ORPHAN_SCAN_STATE"),
])
def test_a_broken_import_ends_the_job_failed_not_running(fn_name, module, attr, state_name,
                                                         tmp_path, monkeypatch):
    """The actual regression: break the import the thread opens with and the page must end
    in a named state. Before this tag the thread died and the state stayed 'running'."""
    from app.web import api as api_mod

    state = getattr(api_mod, state_name)
    state.clear()
    state.update(status="running", done=0, total=0, error=None)

    # a stand-in module without the name the thread imports -> ImportError at that line
    stub = types.ModuleType(module)
    monkeypatch.setitem(sys.modules, module, stub)
    assert not hasattr(stub, attr)

    class _S:
        themes_dir = tmp_path
        plex_url = "http://x"
        plex_token = "t"
        plex_movie_section = "1"
        plex_tv_section = "2"
        plus_equiv_mode = False

    getattr(api_mod, fn_name)(_S(), tmp_path / "motif.db")
    assert state.get("status") == "failed", (
        f"v0.51.366: {state_name} is {state.get('status')!r} after the import failed — a "
        f"page stuck on 'running' that no reaper covers"
    )
    assert state.get("error"), "the failure must say what happened"
