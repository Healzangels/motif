"""v1.19.91 — every _flush_sync_batch call must pass auto_download_new_themes.

Production crash (sync run #91, git-differential path):

    TypeError: _flush_sync_batch() missing 1 required keyword-only
    argument: 'auto_download_new_themes'
      File ".../app/core/sync.py", line 2568, in _run_git_differential_upsert

Root cause: v1.19.71 made `auto_download_new_themes` a *required
keyword-only* param of `_flush_sync_batch` and threaded it through
`run_sync` → `_run_git_differential_upsert`. But THREE of the four
`_flush_sync_batch(` call sites never got the new kwarg added:

  - sync.py:2568 — tail flush in `_run_git_differential_upsert`
    (the git-differential prod path — crashed every run)
  - sync.py:3178 — batch-boundary flush in `run_sync`
  - sync.py:3201 — tail flush in `run_sync`

Only the 2548 call (the batch-boundary flush in the differential
path) passed it. Small changesets (< _SYNC_BATCH items) skipped
the boundary flush entirely and hit the tail flush FIRST → every
git-differential sync TypeError'd before applying any rows.

Why v1.19.71's tests didn't catch it: they pinned the *signature*
(`test_flush_sync_batch_signature_threads_auto_download`) and the
*param threading* but never asserted the CALL SITES pass the kwarg.
A required keyword-only param is a contract between def and EVERY
caller — pinning only the def is half the contract.

## The guard

AST-walk sync.py, find every `_flush_sync_batch(...)` Call node,
assert each carries an `auto_download_new_themes` keyword. This is
structural (parses the actual call expressions, not a string `in`
substring), so it catches a future call site that forgets the
kwarg AND can't be fooled by the param appearing elsewhere in the
arg span. Plus an `inspect`-based pin that the param stays
required + keyword-only (the property that turns an omission into
a hard crash rather than a silent wrong default).
"""
from __future__ import annotations

import ast
import inspect
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

SYNC_PY_PATH = REPO / "app" / "core" / "sync.py"
SYNC_PY = SYNC_PY_PATH.read_text()


def _flush_call_nodes() -> list[ast.Call]:
    """Every `_flush_sync_batch(...)` Call node in sync.py."""
    tree = ast.parse(SYNC_PY)
    calls: list[ast.Call] = []
    for node in ast.walk(tree):
        if (isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "_flush_sync_batch"):
            calls.append(node)
    return calls


# ── call-site coverage (the bug) ─────────────────────────────


def test_at_least_four_flush_call_sites_exist():
    """Sanity floor — if a refactor collapses the call sites the
    per-site assertions below would vacuously pass, so pin the
    count we know shipped (2 in the differential path + 2 in
    run_sync)."""
    calls = _flush_call_nodes()
    assert len(calls) >= 4, (
        f"v1.19.91: expected >=4 _flush_sync_batch call sites, "
        f"found {len(calls)} — did the flush helper get inlined? "
        f"Re-derive this guard if so."
    )


def test_every_flush_call_passes_auto_download_kwarg():
    """THE regression guard. Every _flush_sync_batch call must pass
    auto_download_new_themes by keyword — it's required+keyword-only
    since v1.19.71. Pre-fix, 3 of 4 call sites omitted it and the
    git-differential prod path crashed every run."""
    offenders = []
    for call in _flush_call_nodes():
        kwarg_names = {kw.arg for kw in call.keywords if kw.arg is not None}
        # A bare `**kwargs` splat (kw.arg is None) would also satisfy
        # the contract, but sync.py never uses one here; require the
        # explicit keyword so the intent is legible at the call site.
        if "auto_download_new_themes" not in kwarg_names:
            offenders.append(call.lineno)
    assert not offenders, (
        "v1.19.91: _flush_sync_batch call sites missing the required "
        f"keyword-only `auto_download_new_themes` at lines {offenders}. "
        "This is the sync-crash regression — every caller must pass it."
    )


# ── contract pin: required + keyword-only ────────────────────


def test_auto_download_param_is_required_keyword_only():
    """The crash only happens because the param is REQUIRED and
    KEYWORD-ONLY (no default → omission is a TypeError, not a silent
    wrong value). If a future edit gives it a default, the call-site
    guard above becomes toothless — so pin the contract that makes
    the guard load-bearing."""
    import app.core.sync as sync_mod
    sig = inspect.signature(sync_mod._flush_sync_batch)
    param = sig.parameters.get("auto_download_new_themes")
    assert param is not None, (
        "v1.19.91: _flush_sync_batch must declare auto_download_new_themes"
    )
    assert param.kind is inspect.Parameter.KEYWORD_ONLY, (
        "v1.19.91: auto_download_new_themes must stay keyword-only"
    )
    assert param.default is inspect.Parameter.empty, (
        "v1.19.91: auto_download_new_themes must stay REQUIRED (no "
        "default) — a default would silently mask a forgotten call-site "
        "kwarg with the wrong behavior instead of crashing loud"
    )


def test_run_sync_and_differential_thread_the_param():
    """Both callers that fan out to _flush_sync_batch must themselves
    accept auto_download_new_themes so it's in scope at every flush
    call. (run_sync gives it a default; the differential helper takes
    it as required from run_sync.)"""
    import app.core.sync as sync_mod
    run_sig = inspect.signature(sync_mod.run_sync)
    assert "auto_download_new_themes" in run_sig.parameters
    diff_sig = inspect.signature(sync_mod._run_git_differential_upsert)
    assert "auto_download_new_themes" in diff_sig.parameters


# ── version pin ──────────────────────────────────────────────


def test_v1_19_91_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
