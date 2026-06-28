"""v1.20.66 — fix phantom-P on a PU row after DEL (api_unplace_item).

A PU row (placement_kind='plex_upload', media_folder='') can't enter the
folder-based `placement_owned_rks` skip set, so after DEL it falls through
to the inline HEAD-verify probe. Plex's metadata cache answers a stale 200
to that probe for several seconds after the theme is deleted → the row is
written has_theme=1/verified_ok=1 → renders a phantom SRC=P (recurring bug
class #1, PU-shaped). Worse than the sidecar case: a PU row lands at
verified_ok=1, so plex_enum only re-verifies it after the 30-day TTL
(vs NULL → next pass for sidecars) — a purged/unplaced PU row can show a
ghost P for up to ~a day (typically reconciled by the next enum reading
Plex's dropped theme= attr) and worst-case 30 days.

Fix: the restore loop ALREADY computes, per rk, whether Plex has a
fallback theme to serve (`fallback_rk`/`restored`) — it was just thrown
away. Capture it into pu_kept_rks / pu_zero_rks and have the inline-verify
use it instead of the unreliable HEAD probe:
  - restored → Plex serves its own theme → keep P (optimistic NULL; the
    next plex_enum confirms with an accurate read).
  - deleted but no fallback → nothing serving → write '-' directly.
  - NOT deleted (DELETE failed) → fall through to the HEAD probe, which
    accurately sees motif's still-present theme (no teardown → no stale
    window).

Gated strictly on `was_plex_upload`, so sidecar / +P rows are untouched.

NOTE: the trigger (Plex's stale-200 window) needs a live Plex, so this is
source-pinned — the reconciliation machinery it leans on is already
behaviorally pinned (test_v1_19_10 sweep TTL, test_v1_16_9 SRC render gate).
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()


def _fn() -> str:
    start = API_PY.index("def api_unplace_item(")
    # api_unplace_item is large; slice generously to its end.
    end = API_PY.index("\n    @app.", start + 100)
    return API_PY[start:end]


def test_pu_outcome_sets_declared():
    fn = _fn()
    assert "pu_kept_rks: set[str] = set()" in fn
    assert "pu_zero_rks: set[str] = set()" in fn


def test_pu_sets_populated_from_restore_outcome():
    """The restore loop must classify each PU rk by its real fallback
    outcome — restored→keep, deleted-without-fallback→zero — and must NOT
    zero a row whose DELETE failed (motif's theme is still there)."""
    fn = _fn()
    anchor = fn.index("if was_plex_upload:\n                        if restored:")
    block = fn[anchor:anchor + 260]
    assert "pu_kept_rks.add(rk)" in block
    assert "elif deleted:" in block, (
        "zero must be gated on a SUCCESSFUL delete — a failed DELETE "
        "leaves motif's theme serving, so it must fall through to the probe"
    )
    assert "pu_zero_rks.add(rk)" in block


def test_verify_loop_consumes_pu_sets_before_probe():
    """The inline-verify must handle pu_zero_rks / pu_kept_rks BEFORE the
    folder-based placement_owned_rks skip and the HEAD probe."""
    fn = _fn()
    zero_idx = fn.index("if rk in pu_zero_rks:")
    kept_idx = fn.index("if rk in pu_kept_rks:")
    owned_idx = fn.index("if rk in placement_owned_rks:")
    # v1.21.39: tristate verify_theme_claim (was item_has_theme).
    # v1.22.58: the call is offloaded via run_in_threadpool (event-loop
    # lint) — anchor on the assignment, which still marks the probe site.
    probe_idx = fn.index("verified = await run_in_threadpool(")
    assert zero_idx < owned_idx < probe_idx
    assert kept_idx < owned_idx
    # pu_zero_rks writes the row to '-' directly.
    block = fn[zero_idx:zero_idx + 400]
    assert "has_theme = 0" in block and "plex_theme_verified_ok = 0" in block


def test_restored_initialized_for_no_fallback_path():
    """`restored` must be initialized False so a no-fallback PU rk (where
    the `if fallback_rk:` block never runs) reads False, not a stale value
    from a prior loop iteration."""
    fn = _fn()
    assert "deleted = False\n                    restored = False" in fn


def test_v1_20_66_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
