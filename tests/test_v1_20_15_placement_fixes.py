"""v1.20.15 — audit round 3 placement fixes (MED-1 + MED-2).

MED-1 (worker): the ≥10MB API-push size-rejection sidecar fallback in
`_do_place_collection` wrote a theme.mp3 directly (bypassing
placement.place_theme, which refreshes Plex inline) but never nudged
Plex AND hardcoded plex_refreshed=1 — a false claim. Plex wouldn't
serve the sidecar until the next section-wide refresh. Fix: call
plex.refresh on the fallback (best-effort, isolated) + write
plex_refreshed from the honest result.

MED-2 (api, class 1 phantom-P): `api_unplace_item`'s inline HEAD-verify
ran plex.item_has_theme for every pre-themed rk, with no skip for rks
where motif just unlinked its own sidecar — Plex's /theme cache returns
a stale 200 for seconds → has_theme=1/verified_ok=1 → the row renders a
phantom P. Fix: mirror the PURGE rk_from_placement guard — skip the
verify for placement-owned rks (leave verified_ok=NULL; the TTL +
plex_enum reconcile).

These worker/endpoint paths are source-pinned (test_v1_13_81 precedent):
the behavioral triggers need a live Plex with a real cache-stale window.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
WORKER_PY = (REPO / "app" / "core" / "worker.py").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()


# ── MED-1: fallback refreshes Plex + honest plex_refreshed ───


def test_fallback_refreshes_plex_and_records_honest_flag():
    fn = WORKER_PY[WORKER_PY.index("def _do_place_collection("):]
    fn = fn[:fn.index("\n    def ", 1)]
    # The fallback path calls refresh on a fresh client.
    assert "_rplex.refresh(str(cached_rk))" in fn, (
        "v1.20.15 MED-1: the sidecar fallback must nudge Plex (it bypasses "
        "place_theme's inline refresh)"
    )
    assert "fell_back_refreshed = bool(" in fn
    # plex_refreshed is NO LONGER hardcoded to 1 in the placements UPSERT.
    upsert = fn[fn.index("INSERT INTO placements"):]
    upsert = upsert[:upsert.index("ON CONFLICT")]
    assert "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)" in upsert, (
        "v1.20.15 MED-1: plex_refreshed must be a bound param, not a "
        "hardcoded 1 (v1.21.55: edition_key added → 10 params)"
    )
    assert "VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?)" not in upsert
    # The honest value: 1 for the normal upload path, the refresh result
    # for the fallback.
    assert "_placed_refreshed = 1 if fell_back_kind is None else" in fn


# ── MED-2: unplace skips the verify for placement-owned rks ──


def test_unplace_verify_skips_placement_owned_rks():
    fn = API_PY[API_PY.index("async def api_unplace_item("):]
    fn = fn[:fn.index("\n    @app.", 1)]
    # The placement-owned set is built from the sidecar-unlinked sections.
    assert "sidecar_unlinked_sections" in fn
    assert "placement_owned_rks" in fn
    # The verify loop skips those rks BEFORE the HEAD-probe.
    # v1.21.39: the probe is verify_theme_claim (tristate), not the
    # bool item_has_theme — see test_v1_21_39.
    skip_idx = fn.index("if rk in placement_owned_rks:")
    # v1.22.58: the probe is offloaded via run_in_threadpool (event-loop
    # lint) — anchor on the assignment, which still marks the probe site.
    probe_idx = fn.index("verified = await run_in_threadpool(")
    assert skip_idx < probe_idx, (
        "v1.20.15 MED-2: the placement-owned skip must precede the "
        "HEAD-probe (else the stale-200 sets phantom-P)"
    )
    # And it's a `continue` (leave verified_ok=NULL for the TTL).
    after = fn[skip_idx:skip_idx + 120]
    assert "continue" in after


def test_unplace_mirrors_purge_phantom_p_guard():
    """Both DEL paths must guard the phantom-P class: PURGE via
    rk_from_placement, unplace via placement_owned_rks."""
    assert "if rk in rk_from_placement:" in API_PY  # PURGE (existing)
    assert "if rk in placement_owned_rks:" in API_PY  # unplace (v1.20.15)


def test_v1_20_15_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
