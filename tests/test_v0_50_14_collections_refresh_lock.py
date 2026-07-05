"""v0.50.14 / v0.51.63 — the COLLECTIONS // REFRESH button lock scope.

v0.50.14 (the user): on the collections tab, // REFRESH COLLECTIONS re-enabled
while RECONCILING PLACEMENT PATHS + queued jobs were still running. Root cause:
plex_enum_active only has movies/tv/anime keys — collections have no per-tab
signal — so the lock fell back to globalEnumPipeline, which dropped before the
GLOBAL reconcile phase finished. v0.50.14 locked collections on plexEnumBusy
(ANY plex_enum in flight).

v0.51.63 (the user): "when doing a refresh of a library section only it
shouldn't lock the collection refresh button." Locking on ANY enum was too
broad — a plain library-section refresh (scope='movies'/'tv'/'anime') locked
the collections button. Fix: lock on the COLLECTIONS-scoped enum count instead.
A /collections refresh tags its plex_enum jobs scope='collections'; the count
stays true through the job's reconcile stage (reconcile is a stage INSIDE the
plex_enum job), so it still doesn't re-enable mid-scan — the v0.50.14 concern —
while a section-only refresh no longer trips it. A global scan_all/cascade still
locks it via the separate pipelineBusy check.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()


def test_plex_enum_active_has_no_collections_key():
    """The premise: there is no per-collections plex_enum_active key (so the
    collections tab can't use the per-tab signal)."""
    i = API_PY.index("plex_enum_active = {")
    block = API_PY[i:i + 200]
    assert '"movies"' in block and '"tv"' in block and '"anime"' in block
    assert '"collections"' not in block


def test_stats_exposes_collections_scoped_enum_count():
    """/api/stats surfaces a collections-scoped in-flight count, computed from
    plex_enum jobs tagged scope='collections'."""
    assert "plex_enum_collections_in_flight" in API_PY
    assert "json_extract(payload, '$.scope') = 'collections'" in API_PY


def test_collections_scoped_flag_stashed_for_chip_toggle():
    # the collections-scoped flag is stashed (was the global __motif_plex_enum_busy
    # pre-v0.51.63); the old global flag is gone.
    assert "const collectionsEnumBusy = q.plex_enum_collections_in_flight > 0;" in APP_JS
    assert "window.__motif_plex_enum_collections_busy = collectionsEnumBusy;" in APP_JS
    assert "__motif_plex_enum_busy" not in APP_JS


def test_both_mytabbusy_sites_lock_collections_on_collections_scope_only():
    # collections branch gates on the collections-scoped signal at BOTH sites,
    # NOT the global plexEnumBusy / __motif_plex_enum_busy.
    assert "tabKey === 'collections'" in APP_JS
    # refreshTopbarStatus uses the live collections-scoped local.
    assert "? collectionsEnumBusy" in APP_JS
    assert APP_JS.count("? collectionsEnumBusy") == 1
    # updateLibraryRefreshBtnLabel (chip-toggle) uses the stashed collections flag.
    assert "? window.__motif_plex_enum_collections_busy" in APP_JS
    assert APP_JS.count("? window.__motif_plex_enum_collections_busy") == 1
    # the old any-enum gate is gone from both sites.
    assert "? plexEnumBusy" not in APP_JS
    assert "? window.__motif_plex_enum_busy" not in APP_JS
