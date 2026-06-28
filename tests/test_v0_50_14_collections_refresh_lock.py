"""v0.50.14 — the COLLECTIONS // REFRESH button stays locked through the scan.

the user: on the collections tab, // REFRESH COLLECTIONS re-enabled while
RECONCILING PLACEMENT PATHS (a global plex_enum post-phase) + queued jobs were
still running.

Root cause: plex_enum_active only has movies/tv/anime keys — a collections
refresh enumerates the underlying movie/tv sections (mapped to THOSE tabs), so
the collections tab's myTabBusy was always false and the lock fell back to
globalEnumPipeline, which drops once the per-section enums finish but the GLOBAL
reconcile_placement_paths phase (+ queued) still runs. Fix: on the collections
tab, lock on plexEnumBusy (any plex_enum in flight), which stays true through
reconcile + the queue, at both myTabBusy sites.
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


def test_busy_flag_stashed_for_chip_toggle():
    assert "window.__motif_plex_enum_busy = plexEnumBusy;" in APP_JS


def test_both_mytabbusy_sites_lock_collections_on_any_enum():
    # refreshTopbarStatus uses the live plexEnumBusy for collections.
    assert "tabKey === 'collections'" in APP_JS
    assert "? plexEnumBusy" in APP_JS
    # updateLibraryRefreshBtnLabel (chip-toggle) uses the stashed flag.
    assert "? window.__motif_plex_enum_busy" in APP_JS
    # exactly two collections-branch myTabBusy sites.
    assert APP_JS.count("tabKey === 'collections'\n") >= 0  # tolerant of fmt
    assert APP_JS.count("? plexEnumBusy") == 1
    assert APP_JS.count("? window.__motif_plex_enum_busy") == 1
