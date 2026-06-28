"""v1.20.23 — DOWNLOAD TDB BACKUP gate symmetric with DOWNLOAD PLEX BACKUP.

the user's repro: an adopted-backup (AB) P-row showed DOWNLOAD PLEX BACKUP
but NOT DOWNLOAD TDB BACKUP. Root cause: the cloud-backup gate (v1.19.62)
was widened to `!downloaded || source_kind !== 'plex_cloud'` (swap any
non-PB backup → PB), but the TDB-backup gate (v1.19.49) only reached
`!hasNonCloudCanonical` — which allowed PB → TB but blocked AB → TB and
UB → TB. So backup-source swapping was one-directional. Fix: mirror the
plex gate exactly — `!downloaded || source_kind !== 'themerrdb'`. The
download-backup endpoint's _record_local_file UPSERT already overwrites
source_kind, so the canonical lands as themerrdb (TB).
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def _tdb_backup_block():
    idx = APP_JS.index("'download-tdb-backup', 'DOWNLOAD TDB BACKUP'")
    # walk back to the gate that guards this menuItemHtml push.
    start = APP_JS.rindex("canSwapToTdbBackup", 0, idx)
    return APP_JS[start - 200:idx]


def test_tdb_backup_gate_mirrors_plex_gate():
    block = _tdb_backup_block()
    # the widened gate: any non-themerrdb canonical (or no download).
    assert "!downloaded\n                             || it.source_kind !== 'themerrdb'" in APP_JS \
        or "!downloaded || it.source_kind !== 'themerrdb'" in APP_JS, (
        "v1.20.23: DOWNLOAD TDB BACKUP must gate on canSwapToTdbBackup "
        "= (!downloaded || source_kind !== 'themerrdb'), mirroring the "
        "DOWNLOAD PLEX BACKUP gate"
    )
    assert "canSwapToTdbBackup" in block, (
        "the gate must use canSwapToTdbBackup, not the old "
        "!hasNonCloudCanonical (which blocked AB/UB → TB)"
    )


def test_tdb_and_plex_backup_gates_are_symmetric():
    # both backup actions now use the same `source_kind !== '<own>'`
    # swap-from-anything shape.
    assert "it.source_kind !== 'plex_cloud'" in APP_JS  # cloud gate
    assert "it.source_kind !== 'themerrdb'" in APP_JS   # tdb gate (new)


def test_tdb_backup_swap_tooltip_present():
    # the swap case (existing non-TDB canonical) gets a "replacing the
    # current backup" tooltip so the destructive swap is explicit.
    assert "replacing the current backup" in APP_JS


def test_v1_20_23_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
