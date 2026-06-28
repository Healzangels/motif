"""v1.20.24 — DOWNLOAD TDB BACKUP yields to ACCEPT UPDATE (no duplicate).

After v1.20.23 widened DOWNLOAD TDB BACKUP to AB/UB P-rows, the user
asked: doesn't that make ACCEPT UPDATE a duplicate? On a P-row,
ACCEPT UPDATE also downloads TDB as a backup (v1.19.32), so on a row
with an actionable new_theme_available pending BOTH rendered and did
the identical thing. Fix: gate DOWNLOAD TDB BACKUP on `!acceptUpdateGateOk`
so it's hidden exactly when ACCEPT UPDATE is on the menu — no duplicate,
and no gap (it still shows when ACCEPT UPDATE isn't there: no pending,
or a declined pending).
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def test_download_tdb_backup_yields_to_accept_update():
    idx = APP_JS.index("'download-tdb-backup', 'DOWNLOAD TDB BACKUP'")
    gate = APP_JS[max(0, idx - 1800):idx]
    assert "!acceptUpdateGateOk" in gate, (
        "v1.20.24: DOWNLOAD TDB BACKUP must gate on !acceptUpdateGateOk "
        "so it doesn't duplicate ACCEPT UPDATE on a pending P-row"
    )


def test_accept_update_gate_is_the_shared_predicate():
    # acceptUpdateGateOk is exactly the condition that drives the
    # ACCEPT UPDATE render — reusing it guarantees the two never both
    # show.
    assert "const acceptUpdateGateOk = pendingUpdateActionable(it)" in APP_JS


def test_v1_20_24_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
