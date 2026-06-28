"""v1.22.34 (audit) — frontend feedback on silent no-ops (Tag I).

Two reachable app.js UX swallows from the audit:

1. DOWNLOAD PLEX BACKUP: the cloud-themes-backup-run .then() handled only
   `res.ok` truthy — a 200 with `{ok:false}` left the optimistic placeholder
   ("// QUEUING PLEX BACKUP") hanging forever with no alert. Now an else branch
   clears the placeholder + alerts.

2. Bulk download: a 0-enqueued batch showed only a fleeting "// 0 QUEUED" button
   label that's easy to miss. Now an explicit alert explains why nothing queued.

(The third Tag-I candidate — the bulk-LPS finishWatcher hidden-tab edge — was
left as-is: the watcher is delicate stateful UI with a 30-min timeout backstop,
and the failure window is narrow (op must complete AND age out of op_progress
state while the tab is hidden); a timing fix risks premature button-reset under
queue backlog, so the risk outweighs the papercut.)
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def test_download_plex_backup_handles_ok_false():
    # The unique alert string the audit fix adds (res.ok===false else branch)
    # must be present — it sits right after the placeholder-clear in the else.
    assert "DOWNLOAD PLEX BACKUP could not start" in APP_JS, (
        "v1.22.34: res.ok===false must surface an alert, not swallow")
    i = APP_JS.index("DOWNLOAD PLEX BACKUP could not start")
    near = APP_JS[i - 400:i]
    assert "clearOptimisticPlaceholder('cloud_themes_backup')" in near, (
        "the else must also clear the hanging optimistic placeholder")


def test_bulk_download_alerts_on_zero_enqueued():
    i = APP_JS.index("/api/library/download-batch")
    block = APP_JS[i:i + 1200]
    assert "(r.enqueued || 0) === 0" in block, (
        "v1.22.34: a 0-enqueued bulk download must alert the user")
    assert "Nothing was queued" in block


def test_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
