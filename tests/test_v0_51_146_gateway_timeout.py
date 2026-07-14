"""v0.51.146 — reverse-proxy audit, tag 3/3: gateway-timeout messaging.

(429 already landed in tag 1 via the shared proxyStatusHint decoder.)

The slow DB-admin endpoints (backup create, restore-from-backup, restore-upload)
run in the FastAPI threadpool and usually finish even when a reverse proxy read-
timeout 502/503/504s the client. Before this tag the client read that as a hard
failure and dumped the proxy's HTML page. New gatewayTimeoutNote() reframes 502-504
as "motif may still be finishing — verify before retrying", and each catch refreshes
the backup list / restore-pending banner so a completed-but-timed-out action surfaces.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def test_gateway_helper_matches_502_to_504_only():
    i = APP_JS.index("function gatewayTimeoutNote")
    block = APP_JS[i:i + 500]
    assert "s >= 502 && s <= 504" in block
    assert "may still be finishing" in block
    # non-5xx statuses return null so callers keep their normal handling.
    assert "return (s >= 502 && s <= 504)" in block


def test_backup_create_reframes_and_refreshes_on_timeout():
    i = APP_JS.index("'/api/admin/database-backup'")
    block = APP_JS[i:i + 900]
    assert "gatewayTimeoutNote(e)" in block
    # a timed-out-but-completed backup is surfaced by refreshing the list in the catch.
    assert "refreshList().catch(" in block


def test_restore_from_backup_reframes_and_refreshes_pending():
    i = APP_JS.index("'/api/admin/database-restore'")
    block = APP_JS[i:i + 500]
    assert "gatewayTimeoutNote(e)" in block
    assert "refreshPending()" in block


def test_restore_upload_prefers_gateway_note_before_other_branches():
    i = APP_JS.index("'/api/admin/database-restore/upload'")
    block = APP_JS[i - 200:i + 1700]
    assert "gatewayTimeoutNote(e)" in block
    # the gateway branch is checked before the detail/status/neither discriminator so a
    # 502 upload reads as "may still be finishing", not "unreachable — retry".
    assert block.index("gatewayTimeoutNote(e)") < block.index("e.detail != null")
