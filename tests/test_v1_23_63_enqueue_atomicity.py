"""v1.23.63 — transaction atomicity for the check-then-insert enqueue paths.

From the holistic audit (cluster E). Three sites ran a dedup SELECT + INSERT in
plain autocommit, so two concurrent requests (or a manual action racing the cron)
could both pass the "already queued?" check and both insert → duplicate jobs.
Each is now wrapped in a BEGIN IMMEDIATE that serializes the check-then-insert:

  #7  /api/sync/now      — mirror the cron _enqueue_sync (v1.22.33) one-txn guard.
  #16 download-batch     — per-ITEM transaction around _enqueue_download (atomic
      download-missing     dedup, short lock holds — not a whole-batch hold).
  #19 decline-all        — per-ROW transaction around _set_pending_update_decision
                           (atomic per row, consistent with accept-all).

NOTE — #17 (accept-all holds one BEGIN IMMEDIATE for its entire 167-line
edition-scoped loop → write-lock starvation on a very large bulk-accept) is
DEFERRED: it is a PLAUSIBLE perf concern, and narrowing it requires restructuring
that heavily-audited loop into per-row transactions — the regression risk to a
real user data path outweighs a rare lock stall on a single-tenant homelab.
"""
from __future__ import annotations

import re
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
API = (REPO / "app" / "web" / "api.py").read_text()


def _func_body(name: str) -> str:
    i = API.index(f"async def {name}(")
    # crude slice to the next top-level handler def — enough to scope the asserts.
    nxt = API.find("\n    async def ", i + 1)
    return API[i:nxt if nxt != -1 else i + 8000]


def test_sync_now_serializes_check_then_insert():
    body = _func_body("api_sync_now") if "async def api_sync_now(" in API else None
    # the endpoint may be named differently; fall back to the SELECT marker.
    if body is None:
        i = API.index("Manual sync queued by")
        body = API[i - 1500:i]
    assert "with get_conn(db) as conn, transaction(conn):" in body, (
        "/api/sync/now must wrap its dedup SELECT + INSERT in a transaction "
        "(mirror the cron _enqueue_sync v1.22.33 fix)")


def test_download_batch_enqueue_is_per_item_transaction():
    body = _func_body("api_library_download_batch")
    # the enqueue call is wrapped in a per-item `with transaction(conn):`.
    assert "with transaction(conn):" in body
    # and it's INSIDE the per-item loop (after the item validation), not the whole
    # batch — the transaction open must come after `for it in items:`.
    loop = body.index("for it in items:")
    txn = body.index("with transaction(conn):", loop)
    enqueue = body.index("_enqueue_download(", txn)
    assert loop < txn < enqueue


def test_download_missing_enqueue_is_per_row_transaction():
    body = _func_body("api_library_download_missing")
    loop = body.index("for r in rows:")
    txn = body.index("with transaction(conn):", loop)
    enqueue = body.index("_enqueue_download(", txn)
    assert loop < txn < enqueue


def test_decline_all_decision_is_per_row_transaction():
    body = _func_body("api_decline_all_updates")
    loop = body.index("for tup in tuples:")
    txn = body.index("with transaction(conn):", loop)
    call = body.index("_set_pending_update_decision(", txn)
    assert loop < txn < call
