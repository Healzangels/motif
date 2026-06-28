"""v1.15.104 — _do_place outcome stamp wrapped in transaction (AUDIT_WORKER M2).

AUDIT_WORKER.md M2: the place-outcome bookkeeping block at
worker.py:1729+ ran up to 3 separate execute() calls outside
any `transaction()` — UPDATE local_files + optionally one of
two UPDATE plex_items branches (existing_theme or
plex_has_theme reason).

Under heavy load (many place jobs concurrent with plex_enum
reads), a concurrent reader could see local_files.last_place_attempt_*
updated but plex_items.local_theme_file still stale —
inconsistent observable state, briefly.

The v1.15.94 follow-up bumped the surrounding except handler
from log.debug → log.warning to make stamp failures visible.
This commit closes the second half of M2: wrapping the writes
in `transaction(conn)` so the multi-row update is atomic.

## Why didn't v1.15.37's lint catch this?

The v1.15.37 `test_all_multi_write_get_conn_blocks_now_use_transaction`
test scans only `app/web/api.py` — worker.py is out of scope.
The audit doc's M2 finding was the only signal that this
specific worker.py block needed the wrap.

## Tests

Static guard that the place-outcome stamp block opens a
`get_conn ... transaction()` pair.
"""

from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
WORKER_PY = REPO / "app" / "core" / "worker.py"


def test_place_outcome_stamp_block_uses_transaction():
    """The `_do_place` post-place outcome stamping block at
    `# v1.11.24: stamp the place outcome` must open the
    get_conn inside a `transaction(conn)` so the multi-UPDATE
    sequence is atomic."""
    src = WORKER_PY.read_text()
    anchor = src.index("# v1.11.24: stamp the place outcome")
    # The try/except block follows immediately. Capture enough
    # to find the `with get_conn(...)` opener that follows.
    block = src[anchor:anchor + 2500]
    # The block must have a `with get_conn(...) as conn, \
    # transaction(conn):` form OR an inline `transaction(conn):`
    # right after the get_conn.
    assert "transaction(conn)" in block, (
        "v1.15.104: the place outcome stamp block at "
        "worker.py:`v1.11.24:` must wrap its multi-UPDATE "
        "writes in `transaction(conn)`. Pre-fix the 3 separate "
        "execute() calls (local_files + optionally one of two "
        "plex_items UPDATEs) were observable to concurrent "
        "readers mid-write. AUDIT_WORKER.md M2."
    )


def test_place_outcome_stamp_log_warning_preserved():
    """v1.15.94 bumped the except handler to log.warning.
    v1.15.104 must not regress that — both fixes compose."""
    src = WORKER_PY.read_text()
    # Scan the full `_do_place` body — fixed-width windows from
    # the `# v1.11.24:` anchor stopped reaching the except handler
    # once v1.15.91/.94/.104 narratives accumulated.
    fn_start = src.index("def _do_place(")
    fn_end = src.index("\n    def ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    assert 'log.warning("place outcome stamp failed' in fn_body, (
        "v1.15.104 regression: v1.15.94's log.warning for the "
        "stamp except handler must remain. The v1.15.104 "
        "transaction wrap is orthogonal to v1.15.94's log level."
    )
