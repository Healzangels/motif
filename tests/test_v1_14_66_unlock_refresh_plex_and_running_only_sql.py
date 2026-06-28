"""v1.14.66 — unlock dash REFRESH PLEX during TDB sync + narrow
plex_enum_active SQL to running-only.

Two the user screenshot reproes on v1.14.65:

## Bug 1: REFRESH PLEX button locks during a SYNC THEMERRDB click

v1.14.62 fixed the LABEL (no longer flips to "// REFRESHING
PLEX…" mid-TDB-sync) and v1.14.65 added a tooltip explaining the
lock — but the lock itself was wrong. The disable predicate
`themerrdbBusy || plexEnumBusy` was originally added in v1.13.63
on the rationale of "avoid SQLite writer-lock contention." That
rationale is unsound: the long worker thread is single-threaded
(worker.py:_LONG_JOB_TYPES, one thread serializes
sync/plex_enum/scan), so a plex_enum queued during a TDB sync
simply waits in queue and runs after the sync finishes — no
concurrent writer contention is even possible. v1.11.35's marker
on the topbar SQL spells this out explicitly: "the worker is
single-threaded so plex_enum doesn't start until sync finishes".

v1.14.66 drops `themerrdbBusy` from the disable predicate.
`plexEnumBusy` (the actual operation) stays. The
`themerrdbBusy`-branch tooltip is also retired since the button
no longer disables on that signal.

## Bug 2: queueing a 2nd library scan locks ALL library tabs' REFRESH

The `enum_running_rows` SQL in api.py:_stats_sync was matching
both `pending` and `running` jobs even though the variable name
and every downstream consumer treat it as running-only:

  - per-section REFRESH lock: `enumSectionIds.has(sid)` should
    only lock the actually-running section.
  - per-tab `myTabBusy = enumActive[tab][variant]` should only be
    true when that tab+variant is actually enumerating.
  - `enumTabsActive = ['movies','tv','anime'].filter(...).length`
    feeds `globalEnumPipeline` when > 1 — and that lights up
    every tab's REFRESH button via `libRefreshBusy`.

the user repro: queue REFRESH MOVIES while a REFRESH TV is running.
The pending MOVIES job lit up `enumActive.movies` AND the running
TV kept `enumActive.tv` lit, pushing `enumTabsActive = 2` →
`globalEnumPipeline = true` → EVERY library tab's REFRESH locked.
Single-thread worker means at most one is actually running at any
moment, so the pending job shouldn't have surfaced as "active"
yet. Pending visibility is already exposed via the separate
`plex_enum_in_flight` count (used for `layoutLocked` schema
protection, mini-bar fallback, topbar dot count fallback).

v1.14.66 narrows the SQL to `status = 'running'` only. Variable
name and downstream semantics now agree.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
JS = REPO / "app" / "web" / "static" / "app.js"
API = REPO / "app" / "web" / "api.py"


# ── Bug 1: dash REFRESH PLEX no longer locks on themerrdbBusy ──


def test_dash_refresh_plex_disable_predicate_drops_themerrdb_busy():
    """The disable assignment must read `dashSyncPlexBtn.disabled
    = plexEnumBusy;` — `themerrdbBusy` is dropped. The long worker
    serializes sync/plex_enum naturally; queueing a Plex refresh
    during a TDB sync is safe."""
    js = JS.read_text()
    # Anchor on the v1.14.66 marker so the assertion stays
    # localized to the dashSyncPlexBtn block.
    anchor = js.index("v1.14.66: dropped themerrdbBusy from the disable predicate")
    block = js[anchor:anchor + 2000]
    # The new (narrowed) disable line.
    assert "dashSyncPlexBtn.disabled = plexEnumBusy;" in block
    # The old (broader) line must NOT appear anywhere in the block.
    assert "dashSyncPlexBtn.disabled = themerrdbBusy || plexEnumBusy" not in block


def test_dash_refresh_plex_writer_lock_tooltip_retired():
    """The "Locked while THEMERRDB SYNC runs (avoid SQLite
    writer-lock contention)" tooltip is retired — the underlying
    lock is gone, so the tooltip can't fire. Pin both the user-
    facing tooltip string AND the assignment site so a future
    regression that re-adds the lock + tooltip together fails
    here. (The phrase "writer-lock contention" still appears in a
    historical comment explaining WHY the lock was wrong; only
    the tooltip-assignment form is asserted absent.)"""
    js = JS.read_text()
    # The tooltip-assignment form (the load-bearing site).
    assert "dashSyncPlexBtn.title =\n            'Locked while" not in js
    assert "dashSyncPlexBtn.title = 'Locked while" not in js
    # The visible string itself, in any string-literal form.
    assert "'Locked while THEMERRDB SYNC runs" not in js
    assert '"Locked while THEMERRDB SYNC runs' not in js
    # The else-if branch keying on themerrdbBusy is gone too.
    assert "} else if (themerrdbBusy)" not in js


# ── Bug 2: enum_running_rows SQL is running-only ───────────────


def test_enum_running_rows_sql_filters_to_running_only():
    """The `enum_running_rows` query in api.py:_stats_sync must
    filter on `status = 'running'` only, NOT
    `status IN ('pending','running')`. Pending jobs surface via
    the separate `plex_enum_in_flight` count + (post-v1.14.73)
    the parallel `plex_enum_pending` map for per-tab/per-section
    same-tab locking."""
    py = API.read_text()
    # Anchor on the actual variable assignment (not the long
    # marker comment) so the slice always reaches the SQL body
    # even after future marker additions.
    anchor = py.index("enum_running_rows = conn.execute(")
    block = py[anchor:anchor + 800]
    # The narrowed predicate must be present.
    assert "AND j.status = 'running'" in block
    # The pre-fix predicate must NOT be present in this block.
    assert "AND j.status IN ('pending', 'running')" not in block
    # The v1.14.66 marker still lives in the broader comment
    # block above the assignment.
    full = py[max(0, anchor - 3000):anchor]
    assert "v1.14.66: narrowed to status='running' only" in full


def test_enum_running_rows_variable_still_named_running():
    """Sanity check — the variable name `enum_running_rows` was
    misleading when the SQL included pending. Now the name and
    semantics agree. If a future change widens the SQL again,
    this assertion + the SQL one above pin the consistency."""
    py = API.read_text()
    # Variable assignment is preserved.
    assert "enum_running_rows = conn.execute(" in py


def test_pending_visibility_still_available_via_separate_count():
    """The narrowed SQL doesn't lose pending visibility — callers
    that need pending+running can read the separate
    `plex_enum_in_flight` field (used today by layoutLocked,
    mini-bar, and topbar dot fallback). Pin the contract that
    the field is still exposed in the /api/stats response."""
    py = API.read_text()
    assert '"plex_enum_in_flight": row["plex_enum_in_flight"]' in py


# ── Cross-cutting: single-thread worker premise is intact ──────


def test_long_worker_serialization_premise_documented():
    """Both fixes lean on the long worker being single-threaded
    (one thread handles sync/plex_enum/scan in queue order). If a
    future refactor adds parallelism here, both fixes need to be
    revisited — Bug 1's writer-lock concern would re-emerge, and
    Bug 2's narrowing would mis-represent simultaneously running
    sections.

    Pin the documented premise in worker.py so the tradeoff is
    surfaced when someone tries to multi-thread the long worker."""
    worker_py = (REPO / "app" / "core" / "worker.py").read_text()
    # The comment that explains the premise.
    assert (
        "Long worker: single thread (multi-thread would contend on"
        in worker_py
    )
    assert "writer locks against itself for sync/plex_enum/scan)"\
        in worker_py
    # The job-type tuple itself.
    assert '_LONG_JOB_TYPES = ("sync", "plex_enum", "scan")' in worker_py
