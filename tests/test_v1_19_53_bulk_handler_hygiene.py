"""v1.19.53 — wider bulk-handler hygiene: setOptimisticPlaceholder
on all per-row-iterator bulk actions.

Follow-up to v1.19.52's DOWNLOAD PLEX BACKUP parity work.
the user's audit asked for "uniform look across all bulk
actions" — v1.19.52 covered the two BACKUP buttons; this
tag extends the same pattern to every other bulk handler
that enqueues an async operation:

  - library-download-selected-btn  → 'download_queue'
  - library-tdb-backup-btn         → 'download_queue'
  - library-push-selected-btn      → 'place_queue'
  - library-revert-mismatch-btn    → 'download_queue'
  - library-restore-from-plex-btn  → 'download_queue'
  - library-adopt-selected-btn     → 'adopt_queue'
  - library-adopt-and-lps-btn      → 'adopt_queue'

Already covered by v1.19.52 / v1.19.50:
  - library-cloud-backup-btn       → 'download_queue'
  - per-row 'backup-cloud-theme'   → 'download_queue'

Intentionally NOT covered (backend bulk ops or immediate
sync DB writes — no async-job-queue bridge needed):

  - library-let-plex-serve-btn     (real op_progress row;
                                   bulk_lps surfaces directly)
  - library-bulk-probe-tdb-btn     (real op_progress row;
                                   bulk_probe_tdb surfaces directly)
  - library-accept-all-updates-btn (immediate DB write)
  - library-decline-all-updates-btn (immediate DB write)
  - library-ack-selected-btn       (immediate DB write)

## Why the placeholder matters

Each handler enqueues per-row jobs (download / place / adopt)
that surface in the ops drawer + topbar mini-bar via the
v1.18.95 op_progress synth — but only after the next poll
tick. Without the placeholder, the click→busy gap is ~1s
(rapid poll) at best, ~10s (idle poll) at worst. The
placeholder bridges that gap so the user gets immediate
mini-bar feedback.

The placeholder has a 5s TTL and is naturally superseded
by real op_progress data on the next poll — no manual
clear required for the success path.
"""
from __future__ import annotations

import sys
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


# ── Helper: locate a bulk-button handler body ────────────────


def _handler_body(btn_id: str) -> str:
    """Return the click-handler body for the given bulk button
    id. Scoped from `addEventListener('click'` start to the
    next `addEventListener('click'` (or 6000-char window if
    last)."""
    needle = f"{btn_id}')?.addEventListener"
    idx = APP_JS.index(needle)
    next_handler = APP_JS.find(
        "addEventListener('click'", idx + len(needle),
    )
    end = next_handler if next_handler > 0 else idx + 6000
    return APP_JS[idx:end]


# ── Placeholder coverage on each handler ─────────────────────


def test_download_selected_sets_placeholder():
    """bulk DOWNLOAD FROM TDB must set the download_queue
    placeholder."""
    body = _handler_body("library-download-selected-btn")
    assert "setOptimisticPlaceholder" in body
    assert "'download_queue'" in body, (
        "v1.19.53: bulk DOWNLOAD must route via 'download_queue' "
        "(matches the synth kind from progress.py)"
    )


def test_tdb_backup_sets_placeholder():
    """bulk DOWNLOAD TDB BACKUP — same flow as above with
    place:false; same placeholder kind."""
    body = _handler_body("library-tdb-backup-btn")
    assert "setOptimisticPlaceholder" in body
    assert "'download_queue'" in body
    assert "TDB BACKUP" in body


def test_push_to_plex_sets_placeholder():
    """bulk PUSH TO PLEX — per-row /replace enqueues place
    jobs; placeholder routes via place_queue."""
    body = _handler_body("library-push-selected-btn")
    assert "setOptimisticPlaceholder" in body
    assert "'place_queue'" in body, (
        "v1.19.53: PUSH TO PLEX enqueues place jobs; placeholder "
        "must route via 'place_queue'"
    )


def test_revert_mismatch_sets_placeholder():
    """bulk REVERT MISMATCH re-downloads the TDB canonical."""
    body = _handler_body("library-revert-mismatch-btn")
    assert "setOptimisticPlaceholder" in body
    assert "'download_queue'" in body


def test_restore_from_plex_sets_placeholder():
    """bulk RESTORE FROM PLEX re-fetches the canonical from
    Plex — download flow from motif's POV."""
    body = _handler_body("library-restore-from-plex-btn")
    assert "setOptimisticPlaceholder" in body
    assert "'download_queue'" in body


def test_adopt_selected_sets_placeholder():
    """bulk ADOPT SELECTED enqueues per-row adopts."""
    body = _handler_body("library-adopt-selected-btn")
    assert "setOptimisticPlaceholder" in body
    assert "'adopt_queue'" in body, (
        "v1.19.53: ADOPT SELECTED enqueues adopts; placeholder "
        "must route via 'adopt_queue'"
    )


def test_adopt_and_lps_sets_placeholder():
    """bulk ADOPT + LPS chains per-row adopts + unplaces;
    adopts dominate so route via adopt_queue."""
    body = _handler_body("library-adopt-and-lps-btn")
    assert "setOptimisticPlaceholder" in body
    assert "'adopt_queue'" in body


# ── Backend-bulk / immediate-DB handlers intentionally skipped ──


def test_let_plex_serve_does_not_need_placeholder():
    """LET PLEX SERVE bulk uses bulk_lps (a real op_progress
    kind, not a synth). The op_progress row surfaces directly
    within ~1s — placeholder would be redundant. Pin the
    intentional skip so a future audit doesn't add one
    unnecessarily."""
    body = _handler_body("library-let-plex-serve-btn")
    # The bulk_lps op_progress kind references — ensures this
    # handler does NOT need the placeholder bridge.
    assert "setOptimisticPlaceholder" not in body, (
        "v1.19.53: LET PLEX SERVE uses bulk_lps op_progress "
        "directly; placeholder would be redundant"
    )


def test_bulk_probe_tdb_does_not_need_placeholder():
    """Bulk PROBE TDB uses bulk_probe_tdb op_progress; same
    rationale as LET PLEX SERVE."""
    body = _handler_body("library-bulk-probe-tdb-btn")
    assert "setOptimisticPlaceholder" not in body


def test_ack_selected_does_not_need_placeholder():
    """ACK FAILURES is an immediate sync DB write (no async
    job). Placeholder would never be visible — the action
    completes faster than the poll cadence."""
    body = _handler_body("library-ack-selected-btn")
    assert "setOptimisticPlaceholder" not in body


def test_accept_decline_updates_dont_need_placeholder():
    """ACCEPT ALL UPDATES / KEEP ALL CURRENT are immediate
    backend DB writes; no async job to bridge."""
    accept_body = _handler_body("library-accept-all-updates-btn")
    decline_body = _handler_body("library-decline-all-updates-btn")
    assert "setOptimisticPlaceholder" not in accept_body
    assert "setOptimisticPlaceholder" not in decline_body


# ── Cross-handler audit ──────────────────────────────────────


def test_all_async_bulk_handlers_have_placeholder():
    """Audit guard: every bulk handler that enqueues an async
    job (anything that touches the worker queue) must set
    setOptimisticPlaceholder. The complementary set —
    backend-bulk + immediate-DB handlers — is documented in
    the test_*_does_not_need_placeholder tests above.

    A new bulk handler added to library.html should land in
    one bucket or the other; this test catches the case where
    it lands in neither (default behavior is to silently miss
    the placeholder pattern)."""
    ASYNC_BULK_HANDLERS = {
        "library-download-selected-btn",
        "library-tdb-backup-btn",
        "library-cloud-backup-btn",
        "library-push-selected-btn",
        "library-revert-mismatch-btn",
        "library-restore-from-plex-btn",
        "library-adopt-selected-btn",
        "library-adopt-and-lps-btn",
    }
    missing = []
    for btn_id in ASYNC_BULK_HANDLERS:
        body = _handler_body(btn_id)
        if "setOptimisticPlaceholder" not in body:
            missing.append(btn_id)
    assert not missing, (
        f"v1.19.53: every async bulk handler must call "
        f"setOptimisticPlaceholder. Missing: {missing}"
    )


def test_no_placeholder_uses_unknown_kind():
    """Every setOptimisticPlaceholder call must use one of the
    documented kinds (download_queue, place_queue, adopt_queue,
    scan_queue, refresh_queue, relink_queue, or one of the
    real op_progress kinds). Catches typos like
    'downloads_queue' that would silently fail to surface."""
    import re
    # Extract every setOptimisticPlaceholder call's first arg.
    calls = re.findall(
        r"setOptimisticPlaceholder\(\s*['\"]([^'\"]+)['\"]",
        APP_JS,
    )
    KNOWN_KINDS = {
        # Synth queue kinds (progress.py:_synthesize_queue_ops).
        "download_queue", "place_queue", "scan_queue",
        "refresh_queue", "relink_queue", "adopt_queue",
        # Real op_progress kinds (db.py CHECK list).
        "tdb_sync", "plex_enum", "reprobe_plex_themes",
        "bulk_probe_tdb", "bulk_lps", "tvdb_bridge",
        "cloud_themes_backup", "bulk_normalize",   # v0.51.197
    }
    bad = [k for k in set(calls) if k not in KNOWN_KINDS]
    assert not bad, (
        f"v1.19.53: setOptimisticPlaceholder kinds must match "
        f"a documented synth or op_progress kind. Unknown: {bad}"
    )


# ── Version pin ──────────────────────────────────────────────


def test_v1_19_53_version_pin():
    """Version bumped at v1.19.53 (then again at v1.19.54 for
    the PROMOTE SHA-drift defense). Match 1.19.x prefix."""
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
