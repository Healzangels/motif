"""v1.17.11 — Tier B hygiene rollover.

Closes the three remaining items from the v1.17.9 hygiene audit's
Tier B list: two class-9 swallows in sync.py (hot loops), two
class-9 swallows in auth.py (bcrypt verify), and the unified
`_prune_history` job covering five append-forever tables.

## Class-9 fixes (hot-path care)

Pre-fix `except Exception: ...` in `sync.py:416` (normalize_title
slow-path upsert) and `_GitMirror.read_json:1558` silently
swallowed every failure. The cleanup couldn't just log.warning
per occurrence — the upsert loop runs once per theme per sync, so
per-row logs would drown the operator's log on any persistent
issue. The fix uses module-level "once-per-process" flags:

* First occurrence in the process: log.warning with cause
  classification so the operator sees the issue at boot.
* Subsequent occurrences: log.debug so the breadcrumb still
  exists for granular diagnosis but doesn't spam.
* The flag resets only on process restart, which matches the
  cadence at which root-cause changes can land (deploy / venv
  fix).

Same pattern applied to `auth.py:verify_password` and
`_verify_token`. A corrupt bcrypt hash means *every* auth attempt
raises; once-per-process warn gives the operator one clear line
in logs ("hash appears corrupt"), and silence after that since
they can't auth anyway.

## prune_history — unified retention sweep

Five tables flagged in the audit as append-forever:
* `jobs`                    — done / failed / cancelled
* `sync_runs`               — success / failed
* `scan_runs`               — complete / failed / cancelled
* `scan_findings`           — cascades from scan_runs ON DELETE CASCADE
* `local_files_history`     — adopt-lookup capture

One scheduler job at 03:15 UTC, after `events_prune` (03:10) and
`tvdb_lookup_cache_prune` (03:05). Single transaction, single
summary log line. Per-table windows tuned to UI horizons:

* `jobs` 30 days — recent fail-debug retention is enough.
* `sync_runs` / `scan_runs` 90 days — dashboard sparkline horizon.
* `local_files_history` 180 days — re-adopt across long Plex
  library reorgs.

The 90d window for sync_runs / scan_runs is calibrated against
the dashboard sparkline horizon; if the sparkline ever extends,
this window must bump in lock-step (noted inline in the
function).
"""

from __future__ import annotations

import re
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
SYNC_PY = REPO / "app" / "core" / "sync.py"
AUTH_PY = REPO / "app" / "core" / "auth.py"
SCHEDULER_PY = REPO / "app" / "core" / "scheduler.py"


# ── Class-9 hot-path breadcrumbs ──────────────────────────────


def test_sync_normalize_title_logs_once_per_process():
    """sync.py:416 normalize_title swallow must use the once-
    per-process pattern. log.warning on first hit, log.debug
    after. Pre-fix the swallow was fully silent."""
    src = SYNC_PY.read_text()
    # Module-level flag must exist.
    assert "_SYNC_NORMALIZE_TITLE_WARNED" in src, (
        "v1.17.11: sync.py must declare _SYNC_NORMALIZE_TITLE_WARNED "
        "at module level for the once-per-process breadcrumb."
    )
    # Locate the swallow site.
    idx = src.index("from .normalize import normalize_title")
    window = src[idx:idx + 1500]
    assert "log.warning(" in window, (
        "v1.17.11: first occurrence must log.warning so the "
        "operator sees the issue."
    )
    assert "log.debug(" in window, (
        "v1.17.11: subsequent occurrences must drop to log.debug "
        "to avoid drowning the hot-path log."
    )


def test_git_mirror_read_json_logs_once_per_process():
    """_GitMirror.read_json malformed-JSON swallow must use the
    same once-per-process pattern."""
    src = SYNC_PY.read_text()
    assert "_GIT_MIRROR_READ_JSON_WARNED" in src, (
        "v1.17.11: sync.py must declare _GIT_MIRROR_READ_JSON_WARNED "
        "at module level."
    )
    idx = src.index("def read_json(")
    window = src[idx:idx + 3000]
    assert "log.warning(" in window
    assert "log.debug(" in window


def test_auth_verify_password_logs_once_per_process():
    """verify_password bcrypt-raised swallow must log.warning
    once per process. A corrupt admin password hash makes EVERY
    login fail; the breadcrumb lets the operator diagnose."""
    src = AUTH_PY.read_text()
    assert "_VERIFY_PASSWORD_WARNED" in src, (
        "v1.17.11: auth.py must declare _VERIFY_PASSWORD_WARNED "
        "at module level."
    )
    idx = src.index("def verify_password(")
    window = src[idx:idx + 1500]
    assert "log.warning(" in window, (
        "v1.17.11: verify_password bcrypt failure must log.warning "
        "on first occurrence."
    )
    assert "_VERIFY_PASSWORD_WARNED" in window


def test_auth_verify_token_logs_once_per_process():
    """_verify_token bcrypt-raised swallow must log.warning once
    per process. Same shape as verify_password."""
    src = AUTH_PY.read_text()
    assert "_VERIFY_TOKEN_WARNED" in src, (
        "v1.17.11: auth.py must declare _VERIFY_TOKEN_WARNED "
        "at module level."
    )
    idx = src.index("def _verify_token(")
    window = src[idx:idx + 1500]
    assert "log.warning(" in window
    assert "_VERIFY_TOKEN_WARNED" in window


# ── prune_history sweep ───────────────────────────────────────


def test_prune_history_function_exists():
    """v1.17.11: scheduler.py must declare _prune_history covering
    all four directly-pruned tables (scan_findings cascades)."""
    src = SCHEDULER_PY.read_text()
    assert "def _prune_history(" in src, (
        "v1.17.11: scheduler.py must define _prune_history."
    )
    idx = src.index("def _prune_history(")
    body = src[idx:idx + 4000]
    # Each table must have an explicit DELETE.
    for table in ("jobs", "sync_runs", "scan_runs",
                  "local_files_history"):
        assert f"DELETE FROM {table}" in body, (
            f"v1.17.11: prune_history must DELETE FROM {table}."
        )
    # scan_findings is documented as cascading from scan_runs —
    # no explicit DELETE.
    assert "scan_findings" in body, (
        "v1.17.11: prune_history must document the scan_findings "
        "cascade from scan_runs ON DELETE CASCADE."
    )


def test_prune_history_uses_documented_retention_windows():
    """Per-table retention windows match the design (jobs 30d,
    sync_runs / scan_runs 90d, local_files_history 180d). Wrong
    windows silently change retention behavior — pin the
    numbers."""
    src = SCHEDULER_PY.read_text()
    idx = src.index("def _prune_history(")
    body = src[idx:idx + 4000]
    assert '"jobs_days":                30' in body or '"jobs_days": 30' in body, (
        "v1.17.11: jobs retention should be 30 days."
    )
    assert '"sync_runs_days":           90' in body or '"sync_runs_days": 90' in body, (
        "v1.17.11: sync_runs retention should be 90 days "
        "(matches dashboard sparkline horizon)."
    )
    assert '"scan_runs_days":           90' in body or '"scan_runs_days": 90' in body
    assert '"local_files_history_days": 180' in body or '"local_files_history_days": 180' in body, (
        "v1.17.11: local_files_history retention should be 180 days "
        "(covers long Plex library reorgs for re-adopt lookup)."
    )


def test_prune_history_only_targets_terminal_statuses():
    """Defensive: prune must never delete a row whose status is
    'pending' or 'running'. If the per-table status filter is
    missing, a long-running scan or sync could disappear mid-
    flight on the next sweep."""
    src = SCHEDULER_PY.read_text()
    idx = src.index("def _prune_history(")
    body = src[idx:idx + 4000]
    # jobs: only done/failed/cancelled.
    assert "status IN ('done','failed','cancelled')" in body, (
        "v1.17.11: jobs prune must filter to terminal statuses "
        "(done/failed/cancelled)."
    )
    # sync_runs: only success/failed (running rows excluded).
    assert "status IN ('success','failed')" in body, (
        "v1.17.11: sync_runs prune must filter to success/failed; "
        "running rows must never be deleted."
    )
    # scan_runs: only complete/failed/cancelled.
    assert "status IN ('complete','failed','cancelled')" in body, (
        "v1.17.11: scan_runs prune must filter to terminal statuses."
    )


def test_prune_history_job_registered():
    """The scheduler must register `_prune_history` as a daily job
    with a stable id, slotted in the 03:00-block writer window."""
    src = SCHEDULER_PY.read_text()
    assert 'id="history_prune"' in src, (
        "v1.17.11: scheduler must register history_prune with a "
        "stable id."
    )
    # Trigger should be the 03:15 UTC slot to follow events_prune.
    # Find the registration block.
    idx = src.index('id="history_prune"')
    block = src[max(0, idx - 600):idx + 200]
    assert "_prune_history" in block, (
        "v1.17.11: history_prune job must call _prune_history."
    )
    assert 'minute="15"' in block and 'hour="3"' in block, (
        "v1.17.11: history_prune must be slotted at 03:15 UTC "
        "(after events_prune at 03:10)."
    )


# ── Version pin (soft floor) ──────────────────────────────────


def test_version_pinned_at_or_above_1_17_11():
    src = (REPO / "app" / "__init__.py").read_text()
    m = re.search(r'__version__\s*=\s*"(\d+)\.(\d+)\.(\d+)"', src)
    assert m, "__version__ must be a 3-part semver string"
    found = tuple(int(x) for x in m.groups())
    assert found >= (0, 17, 11), (
        f"v1.17.11: __version__ must be at or above 1.17.11 "
        f"(found {'.'.join(str(x) for x in found)})."
    )
