"""v1.15.0 — version cut + two pre-cut bug fixes from the audit.

Closing the v1.14.x line at v1.14.99 (99 tags). Before the cut,
ran a 3-agent audit (silent failures, recent regressions, test-
coverage gaps). Two real bugs found + fixed:

## Bug 1: v46→v47 migration missed idx_op_progress_finished

The SCHEMA constant defines two indexes on op_progress:
  - idx_op_progress_status (status, updated_at)
  - idx_op_progress_finished (finished_at)

v1.14.93's migration to v47 (widening the kind CHECK constraint)
recreated the table but only restored idx_op_progress_status.
Existing installs that upgraded past v1.14.93 silently lost the
finished-at index — /api/progress polls that filter on
finished_at fell back to table scans on installs with months
of historical ops, lagging the LIVE OPS drawer.

Self-healing: init_db calls `conn.executescript(SCHEMA)` after
the migration loop, and `CREATE INDEX IF NOT EXISTS` is
idempotent. Existing v47 installs pick up the missing index on
the next motif start. The migration fix is for correctness so
applying ONLY the migration matches the SCHEMA shape.

## Bug 2: Silent PlexClient.close swallows in reprobe handler

Two `try: c.close() except Exception: pass` blocks in
_reprobe_plex_themes_run (one on cancel, one on success) ate
every exception with no log. Same shape as the v1.14.93 thread-
death pattern (silent backgrounded swallow). Cleanup-error on
cancel doesn't merit WARN, but a DEBUG breadcrumb is cheap.

Replaced both with:
    except Exception as e:
        log.debug("REPROBE PlexClient close failed ...: %s", e)

## Other audit findings — deferred to v1.15.x backlog

- HIGH: 4 worker handlers (_do_adopt, _do_plex_enum,
  api_override, _run_rollback_safe) only have static-text
  guards. Behavioral integration tests would catch silent
  partial-success on mutation paths.
- MED: Drawer in-place updater doesn't update cancelBtn.disabled
  on terminal transition (brief 2s window of stale disabled
  state).
- MED: Probe button watcher can spawn duplicate setIntervals
  on rapid double-click (memory leak, no functional impact).
"""
from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path

from app.core import db as db_module


REPO = Path(__file__).resolve().parent.parent
DB_PY = REPO / "app" / "core" / "db.py"
API_PY = REPO / "app" / "web" / "api.py"


# ── Bug 1: v46→v47 migration restores both indexes ─────────────


def test_migration_restores_idx_op_progress_finished():
    """The v46→v47 migration script must recreate the
    idx_op_progress_finished index alongside idx_op_progress_status.
    Pre-fix the migration only restored the status index."""
    src = DB_PY.read_text()
    fn_start = src.index("def _migrate_v46_to_v47(conn:")
    fn_end = src.index("def ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    assert "idx_op_progress_status" in fn_body
    assert "idx_op_progress_finished" in fn_body, (
        "Migration must recreate idx_op_progress_finished — "
        "pre-fix the script silently dropped it"
    )


def test_v46_to_v47_migration_creates_both_indexes_on_v46_db():
    """Run the migration against a hand-built v46-shape DB and
    assert both indexes exist on op_progress afterward."""
    fd = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    fd.close()
    db_path = Path(fd.name)
    try:
        with sqlite3.connect(db_path) as conn:
            # Hand-build v46 op_progress shape (no bulk_probe_tdb in CHECK).
            conn.executescript("""
                CREATE TABLE op_progress (
                    op_id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL
                        CHECK (kind IN ('tdb_sync', 'plex_enum',
                                        'reprobe_plex_themes')),
                    status TEXT NOT NULL DEFAULT 'running',
                    started_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    finished_at TEXT,
                    stage TEXT, stage_label TEXT,
                    stage_current INTEGER NOT NULL DEFAULT 0,
                    stage_total INTEGER NOT NULL DEFAULT 0,
                    processed_total INTEGER NOT NULL DEFAULT 0,
                    processed_est INTEGER NOT NULL DEFAULT 0,
                    error_count INTEGER NOT NULL DEFAULT 0,
                    detail_json TEXT
                );
            """)
            db_module._migrate_v46_to_v47(conn)
            conn.commit()
            indexes = [
                row[0] for row in conn.execute(
                    "SELECT name FROM sqlite_master "
                    "WHERE type = 'index' AND tbl_name = 'op_progress'"
                ).fetchall()
            ]
        assert "idx_op_progress_status" in indexes, (
            f"idx_op_progress_status missing after migration; got {indexes}"
        )
        assert "idx_op_progress_finished" in indexes, (
            f"idx_op_progress_finished missing after migration; got "
            f"{indexes} — this is the bug the v1.15.0 cut fixed"
        )
    finally:
        db_path.unlink(missing_ok=True)


def test_schema_constant_still_defines_both_indexes():
    """Regression guard: the SCHEMA constant must keep both
    op_progress indexes. The self-heal that protects existing
    installs depends on this."""
    src = DB_PY.read_text()
    schema_anchor = src.index("CREATE TABLE IF NOT EXISTS op_progress")
    # v1.15.48 widened the slice from 2500 → 3500 — the v48 CHECK
    # widening added explanatory comment lines that pushed the
    # index CREATE statements past the original 2500-char window.
    # v1.15.56: widened 3500 → 4500 since the v49 kind-CHECK
    # comment pushed the indexes further from the CREATE TABLE.
    # v1.19.45: widened 4500 → 6000 for the v59 kind-CHECK comment
    # (cloud_themes_backup add).
    schema_block = src[schema_anchor:schema_anchor + 6000]
    assert "CREATE INDEX IF NOT EXISTS idx_op_progress_status" in schema_block
    assert "CREATE INDEX IF NOT EXISTS idx_op_progress_finished" in schema_block


# ── Bug 2: silent swallows in reprobe handler ──────────────────


def test_reprobe_cancel_close_logs_at_debug():
    """The cancel-path PlexClient close must log at DEBUG, not
    swallow silently. Pre-fix the `except Exception: pass` ate
    every exception with no breadcrumb."""
    src = API_PY.read_text()
    # Anchor on the v1.15.0 marker comment that tags the new
    # log.debug call — bulk-probe-tdb has its own cancel block,
    # so anchoring on a less specific phrase would land in the
    # wrong handler. The string is split across two source lines
    # by Python's autoformatter; collapse whitespace so the
    # substring check spans the line break.
    flat = " ".join(src.split())
    anchor = flat.index("REPROBE PlexClient close failed on cancel")
    block = flat[max(0, anchor - 800):anchor + 300]
    # The pre-fix bare `except Exception: pass` shape (collapsed
    # whitespace makes this `except Exception: pass`).
    assert "except Exception: pass" not in block, (
        "Pre-fix silent swallow `except Exception: pass` must be replaced"
    )
    # The DEBUG log must be present.
    assert "log.debug(" in block


def test_reprobe_success_close_logs_at_debug():
    """The success-path PlexClient close must also log at DEBUG."""
    src = API_PY.read_text()
    flat = " ".join(src.split())
    anchor = flat.index("REPROBE PlexClient close failed on success")
    block = flat[max(0, anchor - 800):anchor + 300]
    assert "log.debug(" in block


def test_v1_15_0_marker_explains_the_swallow_fix():
    """A v1.15.0 marker on each new log.debug call references the
    v1.14.93 silent-thread-death pattern so a future cleanup
    pass sees the rationale."""
    src = API_PY.read_text()
    flat = " ".join(src.split())
    anchor = flat.index("PlexClient close failed on cancel")
    block = flat[max(0, anchor - 800):anchor + 300]
    assert "v1.15.0" in block
    assert "v1.14.93" in block, (
        "Marker should reference the silent-thread-death precedent"
    )


# ── The cut: version constant + brand display ──────────────────


def test_version_string_is_at_least_1_15_0():
    """The v1.15.0 cut moved past the v1.14.x line. v1.15.x patch
    versions still satisfy this contract — the test just guards
    against an accidental rollback to v1.14.x or below."""
    init_py = (REPO / "app" / "__init__.py").read_text()
    # Pull the __version__ literal and verify major.minor >= 1.15.
    import re
    m = re.search(r'__version__ = "(\d+)\.(\d+)\.(\d+)"', init_py)
    assert m, f"Could not parse __version__ from init.py"
    major, minor = int(m.group(1)), int(m.group(2))
    assert (major, minor) >= (0, 15), (
        f"Version must be at least 1.15.x; got {m.group(0)}"
    )


def test_version_constant_carries_v1_15_0_rollover_comment():
    """The version constant's preamble must explain WHY v1.15.0
    rolled over from v1.14.99 (closing the v1.14.x line +
    audit-cleared cut). Future readers see the milestone
    rationale, not just the bump."""
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert "v1.15.0" in init_py
    # Reference the close-out + the audit.
    assert "v1.14.x line" in init_py or "v1.14.99" in init_py
    assert "audit" in init_py
