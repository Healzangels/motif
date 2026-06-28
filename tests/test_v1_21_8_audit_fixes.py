"""v1.21.8 — silent-failure + bug audit fixes.

A 4-agent audit (silent-fail / mirror-drift / DB-concurrency / frontend)
confirmed the codebase is well-hardened but surfaced 6 real bugs. The
two HIGH ones have dedicated guards elsewhere:
  - inline SRC-chip plex_cloud→P drift → test_v1_20_65
    ::test_js_inline_src_chip_mirrors_sql
  - _migrate_v4_to_v5 FK-OFF no-op → test_v1_19_78
    ::test_widen_migrations_commit_before_fk_off[_migrate_v4_to_v5]

This file pins the MED + LOW fixes:
  M1 — collection place-outcome stamp wrapped in transaction()
  M2 — reconcile_placement_paths per-row writes wrapped in transaction()
  M3 — REPROBE PlexClients closed in a finally (no leak on error path)
  F1 — dashboard event-stream innerHTML hash-guarded (no 30s scroll-jump)
  LOW — test-trigger sidecar-fs OSError now breadcrumbed
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
WORKER_PY = (REPO / "app" / "core" / "worker.py").read_text()
PLEX_ENUM_PY = (REPO / "app" / "core" / "plex_enum.py").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


# ── M1: collection place-outcome stamp is atomic ─────────────

def test_collection_stamp_uses_transaction():
    """The collection plex_upload-skip stamp (UPDATE local_files +
    UPDATE plex_items) must be in one transaction so a reader can't
    observe a half-written row — matching the movie/show path
    (v1.15.104)."""
    # Anchor on the v1.19.61 collection-stamp marker.
    idx = WORKER_PY.index("v1.19.61: stamp 'backup_only' (not the log")
    # Walk back to the get_conn that opens this block.
    pre = WORKER_PY.rindex("with get_conn(self.settings.db_path)", 0, idx)
    opener = WORKER_PY[pre:idx]
    assert "transaction(conn)" in opener, (
        "v1.21.8 M1: collection stamp must wrap both UPDATEs in "
        "transaction(conn)"
    )


# ── M2: reconcile placement writes are atomic ────────────────

def test_reconcile_placement_writes_atomic():
    """The cancel + placement-move + force-place enqueue in
    reconcile_placement_paths must be one transaction so an INSERT
    failure can't leave the row's media_folder moved-but-unqueued
    (silent placement drift)."""
    idx = PLEX_ENUM_PY.index("def reconcile_placement_paths(")
    nxt = PLEX_ENUM_PY.index("\ndef ", idx + 1)
    body = PLEX_ENUM_PY[idx:nxt]
    assert "with transaction(conn):" in body, (
        "v1.21.8 M2: the per-row writes must be wrapped in a "
        "transaction so they commit atomically"
    )
    # The force-place INSERT must sit INSIDE the transaction (before
    # the post-commit log_event).
    txn_at = body.index("with transaction(conn):")
    insert_at = body.index("INSERT INTO jobs", txn_at)
    log_at = body.index('log_event(db_path, level="INFO"', txn_at)
    assert txn_at < insert_at < log_at


# ── M3: REPROBE clients closed on every path ─────────────────

def test_reprobe_clients_closed_in_finally():
    """REPROBE must close its PlexClients in a finally so a mid-run
    exception doesn't leak PROBE_MAX_WORKERS httpx clients."""
    idx = API_PY.index("def _reprobe_plex_themes_run(")
    # Slice to the next MODULE-level def (the function has nested defs).
    nxt = API_PY.index("\ndef ", idx + 1)
    body = API_PY[idx:nxt]
    assert "clients: list = []" in body, (
        "v1.21.8 M3: clients must be defined before the try so the "
        "finally can always close them"
    )
    # A finally block that closes clients must exist.
    fin = body.index("finally:")
    seg = body[fin:fin + 400]
    assert "c.close()" in seg


# ── F1: dashboard event-stream hash-guarded ──────────────────

def test_dashboard_event_stream_hash_guarded():
    """The dashboard event-stream innerHTML write (30s poll) must be
    behind a lastHash dirty-check so it doesn't reset scrollTop each
    tick — mirroring the queue-page #event-stream-full guard."""
    # The dashboard stream uses `streamHtml` + a dataset.lastHash gate.
    idx = APP_JS.index("const streamHtml = evs.events.map")
    seg = APP_JS[idx:idx + 900]
    assert "stream.dataset.lastHash !== streamHtml" in seg
    assert "stream.innerHTML = streamHtml" in seg
    assert "stream.dataset.lastHash = streamHtml" in seg


# ── LOW: test-trigger sidecar-fs OSError breadcrumbed ────────

def test_test_trigger_sidecar_oserror_breadcrumbed():
    """The /api/admin/test-trigger-theme-lost sidecar-fs probe must
    log.debug on OSError (was a bare `except OSError: pass`)."""
    idx = API_PY.index('Path(_folder_path) / "theme.mp3"')
    seg = API_PY[idx:idx + 700]
    assert "except OSError as _fe:" in seg
    assert "log.debug(" in seg
    assert "test-trigger: sidecar-fs check raised" in seg


def test_version_bumped():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
