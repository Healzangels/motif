"""v1.13.80 — drawer verb clarity pass: REFRESH QUEUE → RE-SCAN QUEUE.

The 3-verb taxonomy was settled in v1.13.70/71 (handoff context):

  SYNC      = TDB metadata pull          (single op)
  REFRESH   = Plex section enumeration   (per-section ×N)
  RE-SCAN   = Plex per-folder metadata
              nudge that fires after
              every successful place      (per-place auto-trigger)

After v1.13.71 the user-visible surfaces mostly aligned, with one
straggler: the post-place nudge's drawer card title still read
"REFRESH QUEUE" while the queued/running labels read "Plex re-scan
queued" / "Nudging Plex to re-scan". Same op described with two
verbs in the same drawer card → confusion with the user-action
"// REFRESH PLEX" (which is the SECTION enum, plex_enum, NOT the
post-place nudge).

v1.13.80 renames the kind label (no schema/job-type change — only
the user-visible string).

These tests pin the rename in both surfaces (Python + JS) and the
internal kind id staying refresh_queue for state-tracking
continuity.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


def test_progress_label_map_uses_rescan_queue():
    """app/core/progress.py label_map['refresh'] kind label must
    be 'RE-SCAN QUEUE' (was 'REFRESH QUEUE'). Stage label
    'Nudging Plex to re-scan' stays — already on-message."""
    src = (REPO / "app" / "core" / "progress.py").read_text()
    # New value.
    assert '"refresh":  ("RE-SCAN QUEUE",  "Nudging Plex to re-scan")' in src
    # Pre-fix value pattern must NOT appear (the literal tuple form
    # — comments mentioning the rename are fine).
    assert '("REFRESH QUEUE",' not in src


def test_progress_queued_label_map_keys_on_rescan_queue():
    """The queued_label_map keys on the kind label string. After
    v1.13.80 the key must be 'RE-SCAN QUEUE' to match the rename;
    otherwise the lookup misses and queued ops fall back to the
    generic '<stage_label> queued' fallback."""
    src = (REPO / "app" / "core" / "progress.py").read_text()
    assert '"RE-SCAN QUEUE":  "Plex re-scan queued"' in src
    # Pre-fix key must NOT appear inside the queued_label_map dict.
    qmap_start = src.index("queued_label_map = {")
    qmap_end = src.index("}", qmap_start)
    qmap_block = src[qmap_start:qmap_end]
    assert "REFRESH QUEUE" not in qmap_block


def test_ops_js_kind_label_uses_rescan_queue():
    """Frontend KIND_LABEL must use RE-SCAN QUEUE so the drawer
    card header matches the Python-side label."""
    src = (REPO / "app" / "web" / "static" / "ops.js").read_text()
    # New mapping must be present.
    assert "refresh_queue:  'RE-SCAN QUEUE'" in src
    # Pre-fix mapping must NOT appear (the literal entry form —
    # comments mentioning the rename are fine).
    assert "refresh_queue:  'REFRESH QUEUE'" not in src


def test_internal_kind_id_unchanged():
    """The internal `refresh_queue` JS id (used in state tracking,
    DOT_KIND, queue maps, etc.) must NOT be renamed — only the
    user-visible KIND_LABEL string changes. Rename would cascade
    through state and break in-flight tracking."""
    src = (REPO / "app" / "web" / "static" / "ops.js").read_text()
    # The internal id appears in TONE_BY_KIND + KIND_LABEL. The tone
    # VALUE is incidental to this guard (it's the `refresh_queue` id
    # we're pinning) — v1.19.88 retoned the queues 'warn' → 'queue'.
    assert "refresh_queue:  'queue'" in src  # TONE_BY_KIND entry
    assert "refresh_queue:  'RE-SCAN QUEUE'" in src  # KIND_LABEL entry
    # No accidental rename of the internal id.
    assert "rescan_queue" not in src
    assert "re_scan_queue" not in src


def test_taxonomy_verbs_are_distinct_in_user_surfaces():
    """Cross-cutting guard: confirm the 3 verbs map to different
    operations in user-visible labels.
      SYNC    → THEMERRDB SYNC (kind=tdb_sync)
      REFRESH → PLEX REFRESH   (kind=plex_enum)
      RE-SCAN → RE-SCAN QUEUE  (kind=refresh_queue, post-place)
    Catches a future regression that, e.g., reverts plex_enum to
    'PLEX SYNC' or refresh_queue to 'PLEX REFRESH'."""
    ops = (REPO / "app" / "web" / "static" / "ops.js").read_text()
    # Each verb's canonical label must be in the KIND_LABEL block.
    kind_start = ops.index("const KIND_LABEL = {")
    kind_end = ops.index("};", kind_start)
    block = ops[kind_start:kind_end]
    assert "tdb_sync:            'THEMERRDB SYNC'" in block
    assert "plex_enum:           'PLEX REFRESH'" in block
    assert "refresh_queue:  'RE-SCAN QUEUE'" in block
