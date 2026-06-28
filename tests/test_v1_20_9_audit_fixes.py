"""v1.20.9 — silent-bug audit fixes (post-v1.20.x churn).

Audit findings against the v1.20.2 new_theme_available resolver:

  Finding 1 (MED, data corruption): the resolver's kind-blind delegate
  write could flip a coexisting per-section DECLINED upstream change to
  accepted. Fixed by writing inline, scoped to new_theme_available.

  Finding 3 (LOW): download-backup cleared the pill even when zero jobs
  were enqueued.

  LOW-1 (class-9 visibility): the coalescer buffer-fail breadcrumb logged
  at debug.

NOTE: v1.20.10 then re-architected the resolver — it moved to
core.sync.resolve_new_theme_pending_update and now fires from the
worker's download-success path (not the endpoints), which subsumes
Finding 3 (the endpoints no longer resolve at click at all) and carries
the Finding 1 guard. Those behavioral tests now live in
test_v1_20_10_resolve_on_download_success.py. This file retains the
LOW-1 guard, which is independent of that refactor.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── LOW-1: coalescer buffer-fail logs at warning (class-9 visibility) ──


def test_coalescer_buffer_fail_logs_at_warning():
    notify_src = (REPO / "app" / "core" / "notify.py").read_text()
    idx = notify_src.index("notify.dispatch_coalesced buffer failed")
    line_start = notify_src.rfind("\n", 0, idx)
    line = notify_src[line_start:idx]
    assert "log.warning" in line, (
        "v1.20.9: the coalescer buffer-fail breadcrumb must log at warning "
        "(it can jam the window + dark-hole notifications — class-9 needs it "
        "visible at the default level, not debug)"
    )


def test_v1_20_9_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
