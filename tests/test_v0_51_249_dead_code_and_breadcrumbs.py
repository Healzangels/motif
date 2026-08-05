"""v0.51.249 — remove dead definitions whose comments claimed live protections,
and breadcrumb three log-less swallows.

The dead code itself was harmless. The COMMENTS were not: two of the four
asserted a protection that does not exist, which is worse than no comment at
all because a future reader (or a disk-safety review) would believe them.

  _GIT_MIRROR_MAX_BYTES  "Catches a repo that has grown unreasonably large
                          before it fills disk" — nothing ever read it. The
                          real control is _COMPACT_THRESHOLD_BYTES.
  _GENERAL_JOB_TYPES     "Kept as the union for callers/tests" — zero callers,
                          zero tests. Orphaned by the v1.20.40 pool split.
  _fetch_youtube_oembed  "legacy alias kept so a cached app.js doesn't 404" —
                          that rationale belongs to the ROUTE alias; a nested
                          Python function is unreachable from a browser.
  _UNSELECTED_SAMPLE     leftover from the retired unselected-serves probe.

The three swallows are class 9 (CLAUDE.md: "every defensive except needs a log
line + a functional fallback; bare `except: pass` is a bug"). The notify one is
the sharpest: it DROPS a drained notification batch, inside the function whose
entire v1.20.63 purpose is preventing shutdown batch loss.
"""
from __future__ import annotations

import inspect
import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP = REPO / "app"

DEAD = {
    "_fetch_youtube_oembed": "app/web/api.py",
    "_GIT_MIRROR_MAX_BYTES": "app/core/sync.py",
    "_GENERAL_JOB_TYPES": "app/core/worker.py",
    "_UNSELECTED_SAMPLE": "app/web/api.py",
}


def _live_code(path: Path) -> str:
    """Source with comment lines stripped — a name surviving only in this tag's
    own rationale comment is not a resurrection."""
    return "\n".join(ln for ln in path.read_text().splitlines()
                     if not ln.lstrip().startswith("#"))


def test_the_dead_names_are_gone_from_live_code():
    """Each was grep-verified at zero references before removal."""
    offenders = []
    for name in DEAD:
        for f in APP.rglob("*.py"):
            if f.name == "__init__.py":       # the changelog legitimately names them
                continue
            if name in _live_code(f):
                offenders.append(f"{name} in {f.relative_to(REPO)}")
    assert not offenders, offenders


def test_no_test_depended_on_them():
    """_GENERAL_JOB_TYPES' comment claimed tests used it. They did not — proving
    that here stops anyone restoring it on the strength of the old comment."""
    offenders = [f"{n} in {f.name}" for n in DEAD
                 for f in (REPO / "tests").glob("*.py")
                 if f.name != Path(__file__).name and n in f.read_text()]
    assert not offenders, offenders


def test_the_false_disk_safety_claim_is_gone():
    """The specific harm: a reader doing a disk-safety review would have
    concluded a 500MB hard ceiling protected /config. Nothing enforced it."""
    path = REPO / "app" / "core" / "sync.py"
    # Two traps avoided here, both hit while writing this test:
    #  1. NOT `"500 * 1024 * 1024" not in src` — that literal is ALSO
    #     _SNAPSHOT_MAX_BYTES, a live constant, so the check would fail for an
    #     unrelated reason. That is a broken check, not a guard.
    #  2. the name check must run on COMMENT-STRIPPED source, or this tag's own
    #     rationale comment (which names the removed constant) satisfies it.
    assert "before it fills disk" not in path.read_text(), (
        "the false disk-safety claim is back")
    assert "_GIT_MIRROR_MAX_BYTES" not in _live_code(path)
    src = path.read_text()
    assert "_COMPACT_THRESHOLD_BYTES" in src, (
        "the REAL size control must still exist — it is what the removed "
        "constant was falsely credited with doing")


# ── the three breadcrumbs ────────────────────────────────────────────────

def test_notify_shutdown_flush_reports_a_dropped_batch():
    """THE sharpest one. `db_path = None` makes the gate below discard the
    drained items — in the function whose purpose is preventing that loss. It
    must say so, loudly, with the count."""
    src = inspect.getsource(__import__("app.core.notify", fromlist=["x"]).flush_all_coalesced)
    assert "except Exception:\n                    pass" not in src, "bare swallow returned"
    assert "DROPPING" in src, "the loss path must name itself"
    assert "log.warning" in src, "a dropped batch is not a debug-level event"


def test_dead_rk_cleanup_breadcrumbs_its_audit_write():
    """A destructive placements DELETE whose audit trail vanishes silently is
    the class-9 shape exactly."""
    s = (REPO / "app" / "web" / "api.py").read_text()
    i = s.index("Dead-rk placement cleaned up")
    seg = s[i:s.index("return result", i)]
    assert "except Exception:\n            pass" not in seg
    assert "audit log_event failed" in seg


def test_downscale_duration_probe_breadcrumbs():
    """Its siblings in the same function already log; this one didn't, making a
    failed shrink undiagnosable."""
    src = inspect.getsource(
        __import__("app.core.worker", fromlist=["x"])._downscale_audio_to_fit)
    assert "except Exception:\n            duration = None" not in src
    assert "ffprobe duration probe failed" in src


def test_no_bare_except_pass_left_in_the_touched_functions():
    """Scoped lint: these three functions specifically. A repo-wide ban would be
    a different (larger) argument — this pins what this tag actually fixed."""
    import app.core.notify as notify
    import app.core.worker as worker
    for fn in (notify.flush_all_coalesced, worker._downscale_audio_to_fit):
        src = inspect.getsource(fn)
        assert not re.search(r"except\s+Exception\s*:\s*\n\s*pass\s*\n", src), (
            f"{fn.__name__} still has a bare except: pass")
