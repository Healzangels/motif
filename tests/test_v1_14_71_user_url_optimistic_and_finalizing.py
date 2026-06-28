"""v1.14.71 — user-URL optimistic placeholder + "(finalizing)" mini-bar suffix.

the user v1.14.67 follow-up:

> "long sitting at 100% download, and didn't see a placing
>  after doing a user url"

Two related UX gaps surfaced when the user set a user URL on a row:

## Issue 1: click → mini-bar gap

The /api/items/{mt}/{id}/override POST returns immediately, but
the topbar mini-bar didn't light up until the worker claimed
the download job (~1s worker idle wait + /api/progress poll
round-trip). The window felt like "did my click land?"

v1.14.71 wires `setOptimisticPlaceholder('download_queue', '//
THEME DOWNLOAD QUEUED')` right after the override save. The
real `download_queue` synth row hands off the placeholder by
matching kind in ops.js renderTopbar.

## Issue 2: "long sitting at 100%"

yt-dlp emits `bar_pct = 1.0` when the network transfer
completes, but the post-processing phase (mux / hash / sidecar
write / place enqueue) can take several more seconds before
the worker flips `jobs.status` to 'done'. Pre-fix the mini-bar
sat at "Downloading: X" 100% for that whole window with no
cue that anything was still happening, and the subsequent
place job (typically <2s) often completes within a single
poll interval — so the user never sees a "placing" transition
either.

v1.14.71's `_synthesize_queue_ops` appends "(finalizing)" to
the running download label when:
  - the burst has exactly one running download (avoids false
    positives in concurrent-downloads cases), AND
  - the active job's _DOWNLOAD_PROGRESS fraction has reached
    >= 0.99.

The label becomes "Downloading: 100 Foot Wave (finalizing)" so
the user sees the worker is still doing post-download work.
"""
from __future__ import annotations

import re
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
JS = REPO / "app" / "web" / "static" / "app.js"
PROGRESS_PY = REPO / "app" / "core" / "progress.py"


# ── Issue 1: optimistic placeholder on user-URL set ───────────


# v1.19.87: these three originally anchored on the override-dlg
# submit (`/api/items/{mt}/{id}/override`), which was deleted as
# dead code. The v1.14.71 "light the mini-bar on user-URL save" UX
# was relocated to the LIVE manual-url-dlg submit
# (`/api/plex_items/{rk}/manual-url`) — where it actually fires now
# — so these guards re-anchor there.
_MANUAL_URL_POST = (
    "await api('POST', `/api/plex_items/${encodeURIComponent(rk)}"
    "/manual-url`, body);"
)


def test_user_url_save_sets_optimistic_placeholder():
    """The manual-url POST handler must call setOptimisticPlaceholder
    immediately on success so the topbar mini-bar lights up while
    the worker is still picking up the download job."""
    js = JS.read_text()
    anchor = js.index(_MANUAL_URL_POST)
    block = js[anchor:anchor + 1500]
    # Placeholder call must follow the await.
    assert "setOptimisticPlaceholder(" in block
    # Kind must match the synth row's kind so the handoff works.
    assert "'download_queue'" in block
    # Visible label.
    assert "'// THEME DOWNLOAD QUEUED'" in block


def test_user_url_placeholder_call_is_after_await():
    """Order matters: the placeholder must be set AFTER the API
    call succeeds (not before — otherwise it lingers if the
    server rejects the URL)."""
    js = JS.read_text()
    anchor = js.index(_MANUAL_URL_POST)
    block = js[anchor:anchor + 1500]
    await_idx = block.index("await api('POST', `/api/plex_items/")
    placeholder_idx = block.index("setOptimisticPlaceholder(")
    assert await_idx < placeholder_idx, (
        "Placeholder call must follow the await — pre-fix order "
        "would set the placeholder even on a failed save."
    )


def test_user_url_placeholder_swallows_drawer_errors():
    """The motifOps API may not be loaded on every page (some
    minimal templates skip the drawer init). Guard the
    placeholder call so a missing window.motifOps doesn't break
    the success-path UI flow."""
    js = JS.read_text()
    anchor = js.index(_MANUAL_URL_POST)
    block = js[anchor:anchor + 1500]
    # The presence guard.
    assert "window.motifOps && window.motifOps.setOptimisticPlaceholder" in block
    # The try/catch wrapper.
    assert "try {" in block and "} catch (_) " in block


# ── Issue 2: "(finalizing)" suffix at >=99% ───────────────────


def test_synth_label_appends_finalizing_when_dl_at_99pct():
    """The single-download case appends "(finalizing)" to the
    running label once the active job's yt-dlp fraction reaches
    >= 0.99."""
    py = PROGRESS_PY.read_text()
    # Anchor on the v1.14.71 marker.
    anchor = py.index(
        'v1.14.71: append "(finalizing)" when the active job\'s'
    )
    block = py[anchor:anchor + 2500]
    # The single-running guard.
    assert "if running_n == 1:" in block
    # The 99% threshold.
    assert "min(fractions) >= 0.99" in block
    # The label mutation.
    assert 'running_label = f"{running_label} (finalizing)"' in block


def test_finalizing_suffix_only_when_running_n_is_one():
    """Concurrent-downloads safety: when running_n > 1, the
    suffix must NOT fire — one job at 99% next to one at 30%
    would otherwise mislabel the whole burst."""
    py = PROGRESS_PY.read_text()
    anchor = py.index(
        'v1.14.71: append "(finalizing)" when the active job\'s'
    )
    block = py[anchor:anchor + 2500]
    # The guard is written as `if running_n == 1:` (NOT `>=`).
    pattern = re.compile(r"if\s+running_n\s*==\s*1\s*:")
    assert pattern.search(block), (
        "v1.14.71 finalizing suffix must be gated on "
        "running_n == 1, not >= 1, to avoid mislabeling "
        "concurrent-downloads bursts."
    )


def test_finalizing_suffix_swallows_progress_dict_errors():
    """The finalizing branch reads from _DOWNLOAD_PROGRESS (a
    process-local dict written by the yt-dlp progress hook).
    Any KeyError / TypeError must be swallowed — the suffix is
    decorative and must never raise on the /api/progress hot
    path."""
    py = PROGRESS_PY.read_text()
    anchor = py.index(
        'v1.14.71: append "(finalizing)" when the active job\'s'
    )
    block = py[anchor:anchor + 2500]
    # The try/except wrapper. v1.18.99 added `as e:` capture so
    # the except branch can log.debug the exception — accept
    # either shape.
    assert "try:" in block
    assert ("except Exception:" in block
            or "except Exception as e:" in block), (
        "v1.14.71 + v1.18.99: the finalizing branch must keep "
        "its broad except (decorative; cannot raise) — v1.18.99 "
        "added `as e:` to feed log.debug for class-9 minimum"
    )


# ── Integration: the v1.13.18 (6C) bar_pct blender stays untouched ──


def test_bar_pct_blender_still_lives_in_build_queue_detail():
    """v1.14.71 added a label-only suffix; the bar_pct blending
    in _build_queue_detail must stay intact (it's the source of
    the real progress data the suffix is reading from).
    v1.15.37: the dict-access pattern moved from a list-comp
    with two `.get(jid)` calls per item to a single
    `snapshot_download_progress(running_dl_jobs)` helper that
    snapshots under a threading.Lock — closes the writer/reader
    race that pre-fix could length-mismatch fractions vs
    running_dl_jobs."""
    py = PROGRESS_PY.read_text()
    fn_start = py.index("def _build_queue_detail(")
    fn_body = py[fn_start:fn_start + 2000]
    # v1.15.37: the blender now snapshots via the lock-guarded
    # helper instead of touching _DOWNLOAD_PROGRESS directly.
    assert "snapshot_download_progress(running_dl_jobs)" in fn_body
    assert 'detail["bar_pct"]' in fn_body
