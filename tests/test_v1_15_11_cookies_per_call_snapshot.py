"""v1.15.11 — bulk-probe cookies per-call snapshot (kills writeback race).

## Pre-fix

The bulk PROBE TDB URLS / REPROBE FAILURES flow runs 3 yt-dlp
probes in parallel (BULK_PROBE_MAX_WORKERS = 3). Each probe is
passed `cookiefile=str(/config/cookies.txt)` — yt-dlp loads the
cookies on entry and writes back any updated session cookies on
exit. With 3 workers sharing the same path:

  worker A:  open → read → close → truncate+write   ─┐
  worker B:                read ─ sees partial file ─┤  RACE
  worker C:                read ─ sees partial file ─┘

The partial-file reads triggered yt-dlp's "does not look like a
Netscape format cookies file" parse error. Affected probes ran
without cookies → age-gated rows came back as `cookies_expired`
→ those rows fell into the indeterminate bucket → their
`failure_kind` did NOT get cleared even when the URL was
actually fine. the user (v1.15.10 docker logs):
    ERROR: '/config/cookies.txt' does not look like a Netscape
    format cookies file

Repeated 4-8 times across ~150 probes — ~5% indeterminate-rate
floor that the operator can't drive lower without a code fix.

## Fix

Inside `_probe_one` (the per-row callable handed to the worker
pool), snapshot the cookies file to a per-call tempfile, pass
that path to `probe_youtube_url`, unlink it in `finally`. Each
yt-dlp instance now operates on its own private cookies file —
load + writeback both succeed locally, no shared state, no race.

The file is small (~5 KB) and `tempfile.mkstemp` lands on tmpfs
on Linux containers so the copy + unlink is sub-millisecond.
2507 probes adds <2.5 s to a ~10 min bulk run.

If snapshot creation fails (no /tmp space, permission error),
fall back to the shared cookies path — preserves pre-v1.15.11
behavior rather than failing the probe outright.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"


def test_probe_one_snapshots_cookies_per_call():
    """The per-row probe callable must build a private cookies
    snapshot before invoking probe_youtube_url. Pin the
    tempfile.mkstemp call site + the per_call_cookies passthrough
    so a future refactor can't silently revert to the shared-file
    pattern that triggered the writeback race."""
    src = API_PY.read_text()
    fn_start = src.index("def _bulk_probe_tdb_run(")
    fn_end = src.index("\ndef ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    # The per-call snapshot path.
    assert "_tempfile.mkstemp(" in fn_body, (
        "v1.15.11: per-call cookies snapshot must use mkstemp"
    )
    assert 'prefix="motif-probe-cookies-"' in fn_body, (
        "Snapshot tempfile must use a recognizable prefix for "
        "leak diagnosis (operator can `ls /tmp/motif-probe-*`)"
    )
    # The probe call must use the per-call snapshot path, NOT
    # the original shared `cookies` variable directly.
    assert "probe_youtube_url(url, cookies_file=per_call_cookies)" in fn_body, (
        "probe call must pass the per-call snapshot path"
    )


def test_probe_one_unlinks_snapshot_in_finally():
    """The snapshot must be cleaned up regardless of whether the
    probe succeeded, raised, or returned an error result. A try/
    finally with snap_owned guard prevents 2500+ stale tempfiles
    accumulating per bulk run."""
    src = API_PY.read_text()
    fn_start = src.index("def _bulk_probe_tdb_run(")
    fn_end = src.index("\ndef ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    # finally branch with unlink.
    assert "finally:" in fn_body
    assert "if snap_owned and per_call_cookies is not None:" in fn_body
    assert "per_call_cookies.unlink()" in fn_body


def test_probe_one_skips_row_when_snapshot_fails():
    """v1.15.11 fell back to the shared cookies path on snapshot
    failure to preserve pre-v1.15.11 behavior. v1.15.34 changed
    that — silent fallback re-introduced the v1.15.11 race
    condition (multiple workers reading the same cookies file
    concurrently) so the fix loop closed on itself. Now: log +
    return an error tuple to skip the row. Operator sees a
    cookies-snapshot-failed result instead of getting fake
    'probe error' verdicts caused by file-read races."""
    src = API_PY.read_text()
    fn_start = src.index("def _bulk_probe_tdb_run(")
    fn_end = src.index("\ndef ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    snapshot_block = fn_body[fn_body.index("per_call_cookies = None"):]
    assert "except Exception as e:" in snapshot_block
    # v1.15.34: must NOT silently fall back to the shared file.
    assert "per_call_cookies = cookies" not in snapshot_block, (
        "v1.15.34: snapshot-failure must NOT fall back to the "
        "shared cookies path (re-introduces v1.15.11 race)"
    )
    # Must log + return an error tuple (5-element shape).
    assert "log.warning(" in snapshot_block
    assert "cookies snapshot failed:" in snapshot_block


def test_probe_one_skips_snapshot_when_no_cookies_configured():
    """When `cookies` is None (no cookies file configured), the
    snapshot machinery must be skipped entirely — no tempfile
    creation, no unlink, no overhead. Pin the `if cookies:` gate
    so a future refactor can't accidentally always-snapshot
    (which would crash on cookies=None or do pointless I/O)."""
    src = API_PY.read_text()
    fn_start = src.index("def _bulk_probe_tdb_run(")
    fn_end = src.index("\ndef ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    # The snapshot block is gated on `if cookies:`.
    snap_anchor = fn_body.index("per_call_cookies = None")
    pre_snapshot = fn_body[:snap_anchor]
    # Must be inside _probe_one, gated on cookies truthy.
    after_init = fn_body[snap_anchor:snap_anchor + 200]
    assert "snap_owned = False" in after_init
    assert "if cookies:" in after_init, (
        "Snapshot block must be gated on `if cookies:` so a "
        "no-cookies config skips the tempfile dance entirely"
    )


def test_per_call_cookies_default_is_none():
    """Pin the initial per_call_cookies = None default so the
    fallback path always has a defined value to pass to
    probe_youtube_url. probe_youtube_url already handles
    cookies_file=None correctly."""
    src = API_PY.read_text()
    fn_start = src.index("def _bulk_probe_tdb_run(")
    fn_end = src.index("\ndef ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    assert "per_call_cookies = None" in fn_body
    assert "snap_owned = False" in fn_body
