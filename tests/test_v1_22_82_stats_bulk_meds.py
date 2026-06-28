"""v1.22.82 (audit round 2, Batch C #11) — stats/bulk-runner MEDs.

(1) /api/library's post-stat pill filters DOUBLE-paginated: each
block (DL, PL, ATTN) re-sliced the previous block's already-paginated
page, so combining two axes (DL=ON + PL=BROKEN — a supported
workflow) made page 2 always empty and collapsed `total` to one
page's count. Now: filter-only blocks + ONE shared pagination
epilogue gated on a _post_stat_filtered flag.

(2) Three bulk runners (bulk_probe_tdb, bulk_lps,
reprobe_plex_themes) called op_progress.start_progress OUTSIDE the
try whose except runs finish_progress(failed) — a failed start (e.g.
database-is-locked past the busy timeout, bug class 8) killed the
runner thread with the try_acquire'd 'pending' row stuck forever:
every re-run 409'd and op_progress_running pinned anyMutatingOpActive
until container restart. All three now start INSIDE the try
(_cloud_themes_backup_run always had the correct shape).

(3) Two probe pools pinned PlexClients by SUBMISSION INDEX
(clients[idx % N]) under comments claiming one-client-per-thread —
whenever completion order diverged from submission order a freed
thread picked task N (N % N = 0) and two threads drove one client
concurrently. Both _reprobe_plex_themes_run and the v1.22.57
title-probe pool now use threading.local (the v1.22.72
get_item_paths_bulk shape).

Note: the audit's bulk-LPS shared-cookies finding is already resolved
by v1.22.77 — probe_youtube_url's opts builder snapshots internally.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()


# ── (1) single pagination epilogue ───────────────────────────


def test_post_stat_blocks_share_one_pagination():
    i = API_PY.index("_post_stat_filtered = False")
    region = API_PY[i:i + 4500]
    # The three filter blocks only set the flag; exactly ONE
    # total+slice pair survives, in the epilogue.
    assert region.count("_post_stat_filtered = True") == 3
    j = region.index("if _post_stat_filtered:")
    epilogue = region[j:j + 300]
    assert "total = len(items)" in epilogue
    assert "items = items[offset:offset + per_page]" in epilogue
    # No per-block slicing left before the epilogue.
    assert region[:j].count("items = items[offset:offset + per_page]") == 0


# ── (2) start_progress inside the try ────────────────────────


def test_bulk_runners_start_progress_inside_try():
    for kind in ("bulk_probe_tdb", "bulk_lps", "reprobe_plex_themes"):
        i = API_PY.index(f'kind="{kind}",')
        before = API_PY[i - 800:i]
        assert "v1.22.82: start INSIDE the try" in before, kind
    # The marker appears once per runner.
    assert API_PY.count("v1.22.82: start INSIDE the try") == 3


# ── (3) per-thread client pools ──────────────────────────────


def test_reprobe_pool_is_per_thread():
    i = API_PY.index("def _reprobe_plex_themes_run(")
    body = API_PY[i:API_PY.index("\ndef ", i + 1)]
    assert "_probe_tl = _threading.local()" in body
    assert "client = _client_for_thread()" in body
    assert "clients[idx % PROBE_MAX_WORKERS]" not in body, (
        "v1.22.82: submission-index pinning shares a client across "
        "threads when completion order diverges"
    )


def test_title_probe_pool_is_per_thread():
    i = API_PY.index("def _probe_all():")
    body = API_PY[i:i + 2500]
    assert "_ppt_tl = _threading.local()" in body
    assert "_ppt_client()" in body
    assert "clients[i % n_workers]" not in body
