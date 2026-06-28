"""v1.22.57 — PROBE PLEX THEME API diagnostic: off-loop + parallel.

the user: "the diagnostic feature to probe is very slow and becomes slow
and slower as the list increases, is there a way to increase the speed?"

Root cause: `api_admin_probe_plex_themes` ran its Plex HTTP round-trips
(GET /themes + a 4KB Range-GET per theme entry, for up to 8 titles)
SYNCHRONOUSLY inside the async handler AND serially. That (a) blocked
the whole event loop for the probe's duration — the v1.21.20 / v1.22.31
event-loop-block class the SIBLING api_admin_probe_themes was fixed for
but this one was missed — and (b) cost linearly more wall-clock per
added title fragment.

Fix: a PlexClient pool (one client per worker — httpx.Client isn't
thread-safe for concurrent reuse) + a ThreadPoolExecutor run via
run_in_threadpool, so the matched titles probe concurrently OFF the
event loop. Safe: read-only, local Plex, capped at 8 titles.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()


def _handler_body() -> str:
    idx = API_PY.index("async def api_admin_probe_plex_themes(")
    end = API_PY.index("@app.", idx + 1)
    return API_PY[idx:end]


def test_probe_runs_off_event_loop():
    """The Plex-call loop must be dispatched via run_in_threadpool so it
    no longer blocks the async event loop (the v1.21.20/.22.31 class)."""
    body = _handler_body()
    assert "await run_in_threadpool(_probe_all)" in body, (
        "v1.22.57: the Plex round-trips must run off the event loop"
    )
    # The old synchronous `with PlexClient(...) as plex:` driving the
    # whole loop inline must be gone.
    assert "with PlexClient(cfg, plus_mode=settings.plus_equiv_mode) as plex:" not in body


def test_probe_parallelizes_across_titles():
    """The matched titles probe concurrently via a ThreadPoolExecutor
    with a per-THREAD PlexClient (threading.local), capped by
    PROBE_MAX_WORKERS. v1.22.82: the old submission-index round-robin
    (clients[i % n_workers]) let two threads drive one client whenever
    completion order diverged from submission order — the exact unsafe
    sharing the pool was built to prevent."""
    body = _handler_body()
    assert "ThreadPoolExecutor(" in body
    assert "PROBE_MAX_WORKERS" in body
    # One client per worker THREAD (httpx.Client isn't thread-safe
    # for concurrent reuse).
    assert "_threading.local()" in body
    assert "_ppt_client()" in body
    assert "clients[i % n_workers]" not in body


def test_probe_closes_clients():
    """The client pool is closed in a finally so the diagnostic doesn't
    leak httpx connections per run."""
    body = _handler_body()
    assert "finally:" in body
    assert ".close()" in body


def test_probe_still_resolves_and_walks_entries():
    """The behavior the v1.18.85 tests pin is preserved after the
    refactor — title resolution via plex_items + get_themes + per-entry
    byte probe + scheme extraction."""
    body = _handler_body()
    assert "FROM plex_items" in body and "title LIKE" in body
    assert "plex.get_themes(" in body
    assert "probe_theme_entry_bytes(" in body
    assert "scheme" in body
    # Cap + 503 guards intact.
    assert "cap is 8 titles per call" in body
    assert "503" in body
