"""v0.51.296 — holistic review wave 5: class-12 event-loop offloads.

Four confirmed findings, all the same catalogued class (a blocking call in
an async handler freezes the single event loop — every concurrent request,
UI polling included, stalls):
  1. api_accept_all_updates / api_decline_all_updates ran the heavy
     correlated-subquery scan + the whole per-row bulk loop inline (the
     SQL shape the v1.22.69 audit measured at seconds on a 10K library).
  2. AuthMiddleware.dispatch resolved principals inline — every
     API-token request ran a bcrypt rounds=10 checkpw (~60ms pure CPU)
     on the event loop.
  3. api_item_theme_audio ran a /data-mount syscall cluster inline
     (resolve, containment, is_file, the 410-forensics iterdir, the
     12-byte header sniff) — a spun-down disk stalled everything.
  4. api_admin_test_trigger_theme_lost called _notify.dispatch inline —
     since v0.51.147 dispatch records the inbox row (sqlite connect +
     lock budget) in the caller's thread.

Behavioral coverage rides the existing endpoint suites (all green over
the offloaded shapes); these pins make an offload revert go red.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
API = (REPO / "app" / "web" / "api.py").read_text()


def _handler(name: str) -> str:
    i = API.index(f"async def {name}(")
    return API[i:API.index("\n    @app.", i)]


def test_bulk_accept_and_decline_run_off_the_loop():
    for name in ("api_accept_all_updates", "api_decline_all_updates"):
        h = _handler(name)
        assert "def _run():" in h and "return await run_in_threadpool(_run)" in h, (
            f"{name}: the bulk scan+loop must run in the threadpool")


def test_auth_middleware_offloads_principal_resolution():
    i = API.index("async def dispatch(self, request: Request, call_next):")
    blk = API[i:API.index("def ", i + 60)]
    assert "await run_in_threadpool(" in blk
    assert "self._resolve_principal" in blk, (
        "an API-token request runs a ~60ms bcrypt checkpw — inline, every "
        "concurrent request stalls behind it")


def test_theme_audio_offloads_the_syscall_cluster():
    h = _handler("api_item_theme_audio")
    assert "def _resolve_and_sniff(" in h
    assert "await run_in_threadpool(" in h
    # the cluster's pieces live inside the helper, not the async body:
    k = h.index("def _resolve_and_sniff(")
    assert "full.resolve()" in h[k:] and "parent.iterdir()" in h[k:]
    assert "full.resolve()" not in h[:k], (
        "a spun-down /data disk must never stall the event loop")


def test_test_trigger_dispatch_offloaded():
    h = _handler("api_admin_test_trigger_theme_lost")
    assert "def _send():" in h and "await run_in_threadpool(_send)" in h


def test_v0_51_296_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert "0.51.296: " in init_py
