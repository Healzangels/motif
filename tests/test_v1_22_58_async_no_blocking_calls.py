"""v1.22.58 — lint: no synchronous blocking network calls in async handlers.

The recurring "event-loop-block" bug class (v1.21.20 orphan scan, v1.22.31
?rk= probe, v1.22.42 unplace/forget loops, v1.22.57 title-fragment probe):
a synchronous blocking call — a Plex HTTP round-trip, a yt-dlp probe, a
`requests`/`subprocess`/`time.sleep` — runs DIRECTLY inside an `async def`
FastAPI handler instead of being offloaded via `run_in_threadpool`. While
it runs, the single asyncio event loop is frozen, so EVERY other request
(UI polling included) hangs for the duration. Symptom: "the diagnostic is
very slow and the whole app gets slower as the list grows."

Each occurrence was caught one-at-a-time after a user noticed the freeze.
This is the standing guard that catches them statically instead.

How it works: AST-walk every `async def` in app/web/api.py. For each, look
at the calls in its DIRECT body — NOT descending into nested `def` /
`async def` / `lambda`, because those are the offload targets (a sync helper
passed to `run_in_threadpool`, where blocking is correct). Any call to a
known-blocking symbol found in the direct async body is a freeze.

The CORRECT offload patterns this lint passes:
  * `await run_in_threadpool(plex.get_themes, rk)` — the bare attribute is
    an argument, not an inline Call.
  * `def _run(): ...plex.get_themes()...` then
    `await run_in_threadpool(_run)` — the call lives inside a nested def.

If this test fails, the offending line ran a blocking call on the event
loop. Wrap it (or its enclosing sequence) in a sync helper + await it
through `run_in_threadpool` — see api_admin_probe_plex_themes (v1.22.57)
or the three api_admin_probe_* endpoints (v1.22.58) for the shape.
"""
from __future__ import annotations

import ast
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
API_PATH = REPO / "app" / "web" / "api.py"

# PlexClient's network-blocking public methods — DERIVED from plex.py at
# test time (the v1.17.10 contract-drift guard pattern: a hardcoded list
# would silently miss a future blocking method). Every public PlexClient
# method does an HTTP round-trip except the cheap lifecycle ones below.
_PLEX_NON_BLOCKING = {"close"}


def _derive_plex_blocking_methods() -> set[str]:
    src = (REPO / "app" / "core" / "plex.py").read_text()
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == "PlexClient":
            return {
                n.name for n in node.body
                if isinstance(n, ast.FunctionDef)
                and not n.name.startswith("_")
                and n.name not in _PLEX_NON_BLOCKING
            }
    raise AssertionError("PlexClient class not found in app/core/plex.py")


PLEX_BLOCKING_METHODS = _derive_plex_blocking_methods()
# Module-level blocking functions/callables. v1.22.69 adds api.py's own
# _trigger_plex_item_refresh — a Plex HTTP round-trip whose helper
# indirection evaded the attribute-based PlexClient detection. v1.23.42 adds
# refresh_sections — it drives a PlexClient section-discovery round-trip but,
# as a module function taking the client as an arg, evaded the same detection
# (api_refresh_libraries froze the loop on it until v1.23.42).
BLOCKING_FUNCS = {"probe_youtube_url", "urlopen", "_trigger_plex_item_refresh",
                  "refresh_sections",
                  # v0.51.246: the auth entry points. Each runs bcrypt (rounds
                  # 10-12, ~60-470ms of pure CPU) and/or opens a DB connection.
                  # KNOWN BLIND SPOT this set does not close: AuthMiddleware
                  # reaches authenticate_token through the SYNC method
                  # _resolve_principal, and this lint only walks the DIRECT body
                  # of `async def` — which is exactly how a 30s writer-lock
                  # block survived there until v0.51.246. Listing them still
                  # catches the next DIRECT call from an async handler.
                  "authenticate_token", "change_admin_password",
                  "create_api_token", "create_admin", "verify_password"}
# v1.23.62 (audit #1/#10): blocking method NAMES that aren't on PlexClient but
# still do a synchronous network round-trip when called from an async handler —
# matched by attribute name like PLEX_BLOCKING_METHODS (any receiver). The
# PlexClient-only derivation missed these: TMDBClient.test_credentials (httpx GET
# to /authentication, froze /api/tmdb/test) and notify.test_dispatch (Apprise
# network send, froze /api/admin/test-notification).
OTHER_BLOCKING_ATTRS = {"test_credentials", "test_dispatch"}
# Dotted blocking calls (module.attr(...)).
BLOCKING_DOTTED = {
    ("requests", "get"), ("requests", "post"), ("requests", "put"),
    ("requests", "delete"), ("requests", "head"), ("requests", "request"),
    ("subprocess", "run"), ("subprocess", "call"),
    ("subprocess", "check_output"), ("subprocess", "check_call"),
    ("subprocess", "Popen"), ("time", "sleep"),
}


class _ShallowBlockingCallFinder(ast.NodeVisitor):
    """Find blocking Calls in the DIRECT body of an async function — does
    NOT descend into nested def / async def / lambda (offload targets)."""

    def __init__(self) -> None:
        self.hits: list[tuple[int, str]] = []

    # Stop at nested scopes — blocking inside them is the correct
    # (offloaded) pattern.
    def visit_FunctionDef(self, node): return
    def visit_AsyncFunctionDef(self, node): return
    def visit_Lambda(self, node): return

    def visit_Call(self, node: ast.Call):
        f = node.func
        hit = None
        if isinstance(f, ast.Attribute):
            recv = f.value.id if isinstance(f.value, ast.Name) else "?"
            if f.attr in PLEX_BLOCKING_METHODS or f.attr in OTHER_BLOCKING_ATTRS:
                hit = f"{recv}.{f.attr}()"
            elif (recv, f.attr) in BLOCKING_DOTTED:
                hit = f"{recv}.{f.attr}()"
        elif isinstance(f, ast.Name) and f.id in BLOCKING_FUNCS:
            hit = f"{f.id}()"
        if hit:
            self.hits.append((node.lineno, hit))
        # Keep walking — comprehensions / if / for / with all run inline
        # on the loop, so blocking calls inside them DO count.
        self.generic_visit(node)


def _scan(src: str) -> list[tuple[str, int, str]]:
    tree = ast.parse(src)
    offenders: list[tuple[str, int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef):
            finder = _ShallowBlockingCallFinder()
            for stmt in node.body:
                finder.visit(stmt)
            for lineno, what in finder.hits:
                offenders.append((node.name, lineno, what))
    return offenders


def test_no_blocking_calls_in_async_handlers():
    """ZERO synchronous blocking network calls in the direct body of any
    async handler in api.py — they must be offloaded via run_in_threadpool.
    """
    offenders = _scan(API_PATH.read_text())
    assert offenders == [], (
        "Synchronous blocking call(s) on the event loop in async "
        "handler(s) — wrap each in a sync helper + "
        "`await run_in_threadpool(...)` (v1.21.20 / v1.22.57 / v1.22.58 "
        "event-loop-block class):\n"
        + "\n".join(
            f"  api.py:{ln}  {fn}()  ->  {what}"
            for fn, ln, what in offenders
        )
    )


def test_lint_detects_a_planted_offender():
    """Meta-test: the lint must actually FIRE on a known-bad shape, so a
    future refactor can't neuter it into a vacuous pass."""
    bad = (
        "async def handler(request):\n"
        "    pre = plex.get_themes(rating_key=rk)\n"
        "    return pre\n"
    )
    assert _scan(bad), "lint must flag an inline plex.get_themes() in async"


def test_plex_method_list_derived_not_stale():
    """Meta-test: the blocking-method set comes from plex.py itself and
    contains the known core surface — a refactor that renames PlexClient
    or empties the class fails here instead of silently neutering the
    main lint."""
    assert "get_themes" in PLEX_BLOCKING_METHODS
    assert "verify_theme_claim" in PLEX_BLOCKING_METHODS
    assert "upload_theme" in PLEX_BLOCKING_METHODS
    assert "close" not in PLEX_BLOCKING_METHODS
    assert len(PLEX_BLOCKING_METHODS) >= 15


def test_lint_accepts_the_offload_patterns():
    """Meta-test: the two correct offload shapes must NOT be flagged."""
    # (a) bare attribute passed to run_in_threadpool
    ok_a = (
        "async def handler(request):\n"
        "    pre = await run_in_threadpool(plex.get_themes, rk)\n"
        "    return pre\n"
    )
    # (b) blocking call inside a nested sync helper that gets offloaded
    ok_b = (
        "async def handler(request):\n"
        "    def _run():\n"
        "        return plex.get_themes(rating_key=rk)\n"
        "    return await run_in_threadpool(_run)\n"
    )
    assert _scan(ok_a) == []
    assert _scan(ok_b) == []
