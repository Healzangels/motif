"""v1.22.69 (audit round 2, Batch B #5) — event-loop file-I/O offload cluster.

Six more class-12 freezes (CLAUDE.md recurring bug class 12), all multi-MB
file I/O or inline Plex HTTP directly inside async handler bodies in api.py:

  * api_public_stats — the whole stats aggregation (a dozen SELECTs over a
    large DB) ran inline; the public dashboard poll froze every other request.
  * _trigger_plex_item_refresh (x2 call sites) — a Plex HTTP round-trip
    called bare inside async bodies; the helper indirection evaded the
    v1.22.58 lint's attribute-based PlexClient detection.
  * api_upload_theme — target.write_bytes(data) + sha256(data) on a
    multi-MB upload.
  * api_unplace_item — chunked SHA-1 of the canonical file inside the
    plex_upload restore loop.
  * api_adopt_from_plex — hardlink/cross-FS copy + full re-read sha256
    per adopted row.
  * api_restore_canonical — per-row link/copy + chunked-sha256 loop.

Each is now a nested sync helper + await run_in_threadpool(...) (the
v1.21.20 offload shape). The v1.22.58 AST lint gained
_trigger_plex_item_refresh in BLOCKING_FUNCS so a future bare call fails
statically; the pins here lock the wrap sites themselves.
"""
from __future__ import annotations

import ast
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()


def _handler_body(name: str) -> str:
    i = API_PY.index(f"async def {name}(")
    return API_PY[i:API_PY.index("\n    @app.", i)]


def test_public_stats_offloaded():
    body = _handler_body("api_public_stats")
    assert "def _q():" in body
    assert "return await run_in_threadpool(_q)" in body


def test_trigger_refresh_call_sites_offloaded():
    """Both call sites ride run_in_threadpool; the only bare
    `_trigger_plex_item_refresh(` left is the def itself."""
    assert API_PY.count(
        "_trigger_plex_item_refresh, media_type, tmdb_id)") == 2
    assert API_PY.count("_trigger_plex_item_refresh(") == 1


def test_lint_gained_trigger_refresh():
    """The v1.22.58 standing guard now catches a future bare call."""
    from test_v1_22_58_async_no_blocking_calls import BLOCKING_FUNCS, _scan
    assert "_trigger_plex_item_refresh" in BLOCKING_FUNCS
    planted = (
        "async def handler(request):\n"
        "    _trigger_plex_item_refresh(media_type, tmdb_id)\n"
    )
    assert _scan(planted), "lint must flag a bare refresh call in async"


def test_upload_write_and_hash_offloaded():
    body = _handler_body("api_upload_theme")
    assert "def _write_and_hash" in body
    assert "sha = await run_in_threadpool(_write_and_hash)" in body
    # The write itself lives inside the helper, not the async body.
    assert "target.write_bytes(data)" not in body[:body.index("def _write_and_hash")]


def test_unplace_sha1_offloaded():
    body = _handler_body("api_unplace_item")
    assert "def _sha1_file(p=canonical):" in body
    assert "motif_hash = await run_in_threadpool(_sha1_file)" in body


def test_adopt_stage_and_rehash_offloaded():
    body = _handler_body("api_adopt_from_plex")
    assert "def _stage_canon(" in body
    assert "placement_kind = await run_in_threadpool(_stage_canon)" in body
    assert "def _rehash(p=canon_path):" in body
    assert "sha, size = await run_in_threadpool(_rehash)" in body


def test_restore_canonical_offloaded():
    body = _handler_body("api_restore_canonical")
    assert "def _run():" in body
    assert "return await run_in_threadpool(_run)" in body
    # The loop's link/copy + hashing all live inside _run.
    assert "hashlib" not in body[:body.index("def _run():")]


# ── AST backstop: no inline file I/O creeps back into these handlers ──

_IO_ATTRS = {"write_bytes", "read_bytes", "hardlink_to", "copy2"}
_HANDLERS = {"api_public_stats", "api_upload_theme", "api_unplace_item",
             "api_adopt_from_plex", "api_restore_canonical"}


class _ShallowIOFinder(ast.NodeVisitor):
    """File-I/O / hashlib calls in the DIRECT body only — nested defs are
    the offload targets (same shape as the v1.22.58 lint)."""

    def __init__(self) -> None:
        self.hits: list[tuple[int, str]] = []

    def visit_FunctionDef(self, node): return
    def visit_AsyncFunctionDef(self, node): return
    def visit_Lambda(self, node): return

    def visit_Call(self, node: ast.Call):
        f = node.func
        if isinstance(f, ast.Attribute):
            if f.attr in _IO_ATTRS:
                self.hits.append((node.lineno, f.attr))
            elif isinstance(f.value, ast.Name) and f.value.id == "hashlib":
                self.hits.append((node.lineno, f"hashlib.{f.attr}"))
        self.generic_visit(node)


def test_no_inline_file_io_in_the_offloaded_handlers():
    tree = ast.parse(API_PY)
    found: set[str] = set()
    offenders: list[tuple[str, int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name in _HANDLERS:
            found.add(node.name)
            finder = _ShallowIOFinder()
            for stmt in node.body:
                finder.visit(stmt)
            offenders += [(node.name, ln, w) for ln, w in finder.hits]
    assert found == _HANDLERS, f"handler renamed? missing: {_HANDLERS - found}"
    assert offenders == [], (
        "inline file I/O back on the event loop (v1.22.69 class-12):\n"
        + "\n".join(f"  api.py:{ln}  {fn}()  ->  {w}"
                    for fn, ln, w in offenders)
    )
