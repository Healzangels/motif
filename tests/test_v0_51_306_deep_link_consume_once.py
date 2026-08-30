"""v0.51.306 — the ?info_open deep-link is consume-once.

The v1.14.85 gate read info_open/info_mt/info_section/info_edition on
every library page load and nothing removed them from the address bar,
so refreshing after an inbox click-through re-opened the INFO card
forever (the user's report). The gate now strips all four params via
history.replaceState immediately after reading them. Invariants pinned:
the strip lives in the gate, names all four keys, runs BEFORE (not
inside) the deferred-open branch so malformed links clean up too, and
preserves the rest of the URL (pathname + surviving query + hash).
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def _gate() -> str:
    i = APP_JS.index("v1.14.85: ?info_open=<tmdb_id>")
    return APP_JS[i:APP_JS.index("URLSearchParams not supported", i)]


def test_gate_strips_the_params_after_reading():
    g = _gate()
    assert "history.replaceState" in g, (
        "the deep-link params must leave the address bar once consumed — "
        "otherwise every refresh re-opens the card until the user navigates")
    strip = g[g.index("const infoKeys"):g.index("history.replaceState")]
    for k in ("info_open", "info_mt", "info_section", "info_edition"):
        assert f"'{k}'" in strip, (
            f"{k} missing from the strip list — one sticky param is enough "
            f"to keep the URL dirty")
    # v0.51.307 (audit): bind deletion to EVERY key — the first draft
    # accepted any single .delete( call, so sp.delete(infoKeys[0]) passed
    # while three params stayed sticky.
    assert re.search(r"infoKeys\s*\.forEach\(\(k\) => sp\.delete\(k\)\)", strip), (
        "the strip must delete each listed key (loop over infoKeys)")


def test_strip_runs_before_the_open_branch():
    g = _gate()
    assert g.index("history.replaceState") < g.index("if (infoOpen && infoMt)"), (
        "the strip must be synchronous and unconditional-on-validity: gated "
        "behind the open branch, a malformed link (info_open without info_mt) "
        "would stick in the URL forever; inside the setTimeout it would race "
        "a fast refresh")


def test_strip_preserves_the_rest_of_the_url():
    g = _gate()
    call = g[g.index("history.replaceState"):]
    call = call[:call.index(";")]
    assert "path" in call and "window.location.hash" in call, (
        "replaceState must rebuild pathname + surviving query + hash — "
        "flattening to pathname would eat unrelated params")
    assert "qs ? " in call or "qs?" in call, (
        "an empty surviving query must not leave a dangling '?'")


def test_v0_51_306_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert "0.51.306: " in init_py
