"""v0.51.69 — additive completeness guards for two unguarded "mirror" surfaces.

Complexity/regression audit follow-up (the "mirror-drift" class). Two conceptual
registries were hand-synced across surfaces with NO completeness guard, so a future
edit could drift one copy silently. These are ROBUST checks (import the real runtime
values / parse stable literals — NOT fixed char-window source-pins), and both pass on
current code (the surfaces are in-sync today); they exist to catch the NEXT drift.

  1. NOTIFY SEVERITY MAP — notify._EVENT_NOTIFY_TYPE maps event_kind → warning/failure/
     info and is read via `.get(event_kind, "info")`. The two sibling notification
     surfaces (settings toggle, muted chip) have registry-walking guards; this one only
     had a hardcoded 3-key check, and drifted twice (v1.23.62, v1.24.26) → warning/
     failure kinds silently rendered neutral "info". Guard: every dispatchable event in
     config_file._DEFAULT_NOTIFY_EVENTS must have an explicit severity entry.

  2. SERVER PILL-AXIS WHITELIST — the _pset(x_pills, {...}) validation sets in api.py
     are the 4th surface of each multi-select pill axis (template + JS pillAxes +
     deep-link parser are the other three, cross-referenced by test_v1_14_58). Only the
     attn axis had a client<->server set-equality guard (test_v1_15_23); tdb/dl/pl/link/
     ed did not. A token added to the UI but forgotten in _pset silently no-ops the
     filter and shows ALL rows (the v1.15.23 bug for ?attn_pills=cookies). Guard: the
     server _pset set == the JS pillAxes set for each of those 5 axes.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


# ── 1. notify severity map completeness (runtime import — fully robust) ──

def test_every_notifiable_event_has_a_severity():
    from app.core.notify import _EVENT_NOTIFY_TYPE
    from app.core.config_file import _DEFAULT_NOTIFY_EVENTS
    missing = set(_DEFAULT_NOTIFY_EVENTS) - set(_EVENT_NOTIFY_TYPE)
    assert not missing, (
        "these dispatchable events have no _EVENT_NOTIFY_TYPE entry, so notify.dispatch's "
        f".get(kind, 'info') renders them neutral info even if warning/failure: {missing}. "
        "Add each to _EVENT_NOTIFY_TYPE in app/core/notify.py.")


# ── 2. server _pset whitelist == JS pillAxes, per axis ──

# (server _pset arg prefix, JS pillAxes `state:` name) for the 5 axes that were
# missing a server-side set-equality guard. attn has test_v1_15_23; SRC is the
# composite handler (out of this shape), both intentionally excluded.
_AXES = [
    ("tdb", "tdbPills"),
    ("dl", "dlPills"),
    ("pl", "plPills"),
    ("link", "linkPills"),
    ("ed", "edPills"),
]


def _server_pset(axis: str) -> set[str]:
    # matches: _pset(tdb_pills, {"tdb", "update", ...})
    m = re.search(rf"_pset\(\s*{axis}_pills\s*,\s*\{{([^}}]*)\}}\)", API_PY)
    assert m, f"could not locate the server _pset({axis}_pills, {{...}}) whitelist"
    return set(re.findall(r"""["']([^"']+)["']""", m.group(1)))


# Scope JS extraction to the `const pillAxes = [ ... ];` block so `state: 'xxxPills'`
# can't match an earlier unrelated occurrence elsewhere in app.js.
_PILLAXES_BLOCK = APP_JS[APP_JS.index("const pillAxes = ["):
                         APP_JS.index("\n      ];", APP_JS.index("const pillAxes = ["))]


def _js_pillaxes(state: str) -> set[str]:
    # locate the pillAxes entry by its `state: 'xxxPills'` then the nearest
    # following `values: [ ... ]` array (a single-line literal).
    i = _PILLAXES_BLOCK.index(f"state: '{state}'")
    m = re.search(r"values:\s*\[([^\]]*)\]", _PILLAXES_BLOCK[i:i + 1200])
    assert m, f"could not locate values:[...] for pillAxes state '{state}'"
    return set(re.findall(r"""["']([^"']+)["']""", m.group(1)))


def test_server_pset_matches_js_pillaxes_per_axis():
    for prefix, state in _AXES:
        server = _server_pset(prefix)
        client = _js_pillaxes(state)
        assert server == client, (
            f"pill axis '{prefix}' drift — server _pset={sorted(server)} but JS "
            f"pillAxes={sorted(client)}. A token present in one but not the other means "
            f"either the // ALL button leaves a chip inactive (missing from JS) or the "
            f"filter silently shows ALL rows (missing from server _pset). Sync them.")


def test_axis_extractors_are_nonempty():
    # sanity: the regex extractors actually found real token sets (guards against a
    # future refactor silently making the parity test vacuously pass on empty sets).
    for prefix, state in _AXES:
        assert len(_server_pset(prefix)) >= 2, f"server _pset({prefix}) parsed empty"
        assert len(_js_pillaxes(state)) >= 2, f"JS pillAxes({state}) parsed empty"


def test_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
