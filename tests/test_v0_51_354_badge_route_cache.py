"""v0.51.354 — a topbar pill sends you where the rows are, even before the first poll.

the user, with 5 pending TV updates: "clicking the update 5 button on 2nd click brings me to the movie section
where there are no results when it should just always bring me to tv since that's where the 5 that need to be
updated actually live".

The breakdown behind the pill was right — tv was the only impacted tab. What was wrong is WHEN the pill learns it.
v1.13.88 caches the topbar COUNTS in localStorage so a page navigation can paint the pills immediately instead of
leaving them blank until /api/stats answers; it cached no route, so the badge came back visible still wearing
base.html's hardcoded `/movies?attn_pills=update`, and bindBadgeCycle had no breakdown to intercept with. Click in
that window — which is exactly what a second click after the first one navigated does — and you land on movies with
0 matches.

The route now rides with the count, and a click that still finds no breakdown asks /api/stats rather than trusting
the template's default. The real app.js functions run here under node.
"""
from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest

from _slice_helpers import slice_to_next

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
BASE_HTML = (REPO / "app" / "web" / "templates" / "base.html").read_text()
_NODE = shutil.which("node")

pytestmark = pytest.mark.skipif(not _NODE, reason="node not installed")

# the four cycling pills, as base.html ships them: id, count id, dataset key, query, /api/stats block
PILLS = {
    "upd": ("topbar-updates-badge", "topbar-updates-count", "updTabs", "attn_pills=update", "updates"),
    "fail": ("topbar-failures-badge", "topbar-failures-count", "failTabs", "attn_pills=fail", "failures"),
    "drop": ("topbar-drops-badge", "topbar-drops-count", "dropTabs", "tdb_pills=dropped", "drops"),
    "repush": ("topbar-repush-badge", "topbar-repush-count", "repushTabs", "attn_pills=repush", "repush"),
}

DRIVER = r"""
const fs = require("node:fs");
const vm = require("node:vm");
const [, , srcPath, planPath] = process.argv;
const plan = JSON.parse(fs.readFileSync(planPath, "utf8"));

const els = {};
function el(id, href) {
  return els[id] = { id, href, hidden: true, dataset: {}, textContent: "",
                     _listeners: {},
                     addEventListener: (t, fn) => { (els[id]._listeners[t] = els[id]._listeners[t] || []).push(fn); },
                     click: () => {
                       const e = { preventDefault: () => { e.defaultPrevented = true; }, defaultPrevented: false };
                       (els[id]._listeners.click || []).forEach((fn) => fn(e));
                       // an <a> whose handler did not preventDefault navigates to its own href
                       if (!e.defaultPrevented && els[id].href) loc.href = els[id].href;
                       return e;
                     } };
}
for (const [id, href] of Object.entries(plan.badges)) el(id, href);
for (const [id, cid] of Object.entries(plan.counts)) el(cid, null);

const stored = {};
if (plan.cache !== undefined) stored["motif:topbar_counts"] = JSON.stringify(plan.cache);

const nav = [];
const calls = [];
let current = plan.here || "/tv";
const loc = {
  get pathname() { return current.split("?")[0]; },
  get search() { const i = current.indexOf("?"); return i === -1 ? "" : current.slice(i); },
  get href() { return current; },
  set href(v) { nav.push(v); current = v; },
};
const ctx = vm.createContext({
  console,
  URLSearchParams,  // the vm context has no browser globals of its own
  document: { getElementById: (id) => els[id] || null },
  localStorage: {
    getItem: (k) => (k in stored ? stored[k] : null),
    setItem: (k, v) => { stored[k] = v; },
  },
  window: { location: loc, addEventListener: () => {} },
  api: async (method, path) => {
    calls.push(`${method} ${path}`);
    if (plan.stats === "fail") throw new Error("offline");
    return plan.stats;
  },
});
vm.runInContext(fs.readFileSync(srcPath, "utf8"), ctx);

(async () => {
  if (plan.prepopulate) ctx.prepopulateBadgesFromCache();
  for (const [key, [id, , dsKey, query, statsKey]] of Object.entries(plan.pills)) {
    ctx.bindBadgeCycle(id, dsKey, query, statsKey);
  }
  if (plan.persist) {
    // what refreshTopbarStatus stashes once the badges are routed
    stored["motif:topbar_counts"] = JSON.stringify(ctx.topbarCachePayload(plan.persist.stats, plan.persist.routes));
  }
  const clicks = [];
  for (const id of (plan.clicks || [])) {
    const e = els[id].click();
    await new Promise((r) => setImmediate(r));  // let an async route resolve
    clicks.push({ id, prevented: e.defaultPrevented });
  }
  process.stdout.write(JSON.stringify({
    nav, calls, clicks,
    badges: Object.fromEntries(Object.entries(els).map(([id, e]) =>
      [id, { href: e.href, hidden: e.hidden, text: e.textContent, dataset: e.dataset }])),
    cache: stored["motif:topbar_counts"] ? JSON.parse(stored["motif:topbar_counts"]) : null,
  }));
})().catch((e) => { console.error(e); process.exit(1); });
"""


def _src() -> str:
    """The real functions, lifted whole."""
    parts = [
        slice_to_next(APP_JS, "  function bindBadgeCycle(", "\n  function ", "\n  async function "),
        slice_to_next(APP_JS, "  async function routeBadgeFromStats(", "\n  function ", "\n  async function "),
        slice_to_next(APP_JS, "  function topbarCachePayload(", "\n  function ", "\n  async function "),
        # prepopulate is the LAST function in its block — the DOMContentLoaded handler ends the slice
        slice_to_next(APP_JS, "  function prepopulateBadgesFromCache(", "\n  function ", "\n  async function ",
                      "\n  document.addEventListener("),
    ]
    return "\n".join(parts)


def _run(tmp_path, plan) -> dict:
    tmp_path.mkdir(parents=True, exist_ok=True)
    (tmp_path / "src.js").write_text(_src())
    (tmp_path / "plan.json").write_text(json.dumps(plan))
    (tmp_path / "driver.js").write_text(DRIVER)
    r = subprocess.run([_NODE, str(tmp_path / "driver.js"), str(tmp_path / "src.js"), str(tmp_path / "plan.json")],
                       capture_output=True, text=True, timeout=60)
    assert r.returncode == 0, r.stderr[-2000:]
    return json.loads(r.stdout)


def _plan(**over):
    """The operator's shape: 5 pending updates, all of them on tv, page loaded but /api/stats not back yet."""
    plan = {
        "badges": {pid: f"/movies?{q}" for _, (pid, _, _, q, _) in PILLS.items()},
        "counts": {pid: cid for _, (pid, cid, _, _, _) in PILLS.items()},
        "pills": {k: list(v) for k, v in PILLS.items()},
        "here": "/tv?fourk=0&attn_pills=update",
        "prepopulate": True,
        "clicks": ["topbar-updates-badge"],
        "stats": {"updates": {"pending": 5, "tabs": [{"tab": "tv", "fourk": False, "count": 5}]}},
        "cache": {"upd": 5, "routes": {"upd": {"href": "/tv?fourk=0&attn_pills=update",
                                               "tabs": [{"tab": "tv", "fourk": False, "count": 5}]}}},
    }
    plan.update(over)
    return plan


UPD = "topbar-updates-badge"
TV_UPD = "/tv?fourk=0&attn_pills=update"


def test_the_pill_painted_from_cache_goes_where_the_rows_are(tmp_path):
    """The report, exactly: the page has loaded, the count is on screen from cache, /api/stats has not answered
    yet, and the operator clicks. Pre-fix this navigated to base.html's /movies default — 0 matches."""
    out = _run(tmp_path, _plan())
    assert out["nav"] == [TV_UPD], out
    assert out["badges"][UPD]["href"] == TV_UPD, "the cached route must land on the badge, not just the count"
    assert out["calls"] == [], "the route was already known — a click should not need the network"


def test_the_count_still_paints_immediately(tmp_path):
    """v1.13.88's reason for the cache (the user: "pills disappear sometimes for a long period") is preserved."""
    out = _run(tmp_path, _plan(clicks=[]))
    assert out["badges"][UPD]["hidden"] is False
    assert out["badges"]["topbar-updates-count"]["text"] == "5"


def test_a_cache_from_an_older_build_asks_the_server_rather_than_guessing(tmp_path):
    """Upgrading leaves a counts-only cache behind. The badge paints, has no breakdown, and must NOT fall through
    to the template href — that is the bug, one build later."""
    out = _run(tmp_path, _plan(cache={"upd": 5}))
    assert out["calls"] == ["GET /api/stats"], out
    assert out["nav"] == [TV_UPD], out
    assert out["clicks"][0]["prevented"] is True, "the anchor's /movies href must not win the race"


def test_an_unreachable_server_still_takes_you_somewhere(tmp_path):
    """A resolve that cannot happen falls back to the badge's own href — a dead click would be worse."""
    out = _run(tmp_path, _plan(cache={"upd": 5}, stats="fail"))
    assert out["nav"] == ["/movies?attn_pills=update"], out


def test_a_pill_the_server_no_longer_backs_hides_instead_of_landing_nowhere(tmp_path):
    """The rows were dealt with in another tab: the cached count is stale, the server reports no impacted tab.
    Navigating anywhere would show 0 matches, so the pill retires itself."""
    out = _run(tmp_path, _plan(cache={"upd": 5}, stats={"updates": {"pending": 0, "tabs": []}}))
    assert out["nav"] == [], out
    assert out["badges"][UPD]["hidden"] is True


def test_several_impacted_tabs_still_cycle(tmp_path):
    """v1.24.44/.48: with more than one impacted tab the click rotates. Here the operator is ON tv, so the next
    click goes to the other one — and back again."""
    tabs = [{"tab": "tv", "fourk": False, "count": 5}, {"tab": "movies", "fourk": True, "count": 2}]
    out = _run(tmp_path, _plan(
        cache={"upd": 7, "routes": {"upd": {"href": TV_UPD, "tabs": tabs}}},
        clicks=[UPD]))
    assert out["nav"] == ["/movies?fourk=1&attn_pills=update"], out


def test_every_cycling_pill_carries_its_own_route(tmp_path):
    """UPD was the one reported, but FAIL / DROP / RE-PUSH all ship the same /movies default in base.html."""
    routes = {
        "fail": {"href": "/anime?fourk=0&attn_pills=fail", "tabs": [{"tab": "anime", "fourk": False, "count": 1}]},
        "drop": {"href": "/tv?fourk=1&tdb_pills=dropped", "tabs": [{"tab": "tv", "fourk": True, "count": 3}]},
        "repush": {"href": "/collections?fourk=0&attn_pills=repush",
                   "tabs": [{"tab": "collections", "fourk": False, "count": 2}]},
    }
    out = _run(tmp_path, _plan(cache={"fail": 1, "drop": 3, "repush": 2, "routes": routes}, clicks=[]))
    for key, route in routes.items():
        pid = PILLS[key][0]
        assert out["badges"][pid]["href"] == route["href"], key
        assert out["badges"][pid]["hidden"] is False, key


def test_the_cache_the_poll_writes_is_the_cache_the_next_load_reads(tmp_path):
    """The round trip, through both real functions: what refreshTopbarStatus stashes must be what
    prepopulateBadgesFromCache can route from."""
    stats = {"updates": {"pending": 5}, "failures": {"total": 1}, "drops": {"total": 0}, "repush": {"total": 0}}
    routes = {"upd": {"href": TV_UPD, "tabs": [{"tab": "tv", "fourk": False, "count": 5}]}}
    written = _run(tmp_path, _plan(prepopulate=False, clicks=[],
                                   persist={"stats": stats, "routes": routes}))["cache"]
    assert written["upd"] == 5 and written["fail"] == 1, written
    assert written["routes"]["upd"]["href"] == TV_UPD, written
    out = _run(tmp_path, _plan(cache=written))
    assert out["nav"] == [TV_UPD], "a payload written by one page load must route the next one"


def test_the_template_default_is_still_the_no_js_fallback():
    """base.html keeps a plain href for right-click / no-JS; the point of the tag is that JS stops DEPENDING on it
    while the badge is visible."""
    assert '<a href="/movies?attn_pills=update"' in BASE_HTML


# ── the poll side: the REAL refreshTopbarStatus, so the four stash sites are covered ──

POLL_DRIVER = r"""
const fs = require("node:fs");
const vm = require("node:vm");
const [, , srcPath, planPath] = process.argv;
const plan = JSON.parse(fs.readFileSync(planPath, "utf8"));

const els = {};
function stub(sel) {
  if (!els[sel]) {
    els[sel] = {
      sel, hidden: true, textContent: "", title: "", href: null, value: "", checked: false,
      dataset: {}, style: {}, children: [], firstChild: null,
      classList: { add: () => {}, remove: () => {}, toggle: () => {}, contains: () => false },
      setAttribute: () => {}, removeAttribute: () => {}, getAttribute: () => null,
      addEventListener: () => {}, removeEventListener: () => {}, appendChild: () => {}, remove: () => {},
      querySelector: () => null, querySelectorAll: () => [], closest: () => null, focus: () => {},
    };
  }
  return els[sel];
}
const stored = {};
const calls = [];
const noop = () => {};
// anything refreshTopbarStatus leans on that lives outside the slice resolves to a tolerant no-op: the
// badge blocks themselves only use $, api, localStorage and JSON, which are real below.
const anyStub = new Proxy(function () {}, {
  get: (t, k) => (typeof k === "symbol" ? undefined : anyStub),
  apply: () => anyStub, has: () => true,
});
const globals = {
  console, JSON, Math, Object, Array, Number, String, Boolean, Date, URLSearchParams, Promise,
  $: (sel) => stub(sel),
  $$: () => [],
  api: async (method, path) => { calls.push(`${method} ${path}`); return plan.stats; },
  document: {
    getElementById: (id) => stub(`#${id}`),
    querySelector: (sel) => stub(sel),
    querySelectorAll: () => [],
    body: stub("body"), documentElement: stub("html"), hidden: false,
    addEventListener: noop,
  },
  window: { location: { pathname: "/tv", search: "", href: "/tv" }, addEventListener: noop,
            motifOps: { refresh: noop, poll: noop }, innerWidth: 1440 },
  localStorage: { getItem: (k) => (k in stored ? stored[k] : null), setItem: (k, v) => { stored[k] = v; },
                  removeItem: (k) => { delete stored[k]; } },
  sessionStorage: { getItem: () => null, setItem: noop, removeItem: noop },
  setTimeout: () => 0, clearTimeout: noop, setInterval: () => 0, clearInterval: noop,
  fmt: { num: (n) => String(n) },
  motifOps: { refresh: noop, poll: noop },
  loadLibrary: noop, libraryRapidPoll: noop, bumpUnreadBadge: noop, renderStat: noop,
  // helpers refreshTopbarStatus leans on that live outside the slice — the badge blocks don't read them
  _deriveEnumStashes: () => ({}),
};
const ctx = vm.createContext(new Proxy(globals, {
  has: () => true,
  get: (t, k) => (k in t ? t[k] : (typeof k === "symbol" ? undefined : anyStub)),
  set: (t, k, v) => { t[k] = v; return true; },
}));
vm.runInContext(fs.readFileSync(srcPath, "utf8"), ctx);
(async () => {
  await ctx.refreshTopbarStatus();
  process.stdout.write(JSON.stringify({
    calls,
    cache: stored["motif:topbar_counts"] ? JSON.parse(stored["motif:topbar_counts"]) : null,
    badges: Object.fromEntries(Object.keys(els).filter((k) => k.includes("topbar-"))
      .map((k) => [k, { href: els[k].href, hidden: els[k].hidden, text: els[k].textContent,
                        dataset: els[k].dataset }])),
  }));
})().catch((e) => { console.error(e); process.exit(1); });
"""


def _poll_src() -> str:
    return "\n".join([
        slice_to_next(APP_JS, "  async function refreshTopbarStatus()", "\n  function ", "\n  async function "),
        slice_to_next(APP_JS, "  function topbarCachePayload(", "\n  function ", "\n  async function "),
    ])


def _poll(tmp_path, stats) -> dict:
    tmp_path.mkdir(parents=True, exist_ok=True)
    (tmp_path / "src.js").write_text(_poll_src())
    (tmp_path / "plan.json").write_text(json.dumps({"stats": stats}))
    (tmp_path / "poll.js").write_text(POLL_DRIVER)
    r = subprocess.run([_NODE, str(tmp_path / "poll.js"), str(tmp_path / "src.js"), str(tmp_path / "plan.json")],
                       capture_output=True, text=True, timeout=60)
    assert r.returncode == 0, r.stderr[-2000:]
    return json.loads(r.stdout)


POLL_STATS = {
    "updates": {"pending": 5, "tab_hint": "tv", "tabs": [{"tab": "tv", "fourk": False, "count": 5}]},
    "failures": {"total": 2, "tab_hint": "anime", "tabs": [{"tab": "anime", "fourk": False, "count": 2}]},
    "drops": {"total": 1, "tab_hint": "tv", "tabs": [{"tab": "tv", "fourk": True, "count": 1}]},
    "repush": {"total": 3, "tab_hint": "collections",
               "tabs": [{"tab": "collections", "fourk": False, "count": 3}]},
}


def test_the_poll_stashes_the_route_it_just_resolved(tmp_path):
    """The wiring, through the real refreshTopbarStatus: each badge writes its route into the cache the next page
    load reads. Without this the payload is shaped correctly and always empty — the bug, one refactor later."""
    out = _poll(tmp_path, POLL_STATS)
    routes = out["cache"]["routes"]
    assert routes["upd"]["href"] == TV_UPD, routes
    assert routes["fail"]["href"] == "/anime?fourk=0&attn_pills=fail", routes
    assert routes["drop"]["href"] == "/tv?fourk=1&tdb_pills=dropped", routes
    assert routes["repush"]["href"] == "/collections?fourk=0&attn_pills=repush", routes
    assert routes["upd"]["tabs"] == POLL_STATS["updates"]["tabs"], routes


def test_the_poll_still_caches_the_counts(tmp_path):
    out = _poll(tmp_path, POLL_STATS)
    assert {k: out["cache"][k] for k in ("upd", "fail", "drop", "repush")} == {"upd": 5, "fail": 2, "drop": 1,
                                                                              "repush": 3}


def test_what_the_poll_writes_routes_the_next_load(tmp_path):
    """End to end across the two harnesses: poll -> cache -> paint -> click, with no hand-written payload."""
    written = _poll(tmp_path / "poll", POLL_STATS)["cache"]
    out = _run(tmp_path / "click", _plan(cache=written))
    assert out["nav"] == [TV_UPD], out
    assert out["calls"] == [], "the route came from the cache — no network needed for the click"


def test_a_badge_with_nothing_behind_it_caches_no_route(tmp_path):
    """A zero count hides the badge; there is no destination to remember, and the stale one must not linger."""
    stats = dict(POLL_STATS, updates={"pending": 0, "tabs": []})
    out = _poll(tmp_path, stats)
    assert "upd" not in out["cache"]["routes"], out["cache"]["routes"]
    assert out["badges"]["#topbar-updates-badge"]["hidden"] is True
