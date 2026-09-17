"""v0.51.344 (integration review R1-F6): a layout asked for while // MORE is open waits for its close — a progress write never rebuilds the open panel."""
from __future__ import annotations

import json
import os
import shutil
import subprocess

import pytest

from test_v0_51_344_bulk_bar_wrap import APP_CSS, BUTTONS, DESKTOP, LAYOUT_BLOCK, LONG, TWELVE, WRAP_CLASSES, _css_rules, _decls

_NODE = shutil.which("node")

if os.environ.get("MOTIF_REQUIRE_NODE") and not _NODE:
    raise RuntimeError("MOTIF_REQUIRE_NODE=1 but node is not on PATH — the open-panel checks would silently not run")

needs_node = pytest.mark.skipif(not _NODE, reason="node not installed")
PUSH = "library-push-selected-btn"

# The wrap test's fake bar, plus what this finding needs of the DOM: <details>.open fires its toggle listeners, a removed
# subtree drops focus to BODY, and a scroller emptied of its children clamps scrollTop to 0 (measured 105 -> 0 in Chrome).
_HARNESS = r"""
"use strict";
const fs = require("fs");
const vm = require("vm");
const { block, buttons, wrapClass, cases } = JSON.parse(fs.readFileSync(0, "utf8"));
const GAP = 14, INSET_L = 22, INSET_R = 19, LEFT = 28;
const world = { width: 0, bar: null, body: null, active: null, deliveries: 0, loop: false, observers: [], panelMoves: 0 };

function notify(target) {
  for (const o of world.observers) {
    if (!(o.root === target || (o.subtree && o.root.contains(target)))) continue;
    o.queue.push({ type: "childList", target });
    if (o.scheduled) continue;
    o.scheduled = true;
    queueMicrotask(() => {
      o.scheduled = false;
      const records = o.queue.splice(0);
      if (++world.deliveries > 200) { world.loop = true; return; }
      o.cb(records);
    });
  }
}

class El {
  constructor(tag, opts = {}) {
    this.tagName = this.nodeName = tag.toUpperCase();
    this.id = opts.id || "";
    this.kind = opts.kind || "";
    this._text = opts.text || "";
    this._cls = new Set(opts.cls || []);
    this.panel = !!opts.panel;
    this.children = [];
    this.parent = null;
    this.style = { display: opts.display || "" };
    this.disabled = false;
    this.listeners = {};
    this.scrollTop = 0;
    this._open = false;
    const cls = this._cls;
    this.classList = { add: (k) => cls.add(k), remove: (k) => cls.delete(k), contains: (k) => cls.has(k) };
  }
  get textContent() { return this._text; }
  set textContent(v) { this._text = String(v); notify(this); }
  get open() { return this._open; }
  set open(v) {
    if (!!v === this._open) return;
    this._open = !!v;
    for (const fn of this.listeners.toggle || []) fn({ target: this });
  }
  addEventListener(type, fn) { (this.listeners[type] = this.listeners[type] || []).push(fn); }
  focus() { world.active = this; }
  get firstChild() { return this.children[0] || null; }
  get parentNode() { return this.parent; }
  append(node) { node.parent = this; this.children.push(node); return node; }
  insertBefore(node, ref) {
    if (node.parent) {
      const old = node.parent;
      old.children.splice(old.children.indexOf(node), 1);
      if (world.active && node.contains(world.active)) world.active = world.body;
      if (old.panel) { world.panelMoves += 1; if (!old.children.length) old.scrollTop = 0; }
      notify(old);
    }
    const i = ref ? this.children.indexOf(ref) : -1;
    if (i < 0) this.children.push(node); else this.children.splice(i, 0, node);
    node.parent = this;
    notify(this);
    return node;
  }
  contains(n) { for (let x = n; x; x = x.parent) if (x === this) return true; return false; }
  walk() { const out = []; const rec = (e) => { for (const c of e.children) { out.push(c); rec(c); } }; rec(this); return out; }
  querySelector(sel) { return this.walk().find((e) => matches(e, sel)) || null; }
  querySelectorAll(sel) { return this.walk().filter((e) => matches(e, sel)); }
  getBoundingClientRect() { return rect(this); }
}

function matches(e, sel) {
  if (sel === "button.btn-tiny") return e.tagName === "BUTTON" && e._cls.has("btn-tiny");
  if (sel === "[data-bulk-overflow-panel]") return e.panel;
  throw new Error("the fake DOM does not model selector " + sel);
}

function width(e) {
  if (e.kind === "glyph") return 11;
  if (e.kind === "caption") return 160;
  if (e.tagName === "DETAILS") return 95;
  return Math.round(20 + 8.25 * e._text.length);
}

function rect(target) {
  const bar = world.bar;
  const innerL = LEFT + INSET_L, innerR = LEFT + world.width - INSET_R;
  if (target === bar) return { left: LEFT, right: LEFT + world.width, top: 0, height: 56 };
  const wrap = !!wrapClass && bar._cls.has(wrapClass);
  let x = innerL, line = 0;
  for (const c of bar.children) {
    if (c.style.display === "none") continue;
    let w = width(c);
    if (wrap && c.tagName === "BUTTON") w = Math.min(w, innerR - innerL);
    if (wrap && x > innerL && x + w > innerR) { line += 1; x = innerL; }
    if (c === target) return { left: x, right: x + w, top: line * 30, height: 23 };
    x += w + GAP;
  }
  return { left: 0, right: 0, top: 0, height: 0 };
}

function snap(tag, byId, sinceMoves) {
  const bar = world.bar;
  const innerR = LEFT + world.width - INSET_R;
  const menu = byId["library-bulk-overflow-menu"];
  const panel = bar.querySelector("[data-bulk-overflow-panel]");
  return {
    tag,
    open: menu.open,
    menuDisplay: menu.style.display,
    active: world.active === world.body ? "BODY" : (world.active ? world.active.id : null),
    panelScrollTop: panel.scrollTop,
    panelMoves: world.panelMoves - sinceMoves,
    panel: panel.children.map((c) => c.id),
    past: bar.children.filter((c) => c.style.display !== "none" && rect(c).right > innerR + 1).map((c) => c.id || c.kind),
    loop: world.loop,
  };
}

const settle = () => new Promise((r) => setImmediate(r));

async function scenario(c) {
  world.width = c.width; world.deliveries = 0; world.loop = false; world.observers = []; world.panelMoves = 0;
  const byId = {};
  world.body = new El("body");
  world.active = world.body;
  const bar = world.bar = byId["library-bulk-bar"] = world.body.append(new El("div", { id: "library-bulk-bar" }));
  bar.append(new El("span", { kind: "glyph" }));
  bar.append(new El("span", { kind: "caption" }));
  for (const [id, text] of buttons) {
    byId[id] = bar.append(new El("button", { id, text, cls: ["btn", "btn-tiny"], display: c.visible.includes(id) ? "" : "none" }));
  }
  const menu = byId["library-bulk-overflow-menu"] = bar.append(new El("details", { id: "library-bulk-overflow-menu", display: "none" }));
  const panel = menu.append(new El("div", { panel: true }));
  class FakeMutationObserver {
    constructor(cb) { this.cb = cb; }
    observe(root, opts) {
      if (!opts || !opts.childList) throw new Error("a textContent write is a childList record; the observer must ask for childList");
      world.observers.push({ root, subtree: !!opts.subtree, cb: this.cb, queue: [], scheduled: false });
    }
  }
  const ctx = vm.createContext({
    document: { getElementById: (id) => byId[id] || null },
    getComputedStyle: () => ({ paddingRight: "18px", borderRightWidth: "1px" }),
    ResizeObserver: class { observe() {} },
    MutationObserver: FakeMutationObserver,
  });
  vm.runInContext(block, ctx);
  vm.runInContext("_installBulkBarObserver(); _layoutBulkBar();", ctx);
  await settle();
  let moves = world.panelMoves;
  const out = [snap("rest", byId, 0)];
  moves = world.panelMoves;
  for (const s of c.steps || []) {
    let id = s.id;
    if (id === "FIRST_IN_PANEL") id = panel.children[0].id;
    if (s.op === "run") { byId[id].disabled = true; byId[id].textContent = s.text; }
    if (s.op === "done") { byId[id].disabled = false; byId[id].textContent = s.text; }
    if (s.op === "write") byId[id].textContent = s.text;
    if (s.op === "open") menu.open = true;
    if (s.op === "close") menu.open = false;
    if (s.op === "focus") byId[id].focus();
    if (s.op === "scroll") panel.scrollTop = s.top;
    if (s.op === "layout") vm.runInContext("_layoutBulkBar();", ctx);
    await settle();
    out.push(Object.assign(snap(s.op, byId, moves), { id, inPanel: id ? menu.contains(byId[id]) : null }));
    moves = world.panelMoves;
  }
  return out;
}

(async () => {
  const results = [];
  for (const c of cases) results.push(await scenario(c));
  process.stdout.write(JSON.stringify(results));
})().catch((e) => { process.stderr.write(String(e && e.stack || e)); process.exit(1); });
"""


def _drive(cases, tmp_path, block=LAYOUT_BLOCK):
    harness = tmp_path / "bulk_bar_open_panel.js"
    harness.write_text(_HARNESS)
    payload = {"block": block, "buttons": BUTTONS, "wrapClass": WRAP_CLASSES[0] if WRAP_CLASSES else None, "cases": cases}
    r = subprocess.run([_NODE, str(harness)], input=json.dumps(payload), capture_output=True, text=True, timeout=60)
    assert r.returncode == 0, r.stderr[-2000:]
    return json.loads(r.stdout)


@needs_node
def test_a_progress_write_while_more_is_open_keeps_focus_scroll_and_the_panel(tmp_path):
    steps = [{"op": "run", "id": PUSH, "text": "// PUSHING 0/50"}, {"op": "open"}, {"op": "focus", "id": "FIRST_IN_PANEL"},
             {"op": "scroll", "top": 40}, {"op": "write", "id": PUSH, "text": "// PUSHING 1/50"},
             {"op": "write", "id": PUSH, "text": "// PUSHING 1/50"}, {"op": "write", "id": PUSH, "text": LONG},
             {"op": "close", "id": PUSH}]
    rest, ran, opened, focused, scrolled, w1, w2, w3, closed = _drive([{"width": DESKTOP, "visible": TWELVE, "steps": steps}], tmp_path)[0]
    assert rest["panel"] and not ran["inPanel"] and ran["past"] == [], "premise: the desktop bar parks actions in // MORE and keeps the running button inline"
    target = focused["id"]
    assert target in ran["panel"] and opened["open"] and focused["active"] == target and scrolled["panelScrollTop"] == 40, \
        "premise: a button in the open panel has focus and the panel is scrolled"
    for w in (w1, w2, w3):
        assert (w["active"], w["panelScrollTop"], w["open"], w["menuDisplay"] != "none", w["panelMoves"]) == (target, 40, True, True, 0), \
            (w["tag"], "a progress write on the running button rebuilt the open panel", w)
    assert not closed["open"] and closed["panelMoves"] > 0 and closed["past"] == [] and not closed["inPanel"], \
        "the layout the long label asked for did not run at the close"
    assert not closed["loop"]


@needs_node
def test_a_layout_asked_for_while_more_is_open_waits_for_the_close(tmp_path):
    # the poll's updateLibrarySelectionUi and the ResizeObserver call _layoutBulkBar directly
    steps = [{"op": "open"}, {"op": "focus", "id": "FIRST_IN_PANEL"}, {"op": "layout"}, {"op": "close"}]
    rest, _opened, focused, laid, closed = _drive([{"width": DESKTOP, "visible": TWELVE, "steps": steps}], tmp_path)[0]
    assert (laid["active"], laid["panelMoves"], laid["open"], laid["panel"]) == (focused["id"], 0, True, rest["panel"]), \
        "a layout asked for while the panel was open rebuilt it"
    assert closed["panelMoves"] > 0 and closed["past"] == [] and closed["panel"] == rest["panel"], \
        "the deferred layout did not run at the close"


@needs_node
def test_a_close_with_nothing_deferred_leaves_the_panel_untouched(tmp_path):
    rest, opened, closed = _drive([{"width": DESKTOP, "visible": TWELVE, "steps": [{"op": "open"}, {"op": "close"}]}], tmp_path)[0]
    assert opened["open"] and not closed["open"]
    assert closed["panelMoves"] == 0 and closed["panel"] == rest["panel"], "a plain open and close rebuilt the panel for nothing"


def test_the_wrapped_panel_is_capped_at_the_bars_width():
    cls = f"#library-bulk-bar.{WRAP_CLASSES[0]} .row-menu-panel"
    top = {head: _decls(body) for head, body, nested in _css_rules(APP_CSS) if not nested}
    # measured in headless Chrome: a 440px result label parked in the panel painted the open panel to 460px on a
    # 360-375px viewport (document scrollWidth 460); capped at the bar it scrolls inside the panel instead
    assert top[cls].get("max-width") == "100%", top.get(cls)
