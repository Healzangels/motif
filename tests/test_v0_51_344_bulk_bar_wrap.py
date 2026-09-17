"""v0.51.344 PB-071: the library bulk bar wraps when its primaries can't share one row, and re-lays out when a result label grows."""
from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
from html.parser import HTMLParser
from pathlib import Path

import pytest

from _slice_helpers import slice_between

REPO = Path(__file__).resolve().parent.parent
STATIC = REPO / "app" / "web" / "static"
APP_JS = (STATIC / "app.js").read_text()
APP_CSS = (STATIC / "app.css").read_text()
LIBRARY_HTML = (REPO / "app" / "web" / "templates" / "library.html").read_text()
_NODE = shutil.which("node")

if os.environ.get("MOTIF_REQUIRE_NODE") and not _NODE:
    raise RuntimeError("MOTIF_REQUIRE_NODE=1 but node is not on PATH — the bulk-bar layout checks would silently not run")

needs_node = pytest.mark.skipif(not _NODE, reason="node not installed")

LAYOUT_BLOCK = slice_between(APP_JS, "const _BULK_BAR_PRIMARY_IDS = new Set([", "function _activeAxisFilterCount()")
# the class the layout adds when one row cannot hold the bar; read from the code so the CSS test checks the same name
WRAP_CLASSES = re.findall(r"bar\.classList\.add\('([\w-]+)'\)", LAYOUT_BLOCK)
PHONE = 319  # the bar's width at a 375px viewport (28px page padding each side)
DESKTOP = 1224  # the bar's width at 1280px
LONG = "// 12 RESTORED · 3 WAITING ON DOWNLOAD · 2 FAILED"


class _Buttons(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.buttons, self._open = [], None

    def handle_starttag(self, tag, attrs):
        if tag == "button":
            self._open = [dict(attrs)["id"], ""]

    def handle_data(self, data):
        if self._open is not None:
            self._open[1] += data

    def handle_endtag(self, tag):
        if tag == "button" and self._open is not None:
            self.buttons.append((self._open[0], self._open[1].strip()))
            self._open = None


def _bar_buttons():
    bar = slice_between(LIBRARY_HTML, '<div id="library-bulk-bar"', '<details class="row-menu" id="library-bulk-overflow-menu"')
    parser = _Buttons()
    parser.feed(re.sub(r"\{#.*?#\}", "", bar, flags=re.S))
    return parser.buttons


BUTTONS = _bar_buttons()
REST_FIVE = ["library-select-all-filtered-btn", "library-clear-selection-btn", "library-download-selected-btn",
             "library-adopt-selected-btn", "library-export-csv-btn"]
TWELVE = REST_FIVE[:3] + ["library-cloud-backup-btn", "library-adopt-selected-btn", "library-push-selected-btn",
                          "library-restore-from-plex-btn", "library-switch-to-api-btn", "library-let-plex-serve-btn",
                          "library-bulk-probe-tdb-btn", "library-ack-selected-btn", "library-export-csv-btn"]

# A fake DOM whose widths are the ones measured in Chrome at c81ec6e7 (375px: bar 28-347, SELECT ALL 249-451, CLEAR 465-551,
# // MORE 565-660), and whose MutationObserver delivers what the DOM does: a textContent write is a childList record on the button.
_HARNESS = r"""
"use strict";
const fs = require("fs");
const vm = require("vm");
const { block, buttons, wrapClass, cases } = JSON.parse(fs.readFileSync(0, "utf8"));
const GAP = 14, INSET_L = 22, INSET_R = 19, LEFT = 28;
const world = { width: 0, bar: null, layouts: 0, deliveries: 0, loop: false, observers: [] };

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
    const cls = this._cls;
    this.classList = { add: (k) => cls.add(k), remove: (k) => cls.delete(k), contains: (k) => cls.has(k) };
  }
  get textContent() { return this._text; }
  set textContent(v) { this._text = String(v); notify(this); }
  get firstChild() { return this.children[0] || null; }
  get parentNode() { return this.parent; }
  append(node) { node.parent = this; this.children.push(node); return node; }
  insertBefore(node, ref) {
    if (node.parent) {
      const old = node.parent;
      old.children.splice(old.children.indexOf(node), 1);
      notify(old);
    }
    const i = ref ? this.children.indexOf(ref) : -1;
    if (i < 0) this.children.push(node); else this.children.splice(i, 0, node);
    node.parent = this;
    notify(this);
    return node;
  }
  contains(n) { for (let x = n; x; x = x.parent) if (x === this) return true; return false; }
  addEventListener() {}  // v0.51.344: // MORE's toggle listener — this harness never opens the menu
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

function snap(tag, byId) {
  const bar = world.bar;
  const innerR = LEFT + world.width - INSET_R;
  const panel = bar.querySelector("[data-bulk-overflow-panel]");
  return {
    tag,
    wrap: !!wrapClass && bar._cls.has(wrapClass),
    panel: panel.children.map((c) => c.id),
    past: bar.children.filter((c) => c.style.display !== "none" && rect(c).right > innerR + 1).map((c) => c.id || c.kind),
    more: byId["library-bulk-overflow-menu"].style.display !== "none",
    barOrdered: ordered(bar.children.filter((c) => c.tagName === "BUTTON").map((c) => c.id)),
    panelOrdered: ordered(panel.children.map((c) => c.id)),
    layouts: world.layouts,
    loop: world.loop,
  };
}

const settle = () => new Promise((r) => setImmediate(r));
const TEMPLATE = buttons.map(([id]) => id);
const ordered = (ids) => ids.every((id, i) => i === 0 || TEMPLATE.indexOf(ids[i - 1]) < TEMPLATE.indexOf(id));

async function scenario(c) {
  world.width = c.width; world.layouts = 0; world.deliveries = 0; world.loop = false; world.observers = [];
  const byId = {};
  const bar = world.bar = byId["library-bulk-bar"] = new El("div", { id: "library-bulk-bar" });
  bar.append(new El("span", { kind: "glyph" }));
  bar.append(new El("span", { kind: "caption" }));
  for (const [id, text] of buttons) {
    byId[id] = bar.append(new El("button", { id, text, cls: ["btn", "btn-tiny"], display: c.visible.includes(id) ? "" : "none" }));
  }
  const menu = byId["library-bulk-overflow-menu"] = bar.append(new El("details", { id: "library-bulk-overflow-menu", display: "none" }));
  menu.append(new El("div", { panel: true }));
  class FakeMutationObserver {
    constructor(cb) { this.cb = cb; }
    observe(root, opts) {
      if (!opts || !opts.childList) throw new Error("a textContent write is a childList record; the observer must ask for childList");
      world.observers.push({ root, subtree: !!opts.subtree, cb: this.cb, queue: [], scheduled: false });
    }
  }
  const ctx = vm.createContext({
    document: { getElementById: (id) => { if (id === "library-bulk-overflow-menu") world.layouts += 1; return byId[id] || null; } },
    getComputedStyle: () => ({ paddingRight: "18px", borderRightWidth: "1px" }),
    ResizeObserver: class { observe() {} },
    MutationObserver: FakeMutationObserver,
  });
  vm.runInContext(block, ctx);
  vm.runInContext("_installBulkBarObserver(); _layoutBulkBar();", ctx);
  await settle();
  const out = [snap("rest", byId)];
  for (const s of c.steps || []) {
    let id = s.id;
    if (id === "LAST_IN_BAR") id = world.lastInBar = bar.children.filter((e) => e.tagName === "BUTTON" && e.style.display !== "none").pop().id;
    if (id === "SAME") id = world.lastInBar;
    if (s.op === "run") { byId[id].disabled = true; byId[id].textContent = s.text; }
    if (s.op === "done") { byId[id].disabled = false; byId[id].textContent = s.text; }
    if (s.op === "resize") { world.width = s.width; vm.runInContext("_layoutBulkBar();", ctx); }
    if (s.op === "show") { byId[id].style.display = ""; vm.runInContext("_layoutBulkBar();", ctx); }
    await settle();
    out.push(Object.assign(snap(s.op, byId), { id, inPanel: id ? menu.contains(byId[id]) : null }));
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
    harness = tmp_path / "bulk_bar.js"
    harness.write_text(_HARNESS)
    wrap = WRAP_CLASSES[0] if WRAP_CLASSES else None
    payload = {"block": block, "buttons": BUTTONS, "wrapClass": wrap, "cases": cases}
    r = subprocess.run([_NODE, str(harness)], input=json.dumps(payload), capture_output=True, text=True, timeout=60, cwd=REPO)
    assert r.returncode == 0, r.stderr[-2000:]
    return json.loads(r.stdout)


def _label(button_id):
    return dict(BUTTONS)[button_id]


def test_the_fake_bar_is_the_template_bar():
    ids = [b for b, _ in BUTTONS]
    assert ids[:2] == ["library-select-all-filtered-btn", "library-clear-selection-btn"], "the primaries lead the bar"
    assert set(TWELVE) <= set(ids) and ids[-1] == "library-export-csv-btn"
    assert _label("library-select-all-filtered-btn") == "// SELECT ALL FILTERED"


@needs_node
def test_the_harness_reproduces_the_measured_overflow_without_the_wrap(tmp_path):
    assert len(WRAP_CLASSES) == 1, WRAP_CLASSES
    block = LAYOUT_BLOCK.replace(f"bar.classList.add('{WRAP_CLASSES[0]}')", "void 0")
    rest = _drive([{"width": PHONE, "visible": REST_FIVE}], tmp_path, block=block)[0][0]
    assert rest["past"] == ["library-select-all-filtered-btn", "library-clear-selection-btn", "library-bulk-overflow-menu"], \
        "premise: at 375px the one-row bar paints its primaries and // MORE past its edge (the live 660px)"


@needs_node
def test_a_phone_bar_wraps_instead_of_spilling_and_keeps_more(tmp_path):
    rest = _drive([{"width": PHONE, "visible": REST_FIVE}], tmp_path)[0][0]
    assert rest["wrap"] and rest["past"] == [], "the primaries + // MORE spilled past the bar at 375px"
    assert rest["more"] and rest["panel"] == REST_FIVE[2:], "wrapping must not dump every action inline — // MORE still holds them"


@needs_node
def test_a_bar_that_fits_one_row_never_wraps_and_a_widened_bar_unwraps(tmp_path):
    desk, grown = _drive([{"width": DESKTOP, "visible": TWELVE},
                          {"width": PHONE, "visible": REST_FIVE, "steps": [{"op": "resize", "width": DESKTOP}]}], tmp_path)
    assert not desk[0]["wrap"] and desk[0]["past"] == [] and desk[0]["panel"], "a desktop bar wrapped, or stopped using // MORE"
    assert grown[0]["wrap"] and not grown[1]["wrap"] and grown[1]["past"] == [], "a bar that fits again stayed wrapped"


@needs_node
def test_a_grown_result_label_re_lays_out_the_bar_and_stays_readable(tmp_path):
    rest, ran, done = _drive([{"width": DESKTOP, "visible": TWELVE, "steps": [
        {"op": "run", "id": "LAST_IN_BAR", "text": LONG},
        {"op": "done", "id": "SAME", "text": _label("library-cloud-backup-btn")}]}], tmp_path)[0]
    assert ran["id"] == "library-cloud-backup-btn", "premise: the desktop bar ends at DOWNLOAD PLEX BACKUP"
    assert ran["past"] == [], "a result label written after layout left the bar spilling past its edge"
    assert not ran["inPanel"], "the re-layout hid the very result label it ran for inside // MORE"
    assert len(ran["panel"]) > len(rest["panel"]), "premise: the label needed room another action gave up"
    assert not ran["loop"] and ran["layouts"] - rest["layouts"] <= 2, "layout's own moves re-triggered the label observer"
    assert (done["panel"], done["barOrdered"], done["panelOrdered"]) == (rest["panel"], True, True), \
        "the bar did not settle back in template order once the label reverted"


@needs_node
def test_a_running_button_leaves_the_phone_panel_and_returns_when_done(tmp_path):
    restore = "library-restore-from-plex-btn"
    rest, ran, done = _drive([{"width": PHONE, "visible": REST_FIVE + [restore], "steps": [
        {"op": "run", "id": restore, "text": LONG},
        {"op": "done", "id": restore, "text": _label(restore)}]}], tmp_path)[0]
    assert restore in rest["panel"], "premise: at rest the phone bar parks RESTORE in // MORE"
    assert not ran["inPanel"] and ran["wrap"] and ran["past"] == [], "a running label stayed hidden in // MORE, or spilled"
    assert done["inPanel"] and done["panel"] == rest["panel"] and done["barOrdered"], \
        "the bar did not settle back in template order once the label reverted"
    assert not done["loop"]


@needs_node
def test_a_hidden_button_shown_later_keeps_its_template_place(tmp_path):
    ack = "library-ack-selected-btn"
    rest, shown = _drive([{"width": DESKTOP, "visible": [b for b in TWELVE if b != ack],
                           "steps": [{"op": "show", "id": ack}]}], tmp_path)[0]
    assert rest["panel"][-1] == "library-export-csv-btn" and ack not in rest["panel"], "premise: layout parked the tail around hidden ACK"
    assert shown["barOrdered"] and shown["panelOrdered"], "a button hidden during a pass came back out of template order"


def _css_rules(css):
    css = re.sub(r"/\*.*?\*/", "", css, flags=re.S)
    rules, heads, start = [], [], 0
    for i, ch in enumerate(css):
        if ch == "{":
            heads.append((css[start:i].strip(), i + 1))
            start = i + 1
        elif ch == "}":
            head, opened = heads.pop()
            if "{" not in css[opened:i]:
                rules.append((" ".join(head.split()), css[opened:i], bool(heads)))
            start = i + 1
    return rules


def _decls(body):
    return {k.strip(): v.strip() for k, v in (d.split(":", 1) for d in body.split(";") if ":" in d)}


def _bar_wrap_rules_in_breakpoints(css, wrap_class):
    # v0.51.344: only the bar's own wrapped rules — another component may reuse the generic state class inside a breakpoint
    own = re.compile(rf"#library-bulk-bar\.{re.escape(wrap_class)}(?![\w-])")
    return [head for head, _, nested in _css_rules(css) if nested and own.search(head)]


def test_the_breakpoint_scan_reads_only_the_bars_own_wrap_rules():
    media = "@media (max-width: 600px) {{ {} {{ gap: 4px; }} }}\n.x {{ color: red; }}"
    assert _bar_wrap_rules_in_breakpoints(media.format("#library-bulk-bar.is-wrapped > .btn"), "is-wrapped")
    for head in (".notif-row.is-wrapped", "#library-bulk-bar.is-wrapped-x", "#library-bulk-bar > .btn"):
        assert not _bar_wrap_rules_in_breakpoints(media.format(head), "is-wrapped"), head


def test_the_wrap_class_the_layout_adds_is_styled_at_every_width():
    removed = re.findall(r"bar\.classList\.remove\('([\w-]+)'\)", LAYOUT_BLOCK)
    assert len(WRAP_CLASSES) == 1 and removed == WRAP_CLASSES, "the layout must add and reset one wrap class"
    cls = f"#library-bulk-bar.{WRAP_CLASSES[0]}"
    rules = _css_rules(APP_CSS)
    assert not _bar_wrap_rules_in_breakpoints(APP_CSS, WRAP_CLASSES[0]), \
        "the wrap is measured by the layout, so its CSS must apply at every width — not inside a breakpoint"
    top = {head: _decls(body) for head, body, nested in rules if not nested}
    assert top[cls].get("flex-wrap") == "wrap", top.get(cls)
    assert top[f"{cls} > .btn"].get("white-space") == "normal" and top[f"{cls} > .btn"].get("max-width") == "100%", \
        "an in-bar result label wider than the bar must wrap inside its own button"
    assert not [head for head, decls in top.items() if head.startswith(cls + " ") and ".btn" in head
                and not head.startswith(cls + " > ") and decls.get("white-space") == "normal"], \
        "only in-bar buttons may wrap their text — a descendant selector wraps the // MORE panel's labels too"
    assert top[cls].get("position") == "relative" and top[f"{cls} > .row-menu"].get("position") == "static", \
        "a wrapped // MORE must anchor its panel to the bar, not to a summary that can start a row at the left edge"
    assert (top[f"{cls} .row-menu-panel"].get("left"), top[f"{cls} .row-menu-panel"].get("right")) == ("0", "auto"), \
        "the base panel is right-anchored; below a wrapped bar it opened off-screen"
    assert _decls(dict((h, b) for h, b, n in rules if not n)["#library-bulk-bar"])["flex-wrap"] == "nowrap", \
        "the one-row bar is still the default"
