"""v0.51.347 — the dashboard carousel scrolls by hand.

the user: "on the carousel on the dashboard we should be able to click and drag to go backwards in the carousel or to
manually move it in case we want to go back to an entry that's scrolled off screen".

Auto-scroll is ON by default and v1.24.61 hides the strip's scrollbar while it runs, so a mouse had no way back to a
poster that had already scrolled past. The real app.js runs here under node against a scripted pointer: a press that
moves past the slop drags the strip 1:1 (dragging right goes BACK), the click that ends a drag does not open the INFO
card while a plain click still does, the auto-scroll loop freezes while the drag is live and carries on from the new
position, a touch pointer keeps the native scroll, and the 30s poll never rebuilds the strip mid-drag.
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
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
_NODE = shutil.which("node")

pytestmark = pytest.mark.skipif(not _NODE, reason="node not installed")

# a strip 3,000 px wide in a 600 px viewport: 2,400 px of travel, so a drag can be clamped at either end
DOM = r"""
function makeStrip() {
  const listeners = {};
  const el = {
    scrollWidth: 3000, clientWidth: 600, _scroll: 0, style: {}, dataset: {}, captured: null,
    classes: new Set(), hovered: false, prevented: [],
    get scrollLeft() { return this._scroll; },
    set scrollLeft(v) { this._scroll = Math.max(0, Math.min(Math.round(v), this.scrollWidth - this.clientWidth)); },
    classList: {
      add: (c) => el.classes.add(c), remove: (c) => el.classes.delete(c),
      toggle: (c, on) => (on ? el.classes.add(c) : el.classes.delete(c)),
      contains: (c) => el.classes.has(c),
    },
    matches: (sel) => (sel === ':hover' ? el.hovered : false),
    setPointerCapture: (id) => { el.captured = id; },
    hasPointerCapture: (id) => el.captured === id,
    releasePointerCapture: (id) => { if (el.captured === id) el.captured = null; },
    addEventListener: (type, fn, capture) => {
      (listeners[type] = listeners[type] || []).push({ fn, capture: !!capture });
    },
    removeEventListener: (type, fn, capture) => {
      const arr = listeners[type] || [];
      const at = arr.findIndex((l) => l.fn === fn && l.capture === !!capture);
      if (at !== -1) arr.splice(at, 1);
    },
    fire: (type, ev) => {
      const e = Object.assign({ type, preventDefault: () => el.prevented.push(type), stopPropagation: () => {} }, ev);
      let swallowed = false;
      for (const l of (listeners[type] || []).slice()) {
        if (l.capture) { e.stopPropagation = () => { swallowed = true; }; }
        l.fn(e);
      }
      return swallowed;
    },
    listenerCount: (type) => (listeners[type] || []).length,
  };
  return el;
}
function makeEnv(strip) {
  const timers = [];
  const cb = { checked: true, addEventListener: () => {} };
  const stored = {};
  let frame = null;
  return {
    strip, cb, timers,
    runTimers: () => { const t = timers.splice(0); t.forEach((fn) => fn()); },
    frame: (ts) => { const f = frame; frame = null; if (f) f(ts); },
    hasFrame: () => frame !== null,
    ctx: {
      console,
      document: {
        hidden: false, hasFocus: () => true, querySelector: () => null,
        getElementById: (id) => (id === 'recently-added-strip' ? strip : id === 'recent-autoscroll' ? cb : null),
      },
      window: { addEventListener: () => {} },
      localStorage: { getItem: (k) => (k in stored ? stored[k] : null), setItem: (k, v) => { stored[k] = v; } },
      requestAnimationFrame: (fn) => { frame = fn; return 1; },
      cancelAnimationFrame: () => { frame = null; },
      setTimeout: (fn) => { timers.push(fn); return timers.length; },
    },
  };
}
module.exports = { makeStrip, makeEnv };
"""

DRIVER = r"""
const fs = require("node:fs");
const vm = require("node:vm");
const [, , domPath, srcPath, scenarioPath] = process.argv;
const { makeStrip, makeEnv } = require(domPath);
const plan = JSON.parse(fs.readFileSync(scenarioPath, "utf8"));
const strip = makeStrip();
if (plan.scroll !== undefined) strip.scrollLeft = plan.scroll;
const env = makeEnv(strip);
const ctx = vm.createContext(env.ctx);
vm.runInContext(fs.readFileSync(srcPath, "utf8"), ctx);
ctx.setup();
const out = [];
let pid = 7;
for (const step of plan.steps) {
  if (step.down) strip.fire('pointerdown', { pointerType: step.down.type || 'mouse', button: step.down.button ?? 0,
                                             clientX: step.down.x, pointerId: pid });
  if (step.move !== undefined) strip.fire('pointermove', { pointerType: 'mouse', clientX: step.move, pointerId: pid });
  if (step.up) { strip.fire('pointerup', { pointerId: pid }); pid += 1; }
  if (step.cancel) strip.fire('pointercancel', { pointerId: pid });
  if (step.dragstart) strip.fire('dragstart', {});
  if (step.frame !== undefined) env.frame(step.frame);
  if (step.timers) env.runTimers();
  if (step.hover !== undefined) strip.hovered = step.hover;
  if (step.click) out.push({ click_swallowed: strip.fire('click', {}) });
  if (step.read) {
    out.push({ scroll: strip.scrollLeft, dragging: !!ctx._setupCarouselAutoScroll._dragging,
               grabbing: strip.classList.contains('recent-strip-dragging'), captured: strip.captured,
               armed: env.hasFrame(), prevented: strip.prevented.slice(), clicks: strip.listenerCount('click') });
  }
}
process.stdout.write(JSON.stringify(out));
"""

REBUILD_DRIVER = r"""
const fs = require("node:fs");
const vm = require("node:vm");
const [, , srcPath, scenarioPath] = process.argv;
const plan = JSON.parse(fs.readFileSync(scenarioPath, "utf8"));
const created = [];
const strip = { dataset: {}, textContent: 'old', appendChild: () => {}, querySelectorAll: () => [] };
const block = { style: {} };
const ctx = vm.createContext({
  console,
  document: {
    getElementById: (id) => (id === 'recently-added-strip' ? strip : id === 'recently-added-block' ? block : null),
    createElement: (t) => { created.push(t); return { classList: { add: () => {} }, dataset: {}, style: {},
                                                      addEventListener: () => {}, append: () => {}, appendChild: () => {} }; },
  },
  api: async () => ({ items: [{ rating_key: 'rk1', placed_at: '2026-01-01', title: 'T', media_type: 'movie' }] }),
});
vm.runInContext(fs.readFileSync(srcPath, "utf8"), ctx);
ctx._setupCarouselAutoScroll._dragging = plan.dragging;
(async () => {
  await ctx.loadRecentlyAdded();
  process.stdout.write(JSON.stringify({ created, lastHash: strip.dataset.lastHash || null,
                                        text: strip.textContent, display: block.style.display }));
})().catch((e) => { console.error(e); process.exit(1); });
"""


def _autoscroll_src() -> str:
    return slice_to_next(APP_JS, "function _setupCarouselAutoScroll()", "\n  function ", "\n  async function ")


def _run(tmp_path, plan) -> list[dict]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    (tmp_path / "dom.js").write_text(DOM)
    # the real function, with its one-time guard reachable and a `setup()` entry point
    (tmp_path / "src.js").write_text(_autoscroll_src() + "\nfunction setup() { _setupCarouselAutoScroll(); }\n")
    (tmp_path / "plan.json").write_text(json.dumps(plan))
    (tmp_path / "driver.js").write_text(DRIVER)
    r = subprocess.run([_NODE, str(tmp_path / "driver.js"), str(tmp_path / "dom.js"), str(tmp_path / "src.js"),
                        str(tmp_path / "plan.json")], capture_output=True, text=True, timeout=60)
    assert r.returncode == 0, r.stderr[-2000:]
    return json.loads(r.stdout)


def test_a_mouse_drag_moves_the_strip_one_to_one_and_dragging_right_goes_back(tmp_path):
    [mid, end] = _run(tmp_path, {"scroll": 900, "steps": [
        {"down": {"x": 500}}, {"move": 560, "read": True},          # +60 px right → 60 px back
        {"move": 380, "read": True}, {"up": True},                   # -120 px from the press → 120 px forward
    ]})
    assert (mid["scroll"], mid["grabbing"], mid["dragging"], mid["captured"]) == (840, True, True, 7)
    assert end["scroll"] == 1020, end
    assert "pointermove" in end["prevented"], "a drag must preventDefault so the browser starts no text selection"


def test_a_press_under_the_slop_is_a_click_not_a_drag(tmp_path):
    [read, click] = _run(tmp_path, {"scroll": 900, "steps": [
        {"down": {"x": 500}}, {"move": 503, "read": True}, {"up": True}, {"click": True},
    ]})
    assert (read["scroll"], read["grabbing"], read["dragging"]) == (900, False, False)
    assert click["click_swallowed"] is False, "a plain click must still reach the card and open the INFO dialog"


def test_the_click_that_ends_a_drag_is_swallowed_and_only_that_one(tmp_path):
    [drag_click, after_timers, later_click, read] = _run(tmp_path, {"scroll": 900, "steps": [
        {"down": {"x": 500}}, {"move": 600}, {"up": True}, {"click": True},
        {"timers": True, "read": True},
        {"click": True},
        {"read": True},
    ]})
    assert drag_click["click_swallowed"] is True, "the click ending a drag must not open the poster under the pointer"
    assert later_click["click_swallowed"] is False, "the swallow is one-shot"
    assert after_timers["clicks"] == 0 and read["clicks"] == 0, "the swallowing listener must not accumulate"


def test_the_strip_clamps_at_both_ends(tmp_path):
    [start, endd] = _run(tmp_path, {"scroll": 40, "steps": [
        {"down": {"x": 500}}, {"move": 900, "read": True}, {"up": True},        # 400 px back from 40
        {"down": {"x": 500}}, {"move": -2500, "read": True}, {"up": True},      # 3,000 px forward from 0
    ]})
    assert start["scroll"] == 0, "dragging back past the first poster stops at the start"
    assert endd["scroll"] == 2400, "dragging past the last poster stops at the end (scrollWidth - clientWidth)"


def test_auto_scroll_freezes_under_the_drag_and_resumes_from_where_it_was_left(tmp_path):
    reads = _run(tmp_path, {"scroll": 900, "steps": [
        {"frame": 1000}, {"frame": 1100, "read": True},               # a free frame advances the strip
        {"down": {"x": 500}}, {"move": 620},
        {"frame": 1200}, {"frame": 1300, "read": True},               # frames under the drag must not move it
        {"up": True},
        {"frame": 1400}, {"frame": 1500, "read": True},               # and it carries on from the dragged position
    ]})
    free, held, resumed = reads
    assert free["scroll"] > 900, "premise: the loop advances the strip when nothing holds it"
    assert held["scroll"] == free["scroll"] - 120 + (900 - 900), held  # exactly the drag, no frame drift
    assert held["armed"] is True, "the loop must stay armed through a drag, not be cancelled"
    assert resumed["scroll"] > held["scroll"], "auto-scroll resumes from the dragged spot"
    assert resumed["scroll"] < held["scroll"] + 20, "and by one normal step, not a jump for the held frames"


def test_a_touch_pointer_keeps_the_native_scroll(tmp_path):
    [read] = _run(tmp_path, {"scroll": 900, "steps": [
        {"down": {"x": 500, "type": "touch"}}, {"move": 620, "read": True},
    ]})
    assert (read["scroll"], read["dragging"], read["grabbing"]) == (900, False, False)


def test_a_secondary_button_press_is_not_a_drag(tmp_path):
    [read] = _run(tmp_path, {"scroll": 900, "steps": [
        {"down": {"x": 500, "button": 2}}, {"move": 620, "read": True},
    ]})
    assert (read["scroll"], read["dragging"]) == (900, False)


def test_a_cancelled_pointer_ends_the_drag(tmp_path):
    [read] = _run(tmp_path, {"scroll": 900, "steps": [
        {"down": {"x": 500}}, {"move": 620}, {"cancel": True}, {"read": True},
    ]})
    assert (read["dragging"], read["grabbing"], read["captured"]) == (False, False, None)


def test_the_posters_own_image_drag_is_prevented(tmp_path):
    [read] = _run(tmp_path, {"scroll": 900, "steps": [{"dragstart": True, "read": True}]})
    assert "dragstart" in read["prevented"], "a poster's native image drag would steal the press mid-gesture"


def _rebuild(tmp_path, dragging: bool) -> dict:
    src = slice_to_next(APP_JS, "async function loadRecentlyAdded()", "\n  function ", "\n  async function ")
    tmp_path.mkdir(parents=True, exist_ok=True)
    (tmp_path / "src.js").write_text(
        src + "\nfunction _shortDate() { return ''; }\nfunction _recentTypeIcon() { return ''; }\n"
        "function _loadCarouselPosters() {}\nfunction _setupCarouselAutoScroll() {}\n")
    (tmp_path / "plan.json").write_text(json.dumps({"dragging": dragging}))
    (tmp_path / "driver.js").write_text(REBUILD_DRIVER)
    r = subprocess.run([_NODE, str(tmp_path / "driver.js"), str(tmp_path / "src.js"), str(tmp_path / "plan.json")],
                       capture_output=True, text=True, timeout=60)
    assert r.returncode == 0, r.stderr[-2000:]
    return json.loads(r.stdout)


def test_the_poll_does_not_rebuild_the_strip_under_a_drag(tmp_path):
    held = _rebuild(tmp_path / "held", dragging=True)
    assert held["created"] == [] and held["text"] == "old" and held["lastHash"] is None, held
    assert held["display"] == "", "the block still shows — only the rebuild waits"
    free = _rebuild(tmp_path / "free", dragging=False)
    assert free["created"] and free["lastHash"], "premise: with no drag the same poll rebuilds the strip"


def test_the_cursor_says_the_strip_is_draggable():
    # the grab cursor belongs to the ONE .recent-strip rule (a second top-level rule trips the v1.15.116 dup lint),
    # and the dragging rules sit after .recent-card so the v0.51.285 tile-width guard still anchors on the card
    strip_rule = APP_CSS[APP_CSS.index(".recent-strip {"):]
    strip_rule = strip_rule[:strip_rule.index("}")]
    assert "cursor: grab;" in strip_rule, "the strip must look draggable before anyone presses it"
    grabbing = APP_CSS[APP_CSS.index(".recent-strip-dragging,"):]
    grabbing = grabbing[:grabbing.index("}", grabbing.index("cursor: grabbing")) + 1]
    assert ".recent-card" in grabbing, "while dragging, grabbing must win over the cards' own pointer cursor"
    assert APP_CSS.index(".recent-strip-dragging,") > APP_CSS.index(".recent-card {"), \
        "the dragging rules must follow .recent-card, or a selector-anchored guard reads them as the card's rule"
    none_rule = APP_CSS[APP_CSS.index(".recent-strip-dragging {"):]
    assert "user-select: none" in none_rule[:none_rule.index("}")], \
        "a drag must not select the titles it passes over"
