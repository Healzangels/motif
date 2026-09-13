"""v0.51.340 — CLEAR ALL left the topbar INBOX pill reading "1".

The operator: after clearing the inbox the pill kept its unread count "for a
while". The count is painted by the /api/stats poll (notifications_unread =
rows not dismissed and not seen). Every other drawer mutation keeps it honest
between polls — a local badge write, then `setTimeout(refreshTopbarStatus, …)`
landing past the 1s stats cache — but clearAll only POSTed and rendered the
empty list, so the stale count lived until the idle topbar poll.

refreshTopbarStatus hash-skips a byte-identical payload BEFORE the inbox block,
which cuts both ways for a locally zeroed badge, and the node test runs the
REAL poll function to hold both halves:

  * POST ok: a poll that re-reads the pre-mutation stats cache (identical
    payload) must NOT relight the zero — the hash-skip is what holds it.
  * POST failed: the server count never moved, so the scheduled re-read's
    identical payload MUST repaint it — the hash has to drop on that path.

The cross-check derives the mutator set from the drawer's own code (a POST to
/api/notifications, or a call to a badge writer) so the next mutation added
can't forget the re-read either.
"""
from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

from _slice_helpers import slice_between

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()
_NODE = shutil.which("node")

if os.environ.get("MOTIF_REQUIRE_NODE") and not _NODE:
    raise RuntimeError("MOTIF_REQUIRE_NODE=1 but node is not on PATH — the inbox clear-all harness would silently not run")

_FN_HEAD = re.compile(r"\n    (?:async )?function (\w+)\(")
_REFRESH = re.compile(r"setTimeout\(refreshTopbarStatus,\s*(\d+)\)")
_NAMED = ("dismiss", "dismissGroup", "markRead", "markAllRead", "clearAll")


def _drawer_fns(strip_comments: bool = True) -> dict[str, str]:
    """name -> source of every function declared directly in bindNotifInbox,
    each cut at its own 4-space closing brace."""
    scope = slice_between(APP_JS, "function bindNotifInbox(", "\n  }\n")
    out = {}
    for m in _FN_HEAD.finditer(scope):
        body = scope[m.start():scope.index("\n    }", m.end()) + len("\n    }")]
        if strip_comments:
            body = "\n".join(ln for ln in body.split("\n") if not ln.lstrip().startswith("//"))
        out[m.group(1)] = body
    return out


def _reach(name: str, fns: dict[str, str]) -> list[str]:
    """`name` plus every drawer-local function it calls, transitively."""
    seen, stack = [], [name]
    while stack:
        n = stack.pop()
        if n in seen:
            continue
        seen.append(n)
        stack.extend(c for c in re.findall(r"\b(\w+)\(", fns[n]) if c in fns and c not in seen)
    return seen


def _stats_ttl_ms() -> float:
    m = re.search(r"_stats_cache_ttl\s*=\s*([\d.]+)", API_PY)
    assert m, "the /api/stats cache TTL moved — re-anchor the re-poll delay check"
    return float(m.group(1)) * 1000


def _zeroes_the_count(body: str) -> bool:
    m = re.search(r"(?:const|let)\s+(\w+)\s*=\s*document\.getElementById\('topbar-inbox-count'\)", body)
    return bool(m) and bool(re.search(rf"\b{m.group(1)}\.hidden\s*=\s*true\b", body))


def _drops_has_unread_unconditionally(body: str) -> bool:
    return any("pill.classList.remove('has-unread')" in ln and not ln.lstrip().startswith("if")
               for ln in body.split("\n"))


# ── the fix, as source ───────────────────────────────────────


@pytest.mark.parametrize("action", ["clearAll", "markAllRead"])
def test_bulk_actions_zero_the_count_and_drop_has_unread(action):
    fns = _drawer_fns()
    resetters = [n for n in _reach(action, fns) if _zeroes_the_count(fns[n])]
    assert resetters, (
        f"v0.51.340: {action} must hide #topbar-inbox-count (hidden reads as zero) — "
        "CLEAR ALL left the pill reading '1' until the idle poll")
    assert any(_drops_has_unread_unconditionally(fns[n]) for n in resetters), (
        f"{action}: the pill stays lit without dropping .has-unread")


def test_clear_all_rereads_the_server_after_the_post_past_the_stats_ttl():
    body = _drawer_fns()["clearAll"]
    post = re.search(r"await api\('POST',\s*'/api/notifications/dismiss-all'\)", body)
    assert post, "clearAll must await its dismiss-all POST"
    refresh = _REFRESH.search(body)
    assert refresh, "v0.51.340: clearAll must schedule refreshTopbarStatus like every other drawer mutation"
    assert refresh.start() > post.end(), "schedule the re-read AFTER the POST settles, not before it"
    assert int(refresh.group(1)) > _stats_ttl_ms(), "the re-read must land past the /api/stats cache TTL"


# ── the cross-check: the next mutation can't forget it ───────


def test_every_drawer_mutation_schedules_the_topbar_reread():
    fns = _drawer_fns()
    writers = {n for n, b in fns.items() if "getElementById('topbar-inbox-count')" in b}
    assert writers, "no badge writer found in bindNotifInbox — re-anchor the derivation"

    def effects(body):
        spots = [m.start() for m in re.finditer(r"/api/notifications", body)] if "'POST'" in body else []
        for w in writers:
            spots += [m.start() for m in re.finditer(rf"\b{w}\(", body)]
        return spots

    mutators = {n for n, b in fns.items() if n not in writers and effects(b)}
    assert set(_NAMED) <= mutators, (
        f"the derivation lost a known mutator: {sorted(set(_NAMED) - mutators)}")
    for name in sorted(mutators):
        body = fns[name]
        refreshes = list(_REFRESH.finditer(body))
        assert refreshes, (
            f"{name} changes the unread count (POST or badge write) but never schedules "
            "refreshTopbarStatus — the stale pill survives until the idle poll")
        assert refreshes[-1].start() > max(effects(body)), (
            f"{name}: the re-read must be scheduled after its last mutation")
        assert int(refreshes[-1].group(1)) > _stats_ttl_ms(), (
            f"{name}: the re-read must land past the /api/stats cache TTL")


# ── the behaviour, under node, through the REAL topbar poll ──

# The poll source runs in a `with` scope: named stubs for what the inbox path
# touches, an inert chainable proxy for every other page global it reads.
_HARNESS = r"""
const { poll: pollSrc, fns: fnSrc } = JSON.parse(require('fs').readFileSync(0, 'utf8'));
const REAL = new Set(Object.getOwnPropertyNames(globalThis));
const INERT = new Proxy(function () {}, {
  get: (t, k) => (k === Symbol.toPrimitive ? () => 0 : k === Symbol.iterator ? function* () {}
    : k === 'then' ? undefined : k === 'length' ? 0 : INERT),
  apply: () => INERT, construct: () => INERT,
});
const N = 3;
const stats = (unread) => ({ queue: {}, notifications_unread: unread });

async function scenario(action, postFails) {
  const count = { textContent: '', hidden: true };
  const classes = new Set(['op-pill', 'op-notif']);
  const pill = { classList: { add: (c) => classes.add(c), remove: (c) => classes.delete(c),
                              contains: (c) => classes.has(c), toggle: () => {} } };
  let server = stats(N), settled = false, emptied = false;
  const posts = [], errors = [], timers = [];
  const stubs = {
    $: (sel) => (sel === '#topbar-inbox-badge' ? pill : sel === '#topbar-inbox-count' ? count : null),
    document: { getElementById: (id) => (id === 'topbar-inbox-count' ? count : id === 'topbar-inbox-badge' ? pill : null),
                querySelector: () => null, querySelectorAll: () => [] },
    window: {}, localStorage: { setItem() {}, getItem: () => null },
    pill, listEl: { querySelectorAll: () => [], querySelector: () => null }, readAllBtn: { hidden: false },
    renderEmpty: () => { emptied = true; },
    api: (method, url) => {
      if (method === 'GET') return Promise.resolve(JSON.parse(JSON.stringify(url === '/api/stats' ? server : {})));
      posts.push([method, url]);
      return new Promise((res, rej) => setImmediate(() => {
        settled = true;
        if (postFails) rej(new Error('500')); else res({});
      }));
    },
    setTimeout: (fn, ms) => { timers.push({ fn, ms, afterPost: settled }); return 0; },
    console: { error: (...a) => errors.push(a.map(String).join(' ')), log() {}, warn() {} },
  };
  const scope = new Proxy(stubs, {
    has: (o, k) => k in o || !REAL.has(k),
    get: (o, k) => (k === Symbol.unscopables ? undefined : k in o ? o[k] : INERT),
  });
  const live = new Function('scope', 'with (scope) { return (function () {\n' + pollSrc + '\n' + fnSrc
    + '\nreturn { refreshTopbarStatus, clearAll, markAllRead, bumpUnreadBadge };\n})(); }')(scope);
  const snap = () => ({ shown: count.hidden ? null : String(count.textContent), lit: classes.has('has-unread') });
  const r = { errors, posts };
  await live.refreshTopbarStatus();
  r.painted = snap();
  await live[action]();
  r.afterAction = snap();
  const rereads = timers.filter((t) => t.fn === live.refreshTopbarStatus);
  r.timers = timers.map((t) => ({ refresh: t.fn === live.refreshTopbarStatus, ms: t.ms, afterPost: t.afterPost }));
  if (!postFails) {
    // a poll inside the 1s stats cache: the same pre-mutation payload again
    server = stats(N);
    await live.refreshTopbarStatus();
    r.stalePoll = snap();
    server = stats(0);
  }
  for (const t of rereads) await t.fn();
  r.reread = snap();
  r.emptied = emptied;
  live.bumpUnreadBadge(-1);
  r.bumpAfter = { hidden: count.hidden, text: String(count.textContent) };
  return r;
}

(async () => {
  const out = {};
  for (const action of ['clearAll', 'markAllRead']) {
    for (const postFails of [false, true]) out[`${action}:${postFails ? 'fail' : 'ok'}`] = await scenario(action, postFails);
  }
  process.stdout.write(JSON.stringify(out));
})().catch((e) => { console.error((e && e.stack) || String(e)); process.exit(1); });
"""

_DIM = {"shown": None, "lit": False}


def _node_runs() -> dict:
    raw = _drawer_fns(strip_comments=False)
    names = set(_reach("clearAll", raw)) | set(_reach("markAllRead", raw)) | {"bumpUnreadBadge"}
    names.discard("renderEmpty")  # stubbed: it paints the drawer list, not the pill
    poll = slice_between(APP_JS, "  async function refreshTopbarStatus() {", "\n  }\n") + "\n  }\n"
    payload = {"poll": poll, "fns": "\n".join(raw[n] for n in sorted(names))}
    r = subprocess.run([_NODE, "-e", _HARNESS], input=json.dumps(payload),
                       capture_output=True, text=True, timeout=60, cwd=REPO)
    assert r.returncode == 0, r.stderr[-1500:]
    runs = json.loads(r.stdout)
    assert set(runs) == {"clearAll:ok", "clearAll:fail", "markAllRead:ok", "markAllRead:fail"}
    for key, run in runs.items():
        # the poll's catch clears the painted hash, so a throw would void every result below
        assert not run["errors"], f"{key}: the real refreshTopbarStatus threw under the stub scope: {run['errors']}"
        assert run["painted"] == {"shown": "3", "lit": True}, (
            f"{key}: the real poll never painted the inbox count — the harness is not exercising it: {run['painted']}")
        assert run["posts"] and all(m == "POST" for m, _ in run["posts"]), key
    return runs


@pytest.mark.skipif(not _NODE, reason="node not installed")
def test_bulk_actions_dim_the_pill_and_schedule_the_reread_under_node():
    ttl = _stats_ttl_ms()
    for key, run in _node_runs().items():
        assert run["afterAction"] == _DIM, f"{key}: the count must hide and the pill dim — the operator's lingering '1'"
        refreshes = [t for t in run["timers"] if t["refresh"]]
        assert refreshes, f"{key}: no refreshTopbarStatus scheduled — the stale count waits for the idle poll"
        assert all(t["afterPost"] and t["ms"] > ttl for t in refreshes), (
            f"{key}: the re-read must be scheduled after the POST settles and past the stats TTL: {refreshes}")
        if key.endswith(":ok"):
            assert run["bumpAfter"] == {"hidden": True, "text": "0"}, (
                f"{key}: a later -1 on a cleared badge must stay zero (hidden reads as zero)")
        if key.startswith("clearAll"):
            assert run["emptied"], f"{key}: clearAll must render the empty state"


@pytest.mark.skipif(not _NODE, reason="node not installed")
@pytest.mark.parametrize("action", ["clearAll", "markAllRead"])
def test_a_stale_cached_poll_does_not_relight_the_zeroed_pill_under_node(action):
    run = _node_runs()[f"{action}:ok"]
    assert run["stalePoll"] == _DIM, (
        f"{action}: a poll that re-reads the pre-mutation /api/stats cache (identical payload) "
        f"relit the pill the user just cleared: {run['stalePoll']}")
    assert run["reread"] == _DIM, f"{action}: the re-read past the TTL carries the server's zero: {run['reread']}"


@pytest.mark.skipif(not _NODE, reason="node not installed")
@pytest.mark.parametrize("action", ["clearAll", "markAllRead"])
def test_a_failed_post_is_corrected_by_the_reread_under_node(action):
    run = _node_runs()[f"{action}:fail"]
    assert run["reread"] == {"shown": "3", "lit": True}, (
        f"{action}: the POST failed, so the server count never moved — the scheduled re-read's "
        f"identical payload must repaint it, not hash-skip past the local zero: {run['reread']}")
