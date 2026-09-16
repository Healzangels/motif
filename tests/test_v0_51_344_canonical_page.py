"""v0.51.344: the CANONICAL HEALTH page's restore line and the library row's RESTORE FROM PLEX, under node.

  PB-031  an idle answer to a watched run says it stopped without a result; the progress / lost-contact line never outlives it.
  PB-032  a CHECK or REPAIR refused by a running restore attaches the page to that run — one poll chain.
  PB-063  the row's RESTORE FROM PLEX alert words its skips and failures with the page's table, never codes or JSON.
"""
from __future__ import annotations

import json
import subprocess

import pytest

from _slice_helpers import slice_between
from test_v0_51_339_canonical_health_restore import _NODE, _SKIP_WORDING, APP_JS, _app_fn, _report, _row
from test_v0_51_342_canonical_round3 import (
    _BTN, _CHECK_BTN, _PAGE, _PROGRESS, _REPAIR_BTN, _RUNNING, _STATUS, _WORDS, _done, _item, _page, _seed, _unlocked,
)
from test_v0_51_342_restore_from_plex_job import (  # noqa: F401 — env is the job endpoints' fixture
    AUTH, START, HeldRestore, _ago, _finish, env, ssr_running,
)
from app.core import canonical_health as ch

pytestmark = pytest.mark.skipif(not _NODE, reason="node not installed")

_IDLE_ALARM = ("✗ the run stopped without a result — motif restarted before it could record one; "
               "RUN CHECK, then RESTORE FROM PLEX restores what is left")
_CHECK_409 = "RESTORE FROM PLEX is running — run the check when it finishes"
_REPAIR_409 = "RESTORE FROM PLEX is running — repair when it finishes"


# ── PB-031: an idle answer ───────────────────────────────────────────

def test_a_watched_run_answered_by_idle_says_it_stopped_without_a_result(tmp_path, ssr_running):
    running = dict(_RUNNING, started_at=_ago(hours=2))
    s0, s1 = _page(tmp_path, [_PAGE, running, {"status": "idle"}, _PAGE], ["tick"], ssr=ssr_running)
    assert s0[_STATUS]["text"] == _PROGRESS, "premise: the page was watching the run"
    assert (s1[_STATUS]["text"], s1[_STATUS]["className"]) == (_IDLE_ALARM, "form-status form-status-fail"), \
        "the run's progress line outlived the run"
    assert _unlocked(s1) and s1["__timers"] == 0
    assert s1["canon-missing-block"]["display"] == ""


def test_an_idle_answer_after_lost_contact_drops_the_lost_contact_note(tmp_path, ssr_running):
    blip = {"__throw": {"status": 502}}
    running = dict(_RUNNING, started_at=_ago(hours=2))
    snaps = _page(tmp_path, [_PAGE, running, blip, blip, blip, {"status": "idle"}, _PAGE], ["tick"] * 4,
                  ssr=ssr_running)
    assert "— lost contact since " in snaps[3][_STATUS]["text"], "premise: the page said it lost contact"
    assert snaps[4][_STATUS]["text"] == _IDLE_ALARM, "the lost-contact note outlived the answer that restored contact"


def test_the_check_the_idle_alarm_asks_for_quiets_it(tmp_path, ssr_running):
    missing = [_row(2003, "Idle Title", None)]
    page = _report(missing=missing, restorable=0)
    checked = {**page, "check": {"checked": 5, "missing": 1, "skipped": 0},
               "checked": {"tracked": 5, "never": 0, "oldest": _ago(seconds=1), "newest": _ago(seconds=1)}}
    running = dict(_RUNNING, started_at=_ago(hours=2))
    _s0, s1, s2 = _page(tmp_path, [page, running, {"status": "idle"}, page, checked], ["tick", _CHECK_BTN],
                        ssr=ssr_running)
    assert s1[_STATUS]["text"] == _IDLE_ALARM
    assert s2["canon-check-status"]["text"] == "✓ check complete"
    assert (s2[_STATUS]["text"], s2[_STATUS]["className"]) == (
        "last run started 2h ago: cut off by a motif restart", "form-status"), "the alarm still asks for RUN CHECK"


def test_a_refused_check_whose_run_left_no_record_shows_the_alarm_on_a_page_with_nothing_broken(tmp_path):
    empty = _report()
    s0, s1, s2 = _page(tmp_path, [empty, {"status": "idle"}, {"__throw": {"status": 409, "detail": _CHECK_409}},
                                  {"status": "idle"}, {"__hold": {"key": "load", "value": empty}}],
                       [_CHECK_BTN, "release:load"])
    assert s0["canon-missing-block"]["display"] == "none", "premise: nothing broken, so the restore block was hidden"
    assert (s1[_STATUS]["text"], s1["canon-missing-block"]["display"]) == (_IDLE_ALARM, ""), \
        "the alarm was written into a hidden block"
    assert (s2[_STATUS]["text"], s2["canon-missing-block"]["display"]) == (_IDLE_ALARM, ""), \
        "the report that followed the alarm hid it"
    assert _unlocked(s2) and s2["__timers"] == 0


def test_an_idle_answer_on_a_page_that_watched_nothing_paints_no_alarm(tmp_path):
    [s0] = _page(tmp_path, [_PAGE, {"status": "idle"}], [])
    assert s0[_STATUS]["text"] == "" and not any("✗" in t for t in s0[_STATUS]["texts"]), s0[_STATUS]


# ── PB-032: a CHECK / REPAIR refused by a running restore ────────────

def test_a_check_refused_while_the_load_poll_is_in_flight_leaves_one_poll_chain(tmp_path):
    held = {"__hold": {"key": "load", "value": dict(_RUNNING)}}
    _s0, s1, s2 = _page(tmp_path, [_PAGE, held, {"__throw": {"status": 409, "detail": _CHECK_409}}, _RUNNING],
                        [_CHECK_BTN, "release:load"])
    assert (s1[_BTN]["text"], s1["__timers"]) == ("// RESTORING…", 1), "the refused check did not attach to the run"
    assert s2["__timers"] == 1, "the load's poll, answered after the 409 attached, began a second poll chain"
    assert s2[_STATUS]["text"] == _PROGRESS


def test_a_refused_repair_whose_run_has_finished_shows_that_runs_result(tmp_path):
    started = _ago(minutes=1)
    _s0, s1 = _page(tmp_path, [_PAGE, {"status": "idle"}, {"__throw": {"status": 409, "detail": _REPAIR_409}},
                               _done(started, _ago(seconds=1)), _report()], [_REPAIR_BTN])
    assert s1["canon-repair-status"]["text"] == "✗ " + _REPAIR_409
    assert (s1[_STATUS]["text"], s1[_STATUS]["className"]) == (_WORDS, "form-status form-status-ok"), \
        "the run the repair was refused by read as someone else's last run"
    assert _unlocked(s1) and s1["__timers"] == 0


# ── PB-063: the library row's RESTORE FROM PLEX ──────────────────────

_ROW_DRIVER = r"""
"use strict";
const fs = require("node:fs");
const vm = require("node:vm");
const [, , srcPath, scenarioPath] = process.argv;
const answer = JSON.parse(fs.readFileSync(scenarioPath, "utf8"));
const alerts = [];
const calls = [];
const ctx = vm.createContext({
  console,
  fetch: async (path, opts) => {
    calls.push(`${opts.method} ${path}`);
    if (answer.reject) throw new TypeError("Failed to fetch");
    return { ok: answer.status >= 200 && answer.status < 300, status: answer.status, statusText: "",
             text: async () => answer.body, json: async () => JSON.parse(answer.body) };
  },
  confirm: () => true, alert: (m) => { alerts.push(String(m)); }, loadLibrary: async () => {},
});
vm.runInContext(fs.readFileSync(srcPath, "utf8"), ctx);
(async () => {
  await ctx.runRow({ dataset: { mt: "movie", id: "42", title: "Row Title" } });
  process.stdout.write(JSON.stringify({ alerts, calls }));
})().catch((e) => { console.error(e); process.exit(1); });
"""


def _row_alerts(tmp_path, answer) -> list[str]:
    """The library row menu's live restore-canonical branch, with the real api() over a fetch that gives `answer`."""
    api_at = APP_JS.index("\n  async function api(") + 1
    api_fn = APP_JS[api_at:APP_JS.index("\n  }\n", api_at)] + "\n  }\n"
    branch = slice_between(APP_JS, "} else if (act === 'restore-canonical') {", "} else if (act === 'purge') {")
    tmp_path.mkdir(parents=True, exist_ok=True)
    (tmp_path / "row.js").write_text(
        _app_fn("proxyStatusHint") + _app_fn("gatewayTimeoutNote") + _app_fn("restoreSkipWord") + _app_fn("failWords")
        + api_fn + "async function runRow(btn) {\n  const act = 'restore-canonical';\n  if (act === '') {\n"
        + branch + "}\n}\n")
    (tmp_path / "answer.json").write_text(json.dumps(answer))
    (tmp_path / "driver.js").write_text(_ROW_DRIVER)
    r = subprocess.run([_NODE, str(tmp_path / "driver.js"), str(tmp_path / "row.js"), str(tmp_path / "answer.json")],
                       capture_output=True, text=True, timeout=60)
    assert r.returncode == 0, r.stderr[-2000:]
    out = json.loads(r.stdout)
    assert out["calls"] == ["POST /api/items/movie/42/restore-canonical"], out
    return out["alerts"]


def test_the_row_restore_alert_words_every_skip_reason(tmp_path):
    skipped, expected = [], {}
    # a distinct count per reason, so a reason worded as another (or as the fallback) changes a group's total
    for n, (reason, words) in enumerate(_SKIP_WORDING.items(), start=1):
        sample = f"{reason}[Errno 5] detail" if reason.endswith(":") else reason
        skipped += [{"section_id": str(i + 1), "reason": sample} for i in range(n)]
        expected[words] = expected.get(words, 0) + n
    [alert] = _row_alerts(tmp_path, {"status": 200, "body": json.dumps({"ok": True, "restored": 0, "skipped": skipped})})
    head = f"Restored 0; skipped {len(skipped)} ("
    assert alert.startswith(head) and alert.endswith(")"), alert
    shown = {g.split(" ", 1)[1]: int(g.split(" ", 1)[0]) for g in alert[len(head):-1].split(", ")}
    assert shown == expected, (shown, alert)
    assert [k for k in _SKIP_WORDING if k.rstrip(":") in alert] == [], "a reason code reached the alert"


def test_the_row_restore_refused_by_a_running_restore_alerts_motifs_words(env, monkeypatch, tmp_path):
    client, settings, env_dir, events = env
    _seed(settings.db_path, env_dir / "plex", 1807)
    held = HeldRestore()
    monkeypatch.setattr(ch, "restore_from_plex", held)
    assert client.post(START, headers=AUTH).json()["started"] is True
    assert held.entered.wait(10)
    refused = client.post(_item(1807), headers=AUTH)
    held.release.set()
    _finish(client)
    assert refused.status_code == 409, "premise: the item restore is refused while the run goes"
    alerts = _row_alerts(tmp_path / "row", {"status": 409, "body": refused.text})
    assert alerts == ["Restore failed: " + refused.json()["detail"]], "FastAPI's JSON reached the alert"


@pytest.mark.parametrize("answer, words", [
    ({"status": 504, "body": "<html><body>504 Gateway Time-out</body></html>"},
     "Restore failed: 504: the reverse proxy timed out, but motif may still be finishing — verify before retrying."),
    ({"reject": True}, "Restore failed: could not reach motif — a reverse proxy / WAF may have returned a non-motif page"),
], ids=["gateway-timeout-page", "network-dropped"])
def test_the_row_restore_failure_without_motifs_json_says_what_answered(tmp_path, answer, words):
    [alert] = _row_alerts(tmp_path, answer)
    assert alert.startswith(words) and "<html" not in alert, alert
