"""v0.51.344 (integration review R1-F11): PB-024's worded START 500 reaches the operator on the AnimeThemes, loudness-audit and orphans pages — motif's words, never its JSON or a bare statusText."""
from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import app.web.api as api_mod
from _slice_helpers import slice_between
from app.core import animethemes as at
from test_v0_51_158_loudness_audit import _make_app as _loudness_app
from test_v0_51_314_animethemes_resolver import BRIDGE_JSON, FakeAPI
from test_v0_51_325_animethemes_sweep import _client as _at_client
from test_v0_51_325_animethemes_sweep import _reset_state as _reset_at_sweep
from test_v0_51_325_animethemes_sweep import app_env  # noqa: F401 — a fixture
from test_v0_51_339_canonical_health_restore import _NODE, APP_JS, _app_fn
from test_v0_51_344_job_start_failure import AUTH, _refuse_start, _refused_with_words
from test_v1_21_24_orphan_scan_background_op import _make_app as _orphan_app

REPO = Path(__file__).resolve().parent.parent
ORPHANS_HTML = (REPO / "app" / "web" / "templates" / "orphans.html").read_text()
pytestmark = pytest.mark.skipif(not _NODE, reason="node not installed")

# One fetch answers the START; the page's elements record what they showed.
_DRIVER = r"""
"use strict";
const fs = require("node:fs");
const vm = require("node:vm");
const [, , srcPath, scenarioPath] = process.argv;
const sc = JSON.parse(fs.readFileSync(scenarioPath, "utf8"));
const els = new Map();
const byId = (id) => {
  if (!els.has(id)) els.set(id, { id, style: { display: "" }, textContent: "", innerHTML: "", className: "", disabled: false,
                                  listeners: {}, addEventListener(t, f) { this.listeners[t] = f; } });
  return els.get(id);
};
const calls = [];
const ctx = vm.createContext({
  document: { getElementById: byId }, console, setTimeout: () => 1, clearTimeout: () => {},
  fetch: async (path, opts) => {
    calls.push(`${(opts || {}).method || "GET"} ${path}`);
    return { ok: false, status: sc.status, statusText: sc.statusText, text: async () => sc.body, json: async () => JSON.parse(sc.body) };
  },
});
vm.runInContext(fs.readFileSync(srcPath, "utf8"), ctx);
(async () => {
  await ctx.__click();
  const out = {};
  for (const [id, e] of els) out[id] = { text: e.textContent, html: e.innerHTML, className: e.className, disabled: e.disabled };
  process.stdout.write(JSON.stringify({ els: out, calls, polled: !!ctx.__polled }));
})().catch((e) => { console.error(e); process.exit(1); });
"""


def _api_fn() -> str:
    start = APP_JS.index("\n  async function api(") + 1
    return APP_JS[start:APP_JS.index("\n  }\n", start)] + "\n  }\n"


def _helpers() -> str:
    return _app_fn("proxyStatusHint") + _app_fn("gatewayTimeoutNote") + _app_fn("failWords") + _api_fn() + "var __polled = false;\n"


def _anime_src() -> str:
    handler = slice_between(APP_JS, "    runBtn.addEventListener('click', async () => {", "    cancelBtn.addEventListener('click', async () => {")
    return (_helpers() + "const runBtn = document.getElementById('at-sweep-btn');\n"
            "const runStatus = document.getElementById('at-sweep-status');\nlet pollTimer = null;\nfunction poll() { __polled = true; }\n"
            + handler + "\nfunction __click() { return runBtn.listeners.click(); }\n")


def _loudness_src() -> str:
    busy = slice_between(APP_JS, "    function setBusy(busy) {", "    function renderSummary(s) {")
    handler = slice_between(APP_JS, "    btn.addEventListener('click', async () => {\n      summary.style.display = 'none';",
                            "    // On-load restore: reflect a run already in flight")
    return (_helpers() + "const btn = document.getElementById('loudness-audit-btn');\n"
            "const status = document.getElementById('loudness-audit-status');\n"
            "const summary = document.getElementById('loudness-audit-summary');\nfunction poll() { __polled = true; }\n"
            + busy + handler + "\nfunction __click() { return btn.listeners.click(); }\n")


def _orphans_src() -> str:
    run_scan = slice_between(ORPHANS_HTML, "    async function runScan() {", "    runBtn.addEventListener('click', runScan);")
    return ("var __polled = false;\nconst runBtn = document.getElementById('orphan-run');\n"
            "const statusEl = document.getElementById('orphan-status');\nconst summaryEl = document.getElementById('orphan-summary');\n"
            "const htmlEscape = (s) => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/\"/g, '&quot;');\n"
            "function pollStatus() { __polled = true; }\n" + run_scan + "\nfunction __click() { return runScan(); }\n")


def _drive(tmp_path, src: str, answer: dict) -> dict:
    tmp_path.mkdir(parents=True, exist_ok=True)
    (tmp_path / "page.js").write_text(src)
    (tmp_path / "scenario.json").write_text(json.dumps(answer))
    (tmp_path / "driver.js").write_text(_DRIVER)
    r = subprocess.run([_NODE, str(tmp_path / "driver.js"), str(tmp_path / "page.js"), str(tmp_path / "scenario.json")],
                       capture_output=True, text=True, timeout=60)
    assert r.returncode == 0, r.stderr[-2000:]
    return json.loads(r.stdout)


def _answer(response, status_text="Internal Server Error") -> dict:
    return {"status": response.status_code, "statusText": status_text, "body": response.text}


def _anime_start_500(app_env, monkeypatch):
    client, _settings = app_env
    _reset_at_sweep()
    monkeypatch.setattr(api_mod, "log_event", lambda _db, **k: None)
    monkeypatch.setattr(at, "load_bridge", lambda cache_dir, **kw: at.Bridge.from_json(BRIDGE_JSON))
    fake_client = _at_client(FakeAPI())  # built BEFORE the class is patched
    monkeypatch.setattr(at, "AnimeThemesClient", lambda *a, **kw: fake_client)
    loose = TestClient(client.app, raise_server_exceptions=False)
    allow = _refuse_start(monkeypatch, "animethemes-sweep")
    try:
        r = loose.post("/api/admin/animethemes-sweep/start", headers=AUTH)
    finally:
        allow()
        _reset_at_sweep()
    _refused_with_words(r)
    return r


def _loudness_start_500(tmp_path, monkeypatch):
    from app.core import loudness_audit
    monkeypatch.setattr(loudness_audit, "run_loudness_audit", lambda db_path, **k: {})
    loose = TestClient(_loudness_app(tmp_path, monkeypatch).app, raise_server_exceptions=False)
    with api_mod._LOUDNESS_AUDIT_LOCK:
        api_mod._LOUDNESS_AUDIT_STATE.clear()
        api_mod._LOUDNESS_AUDIT_STATE["status"] = "idle"
    allow = _refuse_start(monkeypatch, "loudness-audit")
    try:
        r = loose.post("/api/admin/loudness-audit/start", headers=AUTH)
    finally:
        allow()
        with api_mod._LOUDNESS_AUDIT_LOCK:
            api_mod._LOUDNESS_AUDIT_STATE.clear()
            api_mod._LOUDNESS_AUDIT_STATE["status"] = "idle"
    _refused_with_words(r)
    return r


def _orphan_start_500(tmp_path, monkeypatch):
    import app.core.orphan_scan as orphan_scan
    monkeypatch.setattr(orphan_scan, "scan_plex_upload_placements", lambda db_path, plex, **k: [])
    client, _settings = _orphan_app(tmp_path, monkeypatch)
    loose = TestClient(client.app, raise_server_exceptions=False)
    with api_mod._ORPHAN_SCAN_LOCK:
        api_mod._ORPHAN_SCAN_STATE.clear()
        api_mod._ORPHAN_SCAN_STATE["status"] = "idle"
    allow = _refuse_start(monkeypatch, "orphan-scan")
    try:
        r = loose.post("/api/admin/orphan-scan/start", headers=AUTH)
    finally:
        allow()
        with api_mod._ORPHAN_SCAN_LOCK:
            api_mod._ORPHAN_SCAN_STATE.clear()
            api_mod._ORPHAN_SCAN_STATE["status"] = "idle"
    _refused_with_words(r)
    return r


def _is_the_words(shown: str, detail: str):
    assert "nothing ran" in detail, detail
    assert shown == "✗ " + detail, (shown, detail)
    assert "{" not in shown and "500" not in shown and "Internal Server Error" not in shown, shown


def test_the_anime_themes_page_words_a_start_whose_thread_could_not_start(app_env, monkeypatch, tmp_path):
    r = _anime_start_500(app_env, monkeypatch)
    out = _drive(tmp_path / "page", _anime_src(), _answer(r))
    status = out["els"]["at-sweep-status"]
    _is_the_words(status["text"], r.json()["detail"])
    assert status["className"] == "form-status form-status-fail"
    assert out["els"]["at-sweep-btn"]["disabled"] is False and not out["polled"], out


def test_the_loudness_page_words_a_start_whose_thread_could_not_start(tmp_path, monkeypatch):
    r = _loudness_start_500(tmp_path / "srv", monkeypatch)
    out = _drive(tmp_path / "page", _loudness_src(), _answer(r))
    status = out["els"]["loudness-audit-status"]
    _is_the_words(status["text"], r.json()["detail"])
    assert status["className"] == "form-status form-status-fail"
    assert out["els"]["loudness-audit-btn"]["disabled"] is False and not out["polled"], out


@pytest.mark.parametrize("status_text", ["Internal Server Error", ""])  # HTTP/2 carries no reason phrase
def test_the_orphans_page_words_a_start_whose_thread_could_not_start(tmp_path, monkeypatch, status_text):
    r = _orphan_start_500(tmp_path / "srv", monkeypatch)
    detail = r.json()["detail"]
    out = _drive(tmp_path / "page", _orphans_src(), _answer(r, status_text))
    status, summary = out["els"]["orphan-status"], out["els"]["orphan-summary"]
    assert status["text"] == f"error: {detail}", (status, detail)
    assert "nothing ran" in status["text"] and "{" not in status["text"] and "500" not in status["text"], status
    assert detail in summary["html"] and "scan failed to start" in summary["html"], summary
    assert out["els"]["orphan-run"]["disabled"] is False and not out["polled"], out


def test_the_orphans_page_still_names_the_status_when_a_proxy_answers_the_start(tmp_path):
    out = _drive(tmp_path / "page", _orphans_src(), {"status": 502, "statusText": "Bad Gateway", "body": "<html>proxy page</html>"})
    assert out["els"]["orphan-status"]["text"] == "error: 502: Bad Gateway", out["els"]["orphan-status"]
    assert out["els"]["orphan-run"]["disabled"] is False and not out["polled"]
