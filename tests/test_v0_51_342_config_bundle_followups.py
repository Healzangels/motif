"""v0.51.342: follow-ups to the config-bundle integration review.

  1. An integer no float can hold in a number leaf loads as written: boot goes on, validate() reports it, the leaf rule refuses it by key.
  2. // CREATE BUNDLE NOW answers an over-cap database or manifest with a 422 in words the page shows, and writes nothing.
  3. The restore preview names a member left out when the bundle was made, with its size and cap, on the config and cookies lines.
  4. An over-cap cookies.txt is left as it is: the database and the bundle's motif.yaml still stage, and every surface says so.
  5. The loader docstrings describe the lossless coercion.
"""
from __future__ import annotations

import inspect
import io
import json
import logging
import os
import shutil
import subprocess
import sys
import tarfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core import bundle, config_file, db_backup, events
from app.core.auth import create_admin, init_auth_schema
from app.core.db import CURRENT_SCHEMA_VERSION, init_db
from tests._slice_helpers import slice_between
from tests.test_v0_51_339_bundle_staging_boot import _H, LIVE_YAML, _bundle, _live, _marker_rows

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
STAMP = "20260914-040001"
HUGE = "1" * 310
_NODE = shutil.which("node")

if os.environ.get("MOTIF_REQUIRE_NODE") and not _NODE:
    raise RuntimeError("MOTIF_REQUIRE_NODE=1 but node is not on PATH — the settings restore card harness would silently not run")

needs_node = pytest.mark.skipif(not _NODE, reason="node not installed")


@pytest.fixture
def no_env(monkeypatch):
    for env_name, _dotted, _conv in config_file.ENV_BINDINGS:
        monkeypatch.delenv(env_name, raising=False)


@pytest.fixture
def api(tmp_path, monkeypatch):
    cd = tmp_path / "cfg"
    cd.mkdir()
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(cd))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(cd / "data"))
    from app.config import Settings
    from app.web import api as api_mod
    monkeypatch.setattr(api_mod, "log_event", lambda *a, **k: None)
    settings = Settings(config_dir=cd, data_dir=cd / "data")
    init_db(settings.db_path)
    init_auth_schema(settings.db_path)
    create_admin(settings.db_path, username="testadmin", password="testpassword")
    (cd / "motif.yaml").write_text(LIVE_YAML)
    (cd / "backups").mkdir()
    return TestClient(api_mod.create_app(settings), raise_server_exceptions=False), cd


def _sizes(b: Path) -> dict[str, int]:
    with tarfile.open(b, "r:gz") as t:
        return {ti.name: ti.size for ti in t}


def _with_config(root: Path, yaml_text: str) -> Path:
    src = root / "src"
    src.mkdir(parents=True, exist_ok=True)
    init_db(src / "motif.db")
    (src / "motif.yaml").write_text(yaml_text)
    bf = bundle.create_bundle(src / "motif.db", src, config_file=src / "motif.yaml", cookies_file=None, themes_dir=None,
                              now_stamp=STAMP, motif_version="0.51.342", schema_version=CURRENT_SCHEMA_VERSION)
    return src / "backups" / bf.name


def _node(script: str, payload: dict):
    r = subprocess.run([_NODE, "-e", script], input=json.dumps(payload), capture_output=True, text=True, timeout=60, cwd=REPO)
    assert r.returncode == 0, r.stderr[-1500:]
    return json.loads(r.stdout)


# ── 1. an integer no float can hold ──────────────────────────────────

SIGNS = pytest.mark.parametrize("sign", ["", "-"], ids=["positive", "negative"])


@SIGNS
def test_an_integer_no_float_can_hold_loads_as_written_and_validate_reports_it(tmp_path, no_env, caplog, sign):
    from app.config import Settings
    (tmp_path / "motif.yaml").write_text(f"loudness:\n  target_lufs: {sign}{HUGE}\n")
    with caplog.at_level(logging.WARNING, logger=config_file.log.name):
        cfg = config_file.ConfigFile(tmp_path / "motif.yaml").load()
    assert cfg.loudness.target_lufs == int(sign + HUGE), "kept as written, as the loader before the coercion kept it"
    assert any(r.levelno == logging.WARNING and "no float can hold" in r.getMessage() for r in caplog.records)
    errs = config_file.validate(cfg, require_themes_dir=False)
    assert [e for e in errs if e.startswith("loudness.target_lufs must be between -70 and 0 LUFS")], errs
    boot = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")  # Settings.__init__ calls load() with no try around it
    assert boot.cfg.loudness.target_lufs == cfg.loudness.target_lufs


@SIGNS
def test_the_leaf_rule_refuses_it_by_key_on_both_sides_and_staging_keeps_the_database(tmp_path, no_env, sign):
    text = f"loudness:\n  target_lufs: {sign}{HUGE}\n"
    said = {}
    for side in ("live", "bundle"):
        flat, said[side] = bundle.flatten_config(text, side=side)
        assert flat == {} and said[side] and said[side].startswith("loudness.target_lufs must be "), (side, said[side])
        assert HUGE not in said[side], "by key, never the value"
    db, cd = _live(tmp_path)
    b = _with_config(tmp_path / "carry", text)
    assert bundle.preview(b, cd / "motif.yaml")["config_parse_error"] == {"live": None, "bundle": said["bundle"]}
    (cd / "motif.yaml").write_text(text)
    assert bundle.preview(_bundle(tmp_path / "clean"), cd / "motif.yaml")["config_parse_error"] == {"live": said["live"], "bundle": None}
    (cd / "motif.yaml").write_text(LIVE_YAML)
    with pytest.raises(ValueError, match=r"loudness\.target_lufs"):
        bundle.stage_bundle_restore(db, cd, b, keep_config=False)
    assert bundle.pending_members(db, cd) == []
    assert bundle.stage_bundle_restore(db, cd, b, keep_config=True).staged == ["database"]


# ── 1b. an integer too long to write as text, in a text leaf ─────────

LONG_HEX = "0x" + "f" * 4000  # 16000 bits — YAML builds it without Python's int-to-text digit limit; str() of it raises
TEXT_LEAVES = pytest.mark.parametrize("key", ["token", "url"])


@pytest.fixture
def int_text_limit():
    old = sys.get_int_max_str_digits()
    sys.set_int_max_str_digits(4300)  # the interpreter default the image runs under
    yield
    sys.set_int_max_str_digits(old)


def _is_long_int(v) -> bool:
    return isinstance(v, int) and not isinstance(v, bool) and v.bit_length() == 16000  # never repr'd: that raises too


@TEXT_LEAVES
def test_an_integer_too_long_to_write_as_text_loads_as_written(tmp_path, no_env, caplog, int_text_limit, key):
    from app.config import Settings
    (tmp_path / "motif.yaml").write_text(f"plex:\n  {key}: {LONG_HEX}\n")
    with caplog.at_level(logging.WARNING, logger=config_file.log.name):
        cfg = config_file.ConfigFile(tmp_path / "motif.yaml").load()
    assert _is_long_int(getattr(cfg.plex, key)), "kept as written, as the loader before the coercion kept it"
    warned = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING and "too long to write as text" in r.getMessage()]
    assert len(warned) == 1 and "16000-bit" in warned[0] and "ffff" not in warned[0], warned
    assert isinstance(config_file.validate(cfg, require_themes_dir=False), list)
    boot = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")  # Settings.__init__ calls load() with no try around it
    assert _is_long_int(getattr(boot.cfg.plex, key))


@TEXT_LEAVES
def test_the_leaf_rule_refuses_the_long_integer_by_key_and_staging_keeps_the_database(tmp_path, no_env, int_text_limit, key):
    text = f"plex:\n  {key}: {LONG_HEX}\n"
    said = {}
    for side in ("live", "bundle"):
        flat, said[side] = bundle.flatten_config(text, side=side)
        assert flat == {} and said[side] == f"plex.{key} must be a string, not int", (side, said[side])
    db, cd = _live(tmp_path)
    b = _with_config(tmp_path / "carry", text)
    assert bundle.preview(b, cd / "motif.yaml")["config_parse_error"] == {"live": None, "bundle": said["bundle"]}
    (cd / "motif.yaml").write_text(text)
    assert bundle.preview(_bundle(tmp_path / "clean"), cd / "motif.yaml")["config_parse_error"] == {"live": said["live"], "bundle": None}
    (cd / "motif.yaml").write_text(LIVE_YAML)
    with pytest.raises(ValueError) as refused:
        bundle.stage_bundle_restore(db, cd, b, keep_config=False)
    assert said["bundle"] in str(refused.value) and "KEEP MY CURRENT CONFIG" in str(refused.value), refused.value
    assert bundle.pending_members(db, cd) == [] and not bundle.STAGING_LOCK.locked()
    assert bundle.stage_bundle_restore(db, cd, b, keep_config=True).staged == ["database"]


@TEXT_LEAVES
def test_the_restore_endpoint_names_the_long_integer_by_key_and_stages_its_database_with_keep(api, tmp_path, int_text_limit, key):
    client, cd = api
    b = _with_config(tmp_path / "carry", f"plex:\n  {key}: {LONG_HEX}\n")
    shutil.copyfile(b, cd / "backups" / b.name)
    words = f"plex.{key} must be a string, not int"
    r = client.post("/api/admin/database-restore", json={"name": b.name}, headers=_H)
    assert r.status_code == 200, r.text[:300]
    assert r.json()["preview"]["config_parse_error"] == {"live": None, "bundle": words}
    refused = client.post("/api/admin/database-restore", json={"name": b.name, "confirm": True, "keep_config": False}, headers=_H)
    assert refused.status_code == 422 and words in refused.json()["detail"], refused.text[:300]
    assert client.get("/api/admin/database-restore/pending", headers=_H).json()["members"] == []
    kept = client.post("/api/admin/database-restore", json={"name": b.name, "confirm": True, "keep_config": True}, headers=_H)
    assert kept.status_code == 200 and kept.json()["members"] == ["database"], kept.text[:300]


# ── 2. // CREATE BUNDLE NOW over a cap ───────────────────────────────

@pytest.mark.parametrize("member, words", [(bundle.MEMBER_DB, "the database is about "),  # v0.51.344: a 16-byte cap is refused before the VACUUM, by the estimate
                                           (bundle.MEMBER_MANIFEST, "the bundle manifest is ")], ids=["database", "manifest"])
def test_create_bundle_now_answers_an_over_cap_refusal_in_words_and_writes_nothing(api, tmp_path, monkeypatch, member, words):
    client, cd = api
    monkeypatch.setattr(bundle, "_MEMBER_CAP", {**bundle._MEMBER_CAP, member: 16})
    r = client.post("/api/admin/database-backup?kind=bundle", headers=_H)
    assert r.status_code == 422, r.text
    detail = r.json()["detail"]
    assert detail.startswith(words) and "no bundle was written; take a plain snapshot instead" in detail, detail
    assert str(tmp_path) not in detail and os.path.realpath(tmp_path) not in detail
    assert list((cd / "backups").iterdir()) == [], "no bundle and no temp directory left behind"
    assert client.get("/api/admin/database-backups", headers=_H).json()["backups"] == []
    assert client.post("/api/admin/database-backup", headers=_H).status_code == 200, "the plain snapshot the words point at"


_GW = ("function gatewayTimeoutNote(err) {", "\n  async function api(method, path, body) {")
_BUNDLE_BTN = ("bundleBtn?.addEventListener('click', async () => {", "\n    createBtn.addEventListener('click', async () => {")

_BUNDLE_HARNESS = r"""
const vm = require('vm');
const { gw, btn, cases } = JSON.parse(require('fs').readFileSync(0, 'utf8'));
(async () => {
  const out = [];
  for (const c of cases) {
    let handler = null;
    const classes = [];
    const ctx = vm.createContext({
      bundleBtn: { disabled: false, textContent: '// CREATE BUNDLE NOW', addEventListener: (t, fn) => { handler = fn; } },
      createBtn: { disabled: false },
      status: { textContent: '', className: 'form-status', classList: { add: (k) => classes.push(k) } },
      api: async () => { const e = new Error(c.message); e.status = c.status; e.detail = c.detail; throw e; },
      refreshList: async () => {},
    });
    vm.runInContext(gw + '\n' + btn, ctx);
    await handler();
    out.push({ text: ctx.status.textContent, classes, unlocked: !ctx.bundleBtn.disabled && !ctx.createBtn.disabled });
  }
  process.stdout.write(JSON.stringify(out));
})().catch((e) => { console.error((e && e.stack) || e); process.exit(1); });
"""


@needs_node
def test_the_settings_page_shows_the_refusals_words_not_its_json(api, monkeypatch):
    client, cd = api
    monkeypatch.setattr(bundle, "_MEMBER_CAP", {**bundle._MEMBER_CAP, bundle.MEMBER_DB: 16})
    r = client.post("/api/admin/database-backup?kind=bundle", headers=_H)
    assert r.status_code == 422, r.text
    refused = {"status": r.status_code, "detail": r.json()["detail"], "message": f"{r.status_code}: {r.text}"}
    gateway = {"status": 504, "detail": None, "message": "504: <html>Gateway Time-out</html>"}
    shown, timed_out = _node(_BUNDLE_HARNESS, {"gw": slice_between(APP_JS, *_GW), "btn": slice_between(APP_JS, *_BUNDLE_BTN),
                                               "cases": [refused, gateway]})
    assert shown["text"] == "✗ " + refused["detail"] and "form-status-fail" in shown["classes"], shown
    assert shown["unlocked"]
    assert "reverse proxy timed out" in timed_out["text"] and "warn" in timed_out["classes"], "the gateway note still wins"


# ── 3. a member left out when the bundle was made ────────────────────

def _left_out_bundle(root: Path, monkeypatch, member: str) -> Path:
    src = root / "src"
    src.mkdir(parents=True, exist_ok=True)
    init_db(src / "motif.db")
    (src / bundle.MEMBER_CONFIG).write_text("plex:\n  url: http://plex:32400\n  token: BUNDLE-TOKEN\n")
    (src / bundle.MEMBER_COOKIES).write_text("# bundle cookies\n")
    (src / member).write_text("#" * 64)
    with monkeypatch.context() as m:
        m.setattr(events, "log_event", lambda *a, **k: None)
        m.setattr(bundle, "_MEMBER_CAP", {**bundle._MEMBER_CAP, member: 63})
        bf = bundle.create_bundle(src / "motif.db", src, config_file=src / bundle.MEMBER_CONFIG,
                                  cookies_file=src / bundle.MEMBER_COOKIES, themes_dir=None, now_stamp=STAMP,
                                  motif_version="0.51.342", schema_version=CURRENT_SCHEMA_VERSION)
    return src / "backups" / bf.name


@pytest.mark.parametrize("member", [bundle.MEMBER_CONFIG, bundle.MEMBER_COOKIES])
def test_the_preview_names_a_member_left_out_at_create_with_its_size_and_cap(tmp_path, monkeypatch, member):
    b = _left_out_bundle(tmp_path / "mk", monkeypatch, member)
    assert bundle.read_manifest(b)["left_out"] == {member: {"size": 64, "cap": 63}}, "the premise: create noted it"
    db, cd = _live(tmp_path)
    p = bundle.preview(b, cd / "motif.yaml")
    words = f"{member} was left out when the bundle was made (64 bytes, over its 63-byte cap)"
    assert p["left_out"] == {member: words}, p["left_out"]
    if member == bundle.MEMBER_CONFIG:
        assert p["config_in_bundle"] is False and p["config_parse_error"] == {"live": None, "bundle": None}, \
            "the page's no-config behaviour: nothing forces KEEP"
        assert p["cookies"] == "in bundle"
        assert bundle.stage_bundle_restore(db, cd, b, keep_config=False).staged == ["database", "cookies"]
    else:
        assert p["cookies"].startswith("not in bundle — ") and words in p["cookies"], p["cookies"]
        assert bundle.stage_bundle_restore(db, cd, b, keep_config=False).staged == ["database", "config"]


def _rewrite_manifest(b: Path, left_out) -> None:
    with tarfile.open(b, "r:gz") as t:
        parts = [(ti, t.extractfile(ti).read()) for ti in t]
    with tarfile.open(b, "w:gz") as t:
        for ti, data in parts:
            if ti.name == bundle.MEMBER_MANIFEST:
                data = json.dumps({**json.loads(data), "left_out": left_out}).encode()
                ti.size = len(data)
            t.addfile(ti, io.BytesIO(data))


@pytest.mark.parametrize("left_out", ["cookies.txt", ["cookies.txt"], {"cookies.txt": "big"},
                                      {"cookies.txt": {"size": "64", "cap": 63}}, {"cookies.txt": {"size": True, "cap": 63}},
                                      {"motif.yaml": {"size": 64, "cap": 63}}],
                         ids=["a-string", "a-list", "a-string-entry", "a-string-size", "a-bool-size", "a-member-it-carries"])
def test_a_foreign_manifests_left_out_note_is_guarded_never_trusted(tmp_path, left_out):
    b = _bundle(tmp_path / "mk", cookies=False)
    _rewrite_manifest(b, left_out)
    assert bundle.inspect_bundle(b).ok
    db, cd = _live(tmp_path)
    p = bundle.preview(b, cd / "motif.yaml")
    assert p["left_out"] == {} and p["cookies"] == "not in bundle" and p["config_in_bundle"] is True, p


# ── 4. an over-cap cookies.txt is left as it is ──────────────────────

def _over_cap(tmp_path: Path, monkeypatch, *members: str) -> tuple[Path, dict[str, str]]:
    b = _bundle(tmp_path / "mk")
    sizes = _sizes(b)
    monkeypatch.setattr(bundle, "_MEMBER_CAP", {**bundle._MEMBER_CAP, **{m: sizes[m] - 1 for m in members}})
    return b, {m: f"{m} is {sizes[m]} bytes, over its {sizes[m] - 1}-byte cap" for m in members}


def test_an_over_cap_cookies_txt_is_left_as_it_is_and_the_config_still_stages(tmp_path, monkeypatch, caplog):
    b, words = _over_cap(tmp_path, monkeypatch, bundle.MEMBER_COOKIES)
    db, cd = _live(tmp_path)
    live_cookies = (cd / "cookies.txt").read_bytes()
    assert words[bundle.MEMBER_COOKIES] in bundle.preview(b, cd / "motif.yaml")["cookies"]
    with caplog.at_level(logging.INFO, logger=bundle.log.name):
        c = bundle.stage_bundle_restore(db, cd, b, keep_config=False)
    assert c.staged == ["database", "config"] and bundle.pending_members(db, cd) == ["database", "config"]
    assert list(c.left_as_is) == [bundle.MEMBER_COOKIES], c.left_as_is
    assert words[bundle.MEMBER_COOKIES] in c.left_as_is[bundle.MEMBER_COOKIES] and "left as it is" in c.left_as_is[bundle.MEMBER_COOKIES]
    said = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING and b.name in r.getMessage()]
    assert any(words[bundle.MEMBER_COOKIES] in m and "left as it is" in m for m in said), said
    assert not any("only its database can be restored" in m for m in said), "the check's warning no longer says the config is lost"
    assert "BUNDLE-TOKEN" in (cd / bundle.CONFIG_PENDING).read_text() and _marker_rows(db_backup.restore_pending_path(db)) == 1
    assert db_backup.apply_pending_restore(db, cd, now_stamp=STAMP)["applied"] is True
    assert bundle.apply_pending_config(cd, now_stamp=STAMP)["applied"] == ["motif.yaml"]
    assert bundle.apply_pending_cookies(cd, cd / "cookies.txt", now_stamp=STAMP) is None, "nothing staged for the cookies file"
    assert (cd / "cookies.txt").read_bytes() == live_cookies and not list(cd.glob("cookies.txt.prerestore-*")), "untouched"


def test_keep_my_current_config_still_stages_the_database_alone_beside_an_over_cap_cookies_txt(tmp_path, monkeypatch):
    b, _ = _over_cap(tmp_path, monkeypatch, bundle.MEMBER_COOKIES)
    db, cd = _live(tmp_path)
    c = bundle.stage_bundle_restore(db, cd, b, keep_config=True)
    assert c.staged == ["database"] and c.left_as_is == {} and bundle.pending_members(db, cd) == ["database"]
    assert (cd / "motif.yaml").read_text() == LIVE_YAML and (cd / "cookies.txt").read_text() == "# live cookies\n"


@pytest.mark.parametrize("members", [(bundle.MEMBER_CONFIG,), (bundle.MEMBER_CONFIG, bundle.MEMBER_COOKIES)],
                         ids=["motif.yaml", "motif.yaml-and-cookies.txt"])
def test_an_over_cap_motif_yaml_is_still_refused_without_keep(tmp_path, monkeypatch, members):
    b, words = _over_cap(tmp_path, monkeypatch, *members)
    db, cd = _live(tmp_path)
    assert bundle.preview(b, cd / "motif.yaml")["config_parse_error"]["bundle"] == words[bundle.MEMBER_CONFIG], "the page forces KEEP"
    with pytest.raises(ValueError) as refused:
        bundle.stage_bundle_restore(db, cd, b, keep_config=False)
    assert all(w in str(refused.value) for w in words.values()) and "KEEP MY CURRENT CONFIG" in str(refused.value), refused.value
    assert bundle.pending_members(db, cd) == [] and not bundle.STAGING_LOCK.locked()
    assert bundle.stage_bundle_restore(db, cd, b, keep_config=True).staged == ["database"]


def test_the_endpoint_stages_the_config_beside_an_over_cap_cookies_txt(api, tmp_path, monkeypatch):
    client, cd = api
    b, words = _over_cap(tmp_path, monkeypatch, bundle.MEMBER_COOKIES)
    shutil.copyfile(b, cd / "backups" / b.name)
    pv = client.post("/api/admin/database-restore", json={"name": b.name}, headers=_H).json()["preview"]
    assert words[bundle.MEMBER_COOKIES] in pv["cookies"] and pv["left_out"] == {bundle.MEMBER_COOKIES: words[bundle.MEMBER_COOKIES]}
    r = client.post("/api/admin/database-restore", json={"name": b.name, "confirm": True, "keep_config": False}, headers=_H)
    assert r.status_code == 200, r.text
    assert r.json()["members"] == ["database", "config"]
    assert client.get("/api/admin/database-restore/pending", headers=_H).json()["members"] == ["database", "config"]
    assert not (cd / bundle.COOKIES_PENDING).exists()


# ── the settings restore card, against the real preview and staging ─

_SHOW = ("function showBundlePreview(r) {", "\n    function hideBundlePreview(")
_HIDE = ("function hideBundlePreview() {", "\n    document.getElementById('database-restore-preview-cancel-btn')")
_STAGE = ("document.getElementById('database-restore-stage-btn')?.addEventListener('click', async () => {",
          "\n    function showStaged(msg) {")
_PENDING = ("async function refreshPending() {", "\n    const previewEl = document.getElementById('database-restore-preview');")

_CARD_HARNESS = r"""
const vm = require('vm');
const P = JSON.parse(require('fs').readFileSync(0, 'utf8'));
function contextFor(apiImpl) {
  const els = {}, listeners = {}, seen = { confirms: [], bodies: [] };
  const el = (id) => (els[id] = els[id] || { id, textContent: '', innerHTML: '', hidden: true, checked: false, disabled: false,
    dataset: {}, className: '', addEventListener: (t, fn) => { listeners[id] = fn; }, scrollIntoView() {} });
  const ctx = vm.createContext({
    document: { getElementById: el }, htmlEscape: (s) => String(s),
    confirm: (m) => { seen.confirms.push(String(m)); return true; }, alert: (m) => { seen.alert = String(m); },
    api: apiImpl, gatewayTimeoutNote: () => null, showStaged() {}, refreshPending() {},
    previewEl: el('database-restore-preview'), pendingBanner: el('database-restore-pending'),
  });
  return { ctx, els, listeners, seen };
}
(async () => {
  const out = { runs: [], banners: [] };
  for (const keepWanted of [false, true]) {
    let r;
    r = contextFor(async (method, path, body) => { r.seen.bodies.push(body); return { message: 'staged' }; });
    vm.runInContext(P.show + '\n' + P.hide + '\n' + P.stage, r.ctx);
    vm.runInContext('showBundlePreview', r.ctx)({ preview: P.pv });
    const keep = r.els['database-restore-keep-config'];
    const run = { keep: keepWanted, config: r.els['restore-preview-config'].textContent,
                  cookies: r.els['restore-preview-cookies'].textContent, checked: keep.checked, disabled: keep.disabled };
    if (keep.disabled && keep.checked !== keepWanted) { run.offered = false; out.runs.push(run); continue; }
    keep.checked = keepWanted;
    await r.listeners['database-restore-stage-btn']();
    Object.assign(run, { offered: true, confirm: r.seen.confirms[0], body: r.seen.bodies[0] });
    out.runs.push(run);
  }
  for (const members of P.members) {
    const r = contextFor(async () => ({ ok: true, pending: members.length > 0, members }));
    vm.runInContext(P.pending, r.ctx);
    await vm.runInContext('refreshPending', r.ctx)();
    out.banners.push({ members, hidden: r.els['database-restore-pending'].hidden, text: r.els['database-restore-pending-members'].textContent });
  }
  process.stdout.write(JSON.stringify(out));
})().catch((e) => { console.error((e && e.stack) || e); process.exit(1); });
"""


def _card_case(case: str, tmp_path: Path, monkeypatch) -> tuple[Path, dict]:
    if case == "clean":
        return _bundle(tmp_path / "mk"), {"cookies": ["will replace"]}
    if case == "config long integer":  # v0.51.342: the preview answered a 422 in Python's words, so this card never rendered
        return _with_config(tmp_path / "mk", f"plex:\n  token: {LONG_HEX}\n"), {"config": ["plex.token must be a string, not int"], "forced": True}
    if case.endswith("over cap"):
        member = bundle.MEMBER_COOKIES if case.startswith("cookies") else bundle.MEMBER_CONFIG
        b, words = _over_cap(tmp_path, monkeypatch, member)
        if member == bundle.MEMBER_CONFIG:
            return b, {"config": [words[member]], "forced": True}
        return b, {"cookies": [words[member], "your cookies file stays as it is"]}
    member = bundle.MEMBER_COOKIES if case.startswith("cookies") else bundle.MEMBER_CONFIG
    words = f"{member} was left out when the bundle was made (64 bytes, over its 63-byte cap)"
    b = _left_out_bundle(tmp_path / "mk", monkeypatch, member)
    if member == bundle.MEMBER_CONFIG:
        return b, {"config": [words, "motif.yaml stays as it is"]}
    return b, {"cookies": [words, "your cookies file stays as it is"]}


@needs_node
@pytest.mark.parametrize("case", ["clean", "cookies over cap", "config over cap", "config left out", "cookies left out",
                                  "config long integer"])
def test_the_restore_card_says_what_stays_and_offers_only_what_the_server_stages(api, tmp_path, monkeypatch, int_text_limit, case):
    client, cd = api
    b, want = _card_case(case, tmp_path, monkeypatch)
    shutil.copyfile(b, cd / "backups" / b.name)
    pv = client.post("/api/admin/database-restore", json={"name": b.name}, headers=_H).json()["preview"]
    slices = {"show": slice_between(APP_JS, *_SHOW), "hide": slice_between(APP_JS, *_HIDE),
              "stage": slice_between(APP_JS, *_STAGE), "pending": slice_between(APP_JS, *_PENDING)}
    card = _node(_CARD_HARNESS, {**slices, "pv": pv, "members": []})
    staged: list[list[str]] = []
    for run in card["runs"]:
        for line in ("config", "cookies"):
            assert all(w in run[line] for w in want.get(line, [])), (case, line, run[line])
        assert ("will replace" in run["cookies"]) is (pv["cookies"] == "in bundle" and not want.get("forced")), (case, run["cookies"])
        assert run["disabled"] is bool(want.get("forced")), (case, run)
        body = {"name": b.name, "confirm": True, "keep_config": run["keep"]}
        r = client.post("/api/admin/database-restore", json=body, headers=_H)
        assert run["offered"] is (r.status_code == 200), f"{case}: the page offers keep={run['keep']} iff the server stages it ({r.text})"
        if not run["offered"]:
            continue
        assert run["body"] == body
        members = r.json()["members"]
        staged.append(members)
        if run["keep"]:
            assert "stay as they are" in run["confirm"], run["confirm"]
        else:
            assert ("your cookies file stays as it is" in run["confirm"]) is ("cookies" not in members), (case, run["confirm"], members)
    assert staged, case
    banners = _node(_CARD_HARNESS, {**slices, "pv": pv, "members": staged})["banners"]
    for banner in banners:
        m = banner["members"]
        assert not banner["hidden"] and all(w in banner["text"] for w in m), banner
        assert ("your cookies file stays as it is" in banner["text"]) is ("config" in m and "cookies" not in m), (case, banner)


# ── 5. the loader docstrings ─────────────────────────────────────────

def test_the_loader_docstrings_describe_the_lossless_coercion():
    for fn in (config_file.validate, config_file._hydrate_dataclass):
        doc = inspect.getdoc(fn)
        assert "_coerce_leaf" in doc, f"{fn.__name__} names the coercion it relies on"
        assert "does NOT" not in doc and "intentionally minimal" not in doc, f"{fn.__name__} still says the loader does not coerce"
    assert config_file._coerce_leaf(-18.0, "-16") == -16.0 and config_file._coerce_leaf("", 1) == "1"
