"""v0.51.342: // SAVE DOWNLOADS answered 400 on every click since v0.51.189.

_apply_partial_config had bool / int / list / dict branches and wrote str(v) for
everything else, so loudness.target_lufs (the one float leaf) was stored as "-16" and
validate() refused the whole PATCH with a '<=' float-vs-str type error.

  1. target_lufs saves as a float; a non-number is a 400 in words naming the key.
  2. The page's own load-then-save (populateConfigForms -> collectFieldsForTab, run
     under node over settings.html's real controls) saves, for every save button.
  3. Every scalar leaf of every section saves its default back at its own type.
"""
from __future__ import annotations

import dataclasses
import json
import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml
from fastapi.testclient import TestClient

from app.core.auth import create_admin, init_auth_schema
from app.core.config_file import MotifConfig
from app.core.db import init_db

from tests._slice_helpers import slice_between

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
HTML = (REPO / "app" / "web" / "templates" / "settings.html").read_text()
_H = {"X-Authentik-Username": "testadmin"}

_NODE = shutil.which("node")
if os.environ.get("MOTIF_REQUIRE_NODE") and not _NODE:
    raise RuntimeError("MOTIF_REQUIRE_NODE=1 but node is not on PATH — the settings load-then-save check would not run")


def _api(root: Path, mp):
    mp.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    mp.setenv("MOTIF_CONFIG_DIR", str(root))
    mp.setenv("MOTIF_DATA_DIR", str(root / "data"))
    root.mkdir(parents=True, exist_ok=True)
    from app.config import Settings
    settings = Settings(config_dir=root, data_dir=root / "data")
    init_db(settings.db_path)
    init_auth_schema(settings.db_path)
    create_admin(settings.db_path, username="testadmin", password="testpassword")
    from app.web import api as api_mod
    mp.setattr(api_mod, "log_event", lambda *a, **k: None)
    return TestClient(api_mod.create_app(settings)), settings


def _patch_raw(client, body: dict):
    # v0.51.342: raw bytes so NaN / Infinity reach the server the way a hand-rolled client could send them.
    return client.patch("/api/config", headers={**_H, "content-type": "application/json"}, content=json.dumps(body))


# ── 1. target_lufs over the wire ──────────────────────────────────────

@pytest.mark.parametrize("sent, stored", [(-16, -16.0), (-18.5, -18.5), ("-16", -16.0)])
def test_patch_target_lufs_saves_a_float(tmp_path, monkeypatch, sent, stored):
    client, settings = _api(tmp_path, monkeypatch)
    r = client.patch("/api/config", headers=_H, json={"loudness": {"target_lufs": sent}})
    assert r.status_code == 200, r.text
    assert r.json()["config"]["loudness"]["target_lufs"] == stored
    settings.reload()
    assert settings.cfg.loudness.target_lufs == stored and type(settings.cfg.loudness.target_lufs) is float
    on_disk = yaml.safe_load((tmp_path / "motif.yaml").read_text())["loudness"]["target_lufs"]
    assert on_disk == stored and type(on_disk) is float


@pytest.mark.parametrize("sent", [None, True, "", "abc", float("nan"), float("inf"), "-Infinity",
                                  "-1_6", "１６", " -16"])  # v0.51.344: float() read digit grouping, full-width and Unicode-space spellings
def test_a_non_number_target_is_a_400_in_words_and_saves_nothing(tmp_path, monkeypatch, sent):
    client, settings = _api(tmp_path, monkeypatch)
    assert client.patch("/api/config", headers=_H, json={"downloads": {"concurrency": 2}}).status_code == 200
    before = (tmp_path / "motif.yaml").read_text()
    r = _patch_raw(client, {"downloads": {"concurrency": 3}, "loudness": {"target_lufs": sent}})
    assert r.status_code == 400, r.text
    detail = r.json()["detail"]
    assert isinstance(detail, str) and "loudness.target_lufs" in detail, detail
    assert "unexpected type" not in detail
    if isinstance(sent, str) and sent:
        assert sent not in detail, "the refusal names the key, never echoes the value"
    assert settings.cfg.downloads.concurrency == 2
    assert settings.cfg.loudness.target_lufs == -18.0
    assert (tmp_path / "motif.yaml").read_text() == before


def test_an_out_of_range_target_is_still_refused_by_validate(tmp_path, monkeypatch):
    client, settings = _api(tmp_path, monkeypatch)
    r = client.patch("/api/config", headers=_H, json={"loudness": {"target_lufs": -90}})
    assert r.status_code == 400
    errors = r.json()["detail"]["errors"]
    assert any("loudness.target_lufs must be between -70 and 0" in e for e in errors), errors
    assert settings.cfg.loudness.target_lufs == -18.0


def test_the_save_downloads_body_saves_with_the_target_a_float(tmp_path, monkeypatch):
    client, settings = _api(tmp_path, monkeypatch)
    body = {
        "downloads": {"rate_mode": "adaptive", "adaptive_min_per_hour": 6, "adaptive_max_per_hour": 90,
                      "rate_per_hour": 40, "concurrency": 2, "audio_quality": 3, "geo_bypass": True,
                      "geo_bypass_country": "GB", "proxy_url": ""},
        "loudness": {"normalize_on_download": True, "normalize_auto_added": True, "target_lufs": -16},
    }
    r = client.patch("/api/config", headers=_H, json=body)
    assert r.status_code == 200, r.text
    settings.reload()
    got = dataclasses.asdict(settings.cfg)
    for section, fields in body.items():
        for k, v in fields.items():
            assert got[section][k] == v, f"{section}.{k}"
    assert type(settings.cfg.loudness.target_lufs) is float
    assert type(yaml.safe_load((tmp_path / "motif.yaml").read_text())["loudness"]["target_lufs"]) is float


# ── 2. the settings page's own load-then-save ─────────────────────────

def _controls() -> list[dict]:
    out = []
    for m in re.finditer(r"<(input|select|textarea)\b([^>]*)>", HTML, re.S):
        f = re.search(r'data-cfg-field(-list|-lines)?="([^"]+)"', m.group(2))
        if not f:
            continue
        t = re.search(r'\stype="([^"]+)"', m.group(2))
        typ = t.group(1) if t else {"input": "text", "select": "select-one", "textarea": "textarea"}[m.group(1)]
        out.append({"type": typ, "kind": f.group(1) or "", "path": f.group(2)})
    return out


CONTROLS = _controls()
SAVE_SPECS = re.findall(r'data-save="([^"]+)"', HTML)

_FAKE_DOM = r"""
const CONTROLS = JSON.parse(process.env.CONTROLS);
const ATTR = { 'data-cfg-field': 'cfgField', 'data-cfg-field-list': 'cfgFieldList',
               'data-cfg-field-lines': 'cfgFieldLines', 'data-env-badge': 'envBadge' };
const KIND = { '': 'cfgField', '-list': 'cfgFieldList', '-lines': 'cfgFieldLines' };
class El {
  constructor(c) { this.type = c.type; this.checked = false; this.disabled = false; this._v = '';
                   this.dataset = { [KIND[c.kind]]: c.path }; this.style = {}; }
  get value() { return this._v; }
  set value(v) { const s = String(v);
                 this._v = this.type === 'number' && (s.trim() === '' || !Number.isFinite(Number(s))) ? '' : s; }
  addEventListener() {}
  closest() { return null; }
}
const els = CONTROLS.map((c) => new El(c));
function matches(el, sel) {
  const m = sel.trim().match(/^\[([\w-]+)(?:(\^?=)"([^"]*)")?\]$/);
  if (!m || !(m[1] in ATTR)) throw new Error('selector not modelled: ' + sel);
  const v = el.dataset[ATTR[m[1]]];
  if (v === undefined) return false;
  if (!m[2]) return true;
  return m[2] === '=' ? v === m[3] : v.startsWith(m[3]);
}
const document = {
  querySelectorAll: (sel) => els.filter((el) => sel.split(',').some((s) => matches(el, s))),
  querySelector: (sel) => els.find((el) => sel.split(',').some((s) => matches(el, s))) || null,
};
"""

_DRIVE = r"""
populateConfigForms(JSON.parse(process.env.CONFIG));
const loaded = {};
for (const el of els) if (el.dataset.cfgField) loaded[el.dataset.cfgField] = el.type === 'checkbox' ? el.checked : el.value;
const bodies = {};
for (const spec of JSON.parse(process.env.SPECS)) bodies[spec] = collectFieldsForTab(spec);
console.log(JSON.stringify({ loaded, bodies }));
"""


def _load_then_collect(config_response: dict, specs: list[str]) -> dict:
    fns = slice_between(APP_JS, "function getDotted(obj, dotted) {", "\n  async function loadCacheGauge(")
    r = subprocess.run([_NODE, "-e", _FAKE_DOM + fns + _DRIVE], capture_output=True, text=True, timeout=60,
                       env={**os.environ, "CONTROLS": json.dumps(CONTROLS),
                            "CONFIG": json.dumps(config_response), "SPECS": json.dumps(specs)})
    assert r.returncode == 0, r.stderr
    return json.loads(r.stdout)


@pytest.mark.skipif(not _NODE, reason="node not installed")
@pytest.mark.parametrize("stored", [-18.0, -16.5, -23.0])
def test_downloads_load_then_save_keeps_the_target(tmp_path, monkeypatch, stored):
    client, settings = _api(tmp_path, monkeypatch)
    if stored != settings.cfg.loudness.target_lufs:
        assert client.patch("/api/config", headers=_H, json={"loudness": {"target_lufs": stored}}).status_code == 200
    run = _load_then_collect(client.get("/api/config", headers=_H).json(), ["downloads loudness"])
    assert run["loaded"]["loudness.target_lufs"] != "", "TARGET LOUDNESS loads blank and every save would send null"
    assert float(run["loaded"]["loudness.target_lufs"]) == stored
    body = run["bodies"]["downloads loudness"]
    assert set(body) == {"downloads", "loudness"}
    assert body["loudness"]["target_lufs"] == stored
    r = client.patch("/api/config", headers=_H, json=body)
    assert r.status_code == 200, r.text
    settings.reload()
    assert settings.cfg.loudness.target_lufs == stored and type(settings.cfg.loudness.target_lufs) is float


@pytest.mark.skipif(not _NODE, reason="node not installed")
def test_every_save_button_saves_a_fresh_load_unchanged(tmp_path, monkeypatch):
    client, settings = _api(tmp_path, monkeypatch)
    assert SAVE_SPECS, "settings.html parse found no save buttons"
    before = dataclasses.asdict(settings.cfg)
    run = _load_then_collect(client.get("/api/config", headers=_H).json(), SAVE_SPECS)
    for spec in SAVE_SPECS:
        r = client.patch("/api/config", headers=_H, json=run["bodies"][spec])
        assert r.status_code == 200, f"data-save={spec!r}: {r.text[:300]}"
    settings.reload()
    assert dataclasses.asdict(settings.cfg) == before, "an untouched load-then-save must change nothing"


# ── 3. every scalar leaf saves its default at its own type ────────────

def _scalar_leaves() -> list[tuple[str, str, object]]:
    base = MotifConfig()
    out = []
    for sec in dataclasses.fields(MotifConfig):
        section = getattr(base, sec.name)
        if not dataclasses.is_dataclass(section):
            continue
        for leaf in dataclasses.fields(section):
            default = getattr(section, leaf.name)
            if type(default) in (bool, int, float, str):
                out.append((sec.name, leaf.name, default))
    return out


LEAVES = _scalar_leaves()


def _as_the_form_sends(v):
    # v0.51.342: a number input sends Number(el.value), and JSON.stringify writes -18.0 as -18.
    return int(v) if isinstance(v, float) and v.is_integer() else v


@pytest.fixture(scope="module")
def walk_api(tmp_path_factory):
    with pytest.MonkeyPatch.context() as mp:
        yield _api(tmp_path_factory.mktemp("walk"), mp)


@pytest.mark.parametrize("section, leaf, default", LEAVES, ids=[f"{s}.{k}" for s, k, _ in LEAVES])
def test_every_scalar_leaf_saves_its_default_at_its_own_type(walk_api, section, leaf, default):
    client, settings = walk_api
    r = client.patch("/api/config", headers=_H, json={section: {leaf: _as_the_form_sends(default)}})
    assert r.status_code == 200, f"{section}.{leaf}: {r.text[:300]}"
    settings.reload()
    stored = getattr(getattr(settings.cfg, section), leaf)
    assert type(stored) is type(default), f"{section}.{leaf} came back as {type(stored).__name__}"
    # v0.51.342: WHOLE_SECRET_KEYS default to "", which PATCH reads as keep — still the default; env overrides win on reload.
    if f"{section}.{leaf}" not in settings.env_overrides():
        assert stored == default
    assert type(r.json()["config"][section][leaf]) is type(default)


# ── 4. a float leaf hand-edited as an int or a string still saves a float ──

FLOAT_LEAVES = [(s, k, d) for s, k, d in LEAVES if type(d) is float]
_SEED = {"yaml-int": lambda d: int(d), "yaml-str": lambda d: str(int(d))}


def test_the_walk_finds_a_float_leaf_to_seed():
    assert FLOAT_LEAVES, "no float leaf found — the hand-edited-seed cases below would collect nothing"


def _seeded_api(root: Path, mp, section: str, leaf: str, seeded):
    root.mkdir(parents=True, exist_ok=True)
    (root / "motif.yaml").write_text(yaml.safe_dump({section: {leaf: seeded}}))
    client, settings = _api(root, mp)
    loaded = getattr(getattr(settings.cfg, section), leaf)
    # v0.51.342: reversed — _hydrate_dataclass now coerces a hand-edited `target_lufs: -16` or '-16' to the declared float
    assert loaded == float(seeded) and type(loaded) is float, f"the hand-edited {seeded!r} loaded as {loaded!r}"
    return client, settings


@pytest.mark.parametrize("seed", list(_SEED))
@pytest.mark.parametrize("send", ["form-default", "half-step", "half-step-string"])
@pytest.mark.parametrize("section, leaf, default", FLOAT_LEAVES, ids=[f"{s}.{k}" for s, k, _ in FLOAT_LEAVES])
def test_a_float_leaf_hand_edited_as_an_int_or_string_saves_a_float(tmp_path, monkeypatch, section, leaf, default, seed, send):
    client, settings = _seeded_api(tmp_path, monkeypatch, section, leaf, _SEED[seed](default))
    want = {"form-default": default, "half-step": default - 0.5, "half-step-string": default - 0.5}[send]
    sent = {"form-default": _as_the_form_sends(default), "half-step": want, "half-step-string": str(want)}[send]
    r = client.patch("/api/config", headers=_H, json={section: {leaf: sent}})
    assert r.status_code == 200, f"{section}.{leaf}: {r.text[:300]}"
    settings.reload()
    stored = getattr(getattr(settings.cfg, section), leaf)
    assert stored == want and type(stored) is float, f"{section}.{leaf} stored {stored!r}"
    on_disk = yaml.safe_load((tmp_path / "motif.yaml").read_text())[section][leaf]
    assert on_disk == want and type(on_disk) is float, f"{section}.{leaf} wrote {on_disk!r}"


@pytest.mark.parametrize("seed", list(_SEED))
@pytest.mark.parametrize("sent", [float("inf"), float("nan"), "abc", None, True], ids=["inf", "nan", "abc", "null", "true"])
@pytest.mark.parametrize("section, leaf, default", FLOAT_LEAVES, ids=[f"{s}.{k}" for s, k, _ in FLOAT_LEAVES])
def test_a_hand_edited_float_leaf_refuses_a_non_number_in_words(tmp_path, monkeypatch, section, leaf, default, seed, sent):
    client, settings = _seeded_api(tmp_path, monkeypatch, section, leaf, _SEED[seed](default))
    before = (tmp_path / "motif.yaml").read_text()
    r = _patch_raw(client, {section: {leaf: sent}})
    assert r.status_code == 400, r.text[:300]
    detail = r.json()["detail"]
    assert isinstance(detail, str) and f"{section}.{leaf}" in detail, detail
    if isinstance(sent, str):
        assert sent not in detail, "the refusal names the key, never echoes the value"
    assert (tmp_path / "motif.yaml").read_text() == before


def test_the_walk_covers_every_scalar_control_on_the_settings_page():
    walked = {f"{s}.{k}" for s, k, _ in LEAVES}
    scalar_controls = {c["path"] for c in CONTROLS if not c["kind"] and c["path"].count(".") == 1}
    assert scalar_controls, "settings.html parse found no controls"
    assert scalar_controls <= walked, sorted(scalar_controls - walked)
