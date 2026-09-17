"""v0.51.344: R3-F9 — the restore preview's diff is the FILE's, but env wins at boot for some keys: those rows are
marked with the env var, the payload carries the settings page's badge rule, and the card badges exactly them and
says the file changes while the running value does not."""
from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from app.core import config_file as cf
from tests._slice_helpers import slice_between
from tests.test_v0_51_339_bundle_staging_boot import _H
from tests.test_v0_51_342_config_bundle_followups import (  # noqa: F401 — api is the (client, cd) fixture; no_env clears every binding
    APP_JS, _SHOW, _node, _with_config, api, needs_node, no_env)

ENV = {"MOTIF_PLEX_URL": "http://env-host:32400", "MOTIF_COOKIES_FILE": "/env-mount/cookies.txt",
       "MOTIF_DEFAULT_PLACEMENT_METHOD": "api"}
LIVE = ("paths:\n  cookies_file: /live-mount/cookies.txt\nplex:\n  url: http://live-host:32400\n  token: LIVE\n"
        "placement:\n  default_method: api\n")
BUNDLE = ("paths:\n  cookies_file: /bundle-mount/yt.txt\nplex:\n  url: http://bundle-host:32400\n  token: BUNDLE\n"
          "placement:\n  default_method: file\n")
WORDS = "the file changes, the running value does not"


@pytest.fixture(params=[True, False], ids=["env set", "no env"])
def env_set(request, no_env, monkeypatch):
    """Before the api fixture builds Settings, so the live cookies path is the env's when it is set."""
    monkeypatch.setenv("MOTIF_FORWARD_AUTH_ALLOWED_IPS", "127.0.0.1")  # no_env cleared the allowlist conftest sets — forward-auth is fail-closed
    if request.param:
        for name, value in ENV.items():
            monkeypatch.setenv(name, value)
    return request.param


def _preview(client, cd, tmp_path) -> dict:
    (cd / "motif.yaml").write_text(LIVE)
    b = _with_config(tmp_path / "mk", BUNDLE)
    shutil.copyfile(b, cd / "backups" / b.name)
    r = client.post("/api/admin/database-restore", json={"name": b.name}, headers=_H)
    assert r.status_code == 200, r.text
    return r.json()["preview"]


def test_the_preview_marks_exactly_the_rows_env_overrides_and_only_there_the_running_value_stays(env_set, api, tmp_path):
    client, cd = api
    pv = _preview(client, cd, tmp_path)
    present = cf.env_overrides_present()
    bound = {path for name, path, _conv in cf.ENV_BINDINGS if name in ENV}
    assert (bound <= set(present)) is env_set, "premise: the three bindings are present exactly when the env is set"
    assert pv["env_overrides"] == present, "the payload carries the settings page's badge rule"
    rows = {d["key"]: d for d in pv["config_diff"]}
    assert set(rows) == bound | {"plex.token"}, "premise: four keys differ in the file, three of them env-bound"
    live_cfg, bundle_cfg = cf.load_config_text(LIVE), cf.load_config_text(BUNDLE)  # the boot's own pipeline, env applied
    for key, d in rows.items():
        marked = "env_override" in d
        assert marked is (key in present), (key, d)
        running_same = cf._get_dotted(live_cfg, key) == cf._get_dotted(bundle_cfg, key)
        assert marked is running_same, (key, d, "a marked row is one whose running value the swap cannot change")
        if marked:
            assert d["env_override"] == present[key] and d["env_override"] in ENV, d
        else:
            assert set(d) == {"key", "secret", "live", "bundle"}, ("an unmarked row keeps its shape", d)
    if env_set:
        assert pv["cookies_target"] == pv["cookies_live"] == str(Path(ENV["MOTIF_COOKIES_FILE"])), \
            "the cookies verdict applies env — and the paths.cookies_file row above now says so too"
    else:
        assert pv["cookies_target"] == str(Path("/bundle-mount/yt.txt")) and pv["env_overrides"].get("paths.cookies_file") is None


_CARD = r"""
const vm = require('vm');
const P = JSON.parse(require('fs').readFileSync(0, 'utf8'));
const els = {};
const el = (id) => (els[id] = els[id] || { id, textContent: '', innerHTML: '', hidden: true, checked: false, disabled: false,
  dataset: {}, addEventListener() {}, scrollIntoView() {} });
const ctx = vm.createContext({ document: { getElementById: el }, htmlEscape: (s) => String(s), previewEl: el('database-restore-preview') });
vm.runInContext(P.show, ctx);
vm.runInContext('showBundlePreview', ctx)({ preview: P.pv });
process.stdout.write(JSON.stringify({ config: els['restore-preview-config'].textContent,
  diff: els['restore-preview-diff'].innerHTML, hidden: els['restore-preview-diff'].hidden }));
"""


@needs_node
def test_the_card_badges_exactly_the_marked_rows_and_says_the_running_value_does_not_change(env_set, api, tmp_path):
    client, cd = api
    pv = _preview(client, cd, tmp_path)
    marked = {d["key"] for d in pv["config_diff"] if "env_override" in d}
    assert bool(marked) is env_set, "premise: rows are marked exactly when the env is set"
    card = _node(_CARD, {"show": slice_between(APP_JS, *_SHOW), "pv": pv})
    blocks = card["diff"].split('<div class="restore-diff-row">')[1:]
    assert len(blocks) == len(pv["config_diff"]) and card["hidden"] is False
    for d, block in zip(pv["config_diff"], blocks):
        assert block.startswith(f'<div class="restore-diff-key">{d["key"]}'), block
        badged = "// ENV OVERRIDE" in block
        assert badged is (d["key"] in marked), (d["key"], block)
        if badged:
            assert 'class="form-env-badge"' in block and d["env_override"] in block and WORDS in block, block
    assert card["config"].startswith(f"config: {len(pv['config_diff'])} keys differ from the live motif.yaml"), card["config"]
    assert (WORDS in card["config"]) is bool(marked), card["config"]
    if marked:
        assert f"({len(marked)} under an env override — {WORDS})" in card["config"], card["config"]
