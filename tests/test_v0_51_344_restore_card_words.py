"""v0.51.344: the restore card and the stage answer say which members a bundle restore leaves live, and a manifest stamp nothing can read is shown as unknown."""
from __future__ import annotations

import io
import json
import shutil
import tarfile
from pathlib import Path

import pytest

from app.core import bundle, events
from app.core.db import CURRENT_SCHEMA_VERSION, init_db
from tests._slice_helpers import slice_between
from tests.test_v0_51_339_bundle_staging_boot import _H, _bundle
from tests.test_v0_51_342_config_bundle_followups import (  # noqa: F401 — api is the (client, cd) fixture
    APP_JS, STAMP, _CARD_HARNESS, _HIDE, _PENDING, _SHOW, _STAGE, _left_out_bundle, _node, _over_cap, api, needs_node)


def _database_only(root: Path) -> Path:
    src = root / "src"
    src.mkdir(parents=True, exist_ok=True)
    init_db(src / "motif.db")
    bf = bundle.create_bundle(src / "motif.db", src, config_file=None, cookies_file=None, themes_dir=None,
                              now_stamp=STAMP, motif_version="t", schema_version=CURRENT_SCHEMA_VERSION)
    return src / "backups" / bf.name


def _case(case: str, tmp_path: Path, monkeypatch) -> Path:
    if case == "clean":
        return _bundle(tmp_path / "mk")
    if case == "no cookies":
        return _bundle(tmp_path / "mk", cookies=False)
    if case == "database only":
        return _database_only(tmp_path / "mk")
    if case == "cookies over cap":
        return _over_cap(tmp_path, monkeypatch, bundle.MEMBER_COOKIES)[0]
    return _left_out_bundle(tmp_path / "mk", monkeypatch, bundle.MEMBER_CONFIG if case.startswith("config") else bundle.MEMBER_COOKIES)


def _listed_copy(cd: Path, b: Path) -> str:
    shutil.copyfile(b, cd / "backups" / b.name)
    return b.name


# ── PB-055 / PB-057: the confirm and the cookies line ────────────────

@needs_node
@pytest.mark.parametrize("case", ["clean", "no cookies", "database only", "cookies over cap", "config left out", "cookies left out"])
def test_the_confirm_names_exactly_what_stages_and_the_cookies_line_names_the_file(api, tmp_path, monkeypatch, case):
    client, cd = api
    name = _listed_copy(cd, _case(case, tmp_path, monkeypatch))
    pv = client.post("/api/admin/database-restore", json={"name": name}, headers=_H).json()["preview"]
    assert pv["cookies_target"], "the premise: the server names the cookies file on every branch"
    slices = {"show": slice_between(APP_JS, *_SHOW), "hide": slice_between(APP_JS, *_HIDE),
              "stage": slice_between(APP_JS, *_STAGE), "pending": slice_between(APP_JS, *_PENDING)}
    card = _node(_CARD_HARNESS, {**slices, "pv": pv, "members": []})
    offered = 0
    for run in card["runs"]:
        assert pv["cookies_target"] in run["cookies"], (case, run["cookies"])
        if not run["offered"]:
            continue
        r = client.post("/api/admin/database-restore", json={"name": name, "confirm": True, "keep_config": run["keep"]}, headers=_H)
        assert r.status_code == 200, r.text
        members = r.json()["members"]
        offered += 1
        if run["keep"]:
            assert "stay as they are" in run["confirm"] and members == ["database"], (case, run["confirm"], members)
            continue
        head = run["confirm"].split(" with the bundle's")[0].split(" (")[0]
        assert ("motif.yaml" in head) is ("config" in members), (case, run["confirm"], members)
        assert ("cookies.txt" in head) is ("cookies" in members), (case, run["confirm"], members)
        assert ("your motif.yaml stays as it is" in run["confirm"]) is ("config" not in members), (case, run["confirm"])
        assert ("your cookies file stays as it is" in run["confirm"]) is ("cookies" not in members), (case, run["confirm"])
    assert offered == 2, case


# ── PB-056: the stage answer and its event ───────────────────────────

@pytest.mark.parametrize("case", ["clean", "cookies over cap", "cookies left out", "config left out"])
def test_the_stage_answer_and_its_event_say_what_the_restore_left_as_it_is(api, tmp_path, monkeypatch, case):
    client, cd = api
    from app.web import api as api_mod
    seen: list[dict] = []
    monkeypatch.setattr(api_mod, "log_event", lambda *a, **k: seen.append(k))
    b = _case(case, tmp_path, monkeypatch)
    left = bundle.preview(b, None)["left_out"]  # the preview's own words for each member this bundle cannot restore
    name = _listed_copy(cd, b)
    r = client.post("/api/admin/database-restore", json={"name": name, "confirm": True, "keep_config": False}, headers=_H)
    assert r.status_code == 200, r.text
    body = r.json()
    ev = next(k for k in seen if "staged" in (k.get("message") or ""))
    assert [d["member"] for d in body["left_as_is"]] == list(left) and ev["detail"]["left_as_is"] == body["left_as_is"], (body, ev)
    word = {bundle.MEMBER_CONFIG: "config", bundle.MEMBER_COOKIES: "cookies"}
    assert not [m for m in left if word[m] in body["members"]], "a member left as it is never stages"
    stored = events._scrub(ev["detail"])
    assert stored["left_as_is"] == body["left_as_is"], f"the events scrubber must keep the words: {stored}"
    for d in body["left_as_is"]:
        assert d["why"].startswith(left[d["member"]]) and "left as it is" in d["why"], d
        assert d["why"] in body["message"] and d["why"] in ev["message"], (d, body["message"], ev["message"])
    head = f"Bundle restore staged from {name} ({' + '.join(body['members'])}; schema v{body['schema_version']}); applies on restart"
    said = "".join(f" {d['why']}." for d in body["left_as_is"])
    assert bool(said) is (case != "clean"), (case, body["left_as_is"])  # v0.51.344: the premise — only the clean bundle stages every member
    assert ev["message"] == (head + "." + said if said else head), (case, ev["message"])  # v0.51.344: a sentence break before the words, and a clean restore's message byte-identical


# ── PB-072: a manifest stamp nothing can read ────────────────────────

_MISSING = object()
_READABLE = "2026-09-14T04:00:01+00:00"

_MANIFEST_LINE = r"""
const vm = require('vm');
const P = JSON.parse(require('fs').readFileSync(0, 'utf8'));
const els = {};
const el = (id) => (els[id] = els[id] || { id, textContent: '', innerHTML: '', hidden: true, checked: false, disabled: false,
  dataset: {}, addEventListener() {}, scrollIntoView() {} });
const ctx = vm.createContext({ document: { getElementById: el }, htmlEscape: (s) => String(s), previewEl: el('database-restore-preview') });
vm.runInContext(P.show, ctx);
const out = [];
for (const pv of P.previews) { vm.runInContext('showBundlePreview', ctx)({ preview: pv }); out.push(els['restore-preview-manifest'].textContent); }
process.stdout.write(JSON.stringify(out));
"""


def _restamped(b: Path, out: Path, created) -> bytes:
    with tarfile.open(b, "r:gz") as t:
        members = {m.name: t.extractfile(m).read() for m in t.getmembers()}
    manifest = json.loads(members[bundle.MEMBER_MANIFEST])
    if created is _MISSING:
        del manifest["created_at"]
    else:
        manifest["created_at"] = created
    members[bundle.MEMBER_MANIFEST] = json.dumps(manifest).encode()
    with tarfile.open(out, "w:gz") as t:
        for name, data in members.items():
            ti = tarfile.TarInfo(name)
            ti.size = len(data)
            t.addfile(ti, io.BytesIO(data))
    return out.read_bytes()


@needs_node
def test_a_manifest_stamp_nothing_can_read_is_shown_as_unknown_and_never_refuses_the_bundle(api, tmp_path):
    client, cd = api
    b = _bundle(tmp_path / "mk")
    shapes = {"missing": _MISSING, "a number": 12345, "a list": ["x"], "empty": "", "not a date": "not a date",
              "full-width digits": _READABLE.translate({ord(c): ord(c) + 0xFEE0 for c in "0123456789"}), "a real stamp": _READABLE}
    previews = []
    for i, (label, created) in enumerate(shapes.items()):
        r = client.post("/api/admin/database-restore/upload", headers=_H,
                        files={"file": ("x.tar.gz", _restamped(b, tmp_path / f"up{i}.tar.gz", created), "application/gzip")})
        assert r.status_code == 200, (label, r.text)
        pv = r.json()["preview"]
        assert pv["manifest"]["created_at"] == (created if label == "a real stamp" else None), (label, pv["manifest"])
        staged = client.post("/api/admin/database-restore", json={"name": pv["name"], "confirm": True, "keep_config": False}, headers=_H)
        assert staged.status_code == 200, (label, staged.text)
        previews.append(pv)
    lines = _node(_MANIFEST_LINE, {"show": slice_between(APP_JS, *_SHOW), "previews": previews})
    for pv, line in zip(previews, lines):
        stamp = pv["manifest"]["created_at"]
        assert ("date unknown" in line) is (stamp is None) and (stamp is None or stamp in line), line
