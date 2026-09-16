"""v0.51.343: an uploaded bundle is filed under its upload's UTC time, outside retention, and the backup list says so."""
from __future__ import annotations

import io
import json
import os
import re
import shutil
import subprocess
import tarfile
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from app.core import bundle, db_backup
from app.core.auth import create_admin, init_auth_schema
from app.core.db import CURRENT_SCHEMA_VERSION, init_db
from tests.test_v0_51_339_bundle_staging_boot import _H, LIVE_YAML, _bundle

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
_NODE = shutil.which("node")
_UPLOAD = re.compile(r"motif-bundle-upload-([0-9]{8}-[0-9]{6})(?:-[0-9]+)?\.tar\.gz")  # v0.51.344: a same-second upload's -N

if os.environ.get("MOTIF_REQUIRE_NODE") and not _NODE:
    raise RuntimeError("MOTIF_REQUIRE_NODE=1 but node is not on PATH — the backup list render would silently not run")


def _stamp(dt: datetime | None = None) -> str:
    return (dt or datetime.now(timezone.utc)).strftime("%Y%m%d-%H%M%S")


def _created_at(which: str) -> str:
    now = datetime.now(timezone.utc)
    if which == "future":
        return (now + timedelta(days=3650)).isoformat(timespec="seconds")
    if which == "full-width":
        return now.isoformat(timespec="seconds").translate({ord(c): ord(c) + 0xFEE0 for c in "0123456789"})
    return (now - timedelta(days=5 * 365)).isoformat(timespec="seconds")


def _with_created_at(b: Path, out: Path, created_at: str) -> bytes:
    """Re-pack a real bundle with only manifest.json's created_at changed (the manifest is outside the checksums)."""
    members = {}
    with tarfile.open(b, "r:gz") as t:
        for m in t.getmembers():
            members[m.name] = t.extractfile(m).read()
    j = json.loads(members[bundle.MEMBER_MANIFEST])
    j["created_at"] = created_at
    members[bundle.MEMBER_MANIFEST] = json.dumps(j).encode()
    with tarfile.open(out, "w:gz") as t:
        for name, data in members.items():
            ti = tarfile.TarInfo(name)
            ti.size = len(data)
            t.addfile(ti, io.BytesIO(data))
    return out.read_bytes()


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
    return TestClient(api_mod.create_app(settings), raise_server_exceptions=False), cd, settings


def _listed(client) -> dict[str, dict]:
    r = client.get("/api/admin/database-backups", headers=_H)
    assert r.status_code == 200, r.text
    return {row["name"]: row for row in r.json()["backups"]}


# ── 1. filed by its upload, outside retention ────────────────────────

@pytest.mark.parametrize("created", ["future", "full-width", "years-old"])
def test_an_upload_is_filed_by_its_upload_time_and_outlives_a_nightly_at_retention_1(api, tmp_path, monkeypatch, created):
    client, cd, settings = api
    manifest_created_at = _created_at(created)
    data = _with_created_at(_bundle(tmp_path / "mk"), tmp_path / "up.tar.gz", manifest_created_at)
    older = db_backup.create_backup(settings.db_path, cd, now_stamp=_stamp(datetime.now(timezone.utc) - timedelta(days=2)))
    before = _stamp()
    r = client.post("/api/admin/database-restore/upload", headers=_H, files={"file": ("off-box.tar.gz", data, "application/gzip")})
    after = _stamp()
    assert r.status_code == 200, r.text
    pv = r.json()["preview"]
    name = pv["name"]
    m = _UPLOAD.fullmatch(name)
    assert m and before <= m.group(1) <= after, (name, before, after)
    # v0.51.344: a full-width stamp is no date datetime reads — the preview shows it as unknown (test_v0_51_344_restore_card_words)
    assert pv["manifest"]["created_at"] == (None if created == "full-width" else manifest_created_at), "the preview still shows the manifest's own stamp"
    rows = _listed(client)
    assert (rows[name]["kind"], rows[name]["retained"]) == ("bundle", False)
    assert rows[older.name]["retained"] is True

    from app.core import scheduler as sched
    said: list[str] = []
    monkeypatch.setattr(sched, "log_event", lambda *a, **k: said.append(k.get("message") or ""))
    sched._scheduled_database_backup(SimpleNamespace(
        db_path=settings.db_path, config_dir=cd, cookies_file=None, themes_dir=None,
        config_file=SimpleNamespace(path=cd / "motif.yaml"),  # v0.51.344: create_bundle_for reads the loaded config file's path
        db_backup_enabled=True, db_backup_retention=1, db_backup_bundle=True))
    rows = _listed(client)
    made = [n for n, row in rows.items() if row["retained"]]
    assert len(made) == 1 and db_backup.kind_of(made[0]) == "bundle", rows
    assert set(rows) == {name, made[0]}, "retention 1 keeps the nightly's own bundle and the upload; only the older snapshot goes"
    assert len(said) == 1 and made[0] in said[0], said

    r = client.post("/api/admin/database-restore", json={"name": name}, headers=_H)
    assert r.status_code == 200 and r.json()["staged"] is False and r.json()["preview"]["name"] == name, r.text
    r = client.post("/api/admin/database-restore", json={"name": name, "confirm": True, "keep_config": False}, headers=_H)
    assert r.status_code == 200 and r.json()["members"] == ["database", "config", "cookies"], r.text
    r = client.get(f"/api/admin/database-backup/download/{name}", headers=_H)
    assert r.status_code == 200 and r.headers["content-type"].startswith("application/gzip") and r.content == data
    assert client.post("/api/admin/database-backup/delete", json={"name": name}, headers=_H).status_code == 200
    assert name not in _listed(client)


# ── 2. an upload into an occupied second ─────────────────────────────

@pytest.mark.parametrize("already", ["equal bytes", "different bytes"])
def test_an_upload_into_an_occupied_second_takes_its_own_name_and_leaves_the_held_file(api, tmp_path, already):
    # v0.51.344: retargeted from "dedups equal bytes, 409s different ones" — a same-second upload takes the next free name (decision e)
    client, cd, _ = api
    data = _bundle(tmp_path / "mk").read_bytes()
    held = data if already == "equal bytes" else bytes(len(data))
    bdir = cd / "backups"
    bdir.mkdir()
    base = datetime.now(timezone.utc)
    # v0.51.343: every second the upload can land in already holds a file under the upload's name
    window = {f"motif-bundle-upload-{_stamp(base + timedelta(seconds=s))}.tar.gz" for s in range(-1, 30)}
    for n in window:
        (bdir / n).write_bytes(held)
    r = client.post("/api/admin/database-restore/upload", headers=_H, files={"file": ("x.tar.gz", data, "application/gzip")})
    assert r.status_code == 200, r.text
    name = r.json()["preview"]["name"]
    assert name not in window and _UPLOAD.fullmatch(name) and (bdir / name).read_bytes() == data, name
    assert _listed(client)[name]["retained"] is False
    assert {p.name for p in bdir.iterdir()} == window | {name}, "one new file — no temp file or empty claim left behind"
    assert all((bdir / n).read_bytes() == held for n in window)


# ── 3. the settings list, rendered by the live binder under node ─────

_DRIVER = r"""
"use strict";
const fs = require("node:fs");
const vm = require("node:vm");
const [, , srcPath, scenarioPath] = process.argv;
const scenario = JSON.parse(fs.readFileSync(scenarioPath, "utf8"));
const els = new Map();
const document = {
  getElementById(id) {
    if (!els.has(id)) {
      els.set(id, { id, innerHTML: "", textContent: "", className: "", hidden: false, disabled: false,
                    dataset: {}, style: {}, classList: { add() {} }, addEventListener() {} });
    }
    return els.get(id);
  },
};
const unexpected = [];
async function api(method, url) {
  const key = `${method} ${url}`;
  if (key in scenario.responses) return scenario.responses[key];
  unexpected.push(key);
  throw new Error(`unexpected api call ${key}`);
}
const ctx = vm.createContext({ document, api, console, confirm: () => false, alert: () => {}, FormData: class {} });
vm.runInContext(fs.readFileSync(srcPath, "utf8"), ctx);
const flush = () => new Promise((r) => setImmediate(r));
(async () => {
  ctx.bindDatabaseBackup();
  await flush();
  await flush();
  process.stdout.write(JSON.stringify({ html: document.getElementById("database-backup-list").innerHTML, unexpected }));
})().catch((e) => { console.error(e); process.exit(1); });
"""


def _app_fn(name: str) -> str:
    start = APP_JS.index(f"\n  function {name}(") + 1
    return APP_JS[start:APP_JS.index("\n  }\n", start)] + "\n  }\n"


class _Chips(HTMLParser):
    """(row name, chip label, chip title) for every .backup-row-name."""
    def __init__(self):
        super().__init__()
        self.rows: list[dict] = []
        self._in_row = self._in_chip = False

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if tag == "div" and "backup-row-name" in (a.get("class") or "").split():
            self._in_row = True
            self.rows.append({"name": "", "label": "", "title": None})
        elif tag == "span" and self._in_row and "tier-badge" in (a.get("class") or "").split():
            self._in_chip = True
            self.rows[-1]["title"] = a.get("title")

    def handle_endtag(self, tag):
        if tag == "span" and self._in_chip:
            self._in_chip = False
        elif tag == "div" and self._in_row:
            self._in_row = False

    def handle_data(self, data):
        if self._in_chip:
            self.rows[-1]["label"] += data
        elif self._in_row:
            self.rows[-1]["name"] += data


@pytest.mark.skipif(not _NODE, reason="node not installed")
def test_the_list_words_outside_retention_exactly_where_the_json_says_retained_false(api, tmp_path):
    client, cd, settings = api
    now = datetime.now(timezone.utc)
    made = [db_backup.create_backup(settings.db_path, cd, now_stamp=_stamp(now - timedelta(days=3))),
            db_backup.create_backup(settings.db_path, cd, now_stamp=_stamp(now - timedelta(days=2)), prerestore=True),
            bundle.create_bundle(settings.db_path, cd, config_file=cd / "motif.yaml", cookies_file=None, themes_dir=None,
                                 now_stamp=_stamp(now - timedelta(days=1)), motif_version="t",
                                 schema_version=CURRENT_SCHEMA_VERSION)]
    on_disk = {b.name: b for b in db_backup.list_backups(cd)}
    assert [on_disk[bf.name] for bf in made] == made, "what a create returns is the row the list shows, retained included"
    data = _with_created_at(_bundle(tmp_path / "mk"), tmp_path / "up.tar.gz", _created_at("years-old"))
    r = client.post("/api/admin/database-restore/upload", headers=_H, files={"file": ("x.tar.gz", data, "application/gzip")})
    assert r.status_code == 200, r.text
    listing = client.get("/api/admin/database-backups", headers=_H).json()
    assert sorted((b["kind"], b["retained"]) for b in listing["backups"]) == [
        ("bundle", False), ("bundle", True), ("prerestore", False), ("snapshot", True)]

    start = APP_JS.index("  function bindDatabaseBackup() {")
    (tmp_path / "bind.js").write_text(_app_fn("htmlEscape") + _app_fn("fmtBytes") + _app_fn("proxyStatusHint")
                                      + _app_fn("gatewayTimeoutNote") + APP_JS[start:APP_JS.index("\n  function ", start + 1)])
    (tmp_path / "scenario.json").write_text(json.dumps({"responses": {
        "GET /api/admin/database-backups": listing,
        "GET /api/admin/database-restore/pending": {"pending": False, "members": []}}}))
    (tmp_path / "driver.js").write_text(_DRIVER)
    run = subprocess.run([_NODE, str(tmp_path / "driver.js"), str(tmp_path / "bind.js"), str(tmp_path / "scenario.json")],
                         capture_output=True, text=True, timeout=60)
    assert run.returncode == 0, run.stderr[-2000:]
    out = json.loads(run.stdout)
    assert out["unexpected"] == [], out["unexpected"]
    parser = _Chips()
    parser.feed(out["html"])
    by_name = {row["name"]: row for row in parser.rows}
    assert set(by_name) == {b["name"] for b in listing["backups"]}
    for b in listing["backups"]:
        row = by_name[b["name"]]
        tip = row["title"] or ""
        assert ("outside retention" in tip) == (b["retained"] is False), (b, tip)
        assert "cookies.txt" not in tip or "cap" in tip, f"a bundle may leave cookies.txt out: {tip}"
        if b["kind"] == "bundle":
            assert row["label"] == "BUNDLE", row

    # v0.51.343: the JSON's retained is what retention does — at 1, every retained-false row and the newest retained row survive
    db_backup.prune_backups(cd, 1, now_stamp=_stamp())  # v0.51.344: prune sets aside a stamp after now
    survivors = {b.name for b in db_backup.list_backups(cd)}
    rows = listing["backups"]
    assert survivors == {b["name"] for b in rows if not b["retained"]} | {next(b["name"] for b in rows if b["retained"])}
