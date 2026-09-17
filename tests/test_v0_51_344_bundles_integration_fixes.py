"""v0.51.344: the bundles group of the integration review (R1-F4, F5, F9, F10, F14, F20, F21, F22, F24).

  1. Same-second uploads list newest first, in arrival order (F22).
  2. A retained row stamped after now is flagged in the list and the chip says retention neither counts nor deletes it (F21).
  3. With bundles wanted, fallback snapshots never rotate out the last complete bundle; the event no longer tells the operator to take the snapshot it took (F5).
  4. A .342/.343 bundle that left a member out under the complete name is renamed once, before the first prune (F4).
  5. A billion-laughs motif.yaml inside an uploaded bundle, in its sequence or merge-key form, is refused by key before it expands (F9).
  6. A disk fault at the upload's name claim or rename answers in words; every name taken is a 409 the page reads as motif's (F10).
  7. The restore card names the LIVE cookies file as the one that stays, and the post-swap path only when it differs (F14).
  8. The daily sweep removes the temps a killed create / inspect / stage / upload strands, never one in flight (F20).
  9. A negative-size header is refused in motif's words before it credits the budget; a long member name is echoed truncated (F24).
"""
from __future__ import annotations

import datetime as datetime_mod
import gzip
import inspect
import io
import json
import logging
import os
import shutil
import subprocess
import tarfile
import time
import tracemalloc
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core import bundle, db_backup, events
from app.core import scheduler as sched
from app.core.auth import create_admin, init_auth_schema
from app.core.db import init_db
from tests._slice_helpers import slice_between
from tests.test_v0_51_339_bundle_staging_boot import _H, LIVE_YAML
from tests.test_v0_51_339_bundle_staging_boot import _bundle as _mk_bundle
from tests.test_v0_51_341_config_secrets_preview import _api as _api_with_yaml
from tests.test_v0_51_341_config_secrets_preview import _bundle as _bundle_with
from tests.test_v0_51_342_bundle_tail_bound import _checksum, _within
from tests.test_v0_51_342_config_bundle_followups import (
    _CARD_HARNESS, _HIDE, _PENDING, _SHOW, _STAGE, _node, needs_node)
from tests.test_v0_51_343_backup_upload_retention import (  # noqa: F401 — api is the (client, cd, settings) fixture
    _DRIVER, _NODE, APP_JS, _app_fn, _Chips, api)
from tests.test_v0_51_344_backup_retention_names import _hint, _holds, _make, _nightly, _src, _stamp, _upload
from tests.test_v0_51_344_bundle_core_internals import _spy_fetches


def _forward(monkeypatch, secs: float) -> None:
    real = time.time
    monkeypatch.setattr(time, "time", lambda: real() + secs)


class _Clock(datetime):
    """datetime whose now() moves two seconds per call — the nightly names its file by the second, so runs never collide."""
    _ticks = 0

    @classmethod
    def now(cls, tz=None):
        _Clock._ticks += 1
        return datetime.now(tz) + timedelta(seconds=2 * _Clock._ticks)


# ── 1. same-second uploads (F22) ─────────────────────────────────────

def test_same_second_uploads_list_newest_first_in_arrival_order(tmp_path):
    bdir = tmp_path / "backups"
    bdir.mkdir()
    later = datetime.now(timezone.utc)
    earlier = later - timedelta(seconds=1)
    arrived: dict[str, list[str]] = {}
    for st in (_stamp(earlier), _stamp(later)):  # the production minter, so arrival order is ground truth
        arrived[st] = []
        for i in range(1, 11):
            src = bdir / f".restore-upload.{st}.{i}"
            src.write_bytes(bytes([i]))
            arrived[st].append(bundle.file_upload(src, bdir, st).name)
    first, second, tenth = arrived[_stamp(later)][0], arrived[_stamp(later)][1], arrived[_stamp(later)][9]
    assert first.endswith(f"{_stamp(later)}.tar.gz") and second.endswith("-2.tar.gz") and tenth.endswith("-10.tar.gz")
    listed = [b.name for b in db_backup.list_backups(tmp_path)]
    assert listed == arrived[_stamp(later)][::-1] + arrived[_stamp(earlier)][::-1], listed
    assert (bdir / listed[0]).read_bytes() == bytes([10]) and (bdir / listed[-1]).read_bytes() == bytes([1])


# ── 2. a row stamped after now (F21) ─────────────────────────────────

@pytest.mark.skipif(not _NODE, reason="node not installed")
def test_a_retained_row_stamped_after_now_is_flagged_and_the_chip_says_retention_neither_counts_nor_deletes_it(api, tmp_path):
    client, cd, settings = api
    bdir = cd / "backups"
    bdir.mkdir()
    now = datetime.now(timezone.utc)
    ahead = now + timedelta(days=30)
    shutil.copyfile(_mk_bundle(tmp_path / "mk"), bdir / f"motif-bundle-{_stamp(ahead)}.tar.gz")  # .336-.342 filed an upload under its manifest's stamp
    db_backup.create_backup(settings.db_path, cd, now_stamp=_stamp(ahead + timedelta(hours=1)))
    db_backup.create_backup(settings.db_path, cd, now_stamp=_stamp(ahead), prerestore=True)
    db_backup.create_backup(settings.db_path, cd, now_stamp=_stamp(now - timedelta(days=1)))
    oldest = db_backup.create_backup(settings.db_path, cd, now_stamp=_stamp(now - timedelta(days=2)))
    listing = client.get("/api/admin/database-backups", headers=_H).json()
    rows = {b["name"]: b for b in listing["backups"]}
    for name, b in rows.items():
        assert b["future"] is (b["retained"] and b["created_at"] > now.isoformat(timespec="seconds")), (name, b)
    assert sum(b["future"] for b in rows.values()) == 2, rows
    assert db_backup.prune_backups(cd, 1, now_stamp=_stamp(now)) == [oldest.name], "prune sets aside exactly the flagged rows"
    assert all((bdir / n).exists() for n in rows if n != oldest.name)

    bind = slice_between(APP_JS, "  function bindDatabaseBackup() {", "\n  function ", start_offset=1)
    (tmp_path / "bind.js").write_text(_app_fn("htmlEscape") + _app_fn("fmtBytes") + _app_fn("proxyStatusHint")
                                      + _app_fn("gatewayTimeoutNote") + bind)
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
    tips = {row["name"]: row["title"] or "" for row in parser.rows}
    assert set(tips) == set(rows)
    for name, b in rows.items():
        assert ("retention neither counts nor deletes it" in tips[name]) is b["future"], (name, tips[name])


# ── 3. fallback snapshots and the complete bundle (F5) ───────────────

@pytest.mark.parametrize("bundle_mode", [True, False], ids=["bundles wanted", "snapshot-only"])
def test_fallback_snapshots_keep_the_last_complete_bundle_only_while_bundles_are_wanted(tmp_path, monkeypatch, caplog, bundle_mode):
    cd = _src(tmp_path / "cfg")
    now = datetime.now(timezone.utc)
    complete = _make(tmp_path / "c1", cd, _stamp(now - timedelta(days=5)), monkeypatch)
    assert _holds(cd / "backups" / complete.name, bundle.MEMBER_COOKIES)
    monkeypatch.setattr(bundle, "_MEMBER_CAP", {**bundle._MEMBER_CAP, bundle.MEMBER_DB: 16})  # every nightly falls back to a snapshot
    monkeypatch.setattr(datetime_mod, "datetime", _Clock)
    said: list[tuple[str, str]] = []
    with caplog.at_level(logging.INFO, logger=db_backup.log.name):
        for _ in range(3):
            said += _nightly(cd, 2, monkeypatch, bundle_mode=bundle_mode)
    rows = db_backup.list_backups(cd)
    bundles = [b.name for b in rows if b.kind == "bundle"]
    assert len([b for b in rows if b.kind == "snapshot"]) == 2, rows
    if bundle_mode:
        assert bundles == [complete.name], "the only complete bundle outlives a window of fallback snapshots"
        assert sum(complete.name in r.getMessage() and "kept" in r.getMessage() for r in caplog.records) == 2, caplog.text
        warnings = [m for level, m in said if level == "WARNING"]
        assert len(warnings) == 3 and all(
            m.startswith("Scheduled backup bundle not written (") and m.endswith(") — a plain database snapshot was taken instead")
            and "take a plain snapshot instead" not in m and "no bundle was written" not in m for m in warnings), warnings
    else:
        assert bundles == [], "a snapshot-only schedule keeps its .335 rule: bundles rotate out with the rest"


def test_the_hints_say_a_fallback_snapshot_can_stand_in_and_when_the_complete_bundle_is_kept():
    bundle_hint, retention_hint = _hint("database_backup.bundle"), _hint("database_backup.retention")
    assert ("writes a plain snapshot instead" in bundle_hint) is hasattr(bundle, "BundleOverCap"), bundle_hint
    protects = "protect_complete_bundle" in inspect.signature(db_backup.prune_backups).parameters
    assert ("fallback snapshot" in retention_hint and "WRITE A BUNDLE is on" in retention_hint) is protects, retention_hint
    assert "a bundle uploaded on an earlier version is filed under its own stamp and counts" in retention_hint, retention_hint


# ── 4. pre-.344 bundles that left a member out (F4) ──────────────────

def _legacy_named(cd: Path, bf: db_backup.BackupFile) -> Path:
    """What .342/.343 wrote: the same bytes under the complete name."""
    new = cd / "backups" / bundle.bundle_name(db_backup._stamp_of(bf.name))
    os.rename(cd / "backups" / bf.name, new)
    return new


def test_pre_344_bundles_that_left_a_member_out_are_renamed_once_by_the_nightly_and_its_first_prune_keeps_the_complete_one(tmp_path, monkeypatch, caplog):
    cd = _src(tmp_path / "cfg")
    bdir = cd / "backups"
    now = datetime.now(timezone.utc)
    foreign = bdir / f"motif-bundle-{_stamp(now - timedelta(days=12))}.tar.gz"  # a .336-.342 upload under its manifest's stamp: complete
    bdir.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(_mk_bundle(tmp_path / "mk"), foreign)
    complete = _make(tmp_path / "c1", cd, _stamp(now - timedelta(days=10)), monkeypatch)
    legacy = [_legacy_named(cd, _make(tmp_path / f"l{i}", cd, _stamp(now - timedelta(days=8 - i)), monkeypatch, cookies_cap=63)) for i in range(2)]
    assert all(db_backup._classify(p.name)[3] is False and not _holds(p, bundle.MEMBER_COOKIES) for p in legacy), "the premise: the name says complete"
    assert bundle.bundle_left_out(foreign) == [] and all(bundle.bundle_left_out(p) == [bundle.MEMBER_COOKIES] for p in legacy)
    expected = [bundle.bundle_name(db_backup._stamp_of(p.name), partial=True) for p in legacy]
    monkeypatch.setattr(bundle, "_MEMBER_CAP", {**bundle._MEMBER_CAP, bundle.MEMBER_COOKIES: 63})
    monkeypatch.setattr(datetime_mod, "datetime", _Clock)
    with caplog.at_level(logging.INFO):
        _nightly(cd, 2, monkeypatch)  # the first .344 nightly: the naming pass, then its own bundle, then the prune
    renames = [r.getMessage() for r in caplog.records if r.name == bundle.log.name and "renamed" in r.getMessage()]
    assert len(renames) == 2 and not any(foreign.name in m for m in renames), renames
    assert all(any(p.name in m and n in m and "cookies.txt" in m for m in renames) for p, n in zip(legacy, expected)), "each rename logged"
    assert all(not p.exists() for p in legacy) and (bdir / bundle.PARTIAL_NAMES_MARKER).exists()
    rows = {b.name: b for b in db_backup.list_backups(cd)}
    held = [n for n in rows if _holds(bdir / n, bundle.MEMBER_COOKIES)]
    assert held == [complete.name], (rows, "the first .344 prune keeps the bundle holding cookies.txt")
    assert len(rows) == 3 and rows[expected[1]].partial and not rows[complete.name].partial, rows
    assert expected[0] not in rows and foreign.name not in rows, "retention 2: the older renamed partial and the oldest complete bundle go"
    assert any(complete.name in r.getMessage() and "kept" in r.getMessage() for r in caplog.records), caplog.text

    reads: list[Path] = []
    monkeypatch.setattr(bundle, "bundle_left_out", lambda p: reads.append(p))
    _nightly(cd, 2, monkeypatch)
    assert reads == [], "the marker: no manifest is read again"
    assert [n for n in db_backup.list_backups(cd) if _holds(bdir / n.name, bundle.MEMBER_COOKIES)][0].name == complete.name


def test_the_naming_pass_leaves_unreadable_and_complete_bundles_alone_and_retries_only_a_failed_rename(tmp_path, monkeypatch, caplog):
    cd = _src(tmp_path / "cfg")
    bdir = cd / "backups"
    bdir.mkdir()
    now = datetime.now(timezone.utc)
    junk = bdir / f"motif-bundle-{_stamp(now - timedelta(days=1))}.tar.gz"
    junk.write_bytes(b"not a tar at all")
    complete = _make(tmp_path / "c1", cd, _stamp(now - timedelta(days=2)), monkeypatch)
    marker = bdir / bundle.PARTIAL_NAMES_MARKER
    with caplog.at_level(logging.WARNING, logger=bundle.log.name):
        assert bundle.rename_legacy_partials(cd) == []
    assert junk.exists() and (bdir / complete.name).exists() and marker.exists()
    assert any(junk.name in r.getMessage() and "left as it is" in r.getMessage() for r in caplog.records), caplog.text
    reads: list[Path] = []
    monkeypatch.setattr(bundle, "bundle_left_out", lambda p: reads.append(p))
    assert bundle.rename_legacy_partials(cd) == [] and reads == [], "once per install"
    monkeypatch.undo()

    marker.unlink()
    legacy = _legacy_named(cd, _make(tmp_path / "l1", cd, _stamp(now - timedelta(days=3)), monkeypatch, cookies_cap=63))
    with monkeypatch.context() as m:
        m.setattr(os, "rename", lambda *a: (_ for _ in ()).throw(PermissionError(13, "Permission denied")))
        assert bundle.rename_legacy_partials(cd) == []
    assert legacy.exists() and not marker.exists(), "a failed rename leaves no marker, so the next run tries again"
    assert bundle.rename_legacy_partials(cd) == [bundle.bundle_name(db_backup._stamp_of(legacy.name), partial=True)]
    assert marker.exists() and not legacy.exists()


# ── 5. a billion-laughs motif.yaml (F9) ──────────────────────────────

def _laughs(levels: int, fan: int) -> str:
    lines = [f"l0: &l0 [{', '.join(['a'] * fan)}]"] + [f"l{i}: &l{i} [{', '.join([f'*l{i - 1}'] * fan)}]" for i in range(1, levels)]
    return "\n".join(lines) + "\n"


def _merge_laughs(levels: int, fan: int) -> str:
    # v0.51.344: R1-F9 — the merge-key form: the safe constructor splices each `<<` alias's pairs in, so the walk must judge the graph before it constructs
    lines = ["l0: &l0 {a: 1}"] + [f"l{i}: &l{i} {{<<: [{', '.join([f'*l{i - 1}'] * fan)}]}}" for i in range(1, levels)]
    return "\n".join(lines) + "\n"


LAUGHS = {
    "a sequence alias": (lambda: _laughs(12, 2), "l1 is a YAML alias of a list or mapping, which motif never writes"),  # 2^12 leaves from twelve short lines
    "a merge-key alias": (lambda: _merge_laughs(6, 10), "l1.<< is a YAML alias of a list or mapping, which motif never writes"),  # 10^5 pairs from six
}


@pytest.mark.parametrize("form", list(LAUGHS))
def test_a_billion_laughs_motif_yaml_in_an_uploaded_bundle_is_refused_by_key_before_it_expands(api, tmp_path, form):
    client, cd, _ = api
    make, words = LAUGHS[form]
    text = make()
    assert len(text) < 512
    data = _bundle_with(tmp_path / "mk", text).read_bytes()
    tracemalloc.start()
    try:
        tracemalloc.reset_peak()
        held = tracemalloc.get_traced_memory()[0]
        with _within(2):
            r = _upload(client, data)
        peak = tracemalloc.get_traced_memory()[1] - held
    finally:
        tracemalloc.stop()
    assert r.status_code == 200, r.text
    pv = r.json()["preview"]
    why = pv["config_parse_error"]["bundle"]
    assert why == words, why
    assert pv["config_diff"] == [] and pv["cookies_target"] is None, pv
    assert len(r.content) < 16 << 10 and peak < 32 << 20, (len(r.content), peak)
    r = client.post("/api/admin/database-restore", json={"name": pv["name"], "confirm": True, "keep_config": False}, headers=_H)
    assert r.status_code == 422 and why in r.json()["detail"] and "KEEP MY CURRENT CONFIG" in r.json()["detail"], r.text
    r = client.post("/api/admin/database-restore", json={"name": pv["name"], "confirm": True, "keep_config": True}, headers=_H)
    assert r.status_code == 200 and r.json()["members"] == ["database"], r.text
    assert bundle.cancel_pending(cd / "motif.db", cd)


def test_a_scalar_alias_still_loads_and_a_collection_alias_is_named_by_key_on_either_side():
    flat, err = bundle.flatten_config("a: &x 1\nb: *x\nplex:\n  url: http://plex:32400\n", side="bundle")
    assert err is None and flat["a"] == flat["b"] == 1, (flat, err)
    flat, err = bundle.flatten_config("base: &b {url: http://plex:32400}\nplex: *b\n", side="live")
    assert flat == {} and err == "plex is a YAML alias of a list or mapping, which motif never writes", (flat, err)
    _, err = bundle.flatten_config("plex:\n  url: &u http://plex:32400\n  sections: &s [1, 2]\n  other: *s\n", side="bundle")
    assert err == "plex.other is a YAML alias of a list or mapping, which motif never writes", err
    flat, err = bundle.flatten_config("base: &b {url: http://plex:32400}\nplex: {<<: *b}\n", side="bundle")  # v0.51.344: R1-F9 — the merge key, judged before the constructor flattens it away
    assert flat == {} and err == "plex.<< is a YAML alias of a list or mapping, which motif never writes", (flat, err)
    assert bundle.config_diff("base: &b [1]\nx: *b\n", "plex:\n  url: http://plex:32400\n") == [], "no diff when the live side is refused"


# ── 6. the upload's name claim and rename (F10) ──────────────────────

_UPLOAD_CATCH = r"""
const vm = require('vm');
const P = JSON.parse(require('fs').readFileSync(0, 'utf8'));
const out = [];
for (const e of P.errors) {
  const calls = [];
  const ctx = vm.createContext({ e, refreshPending: () => calls.push('refreshPending'), restoreStatus: { textContent: '', className: '' } });
  vm.runInContext(P.fns + P.catchBody, ctx);
  out.push({ shown: ctx.restoreStatus.textContent, refreshed: calls });
}
process.stdout.write(JSON.stringify(out));
"""


@needs_node
def test_when_every_name_for_the_second_is_taken_the_answer_is_one_the_page_reads_as_motifs_own(api, tmp_path):
    client, cd, _ = api
    bdir = cd / "backups"
    bdir.mkdir()
    base = datetime.now(timezone.utc)
    for s in range(-1, 30):
        for n in range(1, 100):
            (bdir / bundle.uploaded_bundle_name(_stamp(base + timedelta(seconds=s)), n)).touch()
    r = _upload(client, _mk_bundle(tmp_path / "mk").read_bytes())
    assert r.status_code == 409 and "try the upload again" in r.json()["detail"], r.text
    assert not 502 <= r.status_code <= 504, "the page reads any 502-504 as the reverse proxy timing out"
    catch_body = slice_between(APP_JS, "            let msg;\n            const gw = gatewayTimeoutNote(e);",
                               "\n            restoreStatus.className = 'form-status form-status-fail';")
    [shown] = _node(_UPLOAD_CATCH, {"fns": _app_fn("proxyStatusHint") + _app_fn("gatewayTimeoutNote"), "catchBody": catch_body,
                                    "errors": [{"status": r.status_code, "detail": r.json()["detail"]}]})
    assert shown == {"shown": "✗ " + r.json()["detail"], "refreshed": []}, shown


# ── 7. the cookies line names the live file (F14) ────────────────────

@needs_node
@pytest.mark.parametrize("case", ["no cookies member", "cookies over cap", "same path"])
def test_the_cookies_line_names_the_live_file_that_stays_and_the_post_swap_path_only_when_it_differs(tmp_path, monkeypatch, case):
    monkeypatch.delenv("MOTIF_COOKIES_FILE", raising=False)
    live_ck = tmp_path / "live-mount" / "cookies.txt"
    live_ck.parent.mkdir()
    live_ck.write_text("# live cookies\n")
    other = tmp_path / "other-install" / "yt-cookies.txt"
    client, settings = _api_with_yaml(tmp_path / "cfg", monkeypatch, f"paths:\n  cookies_file: {live_ck}\n")
    assert settings.cookies_file == live_ck, "the premise: the live install reads its cookies from the mount"
    target = live_ck if case == "same path" else other
    b = _bundle_with(tmp_path / "mk", f"paths:\n  cookies_file: {target}\n", cookies=case == "cookies over cap")  # every case: a branch where no cookies are restored
    if case == "cookies over cap":
        monkeypatch.setattr(bundle, "_MEMBER_CAP", {**bundle._MEMBER_CAP, bundle.MEMBER_COOKIES: 1})
    bdir = settings.config_dir / "backups"
    bdir.mkdir(exist_ok=True)
    shutil.copyfile(b, bdir / b.name)
    pv = client.post("/api/admin/database-restore", json={"name": b.name}, headers=_H).json()["preview"]
    assert (pv["cookies_live"], pv["cookies_target"]) == (str(live_ck), str(target)) and pv["cookies"] != "in bundle", pv
    slices = {"show": slice_between(APP_JS, *_SHOW), "hide": slice_between(APP_JS, *_HIDE),
              "stage": slice_between(APP_JS, *_STAGE), "pending": slice_between(APP_JS, *_PENDING)}
    card = _node(_CARD_HARNESS, {**slices, "pv": pv, "members": []})
    assert len(card["runs"]) == 2
    for run in card["runs"]:
        line = run["cookies"]
        assert f"your cookies file at {live_ck}" in line and "stays as it is" in line or "not in bundle" in line, (case, line)
        assert (f"motif will read {other} after the swap unless you keep your config" in line) is (case != "same path"), (case, line)
        assert (str(other) in line) is (case != "same path"), (case, line)


# ── 8. stranded bundle temps (F20) ───────────────────────────────────

def _install(tmp_path: Path, monkeypatch):
    cd = tmp_path / "cfg"
    cd.mkdir()
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(cd))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(cd / "data"))
    from app.config import Settings
    from app.web import api as api_mod
    monkeypatch.setattr(api_mod, "log_event", lambda *a, **k: None)
    monkeypatch.setattr(events, "log_event", lambda *a, **k: None)
    s = Settings(config_dir=cd, data_dir=cd / "data")
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    (cd / "motif.yaml").write_text(LIVE_YAML)
    return TestClient(api_mod.create_app(s), raise_server_exceptions=False), cd, s


def _temps(*roots: Path) -> list[Path]:
    return sorted(p for root in roots for p in root.iterdir() if p.name.startswith(bundle._TEMP_SHAPES))


def test_the_daily_sweep_removes_the_temps_a_killed_create_inspect_stage_and_upload_strand(tmp_path, monkeypatch, caplog):
    client, cd, s = _install(tmp_path, monkeypatch)
    bdir = cd / "backups"
    with monkeypatch.context() as m:  # a kill skips every finally — here the cleanups are stubbed out, so the real writers' temps stay
        m.setattr(shutil, "rmtree", lambda *a, **k: None)
        bf = bundle.create_bundle_for(s, _stamp(datetime.now(timezone.utc)))
        assert bundle.inspect_bundle(bdir / bf.name).ok
        assert bundle.stage_bundle_restore(s.db_path, cd, bdir / bf.name, keep_config=True).staged == ["database"]
        real_unlink = os.unlink
        m.setattr(os, "unlink", lambda p, *a, **k: None if Path(p).name.startswith(".restore-upload.") else real_unlink(p, *a, **k))
        with monkeypatch.context() as died:  # a bundle upload's temp is renamed into the list — it strands only when the kill lands during its inspection
            died.setattr(bundle, "inspect_bundle", lambda p: bundle.BundleCheck(False, "not a motif bundle: the process died here"))
            assert _upload(client, (bdir / bf.name).read_bytes()).status_code == 422
        snap = tmp_path / "snap.db"
        db_backup.vacuum_into(s.db_path, snap)
        r = client.post("/api/admin/database-restore/upload", headers=_H, files={"file": ("off-box.db", snap.read_bytes(), "application/octet-stream")})
        assert r.status_code == 200, r.text
    assert bundle.cancel_pending(s.db_path, cd)
    stranded = _temps(cd, bdir)
    kinds = sorted({p.name.split("-")[0] if p.name.startswith(".bundle-") else ".restore-upload." for p in stranded})
    assert len(stranded) == 5 and kinds == [".bundle", ".restore-upload."], stranded
    assert {p.name.rsplit("-", 1)[0] for p in stranded if p.name.startswith(".bundle-")} == {".bundle", ".bundle-inspect", ".bundle-stage"}
    assert {p.suffix for p in stranded if p.name.startswith(".restore-upload.")} == {".gz", ".db"}
    assert not any(os.path.abspath(p) in bundle._LIVE_TEMPS for p in stranded), "every writer released its temp"
    said: list[str] = []
    monkeypatch.setattr(sched, "log_event", lambda *a, **k: said.append(k.get("message")))
    sched._sweep_placement_temps_job(s)
    assert _temps(cd, bdir) == stranded and said == [], "too fresh — a writer may be mid-flight"
    _forward(monkeypatch, 7200)
    with caplog.at_level(logging.INFO, logger=bundle.log.name):
        sched._sweep_placement_temps_job(s)
    assert _temps(cd, bdir) == [], "past the age gate every stranded temp goes"
    assert said == ["Removed 5 stale placement / canonical-restore / bundle temp(s)"], said
    assert all(any(str(p) in r.getMessage() and "removed stale" in r.getMessage() for r in caplog.records) for p in stranded), caplog.text
    assert (bdir / bf.name).exists() and s.db_path.exists() and (cd / "motif.yaml").exists()
    assert [b.name for b in db_backup.list_backups(cd) if b.retained] == [bf.name]


def test_a_temp_this_process_holds_is_never_swept_and_the_sweep_runs_under_the_staging_lock(tmp_path, monkeypatch):
    cd = tmp_path / "cfg"
    bdir = cd / "backups"
    bdir.mkdir(parents=True)
    init_db(cd / "motif.db")
    held, stale, beside = bdir / ".bundle-held", bdir / ".bundle-stale", cd / ".bundle-stage-beside"
    for d in (held, stale, beside):
        d.mkdir()
        (d / "motif.db").write_bytes(b"x")
    _forward(monkeypatch, 7200)
    seen: list[tuple[str, bool]] = []
    real = shutil.rmtree
    monkeypatch.setattr(shutil, "rmtree", lambda p, *a, **k: seen.append((Path(p).name, bundle.STAGING_LOCK.locked())) or real(p, *a, **k))
    bundle.claim_temp(held)
    try:
        assert bundle.sweep_stale_bundle_temps(cd, cd / "motif.db") == 2
    finally:
        bundle.release_temp(held)
    assert held.exists() and not stale.exists() and not beside.exists()
    assert sorted(seen) == [(".bundle-stage-beside", True), (".bundle-stale", True)], seen
    assert bundle.sweep_stale_bundle_temps(cd, cd / "motif.db") == 1 and not held.exists(), "released, it is swept like any other"
    assert not bundle.STAGING_LOCK.locked()


# ── 9. a negative-size header, and the name echo (F24) ───────────────

def _negative_then_longname(path: Path, declared: int) -> None:
    neg = tarfile.TarInfo("././@PaxHeader")
    neg.type = tarfile.XHDTYPE
    hdr = bytearray(neg.tobuf(format=tarfile.GNU_FORMAT))
    hdr[124:136] = tarfile.itn(-(1 << 40), 12, tarfile.GNU_FORMAT)
    assert tarfile.nti(bytes(hdr[124:136])) == -(1 << 40), "the premise: tarfile reads the base-256 size as negative"
    long = tarfile.TarInfo("././@LongLink")
    long.type, long.size = tarfile.GNUTYPE_LONGNAME, declared
    with gzip.open(path, "wb", compresslevel=1) as gz:
        gz.write(bytes(_checksum(hdr)) + long.tobuf(format=tarfile.GNU_FORMAT))
        left, block = -(-declared // tarfile.BLOCKSIZE) * tarfile.BLOCKSIZE, b"n" * (1 << 20)
        while left:
            n = min(left, len(block))
            gz.write(block[:n])
            left -= n
        gz.write(bytes(2 * tarfile.BLOCKSIZE))


def _pre_fix_block(self, count):
    """tarfile.TarInfo._block before the CVE-2025-8194 fix: no negative guard."""
    blocks, remainder = divmod(count, tarfile.BLOCKSIZE)
    if remainder:
        blocks += 1
    return blocks * tarfile.BLOCKSIZE


@pytest.mark.parametrize("stdlib", ["guarded", "pre-fix _block"])
def test_a_negative_size_header_is_refused_in_motifs_words_before_it_credits_the_budget(tmp_path, monkeypatch, stdlib):
    p = tmp_path / "neg.tar.gz"
    _negative_then_longname(p, 32 << 20)
    assert p.stat().st_size < 256 << 10, "the premise: a small upload declaring 32 MiB"
    if stdlib == "pre-fix _block":
        monkeypatch.setattr(tarfile.TarInfo, "_block", _pre_fix_block)
    fetched = _spy_fetches(monkeypatch)
    c = bundle.inspect_bundle(p)
    assert not c.ok and c.error == "not a motif bundle: a header with a negative size, which motif never writes", c.error
    assert sum(fetched) <= 2 * bundle._STREAM_BUF, f"{sum(fetched)} bytes inflated — the 32 MiB longname was read"


def test_an_unexpected_members_name_is_echoed_truncated_with_its_length(api, tmp_path):
    client, _cd, _ = api
    name = "z" * 65000
    p = tmp_path / "long.tar.gz"
    with tarfile.open(p, "w:gz", format=tarfile.GNU_FORMAT) as t:
        ti = tarfile.TarInfo(name)
        ti.size = 0
        t.addfile(ti, io.BytesIO(b""))
    c = bundle.inspect_bundle(p)
    assert c.error == f"not a motif bundle: unexpected member '{'z' * bundle._NAME_ECHO}…' (65000 characters)", c.error[:300]
    r = _upload(client, p.read_bytes())
    assert r.status_code == 422 and r.json()["detail"] == c.error and len(r.content) < 1024, (r.status_code, len(r.content))
    short = tmp_path / "short.tar.gz"
    with tarfile.open(short, "w:gz") as t:
        ti = tarfile.TarInfo("../motif.db")
        ti.size = 0
        t.addfile(ti, io.BytesIO(b""))
    assert bundle.inspect_bundle(short).error == "not a motif bundle: unexpected member '../motif.db'", "a short name is echoed whole"
