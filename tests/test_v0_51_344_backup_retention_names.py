"""v0.51.344: a partial bundle keeps a complete one beside it, retention skips future stamps and the file just written, and same-second uploads get their own names."""
from __future__ import annotations

import inspect
import json
import logging
import os
import shutil
import subprocess
import tarfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.core import bundle, db_backup, events
from app.core.db import CURRENT_SCHEMA_VERSION, init_db
from tests._slice_helpers import slice_between
from tests.test_v0_51_339_bundle_staging_boot import _H, _bundle
from tests.test_v0_51_343_backup_upload_retention import (  # noqa: F401 — api is the (client, cd, settings) fixture
    _DRIVER, _NODE, APP_JS, _app_fn, _Chips, _listed, api)

SETTINGS_HTML = (Path(__file__).resolve().parent.parent / "app" / "web" / "templates" / "settings.html").read_text()
_UPLOAD_URL = "/api/admin/database-restore/upload"


def _stamp(dt: datetime) -> str:
    return dt.strftime("%Y%m%d-%H%M%S")


def _src(root: Path) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    init_db(root / "motif.db")
    (root / "motif.yaml").write_text("plex:\n  url: http://plex:32400\n")
    (root / "cookies.txt").write_text("#" * 64)
    return root


def _make(root: Path, cd: Path, stamp: str, monkeypatch, *, cookies_cap: int | None = None) -> db_backup.BackupFile:
    src = _src(root)
    with monkeypatch.context() as m:
        m.setattr(events, "log_event", lambda *a, **k: None)
        if cookies_cap is not None:
            m.setattr(bundle, "_MEMBER_CAP", {**bundle._MEMBER_CAP, bundle.MEMBER_COOKIES: cookies_cap})
        return bundle.create_bundle(src / "motif.db", cd, config_file=src / "motif.yaml", cookies_file=src / "cookies.txt",
                                    themes_dir=None, now_stamp=stamp, motif_version="t", schema_version=CURRENT_SCHEMA_VERSION)


def _holds(p: Path, member: str) -> bool:
    with tarfile.open(p, "r:gz") as t:
        return member in t.getnames()


def _nightly(cd: Path, retention: int, monkeypatch, *, bundle_mode: bool = True) -> list[tuple[str, str]]:
    from app.core import scheduler as sched
    said: list[tuple[str, str]] = []
    monkeypatch.setattr(sched, "log_event", lambda *a, **k: said.append((k.get("level"), k.get("message") or "")))
    monkeypatch.setattr(events, "log_event", lambda *a, **k: None)
    sched._scheduled_database_backup(SimpleNamespace(
        db_path=cd / "motif.db", config_dir=cd, config_file=SimpleNamespace(path=cd / "motif.yaml"),
        cookies_file=cd / "cookies.txt", themes_dir=None,
        db_backup_enabled=True, db_backup_retention=retention, db_backup_bundle=bundle_mode))
    return said


def _upload(client, data: bytes):
    return client.post(_UPLOAD_URL, headers=_H, files={"file": ("x.tar.gz", data, "application/gzip")})


# ── 1. a partial bundle keeps a complete one beside it ───────────────

@pytest.mark.parametrize("retention", [1, 2])
def test_the_newest_complete_bundle_outlives_newer_partial_ones_until_a_complete_one_lands(tmp_path, monkeypatch, caplog, retention):
    cd = _src(tmp_path / "cfg")
    bdir = cd / "backups"
    now = datetime.now(timezone.utc)
    oldest = _make(tmp_path / "c0", cd, _stamp(now - timedelta(days=10)), monkeypatch)
    complete = _make(tmp_path / "c1", cd, _stamp(now - timedelta(days=9)), monkeypatch)
    partials = [_make(tmp_path / f"p{i}", cd, _stamp(now - timedelta(days=8 - i)), monkeypatch, cookies_cap=63) for i in range(retention)]
    assert not complete.partial and all(p.partial and not _holds(bdir / p.name, bundle.MEMBER_COOKIES) for p in partials)
    cap = dict(bundle._MEMBER_CAP)
    monkeypatch.setattr(bundle, "_MEMBER_CAP", {**cap, bundle.MEMBER_COOKIES: 63})
    with caplog.at_level(logging.INFO, logger=db_backup.log.name):
        _nightly(cd, retention, monkeypatch)
    rows = db_backup.list_backups(cd)
    assert [r.name for r in rows if _holds(bdir / r.name, bundle.MEMBER_COOKIES)] == [complete.name], rows
    assert oldest.name not in {r.name for r in rows} and len(rows) == retention + 1, rows
    assert all(r.partial == (not _holds(bdir / r.name, bundle.MEMBER_COOKIES)) for r in rows), "the name alone says partial"
    assert any(complete.name in r.getMessage() and "kept" in r.getMessage() for r in caplog.records), caplog.text
    monkeypatch.setattr(bundle, "_MEMBER_CAP", cap)
    time.sleep(1.05)  # the nightly names its bundle by the second
    _nightly(cd, retention, monkeypatch)
    held = [r.name for r in db_backup.list_backups(cd) if _holds(bdir / r.name, bundle.MEMBER_COOKIES)]
    assert len(held) == 1 and held[0] != complete.name, (held, "a newer complete bundle makes the old one prunable")


# ── 2. what retention counts ─────────────────────────────────────────

@pytest.mark.parametrize("ahead", ["a legacy upload", "a clock-ahead snapshot"])
@pytest.mark.parametrize("bundle_mode", [True, False], ids=["bundle", "snapshot"])
@pytest.mark.parametrize("retention", [1, 2])
def test_a_retained_name_stamped_after_now_is_set_aside_and_the_new_file_survives(tmp_path, monkeypatch, caplog, ahead, bundle_mode, retention):
    cd = _src(tmp_path / "cfg")
    now = datetime.now(timezone.utc)
    if ahead == "a legacy upload":  # .336-.342 filed an upload under its manifest's stamp
        future = cd / "backups" / f"motif-bundle-{_stamp(now + timedelta(days=365))}.tar.gz"
        future.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(_bundle(tmp_path / "mk"), future)
    else:
        future = cd / "backups" / db_backup.create_backup(cd / "motif.db", cd, now_stamp=_stamp(now + timedelta(days=1))).name
    old1 = db_backup.create_backup(cd / "motif.db", cd, now_stamp=_stamp(now - timedelta(days=1)))
    old2 = db_backup.create_backup(cd / "motif.db", cd, now_stamp=_stamp(now - timedelta(days=2)))
    with caplog.at_level(logging.WARNING, logger=db_backup.log.name):
        said = _nightly(cd, retention, monkeypatch, bundle_mode=bundle_mode)
    names = {b.name for b in db_backup.list_backups(cd)}
    new = names - {future.name, old1.name, old2.name}
    assert len(new) == 1 and next(iter(new)) in said[-1][1], (names, said)
    assert names == {future.name} | new | ({old1.name} if retention == 2 else set()), sorted(names)
    assert any(future.name in r.getMessage() for r in caplog.records if r.levelno == logging.WARNING), caplog.text


def test_a_backup_landing_during_the_nightlys_vacuum_counts_as_now_and_never_pushes_the_nightly_out(tmp_path, monkeypatch, caplog):
    cd = _src(tmp_path / "cfg")
    real, landed = db_backup.vacuum_into, []

    def slow_vacuum(db, dest):
        real(db, dest)
        if not landed:
            landed.append(dest)
            time.sleep(1.05)  # a manual // CREATE BACKUP NOW lands while the nightly is still writing
            landed.append(db_backup.create_backup(db, cd, now_stamp=_stamp(datetime.now(timezone.utc))))
    monkeypatch.setattr(db_backup, "vacuum_into", slow_vacuum)
    with caplog.at_level(logging.WARNING, logger=db_backup.log.name):
        said = _nightly(cd, 1, monkeypatch, bundle_mode=False)
    names = [b.name for b in db_backup.list_backups(cd)]
    assert len(names) == 2 and names[0] == landed[1].name and names[1] in said[-1][1], (names, said)
    assert not [r for r in caplog.records if "stamped after now" in r.getMessage()], caplog.text


@pytest.mark.parametrize("first", ["partial", "complete"])
def test_one_bundle_a_second_whichever_shape_the_first_took(tmp_path, monkeypatch, first):
    cd = _src(tmp_path / "cfg")
    stamp = _stamp(datetime.now(timezone.utc) - timedelta(days=1))
    made = _make(tmp_path / "a", cd, stamp, monkeypatch, cookies_cap=63 if first == "partial" else None)
    with pytest.raises(FileExistsError):
        _make(tmp_path / "b", cd, stamp, monkeypatch, cookies_cap=None if first == "partial" else 63)
    assert [b.name for b in db_backup.list_backups(cd)] == [made.name]


def test_prune_never_deletes_the_name_it_was_told_to_keep(tmp_path):
    db = tmp_path / "motif.db"
    init_db(db)
    now = datetime.now(timezone.utc)
    mine = db_backup.create_backup(db, tmp_path, now_stamp=_stamp(now - timedelta(seconds=10)))
    later = db_backup.create_backup(db, tmp_path, now_stamp=_stamp(now - timedelta(seconds=5)))
    assert db_backup.prune_backups(tmp_path, 1, now_stamp=_stamp(now), keep=mine.name) == []
    assert {b.name for b in db_backup.list_backups(tmp_path)} == {mine.name, later.name}
    assert db_backup.prune_backups(tmp_path, 1, now_stamp=_stamp(now)) == [mine.name], "without keep it is the one retention drops"


@pytest.mark.parametrize("retention", [1, 3, 1000])
def test_prune_classifies_each_file_once_and_keeps_what_the_listing_calls_unretained(tmp_path, monkeypatch, retention):
    bdir = tmp_path / "backups"
    bdir.mkdir()
    now = datetime.now(timezone.utc)
    shapes = ("motif-{}.db", "motif-bundle-{}.tar.gz", "motif-bundle-partial-{}.tar.gz", "motif-prerestore-{}.db",
              "motif-bundle-upload-{}.tar.gz", "motif-bundle-upload-{}-2.tar.gz")
    for i in range(24):
        (bdir / shapes[i % len(shapes)].format(_stamp(now - timedelta(hours=i + 1)))).write_bytes(b"x")
    (bdir / "notes.txt").write_text("not a backup")
    listed = db_backup.list_backups(tmp_path)
    files = len(list(bdir.iterdir()))
    assert len(listed) == 24 and files == 25
    calls: list[str] = []
    real = db_backup._classify
    monkeypatch.setattr(db_backup, "_classify", lambda name: calls.append(name) or real(name))
    removed = db_backup.prune_backups(tmp_path, retention, now_stamp=_stamp(now))
    assert len(calls) == files, f"{len(calls)} classifications for {files} files"
    assert removed == [b.name for b in listed if b.retained][retention:]
    assert all((bdir / b.name).exists() for b in listed if not b.retained)


# ── 3. same-second uploads ───────────────────────────────────────────

def test_two_uploads_into_an_occupied_second_keep_their_own_bytes_and_stage_by_their_own_names(api, tmp_path):
    client, cd, _ = api
    bdir = cd / "backups"
    bdir.mkdir()
    base = datetime.now(timezone.utc)
    window = {f"motif-bundle-upload-{_stamp(base + timedelta(seconds=s))}.tar.gz" for s in range(-1, 30)}
    squatter = _bundle(tmp_path / "sq", token="SQUATTER").read_bytes()
    for n in window:
        (bdir / n).write_bytes(squatter)
    got: dict[str, str] = {}
    for token in ("FIRST", "SECOND"):
        data = _bundle(tmp_path / token, token=token).read_bytes()
        r = _upload(client, data)
        assert r.status_code == 200, r.text
        name = r.json()["preview"]["name"]
        assert name not in window and db_backup.kind_of(name) == "bundle" and (bdir / name).read_bytes() == data, name
        got[token] = name
    assert len(set(got.values())) == 2 and all((bdir / n).read_bytes() == squatter for n in window)
    for token, name in got.items():
        r = client.post("/api/admin/database-restore", json={"name": name, "confirm": True, "keep_config": False}, headers=_H)
        assert r.status_code == 200 and f"token: {token}" in (cd / bundle.CONFIG_PENDING).read_text(), (token, r.text)
    assert not [p.name for p in bdir.iterdir() if p.name.startswith(".")], "no temp file left behind"


def test_a_concurrent_upload_landing_between_the_claim_and_the_rename_is_never_overwritten(tmp_path, monkeypatch):
    bdir = tmp_path / "backups"
    bdir.mkdir()
    stamp = _stamp(datetime.now(timezone.utc))
    a, b = bdir / ".a", bdir / ".b"
    a.write_bytes(b"A")
    b.write_bytes(b"B")
    real, raced, landed = os.replace, [], []

    def racing(src, dst):
        if not raced:
            raced.append(dst)
            landed.append(bundle.file_upload(b, bdir, stamp))  # another request files its upload in the same second first
        return real(src, dst)
    monkeypatch.setattr(os, "replace", racing)
    mine = bundle.file_upload(a, bdir, stamp)
    assert raced and mine != landed[0], (mine, landed)
    assert (mine.read_bytes(), landed[0].read_bytes()) == (b"A", b"B")
    assert sorted(p.name for p in bdir.iterdir()) == sorted([mine.name, landed[0].name])


@pytest.mark.parametrize("claim", ["removed", "unremovable"])
def test_a_rename_that_fails_leaves_the_upload_and_no_silent_empty_claim(tmp_path, monkeypatch, caplog, claim):
    bdir = tmp_path / "backups"
    bdir.mkdir()
    up = bdir / ".restore-upload.x.tar.gz"
    up.write_bytes(b"U")
    real_unlink, claimed = os.unlink, []

    def refuse_rename(src, dst):
        claimed.append(Path(dst).name)
        raise PermissionError(13, "Permission denied")

    def unlink(path, *a, **k):
        if claim == "unremovable" and Path(path).name in claimed:
            raise PermissionError(13, "Permission denied")
        return real_unlink(path, *a, **k)
    monkeypatch.setattr(os, "replace", refuse_rename)
    monkeypatch.setattr(os, "unlink", unlink)
    with caplog.at_level(logging.WARNING, logger=bundle.log.name), pytest.raises(PermissionError):
        bundle.file_upload(up, bdir, _stamp(datetime.now(timezone.utc)))
    assert claimed and up.read_bytes() == b"U", claimed
    left = sorted(p.name for p in bdir.iterdir() if p != up)
    warned = [r.getMessage() for r in caplog.records if claimed[0] in r.getMessage()]
    assert left == ([] if claim == "removed" else claimed), left
    assert bool(warned) is (claim == "unremovable"), caplog.text


def test_when_every_name_for_the_second_is_taken_nothing_is_kept_and_the_answer_says_try_again(api, tmp_path):
    client, cd, _ = api
    bdir = cd / "backups"
    bdir.mkdir()
    base = datetime.now(timezone.utc)
    taken = {bundle.uploaded_bundle_name(_stamp(base + timedelta(seconds=s)), n) for s in range(-1, 30) for n in range(1, 100)}
    assert all(db_backup.is_backup_name(n) for n in taken)
    for n in taken:
        (bdir / n).touch()
    r = _upload(client, _bundle(tmp_path / "mk").read_bytes())
    assert r.status_code == 503 and "try the upload again" in r.json()["detail"], r.text
    assert {p.name for p in bdir.iterdir()} == taken, "no temp file and no claim left behind"


def test_an_upload_whose_minted_name_fails_the_gate_is_refused_and_leaves_nothing(api, tmp_path, monkeypatch):
    client, cd, _ = api
    data = _bundle(tmp_path / "mk").read_bytes()
    real, minted = bundle.uploaded_bundle_name, []

    def unpadded_year(stamp, *a):  # a clock before year 1000 on a libc that does not pad %Y
        minted.append(real(stamp[1:], *a))
        return minted[-1]
    monkeypatch.setattr(bundle, "uploaded_bundle_name", unpadded_year)
    r = _upload(client, data)
    assert minted and not any(db_backup.is_backup_name(n) for n in minted), minted
    assert r.status_code == 422 and "invalid backup stamp" in r.json()["detail"], r.text
    assert list((cd / "backups").iterdir()) == [], "no file under the bad name, no claim, no temp"
    monkeypatch.setattr(bundle, "uploaded_bundle_name", real)
    r = _upload(client, data)
    assert r.status_code == 200 and r.json()["preview"]["name"] in _listed(client), r.text


def test_a_temp_upload_that_cannot_be_removed_is_logged(api, monkeypatch, caplog):
    client, cd, _ = api
    real = os.unlink

    def refuse(path, *a, **k):
        if Path(path).name.startswith(".restore-upload."):
            raise PermissionError(13, "Permission denied")
        return real(path, *a, **k)
    monkeypatch.setattr(os, "unlink", refuse)
    with caplog.at_level(logging.WARNING):
        r = _upload(client, b"\x1f\x8b not a gzip body")
    assert r.status_code == 422, r.text
    assert any("could not remove the temp upload" in rec.getMessage() for rec in caplog.records), caplog.text


# ── 4. what the page says ────────────────────────────────────────────

def _hint(field: str) -> str:
    return " ".join(slice_between(SETTINGS_HTML, f'data-cfg-field="{field}"', "</label>").split())


def test_the_retention_hint_names_every_kind_retention_never_counts_and_the_rules_prune_keeps():
    words = {db_backup._UPLOAD_RE: "uploaded bundles", db_backup._PRERESTORE_RE: "pre-restore copies"}
    exempt = [shape for _kind, shape, retained, _partial in db_backup._KINDS if not retained]
    assert exempt and all(shape in words for shape in exempt), "a new kind outside retention needs its words in the retention hint"
    hint = _hint("database_backup.retention")
    assert all(words[shape] in hint for shape in exempt), hint
    assert ("the newest complete bundle is kept" in hint) is any(partial for *_row, partial in db_backup._KINDS), hint
    assert ("stamped later than now" in hint) is ("now_stamp" in inspect.signature(db_backup.prune_backups).parameters), hint
    assert words[db_backup._UPLOAD_RE] in _hint("database_backup.bundle")


@pytest.mark.skipif(not _NODE, reason="node not installed")
def test_the_create_json_the_list_json_and_the_chip_say_partial_exactly_where_a_member_was_left_out(api, tmp_path, monkeypatch):
    client, cd, settings = api
    monkeypatch.setattr(events, "log_event", lambda *a, **k: None)
    cap = dict(bundle._MEMBER_CAP)
    monkeypatch.setattr(bundle, "_MEMBER_CAP", {**cap, bundle.MEMBER_CONFIG: 1})
    r = client.post("/api/admin/database-backup?kind=bundle", headers=_H)
    assert r.status_code == 200, r.text
    made = r.json()["backup"]
    monkeypatch.setattr(bundle, "_MEMBER_CAP", cap)
    now = datetime.now(timezone.utc)
    bundle.create_bundle(settings.db_path, cd, config_file=cd / "motif.yaml", cookies_file=None, themes_dir=None,
                         now_stamp=_stamp(now - timedelta(days=1)), motif_version="t", schema_version=CURRENT_SCHEMA_VERSION)
    db_backup.create_backup(settings.db_path, cd, now_stamp=_stamp(now - timedelta(days=2)))
    assert _upload(client, _bundle(tmp_path / "mk").read_bytes()).status_code == 200
    listing = client.get("/api/admin/database-backups", headers=_H).json()
    rows = {b["name"]: b for b in listing["backups"]}
    for name, b in rows.items():
        assert b["partial"] is (b["kind"] == "bundle" and not _holds(cd / "backups" / name, bundle.MEMBER_CONFIG)), (name, b)
    assert made["partial"] is True and rows[made["name"]]["partial"] is True and sum(b["partial"] for b in rows.values()) == 1

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
        assert ("partial bundle" in tips[name]) is b["partial"] and ("motif-bundle-partial-" in tips[name]) is b["partial"], (name, tips[name])
