"""v0.51.342 integration review: the config loader, the bundle leaf rule and the settings save agree; motif restores what it writes.

  1. A lossless hand-edited scalar loads at its declared type, and load, validate, the Settings reads, flatten_config on
     both sides, preview and stage treat it as its canonical spelling; a settings save heals a hand-edited bool or integer.
  2. One cap table: create_bundle writes no member inspect refuses; an over-cap config/cookies leaves the database restorable.
  3. A fault writing the extracted database is the disk's own error (507 / 500 in words), never "not a motif bundle".
  4. A StagingError says members and errno words, never an absolute config_dir path.
  5. The staged database is owner-only on the bundle and the snapshot path.
"""
from __future__ import annotations

import dataclasses
import errno
import inspect
import logging
import os
import shutil
import stat
import tarfile
from pathlib import Path

import pytest
import yaml
from fastapi.testclient import TestClient

from app.core import bundle, config_file, db_backup, events
from app.core.auth import create_admin, init_auth_schema
from app.core.config_file import MotifConfig
from app.core.db import CURRENT_SCHEMA_VERSION, init_db
from app.web.api import _apply_partial_config
from tests.test_v0_51_339_bundle_staging_boot import _H, LIVE_YAML, NOW, _bundle, _live, _marker_rows
from tests.test_v0_51_341_staging_boot_hardening import _refuse_chmod, _snapshot

STAMP2 = "20260912-040001"


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
    return TestClient(api_mod.create_app(settings), raise_server_exceptions=False), cd, settings


def _make_bundle(root: Path, yaml_text: str | None, *, cookies: str | None = None, stamp: str = NOW) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    if not (root / "motif.db").exists():
        init_db(root / "motif.db")
    cfg = ck = None
    if yaml_text is not None:
        cfg = root / "motif.yaml"
        cfg.write_text(yaml_text)
    if cookies is not None:
        ck = root / "cookies.txt"
        ck.write_text(cookies)
    bf = bundle.create_bundle(root / "motif.db", root, config_file=cfg, cookies_file=ck, themes_dir=None,
                              now_stamp=stamp, motif_version="0.51.342", schema_version=CURRENT_SCHEMA_VERSION)
    return root / "backups" / bf.name


# ── 1. hand-edited scalars ───────────────────────────────────────────

def _scalar_leaves() -> list[tuple[str | None, str, object]]:
    cfg, out = MotifConfig(), []
    for f in dataclasses.fields(cfg):
        v = getattr(cfg, f.name)
        if dataclasses.is_dataclass(v):
            out += [(f.name, g.name, getattr(v, g.name)) for g in dataclasses.fields(v)
                    if type(getattr(v, g.name)) in (str, int, float, bool)]
        elif type(v) in (str, int, float, bool):
            out.append((None, f.name, v))
    return out


def _doc(pairs) -> dict:
    out: dict = {}
    for sec, leaf, value in pairs:
        (out.setdefault(sec, {}) if sec else out)[leaf] = value
    return out


def _spellings() -> dict[str, tuple[dict, dict]]:
    leaves = _scalar_leaves()
    text = [(s, k) for s, k, d in leaves if type(d) is str]
    empty = [(s, k) for s, k, d in leaves if type(d) is str and d == ""]
    ints = [(s, k, d) for s, k, d in leaves if type(d) is int]
    nums = [(s, k, d) for s, k, d in leaves if type(d) is float]
    return {  # name: (the hand-edited document, the same settings in their canonical types)
        "an int in every text leaf": (_doc((s, k, 1000 + i) for i, (s, k) in enumerate(text)),
                                      _doc((s, k, str(1000 + i)) for i, (s, k) in enumerate(text))),
        "a float in every text leaf": (_doc((s, k, 1000.5 + i) for i, (s, k) in enumerate(text)),
                                       _doc((s, k, str(1000.5 + i)) for i, (s, k) in enumerate(text))),
        "null in every text leaf whose default is empty": (_doc((s, k, None) for s, k in empty),
                                                           _doc((s, k, "") for s, k in empty)),
        "a whole float in every integer leaf": (_doc((s, k, float(d)) for s, k, d in ints), _doc(ints)),
        "an int in every number leaf": (_doc((s, k, int(d) + 2) for s, k, d in nums),
                                        _doc((s, k, float(int(d) + 2)) for s, k, d in nums)),
        "a numeric string in every number leaf": (_doc((s, k, f"{d + 2:g}") for s, k, d in nums),
                                                  _doc((s, k, d + 2) for s, k, d in nums)),
    }


def test_the_walk_reaches_the_leaves_the_review_named():
    sp = _spellings()
    assert sp["an int in every text leaf"][0]["plex"]["movie_section"] is not None
    assert set(sp["null in every text leaf whose default is empty"][0]["plex"]) >= {"url", "token"}
    assert "port" in sp["a whole float in every integer leaf"][0]["web"]
    assert "target_lufs" in sp["a numeric string in every number leaf"][0]["loudness"]


_NOT_READS = {"cfg", "config_file", "config_write_lock", "revision", "config_dir", "data_dir", "db_path", "session_key_file"}


def _reads(root: Path) -> dict:
    from app.config import Settings
    s = Settings(config_dir=root, data_dir=root / "data")
    out = {}
    for name, member in inspect.getmembers(type(s)):
        if isinstance(member, property) and name not in _NOT_READS:
            try:
                out[name] = getattr(s, name)
            except Exception as e:  # a read that raises disagrees with its canonical twin, which never does
                out[name] = ("raised", type(e).__name__)
    return out


@pytest.mark.parametrize("how", list(_spellings()))
def test_a_hand_edited_scalar_agrees_with_its_canonical_spelling_everywhere(tmp_path, no_env, how):
    hand, canon = _spellings()[how]
    hand_text, canon_text = yaml.safe_dump(hand), yaml.safe_dump(canon)
    roots = {}
    for side, text in (("hand", hand_text), ("canon", canon_text)):
        roots[side] = tmp_path / side
        roots[side].mkdir()
        (roots[side] / "motif.yaml").write_text(text)
    loaded = {side: config_file.ConfigFile(r / "motif.yaml").load() for side, r in roots.items()}
    for sec, leaves in canon.items():
        pairs = leaves.items() if isinstance(leaves, dict) else [(sec, leaves)]
        for leaf, want in pairs:
            got = getattr(loaded["hand"], leaf) if not isinstance(leaves, dict) else getattr(getattr(loaded["hand"], sec), leaf)
            assert got == want and type(got) is type(want), f"{sec}.{leaf}: loaded {got!r}, not {want!r}"
    errs = {side: config_file.validate(c) for side, c in loaded.items()}
    assert errs["hand"] == errs["canon"] and not any("unexpected type" in e for e in errs["hand"]), errs["hand"]
    reads = _reads(roots["hand"])
    assert reads == _reads(roots["canon"])
    assert not [n for n, v in reads.items() if isinstance(v, tuple) and v[:1] == ("raised",)], reads
    for side in ("live", "bundle"):
        flat, err = bundle.flatten_config(hand_text, side=side)
        assert err is None and (flat, err) == bundle.flatten_config(canon_text, side=side), side
    db, cd = _live(tmp_path)
    b_hand, b_canon = _make_bundle(tmp_path / "bh", hand_text), _make_bundle(tmp_path / "bc", canon_text)
    for live_text, b in ((hand_text, b_canon), (canon_text, b_hand)):
        (cd / "motif.yaml").write_text(live_text)
        p = bundle.preview(b, cd / "motif.yaml")
        assert p["config_parse_error"] == {"live": None, "bundle": None} and p["config_diff"] == [], p["config_diff"][:3]
    assert bundle.stage_bundle_restore(db, cd, b_hand, keep_config=False).staged == ["database", "config"]
    assert (cd / bundle.CONFIG_PENDING).read_text() == hand_text


def test_a_live_unquoted_movie_section_previews_a_clean_bundle_with_its_diff(tmp_path, no_env):
    db, cd = _live(tmp_path)
    (cd / "motif.yaml").write_text(LIVE_YAML + "  movie_section: 1\n")
    p = bundle.preview(_bundle(tmp_path / "mk"), cd / "motif.yaml")
    assert p["config_parse_error"] == {"live": None, "bundle": None}, p["config_parse_error"]
    assert {"plex.url", "plex.token"} <= {r["key"] for r in p["config_diff"]}, p["config_diff"]
    carrying = _make_bundle(tmp_path / "carry", "plex:\n  url: http://plex:32400\n  movie_section: 1\n")
    assert bundle.stage_bundle_restore(db, cd, carrying, keep_config=False).staged == ["database", "config"]
    assert bundle.apply_pending_config(cd, now_stamp=STAMP2)["applied"] == ["motif.yaml"]
    assert config_file.ConfigFile(cd / "motif.yaml").load().plex.movie_section == "1"


def test_the_endpoint_previews_against_a_live_unquoted_movie_section_with_the_diff(api, tmp_path):
    client, cd, _ = api
    (cd / "motif.yaml").write_text(LIVE_YAML + "  movie_section: 1\n")
    b = _bundle(tmp_path / "mk")
    shutil.copyfile(b, cd / "backups" / b.name)
    r = client.post("/api/admin/database-restore", json={"name": b.name}, headers=_H)
    assert r.status_code == 200, r.text
    pv = r.json()["preview"]
    assert pv["config_parse_error"] == {"live": None, "bundle": None} and pv["config_diff"], pv


def test_a_settings_save_heals_a_hand_edited_bool_or_integer_leaf():
    cfg = MotifConfig()
    cfg.plex.enabled = 1                    # a bool leaf stays strict at load
    cfg.downloads.rate_per_hour = "30"      # and a numeric string in an integer leaf is not coerced there
    _apply_partial_config(cfg, {"plex": {"enabled": "true"}, "downloads": {"rate_per_hour": 45}})
    assert cfg.plex.enabled is True and cfg.downloads.rate_per_hour == 45 and type(cfg.downloads.rate_per_hour) is int
    cfg.plex.enabled = 1
    _apply_partial_config(cfg, {"plex": {"enabled": False}})
    assert cfg.plex.enabled is False


def test_the_settings_save_writes_the_healed_leaves_to_disk(api):
    client, cd, settings = api
    (cd / "motif.yaml").write_text("plex:\n  enabled: 1\ndownloads:\n  rate_per_hour: '30'\n")
    settings.reload()
    r = client.patch("/api/config", headers=_H, json={"plex": {"enabled": True}, "downloads": {"rate_per_hour": 45}})
    assert r.status_code == 200, r.text
    on_disk = yaml.safe_load((cd / "motif.yaml").read_text())
    assert on_disk["plex"]["enabled"] is True and on_disk["downloads"]["rate_per_hour"] == 45
    assert type(on_disk["downloads"]["rate_per_hour"]) is int


# ── 2. one cap table ─────────────────────────────────────────────────

def _sizes(b: Path) -> dict[str, int]:
    with tarfile.open(b, "r:gz") as t:
        return {ti.name: ti.size for ti in t}


def _recreate(root: Path, stamp: str = STAMP2) -> Path:
    src = root / "src"
    bf = bundle.create_bundle(src / "motif.db", src, config_file=src / "motif.yaml", cookies_file=src / "cookies.txt",
                              themes_dir=None, now_stamp=stamp, motif_version="0.51.339",
                              schema_version=CURRENT_SCHEMA_VERSION)
    return src / "backups" / bf.name


@pytest.mark.parametrize("member", bundle.MEMBERS)
def test_create_and_inspect_read_one_cap_table(tmp_path, monkeypatch, member):
    monkeypatch.setattr(events, "log_event", lambda *a, **k: None)
    b = _bundle(tmp_path / "mk")
    size = _sizes(b)[member]
    monkeypatch.setattr(bundle, "_MEMBER_CAP", {**bundle._MEMBER_CAP, member: size})
    c = bundle.inspect_bundle(b)
    assert c.ok and c.oversize == {}, "at its cap, a member is taken"
    monkeypatch.setattr(bundle, "_MEMBER_CAP", {**bundle._MEMBER_CAP, member: size - 1})
    c = bundle.inspect_bundle(b)
    backups = tmp_path / "mk" / "src" / "backups"
    if member in (bundle.MEMBER_DB, bundle.MEMBER_MANIFEST):
        assert not c.ok and f"{member} is {size} bytes, over its {size - 1}-byte cap" in c.error, c.error
        with pytest.raises(ValueError, match="no bundle was written"):
            _recreate(tmp_path / "mk")
        assert [p.name for p in backups.iterdir()] == [b.name], "no bundle and no temp left behind"
    else:
        assert c.ok and c.oversize == {member: size}, "hashed, not refused, until something would stage it"
        again = _recreate(tmp_path / "mk")
        assert member not in _sizes(again) and bundle.inspect_bundle(again).oversize == {}
        assert bundle.read_manifest(again)["left_out"] == {member: {"size": size, "cap": size - 1}}


@pytest.mark.parametrize("member", [bundle.MEMBER_CONFIG, bundle.MEMBER_COOKIES])
def test_create_leaves_an_over_cap_config_or_cookies_out_and_says_so(tmp_path, monkeypatch, caplog, member):
    seen: list[dict] = []
    monkeypatch.setattr(events, "log_event", lambda *a, **k: seen.append(k))
    src = tmp_path / "src"
    src.mkdir()
    init_db(src / "motif.db")
    (src / bundle.MEMBER_CONFIG).write_text("plex:\n  token: BUNDLE-TOKEN\n")
    (src / bundle.MEMBER_COOKIES).write_text("# c\n")
    (src / member).write_text("#" * 64)
    monkeypatch.setattr(bundle, "_MEMBER_CAP", {**bundle._MEMBER_CAP, member: 63})
    other = bundle.MEMBER_COOKIES if member == bundle.MEMBER_CONFIG else bundle.MEMBER_CONFIG
    with caplog.at_level(logging.WARNING, logger=bundle.log.name):
        b = _create_from(src)
    assert member not in _sizes(b) and other in _sizes(b)
    assert bundle.read_manifest(b)["left_out"] == {member: {"size": 64, "cap": 63}}
    said = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert any(member in m and b.name in m for m in said), said
    assert [(e["level"], e["component"]) for e in seen] == [("WARNING", "backup")] and member in seen[0]["message"]
    assert bundle.inspect_bundle(b).ok
    db, cd = _live(tmp_path)
    word = "config" if other == bundle.MEMBER_CONFIG else "cookies"
    assert bundle.stage_bundle_restore(db, cd, b, keep_config=False).staged == ["database", word]


def _create_from(src: Path) -> Path:
    bf = bundle.create_bundle(src / "motif.db", src, config_file=src / bundle.MEMBER_CONFIG,
                              cookies_file=src / bundle.MEMBER_COOKIES, themes_dir=None, now_stamp=NOW,
                              motif_version="0.51.342", schema_version=CURRENT_SCHEMA_VERSION)
    return src / "backups" / bf.name


@pytest.mark.parametrize("member", [bundle.MEMBER_CONFIG, bundle.MEMBER_COOKIES])
def test_an_over_cap_config_or_cookies_leaves_the_database_restorable(tmp_path, monkeypatch, caplog, member):
    b = _bundle(tmp_path / "mk")
    size = _sizes(b)[member]
    monkeypatch.setattr(bundle, "_MEMBER_CAP", {**bundle._MEMBER_CAP, member: size - 1})
    words = f"{member} is {size} bytes, over its {size - 1}-byte cap"
    with caplog.at_level(logging.WARNING, logger=bundle.log.name):
        c = bundle.inspect_bundle(b)
    assert c.ok and c.oversize == {member: size} and (c.config_bytes if member == bundle.MEMBER_CONFIG else c.cookies_bytes) is None
    assert any(words in r.getMessage() for r in caplog.records)
    db, cd = _live(tmp_path)
    p = bundle.preview(b, cd / "motif.yaml")
    if member == bundle.MEMBER_CONFIG:
        assert p["config_parse_error"]["bundle"] == words and p["config_diff"] == [], "the page then keeps the config"
        with pytest.raises(ValueError) as refused:
            bundle.stage_bundle_restore(db, cd, b, keep_config=False)
        assert words in str(refused.value) and "KEEP MY CURRENT CONFIG" in str(refused.value), refused.value
        assert bundle.pending_members(db, cd) == [] and not bundle.STAGING_LOCK.locked()
    else:
        # v0.51.342: reversed — an over-cap cookies.txt is left as it is and the config still stages (test_v0_51_342_config_bundle_followups)
        assert p["cookies"].startswith("in bundle, but " + words) and "your cookies file stays as it is" in p["cookies"]
        assert bundle.stage_bundle_restore(db, cd, b, keep_config=False).staged == ["database", "config"]
    assert bundle.stage_bundle_restore(db, cd, b, keep_config=True).staged == ["database"]
    assert _marker_rows(db_backup.restore_pending_path(db)) == 1, "the bundle's own database is what stages"


def test_a_config_or_cookies_member_past_a_databases_cap_is_refused_outright(tmp_path, monkeypatch):
    src = tmp_path / "src"
    b = _make_bundle(src, "plex: {}\n", cookies="#" * (2 << 20))
    sizes = _sizes(b)
    assert sizes[bundle.MEMBER_COOKIES] > sizes[bundle.MEMBER_DB], "the premise: cookies bigger than the database"
    monkeypatch.setattr(bundle, "_MEMBER_CAP", {**bundle._MEMBER_CAP, bundle.MEMBER_DB: sizes[bundle.MEMBER_DB],
                                                bundle.MEMBER_COOKIES: 16})
    c = bundle.inspect_bundle(b)
    assert not c.ok and f"cookies.txt is {sizes[bundle.MEMBER_COOKIES]} bytes, over its {sizes[bundle.MEMBER_DB]}-byte cap" in c.error


def _scheduled(tmp_path: Path, monkeypatch):
    from app.config import Settings
    from app.core import scheduler
    cd = tmp_path / "cfg"
    cd.mkdir()
    (cd / "cookies.txt").write_text("#" * 64)
    (cd / "motif.yaml").write_text(f"database_backup:\n  enabled: true\n  bundle: true\npaths:\n  cookies_file: {cd / 'cookies.txt'}\n")
    settings = Settings(config_dir=cd, data_dir=cd / "data")
    init_db(settings.db_path)
    seen: list[dict] = []
    monkeypatch.setattr(scheduler, "log_event", lambda *a, **k: seen.append(k))
    monkeypatch.setattr(events, "log_event", lambda *a, **k: seen.append(k))
    return lambda: scheduler._scheduled_database_backup(settings), seen, cd


def test_the_scheduled_bundle_says_what_it_left_out(tmp_path, monkeypatch, no_env):
    run, seen, cd = _scheduled(tmp_path, monkeypatch)
    monkeypatch.setattr(bundle, "_MEMBER_CAP", {**bundle._MEMBER_CAP, bundle.MEMBER_COOKIES: 63})
    run()
    assert [e["level"] for e in seen] == ["WARNING", "INFO"], seen
    assert "left out cookies.txt (64 bytes, over its 63-byte cap)" in seen[0]["message"]
    assert seen[1]["message"].startswith("Scheduled backup bundle created")
    [made] = db_backup.list_backups(cd)
    assert bundle.stage_bundle_restore(*_live(tmp_path), cd / "backups" / made.name, keep_config=False).staged == ["database", "config"]


def test_a_scheduled_bundle_over_the_database_cap_fails_visibly_and_writes_nothing(tmp_path, monkeypatch, no_env):
    run, seen, cd = _scheduled(tmp_path, monkeypatch)
    monkeypatch.setattr(bundle, "_MEMBER_CAP", {**bundle._MEMBER_CAP, bundle.MEMBER_DB: 1024})
    run()
    assert [e["level"] for e in seen] == ["WARNING"] and "Scheduled database backup failed" in seen[0]["message"], seen
    assert "over the 1024-byte cap a bundle's database may be" in seen[0]["message"], seen[0]["message"]
    assert db_backup.list_backups(cd) == [] and not list((cd / "backups").glob(".bundle-*"))


# ── 3. a fault writing the extraction is the disk's ──────────────────

def _disk_refuses_the_extraction(monkeypatch, err: int = errno.ENOSPC, at: str = "write") -> None:
    real_open, real_write = os.open, os.write
    fds: set[int] = set()

    def open_(path, flags, *a, **k):
        p = Path(path)
        if p.name == bundle.MEMBER_DB and p.parent.name.startswith(".bundle-"):
            if at == "open":
                raise OSError(err, os.strerror(err), str(p))
            fd = real_open(path, flags, *a, **k)
            fds.add(fd)
            return fd
        return real_open(path, flags, *a, **k)

    def write(fd, data):
        if fd in fds:
            fds.discard(fd)
            raise OSError(err, os.strerror(err))
        return real_write(fd, data)
    monkeypatch.setattr(os, "open", open_)
    monkeypatch.setattr(os, "write", write)


def test_no_space_writing_the_extraction_is_its_own_error_through_inspect_and_stage(tmp_path, monkeypatch, caplog):
    b = _bundle(tmp_path / "mk")
    db, cd = _live(tmp_path)
    _disk_refuses_the_extraction(monkeypatch)
    with caplog.at_level(logging.WARNING, logger=bundle.log.name):
        with pytest.raises(bundle.ExtractionWriteError) as inspected:
            bundle.inspect_bundle(b)
        with pytest.raises(bundle.ExtractionWriteError) as staged:
            bundle.stage_bundle_restore(db, cd, b, keep_config=False)
    assert str(inspected.value).startswith("could not write the extraction beside the bundle (No space left on device)")
    assert str(staged.value).startswith("could not write the extraction beside motif.db (No space left on device)")
    for e in (inspected.value, staged.value):
        assert e.out_of_space and "not a motif bundle" not in str(e) and str(tmp_path) not in str(e)
    said = [r for r in caplog.records if r.name == bundle.log.name]
    assert not any("refused while reading the archive" in r.getMessage() for r in said), "never blames the archive"
    assert any(r.levelno == logging.ERROR and "the disk refused it" in r.getMessage() and ".bundle-stage-" in r.getMessage()
               for r in said), "the log names where the disk refused it"
    assert bundle.pending_members(db, cd) == [] and not bundle.STAGING_LOCK.locked()
    assert not list(cd.glob(".bundle-*")) and not list(b.parent.glob(".bundle-*"))


def test_a_read_fault_during_the_extraction_is_still_a_refusal(tmp_path, monkeypatch, caplog):
    b = _bundle(tmp_path / "mk")
    db, cd = _live(tmp_path)

    def read(self, n=-1):
        raise OSError(errno.EIO, os.strerror(errno.EIO))
    monkeypatch.setattr(bundle._HashingReader, "read", read)
    with caplog.at_level(logging.WARNING, logger=bundle.log.name):
        c = bundle.inspect_bundle(b)
    assert not c.ok and c.error.startswith("not a motif bundle:") and "Input/output error" in c.error, c.error
    assert any("refused while reading the archive" in r.getMessage() for r in caplog.records)
    with pytest.raises(ValueError, match="not a motif bundle"):
        bundle.stage_bundle_restore(db, cd, b, keep_config=False)


@pytest.mark.parametrize("err, at, status", [(errno.ENOSPC, "write", 507), (errno.EDQUOT, "write", 507),
                                             (errno.EACCES, "open", 500), (errno.EROFS, "open", 500)],
                         ids=["ENOSPC", "EDQUOT", "EACCES", "EROFS"])
def test_the_endpoints_answer_a_write_fault_in_words(api, tmp_path, monkeypatch, err, at, status):
    client, cd, _ = api
    b = _bundle(tmp_path / "mk")
    data = b.read_bytes()
    shutil.copyfile(b, cd / "backups" / b.name)
    _disk_refuses_the_extraction(monkeypatch, err, at)
    calls = {
        "preview": lambda: client.post("/api/admin/database-restore", json={"name": b.name}, headers=_H),
        "stage": lambda: client.post("/api/admin/database-restore", json={"name": b.name, "confirm": True}, headers=_H),
        "upload": lambda: client.post("/api/admin/database-restore/upload", headers=_H,
                                      files={"file": (b.name, data, "application/gzip")}),
    }
    for flow, call in calls.items():
        r = call()
        assert r.status_code == status, f"{flow}: {r.text}"
        detail = r.json()["detail"]
        assert detail.startswith("could not write the extraction beside") and os.strerror(err) in detail, f"{flow}: {detail}"
        assert "not a motif bundle" not in detail and str(tmp_path) not in detail, f"{flow}: {detail}"
    assert client.get("/api/admin/database-restore/pending", headers=_H).json()["members"] == []


# ── 4. a StagingError never names an absolute path ───────────────────

def _real_paths(tmp_path: Path) -> tuple[str, str]:
    return str(tmp_path), os.path.realpath(tmp_path)


def test_a_directory_where_the_staged_cookies_go_is_named_by_member_and_errno(tmp_path, caplog):
    db, cd = _live(tmp_path)
    (cd / (bundle.COOKIES_PENDING + ".tmp")).mkdir()  # the review's probe: EISDIR, its filename in the error
    with caplog.at_level(logging.ERROR, logger=bundle.log.name), pytest.raises(bundle.StagingError) as exc:
        bundle.stage_bundle_restore(db, cd, _bundle(tmp_path / "mk"), keep_config=False)
    msg = str(exc.value)
    assert msg.startswith("not staged: cookies.txt could not be staged (IsADirectoryError: Is a directory)"), msg
    assert not any(p in msg for p in _real_paths(tmp_path)), msg
    assert any(str(cd) in r.getMessage() for r in caplog.records if r.levelno == logging.ERROR), "the log keeps the path"
    assert bundle.pending_members(db, cd) == []


def _unlink_refused_naming_the_file(monkeypatch, *names: str) -> None:
    real = Path.unlink

    def unlink(self, *a, **k):
        if self.name in names:
            raise PermissionError(errno.EACCES, os.strerror(errno.EACCES), str(self))
        return real(self, *a, **k)
    monkeypatch.setattr(Path, "unlink", unlink)


@pytest.mark.parametrize("flow", ["stage a snapshot beside a stuck config", "cancel a stuck config",
                                  "cancel a stuck database", "unstage after a post-swap fault"])
def test_every_staging_error_says_errno_words_never_the_config_dir(tmp_path, monkeypatch, flow):
    db, cd = _live(tmp_path)
    snap = _snapshot(tmp_path / "src")
    b = _bundle(tmp_path / "mk")
    with pytest.raises(bundle.StagingError) as exc:
        if flow == "stage a snapshot beside a stuck config":
            (cd / bundle.CONFIG_PENDING).write_text("plex: {}\n")
            _unlink_refused_naming_the_file(monkeypatch, bundle.CONFIG_PENDING)
            bundle.stage_snapshot_restore(db, cd, snap)
        elif flow == "cancel a stuck config":
            (cd / bundle.CONFIG_PENDING).write_text("plex: {}\n")
            _unlink_refused_naming_the_file(monkeypatch, bundle.CONFIG_PENDING)
            bundle.cancel_pending(db, cd)
        elif flow == "cancel a stuck database":
            bundle.stage_snapshot_restore(db, cd, snap)
            _unlink_refused_naming_the_file(monkeypatch, db_backup.restore_pending_path(db).name)
            bundle.cancel_pending(db, cd)
        else:
            (cd / (bundle.COOKIES_PENDING + ".tmp")).mkdir()
            _unlink_refused_naming_the_file(monkeypatch, db_backup.restore_pending_path(db).name)
            bundle.stage_bundle_restore(db, cd, b, keep_config=False)
    msg = str(exc.value)
    assert "PermissionError: Permission denied" in msg, msg
    assert not any(p in msg for p in _real_paths(tmp_path)), msg


# ── 5. the staged database is owner-only ─────────────────────────────

def test_the_extracted_database_stays_owner_only_through_the_boot_swap(tmp_path):
    db, cd = _live(tmp_path)
    old = os.umask(0o002)  # the Unraid template's UMASK
    try:
        assert bundle.stage_bundle_restore(db, cd, _bundle(tmp_path / "mk"), keep_config=True).staged == ["database"]
    finally:
        os.umask(old)
    assert stat.S_IMODE(db_backup.restore_pending_path(db).stat().st_mode) == 0o600
    assert db_backup.apply_pending_restore(db, cd, now_stamp=STAMP2)["applied"] is True
    assert stat.S_IMODE(db.stat().st_mode) == 0o600 and _marker_rows(db) == 1


def test_a_share_that_refuses_chmod_is_named_when_the_snapshot_copy_cannot_be_narrowed(tmp_path, monkeypatch, caplog):
    db, cd = _live(tmp_path)
    snap = _snapshot(tmp_path / "src")
    snap.chmod(0o664)
    _refuse_chmod(monkeypatch)
    old = os.umask(0o002)
    try:
        with caplog.at_level(logging.WARNING, logger=db_backup.log.name):
            assert bundle.stage_snapshot_restore(db, cd, snap).ok
    finally:
        os.umask(old)
    assert stat.S_IMODE(db_backup.restore_pending_path(db).stat().st_mode) == 0o600, "born owner-only before any chmod"
    assert any("could not make the pending database owner-only" in r.getMessage() for r in caplog.records)
