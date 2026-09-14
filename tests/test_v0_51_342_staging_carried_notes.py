"""v0.51.342: the staging notes carried from the v0.51.341 review.

  1. An in-place cookies restore leaves the mounted file's mode as the host set it; a renamed-into-place one still ends 0600.
  2. The settings STAGE / listed RESTORE / CANCEL catches show motif's own words, never the raw JSON, and re-read the banner.
  3. A member that fails after the database swap unstages the whole bundle, and the refusal says what that leaves.
  4. A wrong-typed leaf in a bundle motif.yaml is refused by its dotted key; every valid config still parses and stages.
  5. An in-place write that fails after its truncate writes the original bytes back, so no boot copies a partial file.
"""
from __future__ import annotations

import errno
import json
import logging
import os
import shutil
import stat
import subprocess
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from _slice_helpers import slice_between
from app.core import bundle, config_file, db_backup
from app.core.auth import create_admin, init_auth_schema
from app.core.config_file import MotifConfig
from app.core.db import init_db
from tests.test_v0_51_339_bundle_staging_boot import _H, LIVE_YAML, _bundle, _live
from tests.test_v0_51_341_staging_boot_hardening import _replace_refused_onto, _unlink_refused_for

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
STAMP = "20260913-010203"
IN_PLACE = pytest.mark.parametrize("err", [errno.EBUSY, errno.EXDEV, errno.EPERM], ids=["EBUSY", "EXDEV", "EPERM"])
_real_write = os.write
_NODE = shutil.which("node")

if os.environ.get("MOTIF_REQUIRE_NODE") and not _NODE:
    raise RuntimeError("MOTIF_REQUIRE_NODE=1 but node is not on PATH — the settings restore harness would silently not run")

needs_node = pytest.mark.skipif(not _NODE, reason="node not installed")


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


# ── 1. a mounted cookies file keeps the host's mode ──────────────────

@IN_PLACE
def test_an_in_place_cookies_restore_leaves_the_mounted_files_mode_as_the_host_set_it(tmp_path, monkeypatch, caplog, err):
    _, cd = _live(tmp_path)
    target = cd / "cookies.txt"
    target.chmod(0o644)  # a cookies file other containers, under other uids, read too
    (cd / bundle.COOKIES_PENDING).write_text("# bundle cookies\n")
    _replace_refused_onto(monkeypatch, "cookies.txt", err)
    with caplog.at_level(logging.INFO, logger=bundle.log.name):
        res = bundle.apply_pending_cookies(cd, target, now_stamp=STAMP)
    assert res["errors"] == {} and target.read_text() == "# bundle cookies\n", "written into the mounted file"
    assert stat.S_IMODE(target.stat().st_mode) == 0o644, "0600 would lock every other reader of the mount out"
    assert any(r.levelno == logging.INFO and "mode is left as the host set it" in r.getMessage() for r in caplog.records)
    assert stat.S_IMODE((cd / f"cookies.txt.prerestore-{STAMP}").stat().st_mode) == 0o600, "motif's own undo copy stays owner-only"


def test_a_renamed_into_place_cookies_restore_still_ends_owner_only(tmp_path):
    _, cd = _live(tmp_path)
    target = cd / "cookies.txt"
    target.chmod(0o644)
    (cd / bundle.COOKIES_PENDING).write_text("# bundle cookies\n")
    (cd / bundle.COOKIES_PENDING).chmod(0o644)  # a pending staged by .336-.338
    old = os.umask(0o022)
    try:
        res = bundle.apply_pending_cookies(cd, target, now_stamp=STAMP)
    finally:
        os.umask(old)
    assert res["errors"] == {} and target.read_text() == "# bundle cookies\n"
    assert stat.S_IMODE(target.stat().st_mode) == 0o600, "a file motif renames into place is its own — owner-only"


# ── 2. the settings restore catches ──────────────────────────────────

_GW = ("function gatewayTimeoutNote(err) {", "\n  async function api(method, path, body) {")
_STAGE = ("document.getElementById('database-restore-stage-btn')?.addEventListener('click', async () => {",
          "\n    function showStaged(msg) {")
_RESTORE = ("async function restoreFromName(name) {", "\n    listEl.addEventListener('click', async (ev) => {")
_CANCEL = ("const cancelBtn = document.getElementById('database-restore-cancel-btn');", "\n    refreshPending();\n")

_HARNESS = r"""
const vm = require('vm');
const { gw, stage, restore, cancel, cases } = JSON.parse(require('fs').readFileSync(0, 'utf8'));
function contextFor(c) {
  const seen = { alerts: [], refreshed: 0 };
  const listeners = {};
  const el = (id) => ({ id, checked: false, hidden: false, dataset: {}, textContent: '', className: '',
    addEventListener: (type, fn) => { listeners[id] = fn; }, scrollIntoView() {} });
  const ctx = vm.createContext({
    alert: (m) => seen.alerts.push(String(m)), confirm: () => true,
    refreshPending: () => { seen.refreshed += 1; },
    api: async () => { const e = new Error(c.message); if ('status' in c) e.status = c.status; e.detail = c.detail; throw e; },
    hideBundlePreview() {}, showStaged() { seen.staged = true; }, showBundlePreview() {},
    previewEl: { dataset: { name: 'motif-bundle-20260912-040000.tar.gz' } },
    pendingBanner: el('database-restore-pending'), restoreStatus: el('database-restore-status'),
    document: { getElementById: (id) => el(id) },
  });
  vm.runInContext(gw, ctx);
  return { ctx, seen, listeners };
}
(async () => {
  const out = [];
  for (const c of cases) {
    const row = {};
    let r = contextFor(c);
    vm.runInContext(stage, r.ctx);
    await r.listeners['database-restore-stage-btn']();
    row.stage = r.seen;
    r = contextFor(c);
    vm.runInContext(restore, r.ctx);
    await vm.runInContext('restoreFromName', r.ctx)('motif-20260101-000000.db');
    row.restore = r.seen;
    r = contextFor(c);
    vm.runInContext(cancel, r.ctx);
    await r.listeners['database-restore-cancel-btn']();
    row.cancel = r.seen;
    out.push(row);
  }
  process.stdout.write(JSON.stringify(out));
})().catch((e) => { console.error((e && e.stack) || e); process.exit(1); });
"""


def _node(script: str, payload: dict) -> list:
    r = subprocess.run([_NODE, "-e", script], input=json.dumps(payload),
                       capture_output=True, text=True, timeout=60, cwd=REPO)
    assert r.returncode == 0, r.stderr[-1500:]
    return json.loads(r.stdout)


@needs_node
def test_a_refused_stage_restore_or_cancel_shows_motifs_words_and_re_reads_the_banner():
    detail = ("not staged: motif.yaml.restore-pending from an earlier restore could not be removed "
              "(Permission denied) — nothing was staged")
    cases = [
        {"status": 500, "detail": detail, "message": "500: " + json.dumps({"detail": detail})},
        {"detail": None, "message": "Failed to fetch"},
        {"status": 504, "detail": None, "message": "504: <html>Gateway Time-out</html>"},
    ]
    staging, network, gateway = _node(_HARNESS, {
        "gw": slice_between(APP_JS, *_GW), "stage": slice_between(APP_JS, *_STAGE),
        "restore": slice_between(APP_JS, *_RESTORE), "cancel": slice_between(APP_JS, *_CANCEL), "cases": cases})
    for flow, word in (("stage", "Restore failed: "), ("restore", "Restore failed: "), ("cancel", "Cancel failed: ")):
        assert staging[flow]["alerts"] == [word + detail], f"{flow}: the StagingError's words, not its JSON"
        assert network[flow]["alerts"] == [word + "Failed to fetch"], f"{flow}: no detail — the message stands in"
        for run in (staging, network, gateway):
            assert run[flow]["refreshed"] == 1, f"{flow}: a stage or cancel that stopped short can change what is pending"
    for flow in ("stage", "restore"):
        assert "reverse proxy timed out" in gateway[flow]["alerts"][0], "the gateway note still wins"


# ── 3. a member that fails after the database swap ───────────────────

def _no_space_for(monkeypatch, pending_name: str) -> None:
    real = os.open

    def open_(path, flags, *a, **k):
        if Path(path).name == pending_name + ".tmp":
            raise OSError(errno.ENOSPC, os.strerror(errno.ENOSPC))
        return real(path, flags, *a, **k)
    monkeypatch.setattr(os, "open", open_)


@pytest.mark.parametrize("earlier", [False, True], ids=["nothing-staged-before", "an-earlier-bundle-staged"])
@pytest.mark.parametrize("fails", [bundle.CONFIG_PENDING, bundle.COOKIES_PENDING])
def test_a_member_that_fails_after_the_database_swap_unstages_the_whole_bundle(tmp_path, monkeypatch, caplog, fails, earlier):
    db, cd = _live(tmp_path)
    if earlier:
        bundle.stage_bundle_restore(db, cd, _bundle(tmp_path / "a", token="EARLIER-TOKEN"), keep_config=False)
    b = _bundle(tmp_path / "b")
    _no_space_for(monkeypatch, fails)
    with caplog.at_level(logging.ERROR, logger=bundle.log.name), pytest.raises(bundle.StagingError) as exc:
        bundle.stage_bundle_restore(db, cd, b, keep_config=False)
    msg = str(exc.value)
    assert bundle.pending_members(db, cd) == [], "a database staged without its config applies at boot beside the live motif.yaml"
    assert msg.startswith(f"not staged: {fails.removesuffix(db_backup.RESTORE_PENDING_SUFFIX)} could not be staged"), msg
    assert "nothing from this bundle is staged" in msg
    assert ("were already dropped" in msg) is earlier, msg
    assert ("the database it staged was already replaced" in msg) is earlier, msg
    assert "BUNDLE-TOKEN" not in msg and not list(cd.glob("*.tmp")) and not list(db.parent.glob("*.tmp"))
    assert any("unstaging this bundle" in r.getMessage() for r in caplog.records)


def test_keep_config_never_reaches_the_member_staging(tmp_path, monkeypatch):
    db, cd = _live(tmp_path)
    _no_space_for(monkeypatch, bundle.CONFIG_PENDING)
    assert bundle.stage_bundle_restore(db, cd, _bundle(tmp_path / "b"), keep_config=True).staged == ["database"]
    assert bundle.pending_members(db, cd) == ["database"]


def test_a_post_swap_failure_that_cannot_unstage_the_database_names_what_applies(tmp_path, monkeypatch):
    db, cd = _live(tmp_path)
    b = _bundle(tmp_path / "b")
    _no_space_for(monkeypatch, bundle.COOKIES_PENDING)
    _unlink_refused_for(monkeypatch, db_backup.restore_pending_path(db).name)
    with pytest.raises(bundle.StagingError) as exc:
        bundle.stage_bundle_restore(db, cd, b, keep_config=False)
    msg = str(exc.value)
    assert bundle.pending_members(db, cd) == ["database"], "the config this call staged is gone; the database would not go"
    assert "motif.db.restore-pending could not be removed" in msg and "it applies at restart" in msg, msg
    assert "nothing from this bundle is staged" not in msg


def test_the_endpoint_answers_a_post_swap_failure_in_words_with_nothing_pending(api, tmp_path, monkeypatch):
    client, cd = api
    b = _bundle(tmp_path / "mk")
    shutil.copyfile(b, cd / "backups" / b.name)
    _no_space_for(monkeypatch, bundle.COOKIES_PENDING)
    r = client.post("/api/admin/database-restore", json={"name": b.name, "confirm": True, "keep_config": False}, headers=_H)
    assert r.status_code == 500, r.text
    assert r.json()["detail"].startswith("not staged: cookies.txt could not be staged"), r.text
    assert client.get("/api/admin/database-restore/pending", headers=_H).json()["members"] == []


# ── 4. wrong-typed leaves ────────────────────────────────────────────

_WRONG_LEAVES = [
    ("plex:\n  url: 53172\n", "plex.url", "53172"),
    ("paths:\n  cookies_file: 53172\n", "paths.cookies_file", "53172"),
    ("paths:\n  cookies_file: null\n", "paths.cookies_file", None),
    ("notifications:\n  apprise_urls: discord://HOOK-VALUE\n", "notifications.apprise_urls", "HOOK-VALUE"),
    ("plex:\n  enabled: 1\n", "plex.enabled", None),
    ("database_backup:\n  retention: true\n", "database_backup.retention", None),
    ("downloads:\n  rate_per_hour: THIRTY-VALUE\n", "downloads.rate_per_hour", "THIRTY-VALUE"),
    ("loudness:\n  target_lufs: LOUD-VALUE\n", "loudness.target_lufs", "LOUD-VALUE"),
    ("loudness:\n  target_lufs: false\n", "loudness.target_lufs", None),
    ("notifications:\n  events: [EVENT-VALUE]\n", "notifications.events", "EVENT-VALUE"),
    ("plex:\n  section_exclude:\n", "plex.section_exclude", None),
    ("schema_version: ONE-VALUE\n", "schema_version", "ONE-VALUE"),
]


@pytest.mark.parametrize("bad_yaml, key, value", _WRONG_LEAVES, ids=[k for _, k, _ in _WRONG_LEAVES])
def test_a_wrong_typed_leaf_is_refused_by_its_dotted_key_never_its_value(tmp_path, bad_yaml, key, value):
    flat, err = bundle.flatten_config(bad_yaml, side="bundle")
    assert flat == {} and err and err.startswith(key + " must be "), err
    if value:
        assert value not in err
    db, cd = _live(tmp_path)
    b = _bundle_with(tmp_path / "bad", bad_yaml)
    assert bundle.preview(b, cd / "motif.yaml")["config_parse_error"]["bundle"] == err
    with pytest.raises(ValueError, match=key.replace(".", r"\.")):
        bundle.stage_bundle_restore(db, cd, b, keep_config=False)
    assert bundle.pending_members(db, cd) == []
    assert bundle.stage_bundle_restore(db, cd, b, keep_config=True).staged == ["database"]


def test_the_probed_leaves_load_without_raising_then_break_the_reads_boot_makes(tmp_path, no_env):
    from app.config import Settings
    from app.core.plex import PlexClient, PlexConfig
    (tmp_path / "motif.yaml").write_text("paths:\n  cookies_file: 5\n")
    with pytest.raises(TypeError):
        Settings(config_dir=tmp_path, data_dir=tmp_path / "data").cookies_file  # the boot's apply_pending_cookies argument
    (tmp_path / "motif.yaml").write_text("paths:\n  cookies_file: null\n")
    with pytest.raises(TypeError):
        Settings(config_dir=tmp_path, data_dir=tmp_path / "data").cookies_file
    (tmp_path / "motif.yaml").write_text("plex:\n  url: 5\n")
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    with pytest.raises(AttributeError):
        PlexClient(PlexConfig(url=s.plex_url, token="t", movie_section="1", tv_section="2"))
    (tmp_path / "motif.yaml").write_text("notifications:\n  apprise_urls: discord://x\n")
    assert Settings(config_dir=tmp_path, data_dir=tmp_path / "data").cfg.notifications.apprise_urls == "discord://x", \
        "hydrated as a string — every reader that iterates it walks characters"


def _bundle_with(root: Path, yaml_text: str) -> Path:
    from app.core.db import CURRENT_SCHEMA_VERSION
    src = root / "src"
    src.mkdir(parents=True, exist_ok=True)
    init_db(src / "motif.db")
    (src / "motif.yaml").write_text(yaml_text)
    bf = bundle.create_bundle(src / "motif.db", src, config_file=src / "motif.yaml", cookies_file=None, themes_dir=None,
                              now_stamp="20260912-040000", motif_version="0.51.342", schema_version=CURRENT_SCHEMA_VERSION)
    return src / "backups" / bf.name


REALISTIC_YAML = """\
schema_version: 1
paths:
  themes_dir: /data/media/themes
  cookies_file: /config/yt/cookies.txt
  min_free_disk_mb: 750
plex:
  enabled: true
  url: https://plex.example.lan:32400
  token: aBcD-eFgH-ijKL
  movie_section: '1'
  tv_section: '2'
  analyze_after_placement: false
  section_exclude: ['7', '9']
  section_include: []
  tmdb_api_key: 0f1e2d3c4b5a
downloads:
  rate_per_hour: 45
  rate_mode: adaptive
  adaptive_min_per_hour: 5
  adaptive_max_per_hour: 90
  concurrency: 2
  audio_quality: 0
  geo_bypass: true
  geo_bypass_country: US
  proxy_url: socks5://proxyuser:proxypass@proxy.example.lan:1080
matching:
  strict_edition: true
  plus_mode: separator
placement:
  auto_place: true
  default_method: file
  auto_restore_sidecar: true
  auto_restore_plex_upload: false
sync:
  db_url: https://app.lizardbyte.dev/ThemerrDB
  cron: 0 13 * * *
  source: git
  database_url: https://codeload.github.com/LizardByte/ThemerrDB/tar.gz/database
  git_url: https://github.com/LizardByte/ThemerrDB.git
  git_branch: database
  auto_enum_after_sync: false
  auto_enum_after_cron_sync: true
  auto_download_new_themes_for_unthemed_rows: false
web:
  host: 0.0.0.0
  port: 5309
  trust_forward_auth: true
  forward_auth_allowed_ips: [172.18.0.5, 172.18.0.0/16]
  forward_auth_trusted_proxies: [172.18.0.5]
  cookie_secure: 'on'
runtime:
  dry_run_default: false
  log_level: INFO
notifications:
  apprise_urls:
    - discord://1234/abcd
    - tgram://bottoken/chatid
  apprise_external_url: http://apprise.example.lan:8000/notify/motif
  events:
    sync_completed: true
    theme_added: false
  inbox_events:
    theme_added: true
database_backup:
  enabled: true
  cron: 0 4 * * *
  retention: 14
  bundle: true
loudness:
  normalize_on_download: true
  normalize_auto_added: false
  target_lufs: -16
"""


def _valid_configs() -> dict[str, str]:
    from tests.test_v0_51_336_bundle_restore import _live as live_336
    from tests.test_v0_51_339_bundle_inspect_preview import GOOD_YAML, SECRETS_YAML
    return {
        "default MotifConfig, rendered": config_file._serialize(MotifConfig(), updated_by="test"),
        "realistic full config": REALISTIC_YAML,
        "a saved realistic config": _saved(REALISTIC_YAML),
        "339 LIVE_YAML": LIVE_YAML, "339 GOOD_YAML": GOOD_YAML, "339 SECRETS_YAML": SECRETS_YAML,
        "336 live": _live_336_yaml(live_336),  # v0.51.342: the config 336's fixture actually writes — tracks it instead of a hand-copied string that could drift
        "paths: {}": "paths: {}\n", "themes_dir only": "paths:\n  themes_dir: /data/themes\n",
        "an int where a float is declared": "loudness:\n  target_lufs: -18\n",
        "an unknown key": "future_section:\n  anything: 5\nplex:\n  someday_key: [1]\n",
    }


def _live_336_yaml(live_336) -> str:
    import tempfile
    d = Path(tempfile.mkdtemp())
    try:
        _db, cd = live_336(d)  # v0.51.342: uses the 336 fixture rather than restating its motif.yaml by hand
        return (cd / "motif.yaml").read_text()
    finally:
        shutil.rmtree(d)


def _saved(text: str) -> str:
    import tempfile
    d = Path(tempfile.mkdtemp())
    try:
        cf = config_file.ConfigFile(d / "motif.yaml")
        (d / "motif.yaml").write_text(text)
        cf.save(cf.load(), updated_by="test")
        return (d / "motif.yaml").read_text()
    finally:
        shutil.rmtree(d)


@pytest.fixture
def no_env(monkeypatch):
    for env_name, _dotted, _conv in config_file.ENV_BINDINGS:
        monkeypatch.delenv(env_name, raising=False)


def test_every_valid_config_still_parses(no_env):
    for name, text in _valid_configs().items():
        flat, err = bundle.flatten_config(text, side="bundle")
        assert err is None, f"{name}: falsely refused ({err})"


def test_a_realistic_full_config_stages_and_loads_clean_at_boot(tmp_path, no_env):
    cfg = config_file.ConfigFile(tmp_path / "check.yaml")
    (tmp_path / "check.yaml").write_text(REALISTIC_YAML)
    assert config_file.validate(cfg.load()) == [], "the fixture is a config motif itself accepts"
    db, cd = _live(tmp_path)
    b = _bundle_with(tmp_path / "real", REALISTIC_YAML)
    p = bundle.preview(b, cd / "motif.yaml")
    assert p["config_parse_error"] == {"live": None, "bundle": None} and p["config_diff"]
    assert bundle.stage_bundle_restore(db, cd, b, keep_config=False).staged == ["database", "config"]


# ── 5. an in-place write that fails after its truncate ──────────────

def _disk_fills_mid_write(monkeypatch, new: bytes, *, write_back_fails: bool = False) -> None:
    real = os.write
    state = {"failed": False}

    def write(fd, buf):
        b = bytes(buf)
        if state["failed"]:
            if write_back_fails:
                raise OSError(errno.ENOSPC, os.strerror(errno.ENOSPC))
            return real(fd, buf)
        if b == new:
            return real(fd, b[: len(b) // 2])  # a short write: half of the restored bytes land
        if b and new.endswith(b):
            state["failed"] = True
            raise OSError(errno.ENOSPC, os.strerror(errno.ENOSPC))
        return real(fd, buf)
    monkeypatch.setattr(os, "write", write)


def _in_place_case(cd: Path, which: str):
    if which == "motif.yaml":
        live, pending, new = cd / "motif.yaml", cd / bundle.CONFIG_PENDING, b"plex:\n  token: BUNDLE-TOKEN\n" * 64
        return live, pending, new, lambda stamp: bundle.apply_pending_config(cd, now_stamp=stamp)
    live, pending, new = cd / "cookies.txt", cd / bundle.COOKIES_PENDING, b"# bundle cookies\n" * 64
    return live, pending, new, lambda stamp: bundle.apply_pending_cookies(cd, live, now_stamp=stamp)


@IN_PLACE
@pytest.mark.parametrize("which", ["motif.yaml", "cookies.txt"])
def test_an_in_place_write_that_fails_after_the_truncate_writes_the_original_back(tmp_path, monkeypatch, caplog, which, err):
    _, cd = _live(tmp_path)
    live, pending, new, apply = _in_place_case(cd, which)
    original = live.read_bytes()
    pending.write_bytes(new)
    _replace_refused_onto(monkeypatch, live.name, err)
    _disk_fills_mid_write(monkeypatch, new)
    with caplog.at_level(logging.ERROR, logger=bundle.log.name):
        res = apply(STAMP)
    assert res["applied"] == [] and res["errors"], res
    assert live.read_bytes() == original, "the truncated file has its own bytes back"
    assert pending.exists() and not list(cd.glob("*.restore-tmp"))
    said = [r.getMessage() for r in caplog.records]
    assert any("original bytes were written back" in m for m in said), said
    assert not any("restore it from its pre-restore copy" in m for m in said), "the live file is whole — nothing to restore by hand"
    monkeypatch.setattr(os, "write", _real_write)  # boot 2: space again, the mount still refuses a rename
    res = apply("20260914-010203")
    assert res["errors"] == {} and live.read_bytes() == new
    assert [p.read_bytes() for p in cd.glob(f"{live.name}.prerestore-*")] == [original], \
        "one undo copy, of the original — never a second one of a partial file"


def test_an_in_place_write_whose_write_back_fails_too_points_at_the_pre_restore_copy(tmp_path, monkeypatch, caplog):
    _, cd = _live(tmp_path)
    live, pending, new, apply = _in_place_case(cd, "cookies.txt")
    original = live.read_bytes()
    pending.write_bytes(new)
    _replace_refused_onto(monkeypatch, live.name, errno.EBUSY)
    _disk_fills_mid_write(monkeypatch, new, write_back_fails=True)
    with caplog.at_level(logging.ERROR, logger=bundle.log.name):
        res = apply(STAMP)
    assert res["applied"] == [] and "may be partly written" in next(iter(res["errors"].values()))
    assert live.read_bytes() != original and pending.exists()
    assert (cd / f"cookies.txt.prerestore-{STAMP}").read_bytes() == original
    said = [r.getMessage() for r in caplog.records]
    assert any("could not be written back" in m for m in said) and any("restore it from its pre-restore copy" in m for m in said)
