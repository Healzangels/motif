"""v0.51.339: review follow-ups for the backup bundle — inspect, preview, stage.

  1. A symlinked motif.yaml / cookies.txt is archived as its target's bytes (was a
     0-byte SYMTYPE that every restore refused); refusals carry the prefix once.
  2. Extracted motif.yaml / cookies.txt are 0600, so the .restore-pending files are too.
  3. The preview diff masks through GET /api/config's rule — one rule, keyed by key.
  4. A bundle motif.yaml that does not parse is named, never diffed, never staged.
  5. A malformed manifest is a refusal, never a 500.
  6. table_counts skips only a missing table.
  7. The census carries one placement per local_files row.
"""
from __future__ import annotations

import io
import json
import logging
import os
import shutil
import sqlite3
import stat
import subprocess
import tarfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core import bundle, db_backup
from app.core.auth import create_admin, init_auth_schema
from app.core.config_file import SECRET_CONFIG_KEYS
from app.core.db import CURRENT_SCHEMA_VERSION, init_db

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
NOW = "20260912-040000"
_H = {"X-Authentik-Username": "testadmin"}

_NODE = shutil.which("node")
if os.environ.get("MOTIF_REQUIRE_NODE") and not _NODE:
    raise RuntimeError("MOTIF_REQUIRE_NODE=1 but node is not on PATH — the restore-preview card check would not run")

GOOD_YAML = "plex:\n  url: http://plex:32400\n  token: BUNDLE-TOKEN\n"


def _bundle(root: Path, *, yaml_text: str = GOOD_YAML, cookies: bool = True, stamp: str = NOW) -> Path:
    src = root / "src"
    src.mkdir(parents=True, exist_ok=True)
    db = src / "motif.db"
    if not db.exists():
        init_db(db)
    cfg = src / "motif.yaml"
    cfg.write_bytes(yaml_text.encode("utf-8") if isinstance(yaml_text, str) else yaml_text)
    ck = src / "cookies.txt"
    ck.write_text("# cookies\n")
    bf = bundle.create_bundle(db, src, config_file=cfg, cookies_file=ck if cookies else None,
                              themes_dir=None, now_stamp=stamp, motif_version="0.51.339",
                              schema_version=CURRENT_SCHEMA_VERSION)
    return src / "backups" / bf.name


def _live(root: Path, yaml_text: str = "plex:\n  url: http://plex.old:32400\n  token: LIVE-TOKEN\n") -> tuple[Path, Path]:
    cd = root / "live"
    cd.mkdir(parents=True, exist_ok=True)
    db = cd / "motif.db"
    init_db(db)
    (cd / "motif.yaml").write_text(yaml_text)
    (cd / "cookies.txt").write_text("# live cookies\n")
    return db, cd


def _repack(b: Path, out: Path, mutate, *, mode: int | None = None) -> Path:
    members = {}
    with tarfile.open(b, "r:gz") as tar:
        for m in tar.getmembers():
            members[m.name] = tar.extractfile(m).read()
    mutate(members)
    with tarfile.open(out, "w:gz") as tar:
        for name, data in members.items():
            ti = tarfile.TarInfo(name)
            ti.size = len(data)
            if mode is not None:
                ti.mode = mode
            tar.addfile(ti, io.BytesIO(data))
    return out


def _manifest_edit(fn):
    def mutate(m):
        j = json.loads(m["manifest.json"])
        m["manifest.json"] = json.dumps(fn(j)).encode()
    return mutate


# ── 1. symlinked members ──────────────────────────────────────────────

def test_symlinked_config_and_cookies_archive_their_bytes_and_restore(tmp_path):
    real = tmp_path / "elsewhere"
    real.mkdir()
    (real / "motif.yaml").write_text(GOOD_YAML)
    (real / "cookies.txt").write_text("# linked cookies\n")
    src = tmp_path / "src"
    src.mkdir()
    db = src / "motif.db"
    init_db(db)
    (src / "motif.yaml").symlink_to(real / "motif.yaml")
    (src / "cookies.txt").symlink_to(real / "cookies.txt")
    bf = bundle.create_bundle(db, src, config_file=src / "motif.yaml", cookies_file=src / "cookies.txt",
                              themes_dir=None, now_stamp=NOW, motif_version="x",
                              schema_version=CURRENT_SCHEMA_VERSION)
    b = src / "backups" / bf.name
    with tarfile.open(b, "r:gz") as tar:
        members = {m.name: m for m in tar.getmembers()}
        assert all(m.isfile() and not m.issym() and not m.linkname for m in members.values())
        assert tar.extractfile("motif.yaml").read() == GOOD_YAML.encode()
        assert tar.extractfile("cookies.txt").read() == b"# linked cookies\n"
    c = bundle.inspect_bundle(b)
    assert c.ok, c.error
    assert str(real).encode() not in b.read_bytes() and str(real) not in json.dumps(c.manifest)
    ldb, cd = _live(tmp_path)
    assert bundle.stage_bundle_restore(ldb, cd, b, keep_config=False).staged == ["database", "config", "cookies"]
    assert (cd / bundle.COOKIES_PENDING).read_text() == "# linked cookies\n"


def test_extract_refusals_carry_the_prefix_once(tmp_path):
    b = _bundle(tmp_path)
    for name, mutate in (("extra", lambda m: m.__setitem__("../evil.sh", b"boom")),):
        c = bundle.inspect_bundle(_repack(b, tmp_path / f"{name}.tar.gz", mutate))
        assert not c.ok and c.error.startswith("not a motif bundle: ") and c.error.count("not a motif bundle:") == 1, c.error
    junk = tmp_path / "junk.tar.gz"
    junk.write_bytes(b"\x1f\x8b\x08\x00garbage")
    c = bundle.inspect_bundle(junk)
    assert c.error.startswith("not a motif bundle: ") and c.error.count("not a motif bundle:") == 1, c.error


# ── 2. file modes ─────────────────────────────────────────────────────

@pytest.mark.parametrize("archived_mode", [None, 0o644, 0o666])
def test_staged_config_and_cookies_are_0600_whatever_the_umask_or_the_archive_says(tmp_path, archived_mode):
    b = _bundle(tmp_path)
    if archived_mode is not None:
        b = _repack(b, tmp_path / "moded.tar.gz", lambda m: None, mode=archived_mode)
    db, cd = _live(tmp_path)
    old = os.umask(0o022)
    try:
        c = bundle.stage_bundle_restore(db, cd, b, keep_config=False)
        assert c.staged == ["database", "config", "cookies"]
        for pending in (bundle.CONFIG_PENDING, bundle.COOKIES_PENDING):
            assert stat.S_IMODE((cd / pending).stat().st_mode) == 0o600, pending
        bundle.apply_pending_config(cd, now_stamp="20260913-010203")
        bundle.apply_pending_cookies(cd, cd / "cookies.txt", now_stamp="20260913-010203")  # v0.51.339: the cookies leg is its own boot hook now
        for live in ("motif.yaml", "cookies.txt"):
            assert stat.S_IMODE((cd / live).stat().st_mode) == 0o600, live
    finally:
        os.umask(old)


# ── 3. one mask rule ──────────────────────────────────────────────────

SECRETS_YAML = """\
plex:
  url: http://plex:32400
  token: PLEXTOK-9f3
  tmdb_api_key: TMDBKEY-77a
  tvdb_api_key: TVDBKEY-12c
downloads:
  proxy_url: socks5://proxyuser:PROXYPASS-4e1@proxy.lan:1080
sync:
  git_url: https://gituser:GITPAT-0b2@git.example.com/mirror.git
  database_url: https://dbuser:DBPAT-5d9@codeload.example.com/db.tar.gz
  db_url: https://rmuser:RMPAT-8c6@remote.example.com/ThemerrDB
notifications:
  apprise_urls:
    - discord://123/DISCORDTOK-3aa
  apprise_external_url: http://apuser:APPASS-6f0@apprise.lan/notify
"""
_SECRET_VALUES = ("PLEXTOK-9f3", "TMDBKEY-77a", "TVDBKEY-12c", "PROXYPASS-4e1", "GITPAT-0b2",
                  "DBPAT-5d9", "RMPAT-8c6", "DISCORDTOK-3aa", "APPASS-6f0")


def _api(tmp_path, monkeypatch, yaml_text: str | None):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    tmp_path.mkdir(parents=True, exist_ok=True)
    if yaml_text is not None:
        (tmp_path / "motif.yaml").write_text(yaml_text)
    from app.config import Settings
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(settings.db_path)
    init_auth_schema(settings.db_path)
    create_admin(settings.db_path, username="testadmin", password="testpassword")
    from app.web import api as api_mod
    monkeypatch.setattr(api_mod, "log_event", lambda *a, **k: None)
    return TestClient(api_mod.create_app(settings)), settings


def test_preview_shows_every_secret_exactly_as_get_api_config_shows_it(tmp_path, monkeypatch):
    client, _ = _api(tmp_path, monkeypatch, SECRETS_YAML)
    r = client.get("/api/config", headers=_H)
    assert r.status_code == 200, r.text
    cfg = r.json()["config"]
    rows = {d["key"]: d for d in bundle.config_diff("", SECRETS_YAML)}
    for dotted in SECRET_CONFIG_KEYS:
        sec, leaf = dotted.split(".", 1)
        assert rows[dotted]["secret"] is True, dotted
        assert rows[dotted]["bundle"] == bundle._render(cfg[sec][leaf]), dotted
        assert rows[dotted]["live"] == "(unset)", dotted
    blob = json.dumps(rows) + r.text
    for s in _SECRET_VALUES:
        assert s not in blob, s
    assert rows["sync.git_url"]["bundle"] == "https://***@git.example.com/mirror.git"
    assert rows["notifications.apprise_urls"]["bundle"] == '["discord://***"]'
    assert rows["downloads.proxy_url"]["bundle"] == "***"
    assert cfg["plex"]["token_set"] is True and cfg["downloads"]["proxy_url_set"] is True
    assert cfg["notifications"]["apprise_urls_set_count"] == 1 and cfg["notifications"]["apprise_external_url_set"] is True


def test_get_api_config_with_nothing_set_is_unchanged(tmp_path, monkeypatch):
    client, settings = _api(tmp_path, monkeypatch, None)
    cfg = client.get("/api/config", headers=_H).json()["config"]
    for sec, leaf in (("plex", "token"), ("plex", "tvdb_api_key"), ("plex", "tmdb_api_key"),
                      ("downloads", "proxy_url"), ("notifications", "apprise_external_url")):
        assert cfg[sec][leaf] == "" and cfg[sec][f"{leaf}_set"] is False, (sec, leaf)
    assert cfg["notifications"]["apprise_urls"] == [] and cfg["notifications"]["apprise_urls_set_count"] == 0
    assert cfg["sync"]["git_url"] == settings.cfg.sync.git_url and "***" not in cfg["sync"]["git_url"]


def test_diff_masks_credentials_yet_shows_every_difference(tmp_path):
    live = ("plex:\n  token: TOK-A1\ndownloads:\n  proxy_url: socks5://u:PW-A2@proxy.lan:1080\n"
            "sync:\n  git_url: https://u:PAT-A3@host-one.example/x.git\n  db_url: https://u:PAT-A4@same.example/db\n"
            "notifications:\n  apprise_urls: [discord://1/HOOK-A5]\n")
    other = ("plex:\n  token: TOK-B1\ndownloads:\n  proxy_url: socks5://u:PW-B2@proxy.lan:1080\n"
             "sync:\n  git_url: https://u:PAT-B3@host-two.example/x.git\n  db_url: https://u:PAT-B4@same.example/db\n"
             "notifications:\n  apprise_urls: [discord://1/HOOK-B5]\n")
    d = {r["key"]: r for r in bundle.config_diff(live, other)}
    assert set(d) == {"plex.token", "downloads.proxy_url", "sync.git_url", "sync.db_url", "notifications.apprise_urls"}
    assert all(r["secret"] for r in d.values())
    assert d["sync.git_url"]["live"] == "https://***@host-one.example/x.git"
    assert d["sync.git_url"]["bundle"] == "https://***@host-two.example/x.git", "a changed host still shows"
    for k in ("plex.token", "downloads.proxy_url", "sync.db_url", "notifications.apprise_urls"):
        assert d[k]["live"] == d[k]["bundle"], f"{k}: differing secrets that mask alike are still listed"
    blob = json.dumps(d)
    assert not any(s in blob for s in ("TOK-", "PW-", "PAT-", "HOOK-"))


def test_diff_shows_the_settings_the_old_regex_hid(tmp_path):
    live = ("paths:\n  cookies_file: /config/cookies.txt\nweb:\n  cookie_secure: auto\n  trust_forward_auth: false\n"
            "  forward_auth_allowed_ips: [192.168.1.0/24]\n  forward_auth_trusted_proxies: [172.18.0.2]\n"
            "notifications:\n  events:\n    cookies_needed: true\n")
    other = ("paths:\n  cookies_file: /config/other-cookies.txt\nweb:\n  cookie_secure: 'on'\n  trust_forward_auth: true\n"
             "  forward_auth_allowed_ips: [192.168.9.0/24]\n  forward_auth_trusted_proxies: [172.18.0.9]\n"
             "notifications:\n  events:\n    cookies_needed: false\n")
    d = {r["key"]: r for r in bundle.config_diff(live, other)}
    assert d["web.forward_auth_allowed_ips"] == {"key": "web.forward_auth_allowed_ips", "secret": False,
                                                 "live": '["192.168.1.0/24"]', "bundle": '["192.168.9.0/24"]'}
    for k in ("paths.cookies_file", "web.cookie_secure", "web.trust_forward_auth",
              "web.forward_auth_trusted_proxies", "notifications.events.cookies_needed"):
        assert d[k]["secret"] is False and d[k]["live"] != bundle.MASK and d[k]["bundle"] != bundle.MASK, k


def test_unknown_keys_fall_back_on_their_last_segment_only():
    live = "extras:\n  my_api_key: K-LIVE\n  auth_mode: basic\n  cookie_jar: a\n  token:\n    name: n1\n"
    other = "extras:\n  my_api_key: K-BUNDLE\n  auth_mode: oidc\n  cookie_jar: b\n  token:\n    name: n2\n"
    d = {r["key"]: r for r in bundle.config_diff(live, other)}
    assert d["extras.my_api_key"]["secret"] and d["extras.my_api_key"]["bundle"] == bundle.MASK
    assert d["extras.auth_mode"] == {"key": "extras.auth_mode", "secret": False, "live": "basic", "bundle": "oidc"}
    assert d["extras.cookie_jar"]["bundle"] == "b" and d["extras.token.name"]["bundle"] == "n2"
    assert "K-BUNDLE" not in json.dumps(d)
    # v0.51.342: a mapping under the str leaf downloads.proxy_url is refused by its key before any diff — the mask rule still hides it whole
    nested = "downloads:\n  proxy_url:\n    user: u\n    pass: NESTED-PW-2\n"
    assert bundle.config_diff(live, other + nested) == []
    assert bundle.flatten_config(nested, side="bundle")[1] == "downloads.proxy_url must be a string, not dict"
    rows = {r["key"]: r for r in bundle._diff_rows({"downloads.proxy_url.pass": "NESTED-PW-1"},
                                                   {"downloads.proxy_url.pass": "NESTED-PW-2"})}
    assert rows["downloads.proxy_url.pass"]["secret"] and rows["downloads.proxy_url.pass"]["bundle"] == bundle.MASK, \
        "a mapping where a credential field belongs is hidden whole"
    assert "NESTED-PW" not in json.dumps(rows)


# ── 4. a bundle motif.yaml that does not parse ────────────────────────

def test_flatten_config_names_the_error_without_quoting_the_line(caplog):
    caplog.set_level(logging.WARNING, logger="app.core.bundle")
    flat, err = bundle.flatten_config('plex:\n  token: "SECRET-XYZ\n', side="bundle")
    assert flat == {} and err and "line" in err
    assert "SECRET-XYZ" not in err and "SECRET-XYZ" not in caplog.text
    assert any(r.levelno == logging.WARNING and "bundle" in r.getMessage() for r in caplog.records)
    flat, err = bundle.flatten_config("- a\n- b\n", side="live")
    assert flat == {} and "not a mapping" in err
    assert bundle.flatten_config(b"plex:\n  token: \xff\xfe\n", side="bundle")[1], "not UTF-8 is not parseable"
    assert bundle.flatten_config("", side="live") == ({}, None), "an empty file is the defaults, as at boot"
    assert bundle.flatten_config("a:\n  b: 1\n", side="live") == ({"a.b": 1}, None)


def test_preview_names_the_side_that_does_not_parse_and_diffs_nothing(tmp_path):
    bad = _bundle(tmp_path / "bad", yaml_text="plex: [unclosed\n  token: X\n")
    _, cd = _live(tmp_path)
    p = bundle.preview(bad, cd / "motif.yaml")
    assert p["config_diff"] == [] and p["config_parse_error"]["live"] is None and p["config_parse_error"]["bundle"]
    good = _bundle(tmp_path / "good")
    (cd / "motif.yaml").write_text("- not\n- a mapping\n")
    p = bundle.preview(good, cd / "motif.yaml")
    assert p["config_diff"] == [] and p["config_parse_error"]["live"] and p["config_parse_error"]["bundle"] is None
    (cd / "motif.yaml").write_text("plex:\n  token: LIVE\n")
    assert bundle.preview(good, cd / "motif.yaml")["config_parse_error"] == {"live": None, "bundle": None}


@pytest.mark.parametrize("bad_yaml", ["plex: [unclosed\n", "- a list\n", b"plex:\n  token: \xff\n"])
def test_stage_refuses_a_bundle_config_that_would_crash_the_boot(tmp_path, bad_yaml):
    b = _bundle(tmp_path, yaml_text=bad_yaml)
    db, cd = _live(tmp_path)
    with pytest.raises(ValueError, match="does not parse"):
        bundle.stage_bundle_restore(db, cd, b, keep_config=False)
    assert bundle.pending_members(db, cd) == [], "nothing staged, not even the database"
    assert bundle.stage_bundle_restore(db, cd, b, keep_config=True).staged == ["database"]


def test_stage_endpoint_answers_422_then_stages_with_keep_config(tmp_path, monkeypatch):
    client, settings = _api(tmp_path / "app", monkeypatch, "plex:\n  token: LIVE-TOKEN\n")
    b = _bundle(tmp_path / "mk", yaml_text="plex: [unclosed\n")
    (settings.config_dir / "backups").mkdir(exist_ok=True)
    (settings.config_dir / "backups" / b.name).write_bytes(b.read_bytes())
    r = client.post("/api/admin/database-restore", json={"name": b.name}, headers=_H)
    assert r.status_code == 200 and r.json()["preview"]["config_parse_error"]["bundle"]
    r = client.post("/api/admin/database-restore", json={"name": b.name, "confirm": True, "keep_config": False}, headers=_H)
    assert r.status_code == 422 and "does not parse" in r.json()["detail"]
    assert client.get("/api/admin/database-restore/pending", headers=_H).json()["members"] == []
    r = client.post("/api/admin/database-restore", json={"name": b.name, "confirm": True, "keep_config": True}, headers=_H)
    assert r.status_code == 200 and r.json()["members"] == ["database"]


def _run_card(previews: list[dict]) -> list[dict]:
    i = APP_JS.index("function showBundlePreview(r) {")
    fn = APP_JS[i:APP_JS.index("\n    function hideBundlePreview(", i)]
    script = (
        "const els = {};\n"
        "const mk = (id) => (els[id] = els[id] || { id, textContent: '', innerHTML: '', hidden: true, checked: false,"
        " disabled: false, dataset: {}, scrollIntoView() {} });\n"
        "const document = { getElementById: (id) => mk(id) };\n"
        "const htmlEscape = (s) => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');\n"
        "const previewEl = mk('database-restore-preview');\n"
        + fn +
        "\nconst out = [];\n"
        "for (const pv of JSON.parse(process.env.PREVIEWS)) {\n"
        "  showBundlePreview({ preview: pv });\n"
        "  const k = els['database-restore-keep-config'], d = els['restore-preview-diff'];\n"
        "  out.push({ config: els['restore-preview-config'].textContent, cookies: els['restore-preview-cookies'].textContent,"
        " diffHidden: d.hidden, diffHtml: d.innerHTML, keepChecked: k.checked, keepDisabled: k.disabled });\n"
        "}\n"
        "console.log(JSON.stringify(out));\n"
    )
    r = subprocess.run([_NODE, "-e", script], capture_output=True, text=True, timeout=60,
                       env={**os.environ, "PREVIEWS": json.dumps(previews)})
    assert r.returncode == 0, r.stderr
    return json.loads(r.stdout)


@pytest.mark.skipif(not _NODE, reason="node not installed")
def test_card_forces_keep_config_and_hides_the_diff_when_the_bundle_config_does_not_parse(tmp_path):
    _, cd = _live(tmp_path, yaml_text="plex:\n  url: http://plex.old:32400\n  token: LIVE-TOKEN\n")
    bad = bundle.preview(_bundle(tmp_path / "bad", yaml_text="plex: [unclosed\n"), cd / "motif.yaml")
    good = bundle.preview(_bundle(tmp_path / "good", yaml_text="plex:\n  url: http://plex.old:32400\n  token: OTHER\n"),
                          cd / "motif.yaml")
    bad_card, good_card = _run_card([bad, good])
    assert "could not be parsed" in bad_card["config"] and "bundle" in bad_card["config"]
    assert bad_card["diffHidden"] is True and bad_card["keepChecked"] is True and bad_card["keepDisabled"] is True
    assert "will replace yours" not in bad_card["cookies"], "cookies stay when the config is forced kept"
    assert good_card["keepChecked"] is False and good_card["keepDisabled"] is False, "the next preview resets the lock"
    assert good_card["diffHidden"] is False and "plex.token" in good_card["diffHtml"]
    assert "(secret — differs, masked)" in good_card["diffHtml"], "sides that mask alike say why they are listed"


# ── 5. malformed manifests ────────────────────────────────────────────

@pytest.mark.parametrize("shape, fn", [
    ("root is a list", lambda j: [j]),
    ("root is a string", lambda j: "motif-bundle"),
    ("members is a list", lambda j: {**j, "members": list(j["members"])}),
    ("members is a string", lambda j: {**j, "members": "motif.db"}),
    ("member meta is a list", lambda j: {**j, "members": {**j["members"], "motif.db": ["sha"]}}),
    ("member meta is a string", lambda j: {**j, "members": {**j["members"], "motif.db": "sha"}}),
])
def test_inspect_refuses_malformed_manifest_shapes_without_raising(tmp_path, shape, fn):
    b = _repack(_bundle(tmp_path), tmp_path / "shape.tar.gz", _manifest_edit(fn))
    c = bundle.inspect_bundle(b)
    assert not c.ok and c.error.startswith("not a motif bundle: "), (shape, c.error)
    with pytest.raises(ValueError):
        bundle.preview(b, None)


def test_preview_survives_a_census_that_is_not_a_list(tmp_path):
    b = _repack(_bundle(tmp_path), tmp_path / "census.tar.gz",
                _manifest_edit(lambda j: {**j, "themes_census": 5, "counts": [1]}))
    p = bundle.preview(b, None)
    assert p["manifest"]["census_rows"] == 0 and p["manifest"]["counts"] == {}


def test_upload_of_a_malformed_manifest_is_a_422_not_a_500(tmp_path, monkeypatch):
    client, _ = _api(tmp_path / "app", monkeypatch, None)
    b = _repack(_bundle(tmp_path / "mk"), tmp_path / "list.tar.gz", _manifest_edit(lambda j: [j]))
    r = client.post("/api/admin/database-restore/upload", headers=_H,
                    files={"file": ("motif-bundle-20260912-040000.tar.gz", b.read_bytes(), "application/gzip")})
    assert r.status_code == 422 and "not a motif bundle" in r.json()["detail"]


# ── 6. table_counts ───────────────────────────────────────────────────

def test_table_counts_skips_only_a_missing_table(tmp_path, caplog):
    caplog.set_level(logging.INFO, logger="app.core.bundle")
    old = tmp_path / "old.db"
    with sqlite3.connect(old) as conn:
        conn.execute("CREATE TABLE themes (x)")
        conn.execute("INSERT INTO themes VALUES (1)")
    assert bundle.table_counts(old) == {"themes": 1}
    assert any("plex_items" in r.getMessage() for r in caplog.records if r.levelno == logging.INFO)
    broken = tmp_path / "broken.db"
    with sqlite3.connect(broken) as conn:
        conn.execute("CREATE VIEW plex_items AS SELECT nosuchfunc(1) AS x")
    with pytest.raises(sqlite3.OperationalError, match="no such function"):
        bundle.table_counts(broken)


# ── 7. census: one placement per file ─────────────────────────────────

def _placement(conn, tmdb_id, folder, kind, present, placed_at):
    conn.execute("INSERT INTO placements (media_type, tmdb_id, section_id, media_folder, placement_kind, "
                 "placed_at, theme_present) VALUES ('movie', ?, '1', ?, ?, ?, ?)",
                 (tmdb_id, folder, kind, placed_at, present))


def test_census_carries_one_placement_per_file_present_first_then_newest(tmp_path):
    db = tmp_path / "motif.db"
    init_db(db)
    with sqlite3.connect(db) as conn:
        for tmdb_id in (1, 2):
            conn.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, file_path, file_size, "
                         "downloaded_at, source_video_id, source_kind) VALUES ('movie', ?, '1', ?, 1, 'x', 'v', 'url')",
                         (tmdb_id, f"movies/{tmdb_id}/theme.mp3"))
        # item 1: the newer placement's sidecar is gone; the older one is present
        _placement(conn, 1, "/media/a", "hardlink", 0, "2026-09-10T00:00:00")
        _placement(conn, 1, "/media/b", "copy", 1, "2026-09-01T00:00:00")
        # item 2: both present — the newest wins
        _placement(conn, 2, "/media/c", "hardlink", 1, "2026-09-01T00:00:00")
        _placement(conn, 2, "/media/d", "copy", 1, "2026-09-10T00:00:00")
        conn.commit()
    census = bundle.themes_census(db)
    assert len(census) == 2, "one row per local_files row"
    by_id = {c["tmdb_id"]: c for c in census}
    assert by_id[1]["placement_kind"] == "copy"
    assert by_id[2]["placement_kind"] == "copy"
