"""v0.51.341: residual config-secret and restore-preview correctness.

  1. plex.url userinfo masks in GET /api/config and the bundle preview; a PATCH
     carrying the mask takes the stored credentials back (host edits kept).
  2. mask_url_credentials hides a "/" or "@" in the password, a scheme-less
     user:pass@host and sensitive query params — and every masked shape
     round-trips through PATCH without the mask ever being written.
  3. A bundle motif.yaml whose section is not a mapping is refused by name.
  4. A YAML date / mixed-key mapping inside a list renders; no preview 500.
  5. A non-UTF-8 live motif.yaml is named unparseable, not diffed.
  6. The cookies preview line names where the cookies will land.
"""
from __future__ import annotations

import dataclasses
import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core import bundle, config_file
from app.core.auth import create_admin, init_auth_schema
from app.core.config_file import USERINFO_URL_KEYS, MotifConfig, mask_config_value, mask_url_credentials
from app.core.db import CURRENT_SCHEMA_VERSION, init_db

from tests._slice_helpers import slice_between

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
_H = {"X-Authentik-Username": "testadmin"}
NOW = "20260913-040000"

_NODE = shutil.which("node")
if os.environ.get("MOTIF_REQUIRE_NODE") and not _NODE:
    raise RuntimeError("MOTIF_REQUIRE_NODE=1 but node is not on PATH — the restore-preview card check would not run")


def _api(tmp_path, monkeypatch, yaml_text: str | None = None):
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


def _bundle(root: Path, yaml_text: str | bytes, *, cookies: bool = True, stamp: str = NOW) -> Path:
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
                              themes_dir=None, now_stamp=stamp, motif_version="0.51.341",
                              schema_version=CURRENT_SCHEMA_VERSION)
    return src / "backups" / bf.name


def _live(root: Path, yaml_text: str | bytes = "plex:\n  token: LIVE-TOKEN\n") -> tuple[Path, Path]:
    cd = root / "live"
    cd.mkdir(parents=True, exist_ok=True)
    db = cd / "motif.db"
    init_db(db)
    (cd / "motif.yaml").write_bytes(yaml_text.encode("utf-8") if isinstance(yaml_text, str) else yaml_text)
    return db, cd


# ── 1. plex.url ───────────────────────────────────────────────────────

PLEX_CRED = "http://puser:PLEXPW-41a@plex.lan:32400"


def test_get_masks_plex_url_userinfo_and_patch_keeps_or_replaces_it(tmp_path, monkeypatch):
    client, settings = _api(tmp_path, monkeypatch)
    assert client.patch("/api/config", json={"plex": {"url": PLEX_CRED}}, headers=_H).status_code == 200
    r = client.get("/api/config", headers=_H)
    assert r.json()["config"]["plex"]["url"] == "http://***@plex.lan:32400"
    assert "PLEXPW-41a" not in r.text and "puser" not in r.text

    r = client.patch("/api/config", json={"plex": {"url": "http://***@plex.lan:32400"}}, headers=_H)
    assert r.status_code == 200, r.text
    assert settings.cfg.plex.url == PLEX_CRED, "the unchanged masked form keeps the stored credentials"
    assert "PLEXPW-41a" not in r.text, "the PATCH response masks too"

    r = client.patch("/api/config", json={"plex": {"url": "http://***@plex-new.lan:32401/"}}, headers=_H)
    assert r.status_code == 200, r.text
    assert settings.cfg.plex.url == "http://puser:PLEXPW-41a@plex-new.lan:32401/", "a host edit keeps the credentials"
    assert "***" not in (tmp_path / "motif.yaml").read_text()

    assert client.patch("/api/config", json={"plex": {"url": "http://plex-other.lan:32400"}}, headers=_H).status_code == 200
    assert settings.cfg.plex.url == "http://plex-other.lan:32400", "a URL without the marker replaces them"

    r = client.patch("/api/config", json={"plex": {"url": "http://***@plex-other.lan:32400"}}, headers=_H)
    assert r.status_code == 400, "a mask with nothing stored behind it is refused, never written"
    assert settings.cfg.plex.url == "http://plex-other.lan:32400"
    assert "***" not in (tmp_path / "motif.yaml").read_text()


def test_test_plex_after_a_masked_save_connects_with_the_stored_credentials(tmp_path, monkeypatch):
    client, settings = _api(tmp_path, monkeypatch)
    client.patch("/api/config", json={"plex": {"url": PLEX_CRED, "token": "TOK-1"}}, headers=_H)
    shown = client.get("/api/config", headers=_H).json()["config"]["plex"]["url"]
    assert client.patch("/api/config", json={"plex": {"url": shown}}, headers=_H).status_code == 200  # the settings form SAVE
    seen = []

    class FakePlex:
        def __init__(self, cfg, plus_mode=None):
            seen.append(cfg.url)
        def __enter__(self):
            return self
        def __exit__(self, *a):
            return False
        def get_server_info(self):
            return {"friendly_name": "srv", "version": "1", "machine_identifier": "m"}

    import app.core.plex as plex_mod
    monkeypatch.setattr(plex_mod, "PlexClient", FakePlex)
    r = client.post("/api/admin/test-plex", headers=_H)
    assert r.status_code == 200 and r.json()["ok"] is True, r.text
    assert seen == [PLEX_CRED]


@pytest.mark.parametrize("dotted", USERINFO_URL_KEYS)
def test_every_userinfo_masked_key_round_trips_through_patch(dotted):
    from app.web.api import _apply_partial_config
    sec, leaf = dotted.split(".", 1)
    stored = "https://ci:PAT-77z@host-one.example/path/x"
    cfg = MotifConfig()
    setattr(getattr(cfg, sec), leaf, stored)
    shown = mask_config_value(dotted, stored)
    assert "PAT-77z" not in shown and "ci:" not in shown, dotted
    _apply_partial_config(cfg, {sec: {leaf: shown}})
    assert getattr(getattr(cfg, sec), leaf) == stored, dotted
    _apply_partial_config(cfg, {sec: {leaf: shown.replace("host-one", "host-two")}})
    assert getattr(getattr(cfg, sec), leaf) == "https://ci:PAT-77z@host-two.example/path/x", dotted


def test_bundle_preview_masks_plex_url_userinfo(tmp_path):
    b = _bundle(tmp_path, "plex:\n  url: http://buser:BUNDLEPW-9c@plex.lan:32400\n")
    _, cd = _live(tmp_path, "plex:\n  url: http://plex.lan:32400\n")
    p = bundle.preview(b, cd / "motif.yaml")
    row = {r["key"]: r for r in p["config_diff"]}["plex.url"]
    assert row["secret"] is True and row["bundle"] == "http://***@plex.lan:32400" and row["live"] == "http://plex.lan:32400"
    assert "BUNDLEPW-9c" not in json.dumps(p) and "buser" not in json.dumps(p)


# ── 2. mask_url_credentials shapes ────────────────────────────────────

_SHAPES = [
    ("https://u:ab/cd@host.example/x", ("ab/cd", "u:ab"), "https://***@host.example/x"),
    ("https://u:p@ss@host.example/x", ("p@ss", "ss@host"), "https://***@host.example/x"),
    ("user:PW-scheme-less@host.example/repo", ("PW-scheme-less", "user:"), "***@host.example/repo"),
    ("https://host.example/x?token=TOK-q1&a=1", ("TOK-q1",), "https://host.example/x?token=***&a=1"),
    ("https://host.example/x?a=1&api_key=KEY-q2", ("KEY-q2",), "https://host.example/x?a=1&api_key=***"),
    ("http://plex.lan:32400/?X-Plex-Token=PT-q3", ("PT-q3",), "http://plex.lan:32400/?X-Plex-Token=***"),
    ("https://u:PW-both@host.example/x?access_token=AT-q4", ("PW-both", "AT-q4"),
     "https://***@host.example/x?access_token=***"),
]


@pytest.mark.parametrize("url, secrets, shown", _SHAPES)
def test_every_credential_shape_masks_and_round_trips(url, secrets, shown):
    from app.core.config_file import _is_masked_url_credentials, unmask_url_credentials
    masked = mask_url_credentials(url)
    assert masked == shown
    assert not any(s in masked for s in secrets), masked
    assert _is_masked_url_credentials(masked), "PATCH must recognise every masked shape"
    assert unmask_url_credentials(masked, url) == url, "the stored credentials come back whole"


@pytest.mark.parametrize("url", ["https://github.com/LizardByte/ThemerrDB.git", "http://plex.lan:32400",
                                 "https://host.example/x?page=2", ""])
def test_credential_free_urls_pass_through_and_are_not_masked(url):
    from app.core.config_file import _is_masked_url_credentials
    assert mask_url_credentials(url) == url
    assert not _is_masked_url_credentials(url)


def test_a_masked_query_token_round_trips_through_patch_and_is_never_written(tmp_path, monkeypatch):
    client, settings = _api(tmp_path, monkeypatch)
    stored = "https://codeload.example.com/o/r/tar.gz/db?token=GHTOK-5e"
    assert client.patch("/api/config", json={"sync": {"database_url": stored}}, headers=_H).status_code == 200
    shown = client.get("/api/config", headers=_H).json()["config"]["sync"]["database_url"]
    assert "GHTOK-5e" not in shown
    assert client.patch("/api/config", json={"sync": {"database_url": shown}}, headers=_H).status_code == 200
    assert settings.cfg.sync.database_url == stored
    r = client.patch("/api/config", json={"sync": {"database_url": "https://codeload.example.com/x?secret=***"}}, headers=_H)
    assert r.status_code == 400 and settings.cfg.sync.database_url == stored
    assert "***" not in (tmp_path / "motif.yaml").read_text()


# ── 3. bundle YAML against the loader's contract ──────────────────────

@pytest.mark.parametrize("bad_yaml, key, value", [
    ("plex: 5\n", "plex", "5"),
    ("sync: [a-VALUE]\n", "sync", "a-VALUE"),
    ("web: hello-VALUE\n", "web", "hello-VALUE"),
    ("plex: [1, 2]\n", "plex", None),
    ("plex:\n", "plex", None),
])
def test_a_section_that_is_not_a_mapping_is_refused_by_name(tmp_path, bad_yaml, key, value):
    flat, err = bundle.flatten_config(bad_yaml, side="bundle")
    assert flat == {} and err and key in err, err
    if value:
        assert value not in err
    b = _bundle(tmp_path, bad_yaml)
    db, cd = _live(tmp_path)
    p = bundle.preview(b, cd / "motif.yaml")
    assert p["config_parse_error"]["bundle"] and p["config_diff"] == []
    with pytest.raises(ValueError, match=key):
        bundle.stage_bundle_restore(db, cd, b, keep_config=False)
    assert bundle.pending_members(db, cd) == []
    assert bundle.stage_bundle_restore(db, cd, b, keep_config=True).staged == ["database"]


def test_the_refused_shapes_are_the_ones_that_break_the_loaded_config(tmp_path, monkeypatch):
    for env_name, _dotted, _conv in config_file.ENV_BINDINGS:
        monkeypatch.delenv(env_name, raising=False)  # an env override into a non-mapping section raises inside load() instead
    for bad in ("plex: 5\n", "sync: [a]\n", "web: hello\n", "plex:\n"):
        (tmp_path / "motif.yaml").write_text(bad)
        cfg = config_file.ConfigFile(tmp_path / "motif.yaml").load()
        sec = bad.split(":", 1)[0]
        assert not dataclasses.is_dataclass(getattr(cfg, sec)), "the loader hydrates it without raising — boot reads crash"


def test_a_nested_section_of_the_wrong_type_is_refused_by_its_dotted_name(monkeypatch):
    @dataclasses.dataclass
    class Leaf:
        x: int = 1

    @dataclasses.dataclass
    class Inner:
        leaf: Leaf = dataclasses.field(default_factory=Leaf)

    @dataclasses.dataclass
    class Root:
        inner: Inner = dataclasses.field(default_factory=Inner)

    monkeypatch.setattr(config_file, "MotifConfig", Root)
    _, err = bundle.flatten_config("inner:\n  leaf: SEVEN-VALUE\n", side="bundle")
    assert err and "inner.leaf" in err and "SEVEN-VALUE" not in err
    assert bundle.flatten_config("inner:\n  leaf:\n    x: 3\n", side="bundle") == ({"inner.leaf.x": 3}, None)


def test_a_loader_exception_is_a_refusal_without_its_text(monkeypatch):
    def boom(target, src):
        raise RuntimeError("LOADER-TEXT-SECRET")
    monkeypatch.setattr(config_file, "_hydrate_dataclass", boom)
    _, err = bundle.flatten_config("plex:\n  url: http://plex.lan:32400\n", side="bundle")
    assert err and "RuntimeError" in err and "LOADER-TEXT-SECRET" not in err


def test_a_full_valid_config_still_parses_and_stages(tmp_path):
    good = config_file._serialize(MotifConfig(), updated_by="test")
    flat, err = bundle.flatten_config(good, side="bundle")
    assert err is None and "plex.url" in flat and "sync.git_url" in flat
    b = _bundle(tmp_path, good)
    db, cd = _live(tmp_path)
    assert bundle.preview(b, cd / "motif.yaml")["config_parse_error"] == {"live": None, "bundle": None}
    assert bundle.stage_bundle_restore(db, cd, b, keep_config=False).staged == ["database", "config", "cookies"]


# ── 4. non-JSON leaves inside a list ──────────────────────────────────

@pytest.mark.parametrize("leaf_yaml, rendered", [
    ("[2024-01-01]", '["2024-01-01"]'),
    ("[{1: a, b: c}]", '[{"1": "a", "b": "c"}]'),
    ("[{2024-01-01: a}]", '[{"2024-01-01": "a"}]'),
])
def test_preview_renders_dates_and_mixed_keys_inside_a_list(tmp_path, leaf_yaml, rendered):
    b = _bundle(tmp_path, f"plex:\n  section_include: {leaf_yaml}\n")
    _, cd = _live(tmp_path)
    p = bundle.preview(b, cd / "motif.yaml")
    row = {r["key"]: r for r in p["config_diff"]}["plex.section_include"]
    assert row["bundle"] == rendered and row["live"] == "(unset)"


def test_diff_rows_still_compare_raw_values_that_render_alike():
    rows = bundle.config_diff("plex:\n  section_include: [2024-01-01]\n",
                              "plex:\n  section_include: ['2024-01-01']\n")
    assert [r["key"] for r in rows] == ["plex.section_include"]
    assert rows[0]["live"] == rows[0]["bundle"]


# ── 5. decoding symmetry ──────────────────────────────────────────────

def test_a_non_utf8_live_config_is_named_not_diffed(tmp_path):
    b = _bundle(tmp_path, "plex:\n  url: http://plex.lan:32400\n")
    _, cd = _live(tmp_path, b"plex:\n  url: http://plex.old:32400\n  token: \xff\xfe\n")
    p = bundle.preview(b, cd / "motif.yaml")
    assert p["config_parse_error"]["live"] and p["config_parse_error"]["bundle"] is None
    assert p["config_diff"] == []


# ── 6. where the cookies land ─────────────────────────────────────────

BUNDLE_COOKIES = "# cookies\n"


def _no_config_bundle(root: Path, *, stamp: str = NOW) -> Path:
    src = root / "src"
    src.mkdir(parents=True, exist_ok=True)
    init_db(src / "motif.db")
    ck = src / "cookies.txt"
    ck.write_text(BUNDLE_COOKIES)
    bf = bundle.create_bundle(src / "motif.db", src, config_file=None, cookies_file=ck, themes_dir=None,
                              now_stamp=stamp, motif_version="0.51.341", schema_version=CURRENT_SCHEMA_VERSION)
    return src / "backups" / bf.name


def test_the_cookies_target_is_the_path_of_the_config_the_bundle_swaps_in(tmp_path, monkeypatch):
    monkeypatch.delenv("MOTIF_COOKIES_FILE", raising=False)
    live_ck, bundle_ck = tmp_path / "live-mount" / "cookies.txt", tmp_path / "bundle-mount" / "yt-cookies.txt"
    _, cd = _live(tmp_path, f"paths:\n  cookies_file: {live_ck}\n")
    own = _bundle(tmp_path / "own", f"paths:\n  cookies_file: {bundle_ck}\n")
    assert bundle.preview(own, cd / "motif.yaml", cookies_target=live_ck)["cookies_target"] == str(bundle_ck)
    unset = _bundle(tmp_path / "unset", "plex:\n  token: B\n")
    assert bundle.preview(unset, cd / "motif.yaml", cookies_target=live_ck)["cookies_target"] == \
        str(Path(MotifConfig().paths.cookies_file)), "a bundle config without the key swaps in the loader default"
    nocfg = _no_config_bundle(tmp_path / "nocfg")
    assert bundle.preview(nocfg, cd / "motif.yaml", cookies_target=live_ck)["cookies_target"] == str(live_ck), \
        "no motif.yaml in the bundle — the live config stays, and so does its path"
    assert bundle.preview(nocfg, cd / "motif.yaml")["cookies_target"] is None, "an old caller names no target"
    nob = _bundle(tmp_path / "nocookies", "plex:\n  token: B\n", cookies=False)
    assert bundle.preview(nob, cd / "motif.yaml", cookies_target=live_ck)["cookies"] == "not in bundle"
    env_ck = tmp_path / "env-mount" / "cookies.txt"
    monkeypatch.setenv("MOTIF_COOKIES_FILE", str(env_ck))
    assert bundle.preview(own, cd / "motif.yaml", cookies_target=env_ck)["cookies_target"] == str(env_ck), \
        "the env override beats the bundle's key, as load() applies it after hydration"


def test_no_cookies_target_when_the_bundle_config_cannot_name_one(tmp_path, monkeypatch):
    monkeypatch.delenv("MOTIF_COOKIES_FILE", raising=False)
    live_ck = tmp_path / "live-mount" / "cookies.txt"
    _, cd = _live(tmp_path, f"paths:\n  cookies_file: {live_ck}\n")
    refused = _bundle(tmp_path / "refused", "plex: 5\n")
    assert bundle.preview(refused, cd / "motif.yaml", cookies_target=live_ck)["cookies_target"] is None, \
        "a refused bundle config forces KEEP, and a kept config restores no cookies"
    odd = _bundle(tmp_path / "odd", "paths:\n  cookies_file: 5\n")
    p = bundle.preview(odd, cd / "motif.yaml", cookies_target=live_ck)
    assert p["cookies_target"] is None and "paths.cookies_file" in (p["config_parse_error"]["bundle"] or ""), \
        "v0.51.342: refused by its key — staged, the boot built Path(5) for settings.cookies_file"


@pytest.mark.parametrize("case", ["bundle-key", "bundle-key-env", "no-config-member"])
def test_the_previewed_cookies_target_is_where_the_boot_restores_them(tmp_path, monkeypatch, case):
    from app.config import Settings
    monkeypatch.delenv("MOTIF_COOKIES_FILE", raising=False)
    live_ck = tmp_path / "live-mount" / "cookies.txt"
    bundle_ck = tmp_path / "bundle-mount" / "yt-cookies.txt"
    env_ck = tmp_path / "env-mount" / "cookies.txt"
    for p in (live_ck, bundle_ck, env_ck):
        p.parent.mkdir(parents=True)
    live_ck.write_text("# live cookies\n")
    db, cd = _live(tmp_path, f"paths:\n  cookies_file: {live_ck}\n")
    if case == "no-config-member":
        b = _no_config_bundle(tmp_path / "mk")
    else:
        b = _bundle(tmp_path / "mk", f"paths:\n  cookies_file: {bundle_ck}\n")
    if case == "bundle-key-env":
        monkeypatch.setenv("MOTIF_COOKIES_FILE", str(env_ck))
    live_settings = Settings(config_dir=cd, data_dir=tmp_path / "data")
    pv = bundle.preview(b, cd / "motif.yaml", cookies_target=live_settings.cookies_file)  # what both endpoints pass
    bundle.stage_bundle_restore(db, cd, b, keep_config=False)
    bundle.apply_pending_config(cd, now_stamp=NOW)  # main.py's order: the config swaps, settings load, then cookies
    boot = Settings(config_dir=cd, data_dir=tmp_path / "data")
    assert bundle.apply_pending_cookies(cd, boot.cookies_file, now_stamp=NOW)["applied"]
    landed = Path(pv["cookies_target"])
    assert landed.read_text() == BUNDLE_COOKIES, case
    for other in {live_ck, bundle_ck, env_ck} - {landed}:
        assert not other.exists() or other.read_text() != BUNDLE_COOKIES, (case, other)


def test_both_restore_endpoints_pass_the_live_cookies_path(tmp_path, monkeypatch):
    monkeypatch.delenv("MOTIF_COOKIES_FILE", raising=False)
    live_ck = tmp_path / "live-mount" / "cookies.txt"
    client, settings = _api(tmp_path / "app", monkeypatch, f"paths:\n  cookies_file: {live_ck}\n")
    b = _no_config_bundle(tmp_path / "mk")
    (settings.config_dir / "backups").mkdir(exist_ok=True)
    (settings.config_dir / "backups" / b.name).write_bytes(b.read_bytes())
    r = client.post("/api/admin/database-restore", json={"name": b.name}, headers=_H)
    assert r.status_code == 200, r.text
    assert r.json()["preview"]["cookies_target"] == str(live_ck)
    up = _no_config_bundle(tmp_path / "mk2", stamp="20260913-050000")
    r = client.post("/api/admin/database-restore/upload", headers=_H,
                    files={"file": (up.name, up.read_bytes(), "application/gzip")})
    assert r.status_code == 200, r.text
    assert r.json()["preview"]["cookies_target"] == str(live_ck)
    bundle_ck = tmp_path / "bundle-mount" / "yt-cookies.txt"
    own = _bundle(tmp_path / "mk3", f"paths:\n  cookies_file: {bundle_ck}\n", stamp="20260913-060000")
    r = client.post("/api/admin/database-restore/upload", headers=_H,
                    files={"file": (own.name, own.read_bytes(), "application/gzip")})
    assert r.status_code == 200, r.text
    assert r.json()["preview"]["cookies_target"] == str(bundle_ck), "a bundle config's own path, not the live one"


def _run_card(previews: list[dict]) -> list[dict]:
    fn = slice_between(APP_JS, "function showBundlePreview(r) {", "\n    function hideBundlePreview(")
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
        "  out.push(els['restore-preview-cookies'].textContent);\n"
        "}\n"
        "console.log(JSON.stringify(out));\n"
    )
    r = subprocess.run([_NODE, "-e", script], capture_output=True, text=True, timeout=60,
                       env={**os.environ, "PREVIEWS": json.dumps(previews)})
    assert r.returncode == 0, r.stderr
    return json.loads(r.stdout)


@pytest.mark.skipif(not _NODE, reason="node not installed")
def test_card_says_where_the_cookies_will_land(tmp_path):
    _, cd = _live(tmp_path)
    target = tmp_path / "mnt" / "yt-cookies.txt"
    good = bundle.preview(_bundle(tmp_path / "good", f"paths:\n  cookies_file: {target}\n"), cd / "motif.yaml",
                          cookies_target=tmp_path / "live-mount" / "cookies.txt")
    legacy = {**good, "cookies_target": None}
    bad = bundle.preview(_bundle(tmp_path / "bad", "plex: 5\n"), cd / "motif.yaml", cookies_target=target)
    nob = bundle.preview(_bundle(tmp_path / "nob", "plex:\n  token: B\n", cookies=False), cd / "motif.yaml",
                         cookies_target=target)
    good_t, legacy_t, bad_t, nob_t = _run_card([good, legacy, bad, nob])
    assert str(target) in good_t and "will replace" in good_t
    assert "will replace" in legacy_t and "yours" in legacy_t, "no target named — the old wording"
    assert "will replace" not in bad_t, "cookies stay when the config is forced kept"
    assert "will replace" not in nob_t and "not in bundle" in nob_t
