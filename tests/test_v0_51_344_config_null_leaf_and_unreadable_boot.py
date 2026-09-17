"""v0.51.344: PATCH null (and a blank COOKIES FILE) saves the declared default the loader reads; a scalar YAML cannot build fails boot by its key."""
from __future__ import annotations

import dataclasses
import re
from pathlib import Path

import pytest
import yaml

import app
from app.core import bundle, config_file
from app.core.config_file import WHOLE_SECRET_KEYS, ConfigValidationError, MotifConfig, PathsConfig

from tests.test_v0_51_342_loudness_patch_float import _H, _api
from tests.test_v0_51_344_bundle_unreadable_values import UNBUILDABLE, int_text_limit, no_env


def _client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_FORWARD_AUTH_ALLOWED_IPS", "127.0.0.1")  # no_env strips conftest's allowlist; forward-auth fails closed without it
    return _api(tmp_path, monkeypatch)


# ── 1. PATCH null / a blank COOKIES FILE is the declared path, never Path(".") ──

@pytest.mark.parametrize("sent", [None, "", "   "], ids=["null", "blank", "spaces"])
def test_patch_null_or_a_blank_cookies_file_saves_the_declared_path(tmp_path, monkeypatch, no_env, sent):
    client, settings = _client(tmp_path, monkeypatch)
    live = tmp_path / "live" / "cookies.txt"
    assert client.patch("/api/config", headers=_H, json={"paths": {"cookies_file": str(live)}}).status_code == 200
    assert settings.cookies_file == live, "the premise: a real path saves"
    r = client.patch("/api/config", headers=_H, json={"paths": {"cookies_file": sent}})
    assert r.status_code == 200, r.text
    default = PathsConfig().cookies_file
    assert r.json()["config"]["paths"]["cookies_file"] == default
    assert yaml.safe_load((tmp_path / "motif.yaml").read_text())["paths"]["cookies_file"] == default
    assert settings.cookies_file == Path(default) and settings.cookies_file != Path(".")
    assert not settings.cookies_file.is_dir(), "yt-dlp's cookiefile copy never meets a directory"
    loaded = config_file.load_config_text("paths:\n  cookies_file: null\n").paths.cookies_file
    assert loaded == settings.cfg.paths.cookies_file, "PATCH and the loader answer null alike"


def test_the_cookies_file_input_placeholder_is_the_declared_path():
    html = (Path(app.__file__).parent / "web" / "templates" / "settings.html").read_text()
    tags = [m.group(0) for m in re.finditer(r"<input\b[^>]*>", html) if 'data-cfg-field="paths.cookies_file"' in m.group(0)]
    assert len(tags) == 1, tags
    placeholder = re.search(r'placeholder="([^"]*)"', tags[0])
    assert placeholder and placeholder.group(1) == PathsConfig().cookies_file, "a blank input shows the path a blank save writes"


# ── 2. every text leaf: null is the declared default (a secret's null still clears) ──

def test_patch_null_on_every_text_leaf_mirrors_the_loader(no_env):
    from app.web.api import _ALLOWED_TOP_LEVEL, _apply_partial_config
    defaults = MotifConfig()
    seen: list[str] = []
    for f in dataclasses.fields(defaults):
        section = getattr(defaults, f.name)
        if f.name not in _ALLOWED_TOP_LEVEL or not dataclasses.is_dataclass(section):
            continue
        for g in dataclasses.fields(section):
            declared = getattr(section, g.name)
            if not isinstance(declared, str):
                continue
            dotted = f"{f.name}.{g.name}"
            cfg = MotifConfig()
            setattr(getattr(cfg, f.name), g.name, "something-else")
            _apply_partial_config(cfg, {f.name: {g.name: None}})
            got = getattr(getattr(cfg, f.name), g.name)
            if dotted in WHOLE_SECRET_KEYS:
                assert got == "", (dotted, got)  # null clears a secret, as the settings page's CLEAR sends it
            else:
                assert got == config_file._coerce_leaf(declared, None) == declared, (dotted, got)
            seen.append(dotted)
    assert "paths.cookies_file" in seen and "sync.cron" in seen and len(seen) > 20, seen


def test_patch_null_on_sync_cron_saves_the_declared_schedule(tmp_path, monkeypatch, no_env):
    client, settings = _client(tmp_path, monkeypatch)
    assert client.patch("/api/config", headers=_H, json={"sync": {"cron": "5 4 * * *"}}).status_code == 200
    r = client.patch("/api/config", headers=_H, json={"sync": {"cron": None}})
    assert r.status_code == 200, r.text  # was a 400: "" is not a 5-field cron
    assert settings.sync_cron == config_file.SyncConfig().cron
    assert yaml.safe_load((tmp_path / "motif.yaml").read_text())["sync"]["cron"] == config_file.SyncConfig().cron


# ── 3. a scalar YAML cannot build fails the load, and the boot, by its key ──

@pytest.mark.parametrize("case", list(UNBUILDABLE))
def test_a_scalar_yaml_cannot_build_fails_the_load_by_its_key(tmp_path, no_env, int_text_limit, case):
    from app.config import Settings
    make, words = UNBUILDABLE[case]
    text = make()
    p = tmp_path / "motif.yaml"
    p.write_text(text)
    with pytest.raises(ConfigValidationError) as refused:
        config_file.ConfigFile(p).load()
    assert refused.value.errors == [f"motif.yaml: {words}"], refused.value.errors
    cause = refused.value.__cause__
    if "YAML alias" in words:  # v0.51.344: an alias of a collection is refused on the composer's graph, before any constructor runs (R1-F9)
        assert cause is None, "the premise: nothing was built"
    else:
        assert isinstance(cause, ValueError) and not isinstance(cause, yaml.YAMLError), "the premise: it composes, the constructor raises"
    assert not re.search(r"9{20}|13-45|unknown", str(refused.value)), "by key, never the value"
    assert bundle.flatten_config(text, side="bundle") == ({}, words), "one rule names it on both doors"
    with pytest.raises(ConfigValidationError, match=re.escape(words)):
        Settings(config_dir=tmp_path, data_dir=tmp_path / "data")


def test_the_boot_dies_in_words_that_name_the_key(tmp_path, monkeypatch, no_env, caplog):
    from app import config as config_mod
    from app import main as main_mod
    cd = tmp_path / "cfg"
    cd.mkdir()
    (cd / "motif.yaml").write_text("sync:\n  cron: 2026-13-45\n")
    monkeypatch.setattr(config_mod, "_DEFAULT_CONFIG_DIR", cd)
    monkeypatch.setattr(main_mod, "get_settings", lambda: config_mod.Settings(data_dir=cd / "data"))
    monkeypatch.setattr(main_mod, "configure_logging", lambda *a, **k: None)
    with pytest.raises(ConfigValidationError) as died:
        main_mod.main()
    assert str(died.value) == "motif.yaml: sync.cron holds a date that does not exist"
    assert "month must be in" not in str(died.value) and "13-45" not in str(died.value)
    assert isinstance(died.value.__cause__, ValueError)


def test_a_value_error_the_walker_cannot_place_is_named_by_type_never_its_text(monkeypatch):
    def boom(text):
        raise ValueError("SECRET-VALUE would be echoed here")
    monkeypatch.setattr(config_file.yaml, "safe_load", boom)
    with pytest.raises(ConfigValidationError) as refused:
        config_file.load_config_text("plex:\n  token: T\n")
    assert refused.value.errors == ["motif.yaml: a value YAML cannot read (ValueError)"], refused.value.errors
    assert "SECRET" not in str(refused.value)
