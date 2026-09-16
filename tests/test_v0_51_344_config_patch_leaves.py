"""v0.51.344: the settings PATCH answers a wrong-typed section or integer in words; a null text leaf and a 0/1 bool leaf hydrate losslessly."""
from __future__ import annotations

import dataclasses
import logging
import threading
from pathlib import Path

import pytest
import yaml

from app.core import bundle, config_file
from app.core.config_file import LoudnessConfig, PathsConfig, PlacementConfig, SyncConfig

from tests.test_v0_51_342_loudness_patch_float import _H, _api, _patch_raw


@pytest.fixture
def no_env(monkeypatch):
    for env_name, _dotted, _conv in config_file.ENV_BINDINGS:
        monkeypatch.delenv(env_name, raising=False)


# ── 1. a section hand-edited into a scalar saves from its defaults ────

@pytest.mark.parametrize("seed", ["loudness: 2026-09-14\n", "loudness: 5\n", "loudness:\n", "loudness: [1]\n"],
                         ids=["date", "int", "null", "list"])
def test_a_section_hand_edited_into_a_scalar_saves_from_its_defaults(tmp_path, monkeypatch, caplog, seed):
    (tmp_path / "motif.yaml").write_text(seed)
    client, settings = _api(tmp_path, monkeypatch)
    caplog.set_level(logging.WARNING, logger="app.web.api")
    before = (tmp_path / "motif.yaml").read_bytes()
    r = client.patch("/api/config", headers=_H, json={"loudness": {"bogus": 1}})
    assert r.status_code == 400 and r.json()["detail"] == "unknown field: loudness.bogus", r.text
    assert (tmp_path / "motif.yaml").read_bytes() == before, "a refused save heals nothing"
    caplog.clear()
    r = client.patch("/api/config", headers=_H, json={"loudness": {"target_lufs": -16}})
    assert r.status_code == 200, r.text
    on_disk = yaml.safe_load((tmp_path / "motif.yaml").read_text())["loudness"]
    assert on_disk == {**dataclasses.asdict(LoudnessConfig()), "target_lufs": -16.0}, on_disk
    kind = type(yaml.safe_load(seed)["loudness"]).__name__
    warned = [rec.getMessage() for rec in caplog.records if rec.name == "app.web.api" and rec.levelno == logging.WARNING]
    assert len(warned) == 1 and "loudness" in warned[0] and kind in warned[0], warned
    assert client.patch("/api/config", headers=_H, json={"downloads": {"concurrency": 2}}).status_code == 200


# ── 2. a digit-grouped number: refused on save, still read at load ────

def test_a_hand_edited_digit_grouped_target_still_loads_and_reads(tmp_path, no_env):
    from app.config import Settings
    (tmp_path / "motif.yaml").write_text("loudness:\n  target_lufs: '-1_6'\n")
    assert config_file.ConfigFile(tmp_path / "motif.yaml").load().loudness.target_lufs == -16.0
    assert Settings(config_dir=tmp_path, data_dir=tmp_path / "data").loudness_target_lufs == -16.0


# ── 3. an integer leaf refuses a non-integer in words ─────────────────

@pytest.mark.parametrize("leaf, sent", [
    ("rate_per_hour", float("inf")), ("rate_per_hour", float("nan")), ("rate_per_hour", 1.5), ("rate_per_hour", "x"),
    ("rate_per_hour", "1_6"), ("rate_per_hour", [1]), ("concurrency", True),
], ids=["Infinity", "NaN", "fraction", "text", "digit-grouped", "list", "bool"])
def test_a_non_integer_is_a_400_in_words_and_saves_nothing(tmp_path, monkeypatch, leaf, sent):
    client, settings = _api(tmp_path, monkeypatch)
    assert client.patch("/api/config", headers=_H, json={"downloads": {leaf: 3}}).status_code == 200
    before = (tmp_path / "motif.yaml").read_bytes()
    r = _patch_raw(client, {"downloads": {leaf: sent}})
    assert r.status_code == 400, r.text
    detail = r.json()["detail"]
    assert isinstance(detail, str) and f"downloads.{leaf}" in detail and "integer" in detail, detail
    assert str(sent) not in detail, "the refusal names the key, never echoes the value"
    assert getattr(settings.cfg.downloads, leaf) == 3
    assert (tmp_path / "motif.yaml").read_bytes() == before


# ── 4. 0 and 1 in a bool leaf hydrate to false and true, nothing wider ─

def test_a_hand_edited_zero_or_one_in_a_bool_leaf_loads_as_false_or_true(tmp_path, no_env):
    (tmp_path / "motif.yaml").write_text("plex:\n  enabled: 0\ndownloads:\n  geo_bypass: 1\n")
    cfg = config_file.ConfigFile(tmp_path / "motif.yaml").load()
    assert cfg.plex.enabled is False and cfg.downloads.geo_bypass is True
    assert bundle.flatten_config("plex:\n  enabled: 1\n", side="bundle") == ({"plex.enabled": True}, None)
    for wider in ("2", "-1", "'1'", "1.0"):
        flat, err = bundle.flatten_config(f"plex:\n  enabled: {wider}\n", side="bundle")
        assert flat == {} and err and err.startswith("plex.enabled must be true or false"), (wider, err)


# ── 5. a null text leaf is its declared default ───────────────────────

def test_a_null_text_leaf_loads_as_its_declared_default_and_the_boot_reads_work(tmp_path, monkeypatch, no_env):
    from app.config import Settings
    from app.core import scheduler
    (tmp_path / "motif.yaml").write_text("sync:\n  cron: null\npaths:\n  cookies_file: null\nplacement:\n  default_method: null\n")
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    assert s.sync_cron == SyncConfig().cron
    assert s.cookies_file == Path(PathsConfig().cookies_file)
    assert s.cfg.placement.default_method == PlacementConfig().default_method
    flat, err = bundle.flatten_config("placement:\n  default_method: null\n", side="bundle")
    assert (flat, err) == ({"placement.default_method": PlacementConfig().default_method}, None)
    monkeypatch.setattr(scheduler.BackgroundScheduler, "start", lambda self, *a, **k: None)
    monkeypatch.setattr(scheduler, "_check_release_update", lambda settings: None)  # v0.51.344: the boot's release-check thread never reaches GitHub from a test
    before = set(threading.enumerate())
    assert scheduler.start_scheduler(s).get_job("daily_sync") is not None, "the boot's sync-cron read"
    for started in set(threading.enumerate()) - before:
        started.join(timeout=5)
