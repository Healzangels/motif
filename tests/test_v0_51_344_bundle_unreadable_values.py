"""v0.51.344: a motif.yaml scalar YAML cannot build is named by its key, and an integer too long to show never 422s a preview.

  1. A decimal integer past Python's int-to-text limit, or a date that does not exist, names its dotted key — never 'ValueError'.
  2. A hex integer that loads but cannot be written as text shows as its bit length in the diff; the preview answers 200.
"""
from __future__ import annotations

import re
import shutil
import sys

import pytest
from fastapi.testclient import TestClient

from app.core import bundle, config_file
from app.core.auth import create_admin, init_auth_schema
from app.core.db import init_db
from tests.test_v0_51_339_bundle_staging_boot import _H, LIVE_YAML, _bundle, _live
from tests.test_v0_51_341_config_secrets_preview import _bundle as _bundle_with


@pytest.fixture
def no_env(monkeypatch):
    for env_name, _dotted, _conv in config_file.ENV_BINDINGS:
        monkeypatch.delenv(env_name, raising=False)


@pytest.fixture
def int_text_limit():
    old = sys.get_int_max_str_digits()
    sys.set_int_max_str_digits(4300)  # the interpreter default the image runs under
    yield
    sys.set_int_max_str_digits(old)


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


def _too_long() -> str:
    return "9" * (sys.get_int_max_str_digits() + 1)


def _long_hex() -> str:
    return "0x" + "f" * (sys.get_int_max_str_digits() + 1)  # 4 bits a digit: YAML builds it past the limit, and str() of it raises


# ── 1. a scalar YAML cannot build ────────────────────────────────────

UNBUILDABLE = {
    "a declared text leaf": (lambda: f"plex:\n  url: {_too_long()}\n", "plex.url holds an integer too long to read"),
    "a declared integer leaf": (lambda: f"downloads:\n  rate_per_hour: {_too_long()}\n",
                                "downloads.rate_per_hour holds an integer too long to read"),
    "a list element": (lambda: f"extra:\n  - ok\n  - {_too_long()}\n", "extra holds an integer too long to read"),
    "a date that does not exist": (lambda: "sync:\n  cron: 2026-13-45\n", "sync.cron holds a date that does not exist"),
    "past a tag the safe loader refuses": (lambda: f"a:\n  b: !unknown x\nextra: {_too_long()}\n",
                                           "extra holds an integer too long to read"),
    "inside a list that holds itself": (lambda: f"a: &x [*x, {_too_long()}]\n", "a is a YAML alias of a list or mapping, which motif never writes"),  # v0.51.344: R1-F9 — the alias gate answers first, by key; motif writes neither
}


def test_the_unreadable_walk_ends_on_a_list_that_holds_itself(int_text_limit):
    # v0.51.344: R1-F9 — flatten_config refuses the alias before this walk runs; reached directly, its memo still meets its own node again and ends
    assert bundle._unreadable_scalar(f"a: &x [*x, {_too_long()}]\n") == "a holds an integer too long to read"


@pytest.mark.parametrize("case", list(UNBUILDABLE))
def test_a_scalar_yaml_cannot_build_is_named_by_its_key(tmp_path, no_env, int_text_limit, case):
    make, words = UNBUILDABLE[case]
    text = make()
    for side in ("live", "bundle"):
        flat, err = bundle.flatten_config(text, side=side)
        assert flat == {} and err == words, (side, err)
        assert not re.search(r"9{20}|13-45|unknown", err), "by key, never the value"
    db, cd = _live(tmp_path)
    b = _bundle_with(tmp_path / "carry", text)
    assert bundle.preview(b, cd / "motif.yaml")["config_parse_error"] == {"live": None, "bundle": words}
    with pytest.raises(ValueError) as refused:
        bundle.stage_bundle_restore(db, cd, b, keep_config=False)
    assert words in str(refused.value) and "KEEP MY CURRENT CONFIG" in str(refused.value), refused.value
    assert bundle.pending_members(db, cd) == [] and not bundle.STAGING_LOCK.locked()
    assert bundle.stage_bundle_restore(db, cd, b, keep_config=True).staged == ["database"]


def test_a_parse_error_that_is_not_a_scalar_keeps_its_summary(no_env):
    _, err = bundle.flatten_config("plex: [unclosed\n", side="bundle")
    assert err and err.split(" at line ")[0].endswith("Error") and "holds" not in err, err
    _, err = bundle.flatten_config(b"plex:\n  token: \xff\xfe\n", side="bundle")
    assert err == "UnicodeDecodeError", err


# ── 2. an integer too long to show ───────────────────────────────────

TOO_LONG_TO_SHOW = {
    "a declared integer leaf": (lambda: f"downloads:\n  rate_per_hour: {_long_hex()}\n", "downloads.rate_per_hour"),
    "an undeclared leaf": (lambda: f"extra:\n  foo: {_long_hex()}\n", "extra.foo"),
    "a list element": (lambda: f"extra:\n  - {_long_hex()}\n", "extra"),
}


@pytest.mark.parametrize("case", list(TOO_LONG_TO_SHOW))
def test_an_integer_too_long_to_show_previews_by_its_bit_length(api, tmp_path, int_text_limit, case):
    client, cd = api
    make, key = TOO_LONG_TO_SHOW[case]
    words = f"a {int(_long_hex(), 16).bit_length()}-bit integer, too long to show"
    b = _bundle_with(tmp_path / "carry", make())
    shutil.copyfile(b, cd / "backups" / b.name)
    r = client.post("/api/admin/database-restore", json={"name": b.name}, headers=_H)
    assert r.status_code == 200, r.text[:300]
    pv = r.json()["preview"]
    assert pv["config_parse_error"] == {"live": None, "bundle": None}, pv["config_parse_error"]
    rows = {row["key"]: row for row in pv["config_diff"]}
    assert words in rows[key]["bundle"] and rows[key]["live"] == "(unset)", rows.get(key)
    assert "ffff" not in r.text
    (cd / "motif.yaml").write_text(make())
    lived = {row["key"]: row for row in bundle.preview(_bundle(tmp_path / "clean"), cd / "motif.yaml")["config_diff"]}
    assert words in lived[key]["live"], lived.get(key)
