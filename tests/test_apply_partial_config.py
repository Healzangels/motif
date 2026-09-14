"""v1.13.55: tests for `_apply_partial_config` PATCH semantics.

Pins the empty / "***" / null behavior for masked fields
(plex.token, plex.tmdb_api_key, downloads.proxy_url) and the
type coercion for bool / int / list / str scalars.
"""
from __future__ import annotations

import pytest

from app.core.config_file import MotifConfig
from app.web.api import _apply_partial_config


@pytest.fixture
def cfg() -> MotifConfig:
    return MotifConfig()


# ── plex.token mask semantics (legacy, but pin it as a regression guard) ──

def test_plex_token_empty_preserves_existing(cfg):
    cfg.plex.token = "secret123"
    _apply_partial_config(cfg, {"plex": {"token": ""}})
    assert cfg.plex.token == "secret123"


def test_plex_token_mask_preserves_existing(cfg):
    cfg.plex.token = "secret123"
    _apply_partial_config(cfg, {"plex": {"token": "***"}})
    assert cfg.plex.token == "secret123"


def test_plex_token_null_clears(cfg):
    cfg.plex.token = "secret123"
    _apply_partial_config(cfg, {"plex": {"token": None}})
    assert cfg.plex.token == ""


def test_plex_token_explicit_value_overwrites(cfg):
    cfg.plex.token = "old"
    _apply_partial_config(cfg, {"plex": {"token": "new"}})
    assert cfg.plex.token == "new"


# ── tmdb_api_key mask semantics ─────────────────────────────────────

def test_tmdb_api_key_empty_preserves_existing(cfg):
    cfg.plex.tmdb_api_key = "abc123"
    _apply_partial_config(cfg, {"plex": {"tmdb_api_key": ""}})
    assert cfg.plex.tmdb_api_key == "abc123"


def test_tmdb_api_key_mask_preserves_existing(cfg):
    cfg.plex.tmdb_api_key = "abc123"
    _apply_partial_config(cfg, {"plex": {"tmdb_api_key": "***"}})
    assert cfg.plex.tmdb_api_key == "abc123"


def test_tmdb_api_key_null_clears(cfg):
    cfg.plex.tmdb_api_key = "abc123"
    _apply_partial_config(cfg, {"plex": {"tmdb_api_key": None}})
    assert cfg.plex.tmdb_api_key == ""


# ── v1.13.53: downloads.proxy_url mask semantics ────────────────────

def test_proxy_url_empty_preserves_existing(cfg):
    cfg.downloads.proxy_url = "socks5://user:pass@host:1080"
    _apply_partial_config(cfg, {"downloads": {"proxy_url": ""}})
    assert cfg.downloads.proxy_url == "socks5://user:pass@host:1080"


def test_proxy_url_mask_preserves_existing(cfg):
    cfg.downloads.proxy_url = "socks5://user:pass@host:1080"
    _apply_partial_config(cfg, {"downloads": {"proxy_url": "***"}})
    assert cfg.downloads.proxy_url == "socks5://user:pass@host:1080"


def test_proxy_url_null_clears(cfg):
    cfg.downloads.proxy_url = "socks5://user:pass@host:1080"
    _apply_partial_config(cfg, {"downloads": {"proxy_url": None}})
    assert cfg.downloads.proxy_url == ""


def test_proxy_url_explicit_value_overwrites(cfg):
    cfg.downloads.proxy_url = "old"
    _apply_partial_config(cfg, {"downloads": {"proxy_url": "http://proxy:8080"}})
    assert cfg.downloads.proxy_url == "http://proxy:8080"


def test_proxy_url_does_not_get_token_treatment(cfg):
    """v1.13.53: only proxy_url shares the token-style mask; other
    string fields under downloads should still take "" as a literal
    write (not preserve)."""
    cfg.downloads.audio_quality = 5
    # other fields aren't masked — only proxy_url has the special path
    _apply_partial_config(cfg, {"downloads": {"geo_bypass_country": ""}})
    assert cfg.downloads.geo_bypass_country == ""


# ── coercion for bool / int / list ──────────────────────────────────

def test_bool_string_truthy_coerces(cfg):
    _apply_partial_config(cfg, {"downloads": {"geo_bypass": "true"}})
    assert cfg.downloads.geo_bypass is True


def test_bool_string_falsy_coerces(cfg):
    cfg.downloads.geo_bypass = True
    _apply_partial_config(cfg, {"downloads": {"geo_bypass": "no"}})
    assert cfg.downloads.geo_bypass is False


def test_int_field_coerces(cfg):
    _apply_partial_config(cfg, {"downloads": {"audio_quality": "7"}})
    assert cfg.downloads.audio_quality == 7
    assert isinstance(cfg.downloads.audio_quality, int)


def test_list_must_be_list(cfg):
    with pytest.raises(ValueError, match="must be a list"):
        _apply_partial_config(cfg, {"plex": {"section_exclude": "Movies"}})


def test_list_coerces_strings(cfg):
    _apply_partial_config(cfg, {"plex": {"section_exclude": ["Home Movies", 123]}})
    assert cfg.plex.section_exclude == ["Home Movies", "123"]


# ── shape validation ────────────────────────────────────────────────

def test_unknown_section_raises(cfg):
    with pytest.raises(ValueError, match="unknown config section"):
        _apply_partial_config(cfg, {"madeup": {"foo": "bar"}})


def test_unknown_field_raises(cfg):
    with pytest.raises(ValueError, match="unknown field"):
        _apply_partial_config(cfg, {"plex": {"madeup_field": "x"}})


def test_section_must_be_object(cfg):
    with pytest.raises(ValueError, match="must be an object"):
        _apply_partial_config(cfg, {"plex": "not-a-dict"})


def test_null_section_skipped(cfg):
    """A null section in the patch is a no-op (caller sent
    {section: null} to indicate "don't touch this section")."""
    cfg.plex.url = "http://orig"
    _apply_partial_config(cfg, {"plex": None})
    assert cfg.plex.url == "http://orig"


# ── v0.51.342: float leaves (loudness.target_lufs) ──────────────────

@pytest.mark.parametrize("sent, stored", [
    (-16, -16.0), (-18.5, -18.5), (0, 0.0), ("-16", -16.0), (" -12.5 ", -12.5), ("-7e0", -7.0),
])
def test_float_field_takes_a_json_number_or_numeric_string(cfg, sent, stored):
    _apply_partial_config(cfg, {"loudness": {"target_lufs": sent}})
    assert cfg.loudness.target_lufs == stored
    assert type(cfg.loudness.target_lufs) is float


@pytest.mark.parametrize("sent", [
    True, False, None, "", "abc", "-16 LUFS", "nan", "inf", "-Infinity", "1e999",
    float("nan"), float("inf"), float("-inf"), pytest.param(10 ** 400, id="int-too-big-for-float"), [-16], {"v": -16},
])
def test_float_field_refuses_a_non_number_naming_the_key_not_the_value(cfg, sent):
    with pytest.raises(ValueError) as e:
        _apply_partial_config(cfg, {"loudness": {"target_lufs": sent}})
    msg = str(e.value)
    assert "loudness.target_lufs" in msg
    if isinstance(sent, str) and sent.strip():
        assert sent.strip() not in msg, "the refusal names the key, never echoes the value"
    if isinstance(sent, int) and not isinstance(sent, bool):
        assert str(sent) not in msg
    assert cfg.loudness.target_lufs == -18.0 and type(cfg.loudness.target_lufs) is float


@pytest.mark.parametrize("loaded", [-16, "-16"], ids=["loaded-int", "loaded-str"])
@pytest.mark.parametrize("sent, stored", [(-18.5, -18.5), (-16.5, -16.5), ("-16.5", -16.5), (-16, -16.0)])
def test_float_field_saves_a_float_whatever_type_the_yaml_loaded(cfg, loaded, sent, stored):
    # v0.51.342: _hydrate_dataclass keeps YAML's type, so a hand-edited `target_lufs: -16` is an int here.
    cfg.loudness.target_lufs = loaded
    _apply_partial_config(cfg, {"loudness": {"target_lufs": sent}})
    assert cfg.loudness.target_lufs == stored
    assert type(cfg.loudness.target_lufs) is float


@pytest.mark.parametrize("loaded", [-16, "-16"], ids=["loaded-int", "loaded-str"])
@pytest.mark.parametrize("sent", [True, None, "abc", "-16.5 LUFS", float("inf"), float("nan"), [-16]])
def test_float_field_refusal_names_the_key_whatever_type_the_yaml_loaded(cfg, loaded, sent):
    cfg.loudness.target_lufs = loaded
    with pytest.raises(ValueError) as e:
        _apply_partial_config(cfg, {"loudness": {"target_lufs": sent}})
    msg = str(e.value)
    assert "loudness.target_lufs" in msg
    if isinstance(sent, str):
        assert sent not in msg, "the refusal names the key, never echoes the value"
    assert cfg.loudness.target_lufs == loaded


def test_float_field_range_stays_validates_job(cfg):
    from app.core.config_file import validate
    _apply_partial_config(cfg, {"loudness": {"target_lufs": -90}})
    assert cfg.loudness.target_lufs == -90.0
    errors = validate(cfg, require_themes_dir=False)
    assert any("loudness.target_lufs must be between -70 and 0" in e for e in errors), errors
    assert not any("unexpected type" in e for e in errors), errors
