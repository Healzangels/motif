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
