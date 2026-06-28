"""v1.22.41 (holistic audit, SECURITY) — events scrubber redacts URL
credentials buried deeper than one level.

Pre-fix `_scrub`'s list branch only recursed dict ELEMENTS:

    elif isinstance(v, list):
        out[k] = [_scrub(item) if isinstance(item, dict) else item for item in v]

So a credential leaked whenever it sat in:
  - a STRING list-element:  {"urls": ["https://user:pass@host"]}
  - a NESTED list:          {"a": [["https://user:pass@host"]]}
  - a string inside a dict inside a list inside a list, etc.

v1.22.41 routes every non-suspicious value through a shape-aware `_scrub_value`
helper so URL userinfo + query-secret redaction reaches any depth.
"""
from __future__ import annotations

from app.core.events import _scrub, _scrub_value


_CRED_URL = "https://user:pass@host.example/path"
_REDACTED_URL = "https://***@host.example/path"


def test_string_list_element_url_creds_redacted():
    out = _scrub({"urls": [_CRED_URL, "https://plain.example"]})
    assert out["urls"][0] == _REDACTED_URL
    assert out["urls"][1] == "https://plain.example"


def test_nested_list_url_creds_redacted():
    out = _scrub({"a": [[_CRED_URL]]})
    assert out["a"][0][0] == _REDACTED_URL


def test_deep_mixed_structure_redacted():
    out = _scrub({"outer": [{"inner_urls": [_CRED_URL]}]})
    assert out["outer"][0]["inner_urls"][0] == _REDACTED_URL


def test_tuple_element_url_creds_redacted():
    # json.dumps serializes a tuple to a JSON array → a tuple of strings is a
    # real leak path, so it must be scrubbed too.
    out = _scrub({"pair": (_CRED_URL, "ok")})
    assert out["pair"][0] == _REDACTED_URL
    assert out["pair"][1] == "ok"


def test_query_secret_in_list_element_redacted():
    out = _scrub({"links": ["https://host.example/x?token=abc123&keep=1"]})
    assert "abc123" not in out["links"][0]
    assert "token=***" in out["links"][0]
    assert "keep=1" in out["links"][0]


def test_suspicious_key_still_wins_over_shape():
    # A key matching a scrub substring redacts the whole value regardless of
    # shape — unchanged from v1.14.54.
    out = _scrub({"auth_tokens": [_CRED_URL, "anything"]})
    assert out["auth_tokens"] == "***REDACTED***"


def test_dict_in_list_recursion_preserved():
    # v1.14.54 behavior must survive the refactor.
    out = _scrub({"items": [{"token": "abc", "label": "ok"}]})
    assert out["items"][0]["token"] == "***REDACTED***"
    assert out["items"][0]["label"] == "ok"


def test_scrub_value_passthrough_for_scalars():
    assert _scrub_value(42) == 42
    assert _scrub_value(None) is None
    assert _scrub_value(True) is True


def test_scrub_value_redacts_bare_string():
    assert _scrub_value(_CRED_URL) == _REDACTED_URL
