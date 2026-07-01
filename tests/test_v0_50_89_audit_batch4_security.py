"""v0.50.89 — holistic-audit Batch 4: security findings.

1. events.py `_URL_QUERY_SECRET_RE` matched sensitive query-param names by
   exact alternation right after `[?&]`, so compound names like
   `session_token=`, `refresh_token=`, `client_secret=` passed through
   completely unredacted. Rebuilt from the substring-matching philosophy
   `_SCRUB_SUBSTRINGS` already uses for dict keys.
2. events.py's webhook-path regex comment claimed "Slack-compatible"
   coverage it didn't actually have (Slack's real shape has no
   `/webhooks/` segment). Added a dedicated Slack pattern.
3. config.py `resolve_session_key()`'s chmod failure was a bare
   `except OSError: pass` — the one un-logged secret-file chmod in an
   otherwise consistently-logged codebase.
"""
from __future__ import annotations

import logging

from app.core.events import _redact_url_credentials


# ── 1. compound query-param secret names now redacted ───────────────────

def test_compound_query_param_names_are_redacted():
    for param, value in [
        ("session_token", "abc123SECRET"),
        ("refresh_token", "abc123SECRET"),
        ("client_secret", "abc123SECRET"),
        ("sessiontoken", "abc123SECRET"),
    ]:
        raw = f"https://example.com/x?{param}={value}&other=fine"
        out = _redact_url_credentials(raw)
        assert value not in out, f"{param}= must be redacted, got: {out}"
        assert f"{param}=***" in out, f"param name must stay visible: {out}"
        assert "other=fine" in out, "unrelated params must survive untouched"


def test_exact_names_still_redacted_no_regression():
    out = _redact_url_credentials("https://h/a?X-Plex-Token=abc123")
    assert "abc123" not in out
    assert "***" in out


# ── 2. Slack webhook URLs now redacted ───────────────────────────────────

def test_slack_webhook_token_is_redacted():
    raw = "POST https://hooks.slack.com/services/T00000000/B00000000/SuperSecretSlackToken failed"
    out = _redact_url_credentials(raw)
    assert "SuperSecretSlackToken" not in out
    assert "hooks.slack.com/services/T00000000/B00000000/***" in out


def test_discord_webhook_still_redacted_no_regression():
    raw = "POST https://discord.com/api/webhooks/123456789/SuperSecretTokenABC failed"
    out = _redact_url_credentials(raw)
    assert "SuperSecretTokenABC" not in out
    assert "/webhooks/123456789/***" in out


# ── 3. session key chmod failure is logged, not swallowed ───────────────

def test_session_key_chmod_failure_is_logged(tmp_path, monkeypatch, caplog):
    from app.config import Settings

    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    real_chmod = type(s.session_key_file).chmod

    def _boom_chmod(self, mode):
        raise OSError("simulated chmod failure")

    monkeypatch.setattr("pathlib.Path.chmod", _boom_chmod)

    with caplog.at_level(logging.WARNING):
        key = s.resolve_session_key()

    assert key  # still returns a usable key despite the chmod failure
    assert any("chmod" in rec.message.lower() for rec in caplog.records), (
        "v0.50.89: a chmod failure on the session key file must be logged, "
        "not silently swallowed by a bare except"
    )
