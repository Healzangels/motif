"""v0.51.76 — forwarded_allow_ips is configurable (XFF-spoof forward-auth bypass).

Adversarial security audit finding (HIGH, unauth-remote, code-side of a deploy fix).
motif ran uvicorn with a HARDCODED forwarded_allow_ips="*" whenever trust_forward_auth
was on. With "*", uvicorn's ProxyHeadersMiddleware overwrites request.client.host with
the attacker-controlled leftmost X-Forwarded-For token for ANY peer — so a direct-to-
:5309 attacker (off-path LAN host / sibling container) could send
`X-Forwarded-For: <NPM's allowlisted IP>` + `X-Authentik-Username: admin`, satisfy
AuthMiddleware's forward_auth_allowed_ips check against the spoofed IP, and be granted
Principal(scope="admin") — full unauth admin over every _require_admin endpoint.

The full close needs the operator's NPM topology, so v0.51.76 adds the CODE side: a
new web.forward_auth_trusted_proxies config (env MOTIF_FORWARD_AUTH_TRUSTED_PROXIES)
mapped to uvicorn's forwarded_allow_ips. Empty (default) keeps the historical "*" for
backward-compat (no behavior change); the operator sets it to NPM's container IP so
uvicorn honors XFF only from NPM and a direct attacker's real peer IP fails the
allowlist. Also fixes the stale config comment that still called an empty
forward_auth_allowed_ips "legacy permissive" (v1.24.12 made it FAIL-CLOSED).
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent


def test_web_config_has_trusted_proxies_defaulting_empty():
    from app.core.config_file import WebConfig
    cfg = WebConfig()
    assert hasattr(cfg, "forward_auth_trusted_proxies")
    assert cfg.forward_auth_trusted_proxies == [], "default must be empty (→ historical '*')"


def test_env_var_binds_trusted_proxies():
    from app.core.config_file import ENV_BINDINGS
    names = [b[0] for b in ENV_BINDINGS]
    assert "MOTIF_FORWARD_AUTH_TRUSTED_PROXIES" in names
    binding = next(b for b in ENV_BINDINGS
                   if b[0] == "MOTIF_FORWARD_AUTH_TRUSTED_PROXIES")
    assert binding[1] == "web.forward_auth_trusted_proxies"


def test_settings_property_roundtrips_env(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_FORWARD_AUTH_TRUSTED_PROXIES", "172.17.0.5,10.0.0.0/8")
    from app.config import Settings
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    assert s.forward_auth_trusted_proxies == ["172.17.0.5", "10.0.0.0/8"]


def test_main_no_longer_hardcodes_star_forwarded_allow_ips():
    main_py = (REPO / "app" / "main.py").read_text()
    # the knob is used...
    assert "forward_auth_trusted_proxies" in main_py
    assert 'forwarded_allow_ips=(' in main_py
    # ...and the bare hardcoded form is gone.
    assert 'forwarded_allow_ips="*" if settings.trust_forward_auth else None' not in main_py


def test_stale_legacy_permissive_comment_is_fixed():
    # v1.24.12 made an empty allowlist FAIL-CLOSED; the "trust the header from any IP"
    # wording contradicted that and could mislead an operator into thinking they're
    # exposed (or safe) when the opposite is true.
    for rel in ("app/core/config_file.py", "app/config.py"):
        src = (REPO / rel).read_text()
        assert "legacy permissive (trust the header from any IP)" not in src, rel
        assert "legacy permissive (any IP)" not in src, rel
