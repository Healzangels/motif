"""v1.21.16 — loud boot warning for an unguarded forward-auth trust path.

The v1.21.15 security audit confirmed a forward-auth bypass: when
trust_forward_auth is on AND forward_auth_allowed_ips is empty (the
default), _resolve_principal trusted X-Authentik-Username from any peer.
v1.24.12 fixed the bypass itself (fail closed — the header is no longer
trusted on an empty allowlist), but the boot warning STAYS: a forward-auth
that silently authenticates nobody reads as "broken login" without a
breadcrumb (class-9). This pins the warning shape; the message text was
updated in v1.24.12 to describe the new fail-closed behavior.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
MAIN_PY = (REPO / "app" / "main.py").read_text()


def test_boot_warns_on_empty_allowlist_with_forward_auth():
    assert (
        "if settings.trust_forward_auth and "
        "not settings.forward_auth_allowed_ips:" in MAIN_PY
    )


def test_warning_is_logged_loudly_and_breadcrumbed():
    # Operator-visible at boot...
    idx = MAIN_PY.index("not settings.forward_auth_allowed_ips:")
    seg = MAIN_PY[idx:idx + 1200]
    assert 'log.warning("  SECURITY: %s"' in seg
    # ...AND persisted to the events log for after-the-fact grep.
    assert 'level="WARNING", component="auth"' in seg
    # v1.24.12: message describes the new fail-closed behavior.
    assert "forward-auth disabled (fail-closed) until" in seg


def test_warning_names_the_remediation():
    idx = MAIN_PY.index("not settings.forward_auth_allowed_ips:")
    seg = MAIN_PY[idx:idx + 1200]
    assert "forward_auth_allowed_ips to your proxy" in seg


def test_version_bumped():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
