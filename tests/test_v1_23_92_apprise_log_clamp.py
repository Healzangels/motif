"""v1.23.92 (SECURITY) — apprise's credential-bearing logs are clamped.

the user's pasted prod logs (running at DEBUG to watch a sync) printed the FULL
Discord webhook URL incl. the secret token on every notify:
    DEBUG apprise  Discord POST URL: https://discord.com/api/webhooks/<id>/<token>
apprise logs that at DEBUG, plus the Loaded-URL + payload frames. motif clamps
other noisy third-party loggers (apscheduler/httpx/dulwich/…) to WARNING but
missed apprise, so at DEBUG the token leaked to stdout/the log file.

Fix: configure_logging clamps the `apprise` logger to WARNING — its failures still
surface, but the credential-bearing DEBUG/INFO frames never emit regardless of the
configured log level. motif's own `notify` logger keeps the "Notification sent"
signal.
"""
from __future__ import annotations

import logging
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent


def test_apprise_logger_clamped_even_at_debug():
    from app.main import configure_logging
    configure_logging("DEBUG")  # operator opts into DEBUG for diagnostics
    apprise_log = logging.getLogger("apprise")
    assert apprise_log.level == logging.WARNING
    # the credential-bearing DEBUG line ("Discord POST URL: …/<token>") and the
    # INFO frames are suppressed even though the root is at DEBUG.
    assert not apprise_log.isEnabledFor(logging.DEBUG)
    assert not apprise_log.isEnabledFor(logging.INFO)
    # child loggers (apprise.plugins.NotifyDiscord, …) inherit the clamp.
    assert not logging.getLogger("apprise.plugins").isEnabledFor(logging.DEBUG)
    # real failures still surface.
    assert apprise_log.isEnabledFor(logging.WARNING)


def test_main_source_pins_the_clamp():
    src = (REPO / "app" / "main.py").read_text()
    assert 'logging.getLogger("apprise").setLevel(logging.WARNING)' in src


def test_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
