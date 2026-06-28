"""v1.22.4 — runtime PUID/PGID support + boot-time writability probe.

The Watchmen/Hokum permission saga root-caused to motif being the only
container pinned to uid 99 (baked USER, no PUID handling) on a uid-1000
*arr/Plex stack: a template reset to --user 99:100 silently broke every write
to the 1000-owned shfs share, and surfaced only hours later as crash-looping
downloads. v1.22.4 (a) adds a docker-entrypoint that adopts PUID/PGID like the
linuxserver images + gosu-drops, (b) switches the Unraid template / README /
compose off the hardcoded --user, and (c) adds a boot writability probe that
logs a loud uid-vs-owner error instead of failing silently.
"""
from __future__ import annotations

import logging
import os
import re
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent


# ── A: the boot writability probe ───────────────────────────────────────


class _Settings:
    """Minimal stand-in exposing what _probe_writability reads."""
    def __init__(self, config_dir, themes_dir=None):
        self.config_dir = Path(config_dir)
        self._themes = Path(themes_dir) if themes_dir else None

    def is_paths_ready(self):
        return self._themes is not None

    @property
    def themes_dir(self):
        return self._themes


def test_probe_silent_when_writable(tmp_path, caplog):
    from app.main import _probe_writability
    s = _Settings(tmp_path / "config", tmp_path / "themes")
    with caplog.at_level(logging.ERROR, logger="motif.main"):
        _probe_writability(s, logging.getLogger("motif.main"))
    assert not [r for r in caplog.records if "WRITABILITY" in r.getMessage()], (
        "writable dirs must not log a WRITABILITY error")


def test_probe_errors_loudly_on_unwritable_dir(tmp_path, caplog):
    from app.main import _probe_writability
    cfg = tmp_path / "config"
    cfg.mkdir()
    os.chmod(cfg, 0o555)  # read+execute, no write
    # Self-validating: if the env ignores the mode (running as root), the probe
    # can't be exercised — skip rather than pass vacuously.
    try:
        p = cfg / ".selfcheck"
        p.write_bytes(b"")
        p.unlink()
        os.chmod(cfg, 0o755)
        pytest.skip("env ignores 0555 (root?) — probe can't be exercised")
    except PermissionError:
        pass

    try:
        with caplog.at_level(logging.ERROR, logger="motif.main"):
            _probe_writability(_Settings(cfg), logging.getLogger("motif.main"))
    finally:
        os.chmod(cfg, 0o755)

    errs = [r for r in caplog.records if "WRITABILITY" in r.getMessage()]
    assert len(errs) == 1, [r.getMessage() for r in caplog.records]
    msg = errs[0].getMessage()
    assert "config_dir" in msg and "NOT writable" in msg
    # The error must name the process uid AND the dir's owner so the operator
    # can see the mismatch at a glance (the whole point of the probe).
    assert "uid=" in msg and "owned" in msg


def test_probe_never_raises_even_on_missing_parent(tmp_path):
    """It's a probe, not a gate — a bad path must not crash boot."""
    from app.main import _probe_writability
    # A path whose creation will fail (file in the way of a dir component).
    blocker = tmp_path / "blocker"
    blocker.write_bytes(b"x")
    _probe_writability(_Settings(blocker / "config"),
                       logging.getLogger("motif.main"))  # must not raise


# ── B: entrypoint adopts PUID/PGID + drops privileges ───────────────────


def test_entrypoint_handles_puid_pgid_and_drops_via_gosu():
    src = (REPO / "docker-entrypoint.sh").read_text()
    assert 'PUID="${PUID:-99}"' in src
    assert 'PGID="${PGID:-100}"' in src
    # Legacy explicit --user is honored (non-root passthrough).
    assert '[ "$(id -u)" != "0" ]' in src
    # Privilege drop to the numeric ids (works without a passwd entry).
    assert 'gosu "${PUID}:${PGID}"' in src
    # Chowns /config (its appdata) but MUST NEVER chown /data (shared media).
    assert 'chown -R "${PUID}:${PGID}" /config /home/motif' in src
    for line in src.splitlines():
        if "chown" in line:
            assert "/data" not in line, "entrypoint must never chown /data"


def test_dockerfile_wires_entrypoint_and_drops_static_user():
    df = (REPO / "Dockerfile").read_text()
    assert "gosu" in df, "gosu must be installed for the privilege drop"
    assert "docker-entrypoint.sh" in df, "entrypoint must be copied + wired"
    assert "/usr/local/bin/docker-entrypoint.sh" in df
    # The static `USER motif` is gone — the entrypoint drops instead (it must
    # start as root to usermod/chown/gosu).
    assert not re.search(r'(?m)^\s*USER\s+motif\s*$', df), (
        "static `USER motif` must be removed so the entrypoint can drop privs")


# ── C: Unraid template / compose off the hardcoded --user ───────────────


def test_unraid_template_uses_puid_pgid_vars_not_hardcoded_user():
    xml = (REPO / "unraid" / "motif.xml").read_text()
    assert 'Target="PUID"' in xml and 'Target="PGID"' in xml, (
        "template must expose PUID/PGID as env variables")
    m = re.search(r"<ExtraParams>(.*?)</ExtraParams>", xml, re.S)
    assert m is not None
    assert "--user" not in m.group(1), (
        "ExtraParams must not hardcode --user (PUID/PGID handle the user now)")


def test_compose_sets_puid_pgid_env():
    yml = (REPO / "docker-compose.yml").read_text()
    assert re.search(r"(?m)^\s*PUID:", yml) and re.search(r"(?m)^\s*PGID:", yml)


def test_readme_documents_permissions():
    rd = (REPO / "README.md").read_text()
    assert "Permissions / PUID/PGID" in rd
    assert "PUID=99" in rd or "PUID=1000" in rd or "-e PUID" in rd


def test_v1_22_4_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
