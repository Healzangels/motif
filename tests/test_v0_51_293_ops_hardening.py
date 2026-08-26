"""v0.51.293 — holistic review: ops/deploy contract fixes.

Three confirmed findings:
  1. docker-compose.yml kept `cap_drop: ALL` from the pre-v1.22.4
     static-USER era, but the current entrypoint starts as root and
     gosu-drops — which needs SETUID/SETGID (the drop) and CHOWN (the
     /config chown). As shipped, the compose crash-looped at boot: the
     catalogued contract-drift class, unguarded (test_v1_22_4 only
     asserted the PUID/PGID env vars).
  2. The Dockerfile HEALTHCHECK hardcoded :5309 while the bind port is
     MOTIF_WEB_PORT-configurable — a port override made the container
     permanently unhealthy.
  3. release.yml's workflow_dispatch path (default version 0.0.0-dev,
     "manual builds without a git tag") pushed the rolling :nightly tag
     unconditionally — one manual build moved the channel every
     deployment tracks onto an arbitrary commit.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
COMPOSE = (REPO / "docker-compose.yml").read_text()
DOCKERFILE = (REPO / "Dockerfile").read_text()
RELEASE = (REPO / ".github" / "workflows" / "release.yml").read_text()


def test_compose_grants_the_gosu_boot_caps():
    # the caps contract: drop ALL, add back exactly what the root->gosu
    # boot phase needs. Order-insensitive parse, not a source-shape pin.
    assert re.search(r"(?m)^\s*cap_drop:\s*\n\s*- ALL", COMPOSE)
    cap_add = re.search(r"(?ms)^\s*cap_add:\s*\n((?:\s*- \w+\n)+)", COMPOSE)
    assert cap_add, "cap_add block missing — the gosu drop needs caps"
    caps = set(re.findall(r"- (\w+)", cap_add.group(1)))
    assert {"SETUID", "SETGID", "CHOWN"} <= caps, (
        f"gosu needs SETUID/SETGID and the /config chown needs CHOWN; "
        f"got {caps} — without them the composed container crash-loops")
    assert not re.search(r"(?m)^\s*user:", COMPOSE), (
        "a compose user: directive would skip the entrypoint's root phase "
        "and fight the PUID/PGID contract")


def test_compose_keeps_no_new_privileges():
    # gosu LOWERS privileges — the hardening flag must survive the fix.
    assert "no-new-privileges:true" in COMPOSE


def test_dockerfile_healthcheck_honors_the_port_env():
    m = re.search(r"(?m)^\s*CMD curl.*healthz", DOCKERFILE)
    assert m and "${MOTIF_WEB_PORT" in m.group(0), (
        "a MOTIF_WEB_PORT override made the container permanently "
        "unhealthy — the healthcheck must probe the configured port")


def test_release_dispatch_cannot_move_nightly():
    # the build step's tags must come from the computed output, never a
    # hardcoded :nightly literal.
    tags_block = re.search(r"(?ms)^\s*tags: \|\n(.*?)\n\s*# Cache", RELEASE)
    assert tags_block, "build-push tags block not found"
    assert ":nightly" not in tags_block.group(1), (
        "hardcoded :nightly in the build tags — a workflow_dispatch "
        "0.0.0-dev build would move the channel every deployment tracks")
    assert "nightly_tag" in tags_block.group(1)
    assert "workflow_dispatch" in RELEASE and "nightly_tag=" in RELEASE, (
        "the compute step must gate nightly on the event kind")


def test_readme_describes_the_gosu_model():
    rd = (REPO / "README.md").read_text()
    assert "gosu" in rd and "SETUID" in rd, (
        "the security-model section described the retired static-UID era")


def test_v0_51_293_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert "0.51.293: " in init_py
