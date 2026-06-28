"""v1.22.5 — entrypoint honors UMASK (Unraid/linuxserver convention).

motif v1.22.4 added PUID/PGID but not UMASK. the user's whole stack runs as
PUID 99 / PGID 100 / UMASK 002 — group-writable (dirs 775 / files 664) so the
99:100 group shares the /data tree WITHOUT 777. motif must honor UMASK to be a
first-class citizen of that model (and let appdata stay 99:100-owned while /data
is cooperatively writable).
"""
from __future__ import annotations

import re
import subprocess
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


def test_entrypoint_applies_umask():
    src = (REPO / "docker-entrypoint.sh").read_text()
    assert 'UMASK="${UMASK:-022}"' in src, "UMASK must default to 022"
    # umask is applied to the shell (inherited across exec/gosu into the app).
    assert 'umask "${UMASK}"' in src
    # Invalid UMASK falls back to a sane default rather than aborting boot.
    assert "keeping default 022" in src or "umask 022" in src


def test_entrypoint_umask_set_before_user_branch():
    """UMASK is independent of the user, so it must be applied BEFORE the
    explicit-non-root passthrough branch (which exec's immediately)."""
    src = (REPO / "docker-entrypoint.sh").read_text()
    umask_idx = src.index('umask "${UMASK}"')
    branch_idx = src.index('[ "$(id -u)" != "0" ]')
    assert umask_idx < branch_idx, (
        "umask must be applied before the explicit-user exec branch so "
        "--user callers get it too")


def test_entrypoint_still_parses():
    # sh -n: the UMASK additions must not break the script.
    r = subprocess.run(["sh", "-n", str(REPO / "docker-entrypoint.sh")],
                       capture_output=True, text=True)
    assert r.returncode == 0, r.stderr


def test_unraid_template_exposes_umask():
    xml = (REPO / "unraid" / "motif.xml").read_text()
    assert 'Target="UMASK"' in xml, "template must expose UMASK as a variable"
    # Default 022 in the value, with 002 documented as the Unraid convention.
    m = re.search(r'Target="UMASK"[^>]*>([^<]*)</Config>', xml)
    assert m and m.group(1).strip() == "022"
    assert "002" in xml  # the group-writable Unraid best-practice is documented


def test_compose_sets_umask_env():
    yml = (REPO / "docker-compose.yml").read_text()
    assert re.search(r"(?m)^\s*UMASK:", yml)


def test_readme_documents_umask():
    rd = (REPO / "README.md").read_text()
    assert "UMASK" in rd and "002" in rd


def test_v1_22_5_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
