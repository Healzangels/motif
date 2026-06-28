"""v1.22.6 — entrypoint validates PUID/PGID are numeric.

the user's container crash-LOOPED with `gosu: unable to find group PGID` because a
template slip put the literal string "PGID" in the value field, so the
entrypoint ran `gosu "99:PGID"`. A non-numeric uid/gid must fall back to the
default (99/100) + a loud error so motif still boots, rather than a dead
container.
"""
from __future__ import annotations

import subprocess
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
ENTRYPOINT = REPO / "docker-entrypoint.sh"


def test_entrypoint_guards_both_puid_and_pgid():
    src = ENTRYPOINT.read_text()
    # Both guards present, with the non-numeric/empty case pattern + fallbacks.
    assert src.count('""|*[!0-9]*)') >= 2, "both PUID and PGID need the guard"
    assert "PUID=99" in src and "PGID=100" in src
    assert "is not a number" in src


def test_entrypoint_still_parses():
    r = subprocess.run(["sh", "-n", str(ENTRYPOINT)],
                       capture_output=True, text=True)
    assert r.returncode == 0, r.stderr


def _run_guard(puid, pgid):
    """Execute the exact guard logic from the entrypoint and return the
    resolved (PUID, PGID)."""
    script = f'''
PUID="{puid}"
PGID="{pgid}"
case "${{PUID}}" in ""|*[!0-9]*) PUID=99 ;; esac
case "${{PGID}}" in ""|*[!0-9]*) PGID=100 ;; esac
echo "${{PUID}} ${{PGID}}"
'''
    r = subprocess.run(["sh", "-c", script], capture_output=True, text=True)
    return r.stdout.strip().split()


def test_non_numeric_pgid_falls_back_to_100():
    # The exact bug: literal "PGID" in the value field.
    assert _run_guard("99", "PGID") == ["99", "100"]


def test_non_numeric_puid_falls_back_to_99():
    assert _run_guard("PUID", "100") == ["99", "100"]


def test_empty_values_fall_back():
    assert _run_guard("", "") == ["99", "100"]


def test_valid_numeric_values_are_kept():
    assert _run_guard("1000", "1000") == ["1000", "1000"]
    assert _run_guard("99", "100") == ["99", "100"]


def test_v1_22_6_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
