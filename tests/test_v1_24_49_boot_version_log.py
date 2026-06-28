"""v1.24.49 — stamp the running version on the first boot log line.

The motif version showed only in the UI topbar; a pasted `docker logs` snippet
carried no version marker, so logs alone couldn't tell which build emitted them.
The boot now logs `motif v<__version__> starting`. the user's ask while diagnosing
the worker-startup lines.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent


def test_boot_line_interpolates_version():
    src = (REPO / "app" / "main.py").read_text()
    # __version__ is imported from the package and used on the first boot line.
    assert "from . import __version__" in src
    assert 'log.info("motif v%s starting", __version__)' in src
    # the bare (version-less) line must be gone so logs always carry the build.
    assert 'log.info("motif starting")' not in src
