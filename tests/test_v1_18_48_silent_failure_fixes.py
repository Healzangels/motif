"""v1.18.48 — silent-failure audit fixes.

the user on v1.18.47 close: "let's do a check for potential silent
failures and bugs that may have been introduced during our last
sessions." Audit surfaced two Tier-A class-9 sites from the
v1.18.31-47 cascade — both fixed here.

## Bug 1 — orphan dashboard swallowed non-2xx fetches

The dashboard's per-row action handler (RE-PUSH / LET PLEX SERVE /
PURGE / DELETE SIDECAR) wrapped each fetch in try/catch but never
checked `r.ok` — fetch() only throws on network errors, so any
backend 500/403/404 completed the await normally, the button
flipped to ✓, and a re-scan fired against unchanged state. The
operator had no way to tell the action failed. Same class-9
silent-failure shape that surfaced multiple times in the v1.17.13
audit — fix mirror: explicit r.ok check + alert + FAIL label.

Additionally `delete-sidecar` returns ok=true / deleted=false
when no sidecar lands at the resolved folder path (idempotent
"already gone" case). Pre-fix that rendered ✓ DONE which
misrepresents the outcome. v1.18.48 surfaces it as a NO-OP
button state so the operator sees "nothing was deleted" plainly.

## Bug 2 — find_theme_sidecar_path swallowed OSError silently

The v1.18.43 helper iterated candidate paths and treated any
OSError (stale NFS mount, vanished folder, permission denied) as
"no sidecar here" — same class-9 shape as the v1.17.11 hot-path
sub-pattern. Pre-fix a transient mount fault returned None on
every orphan-scan row, and the dashboard showed "no orphan
sidecar found" for paths that were really just unreachable.

Fix uses the v1.17.11 hot-path sub-pattern: module-level
`_FIND_THEME_SIDECAR_OSERROR_WARNED` flag, first occurrence
logs at WARN (operator sees one line in logs), subsequent
occurrences drop to DEBUG so a fleet-wide fault doesn't drown
the log. Pattern mirrors auth.py:_VERIFY_PASSWORD_WARNED and
sync.py:_GIT_MIRROR_READ_JSON_WARNED.
"""

from __future__ import annotations

import logging
from pathlib import Path
from unittest.mock import patch

import pytest


REPO = Path(__file__).resolve().parent.parent
ORPHANS_HTML = REPO / "app" / "web" / "templates" / "orphans.html"
PLEX_ENUM_PY = REPO / "app" / "core" / "plex_enum.py"


# ── Bug 1: orphan dashboard r.ok check ───────────────────────


def test_orphan_action_handler_checks_response_ok():
    """The per-row action handler must explicitly inspect r.ok
    after each fetch. fetch() doesn't throw on non-2xx, so the
    pre-fix try/except absorbed every backend failure silently."""
    src = ORPHANS_HTML.read_text()
    # The check must appear in the action-handler region (after
    # the await-fetch calls, before the success branch).
    assert "r && !r.ok" in src or "!r.ok" in src, (
        "v1.18.48: orphan dashboard action handler must inspect "
        "r.ok before treating the fetch as successful"
    )
    # The fail branch must throw so the catch-block's FAIL label
    # + alert fire (preserves the visible-failure UX from
    # v1.17.13's loader audit).
    assert "throw new Error(`HTTP ${r.status}" in src


def test_orphan_action_handler_surfaces_detail_on_failure():
    """When the backend returns JSON {detail: ...} or {error:
    ...}, the alert must include it (so the operator can
    distinguish 'plex returned 500' from 'rk not found')."""
    src = ORPHANS_HTML.read_text()
    assert "j.detail || j.error" in src, (
        "v1.18.48: failure alert must surface the backend's "
        "detail/error message rather than just the status code"
    )


def test_orphan_delete_sidecar_surfaces_noop():
    """delete-sidecar returns ok=true / deleted=false when no
    sidecar lands at the resolved folder. v1.18.48 surfaces
    that as a NO-OP button state so the operator sees 'nothing
    was deleted' rather than a misleading ✓ DONE."""
    src = ORPHANS_HTML.read_text()
    assert "deleted === false" in src
    assert "'NO-OP'" in src or '"NO-OP"' in src


def test_orphan_action_handler_keeps_visible_fail_label():
    """The catch block must continue to set the FAIL label +
    alert (mirror of the v1.17.13 visible-failure pattern).
    Regression guard so a future refactor doesn't silently
    drop the user-facing breadcrumb."""
    src = ORPHANS_HTML.read_text()
    assert "btn.textContent = 'FAIL'" in src
    assert "alert(`Action ${act} failed:" in src


# ── Bug 2: find_theme_sidecar_path warns on OSError ──────────


def test_find_theme_sidecar_path_warns_on_oserror_first_time():
    """First OSError occurrence must log at WARN so the
    operator sees one breadcrumb in logs. Mirrors v1.17.11
    hot-path sub-pattern (auth.py / sync.py)."""
    from app.core import plex_enum

    # Reset the once-flag so we observe the first-call behavior.
    plex_enum._FIND_THEME_SIDECAR_OSERROR_WARNED = False

    # Patch _candidate_local_paths to yield a single path whose
    # is_dir() returns True but iterdir() raises OSError —
    # matches the stale-NFS / unmounted-share failure mode.
    fake_path = Path("/nonexistent-stale-mount/some-folder")

    def fake_candidates(folder_path):
        yield fake_path

    with patch.object(plex_enum, "_candidate_local_paths", fake_candidates):
        with patch.object(Path, "is_dir", return_value=True):
            with patch.object(
                Path, "iterdir",
                side_effect=OSError("Stale NFS file handle"),
            ):
                with patch.object(plex_enum.log, "warning") as mock_warn:
                    with patch.object(plex_enum.log, "debug") as mock_debug:
                        result = plex_enum.find_theme_sidecar_path(
                            "/some/folder"
                        )
                        # Returns None (couldn't find sidecar).
                        assert result is None
                        # First occurrence: WARN, not debug.
                        assert mock_warn.called, (
                            "v1.18.48: first OSError must log at "
                            "warn so the operator sees a breadcrumb"
                        )
                        # The warn payload must reference the
                        # candidate path + the OSError.
                        warn_args = mock_warn.call_args
                        assert "OSError" in warn_args[0][0] \
                            or "scanning" in warn_args[0][0]
                        # Flag must be set so the next call goes
                        # to debug.
                        assert plex_enum._FIND_THEME_SIDECAR_OSERROR_WARNED


def test_find_theme_sidecar_path_drops_to_debug_after_first():
    """Subsequent OSError occurrences drop to DEBUG so a
    fleet-wide stale mount doesn't drown the log. Pattern
    mirrors auth.py:_VERIFY_PASSWORD_WARNED."""
    from app.core import plex_enum

    # Pretend the WARN already fired (e.g., we're mid-scan).
    plex_enum._FIND_THEME_SIDECAR_OSERROR_WARNED = True

    fake_path = Path("/nonexistent-stale-mount/another-folder")

    def fake_candidates(folder_path):
        yield fake_path

    with patch.object(plex_enum, "_candidate_local_paths", fake_candidates):
        with patch.object(Path, "is_dir", return_value=True):
            with patch.object(
                Path, "iterdir",
                side_effect=OSError("Stale NFS file handle"),
            ):
                with patch.object(plex_enum.log, "warning") as mock_warn:
                    with patch.object(plex_enum.log, "debug") as mock_debug:
                        result = plex_enum.find_theme_sidecar_path(
                            "/some/folder"
                        )
                        assert result is None
                        # Second occurrence: no WARN, only DEBUG.
                        assert not mock_warn.called, (
                            "v1.18.48: subsequent OSErrors must "
                            "drop to debug to avoid drowning logs"
                        )
                        assert mock_debug.called, (
                            "v1.18.48: subsequent OSErrors must "
                            "still leave a debug breadcrumb"
                        )


def test_find_theme_sidecar_path_still_returns_none_on_empty_input():
    """Empty folder_path is the documented happy-path None
    return (not an error condition). Make sure the new logging
    didn't break that branch."""
    from app.core.plex_enum import find_theme_sidecar_path
    assert find_theme_sidecar_path("") is None
    assert find_theme_sidecar_path(None) is None  # type: ignore[arg-type]


def test_find_theme_sidecar_path_finds_real_sidecar_in_tmp(tmp_path):
    """End-to-end: actual disk read still works. v1.18.48's
    logging additions must not have changed the happy path."""
    from app.core.plex_enum import find_theme_sidecar_path

    folder = tmp_path / "Some Movie (2024)"
    folder.mkdir()
    theme = folder / "theme.mp3"
    theme.write_bytes(b"fake-audio")

    found = find_theme_sidecar_path(str(folder))
    assert found is not None
    assert found.name == "theme.mp3"


# ── Module-level flag is declared the canonical way ──────────


def test_find_theme_sidecar_warned_flag_module_level():
    """The once-flag must live at module level (not function
    local) so it survives across calls — same shape as
    _VERIFY_PASSWORD_WARNED / _GIT_MIRROR_READ_JSON_WARNED."""
    src = PLEX_ENUM_PY.read_text()
    assert "_FIND_THEME_SIDECAR_OSERROR_WARNED: bool = False" in src, (
        "v1.18.48: declare the once-flag at module scope with "
        "the same shape as auth.py / sync.py precedents"
    )
    # The except handler must use the `global` declaration before
    # assignment (the v1.17.11 pattern).
    assert "global _FIND_THEME_SIDECAR_OSERROR_WARNED" in src
