"""v1.14.64 — quiet dulwich + httpcore loggers (DEBUG noise).

the user's log review surfaced a signal-to-noise problem with
DEBUG logging enabled in production: every git fetch dumps ~600
lines of `dulwich.protocol git< b'... refs/pull/...'` (the full
ThemerrDB ref advertisement), plus per-byte packfile receive
frames. `httpcore` (httpx's underlying library) similarly
floods with connection / send / receive frames.

motif's `configure_logging` already quiets `apscheduler`,
`httpx`, `urllib3`, `yt_dlp` — but missed `dulwich` (used by
the git mirror sync tier) and `httpcore` (httpx's transport
layer; quieting httpx alone is a half-fix).

Fix: add both to the existing quiet-list in
`app/main.py:configure_logging`. The motif-side INFO log lines
("Sync run #N: git mirror acquired" etc.) carry the operator-
visible signal already; the per-frame DEBUG output is
diagnostic-only.

This test pins the quiet-list shape so a future contributor
who adds new third-party imports (or removes a quiet rule)
gets a clear regression signal.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── The quiet-list pin ──────────────────────────────────────


def test_configure_logging_quiets_dulwich():
    """`dulwich` (and its sub-loggers `dulwich.protocol`,
    `dulwich.config`) flood DEBUG with git protocol frames on
    every sync. Quiet at the parent-logger level so all
    sub-loggers inherit."""
    src = (REPO / "app" / "main.py").read_text()
    assert 'logging.getLogger("dulwich").setLevel(logging.WARNING)' in src


def test_configure_logging_quiets_httpcore():
    """`httpcore` is httpx's underlying transport — emits its own
    DEBUG connection/send/receive frames per request. Quieting
    only `httpx` (already in the list) leaves these visible."""
    src = (REPO / "app" / "main.py").read_text()
    assert 'logging.getLogger("httpcore").setLevel(logging.WARNING)' in src


def test_configure_logging_preserves_existing_quiet_rules():
    """Sanity: the v1.14.64 additions don't accidentally remove
    the pre-existing apscheduler/httpx/urllib3/yt_dlp rules.
    Same shape — `getLogger(<name>).setLevel(logging.WARNING)`."""
    src = (REPO / "app" / "main.py").read_text()
    for name in ("apscheduler", "httpx", "urllib3", "yt_dlp"):
        assert f'logging.getLogger("{name}").setLevel(logging.WARNING)' in src, (
            f"pre-existing quiet rule for {name} dropped"
        )


# ── Behavioral: configure_logging actually applies the levels ──


def test_configure_logging_applies_levels_at_call_time():
    """End-to-end: calling configure_logging('DEBUG') must leave
    dulwich + httpcore at WARNING, even though the root level is
    DEBUG. Without the explicit setLevel, the loggers would
    inherit the root's DEBUG level and flood."""
    import logging
    from app.main import configure_logging

    # Snapshot prior state so we don't pollute other tests.
    snapshot = {
        name: logging.getLogger(name).level
        for name in ("dulwich", "httpcore", "apscheduler",
                     "httpx", "urllib3", "yt_dlp")
    }
    try:
        configure_logging("DEBUG")
        # Each chatty logger should be WARNING regardless of root.
        for name in ("dulwich", "httpcore", "apscheduler",
                     "httpx", "urllib3", "yt_dlp"):
            assert logging.getLogger(name).level == logging.WARNING, (
                f"{name} logger not pinned to WARNING by "
                f"configure_logging('DEBUG') — would flood DEBUG output."
            )
    finally:
        # Restore prior state.
        for name, lvl in snapshot.items():
            logging.getLogger(name).setLevel(lvl)
