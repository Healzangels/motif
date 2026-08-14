"""v0.51.262 — persistent rotating log under /config.

stdout was the ONLY place motif's Python logs ever went, and an Unraid Force
Update recreates the container and discards its JSON log. So every deploy
erased all history.

That is not hypothetical. On 2026-08-13 two forensic questions were asked of
prod and BOTH were unanswerable for exactly this reason:

  * had the events flusher ever logged "DROPPING batch"? (the consequence
    v0.51.260 fixed — we could not tell whether it had ever fired)
  * how slow does upload_collection_theme actually get in the field?

The probe ran against a 35-line, six-minute-old container log, so its `0` was
an artifact rather than an answer. Neither is reconstructable from the `events`
table: the flusher IS the events writer (it cannot record its own failure
there), and the table prunes at 30 days. /config survives container
replacement; stdout does not.

Scope note: this is a HANDLER, not a new log stream. Everything already logged
lands in the file with the identical format, so the file and `docker logs`
correlate line-for-line.
"""
from __future__ import annotations

import logging
from pathlib import Path

import pytest

from app import main as m


REPO = Path(__file__).resolve().parent.parent


@pytest.fixture(autouse=True)
def _detach():
    """Remove any handler this module attaches — a leaked file handler would
    write every LATER test's log output into a tmp dir that pytest deletes."""
    def _strip():
        root = logging.getLogger()
        for h in list(root.handlers):
            if getattr(h, "_motif_file_log", False):
                root.removeHandler(h)
                h.close()
    _strip()
    yield
    _strip()


def _file_handlers():
    return [h for h in logging.getLogger().handlers
            if getattr(h, "_motif_file_log", False)]


# ── it actually persists ─────────────────────────────────────


def test_log_lands_in_a_file_under_config_dir(tmp_path):
    m._attach_file_log(tmp_path)
    logging.getLogger("motif.probe").warning("persisted line %d", 42)
    for h in _file_handlers():
        h.flush()

    logfile = tmp_path / "logs" / "motif.log"
    assert logfile.exists(), "no file created under <config_dir>/logs"
    body = logfile.read_text()
    assert "persisted line 42" in body
    assert "motif.probe" in body and "WARNING" in body


def test_file_and_stdout_share_one_format():
    """They must correlate line-for-line with `docker logs`. Two format
    strings would drift; there is exactly one definition."""
    src = (REPO / "app" / "main.py").read_text()
    assert src.count('_LOG_FORMAT = "') == 1
    assert src.count("_LOG_DATEFMT = ") == 1
    # basicConfig and the file handler both consume the constants, not literals.
    assert "format=_LOG_FORMAT" in src
    assert "logging.Formatter(_LOG_FORMAT, datefmt=_LOG_DATEFMT)" in src


def test_rotation_is_bounded(tmp_path):
    """An unbounded log on the appdata share would eventually fill it. Drive a
    real rotation rather than asserting the constructor args."""
    m._attach_file_log(tmp_path)
    h = _file_handlers()[0]
    h.maxBytes = 2048          # shrink so the test costs milliseconds
    h.backupCount = 2
    # WARNING, not INFO: the handler inherits the ROOT level (by design — the
    # file must mirror stdout exactly), and the root sits at WARNING under
    # pytest, so INFO filler would be dropped and nothing would ever rotate.
    for i in range(400):
        logging.getLogger("motif.probe").warning("filler line %04d padded", i)
    h.flush()

    logs = sorted(p.name for p in (tmp_path / "logs").iterdir())
    assert "motif.log" in logs
    assert any(n.startswith("motif.log.") for n in logs), "never rotated"
    assert len(logs) <= 3, f"backupCount not honoured: {logs}"


def test_shipped_ceiling_is_sane():
    assert m._LOG_FILE_MAX_BYTES * (m._LOG_FILE_BACKUPS + 1) <= 128 * 1024 * 1024


# ── it must never break boot ─────────────────────────────────


def test_unwritable_config_dir_warns_and_continues(tmp_path, caplog):
    """A read-only /config or a uid mismatch must cost persistence, NOT the
    process. And it must say so — a silently absent log file is the exact
    'I looked and there was nothing there' trap this handler exists to close."""
    blocked = tmp_path / "ro"
    blocked.mkdir()
    blocked.chmod(0o500)  # r-x: mkdir of logs/ will EACCES
    try:
        with caplog.at_level(logging.WARNING, logger="motif.main"):
            m._attach_file_log(blocked)          # must NOT raise
        assert "persistent log unavailable" in caplog.text
        assert "history will NOT survive" in caplog.text
        assert _file_handlers() == []
    finally:
        blocked.chmod(0o700)


def test_attach_is_idempotent(tmp_path):
    """configure_logging can be called more than once (tests, a re-entry).
    Stacking handlers would duplicate every line in the file."""
    m._attach_file_log(tmp_path)
    m._attach_file_log(tmp_path)
    m._attach_file_log(tmp_path)
    assert len(_file_handlers()) == 1


def test_configure_logging_without_config_dir_stays_stdout_only(tmp_path):
    """The v1.14.64 logger tests call configure_logging('DEBUG') with one arg.
    That signature must keep working and must NOT write a file anywhere."""
    m.configure_logging("DEBUG")
    assert _file_handlers() == []


def test_boot_wires_the_config_dir_through():
    """The handler is worthless if main() never passes a directory — the
    v1.18.81 phantom lesson: assert the pipe, not just the branch."""
    src = (REPO / "app" / "main.py").read_text()
    assert "configure_logging(settings.log_level, settings.config_dir)" in src


# ── the security consequence of persisting ───────────────────


def test_apprise_stays_clamped_now_that_logs_persist():
    """v1.23.92 clamps apprise to WARNING because it logs full webhook URLs
    INCLUDING the token at DEBUG. That mattered when the line scrolled away in
    `docker logs`; it matters more now that it would land in a file on the
    appdata share. Pinned as a pair with the file handler."""
    m.configure_logging("DEBUG")
    assert logging.getLogger("apprise").level == logging.WARNING


def test_v0_51_262_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
