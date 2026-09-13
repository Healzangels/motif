"""v0.51.341 merge-time fixes: the boot banner masks plex.url credentials, and a
rejected staged database that cannot be discarded is logged, not swallowed."""
from __future__ import annotations

import logging
from pathlib import Path

from _slice_helpers import slice_between

from app.core import db_backup

REPO = Path(__file__).resolve().parent.parent


def test_boot_banner_masks_plex_url_credentials():
    src = (REPO / "app" / "main.py").read_text()
    line = slice_between(src, 'log.info("  plex_url     = %s"', "\n")
    assert "_mask_url(settings.plex_url)" in line, line
    from app.core.config_file import mask_url_credentials
    assert "pw" not in mask_url_credentials("http://user:pw@plex:32400")


def test_a_rejected_pending_that_cannot_be_discarded_is_logged(tmp_path, monkeypatch, caplog):
    db = tmp_path / "motif.db"
    pending = db_backup.restore_pending_path(db)
    pending.write_bytes(b"not a sqlite database")
    real_unlink = Path.unlink

    def unlink(self, *a, **k):
        if self == pending:
            raise PermissionError(13, "Permission denied", str(self))
        return real_unlink(self, *a, **k)
    monkeypatch.setattr(Path, "unlink", unlink)
    with caplog.at_level(logging.ERROR, logger=db_backup.log.name):
        res = db_backup.apply_pending_restore(db, tmp_path, now_stamp="20260913-120000")
    assert res and res.get("applied") is False
    assert pending.exists()
    assert any(r.levelno == logging.ERROR and pending.name in r.getMessage() for r in caplog.records), (
        "a rejected pending that stays must say so — it is refused again at every restart")
