"""v1.21.98 — RE-DOWNLOAD TDB BACKUP source-menu item for TB rows.

the user: REPLACE/PLACE on a TB (themerrdb-backup) row whose EDITION has no own
local_files (Watchmen Theatrical Cut — the row rendered TB via the '' fallback)
409'd "no local file to replace from — re-download first", but TB rows had NO
re-download option on the SOURCE menu. This adds RE-DOWNLOAD TDB BACKUP on TB
rows, reusing the edition-scoped /download-backup endpoint (download +
auto_place=False, behaviorally tested in v1.21.62) so a missing/mis-keyed
per-edition backup recovers + can then be PLACEd / PROMOTEd.
"""
from pathlib import Path


APP_JS = (Path(__file__).resolve().parent.parent
          / "app" / "web" / "static" / "app.js").read_text()


def test_redownload_tdb_backup_item_present_and_wired():
    idx = APP_JS.index("'RE-DOWNLOAD TDB BACKUP'")
    block = APP_JS[idx - 500:idx + 400]
    # Gated on the TB-row class (the LINK=TB classification).
    assert "isTdbBackup" in block
    # Reuses the existing edition-scoped download-backup action dispatch.
    assert "'download-tdb-backup'" in block
    # Edition-scoped: passes the clicked row's rating_key.
    assert "rk: it.rating_key" in block


def test_redownload_tdb_backup_gate():
    """The item shows on TB rows with a live TDB url that isn't blocked."""
    idx = APP_JS.index("'RE-DOWNLOAD TDB BACKUP'")
    gate = APP_JS[idx - 300:idx]
    assert "isTdbBackup && it.youtube_url && !tdbReplaceBlocked" in gate


def test_v1_21_98_version_pin():
    init_py = (Path(__file__).resolve().parent.parent
               / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
