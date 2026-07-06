"""v0.51.86 — Plex-backup message distinguishes first-capture from re-capture.

the user (deployed): "confused on what this message means when downloading a
plex row as a backup". The DOWNLOAD PLEX BACKUP success alert always said
"Captured — Plex was serving a theme different from your existing backup" — but
that "captured" branch fires on a FIRST-ever backup too (the common case for a
NO-TDB anime), where there was no existing backup to differ from.

Proper fix: the worker already knows (it looks up the row's existing local_files
entry). Surface it — backup_cloud_theme returns replaced_prior; the op stamps
backup_outcome.replaced; the UI branches "Updated (re-captured)" vs "Captured
(first backup)".
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
CTB_PY = (REPO / "app" / "core" / "cloud_theme_backup.py").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def test_worker_returns_replaced_prior():
    # the success return carries whether a prior backup row existed.
    assert '"replaced_prior": existing is not None,' in CTB_PY


def test_api_stamps_replaced_count():
    # the op outcome carries the replaced count for the UI to branch on.
    assert '"replaced": sum(' in API_PY
    assert "d.get(\"replaced_prior\")" in API_PY


def test_js_branches_first_capture_vs_replaced():
    idx = APP_JS.index("function cloudBackupForceCapture(")
    fn = APP_JS[idx:idx + 8400]
    # the old unconditional false-premise copy is gone.
    assert "Captured — Plex was serving a theme different from" not in fn, (
        "v0.51.86: the 'captured' branch must not unconditionally claim the "
        "theme differed from an existing backup")
    # both branches present, keyed on the stamped replaced count.
    assert "outcome.replaced > 0" in fn
    assert "Updated — Plex was serving a theme different from your" in fn, (
        "the re-capture branch")
    assert "Captured — motif saved the theme Plex is currently" in fn, (
        "the first-capture branch")
    # the TDB-revert suffix is shared by both branches.
    assert "This row has no ThemerrDB theme to revert to." in fn
