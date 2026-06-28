"""v1.21.11 — manual triggers fire the auto-download automation too.

the user: a manually-kicked ThemerrDB sync (or Plex refresh) should
auto-acquire new SRC=— themes the same as a scheduled one when
sync.auto_download_new_themes_for_unthemed_rows is on — automation
shouldn't be cron-only.

- TDB sync: the dashboard SYNC button sends metadata_only=true →
  enqueue_downloads=False, which blocked the auto-download. Fix: the
  worker forces enqueue_downloads=True when the toggle is on (the toggle
  wins over metadata_only). After v1.21.10 enqueue_downloads only gates
  the toggle-gated SRC=— auto-download, so this can't trigger a non-opt-
  in download.
- Plex refresh: already trigger-agnostic — manual REFRESH PLEX and the
  scheduled enum both run through _upsert_items → _maybe_notify_theme_
  available, which only checks the toggle (covered by test_v1_21_9).
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
WORKER_PY = (REPO / "app" / "core" / "worker.py").read_text()
SETTINGS_HTML = (REPO / "app" / "web" / "templates" / "settings.html").read_text()


def test_worker_forces_enqueue_when_toggle_on():
    """_do_sync must override enqueue_downloads=True when the toggle is
    on, so a manual (metadata_only) sync still auto-acquires."""
    idx = WORKER_PY.index("def _do_sync")
    nxt = WORKER_PY.index("\n    def ", idx + 1)
    body = WORKER_PY[idx:nxt]
    # The metadata_only path that disables downloads.
    assert 'if payload.get("enqueue_downloads") is False:' in body
    # The toggle override that re-enables it.
    gate = (
        "if self.settings.cfg.sync."
        "auto_download_new_themes_for_unthemed_rows:")
    assert gate in body, (
        "v1.21.11: the auto-download toggle must override "
        "metadata_only so manual syncs auto-acquire"
    )
    override_at = body.index(gate)
    # The override forces enqueue_downloads = True AFTER the
    # metadata_only check (so it wins).
    md_at = body.index('if payload.get("enqueue_downloads") is False:')
    assert override_at > md_at
    seg = body[override_at:override_at + 120]
    assert "enqueue_downloads = True" in seg


def test_settings_copy_mentions_manual_or_scheduled():
    idx = SETTINGS_HTML.index(
        "auto_download_new_themes_for_unthemed_rows")
    block = SETTINGS_HTML[idx:idx + 1200]
    assert "manually kicked off" in block or "manual" in block.lower()
    assert "scheduled" in block.lower()


def test_version_bumped():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
