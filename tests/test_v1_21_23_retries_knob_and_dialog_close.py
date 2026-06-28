"""v1.21.23 — two simplification-review follow-ups (the user's decisions).

1. Remove the vestigial downloads.retries knob: the config field, the
   MOTIF_DL_RETRIES env binding, and the settings.html control all existed
   but the downloader hardcodes retries=2/fragment_retries=2 and never read
   the config value (the lone reader, the download_max_retries property, was
   already removed in v1.21.22). yt-dlp keeps its built-in retries.

2. Restore the info-dialog close after PROMOTE TO ACTIVE: the handler was
   meant to close the dialog so the user sees the row's new state, but
   referenced a nonexistent 'item-info-dialog' id (no-op for 3 tags).
   v1.21.22 removed the dead line; this wires the canonical closeInfoDialog()
   (targets info-dlg + does focus/audio teardown).
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


def test_retries_field_removed_from_config():
    from app.core.config_file import MotifConfig
    assert not hasattr(MotifConfig().downloads, "retries")


def test_retries_env_binding_removed():
    cf = (REPO / "app" / "core" / "config_file.py").read_text()
    assert "MOTIF_DL_RETRIES" not in cf
    assert '"downloads.retries"' not in cf


def test_retries_control_removed_from_settings():
    html = (REPO / "app" / "web" / "templates" / "settings.html").read_text()
    assert 'data-cfg-field="downloads.retries"' not in html
    assert "MAX RETRIES" not in html


def test_promote_to_active_closes_info_dialog():
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # the no-op dead id is gone...
    assert "item-info-dialog" not in js
    # ...and the canonical close is wired in the intent-flip success branch
    idx = js.index("if (typeof closeInfoDialog === 'function') closeInfoDialog();")
    # it sits in the intent-flip success branch, just after the
    # libraryRapidPoll kick (a multi-line comment sits between them).
    assert "libraryRapidPoll();" in js[idx - 800:idx]


def test_version_bumped():
    assert '__version__ = "0.' in (REPO / "app" / "__init__.py").read_text()
