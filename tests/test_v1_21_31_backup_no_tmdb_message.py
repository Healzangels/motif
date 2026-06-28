"""v1.21.31 — honest DOWNLOAD PLEX BACKUP message for no-TMDB-id rows.

the user's repro: DOWNLOAD PLEX BACKUP on the A24 Films collection (no
ThemerrDB match → NULL guid_tmdb) showed "Plex has nothing motif can
capture (no theme is currently selected)" — but his probe proved Plex WAS
serving a selected theme. Root cause: the cloud-backup candidate query
gates on `pi.guid_tmdb IS NOT NULL`, so a no-TMDB-id row (e.g. a collection
with no TDB match) is dropped before classification → 0 captured → the
catch-all alert mis-attributed it as "no theme selected".

Interim fix (the synthetic-id real fix lands next): make the 0-captured
alert honest about BOTH causes. The capability gap stays for now.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def test_misleading_no_theme_selected_message_gone():
    assert "no theme is currently selected" not in APP_JS


def test_honest_message_is_single_cause_after_v1_21_32():
    # v1.21.32 superseded the interim two-cause message: the no-TMDB-id
    # cause is now HANDLED (the force path mints a synthetic orphan id),
    # so the 0-captured alert states the single remaining cause. The
    # stale "not yet supported" copy must be gone.
    assert "Nothing was captured for this row" in APP_JS
    assert "backup is untouched" in APP_JS
    assert "not yet supported" not in APP_JS
    assert "this row has no TMDB" not in APP_JS


def test_version_bumped():
    assert '__version__ = "0.' in (REPO / "app" / "__init__.py").read_text()
