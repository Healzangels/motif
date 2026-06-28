"""v1.23.93 — extend v1.23.91 so single RE-DOWNLOAD + SET URL labels are coherent.

A code-review of v1.23.91 surfaced a gap: that tag fixed only the BULK download
('bulk_select') → "DOWNLOAD TDB", but the single-row RE-DOWNLOAD TDB ('manual',
api.py:20544) and SET URL ('override', api.py:20782) force-place paths still read
"via REPLACE TDB" — so a single RE-DOWNLOAD read "REPLACE TDB" while the bulk read
"DOWNLOAD TDB" (incoherent), and SET URL (a USER-provided URL, not TDB) was doubly
wrong.

Rule: "REPLACE TDB" appears ONLY for actions that swap the active source TO TDB —
ACCEPT UPDATE (upstream_update_accepted / bulk_update_accepted) and the explicit
REPLACE TDB action ('replace_with_themerrdb'). DOWNLOAD / RE-DOWNLOAD TDB
(bulk_select / bulk_missing / manual) → "DOWNLOAD TDB". SET URL ('override') →
"SET URL" with the neutral "Theme pushed to Plex" title (not "via TDB").
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
WORKER_PY = (REPO / "app" / "core" / "worker.py").read_text()


def test_set_url_label_registered_and_not_a_tdb_title():
    from app.core.notify_content import (
        _PUSH_REASON_LABEL, format_theme_pushed_item, format_theme_pushed_title)
    assert _PUSH_REASON_LABEL["set_url"] == "SET URL"
    ctx = {"display_title": "Some Movie (2020)"}
    assert format_theme_pushed_item(ctx, reason="set_url") == (
        "Some Movie (2020) — via SET URL")
    # SET URL is NOT a TDB source → neutral title, no "via TDB".
    title = format_theme_pushed_title(ctx, reason="set_url", had_prior_theme=True)
    assert title == "📤 Theme pushed to Plex — Some Movie (2020)"
    assert "via TDB" not in title


def test_worker_maps_single_redownload_and_set_url():
    """The _DOWNLOAD_PUSH_REASON table maps 'manual' (single RE-DOWNLOAD TDB) →
    download_tdb (coherent with the bulk), and 'override' (SET URL) → set_url —
    while ACCEPT UPDATE / REPLACE TDB are absent → the replace_with_themerrdb
    default. (v1.23.94 centralized this as a table + added manual_url/bulk_import.)"""
    from app.core.worker import _DOWNLOAD_PUSH_REASON
    assert _DOWNLOAD_PUSH_REASON["manual"] == "download_tdb"
    assert _DOWNLOAD_PUSH_REASON["override"] == "set_url"
    # genuine source-swaps-to-TDB are NOT in the table → fall to the default.
    assert "upstream_update_accepted" not in _DOWNLOAD_PUSH_REASON
    assert "bulk_update_accepted" not in _DOWNLOAD_PUSH_REASON
    assert "replace_with_themerrdb" not in _DOWNLOAD_PUSH_REASON
    assert "_DOWNLOAD_PUSH_REASON.get(" in WORKER_PY


def test_download_tdb_still_works():
    """v1.23.91 behavior preserved: download_tdb → DOWNLOAD TDB + 'via TDB' title."""
    from app.core.notify_content import (
        _PUSH_REASON_LABEL, format_theme_pushed_title)
    assert _PUSH_REASON_LABEL["download_tdb"] == "DOWNLOAD TDB"
    ctx = {"display_title": "X (2000)"}
    assert "set via TDB" in format_theme_pushed_title(
        ctx, reason="download_tdb", had_prior_theme=False)


def test_replace_label_unchanged():
    from app.core.notify_content import _PUSH_REASON_LABEL
    assert _PUSH_REASON_LABEL["replace_with_themerrdb"] == "REPLACE TDB"


def test_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
