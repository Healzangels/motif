"""v1.23.91 — a bulk DOWNLOAD reads "via DOWNLOAD TDB", not "via REPLACE TDB".

the user (Discord review): a bulk download of green-TDB rows produced theme_pushed
notifications saying "via REPLACE TDB" — "the notification doesn't track for what
occurred." Root cause: worker._do_download's force-place chain HARDCODED the
chained place job's theme_pushed reason to 'replace_with_themerrdb' regardless of
what triggered the download (the comment even admitted "the download→place chain
drops all action context").

A bulk download (reason 'bulk_select') just re-fetches + pushes the TDB theme — it
does NOT swap the active source TO TDB the way ACCEPT UPDATE / the REPLACE TDB
action do. Fix: the chained place reason now tracks the download's reason — a bulk
DOWNLOAD maps to a new 'download_tdb' push-reason ("DOWNLOAD TDB"); everything else
(ACCEPT UPDATE's upstream_update_accepted / bulk_update_accepted, the single
REPLACE TDB's own 'replace_with_themerrdb', any unknown) keeps "REPLACE TDB". The
title keeps its "set/replaced via TDB" framing (the bytes are still from TDB).
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
WORKER_PY = (REPO / "app" / "core" / "worker.py").read_text()


# ── notify_content: download_tdb label + title verb ──────────


def test_download_tdb_item_label_says_download_not_replace():
    from app.core.notify_content import format_theme_pushed_item
    ctx = {"display_title": "1408 (2007) · Director's Cut"}
    assert format_theme_pushed_item(ctx, reason="download_tdb") == (
        "1408 (2007) · Director's Cut — via DOWNLOAD TDB")


def test_download_tdb_label_registered():
    from app.core.notify_content import _PUSH_REASON_LABEL
    assert _PUSH_REASON_LABEL["download_tdb"] == "DOWNLOAD TDB"
    # the replace label is unchanged (ACCEPT UPDATE / REPLACE TDB still use it).
    assert _PUSH_REASON_LABEL["replace_with_themerrdb"] == "REPLACE TDB"


def test_download_tdb_title_shares_via_tdb_verb():
    """The bytes are from TDB, so the title still reads 'set/replaced via TDB'
    (the user was fine with the title — only the 'via REPLACE TDB' label was off)."""
    from app.core.notify_content import format_theme_pushed_title
    ctx = {"display_title": "1408 (2007)"}
    assert format_theme_pushed_title(
        ctx, reason="download_tdb", had_prior_theme=False) == (
        "📤 Theme set via TDB — 1408 (2007)")
    assert "replaced" in format_theme_pushed_title(
        ctx, reason="download_tdb", had_prior_theme=True)


def test_download_tdb_body_appends_download_label():
    from app.core.notify_content import format_theme_pushed_body
    ctx = {"display_title": "1408 (2007)"}
    body = format_theme_pushed_body(ctx, reason="download_tdb")
    assert "· via DOWNLOAD TDB" in body
    assert "REPLACE TDB" not in body


# ── worker: the force-place chain tracks the download reason ──


def test_worker_bulk_download_maps_to_download_tdb():
    """The chained place job's reason is DERIVED from the download's reason via
    the _DOWNLOAD_PUSH_REASON table (v1.23.94), not blanket-hardcoded — a bulk
    DOWNLOAD (bulk_select) → download_tdb; unlisted reasons fall to the
    replace_with_themerrdb default."""
    from app.core.worker import _DOWNLOAD_PUSH_REASON
    assert _DOWNLOAD_PUSH_REASON["bulk_select"] == "download_tdb"
    # consulted as a table with the REPLACE-TDB default (not an unconditional set).
    assert "_DOWNLOAD_PUSH_REASON.get(" in WORKER_PY
    assert '"replace_with_themerrdb")' in WORKER_PY


def test_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
