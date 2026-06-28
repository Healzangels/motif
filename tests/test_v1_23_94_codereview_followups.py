"""v1.23.94 — code-review follow-ups on the v1.23.89-93 session.

A formal 10-angle code-review of the session's diffs found the v1.23.89-92 work
clean, but surfaced a real CORRECTNESS gap in v1.23.93 (the bulk/single SET-URL
label fix): it mapped only the '/override' dialog SET URL ('override'); the
per-row SET URL endpoint (/api/plex_items/{rk}/manual-url, api.py:13170) builds an
INLINE force-place download payload with reason='manual_url', which fell through
to the REPLACE-TDB default — the exact mislabel v1.23.93 claimed to fix. The CSV
import ('bulk_import', api.py:14070, force_place=True) had the same problem.

Root cause (altitude finding): the worker reverse-engineered the user action from
an inline if/elif over a HARDCODED reason vocabulary, so each new force-place
download reason drifted into the wrong label. Fix: a centralized
_DOWNLOAD_PUSH_REASON table covering every force-place download reason, with the
REPLACE-TDB default reserved for genuine source-swaps-to-TDB.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent


def test_per_row_set_url_and_csv_import_are_not_replace_tdb():
    """manual_url (per-row SET URL) + bulk_import (CSV import) → set_url, NOT the
    REPLACE-TDB default. This is the v1.23.93 half-applied-fix gap closed."""
    from app.core.worker import _DOWNLOAD_PUSH_REASON
    assert _DOWNLOAD_PUSH_REASON["manual_url"] == "set_url"
    assert _DOWNLOAD_PUSH_REASON["bulk_import"] == "set_url"


def test_download_push_reason_values_are_valid_labels():
    """Every value the table can produce must be a known _PUSH_REASON_LABEL key
    (or the replace default) — so no reason maps to a missing/blank label."""
    from app.core.worker import _DOWNLOAD_PUSH_REASON
    from app.core.notify_content import _PUSH_REASON_LABEL
    for push_reason in set(_DOWNLOAD_PUSH_REASON.values()):
        assert push_reason in _PUSH_REASON_LABEL, push_reason
    # the default is also a valid label.
    assert "replace_with_themerrdb" in _PUSH_REASON_LABEL


def test_all_pure_download_reasons_map_to_download_tdb():
    from app.core.worker import _DOWNLOAD_PUSH_REASON
    for r in ("bulk_select", "bulk_missing", "manual"):
        assert _DOWNLOAD_PUSH_REASON[r] == "download_tdb"


def test_notify_docstring_reflects_apprise_clamp():
    """The dispatch docstring no longer tells operators to 'enable DEBUG' for
    per-URL apprise diagnostics — v1.23.92's clamp suppresses that (it leaks the
    webhook token). The stale advice would send operators down a dead path."""
    src = (REPO / "app" / "core" / "notify.py").read_text()
    i = src.index("Apprise.notify() returns True")
    block = src[i:i + 700]
    assert "wants to enable DEBUG" not in block
    assert "clamped to WARNING" in block


def test_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
