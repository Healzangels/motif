"""v1.21.35 — DOWNLOAD PLEX BACKUP force-capture copy is accurate for
TDB-less rows (Kometa collections).

the user: the A24 Films collection theme came from Kometa (no ThemerrDB
match → plex_orphan). The force-capture confirm + success copy implied a
green ThemerrDB pill and told the user to revert via SOURCE → DOWNLOAD
TDB BACKUP — neither exists for a TDB-less row. v1.21.35 branches the copy
on a hasTdb flag plumbed from the row's upstream_source.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def _force_capture_fn():
    idx = APP_JS.index("function cloudBackupForceCapture(")
    return APP_JS[idx:idx + 7200]


def test_has_tdb_flag_plumbed_from_upstream_source():
    # The menu item computes hasTdb from a real (non-orphan)
    # upstream_source + a TDB youtube_url, emits it as a data attr,
    # the handler reads it, and passes it to the force-capture helper.
    assert "it.upstream_source !== 'plex_orphan'" in APP_JS
    assert "hasTdb: cloudBackupHasTdb ? '1' : '0'" in APP_JS
    assert 'data-has-tdb="${extras.hasTdb}"' in APP_JS
    assert "btn.dataset.hasTdb === '1'" in APP_JS
    # v0.51.51: the per-row handler passes the swap flag as the 3rd arg.
    assert "cloudBackupForceCapture(rk, hasTdb, allowExistingLocal)" in APP_JS


def test_force_capture_signature_takes_has_tdb():
    assert "function cloudBackupForceCapture(rk, hasTdb, isSwap)" in APP_JS


def test_copy_branches_on_has_tdb():
    fn = _force_capture_fn()
    assert "hasTdb" in fn
    # TDB branch keeps the revert instructions (accurate for real TDB rows)
    assert "re-pull the ThemerrDB theme anytime via SOURCE" in fn
    # no-TDB branch is honest — names the Kometa case, no revert promise
    assert "no ThemerrDB theme" in fn
    assert "Kometa-built" in fn
    assert "no green TDB pill to revert to" in fn
    assert "no ThemerrDB theme to revert to" in fn


def test_version_pin():
    assert '__version__ = "0.' in (REPO / "app" / "__init__.py").read_text()
