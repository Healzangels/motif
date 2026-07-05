"""v0.51.66 — empty-state copy names the ACTUAL refresh button.

Collections/library polish sweep. Two user-facing empty-state messages in
app.js told the user to "click REFRESH FROM PLEX" — but the per-tab library /
collections refresh button was renamed to "// REFRESH COLLECTIONS" /
"// REFRESH MOVIES" / … back in v1.13.63 (label) + v1.18.4 (collections). No
button reads "REFRESH FROM PLEX" anymore; the phrase survives only as an
internal comment shorthand for that button.

  1. The library-table empty-state (loadLibrary's zero-rows `else` branch —
     renders on /collections when a managed section has no rows) now reads
     "click the // REFRESH button above" (tab-agnostic — every variant of the
     button starts with "// REFRESH" and sits above the table).

  2. The jobs-queue empty-state now names both buttons by their real
     "// "-prefixed labels: "// SYNC THEMERRDB" and "// REFRESH".

These are the two USER-FACING strings only — the comment shorthands
("REFRESH FROM PLEX" as a code-archaeology name for the button) are left
untouched, so a blanket substring check would false-match them. Anchor on the
message-unique phrases instead.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def test_library_empty_state_points_at_the_real_refresh_button():
    """The loadLibrary zero-rows else-branch must reference "// REFRESH", not
    the retired "REFRESH FROM PLEX" label."""
    # New copy present.
    assert "then click the // REFRESH button above" in JS
    # Old message tail gone (message-unique — the comment shorthand is just
    # "click REFRESH FROM PLEX", never "and click REFRESH FROM PLEX").
    assert "and click REFRESH FROM PLEX" not in JS


def test_jobs_queue_empty_state_names_buttons_by_real_labels():
    """The jobs-list-empty message names both buttons by their actual
    "// "-prefixed labels."""
    assert "click // SYNC THEMERRDB on the dashboard, // REFRESH on a library" in JS
    # Old un-prefixed / retired-label form gone.
    assert "SYNC THEMERRDB on the dashboard, REFRESH FROM PLEX on a library" not in JS


def test_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.51.66"' in init_py
