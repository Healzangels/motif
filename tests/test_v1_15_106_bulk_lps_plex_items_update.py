"""v1.15.106 — BULK LPS must update plex_items.local_theme_file=0.

the user's repro (v1.15.100): a U row with active user-override URL,
BULK LPS clicked, row then rendered as M (+P) instead of staying U
or dropping to P. Subsequent ADOPT click failed with
`no theme.mp3 at /data/media/tv/<title>` — because the actual
canonical was at `/data/media/themes/tv/<title>/theme.mp3` (motif's
themes_dir), not the content folder.

## Root cause

`_bulk_lps_run` at `api.py:3390-3406` deleted the placements row
but did NOT update `plex_items.local_theme_file = 0`. The SRC SQL
at `_SRC_LETTER_SQL` then read:

  - `p.media_folder IS NULL` (placement deleted) ✓ correct
  - `pi.local_theme_file = 1` (STALE) ✗ never reset by BULK LPS

The M branch fires when `media_folder IS NULL AND local_theme_file
= 1`, so the row was misclassified.

## Fix

Mirror `api_unplace_item`'s pattern (api.py:10870) — after the
DELETE FROM placements, UPDATE plex_items SET local_theme_file=0,
plex_theme_verified_ok=NULL using the same natural-key lookup
(theme_id FK preferred, guid_tmdb+media_type fallback) introduced
in v1.15.90.

## Static guard

The fix is hard to test in-process without standing up a DB —
this test is a static guard that the `_bulk_lps_run` body
contains both the DELETE FROM placements AND the UPDATE
plex_items with local_theme_file=0 inside the same
`transaction(conn)` block.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"


def _bulk_lps_body() -> str:
    """Scan from `def _bulk_lps_run(` to the next module-level
    `def `. Window is large (~10k chars) so the audit narrative
    + the actual fix both fit comfortably."""
    src = API_PY.read_text()
    start = src.index("def _bulk_lps_run(")
    end = src.index("\ndef ", start + 1)
    return src[start:end]


def test_bulk_lps_updates_plex_items_local_theme_file():
    """The fix must add an UPDATE plex_items SET local_theme_file=0
    inside the same DELETE-FROM-placements transaction. Pre-fix
    the DELETE landed without the matching plex_items reset,
    leaving the SRC letter SQL to render M instead of dropping to
    P or '-' when the canonical was at themes_dir."""
    body = _bulk_lps_body()
    # Both writes must be present in the BULK LPS handler.
    assert "DELETE FROM placements" in body
    assert "local_theme_file = 0" in body
    assert "plex_theme_verified_ok = NULL" in body


def test_bulk_lps_uses_natural_key_lookup_for_plex_items():
    """Natural-key lookup mirrors v1.15.90 — theme_id FK preferred,
    guid_tmdb + media_type fallback. Folder-path lookups are
    forbidden for the plex_items UPDATE because BULK LPS doesn't
    have the content-folder path in scope (and even if it did,
    folder_path is host-domain vs container-domain on multi-volume
    Unraid setups — the v1.15.90/.91/.92 path-mismatch class)."""
    body = _bulk_lps_body()
    # Strip comments + docstrings so narrative references to
    # forbidden patterns (e.g. v1.15.90 prose) don't false-positive.
    code = re.sub(r'""".*?"""', "", body, flags=re.DOTALL)
    code = re.sub(r"#.*", "", code)
    # The natural-key pattern: theme_id OR guid_tmdb branches.
    assert "theme_id = ?" in code
    assert "guid_tmdb = ?" in code
    # The host-vs-container folder_path lookup must NOT appear in
    # the BULK LPS plex_items update — placements has its own
    # (mt, tmdb) key, and plex_items lookups must use the natural
    # key.
    upd_idx = code.index("UPDATE plex_items")
    after_upd = code[upd_idx:upd_idx + 800]
    assert "folder_path" not in after_upd, (
        "BULK LPS plex_items UPDATE must not key on folder_path "
        "— that's the v1.15.90 phantom-M bug class."
    )


def test_bulk_lps_plex_items_update_respects_section_id():
    """When BULK LPS targets a specific section (per-section
    unplace), the plex_items UPDATE must filter to that section.
    Otherwise the cross-section blast radius would reset the
    local_theme_file flag on rows BULK LPS didn't actually
    unplace."""
    body = _bulk_lps_body()
    code = re.sub(r'""".*?"""', "", body, flags=re.DOTALL)
    code = re.sub(r"#.*", "", code)
    upd_idx = code.index("UPDATE plex_items")
    after_upd = code[upd_idx:upd_idx + 800]
    # The per-section guard must be present in the UPDATE branch.
    assert "section_id" in after_upd


def test_bulk_lps_plex_items_update_inside_transaction():
    """The DELETE FROM placements + the new UPDATE plex_items
    must run inside the same `transaction(conn)` block — partial
    writes would resurrect the v1.15.104 outcome-observability
    class (concurrent reader sees one but not the other).

    `_bulk_lps_run` has multiple `transaction(conn)` blocks
    (the probe stage has one; the unplace stage has another).
    Anchor on `DELETE FROM placements` and walk back to the
    enclosing `with transaction(conn):` opener — the UPDATE
    plex_items must sit between those two markers."""
    body = _bulk_lps_body()
    del_idx = body.index("DELETE FROM placements")
    # Walk back to the nearest `with transaction(conn):` opener.
    txn_idx = body.rfind("with transaction(conn):", 0, del_idx)
    assert txn_idx != -1, "DELETE FROM placements not inside a transaction"
    # v1.16.12 widened the gap between DELETE and UPDATE plex_items
    # (the unplace stage now also stamps last_place_attempt_reason
    # in the same transaction). Widen the search window from 3000
    # to 4500 chars so the UPDATE plex_items still falls inside.
    window = body[txn_idx:del_idx + 4500]
    assert "DELETE FROM placements" in window
    assert "UPDATE plex_items" in window
