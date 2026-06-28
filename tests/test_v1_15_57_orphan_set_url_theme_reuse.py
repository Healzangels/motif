"""v1.15.57 — SET URL / UPLOAD MP3 on plex_orphan rows reuse the
existing synthetic theme via pi.theme_id.

the user (screenshot of INFO card on a plex_orphan SET URL row,
showing `previous url: —` despite multiple SET URL calls): "if
a user provides a url and a row is U and then a different user
provided url is added to replace the existing the original user
provided url should be become the previous url currently it
goes away entirely. This should allow you to restore back to
the original user url, if you do then the now current would
become the previous."

## Bug

The SET URL endpoint at api.py:8849 + the UPLOAD MP3 endpoint
at api.py:~8670 both look up the matching themes row using
ONLY pi.guid_tmdb. For plex_orphan rows (no IMDB/TMDB binding
in Plex's metadata), pi.guid_tmdb is NULL — the lookup misses,
and the fallback branch creates a NEW synthetic tmdb_id every
time. Result: each SET URL/UPLOAD on an orphan row gets a fresh
themes row, orphaning the previous SET's user_overrides AND
previous_urls entries. The REVERT button can't surface because
previous_urls is empty for the brand-new synthetic id.

## Fix

Both endpoints now prefer pi.theme_id (set by the FIRST SET
URL / UPLOAD MP3 via line ~8934-8937 — `UPDATE plex_items SET
theme_id = ?`) before falling through to the guid_tmdb lookup
and synthetic creation. The chain naturally repairs:

* SET URL #1 on orphan: pi.theme_id NULL → synthetic created,
  theme_id stamped. No previous URL to capture.
* SET URL #2: pi.theme_id non-NULL → reuse existing synthetic.
  _capture_previous_url sees the existing user_override (URL=A)
  → writes to previous_urls. New user_override (URL=B) replaces.
  previous_url now shows A, REVERT button reveals.
* REVERT: swap current ↔ previous (existing v1.12.86 infra).

Static-text guards consistent with v1.12.105 SET URL fix-test
patterns.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"


def _set_url_theme_lookup_block() -> str:
    """Return the SET URL endpoint's theme-lookup block (the ~30
    lines around the theme=None / theme is None branches at
    api.py:~8897). Anchor on the v1.15.57 marker comment."""
    src = API_PY.read_text()
    anchor = src.index(
        "v1.15.57: prefer the cached pi.theme_id lookup so"
    )
    # Walk back to the theme_media_type assignment that starts
    # the block (about 5 lines above the marker).
    block_start = src.rindex("theme_media_type = ", 0, anchor)
    # Walk forward to the `else:` branch + theme_id_pk = theme["id"]
    # assignment that closes the block.
    block_end = src.index('theme_id_pk = theme["id"]', anchor) + len(
        'theme_id_pk = theme["id"]')
    return src[block_start:block_end]


def _upload_mp3_theme_lookup_block() -> str:
    """Return the UPLOAD MP3 endpoint's theme-lookup block — the
    mirror of the SET URL block, with its own v1.15.57 marker."""
    src = API_PY.read_text()
    anchor = src.index(
        "v1.15.57: prefer pi.theme_id over guid_tmdb so plex_orphan"
    )
    block_start = src.rindex("theme_media_type = ", 0, anchor)
    block_end = src.index('theme_id_pk = theme["id"]', anchor) + len(
        'theme_id_pk = theme["id"]')
    return src[block_start:block_end]


def test_set_url_checks_pi_theme_id_before_guid_tmdb():
    """The SET URL endpoint must look up the existing synthetic
    theme via pi.theme_id BEFORE falling through to pi.guid_tmdb.
    Pre-fix only guid_tmdb was checked (NULL for orphans), so
    every SET URL created a fresh synthetic — orphaning the
    user_overrides + previous_urls chain."""
    block = _set_url_theme_lookup_block()
    # The pi.theme_id check must precede the guid_tmdb check.
    theme_id_idx = block.index('if pi["theme_id"]:')
    guid_tmdb_idx = block.index('if theme is None and pi["guid_tmdb"]:')
    assert theme_id_idx < guid_tmdb_idx, (
        "v1.15.57: pi.theme_id lookup must come BEFORE the "
        "pi.guid_tmdb fallback — pre-fix orphans skipped the "
        "lookup entirely (guid_tmdb=NULL) and got a fresh "
        "synthetic on every SET URL"
    )
    # The lookup must query by the themes PK (id), not (media_type, tmdb_id).
    assert '"SELECT * FROM themes WHERE id = ?"' in block, (
        "v1.15.57: theme_id lookup must SELECT by the themes PK "
        "(id), not by (media_type, tmdb_id) — theme_id IS the PK"
    )


def test_upload_mp3_checks_pi_theme_id_before_guid_tmdb():
    """The UPLOAD MP3 endpoint has the same orphan-lookup shape
    and gets the same v1.15.57 fix. Without this UPLOAD MP3
    sequence on an orphan would create a fresh synthetic on
    every upload (same bug class as SET URL)."""
    block = _upload_mp3_theme_lookup_block()
    theme_id_idx = block.index('if pi["theme_id"]:')
    guid_tmdb_idx = block.index('if theme is None and pi["guid_tmdb"]:')
    assert theme_id_idx < guid_tmdb_idx, (
        "v1.15.57: UPLOAD MP3 must also check pi.theme_id first"
    )
    assert '"SELECT * FROM themes WHERE id = ?"' in block


def test_both_endpoints_share_the_lookup_pattern():
    """Cross-source consistency: both SET URL + UPLOAD MP3 use
    the identical lookup pattern (`pi.theme_id` → `guid_tmdb`
    → synthetic). Drift between the two = same bug returns on
    only one path."""
    set_block = _set_url_theme_lookup_block()
    upload_block = _upload_mp3_theme_lookup_block()
    # Extract just the lookup chain (the three if/branch lines)
    # from each and compare structurally — they should both
    # have the same SELECT for theme_id, the same fallback to
    # guid_tmdb, the same synthetic-creation branch.
    for marker in (
        'if pi["theme_id"]:',
        '"SELECT * FROM themes WHERE id = ?"',
        'if theme is None and pi["guid_tmdb"]:',
        'if theme is None:',  # synthetic creation branch
    ):
        assert marker in set_block, (
            f"v1.15.57: SET URL block missing {marker!r}"
        )
        assert marker in upload_block, (
            f"v1.15.57: UPLOAD MP3 block missing {marker!r}"
        )


def test_synthetic_creation_only_when_both_lookups_fail():
    """The synthetic-creation branch (`tmdb_id = min(min_tmdb, 0)
    - 1`) must fire only when BOTH the theme_id lookup AND the
    guid_tmdb lookup miss. Pre-fix the synthetic branch fired
    whenever guid_tmdb missed — orphaning everything on every
    call. Post-fix it only fires on the very FIRST SET URL/UPLOAD
    when there's no theme_id cached yet."""
    set_block = _set_url_theme_lookup_block()
    upload_block = _upload_mp3_theme_lookup_block()
    for name, block in (("SET URL", set_block), ("UPLOAD MP3", upload_block)):
        # The synthetic branch is the `if theme is None:` AFTER
        # both lookups. It must be the LAST branch in the chain.
        theme_id_check_pos = block.index('if pi["theme_id"]:')
        guid_check_pos = block.index('if theme is None and pi["guid_tmdb"]:')
        synthetic_branch_pos = block.index(
            'if theme is None:', guid_check_pos + 1,
        )
        assert theme_id_check_pos < guid_check_pos < synthetic_branch_pos, (
            f"v1.15.57: {name} branch ordering wrong — synthetic "
            "creation must come AFTER both theme_id + guid_tmdb "
            "lookups so a row with either binding is reused"
        )
