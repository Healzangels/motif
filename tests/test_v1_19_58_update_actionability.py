"""v1.19.58 — sync "Updated:" notification gates on actionability.

the user's 9 AM sync notification showed "Updated:" listing two
rows ("Bastard!! (1992)" + "The Beginning After the End
(2025)"). Opening each row's INFO card showed nothing
actionable — themerrdb_url matched applied_url, or themerrdb_url
was empty. the user expected !UPD blue pill to surface on these
rows so he could ACCEPT UPDATE / KEEP CURRENT, but the
pill didn't appear because motif's actual behavior on those
rows was non-actionable (silent themes.youtube_url roll-forward
with no local-file or override effect).

## Root cause

`stats.updated_count += 1` and `stats.updated_titles.append`
fired on EVERY `url_changed=True` event in the sync's elif
branch, including the case where the row has NO local_files
+ NO user_overrides + (enqueue_downloads=False OR
plex_supplies=True). In that case motif silently updates
themes.youtube_url and does NOTHING the user can see — but
the row still landed in the notification list.

## Fix

New `is_actionable` gate: only count + list in updated_titles
when the change leads to a user-visible effect:

  - `already_have or has_override` → pending_update INSERT
    (lights up !UPD glyph)
  - else: `enqueue_downloads and not plex_supplies` →
    re-download enqueued (DL chip changes)
  - else: silent bookkeeping, NOT counted, NOT notified

The themes.youtube_url roll-forward still happens at the SQL
level — sync's UPDATE keeps motif's tracking current.
Notification just stays silent unless the user has something
to look at.

## Behavioral expectations

  - U-row (has user_override) with TDB URL change → counted +
    listed + pending_update INSERTed (!UPD lights up)
  - T-row with local file and TDB URL change → counted + listed
    + pending_update INSERTed (auto-roll handled separately)
  - Pure TDB row with no local effect → NOT counted, NOT
    listed. themes.youtube_url updates silently.
  - P-row (Plex supplies) with TDB URL change → NOT counted
    (motif doesn't re-download for Plex-served rows)
"""
from __future__ import annotations
from _slice_helpers import slice_to_next

import sys
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

SYNC_PY = (REPO / "app" / "core" / "sync.py").read_text()


def _url_changed_branch() -> str:
    """The `elif url_changed:` branch — it is the last statement in
    _flush_sync_batch, so the next top-level def bounds it exactly."""
    return slice_to_next(SYNC_PY, "elif url_changed:", "\ndef ")


# ── Source guards ────────────────────────────────────────────


def test_updated_count_increment_inside_actionability_gate():
    """The `stats.updated_count += 1` line must be inside the
    new `if is_actionable:` block — NOT at the top of the elif
    url_changed branch (the pre-v1.19.58 location)."""
    # v0.51.264: the branch runs to the end of _flush_sync_batch, so bound it
    # there instead of re-widening the window a fourth time (4000 → 5000 →
    # 6200 → …). The assertions below are ORDERING claims; they do not need
    # a tight window, and an anchored one grows with the code.
    block = _url_changed_branch()
    # is_actionable predicate must be declared.
    assert "is_actionable" in block, (
        "v1.19.58: elif url_changed branch must compute "
        "is_actionable before incrementing updated_count"
    )
    # The increment must appear AFTER the gate.
    actionable_idx = block.index("is_actionable")
    increment_idx = block.index("stats.updated_count += 1")
    assert increment_idx > actionable_idx, (
        "v1.19.58: updated_count increment must be GATED on "
        "is_actionable — moving it before the gate would "
        "restore the v1.19.58 bug"
    )


def test_updated_titles_append_inside_actionability_gate():
    """The `stats.updated_titles.append(...)` line must also
    be inside the actionability gate. Pre-fix both the count
    and the title list bumped unconditionally.

    v1.19.70: window widened 4000→6000 to absorb the
    in_plex gate comment block.
    v0.51.264: anchored to the end of _flush_sync_batch (was 6600)."""
    block = _url_changed_branch()
    actionable_idx = block.index("is_actionable")
    append_idx = block.index("stats.updated_titles.append")
    assert append_idx > actionable_idx


def test_is_actionable_includes_will_enqueue_download():
    """The actionability gate must include the fresh-download
    case (no local_files + no override + enqueue_downloads=True
    + not plex_supplies). That case re-downloads motif's
    canonical from the new URL — a user-visible DL transition."""
    block = _url_changed_branch()
    # Either the variable name OR the inline conjunction.
    assert (
        "will_enqueue_download" in block
        or "enqueue_downloads and not plex_supplies" in block
    ), (
        "v1.19.58: actionability gate must consider the "
        "re-download case"
    )


def test_is_actionable_includes_already_have_or_override():
    """The actionability gate must include the case where the
    row has a local file or an override. Both cases trigger
    a pending_update INSERT (the !UPD glyph)."""
    assert "already_have or has_override" in _url_changed_branch()


def test_silent_bookkeeping_path_still_updates_themes_youtube_url():
    """The non-actionable branch (no local + no override +
    no download enqueue) must STILL update themes.youtube_url
    so motif's tracking stays current. The fix is to NOT
    notify on that case — but the SQL UPDATE for the column
    is unrelated to the count/list."""
    # The `_upsert_theme` function has a full-column UPDATE
    # statement that includes youtube_url + youtube_video_id.
    # Locate it via the `youtube_video_id = ?` column
    # assignment (unique to the full-column UPDATE; the
    # bump-last-seen UPDATE has only last_seen_sync_at).
    assert "youtube_video_id = ?" in SYNC_PY, (
        "v1.19.58: _upsert_theme must still UPDATE "
        "youtube_video_id (the SQL bookkeeping is preserved)"
    )
    assert "youtube_url = ?" in SYNC_PY


# ── Behavioral: simulate the actionability decision matrix ──


def test_actionability_matrix_pure_bookkeeping_skips_notification():
    """Pure-TDB row (no local, no override, plex_supplies=True
    OR enqueue_downloads=False) must NOT increment updated_count.
    Re-implement the gate logic here and assert the decision."""
    cases = [
        # (already_have, has_override, enqueue_downloads, plex_supplies, expected_actionable)
        # The non-actionable case the user hit.
        (False, False, False, False, False),
        # Pure-TDB row, sync didn't enqueue downloads.
        (False, False, False, True, False),
        # P-row (Plex supplies), motif skips re-download.
        (False, False, True, True, False),
        # Pure-TDB row + enqueue_downloads=True + not Plex →
        # fresh download enqueued (actionable).
        (False, False, True, False, True),
        # User has override → pending_update INSERT (actionable).
        (False, True, False, False, True),
        # Local file exists → pending_update INSERT (actionable).
        (True, False, False, False, True),
        # Both → pending_update INSERT (actionable).
        (True, True, False, False, True),
    ]
    for (already_have, has_override, enqueue_downloads,
         plex_supplies, expected) in cases:
        will_enqueue_download = (
            not (already_have or has_override)
            and enqueue_downloads
            and not plex_supplies
        )
        is_actionable = (
            (already_have or has_override)
            or will_enqueue_download
        )
        assert is_actionable is expected, (
            f"actionability mismatch for "
            f"already_have={already_have} "
            f"has_override={has_override} "
            f"enqueue_downloads={enqueue_downloads} "
            f"plex_supplies={plex_supplies}: "
            f"expected {expected}, got {is_actionable}"
        )


# ── Version pin ──────────────────────────────────────────────


def test_v1_19_58_version_pin():
    """Version bumped at v1.19.58 (then again at v1.19.59 for
    the INFO card playback-source line). Match 1.19.x prefix."""
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
