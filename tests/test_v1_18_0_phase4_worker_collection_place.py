"""v1.18.0 Phase 4 — Worker place adapter routes collections via HTTP.

Plex collections have no media folder on disk; a collection's
theme lives inside Plex's metadata bundle. So `_do_place`'s
hardlink-into-folder path (place_theme) is a category error for
collections. v1.18.0 adds an early `media_type == "collection"`
branch in `_do_place` that delegates to a new helper
`_do_place_collection`, which:

  1. resolves the plex_items row for the collection's
     (tmdb_id, section_id)
  2. honors dry-run + skip_if_plex_has_theme + force_overwrite
  3. POSTs the local mp3 bytes via
     PlexClient.upload_collection_theme(rk, audio_bytes)
  4. upserts placements with placement_kind='plex_upload' and
     media_folder='' (schema v55 widens both CHECKs)
  5. stamps local_files.last_place_attempt_at
  6. fires theme_added notification

This file pins the source structure (presence of the branch +
helper) and the helper's important behaviors via source-grep,
since the helper is deeply intertwined with the Worker class's
runtime state (settings, db_path, plex client, etc.) and a
behavioral mock-test would essentially re-implement the helper.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

WORKER_PY = REPO / "app" / "core" / "worker.py"


def _read_worker() -> str:
    return WORKER_PY.read_text()


# ── Branch wired into _do_place ───────────────────────────────


def test_do_place_branches_to_collection_helper():
    """`_do_place` must dispatch media_type='collection' to
    `_do_place_collection` BEFORE attempting any FolderIndex
    work. The branch must return immediately after the helper
    fires — falling through would run place_theme on a non-
    existent folder."""
    src = _read_worker()
    fn_start = src.index("def _do_place(self, job:")
    # End at the next sibling method definition.
    fn_end = src.index("\n    def _do_place_collection(", fn_start + 1)
    body = src[fn_start:fn_end]
    # The branch must use the helper.
    assert "media_type == \"collection\"" in body, (
        "v1.18.0: _do_place must branch on media_type='collection'"
    )
    assert "self._do_place_collection(" in body, (
        "v1.18.0: _do_place must delegate collection placement "
        "to _do_place_collection — falling through would run "
        "place_theme on a non-existent folder."
    )
    # And the branch must return.
    branch_idx = body.index("media_type == \"collection\"")
    # Look at the next ~400 chars after the branch.
    near = body[branch_idx:branch_idx + 600]
    assert "return" in near, (
        "v1.18.0: collection branch must return — falling through "
        "to place_theme would error out on the missing folder."
    )


# ── Helper exists with the expected shape ─────────────────────


def test_do_place_collection_method_present():
    """The new helper must exist on the Worker class with the
    keyword-only (job, theme, local) signature."""
    src = _read_worker()
    assert "def _do_place_collection(" in src, (
        "v1.18.0: _do_place_collection helper missing"
    )
    # Signature pin.
    assert re.search(
        r"def _do_place_collection\(\s*self,\s*\*,\s*job:.+theme:.+local:",
        src, re.DOTALL,
    ), "Signature must be keyword-only (job, theme, local)"


def test_do_place_collection_uses_upload_endpoint():
    """The helper must call PlexClient.upload_collection_theme(
    rating_key=..., audio_bytes=...) — pin the symbol so a
    rename / removal can't slip through."""
    src = _read_worker()
    fn_start = src.index("def _do_place_collection(")
    fn_end = src.index("\n    def _do_refresh(", fn_start + 1)
    body = src[fn_start:fn_end]
    assert "upload_collection_theme(" in body, (
        "v1.18.0: helper must POST via "
        "PlexClient.upload_collection_theme"
    )
    # Pin the keyword args (audited shape).
    assert "rating_key=" in body
    assert "audio_bytes=" in body


def test_do_place_collection_upserts_plex_upload_placement_kind():
    """Successful upload must write a placements row with
    placement_kind='plex_upload' and media_folder='' for the
    plex_upload path — these are the schema v55 widenings the
    Phase 1 migration introduced.

    v1.18.69: placement_kind + media_folder are now parameter-
    bound (not SQL literals) so the size-rejection sidecar
    fallback can write 'hardlink'/'copy' + folder_path on the
    fallback path. The plex_upload-path values are computed via
    `_placed_kind = fell_back_kind or 'plex_upload'` and
    `_placed_folder = fell_back_folder or ''` — pinning the
    fallback expressions confirms the no-fallback case still
    produces the schema v55 values."""
    src = _read_worker()
    fn_start = src.index("def _do_place_collection(")
    fn_end = src.index("\n    def _do_refresh(", fn_start + 1)
    body = src[fn_start:fn_end]
    # The fallback expressions default to the schema v55 values
    # when fell_back_* are None (the happy plex_upload path).
    assert '_placed_kind = fell_back_kind or "plex_upload"' in body, (
        "v1.18.0 + v1.18.69: plex_upload is still the default "
        "placement_kind when no fallback fired"
    )
    assert '_placed_folder = fell_back_folder or ""' in body, (
        "v1.18.0 + v1.18.69: media_folder='' is still the default "
        "for plex_upload rows (collections have no folder)"
    )
    # And the INSERT binds those values via parameter placeholders.
    assert "INSERT INTO placements" in body
    assert "_placed_kind" in body
    assert "_placed_folder" in body


def test_do_place_collection_honors_dry_run():
    """Helper must honor dry-run by short-circuiting the network
    + DB write side effects."""
    src = _read_worker()
    fn_start = src.index("def _do_place_collection(")
    fn_end = src.index("\n    def _do_refresh(", fn_start + 1)
    body = src[fn_start:fn_end]
    assert "is_dry_run(" in body
    # Dry-run must log via DRY-RUN prefix consistent with the
    # parent _do_place's pattern.
    assert "DRY-RUN" in body


def test_do_place_collection_honors_force_overwrite_payload():
    """Helper must read force_overwrite from job payload (same
    semantic as the parent _do_place's `force_overwrite` flag)
    so /pending APPROVE → force=true bypasses the
    skip_if_plex_has_theme gate."""
    src = _read_worker()
    fn_start = src.index("def _do_place_collection(")
    fn_end = src.index("\n    def _do_refresh(", fn_start + 1)
    body = src[fn_start:fn_end]
    assert "force_overwrite" in body
    assert "payload.get(\"force\")" in body, (
        "v1.18.0: helper must read payload.get('force') so "
        "/pending APPROVE → force=true bypasses the "
        "plex_has_theme skip."
    )


def test_do_place_collection_skips_when_plex_has_theme():
    """Helper must respect the existing skip_if_plex_has_theme
    semantic — if Plex already has a theme attached and force is
    False, the upload is skipped without consuming bandwidth."""
    src = _read_worker()
    fn_start = src.index("def _do_place_collection(")
    fn_end = src.index("\n    def _do_refresh(", fn_start + 1)
    body = src[fn_start:fn_end]
    # We pin on the cached_has_theme symbol + the not-force
    # condition.
    assert "cached_has_theme" in body
    assert "plex_has_theme" in body, (
        "v1.18.0: skip path must log reason='plex_has_theme' "
        "(matches _do_place's vocabulary)."
    )


def test_do_place_collection_fires_theme_added_notification():
    """On successful upload, the helper must dispatch the
    theme_added Apprise event — mirrors the parent _do_place's
    success path so per-row notifications fire consistently
    across media types."""
    src = _read_worker()
    fn_start = src.index("def _do_place_collection(")
    fn_end = src.index("\n    def _do_refresh(", fn_start + 1)
    body = src[fn_start:fn_end]
    assert "event_kind=\"theme_added\"" in body, (
        "v1.18.0: helper must dispatch theme_added on success"
    )
    assert "format_theme_added_body" in body
    assert "format_theme_added_title" in body


def test_do_place_collection_uses_themes_plex_items_alignment():
    """The helper's plex_items lookup must align themes.media_type
    with plex_items.media_type. v1.18.0 hardcoded 'collection'
    here because the helper was collection-only; v1.18.23
    generalized the fallback for movie/TV API push (the helper is
    now invoked for any media type when payload kind='api'). The
    new mapping (`'show' if media_type == 'tv' else media_type`)
    still resolves collections as 'collection' — same semantics,
    parameterized."""
    src = _read_worker()
    fn_start = src.index("def _do_place_collection(")
    fn_end = src.index("\n    def _do_refresh(", fn_start + 1)
    body = src[fn_start:fn_end]
    # v1.18.23: instead of pinning the literal, verify the
    # generalized mapping is present.
    assert "pi_media_type_fallback" in body, (
        "v1.18.23: plex_items fallback lookup must use the "
        "generalized pi_media_type_fallback variable (tv → show, "
        "collection → collection, movie → movie)"
    )
    assert "\"show\" if media_type == \"tv\" else media_type" in body


def test_do_place_collection_stamps_local_files_attempt():
    """Helper must stamp local_files.last_place_attempt_at +
    last_place_attempt_reason='placed' (parallel to _do_place's
    bookkeeping)."""
    src = _read_worker()
    fn_start = src.index("def _do_place_collection(")
    fn_end = src.index("\n    def _do_refresh(", fn_start + 1)
    body = src[fn_start:fn_end]
    assert "last_place_attempt_at" in body
    assert "last_place_attempt_reason" in body


def test_do_place_collection_updates_plex_items_has_theme():
    """Post-upload, the helper must stamp plex_items.has_theme=1
    by rating_key so the row's library UI surface reflects the
    new theme without waiting for the next plex_enum sweep."""
    src = _read_worker()
    fn_start = src.index("def _do_place_collection(")
    fn_end = src.index("\n    def _do_refresh(", fn_start + 1)
    body = src[fn_start:fn_end]
    assert "UPDATE plex_items SET has_theme = 1" in body
