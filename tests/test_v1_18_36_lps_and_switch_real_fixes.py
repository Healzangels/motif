"""v1.18.36 — The big production landing for plex_upload lifecycle.

After 5 tags of probing (v1.18.31-35), we have the empirical
answers to wire the real fixes:

  - DELETE /library/metadata/{rk}/theme (SINGULAR) clears the
    active theme association; entries in /themes survive.
  - POST /library/metadata/{rk}/themes (PLURAL) with audio
    bytes uploads + auto-selects + content-dedupes by SHA-1.
  - No native "select existing entry" API — POST/PUT singular
    `?url=...` both fail (404 / 500). Re-upload trick is the
    only viable path to make a specific existing entry active.

## What v1.18.36 ships

### 1. URL fix (latent bug since v1.18.0)

`delete_collection_theme` + `delete_theme` (alias) URL flipped
plural → singular. Pre-fix every collection UNPLACE/PURGE
returned 404 silently — Plex's theme kept playing even after
motif's UI marked the placement removed.

### 2. New PlexClient.set_active_theme_via_reupload

The re-upload trick promoted to a production-grade method:
GET existing entry's bytes → POST those bytes back. Plex
content-dedupes → existing entry becomes selected. Returns
bool. Diagnostic probe path (try_reupload_existing_theme)
retained as a thin alias for the v1.18.35 probe endpoint.

### 3. api_unplace_item plex_upload branch (LPS fix)

Pre-fix the unplace handler only walked `media_folder` and
unlinked sidecars. For plex_upload rows (media_folder='')
the unlink silently no-op'd and the Plex API upload remained
active. Same shape as the v1.18.0 delete-URL bug — motif's
UI updated correctly while Plex kept serving the theme.

v1.18.36 adds a branch:
  - Identify motif's own entry by SHA-1 of canonical file
  - GET /themes; pick fallback (prefer metadata://, fall back
    to upload:// where hash != motif's)
  - DELETE singular (clear motif's selection)
  - If a fallback exists, re-upload trick on it (re-select
    Plex's pre-motif theme so the row keeps serving)

### 4. api_switch_placement bug fix

Pre-fix SWITCH dropped the placement row and queued a new
place job — never tore down the OUTGOING placement's
artifact:
  - file→api: sidecar theme.mp3 stayed at the Plex media
    folder (the user's M*A*S*H bug — UI showed cyan PL + PU
    LINK but sidecar still on disk).
  - api→file: motif's prior Plex upload stayed in /themes
    selected, and the new sidecar may have lost the
    resolution race.

v1.18.36 explicitly tears down the outgoing artifact before
queueing the new place job. file→api unlinks the sidecar
file; api→file calls plex.delete_theme (now singular,
working) to clear motif's prior API selection.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


REPO = Path(__file__).resolve().parent.parent
PLEX_PY = REPO / "app" / "core" / "plex.py"
API_PY = REPO / "app" / "web" / "api.py"


# ── URL change in delete_collection_theme ────────────────────


def test_delete_collection_theme_uses_singular_theme_url():
    """The URL string in delete_collection_theme must be
    SINGULAR /theme — v1.18.36 latent bug fix. Pin against the
    actual source line so a future refactor doesn't silently
    revert."""
    src = PLEX_PY.read_text()
    fn_idx = src.index("def delete_collection_theme(")
    body = src[fn_idx:fn_idx + 2000]
    assert (
        'url = f"/library/metadata/{rating_key}/theme"' in body
    ), (
        "v1.18.36: delete_collection_theme must hit SINGULAR "
        "/theme (the python-plexapi-attested, OpenAPI-documented "
        "endpoint). Pre-fix this hit PLURAL /themes which Plex "
        "returns 404 on — latent bug since v1.18.0."
    )
    # The plural variant must NOT appear in this function.
    assert (
        'url = f"/library/metadata/{rating_key}/themes"' not in body
    ), (
        "v1.18.36: the plural URL must not be in "
        "delete_collection_theme's body (the upload variant in "
        "upload_collection_theme does use plural — that's correct)."
    )


def test_upload_collection_theme_still_uses_plural_themes_url():
    """Regression guard: v1.18.36 only flipped the DELETE URL.
    Upload still uses PLURAL /themes because that's the
    documented + working POST endpoint."""
    src = PLEX_PY.read_text()
    fn_idx = src.index("def upload_collection_theme(")
    body = src[fn_idx:fn_idx + 4000]
    assert (
        'url = f"/library/metadata/{rating_key}/themes"' in body
    )


# ── set_active_theme_via_reupload promoted ───────────────────


def test_set_active_theme_via_reupload_method_exists():
    """The production method must exist as a public PlexClient
    method (not just the v1.18.35 probe form)."""
    from app.core.plex import PlexClient
    assert hasattr(PlexClient, "set_active_theme_via_reupload")
    assert callable(PlexClient.set_active_theme_via_reupload)


def test_set_active_theme_via_reupload_returns_bool_on_success():
    """Production contract: returns bool. False on any failure,
    True on full success (fetch + upload)."""
    from app.core.plex import PlexClient, PlexConfig
    cfg = PlexConfig(
        url="http://plex.test:32400", token="t",
        movie_section="1", tv_section="2", enabled=True,
    )
    client = PlexClient(cfg)
    fake = MagicMock()
    fake_resp = MagicMock()
    fake_resp.status_code = 200
    fake_resp.content = b"audio bytes"
    fake.get = MagicMock(return_value=fake_resp)
    client._client = fake
    # v1.18.68: upload_collection_theme returns 3-tuple
    # (ok, status, body_snip) — mock must match new shape.
    client.upload_collection_theme = MagicMock(
        return_value=(True, 200, "(empty)")
    )
    out = client.set_active_theme_via_reupload(
        rating_key="124233",
        theme_rating_key="upload://themes/abc",
    )
    assert out is True
    client.upload_collection_theme.assert_called_once_with(
        rating_key="124233", audio_bytes=b"audio bytes",
    )


def test_set_active_theme_via_reupload_false_on_fetch_failure():
    """Fetch returns non-2xx → False, no upload attempt."""
    from app.core.plex import PlexClient, PlexConfig
    cfg = PlexConfig(
        url="http://plex.test:32400", token="t",
        movie_section="1", tv_section="2", enabled=True,
    )
    client = PlexClient(cfg)
    fake = MagicMock()
    fake_resp = MagicMock()
    fake_resp.status_code = 404
    fake_resp.text = "not found"
    fake.get = MagicMock(return_value=fake_resp)
    client._client = fake
    client.upload_collection_theme = MagicMock()
    out = client.set_active_theme_via_reupload(
        rating_key="124233",
        theme_rating_key="upload://themes/missing",
    )
    assert out is False
    client.upload_collection_theme.assert_not_called()


def test_try_reupload_existing_theme_still_available_as_probe():
    """The diagnostic probe entry point (used by the v1.18.35
    /api/admin/probe-reupload-theme endpoint) must continue to
    return the rich-shape dict — not the simpler bool of the
    production method. Single shared helper underneath; no
    drift risk."""
    from app.core.plex import PlexClient
    assert hasattr(PlexClient, "try_reupload_existing_theme")
    assert hasattr(PlexClient, "_fetch_and_reupload_theme")


# ── api_unplace_item plex_upload branch ──────────────────────


def test_api_unplace_item_splits_placements_by_kind():
    """The unplace handler must split placements into sidecar
    vs plex_upload buckets so each gets the appropriate
    teardown path."""
    src = API_PY.read_text()
    fn_idx = src.index("async def api_unplace_item(")
    # Wide enough to span the new v1.18.36 branch + the
    # existing inline-verify block.
    body = src[fn_idx:fn_idx + 28000]
    assert "sidecar_placements" in body
    assert "api_placements" in body
    # The placement_kind discriminator must be on a stable line.
    assert (
        'pr["placement_kind"] or ""' in body
    )


def test_api_unplace_item_plex_upload_uses_delete_theme_and_reupload():
    """The plex_upload branch must call both plex.delete_theme
    (clear motif's selection) and plex.set_active_theme_via_reupload
    (re-select fallback)."""
    src = API_PY.read_text()
    fn_idx = src.index("async def api_unplace_item(")
    body = src[fn_idx:fn_idx + 28000]
    # v1.22.42: both calls off-loaded to threads (run_in_threadpool) so the
    # event loop isn't frozen during the per-placement Plex round-trips.
    assert "plex.delete_theme, rating_key=rk" in body
    assert "plex.set_active_theme_via_reupload," in body


def test_api_unplace_item_fallback_prefers_metadata_then_non_motif_upload():
    """The fallback picker must prefer metadata:// entries
    over upload:// entries (Plex Pass agent theme beats
    themerr-plex leftover), and only pick upload:// entries
    whose hash ≠ motif's own canonical hash."""
    src = API_PY.read_text()
    fn_idx = src.index("async def api_unplace_item(")
    body = src[fn_idx:fn_idx + 28000]
    # The metadata:// prefix is checked first.
    assert 'startswith("metadata://")' in body
    # Then upload:// with a motif_hash check.
    assert 'startswith("upload://")' in body
    assert "motif_hash" in body
    assert "entry_hash != motif_hash" in body


def test_api_unplace_item_hashes_canonical_via_sha1():
    """Motif's own entry identification uses SHA-1 of the
    canonical file (same hash function Plex uses for content
    dedup). Pin the import + computation so a future refactor
    doesn't accidentally use md5 or sha256 (which wouldn't
    match Plex's entry hashes)."""
    src = API_PY.read_text()
    fn_idx = src.index("async def api_unplace_item(")
    body = src[fn_idx:fn_idx + 28000]
    assert "import hashlib" in body
    assert "hashlib.sha1()" in body


# ── api_switch_placement bug fix ─────────────────────────────


def test_switch_placement_file_to_api_schedules_sidecar_removal():
    """SWITCH TO API (target_kind='api') must arrange for the
    sidecar theme.mp3 to be removed when the place job's Plex
    upload succeeds. v1.18.36 originally removed the sidecar
    synchronously (preventing the M*A*S*H orphan-sidecar bug);
    v1.18.68 moves the removal into the place job so a Plex
    upload failure doesn't leave the row unthemed.

    Pin both: (a) the per-section sidecar-path collection AND
    (b) the place-job payload field. the user's repro on v1.18.66:
    a 27MB file failed Plex upload with HTTP 500, sidecar was
    already gone, row stuck unthemed."""
    src = API_PY.read_text()
    fn_idx = src.index("async def api_switch_placement(")
    fn_end = src.index("@app.post", fn_idx + 1)
    body = src[fn_idx:fn_end]
    # Collection — per-section path dict.
    assert "sidecar_paths_per_section" in body, (
        "v1.18.68: collect per-section sidecar paths for the "
        "place job's payload"
    )
    # Payload field name pinned so worker can read it.
    assert "remove_sidecar_paths" in body, (
        "v1.18.68: payload field must be named "
        "remove_sidecar_paths"
    )
    # And the synchronous unlink that v1.18.36 used must be GONE.
    assert "side.unlink()" not in body, (
        "v1.18.68: synchronous sidecar unlink in the endpoint "
        "must be moved to the place job (see test_v1_18_68)"
    )


def test_switch_placement_api_to_file_schedules_delete_theme():
    """SWITCH TO SIDECAR (target_kind='file') must arrange for
    `plex.delete_theme` to clear motif's prior Plex API upload
    selection AFTER the new sidecar place succeeds.

    v1.18.36 originally fired the delete_theme synchronously in
    the endpoint (preventing the M*A*S*H phantom-API-entry bug).
    v1.18.71 moves the delete_theme into the worker's place job
    so a hardlink failure leaves the Plex API selection intact —
    same atomic-teardown pattern as v1.18.68 in reverse.

    Pin both: (a) the per-section rk collection AND (b) the
    place-job payload field name."""
    src = API_PY.read_text()
    fn_idx = src.index("async def api_switch_placement(")
    fn_end = src.index("@app.post", fn_idx + 1)
    body = src[fn_idx:fn_end]
    # Per-section rk dict.
    assert "clear_rks_per_section" in body, (
        "v1.18.71: per-section rk collection required for the "
        "atomic api→file teardown flow"
    )
    # Payload field name pinned so the worker can read it.
    assert "clear_plex_api_rks" in body, (
        "v1.18.71: payload field must be named clear_plex_api_rks"
    )
    # And the synchronous in-endpoint delete_theme must be GONE.
    assert "plex.delete_theme(rating_key=rk)" not in body, (
        "v1.18.71: synchronous delete_theme call moved to the "
        "worker (see test_v1_18_71_*)"
    )
    assert "switch_placement[api→file]" in body


# ── audit log + return shape ─────────────────────────────────


def test_unplace_audit_log_includes_api_counts():
    """The audit + log + return for /unplace must include the
    new api_handled and api_restored counts. Operators reading
    the log line need to see whether the plex_upload teardown
    fired."""
    # v1.18.46: anchor-based slicing. Pre-fix this was a fixed
    # 24000 → 28000 window across v1.18.36-38. Now slices to
    # the next route decorator.
    from _slice_helpers import slice_to_next, PY_NEXT_ROUTE
    src = API_PY.read_text()
    body = slice_to_next(
        src, "async def api_unplace_item(", *PY_NEXT_ROUTE,
    )
    assert '"api_handled": api_handled' in body
    assert '"api_restored": api_restored' in body
    # Return dict carries them too.
    assert '"api_handled": api_handled,' in body
    assert '"api_restored": api_restored' in body
