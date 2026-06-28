"""v1.18.69 — Plex upload-too-large → sidecar fallback for movie/TV.

the user's ask after v1.18.68:

  > Also is a user provides a url or uploads a file that ends up being
  > too large, for example user provides a link to a video once
  > downloaded is 25mb, when motif attempts to place if use api is
  > the default choice it would fail due to the size issue, let's
  > have it fallback to placing or hardlinking the theme and becoming
  > a PS link instead of the PU state with a warning maybe in logs?
  > or in the info card at that point saying the current theme is too
  > large to be uploaded via api fallback to local sidecar required

## Scope

In `_do_place_collection` (which v1.18.23 promoted to the universal
plex_upload path for movie/TV too), when the Plex API upload fails
AND the failure looks size-related (HTTP 500 + size >= 20MB), motif
now:

  1. Looks up plex_items.folder_path for the row's rating_key.
  2. Hardlinks (or copies cross-FS) the canonical local file to
     folder/theme.mp3 via the same `_safe_link_or_copy` helper
     `_do_place` uses for the normal sidecar path.
  3. Writes the placements row with placement_kind='hardlink'/'copy'
     and media_folder=folder_path (instead of plex_upload / '').
  4. SKIPS the v1.18.59 leftover-sidecar sweep (the "leftover" would
     be the sidecar we just wrote — removing it undoes the fallback).
  5. SKIPS the v1.18.68 payload-driven sidecar removal (the
     payload-listed sidecar IS the folder we wrote to).
  6. Emits a WARNING event with structured detail so MOTIF INFO →
     HISTORY shows why the kind is hardlink/copy on a row that was
     supposed to land as plex_upload.

Collections still raise the v1.18.68 enriched error — they have no
media folder (media_folder='' is the plex_upload sentinel) so no
sidecar fallback is possible.

Non-size failures (HTTP 403/404/network errors / 500 on small files)
still raise the v1.18.68 enriched error — the fallback is gated on
the empirical "Plex's theme upload ceiling is ~25MB" heuristic.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


def _place_collection_body() -> str:
    src = (REPO / "app" / "core" / "worker.py").read_text()
    fn_idx = src.index("def _do_place_collection(")
    nxt = src.find("\n    def ", fn_idx + 1)
    if nxt == -1:
        nxt = len(src)
    return src[fn_idx:nxt]


def _flatten_concatenated(body: str) -> str:
    """Collapse Python adjacent-string-literal concatenation so
    substring asserts work on the runtime value (same helper
    pattern as test_v1_18_59_settings_layout_and_sidecar_cleanup)."""
    import re
    return re.sub(r'"\s*"', '', body)


def _flatten_call_args(body: str) -> str:
    """Collapse intra-call-arg line wrapping so asserts on call
    sites like `foo(a, b)` match when the source has
    `foo(\n    a, b)`."""
    import re
    return re.sub(r"\s+", " ", body)


# ── Detection: size + status threshold ──────────────────────


def test_fallback_gated_on_movie_tv_and_size_500():
    """The fallback fires when:
      - media_type != 'collection' (collections have no sidecar)
      - http_status in (500, None) — v1.19.99 widened from 500-only
        to also cover no-response (timeout / connection reset)
      - size_mb >= the ceiling constant (v1.19.94, lowered 20→10)
    Pin the gates so a refactor can't silently re-narrow or
    over-widen the fallback scope."""
    body = _place_collection_body()
    assert 'media_type != "collection"' in body, (
        "v1.18.69: collections must be excluded from fallback"
    )
    # v1.19.99: the fallback gate accepts 500 AND None (timeout).
    assert "http_status in (500, None)" in body, (
        "v1.19.99: fallback gate covers HTTP 500 + no-response timeouts"
    )
    # v1.19.94: threshold moved to a named constant + lowered 20→10
    # to match the real Plex ceiling (9.4MB OK, 10.5MB + 11.7MB fail).
    assert "size_mb >= _PLEX_THEME_UPLOAD_CEILING_MB" in body, (
        "v1.19.94: fallback gates on the named ceiling constant"
    )


def test_fallback_uses_safe_link_or_copy_helper():
    """The fallback must use _safe_link_or_copy (the same helper
    _do_place uses for the regular sidecar path) — gets hardlink-
    or-copy semantics + cross-FS fallback + atomic temp+rename
    for free. Pin so a future refactor doesn't inline a less-safe
    implementation."""
    body = _place_collection_body()
    assert "from .placement import _safe_link_or_copy" in body
    # The call arguments may wrap across lines.
    body_flat = _flatten_call_args(body)
    assert "_safe_link_or_copy( source_file, dst)" in body_flat


def test_fallback_reads_folder_path_from_plex_items():
    """The fallback target is plex_items.folder_path for the row's
    rating_key — mirrors the v1.18.59 cleanup lookup pattern."""
    body = _place_collection_body()
    # The query shape mirrors v1.18.59. Just pin that the
    # fallback block performs the lookup.
    fallback_idx = body.index("size-rejection sidecar fallback")
    # Walk to the next un-related anchor (sidecar removal block).
    end = body.index("upload confirmed", fallback_idx)
    block = body[fallback_idx:end]
    assert "SELECT folder_path FROM plex_items" in block
    assert "WHERE rating_key = ? LIMIT 1" in block


def test_fallback_resolves_container_path_via_candidate_local_paths():
    """v1.22.15: plex_items.folder_path is Plex's HOST view (/mnt/user/... on
    the user's Unraid), which does NOT exist inside the container. The fallback
    must resolve it to a container-visible dir via _candidate_local_paths before
    writing. Pre-fix `Path(folder_path)/"theme.mp3"` + `mkdir(parents=True)`
    CREATED a phantom container-local tree Plex never serves, then logged
    success — the over-ceiling theme silently went nowhere (audit #1)."""
    body = _place_collection_body()
    assert "_candidate_local_paths(" in body, (
        "v1.22.15: fallback must resolve the host folder_path to a container "
        "path via _candidate_local_paths"
    )
    fallback_idx = body.index("size-rejection sidecar fallback")
    end = body.index("upload confirmed", fallback_idx)
    block = body[fallback_idx:end]
    # The raw mkdir on the (possibly host-view) folder_path was the bug — it
    # must be GONE (the real /data media folder already exists; we pick the
    # is_dir() candidate, never fabricate a tree).
    assert "mkdir(parents=True" not in block, (
        "v1.22.15: must NOT mkdir the untranslated folder_path — that created "
        "a phantom container-local dir on Unraid"
    )
    assert "if c.is_dir()" in block, (
        "v1.22.15: fallback picks the first container candidate that is_dir()"
    )
    assert 'dst = _fb_dir / "theme.mp3"' in body


# ── State writes: placements row reflects fallback ──────────


def test_placements_insert_uses_fallback_kind_and_folder():
    """When fell_back_kind is set, the placements INSERT must
    use it (instead of 'plex_upload') AND use the actual folder
    path (instead of ''). This is what makes the row's LINK pill
    render as HL/C rather than PU."""
    body = _place_collection_body()
    # The placeholders threaded through the INSERT.
    assert '_placed_kind = fell_back_kind or "plex_upload"' in body, (
        "v1.18.69: placement_kind binds to the fallback kind "
        "when set"
    )
    assert '_placed_folder = fell_back_folder or ""' in body, (
        "v1.18.69: media_folder binds to the fallback folder "
        "when set (empty string is the plex_upload sentinel)"
    )


def test_placements_insert_uses_dynamic_kind_in_query():
    """The INSERT query must use a placeholder for placement_kind
    (NOT a hardcoded 'plex_upload') so the fallback's kind value
    actually lands in the row."""
    body = _place_collection_body()
    # Find the INSERT block.
    insert_idx = body.index("INSERT INTO placements")
    # Walk to the next ON CONFLICT or end of statement.
    block = body[insert_idx:insert_idx + 1500]
    # The pre-fix shape "VALUES (?, ?, ?, '', ?, 'plex_upload', ...)"
    # must be GONE. Post-fix uses parameter placeholders for both.
    assert "'plex_upload'" not in block, (
        "v1.18.69: VALUES clause must use a ? placeholder for "
        "placement_kind (filled by _placed_kind) — the hardcoded "
        "'plex_upload' literal blocked the fallback from "
        "actually changing the kind"
    )
    # The placeholder count: media_folder (?) + placement_kind (?)
    # in the VALUES list. v1.20.15: plex_refreshed also became a bound
    # param (was a hardcoded 1) so the sidecar fallback can record an
    # honest refresh result. v1.21.55: edition_key added → 10 params.
    assert "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)" in block


# ── Cleanup skipped on fallback ─────────────────────────────


def test_payload_sidecar_removal_skipped_on_fallback():
    """The v1.18.68 payload-driven sidecar removal must skip when
    we fell back. Otherwise the SWITCH PLACEMENT [file→api] flow
    would write a sidecar (fallback) then immediately remove it
    (because the payload listed the same path as the OLD sidecar)."""
    body = _place_collection_body()
    # The payload-removal block must be gated on `fell_back_kind
    # is None`.
    removal_idx = body.index("remove_sidecar_paths")
    pre_removal = body[max(0, removal_idx - 600):removal_idx]
    assert "fell_back_kind is None" in pre_removal, (
        "v1.18.69: payload sidecar removal must be gated on the "
        "non-fallback path — otherwise SWITCH PLACEMENT [file→api] "
        "fallback would self-destruct"
    )


def test_v1_18_59_cleanup_skipped_on_fallback():
    """The v1.18.59 leftover-sidecar sweep MUST skip when we
    fell back — the "leftover" would be the sidecar we JUST
    wrote. Removing it puts the row back into the unthemed
    state the fallback was designed to escape."""
    body = _place_collection_body()
    sweep_idx = body.index("find_theme_sidecar_path")
    pre_sweep = body[max(0, sweep_idx - 400):sweep_idx]
    assert "fell_back_kind is None" in pre_sweep, (
        "v1.18.69: leftover-sidecar sweep must be gated on the "
        "non-fallback path"
    )


# ── Event logging: WARNING with structured detail ───────────


def test_fallback_path_emits_warning_event():
    """Successful fallback emits a WARNING log_event so the row's
    HISTORY tab in MOTIF INFO shows the operator WHY the kind
    isn't plex_upload. the user's ask: 'in the info card at that
    point saying the current theme is too large to be uploaded
    via api fallback to local sidecar required'."""
    body = _place_collection_body()
    # The WARNING branch must exist + reference the fallback kind.
    assert 'level="WARNING"' in body
    assert 'fell_back_kind is not None' in body
    # Operator-facing message includes size + the practical-ceiling
    # context so HISTORY surfaces the actionable info.
    body_flat = " ".join(body.split())
    # v1.19.99: cause-aware message — the 500 branch still names the
    # ~10MB ceiling, and the line explicitly states the row is local,
    # NOT API-uploaded (the user's "make it clear why it's local").
    assert "Plex API theme upload" in body_flat
    assert "10MB theme-upload ceiling" in body_flat
    assert "NOT uploaded to Plex's metadata store" in body_flat
    assert "LINK={fell_back_kind}" in body_flat


def test_event_detail_carries_structured_reason():
    """The event's detail JSON must include the fallback reason +
    fallback_from='plex_upload' + size_mb so future analytics /
    reprobe queries can target the fallback population. v1.19.99 made
    `reason` cause-aware (too-large vs no-response)."""
    body = _place_collection_body()
    # Detail dict fields — reason is now a conditional with both tokens.
    assert '"api_upload_too_large"' in body
    assert '"api_upload_no_response"' in body
    assert '"reason": (' in body
    assert '"fallback_from": "plex_upload"' in body
    assert '"size_mb": round(size_mb, 1)' in body


def test_happy_path_event_still_info_level():
    """When the upload succeeds (no fallback), the event must
    still be INFO with reason=None — preserves the existing
    plex_upload success log shape."""
    body = _place_collection_body()
    # The else branch (no fallback) keeps the original INFO event.
    assert 'level="INFO"' in body
    assert '"reason": None' in body
    assert '"kind": "plex_upload"' in body


# ── Negative cases: fallback NOT triggered ──────────────────


def test_collection_upload_failure_still_raises():
    """Collections have no media folder → no sidecar fallback
    possible. They still raise the v1.18.68 enriched error."""
    body = _place_collection_body()
    # The collection-exclusion gate must be on the fallback ENTRY
    # condition.
    fallback_idx = body.index("size-rejection sidecar fallback")
    end = body.index("upload confirmed", fallback_idx)
    block = body[fallback_idx:end]
    assert 'media_type != "collection"' in block, (
        "v1.18.69: fallback must be gated on movie/TV"
    )
    # And the raise still fires when fell_back_kind stays None.
    assert "if fell_back_kind is None:" in body
    assert "raise RuntimeError(note)" in body


def test_non_size_failure_still_raises():
    """A failure that ISN'T size-shaped (HTTP 403 / 404 / network
    error / 500 on a small file) must NOT fall back — it raises
    the v1.18.68 enriched error so the operator can diagnose the
    real cause."""
    body = _place_collection_body()
    # The gate must require BOTH http_status == 500 AND size_mb >= 20.
    fallback_idx = body.index("size-rejection sidecar fallback")
    end = body.index("upload confirmed", fallback_idx)
    block = body[fallback_idx:end]
    assert "http_status == 500" in block
    assert "size_mb >= _PLEX_THEME_UPLOAD_CEILING_MB" in block


def test_fallback_exception_logged_and_drops_to_raise():
    """If the fallback ITSELF fails (FS permissions, missing
    folder), motif must log the fallback failure AND raise the
    original upload error. The operator sees both failures in
    docker logs but the job still marks failed (no false-
    positive success)."""
    body = _place_collection_body()
    # Adjacent-string-literal log messages — flatten before assert.
    flat = _flatten_concatenated(body)
    assert "sidecar fallback failed" in flat
    assert "raising original upload error" in flat


# ── Markers + archaeology ───────────────────────────────────


def test_v1_18_69_marker_present():
    """v1.18.69 marker must sit inside the fallback block so a
    future refactor reading the area sees WHY it exists."""
    body = _place_collection_body()
    assert "v1.18.69" in body
    # the user's request preserved for archaeology.
    body_flat = " ".join(body.split())
    assert ("hardlinking the theme" in body_flat
            or "fall back to placing" in body_flat
            or "fell back to" in body_flat.lower())
