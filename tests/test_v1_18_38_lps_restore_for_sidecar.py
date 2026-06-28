"""v1.18.38 — LPS restore fires for sidecar placements too.

the user's 100% Wolf test on v1.18.36 revealed a bug in the
v1.18.36 plex_upload LPS branch. Reproduction sequence:

  1. REPLACE TDB (kind=api)  → row T+P / cyan PL / PU LINK
  2. SWITCH PLACEMENT (api→file) → row T / green PL / HL LINK
  3. LET PLEX SERVE (UNPLACE)

Expected: row returns to P / gray PL / PS LINK (Plex's
original theme re-selected).

Actual: row went to `-` / gray PL / PS LINK (no theme served).
Audit shows `api_handled: 0, api_restored: 0` — the v1.18.36
plex_upload branch didn't fire because by LPS time the
placement_kind was 'hardlink' (from step 2's SWITCH).

## Root cause

v1.18.36's plex_upload LPS restore (DELETE singular +
re-upload trick on fallback) was gated on
`placement_kind == 'plex_upload'`. After a SWITCH from api,
the placement becomes 'hardlink' but Plex's /themes still
has BOTH motif's API upload (unselected from the SWITCH's
DELETE singular) AND themerr-plex's original entry
(unselected since motif's upload was the most recent).
Then LPS unlinks the sidecar but doesn't touch Plex's
/themes — both entries stay unselected → Plex serves
nothing.

## Fix

The LPS restore shouldn't depend on the placement kind.
It should depend on "does Plex have a non-motif entry we
can promote?" — true for:

  - plex_upload rows (v1.18.36 already handled)
  - sidecar rows after SWITCH from api (new in v1.18.38)
  - sidecar rows with Plex Pass themes (new in v1.18.38)

v1.18.38 unifies the restore: loop over ALL placements (not
just plex_upload), GET Plex's /themes, identify motif's hash,
pick first non-motif entry, re-upload trick on it. The DELETE
singular step still only fires for plex_upload (where motif's
hash is the current Plex selection); sidecar rows skip it.
"""

from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"


# ── LPS restore extended to all placements ───────────────────


def test_lps_restore_loop_covers_all_placement_kinds():
    """The restore loop must iterate `all_placements_for_restore`
    (= sidecar + api), not just `api_placements`. This is the
    the user-100%-Wolf bug fix."""
    src = API_PY.read_text()
    fn_idx = src.index("async def api_unplace_item(")
    body = src[fn_idx:fn_idx + 28000]
    assert (
        "all_placements_for_restore = sidecar_placements + api_placements"
        in body
    ), (
        "v1.18.38: the restore loop must walk ALL placements, "
        "not just plex_upload ones"
    )
    # The for-loop iterates the unified list, not just api_placements.
    assert "for pr in all_placements_for_restore:" in body
    # The v1.18.36 was_only-plex_upload loop must be gone.
    assert "for pr in api_placements:" not in body, (
        "v1.18.38: the kind-gated loop is replaced by the "
        "unified all_placements_for_restore loop"
    )


def test_delete_singular_gated_by_was_plex_upload():
    """DELETE singular (which clears motif's prior selection in
    Plex's /themes) must only fire when the placement was
    plex_upload. Sidecar placements have no motif selection in
    /themes — their teardown is the filesystem unlink."""
    src = API_PY.read_text()
    fn_idx = src.index("async def api_unplace_item(")
    body = src[fn_idx:fn_idx + 28000]
    assert "was_plex_upload" in body
    # Gate keyword + DELETE call must be adjacent.
    # v1.18.46: anchor-based slicing. Pre-fix this was a fixed
    # window (2500 → 5500 → 8000 across the v1.18.38 refactor).
    # Now slices from `was_plex_upload = (` to the next
    # structural anchor (the "Re-upload trick" comment that
    # introduces the post-delete block).
    from _slice_helpers import slice_to_next
    gate_block = slice_to_next(
        body, "was_plex_upload = (",
        "# Re-upload trick on the fallback",
    )
    assert 'pr["placement_kind"] or ""' in gate_block
    assert "if was_plex_upload:" in gate_block
    # v1.22.42: the delete is off-loaded to a thread so the event loop isn't
    # frozen during the Plex round-trip.
    assert "deleted = await run_in_threadpool(" in gate_block
    assert "plex.delete_theme, rating_key=rk" in gate_block


def test_reupload_fallback_fires_for_both_kinds():
    """The re-upload trick must run for any placement (sidecar
    or plex_upload) when a fallback entry exists. This is what
    restores Plex's serving after motif withdraws."""
    src = API_PY.read_text()
    fn_idx = src.index("async def api_unplace_item(")
    body = src[fn_idx:fn_idx + 28000]
    # Find the if-fallback_rk block.
    assert "if fallback_rk:" in body
    fb_idx = body.index("if fallback_rk:")
    fb_block = body[fb_idx:fb_idx + 1000]
    # v1.22.42: re-upload off-loaded to a thread (comma after the bound method
    # now, not an open-paren call).
    assert "await run_in_threadpool(" in fb_block
    assert "plex.set_active_theme_via_reupload," in fb_block
    # Restored log line names the placement_kind for diagnostics.
    assert "unplace[restore]:" in fb_block
    assert 'pr["placement_kind"] or "sidecar"' in fb_block


def test_restore_log_summary_renamed_to_unplace_restore():
    """The per-row summary log line was renamed from
    `unplace[plex_upload]:` (v1.18.36) to `unplace[restore]:`
    (v1.18.38) since it now fires for both placement kinds.
    Pin so a refactor doesn't silently break log parsing."""
    src = API_PY.read_text()
    fn_idx = src.index("async def api_unplace_item(")
    body = src[fn_idx:fn_idx + 28000]
    assert '"unplace[restore]:' in body, (
        "v1.18.38: summary log line must use the unified "
        "[restore] tag (was [plex_upload] in v1.18.36)"
    )
    # Old plex_upload-specific summary line should be gone.
    # The motif_hash diagnostic warning is unrelated and stays.
    summary_lines = [
        line for line in body.splitlines()
        if "unplace[plex_upload]:" in line
        and "motif_hash" not in line
    ]
    assert not summary_lines, (
        f"v1.18.38: stale [plex_upload] summary log lines: "
        f"{summary_lines}"
    )
