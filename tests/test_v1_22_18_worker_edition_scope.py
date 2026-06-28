"""v1.22.18 — worker edition-scope cluster (audit MED #8, #9, #10, #11).

Four worker.py writes in the placement path leaked across editions or read a
stale signal. Same class as the v1.21.5x–9x edition arc — pinned here as
shape-lints (these are query-scope changes; the shape IS the fix).

#8  _do_place post-place plex_items "hint" UPDATE (local_theme_file=1 /
    has_theme on a skipped place) was section-scoped but NOT edition-scoped, so
    a skipped place on one edition stamped EVERY sibling edition's row in the
    section (same tmdb_id, distinct {edition-X} folders). Now edition-scoped
    when the job named an edition.
#9  _do_place pre-place +P capture read the raw pi["has_theme"]=1, ignoring the
    stale-cache override that demoted cached_has_theme to False when
    verified_ok=0 (Plex's /theme 404s — the has_theme claim is a lie). Pre-fix
    it stamped plex_independent_theme=1 on a row whose Plex theme is broken →
    a phantom +P yellow dot. Now gates on cached_has_theme + a SQL verified_ok
    guard.
#10 _do_relink's success UPDATE (placement_kind='hardlink') omitted edition_key
    though the sibling DELETE 6 lines up includes it. media_folder is per-
    edition distinct for sidecar placements, but plex_upload editions share
    media_folder='' — the bare UPDATE would stamp every sharing edition. Now
    the PK is fully pinned.
#11 _do_place's successful-place mismatch-clear used place_edition_key (the
    REQUESTED edition) while the placements INSERT in the same transaction used
    placed_edition_key (the folder it PHYSICALLY landed in). They diverge when
    a place lands in a different edition's folder than asked; the two writes
    then split. Now both key on placed_edition_key.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
WORKER_PY = (REPO / "app" / "core" / "worker.py").read_text()


def _do_place_body() -> str:
    i = WORKER_PY.index("def _do_place(self")
    j = WORKER_PY.index("def _do_place_collection(", i)
    return WORKER_PY[i:j]


def _do_relink_body() -> str:
    i = WORKER_PY.index("def _do_relink(self")
    j = WORKER_PY.index("\n    def ", i + 10)
    return WORKER_PY[i:j]


# ── #8: post-place hint UPDATE is edition-scoped ─────────────


def test_post_place_hint_update_is_edition_scoped():
    body = _do_place_body()
    # The hint block builds pi_where from theme_id/guid_tmdb, then appends
    # section scope, then (v1.22.18) edition scope when an edition is named.
    anchor = body.index("audit MED #8")
    block = body[anchor:anchor + 900]
    assert "if place_edition_key:" in block, (
        "v1.22.18 #8: the plex_items hint UPDATE must edition-scope when the "
        "place job named an edition"
    )
    assert 'pi_where += " AND edition_key = ?"' in block
    assert "pi_args.append(place_edition_key)" in block


# ── #9: +P stamp honors the verified_ok demotion ─────────────


def test_plus_p_capture_gates_on_cached_has_theme_not_raw():
    body = _do_place_body()
    anchor = body.index("audit MED #9")
    block = body[anchor:anchor + 1400]
    # The condition gates on cached_has_theme (post-demotion), not pi["has_theme"].
    assert "and cached_has_theme" in block, (
        "v1.22.18 #9: +P capture must gate on cached_has_theme (which the "
        "stale-cache override demotes when verified_ok=0)"
    )
    # And the SQL has a write-time verified guard.
    assert "COALESCE(plex_theme_verified_ok, 1) <> 0" in block, (
        "v1.22.18 #9: the +P UPDATE must exclude verified_ok=0 rows"
    )
    # The raw pi["has_theme"] must no longer be the condition's gate (it is
    # demoted into cached_has_theme); the condition line itself is checked above.
    cond = block[block.index("if (pi is not None"):block.index("):")]
    assert 'pi["has_theme"]' not in cond


# ── #10: relink success UPDATE pins edition_key ──────────────


def test_relink_update_pins_edition_key():
    body = _do_relink_body()
    idx = body.index("UPDATE placements SET placement_kind = 'hardlink'")
    upd = body[idx:idx + 460]
    assert "AND edition_key = ?" in upd, (
        "v1.22.18 #10: the relink success UPDATE must pin edition_key (mirror "
        "the DELETE) so it doesn't stamp sibling plex_upload editions"
    )
    assert 'r["edition_key"]' in upd


def test_relink_delete_still_edition_scoped():
    """Regression guard: the sibling DELETE keeps its edition_key (the shape
    #10 mirrors)."""
    body = _do_relink_body()
    idx = body.index("DELETE FROM placements")
    dele = body[idx:idx + 320]
    assert "AND edition_key = ?" in dele


# ── #11: mismatch-clear keys on the placed (physical) edition ─


def test_mismatch_clear_keys_on_placed_edition():
    """v1.22.73 superseded the v1.22.18 choice: the clear keys on
    _lf_edition — the SOURCE local_files row the place read (possibly
    the shared '' fallback). The placed/requested editions may have no
    local_files row at all, so the pre-fix clear was a 0-row no-op and
    the stale mismatch_state kept the row in the hourly sweep forever."""
    body = _do_place_body()
    anchor = body.index("audit MED #11")
    block = body[anchor:anchor + 1300]
    upd = block[block.index("UPDATE local_files SET mismatch_state = NULL"):]
    upd = upd[:upd.index(")", upd.index("_lf_edition")) + 1]
    assert "_lf_edition" in upd, (
        "v1.22.73: the mismatch-clear must key on _lf_edition "
        "(the source row the place read)"
    )
    assert "placed_edition_key" not in upd


def test_placement_insert_and_mismatch_clear_use_same_edition_key():
    """v1.22.73: the two writes now INTENTIONALLY diverge — the
    placements INSERT keys on placed_edition_key (the physical landing
    folder), the local_files clear on _lf_edition (the source row that
    actually exists). Pin each key in its own write."""
    body = _do_place_body()
    # The placements INSERT computes placed_edition_key just before it.
    assert "placed_edition_key = edition_key_for_folder(" in body
    ins = body.index("INSERT INTO placements")
    clr = body.index("UPDATE local_files SET mismatch_state = NULL")
    assert ins < clr, "mismatch-clear should follow the placements INSERT"
    assert "placed_edition_key" in body[ins:clr]
    assert "_lf_edition" in body[clr:clr + 400]


def test_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
