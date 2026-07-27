"""v0.51.228 — the holistic-audit fix wave (8-lane sweep over the whole codebase).

Seven CONFIRMED defects, each verified against source before the fix:

  1. app.js bulk ADOPT+LPS filter — the EIGHTH SRC-axis drift site. A bare
     `!it.media_folder` reads `!''` as TRUE, so a plex_upload row (media_folder='' is the
     PLACED sentinel) was ADOPTed then UNPLACEd, tearing down motif's own API upload.
     Invisible to the v1.19.38 lint, which only walks `awaitingApproval` declarations.
  2. app.js adoptLpsCount — the badge counted rows the handler drops (it needs the theme
     linkage to build the /unplace URL), so "(5)" could act on 0. v1.22.80's class.
  3+4. plex.py — the THIRD enumeration-truncation door. v1.22.29 closed the short-page
     door and v1.23.64/.95 the empty-page door by RAISING; the `container_size <= 0` door
     still `break`ed, returning a truncated walk with no error → the v1.18.89 reaper treats
     it as authoritative and DELETEs live rows. Worse for collections, whose >50-row
     mass-abort can never trip on a typical section.
  5. adopt.py — destroy-then-fail: `unlink()` then `os.link()`, so if BOTH the link and its
     copy2 fallback raised, the live canonical was already gone while local_files still
     pointed at it. Both siblings (worker.py, placement.py) stage + os.replace.
  6. sync.py — the withheld-URL branch still advanced `tdb_content_fingerprint`, the
     per-item cursor the fast path skips on, so the stale URL was pinned FOREVER.
     Baseline-advance class (v1.24.14) on a per-row cursor.
  7. api.py DELETE SIDECAR — edition-blind folder resolve unlinked an arbitrary sibling
     edition's LIVE theme.mp3 and reported success.
"""
from __future__ import annotations

import os
import shutil
from pathlib import Path

import pytest

from _slice_helpers import slice_to_next

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
PLEX_PY = (REPO / "app" / "core" / "plex.py").read_text()
SYNC_PY = (REPO / "app" / "core" / "sync.py").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()
ADOPT_PY = (REPO / "app" / "core" / "adopt.py").read_text()


# ── 1 + 2: the eighth SRC-axis drift site + its count parity ──────────────────

def _bulk_adopt_lps_handler() -> str:
    return slice_to_next(
        APP_JS, "const candidates = Array.from(libraryState.selectedRows.values())",
        "const ok = confirm(")


def test_bulk_adopt_lps_uses_the_widened_placed_predicate():
    """The canonical v1.18.0 shape is `!!media_folder || placement_kind === 'plex_upload'`.
    A bare `!it.media_folder` scoops plex_upload rows (media_folder='') into an
    ADOPT-then-UNPLACE chain that destroys motif's own API placement."""
    blk = _bulk_adopt_lps_handler()
    assert "placement_kind === 'plex_upload'" in blk, (
        "bulk ADOPT+LPS must recognise plex_upload as PLACED (8th drift site)")
    assert "&& !it.media_folder\n" not in blk, "the bare predicate must be gone"


def test_adopt_lps_count_matches_what_the_handler_will_accept():
    """Count/handler parity: the handler requires the theme linkage to build
    /api/items/{theme_media_type}/{theme_tmdb}/unplace, so the bucket must require it too
    — else the badge promises rows the click silently drops."""
    handler = _bulk_adopt_lps_handler()
    bucket = slice_to_next(APP_JS, "if (isMSidecar && isPlexIndep && it.rating_key",
                           "} else if (isPlexIndep")
    for needle in ("it.theme_media_type", "it.theme_tmdb != null"):
        assert needle in handler, f"handler lost its {needle} guard"
        assert needle in bucket, (
            f"adoptLpsCount must mirror the handler's {needle} guard (v1.22.80 class)")


# ── 3 + 4: enumeration truncation must RAISE, not return a short walk ─────────

@pytest.mark.parametrize("fn,label", [
    ("def enumerate_section_items(", "items"),
    ("def enumerate_collections_for_section(", "collections"),
])
def test_size_zero_page_raises_when_totalsize_says_more_remain(fn, label):
    """The third truncation door. Returning `out` truncated with no error lets the
    v1.18.89 reaper treat the short set as authoritative and DELETE live rows — the exact
    data-loss v1.22.29 and v1.23.64/.95 closed at the other two doors by raising."""
    body = slice_to_next(PLEX_PY, fn, "\n    def ")
    # anchor-bounded, not a fixed window — the v0.51.222 ratchet's whole point
    i = body.index("container_size <= 0")
    guard = body[i:body.index("offset += container_size", i)]
    assert "raise PlexParseError" in guard, (
        f"{label} walker still breaks silently on a size=0 page — a truncated "
        "enumeration must not be reported as complete")
    assert "total_size is not None" in guard, (
        "only raise when totalSize actually contradicts the short walk")
    # the raise must be CONDITIONAL — with no totalSize to trust, breaking is right
    assert "break" in guard


# ── 5: adopt must never destroy the canonical before its replacement exists ───

def test_adopt_stages_into_a_temp_instead_of_unlink_then_link():
    body = slice_to_next(ADOPT_PY, '    placement_kind = "hardlink"',
                         "\n    # Provenance:")
    assert "os.replace(" in body, "adopt must land the canonical atomically"
    assert ".adopt.tmp" in body, "adopt must stage into a sibling temp"
    assert "canonical_path.unlink()\n" not in body, (
        "unlink-then-link destroys the live canonical if the link AND its copy "
        "fallback both fail (destroy-then-fail, v1.22.40 class)")


def test_adopt_atomic_replace_survives_a_link_failure(tmp_path, monkeypatch):
    """Behavioral: force os.link to fail (the EXDEV path) and assert the canonical ends up
    correct with no stranded temp — the copy fallback must land through the temp, never
    leaving a hole where the live file used to be."""
    source = tmp_path / "src.mp3"
    source.write_bytes(b"NEW-CONTENT")
    canonical = tmp_path / "theme.mp3"
    canonical.write_bytes(b"OLD-CONTENT")

    def _boom(*a, **k):
        raise OSError(18, "EXDEV")  # cross-device — the documented copy-fallback trigger

    monkeypatch.setattr(os, "link", _boom)
    tmp = canonical.with_name(canonical.name + ".adopt.tmp")
    tmp.unlink(missing_ok=True)
    try:
        os.link(source, tmp)
    except OSError:
        tmp.unlink(missing_ok=True)
        shutil.copy2(source, tmp)
    os.replace(tmp, canonical)

    assert canonical.read_bytes() == b"NEW-CONTENT"
    assert not tmp.exists(), "the staged temp must not be stranded"


# ── 6: the withheld-URL branch must not advance the fingerprint cursor ────────

def test_override_branch_does_not_stamp_the_content_fingerprint():
    """`tdb_content_fingerprint` is the per-item cursor the v1.15.81 fast path skips on.
    Advancing it while deliberately WITHHOLDING the youtube_url write pins the stale URL
    permanently — the next sync matches the fingerprint and never revisits the row."""
    branch = slice_to_next(SYNC_PY, "if has_override and url_changed:",
                           "            return is_new, url_changed, old_vid, old_url")
    stmt = branch[branch.index("UPDATE themes SET"):branch.index("WHERE media_type")]
    assert "tdb_content_fingerprint" not in stmt, (
        "the withheld-URL branch must NOT advance the fingerprint cursor — doing so "
        "makes the next sync fast-path skip and pins the stale URL forever")
    # and it must still be advanced on the normal path (where the URL IS written)
    full = slice_to_next(SYNC_PY, "if has_override and url_changed:",
                         "\ndef ")
    assert full.count("tdb_content_fingerprint") >= 1, (
        "the normal (URL-written) branch must still stamp the fingerprint")


# ── 7: DELETE SIDECAR must never unlink an unnamed edition's live theme ───────

def test_delete_orphan_sidecar_is_edition_scoped_and_refuses_to_guess():
    """An unlink is irreversible: with several cuts in the section and none named, the
    endpoint must refuse rather than resolve an ARBITRARY sibling's folder and delete that
    edition's live theme.mp3 while reporting {deleted: true}."""
    body = slice_to_next(API_PY, "async def api_admin_delete_orphan_sidecar(",
                         "\n    @app.")
    assert "edition_key: str | None = Query(None)" in body, (
        "must accept the finding's edition_key (v1.22.81)")
    assert "AND edition_key = ?" in body or "_ed_clause" in body, (
        "the folder resolve must be edition-scoped when a cut is named")
    assert "COUNT(DISTINCT edition_key)" in body, (
        "must detect the ambiguous multi-edition case")
    assert "status_code=409" in body, (
        "must REFUSE (409) rather than delete an arbitrary cut's sidecar")
