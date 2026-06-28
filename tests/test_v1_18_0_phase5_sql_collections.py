"""v1.18.0 Phase 5 — SQL + plex_enum routing for /collections tab.

Two surfaces here:

  1. `/api/library`, `/api/library/download-missing`, and
     `/api/libraries/refresh` now accept `tab='collections'` as a
     fourth value parallel to movies/tv/anime. Each route's
     tab_where (or sec_where) maps `collections` to
     `pi.media_type = 'collection'` with no anime/4K split (since
     collections live within both 4K and non-4K parent sections,
     and Plex doesn't tag a collection as 4K at the metadata level).

  2. `plex_enum` per-section main loop adds a collections-pass:
     after `enumerate_section_items` + `_upsert_items` for the
     section's regular items, also call
     `enumerate_collections_for_section(section_id)` and feed those
     PlexLibraryItem(media_type='collection') rows through the same
     `_upsert_items` so they land in plex_items keyed the same way
     as movies/shows.

`_SRC_LETTER_SQL` is intentionally NOT changed in this phase:
the existing CASE expression's branches already classify
media_type='collection' rows correctly because:

  - placement_kind='plex_upload' rows have `media_folder=''`
    which is NOT NULL → T / U branches still fire by source_kind
  - A (adopt) / M (sidecar) never fire — collections have no
    folder to adopt-from or sidecar-into
  - P fires when Plex serves a theme and motif didn't place
    (the same way movie/show P does)

So a separate `_SRC_LETTER_SQL` variant for collections is
unnecessary — one source-of-truth keeps the row pill, sort,
and pill filter in lockstep.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

API_PY = REPO / "app" / "web" / "api.py"
PLEX_ENUM_PY = REPO / "app" / "core" / "plex_enum.py"


# ── /api/library accepts 'collections' tab ────────────────────


def test_api_library_route_regex_includes_collections():
    """The Query(..., pattern=...) on the /api/library route must
    accept 'collections' so a GET /api/library?tab=collections
    isn't rejected with 422."""
    src = API_PY.read_text()
    # The regex pattern must explicitly include 'collections'.
    assert 'pattern="^(movies|tv|anime|collections)$"' in src, (
        "v1.18.0: /api/library tab regex must include 'collections'"
    )


def test_api_library_tab_where_branches_to_collection_media_type():
    """`_library_query` (the SQL helper) must map tab='collections'
    to `pi.media_type = 'collection'` so the row filter pulls the
    right plex_items rows."""
    src = API_PY.read_text()
    # The collections branch must be present in the tab_where
    # dispatch.
    assert 'elif tab == "collections":' in src
    # The body must set tab_where to the collection predicate.
    coll_idx = src.index('elif tab == "collections":')
    nearby = src[coll_idx:coll_idx + 600]
    assert "pi.media_type = 'collection'" in nearby


def test_api_library_skips_fourk_split_for_collections():
    """Collections aren't 4K-tagged in Plex, so the tab_where
    must skip the `AND ps.is_4k = ?` suffix for them. Otherwise a
    user with only non-4K parent sections would see an empty page
    when fourk=true is the default."""
    src = API_PY.read_text()
    # Anchor on the line where the fourk filter is conditionally
    # skipped for collections.
    assert "if tab != \"collections\":" in src, (
        "v1.18.0: /api/library must skip the fourk filter when "
        "tab='collections' — collections aren't 4K-tagged."
    )


# ── /api/library/download-missing accepts 'collections' ───────


def test_download_missing_validates_collections_tab():
    """The download-missing endpoint's tab validation must accept
    'collections' (was movies/tv/anime only)."""
    src = API_PY.read_text()
    # Two validations to grep — explicit set check + branch.
    pat = re.search(
        r'tab\s+not\s+in\s+\(\s*"movies"\s*,\s*"tv"\s*,\s*"anime"\s*,\s*"collections"\s*\)',
        src,
    )
    assert pat is not None, (
        "v1.18.0: download-missing must include 'collections' in "
        "its tab validation tuple"
    )


# ── /api/libraries/refresh accepts 'collections' ──────────────


def test_libraries_refresh_validates_collections_tab():
    """The libraries/refresh route's tab validation must accept
    'collections'."""
    src = API_PY.read_text()
    pat = re.search(
        r'tab\s+not\s+in\s+\(\s*"movies"\s*,\s*"tv"\s*,\s*"anime"\s*,\s*"collections"\s*\)',
        src,
    )
    assert pat is not None


def test_libraries_refresh_sec_where_covers_all_sections_for_collections():
    """For the collections tab, the section selector must visit
    EVERY included section (both movie- and show-typed) since
    collections live within both. sec_where = '1=1' is the right
    no-op filter."""
    src = API_PY.read_text()
    # Find the libraries/refresh tab dispatch and inspect its
    # branches.
    refresh_idx = src.index("if tab in (\"movies\", \"tv\", \"anime\", \"collections\"):")
    block = src[refresh_idx:refresh_idx + 2000]
    assert 'elif tab == "collections":' in block
    assert 'sec_where = "1=1"' in block, (
        "v1.18.0: collections sec_where must be '1=1' so plex_enum "
        "is enqueued on every included section regardless of type."
    )


def test_libraries_refresh_skips_4k_filter_for_collections():
    """Collections aren't 4K-tagged → enqueue on all included
    sections without the requested_4k / fallback_4k pair the
    movie/tv/anime path uses."""
    src = API_PY.read_text()
    # Anchor inside the libraries/refresh route's section-select
    # SQL by walking past the (earlier) sec_where dispatch to the
    # branch where the SELECT actually runs.
    refresh_idx = src.index("if tab in (\"movies\", \"tv\", \"anime\", \"collections\"):")
    block = src[refresh_idx:refresh_idx + 4000]
    # The SELECT-specific collections branch is the SECOND
    # `if tab == "collections":` in this block (the first is the
    # sec_where branch). Walk past the first occurrence.
    first_coll = block.index("if tab == \"collections\":")
    second_coll = block.index("if tab == \"collections\":", first_coll + 1)
    # Clip the branch at the `else:` that immediately follows it
    # so we don't pick up the else-branch's is_4k filter.
    branch_chunk = block[second_coll:second_coll + 1200]
    else_offset = branch_chunk.find("\n                else:")
    branch = branch_chunk if else_offset < 0 else branch_chunk[:else_offset]
    assert "SELECT section_id FROM plex_sections" in branch
    # The if-branch (collections-only path) must NOT add the
    # is_4k suffix.
    assert "AND is_4k =" not in branch, (
        "v1.18.0: the collections sec-query must NOT filter on "
        "is_4k — collections aren't 4K-tagged."
    )


# ── plex_enum runs a collections pass ─────────────────────────


def test_plex_enum_calls_enumerate_collections_for_section():
    """The plex_enum per-section loop must call
    `enumerate_collections_for_section(section_id=...)` after the
    main `enumerate_section_items` call. Without this, the
    /collections tab stays empty even though motif knows about
    the Plex section."""
    src = PLEX_ENUM_PY.read_text()
    assert "enumerate_collections_for_section(" in src, (
        "v1.18.0: plex_enum per-section loop must enumerate "
        "collections via PlexClient.enumerate_collections_for_section"
    )


def test_plex_enum_collections_pass_feeds_upsert_items():
    """The collections-pass result must feed into the same
    `_upsert_items` helper that the main items pass uses — so
    plex_items rows for collections land via the same code path."""
    src = PLEX_ENUM_PY.read_text()
    # Find the collections-pass block and assert _upsert_items is
    # called with the collections list.
    idx = src.index("enumerate_collections_for_section(")
    block = src[idx:idx + 1500]
    assert "_upsert_items(" in block
    assert "collections" in block


def test_plex_enum_collections_pass_swallows_failures_safely():
    """A collection fetch failure on one section must NOT kill the
    whole plex_enum run — class-9 hygiene: log + continue with an
    empty list. The next enum retries."""
    src = PLEX_ENUM_PY.read_text()
    idx = src.index("enumerate_collections_for_section(")
    block = src[idx:idx + 1500]
    # Defensive except + a log.warning breadcrumb.
    assert "except Exception" in block
    assert "log.warning" in block, (
        "v1.18.0: collections-pass failure must log a breadcrumb "
        "(class-9 hygiene) — not silent-fail."
    )


# ── _SRC_LETTER_SQL stays canonical (no collection variant) ────


def test_src_letter_sql_remains_single_source_of_truth():
    """v1.18.0 intentionally does NOT branch _SRC_LETTER_SQL on
    media_type='collection'. The existing CASE branches already
    classify collections correctly (placement_kind='plex_upload'
    with media_folder='' is NOT NULL → T/U fire; A/M never fire;
    P fires when Plex serves a theme motif didn't place).
    Splitting into a collection-specific variant would drift the
    row pill / sort / pill filter trio — strongly resist.

    This test pins that there's still exactly one _SRC_LETTER_SQL
    definition."""
    src = API_PY.read_text()
    # v1.21.57: the canonical definition is now the _src_letter_sql()
    # builder (one def); the bare constant binds to its default once.
    matches = re.findall(r"^def _src_letter_sql\(", src, re.MULTILINE)
    assert len(matches) == 1, (
        f"v1.18.0: _SRC_LETTER_SQL must remain a single canonical "
        f"definition (found {len(matches)} — splitting into a "
        f"collection variant would drift the SRC pill / sort / "
        f"filter trio)"
    )
