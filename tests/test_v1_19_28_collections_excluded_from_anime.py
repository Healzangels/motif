"""v1.19.28 — exclude collections from the /anime library tab.

the user's screenshot on /anime showed "Dragon Ball Collection",
"My Hero Academia Collection", "One Piece Collection" rendering
alongside actual TV shows. Plex stores TMDB-collection items
INSIDE anime sections with plex_items.media_type='collection'.
v1.18.0's collections work gave them a dedicated /collections
tab, but the /anime tab's where-clause was `ps.is_anime = 1`
with NO media_type constraint — collections leaked through.

The /movies and /tv tabs already constrained media_type ('movie'
and 'show' respectively), implicitly excluding collections.
The anime tab was the lone outlier.

## What's pinned

- Anime tab where-clause (api_library): `ps.is_anime = 1 AND
  pi.media_type IN ('movie', 'show')` — explicit allowlist.
- Anime branch of api_library_download_missing: same constraint
  (the bulk download endpoint must follow what the user sees).
- Collections tab still uses `pi.media_type = 'collection'`
  unchanged — collections continue to surface there.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()


def test_anime_tab_excludes_collections_in_library_query():
    """The api_library tab_where for tab=='anime' must filter
    pi.media_type to ('movie', 'show') so collection-type
    plex_items in anime sections don't render on /anime."""
    # Find the anime branch of the tab_where ladder. It's the
    # `else:` clause after the explicit 'movies'/'tv'/'collections'
    # branches.
    import re
    # The first anime branch (api_library) uses the `else:` form.
    # Search for the multi-line string concatenation including the
    # IN clause that v1.19.28 added.
    matches = re.findall(
        r'tab_where\s*=\s*\(\s*"ps\.is_anime\s*=\s*1\s*"\s*'
        r'"AND pi\.media_type IN \(\'movie\', \'show\'\)"\s*\)',
        API_PY,
    )
    assert len(matches) >= 2, (
        f"v1.19.28: BOTH api_library AND "
        f"api_library_download_missing must constrain the anime "
        f"tab to movie+show media_types; found {len(matches)} "
        "matching tab_where assignment(s) — expected 2"
    )


def test_movies_and_tv_branches_unchanged():
    """Verify the /movies and /tv branches still have their
    explicit media_type constraints — they were already correct."""
    assert "pi.media_type = 'movie' AND ps.is_anime = 0" in API_PY
    assert "pi.media_type = 'show' AND ps.is_anime = 0" in API_PY


def test_collections_branch_unchanged():
    """The /collections branch must still match
    pi.media_type='collection' so collections continue to render
    on that tab."""
    assert "pi.media_type = 'collection'" in API_PY


def test_anime_branch_does_not_carry_naked_is_anime_filter():
    """Counter-guard: the anime branch must NOT use the pre-v1.19.28
    naked `ps.is_anime = 1` (no trailing media_type filter). A bare
    re-introduction would silently re-leak collections."""
    # Search for tab_where = "ps.is_anime = 1" as a complete
    # assignment (not the multi-line tuple with media_type filter).
    naked_assignments = [
        line for line in API_PY.split("\n")
        if 'tab_where = "ps.is_anime = 1"' in line
    ]
    assert not naked_assignments, (
        f"v1.19.28: found naked `tab_where = \"ps.is_anime = 1\"` "
        f"assignment(s) — must include the media_type filter to "
        f"exclude collections from the anime tab. Lines: "
        f"{naked_assignments}"
    )
