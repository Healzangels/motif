"""v0.51.240 — the de-orphan kind guard was a 2-class test on a 3-class space.

`_resolve_real_tmdb` refuses a re-key when TMDB's kind disagrees with the
orphan's media_type, "otherwise we'd re-key into the wrong identity space". It
expressed that as:

    if (cand_kind == "tv") != (media_type == "tv"):

which only separates tv from not-tv. themes.media_type is movie / tv /
COLLECTION, and _lookup_by_imdb only ever returns 'movie' or 'tv' (it reads
movie_results then tv_results and returns None otherwise). So a COLLECTION
orphan resolving to a movie evaluated `False != False` → passed the guard → the
collection row was re-keyed onto a MOVIE's tmdb_id while keeping
media_type='collection'.

Reachable because the v1.22.52 mint stamps plex_items.guid_imdb onto the minted
orphan regardless of media_type, and this walker's candidate SELECT filters on
upstream_source / tmdb_id / imdb_id but NOT media_type.
"""
from __future__ import annotations

import pytest

from app.core.deorphan import _resolve_real_tmdb


class _Client:
    """Stands in for TMDBClient.lookup_by_imdb, which yields only movie/tv."""

    def __init__(self, kind, tmdb_id=555):
        self._kind, self._tmdb_id = kind, tmdb_id

    def lookup_by_imdb(self, imdb_id):
        return {"tmdb_id": self._tmdb_id, "kind": self._kind}


# ── matching kinds still resolve ─────────────────────────────────────────

@pytest.mark.parametrize("kind", ["movie", "tv"])
def test_matching_kind_resolves(kind):
    assert _resolve_real_tmdb(_Client(kind), "tt1", kind) == 555


# ── mismatched kinds are refused ─────────────────────────────────────────

@pytest.mark.parametrize("orphan_mt,cand_kind", [
    ("movie", "tv"),          # already refused pre-fix
    ("tv", "movie"),          # already refused pre-fix
    ("collection", "movie"),  # THE BUG: passed pre-fix
    ("collection", "tv"),     # refused pre-fix only by accident of the tv test
])
def test_mismatched_kind_is_refused(orphan_mt, cand_kind):
    assert _resolve_real_tmdb(_Client(cand_kind), "tt1", orphan_mt) is None, (
        f"a {orphan_mt} orphan must not be re-keyed onto a {cand_kind} id")


def test_a_collection_orphan_is_never_rekeyed_from_an_imdb_hit():
    """The consequential case stated plainly: TMDB's /find cannot resolve a
    collection identity from an imdb id (it returns only movie_results /
    tv_results), so ANY hit for a collection orphan is the wrong namespace."""
    for kind in ("movie", "tv"):
        assert _resolve_real_tmdb(_Client(kind), "tt0468569", "collection") is None


# ── the guard's shape ────────────────────────────────────────────────────

def test_guard_is_equality_not_a_tv_vs_not_tv_xor():
    import inspect
    src = inspect.getsource(_resolve_real_tmdb)
    code = "\n".join(ln for ln in src.splitlines()
                     if not ln.lstrip().startswith("#"))
    assert 'if cand_kind != media_type:' in code
    assert '(cand_kind == "tv") != (media_type == "tv")' not in code, (
        "the XOR silently admits every non-tv pairing, including "
        "collection-vs-movie")


def test_no_hit_and_partial_hit_still_return_none():
    """Regression lock on the surrounding early-outs."""
    class _NoHit:
        def lookup_by_imdb(self, imdb_id):
            return None

    class _Partial:
        def lookup_by_imdb(self, imdb_id):
            return {"tmdb_id": None, "kind": "movie"}

    assert _resolve_real_tmdb(_NoHit(), "tt1", "movie") is None
    assert _resolve_real_tmdb(_Partial(), "tt1", "movie") is None
