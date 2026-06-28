"""v1.14.78 — Plex client switched XML → JSON parse (perf #3).

The v1.14.71 perf research note ranked XML→JSON as ~10-25%
parse speedup on the bulk-listing hot path: json.loads is
C-implemented vs ET.fromstring's partly-Python parse, and the
JSON payload is also smaller per-item. Plex has supported JSON
output on every endpoint since 2014.

Per CLAUDE.md "no fallback branches": this is a full commit
to JSON. The XML import + parsers are gone. If a Plex build
returns XML anyway (extremely rare on modern installs), motif
fails loudly and we revert.

This test file pins the JSON contract: each parser correctly
handles Plex JSON's typed shapes (ints for size/year/contentChangedAt),
its array-of-objects layout (Metadata/Directory/Guid/Media/
Part/Location), and the unified Metadata array (vs XML's
separate <Video>/<Directory>).
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from app.core.plex import (  # noqa: E402
    PlexClient, PlexConfig,
    _extract_folder_path, _extract_guids,
)


def _cfg() -> PlexConfig:
    return PlexConfig(url="http://x", token="t",
                      movie_section="1", tv_section="2")


# ── Accept header switched ────────────────────────────────────


def test_client_accept_header_is_json():
    """The HTTP Accept header on the shared httpx client must
    be application/json. Plex respects this and returns JSON
    when the request asks for it."""
    client = PlexClient(_cfg())
    try:
        assert client._headers["Accept"] == "application/json"
    finally:
        client.close()


def test_xml_etree_import_removed():
    """No XML parser code should survive in plex.py — full
    commit to JSON per CLAUDE.md 'no fallback branches'."""
    src = (REPO / "app" / "core" / "plex.py").read_text()
    assert "from xml.etree" not in src
    assert "ET.fromstring" not in src.replace(
        "ET.fromstring's", "")  # allow the comment reference


# ── JSON helpers ──────────────────────────────────────────────


def test_extract_guids_reads_json_guid_array():
    """Guid is a JSON array of objects under each item:
    `[{"id": "imdb://tt..."}, {"id": "tmdb://12345"}]`."""
    item = {
        "Guid": [
            {"id": "imdb://tt0000001"},
            {"id": "tmdb://12345"},
            {"id": "tvdb://67890"},
        ]
    }
    out = _extract_guids(item)
    assert out == {
        "imdb": "tt0000001",
        "tmdb": "12345",
        "tvdb": "67890",
    }


def test_extract_guids_handles_missing_guid_key():
    """No Guid array on the item → empty dict, no exception."""
    assert _extract_guids({}) == {}
    assert _extract_guids({"Guid": None}) == {}
    assert _extract_guids({"Guid": []}) == {}


def test_extract_guids_skips_malformed_ids():
    """Guid entries without `://` separator must be skipped."""
    item = {"Guid": [
        {"id": "valid://abc"},
        {"id": "no-separator"},
        {"id": ""},
        {},  # missing id key
    ]}
    assert _extract_guids(item) == {"valid": "abc"}


def test_extract_folder_path_reads_movie_part_file():
    """For movies: Media[0].Part[0].file → parent dir."""
    item = {"Media": [{"Part": [{"file": "/data/movies/Foo (2020)/Foo.mkv"}]}]}
    assert _extract_folder_path(item) == "/data/movies/Foo (2020)"


def test_extract_folder_path_reads_show_location_path():
    """For shows: Location[0].path → folder directly."""
    item = {"Location": [{"path": "/data/tv/Breaking Bad"}]}
    assert _extract_folder_path(item) == "/data/tv/Breaking Bad"


def test_extract_folder_path_prefers_part_over_location():
    """Part comes first in the iteration; Location only fires
    when no Part is present (or no file). This matches the
    XML-era behavior: movies always had Part, shows always had
    Location (XML never had both on the same item, but the
    ordering matters for the rare hybrid case)."""
    item = {
        "Media": [{"Part": [{"file": "/data/foo/Foo.mkv"}]}],
        "Location": [{"path": "/data/foo"}],
    }
    assert _extract_folder_path(item) == "/data/foo"


def test_extract_folder_path_handles_missing_keys():
    """No Media AND no Location → empty string."""
    assert _extract_folder_path({}) == ""
    assert _extract_folder_path({"Media": None, "Location": None}) == ""
    assert _extract_folder_path({"Media": [{"Part": []}]}) == ""


# ── discover_sections JSON parse ──────────────────────────────


def test_discover_sections_parses_json_directory_array():
    """MediaContainer.Directory is the JSON home for sections.
    Each Directory has key/type/title/uuid/agent/language/
    contentChangedAt + a Location array."""
    class _Resp:
        status_code = 200
        text = (
            '{"MediaContainer": {"Directory": ['
            '{"key": "1", "type": "movie", "uuid": "u1", '
            ' "title": "Movies", "agent": "tv.plex.agents.movie", '
            ' "language": "en-US", "contentChangedAt": 1700000000, '
            ' "Location": [{"path": "/data/movies"}, {"path": "/data/4k"}]},'
            '{"key": "2", "type": "show", "uuid": "u2", '
            ' "title": "TV Shows", "agent": "x", "language": "en", '
            ' "Location": [{"path": "/data/tv"}]}'
            ']}}'
        )

    with patch.object(PlexClient, "_get", return_value=_Resp()):
        client = PlexClient(_cfg())
        sections = client.discover_sections()
    by_id = {s.section_id: s for s in sections}
    movies = by_id["1"]
    assert movies.title == "Movies"
    assert movies.type == "movie"
    assert movies.uuid == "u1"
    assert movies.location_paths == ["/data/movies", "/data/4k"]
    assert movies.content_changed_at == "1700000000"

    tv = by_id["2"]
    assert tv.location_paths == ["/data/tv"]
    assert tv.content_changed_at == ""


def test_discover_sections_skips_unsupported_types():
    """type ∈ {movie, show}; music/photo/etc must be filtered."""
    class _Resp:
        status_code = 200
        text = (
            '{"MediaContainer": {"Directory": ['
            '{"key": "9", "type": "music", "title": "Music"},'
            '{"key": "1", "type": "movie", "title": "Movies"}'
            ']}}'
        )
    with patch.object(PlexClient, "_get", return_value=_Resp()):
        client = PlexClient(_cfg())
        sections = client.discover_sections()
    assert {s.section_id for s in sections} == {"1"}


def test_discover_sections_handles_empty_payload():
    class _Resp:
        status_code = 200
        text = '{"MediaContainer": {}}'
    with patch.object(PlexClient, "_get", return_value=_Resp()):
        client = PlexClient(_cfg())
        assert client.discover_sections() == []


def test_discover_sections_handles_malformed_json():
    """A non-JSON body (e.g. server returned HTML error page)
    must not raise — return [] like the XML ParseError branch
    used to."""
    class _Resp:
        status_code = 200
        text = '<html>error</html>'
    with patch.object(PlexClient, "_get", return_value=_Resp()):
        client = PlexClient(_cfg())
        assert client.discover_sections() == []


# ── _parse_candidates JSON parse ──────────────────────────────


def test_parse_candidates_unifies_movies_and_shows():
    """In JSON, both movies and shows live under
    MediaContainer.Metadata as a unified array; the per-item
    `type` field tells them apart. The XML version had two
    distinct element tags (<Video> vs <Directory>) — JSON
    unifies which is actually cleaner since _parse_candidates
    treats them identically."""
    text = (
        '{"MediaContainer": {"Metadata": ['
        '{"ratingKey": "100", "type": "movie", "title": "Foo", '
        ' "year": 2020, "theme": "/library/metadata/100/theme"},'
        '{"ratingKey": "200", "type": "show", "title": "Bar", '
        ' "year": 2018, "editionTitle": "Director\'s Cut"}'
        ']}}'
    )
    client = PlexClient(_cfg())
    cands = client._parse_candidates(text)
    by_rk = {c.rating_key: c for c in cands}
    assert by_rk["100"].title == "Foo"
    assert by_rk["100"].year == "2020"  # JSON int → str
    assert by_rk["100"].has_theme is True
    assert by_rk["200"].edition_title == "Director's Cut"
    assert by_rk["200"].has_theme is False


def test_parse_candidates_handles_empty_metadata():
    """No Metadata key → []."""
    client = PlexClient(_cfg())
    assert client._parse_candidates('{"MediaContainer": {}}') == []


def test_parse_candidates_handles_malformed_body():
    """Non-JSON body → []; the early-bail XML pre-check
    (<Video / <Directory) was retired in v1.14.78 because
    _parse_candidates handles empty cases natively."""
    client = PlexClient(_cfg())
    assert client._parse_candidates('not json') == []
    assert client._parse_candidates('') == []


# ── enumerate_section_items end-to-end JSON parse ─────────────


def test_enumerate_parses_json_metadata_with_guids_and_paths():
    """One full mock pass: a single page of Metadata items with
    Guid, Media[].Part[], Location children. Verifies the
    PlexLibraryItem dataclass is populated identically to the
    XML era."""
    text = (
        '{"MediaContainer": {"size": 2, "totalSize": 2, "Metadata": ['
        '{"ratingKey": "100", "type": "movie", "title": "Foo", '
        ' "year": 2020, "theme": "/library/metadata/100/theme", '
        ' "Guid": [{"id": "imdb://tt0000001"}, {"id": "tmdb://12345"}], '
        ' "Media": [{"Part": [{"file": "/data/movies/Foo (2020)/Foo.mkv"}]}]},'
        '{"ratingKey": "200", "type": "show", "title": "Bar", '
        ' "year": 2018, '
        ' "Guid": [{"id": "tvdb://67890"}], '
        ' "Location": [{"path": "/data/tv/Bar"}]}'
        ']}}'
    )

    class _Resp:
        status_code = 200

    resp = _Resp()
    resp.text = text
    with patch.object(PlexClient, "_get", return_value=resp):
        client = PlexClient(_cfg())
        items = client.enumerate_section_items(
            section_id="1", media_type="movie")

    by_rk = {it.rating_key: it for it in items}
    foo = by_rk["100"]
    assert foo.title == "Foo"
    assert foo.year == "2020"
    assert foo.guid_imdb == "tt0000001"
    assert foo.guid_tmdb == 12345
    assert foo.folder_path == "/data/movies/Foo (2020)"
    assert foo.has_theme is True
    assert foo.plex_theme_uri == "/library/metadata/100/theme"

    bar = by_rk["200"]
    assert bar.guid_tvdb == 67890
    assert bar.folder_path == "/data/tv/Bar"
    assert bar.has_theme is False
    assert bar.plex_theme_uri == ""


def test_enumerate_reads_container_size_and_total_as_ints():
    """Plex JSON's MediaContainer.size and totalSize come back
    as ints (not str like XML attrs). The pagination loop's
    int() conversion handles both — pin that the parser doesn't
    blow up on a JSON int input."""
    text = (
        '{"MediaContainer": {"size": 1, "totalSize": 1, "Metadata": ['
        '{"ratingKey": "1", "type": "movie", "title": "A", '
        ' "Media": [{"Part": [{"file": "/m/A/A.mkv"}]}]}'
        ']}}'
    )

    class _Resp:
        status_code = 200

    resp = _Resp()
    resp.text = text
    with patch.object(PlexClient, "_get", return_value=resp):
        client = PlexClient(_cfg())
        items = client.enumerate_section_items(
            section_id="1", media_type="movie")
    assert len(items) == 1
    assert items[0].rating_key == "1"


# ── get_item_paths JSON parse ────────────────────────────────


def test_get_item_paths_reads_json_metadata():
    """v1.14.78: single-rk get_item_paths now parses JSON
    too. Returns a list of all paths the response yields
    across Media[].Part[].file + Location[].path."""
    text = (
        '{"MediaContainer": {"Metadata": ['
        '{"ratingKey": "1", '
        ' "Media": [{"Part": [{"file": "/data/foo/foo.mkv"}]}], '
        ' "Location": [{"path": "/data/foo"}]}'
        ']}}'
    )

    class _Resp:
        status_code = 200

    resp = _Resp()
    resp.text = text
    with patch.object(PlexClient, "_get", return_value=resp):
        client = PlexClient(_cfg())
        paths = client.get_item_paths("1")
    assert "/data/foo/foo.mkv" in paths
    assert "/data/foo" in paths
