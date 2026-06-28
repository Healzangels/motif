"""v1.18.0 Phase 2 — Plex client gains collection methods.

Three new methods on PlexClient land in this phase, mirroring the
movie/show shape but for the collections endpoint:

  * `enumerate_collections_for_section(section_id)` —
    GET `/library/sections/{id}/collections` (paginated), returns
    `list[PlexLibraryItem]` with `media_type='collection'`,
    `folder_path=''` (collections have no media folder), and
    `guid_tmdb` populated from the Guid array's `tmdb://` entries
    (the TMDB-collection ID space ThemerrDB tracks under
    `movie_collections/themoviedb/{id}.json`).

  * `upload_collection_theme(rating_key, audio_bytes, content_type)`
    — POST raw audio bytes to `/library/metadata/{rk}/themes`.
    Fire-and-forget contract: returns True on 2xx, False otherwise,
    with a log breadcrumb on the failure path (Class-9 hygiene).

  * `delete_collection_theme(rating_key)` —
    DELETE `/library/metadata/{rk}/themes`. Same return contract.

Reuses PlexLibraryItem (vs a separate dataclass) so plex_enum's
upsert path stays unified — the v55 plex_items.media_type CHECK
widening from Phase 1 makes 'collection' a first-class value.

Class-9 hygiene: every defensive `try/except` in the new methods
must emit a `log.warning` breadcrumb (verified via
`upload_*` / `delete_*` failure-path tests below). No bare swallow.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch, MagicMock


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from app.core.plex import (  # noqa: E402
    PlexClient, PlexConfig, PlexParseError,
)


def _cfg() -> PlexConfig:
    return PlexConfig(url="http://x", token="t",
                      movie_section="1", tv_section="2")


# ── enumerate_collections_for_section ─────────────────────────


def test_enumerate_collections_returns_collection_media_type():
    """Items must carry media_type='collection' (NOT 'movie' or
    'show') so plex_enum upserts route into the right schema row.
    The v55 plex_items.media_type CHECK widening accepts this."""
    text = (
        '{"MediaContainer": {"size": 1, "totalSize": 1, "Metadata": ['
        '{"ratingKey": "501", "type": "collection", '
        ' "title": "Harry Potter Collection", '
        ' "Guid": [{"id": "tmdb://1241"}]}'
        ']}}'
    )

    class _Resp:
        status_code = 200

    resp = _Resp()
    resp.text = text
    with patch.object(PlexClient, "_get", return_value=resp):
        client = PlexClient(_cfg())
        items = client.enumerate_collections_for_section(section_id="1")
    assert len(items) == 1
    assert items[0].media_type == "collection"


def test_enumerate_collections_extracts_tmdb_guid():
    """Plex serves a collection's TMDB-collection ID under
    `tmdb://`. ThemerrDB's `movie_collections/themoviedb/{id}.json`
    path uses the same space, so a direct match enables Phase 3's
    sync to find a theme for the collection."""
    text = (
        '{"MediaContainer": {"size": 1, "totalSize": 1, "Metadata": ['
        '{"ratingKey": "501", "type": "collection", '
        ' "title": "Spider-Man Collection", '
        ' "Guid": [{"id": "tmdb://556"}]}'
        ']}}'
    )

    class _Resp:
        status_code = 200

    resp = _Resp()
    resp.text = text
    with patch.object(PlexClient, "_get", return_value=resp):
        client = PlexClient(_cfg())
        items = client.enumerate_collections_for_section(section_id="1")
    assert items[0].guid_tmdb == 556


def test_enumerate_collections_folder_path_is_empty_string():
    """Collections have no media folder on disk — folder_path
    must be '' (not None). Downstream placement logic gates on
    media_type='collection' OR folder_path to decide hardlink vs
    HTTP-upload; both must agree."""
    text = (
        '{"MediaContainer": {"size": 1, "totalSize": 1, "Metadata": ['
        '{"ratingKey": "501", "type": "collection", '
        ' "title": "X", "Guid": [{"id": "tmdb://1"}]}'
        ']}}'
    )

    class _Resp:
        status_code = 200

    resp = _Resp()
    resp.text = text
    with patch.object(PlexClient, "_get", return_value=resp):
        client = PlexClient(_cfg())
        items = client.enumerate_collections_for_section(section_id="1")
    assert items[0].folder_path == ""


def test_enumerate_collections_has_theme_reflects_plex_theme_attr():
    """If Plex's metadata response carries a `theme` attribute
    (e.g. an upload already exists), has_theme=True so the
    placement diff knows the slot is occupied."""
    text = (
        '{"MediaContainer": {"size": 2, "totalSize": 2, "Metadata": ['
        '{"ratingKey": "501", "type": "collection", "title": "A", '
        ' "theme": "/library/metadata/501/theme", '
        ' "Guid": [{"id": "tmdb://1"}]},'
        '{"ratingKey": "502", "type": "collection", "title": "B", '
        ' "Guid": [{"id": "tmdb://2"}]}'
        ']}}'
    )

    class _Resp:
        status_code = 200

    resp = _Resp()
    resp.text = text
    with patch.object(PlexClient, "_get", return_value=resp):
        client = PlexClient(_cfg())
        items = client.enumerate_collections_for_section(section_id="1")
    by_rk = {it.rating_key: it for it in items}
    assert by_rk["501"].has_theme is True
    assert by_rk["501"].plex_theme_uri == "/library/metadata/501/theme"
    assert by_rk["502"].has_theme is False
    assert by_rk["502"].plex_theme_uri == ""


def test_enumerate_collections_uses_collections_endpoint():
    """The GET URL must be `/library/sections/{id}/collections`
    (NOT `/all`) — the `/all` endpoint excludes collection
    metadata items even with type=18 hints."""
    text = '{"MediaContainer": {"size": 0, "totalSize": 0, "Metadata": []}}'

    class _Resp:
        status_code = 200

    resp = _Resp()
    resp.text = text
    captured = {}

    def _capture(path, params=None, timeout=None):
        captured["path"] = path
        captured["params"] = params
        return resp

    with patch.object(PlexClient, "_get", side_effect=_capture):
        client = PlexClient(_cfg())
        client.enumerate_collections_for_section(section_id="42")
    assert captured["path"] == "/library/sections/42/collections"


def test_enumerate_collections_raises_on_http_failure():
    """Same contract as enumerate_section_items: non-200 raises
    PlexParseError so the plex_enum caller's try/except routes
    through identically (no silent `[]` return — Class-9)."""
    class _Resp:
        status_code = 500
        text = "boom"

    with patch.object(PlexClient, "_get", return_value=_Resp()):
        client = PlexClient(_cfg())
        try:
            client.enumerate_collections_for_section(section_id="1")
        except PlexParseError:
            pass
        else:
            raise AssertionError(
                "enumerate_collections_for_section must raise "
                "PlexParseError on HTTP failure — class-9 hygiene."
            )


def test_enumerate_collections_raises_on_json_parse_failure():
    """Malformed JSON must raise PlexParseError (not silent `[]`)."""
    class _Resp:
        status_code = 200
        text = "not json {"

    with patch.object(PlexClient, "_get", return_value=_Resp()):
        client = PlexClient(_cfg())
        try:
            client.enumerate_collections_for_section(section_id="1")
        except PlexParseError:
            pass
        else:
            raise AssertionError(
                "enumerate_collections_for_section must raise "
                "PlexParseError on JSON parse failure."
            )


# ── upload_collection_theme ───────────────────────────────────


def test_upload_collection_theme_hits_correct_url():
    """POST must land on /library/metadata/{rk}/themes — the
    exact path themerr-plex used. Audited from python-plexapi's
    Collection.uploadTheme() and the Plex OpenAPI.

    v1.18.9: the upload now tries multipart/form-data first
    (matches python-plexapi's `files={'file': fp}` shape) then
    falls back to raw-body POST on 4xx. The URL is constant
    across both attempts."""
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.text = ""

    client = PlexClient(_cfg())
    try:
        with patch.object(client._client, "post", return_value=mock_resp) as mp:
            ok, _status, _body = client.upload_collection_theme(
                rating_key="501", audio_bytes=b"\xff\xfb\x90fakebytes",
            )
        # v1.18.68: upload_collection_theme returns (ok, status, body).
        assert ok is True
        # First call is multipart; check the URL.
        call = mp.call_args_list[0]
        assert call.args[0] == "/library/metadata/501/themes"
        # The multipart upload uses `files={'file': (name, bytes,
        # content_type)}` — body bytes are inside the tuple.
        files = call.kwargs.get("files", {})
        assert "file" in files, (
            "v1.18.9: multipart attempt must pass files={'file': ...}"
        )
        # Tuple shape: (filename, bytes, content_type)
        file_tuple = files["file"]
        assert file_tuple[1] == b"\xff\xfb\x90fakebytes", (
            "v1.18.9: body bytes must be passed through unchanged"
        )
    finally:
        client.close()


def test_upload_collection_theme_sends_content_type():
    """Content-Type must be set to audio/mpeg by default so Plex
    knows how to ingest the body.

    v1.18.9: in the multipart shape, the file's MIME type lives
    in the files-tuple, not the request headers (httpx generates
    a multipart/form-data Content-Type for the request itself
    with the proper boundary). Pin the audio/mpeg in the file
    tuple."""
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.text = ""

    client = PlexClient(_cfg())
    try:
        with patch.object(client._client, "post", return_value=mock_resp) as mp:
            client.upload_collection_theme(
                rating_key="501", audio_bytes=b"x",
            )
        # First (multipart) attempt — check the file-tuple's
        # content_type.
        call = mp.call_args_list[0]
        files = call.kwargs.get("files", {})
        file_tuple = files["file"]
        assert file_tuple[2] == "audio/mpeg", (
            "v1.18.9: multipart file tuple's third element must "
            "be the content_type (audio/mpeg default)"
        )
    finally:
        client.close()


def test_upload_collection_theme_returns_false_on_non_2xx():
    """Non-2xx must return False (not raise) — same fire-and-
    forget contract as other PlexClient mutating methods."""
    mock_resp = MagicMock()
    mock_resp.status_code = 500
    mock_resp.text = "server error"

    client = PlexClient(_cfg())
    try:
        with patch.object(client._client, "post", return_value=mock_resp):
            ok, _status, _body = client.upload_collection_theme(
                rating_key="501", audio_bytes=b"x",
            )
        # v1.18.68: upload_collection_theme returns 3-tuple.
        assert ok is False
    finally:
        client.close()


def test_upload_collection_theme_logs_on_failure():
    """Class-9 hygiene: the non-2xx branch must log a breadcrumb,
    not silent-return. Verifies a log call is emitted.

    v1.18.9: success/failure status logs now land at log.info
    (more verbose by default so the operator sees the upload's
    HTTP status without enabling debug). Exception paths still
    log at log.warning. This test checks that SOME log was
    emitted on the non-2xx path — either info (for status
    logging) or warning (for the fallback's exception path)."""
    mock_resp = MagicMock()
    mock_resp.status_code = 500
    mock_resp.text = "server error"

    client = PlexClient(_cfg())
    try:
        with patch.object(client._client, "post", return_value=mock_resp), \
             patch("app.core.plex.log") as mock_log:
            client.upload_collection_theme(
                rating_key="501", audio_bytes=b"x",
            )
        # The new implementation logs status at info (success or
        # failure). The 500 path lands here twice — once for
        # multipart attempt, once for raw-body fallback.
        assert mock_log.info.called, (
            "v1.18.9: upload_collection_theme must log status "
            "— Class-9 silent-defensive-catch hygiene."
        )
    finally:
        client.close()


def test_upload_collection_theme_catches_exception_and_logs():
    """If httpx raises (network down), the method must catch +
    log + return False (not propagate). Class-9 hygiene."""
    client = PlexClient(_cfg())
    try:
        with patch.object(client._client, "post",
                          side_effect=RuntimeError("net down")), \
             patch("app.core.plex.log") as mock_log:
            ok, _status, _body = client.upload_collection_theme(
                rating_key="501", audio_bytes=b"x",
            )
        # v1.18.68: upload_collection_theme returns 3-tuple.
        assert ok is False
        assert mock_log.warning.called
    finally:
        client.close()


# ── delete_collection_theme ───────────────────────────────────


def test_delete_collection_theme_hits_correct_url():
    """DELETE must land on /library/metadata/{rk}/theme (SINGULAR).

    v1.18.36 flipped the URL: pre-fix this asserted PLURAL
    /themes, which Plex's router has no DELETE handler for —
    every call returned 404 silently. v1.18.31-35 probe series
    confirmed the SINGULAR /theme endpoint is the working
    documented Plex API for clearing the active theme
    association (python-plexapi's deleteTheme uses this URL).

    Latent bug fix: every collection UNPLACE/PURGE since v1.18.0
    has been a Plex-side no-op until v1.18.36."""
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.text = ""

    client = PlexClient(_cfg())
    try:
        with patch.object(client._client, "delete", return_value=mock_resp) as md:
            ok = client.delete_collection_theme(rating_key="501")
        assert ok is True
        assert md.call_args.args[0] == "/library/metadata/501/theme"
    finally:
        client.close()


def test_delete_collection_theme_returns_false_on_non_2xx():
    mock_resp = MagicMock()
    mock_resp.status_code = 404
    mock_resp.text = "not found"

    client = PlexClient(_cfg())
    try:
        with patch.object(client._client, "delete", return_value=mock_resp):
            ok = client.delete_collection_theme(rating_key="501")
        assert ok is False
    finally:
        client.close()


def test_delete_collection_theme_catches_exception_and_logs():
    """Class-9 hygiene: network errors caught + logged + False."""
    client = PlexClient(_cfg())
    try:
        with patch.object(client._client, "delete",
                          side_effect=RuntimeError("net down")), \
             patch("app.core.plex.log") as mock_log:
            ok = client.delete_collection_theme(rating_key="501")
        assert ok is False
        assert mock_log.warning.called
    finally:
        client.close()


# ── Source-level pins ─────────────────────────────────────────


def test_collection_methods_present_on_plex_client():
    """Source-level guard: the three new methods exist on the
    class. Catches accidental deletion / rename during refactors."""
    assert hasattr(PlexClient, "enumerate_collections_for_section")
    assert hasattr(PlexClient, "upload_collection_theme")
    assert hasattr(PlexClient, "delete_collection_theme")


def test_collection_methods_use_themes_endpoint_not_theme():
    """Plex's binary-upload endpoint is `/themes` (plural,
    the collection of attached themes) not `/theme` (singular,
    the GET URI for the currently-served file). themerr-plex's
    audited source used `/themes` for both POST and DELETE."""
    src = (REPO / "app" / "core" / "plex.py").read_text()
    # The new methods must reference /themes (with the trailing s)
    # and use rating_key path interpolation.
    assert '/library/metadata/{rating_key}/themes' in src, (
        "v1.18.0: collection upload/delete must POST/DELETE to "
        "/library/metadata/{rk}/themes (plural)."
    )
