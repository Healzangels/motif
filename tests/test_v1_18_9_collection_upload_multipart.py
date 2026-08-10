"""v1.18.9 — collection theme upload diagnostics + multipart + force.

the user reported: "the download is happening but the push to plex
stalls and ends up just a flashing amber PL but never links to
plex properly."

PL=amber means the row's local_files exists but no placements
row — the download landed, the place job ran, but didn't write
the placement. Three coordinated bugs:

  1. **`skip_if_plex_has_theme` fires silently on collections.**
     The v1.18.0 `_do_place_collection` mirrored `_do_place`'s
     skip gate: if `pi.has_theme=1` and not `force_overwrite`,
     skip the upload and return. For movies/shows that protects
     themerr-plex's cloud embeds (the user opted into "Plex
     serves its own"). For collections there's no equivalent
     concept — any user-initiated SET URL / UPLOAD MP3 /
     DOWNLOAD TDB flow on a collection clearly intends for
     motif to push its theme. Pre-fix the user's symptom: download
     succeeded, place job fired, `cached_has_theme=1` (Plex
     had some legacy theme attribute), the skip gate fired
     silently, no placement row written, PL stayed at await
     forever.

  2. **Upload uses raw-body POST.** python-plexapi's
     `Collection.uploadTheme(filepath=...)` uses multipart/form-
     data with a `file` field. Some Plex builds reject raw-body
     uploads as 400 because they expect the multipart wrapper.
     v1.18.0's raw-body POST might have been working for some
     installs and silently failing for others.

  3. **No diagnostic logging.** When the upload failed, the
     log line was `"upload_collection_theme: rk=X HTTP 500 — ..."`
     — no signal about which attempt fired, which method, body
     size, or wall-clock time. the user couldn't tell whether the
     upload was hanging, retrying, succeeding-but-not-persisting,
     or skipping.

Fixes:

  * `_do_place_collection` defaults `force_overwrite=True` for
    media_type='collection'. Payload `force=true` still wins;
    other paths inherit the collection-friendly default. The
    skip-if-plex-has-theme branch can no longer fire on
    standard collection flows.

  * `upload_collection_theme` tries multipart first (matches
    python-plexapi's documented filepath shape), falls back to
    raw-body POST on a 4xx. Body size + URL + response status
    + truncated body + total wall-clock duration log at INFO
    for every attempt so the operator can diagnose without
    enabling debug logging.

  * `_do_place_collection` logs its resolved state tuple
    (cached_rk, cached_has_theme, force_overwrite,
    local_present) BEFORE deciding skip vs upload. Pairs with
    the new upload-side logs to give a full timeline of what
    the worker saw + decided + sent + got back.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

PLEX_PY = REPO / "app" / "core" / "plex.py"
WORKER_PY = REPO / "app" / "core" / "worker.py"

from app.core.plex import PlexClient, PlexConfig  # noqa: E402


def _cfg() -> PlexConfig:
    return PlexConfig(url="http://x", token="t",
                      movie_section="1", tv_section="2")


# ── Multipart upload behavior ─────────────────────────────────


def test_upload_collection_theme_tries_multipart_first():
    """First POST attempt must be multipart/form-data — pass
    `files={'file': (...)}` to httpx (which generates the
    multipart boundary + Content-Type header). The python-plexapi
    Collection.uploadTheme() reference uses this shape."""
    mock_ok = MagicMock()
    mock_ok.status_code = 200
    mock_ok.text = ""
    client = PlexClient(_cfg())
    try:
        with patch.object(client._client, "post",
                          return_value=mock_ok) as mp:
            client.upload_collection_theme(
                rating_key="501", audio_bytes=b"data",
            )
        first_call = mp.call_args_list[0]
        # Multipart: `files=` kwarg present, `content=` absent.
        assert "files" in first_call.kwargs
        assert "content" not in first_call.kwargs, (
            "v1.18.9: first attempt must be multipart, NOT raw "
            "body — multipart matches python-plexapi's shape "
            "and is the form Plex's web UI uses"
        )
    finally:
        client.close()


def test_upload_collection_theme_falls_back_to_raw_body_on_4xx():
    """If the multipart attempt returns 4xx, the helper must
    fall back to raw-body POST. Some older Plex builds accept
    raw body but reject multipart with a 400."""
    mock_4xx = MagicMock()
    mock_4xx.status_code = 400
    mock_4xx.text = "Bad Request"
    mock_ok = MagicMock()
    mock_ok.status_code = 200
    mock_ok.text = ""
    client = PlexClient(_cfg())
    try:
        with patch.object(
            client._client, "post",
            side_effect=[mock_4xx, mock_ok],
        ) as mp:
            ok, _status, _body = client.upload_collection_theme(
                rating_key="501", audio_bytes=b"data",
            )
        # v1.18.68: upload_collection_theme returns 3-tuple.
        assert ok is True, (
            "v1.18.9: helper must return True when the raw-body "
            "fallback succeeds even after multipart 4xx"
        )
        assert mp.call_count == 2, (
            "v1.18.9: helper must make exactly two POST attempts "
            "(multipart then raw-body)"
        )
        # Second call uses content= (raw body), not files=.
        second_call = mp.call_args_list[1]
        assert "content" in second_call.kwargs
        assert second_call.kwargs.get("content") == b"data"
    finally:
        client.close()


def test_upload_collection_theme_logs_url_body_size_and_status():
    """Every attempt must emit log lines with the URL, body size,
    HTTP status, and wall-clock time so the operator can diagnose
    failures without enabling debug logging."""
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.text = "ok"
    client = PlexClient(_cfg())
    try:
        with patch.object(client._client, "post",
                          return_value=mock_resp), \
             patch("app.core.plex.log") as mock_log:
            client.upload_collection_theme(
                rating_key="501",
                audio_bytes=b"some-mp3-bytes-go-here",
            )
        # Concatenate all info-level log messages and search.
        all_msgs = " ".join(
            str(c) for c in mock_log.info.call_args_list
        )
        assert "/library/metadata/501/themes" in all_msgs, (
            "v1.18.9: upload log must include the URL"
        )
        # Body size (22 bytes here) should appear in the log.
        assert "22" in all_msgs, (
            "v1.18.9: upload log must include the body byte count"
        )
        # HTTP status code.
        assert "200" in all_msgs, (
            "v1.18.9: upload log must include the response status"
        )
    finally:
        client.close()


def test_upload_collection_theme_exception_fallback_to_raw():
    """Network exception on the multipart attempt must trigger
    the raw-body fallback — not return False immediately."""
    mock_ok = MagicMock()
    mock_ok.status_code = 200
    mock_ok.text = ""
    client = PlexClient(_cfg())
    try:
        with patch.object(
            client._client, "post",
            side_effect=[RuntimeError("multipart broke"), mock_ok],
        ) as mp:
            ok, _status, _body = client.upload_collection_theme(
                rating_key="501", audio_bytes=b"data",
            )
        # v1.18.68: upload_collection_theme returns 3-tuple.
        assert ok is True
        assert mp.call_count == 2, (
            "v1.18.9: multipart exception must trigger raw-body "
            "fallback, not give up immediately"
        )
    finally:
        client.close()


# ── _do_place_collection default-force-overwrite ──────────────


def test_do_place_collection_default_force_overwrite_false():
    """v1.18.10 reverted v1.18.9's default-force-overwrite for
    collections. the user's preference: "treated same as movies as
    if plex has a theme that's best case." Plex-served themes
    (themerr-plex cloud embeds, Plex Pass) should be respected
    the same way they are for movies/shows. The associated
    "PL=await flashing amber" symptom is fixed in v1.18.10 by
    stamping plex_independent_theme=1 on the skip path so the
    row renders as PS state instead of nagging !P attention."""
    src = WORKER_PY.read_text()
    fn_start = src.index("def _do_place_collection(")
    fn_end = src.index("\n    def _do_refresh(", fn_start + 1)
    body = src[fn_start:fn_end]
    # The default initializer must be `force_overwrite = False`
    # — same as the movie/show path. Payload force=true still
    # wins via the explicit branch below.
    assert "force_overwrite = False" in body, (
        "v1.18.10: _do_place_collection must default "
        "force_overwrite=False so Plex-served themes are "
        "respected — same skip semantic as movies/shows"
    )


def test_do_place_collection_payload_force_still_wins():
    """The payload `force=true` (e.g. from /pending APPROVE) must
    still set force_overwrite=True. The collection-default just
    means OTHER paths also get the True default — payload
    explicit values don't get overwritten."""
    src = WORKER_PY.read_text()
    fn_start = src.index("def _do_place_collection(")
    fn_end = src.index("\n    def _do_refresh(", fn_start + 1)
    body = src[fn_start:fn_end]
    # Pin that the payload-force branch is still present + still
    # sets force_overwrite=True (not False).
    assert 'payload.get("force")' in body
    # The if-block sets force_overwrite=True.
    idx = body.index('payload.get("force")')
    nearby = body[idx:idx + 200]
    assert "force_overwrite = True" in nearby


# ── State-log at the top of the helper ────────────────────────


def test_do_place_collection_logs_resolved_state():
    """Every _do_place_collection invocation must log its
    resolved state tuple before deciding skip vs upload. Pairs
    with the upload-side logging to give a full timeline."""
    src = WORKER_PY.read_text()
    fn_start = src.index("def _do_place_collection(")
    fn_end = src.index("\n    def _do_refresh(", fn_start + 1)
    body = src[fn_start:fn_end]
    # Pin the diagnostic log line. v0.51.253: the leading field is `job=`
    # — it was mislabeled `rk=` while carrying job["id"], which read as a
    # second rating key next to cached_rk during the dead-rk incident.
    assert "_do_place_collection: job=%s" in body, (
        "v1.18.9: helper must log its resolved state tuple"
    )
    assert "_do_place_collection: rk=%s" not in body, (
        "v0.51.253: the job id must not be labeled rk="
    )
    # Required fields in the log: cached_rk, cached_has_theme,
    # force_overwrite, local-presence.
    assert "cached_rk=" in body
    assert "cached_has_theme=" in body
    assert "force_overwrite=" in body
    assert "local_present=" in body
