"""v0.51.353 — the quarterly floors, and what makes a MAJOR one safe.

yt-dlp 2026.7.4 → 2026.8.19 is the ordinary quarterly bump. apprise 1.12.0 → 2.0.0 is not: v2 is a breaking
release for anyone embedding apprise, and motif embeds it. requirements.txt has never carried an upper bound,
so `pip install -r requirements.txt` in the image resolves to the newest apprise the day it builds — v2 was
already arriving whatever the floor said. The floor is documentation; THIS is the guard.

It drives the real call site (`notify._send_embedded`) against a loopback sink and pins the three things that
file reads out of apprise: the NotifyType / NotifyFormat names it maps onto, the truthiness of `add()`, and
what a falsy notify() means. v2 returns an `AppriseResult` instead of a bool, and its `bool()` keeps v1's
meaning — PARTIAL included, which is the one an embedder would get wrong: a batch where one URL failed must
still report failed, or a dead webhook shows up as "sent" in the events log.
"""
from __future__ import annotations

import base64
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent

# a 1x1 png — apprise attaches the file, the sink only counts bytes
THUMB = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8DwHwAFAAH/q842iQAAAABJRU5ErkJggg=="
)


class _Handler(BaseHTTPRequestHandler):
    """POST /ok answers 200, POST /bad answers 500 — a service that is up, and one that is down."""

    def do_POST(self):
        raw = self.rfile.read(int(self.headers.get("Content-Length") or 0))
        self.server.hits.append(self.path)
        self.server.bodies.append(json.loads(raw or b"{}"))
        self.send_response(500 if self.path.rstrip("/").endswith("bad") else 200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"ok": true}')

    def log_message(self, *a):
        pass


@pytest.fixture
def sink():
    srv = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    srv.hits = []
    srv.bodies = []
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    try:
        yield srv
    finally:
        srv.shutdown()
        srv.server_close()


@pytest.fixture
def urls(sink):
    port = sink.server_address[1]
    return {"ok": f"json://127.0.0.1:{port}/ok", "bad": f"json://127.0.0.1:{port}/bad",
            "malformed": "nonsense://zzz"}


# ── the apprise surface notify.py reads ──────────────────────


def test_apprise_still_publishes_every_name_notify_maps():
    """_send_embedded reads these at dispatch time — a rename is an AttributeError in production, on the
    notification that was trying to tell the operator something had gone wrong."""
    import apprise
    for name in ("INFO", "WARNING", "FAILURE"):
        assert getattr(apprise.NotifyType, name, None) is not None, name
    for name in ("TEXT", "MARKDOWN", "HTML"):
        assert getattr(apprise.NotifyFormat, name, None) is not None, name


def test_add_truthiness_still_separates_a_good_url_from_a_typo(urls):
    """The rejected count is how the TEST UI surfaces a typo'd URL (v1.17.1) — it exists only because add()
    answers falsy for something it can't parse."""
    import apprise
    for label, expected in (("ok", True), ("malformed", False)):
        ap = apprise.Apprise()
        assert bool(ap.add(urls[label])) is expected, label
    assert bool(apprise.Apprise().add("")) is False


# ── the real dispatch, end to end ────────────────────────────


def test_a_reachable_service_reports_one_sent(sink, urls):
    from app.core.notify import _send_embedded
    assert _send_embedded([urls["ok"]], "t", "b") == (1, 0)
    assert sink.hits == ["/ok"], "the probe must actually dispatch, not skip"


def test_a_failing_service_reports_a_failure(sink, urls):
    from app.core.notify import _send_embedded
    assert _send_embedded([urls["bad"]], "t", "b") == (0, 1)


def test_one_dead_url_in_a_batch_still_counts_as_failed(sink, urls):
    """v2's PARTIAL status. v1 returned False here; if bool(AppriseResult) were true for PARTIAL, a batch with
    a dead webhook would be logged as fully sent and the operator would never learn the channel went quiet."""
    from app.core.notify import _send_embedded
    assert _send_embedded([urls["ok"], urls["bad"]], "t", "b") == (0, 2)
    assert sorted(sink.hits) == ["/bad", "/ok"]


def test_an_unparseable_url_rides_the_fail_count(sink, urls):
    from app.core.notify import _send_embedded
    assert _send_embedded([urls["ok"], urls["malformed"]], "t", "b") == (1, 1)


def test_the_severity_motif_asks_for_is_the_severity_that_goes_out(sink, urls):
    """notify.py's type_map is the whole point of threading NotifyType through (v1.17.1: Discord embed colour
    and friends key off it), so read it off the wire rather than trusting the call returned (1, 0)."""
    from app.core.notify import _send_embedded
    for asked, expected in (("info", "info"), ("warning", "warning"), ("failure", "failure")):
        assert _send_embedded([urls["ok"]], "t", "b", asked, "text") == (1, 0), asked
        assert sink.bodies[-1]["type"] == expected, asked


def test_a_markdown_body_and_a_thumbnail_both_ride_along(tmp_path, sink, urls):
    """The per-item callers' shape: a markdown body (v1.17.12) and the v1.22.94 attachment."""
    from app.core.notify import _send_embedded
    thumb = tmp_path / "thumb.png"
    thumb.write_bytes(THUMB)
    assert _send_embedded([urls["ok"]], "t", "**b**", "warning", "markdown") == (1, 0)
    assert sink.bodies[-1]["message"] == "**b**", "the composed body must reach the service unrewritten"
    assert _send_embedded([urls["ok"]], "t", "b", "info", "text", str(thumb)) == (1, 0)
    assert sink.bodies[-1]["attachments"], "the thumbnail must be attached, not silently dropped"


def test_no_urls_dispatches_nothing(sink):
    from app.core.notify import _send_embedded
    assert _send_embedded([], "t", "b") == (0, 0)
    assert sink.hits == []


# ── the yt-dlp side of the same bump ─────────────────────────


@pytest.mark.parametrize("source", ["youtube", "soundcloud", "url"])
def test_yt_dlp_builds_every_opts_shape_motif_passes(tmp_path, source):
    yt_dlp = pytest.importorskip("yt_dlp")
    from app.core.downloader import _opts
    opts = _opts(output_path=tmp_path / "theme.%(ext)s", cookies_file=None, source=source,
                 geo_bypass=True, geo_bypass_country="US", proxy_url="")
    with yt_dlp.YoutubeDL(opts) as ydl:
        assert ydl.params["format"], source
        assert ydl.params["postprocessors"], source


def test_the_node_js_runtime_is_still_supported():
    """yt-dlp DROPS a runtime it no longer knows (a bogus name comes back as {}), so the surviving entry is
    real evidence — not just the dict we handed in. Without node the YouTube path falls back to android_vr,
    which the v1.12.89 comment records as answering "not available" for playable videos."""
    yt_dlp = pytest.importorskip("yt_dlp")
    from app.core.downloader import _opts
    opts = _opts(output_path=Path("/tmp/theme.%(ext)s"), cookies_file=None, source="youtube")
    with yt_dlp.YoutubeDL(opts) as ydl:
        assert ydl.params.get("js_runtimes") == {"node": {}}
        assert set(ydl.params.get("remote_components")) == {"ejs:github"}  # yt-dlp normalises to a set
        assert ydl.params["extractor_args"]["youtube"]["player_client"][0] == "default"


def test_the_probe_path_opts_build_too():
    """probe_youtube_url mirrors the download opts (v1.15.26) but builds its own dict — it has to be checked
    separately or a removed option only shows up as "indeterminate / could be dead" in the INFO card."""
    yt_dlp = pytest.importorskip("yt_dlp")
    opts = {"quiet": True, "noprogress": True, "no_warnings": True, "skip_download": True,
            "socket_timeout": 10, "extract_flat": False, "playlistend": 1,
            "js_runtimes": {"node": {}}, "remote_components": ["ejs:github"],
            "extractor_args": {"youtube": {"player_client": ["default", "android", "ios", "mweb"]}}}
    with yt_dlp.YoutubeDL(opts) as ydl:
        assert ydl.params.get("js_runtimes") == {"node": {}}
