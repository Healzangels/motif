"""v0.51.285 — dashboard carousel de-hitch: rAF scroll + transcoded posters.

The user: the auto-scroll "hitches every few seconds". Two independent causes:

  1. The scroll loop ran on setInterval(30ms) — not frame-aligned. Under load
     Chrome coalesces timers, landing two ticks inside one 60Hz frame and none
     in the next: the strip advanced in 2px/0px bursts. Now a
     requestAnimationFrame loop scaled by the measured frame gap — the same
     33.3px/s, one evenly-sized step per painted frame.
  2. /api/plex/art served the FULL-RES Plex poster into 150px tiles. Every
     tile scrolling into view rasterized a multi-megapixel bitmap — one new
     tile every ~5s at scroll speed, which IS the reported beat. The proxy
     gains opt-in ?w= (routed through Plex's photo transcoder, full-thumb
     fallback); only the carousel passes w=300. INFO-card heroes keep the
     full-res URL.
"""
from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from _slice_helpers import slice_to_next
from app.core.db import init_db

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()

AUTH = {"X-Authentik-Username": "testadmin"}


# ── the scroll loop (source pins, structurally anchored) ─────────────────────

def _autoscroll_fn() -> str:
    return slice_to_next(
        APP_JS,
        "function _setupCarouselAutoScroll()",
        "\n  function ", "\n  async function ")


def test_autoscroll_runs_on_raf_not_a_timer():
    fn = _autoscroll_fn()
    assert fn.count("requestAnimationFrame(tick)") == 2, (
        "the carousel must advance once per painted frame (rAF), not on a "
        "wall-clock timer — and BOTH arms matter: start() kicks the loop off, "
        "tick() re-arms itself each frame (dropping the re-arm advances one "
        "frame and silently stops)"
    )
    assert "setInterval(" not in fn, (
        "the setInterval scroll loop is the hitch — no wall-clock timer may "
        "come back anywhere in the auto-scroll function (the word in a "
        "comment is fine; a call is not)"
    )


def test_step_scales_by_the_measured_frame_gap():
    fn = _autoscroll_fn()
    assert "dt * SPEED_PX_PER_MS" in fn, (
        "the per-frame step must scale by elapsed time so speed stays "
        "33.3px/s at any refresh rate — a fixed +1px/frame would double the "
        "speed at 60fps vs the old 30ms timer"
    )
    assert "Math.min(ts - lastTs, 100)" in fn, (
        "the frame gap must be clamped — resuming after a throttled stretch "
        "(hidden tab, long-held dialog) must step once, not teleport the "
        "strip by the whole pause"
    )


def test_all_four_freeze_guards_survive_the_rewrite():
    fn = _autoscroll_fn()
    for guard in ("document.hidden", "!document.hasFocus()",
                  "strip.matches(':hover')", "dialog[open]"):
        assert guard in fn, f"the rewrite must keep the freeze guard: {guard}"


def test_end_dwell_survives_the_rewrite():
    fn = _autoscroll_fn()
    assert "_carouselEndHold = now + 3000" in fn, (
        "the v0.51.113 3s end-of-strip dwell must survive the rAF rewrite"
    )


def test_stop_cancels_the_raf():
    fn = _autoscroll_fn()
    assert "cancelAnimationFrame(rafId)" in fn, (
        "toggling auto-scroll OFF must cancel the rAF loop — otherwise the "
        "tick re-arms itself forever and burns a frame callback per paint"
    )


def test_carousel_requests_the_300px_transcode():
    fn = slice_to_next(
        APP_JS,
        "async function loadRecentlyAdded()",
        "\n  function ", "\n  async function ")
    assert "/api/plex/art/${encodeURIComponent(rk)}?w=300`" in fn, (
        "the carousel tiles paint at 150 CSS px (300 device px @2x) — they "
        "must request the ?w=300 transcode, not the full-res poster whose "
        "rasterize spike on scroll-in was the reported hitch"
    )


# ── the art proxy ?w= behavior (real endpoint, faked Plex) ───────────────────

@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.web.api import create_app
    monkeypatch.setattr(Settings, "plex_url",
                        property(lambda self: "http://plex.test"))
    monkeypatch.setattr(Settings, "plex_token",
                        property(lambda self: "tok-abc"))
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    return TestClient(create_app(s))


class _FakeResp:
    def __init__(self, status, ctype, body):
        self.status_code = status
        self.headers = {"content-type": ctype}
        self.content = body


def _fake_httpx(monkeypatch, handler):
    """Patch httpx.Client with a fake whose .get() delegates to handler(url)
    and records every call. Returns the calls list."""
    calls = []

    class _FakeClient:
        def __init__(self, *a, **kw):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def get(self, url, params=None, headers=None):
            calls.append({"url": url, "params": params, "headers": headers})
            return handler(url)

    import httpx
    monkeypatch.setattr(httpx, "Client", _FakeClient)
    return calls


def test_w_outside_60_1200_is_rejected(client):
    assert client.get("/api/plex/art/123?w=59", headers=AUTH).status_code == 422
    assert client.get("/api/plex/art/123?w=1201", headers=AUTH).status_code == 422
    assert client.get("/api/plex/art/123?w=abc", headers=AUTH).status_code == 422


def test_w_routes_through_the_photo_transcoder(client, monkeypatch):
    calls = _fake_httpx(monkeypatch, lambda url: _FakeResp(
        200, "image/jpeg", b"small-poster"))
    r = client.get("/api/plex/art/123?w=300", headers=AUTH)
    assert r.status_code == 200
    assert r.content == b"small-poster"
    assert r.headers["content-type"].startswith("image/jpeg")
    assert len(calls) == 1
    c = calls[0]
    assert c["url"] == "http://plex.test/photo/:/transcode"
    assert c["params"]["width"] == 300
    assert c["params"]["height"] == 450  # 2:3, the poster aspect the strip renders
    assert c["params"]["url"] == "/library/metadata/123/thumb"
    assert c["headers"]["X-Plex-Token"] == "tok-abc"  # header, never the URL


def test_transcode_decline_falls_back_to_the_full_thumb(client, monkeypatch):
    def handler(url):
        if "transcode" in url:
            return _FakeResp(404, "text/plain", b"nope")
        return _FakeResp(200, "image/png", b"full-poster")
    calls = _fake_httpx(monkeypatch, handler)
    r = client.get("/api/plex/art/123?w=300", headers=AUTH)
    assert r.status_code == 200
    assert r.content == b"full-poster"
    assert [("transcode" in c["url"]) for c in calls] == [True, False], (
        "a transcoder refusal must fall through to the raw thumb fetch — a "
        "PMS without the photo endpoint behaves exactly as before v0.51.285"
    )


def test_transcode_exception_falls_back_to_the_full_thumb(client, monkeypatch):
    def handler(url):
        if "transcode" in url:
            raise OSError("connection refused")
        return _FakeResp(200, "image/png", b"full-poster")
    _fake_httpx(monkeypatch, handler)
    r = client.get("/api/plex/art/123?w=300", headers=AUTH)
    assert r.status_code == 200
    assert r.content == b"full-poster"


def test_no_w_never_touches_the_transcoder(client, monkeypatch):
    """The INFO-card heroes still request the bare URL — full-res must stay
    the no-param behavior."""
    calls = _fake_httpx(monkeypatch, lambda url: _FakeResp(
        200, "image/jpeg", b"orig"))
    r = client.get("/api/plex/art/123", headers=AUTH)
    assert r.status_code == 200
    assert r.content == b"orig"
    assert len(calls) == 1
    assert "transcode" not in calls[0]["url"]
    assert calls[0]["url"].endswith("/library/metadata/123/thumb")


def test_v0_51_285_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
