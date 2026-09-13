"""v0.51.322 — preview the theme Plex serves on a P row.

A same-origin proxy for the singular /library/metadata/{rk}/theme association
(the art proxy's posture: token server-side, 204 cacheable on no-theme, 204
no-store on failure, .mp3 spelling) with single-range 206 support for
Safari's <audio>; the INFO card and the bare card gain a "plex theme" row
with preload="none".
"""
from __future__ import annotations

import logging
from pathlib import Path

import httpx
import pytest

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
AUTH = {"X-Authentik-Username": "testadmin"}


@pytest.fixture
def client(tmp_path, monkeypatch):
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web.api import create_app
    from fastapi.testclient import TestClient
    (tmp_path / "data").mkdir()
    (tmp_path / "motif.yaml").write_text("paths: {}\n")
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path); init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    return TestClient(create_app(s)), s


class _Resp:
    def __init__(self, status, content=b"", ctype="audio/mpeg"):
        self.status_code = status; self.content = content
        self.headers = {"content-type": ctype} if ctype else {}


def _fake_plex(monkeypatch, s, handler):
    """Point settings at a Plex and replace httpx.Client with a recorder."""
    monkeypatch.setattr(type(s), "plex_url", property(lambda self: "http://plex.local:32400"))
    monkeypatch.setattr(type(s), "plex_token", property(lambda self: "tok-secret"))
    calls = []

    class _C:
        def __init__(self, *a, **kw):
            calls.append({"ctor": kw})

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def get(self, url, headers=None, **kw):
            calls.append({"url": url, "headers": headers or {}})
            return handler(url)
    monkeypatch.setattr(httpx, "Client", _C)
    return calls


def test_no_plex_configured_is_a_cacheable_204(client):
    c, s = client
    r = c.get("/api/plex/theme/123.mp3", headers=AUTH)
    assert r.status_code == 204 and "max-age" in r.headers["cache-control"]


def test_bad_rating_key_is_400_and_auth_is_required(client):
    c, s = client
    assert c.get("/api/plex/theme/abc.mp3", headers=AUTH).status_code == 400
    assert c.get("/api/plex/theme/123.mp3").status_code in (401, 403), "auth-gated like every /api endpoint"


def test_streams_plex_theme_with_token_in_header_not_url(client, monkeypatch):
    c, s = client
    body = b"ID3" + bytes(range(256)) * 4
    calls = _fake_plex(monkeypatch, s, lambda url: _Resp(200, body, "audio/mpeg; charset=binary"))
    r = c.get("/api/plex/theme/777.mp3", headers=AUTH)
    assert r.status_code == 200 and r.content == body
    assert r.headers["content-type"].startswith("audio/mpeg") and r.headers["accept-ranges"] == "bytes"
    assert r.headers["cache-control"] == "private, max-age=300"
    get = [x for x in calls if "url" in x][0]
    assert get["url"] == "http://plex.local:32400/library/metadata/777/theme"
    assert get["headers"]["X-Plex-Token"] == "tok-secret" and "tok-secret" not in get["url"]
    assert calls[0]["ctor"].get("follow_redirects") is False


def test_single_byte_range_returns_206(client, monkeypatch):
    c, s = client
    body = bytes(range(256)) * 10  # 2560 bytes
    _fake_plex(monkeypatch, s, lambda url: _Resp(200, body))
    r = c.get("/api/plex/theme/777.mp3", headers={**AUTH, "Range": "bytes=0-99"})
    assert r.status_code == 206 and r.content == body[:100]
    assert r.headers["content-range"] == "bytes 0-99/2560" and r.headers["content-length"] == "100"
    r2 = c.get("/api/plex/theme/777.mp3", headers={**AUTH, "Range": "bytes=2500-"})
    assert r2.status_code == 206 and r2.content == body[2500:] and r2.headers["content-range"] == "bytes 2500-2559/2560"
    r3 = c.get("/api/plex/theme/777.mp3", headers={**AUTH, "Range": "bytes=-60"})
    assert r3.status_code == 206 and r3.content == body[-60:]
    r4 = c.get("/api/plex/theme/777.mp3", headers={**AUTH, "Range": "bytes=9999-"})
    assert r4.status_code == 416 and r4.headers["content-range"] == "bytes */2560"


def test_plex_404_is_cacheable_no_theme_but_failures_are_no_store(client, monkeypatch, caplog):
    c, s = client
    _fake_plex(monkeypatch, s, lambda url: _Resp(404, b"", None))
    r = c.get("/api/plex/theme/777.mp3", headers=AUTH)
    assert r.status_code == 204 and "max-age=300" in r.headers["cache-control"]
    import app.web.api as apimod
    apimod._PLEX_THEME_FETCH_WARNED = False
    _fake_plex(monkeypatch, s, lambda url: _Resp(500, b"x"))
    with caplog.at_level(logging.DEBUG, logger="motif"):
        r = c.get("/api/plex/theme/777.mp3", headers=AUTH)
        assert r.status_code == 204 and r.headers["cache-control"] == "no-store"
        r = c.get("/api/plex/theme/777.mp3", headers=AUTH)
    warns = [x for x in caplog.records if x.levelno == logging.WARNING and "plex theme proxy" in x.getMessage()]
    assert len(warns) == 1, "class-9 hot path: the first failure warns, the repeat drops to debug"
    _fake_plex(monkeypatch, s, lambda url: _Resp(200, b""))
    assert c.get("/api/plex/theme/777.mp3", headers=AUTH).headers["cache-control"] == "no-store", "an empty 200 is a failure"
    _fake_plex(monkeypatch, s, lambda url: _Resp(200, b"x" * (30 * 1024 * 1024 + 1)))
    assert c.get("/api/plex/theme/777.mp3", headers=AUTH).status_code == 204, "oversize is refused, not proxied"


def test_non_audio_content_type_falls_back_to_mpeg(client, monkeypatch):
    c, s = client
    _fake_plex(monkeypatch, s, lambda url: _Resp(200, b"abc", "application/octet-stream"))
    r = c.get("/api/plex/theme/777.mp3", headers=AUTH)
    assert r.status_code == 200 and r.headers["content-type"].startswith("audio/mpeg")


# ── card pins ────────────────────────────────────────────────


def _blk(anchor: str, end: str) -> str:
    i = APP_JS.index(anchor)
    return APP_JS[i:APP_JS.index(end, i)]


def test_full_card_plex_theme_row_gate_and_placement():
    b = _blk("const plexThemeBlock = (data.plex_has_theme === 1 && _plexRk && (!lf || _plexSrc === 'P'))", "      : '';")
    assert 'preload="none"' in b, "nothing is fetched until play (IDS-friendly)"
    assert "/api/plex/theme/${encodeURIComponent(_plexRk)}.mp3" in b and 'data-plex-theme="1"' in b
    assert '<dt class="info-ctl-label info-ctl-label-play">plex serves' in b and '<dd class="info-play-row"><audio' in b, (
        "the existing play-row primitive (v0.51.340: the badge rides the <dt>)")
    assert "const _onDiskRows = _ambiguousCut ? '' : `" in APP_JS, "the v0.51.223 ambiguous-cut contract is untouched"
    # v0.51.323: the players live in the AUDIO group, Plex's row first.
    assert "const _audioRows = _ambiguousCut ? '' : `" in APP_JS
    assert "${plexThemeBlock}\n        ${audioBlock}`;" in APP_JS


def test_bare_card_plex_theme_row_is_pure_and_bound():
    bare = _blk("function renderBareInfoCard(it, { anime = false } = {}) {", "\n  // v0.50.64: open the bare card")
    assert "${it.plex_has_theme" in bare and 'data-plex-theme="1"' in bare and 'preload="none"' in bare
    assert "libraryState" not in bare
    assert APP_JS.count("_bindPlexThemePlayer(body);") == 2, "the full card and the bare card"
    b = _blk("function _bindPlexThemePlayer(body) {", "\n  }")
    assert "addEventListener('error'" in b and "did not play" in b and "a.remove();" in b


def test_v0_51_322_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert "0.51.322: " in init_py
