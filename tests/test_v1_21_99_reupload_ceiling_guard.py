"""v1.21.99 — LPS re-upload restore skips the doomed POST over Plex's ceiling.

the user: LET PLEX SERVE on Watchmen Theatrical Cut (15.3MB theme) "did nothing".
The unplace unlinked the sidecar fine, but the Plex-side restore — which
re-uploads the fallback theme's bytes to re-select it (the v1.18.36 trick) —
POSTed 15MB and hit Plex's ~10MB theme-upload ceiling (HTTP 500), so the
restore "failed" (api_restored: 0) with a scary upload-500 in the logs.

A >10MB fallback can't be re-selected this way. For a SIDECAR LPS that's fine:
removing the theme.mp3 already lets Plex's Local Media Assets fall back to its
own (cloud/agent) selected theme — no re-select needed. So _fetch_and_reupload_
theme now checks the fetched size and SKIPS the doomed POST, returning
step_failed='over_ceiling' (the caller logs it at INFO as "Plex's own theme
serves via auto-fallback", not a 500).
"""
from __future__ import annotations

from unittest.mock import MagicMock

from app.core import plex as plex_mod
from app.core.plex import PlexConfig


def _stub(get_response, upload_ok=True):
    cfg = PlexConfig(url="http://plex.test:32400", token="t",
                     movie_section="1", tv_section="2", enabled=True)
    client = plex_mod.PlexClient(cfg)
    fake = MagicMock()
    fake.get = MagicMock(return_value=get_response)
    client._client = fake
    _status = 200 if upload_ok else 500
    client.upload_collection_theme = MagicMock(
        return_value=(upload_ok, _status, "(empty)" if upload_ok else "err"))
    return client


def _resp(content):
    r = MagicMock()
    r.status_code = 200
    r.content = content
    return r


def test_reupload_skips_post_over_ceiling():
    client = _stub(_resp(bytes(11 * 1024 * 1024)))  # 11MB > ~10MB ceiling
    out = client._fetch_and_reupload_theme(
        item_rating_key="417795",
        theme_rating_key="metadata://themes/199d63a4")
    assert out["ok"] is False
    assert out["step_failed"] == "over_ceiling"
    assert out["fetch"]["bytes"] == 11 * 1024 * 1024
    assert out["upload"] is None
    # The doomed 11MB POST must NOT have been attempted.
    client.upload_collection_theme.assert_not_called()


def test_reupload_under_ceiling_still_posts():
    client = _stub(_resp(bytes(2 * 1024 * 1024)))  # 2MB, under the ceiling
    out = client._fetch_and_reupload_theme(
        item_rating_key="x", theme_rating_key="metadata://themes/abc")
    assert out["ok"] is True
    client.upload_collection_theme.assert_called_once()


def test_set_active_returns_false_and_no_post_over_ceiling():
    client = _stub(_resp(bytes(15310172)))  # the user's exact Watchmen size
    ok = client.set_active_theme_via_reupload(
        rating_key="417795", theme_rating_key="metadata://themes/199d63a4")
    assert ok is False
    client.upload_collection_theme.assert_not_called()


def test_v1_21_99_version_pin():
    from pathlib import Path
    init_py = (Path(__file__).resolve().parent.parent
               / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
