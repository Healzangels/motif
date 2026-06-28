"""v1.21.18 — active security controls (request-gating).

The ground-up review's two findings that change how a request is handled
(so they ship separately from the v1.21.17 passive batch, with focused
behavioral tests):

  - MP3 upload magic-byte validation: pre-fix any bytes were accepted and
    written as theme.mp3 (format only sniffed later at serve time). Now the
    UPLOAD MP3 endpoint rejects non-audio with 415. Lenient on container
    (MP3/Ogg/FLAC/WAV/AIFF/MP4) so a real theme isn't blocked.

  - Login rate-limiting: the bcrypt /login endpoint had no throttle. Now a
    per-IP rolling-window failed-attempt counter refuses with 429 after
    LOGIN_MAX_FAILURES without checking the password. (Source IP is
    spoofable until the deferred forward-auth item lands; Authentik fronts
    /login in prod regardless — this is defense for the direct-:5309 path.)
"""
from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core.auth import (
    LOGIN_MAX_FAILURES, clear_login_failures, create_admin, init_auth_schema,
    login_rate_limited, record_login_failure, _reset_login_failures_for_test,
)
from app.core.db import init_db
from app.web.api import _looks_like_audio


REPO = Path(__file__).resolve().parent.parent
FWD_HDR = {"X-Authentik-Username": "testadmin"}


def _make_app(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    # themes_dir must be configured or upload-theme 409s before the gate.
    themes = tmp_path / "themes"
    themes.mkdir(exist_ok=True)
    monkeypatch.setenv("MOTIF_THEMES_DIR", str(themes))
    from app.config import Settings
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(settings.db_path)
    init_auth_schema(settings.db_path)
    create_admin(settings.db_path, username="testadmin", password="testpassword")
    from app.web.api import create_app
    return TestClient(create_app(settings)), settings


# ── MP3 magic-byte gate (unit) ───────────────────────────────

@pytest.mark.parametrize("head,ok", [
    (b"ID3\x04\x00\x00\x00", True),       # ID3v2 MP3
    (b"\xff\xfb\x90\x64rest", True),       # MPEG-1 Layer3 frame sync
    (b"\xff\xf3\x00\x00rest", True),       # MPEG-2 sync
    (b"OggS\x00\x02\x00\x00", True),       # Ogg
    (b"fLaC\x00\x00\x00\x22", True),       # FLAC
    (b"RIFF\x24\x08\x00\x00WAVEfmt ", True),  # WAV
    (b"FORM\x00\x00\x00\x00AIFF", True),   # AIFF
    (b"\x00\x00\x00\x20ftypM4A ", True),   # MP4/M4A
    (b"<!DOCTYPE html><body>", False),     # HTML
    (b"\x89PNG\r\n\x1a\n\x00\x00", False), # PNG
    (b"#!/bin/sh\nrm -rf", False),         # script
    (b"MZ\x90\x00\x03\x00\x00", False),    # PE exe
    (b"", False),
    (b"ab", False),
])
def test_looks_like_audio(head, ok):
    assert _looks_like_audio(head) is ok


def test_upload_theme_rejects_non_audio(tmp_path, monkeypatch):
    client, _ = _make_app(tmp_path, monkeypatch)
    # rating_key 999 won't exist, but the magic-byte gate runs BEFORE the
    # DB lookup — a non-audio body must 415, never reach 404.
    r = client.post(
        "/api/plex_items/999/upload-theme",
        files={"file": ("theme.mp3", b"<html>not audio</html>", "audio/mpeg")},
        headers=FWD_HDR,
    )
    assert r.status_code == 415, r.text


def test_upload_theme_audio_passes_the_gate(tmp_path, monkeypatch):
    client, _ = _make_app(tmp_path, monkeypatch)
    # Valid ID3 header → passes the gate, then 404 on the missing rating_key
    # (proves the gate let it through to the DB lookup, not a 415).
    r = client.post(
        "/api/plex_items/999/upload-theme",
        files={"file": ("theme.mp3", b"ID3\x04\x00\x00\x00\x00\x00ZZmp3data",
                        "audio/mpeg")},
        headers=FWD_HDR,
    )
    assert r.status_code == 404, r.text


# ── login rate-limiting (unit) ───────────────────────────────

def test_rate_limiter_trips_after_max_failures():
    _reset_login_failures_for_test()
    ip = "192.0.2.7"
    assert not login_rate_limited(ip)
    for _ in range(LOGIN_MAX_FAILURES - 1):
        record_login_failure(ip)
    assert not login_rate_limited(ip)          # still under the limit
    record_login_failure(ip)
    assert login_rate_limited(ip)              # at the limit → tripped
    clear_login_failures(ip)
    assert not login_rate_limited(ip)          # success clears it


def test_rate_limiter_ignores_empty_ip():
    _reset_login_failures_for_test()
    for _ in range(LOGIN_MAX_FAILURES + 5):
        record_login_failure("")
    assert not login_rate_limited("")          # never lock out a missing IP


# ── login rate-limiting (behavioral) ─────────────────────────

def test_login_429s_after_repeated_failures(tmp_path, monkeypatch):
    _reset_login_failures_for_test()
    client, _ = _make_app(tmp_path, monkeypatch)
    bad = {"username": "testadmin", "password": "wrong"}
    last = None
    for _ in range(LOGIN_MAX_FAILURES):
        last = client.post("/login", data=bad, follow_redirects=False)
        assert last.status_code == 401, last.status_code
    # next attempt is throttled — 429, even with the CORRECT password
    blocked = client.post(
        "/login",
        data={"username": "testadmin", "password": "testpassword"},
        follow_redirects=False,
    )
    assert blocked.status_code == 429, blocked.status_code


def test_successful_login_clears_failure_count(tmp_path, monkeypatch):
    _reset_login_failures_for_test()
    client, _ = _make_app(tmp_path, monkeypatch)
    # A few failures, then a success, then failures again must not be
    # pre-tripped (success reset the counter).
    for _ in range(LOGIN_MAX_FAILURES - 1):
        client.post("/login", data={"username": "testadmin", "password": "x"},
                    follow_redirects=False)
    ok = client.post(
        "/login",
        data={"username": "testadmin", "password": "testpassword"},
        follow_redirects=False,
    )
    assert ok.status_code in (302, 303), ok.status_code
    # counter cleared → a fresh failure is 401, not 429
    again = client.post("/login", data={"username": "testadmin", "password": "x"},
                        follow_redirects=False)
    assert again.status_code == 401, again.status_code


def test_version_bumped():
    assert '__version__ = "0.' in (REPO / "app" / "__init__.py").read_text()
