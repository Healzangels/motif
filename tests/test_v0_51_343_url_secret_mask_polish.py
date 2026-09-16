"""v0.51.343: URL-secret mask polish (v0.51.341 review residuals).

  1. The userinfo "@" is looked for before the query/fragment only: a URL with
     an "@" in its query or fragment keeps its host on GET /api/config and in the
     restore preview.
  2. A secret parameter in the fragment (#access_token=) masks like a query one
     and round-trips through PATCH without the mask ever being written.
  3. PATCH matches a masked parameter's name case-blind, as the mask does, and
     keeps the submitted spelling.
"""
from __future__ import annotations

import json
from urllib.parse import urlsplit

import pytest

from app.core import bundle
from app.core.config_file import _is_masked_url_credentials, mask_config_value, mask_url_credentials, unmask_url_credentials

from tests.test_v0_51_341_config_secrets_preview import _H, _api, _bundle, _live


# ── 1. an "@" after "?" or "#" is not userinfo ────────────────────────

@pytest.mark.parametrize("url", [
    "https://host.example/x?email=ops@example.org",
    "https://host.example/x#to=ops@example.org",
    "http://plex.lan:32400/?next=/web@x&page=2",
    "https://host.example?cc=a@b.com#to=c@d.org",
])
def test_an_at_sign_in_the_query_or_fragment_keeps_the_host(url):
    shown = mask_url_credentials(url)
    assert urlsplit(shown).hostname == urlsplit(url).hostname, shown
    assert shown == url, "nothing secret in it — shown whole"
    assert not _is_masked_url_credentials(shown), "PATCH must not read it as a mask"


@pytest.mark.parametrize("url, secrets, shown", [
    ("https://u:ab/cd@host.example/x?email=ops@example.org", ("ab/cd", "u:"), "https://***@host.example/x?email=ops@example.org"),
    ("https://u:p@ss@host.example/x#to=ops@example.org", ("p@ss", "u:"), "https://***@host.example/x#to=ops@example.org"),
    ("user:PW-q@host.example/repo?by=a@b.com", ("PW-q", "user:"), "***@host.example/repo?by=a@b.com"),
])
def test_real_userinfo_still_masks_beside_an_at_sign_in_the_query(url, secrets, shown):
    masked = mask_url_credentials(url)
    assert masked == shown
    assert not any(s in masked for s in secrets), masked
    assert unmask_url_credentials(masked, url) == url


def test_get_config_shows_the_host_and_a_host_edit_saves(tmp_path, monkeypatch):
    client, settings = _api(tmp_path, monkeypatch)
    stored = "https://codeload.example.com/o/r/tar.gz/db?contact=ops@example.org"
    assert client.patch("/api/config", json={"sync": {"database_url": stored}}, headers=_H).status_code == 200
    shown = client.get("/api/config", headers=_H).json()["config"]["sync"]["database_url"]
    assert shown == stored
    edited = shown.replace("codeload.example.com", "mirror.example.net")
    assert client.patch("/api/config", json={"sync": {"database_url": edited}}, headers=_H).status_code == 200
    assert settings.cfg.sync.database_url == edited


def test_the_restore_preview_shows_the_host(tmp_path):
    url = "http://plex.lan:32400/?contact=ops@example.org"
    b = _bundle(tmp_path, f"plex:\n  url: {url}\n")
    _, cd = _live(tmp_path, "plex:\n  url: http://plex.old:32400\n")
    row = {r["key"]: r for r in bundle.preview(b, cd / "motif.yaml")["config_diff"]}["plex.url"]
    assert row["bundle"] == url and row["live"] == "http://plex.old:32400"


# ── 2. fragment secrets ───────────────────────────────────────────────

_FRAGMENT_SHAPES = [
    ("https://host.example/cb#access_token=AT-f1", ("AT-f1",), "https://host.example/cb#access_token=***"),
    ("https://host.example/x?a=1#id_token=IT-f2&state=s", ("IT-f2",), "https://host.example/x?a=1#id_token=***&state=s"),
    ("https://u:PW-f3@host.example/#Token=TK-f3&api_key=AK-f3", ("PW-f3", "TK-f3", "AK-f3"),
     "https://***@host.example/#Token=***&api_key=***"),
    ("https://host.example/x?token=QT-f4#secret=FS-f4", ("QT-f4", "FS-f4"), "https://host.example/x?token=***#secret=***"),
]


@pytest.mark.parametrize("url, secrets, shown", _FRAGMENT_SHAPES)
def test_a_fragment_secret_masks_and_round_trips(url, secrets, shown):
    masked = mask_url_credentials(url)
    assert masked == shown
    assert not any(s in masked for s in secrets), masked
    assert _is_masked_url_credentials(masked), "PATCH must recognise the fragment mask"
    assert unmask_url_credentials(masked, url) == url


def test_the_restore_preview_masks_a_fragment_secret(tmp_path):
    b = _bundle(tmp_path, "sync:\n  database_url: https://auth.example/cb#access_token=PREVIEW-FRAG-7\n")
    _, cd = _live(tmp_path)
    p = bundle.preview(b, cd / "motif.yaml")
    row = {r["key"]: r for r in p["config_diff"]}["sync.database_url"]
    assert row["bundle"] == "https://auth.example/cb#access_token=***"
    assert "PREVIEW-FRAG-7" not in json.dumps(p)


def test_a_masked_fragment_secret_round_trips_through_patch_and_is_never_written(tmp_path, monkeypatch):
    client, settings = _api(tmp_path, monkeypatch)
    stored = "https://auth.example/cb#access_token=FRAG-9d"
    assert client.patch("/api/config", json={"sync": {"database_url": stored}}, headers=_H).status_code == 200
    r = client.get("/api/config", headers=_H)
    assert "FRAG-9d" not in r.text
    shown = r.json()["config"]["sync"]["database_url"]
    assert client.patch("/api/config", json={"sync": {"database_url": shown}}, headers=_H).status_code == 200
    assert settings.cfg.sync.database_url == stored
    edited = shown.replace("auth.example", "auth2.example")
    assert client.patch("/api/config", json={"sync": {"database_url": edited}}, headers=_H).status_code == 200
    assert settings.cfg.sync.database_url == "https://auth2.example/cb#access_token=FRAG-9d", "a host edit keeps the secret"
    r = client.patch("/api/config", json={"sync": {"database_url": "https://auth2.example/cb#refresh_token=***"}}, headers=_H)
    assert r.status_code == 400, "a masked name with nothing stored behind it is refused"
    assert settings.cfg.sync.database_url == "https://auth2.example/cb#access_token=FRAG-9d"
    assert "***" not in (tmp_path / "motif.yaml").read_text()


def test_mask_config_value_masks_a_fragment_secret_on_every_userinfo_key():
    for dotted in ("sync.git_url", "sync.database_url", "sync.db_url", "plex.url"):
        assert mask_config_value(dotted, "https://host.example/#access_token=MCV-1") == "https://host.example/#access_token=***", dotted


# ── 3. case-blind names ───────────────────────────────────────────────

@pytest.mark.parametrize("submitted, stored, expected", [
    ("https://host.example/x?TOKEN=***", "https://host.example/x?token=T-c1", "https://host.example/x?TOKEN=T-c1"),
    ("http://plex.lan:32400/?x-plex-token=***", "http://plex.lan:32400/?X-Plex-Token=T-c2", "http://plex.lan:32400/?x-plex-token=T-c2"),
    ("https://host.example/#Access_Token=***", "https://host.example/#access_token=T-c3", "https://host.example/#Access_Token=T-c3"),
    # v0.51.344: the exact spelling binds first — token= keeps token's A-c4, and TOKEN= takes the case-blind Token's B-c4
    ("https://host.example/x?TOKEN=***&token=***", "https://host.example/x?token=A-c4&Token=B-c4",
     "https://host.example/x?TOKEN=B-c4&token=A-c4"),
])
def test_a_masked_name_takes_its_stored_value_case_blind_in_the_submitted_spelling(submitted, stored, expected):
    assert _is_masked_url_credentials(submitted)
    assert unmask_url_credentials(submitted, stored) == expected


def test_a_recased_masked_query_param_saves_through_patch(tmp_path, monkeypatch):
    client, settings = _api(tmp_path, monkeypatch)
    assert client.patch("/api/config", json={"sync": {"git_url": "https://git.example.com/m.git?token=CASE-5f"}},
                        headers=_H).status_code == 200
    r = client.patch("/api/config", json={"sync": {"git_url": "https://git.example.com/m.git?TOKEN=***"}}, headers=_H)
    assert r.status_code == 200, r.text
    assert settings.cfg.sync.git_url == "https://git.example.com/m.git?TOKEN=CASE-5f"
    assert "***" not in (tmp_path / "motif.yaml").read_text()
