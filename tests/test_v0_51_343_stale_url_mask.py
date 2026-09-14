"""v0.51.343: a settings tab loaded on <=v0.51.342 saves a USERINFO_URL_KEYS field it never touched without moving or refusing the stored URL."""
from __future__ import annotations

import itertools
import re
from urllib.parse import urlsplit

import pytest
import yaml

from tests.test_v0_51_341_config_secrets_preview import _H, _api


# ── frozen copy of the v0.51.342 mask (176c9c4a app/core/config_file.py + events regex): the oracle, never edit ──

_OLD_URL_SCHEME_PREFIX_RE = re.compile(r"[A-Za-z][A-Za-z0-9+.\-]*://")
_OLD_URL_QUERY_SECRET_RE = re.compile(
    r"(?i)([?&][A-Za-z0-9_\-]*(?:token|secret|password|cookie|auth|api_key|apikey|bearer"
    r"|private_key|signing_key|encryption_key|master_key)[A-Za-z0-9_\-]*=)[^&\s#\"']+")
_OLD_APPRISE_MASK = "***"


def _old_split_userinfo(url):
    m = _OLD_URL_SCHEME_PREFIX_RE.match(url)
    prefix = m.group(0) if m else ""
    body = url[len(prefix):]
    at = body.rfind("@")
    return (prefix, None, body) if at < 0 else (prefix, body[:at], body[at + 1:])


def _old_mask_url_credentials(url):
    if not url:
        return url or ""
    prefix, userinfo, rest = _old_split_userinfo(url)
    rest = _OLD_URL_QUERY_SECRET_RE.sub(lambda m: f"{m.group(1)}{_OLD_APPRISE_MASK}", rest)
    return f"{prefix}{_OLD_APPRISE_MASK}@{rest}" if userinfo is not None else f"{prefix}{rest}"


# ── helpers ───────────────────────────────────────────────────────────

def _patch(client, dotted, value):
    sec, leaf = dotted.split(".", 1)
    return client.patch("/api/config", json={sec: {leaf: value}}, headers=_H)


def _shown(client, dotted):
    sec, leaf = dotted.split(".", 1)
    return client.get("/api/config", headers=_H).json()["config"][sec][leaf]


def _saved(tmp_path, settings, dotted):
    sec, leaf = dotted.split(".", 1)
    disk = yaml.safe_load((tmp_path / "motif.yaml").read_text())[sec][leaf]
    return disk, getattr(getattr(settings.cfg, sec), leaf)


# ── 1. the stale mask of an untouched field keeps the stored URL ──────

_STALE = [
    ("sync.git_url", "https://ci:PAT-77z@git.example.com/m.git?by=ops@example.org", "https://***@example.org"),
    ("plex.url", "http://pu:PW@plex.lan:32400/?contact=ops@example.org", "http://***@example.org"),
    ("sync.git_url", "https://tok@host.example/cb#from=a@b.c", "https://***@b.c"),
    ("sync.database_url", "https://codeload.example.com/x?contact=ops@example.org", "https://***@example.org"),
    ("sync.db_url", "https://u:p?w@host.example/x", "https://***@host.example/x"),
    ("sync.database_url", "https://h.example/x?token=a#token=b&token=c", "https://h.example/x?token=***#token=b&token=***"),
]


@pytest.mark.parametrize("dotted, stored, old_shown", _STALE)
def test_the_mask_a_pre_upgrade_tab_holds_saves_the_stored_url_byte_identical(tmp_path, monkeypatch, dotted, stored, old_shown):
    assert _old_mask_url_credentials(stored) == old_shown
    client, settings = _api(tmp_path, monkeypatch)
    assert _patch(client, dotted, stored).status_code == 200
    assert _shown(client, dotted) != old_shown, "this build shows another mask — the case needs the stale one"
    before = (tmp_path / "motif.yaml").read_bytes()
    r = _patch(client, dotted, old_shown)
    assert r.status_code == 200, r.text
    assert (tmp_path / "motif.yaml").read_bytes() == before
    assert _saved(tmp_path, settings, dotted) == (stored, stored)


def test_the_rest_of_the_stale_tab_saves_with_it(tmp_path, monkeypatch):
    client, settings = _api(tmp_path, monkeypatch)
    stored = "https://codeload.example.com/x?contact=ops@example.org"
    assert _patch(client, "sync.database_url", stored).status_code == 200
    r = client.patch("/api/config", json={"sync": {"database_url": _old_mask_url_credentials(stored), "cron": "15 3 * * *"}},
                     headers=_H)
    assert r.status_code == 200, r.text
    assert settings.cfg.sync.cron == "15 3 * * *"
    assert _saved(tmp_path, settings, "sync.database_url") == (stored, stored)


# ── 2. controls: this build's masks and edits behave as before ────────

def test_this_builds_mask_round_trips_and_a_host_edit_keeps_the_credential(tmp_path, monkeypatch):
    client, settings = _api(tmp_path, monkeypatch)
    stored = "https://ci:PAT-77z@git.example.com/m.git?by=ops@example.org"
    assert _patch(client, "sync.git_url", stored).status_code == 200
    shown = _shown(client, "sync.git_url")
    assert "PAT-77z" not in shown and urlsplit(shown).hostname == "git.example.com"
    before = (tmp_path / "motif.yaml").read_bytes()
    assert _patch(client, "sync.git_url", shown).status_code == 200
    assert (tmp_path / "motif.yaml").read_bytes() == before
    edited = shown.replace("git.example.com", "mirror.example.net")
    assert edited.startswith("https://***@")
    assert _patch(client, "sync.git_url", edited).status_code == 200
    want = "https://ci:PAT-77z@mirror.example.net/m.git?by=ops@example.org"
    assert _saved(tmp_path, settings, "sync.git_url") == (want, want)


def test_a_recased_query_mask_and_a_fragment_mask_still_restore(tmp_path, monkeypatch):
    client, settings = _api(tmp_path, monkeypatch)
    assert _patch(client, "sync.database_url", "https://git.example.com/m.git?token=CASE-5f&by=ops@example.org").status_code == 200
    r = _patch(client, "sync.database_url", "https://git.example.com/m.git?TOKEN=***&by=ops@example.org")
    assert r.status_code == 200, r.text
    want = "https://git.example.com/m.git?TOKEN=CASE-5f&by=ops@example.org"
    assert _saved(tmp_path, settings, "sync.database_url") == (want, want)

    stored = "https://tok-9@auth.example/cb?by=a@b.c#access_token=FRAG-9d"
    assert _patch(client, "plex.url", stored).status_code == 200
    shown = _shown(client, "plex.url")
    assert "FRAG-9d" not in shown and "tok-9" not in shown
    assert _patch(client, "plex.url", shown).status_code == 200
    assert _saved(tmp_path, settings, "plex.url") == (stored, stored)
    assert _patch(client, "plex.url", shown.replace("auth.example", "auth2.example")).status_code == 200
    want = "https://tok-9@auth2.example/cb?by=a@b.c#access_token=FRAG-9d"
    assert _saved(tmp_path, settings, "plex.url") == (want, want)
    assert "***" not in (tmp_path / "motif.yaml").read_text()


def test_a_mask_with_nothing_stored_behind_it_is_still_refused(tmp_path, monkeypatch):
    client, settings = _api(tmp_path, monkeypatch)
    stored = "https://codeload.example.com/x?contact=ops@example.org"
    assert _patch(client, "sync.database_url", stored).status_code == 200
    r = _patch(client, "sync.database_url", "https://***@mirror.example.net/x")
    assert r.status_code == 400, "only the stale mask of THIS stored value means keep"
    assert _saved(tmp_path, settings, "sync.database_url") == (stored, stored)
    assert "***" not in (tmp_path / "motif.yaml").read_text()


# ── 3. fuzz: both masks of every generated URL save it unchanged ──────

_KEYS = (("sync.git_url", "https://", "git.example.com"), ("sync.database_url", "http://", "codeload.example.com"),
         ("sync.db_url", "https://", "app.example.dev"), ("plex.url", "", "plex.lan:32400"))
_USERINFO = (None, "ci:PAT-1", "u:p@ss", "u:ab/cd", "u:p?w")
_PATH = ("/m.git", "/a@b/x")
_QUERY = (None, "by=ops@example.org", "token=T-q1", "TOKEN=T-q2&by=a@b.c", "X-Plex-Token=P@T-3", "page=2&Api_Key=K-4")
_FRAGMENT = (None, "from=a@b.c", "access_token=AT-f", "Access_Token=AT-g&to=c@d.e")


def _corpus():
    urls = []
    for i, (ui, path, q, frag) in enumerate(itertools.product(_USERINFO, _PATH, _QUERY, _FRAGMENT)):
        dotted, scheme, host = _KEYS[i % len(_KEYS)]
        url = scheme + (f"{ui}@" if ui else "") + host + path + (f"?{q}" if q else "") + (f"#{frag}" if frag else "")
        urls.append((dotted, url))
    return urls


def test_the_corpus_reaches_every_stale_mask_outcome():
    from app.core.config_file import mask_url_credentials
    stale = [(d, u) for d, u in _corpus() if _old_mask_url_credentials(u) != mask_url_credentials(u)]
    assert any(mask_url_credentials(u).split("://", 1)[-1].startswith("***@") for _, u in stale), "userinfo this build sees"
    assert any(not mask_url_credentials(u).split("://", 1)[-1].startswith("***@") for _, u in stale), "no userinfo this build sees"
    assert any(_old_mask_url_credentials(u) == mask_url_credentials(u) != u for _, u in _corpus()), "a mask both builds agree on"
    assert {d for d, _ in stale} == {d for d, _, _ in _KEYS}


def _tab(values):
    body = {}
    for dotted, v in values:
        sec, leaf = dotted.split(".", 1)
        body.setdefault(sec, {})[leaf] = v
    return body


def test_the_old_and_the_new_mask_of_every_generated_url_save_it_unchanged(tmp_path, monkeypatch):
    from app.core.config_file import mask_url_credentials
    client, settings = _api(tmp_path, monkeypatch)
    corpus, bad = _corpus(), []
    for start in range(0, len(corpus), len(_KEYS)):
        batch = corpus[start:start + len(_KEYS)]
        for label, mask in (("old", _old_mask_url_credentials), ("new", mask_url_credentials)):
            assert client.patch("/api/config", json=_tab(batch), headers=_H).status_code == 200, batch
            r = client.patch("/api/config", json=_tab([(d, mask(u)) for d, u in batch]), headers=_H)
            for dotted, stored in batch:
                got = _saved(tmp_path, settings, dotted)
                if r.status_code != 200 or got != (stored, stored):
                    bad.append((label, stored, mask(stored), r.status_code, got))
    assert not bad, bad[:10]
    assert "***" not in (tmp_path / "motif.yaml").read_text()
