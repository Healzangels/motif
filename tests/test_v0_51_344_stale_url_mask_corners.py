"""v0.51.344: the stale-tab mask corners v0.51.343 left — a "?" inside a fragment secret (.342 tab), the .340 first-"@" mask — and the pin that every other URL masks as .342 did."""
from __future__ import annotations

import itertools
import re

import pytest

from app.core.config_file import _pre341_url_mask, mask_url_credentials, unmask_url_credentials
from app.core.events import _SCRUB_SUBSTRINGS

from tests.test_v0_51_341_config_secrets_preview import _api
from tests.test_v0_51_343_stale_url_mask import (
    _OLD_URL_QUERY_SECRET_RE, _OLD_URL_SCHEME_PREFIX_RE, _corpus, _old_mask_url_credentials, _patch, _saved, _shown,
)


def _v340_mask_url_credentials(url):  # frozen copy of v0.51.340 app/core/config_file.py mask_url_credentials: the oracle, never edit
    if not url or "://" not in url:
        return url or ""
    scheme, rest = url.split("://", 1)
    if "@" not in rest.split("/", 1)[0]:
        return url
    return f"{scheme}://***@{rest.split('@', 1)[1]}"


# ── 1. a .342 tab: a "?" inside a fragment secret ─────────────────────

_FRAGMENT_Q = [f"https://host.example/x#{a}=v?{b}=S-{i}"
               for i, (a, b) in enumerate(itertools.product(("secret", "access_token"), _SCRUB_SUBSTRINGS[:4]))]


@pytest.mark.parametrize("stored", _FRAGMENT_Q)
def test_a_342_tab_holding_a_question_mark_in_a_fragment_secret_saves_the_stored_url(tmp_path, monkeypatch, stored):
    stale = _old_mask_url_credentials(stored)
    assert "***" in stale and unmask_url_credentials(stale, "") == stale, "the case: a mask only the stale-tab arm can see — read as typed with nothing stored"
    client, settings = _api(tmp_path, monkeypatch)
    assert _patch(client, "sync.database_url", stored).status_code == 200
    before = (tmp_path / "motif.yaml").read_bytes()
    r = _patch(client, "sync.database_url", stale)
    assert r.status_code == 200, r.text
    assert (tmp_path / "motif.yaml").read_bytes() == before
    assert _saved(tmp_path, settings, "sync.database_url") == (stored, stored)


# ── 2. a .340 tab: userinfo split at the first "@" ────────────────────

_V340 = ["https://u:p@ss@git.example.com/m.git", "https://ci:PAT-7@git.example.com/a@b/m.git", "https://u:p@ss@h.example/x?by=a@b.c"]


@pytest.mark.parametrize("stored", _V340)
def test_a_340_tab_saves_the_stored_url_byte_identical(tmp_path, monkeypatch, stored):
    stale = _v340_mask_url_credentials(stored)
    client, settings = _api(tmp_path, monkeypatch)
    assert _patch(client, "sync.git_url", stored).status_code == 200
    assert _shown(client, "sync.git_url") != stale, "this build shows another mask — the case needs the stale one"
    before = (tmp_path / "motif.yaml").read_bytes()
    r = _patch(client, "sync.git_url", stale)
    assert r.status_code == 200, r.text
    assert (tmp_path / "motif.yaml").read_bytes() == before
    assert _saved(tmp_path, settings, "sync.git_url") == (stored, stored)


def test_the_340_stale_tab_arm_reproduces_the_340_mask_over_the_corpus():
    # v0.51.344: pins _pre341_url_mask to the frozen oracle — reading an "@" past the authority invents a mask no .340 tab showed (a typed https://***@b.c would silently keep the stored URL)
    urls = [u for _, u in _corpus()] + _V340 + _FRAGMENT_Q
    assert sum("@" in u.split("://", 1)[-1].split("/", 1)[-1] for u in urls) > 100, "the corpus holds an \"@\" past the authority"
    for u in urls:
        assert _pre341_url_mask(u) == _v340_mask_url_credentials(u), u


def test_every_340_mask_of_the_generated_corpus_saves_unchanged(tmp_path, monkeypatch):
    client, settings = _api(tmp_path, monkeypatch)
    bad = []
    for dotted, stored in _corpus():
        if not dotted.startswith("sync."):
            continue  # v0.51.340 masked only the sync URLs
        assert _patch(client, dotted, stored).status_code == 200
        r = _patch(client, dotted, _v340_mask_url_credentials(stored))
        if r.status_code != 200 or _saved(tmp_path, settings, dotted) != (stored, stored):
            bad.append((stored, _v340_mask_url_credentials(stored), r.status_code))
    assert not bad, bad[:10]
    assert "***" not in (tmp_path / "motif.yaml").read_text()


# ── 3. what the PATCH gate's removal relies on ────────────────────────

def test_unmask_returns_every_unmasked_value_as_submitted():
    urls = [u for _, u in _corpus()] + _FRAGMENT_Q + _V340
    for submitted, stored in itertools.product(urls, urls[::17]):
        if "***" in submitted:
            continue
        assert unmask_url_credentials(submitted, stored) == submitted, (submitted, stored)


@pytest.mark.parametrize("dotted, hand, kind", [
    ("plex.url", "plex:\n  url:\n    host: plex.lan\n", "an object"),
    ("sync.git_url", "sync:\n  git_url: [https://git.example.com/m.git]\n", "a list"),
], ids=["mapping", "list"])
def test_a_url_save_over_a_hand_edited_mapping_or_list_is_still_refused_in_words(tmp_path, monkeypatch, dotted, hand, kind):
    client, settings = _api(tmp_path, monkeypatch, hand)
    before = (tmp_path / "motif.yaml").read_bytes()
    for typed in ("https://git.example.com/x.git?token=T-9", "https://git.example.com/x.git?token=***"):
        r = _patch(client, dotted, typed)
        assert r.status_code == 400, r.text
        detail = r.json()["detail"]
        assert detail.startswith(f"{dotted} must be {kind}") or "no stored value to keep" in detail, detail
        assert "T-9" not in detail and "expected string" not in detail, "unmask never reads a mapping or a list as the stored URL"
    assert (tmp_path / "motif.yaml").read_bytes() == before


# ── 4. every URL outside the .343 shapes masks as .342 did ────────────

_OLD_FRAGMENT_SECRET_RE = re.compile(_OLD_URL_QUERY_SECRET_RE.pattern.replace("([?&]", "([?&#]", 1))

_G_SCHEME = ("https://", "http://", "HTTPS://", "git+ssh://", "")
_G_USERINFO = (None, "u", "u:p", "u:ab/cd", "u:p@ss", "u:p%40ss", ":tok", "u:")
_G_HOST = ("host.example", "plex.lan:32400", "[::1]:8080", "")
_G_PATH = ("", "/", "/x", "/a@b/x", "/x;token=v", "/p%3Fq")
_G_QUERY = (None, "", "a=1", "token=T", "TOKEN=T&by=x", "api_key=K&token=T2", "x=1&access_token=A", "token=", "q=a b&secret=S")
_G_FRAGMENT = (None, "", "frag", "x=1&token=F", "access_token=F", "from=a@b.c", "page=2")


def _grid():
    for sch, ui, host, path, q, frag in itertools.product(_G_SCHEME, _G_USERINFO, _G_HOST, _G_PATH, _G_QUERY, _G_FRAGMENT):
        yield (sch + (f"{ui}@" if ui is not None else "") + host + path
               + (f"?{q}" if q is not None else "") + (f"#{frag}" if frag is not None else ""))


def _in_promise(url: str) -> bool:
    m = _OLD_URL_SCHEME_PREFIX_RE.match(url)
    body = url[m.end():] if m else url
    cut = re.search(r"[?#]", body)
    after = body[cut.start():] if cut else ""
    fragment = body[body.index("#"):] if "#" in body else ""
    return "@" not in after and not _OLD_FRAGMENT_SECRET_RE.search(fragment)


def test_a_url_outside_the_new_shapes_masks_byte_identical_to_v0_51_342():
    # v0.51.344: pins the .343 promise that every URL outside the new shapes masks byte-identical to .342 — an intended mask change turns it red on purpose
    inside, outside_changed, bad = 0, 0, []
    for url in _grid():
        new, old = mask_url_credentials(url), _old_mask_url_credentials(url)
        if _in_promise(url):
            inside += 1
            if new != old:
                bad.append((url, old, new))
        elif new != old:
            outside_changed += 1
    assert inside > 10_000 and outside_changed > 0, (inside, outside_changed)
    assert not bad, (len(bad), bad[:5])
