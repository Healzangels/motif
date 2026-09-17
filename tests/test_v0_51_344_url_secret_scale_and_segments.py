"""v0.51.344 (integration review): a masked URL param binds inside its own segment first and in O(k); the events scrubber redacts userinfo as the settings mask shows it; a 16k name run is read once."""
from __future__ import annotations

import contextlib
import json
import queue
import random
import re
import sqlite3
import time

import pytest

from app.core import events, progress
from app.core.config_file import mask_url_credentials, unmask_url_credentials
from app.core.db import init_db

from tests.test_v0_51_341_config_secrets_preview import _SHAPES, _H, _api
from tests.test_v0_51_343_stale_url_mask import _patch, _saved
from tests.test_v0_51_343_url_secret_mask_polish import _FRAGMENT_SHAPES


# ── 1. a masked param binds to the value stored in its own segment ────

_THREE = "https://h.example/x?token=QV&TOKEN=RV#token=FV"


@pytest.mark.parametrize("submitted, stored, expected", [
    ("https://h.example/x?TOKEN=***#token=***", _THREE, "https://h.example/x?TOKEN=RV#token=FV"),
    ("https://h.example/x#token=***", "https://h.example/x?token=QV#token=FV", "https://h.example/x#token=FV"),
    ("https://h.example/x?token=***", "https://h.example/x?token=QV#token=FV", "https://h.example/x?token=QV"),
    ("https://h.example/x#token=***", "https://h.example/x?token=a#TOKEN=b", "https://h.example/x#token=b"),
    ("https://h.example/x?a=1#b=2&token=***", "https://h.example/x?token=QV#x=1&token=FV", "https://h.example/x?a=1#b=2&token=FV"),
    ("https://h.example/x#Token=***", "https://h.example/x?token=QV", "https://h.example/x#Token=QV"),
    ("https://h.example/x?token=***&token=***", "https://h.example/x?TOKEN=AV&token=BV", "https://h.example/x?token=BV&token=AV"),
    ("https://h.example/x?token=***&TOKEN=***#token=***", _THREE, _THREE),
], ids=["query-param-deleted", "same-name-in-both-segments", "fragment-param-deleted", "segment-before-spelling",
        "ampersand-past-the-hash", "moved-across-segments", "exact-then-case-blind", "unchanged"])
def test_a_masked_param_takes_the_value_stored_in_its_own_segment_first(submitted, stored, expected):
    # v0.51.344: one flat pool bound ?TOKEN=***#token=*** to the deleted query token's value and lost the fragment's
    assert unmask_url_credentials(submitted, stored) == expected


def test_deleting_one_of_two_same_named_masked_params_keeps_the_other_segments_value(tmp_path, monkeypatch):
    client, settings = _api(tmp_path, monkeypatch)
    stored = "https://git.example.com/m.git?token=QV&TOKEN=RV#token=FV"
    assert _patch(client, "sync.git_url", stored).status_code == 200
    assert client.get("/api/config", headers=_H).json()["config"]["sync"]["git_url"] == \
        "https://git.example.com/m.git?token=***&TOKEN=***#token=***"
    r = _patch(client, "sync.git_url", "https://git.example.com/m.git?TOKEN=***#token=***")
    assert r.status_code == 200, r.text
    want = "https://git.example.com/m.git?TOKEN=RV#token=FV"
    assert _saved(tmp_path, settings, "sync.git_url") == (want, want)
    assert "QV" not in (tmp_path / "motif.yaml").read_text() and "***" not in (tmp_path / "motif.yaml").read_text()


# ── 2. thousands of masked params bind in linear time ─────────────────

_K = 2000
_STORED_K = "https://h.example/x?" + "&".join(f"a{i}token=v{i}" for i in range(_K))


@pytest.mark.parametrize("submitted, expected", [
    ("https://h.example/x?" + "&".join(f"b{i}token=***" for i in range(_K)), ValueError),
    ("https://h.example/x?" + "&".join(f"a{i}token=***" for i in reversed(range(_K))),
     "https://h.example/x?" + "&".join(f"a{i}token=v{i}" for i in reversed(range(_K)))),
    (mask_url_credentials(_STORED_K), _STORED_K),
], ids=["disjoint-names", "reversed-order", "round-trip"])
def test_two_thousand_masked_params_bind_well_under_a_second(submitted, expected):
    # v0.51.344: the fold loop scanned the whole pool once per masked name — 2,000 disjoint names took ~0.5 s and 4,000 ~2 s, on the event loop
    started = time.perf_counter()
    if expected is ValueError:
        with pytest.raises(ValueError):
            unmask_url_credentials(submitted, _STORED_K)
    else:
        assert unmask_url_credentials(submitted, _STORED_K) == expected
    elapsed = time.perf_counter() - started
    assert elapsed < 1.0, f"{_K} masked params took {elapsed:.3f} s"  # v0.51.344: the quadratic fold took ~0.5 s here and 2 s at 4,000; 0.15 s mirrored one idle machine (0.179 s in a full-suite run)


# ── 3. the scrubber shows a URL as the settings mask does ─────────────

_URL_ROWS = [u for u, _, _ in _SHAPES + _FRAGMENT_SHAPES if "://" in u] + [  # a scheme-less user:pw@host is an address in a log line
    "https://u:ab/cd@host.example/x?email=ops@example.org",
    "https://u:p@ss@host.example/x#to=ops@example.org",
    "https://u:pa@SECRETTAIL@h.example/repo.git",
    "https://h.example#access_token=abc@SECRETTAIL",
    "https://h.example?token=abc@SECRETTAIL",
    "https://host.example/x?email=ops@example.org",
]


@pytest.mark.parametrize("url", _URL_ROWS)
def test_the_scrubber_redacts_a_url_exactly_as_the_settings_mask_shows_it(url):
    # v0.51.344: the scrubber cut userinfo at the FIRST "@" — "u:pa/TAIL@h" and "u:pa@TAIL@h" reached the events table in clear
    shown = mask_url_credentials(url)
    assert events._redact_url_credentials(url) == shown
    assert events._redact_url_credentials(f"Sync run #7: git mirror acquired from {url} in 3s") == \
        f"Sync run #7: git mirror acquired from {shown} in 3s"


def test_a_sync_event_and_the_activity_feed_hide_a_password_holding_a_slash_or_an_at_sign(tmp_path, monkeypatch):
    q: queue.Queue = queue.Queue()
    monkeypatch.setattr(events, "_EVENT_QUEUE", q)
    monkeypatch.setattr(events, "_ensure_flusher_running", lambda db_path: None)
    git_url, tar_url = "https://u:pa@SECRETTAIL@h.example/repo.git", "https://u:pa/SECRETTAIL@h.example/tar.gz"
    events.log_event(tmp_path / "motif.db", level="INFO", component="sync",
                     message=f"Sync run #7: git mirror acquired from {git_url}", detail={"tar_url": tar_url})
    message, detail = q.get_nowait()[-2:]
    assert message == "Sync run #7: git mirror acquired from https://***@h.example/repo.git"
    assert json.loads(detail) == {"tar_url": "https://***@h.example/tar.gz"}
    db = tmp_path / "motif.db"
    init_db(db)
    progress.start_progress(db, "tdb_sync", "tdb_sync", activity=f"git fetch {git_url}")
    progress.update_progress(db, "tdb_sync", activity=f"GET {tar_url}")
    with contextlib.closing(sqlite3.connect(db)) as c:
        row = c.execute("SELECT detail_json FROM op_progress WHERE op_id = 'tdb_sync'").fetchone()
    msgs = {a["msg"] for a in json.loads(row[0])["activity"]}
    assert msgs == {"git fetch https://***@h.example/repo.git", "GET https://***@h.example/tar.gz"}


# ── 3b. never less than the .343 scrubber hid: "?"/"#" in a password, a URL nested past "?" ──

_NESTED = "https://***@h2.example/y"


@pytest.mark.parametrize("text, expected", [
    ("https://user:pa?ss@host.example/x", "https://***@host.example/x"),
    ("https://user:pa#ss@host.example/x", "https://***@host.example/x"),
    ("https://h.example/x?next=https://u:p@h2.example/y", f"https://h.example/x?next={_NESTED}"),
    ("https://h.example/x?q=1,https://u:p@h2.example/y", f"https://h.example/x?q=1,{_NESTED}"),
    ("https://h.example/x#https://u:p@h2.example/y", f"https://h.example/x#{_NESTED}"),
    ("https://h.example/x?a=1&u=https://u:p@h2.example/y", f"https://h.example/x?a=1&u={_NESTED}"),
    ("https://h.example/x?next=https://u:pa/x@h2.example/y", f"https://h.example/x?next={_NESTED}"),
    ('{"a":"https://h.example/x?q=1","b":"https://u:p@h2.example/y"}', '{"a":"https://h.example/x?q=1","b":"' + _NESTED + '"}'),
    ("https://h.example#access_token=abc@TAIL", "https://h.example#access_token=***"),
    ("https://***@host.example/x?next=https://***@h2.example/y", "https://***@host.example/x?next=https://***@h2.example/y"),
], ids=["query-mark-in-password", "hash-in-password", "nested-after-next", "comma-joined", "nested-in-fragment",
        "nested-after-ampersand", "nested-slash-in-password", "compact-json", "masked-value-keeps-the-host", "idempotent"])
def test_a_password_holding_a_query_mark_and_a_url_nested_past_the_query_still_redact(text, expected):
    # v0.51.344: one whole-run match read "pa?ss@host" as a query and "?next=https://u:p@h" as its value — both logged in clear where .343 redacted them
    assert events._redact_url_credentials(text) == expected
    assert events._redact_url_credentials(f"see {text} now") == f"see {expected} now"


def test_a_sync_activity_line_and_a_detail_note_hide_a_password_holding_a_query_mark(tmp_path, monkeypatch):
    q: queue.Queue = queue.Queue()
    monkeypatch.setattr(events, "_EVENT_QUEUE", q)
    monkeypatch.setattr(events, "_ensure_flusher_running", lambda db_path: None)
    tar_url, repo_url = "https://user:pa?ss@host.example/tar.gz", "https://user:pa#ss@host.example/repo.git"
    events.log_event(tmp_path / "motif.db", level="INFO", component="sync",
                     message=f"Sync run #7: snapshot acquired from {tar_url}",
                     detail={"note": "https://h.example/x?next=https://u:p@evil.example/cb"})
    message, detail = q.get_nowait()[-2:]
    assert message == "Sync run #7: snapshot acquired from https://***@host.example/tar.gz"
    assert json.loads(detail) == {"note": "https://h.example/x?next=https://***@evil.example/cb"}
    db = tmp_path / "motif.db"
    init_db(db)
    progress.start_progress(db, "tdb_sync", "tdb_sync", activity=f"git clone --bare --depth 1 -b main {repo_url}")
    progress.update_progress(db, "tdb_sync", activity=f"GET {tar_url}")
    with contextlib.closing(sqlite3.connect(db)) as c:
        row = c.execute("SELECT detail_json FROM op_progress WHERE op_id = 'tdb_sync'").fetchone()
    msgs = {a["msg"] for a in json.loads(row[0])["activity"]}
    assert msgs == {"git clone --bare --depth 1 -b main https://***@host.example/repo.git", "GET https://***@host.example/tar.gz"}


_PRE344_USERINFO_RE = re.compile(r"(?P<scheme>[a-zA-Z][a-zA-Z0-9+\-.]*://)(?P<userinfo>[^/@\s]+)@")  # the .343 scrubber's userinfo rule, frozen


def test_the_scrubber_hides_every_password_the_343_first_at_rule_hid():
    # v0.51.344: the mask's rule alone (last "@" before ?/#) showed "pa?ss@" and a nested "?next=https://u:p@h" the first-"@" rule hid; no secret param names here — a masked value's "@" may no longer hide the host
    rng = random.Random(344)
    lead = ["", "see ", '{"u":"', "(", "1.", "x,", "https://h.example/x?next=", "https://h.example/x#", "https://a.example/b?c=d,"]
    userinfo = ["u:", "PW", "pa?ss", "pa#ss", "@", "?", "#", "&", "=", ",", "/x", " ", "https://"]
    tail = ["", "/x", "?a=1", " now", '"}', "@h2", "?next=https://u:PW@h3/y", "#token=PW@h3"]
    hidden_by_old = 0
    for _ in range(4000):
        s = rng.choice(lead) + "https://" + "".join(rng.choice(userinfo) for _ in range(rng.randint(1, 5))) + "@h.example" + rng.choice(tail)
        old = _PRE344_USERINFO_RE.sub(lambda m: f"{m.group('scheme')}***@", s)
        if "PW" in s and "PW" not in old:
            hidden_by_old += 1
            assert "PW" not in events._redact_url_credentials(s), s
    assert hidden_by_old > 500, hidden_by_old


# ── 4. a 16k name run is read once ────────────────────────────────────

@pytest.mark.parametrize("text", ["#" + "token" * 3200, "?" + "token" * 3200, "a" * 16000, "a://" * 4000],
                         ids=["hash-name-run", "query-name-run", "letters", "schemes"])
def test_a_16k_run_is_scrubbed_in_linear_time(text):
    # v0.51.344: "#" + "token" * 3200 took ~360 ms (x16 input = x245 time) before the 2 KB cap could apply
    started = time.perf_counter()
    assert events._redact_url_credentials(text) == text
    elapsed = time.perf_counter() - started
    assert elapsed < 0.1, f"{len(text)} chars took {elapsed:.3f} s"


def test_a_16k_url_masks_and_unmasks_in_linear_time():
    url = "https://h.example/x?" + "token" * 3200
    started = time.perf_counter()
    assert mask_url_credentials(url) == url
    assert unmask_url_credentials(url, url) == url
    assert events._scrub_text(url) == url[:2048] + "…"
    elapsed = time.perf_counter() - started
    assert elapsed < 0.1, f"{len(url)} chars took {elapsed:.3f} s"


_TWO_STAR_NAME = r"[A-Za-z0-9_\-]*(?:" + "|".join(re.escape(s) for s in events._SCRUB_SUBSTRINGS) + r")[A-Za-z0-9_\-]*="


@pytest.mark.parametrize("starts", ["[?&]", "[?&#]"], ids=["query", "param"])
def test_the_single_pass_name_regex_matches_the_two_star_shape_byte_for_byte(starts):
    # the .343 shape: two backtracking stars around the word — the same language, read in one pass since v0.51.344
    two_star = re.compile(r"(?i)(" + starts + _TWO_STAR_NAME + r")[^&\s#\"']+")
    live = events._URL_QUERY_SECRET_RE if starts == "[?&]" else events._URL_PARAM_SECRET_RE
    rng = random.Random(344)
    words = list(events._SCRUB_SUBSTRINGS) + ["TOKEN", "Secret", "x", "ab", "1", "-", "_", "ſ", "K"]
    seen = 0
    for _ in range(20000):
        parts = []
        for _ in range(rng.randint(1, 4)):
            parts.append(rng.choice(("?", "&", "#", " ", "/", "=")))
            parts.append("".join(rng.choice(words) for _ in range(rng.randint(0, 3))))
            parts.append(rng.choice(("=", "=", "", ":")))
            parts.append("".join(rng.choice(("v", "1", "@", ".", "&", "#", "\"", "'", " ", "%")) for _ in range(rng.randint(0, 4))))
        s = "".join(parts)
        assert live.sub(r"<\1>", s) == two_star.sub(r"<\1>", s), s
        seen += live.sub(r"<\1>", s) != s
    assert seen > 1000, seen
