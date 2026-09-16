"""v0.51.344: the events scrubber redacts a fragment's first secret param; one name list builds both URL regexes; unmask binds a masked name to its own spelling first."""
from __future__ import annotations

import contextlib
import json
import queue
import sqlite3

import pytest

from app.core import events, progress
from app.core.config_file import _pre343_url_mask, mask_url_credentials, unmask_url_credentials
from app.core.db import init_db

from tests.test_v0_51_341_config_secrets_preview import _H, _api
from tests.test_v0_51_343_stale_url_mask import _old_mask_url_credentials


# ── 1. the scrubber redacts a fragment's first secret param ──────────

@pytest.fixture
def queued(monkeypatch):
    q: queue.Queue = queue.Queue()
    monkeypatch.setattr(events, "_EVENT_QUEUE", q)
    monkeypatch.setattr(events, "_ensure_flusher_running", lambda db_path: None)
    return q


def test_log_event_redacts_a_fragment_secret_in_the_message_and_the_detail(tmp_path, queued):
    events.log_event(tmp_path / "motif.db", level="INFO", component="t",
                     message="fetch https://h.example/cb#access_token=FRAG-1a",
                     detail={"url": "https://h.example/#token=FRAG-2b&x=1", "list": ["https://h.example/?a=1#access_token=FRAG-3c"]})
    events.log_event(tmp_path / "motif.db", level="INFO", component="t", message="see https://h.example/#api_key=FRAG-4d now",
                     detail="GET https://h.example/#secret=FRAG-5e")
    rows = [queued.get_nowait()[-2:] for _ in range(2)]
    assert "FRAG-" not in json.dumps(rows), rows
    (m1, d1), (m2, d2) = rows
    assert m1 == "fetch https://h.example/cb#access_token=***"
    assert json.loads(d1) == {"url": "https://h.example/#token=***&x=1", "list": ["https://h.example/?a=1#access_token=***"]}
    assert (m2, d2) == ("see https://h.example/#api_key=*** now", "GET https://h.example/#secret=***")


def test_the_progress_feed_redacts_a_fragment_secret(tmp_path):
    db = tmp_path / "motif.db"
    init_db(db)
    progress.start_progress(db, "tdb_sync", "tdb_sync", activity="git fetch https://git.example.com/m.git#access_token=FRAG-6f")
    progress.update_progress(db, "tdb_sync", activity="GET https://h.example/x#token=FRAG-7g&x=1")
    with contextlib.closing(sqlite3.connect(db)) as c:
        row = c.execute("SELECT detail_json FROM op_progress WHERE op_id = 'tdb_sync'").fetchone()
    msgs = [a["msg"] for a in json.loads(row[0])["activity"]]
    assert not [m for m in msgs if "FRAG-" in m], msgs
    assert set(msgs) == {"git fetch https://git.example.com/m.git#access_token=***", "GET https://h.example/x#token=***&x=1"}


# ── 2. one name list: the mask, the scrubber and the .342 oracle shape ─

@pytest.mark.parametrize("name", events._SCRUB_SUBSTRINGS)
def test_every_secret_name_masks_after_a_question_mark_an_ampersand_and_a_hash(name):
    for url in (f"https://h.example/x?{name}=V-1", f"https://h.example/x?a=1&{name}=V-1",
                f"https://h.example/x#{name}=V-1", f"https://h.example/x?a=1#b=2&{name}=V-1"):
        masked = mask_url_credentials(url)
        assert "V-1" not in masked and masked.endswith(f"{name}=***"), (url, masked)
        assert unmask_url_credentials(masked, url) == url
        assert events._redact_url_credentials(url) == masked, "the scrubber and the mask read one name list"
        assert _pre343_url_mask(url) == _old_mask_url_credentials(url), "the stale-tab check must reproduce the .342 mask"
    assert _pre343_url_mask(f"https://h.example/x#{name}=V-1") == f"https://h.example/x#{name}=V-1"


# ── 3. unmask binds a masked name to its own spelling first ───────────

_TWO = "https://h.example/x?TOKEN=A-x&token=B-x"


@pytest.mark.parametrize("submitted, stored, expected", [
    ("https://h.example/x?token=***", _TWO, "https://h.example/x?token=B-x"),
    ("https://h.example/x?TOKEN=***", _TWO, "https://h.example/x?TOKEN=A-x"),
    ("https://h.example/x?token=***&TOKEN=***", _TWO, "https://h.example/x?token=B-x&TOKEN=A-x"),
    ("https://h.example/x?Token=***", _TWO, "https://h.example/x?Token=A-x"),
    ("https://h.example/x?Token=***&tOKEN=***", _TWO, "https://h.example/x?Token=A-x&tOKEN=B-x"),
    ("https://h.example/x?token=***#Token=***", "https://h.example/x?token=Q-x#Token=F-x", "https://h.example/x?token=Q-x#Token=F-x"),
    ("https://h.example/x#Token=***", "https://h.example/x?token=Q-x#Token=F-x", "https://h.example/x#Token=F-x"),
    ("https://***@h.example/x?token=***", "https://u:p@h.example/x?TOKEN=A-x&token=B-x", "https://u:p@h.example/x?token=B-x"),
    ("https://h.example/x?ſecret=***", "https://h.example/x?secret=S-x", "https://h.example/x?ſecret=S-x"),
], ids=["exact-lower", "exact-upper", "both-swapped", "case-blind-first", "case-blind-in-order", "query-and-fragment",
        "query-removed", "userinfo-and-param", "unicode-fold"])
def test_a_masked_name_takes_its_own_spellings_value_first(submitted, stored, expected):
    assert unmask_url_credentials(submitted, stored) == expected


@pytest.mark.parametrize("submitted, named", [
    ("https://h.example/x?refresh_token=***", "refresh_token"),
    ("https://h.example/x?token=***&TOKEN=***&Token=***", "Token"),
])
def test_a_masked_name_with_no_stored_value_left_is_still_refused_by_name(submitted, named):
    with pytest.raises(ValueError) as e:
        unmask_url_credentials(submitted, _TWO)
    assert f"the masked {named} has" in str(e.value) and "A-x" not in str(e.value) and "B-x" not in str(e.value)


def test_a_patch_keeps_the_exact_spellings_value(tmp_path, monkeypatch):
    client, settings = _api(tmp_path, monkeypatch)
    stored = "https://git.example.com/m.git?TOKEN=A-x&token=B-x"
    assert client.patch("/api/config", json={"sync": {"git_url": stored}}, headers=_H).status_code == 200
    shown = client.get("/api/config", headers=_H).json()["config"]["sync"]["git_url"]
    assert "A-x" not in shown and "B-x" not in shown, shown
    r = client.patch("/api/config", json={"sync": {"git_url": "https://git.example.com/m.git?token=***"}}, headers=_H)
    assert r.status_code == 200, r.text
    assert settings.cfg.sync.git_url == "https://git.example.com/m.git?token=B-x"
    assert "***" not in (tmp_path / "motif.yaml").read_text()
