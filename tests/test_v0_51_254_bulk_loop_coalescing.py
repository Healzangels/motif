"""v0.51.254 — a client-side bulk loop is ONE action, and Discord 429s retry.

2026-08-09, straight after the v0.51.253 dead-rk fix let the operator's 72-row
recovery push actually succeed: 72 separate Discord messages, and a run of

    discord native embed send failed: HTTP 429 {"retry_after": 1.657}
    Notification dispatch failed: theme_pushed | {"sent_ok": 0, "sent_fail": 1}

Two independent defects, both pre-existing — the failed uploads had simply been
hiding them (a place that 404s never notifies).

## 1. The bulk flag never reached the loop endpoints

v1.23.46 made coalescing depend on an EXPLICIT `bulk` flag stamped on the job,
because inferring "bulk" from a time window wrongly collapsed N separate manual
actions into one fake batch. Bulk endpoints (`/library/download-batch`,
bulk-accept, CSV import) stamp it. But bulk PUSH and bulk SWITCH TO API are
CLIENT-SIDE loops over the SINGLE-row `/replace` and `/switch-placement`
endpoints — nothing in the request said "these 72 calls are one action", so
every place job took dispatch_coalesced's bulk=False branch: immediate, rich,
per item. The endpoints now accept `?bulk=1` and stamp it into the place job
payload the worker already reads (`bulk=bool(payload.get("bulk"))`).

Scope was MEASURED, not assumed. The other two client bulk loops —
`/restore-canonical` and `/adopt-sidecar` — enqueue no place job and dispatch
no notification, so they cannot flood and are deliberately untouched.

## 2. A 429 threw the message away

`_send_discord_embed` treated 429 as a generic failure: log, return False,
done — while the response body names the exact wait. Those notifications were
LOST, not delayed. Coalescing cuts the volume that provokes 429s, but any burst
can still trip one, so the sender honors `retry_after` once, capped by
`_DISCORD_429_MAX_WAIT_S` (a global 429 can name minutes; blocking the dispatch
thread that long is worse than dropping one message).
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from app.core.db import init_db

AUTH = {"X-Authentik-Username": "testadmin"}
REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
NOW = "2026-08-09T21:17:00+00:00"
TMDB = 68726


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.web.api import create_app
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    with sqlite3.connect(s.db_path) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime,"
            " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
            " VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, year,"
            " upstream_source, last_seen_sync_at, first_seen_sync_at)"
            " VALUES ('movie',?,'Pacific Rim','2013','imdb',?,?)",
            (TMDB, NOW, NOW))
        # The only prerequisite /replace has: a canonical to replace FROM.
        conn.execute(
            "INSERT INTO local_files (media_type, tmdb_id, section_id,"
            " edition_key, file_path, downloaded_at, source_video_id,"
            " provenance, source_kind) VALUES ('movie',?,'1','',"
            "'movies/pr.mp3',?,'v','auto','themerrdb')", (TMDB, NOW))
        conn.commit()
    c = TestClient(create_app(s))
    c._db = s.db_path                      # noqa: SLF001 — test convenience
    return c


def _place_payloads(db) -> list[dict]:
    conn = sqlite3.connect(db)
    rows = conn.execute(
        "SELECT payload FROM jobs WHERE job_type='place' AND tmdb_id=?",
        (TMDB,)).fetchall()
    return [json.loads(r[0]) for r in rows]


# ── 1. the flag reaches the job payload (v1.18.81: exercise the endpoint) ──

def test_bulk_push_marks_the_place_job_bulk(client):
    """?bulk=1 → the place job carries bulk, so the worker coalesces. Asserting
    the Query param exists in source would be a phantom guard (v1.18.81) — it
    proves nothing about the payload the worker actually reads."""
    r = client.post(f"/api/items/movie/{TMDB}/replace?bulk=1", headers=AUTH)
    assert r.status_code == 200, r.text

    payloads = _place_payloads(client._db)
    assert payloads, "no place job enqueued"
    assert all(p.get("bulk") is True for p in payloads), payloads


def test_single_push_leaves_the_job_unmarked(client):
    """The v1.23.46 property, preserved: ONE user action must stay immediate
    and rich. If this flips, a per-row PUSH from the SOURCE menu silently
    stops previewing and waits on a coalesce window."""
    r = client.post(f"/api/items/movie/{TMDB}/replace", headers=AUTH)
    assert r.status_code == 200, r.text

    payloads = _place_payloads(client._db)
    assert payloads, "no place job enqueued"
    assert all("bulk" not in p for p in payloads), payloads


def test_switch_placement_carries_bulk_too(client):
    """The sibling loop. v0.51.253 shipped a fix to one of two resolves and the
    tests caught it; this pins both notification-producing loops together."""
    with sqlite3.connect(client._db) as conn:
        conn.execute(
            "INSERT INTO placements (media_type, tmdb_id, section_id,"
            " edition_key, media_folder, placed_at, placement_kind,"
            " plex_refreshed, provenance) VALUES ('movie',?,'1','',"
            "'/data/Movies/Pacific Rim (2013)',?, 'hardlink',1,'auto')",
            (TMDB, NOW))
        conn.commit()

    r = client.post(f"/api/items/movie/{TMDB}/switch-placement?bulk=1",
                    headers=AUTH)
    assert r.status_code == 200, r.text

    payloads = _place_payloads(client._db)
    assert payloads, "no place job enqueued"
    assert all(p.get("bulk") is True for p in payloads), payloads


# ── 2. the client loops actually send it ─────────────────────────────────

def _live_js() -> str:
    """Comment-stripped app.js. This tag's own rationale comments say
    "bulk=1" in prose, and counting those instead of call sites is the
    comment-trap that bit six times during the v0.51.246-252 review."""
    return "\n".join(ln for ln in APP_JS.splitlines()
                     if not ln.lstrip().startswith("//"))


def _loop_body(start_anchor: str, end_anchor: str) -> str:
    """The window must span from the query-string build to the api() call —
    the flag is pushed BEFORE the call, so a forward-only window from the
    call misses it (this test failed that way first)."""
    live = _live_js()
    i = live.index(start_anchor)
    j = live.index(end_anchor, i)
    return live[i:j + len(end_anchor)]


def test_bulk_push_loop_sends_bulk():
    body = _loop_body("const _cP = [];",
                      "`/api/items/${c.mt}/${c.id}/replace`")
    assert "bulk=1" in body, (
        "the bulk PUSH loop must mark its calls — the endpoint defaults to "
        "single, so an unmarked loop floods exactly like the 72-message push")


def test_bulk_switch_loop_sends_bulk():
    body = _loop_body("const _qs = c.rk", "/switch-placement${_qs}")
    assert "bulk=1" in body


def test_per_row_actions_do_not_send_bulk():
    """The discriminator for the whole tag: ONLY the two bulk loops mark bulk.
    A blanket `bulk=1` on every /replace call would kill single-action
    previews — the exact regression v1.23.46 was written to prevent.

    Stated as "nowhere outside the two loop bodies", not as an occurrence
    count: the switch loop's ternary carries the flag in BOTH branches, so a
    count pins an accident of formatting rather than the property."""
    live = _live_js()
    for start, end in (("const _cP = [];",
                        "`/api/items/${c.mt}/${c.id}/replace`"),
                       ("const _qs = c.rk", "/switch-placement${_qs}")):
        i = live.index(start)
        j = live.index(end, i) + len(end)
        live = live[:i] + live[j:]          # excise the sanctioned loop
    assert "bulk=1" not in live, (
        "a call site OUTSIDE the two bulk loops sends bulk=1 — single actions "
        "must stay immediate + rich (v1.23.46)")


# ── 3. Discord 429 retries instead of dropping ───────────────────────────

class _Resp:
    def __init__(self, status, payload=None):
        self.status_code = status
        self._payload = payload or {}
        self.text = json.dumps(self._payload)

    def json(self):
        return self._payload


def _fake_client_factory(responses: list, calls: list):
    class _C:
        def __init__(self, *a, **k):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def post(self, url, **kw):
            calls.append(url)
            return responses[len(calls) - 1]
    return _C


def test_429_is_retried_after_the_advertised_wait(tmp_path, monkeypatch):
    """Discord names the wait; pre-fix motif discarded the message anyway."""
    import httpx
    from app.core import notify as n
    calls: list = []
    slept: list = []
    monkeypatch.setattr(httpx, "Client", _fake_client_factory(
        [_Resp(429, {"retry_after": 1.5}), _Resp(204)], calls))
    monkeypatch.setattr(n.time, "sleep", lambda s: slept.append(s))
    att = tmp_path / "t.jpg"
    att.write_bytes(b"jpegbytes")

    ok = n._send_discord_embed(
        "https://discord.com/api/webhooks/123/tok",
        title="t", body="b", notify_type="info", attach_path=str(att))

    assert ok is True, "a 429 that Discord invited us to retry must not drop"
    assert len(calls) == 2, f"expected one retry, got {len(calls)} attempts"
    assert slept == [1.5], slept


def test_429_wait_is_capped(tmp_path, monkeypatch):
    """A global 429 can name minutes. Blocking the dispatch thread that long is
    worse than losing one message — clamp, don't obey blindly."""
    import httpx
    from app.core import notify as n
    calls: list = []
    slept: list = []
    monkeypatch.setattr(httpx, "Client", _fake_client_factory(
        [_Resp(429, {"retry_after": 600}), _Resp(204)], calls))
    monkeypatch.setattr(n.time, "sleep", lambda s: slept.append(s))
    att = tmp_path / "t.jpg"
    att.write_bytes(b"jpegbytes")

    n._send_discord_embed(
        "https://discord.com/api/webhooks/123/tok",
        title="t", body="b", notify_type="info", attach_path=str(att))

    assert slept == [n._DISCORD_429_MAX_WAIT_S], slept


def test_second_429_gives_up_rather_than_looping(tmp_path, monkeypatch):
    """Bounded at ONE retry. An unbounded loop on a rate-limited webhook would
    stall the worker's notify path for the whole burst."""
    import httpx
    from app.core import notify as n
    calls: list = []
    monkeypatch.setattr(httpx, "Client", _fake_client_factory(
        [_Resp(429, {"retry_after": 0.1}), _Resp(429, {"retry_after": 0.1})],
        calls))
    monkeypatch.setattr(n.time, "sleep", lambda s: None)
    att = tmp_path / "t.jpg"
    att.write_bytes(b"jpegbytes")

    ok = n._send_discord_embed(
        "https://discord.com/api/webhooks/123/tok",
        title="t", body="b", notify_type="info", attach_path=str(att))

    assert ok is False
    assert len(calls) == 2, f"must stop at 2 attempts, made {len(calls)}"


def test_non_429_failure_still_fails_fast(tmp_path, monkeypatch):
    """A 500 carries no retry_after — retrying it is just latency."""
    import httpx
    from app.core import notify as n
    calls: list = []
    monkeypatch.setattr(httpx, "Client", _fake_client_factory(
        [_Resp(500, {"message": "boom"}), _Resp(204)], calls))
    att = tmp_path / "t.jpg"
    att.write_bytes(b"jpegbytes")

    ok = n._send_discord_embed(
        "https://discord.com/api/webhooks/123/tok",
        title="t", body="b", notify_type="info", attach_path=str(att))

    assert ok is False
    assert len(calls) == 1, "a 500 must not be retried"


def test_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
