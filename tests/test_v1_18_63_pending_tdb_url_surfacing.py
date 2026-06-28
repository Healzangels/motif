"""v1.18.63 — surface the pending TDB URL + fix stale thumbnail.

the user's deploy report on the v1.18.61 build (the v1.18.62 fix
landed but the further confusion was still there):

> "the proposed video is the new url from themerrdb which opens
>  to a working video and makes sense being offered as an
>  upgrade ... but not sure what the broken thumbnail below is
>  coming from also the themerrdb url listed is still the old
>  url not the new updated url so when clicking probe tdb it
>  shows failed even though the new url I have confirmed does
>  work. it seems the themerrdb url in the info isn't being
>  updated to the new url that themerrdb is now providing so
>  it masks as a red pill url since the old url probe does
>  indeed fail"

Three bugs:

## A — info card "themerrdb url" shows stale committed URL

`themes.youtube_url` is the URL motif's SYNC last committed.
`pending_updates.new_youtube_url` is what TDB CURRENTLY claims.
Pre-v1.18.63 the info card always displayed
`themes.youtube_url`, so when TDB had pushed a fix (committed
URL still dead, new URL alive), the "themerrdb url" line
showed the dead URL. Operator's mental model: "themerrdb url"
= what TDB has now.

Fix: when a pending_update is awaiting decision, prefer
`pu.new_youtube_url`. Add a "(pending — ACCEPT UPDATE to
commit)" suffix so the user knows the displayed URL hasn't
been committed yet.

## B — PROBE TDB URL probes the committed (stale) URL

Same root cause as A. The button click hits
`/api/items/{mt}/{id}/probe-tdb` which read
`themes.youtube_url`. With TDB pushing a working fix in
pending_updates, probing the committed dead URL returned
DEAD → red TDB ✗ pill even though TDB had fixed it.

Fix: backend prefers `pu.new_youtube_url` when a
pending_update awaits decision. Critically, when probing
the PENDING URL, the probe must NOT write `failure_kind`
back to themes — those columns describe the committed URL's
state. A pending-URL probe just stamps last_probed_at + returns
the result; the user makes the call via ACCEPT UPDATE.

## C — broken thumbnail from stale lf.source_video_id

the user's "Am I Actually the Strongest?" row had:
  - currentUrl = je_uIV5zv5c (user override)
  - lf.source_video_id = kEp_ZMPWWdU (the pre-override TDB id)

The info card's "click to watch on YouTube" thumbnail used
`lf.source_video_id` to build the img.youtube.com URL —
which pointed at the dead/removed kEp_ video. YouTube serves
its default placeholder ("..." icon) for missing videos,
hence the broken-looking thumbnail.

Fix: for YouTube URLs, extract the video id from `currentUrl`
FIRST. URL parsing is deterministic and always matches what's
being rendered. Fall back to `lf.source_video_id` only for
non-YT URLs or parse failures. The 'recovered' (v1.18.13) and
'sc-' (v1.18.21) guards stay in the fallback path.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
APP_JS = REPO / "app" / "web" / "static" / "app.js"
API_PY = REPO / "app" / "web" / "api.py"


# ── Fix A: pending TDB URL surfaces in info card ─────────────


def test_info_card_prefers_pending_tdb_url():
    """When `data.pending_update.new_youtube_url` is set and
    `decision='pending'`, the info card's tdbUrl must come from
    the pending value, not `t.youtube_url`."""
    src = APP_JS.read_text()
    # The new ternary must compute _pendingTdbUrl from
    # data.pending_update.
    assert "_pendingTdbUrl" in src
    assert "data.pending_update.new_youtube_url" in src, (
        "v1.18.63: must read pending_update.new_youtube_url"
    )
    # The dispatch must prefer pending → committed.
    assert "const tdbUrl = _pendingTdbUrl || _committedTdbUrl" in src, (
        "v1.18.63: tdbUrl assignment must prefer pending over committed"
    )


def test_info_card_label_shows_pending_suffix():
    """When the displayed TDB URL is the pending one, the label
    must include a '(pending — ACCEPT UPDATE to commit)' hint
    so the user knows the URL hasn't been committed yet."""
    src = APP_JS.read_text()
    assert "(pending — ACCEPT UPDATE to commit)" in src, (
        "v1.18.63: pending suffix string must appear in the label"
    )
    # Must be gated on tdbUrlIsPending so T rows + accepted rows
    # don't get the suffix.
    assert "tdbUrlIsPending" in src


def test_pending_only_when_decision_pending():
    """The _pendingTdbUrl computation must require
    `decision === 'pending'` so accepted/declined updates don't
    leak into the display."""
    src = APP_JS.read_text()
    fn_idx = src.index("_pendingTdbUrl")
    block = src[fn_idx:fn_idx + 600]
    assert "decision === 'pending'" in block


def test_pending_only_when_kind_is_not_urls_match():
    """`pending_updates.kind='urls_match'` is a side-effect-only
    rewrite of the youtube_url (different format, same video).
    Don't display it as a pending TDB-URL change."""
    src = APP_JS.read_text()
    fn_idx = src.index("_pendingTdbUrl")
    block = src[fn_idx:fn_idx + 600]
    assert "kind !== 'urls_match'" in block


# ── Fix B: probe-tdb endpoint targets pending URL ───────────


def test_probe_tdb_reads_pending_url():
    """The probe-tdb endpoint must SELECT pending_updates.
    new_youtube_url when one is awaiting decision."""
    src = API_PY.read_text()
    fn_idx = src.index("async def api_probe_tdb(")
    fn_end = src.index("@app.post", fn_idx + 1)
    body = src[fn_idx:fn_end]
    assert "SELECT new_youtube_url FROM pending_updates" in body, (
        "v1.18.63: probe-tdb must SELECT from pending_updates"
    )
    assert "decision = 'pending'" in body
    # Don't include urls_match kind (formatting-only rewrites).
    assert "kind != 'urls_match'" in body


def test_probe_tdb_target_url_prefers_pending():
    """target_url must be set to pending_url || committed_url
    so the probe hits TDB's current claim, not the stale
    committed value."""
    src = API_PY.read_text()
    fn_idx = src.index("async def api_probe_tdb(")
    fn_end = src.index("@app.post", fn_idx + 1)
    body = src[fn_idx:fn_end]
    assert "target_url = pending_url or committed_url" in body, (
        "v1.18.63: target_url must prefer pending over committed"
    )


def test_probe_tdb_skips_failure_writes_for_pending():
    """When probing the pending URL (not the committed one), the
    probe must NOT write `failure_kind` to themes — those columns
    describe the committed URL's state. Pending-URL probe just
    stamps last_probed_at."""
    src = API_PY.read_text()
    fn_idx = src.index("async def api_probe_tdb(")
    fn_end = src.index("@app.post", fn_idx + 1)
    body = src[fn_idx:fn_end]
    # The failure-write block must be guarded.
    assert "if not target_is_pending:" in body, (
        "v1.18.63: failure_kind writes must be gated on "
        "`not target_is_pending` so pending-URL probes don't "
        "mutate themes columns describing the committed URL"
    )


def test_probe_tdb_response_includes_target_is_pending():
    """The probe response must include `target_is_pending` so
    the JS-side handler can render a distinct status message
    when relevant."""
    src = API_PY.read_text()
    fn_idx = src.index("async def api_probe_tdb(")
    fn_end = src.index("@app.post", fn_idx + 1)
    body = src[fn_idx:fn_end]
    assert '"target_is_pending": target_is_pending' in body


# ── Fix C: thumbnail derives ytId from URL first ────────────


def test_ytid_extracted_from_url_first_for_youtube():
    """For YouTube currentUrls, extract video id from URL FIRST
    (deterministic, always matches the rendering URL). Fall
    back to lf.source_video_id only when URL extraction fails
    or the URL isn't YouTube."""
    src = APP_JS.read_text()
    # The new ordering: URL extraction first, lf fallback after.
    idx = src.index("const ytUrl = currentUrl;")
    block = src[idx:idx + 2000]
    # The youtube branch must run first.
    yt_branch_idx = block.index("if (urlSource(currentUrl) === 'youtube')")
    lf_fallback_idx = block.index("(lf && lf.source_video_id)")
    assert yt_branch_idx < lf_fallback_idx, (
        "v1.18.63: URL-extraction branch must run BEFORE the "
        "lf.source_video_id fallback for YouTube URLs"
    )
    # The fallback must be guarded behind `if (!ytId)`.
    fallback_window = block[yt_branch_idx:lf_fallback_idx + 200]
    assert "if (!ytId)" in fallback_window


def test_recovered_and_sc_guards_preserved():
    """The 'recovered' (v1.18.13) and 'sc-' (v1.18.21) sentinel
    guards stay in the fallback path so they continue to fire
    when the URL-extraction branch doesn't apply (e.g., the
    rare non-YT-but-still-watchable case)."""
    src = APP_JS.read_text()
    idx = src.index("const ytUrl = currentUrl;")
    block = src[idx:idx + 2500]
    assert "ytId === 'recovered'" in block, (
        "v1.18.63: 'recovered' guard preserved in fallback"
    )
    assert "ytId.startsWith('sc-')" in block, (
        "v1.18.63: 'sc-' guard preserved in fallback"
    )


# ── Version marker + smoke test ─────────────────────────────


def test_v1_18_63_markers_present():
    assert "v1.18.63" in APP_JS.read_text()
    assert "v1.18.63" in API_PY.read_text()


# ── End-to-end: probe-tdb endpoint targets the pending URL ──


def test_probe_endpoint_targets_pending_url_end_to_end(tmp_path, monkeypatch):
    """Seed a row with a committed URL + a pending URL. Mock
    the probe_youtube_url to capture which URL gets passed.
    The pending URL must be the one probed."""
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from fastapi.testclient import TestClient
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db, get_conn
    from app.web.api import create_app
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    db = settings.db_path
    init_db(db)
    init_auth_schema(db)
    create_admin(db, username="testadmin", password="testpassword")

    ts = "2026-05-22T00:00:00Z"
    committed = "https://www.youtube.com/watch?v=DEAD_committed"
    pending = "https://www.youtube.com/watch?v=NEW_alive_url"
    with get_conn(db) as c:
        c.execute(
            "INSERT INTO plex_sections (section_id, title, type, "
            "                            included, discovered_at, "
            "                            last_seen_at) "
            "VALUES ('1', 'Movies', 'movie', 1, ?, ?)",
            (ts, ts),
        )
        c.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, "
            "                     youtube_url, upstream_source, "
            "                     last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('movie', 9999, 'Test', ?, 'themoviedb', ?, ?)",
            (committed, ts, ts),
        )
        c.execute(
            "INSERT INTO pending_updates "
            "    (media_type, tmdb_id, section_id, "
            "     old_youtube_url, new_youtube_url, decision, "
            "     decision_at, decision_by, detected_at, kind) "
            "VALUES ('movie', 9999, '', ?, ?, 'pending', "
            "        NULL, NULL, ?, 'upstream_changed')",
            (committed, pending, ts),
        )
        c.commit()

    captured = {}

    def fake_probe(url, cookies_file=None):
        captured["url"] = url
        return None  # alive

    monkeypatch.setattr(
        "app.core.downloader.probe_youtube_url", fake_probe,
    )

    client = TestClient(create_app(settings))
    r = client.post(
        "/api/items/movie/9999/probe-tdb",
        headers={"X-Authentik-Username": "testadmin"},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert captured.get("url") == pending, (
        f"v1.18.63: probe must target the pending URL "
        f"({pending!r}), got {captured.get('url')!r}"
    )
    assert body.get("target_is_pending") is True
    assert body.get("url_probed") == pending
