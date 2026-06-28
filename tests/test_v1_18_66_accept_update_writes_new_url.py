"""v1.18.66 — ACCEPT UPDATE must commit the new URL into themes.

the user's testing of v1.18.65 on "Am I Actually the Strongest?":

  > after doing the accept update see the tdb pill briefly turn red
  > then to green where it should be. Big problem though is it looked
  > like it updated to the old themerrdb link and not the actual
  > updated pending one.

The screenshot after ACCEPT UPDATE showed:
  themerrdb url:    https://www.youtube.com/watch?v=kEp_ZMPWWdU  ← STALE
  applied url:      https://www.youtube.com/watch?v=kEp_ZMPWWdU themerrdb
  video id:         kEp_ZMPWWdU
  ACCEPT provenance: old=je_uIV5zv5c new=8budHRQkBLU ← what was accepted

The download recorded video_id=kEp_ZMPWWdU and the local file's
source_video_id is kEp_ZMPWWdU. The user got the OLD content
(the dead TDB URL) even though they explicitly accepted 8budHRQkBLU.

## Root cause

v1.14.55 / v1.18.62: when sync detects a URL change on a row with a
user override, it does NOT update themes.youtube_url (the override
is the authoritative source). The new URL goes into
pending_updates.new_youtube_url, ready for user decision.

ACCEPT UPDATE then:
1. Deletes the override (correct).
2. Sets pending_updates.decision = 'accepted' (correct).
3. Enqueues a download (correct).
4. ❌ NEVER updates themes.youtube_url to the accepted URL.

Worker download then runs `yt_url = override or theme.youtube_url`
— override is gone → falls back to themes.youtube_url which is
STILL the pre-sync OLD TDB URL. Downloads the wrong content.

## Fix

ACCEPT UPDATE handler must UPDATE themes.youtube_url + youtube_video_id
to the accepted new_tdb_url, AND clear failure_kind/failure_message/
failure_acked_at on themes (the failure was on the OLD URL — the user
is moving past it). Mirrors sync.py's non-override row UPDATE branch
which DOES write the new URL when there's nothing to preserve.

The brief "red flash" the user saw on ACCEPT is also resolved by
clearing failure_kind in the same write — pre-fix, pending_update
flag dropped (decision='accepted'), the old failure_kind was still
set, pill flashed red before the download cleared it. Now it goes
straight to green.

v1.13.74 archaeology ("don't clear failure_kind on REVERT") applies
to REVERT specifically — REVERT may be the user escaping a failed
ACCEPT and clearing the flag would lie about the broken upstream.
ACCEPT is the OPPOSITE direction — the user committed to the new
URL, prior failure on the old URL no longer applies.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


def _accept_handler_body() -> str:
    """Return the api_accept_update handler body (the function's
    full source — used by the static-string assertions below)."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    start = src.index("async def api_accept_update(")
    # Walk to the next `@app.post` or `async def` past the function.
    end = src.index("@app.post", start + 1)
    return src[start:end]


# ── Critical write: new URL lands in themes ─────────────────


def test_accept_writes_new_tdb_url_into_themes():
    """ACCEPT UPDATE must UPDATE themes.youtube_url to new_tdb_url
    inside the transaction. Pre-fix the new URL only lived in
    pending_updates.new_youtube_url + the audit log — the worker's
    download path read themes.youtube_url and got the stale OLD
    URL. Pin the UPDATE statement shape so a refactor can't
    silently drop it."""
    body = _accept_handler_body()
    assert "UPDATE themes" in body, (
        "v1.18.66: ACCEPT UPDATE must write the accepted URL into "
        "themes.youtube_url. Worker's URL resolution falls back to "
        "this column when no override exists. the user's data-integrity "
        "repro: ACCEPT wrote nothing to themes → worker downloaded "
        "the OLD dead URL despite the audit log showing the new one."
    )
    # The UPDATE must set youtube_url and youtube_video_id.
    assert "youtube_url = ?" in body
    assert "youtube_video_id = ?" in body


def test_accept_extracts_video_id_for_themes_write():
    """The themes.youtube_video_id column must get the extracted
    video id of new_tdb_url. Pre-fix this column stayed at the
    old vid, leaving the row's video id display + downstream
    diff-tile lookups pointing at the wrong vid."""
    body = _accept_handler_body()
    # The extract_video_id call must operate on new_tdb_url.
    assert "extract_video_id(new_tdb_url)" in body, (
        "v1.18.66: themes.youtube_video_id must be derived from "
        "new_tdb_url via extract_video_id"
    )


def test_accept_clears_failure_kind_on_themes():
    """ACCEPT UPDATE must clear themes.failure_kind / failure_message
    / failure_acked_at on the same write. Pre-fix the row's old
    failure_kind survived ACCEPT, causing a brief red pill flash
    between "pending_update flag drops" and "successful download
    clears failure". the user's repro: 'see the tdb pill briefly
    turn red then to green'."""
    body = _accept_handler_body()
    # All three failure columns must be NULLed.
    assert "failure_kind = NULL" in body
    assert "failure_message = NULL" in body
    assert "failure_acked_at = NULL" in body


def test_accept_v1_18_66_marker_present():
    """v1.18.66 archaeology marker must sit in the handler body so a
    future refactor that "simplifies" the UPDATE has to confront the
    data-integrity history."""
    body = _accept_handler_body()
    assert "v1.18.66" in body, (
        "v1.18.66: marker required in ACCEPT UPDATE handler"
    )
    # Repro reference.
    assert "kEp_ZMPWWdU" in body or "8budHRQkBLU" in body, (
        "v1.18.66: repro URL reference helps future code archaeology"
    )


def test_accept_themes_update_gated_on_new_tdb_url_truthy():
    """The UPDATE must be gated on `if new_tdb_url` — pending_updates
    rows can theoretically have NULL new_youtube_url (the schema
    allows it for the cross-match prompt case where new_video_id
    was unparseable). Don't blow away themes.youtube_url with NULL
    in that pathological case."""
    body = _accept_handler_body()
    # The write block sits inside `if new_tdb_url:` gate.
    update_idx = body.index("UPDATE themes")
    pre_update = body[:update_idx]
    # The closest preceding control statement must reference
    # new_tdb_url truthiness.
    assert "if new_tdb_url:" in pre_update[-300:], (
        "v1.18.66: gate the themes UPDATE on `if new_tdb_url:` "
        "so a NULL new_youtube_url doesn't NULL out the committed URL"
    )


def test_accept_update_runs_inside_the_transaction():
    """The UPDATE must run inside the existing `with get_conn(db)
    as conn, transaction(conn):` block so the override delete + the
    decision write + the themes update + the download enqueue all
    commit atomically. A failure between writes shouldn't leave
    the row half-converted."""
    body = _accept_handler_body()
    # Find the transaction context block; the UPDATE must occur
    # before the block ends. Use the _enqueue_download call as the
    # end anchor — that's the last thing inside the txn.
    txn_idx = body.index("with get_conn(db) as conn, transaction(conn):")
    enqueue_idx = body.index("_enqueue_download(", txn_idx)
    txn_body = body[txn_idx:enqueue_idx]
    assert "UPDATE themes" in txn_body, (
        "v1.18.66: themes UPDATE must occur inside the same "
        "transaction as the override delete + decision write"
    )


def test_accept_update_writes_after_override_delete():
    """Sequence: override DELETE first, then themes UPDATE. The
    UPDATE relies on the override being gone (otherwise the next
    sync may overwrite themes.youtube_url and undo the accept).
    Pin the order so a refactor doesn't accidentally invert it."""
    body = _accept_handler_body()
    delete_idx = body.index("DELETE FROM user_overrides")
    update_idx = body.index("UPDATE themes")
    assert delete_idx < update_idx, (
        "v1.18.66: override DELETE must precede themes UPDATE — "
        "otherwise a subsequent sync could re-preserve the OLD "
        "themes.youtube_url because an override still exists"
    )


# ── Worker URL resolution unchanged ─────────────────────────


def test_worker_download_url_resolution_unchanged():
    """The worker's `yt_url = override or theme.youtube_url`
    resolution is unchanged — the bug was upstream of it. Pin
    that the worker still reads themes.youtube_url as the
    fallback, so v1.18.66's fix actually plumbs through."""
    worker = (REPO / "app" / "core" / "worker.py").read_text()
    assert 'override["youtube_url"] if override else theme["youtube_url"]' in worker, (
        "worker fallback to themes.youtube_url is the contract "
        "v1.18.66 relies on — ACCEPT UPDATE writes the new URL "
        "to themes so this fallback returns the right value"
    )
