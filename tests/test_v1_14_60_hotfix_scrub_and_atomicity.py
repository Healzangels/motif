"""v1.14.60 HOTFIX — _scrub overbroad 'key' substring + atomicity gaps.

Audit follow-up surfaced 1 HIGH + 2 MEDIUMs from the v1.14.54-57
bundle of changes:

## H1 (CRITICAL HOTFIX): _scrub redacted legitimate non-secret fields

v1.14.54 added `"key"` to `_SCRUB_SUBSTRINGS` to catch `api_key`.
Substring match → ALSO matched `rating_key`, `cache_key`,
`changed_top_keys`, `theme_key`, etc. Every event row written
since v1.14.54 with a `rating_key` in `detail` has `"***REDACTED***"`
in place of the real value — silently destroyed audit log data
needed for diagnosing per-rk Plex-cache races (CLAUDE.md class 1
"phantom P after PURGE").

Fix: drop bare `"key"` from substring set; add `private_key`,
`signing_key`, `encryption_key`, `master_key` for the real
secret-bearing patterns. `api_key` and `apikey` already covered
the prod secret case.

## H2: api_probe_tdb threadpool writes not in transaction

v1.14.54 H3 wrapped api_probe_tdb's sync block in run_in_threadpool
but didn't add `transaction(conn)`. v1.14.55 M10 closed exactly
this shape for `_do_download` — concurrent readers (the topbar
poll, /api/recovery_options, library list) could observe the
intermediate state where themes.failure_kind=NULL was committed
but section_failure_acks DELETE hadn't landed yet. Mirror-
principle drift: v1.14.55 closed _do_download but missed _probe.

Fix: wrap the post-probe write block in `transaction(conn)`.

## H3: _do_relink LIMIT 5000 with no ORDER BY → cancel-resume livelock

v1.14.57 L12 capped the bulk SELECT at LIMIT 5000 to bound writer-
lock hold time, but didn't add ORDER BY. SQLite's row order is
implementation-defined → a CANCEL → re-enqueue cycle on a >5000-
row library can deterministically re-pick the same 5000 rows the
cancelled run already attempted, never reaching the tail.

Fix: ORDER BY p.placed_at ASC so successive sweeps walk older
rows first (forward progress under cancel-retry).
"""
from __future__ import annotations

from pathlib import Path

from app.core.events import _scrub


REPO = Path(__file__).resolve().parent.parent


# ── H1: _scrub no longer redacts legitimate `*_key` fields ───


def test_scrub_does_not_redact_rating_key():
    """rating_key is the canonical Plex item identifier — appears
    in dozens of event details across the codebase. Pre-v1.14.60
    it got `"***REDACTED***"` written to the events table,
    silently destroying the audit log's value for diagnosing
    per-rk races."""
    out = _scrub({"rating_key": "12345", "title": "1408"})
    assert out["rating_key"] == "12345", (
        f"rating_key must NOT be redacted; got {out['rating_key']!r}. "
        "v1.14.60 hotfix removed bare `key` from _SCRUB_SUBSTRINGS."
    )


def test_scrub_does_not_redact_other_non_secret_key_fields():
    """Same fix covers cache_key, changed_top_keys, theme_key,
    event_key, partition_key, primary_key, foreign_key — all
    non-secret fields that the bare-`key` substring caught."""
    out = _scrub({
        "cache_key": "ts:db:foo",
        "changed_top_keys": ["a", "b"],
        "theme_key": "movie:tt0123",
        "partition_key": "p1",
        "primary_key": 42,
        "foreign_key": "fk_users_id",
    })
    for k, expected in [
        ("cache_key", "ts:db:foo"),
        ("changed_top_keys", ["a", "b"]),
        ("theme_key", "movie:tt0123"),
        ("partition_key", "p1"),
        ("primary_key", 42),
        ("foreign_key", "fk_users_id"),
    ]:
        assert out[k] == expected, (
            f"{k} unexpectedly redacted to {out[k]!r}"
        )


# ── H1 sanity: secret-bearing fields STILL redact ───────────


def test_scrub_still_redacts_real_secrets():
    """Sanity: api_key, apikey, password, token, cookie, auth,
    secret, bearer all still get redacted (the v1.14.54 expansion
    survives the hotfix). Plus the new explicit secret-key
    patterns (private_key, signing_key, encryption_key,
    master_key)."""
    secrets = {
        "api_key": "sk_test_xyz",
        "apikey": "abc",
        "password": "hunter2",
        "PASSWORD": "hunter2",  # case-insensitive substring match
        "tdb_token": "tok",
        "session_cookie": "sid",
        "Authorization": "Bearer x",  # contains 'auth'
        "client_secret": "shh",
        "bearer_token": "bt",
        "private_key": "-----BEGIN-----",
        "signing_key": "sk",
        "encryption_key": "ek",
        "master_key": "mk",
    }
    out = _scrub(secrets)
    for k in secrets:
        assert out[k] == "***REDACTED***", (
            f"{k!r} should be redacted but is {out[k]!r}"
        )


def test_scrub_substring_set_no_longer_includes_bare_key():
    """Pin the substring tuple itself so a future contributor
    can't re-add bare `"key"` without this test failing."""
    from app.core.events import _SCRUB_SUBSTRINGS
    assert "key" not in _SCRUB_SUBSTRINGS, (
        "Bare `key` substring is too broad — matches rating_key, "
        "cache_key, changed_top_keys, etc. Add the specific "
        "secret-bearing key pattern instead (e.g. private_key)."
    )
    # The new explicit patterns are present.
    assert "private_key" in _SCRUB_SUBSTRINGS
    assert "api_key" in _SCRUB_SUBSTRINGS  # unchanged from v1.14.54


# ── H2: api_probe_tdb writes wrapped in transaction ─────────


def test_api_probe_tdb_writes_in_transaction():
    """The post-probe write block in api_probe_tdb's _probe_sync
    helper must run inside transaction(conn) so concurrent
    readers don't observe intermediate state. Mirror of v1.14.55
    M10 fix to _do_download."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("def _probe_sync():")
    body = src[fn_anchor:fn_anchor + 15000]
    # The transaction wrap.
    assert "with get_conn(db) as conn, transaction(conn):" in body
    # v1.14.60 marker.
    assert "v1.14.60:" in body
    # Sanity: the failure-clear block is still inside the wrapped block.
    assert "if result is None:" in body
    assert "DELETE FROM section_failure_acks" in body


# ── H3: _do_relink ORDER BY for forward progress ────────────


def test_do_relink_bulk_select_orders_by_placed_at():
    """The bulk-relink SELECT (LIMIT 5000 since v1.14.57) must
    have ORDER BY so successive cancel-retry cycles make forward
    progress instead of livelocking on the same 5000 rows."""
    src = (REPO / "app" / "core" / "worker.py").read_text()
    fn_anchor = src.index("def _do_relink(self, job: sqlite3.Row) -> None:")
    body = src[fn_anchor:fn_anchor + 15000]
    # The ORDER BY clause must precede the LIMIT.
    assert "ORDER BY p.placed_at ASC\n                       LIMIT 5000" in body
    # v1.14.60 marker.
    assert "v1.14.60:" in body


# ── Behavioral: nested-list scrub still works (v1.14.54 M13) ──


def test_scrub_list_recursion_still_works_after_hotfix():
    """Sanity: the v1.14.54 list-recursion fix survives the
    hotfix. A list of dicts containing a token gets scrubbed."""
    out = _scrub({"items": [{"token": "abc", "label": "ok"}]})
    assert out["items"][0]["token"] == "***REDACTED***"
    assert out["items"][0]["label"] == "ok"
