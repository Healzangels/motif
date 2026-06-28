"""v1.22.75 (audit round 2, Batch C #4) — api.py action-endpoint MEDs.

(1) api_item_audit_clear with section_id ran
`AND (section_id = ? OR section_id IS NULL)` — the OR-NULL half
deleted title-global provenance rows (ADOPT etc.), the exact thing
its docstring promises to preserve. The GET includes NULL rows for
DISPLAY; copying that predicate into the DELETE was the bug. Now
section-scoped only (mirroring api_item_events_clear).

(2) PURGE's v1.20.67 plex_upload-rk collection missed the v1.21.83
edition narrowing: its two plex_items SELECTs had no edition filter,
so an edition-scoped PURGE force-appended SIBLING editions' rks to
rk_zero (has_theme/local_theme_file/verified falsely zeroed,
verify-skipped). Both queries now carry _ed_sql/_ed_args.

(3) PURGE + DELETE computed the Plex API teardown count
(api_deleted) then dropped it — Plex unreachable meant 0 deletions
behind ok:true with only a docker-log WARN (the v1.22.43 fake-success
shape). The count now rides the audit details, the log_event detail,
and the PURGE response; a WARNING log_event fires when plex_upload
placements existed but 0 artifacts were removed.

(4) The PROMOTE BK-no-override defensive place-cancel was title-wide
while its with-override sibling was edition-scoped in v1.21.88 — it
killed sibling editions'/sections' legitimately queued place jobs.
Now mirrors the v1.21.88 payload-edition match.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from test_v1_14_59_recovery_options_behavioral import (  # noqa: F401
    app_client)

REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()
AUTH = {"X-Authentik-Username": "testadmin"}


# ── (1) audit-clear preserves title-global rows ──────────────


def test_section_scoped_audit_clear_preserves_title_global(app_client):
    client, db = app_client
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO audit_events (occurred_at, actor, action,"
            " media_type, tmdb_id, section_id, details)"
            " VALUES ('2026-06-11T00:00:00+00:00','admin','set_url',"
            " 'movie', 100, '1', '{}')")
        conn.execute(
            "INSERT INTO audit_events (occurred_at, actor, action,"
            " media_type, tmdb_id, section_id, details)"
            " VALUES ('2026-06-11T00:00:00+00:00','admin','adopt',"
            " 'movie', 100, NULL, '{}')")
        conn.commit()
    r = client.delete("/api/items/movie/100/audit?section_id=1",
                      headers=AUTH)
    assert r.status_code == 200, r.text
    assert r.json()["deleted"] == 1, (
        "v1.22.75: only the section row may be deleted"
    )
    with sqlite3.connect(db) as conn:
        survivors = conn.execute(
            "SELECT action, section_id FROM audit_events"
            " WHERE tmdb_id = 100").fetchall()
    assert survivors == [("adopt", None)], (
        "the title-global ADOPT row must survive a per-section clear"
    )


def test_title_wide_audit_clear_still_deletes_everything(app_client):
    client, db = app_client
    with sqlite3.connect(db) as conn:
        for sec in ("1", None):
            conn.execute(
                "INSERT INTO audit_events (occurred_at, actor, action,"
                " media_type, tmdb_id, section_id, details)"
                " VALUES ('2026-06-11T00:00:00+00:00','admin','x',"
                " 'movie', 100, ?, '{}')", (sec,))
        conn.commit()
    r = client.delete("/api/items/movie/100/audit", headers=AUTH)
    assert r.status_code == 200 and r.json()["deleted"] == 2


# ── (2) PURGE PU-rk collection carries the edition filter ────


def test_purge_pu_rk_queries_carry_edition_filter():
    i = API_PY.index("plex_upload_rks: set[str] = set()")
    block = API_PY[i:i + 1700]
    assert block.count("+ _ed_sql") >= 1 or block.count("_ed_sql") >= 2, (
        "v1.22.75: both PU-rk plex_items SELECTs must carry _ed_sql"
    )
    assert "*_ed_args" in block
    assert '"WHERE theme_id = ? AND section_id = ?" + _ed_sql' in block


# ── (3) teardown count surfaced ──────────────────────────────


def test_purge_response_and_audit_carry_api_deleted():
    assert '"plex_api_deleted": api_deleted}' in API_PY
    assert API_PY.count('"plex_api_deleted": api_deleted') >= 3, (
        "v1.22.75: api_deleted must ride the PURGE audit details, the "
        "log_event detail, the PURGE response, and the DELETE log"
    )


def test_zero_teardown_for_pu_rows_logs_warning():
    for verb in ("PURGE", "DELETE"):
        i = API_PY.index(
            f"{verb}: Plex API teardown removed 0 artifacts")
        region = API_PY[i - 600:i]
        assert 'level="WARNING"' in region, verb


# ── (4) PROMOTE BK-no-override cancel is edition-scoped ──────


def test_no_override_promote_cancel_edition_scoped():
    i = API_PY.index("no user_override and no backup-only")
    block = API_PY[i:i + 2200]
    j = block.index("UPDATE jobs SET status = 'cancelled'")
    cancel = block[j:j + 700]
    assert "json_extract(payload, '$.edition_key')" in cancel, (
        "v1.22.75: the defensive cancel must mirror the v1.21.88 "
        "payload-edition match"
    )
    assert "_intent_edition" in cancel
