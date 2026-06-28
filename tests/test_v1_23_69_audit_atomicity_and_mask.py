"""v1.23.69 — fresh-audit fixes: a credential-corruption gap + two enqueue races.

From a fresh silent-bug sweep (multi-agent + hand-verified):

  - db_url PATCH keep-on-mask (HIGH): v1.23.62 added the 3rd credential-capable
    sync URL (db_url) to the GET /api/config userinfo MASK, but not to the
    PATCH-side keep-on-`***`-marker guard. So the standard settings SAVE
    round-trip (GET masks → user edits → PATCH posts every field back) wrote the
    masked `https://***@host` literally over a real db_url credential. Added
    db_url to the PATCH guard tuple so the keep-on-mask contract is symmetric.

  - relink_all + section_refresh enqueue races (MED): both run a jobs-table dedup
    SELECT + INSERT in plain autocommit (jobs has no UNIQUE), so two concurrent
    clicks both pass the "already queued?" check and double-insert. Wrapped each
    in BEGIN IMMEDIATE — the v1.23.63 pattern. section_refresh's twin
    (api_library_refresh) was already wrapped; the per-section sibling was missed.

(The scan/decide-finding enqueues have the same shape but dedup against a
WORKER-stamped field, not the jobs table, so a plain wrap wouldn't serialize
them — left for a deeper follow-up; lower severity + the worker re-checks.)
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
API = (REPO / "app" / "web" / "api.py").read_text()


def _handler(name: str) -> str:
    i = API.index(f"async def {name}(")
    nxt = API.find("\n    async def ", i + 1)
    return API[i:nxt if nxt != -1 else i + 6000]


def test_db_url_kept_on_mask_marker_in_patch():
    # the PATCH keep-on-`***` guard must cover all three credential URLs.
    assert 'k in ("git_url", "database_url", "db_url"):' in API, (
        "v1.23.69: db_url must be kept (not overwritten with its mask) on a "
        "config SAVE round-trip"
    )


def test_relink_all_enqueue_is_transactional():
    body = _handler("api_relink_all")
    assert "with get_conn(db) as conn, transaction(conn):" in body, (
        "v1.23.69: the relink dedup SELECT + INSERT must be one BEGIN IMMEDIATE"
    )


def test_section_refresh_enqueue_is_transactional():
    body = _handler("api_libraries_section_refresh")
    assert "with get_conn(db) as conn, transaction(conn):" in body, (
        "v1.23.69: per-section refresh dedup + INSERT must be one BEGIN IMMEDIATE "
        "(its twin api_library_refresh already is)"
    )
    # the dedup it serializes is the jobs-table check (not a worker-stamped field).
    assert "job_type = 'plex_enum'" in body and "json_extract(payload, '$.section_id')" in body
