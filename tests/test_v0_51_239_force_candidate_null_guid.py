"""v0.51.239 — the force walk's exclusion gates were inert for NULL-guid rows.

The candidate query's three NOT EXISTS gates compared `pi.guid_tmdb`. The FORCE
path deliberately DROPS the `guid_tmdb IS NOT NULL` filter so a no-TDB row (a
collection, the operator's A24 Films repro) can be captured — which means every
one of those comparisons was `= NULL` for exactly the rows force exists to
serve, so each gate was vacuously true and excluded nothing.

The one that matters is the allow_existing_local branch, whose stated purpose is
"exclude rows that ALREADY have plex_cloud local_files — nothing to do, the row
is already cloud-backed... so the click is a no-op." For a NULL-guid collection
that no-op could never fire: DOWNLOAD PLEX BACKUP re-fetched the bytes from Plex
and rewrote local_files every single time.

Fix: the gates resolve the id the row will actually be keyed to, mirroring
_resolve_or_mint_tmdb_id (real guid wins, else the orphan id via theme_id). On
the non-force walk guid_tmdb is NOT NULL by construction, so that path is
provably unchanged.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from app.core.db import get_conn, init_db

NOW = datetime.now(timezone.utc).isoformat(timespec="seconds")


@pytest.fixture
def db(tmp_path):
    d = tmp_path / "m.db"
    init_db(d)
    with get_conn(d) as c:
        c.execute("INSERT INTO plex_sections (section_id, title, type, included,"
                  " discovered_at, last_seen_at) "
                  "VALUES ('9','Collections','movie',1,?,?)", (NOW, NOW))
        c.commit()
    return d


def _seed_backed_up_null_guid_collection(db):
    """A collection with NULL guid_tmdb that has ALREADY been captured: the
    force path minted it a plex_orphan theme (id 7 / tmdb -1), stamped
    plex_items.theme_id, and wrote its plex_cloud local_files row."""
    with get_conn(db) as c:
        c.execute("INSERT INTO themes (id, media_type, tmdb_id, title, "
                  " upstream_source, last_seen_sync_at, first_seen_sync_at) "
                  "VALUES (7,'collection',-1,'A24 Films','plex_orphan',?,?)",
                  (NOW, NOW))
        c.execute("INSERT INTO plex_items (rating_key, section_id, media_type,"
                  " guid_tmdb, theme_id, title, edition_key, has_theme,"
                  " first_seen_at, last_seen_at) "
                  "VALUES ('c1','9','collection',NULL,7,'A24 Films','',1,?,?)",
                  (NOW, NOW))
        c.execute("INSERT INTO local_files (media_type, tmdb_id, section_id,"
                  " edition_key, file_path, source_video_id, source_kind,"
                  " downloaded_at) "
                  "VALUES ('collection',-1,'9','','c.mp3','','plex_cloud',?)",
                  (NOW,))
        c.commit()


class _ProbeClient:
    """Records which rating_keys the walker actually probes. A row is only
    probed if it SURVIVED the candidate query — so this observes the real
    production gates instead of re-deriving them in the test (which would
    pass even against reverted SQL)."""

    def __init__(self):
        self.asked: list[str] = []

    def get_themes(self, rating_key):
        self.asked.append(rating_key)
        return {"ok": True, "http_status": 200, "body": {}}


def _offered(db, *, force):
    """True when the real identify_c1_rows walk still considers 'c1'."""
    from app.core.cloud_theme_backup import identify_c1_rows
    cl = _ProbeClient()
    with get_conn(db) as c:
        identify_c1_rows(c, cl, inter_call_sleep_s=0, use_cursor=False,
                         force=force, allow_existing_local=force)
    return "c1" in cl.asked


def test_already_backed_null_guid_row_is_not_offered_again(db):
    """The regression: pre-fix this returned True — the row was re-offered and
    DOWNLOAD PLEX BACKUP re-fetched ~1MB from Plex on every click."""
    _seed_backed_up_null_guid_collection(db)
    assert _offered(db, force=True) is False, (
        "a row already cloud-backed under its minted orphan id must be a no-op")


def test_a_not_yet_backed_null_guid_row_is_still_offered(db):
    """The fix must not over-exclude: with no plex_cloud row yet, force must
    still offer it — that capture is the whole point of the force path."""
    with get_conn(db) as c:
        c.execute("INSERT INTO plex_items (rating_key, section_id, media_type,"
                  " guid_tmdb, theme_id, title, edition_key, has_theme,"
                  " first_seen_at, last_seen_at) "
                  "VALUES ('c1','9','collection',NULL,NULL,'New Coll','',1,?,?)",
                  (NOW, NOW))
        c.commit()
    assert _offered(db, force=True) is True


def test_the_non_force_walk_is_unchanged(db):
    """guid_tmdb IS NOT NULL is enforced there, so the COALESCE reduces to the
    old expression — the NULL-guid row simply isn't a candidate at all."""
    _seed_backed_up_null_guid_collection(db)
    assert _offered(db, force=False) is False, (
        "the strict walk still filters NULL-guid rows out entirely")


def test_all_three_gates_use_the_shared_resolver(db):
    """Mechanism guard. Two local_files gates + one placements gate; a bare
    pi.guid_tmdb in any of them re-opens the hole for force rows."""
    import inspect
    from app.core import cloud_theme_backup as ctb
    src = inspect.getsource(ctb.identify_c1_rows)
    code = "\n".join(ln for ln in src.splitlines()
                     if not ln.lstrip().startswith("#"))
    assert code.count("_CTB_EFFECTIVE_TMDB") == 3, (
        "expected the two local_files gates + the placements gate to resolve "
        f"the effective id; found {code.count('_CTB_EFFECTIVE_TMDB')}")
    assert "lf.tmdb_id = pi.guid_tmdb" not in code
    assert "pl.tmdb_id = pi.guid_tmdb" not in code


def test_resolver_mirrors_resolve_or_mint_precedence(db):
    """The SQL and the Python resolver must agree on WHICH id, or a row could
    be excluded under one id and written under another."""
    from app.core.cloud_theme_backup import _resolve_or_mint_tmdb_id
    _seed_backed_up_null_guid_collection(db)
    with get_conn(db) as c:
        row = c.execute("SELECT * FROM plex_items WHERE rating_key='c1'").fetchone()
        py = _resolve_or_mint_tmdb_id(c, row, "collection", mint=False)
        from app.core.cloud_theme_backup import _CTB_EFFECTIVE_TMDB
        sql = c.execute(
            f"SELECT {_CTB_EFFECTIVE_TMDB} AS eff FROM plex_items pi "
            "WHERE pi.rating_key='c1'").fetchone()["eff"]
    assert py == sql == -1, (py, sql)
