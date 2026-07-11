"""v0.51.129 — manual REFRESH PLEX forces a full enum (bypasses the CCA-skip).

Code-review follow-up to v0.51.128. The reaper miss-counter only advances on
enums that actually WALK the section, but the contentChangedAt-skip short-
circuits before the reaper. After a removal bumps contentChangedAt once, later
enums skip until the 24h-overdue bypass — so a genuine removal would take ~24h
to reap and a manual REFRESH couldn't force it (run_plex_enum had no force path).

v0.51.129 threads a `force` flag: user-initiated refresh endpoints stamp
`force=true` on the plex_enum job → `_do_plex_enum` passes `force=True` →
`run_plex_enum` bypasses the skip. Cron + the post-sync cascade omit it, so the
skip optimization is preserved for automatic enums.

The behavioral bypass (force=True runs the enum even when CCA matches) is proved
in test_v1_14_74_content_changed_at_delta_gate.py::
test_force_bypasses_skip_even_when_cca_matches. These are the plumbing pins.
"""
from __future__ import annotations

import inspect
import sqlite3
from pathlib import Path

import pytest

from app.core import plex_enum
from app.core.plex import PlexConfig
from app.core.plex_enum import _REAP_MISS_THRESHOLD

# reuse the established contentChangedAt-skip harness (cross-test import is the
# repo idiom — test_v1_23_77 imports the same _seed_db).
from tests.test_v1_14_74_content_changed_at_delta_gate import _seed_db

REPO = Path(__file__).resolve().parent.parent
WORKER_PY = (REPO / "app" / "core" / "worker.py").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()
NOW = "2099-01-01T00:00:00+00:00"  # far-future last_seen → never "overdue"


def test_run_plex_enum_accepts_force_kwarg():
    sig = inspect.signature(plex_enum.run_plex_enum)
    assert "force" in sig.parameters
    assert sig.parameters["force"].default is False


def test_worker_reads_and_forwards_payload_force():
    # _do_plex_enum reads the payload flag and threads it into run_plex_enum.
    assert 'force_enum = bool(payload.get("force"))' in WORKER_PY
    assert "force=force_enum," in WORKER_PY


def test_post_sync_cascade_does_not_force():
    # The automatic post-sync cascade payload must NOT carry force — cron-cadence
    # enums keep the contentChangedAt-skip; only user refreshes force.
    assert '{"section_id": sid, "scope": "cascade"}' in WORKER_PY
    # the cascade INSERT block has no force flag adjacent to its scope tag.
    cascade_idx = WORKER_PY.index('"scope": "cascade"')
    assert '"force"' not in WORKER_PY[cascade_idx:cascade_idx + 200]


def test_manual_refresh_endpoints_stamp_force():
    # All four user-initiated plex_enum enqueue sites carry force=True:
    #   per-section refresh, tab-scoped REFRESH FROM PLEX, global scan_all,
    #   and manual discovery scan_all.
    assert '{"section_id": section_id, "force": True}' in API_PY
    assert '{"scope": "scan_all", "force": True}' in API_PY
    assert API_PY.count('"force": True') >= 4


# ── end-to-end: force-bypass + the 2-miss reaper working together ─────────────

class _ForceFakeClient:
    """PlexClient stub: live contentChangedAt = `cca`, section walk returns
    exactly `items` (the removed row is simply absent)."""

    def __init__(self, cca, items):
        self._cca, self._items = cca, items

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def discover_sections(self):
        from app.core.plex import PlexSection
        return [PlexSection(section_id="1", uuid="u1", title="Movies",
                            type="movie", agent="a", language="en",
                            location_paths=[], content_changed_at=self._cca)]

    def enumerate_section_items(self, **kw):
        return self._items

    def enumerate_collections_for_section(self, **kw):
        return []


def _keeper():
    from app.core.plex import PlexLibraryItem
    return PlexLibraryItem(
        rating_key="KEEP", section_id="1", media_type="movie", title="Keeper",
        year="2020", guid_imdb=None, guid_tmdb=None, guid_tvdb=None,
        folder_path="/data/movies/Keeper", has_theme=False, plex_theme_uri="")


def _seed_rows(db):
    # a keeper Plex keeps returning + a THEMED removed row already at 1 miss
    # (one below the reap threshold), so a SINGLE walked enum crosses it.
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type, title,"
            " year, has_theme, local_theme_file, folder_path, first_seen_at,"
            " last_seen_at) VALUES ('KEEP','1','movie','Keeper','2020',0,0,"
            "'/data/movies/Keeper',?,?)", (NOW, NOW))
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type, title,"
            " year, has_theme, local_theme_file, guid_tmdb, consecutive_missing,"
            " folder_path, first_seen_at, last_seen_at) VALUES ('STALE','1',"
            "'movie','Gone','2020',1,0,55555,?,'/data/movies/Gone',?,?)",
            (_REAP_MISS_THRESHOLD - 1, NOW, NOW))
        conn.commit()


def _present(db, rk):
    with sqlite3.connect(db) as conn:
        return conn.execute(
            "SELECT 1 FROM plex_items WHERE rating_key=?", (rk,)).fetchone() \
            is not None


@pytest.fixture
def _cfg(monkeypatch):
    monkeypatch.setattr("app.core.notify.dispatch", lambda *a, **k: None)
    return PlexConfig(url="http://x", token="t",
                      movie_section="1", tv_section="2")


def test_forced_refresh_reaps_removed_item_when_cca_unchanged(
        tmp_path, monkeypatch, _cfg):
    """THE integration guarantee: on a section whose contentChangedAt is
    UNCHANGED (so a cron enum would skip it), a forced REFRESH walks the section
    and the reaper reaps a genuinely-removed themed row that has crossed the
    miss threshold. Proves force-bypass (v0.51.129) + the 2-miss reaper
    (v0.51.128) work end-to-end through run_plex_enum in one call."""
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    db = _seed_db(tmp_path, stored_cca="CCA1")
    _seed_rows(db)
    # live CCA MATCHES stored → this section would be skipped without force.
    monkeypatch.setattr(
        plex_enum, "PlexClient", lambda cfg: _ForceFakeClient("CCA1", [_keeper()]))
    plex_enum.run_plex_enum(db, _cfg, force=True)
    assert _present(db, "STALE") is False, (
        "forced refresh must bypass the CCA-skip and reap the removed row")
    assert _present(db, "KEEP") is True


def test_unforced_refresh_skips_and_leaves_row_intact(
        tmp_path, monkeypatch, _cfg):
    """The cron-safe negative: same unchanged-CCA section WITHOUT force is
    skipped (the reaper never runs), so the removed row survives with its
    counter untouched — the skip optimization is preserved for automatic enums."""
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    db = _seed_db(tmp_path, stored_cca="CCA1")
    _seed_rows(db)
    monkeypatch.setattr(
        plex_enum, "PlexClient", lambda cfg: _ForceFakeClient("CCA1", [_keeper()]))
    stats = plex_enum.run_plex_enum(db, _cfg)  # force defaults to False
    assert stats["skipped_unchanged"] == 1, "unchanged CCA + no force must skip"
    assert _present(db, "STALE") is True, "a skipped enum must not reap"
    with sqlite3.connect(db) as conn:
        cm = conn.execute(
            "SELECT consecutive_missing FROM plex_items WHERE rating_key='STALE'"
        ).fetchone()[0]
    assert cm == _REAP_MISS_THRESHOLD - 1, "a skipped enum must not touch the counter"
