"""v0.51.356 — the slow-/api/library warning answers its own question.

v1.23.70 added the warning and asked, in its own text, "subquery cost or sync/enum CPU contention?" — then logged
nothing that could tell those apart. The operator's persistent log holds 67 of them (2026-08-14 → 2026-09-28),
and the shape of the sample is the giveaway: every line is an unfiltered `status=all` tab view, including

    tab=movies rows=5 total=5 → 868.3ms      tab=collections rows=5 total=5 → 1040.9ms

Five rows cannot cost 868 ms of query work, and a local replay of the worst-looking view at four times the
operator's library size costs 8 ms. So the line was pointing at the wrong suspect and had no way to say so.

Each request now carries a phase ledger — connection, count, ids, hydrate, meta, stats (with the number of
stat() calls) — in the warning and in the response beside query_ms. An even spread across the phases is the box
(I/O or CPU starvation); one fat phase is motif's own work. The warning also samples what motif itself was
doing, which is the other half of the question v1.23.70 asked.
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

AUTH = {"X-Authentik-Username": "testadmin"}


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@pytest.fixture
def lib(tmp_path, monkeypatch):
    """A small TV library: 12 themed rows (file + placement) and 4 unthemed."""
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web.api import create_app
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    themes_dir = tmp_path / "themes"
    (themes_dir / "tv").mkdir(parents=True)
    settings._cfg.paths.themes_dir = str(themes_dir)
    init_db(settings.db_path)
    init_auth_schema(settings.db_path)
    create_admin(settings.db_path, username="testadmin", password="testpassword")
    now = _now()
    with sqlite3.connect(settings.db_path) as c:
        c.execute("INSERT INTO plex_sections (section_id,title,type,is_anime,is_4k,themes_subdir,included,"
                  "discovered_at,last_seen_at) VALUES ('2','TV','show',0,0,'tv',1,?,?)", (now, now))
        for i in range(16):
            tmdb = 500 + i
            tid = c.execute("INSERT INTO themes (media_type,tmdb_id,title,upstream_source,last_seen_sync_at,"
                            "first_seen_sync_at,youtube_url) VALUES ('tv',?,?,'imdb',?,?,?)",
                            (tmdb, f"Show {i:02d}", now, now,
                             f"https://www.youtube.com/watch?v=v{i:010d}")).lastrowid
            c.execute("INSERT INTO plex_items (rating_key,section_id,media_type,theme_id,guid_tmdb,title,year,"
                      "edition_key,folder_path,has_theme,local_theme_file,plex_independent_theme,"
                      "plex_theme_verified_ok,first_seen_at,last_seen_at)"
                      " VALUES (?,'2','show',?,?,?,2024,'',?,?,0,0,1,?,?)",
                      (f"rk{tmdb}", tid, tmdb, f"Show {i:02d}", f"/media/tv/{i}", 1 if i < 12 else 0, now, now))
            if i < 12:
                (themes_dir / "tv" / f"{i}.mp3").write_bytes(b"\0" * 32)
                c.execute("INSERT INTO local_files (media_type,tmdb_id,section_id,edition_key,file_path,"
                          "downloaded_at,source_video_id,provenance,source_kind)"
                          " VALUES ('tv',?,'2','',?,?,?,'auto','themerrdb')",
                          (tmdb, f"tv/{i}.mp3", now, f"v{i:010d}"))
                c.execute("INSERT INTO placements (media_type,tmdb_id,section_id,edition_key,media_folder,"
                          "placed_at,placement_kind,plex_refreshed,theme_present)"
                          " VALUES ('tv',?,'2','',?,?,'hardlink',1,1)", (tmdb, f"/media/tv/{i}", now))
        c.commit()
    return TestClient(create_app(settings)), settings.db_path


# ── the ledger ───────────────────────────────────────────────


def test_every_request_reports_where_its_time_went(lib):
    client, _ = lib
    body = client.get("/api/library?tab=tv&fourk=0", headers=AUTH).json()
    ph = body["query_phases"]
    for phase in ("conn", "count", "ids", "hydrate", "meta"):
        assert phase in ph, (phase, ph)
    assert sum(v for k, v in ph.items() if not k.endswith("_rows")) <= body["query_ms"] + 1.0, ph


def test_the_stat_phase_counts_the_stats_it_made(lib):
    """The stat count is the number that matters on the operator's NAS — ~0.5 ms each there, nothing here."""
    client, _ = lib
    ph = client.get("/api/library?tab=tv&fourk=0&dl_pills=on", headers=AUTH).json()["query_phases"]
    assert ph.get("stats_rows", 0) >= 12, ph  # 12 canonicals, plus placements where a PL matcher read one


def test_a_view_that_matches_nothing_still_says_where_the_time_went(lib):
    """The operator's shape — rows=0 total=0. The ledger must not be empty just because the page is."""
    client, _ = lib
    body = client.get("/api/library?tab=tv&fourk=0&attn_pills=update", headers=AUTH).json()
    assert (len(body["items"]), body["total"]) == (0, 0)
    assert body["query_phases"].get("ids", 0) >= 0 and "conn" in body["query_phases"]


def test_each_request_gets_its_own_ledger(lib):
    """The ledger is thread-local and the threadpool reuses threads — without a reset per request the numbers
    would accumulate for as long as the process lived, which is worse than no numbers at all."""
    client, _ = lib
    first = client.get("/api/library?tab=tv&fourk=0", headers=AUTH).json()["query_phases"]
    for _ in range(4):
        later = client.get("/api/library?tab=tv&fourk=0", headers=AUTH).json()["query_phases"]
    assert later["hydrate_rows"] == first["hydrate_rows"], (first, later)
    assert later["conn"] < first["conn"] * 50 + 10, (first, later)


def test_a_new_request_starts_from_zero():
    """The behavioural test above only catches this if the threadpool happens to reuse the thread, which it need
    not do under TestClient — so pin the invariant itself: beginning a request clears whatever the last one on
    this thread left behind. Without it the numbers would grow for the life of the process."""
    from app.web import api as api_mod
    first = api_mod._phases_begin()
    api_mod._phase_add("ids", 12.0, rows=3)
    assert (first["ids"], first["ids_rows"]) == (12.0, 3)
    second = api_mod._phases_begin()
    assert second == {}, "a reused thread must not inherit the previous request's phases"
    api_mod._lib_phase_state.__dict__.pop("ledger", None)


def test_phases_outside_a_library_request_are_a_no_op():
    """_annotate_canonical_state is called from other endpoints too; with no ledger it must record nothing
    rather than raise or leak into the next library request."""
    from app.web import api as api_mod
    api_mod._lib_phase_state.__dict__.pop("ledger", None)
    api_mod._phase_add("stats", 12.0, rows=3)
    with api_mod._phase("ids"):
        pass
    assert getattr(api_mod._lib_phase_state, "ledger", None) is None


# ── the warning ──────────────────────────────────────────────


def test_a_fast_request_logs_nothing(lib, caplog):
    client, _ = lib
    with caplog.at_level(logging.WARNING, logger="app.web.api"):
        client.get("/api/library?tab=tv&fourk=0", headers=AUTH)
    assert not [r for r in caplog.records if "slow /api/library" in r.getMessage()]


def test_the_slow_line_carries_the_phases_and_what_motif_was_doing(lib, caplog, monkeypatch):
    from app.web import api as api_mod
    monkeypatch.setattr(api_mod, "_SLOW_LIBRARY_MS", 0.0)
    client, _ = lib
    with caplog.at_level(logging.WARNING, logger="app.web.api"):
        client.get("/api/library?tab=tv&fourk=0", headers=AUTH)
    line = next(r.getMessage() for r in caplog.records if "slow /api/library" in r.getMessage())
    for part in ("conn=", "ids=", "hydrate=", "meta=", "busy=", "tab=tv", "rows=16"):
        assert part in line, (part, line)


def test_the_busy_sample_names_a_running_op_and_counts_queued_jobs(lib):
    from app.web import api as api_mod
    client, db = lib
    with sqlite3.connect(db) as c:
        c.execute("INSERT INTO op_progress (op_id,kind,status,started_at,updated_at)"
                  " VALUES ('op1','plex_enum','running',?,?)", (_now(), _now()))
        c.execute("INSERT INTO jobs (job_type,media_type,tmdb_id,section_id,payload,status,created_at)"
                  " VALUES ('download','tv',500,'2','{}','pending',?)", (_now(),))
        c.commit()
    sample = api_mod._busy_sample(db)
    assert sample == "plex_enum+1jobs", sample


def test_the_busy_sample_is_quiet_when_nothing_runs(lib):
    from app.web import api as api_mod
    _, db = lib
    assert api_mod._busy_sample(db) == "no-ops+0jobs"


def test_a_diagnostic_never_breaks_the_thing_it_diagnoses(tmp_path):
    """A sample that raises would turn a slow request into a 500 — the one outcome worse than a slow request."""
    from app.web import api as api_mod
    assert api_mod._busy_sample(tmp_path / "does-not-exist.db").startswith("unavailable:")
