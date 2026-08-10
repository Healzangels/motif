"""v1.24.18 — STANDARD ('') edition place-resolution scope (held edition fix #1).

The recurring ''-as-falsy bug, on the worker's cached_rk resolution. Both
`_do_place` (the hardlink FILE path) and `_do_place_collection` (the API-upload
path, also reached by movie/TV kind='api') resolve the rating_key they operate
on by (theme_id / guid_tmdb, section_id) + an edition predicate. v1.21.66 scoped
that predicate per-edition — but gated it on TRUTHINESS:

    _ed_clause = " AND pi.edition_key = ?" if place_edition_key else ""

so a TAGGED edition ('theatrical') scoped correctly, while the STANDARD ''
edition fell through to the UNSCOPED `LIMIT 1` — grabbing an arbitrary sibling
across a multi-edition title (theme hardlinked into the wrong folder, wrong-rk
refresh, a split-brain re-enqueue loop). '' is a REAL edition (the standard
one); scope by PRESENCE, always.

v1.24.18 makes the predicate unconditional in both helpers and adds a ''-ONLY
unscoped fallback (the held carve-out): a default-'' payload on a title whose
only plex_items row is TAGGED still resolves that row rather than no-op'ing.
TAGGED payloads deliberately do NOT fall back (v1.21.66 strict — a tagged miss
must not grab a sibling).

The behavioral tests seed a multi-edition movie with the TAGGED sibling FIRST
(lower rowid) so the pre-fix unscoped `LIMIT 1` would return IT — the
discriminator. `_do_place_collection` is observed via its cached_rk state log;
`_do_place` (no such log) is observed by capturing the cached_rk that reaches
`place_theme`.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import threading
from pathlib import Path

from app.core.db import init_db
from app.core.editions import edition_key_for_folder
from app.core.events import now_iso


NOW = now_iso()
TMDB = 949
# A multi-edition movie: one STANDARD ('') folder + one TAGGED sibling.
STD_RK = "9001"
TAG_RK = "9002"
STD_FOLDER = "/data/Movies/Heat (1995)"                       # → edition_key ''
TAG_FOLDER = "/data/Movies/Heat (1995) {edition-Directors Cut}"
STD_KEY = edition_key_for_folder(STD_FOLDER)                  # ''
TAG_KEY = edition_key_for_folder(TAG_FOLDER)                  # 'directors cut'
# v1.24.24: a SECOND tagged sibling so a '' place sees 2+ candidates + no '' row
# (the LotR Theatrical/Extended/Sam shape) — the ambiguous case the fallback now
# refuses to guess at.
TAG2_RK = "9003"
TAG2_FOLDER = "/data/Movies/Heat (1995) {edition-Extended}"
TAG2_KEY = edition_key_for_folder(TAG2_FOLDER)               # 'extended'


# ── worker harness (mirrors test_v1_21_66) ───────────────────────

def _settings(tmp_path, *, dry):
    from app.config import Settings
    from app.core.runtime import set_dry_run
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    s._cfg.paths.themes_dir = str(tmp_path / "themes")
    set_dry_run(s.db_path, dry, updated_by="test")
    return s


def _worker(settings):
    from app.core.worker import Worker, TokenBucket
    return Worker(settings=settings, stop_event=threading.Event(),
                  bucket=TokenBucket(60, 60))


def _seed(db, *, with_std=True, with_second_tag=False, with_local=False,
          themes_dir=None):
    """Multi-edition movie. The TAGGED sibling is inserted FIRST (lower rowid)
    so the pre-fix UNSCOPED `LIMIT 1` would return it — the discriminator.

    with_std=False drops the '' row entirely (the held carve-out: only a tagged
    folder exists, so a default-'' payload FALLS BACK to it when it's the lone
    candidate). with_second_tag adds a 2nd tagged sibling → 2 candidates + no ''
    (the LotR shape) where v1.24.24 refuses to guess. with_local writes a real
    '' local_files row + theme.mp3 — the _do_place FILE path raises without one.
    """
    assert STD_KEY == "", STD_KEY            # the standard folder really is ''
    assert TAG_KEY and TAG_KEY != "", TAG_KEY
    rows = [(TAG_RK, TAG_KEY, TAG_FOLDER)]
    if with_second_tag:
        rows.append((TAG2_RK, TAG2_KEY, TAG2_FOLDER))
    if with_std:
        rows.append((STD_RK, STD_KEY, STD_FOLDER))
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime,"
            " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
            " VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        cur = conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, year,"
            " upstream_source, last_seen_sync_at, first_seen_sync_at,"
            " youtube_url) VALUES ('movie',?,'Heat','1995','imdb',?,?,"
            "'https://www.youtube.com/watch?v=tdb00000949')", (TMDB, NOW, NOW))
        tid = cur.lastrowid
        for rk, ek, fp in rows:
            conn.execute(
                "INSERT INTO plex_items (rating_key, section_id, media_type,"
                " theme_id, guid_tmdb, title, year, edition_key, folder_path,"
                " has_theme, local_theme_file, first_seen_at, last_seen_at)"
                " VALUES (?,'1','movie',?,?,'Heat','1995',?,?,0,0,?,?)",
                (rk, tid, TMDB, ek, fp, NOW, NOW))
        if with_local:
            rel = "movies/heat.mp3"
            p = Path(themes_dir) / rel
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_bytes(b"ID3standardtheme")
            conn.execute(
                "INSERT INTO local_files (media_type, tmdb_id, section_id,"
                " edition_key, file_path, downloaded_at, source_video_id,"
                " provenance, source_kind) VALUES ('movie',?,'1','',?,?,"
                "'v','auto','themerrdb')", (TMDB, rel, NOW))
        conn.commit()
    return tid


def _place_job(db, *, kind, edition_key):
    payload = json.dumps({"kind": kind, "edition_key": edition_key})
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO jobs (job_type, media_type, tmdb_id, section_id,"
            " payload, status, created_at) VALUES ('place','movie',?,'1',"
            "?,'running',?)", (TMDB, payload, NOW))
        conn.commit()
    c = sqlite3.connect(db)
    c.row_factory = sqlite3.Row
    return c.execute("SELECT * FROM jobs WHERE tmdb_id=?", (TMDB,)).fetchone()


def _theme_row(db, tid):
    c = sqlite3.connect(db)
    c.row_factory = sqlite3.Row
    return c.execute("SELECT * FROM themes WHERE id=?", (tid,)).fetchone()


class _Outcome:
    """A not-placed place_theme outcome — enough shape for the _do_place
    bookkeeping tail (reads .placed / .reason / .kind / .target_folder)."""
    placed = False
    reason = "no_match"
    kind = "hardlink"
    target_folder = None
    plex_rating_key = None
    plex_refreshed = False


# ── _do_place_collection: STANDARD '' resolves the '' rk ──────────

def test_do_place_collection_resolves_standard_edition_rk(tmp_path, caplog):
    """A place job carrying the STANDARD edition (edition_key='') must resolve
    cached_rk = the '' plex_items row (STD_RK), NOT an arbitrary tagged sibling.
    Pre-v1.24.18 the '' gate evaluated falsy → unscoped → TAG_RK (inserted
    first)."""
    s = _settings(tmp_path, dry=True)
    tid = _seed(s.db_path)
    job = _place_job(s.db_path, kind="api", edition_key="")
    theme = _theme_row(s.db_path, tid)

    with caplog.at_level(logging.INFO, logger="app.core.worker"):
        _worker(s)._do_place_collection(job=job, theme=theme, local=None)

    line = next((r.getMessage() for r in caplog.records
                 if "_do_place_collection: job=" in r.getMessage()), None)
    assert line is not None, "expected the _do_place_collection state log"
    assert f"cached_rk={STD_RK}" in line, line
    assert f"cached_rk={TAG_RK}" not in line, line


def test_do_place_collection_standard_payload_falls_back_to_tagged_only(
        tmp_path, caplog):
    """Held carve-out: a default-'' payload on a title whose ONLY plex_items
    row is TAGGED (no '' row) must FALL BACK to that tagged row's rk — a
    standard place still lands rather than no-op'ing (cached_rk=None)."""
    s = _settings(tmp_path, dry=True)
    tid = _seed(s.db_path, with_std=False)
    job = _place_job(s.db_path, kind="api", edition_key="")
    theme = _theme_row(s.db_path, tid)

    with caplog.at_level(logging.INFO, logger="app.core.worker"):
        _worker(s)._do_place_collection(job=job, theme=theme, local=None)

    line = next((r.getMessage() for r in caplog.records
                 if "_do_place_collection: job=" in r.getMessage()), None)
    assert line is not None, "expected the _do_place_collection state log"
    assert f"cached_rk={TAG_RK}" in line, line
    assert "cached_rk=None" not in line, line


# ── _do_place (FILE path): STANDARD '' resolves the '' rk ─────────

def test_do_place_file_path_resolves_standard_edition_rk(tmp_path, monkeypatch):
    """The hardlink FILE path resolves cached_rk for the STANDARD '' edition,
    not a tagged sibling. Observed by capturing the cached_rk that reaches
    place_theme (which we stub — _do_place has no cached_rk state log)."""
    import app.core.worker as worker_mod

    s = _settings(tmp_path, dry=False)
    _seed(s.db_path, with_local=True, themes_dir=s.themes_dir)
    job = _place_job(s.db_path, kind="file", edition_key="")

    captured: dict = {}

    def _fake_place_theme(**kwargs):
        captured.update(kwargs)
        return _Outcome()

    monkeypatch.setattr(worker_mod, "place_theme", _fake_place_theme)
    _worker(s)._do_place(job)

    assert captured, "place_theme was never reached"
    assert captured.get("cached_rk") == STD_RK, captured.get("cached_rk")


def test_do_place_file_path_standard_payload_falls_back_to_tagged_only(
        tmp_path, monkeypatch):
    """FILE-path mirror of the held carve-out: default-'' payload, only a
    tagged plex_items row exists → cached_rk falls back to TAG_RK (not None),
    so place_theme still operates on a real rk."""
    import app.core.worker as worker_mod

    s = _settings(tmp_path, dry=False)
    _seed(s.db_path, with_std=False, with_local=True, themes_dir=s.themes_dir)
    job = _place_job(s.db_path, kind="file", edition_key="")

    captured: dict = {}

    def _fake_place_theme(**kwargs):
        captured.update(kwargs)
        return _Outcome()

    monkeypatch.setattr(worker_mod, "place_theme", _fake_place_theme)
    _worker(s)._do_place(job)

    assert captured, "place_theme was never reached"
    assert captured.get("cached_rk") == TAG_RK, captured.get("cached_rk")


# ── Source guards: both gates always-scope + carry a ''-only fallback ──

def test_both_resolution_gates_scope_by_presence_not_truthiness():
    """Neither helper may gate the edition predicate on `if <edition> else ''`
    — that's the exact ''-as-falsy regression. The clause is unconditional."""
    src = (Path(__file__).resolve().parent.parent
           / "app" / "core" / "worker.py").read_text()
    # The always-scope clauses both helpers now build.
    assert '_ed_clause = " AND pi.edition_key = ?"' in src
    assert '_ce_clause = " AND pi.edition_key = ?"' in src
    # The pre-fix truthiness gate must NOT reappear in either form.
    assert 'if place_edition_key else ""' not in src
    assert 'if _coll_payload_edition else ""' not in src


def test_both_helpers_have_standard_only_unscoped_fallback():
    """The ''-only carve-out: when the scoped resolve misses AND the payload
    edition is '' (standard), each helper falls back to the unscoped resolve."""
    src = (Path(__file__).resolve().parent.parent
           / "app" / "core" / "worker.py").read_text()
    assert "if pi is None and not place_edition_key:" in src
    assert "if pi is None and not _coll_payload_edition:" in src


# ── v1.24.24: the ''-fallback refuses to GUESS across 2+ tagged siblings ──

def test_do_place_file_path_ambiguous_multitag_does_not_guess(
        tmp_path, monkeypatch):
    """A '' place on a tagged-only title with 2+ editions (LotR shape: no '' row)
    must NOT grab an arbitrary sibling — cached_rk stays None so place_theme's
    find_target_folder decides (and cleanly no-matches under strict_edition),
    instead of hardlinking the theme into the wrong tagged folder."""
    import app.core.worker as worker_mod

    s = _settings(tmp_path, dry=False)
    _seed(s.db_path, with_std=False, with_second_tag=True,
          with_local=True, themes_dir=s.themes_dir)
    job = _place_job(s.db_path, kind="file", edition_key="")

    captured: dict = {}

    def _fake_place_theme(**kwargs):
        captured.update(kwargs)
        return _Outcome()

    monkeypatch.setattr(worker_mod, "place_theme", _fake_place_theme)
    _worker(s)._do_place(job)

    assert captured, "place_theme was never reached"
    assert captured.get("cached_rk") is None, (
        "ambiguous multi-tag '' place must NOT guess a sibling rk — got "
        f"{captured.get('cached_rk')}")
    assert captured.get("cached_folder_path") is None, (
        "no arbitrary folder either — find_target_folder owns the decision")


def test_do_place_collection_ambiguous_multitag_does_not_guess(
        tmp_path, caplog):
    """Collection-path mirror: 2+ tagged siblings, no '' row → cached_rk=None
    (no arbitrary sibling pick)."""
    s = _settings(tmp_path, dry=True)
    tid = _seed(s.db_path, with_std=False, with_second_tag=True)
    job = _place_job(s.db_path, kind="api", edition_key="")
    theme = _theme_row(s.db_path, tid)

    with caplog.at_level(logging.INFO, logger="app.core.worker"):
        _worker(s)._do_place_collection(job=job, theme=theme, local=None)

    line = next((r.getMessage() for r in caplog.records
                 if "_do_place_collection: job=" in r.getMessage()), None)
    assert line is not None, "expected the _do_place_collection state log"
    assert "cached_rk=None" in line, line
    assert f"cached_rk={TAG_RK}" not in line, line
    assert f"cached_rk={TAG2_RK}" not in line, line


def test_ambiguous_fallback_gated_on_single_candidate():
    """Both helpers' fallback must be gated on EXACTLY ONE candidate, not a
    bare arbitrary `LIMIT 1`.

    v0.51.253: the count is now over LIVE candidates — a re-added title holds
    a dead row + a live row and was wrongly refused as ambiguous. The guard's
    intent is unchanged (never guess across 2+ real siblings); only its
    expression moved from `len(_cands) == 1` to `len(_live) == 1`, plus an
    all-dead fallback that keeps the old count. Behavioural coverage of the
    ambiguity refusal itself lives in this file's tagged-sibling tests and in
    test_v0_51_253's two-live-editions case.
    """
    src = (Path(__file__).resolve().parent.parent
           / "app" / "core" / "worker.py").read_text()
    assert src.count("if len(_live) == 1:") >= 2, (
        "both resolves must gate the fallback on exactly one LIVE candidate")
    assert src.count("elif not _live and len(_cands) == 1:") >= 2, (
        "both resolves must keep the all-dead single-candidate fallback — "
        "dropping it strands a place during a section-wide Plex glitch")
