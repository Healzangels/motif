"""v0.51.359 — a bulk path must do everything its per-row twin does.

Three bugs in three weeks came from the same shape: one action, two implementations, and the bulk
one quietly missing a step.

  * v0.51.355 — bulk ACCEPT ALL never landed the themes.youtube_url write the per-row ACCEPT has
    done since v1.12.37, so accepting in bulk queued downloads with nothing to resolve.
  * v0.51.358 — bulk DECLINE wrote no audit row; the per-row DECLINE has since v1.12.80.
  * this tag — bulk LET PLEX SERVE deleted the same placements rows as the per-row UNPLACE without
    first cancelling the row's in-flight jobs (v1.18.73's fix, whose comment warns of a "ghost
    placements row reborn, motif's tracking drifts silently from on-disk state"), and wrote no
    audit row either.

Each was found by reading. This compares the two sides mechanically instead: every effect the
per-row path has — a table it writes, a state helper it calls — the bulk path must have too. A
bulk path may do MORE (its progress row, its summary event); it may not do less.

Table names are read from the real schema, so prose in a docstring cannot pass for SQL — an
earlier draft of this comparison matched the word "UPDATE" in an English sentence.
"""
from __future__ import annotations

import ast
import re
import sqlite3
import tempfile
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
API_SRC = (REPO / "app" / "web" / "api.py").read_text()
TREE = ast.parse(API_SRC)
FNS = {n.name: n for n in ast.walk(TREE) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}

# the state-changing helpers; a call to one of these IS an effect, wherever it lives
HELPERS = {"_record_audit", "_set_pending_update_decision", "_capture_previous_url", "_enqueue_download",
           "_drop_motif_tracking", "_cancel_jobs_for_row", "_mark_failed_terminal", "_ack_failure",
           "record_notification", "resolve_new_theme_pending_update"}
SQL = re.compile(r"\b(INSERT\s+(?:OR\s+\w+\s+)?INTO|UPDATE|DELETE\s+FROM)\s+([a-z_][a-z0-9_]*)\b", re.I)

#: per-row handler → the bulk handler plus any worker it hands the work to.
PAIRS = {
    "api_accept_update": ["api_accept_all_updates"],
    "api_decline_update": ["api_decline_all_updates"],
    "api_probe_tdb": ["api_admin_bulk_probe_tdb", "_bulk_probe_tdb_run"],
    "api_unplace_item": ["api_admin_bulk_let_plex_serve", "_bulk_lps_run"],
    "api_relink_item": ["api_relink_all"],
    "api_notifications_dismiss": ["api_notifications_dismiss_all"],
    "api_admin_loudness_normalize_one": ["api_admin_loudness_bulk_normalize", "_bulk_normalize_run"],
}

#: effects the per-row path has that its bulk twin deliberately does not. Empty today. An entry
#: here is a decision someone made on purpose, with the reason written down — not a silenced lint.
ALLOWED_DIFFERENCES: dict[str, dict[str, str]] = {}

#: bulk routes with no per-row twin at all, and why.
NO_TWIN = {
    "api_decide_findings_bulk": "the scan-findings page has no per-row decide endpoint; the UI posts "
                                "one list",
    "api_admin_loudness_bulk_undo": "undo is paired with the bulk apply, not with a per-row action",
}


@pytest.fixture(scope="module")
def tables() -> set[str]:
    from app.core.db import init_db
    db = Path(tempfile.mkdtemp()) / "m.db"
    init_db(db)
    with sqlite3.connect(db) as c:
        return {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def _effects(names: list[str], tables: set[str]) -> set[str]:
    out: set[str] = set()
    for name in names:
        fn = FNS.get(name)
        assert fn is not None, f"{name} is not a function in api.py — the pair table is stale"
        for n in ast.walk(fn):
            if isinstance(n, ast.Constant) and isinstance(n.value, str):
                flat = " ".join(n.value.split())
                upper = flat.upper()
                for verb, table in SQL.findall(flat):
                    v = verb.split()[0].upper()
                    if table.lower() not in tables:
                        continue          # prose, not SQL
                    if v == "UPDATE" and " SET " not in upper:
                        continue
                    if v == "INSERT" and not re.search(r"\bVALUES\b|\bSELECT\b", upper):
                        continue
                    out.add(f"{v} {table.lower()}")
            elif isinstance(n, ast.Call):
                f = n.func
                called = f.attr if isinstance(f, ast.Attribute) else (f.id if isinstance(f, ast.Name) else "")
                if called in HELPERS:
                    out.add(f"call {called}")
    return out


@pytest.mark.parametrize("single", sorted(PAIRS))
def test_the_bulk_path_does_everything_the_per_row_path_does(single, tables):
    bulk = PAIRS[single]
    missing = sorted(_effects([single], tables) - _effects(bulk, tables))
    allowed = ALLOWED_DIFFERENCES.get(single, {})
    unexplained = [m for m in missing if m not in allowed]
    assert not unexplained, (
        f"{' + '.join(bulk)} does not do what {single} does: {unexplained}. Either add it to the bulk "
        f"path, or record why not in ALLOWED_DIFFERENCES — a bulk action that silently skips a step "
        f"is how v0.51.355, v0.51.358 and v0.51.359 each happened.")


def test_every_bulk_route_is_accounted_for():
    """The forcing function: a new bulk endpoint has to name its per-row twin (or say it has none)
    before this suite will pass."""
    routes = re.findall(r'@app\.(?:post|patch|delete)\("([^"]+)"\)\s*\n\s*async def (\w+)', API_SRC)
    bulky = {fn for path, fn in routes
             if "bulk" in path or path.endswith("-all") or path.endswith("/all")}
    declared = {b for bulks in PAIRS.values() for b in bulks} | set(NO_TWIN)
    assert not (bulky - declared), (
        f"bulk route(s) with no entry in PAIRS or NO_TWIN: {sorted(bulky - declared)}")


def test_the_allowlist_explains_itself():
    for single, entries in ALLOWED_DIFFERENCES.items():
        assert single in PAIRS, single
        for effect, reason in entries.items():
            assert len(reason) > 20, f"{single}/{effect} needs a real reason, not {reason!r}"


# ── the two differences this tag closed, driven for real ─────


@pytest.fixture
def lps_db(tmp_path, monkeypatch):
    """A themed, placed row with a download already in flight against it."""
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from datetime import datetime, timezone
    from app.config import Settings
    from app.core.db import init_db
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    (tmp_path / "themes" / "movies").mkdir(parents=True)
    settings._cfg.paths.themes_dir = str(tmp_path / "themes")
    init_db(settings.db_path)
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    folder = tmp_path / "media"
    folder.mkdir()
    (folder / "theme.mp3").write_bytes(b"\0" * 64)
    (tmp_path / "themes" / "movies" / "7.mp3").write_bytes(b"\0" * 64)
    with sqlite3.connect(settings.db_path) as c:
        c.execute("INSERT INTO plex_sections (section_id,title,type,is_anime,is_4k,themes_subdir,included,"
                  "discovered_at,last_seen_at) VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (now, now))
        tid = c.execute("INSERT INTO themes (media_type,tmdb_id,title,upstream_source,last_seen_sync_at,"
                        "first_seen_sync_at,youtube_url) VALUES ('movie',7,'Wonka','imdb',?,?,?)",
                        (now, now, "https://y.t/watch?v=a1")).lastrowid
        c.execute("INSERT INTO plex_items (rating_key,section_id,media_type,theme_id,guid_tmdb,title,year,"
                  "edition_key,folder_path,has_theme,local_theme_file,plex_independent_theme,"
                  "plex_theme_verified_ok,first_seen_at,last_seen_at)"
                  " VALUES ('rk7','1','movie',?,7,'Wonka',1971,'',?,1,1,1,1,?,?)", (tid, str(folder), now, now))
        c.execute("INSERT INTO local_files (media_type,tmdb_id,section_id,edition_key,file_path,downloaded_at,"
                  "source_video_id,provenance,source_kind) VALUES ('movie',7,'1','','movies/7.mp3',?,'a1',"
                  "'auto','themerrdb')", (now,))
        c.execute("INSERT INTO placements (media_type,tmdb_id,section_id,edition_key,media_folder,placed_at,"
                  "placement_kind,plex_refreshed,theme_present) VALUES ('movie',7,'1','',?,?,'hardlink',1,1)",
                  (str(folder), now))
        c.execute("INSERT INTO jobs (job_type,media_type,tmdb_id,section_id,payload,status,created_at)"
                  " VALUES ('download','movie',7,'1','{\"edition_key\": \"\"}','pending',?)", (now,))
        c.commit()
    # the probe stage must not touch the network
    import app.core.downloader as dl
    monkeypatch.setattr(dl, "probe_youtube_url", lambda *a, **k: dl.FailureKind.UNKNOWN, raising=False)
    return settings


def _run_bulk_lps(settings, actor="tester"):
    from app.web.api import _bulk_lps_run
    _bulk_lps_run(settings.db_path, settings,
                  targets=[{"media_type": "movie", "tmdb_id": 7, "section_id": "1"}], actor=actor)


def test_bulk_lps_cancels_the_rows_jobs_before_it_unplaces(lps_db):
    """v1.18.73 on the per-row path: a place/download job in flight when the placement is swept can
    commit afterwards and rebuild the row's tracking — a ghost placement, with nothing in the log."""
    _run_bulk_lps(lps_db)
    with sqlite3.connect(lps_db.db_path) as c:
        status = c.execute("SELECT status FROM jobs WHERE tmdb_id = 7").fetchone()[0]
        placements = c.execute("SELECT COUNT(*) FROM placements WHERE tmdb_id = 7").fetchone()[0]
    assert placements == 0, "the point of LET PLEX SERVE — motif's placement goes"
    assert status == "cancelled", f"the in-flight download was left {status!r} to race the teardown"


def test_bulk_lps_records_who_let_plex_serve(lps_db):
    _run_bulk_lps(lps_db, actor="operator-9")
    with sqlite3.connect(lps_db.db_path) as c:
        c.row_factory = sqlite3.Row
        rows = [dict(r) for r in c.execute("SELECT * FROM audit_events WHERE action = 'unplace'")]
    assert len(rows) == 1, rows
    assert rows[0]["actor"] == "operator-9" and rows[0]["tmdb_id"] == 7
    assert "bulk_lps" in (rows[0]["details"] or "")
