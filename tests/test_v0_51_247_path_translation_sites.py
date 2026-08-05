"""v0.51.247 — every sidecar check on a plex_items.folder_path translates first.

`plex_items.folder_path` stores PLEX's view of the path (host form, e.g.
/mnt/user/data/movies/X). motif runs in a container and sees /data/movies/X.
Translation is ALWAYS ACTIVE — `_HARDCODED_PATH_PREFIX_TRANSLATIONS` ships
/mnt/user/data/ -> /data/ and friends even with MOTIF_PATH_TRANSLATIONS unset —
so a raw `Path(folder_path).exists()` is, in v1.22.15's words, "ALWAYS False
inside the container".

v1.22.15 fixed two sites. Four more were left, all reachable on a normal Unraid
install:

  api.py   _probe_one              REPROBE said "sidecar missing" for every row
  adopt.py adopt_folder            ADOPT FROM PLEX 409'd on adoptable rows
  api.py   test-trigger-theme-lost dispatched a different tier than the reaper
  scheduler _restore_lost_placements  compared host-form folder_path against
                                   container-form media_folder IN SQL, so the
                                   sidecar auto-restore matched ZERO candidates
                                   and was silently inert

The last one is the expensive one: it is the sweep that puts a theme back after
Plex drops a sidecar, and it had never fired on this install.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

from app.core.plex_enum import _candidate_local_paths, find_theme_sidecar_path

REPO = Path(__file__).resolve().parent.parent


# ── the premise, measured rather than assumed ────────────────────────────

def test_a_host_path_really_does_translate_to_the_container_form():
    """If this ever stops being true the whole tag is pointless — so pin it."""
    cands = [str(c) for c in _candidate_local_paths("/mnt/user/data/movies/X")]
    assert "/data/movies/X" in cands, cands
    assert "/mnt/user/data/movies/X" in cands, "the raw form must stay a candidate"


def test_a_container_path_is_left_alone():
    assert [str(c) for c in _candidate_local_paths("/data/movies/X")] == ["/data/movies/X"]


def test_a_raw_stat_on_a_host_path_finds_nothing(tmp_path):
    """The failure mode, demonstrated: the file exists at the CONTAINER path and
    a raw stat of the HOST path misses it. This is what all four sites did."""
    container = tmp_path / "data" / "movies" / "X"
    container.mkdir(parents=True)
    (container / "theme.mp3").write_bytes(b"ID3")
    host = tmp_path / "mnt" / "user" / "data" / "movies" / "X"
    assert not (Path(host) / "theme.mp3").is_file(), "premise broken"


def test_the_helper_finds_a_sidecar_the_raw_stat_would_miss(tmp_path, monkeypatch):
    """find_theme_sidecar_path walks the translated candidates, so it resolves
    what a raw stat cannot. Uses a temp translation pair so the test doesn't
    depend on /mnt/user existing."""
    import app.core.plex_enum as pe
    container = tmp_path / "container" / "movies" / "X"
    container.mkdir(parents=True)
    (container / "theme.mp3").write_bytes(b"ID3")
    host = str(tmp_path / "host" / "movies" / "X")
    monkeypatch.setattr(pe, "_PATH_PREFIX_TRANSLATIONS",
                        ((f"{tmp_path}/host/", f"{tmp_path}/container/"),))
    assert not (Path(host) / "theme.mp3").is_file()
    found = find_theme_sidecar_path(host)
    assert found is not None and found.is_file(), "helper missed a real sidecar"


# ── the four sites ───────────────────────────────────────────────────────

def _fn_src(path: str, name: str) -> str:
    s = (REPO / path).read_text()
    i = s.index(f"def {name}")
    j = s.find("\ndef ", i + 1)
    return s[i:j if j > 0 else len(s)]


def test_adopt_folder_uses_the_helper():
    src = _fn_src("app/core/adopt.py", "adopt_folder")
    assert "find_theme_sidecar_path(folder_path)" in src
    code = "\n".join(l for l in src.splitlines() if not l.lstrip().startswith("#"))
    assert 'Path(folder_path) / "theme.mp3"' not in code


def test_probe_one_uses_the_helper():
    s = (REPO / "app" / "web" / "api.py").read_text()
    i = s.index("def _reprobe_plex_themes_run")
    j = s.index("\n    @app.", i)      # structural bound, not a byte window
    seg = s[i:j]
    code = "\n".join(l for l in seg.splitlines() if not l.lstrip().startswith("#"))
    assert "find_theme_sidecar_path(folder_path)" in code
    assert 'Path(folder_path) / "theme.mp3"' not in code


def test_test_trigger_mirrors_the_prod_reaper():
    """It CLAIMS to mirror plex_enum's reaper probe; the reaper moved to
    find_theme_sidecar_path in v1.22.15/v1.22.72 and this lagged three fixes."""
    s = (REPO / "app" / "web" / "api.py").read_text()
    i = s.index("test-trigger-theme-lost")
    # bounded by the NEXT route decorator, not a byte count — a fixed window
    # here was already 210 chars short once (the +N slice treadmill).
    j = s.index("@app.", i + 20)
    seg = s[i:j]
    code = "\n".join(l for l in seg.splitlines() if not l.lstrip().startswith("#"))
    assert "find_theme_sidecar_path(_folder_path)" in code
    assert 'Path(_folder_path) / "theme.mp3"' not in code


def test_restore_sweep_no_longer_compares_paths_in_sql():
    """The SQL could not reach the translation table, so the comparison had to
    move to Python. If it comes back, the sweep goes inert again — silently,
    which is why this one went unnoticed."""
    src = _fn_src("app/core/scheduler.py", "_restore_lost_placements")
    code = "\n".join(l for l in src.splitlines()
                     if not l.lstrip().startswith(("#", "--")))
    assert "pi.folder_path = p.media_folder" not in code, (
        "host-form folder_path compared to container-form media_folder in SQL")
    assert "_plex_claimed_folders(" in code, "lost the Python-side gate"


def test_claimed_folders_returns_container_form(tmp_path, monkeypatch):
    """Behavioural: the set must contain the CONTAINER path, because that is
    what placements.media_folder holds."""
    import app.core.plex_enum as pe
    from app.core.db import get_conn, init_db
    from app.core.scheduler import _plex_claimed_folders
    monkeypatch.setattr(pe, "_PATH_PREFIX_TRANSLATIONS",
                        (("/mnt/user/data/", "/data/"),))
    d = tmp_path / "m.db"
    init_db(d)
    with get_conn(d) as c:
        c.execute("INSERT INTO plex_sections (section_id, title, type, included,"
                  " discovered_at, last_seen_at)"
                  " VALUES ('1','M','movie',1,'2026-01-01','2026-01-01')")
        c.execute("INSERT INTO plex_items (rating_key, section_id, media_type,"
                  " folder_path, edition_key, title, first_seen_at, last_seen_at)"
                  " VALUES ('r1','1','movie','/mnt/user/data/movies/X','','X',"
                  "'2026-01-01','2026-01-01')")
        c.commit()
    claimed = _plex_claimed_folders(d)
    assert ("1", "movie", "/data/movies/X") in claimed, (
        f"container form missing — the sweep will skip every row: {claimed}")


# ── the class lint ───────────────────────────────────────────────────────

def test_no_raw_sidecar_stat_on_a_folder_path_variable():
    """The durable guard. Six sites have now had this bug; the seventh should
    fail the suite instead of shipping. A stat is only safe on media_folder
    (placements — already container form), never on a folder_path (plex_items —
    host form)."""
    offenders = []
    for rel in ("app/core/adopt.py", "app/core/scheduler.py",
                "app/core/worker.py", "app/web/api.py", "app/core/plex_enum.py"):
        src = (REPO / rel).read_text()
        for n, line in enumerate(src.splitlines(), 1):
            if line.lstrip().startswith("#"):
                continue
            # Path(<something folder_path>) / "theme.<ext>"
            if re.search(r'Path\([^)]*folder_path[^)]*\)\s*/\s*"theme\.', line):
                offenders.append(f"{rel}:{n}")
    # plex_enum owns the translator, so its own internals are the one exemption
    offenders = [o for o in offenders if not o.startswith("app/core/plex_enum.py")]
    assert not offenders, (
        "raw sidecar stat on a host-form folder_path — use "
        f"find_theme_sidecar_path(): {offenders}")


# ── v0.51.248: regression parity for the SQL->Python gate rewrite ────────

def test_the_new_gate_matches_the_old_sql_except_where_translation_applies(tmp_path):
    """Differential. The v0.51.247 rewrite moved a candidate gate out of SQL, so
    the risk is not "does the fix work" but "did anything ELSE change". Runs the
    OLD `pi.folder_path = p.media_folder` predicate and the NEW set membership
    over identical rows: they must agree on every case EXCEPT the translated one
    (the bug), and in particular must still agree on untranslated installs — who
    were never affected and must not be disturbed."""
    from app.core.db import get_conn, init_db
    from app.core.scheduler import _plex_claimed_folders
    NOW = "2026-01-01T00:00:00+00:00"
    cases = [
        ("untranslated exact match",   "/data/movies/A",          "/data/movies/A",     "1", "movie", "movie", "same"),
        ("translated host->container", "/mnt/user/data/movies/B", "/data/movies/B",     "1", "movie", "movie", "differs"),
        ("genuinely different folder", "/data/movies/C",          "/data/movies/OTHER", "1", "movie", "movie", "same"),
        ("tv show media_type mapping", "/data/tv/E",              "/data/tv/E",         "2", "tv",    "show",  "same"),
    ]
    d = tmp_path / "m.db"
    init_db(d)
    with get_conn(d) as c:
        c.execute("INSERT INTO plex_sections (section_id,title,type,included,"
                  "discovered_at,last_seen_at) VALUES ('1','M','movie',1,?,?)", (NOW, NOW))
        c.execute("INSERT INTO plex_sections (section_id,title,type,included,"
                  "discovered_at,last_seen_at) VALUES ('2','T','show',1,?,?)", (NOW, NOW))
        for i, (lbl, pi_fp, pl_mf, sec, mt, pi_mt, _exp) in enumerate(cases):
            c.execute("INSERT INTO plex_items (rating_key,section_id,media_type,"
                      "folder_path,edition_key,title,first_seen_at,last_seen_at)"
                      " VALUES (?,?,?,?,'',?,?,?)", (f"rk{i}", sec, pi_mt, pi_fp, lbl, NOW, NOW))
            c.execute("INSERT INTO themes (media_type,tmdb_id,title,upstream_source,"
                      "last_seen_sync_at,first_seen_sync_at) VALUES (?,?,?,'themoviedb',?,?)",
                      (mt, 100 + i, lbl, NOW, NOW))
            c.execute("INSERT INTO placements (media_type,tmdb_id,section_id,edition_key,"
                      "media_folder,placed_at,placement_kind) VALUES (?,?,?,'',?,?,'hardlink')",
                      (mt, 100 + i, sec, pl_mf, NOW))
        c.commit()

    old_sql = ("SELECT 1 FROM plex_items pi WHERE pi.folder_path = ? "
               "AND pi.section_id = ? AND (CASE pi.media_type WHEN 'show' "
               "THEN 'tv' ELSE pi.media_type END) = ?")
    claimed = _plex_claimed_folders(d)
    with get_conn(d) as c:
        rows = c.execute("SELECT media_type, section_id, media_folder FROM "
                         "placements ORDER BY tmdb_id").fetchall()
    for r, (lbl, *_m, expect) in zip(rows, cases):
        with get_conn(d) as conn:
            old = conn.execute(old_sql, (r["media_folder"], r["section_id"],
                                         r["media_type"])).fetchone() is not None
        new = (str(r["section_id"]), r["media_type"], r["media_folder"]) in claimed
        if expect == "same":
            assert old == new, (
                f"{lbl}: gate behaviour CHANGED (old={old} new={new}) — this case "
                "was never broken and must not be disturbed")
        else:
            assert not old and new, f"{lbl}: expected the translated fix, got old={old} new={new}"
