"""v0.51.369 — a collection Plex 404s is reaped on the walk that misses it.

Kometa re-creates a collection under a new rating_key; the old key 404s for
good. The v0.51.128 grace window held the old row for a second walk (up to ~24h
on cron), so /collections listed it beside its replacement, and since both map
to one title-keyed theme, deleting the theme on one deleted it on both.

The reaper now asks Plex about each newly-missing collection row: 404 reaps it
on this walk, 200 or any error keeps the grace window, at most _REAP_PROBE_CAP
per walk, and the v1.18.89 mass-guard still aborts a large miss. Movies and
shows are not probed — one re-added mid-walk gets its theme back, so its old
row's reap is what the grace window keeps from a false 💔 Theme lost.

Behavioural tests against the real _upsert_items reaper and run_plex_enum.
"""
from __future__ import annotations

import sqlite3

import pytest

from app.core import plex_enum
from app.core.db import init_db
from app.core.plex import PlexClient, PlexConfig, PlexLibraryItem, PlexSection
from app.core.plex_enum import _REAP_MISS_THRESHOLD

SEC = "1"
NOW = "2026-10-15T00:00:00+00:00"


def _coll(rk, title="Keeper"):
    return PlexLibraryItem(
        rating_key=rk, section_id=SEC, media_type="collection",
        title=title, year=None, guid_imdb=None, guid_tmdb=None,
        guid_tvdb=None, folder_path="", has_theme=False, plex_theme_uri="",
    )


def _movie(rk, title="Film"):
    return PlexLibraryItem(
        rating_key=rk, section_id=SEC, media_type="movie",
        title=title, year="2020", guid_imdb=None, guid_tmdb=None,
        guid_tvdb=None, folder_path=f"/data/movies/{title}", has_theme=False,
        plex_theme_uri="",
    )


@pytest.fixture
def db(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    path = tmp_path / "motif.db"
    init_db(path)
    with sqlite3.connect(path) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, uuid, title, type, agent,"
            " language, location_paths, included, themes_subdir,"
            " discovered_at, last_seen_at) VALUES"
            " ('1','u1','Movies','movie','a','en','[]',1,'movies',?,?)",
            (NOW, NOW))
        conn.commit()
    return path


def _seed(db, rks, media_type="collection", missing=0):
    with sqlite3.connect(db) as conn:
        for rk in rks:
            conn.execute(
                "INSERT INTO plex_items (rating_key, section_id, media_type,"
                " title, has_theme, local_theme_file, folder_path,"
                " consecutive_missing, first_seen_at, last_seen_at)"
                " VALUES (?,?,?,?,0,0,'',?,?,?)",
                (rk, SEC, media_type, f"T{rk}", missing, NOW, NOW))
        conn.commit()


def _rows(db):
    with sqlite3.connect(db) as conn:
        return {r[0]: r[1] for r in conn.execute(
            "SELECT rating_key, consecutive_missing FROM plex_items")}


class _Probe:
    """Records every rk asked about; answers from a verdict map."""

    def __init__(self, verdicts, default=False):
        self.verdicts = verdicts
        self.default = default
        self.asked = []

    def __call__(self, rk):
        self.asked.append(rk)
        v = self.verdicts.get(rk, self.default)
        if isinstance(v, Exception):
            raise v
        return v


def _walk(db, returned, probe):
    plex_enum._upsert_items(db, returned, section_id=SEC, exists_probe=probe)


# ── PlexClient.item_exists is a real tristate ─────────────────


@pytest.mark.parametrize("status,expected", [
    (200, True), (404, False), (500, None), (401, None), (403, None),
    (None, None),
])
def test_item_exists_is_a_tristate(status, expected, monkeypatch):
    client = PlexClient(PlexConfig(url="http://x", token="t",
                                   movie_section="1", tv_section="2"))
    asked = []

    def _status(path):
        asked.append(path)
        return status

    monkeypatch.setattr(client, "_head_or_get_status", _status)
    try:
        assert client.item_exists("454778") is expected
    finally:
        client.close()
    # The item itself — NOT its /theme, whose 404 means "no theme", not "gone".
    assert asked == ["/library/metadata/454778"]


# ── the reaper ────────────────────────────────────────────────


def test_a_404_reaps_on_the_first_miss(db):
    _seed(db, ["KEEP", "GONE", "ALIVE"])
    probe = _Probe({"GONE": False, "ALIVE": True})
    _walk(db, [_coll("KEEP")], probe)
    rows = _rows(db)
    assert "GONE" not in rows, "a collection Plex 404s must reap on this walk"
    assert rows["ALIVE"] == 1, (
        "a row Plex still has (200) keeps the grace window: counted, not reaped")
    assert rows["KEEP"] == 0
    assert sorted(probe.asked) == ["ALIVE", "GONE"], (
        "only the missing rows are asked about — never one Plex just listed")


def test_an_error_keeps_the_grace_window_and_stops_probing(db):
    _seed(db, ["KEEP", "A", "B"])
    probe = _Probe({"A": None, "B": False})
    _walk(db, [_coll("KEEP")], probe)
    rows = _rows(db)
    assert rows["A"] == 1, "an unknown answer must not reap"
    assert rows["B"] == 1, (
        "after Plex fails once the rest keep the grace window, even a row "
        "that would have 404'd")
    assert probe.asked == ["A"], "probing stops at the first unknown answer"


def test_a_raising_probe_is_an_unknown_answer(db, caplog):
    _seed(db, ["KEEP", "A"])
    probe = _Probe({"A": RuntimeError("socket closed")})
    with caplog.at_level("WARNING", logger=plex_enum.log.name):
        _walk(db, [_coll("KEEP")], probe)
    assert _rows(db)["A"] == 1
    assert any("socket closed" in r.getMessage() for r in caplog.records), (
        "class 9: a swallowed probe failure needs a breadcrumb")


def test_rows_past_the_cap_keep_the_grace_window(db, monkeypatch):
    monkeypatch.setattr(plex_enum, "_REAP_PROBE_CAP", 3)
    _seed(db, ["KEEP"] + [f"G{i}" for i in range(5)])
    probe = _Probe({}, default=False)
    _walk(db, [_coll("KEEP")], probe)
    rows = _rows(db)
    assert len(probe.asked) == 3, "never more round-trips than the cap"
    reaped = {f"G{i}" for i in range(5)} - set(rows)
    assert reaped == set(probe.asked), "exactly the probed 404s reap"
    assert all(rows[rk] == 1 for rk in rows if rk.startswith("G")), (
        "the unprobed rows are counted, as before")


def test_the_real_cap_matches_the_mass_guard_floor():
    # The guard aborts only above 50 rows — every batch it lets through
    # unconditionally is fully probed.
    assert plex_enum._REAP_PROBE_CAP == 50


def test_a_row_already_flagged_is_not_asked_about(db):
    # It reaches the threshold on this miss and reaps anyway.
    _seed(db, ["KEEP"])
    _seed(db, ["FLAGGED"], missing=_REAP_MISS_THRESHOLD - 1)
    probe = _Probe({}, default=True)
    _walk(db, [_coll("KEEP")], probe)
    assert probe.asked == []
    assert "FLAGGED" not in _rows(db)


def test_the_mass_guard_still_aborts_a_large_miss(db):
    # 60 of 100 missing (> 50 rows AND > 20%) — every one 404s, the cap lets
    # 50 jump the grace window, and the guard must still refuse the reap.
    _seed(db, [f"K{i:03d}" for i in range(40)] + [f"M{i:03d}" for i in range(60)])
    probe = _Probe({}, default=False)
    _walk(db, [_coll(f"K{i:03d}") for i in range(40)], probe)
    rows = _rows(db)
    assert len(rows) == 100, "the v1.18.89 mass-guard must still abort"


def test_plex_is_asked_before_the_write_lock_is_taken(db):
    _seed(db, ["KEEP", "GONE"])
    held = []

    def probe(rk):
        # A writer would fail at once if the reaper's txn were open.
        c = sqlite3.connect(db, timeout=0)
        try:
            c.execute("BEGIN IMMEDIATE")
            c.execute("ROLLBACK")
        except sqlite3.OperationalError as e:
            held.append(str(e))
        finally:
            c.close()
        return False

    _walk(db, [_coll("KEEP")], probe)
    assert held == [], "HTTP must not run while the writer lock is held"
    assert "GONE" not in _rows(db)


def test_no_probe_keeps_the_old_grace(db):
    _seed(db, ["KEEP", "GONE"])
    plex_enum._upsert_items(db, [_coll("KEEP")], section_id=SEC)
    assert _rows(db)["GONE"] == 1


# ── wiring: collections are probed, movies and shows are not ──


class _FakeClient:
    def __init__(self):
        self.asked = []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def discover_sections(self):
        return [PlexSection(
            section_id=SEC, uuid="u1", title="Movies", type="movie",
            agent="a", language="en", location_paths=[],
            content_changed_at="1700000005",
        )]

    def enumerate_section_items(self, **kwargs):
        return [_movie("M-KEEP", "Kept")]

    def enumerate_collections_for_section(self, **kwargs):
        return [_coll("C-KEEP")]

    def item_exists(self, rk):
        self.asked.append(rk)
        return False


def test_run_plex_enum_probes_collections_only(db, monkeypatch):
    _seed(db, ["M-KEEP", "M-GONE"], media_type="movie")
    _seed(db, ["C-KEEP", "C-GONE"])
    fake = _FakeClient()
    monkeypatch.setattr(plex_enum, "PlexClient", lambda cfg: fake)
    plex_enum.run_plex_enum(db, PlexConfig(
        url="http://x", token="t", movie_section="1", tv_section="2"))
    rows = _rows(db)
    assert "C-GONE" not in rows, "the collections pass must probe and reap"
    assert rows.get("M-GONE") == 1, (
        "a movie keeps the grace window — no probe on the items pass")
    assert fake.asked == ["C-GONE"]
