"""v0.51.342: RESTORE FROM PLEX's store fetches run on a bounded pool, and the run keeps the serial answers.

  1. A pooled run (a client factory, 4 workers) and a serial one (one client) give the same summary,
     skipped order, database rows and bytes. Rows that share a canonical path keep the serial winner.
  2. One connection per run, and one transaction per stamp.
  3. A passed client is driven by one thread. Each factory client is driven by one thread, and every
     factory client is closed. Fetched bodies waiting to be written stay bounded.
  4. The canonical is re-checked right before store bytes are written.
  5. No-answers (transport, 502, 503, 504, listing or serving) back off, and stop the run only after a count
     AND a time window. Answers never do, and
     an answer starts the count and the window over. A stopped run does not ask for shared-path rows.
  6. Cancel stops new fetches (queued, or backing off) and the folder pass. A stamp that raises fails the run
     instead of hanging it.
"""
from __future__ import annotations

import functools
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from app.core import canonical_health as ch
from app.core import db as db_mod
from app.core.db import init_db

NOW = "2026-09-13T00:00:00"
SHARED = "movies/shared/theme.mp3"


# ── seed helpers ──────────────────────────────────────────────────────

def _section(conn):
    conn.execute(
        "INSERT INTO plex_sections (section_id, title, type, is_anime, is_4k,"
        " themes_subdir, included, discovered_at, last_seen_at)"
        " VALUES ('1', 'M', 'movie', 0, 0, 'movies', 1, ?, ?)"
        " ON CONFLICT(section_id) DO NOTHING", (NOW, NOW))


def _theme(conn, tmdb):
    conn.execute(
        "INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source,"
        " last_seen_sync_at, first_seen_sync_at, youtube_url)"
        " VALUES (?, 'movie', ?, ?, 'plex_orphan', ?, ?, NULL)",
        (tmdb, tmdb, f"T{tmdb}", NOW, NOW))


def _lf(conn, tmdb, *, file_path=None):
    conn.execute(
        "INSERT INTO local_files (media_type, tmdb_id, section_id, theme_id, file_path, file_size,"
        " file_sha256, downloaded_at, source_video_id, provenance, source_kind, canonical_present,"
        " edition_key) VALUES ('movie', ?, '1', ?, ?, 10, ?, ?, '', 'manual', 'upload', 0, '')",
        (tmdb, tmdb, file_path or f"movies/{tmdb}/theme.mp3", "0" * 64, NOW))


def _placement(conn, tmdb, media_folder, *, kind="hardlink", rk=None):
    conn.execute(
        "INSERT INTO placements (media_type, tmdb_id, section_id, media_folder, placement_kind,"
        " provenance, placed_at, plex_rating_key, theme_present, edition_key)"
        " VALUES ('movie', ?, '1', ?, ?, 'manual', ?, ?, NULL, '')",
        (tmdb, media_folder, kind, NOW, rk))


def _folder(plexdir: Path, name: str, data: bytes | None) -> str:
    folder = plexdir / name
    folder.mkdir(parents=True, exist_ok=True)
    if data is not None:
        (folder / "theme.mp3").write_bytes(data)
    return str(folder)


def _lf_cols(db, tmdb, cols):
    with sqlite3.connect(db) as conn:
        return conn.execute(f"SELECT {', '.join(cols)} FROM local_files WHERE tmdb_id = ?", (tmdb,)).fetchone()


class Meter:
    """Shared by every client of one run: the Plex calls, how many ran at once, the bodies handed back."""
    def __init__(self):
        self.lock = threading.Lock()
        self.calls: list[tuple] = []
        self.in_flight = 0
        self.max_in_flight = 0
        self.bodies = 0


class FakePlex:
    """A PlexClient stand-in: answers by rating key, records the threads that drive it, can be slow or silent.
    `status` fails the themes step and `fetch_fail` the fetch step, by rating key: an HTTP code or "transport"."""
    def __init__(self, meter=None, *, latency=0.0, slow=(), status=None, no_answer=None, on_fetch=None,
                 fetch_fail=None):
        self.meter = meter if meter is not None else Meter()
        self.latency, self.slow, self.status = latency, set(slow), dict(status or {})
        self.no_answer, self.on_fetch, self.fetch_fail = no_answer, on_fetch, dict(fetch_fail or {})
        self.threads: set = set()
        self.closed = False

    def _call(self, what, rk):
        m = self.meter
        with m.lock:
            self.threads.add(threading.get_ident())
            m.calls.append((what, rk))
            m.in_flight += 1
            m.max_in_flight = max(m.max_in_flight, m.in_flight)
        try:
            delay = 0.15 if rk in self.slow else self.latency
            if delay:
                time.sleep(delay)
        finally:
            with m.lock:
                m.in_flight -= 1

    def get_themes(self, *, rating_key):
        self._call("themes", rating_key)
        if self.no_answer is not None and self.no_answer():
            return {"ok": False, "http_status": None, "error": "transport: ConnectError('refused')", "body": None}
        if rating_key in self.status:
            return {"ok": False, "http_status": self.status[rating_key], "error": None, "body": None}
        return {"ok": True, "http_status": 200, "error": None,
                "body": {"MediaContainer": {"Metadata": [{"ratingKey": f"upload://themes/{rating_key}",
                                                          "selected": True}]}}}

    def fetch_theme_bytes(self, *, item_rating_key, entry_uri):
        self._call("fetch", item_rating_key)
        if self.on_fetch is not None:
            self.on_fetch(item_rating_key)
        # the real client's failure shapes (PlexClient.fetch_theme_bytes): a transport error, or a non-2xx status
        fail = self.fetch_fail.get(item_rating_key)
        if fail == "transport":
            return {"ok": False, "http_status": None, "bytes": None, "error": "transport: ReadTimeout('timed out')"}
        if fail is not None:
            return {"ok": False, "http_status": fail, "bytes": None, "error": f"HTTP {fail}"}
        with self.meter.lock:
            self.meter.bodies += 1
        return {"ok": True, "http_status": 200, "bytes": f"store-{item_rating_key}".encode()}

    def close(self):
        self.closed = True


def _seed_mix(root: Path):
    db = root / "m.db"
    init_db(db)
    themes, plexdir = root / "themes", root / "plex"
    with sqlite3.connect(db) as conn:
        _section(conn)
        for tmdb in range(401, 416):
            _theme(conn, tmdb)
            _lf(conn, tmdb, file_path=SHARED if tmdb in (414, 415) else None)
        # 401 is first in report order, the slowest fetch, and an answered failure
        _placement(conn, 401, "", kind="plex_upload", rk="9401")
        for tmdb in (402, 403, 404, 405):
            _placement(conn, tmdb, _folder(plexdir, str(tmdb), f"side-{tmdb}".encode()))
        for tmdb in (406, 407, 408, 412, 413, 414):
            _placement(conn, tmdb, "", kind="plex_upload", rk=f"9{tmdb}")
        for tmdb in (409, 410):
            _placement(conn, tmdb, _folder(plexdir, str(tmdb), None))
        # 411 has no placement; 414 (store) and 415 (sidecar) share one canonical path
        _placement(conn, 415, _folder(plexdir, "415", b"side-415"))
        conn.commit()
    present = themes / "movies" / "412" / "theme.mp3"
    present.parent.mkdir(parents=True, exist_ok=True)
    present.write_bytes(b"already-here")
    return db, themes


def _mix_plex(meter=None):
    return FakePlex(meter, slow={"9401"}, status={"9401": 500, "9413": 404})


_MIX_SKIPS = [(401, "plex_themes:500"), (409, "placement_file_missing"), (410, "placement_file_missing"),
              (411, "no_plex_copy"), (412, "canonical_already_present"), (413, "plex_themes:404"),
              (415, "canonical_already_present")]


def _state(db, themes):
    with sqlite3.connect(db) as conn:
        lf = conn.execute("SELECT tmdb_id, canonical_present, file_size, file_sha256 FROM local_files "
                          "ORDER BY tmdb_id").fetchall()
        pl = conn.execute("SELECT tmdb_id, media_folder, placement_kind FROM placements "
                          "ORDER BY tmdb_id, media_folder").fetchall()
    files = {p.relative_to(themes).as_posix(): p.read_bytes() for p in sorted(themes.rglob("theme.mp3"))}
    return lf, [(t, Path(f).name if f else f, k) for t, f, k in pl], files


def _seed_store(root: Path, n=8, present=(), shared=()):
    db = root / "m.db"
    init_db(db)
    themes = root / "themes"
    with sqlite3.connect(db) as conn:
        _section(conn)
        for tmdb in range(601, 601 + n):
            _theme(conn, tmdb)
            _lf(conn, tmdb, file_path=SHARED if tmdb in shared else None)
            _placement(conn, tmdb, "", kind="plex_upload", rk=f"9{tmdb}")
        conn.commit()
    for tmdb in present:
        c = themes / "movies" / str(tmdb) / "theme.mp3"
        c.parent.mkdir(parents=True, exist_ok=True)
        c.write_bytes(b"on-disk")
    return db, themes


# ── 1: the pool answers what the serial run answers ──────────────────

def test_the_pool_answers_exactly_what_the_serial_run_answers(tmp_path):
    db_s, th_s = _seed_mix(tmp_path / "serial")
    db_p, th_p = _seed_mix(tmp_path / "pool")
    serial = ch.restore_from_plex(db_s, th_s, _mix_plex())
    ticks, meter = [], Meter()
    pooled = ch.restore_from_plex(db_p, th_p, None, workers=4, plex_client_factory=lambda: _mix_plex(meter),
                                  progress_cb=lambda done, total, counts: ticks.append((done, total, counts)))
    assert [(s["tmdb_id"], s["reason"]) for s in serial["skipped"]] == _MIX_SKIPS, "skipped in report order"
    assert (serial["broken"], serial["restored_sidecar"], serial["restored_store"]) == (15, 4, 4)
    assert pooled == serial, "the pooled summary (skipped order included) must equal the serial one"
    assert _state(db_p, th_p) == _state(db_s, th_s)
    assert (th_p / SHARED).read_bytes() == b"store-9414", "the shared path keeps the first row's bytes"
    assert meter.max_in_flight >= 2, "premise: the pooled run really fetched side by side"
    assert ticks[0][:2] == (0, 15) and ticks[-1][:2] == (15, 15)
    assert [d for d, _t, _c in ticks] == sorted(d for d, _t, _c in ticks), "progress went backwards"
    assert ticks[-1][2] == {"restored_sidecar": 4, "restored_store": 4, "skipped_count": 7}


@pytest.mark.parametrize("pooled", [False, True], ids=["serial", "pooled"])
def test_a_shared_canonical_path_keeps_the_serial_winner(tmp_path, pooled):
    db = tmp_path / "m.db"
    init_db(db)
    themes, plexdir = tmp_path / "themes", tmp_path / "plex"
    with sqlite3.connect(db) as conn:
        _section(conn)
        for tmdb in (501, 502):
            _theme(conn, tmdb)
            _lf(conn, tmdb, file_path=SHARED)
        # 501 is first in report order and its fetch is slow; 502's sidecar sits on local disk
        _placement(conn, 501, "", kind="plex_upload", rk="9501")
        _placement(conn, 502, _folder(plexdir, "502", b"sidecar-502"))
        conn.commit()

    def make():
        return FakePlex(slow={"9501"})
    res = (ch.restore_from_plex(db, themes, None, workers=4, plex_client_factory=make) if pooled
           else ch.restore_from_plex(db, themes, make()))
    assert (res["restored_store"], res["restored_sidecar"]) == (1, 0)
    assert [(s["tmdb_id"], s["reason"]) for s in res["skipped"]] == [(502, "canonical_already_present")]
    assert (themes / SHARED).read_bytes() == b"store-9501"
    assert _lf_cols(db, 502, ("canonical_present",)) == (1,), "the second row is stamped present"


# ── 2: one connection, one transaction per stamp ─────────────────────

def test_one_connection_per_run_and_one_transaction_per_stamp(tmp_path, monkeypatch):
    db, themes = _seed_mix(tmp_path)
    real = sqlite3.connect
    seen = {"connects": 0, "begins": 0}

    def trace(sql):
        if sql.lstrip().upper().startswith("BEGIN"):
            seen["begins"] += 1

    def connect(*a, **k):
        c = real(*a, **k)
        seen["connects"] += 1
        c.set_trace_callback(trace)
        return c
    monkeypatch.setattr(db_mod.sqlite3, "connect", connect)
    res = ch.restore_from_plex(db, themes, None, workers=4, plex_client_factory=_mix_plex)
    monkeypatch.undo()
    present = sum(1 for s in res["skipped"] if s["reason"] == "canonical_already_present")
    assert res["restored"] == 8 and present == 2
    assert seen["connects"] == 1, f"{seen['connects']} connections for one run"
    assert seen["begins"] == res["restored"] + present


# ── 3: threads, clients, bounded bodies ──────────────────────────────

def test_a_passed_client_is_one_thread_and_each_factory_client_its_own(tmp_path):
    db, themes = _seed_store(tmp_path / "passed")
    meter = Meter()
    plex = FakePlex(meter, latency=0.03)
    res = ch.restore_from_plex(db, themes, plex, workers=4)
    assert res["restored_store"] == 8
    assert meter.max_in_flight == 1 and len(plex.threads) == 1, "one PlexClient driven by several threads"
    db2, themes2 = _seed_store(tmp_path / "factory")
    meter2, made = Meter(), []
    res2 = ch.restore_from_plex(db2, themes2, None, workers=4,
                                plex_client_factory=lambda: made.append(FakePlex(meter2, latency=0.03)) or made[-1])
    assert res2["restored_store"] == 8
    assert 2 <= meter2.max_in_flight <= 4
    assert all(len(c.threads) == 1 for c in made), "a factory client was shared across threads"


def test_fetched_bodies_waiting_to_be_written_stay_bounded(tmp_path, monkeypatch):
    db, themes = _seed_store(tmp_path, n=30)
    meter, published, waiting = Meter(), [], []
    real = ch._publish_store_bytes

    def publish(*a, **k):
        with meter.lock:
            waiting.append(meter.bodies - len(published))
        time.sleep(0.01)
        published.append(1)
        return real(*a, **k)
    monkeypatch.setattr(ch, "_publish_store_bytes", publish)
    res = ch.restore_from_plex(db, themes, None, workers=4, plex_client_factory=lambda: FakePlex(meter))
    assert res["restored_store"] == 30
    assert max(waiting) >= 2, "premise: bodies did pile up behind a slow writer"
    assert max(waiting) <= 2 * 4, f"{max(waiting)} fetched bodies waited at once"


def test_every_factory_client_is_closed_pool_and_shared_path_tail(tmp_path):
    db, themes = _seed_store(tmp_path, n=6, shared=(605, 606))
    made = []
    res = ch.restore_from_plex(db, themes, None, workers=4,
                               plex_client_factory=lambda: made.append(FakePlex(latency=0.01)) or made[-1])
    assert res["restored_store"] == 5
    assert [(s["tmdb_id"], s["reason"]) for s in res["skipped"]] == [(606, "canonical_already_present")]
    assert len(made) >= 2, "premise: the pool and the shared-path tail each minted a client"
    assert all(c.closed for c in made), "a factory client was left open"


# ── 4: the re-check before the write ─────────────────────────────────

def test_a_canonical_written_while_the_bytes_were_in_flight_wins(tmp_path):
    fresh = b"downloaded-while-plex-answered"
    db, themes = _seed_store(tmp_path, n=1)
    canonical = themes / "movies" / "601" / "theme.mp3"

    def a_download_lands(_rk):
        canonical.parent.mkdir(parents=True, exist_ok=True)
        canonical.write_bytes(fresh)
    before = _lf_cols(db, 601, ("file_size", "file_sha256"))
    res = ch.restore_from_plex(db, themes, FakePlex(on_fetch=a_download_lands))
    assert res["restored_store"] == 0
    assert [(s["tmdb_id"], s["reason"]) for s in res["skipped"]] == [(601, "canonical_already_present")]
    assert canonical.read_bytes() == fresh, "the store bytes overwrote a fresh download"
    assert _lf_cols(db, 601, ("canonical_present", "file_size", "file_sha256")) == (1, *before)


# ── 5: the Plex stop rule ────────────────────────────────────────────

class FakeClock:
    def __init__(self):
        self.t = 0.0
        self.sleeps: list[float] = []

    def __call__(self):
        return self.t

    def sleep(self, s):
        self.sleeps.append(s)
        self.t += s


@pytest.fixture
def clock(monkeypatch):
    c = FakeClock()
    monkeypatch.setattr(ch, "_PlexGate", functools.partial(ch._PlexGate, clock=c, sleep=c.sleep))
    return c


def test_a_dead_plex_stops_being_asked_only_after_the_count_and_the_window(tmp_path, clock):
    db, themes = _seed_store(tmp_path, n=40, present=(640,))
    fetchable = 39
    asked = []
    res = ch.restore_from_plex(db, themes, FakePlex(no_answer=lambda: asked.append(clock()) or True))
    assert res["plex_unreachable"] is True
    assert len(asked) < fetchable, "Plex was asked for every row"
    assert len(asked) >= ch._PLEX_TRIP_AFTER and asked[-1] - asked[0] >= ch._PLEX_TRIP_WINDOW_S
    assert asked[-2] - asked[0] < ch._PLEX_TRIP_WINDOW_S, "it kept asking past the first request over the window"
    reasons = {s["tmdb_id"]: s["reason"] for s in res["skipped"]}
    assert reasons[640] == "canonical_already_present", "the present guard still runs after the trip"
    assert _lf_cols(db, 640, ("canonical_present",)) == (1,)
    assert sum(r.startswith("plex_themes:transport") for r in reasons.values()) == len(asked)
    assert list(reasons.values()).count("plex_unreachable") == fetchable - len(asked)


def test_a_plex_restart_shorter_than_the_window_does_not_end_the_run(tmp_path, clock):
    db, themes = _seed_store(tmp_path, n=20)
    res = ch.restore_from_plex(db, themes, FakePlex(no_answer=lambda: clock() < 45.0))
    reasons = [s["reason"] for s in res["skipped"]]
    assert res["plex_unreachable"] is False and "plex_unreachable" not in reasons
    assert len(reasons) >= ch._PLEX_TRIP_AFTER, "premise: more no-answers in a row than the count alone allows"
    assert all(r.startswith("plex_themes:transport") for r in reasons)
    assert res["restored_store"] == 20 - len(reasons) > 0


def test_an_answer_starts_the_count_and_the_window_over(tmp_path, clock):
    db, themes = _seed_store(tmp_path, n=40)
    calls: list[tuple[float, bool]] = []
    marks: dict = {}

    def no_answer():
        silent = clock() < 45.0 or 20 <= len(calls) + 1 <= 28
        if silent and calls and not calls[-1][1]:
            marks.setdefault("second_restart", len(clock.sleeps))
        calls.append((clock(), silent))
        return silent

    def on_fetch(_rk):
        marks.setdefault("answered", len(clock.sleeps))
    res = ch.restore_from_plex(db, themes, FakePlex(no_answer=no_answer, on_fetch=on_fetch))
    reasons = [s["reason"] for s in res["skipped"]]
    assert res["plex_unreachable"] is False and "plex_unreachable" not in reasons, \
        "a second Plex restart was counted on top of the first, though Plex answered in between"
    assert marks["second_restart"] == marks["answered"], "Plex answered, yet the fetches kept backing off"
    runs, cur = [], []
    for t, silent in calls + [(clock(), False)]:
        if silent:
            cur.append(t)
        elif cur:
            runs, cur = runs + [cur], []
    assert len(runs) == 2, "premise: two restarts with answers between them"
    first, second = runs
    assert len(second) >= ch._PLEX_TRIP_AFTER, "premise: the second restart alone reaches the count"
    assert max(first[-1] - first[0], second[-1] - second[0]) < ch._PLEX_TRIP_WINDOW_S
    assert second[-1] - first[0] >= ch._PLEX_TRIP_WINDOW_S, "premise: a window kept from the first restart trips"
    assert all(r.startswith("plex_themes:transport") for r in reasons)
    assert res["restored_store"] == 40 - len(reasons) > 0


def test_shared_path_rows_are_not_asked_once_the_run_stopped_asking(tmp_path, clock):
    db, themes = _seed_store(tmp_path, n=40, shared=(639, 640))
    meter = Meter()
    res = ch.restore_from_plex(db, themes, FakePlex(meter, no_answer=lambda: True))
    assert res["plex_unreachable"] is True
    reasons = {s["tmdb_id"]: s["reason"] for s in res["skipped"]}
    assert (reasons[639], reasons[640]) == ("plex_unreachable", "plex_unreachable")
    assert not {rk for _what, rk in meter.calls} & {"9639", "9640"}, \
        "the shared-path tail asked a Plex the run had stopped asking"


@pytest.mark.parametrize("step", ["themes", "fetch"])
@pytest.mark.parametrize("failure", ["transport", 502, 503, 504])
def test_no_answer_at_either_step_backs_off_and_stops_the_run(tmp_path, clock, step, failure):
    # the design's rule: a transport failure, 502, 503 or 504 is no answer, whether Plex fails listing or serving
    n = 40
    db, themes = _seed_store(tmp_path, n=n)
    fails = {f"9{601 + k}": failure for k in range(n)}
    asked: list[float] = []
    if step == "themes":
        plex = FakePlex(status=fails, no_answer=lambda: asked.append(clock()) or failure == "transport")
    else:
        plex = FakePlex(fetch_fail=fails, on_fetch=lambda _rk: asked.append(clock()))
    res = ch.restore_from_plex(db, themes, plex)
    assert res["plex_unreachable"] is True, f"a Plex answering {failure} at the {step} step never stopped the run"
    assert clock.sleeps, f"{failure} at the {step} step did not back the fetches off"
    assert clock.sleeps == sorted(clock.sleeps), "a backoff shrank while Plex still gave no answer"
    assert ch._PLEX_TRIP_AFTER <= len(asked) < n
    assert asked[-1] - asked[0] >= ch._PLEX_TRIP_WINDOW_S, "the run stopped before the window"
    reasons = [s["reason"] for s in res["skipped"]]
    assert sum(r.startswith(f"plex_{step}:{failure}") for r in reasons) == len(asked)
    assert reasons.count("plex_unreachable") == n - len(asked) and res["restored_store"] == 0


@pytest.mark.parametrize("step", ["themes", "fetch"])
def test_answered_failures_never_back_off_or_stop_the_run(tmp_path, clock, step):
    db, themes = _seed_store(tmp_path, n=20)
    codes = (500, 401, 404)
    meter = Meter()
    answers = {f"9{601 + k}": codes[k % 3] for k in range(20)}
    plex = FakePlex(meter, status=answers) if step == "themes" else FakePlex(meter, fetch_fail=answers)
    res = ch.restore_from_plex(db, themes, plex)
    assert sum(1 for what, _rk in meter.calls if what == step) == 20, "every row was asked"
    assert clock.sleeps == [] and res["plex_unreachable"] is False
    assert {s["reason"] for s in res["skipped"]} == {f"plex_{step}:{c}" for c in codes}


def test_backoff_sleeps_grow_and_are_capped(tmp_path, clock):
    db, themes = _seed_store(tmp_path, n=40)
    ch.restore_from_plex(db, themes, FakePlex(no_answer=lambda: True))
    assert clock.sleeps, "premise: a dead Plex backed the fetches off"
    assert clock.sleeps == sorted(clock.sleeps), "a backoff shrank while Plex still gave no answer"
    assert max(clock.sleeps) <= ch._PLEX_BACKOFF_CAP_S and ch._PLEX_BACKOFF_CAP_S in clock.sleeps


# ── 6: cancel, and a stamp that raises ───────────────────────────────

@pytest.mark.parametrize("pooled", [False, True], ids=["serial", "pooled"])
def test_cancel_stops_new_fetches_and_accounts_for_every_row(tmp_path, monkeypatch, pooled):
    db, themes = _seed_store(tmp_path, n=24)
    flag = {"cancel": False, "seen": False, "late_submits": 0, "fetches": 0}
    lock = threading.Lock()

    class CountingPool(ThreadPoolExecutor):
        def submit(self, fn, /, *a, **k):
            if flag["seen"]:
                flag["late_submits"] += 1
            return super().submit(fn, *a, **k)
    monkeypatch.setattr(ch, "ThreadPoolExecutor", CountingPool)

    def on_fetch(_rk):
        with lock:
            flag["fetches"] += 1
            flag["cancel"] = flag["cancel"] or flag["fetches"] >= 3

    def cancel_check():
        flag["seen"] = flag["seen"] or flag["cancel"]
        return flag["cancel"]

    def make():
        return FakePlex(latency=0.01, on_fetch=on_fetch)
    res = (ch.restore_from_plex(db, themes, None, workers=4, plex_client_factory=make, cancel_check=cancel_check)
           if pooled else ch.restore_from_plex(db, themes, make(), cancel_check=cancel_check))
    assert res["cancelled"] is True and res["not_attempted"] > 0
    assert res["restored"] + len(res["skipped"]) + res["not_attempted"] == 24
    assert flag["late_submits"] == 0, "a fetch was submitted after the run saw the cancel"


def test_cancel_drops_queued_fetches_and_ends_when_the_running_ones_return(tmp_path, monkeypatch):
    n, workers = 16, 4
    db, themes = _seed_store(tmp_path, n=n)
    lock = threading.Lock()
    entered, late = [], []
    all_in, seen, waiting, release, never = (threading.Event() for _ in range(5))

    class HungPlex(FakePlex):
        """Every fetch hangs until released; one that starts after the cancel was seen hangs for good."""
        def get_themes(self, *, rating_key):
            with lock:
                if seen.is_set():
                    late.append(rating_key)
                    hold = never
                else:
                    entered.append(rating_key)
                    if len(entered) == workers:
                        all_in.set()
                    hold = release
            hold.wait(30)
            return super().get_themes(rating_key=rating_key)

    def cancel_check():
        if all_in.is_set():
            seen.set()
        return seen.is_set()
    starts_after = []
    monkeypatch.setattr(ch, "_PlexGate", _gate_spy(lambda: seen.is_set() and starts_after.append(1)))
    monkeypatch.setattr(ch, "wait", _wait_spy(seen, waiting))
    out = {}
    t = threading.Thread(target=lambda: out.update(res=ch.restore_from_plex(
        db, themes, None, workers=workers, plex_client_factory=HungPlex, cancel_check=cancel_check)), daemon=True)
    t.start()
    try:
        assert seen.wait(10), "premise: every worker hung in Plex and the run saw the cancel"
        assert waiting.wait(10), "premise: the run went back to waiting on its fetches"
        release.set()
        t.join(10)
        assert not t.is_alive(), "the cancel waited on fetches queued behind a hung Plex"
    finally:
        release.set()
        never.set()
        t.join(10)
    res = out["res"]
    assert late == [], "a queued fetch asked Plex after the run saw the cancel"
    assert starts_after == [], "a queued fetch started after the run saw the cancel"
    assert (res["cancelled"], res["restored_store"], res["not_attempted"]) == (True, workers, n - workers)
    assert res["restored"] + len(res["skipped"]) + res["not_attempted"] == n


def _gate_spy(on_before, **kw):
    """The run's _PlexGate, reporting each fetch's first step (its before()) to on_before first."""
    class Spy(ch._PlexGate):
        def before(self):
            on_before()
            return super().before()
    return functools.partial(Spy, **kw)


def _wait_spy(seen, waiting):
    """ch.wait, setting `waiting` once the run waits on its fetches again after it saw the cancel."""
    real_wait = ch.wait

    def spy(fs, *a, **k):
        if seen.is_set():
            waiting.set()
        return real_wait(fs, *a, **k)
    return spy


@pytest.mark.parametrize("backoff", ["default-wait", "sleep-released-after-cancel"])
def test_a_fetch_backing_off_when_the_cancel_is_seen_never_asks_plex(tmp_path, monkeypatch, backoff):
    n, workers = 16, 4
    db, themes = _seed_store(tmp_path, n=n)
    lock = threading.Lock()
    first = threading.Barrier(workers)
    asked, begun, backing = [], set(), set()
    in_backoff, seen, waiting, release = (threading.Event() for _ in range(4))

    def no_answer():
        # every worker's first fetch gives no answer once all are in, so each one's next fetch backs off
        with lock:
            k = len(asked)
            asked.append(seen.is_set())
        if k < workers:
            first.wait(10)
            return True
        return False

    def on_before():
        me = threading.get_ident()
        with lock:
            if me in begun:
                backing.add(me)
                if len(backing) == workers:
                    in_backoff.set()
            begun.add(me)

    if backoff == "default-wait":
        monkeypatch.setattr(ch, "_PLEX_BACKOFF_BASE_S", 30.0)
        monkeypatch.setattr(ch, "_PLEX_BACKOFF_CAP_S", 30.0)
        monkeypatch.setattr(ch, "_PlexGate", _gate_spy(on_before))
    else:
        monkeypatch.setattr(ch, "_PlexGate", _gate_spy(on_before, sleep=lambda _s: release.wait(30)))
    monkeypatch.setattr(ch, "wait", _wait_spy(seen, waiting))
    meter = Meter()

    def cancel_check():
        if in_backoff.is_set():
            seen.set()
        return seen.is_set()
    out = {}
    t = threading.Thread(target=lambda: out.update(res=ch.restore_from_plex(
        db, themes, None, workers=workers, plex_client_factory=lambda: FakePlex(meter, no_answer=no_answer),
        cancel_check=cancel_check)), daemon=True)
    t.start()
    try:
        assert seen.wait(10), "premise: every worker was backing off after a no-answer when the run saw the cancel"
        assert waiting.wait(10), "premise: the run went back to waiting on its fetches"
        release.set()
        t.join(10)
        assert not t.is_alive(), "the cancel waited out a backoff"
    finally:
        release.set()
        t.join(10)
    res = out["res"]
    assert asked.count(True) == 0, "a fetch backing off when the cancel was seen asked Plex after it"
    assert len(asked) == workers
    assert (res["cancelled"], res["restored"], res["not_attempted"]) == (True, 0, n - workers)
    assert all(s["reason"].startswith("plex_themes:transport") for s in res["skipped"])
    assert res["restored"] + len(res["skipped"]) + res["not_attempted"] == n


def test_cancel_during_the_folder_pass_stops_before_the_next_row(tmp_path):
    db = tmp_path / "m.db"
    init_db(db)
    themes, plexdir = tmp_path / "themes", tmp_path / "plex"
    with sqlite3.connect(db) as conn:
        _section(conn)
        for tmdb in range(701, 707):
            _theme(conn, tmdb)
            _lf(conn, tmdb)
            _placement(conn, tmdb, _folder(plexdir, str(tmdb), f"side-{tmdb}".encode()))
        conn.commit()
    ticks: list[int] = []
    res = ch.restore_from_plex(db, themes, None, progress_cb=lambda done, _total, _counts: ticks.append(done),
                               cancel_check=lambda: ticks[-1] >= 2)
    assert res["cancelled"] is True
    assert (res["restored_sidecar"], res["not_attempted"]) == (2, 4)
    assert [t for t in range(701, 707) if (themes / "movies" / str(t) / "theme.mp3").exists()] == [701, 702], \
        "a folder row was restored after the run saw the cancel"


def test_a_stamp_that_raises_fails_the_run_instead_of_hanging(tmp_path, monkeypatch):
    real = ch._stamp_restored

    def stamp(db_path, r, *a, **k):
        if r["tmdb_id"] == 603:
            raise sqlite3.OperationalError("database is locked")
        return real(db_path, r, *a, **k)
    monkeypatch.setattr(ch, "_stamp_restored", stamp)
    db, themes = _seed_store(tmp_path / "pooled", n=12)
    out = {}

    def run():
        try:
            ch.restore_from_plex(db, themes, None, workers=4, plex_client_factory=lambda: FakePlex(latency=0.02))
        except sqlite3.OperationalError as e:
            out["err"] = e
    t = threading.Thread(target=run, daemon=True)
    t.start()
    t.join(10)
    assert not t.is_alive(), "the run hung on a failed stamp"
    assert "locked" in str(out.get("err")), "the failed stamp must fail the run"
    db2, themes2 = _seed_store(tmp_path / "serial", n=6)
    with pytest.raises(sqlite3.OperationalError, match="locked"):
        ch.restore_from_plex(db2, themes2, FakePlex())
    assert [_lf_cols(db2, t, ("canonical_present",))[0] for t in (601, 602, 603)] == [1, 1, 0], \
        "the rows written before the failure stay committed"
