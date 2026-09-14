"""v0.51.339: // CANONICAL HEALTH restore follow-ups (review of v0.51.328..337, tag 2).

  1. RESTORE FROM PLEX (N) promises exactly what the bulk restores: the sidecar by
     stat (never the stored theme_present), the store only with Plex configured, a
     numeric rating key, and a plex_upload placement or no folder at all.
  2. Two placement rows for one canonical: the folder that still holds the sidecar
     wins (theme_present then recency among survivors); with none surviving, a row
     Plex's store can serve; a folder restore re-kinds only its own placement row.
  3. The missing block repaints on every render — no stale rows under the bulk.
  4. The bulk's status line words each skip reason instead of "had no Plex copy".
  5. A canonical_already_present skip stamps canonical_present = 1 (verify's rule).
  6. The INFO card's per-item restore runs the bulk's restore_from_placement.
  7. adopt and the cloud backup stamp canonical_present for the canonical they wrote.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from app.core import canonical_health as ch
from app.core.db import get_conn, init_db

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
NOW = "2026-09-13T00:00:00"
AUTH = {"X-Authentik-Username": "testadmin"}
_NODE = shutil.which("node")

if os.environ.get("MOTIF_REQUIRE_NODE") and not _NODE:
    raise RuntimeError("MOTIF_REQUIRE_NODE=1 but node is not on PATH — the CANONICAL HEALTH "
                       "render harness would silently not run")

_NORM_COLS = ("loudness_i", "loudness_tp", "loudness_lra", "loudness_measured_at",
              "loudness_measured_sha256", "norm_state", "norm_gain_db", "norm_target",
              "norm_at", "norm_orig_sha256", "norm_orig_pcm_sha256", "norm_plex_entry_uri")
_LEVELLED = {"loudness_i": -18.1, "loudness_tp": -1.2, "loudness_lra": 6.0,
             "loudness_measured_at": NOW, "loudness_measured_sha256": "0" * 64,
             "norm_state": "normalized", "norm_gain_db": -3.0, "norm_target": -18.0,
             "norm_at": NOW, "norm_orig_sha256": "a" * 64, "norm_orig_pcm_sha256": "b" * 64,
             "norm_plex_entry_uri": "upload://themes/old"}


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


def _lf(conn, tmdb, *, canonical_present=0, file_size=None, file_sha256=None, extra=None):
    cols = {"media_type": "movie", "tmdb_id": tmdb, "section_id": "1", "theme_id": tmdb,
            "file_path": f"movies/{tmdb}/theme.mp3", "file_size": file_size,
            "file_sha256": file_sha256, "downloaded_at": NOW, "source_video_id": "",
            "provenance": "manual", "source_kind": "upload",
            "canonical_present": canonical_present, "edition_key": ""}
    cols.update(extra or {})
    conn.execute(f"INSERT INTO local_files ({','.join(cols)}) VALUES ({','.join('?' * len(cols))})",
                 tuple(cols.values()))


def _placement(conn, tmdb, media_folder, *, kind="hardlink", rk=None, theme_present=None, placed_at=NOW):
    conn.execute(
        "INSERT INTO placements (media_type, tmdb_id, section_id, media_folder, placement_kind,"
        " provenance, placed_at, plex_rating_key, theme_present, edition_key)"
        " VALUES ('movie', ?, '1', ?, ?, 'manual', ?, ?, ?, '')",
        (tmdb, media_folder, kind, placed_at, rk, theme_present))


def _db(tmp_path: Path):
    db = tmp_path / "m.db"
    init_db(db)
    return db, tmp_path / "themes", tmp_path / "plex"


def _folder(plexdir: Path, name: str, data: bytes | None = b"sidecar-bytes") -> str:
    folder = plexdir / name
    folder.mkdir(parents=True, exist_ok=True)
    if data is not None:
        (folder / "theme.mp3").write_bytes(data)
    return str(folder)


def _canonical(themes: Path, tmdb: int) -> Path:
    return themes / "movies" / str(tmdb) / "theme.mp3"


def _lf_cols(db, tmdb, cols):
    with sqlite3.connect(db) as conn:
        return conn.execute(f"SELECT {', '.join(cols)} FROM local_files WHERE tmdb_id = ?",
                            (tmdb,)).fetchone()


class FakePlex:
    """The two calls the store path makes; records every one."""
    def __init__(self, body=b"plex-store-bytes"):
        self.body = body
        self.calls: list[tuple] = []

    def get_themes(self, *, rating_key):
        self.calls.append(("themes", rating_key))
        return {"ok": True, "http_status": 200, "error": None,
                "body": {"MediaContainer": {"Metadata": [{"ratingKey": "upload://themes/abc",
                                                          "selected": True}]}}}

    def fetch_theme_bytes(self, *, item_rating_key, entry_uri):
        self.calls.append(("fetch", item_rating_key, entry_uri))
        return {"ok": True, "http_status": 200, "bytes": self.body}


@pytest.fixture
def admin_client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.web import api as api_mod
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    (tmp_path / "themes").mkdir(parents=True, exist_ok=True)
    settings._cfg.paths.themes_dir = str(tmp_path / "themes")
    init_db(settings.db_path)
    init_auth_schema(settings.db_path)
    create_admin(settings.db_path, username="testadmin", password="testpassword")
    monkeypatch.setattr(api_mod, "log_event", lambda *a, **k: None)
    return TestClient(api_mod.create_app(settings)), settings, tmp_path


# ── 1: the restorable count is what the bulk restores ────────────────

def _seed_gates(tmp_path):
    db, themes, plexdir = _db(tmp_path)
    with sqlite3.connect(db) as conn:
        _section(conn)
        for tmdb in (301, 302, 303, 304, 305, 306, 307):
            _theme(conn, tmdb)
            _lf(conn, tmdb)
        _placement(conn, 301, "", kind="plex_upload", rk="9301")
        # a database restored onto a new box: theme_present says live, the folder is empty
        _placement(conn, 302, _folder(plexdir, "302", data=None), theme_present=1)
        _placement(conn, 303, "", kind="plex_upload", rk="upload-key")
        _placement(conn, 304, "", kind="hardlink", rk="9304")
        _placement(conn, 305, _folder(plexdir, "305"))
        # the commonest shape: a sidecar placement records its rating key too, but the store never serves a folder row
        _placement(conn, 307, _folder(plexdir, "307", data=None), kind="hardlink", rk="9307")
        conn.commit()
    return db, themes


@pytest.mark.parametrize("plex_on", [False, True], ids=["plex-off", "plex-on"])
def test_restorable_count_is_exactly_what_the_bulk_restores(tmp_path, plex_on):
    db, themes = _seed_gates(tmp_path)
    with get_conn(db) as conn:
        rep = ch.broken_canonical_report(conn, themes, plex_available=plex_on)
    promised = {e["tmdb_id"]: e["plex_copy"] for e in rep["redownloadable"] + rep["canonical_missing"]}
    store = "store" if plex_on else None
    assert promised == {301: store, 302: None, 303: None, 304: store, 305: "sidecar", 306: None, 307: None}
    res = ch.restore_from_plex(db, themes, FakePlex() if plex_on else None)
    restored = set(promised) - {s["tmdb_id"] for s in res["skipped"]}
    assert {t for t, c in promised.items() if c} == restored, "every promised row restores, and no other"
    assert rep["counts"]["restorable_from_plex"] == res["restored"]
    kinds = list(promised.values())
    assert (kinds.count("sidecar"), kinds.count("store")) == (res["restored_sidecar"], res["restored_store"])


def test_both_report_endpoints_count_the_store_only_with_plex_configured(admin_client):
    client, settings, tmp_path = admin_client
    with sqlite3.connect(settings.db_path) as conn:
        _section(conn)
        _theme(conn, 401)
        _lf(conn, 401)
        _placement(conn, 401, "", kind="plex_upload", rk="9401")
        conn.commit()

    def counts():
        rep = client.get("/api/admin/canonical-health/report", headers=AUTH)
        chk = client.post("/api/admin/canonical-health/check", headers=AUTH)
        assert rep.status_code == 200 and chk.status_code == 200, (rep.text, chk.text)
        return rep.json()["counts"]["restorable_from_plex"], chk.json()["counts"]["restorable_from_plex"]

    assert counts() == (0, 0), "Plex off: the bulk would skip this row as plex_unavailable"
    settings._cfg.plex.enabled = True
    settings._cfg.plex.url = "http://plex.test:32400"
    settings._cfg.plex.token = "token-for-test"
    assert counts() == (1, 1)


# ── 2: which placement row a canonical restores from ─────────────────

@pytest.mark.parametrize("live_inserted_first", [True, False], ids=["live-first", "dead-first"])
def test_the_folder_still_holding_the_sidecar_is_the_one_restored(tmp_path, live_inserted_first):
    db, themes, plexdir = _db(tmp_path)
    # the dead folder sorts first by every other key: PK order, a stale theme_present, recency
    dead = (_folder(plexdir, "A-moved-away", data=None), 1, "2026-09-10T00:00:00")
    live = (_folder(plexdir, "B-current", data=b"live-sidecar-bytes"), None, "2026-09-01T00:00:00")
    with sqlite3.connect(db) as conn:
        _section(conn)
        _theme(conn, 701)
        _lf(conn, 701)
        for folder, present, at in ((live, dead) if live_inserted_first else (dead, live)):
            _placement(conn, 701, folder, theme_present=present, placed_at=at)
        conn.commit()
    with get_conn(db) as conn:
        rep = ch.broken_canonical_report(conn, themes)
    assert rep["canonical_missing"][0]["plex_copy"] == "sidecar"
    res = ch.restore_from_plex(db, themes, None)
    assert (res["restored_sidecar"], res["skipped"]) == (1, [])
    assert _canonical(themes, 701).read_bytes() == b"live-sidecar-bytes"


@pytest.mark.parametrize("winner_present, winner_at, other_present, other_at", [
    (1, "2026-09-01T00:00:00", 0, "2026-09-10T00:00:00"),
    (None, "2026-09-10T00:00:00", None, "2026-09-01T00:00:00"),
], ids=["theme-present-outranks-recency", "tie-goes-to-the-most-recent"])
def test_with_two_surviving_sidecars_the_order_is_theme_present_then_recency(
        tmp_path, winner_present, winner_at, other_present, other_at):
    db, themes, plexdir = _db(tmp_path)
    with sqlite3.connect(db) as conn:
        _section(conn)
        _theme(conn, 702)
        _lf(conn, 702)
        # the loser is inserted first AND first by media_folder — only the ORDER BY can put the winner ahead
        _placement(conn, 702, _folder(plexdir, "A-other", b"other-bytes"),
                   theme_present=other_present, placed_at=other_at)
        _placement(conn, 702, _folder(plexdir, "Z-winner", b"winner-bytes"),
                   theme_present=winner_present, placed_at=winner_at)
        conn.commit()
    with get_conn(db) as conn:
        rep = ch.broken_canonical_report(conn, themes)
    assert rep["canonical_missing"][0]["plex_copy"] == "sidecar"
    res = ch.restore_from_plex(db, themes, None)
    assert (res["restored_sidecar"], res["skipped"]) == (1, [])
    assert _canonical(themes, 702).read_bytes() == b"winner-bytes"


@pytest.mark.parametrize("folder_present", [1, 0, None], ids=["stale-present", "stamped-missing", "unverified"])
def test_an_unverified_plex_upload_beside_a_dead_folder_restores_from_the_store(tmp_path, folder_present):
    db, themes, plexdir = _db(tmp_path)
    with sqlite3.connect(db) as conn:
        _section(conn)
        _theme(conn, 703)
        _lf(conn, 703)
        # the leftover folder row outranks by theme_present and by recency
        _placement(conn, 703, _folder(plexdir, "Movie (2020)", data=None),
                   theme_present=folder_present, placed_at="2026-09-10T00:00:00")
        # the worker's and the re-upload path's plex_upload inserts never set theme_present
        _placement(conn, 703, "", kind="plex_upload", rk="9703", theme_present=None,
                   placed_at="2026-09-01T00:00:00")
        conn.commit()
    with get_conn(db) as conn:
        rep = ch.broken_canonical_report(conn, themes, plex_available=True)
    assert (rep["canonical_missing"][0]["plex_copy"], rep["counts"]["restorable_from_plex"]) == ("store", 1)
    res = ch.restore_from_plex(db, themes, FakePlex(b"store-703"))
    assert (res["restored_store"], res["skipped"]) == (1, [])
    assert _canonical(themes, 703).read_bytes() == b"store-703"


def test_restoring_through_a_folder_leaves_a_plex_upload_sibling_its_kind(tmp_path):
    db, themes, plexdir = _db(tmp_path)
    live = _folder(plexdir, "Movie (2020)", b"sidecar-704")
    with sqlite3.connect(db) as conn:
        _section(conn)
        _theme(conn, 704)
        _lf(conn, 704)
        # a re-upload inserts the plex_upload row without deleting the folder row
        _placement(conn, 704, live, kind="copy", theme_present=1, placed_at="2026-09-01T00:00:00")
        _placement(conn, 704, "", kind="plex_upload", rk="9704", theme_present=1,
                   placed_at="2026-09-10T00:00:00")
        conn.commit()
    res = ch.restore_from_plex(db, themes, FakePlex())
    assert (res["restored_sidecar"], res["skipped"]) == (1, [])
    linked = _canonical(themes, 704).stat().st_ino == (Path(live) / "theme.mp3").stat().st_ino
    assert linked, "same filesystem under tmp_path — the restore must change the kind for the sibling to be at risk"
    with sqlite3.connect(db) as conn:
        kinds = dict(conn.execute("SELECT media_folder, placement_kind FROM placements WHERE tmdb_id = 704"))
    assert kinds == {live: "hardlink", "": "plex_upload"}, "the store-side row keeps the kind every staleness pass keys on"


# ── 3 + 4: the page (the live bindCanonicalHealth, driven under node) ──

_DRIVER = r"""
"use strict";
const fs = require("node:fs");
const vm = require("node:vm");
const [, , srcPath, scenarioPath] = process.argv;
const scenario = JSON.parse(fs.readFileSync(scenarioPath, "utf8"));
const els = new Map();
const document = {
  getElementById(id) {
    if (!els.has(id)) {
      const el = { id, style: { display: "none" }, innerHTML: "", className: "", disabled: false,
                   listeners: {}, texts: [], shown: "", dataset: {},
                   addEventListener(type, fn) { this.listeners[type] = fn; } };
      // v0.51.342: every text an element showed, so a note the restore poll replaces still counts.
      Object.defineProperty(el, "textContent", {
        get() { return this.shown; },
        set(v) { this.shown = String(v); this.texts.push(this.shown); },
      });
      // v0.51.342: the server-rendered state of the element, before the binder runs.
      const ssr = (scenario.ssr || {})[id];
      if (ssr) {
        if ("display" in ssr) el.style.display = ssr.display;
        if ("disabled" in ssr) el.disabled = ssr.disabled;
        if ("text" in ssr) el.shown = ssr.text;
        Object.assign(el.dataset, ssr.dataset || {});
      }
      els.set(id, el);
    }
    return els.get(id);
  },
};
const queue = scenario.responses.slice();
const unexpected = [];
// v0.51.342: a held response answers only at its "release:<key>" step — a request still in flight.
const holds = new Map();
async function api(method, url) {
  if (!queue.length) {
    unexpected.push(`${method} ${url}`);
    throw new Error(`unexpected api call ${method} ${url}`);
  }
  const next = queue.shift();
  if (next && next.__hold) {
    return new Promise((resolve) => holds.set(next.__hold.key, () => resolve(next.__hold.value)));
  }
  if (next && next.__throw) {
    const err = new Error(`${next.__throw.status}: ${next.__throw.detail || "error"}`);
    err.status = next.__throw.status;
    throw err;
  }
  return next;
}
// v0.51.342: timers wait for a "tick" step — the restore poll re-arms itself.
const timers = new Map();
let timerSeq = 0;
const ctx = vm.createContext({
  document, api, console, URLSearchParams,
  htmlEscape: (s) => String(s), fmtBytes: (n) => String(n), _autoDismissOpStatus: () => {},
  setTimeout: (fn) => { timerSeq += 1; timers.set(timerSeq, fn); return timerSeq; },
  clearTimeout: (id) => { timers.delete(id); },
});
vm.runInContext(fs.readFileSync(srcPath, "utf8"), ctx);
const flush = () => new Promise((r) => setImmediate(r));
// v0.51.342: __timers = the armed timers, so a second poll chain shows as two.
const snap = () => Object.assign(Object.fromEntries([...els].map(([id, e]) =>
  [id, { display: e.style.display, text: e.textContent, html: e.innerHTML, disabled: e.disabled,
         className: e.className, texts: e.texts.slice(), title: e.title || "" }])), { __timers: timers.size });
(async () => {
  const snaps = [];
  ctx.bindCanonicalHealth();
  await flush();
  snaps.push(snap());
  for (const step of scenario.clicks) {
    if (step === "tick") {
      for (const [id, fn] of [...timers]) { timers.delete(id); await fn(); }
    } else if (step.startsWith("release:")) {
      holds.get(step.slice("release:".length))();
      await flush();
    } else {
      await document.getElementById(step).listeners.click();
    }
    await flush();
    snaps.push(snap());
  }
  process.stdout.write(JSON.stringify({ snaps, left: queue.length, unexpected }));
})().catch((e) => { console.error(e); process.exit(1); });
"""


def _app_fn(name: str) -> str:
    """One module-level app.js helper the binder calls, whole (to its closing brace)."""
    start = APP_JS.index(f"\n  function {name}(") + 1
    return APP_JS[start:APP_JS.index("\n  }\n", start)] + "\n  }\n"


def _run_page(tmp_path, responses, clicks, ssr=None):
    start = APP_JS.index("  function bindCanonicalHealth() {")
    tmp_path.mkdir(parents=True, exist_ok=True)
    # v0.51.342: the page's own fmtRelativePast + gatewayTimeoutNote ride along — the restore poll words with them.
    (tmp_path / "bind.js").write_text(_app_fn("fmtRelativePast") + _app_fn("proxyStatusHint") + _app_fn("gatewayTimeoutNote")
                                      + APP_JS[start:APP_JS.index("\n  function ", start + 1)])
    (tmp_path / "scenario.json").write_text(json.dumps({"responses": responses, "clicks": clicks, "ssr": ssr or {}}))
    (tmp_path / "driver.js").write_text(_DRIVER)
    r = subprocess.run([_NODE, str(tmp_path / "driver.js"), str(tmp_path / "bind.js"),
                        str(tmp_path / "scenario.json")],
                       capture_output=True, text=True, timeout=60)
    assert r.returncode == 0, r.stderr[-2000:]
    out = json.loads(r.stdout)
    assert out["unexpected"] == [], f"the page made api calls the scenario did not expect: {out['unexpected']}"
    assert out["left"] == 0, "the page made fewer api calls than the scenario expects"
    return out["snaps"]


def _row(tmdb, title, plex_copy=None):
    return {"media_type": "movie", "tmdb_id": tmdb, "section_id": "1", "edition_key": "",
            "title": title, "year": None, "source_kind": "upload", "file_path": "x",
            "is_anime": False, "plex_copy": plex_copy, "has_live_placement": False}


def _report(missing=(), redownloadable=(), restorable=0):
    broken = len(missing) + len(redownloadable)
    return {"broken": broken, "redownloadable": list(redownloadable),
            "canonical_missing": list(missing), "changed": [],
            "counts": {"broken": broken, "redownloadable": len(redownloadable),
                       "canonical_missing": len(missing), "restorable_from_plex": restorable,
                       "changed": 0}}


@pytest.mark.skipif(not _NODE, reason="node not installed")
def test_the_missing_block_repaints_instead_of_keeping_the_last_rows(tmp_path):
    first = _report(missing=[_row(801, "Stale Manual Title", "sidecar")], restorable=1)
    # that row got fixed; a re-downloadable row still has a Plex copy, so the bulk still shows
    second = _report(redownloadable=[_row(802, "Fresh Redownload", "sidecar")], restorable=1)
    # v0.51.342: the page also asks the restore job's status on load.
    s1, s2, s3 = _run_page(tmp_path, [first, {"status": "idle"}, second, _report()],
                           ["canon-check-btn", "canon-check-btn"])
    assert "Stale Manual Title" in s1["canon-missing-tbody"]["html"]
    assert s1["canon-missing-block"]["display"] == ""
    assert "Stale Manual Title" not in s2["canon-missing-tbody"]["html"], "the previous render's row survived"
    assert s2["canon-missing-count"]["text"].startswith("0 "), s2["canon-missing-count"]["text"]
    assert s2["canon-missing-block"]["display"] == "" and s2["canon-restore-plex-btn"]["display"] == ""
    assert s3["canon-missing-block"]["display"] == "none"
    assert s3["canon-missing-tbody"]["html"] == "" and s3["canon-restore-plex-btn"]["display"] == "none"


# v0.51.341: every reason the restore paths emit, with its words — the old list missed four, so a deleted entry stayed green.
_SKIP_WORDING = {
    "canonical_already_present": "already on disk",
    "no_placement": "no Plex placement",
    "placement_file_missing": "Plex folder copy gone",
    "link_failed:": "copy failed",
    "no_rating_key": "no Plex rating key",
    "plex_unavailable": "Plex not configured",
    "plex_themes:": "Plex fetch failed",
    "no_theme_entry": "no theme selected in Plex",
    "plex_fetch:": "Plex fetch failed",
    "write_failed:": "copy failed",
    "no_plex_copy": "no Plex copy",
    # v0.51.342: the run stopped asking a Plex that gave no answer.
    "plex_unreachable": "Plex gave no answer — not tried",
    # v0.51.342: a row whose download is in flight is left to it.
    "download_in_flight": "download still in flight — not touched",
}


def _emitted_skip_reasons() -> set[str]:
    """The reason literals (an f-string's literal prefix) the restore functions and their store helpers can return."""
    import ast
    tree = ast.parse((REPO / "app" / "core" / "canonical_health.py").read_text())
    fns = {n.name: n for n in tree.body if isinstance(n, ast.FunctionDef)}
    out: set[str] = set()
    # v0.51.342: the store path's reasons moved into the three helpers the bulk drives directly.
    for name in ("restore_from_placement", "refetch_from_plex_store", "restore_from_plex",
                 "_store_guard", "_fetch_from_plex_store", "_publish_store_bytes"):
        for node in ast.walk(fns[name]):
            if isinstance(node, ast.Dict):
                for k, v in zip(node.keys, node.values):
                    if isinstance(k, ast.Constant) and k.value == "reason":
                        if isinstance(v, ast.Constant):
                            out.add(v.value)
                        elif isinstance(v, ast.JoinedStr):
                            out.add(v.values[0].value)
            elif isinstance(node, ast.BoolOp) and isinstance(node.op, ast.Or):
                out.update(v.value for v in node.values
                           if isinstance(v, ast.Constant) and isinstance(v.value, str))
    return out


@pytest.mark.skipif(not _NODE, reason="node not installed")
def test_the_restore_status_words_each_skip_reason(tmp_path):
    assert _emitted_skip_reasons() == set(_SKIP_WORDING), "a restore reason has no wording here (and in SKIP_WORDS)"
    samples = {"link_failed:": "link_failed:[Errno 1] Operation not permitted", "plex_themes:": "plex_themes:None",
               "plex_fetch:": "plex_fetch:500", "write_failed:": "write_failed:[Errno 28] No space left"}
    # a distinct count per reason, so a reason worded as another (or as the fallback) changes a group's total
    skipped, expected = [], {}
    for n, (reason, words) in enumerate(_SKIP_WORDING.items(), start=1):
        skipped += [{"title": f"T{reason}{i}", "media_type": "movie", "tmdb_id": i, "section_id": "1",
                     "reason": samples.get(reason, reason)} for i in range(n)]
        expected[words] = expected.get(words, 0) + n
    restore = {"ok": True, "broken": len(skipped), "restored": 0, "restored_sidecar": 0,
               "restored_store": 0, "skipped": skipped}
    page = _report(missing=[_row(901, "Anything", "sidecar")], restorable=1)
    # v0.51.342: idle on load → the start → the finished run's status → the reloaded report.
    snaps = _run_page(tmp_path, [page, {"status": "idle"}, {"ok": True, "started": True},
                                 {"status": "done", **restore}, _report()], ["canon-restore-plex-btn"])
    text = snaps[1]["canon-restore-plex-status"]["text"]
    assert "had no Plex copy" not in text, text
    head = f" · {len(skipped)} skipped ("
    assert head in text and text.endswith(")"), text
    groups = text[text.index(head) + len(head):-1].split(", ")
    shown = {g.split(" ", 1)[1]: int(g.split(" ", 1)[0]) for g in groups}
    assert shown == expected, (shown, text)


# ── 5: an already-present skip leaves the broken list ────────────────

def test_an_already_present_skip_stamps_present_and_changed_still_reports_the_size(tmp_path):
    db, themes, plexdir = _db(tmp_path)
    with sqlite3.connect(db) as conn:
        _section(conn)
        for tmdb in (501, 502):
            _theme(conn, tmdb)
            _lf(conn, tmdb, canonical_present=0, file_size=5)
        _placement(conn, 501, _folder(plexdir, "501"))
        _placement(conn, 502, "", kind="plex_upload", rk="9502")
        conn.commit()
    for tmdb in (501, 502):
        _canonical(themes, tmdb).parent.mkdir(parents=True)
        _canonical(themes, tmdb).write_bytes(b"downloaded-meanwhile")
    plex = FakePlex()
    res = ch.restore_from_plex(db, themes, plex)
    assert {s["tmdb_id"]: s["reason"] for s in res["skipped"]} == {
        501: "canonical_already_present", 502: "canonical_already_present"}
    assert plex.calls == []
    assert [_lf_cols(db, t, ("canonical_present", "file_size")) for t in (501, 502)] == [(1, 5), (1, 5)], \
        "present is stamped; the recorded size is left for CHANGED"
    with get_conn(db) as conn:
        rep = ch.broken_canonical_report(conn, themes, plex_available=True)
    assert rep["counts"]["broken"] == 0, "the rows leave the broken list now, not at the daily verify"
    assert sorted(c["tmdb_id"] for c in rep["changed"]) == [501, 502]


# ── 6: the INFO card's per-item restore ──────────────────────────────

@pytest.mark.parametrize("stub", [True, False], ids=["zero-byte-stub", "missing"])
def test_info_card_restore_heals_the_row_the_way_the_bulk_does(admin_client, stub):
    client, settings, tmp_path = admin_client
    sidecar = b"sidecar-bytes-601"
    with sqlite3.connect(settings.db_path) as conn:
        _section(conn)
        _theme(conn, 601)
        _lf(conn, 601, canonical_present=0, file_size=3, file_sha256="0" * 64, extra=_LEVELLED)
        _placement(conn, 601, _folder(tmp_path / "plex", "601", sidecar), kind="copy", theme_present=1)
        conn.commit()
    canonical = _canonical(tmp_path / "themes", 601)
    if stub:
        canonical.parent.mkdir(parents=True)
        canonical.write_bytes(b"")
    r = client.post("/api/items/movie/601/restore-canonical", headers=AUTH)
    assert r.status_code == 200, r.text
    assert r.json() == {"ok": True, "restored": 1, "skipped": []}, "a 0-byte stub is broken, not present"
    assert canonical.read_bytes() == sidecar
    assert _lf_cols(settings.db_path, 601, ("canonical_present", "file_size", "file_sha256")) == (
        1, len(sidecar), hashlib.sha256(sidecar).hexdigest())
    assert dict(zip(_NORM_COLS, _lf_cols(settings.db_path, 601, _NORM_COLS))) == dict.fromkeys(_NORM_COLS), \
        "new bytes void the loudness/norm anchors"
    rep = client.get("/api/admin/canonical-health/report", headers=AUTH).json()
    assert rep["counts"]["broken"] == 0, "CANONICAL HEALTH must not list a row the INFO card just restored"


def test_info_card_restore_keeps_its_skip_shape_and_404(admin_client):
    client, settings, tmp_path = admin_client
    with sqlite3.connect(settings.db_path) as conn:
        _section(conn)
        _theme(conn, 602)
        _lf(conn, 602)
        conn.commit()
    r = client.post("/api/items/movie/602/restore-canonical", headers=AUTH)
    assert r.status_code == 200, r.text
    assert r.json() == {"ok": True, "restored": 0, "skipped": [{"section_id": "1", "reason": "no_placement"}]}
    assert client.post("/api/items/movie/999999/restore-canonical", headers=AUTH).status_code == 404


# ── 7: the other canonical writers stamp canonical_present ───────────

@pytest.mark.parametrize("prior, sidecar_bytes, expected", [
    (0, b"ID3-adopted-bytes", 1),
    (None, b"ID3-adopted-bytes", 1),
    (1, b"", 0),
], ids=["stale-zero", "fresh-insert", "empty-sidecar-is-broken"])
def test_adopt_stamps_canonical_present_for_the_canonical_it_linked(
        tmp_path, monkeypatch, prior, sidecar_bytes, expected):
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.adopt import _do_adopt
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    (tmp_path / "themes").mkdir()
    s._cfg.paths.themes_dir = str(tmp_path / "themes")
    init_db(s.db_path)
    media_folder = tmp_path / "Movies" / "Adopted (2001)"
    media_folder.mkdir(parents=True)
    src = media_folder / "theme.mp3"
    src.write_bytes(sidecar_bytes)
    with sqlite3.connect(s.db_path) as conn:
        _section(conn)
        _theme(conn, 770)
        if prior is not None:
            _lf(conn, 770, canonical_present=prior, file_size=9, file_sha256="ab" * 32)
        conn.commit()
    finding = {"section_id": "1", "section_type": "movie", "finding_kind": "content_mismatch",
               "theme_id": 770, "file_path": str(src),
               "file_sha256": hashlib.sha256(sidecar_bytes).hexdigest(),
               "file_size": len(sidecar_bytes), "media_folder": str(media_folder)}
    _do_adopt(s.db_path, finding, s, decided_by="t")
    assert _lf_cols(s.db_path, 770, ("canonical_present",)) == (expected,)


@pytest.mark.parametrize("prior", [0, None], ids=["stale-zero", "fresh-insert"])
def test_cloud_backup_stamps_canonical_present_for_the_canonical_it_wrote(tmp_path, monkeypatch, prior):
    from app.core import revisions
    from app.core.cloud_theme_backup import backup_cloud_theme
    monkeypatch.setattr(revisions, "capture_revision", lambda *a, **k: None)
    db, themes, _plexdir = _db(tmp_path)
    themes.mkdir()
    body = b"ID3" + b"cloud-bytes" * 16
    plex = MagicMock()
    plex._rk_path.return_value = "/library/metadata/rk-800/file"
    plex._headers = {}
    resp = MagicMock()
    resp.status_code, resp.content, resp.text, resp.headers = 200, body, "", {}
    plex._client.get.return_value = resp
    target = {"rating_key": "rk-800", "guid_tmdb": 800, "media_type": "movie", "section_id": "1",
              "title": "Cloud", "year": "2009", "edition_key": "",
              "entry_uri": "metadata://themes/" + "b" * 40, "sha1": "b" * 40}
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    try:
        _section(conn)
        _theme(conn, 800)
        if prior is not None:
            _lf(conn, 800, canonical_present=prior, file_size=4, file_sha256="0" * 64)
        conn.commit()
        result = backup_cloud_theme(conn, target, themes, plex)
    finally:
        conn.close()
    assert result["ok"] is True, result
    assert _lf_cols(db, 800, ("canonical_present", "file_size")) == (1, len(body))
