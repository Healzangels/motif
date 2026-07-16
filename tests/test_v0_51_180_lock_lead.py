"""v0.51.180 — chase the lock lead, and delete the two paths measured dead.

The lead. Nothing in motif EVER unlocks Plex's theme field: set_theme_field_lock has no
production caller (v0.51.178 added it for a probe). But delete_collection_theme locks the
field — Plex's own docs say so, and rk 261711 confirmed it — so every delete_theme call
leaves that field locked forever. Two shipped paths delete and then rely on someone ELSE
writing the theme afterwards:

  - LET PLEX SERVE — deletes motif's theme so Plex's agent supplies one.
  - _teardown_plex_api_artifacts_for_placements (PURGE / DELETE / SWITCH api→file) —
    deletes motif's uploaded entry; the SWITCH case expects Local Media Assets to ingest
    the new sidecar.

A locked field is exactly what stops an agent writing. The push path never noticed because
a POST overrides the lock (v1.18.33) — these have no POST.

This is UNVERIFIED and the probe must not pretend otherwise. The smoking gun is a row that
is theme_locked AND has no selected entry: an item Plex cannot fill on its own. The
operator's control sample (6/6 untouched sidecar rows, all unlocked, all serving) cannot
answer it — those are rows motif never deleted from.

Also removed here, after being MEASURED dead:
  - // DELETE + RE-DETECT — the DELETE leaves the entry and only clears the selection, so
    Plex never "lacks" the theme; it stranded rk 261711 and bought nothing.
  - // MAKE PLEX RE-READ IT — refresh?force=1 does not re-read a changed sidecar
    (v0.51.173), and v0.51.179 proved the field was unlocked when it ran, so the lock was
    never the excuse.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest
from starlette.testclient import TestClient

from app.core.db import init_db

NOW = datetime.now(timezone.utc).isoformat(timespec="seconds")
AUTH = {"X-Authentik-Username": "testadmin"}
REPO = Path(__file__).resolve().parent.parent
API = (REPO / "app" / "web" / "api.py").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
HTML = (REPO / "app" / "web" / "templates" / "settings.html").read_text()


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.web.api import create_app
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    monkeypatch.setattr(Settings, "themes_dir", property(lambda self: tmp_path / "themes"))
    monkeypatch.setattr(Settings, "plex_url", property(lambda self: "http://plex.test"))
    monkeypatch.setattr(Settings, "plex_token", property(lambda self: "tok"))
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    with sqlite3.connect(s.db_path) as c:
        c.execute("INSERT INTO plex_sections (section_id, title, type, is_anime,"
                  " is_4k, themes_subdir, included, discovered_at, last_seen_at) "
                  "VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        c.commit()
    return TestClient(create_app(s)), s.db_path


def _sidecar(db, *, tmdb_id, rating_key):
    with sqlite3.connect(db) as c:
        c.execute("PRAGMA foreign_keys = OFF")
        c.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, edition_key, "
                  " file_path, file_sha256, downloaded_at, source_video_id, loudness_i, "
                  " loudness_tp, file_size) VALUES ('movie', ?, '1', '', ?, ?, ?, 'vid',"
                  " -5.0, -2.0, 900000)",
                  (tmdb_id, f"movies/{tmdb_id}/theme.mp3", f"sha{tmdb_id}", NOW))
        c.execute("INSERT INTO placements (media_type, tmdb_id, section_id, media_folder, "
                  " edition_key, placement_kind, placed_at) "
                  "VALUES ('movie', ?, '1', ?, '', 'hardlink', ?)",
                  (tmdb_id, f"/data/movies/{tmdb_id}", NOW))
        c.execute("INSERT INTO plex_items (rating_key, media_type, section_id, title, "
                  " guid_tmdb, edition_key, has_theme, first_seen_at, last_seen_at) "
                  "VALUES (?, 'movie', '1', ?, ?, '', 1, ?, ?)",
                  (rating_key, f"Movie{tmdb_id}", tmdb_id, NOW, NOW))
        c.commit()


def _motif_upload(db, *, tmdb_id, rating_key):
    """A row motif pushed via the API — placement_kind='plex_upload'."""
    with sqlite3.connect(db) as c:
        c.execute("PRAGMA foreign_keys = OFF")
        # media_folder='' is the plex_upload sentinel (CLAUDE.md § file-path conventions)
        c.execute("INSERT INTO placements (media_type, tmdb_id, section_id, media_folder, "
                  " edition_key, placement_kind, placed_at) "
                  "VALUES ('movie', ?, '1', '', '', 'plex_upload', ?)", (tmdb_id, NOW))
        c.execute("INSERT INTO plex_items (rating_key, media_type, section_id, title, "
                  " guid_tmdb, edition_key, has_theme, first_seen_at, last_seen_at) "
                  "VALUES (?, 'movie', '1', ?, ?, '', 1, ?, ?)",
                  (rating_key, f"Upload{tmdb_id}", tmdb_id, NOW, NOW))
        c.commit()


def _plex_served(db, *, tmdb_id, rating_key, has_theme=1):
    """Plex supplies the theme; motif placed nothing — the LET PLEX SERVE'd cohort."""
    with sqlite3.connect(db) as c:
        c.execute("PRAGMA foreign_keys = OFF")
        c.execute("INSERT INTO plex_items (rating_key, media_type, section_id, title, "
                  " guid_tmdb, edition_key, has_theme, first_seen_at, last_seen_at) "
                  "VALUES (?, 'movie', '1', ?, ?, '', ?, ?, ?)",
                  (rating_key, f"Served{tmdb_id}", tmdb_id, has_theme, NOW, NOW))
        c.commit()


def _themeless_placed(db, *, tmdb_id, rating_key):
    """motif placed a theme; Plex reports none — rk 3487's shape, and the only cohort
    that can answer the lead."""
    with sqlite3.connect(db) as c:
        c.execute("PRAGMA foreign_keys = OFF")
        c.execute("INSERT INTO placements (media_type, tmdb_id, section_id, media_folder, "
                  " edition_key, placement_kind, placed_at) "
                  "VALUES ('movie', ?, '1', ?, '', 'hardlink', ?)",
                  (tmdb_id, f"/data/movies/{tmdb_id}", NOW))
        c.execute("INSERT INTO plex_items (rating_key, media_type, section_id, title, "
                  " guid_tmdb, edition_key, has_theme, first_seen_at, last_seen_at) "
                  "VALUES (?, 'movie', '1', ?, ?, '', 0, ?, ?)",
                  (rating_key, f"Themeless{tmdb_id}", tmdb_id, NOW, NOW))
        c.commit()


def _stub(monkeypatch, *, locks, entries=None):
    """entries: rk -> selected entry uri, or None meaning Plex has NO theme selected."""
    from app.web import api as api_mod
    entries = entries or {}

    class _FakePlex:
        def __init__(self, *a, **k):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def get_field_locks(self, *, rating_key):
            v = locks.get(str(rating_key), False)
            if v is None:
                return {"ok": False, "http_status": None, "error": "transport: boom",
                        "locked_fields": None, "theme_locked": None}
            return {"ok": True, "http_status": 200, "error": None,
                    "locked_fields": (["theme"] if v else []), "theme_locked": v}

        def set_theme_field_lock(self, *, rating_key, locked, shape="metadata",
                                 section_id=None, plex_type=None):
            if shape == "metadata":
                locks[str(rating_key)] = locked
            return 200

        def get_themes(self, *, rating_key):
            uri = entries.get(str(rating_key), "metadata://themes/aaa")
            if uri is None:                       # Plex has nothing selected
                return {"ok": True, "http_status": 200, "error": None, "body": {
                    "MediaContainer": {"Metadata": [
                        {"ratingKey": "metadata://themes/old", "selected": False}]}}}
            return {"ok": True, "http_status": 200, "error": None, "body": {
                "MediaContainer": {"Metadata": [{"ratingKey": uri, "selected": True}]}}}

    monkeypatch.setattr(api_mod, "PlexClient", _FakePlex)


# ── the smoking gun ──────────────────────────────────────────────────────

def test_locked_row_with_no_theme_is_called_out(client, monkeypatch):
    """A LOCKED row with NOTHING selected is the shape the lead predicts, so it must be
    surfaced. v0.51.181: surfaced as a CANDIDATE — see the next test for why the verdict
    must not call it a confirmed cause."""
    c, db = client
    _sidecar(db, tmdb_id=1, rating_key="1")
    _plex_served(db, tmdb_id=50, rating_key="50")          # LPS'd-shaped row
    _stub(monkeypatch, locks={"1": False, "50": True}, entries={"50": None})

    b = c.post("/api/admin/loudness/theme-lock-probe", headers=AUTH).json()
    gun = b["lock_lead"]["locked_with_no_theme"]
    assert [g["rating_key"] for g in gun] == ["50"]
    assert "LOCKED with NO theme selected" in b["lock_lead"]["verdict"]


def test_the_verdict_does_not_assert_causation(client, monkeypatch):
    """v0.51.181. v0.51.180's string said the agent "cannot write a locked field, SO these
    items cannot get a theme back" — a conclusion drawn from one sampled row, printed as a
    finding. The operator's single hit (rk 3487) was consistent with two innocent
    explanations (a broken canonical with nothing to push; the known stale-plex_upload/RP
    class). Reporting an inference as a measurement is the habit this whole arc has been
    correcting, so it gets an executable guard."""
    c, db = client
    _sidecar(db, tmdb_id=1, rating_key="1")
    _plex_served(db, tmdb_id=50, rating_key="50")
    _stub(monkeypatch, locks={"1": False, "50": True}, entries={"50": None})

    v = c.post("/api/admin/loudness/theme-lock-probe",
               headers=AUTH).json()["lock_lead"]["verdict"]
    assert "cannot get a theme back on their own" not in v
    assert "CAUSE is not established" in v
    assert "// TEST UNLOCK ON A ROW" in v      # names what would settle it


def test_candidates_carry_enough_to_triage_without_a_round_trip(client, monkeypatch):
    """rk 3487 needed three separate lookups to interpret and the report carried none of
    them. motif_placement is the load-bearing one: it separates "Plex lost our theme"
    from "motif never had one to give"."""
    c, db = client
    _sidecar(db, tmdb_id=1, rating_key="1")
    _plex_served(db, tmdb_id=50, rating_key="50")
    _stub(monkeypatch, locks={"1": False, "50": True}, entries={"50": None})

    g = c.post("/api/admin/loudness/theme-lock-probe",
               headers=AUTH).json()["lock_lead"]["locked_with_no_theme"][0]
    for k in ("rating_key", "title", "tmdb_id", "media_type", "locked_fields",
              "motif_placement"):
        assert k in g, f"{k} missing — a candidate must be triageable from the report"


def test_library_wide_count_sizes_the_lead(client, monkeypatch):
    """v0.51.176 sized the ceiling before designing around it; same move. A row can only
    be locked-with-no-theme if Plex has no theme, which motif already tracks locally — so
    the CEILING on the blast radius is a local count, no Plex round trip per row."""
    c, db = client
    _sidecar(db, tmdb_id=1, rating_key="1")
    _themeless_placed(db, tmdb_id=70, rating_key="70")     # motif placed, Plex has none
    _plex_served(db, tmdb_id=80, rating_key="80", has_theme=0)   # motif placed nothing
    _stub(monkeypatch, locks={"1": False, "70": True})

    lib = c.post("/api/admin/loudness/theme-lock-probe",
                 headers=AUTH).json()["lock_lead"]["library"]
    assert lib["plex_items_with_no_theme"] == 2
    # only the motif-placed one is a candidate; where motif placed nothing, no theme is
    # expected and its absence means nothing.
    assert lib["motif_placed_but_plex_themeless"] == 1
    assert "known_stale_plex_uploads" in lib


def test_themeless_cohort_is_sampled(client, monkeypatch):
    """The cohort that can actually answer the lead. v0.51.180 hit rk 3487 by luck of the
    ordering; this targets it."""
    c, db = client
    _sidecar(db, tmdb_id=1, rating_key="1")
    _themeless_placed(db, tmdb_id=70, rating_key="70")
    _stub(monkeypatch, locks={"1": False, "70": True})

    b = c.post("/api/admin/loudness/theme-lock-probe", headers=AUTH).json()
    assert [r["rating_key"] for r in b["plex_themeless_rows"]] == ["70"]
    assert b["plex_themeless_rows"][0]["motif_placement"] == "hardlink"


def test_locked_but_serving_is_not_called_a_problem(client, monkeypatch):
    """A locked field does not stop Plex SERVING an already-selected theme (rk 261711 is
    locked and serving). Only locked-AND-themeless is evidence of harm."""
    c, db = client
    _sidecar(db, tmdb_id=1, rating_key="1")
    _motif_upload(db, tmdb_id=60, rating_key="60")
    _stub(monkeypatch, locks={"1": False, "60": True},
          entries={"60": "upload://themes/ccc"})

    b = c.post("/api/admin/loudness/theme-lock-probe", headers=AUTH).json()
    assert b["lock_lead"]["locked_with_no_theme"] == []
    assert b["lock_lead"]["locked_and_serving"] == 1
    assert "no evidence the lock strands anything" in b["lock_lead"]["verdict"]


def test_no_locked_rows_is_inconclusive_not_a_pass(client, monkeypatch):
    """If the sample contains no locked rows at all, that says nothing about the lead —
    the cohorts may simply not include a row motif deleted from. Calling that "fine" is
    exactly the gap-reads-as-an-answer shape (class-9)."""
    c, db = client
    _sidecar(db, tmdb_id=1, rating_key="1")
    _stub(monkeypatch, locks={"1": False})

    b = c.post("/api/admin/loudness/theme-lock-probe", headers=AUTH).json()
    v = b["lock_lead"]["verdict"]
    assert "INCONCLUSIVE" in v
    assert "not evidence the lock is harmless" in v


def test_unreadable_sample_yields_no_lead_verdict(client, monkeypatch):
    c, db = client
    _sidecar(db, tmdb_id=1, rating_key="1")
    _stub(monkeypatch, locks={"1": None})

    b = c.post("/api/admin/loudness/theme-lock-probe", headers=AUTH).json()
    assert "NO verdict" in b["lock_lead"]["verdict"]
    assert "Do not read this as" in b["lock_lead"]["verdict"]


# ── the cohorts ──────────────────────────────────────────────────────────

def test_samples_motif_uploaded_and_plex_served_cohorts(client, monkeypatch):
    """The control (untouched sidecar rows) cannot answer the lead — motif never deleted
    from those. These two cohorts are where a locked field would actually live."""
    c, db = client
    _sidecar(db, tmdb_id=1, rating_key="1")
    _motif_upload(db, tmdb_id=60, rating_key="60")
    _plex_served(db, tmdb_id=50, rating_key="50")
    _stub(monkeypatch, locks={"1": False, "60": True, "50": False})

    b = c.post("/api/admin/loudness/theme-lock-probe", headers=AUTH).json()
    assert [r["rating_key"] for r in b["motif_uploaded_rows"]] == ["60"]
    assert [r["rating_key"] for r in b["plex_served_rows"]] == ["50"]
    # the control stays the untouched sidecar row and is unaffected by the cohorts
    assert [r["rating_key"] for r in b["control"]["rows"]] == ["1"]


def test_plex_served_cohort_excludes_rows_motif_placed(client, monkeypatch):
    """A row motif hardlink-placed is NOT Plex-served — counting it would put an untouched
    row in the deleted-from cohort and muddy the answer."""
    c, db = client
    _sidecar(db, tmdb_id=1, rating_key="1")
    _stub(monkeypatch, locks={"1": False})

    b = c.post("/api/admin/loudness/theme-lock-probe", headers=AUTH).json()
    assert b["plex_served_rows"] == []


# ── the two dead paths are GONE, not deprecated ──────────────────────────

def test_delete_redetect_is_removed_entirely(client):
    """Measured dead AND it strands items (rk 261711 ended with no theme). Leaving a
    known-dead destructive button in Settings is a foot-gun; CLAUDE.md forbids
    backwards-compat paths for removed features."""
    c, _ = client
    assert '/api/admin/loudness/plex-redetect' not in API
    assert "loud-plex-redetect-btn" not in APP_JS
    assert "loud-plex-redetect-btn" not in HTML
    assert "// DELETE + RE-DETECT" not in HTML
    assert c.post("/api/admin/loudness/plex-redetect", headers=AUTH).status_code == 404


def test_plex_reread_is_removed_entirely(client):
    c, _ = client
    assert '/api/admin/loudness/plex-reread' not in API
    assert "loud-plex-reread-btn" not in APP_JS
    assert "// MAKE PLEX RE-READ IT" not in HTML
    assert c.post("/api/admin/loudness/plex-reread", headers=AUTH).status_code == 404


def test_the_surviving_probes_still_exist(client):
    """PUSH is the propagation mechanism and WHAT IS PLEX SERVING? verifies it — the
    removal must not take out the paths that actually work."""
    assert '/api/admin/loudness/plex-push' in API
    assert '/api/admin/loudness/plex-serving' in API
    assert "// PUSH NORMALIZED TO PLEX" in HTML
    assert "// WHAT IS PLEX SERVING?" in HTML


def test_measurement_helper_is_still_shared_not_copy_pasted():
    """Carried over from the deleted v0.51.173 file — it guarded a live invariant that
    outlived the probe it was written for."""
    assert API.count("def _measure_plex_serving(") == 1
    assert API.count("_measure_plex_serving(settings") >= 2


# ── v0.51.181: the intervention that settles causation ───────────────────
# The probe finds CORRELATION (locked + themeless). Only an intervention separates "the
# lock blocks recovery" from "nothing was ever pushed here".

def _unlock_stub(monkeypatch, *, locked, entry_before, entry_after_unlock,
                 unlock_works=True):
    from app.web import api as api_mod
    st = {"locked": locked, "entry": entry_before, "unlocked_at": None, "refreshed": 0}

    class _FakePlex:
        def __init__(self, *a, **k):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def get_field_locks(self, *, rating_key):
            if st["locked"] is None:
                return {"ok": False, "http_status": None, "error": "boom",
                        "locked_fields": None, "theme_locked": None}
            return {"ok": True, "http_status": 200, "error": None,
                    "locked_fields": (["theme"] if st["locked"] else []),
                    "theme_locked": st["locked"]}

        def set_theme_field_lock(self, *, rating_key, locked, shape="metadata",
                                 section_id=None, plex_type=None):
            if unlock_works:
                st["locked"] = locked
            return 200            # a 200 regardless — only the re-read tells the truth

        def refresh(self, rating_key):
            st["refreshed"] += 1
            # Plex's agent only fills the field when it is OPEN.
            if not st["locked"]:
                st["entry"] = entry_after_unlock
            return True

        def get_themes(self, *, rating_key):
            if st["entry"] is None:
                return {"ok": True, "http_status": 200, "error": None,
                        "body": {"MediaContainer": {"Metadata": []}}}
            return {"ok": True, "http_status": 200, "error": None, "body": {
                "MediaContainer": {"Metadata": [
                    {"ratingKey": st["entry"], "selected": True}]}}}

    monkeypatch.setattr(api_mod, "PlexClient", _FakePlex)
    monkeypatch.setattr("time.sleep", lambda *_a: None)
    return st


def test_unlocking_that_yields_a_theme_confirms_the_lead(client, monkeypatch):
    c, _ = client
    _unlock_stub(monkeypatch, locked=True, entry_before=None,
                 entry_after_unlock="metadata://themes/new")

    b = c.post("/api/admin/plex/theme-unlock-experiment",
               headers=AUTH, json={"rating_key": "3487"}).json()
    assert b["gained_a_theme"] is True
    assert "UNLOCKING WORKED" in b["verdict"]
    assert "degraded on every row motif deleted from" in b["verdict"]


def test_unlocking_that_yields_nothing_does_not_confirm_the_lead(client, monkeypatch):
    """And must not be read as 'the lock is fine everywhere' — Plex's agent may simply
    have nothing to give for this title."""
    c, _ = client
    _unlock_stub(monkeypatch, locked=True, entry_before=None, entry_after_unlock=None)

    b = c.post("/api/admin/plex/theme-unlock-experiment",
               headers=AUTH, json={"rating_key": "3487"}).json()
    assert b["gained_a_theme"] is False
    assert "the lead is not supported here" in b["verdict"]
    assert "try another locked row before concluding" in b["verdict"]


def test_the_experiment_restores_the_lock(client, monkeypatch):
    """Measure, don't change things. A theme that arrived is not un-arrived by re-locking."""
    c, _ = client
    st = _unlock_stub(monkeypatch, locked=True, entry_before=None,
                      entry_after_unlock="metadata://themes/new")

    b = c.post("/api/admin/plex/theme-unlock-experiment",
               headers=AUTH, json={"rating_key": "3487"}).json()
    assert b["lock_restored"] is True
    assert st["locked"] is True


def test_an_unlock_that_does_not_move_the_flag_yields_no_verdict(client, monkeypatch):
    """If the flag never opened, the experiment never ran. Reporting the refresh result
    would be measuring nothing (v0.51.178's lesson, one layer up)."""
    c, _ = client
    _unlock_stub(monkeypatch, locked=True, entry_before=None,
                 entry_after_unlock="metadata://themes/new", unlock_works=False)

    b = c.post("/api/admin/plex/theme-unlock-experiment",
               headers=AUTH, json={"rating_key": "3487"}).json()
    assert b["ok"] is False
    assert "never ran" in b["error"]
    assert "gained_a_theme" not in b


def test_an_already_unlocked_row_is_the_wrong_subject(client, monkeypatch):
    c, _ = client
    _unlock_stub(monkeypatch, locked=False, entry_before=None, entry_after_unlock=None)

    b = c.post("/api/admin/plex/theme-unlock-experiment",
               headers=AUTH, json={"rating_key": "999"}).json()
    assert b["already_unlocked"] is True
    assert "cannot be what stops Plex" in b["verdict"]


def test_experiment_requires_a_named_row(client):
    """No library sweeps on a hunch."""
    c, _ = client
    b = c.post("/api/admin/plex/theme-unlock-experiment", headers=AUTH, json={}).json()
    assert b["ok"] is False
    assert "name a rating_key" in b["error"]


def test_experiment_never_deletes_or_uploads():
    i = API.index('@app.post("/api/admin/plex/theme-unlock-experiment")')
    body = API[i:API.index('@app.post("/api/admin/loudness/theme-lock-probe")', i)]
    for mutating in ("delete_theme(", "delete_collection_theme(", "upload_theme(",
                     "upload_collection_theme("):
        assert mutating not in body, f"the experiment must not call {mutating}"
    assert "def _run():" in body and "run_in_threadpool(_run)" in body   # class-12
