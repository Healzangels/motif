"""v0.51.290 — ultra-review fixes on the .289 INFO card rework.

The review's confirmed set:
  1. Fold open-state is view-state — the loudness handlers re-render the
     card and snapped user-opened folds shut. Captured beside _keepScroll
     (v0.51.244's site) and restored BEFORE the scroll (open folds change
     layout).
  2. The loudness open-rule missed UNMEASURED and CLIPPING rows (both ride
     marker 'raw'); the note said 'outlier' where every other surface says
     LOUD, and 'raw' for rows never measured.
  3. TIMELINE hid the accent-red last-failure line behind a 'dates' label.
  4. derivationRow leaked an arbitrary cut's codes on ambiguous cards
     (the v0.51.223 invariant); revisions listed BOTH cuts with live
     RESTORE buttons — client gates on ambiguity, endpoint scopes to the
     resolved cut.
  5. The .289 [open] title override TIED (0,3,0) with the later
     green-bright rule and LOST on source order — a mirror test kept it
     green. Class-stacked selectors win position-independently; the caret
     override was missing entirely; fold bodies gained the 480px bound.
"""
from __future__ import annotations

import hashlib
import sqlite3
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
NOW = "2026-08-26T00:00:00+00:00"
MT, TID = "movie", 290001


# ── 1. fold state survives the same-card re-render ───────────


def test_fold_state_captured_beside_keepscroll():
    i_scroll = APP_JS.index("const _keepScroll = _sameCard")
    i_folds = APP_JS.index("const _keepFolds = _sameCard")
    i_paint = APP_JS.index("body.innerHTML = recordLoaderHtml", i_scroll)
    assert i_scroll < i_folds < i_paint, (
        "capture must read the OLD dom before any repaint")
    cap = APP_JS[i_folds:i_paint]
    assert "details.info-fold[open]" in cap
    assert "dataset.infoSection" in cap


def test_fold_state_restored_before_scroll():
    i_restore = APP_JS.index("for (const _fk of _keepFolds)")
    i_scroll = APP_JS.index(
        "if (_keepScroll !== null && _scroller) _scroller.scrollTop")
    assert i_restore < i_scroll, (
        "open folds change layout — restore them before the scroll lands")
    blk = APP_JS[i_restore:i_scroll]
    assert "CSS.escape(_fk)" in blk
    assert "_fd.open = true" in blk


# ── 2 + 3. actionable open rules ─────────────────────────────


def _loudness_fold_call() -> str:
    i = APP_JS.index("_fold('loudness', _loudnessRows")
    return APP_JS[i:APP_JS.index("})}", i)]


def test_loudness_opens_for_unmeasured_and_clipping():
    call = _loudness_fold_call()
    assert "lf.loudness_i == null" in call, "unmeasured rows carry MEASURE NOW"
    assert "lf.loudness_tp === 'number' && lf.loudness_tp > 0" in call, (
        "clipping rows ride marker 'raw' below the LUFS threshold")


def test_loudness_note_speaks_the_card_vocabulary():
    call = _loudness_fold_call()
    assert "{ outlier: 'loud' }" in call, (
        "every other surface renders outlier as LOUD — one vocabulary")
    assert "'not measured'" in call
    assert "'no file'" not in call, "unreachable arm removed"


def test_timeline_opens_on_last_failure():
    i = APP_JS.index("_fold('timeline', _timelineRows")
    call = APP_JS[i:APP_JS.index("})}", i)]
    assert "open: !!failBlock" in call
    assert "'dates · last failure'" in call


# ── 4. ambiguity gates ───────────────────────────────────────


def test_derivation_row_gated_on_known_cut():
    i = APP_JS.index("const derivationRow =")
    blk = APP_JS[i:APP_JS.index(": '';", i)]
    assert "!_ambiguousCut &&" in blk, (
        "v0.51.223: never assert an arbitrary cut's data behind the picker")


def test_revisions_blank_on_ambiguous_card():
    i = APP_JS.index("const revs = data.revisions || [];")
    blk = APP_JS[i - 400:i]
    assert "if (_ambiguousCut) return '';" in blk


# ── 5. the cascade actually applies ──────────────────────────


def test_fold_open_overrides_out_specify_the_audit_rules():
    # class-stacked (0,4,0)/(0,3,0) beats the audit rules position-
    # independently; the .289 losing spelling must be GONE, not coexist.
    assert (".history-section.info-fold[open] .history-section-title "
            "{ color: var(--fg); }") in APP_CSS
    assert (".history-section.info-fold[open] > summary::before "
            "{ color: var(--fg-dim); }") in APP_CSS
    lose = ".info-fold[open] .history-section-title"
    assert f"\n{lose} {{" not in APP_CSS, (
        "the tied-specificity spelling loses to the later green-bright rule")


def test_fold_body_carries_the_bounding_half_of_the_idiom():
    i = APP_CSS.index(".info-fold .info-fold-body {")
    blk = APP_CSS[i:APP_CSS.index("}", i)]
    assert "max-height: 480px" in blk and "overflow-y: auto" in blk, (
        "revisions metadata outlives audio retention — the list must scroll "
        "like .history-body, not grow unbounded")


def test_fold_casing_lives_in_css_not_js():
    i = APP_CSS.index(".info-fold .history-section-title {")
    blk = APP_CSS[i:APP_CSS.index("}", i)]
    assert "text-transform: uppercase" in blk
    assert "title.toUpperCase()" not in APP_JS, (
        "one casing mechanism — CSS, same as _grp's h4")


# ── endpoint: revisions scoped to the resolved cut ───────────


@pytest.fixture
def client(tmp_path, monkeypatch):
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import get_conn, init_db, transaction
    from app.web.api import create_app
    from fastapi.testclient import TestClient
    themes = tmp_path / "data" / "themes"
    (themes / "movies" / "T (2020)").mkdir(parents=True)
    (themes / "movies" / "T (2020) {edition-extended}").mkdir(parents=True)
    (tmp_path / "motif.yaml").write_text(f"paths:\n  themes_dir: {themes}\n")
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    with get_conn(s.db_path) as conn, transaction(conn):
        conn.execute(
            """INSERT INTO plex_sections (section_id, title, type, is_anime,
                 is_4k, themes_subdir, included, discovered_at, last_seen_at)
               VALUES ('1', 'Movies', 'movie', 0, 0, 'movies', 1, ?, ?)""",
            (NOW, NOW))
        conn.execute(
            """INSERT INTO themes (media_type, tmdb_id, title, year,
                 upstream_source, last_seen_sync_at, first_seen_sync_at)
               VALUES (?, ?, 'T', '2020', 'themoviedb', ?, ?)""",
            (MT, TID, NOW, NOW))
        for edn, rel in (("", "movies/T (2020)/theme.mp3"),
                         ("extended",
                          "movies/T (2020) {edition-extended}/theme.mp3")):
            (s.themes_dir / rel).write_bytes(b"CUR" + edn.encode())
            conn.execute(
                """INSERT INTO local_files (media_type, tmdb_id, section_id,
                     edition_key, file_path, file_sha256, file_size,
                     downloaded_at, source_video_id, provenance, source_kind)
                   VALUES (?, ?, '1', ?, ?, ?, 7, ?, 'vid', 'auto',
                           'themerrdb')""",
                (MT, TID, edn, rel,
                 hashlib.sha256(rel.encode()).hexdigest(), NOW))
            conn.execute(
                """INSERT INTO theme_revisions (media_type, tmdb_id,
                     section_id, edition_key, created_at, source_kind,
                     source_video_id, content_sha256, file_size, reason,
                     actor, retained_path)
                   VALUES (?, ?, '1', ?, ?, 'themerrdb', ?, ?, 3,
                           'replaced_by_download', 'system', NULL)""",
                (MT, TID, edn, NOW, f"old-{edn or 'std'}",
                 hashlib.sha256(edn.encode()).hexdigest()))
    return TestClient(create_app(s)), s


AUTH = {"X-Authentik-Username": "testadmin"}


def test_named_cut_lists_only_its_own_revisions(client):
    c, _s = client
    r = c.get(f"/api/items/{MT}/{TID}?section_id=1&edition_key=extended",
              headers=AUTH)
    assert r.status_code == 200, r.text
    revs = r.json().get("revisions")
    assert [rv["source_video_id"] for rv in revs] == ["old-extended"], (
        "a card that names its cut must not list the sibling's revisions "
        "with a live RESTORE button")


def test_legacy_unscoped_call_still_lists_everything(client):
    c, _s = client
    r = c.get(f"/api/items/{MT}/{TID}", headers=AUTH)
    assert r.status_code == 200, r.text
    vids = sorted(rv["source_video_id"] for rv in r.json()["revisions"])
    assert vids == ["old-extended", "old-std"], (
        "no section named -> the pre-.290 global list is unchanged")


def test_v0_51_290_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert "0.51.290: " in init_py
