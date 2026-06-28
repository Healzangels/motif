"""v1.14.74 — Plex section-level delta gate via contentChangedAt.

the user: "can we go back to the plex scans speed now"

The Plex API perf research (saved as a research note in the
v1.14.71 conversation) ranked section-level delta gating via
`scannedAt`/`contentChangedAt` as the highest-leverage,
lowest-risk optimization for `plex_enum`. Stable libraries
(90%+ of refreshes — i.e., daily cron syncs where nothing
actually changed) drop from ~30s-2m per section to ~50ms.

## Fix

  1. Schema v46: `plex_sections.last_enum_content_changed_at`
     stores the contentChangedAt motif observed at the start of
     the most recent successful enum.
  2. `PlexSection` + `discover_sections` capture
     `contentChangedAt` from the `<Directory>` XML.
  3. `run_plex_enum`:
     a. Calls `discover_sections` once to read live values.
     b. Per section: skip the full enumerate_section_items +
        upsert + verify when live equals stored.
     c. Surfaces "(no changes)" in the op_progress stage_label
        + the final log_event so the user sees the gate fired.
     d. Stamps stored=live on success (race-safe: stores OLD
        live_value so a mid-enum bump triggers re-run next time).
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path
from unittest.mock import patch


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from app.core import db as core_db  # noqa: E402
from app.core import plex_enum  # noqa: E402
from app.core import sections as sections_mod  # noqa: E402
from app.core.plex import PlexConfig, PlexLibraryItem, PlexSection  # noqa: E402


# ── Schema v46 ────────────────────────────────────────────────


def test_schema_version_bumped_to_46():
    # Asserts the version is AT LEAST 46 — the column this test
    # exercises was added in v46. Subsequent versions widen the
    # schema further (v47: op_progress.kind CHECK widened) but
    # the column under test remains.
    assert core_db.CURRENT_SCHEMA_VERSION >= 46


def test_migration_v45_to_v46_adds_column(tmp_path):
    """A DB at schema v45 must successfully migrate to v46 by
    adding the last_enum_content_changed_at column."""
    db_file = tmp_path / "motif.db"
    # Hand-build a minimal v45 plex_sections + schema_version row,
    # then run the migration.
    conn = sqlite3.connect(db_file)
    conn.execute(
        "CREATE TABLE plex_sections ("
        "section_id TEXT PRIMARY KEY,"
        "uuid TEXT, title TEXT, type TEXT, agent TEXT,"
        "language TEXT, location_paths TEXT,"
        "included INTEGER NOT NULL DEFAULT 1,"
        "is_anime INTEGER NOT NULL DEFAULT 0,"
        "is_4k INTEGER NOT NULL DEFAULT 0,"
        "themes_subdir TEXT NOT NULL DEFAULT '',"
        "discovered_at TEXT, last_seen_at TEXT)"
    )
    core_db._migrate_v45_to_v46(conn)  # noqa: SLF001 — direct test
    cols = {row[1] for row in conn.execute(
        "PRAGMA table_info(plex_sections)").fetchall()}
    assert "last_enum_content_changed_at" in cols
    conn.close()


def test_fresh_install_schema_includes_new_column(tmp_path):
    """A clean motif install (no migration path — runs SCHEMA
    directly) must also have the new column."""
    db_file = tmp_path / "motif.db"
    core_db.init_db(db_file)
    conn = sqlite3.connect(db_file)
    cols = {row[1] for row in conn.execute(
        "PRAGMA table_info(plex_sections)").fetchall()}
    assert "last_enum_content_changed_at" in cols
    conn.close()


# ── PlexSection captures contentChangedAt ─────────────────────


def test_plex_section_dataclass_has_content_changed_at_field():
    """PlexSection's new field must default to '' so a Plex
    build that doesn't surface contentChangedAt doesn't break
    the dataclass instantiation."""
    section = PlexSection(
        section_id="1", uuid="u", title="t", type="movie",
        agent="a", language="en", location_paths=[],
    )
    assert section.content_changed_at == ""


def test_discover_sections_extracts_content_changed_at_attr():
    """v1.14.78: parser switched XML → JSON. Plex JSON serves
    contentChangedAt as an int (epoch seconds) on each Directory
    object; motif stringifies for storage stability with the v46
    TEXT column."""
    from app.core.plex import PlexClient

    class _Resp:
        status_code = 200
        text = (
            '{"MediaContainer": {"Directory": ['
            '{"key": "1", "type": "movie", "uuid": "u1", '
            ' "title": "Movies", "agent": "a", "language": "en", '
            ' "contentChangedAt": 1700000000, '
            ' "Location": [{"path": "/movies"}]},'
            '{"key": "2", "type": "show", "uuid": "u2", '
            ' "title": "TV", "agent": "a", "language": "en"}'
            ']}}'
        )

    cfg = PlexConfig(url="http://x", token="t",
                     movie_section="1", tv_section="2")
    with patch.object(PlexClient, "_get", return_value=_Resp()):
        client = PlexClient(cfg)
        sections = client.discover_sections()
    by_id = {s.section_id: s for s in sections}
    assert by_id["1"].content_changed_at == "1700000000"
    # Fallback when the attribute is absent.
    assert by_id["2"].content_changed_at == ""


# ── End-to-end gate behavior ──────────────────────────────────


def _seed_db(tmp_path: Path,
             stored_cca: str | None,
             included: bool = True) -> Path:
    db_file = tmp_path / "motif.db"
    core_db.init_db(db_file)
    with sqlite3.connect(db_file) as conn:
        conn.execute(
            "INSERT INTO plex_sections "
            "(section_id, uuid, title, type, agent, language, "
            " location_paths, included, themes_subdir, "
            " discovered_at, last_seen_at, "
            " last_enum_content_changed_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            ("1", "u1", "Movies", "movie", "a", "en", "[]",
             1 if included else 0, "movies",
             "2025-01-01T00:00:00Z", "2025-01-01T00:00:00Z",
             stored_cca),
        )
        conn.commit()
    return db_file


class _FakeClient:
    """Stub PlexClient with controllable contentChangedAt + a
    boolean tracking whether enumerate_section_items was hit."""

    def __init__(self, live_cca: str):
        self._live_cca = live_cca
        self.enumerate_called = False

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def discover_sections(self):
        return [PlexSection(
            section_id="1", uuid="u1", title="Movies",
            type="movie", agent="a", language="en",
            location_paths=[],
            content_changed_at=self._live_cca,
        )]

    def enumerate_section_items(self, **kwargs):
        self.enumerate_called = True
        return [PlexLibraryItem(
            rating_key="42", section_id="1", media_type="movie",
            title="Test", year="2020",
            guid_imdb=None, guid_tmdb=None, guid_tvdb=None,
            folder_path="/movies/Test (2020)",
            has_theme=False, plex_theme_uri="",
        )]

    def enumerate_collections_for_section(self, **kwargs):
        # v1.22.72: the real client always has this (v1.18.0); without
        # it the AttributeError reads as a FAILED collections fetch,
        # which now (correctly) blocks the CCA stamp under test.
        return []


def test_gate_skips_enum_when_live_equals_stored(tmp_path, monkeypatch):
    """When stored CCA matches live CCA, run_plex_enum must NOT
    call enumerate_section_items for that section. Stats track
    the skip via skipped_unchanged."""
    db_file = _seed_db(tmp_path, stored_cca="1700000000")
    fake = _FakeClient(live_cca="1700000000")
    monkeypatch.setattr(plex_enum, "PlexClient", lambda cfg: fake)
    cfg = PlexConfig(url="http://x", token="t",
                     movie_section="1", tv_section="2")
    stats = plex_enum.run_plex_enum(db_file, cfg)
    assert fake.enumerate_called is False
    assert stats["skipped_unchanged"] == 1
    assert stats["items_seen"] == 0


def test_gate_runs_enum_when_live_differs_from_stored(tmp_path, monkeypatch):
    """When stored CCA is older than live CCA (Plex bumped it
    after the last enum), the full enum must run."""
    db_file = _seed_db(tmp_path, stored_cca="1700000000")
    fake = _FakeClient(live_cca="1700000001")
    monkeypatch.setattr(plex_enum, "PlexClient", lambda cfg: fake)
    cfg = PlexConfig(url="http://x", token="t",
                     movie_section="1", tv_section="2")
    stats = plex_enum.run_plex_enum(db_file, cfg)
    assert fake.enumerate_called is True
    assert stats["skipped_unchanged"] == 0
    assert stats["items_seen"] == 1


def test_gate_runs_enum_on_first_run_when_baseline_is_null(
        tmp_path, monkeypatch):
    """No baseline yet (NULL stored CCA) → must run full enum.
    The skip is opt-in; without a baseline we have nothing to
    compare and falling through is the safe default."""
    db_file = _seed_db(tmp_path, stored_cca=None)
    fake = _FakeClient(live_cca="1700000000")
    monkeypatch.setattr(plex_enum, "PlexClient", lambda cfg: fake)
    cfg = PlexConfig(url="http://x", token="t",
                     movie_section="1", tv_section="2")
    stats = plex_enum.run_plex_enum(db_file, cfg)
    assert fake.enumerate_called is True
    assert stats["skipped_unchanged"] == 0


def test_gate_runs_enum_when_live_value_is_empty(tmp_path, monkeypatch):
    """If Plex doesn't surface contentChangedAt for a section
    (degraded build / missing attribute), the gate must NOT fire
    — even if the stored CCA is non-empty. Otherwise we'd skip
    forever once we lost the live signal."""
    db_file = _seed_db(tmp_path, stored_cca="1700000000")
    fake = _FakeClient(live_cca="")
    monkeypatch.setattr(plex_enum, "PlexClient", lambda cfg: fake)
    cfg = PlexConfig(url="http://x", token="t",
                     movie_section="1", tv_section="2")
    stats = plex_enum.run_plex_enum(db_file, cfg)
    assert fake.enumerate_called is True
    assert stats["skipped_unchanged"] == 0


def test_baseline_is_stamped_after_successful_enum(tmp_path, monkeypatch):
    """After a successful enum, last_enum_content_changed_at
    must be UPDATEd to the live value motif observed. This is
    what enables the gate to fire on the NEXT run."""
    db_file = _seed_db(tmp_path, stored_cca=None)
    fake = _FakeClient(live_cca="1700000005")
    monkeypatch.setattr(plex_enum, "PlexClient", lambda cfg: fake)
    cfg = PlexConfig(url="http://x", token="t",
                     movie_section="1", tv_section="2")
    plex_enum.run_plex_enum(db_file, cfg)
    with sqlite3.connect(db_file) as conn:
        row = conn.execute(
            "SELECT last_enum_content_changed_at FROM plex_sections "
            "WHERE section_id = '1'"
        ).fetchone()
    assert row[0] == "1700000005"


def test_baseline_uses_old_live_value_not_fresh_reread(
        tmp_path, monkeypatch):
    """Race-safety: if Plex bumps contentChangedAt mid-enum (a
    user adds a file during the fetch), the v1.14.74 design
    stores the OLD live_value (read at enum start) so the next
    run sees `live > stored` and fires another enum to pick up
    the missed items. Storing the new value would silently mask
    the gap.

    Verified indirectly: the FakeClient's discover_sections
    returns the same value throughout the run; on success the
    stored value matches that single live read. A real Plex
    bump mid-enum would have produced a different `live_cca` on
    the test client's second discover_sections call (which
    plex_enum doesn't make — it only reads ONCE upfront)."""
    db_file = _seed_db(tmp_path, stored_cca=None)
    fake = _FakeClient(live_cca="1700000005")
    discover_call_count = [0]
    original_discover = fake.discover_sections

    def _track_discover():
        discover_call_count[0] += 1
        return original_discover()

    fake.discover_sections = _track_discover  # type: ignore
    monkeypatch.setattr(plex_enum, "PlexClient", lambda cfg: fake)
    cfg = PlexConfig(url="http://x", token="t",
                     movie_section="1", tv_section="2")
    plex_enum.run_plex_enum(db_file, cfg)
    # Exactly ONE discover_sections call — we do NOT re-read at
    # the end of the enum to "freshen" the stored value.
    assert discover_call_count[0] == 1
