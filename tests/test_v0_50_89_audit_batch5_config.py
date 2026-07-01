"""v0.50.89 — holistic-audit Batch 5: config correctness.

1. config_file.py `validate()` crashed with an unhandled TypeError when a
   hand-edited/corrupted motif.yaml field had the wrong type (e.g. a string
   where a number was expected) — `_hydrate_dataclass` does no type
   coercion by design, so a bad field only surfaced as a raw 500 the first
   time validate() touched it. Wrapped the whole checks body in a
   try/except that turns it into a normal validation error message.
2. config_file.py `ConfigFile.save()` silently baked a currently-active env
   var override into motif.yaml on every save, since `cfg` (from `load()`)
   always carries the env-applied value for that field. Once the env var
   was later unset, the config would keep behaving as if it were still
   set because the disk value had been overwritten. `save()` now detects
   fields that are unchanged from what the env currently mandates and
   restores the genuine on-disk value for those before serializing.
3. sections.py `refresh_sections`'s include/exclude title matching was
   exact-string case-sensitive, so a title typo'd in casing silently
   excluded the whole library with no diagnostic. Matching is now
   case-insensitive, and a non-empty include list matching zero sections
   now logs a warning naming the mismatch.
"""
from __future__ import annotations

import logging
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parent.parent


# ── 1. validate() TypeError guard ────────────────────────────────────────


def test_validate_does_not_crash_on_wrong_typed_field(tmp_path):
    from app.core.config_file import MotifConfig, validate

    cfg = MotifConfig()
    # Simulate a hand-edited motif.yaml where a numeric field became a
    # string that can't be compared the way validate() expects.
    cfg.downloads.rate_per_hour = "not-a-number"  # type: ignore[assignment]

    errors = validate(cfg, require_themes_dir=False)

    assert errors, "a broken field must produce a validation error, not raise"
    assert any("unexpected type" in e for e in errors)


def test_validate_still_reports_normal_errors():
    from app.core.config_file import MotifConfig, validate

    cfg = MotifConfig()
    cfg.paths.themes_dir = ""
    errors = validate(cfg, require_themes_dir=True)
    assert any("themes_dir" in e for e in errors)


# ── 2. save() doesn't bake env overrides into YAML ──────────────────────


def test_save_restores_disk_value_for_untouched_env_overridden_field(
    tmp_path, monkeypatch,
):
    from app.core.config_file import ConfigFile

    path = tmp_path / "motif.yaml"
    path.write_text(yaml.safe_dump({"downloads": {"rate_per_hour": 50}}))

    monkeypatch.setenv("MOTIF_DL_RATE_HOUR", "999")

    cf = ConfigFile(path)
    cfg = cf.load()
    assert cfg.downloads.rate_per_hour == 999  # env wins at load time

    # Caller changes something unrelated; leaves the overridden field as-is.
    cfg.matching.plus_mode = "literal"
    cf.save(cfg, updated_by="test")

    on_disk = yaml.safe_load(path.read_text())
    assert on_disk["downloads"]["rate_per_hour"] == 50, (
        "v0.50.89: an untouched env-overridden field must NOT be baked "
        "into the saved YAML — the genuine disk value must survive"
    )
    assert on_disk["matching"]["plus_mode"] == "literal"


def test_save_respects_explicit_edit_of_env_overridden_field(
    tmp_path, monkeypatch,
):
    from app.core.config_file import ConfigFile

    path = tmp_path / "motif.yaml"
    path.write_text(yaml.safe_dump({"downloads": {"rate_per_hour": 50}}))

    monkeypatch.setenv("MOTIF_DL_RATE_HOUR", "999")

    cf = ConfigFile(path)
    cfg = cf.load()
    assert cfg.downloads.rate_per_hour == 999

    # Caller explicitly sets the overridden field to something else — this
    # is a real edit and must be respected, not silently discarded.
    cfg.downloads.rate_per_hour = 123
    cf.save(cfg, updated_by="test")

    on_disk = yaml.safe_load(path.read_text())
    assert on_disk["downloads"]["rate_per_hour"] == 123, (
        "an explicit edit to an env-overridden field must persist"
    )


def test_save_with_no_env_overrides_is_unaffected(tmp_path):
    from app.core.config_file import ConfigFile

    path = tmp_path / "motif.yaml"
    cf = ConfigFile(path)
    cfg = cf.load()
    cfg.downloads.rate_per_hour = 42
    cf.save(cfg, updated_by="test")

    on_disk = yaml.safe_load(path.read_text())
    assert on_disk["downloads"]["rate_per_hour"] == 42


def test_save_handles_missing_disk_file_with_env_override(tmp_path, monkeypatch):
    """First-ever save (no motif.yaml on disk yet) with an env override
    active must not crash — there's no disk value to restore, so the env
    value is written (nothing better to fall back to)."""
    from app.core.config_file import ConfigFile

    path = tmp_path / "motif.yaml"
    monkeypatch.setenv("MOTIF_DL_RATE_HOUR", "999")

    cf = ConfigFile(path)
    cfg = cf.load()
    cf.save(cfg, updated_by="test")

    on_disk = yaml.safe_load(path.read_text())
    assert on_disk["downloads"]["rate_per_hour"] == 999


# ── 3. sections.py case-insensitive include/exclude + zero-match log ────


class _FakeSection:
    def __init__(self, section_id, title, type_="movie"):
        self.section_id = section_id
        self.title = title
        self.type = type_
        self.agent = "tv.plex.agents.movie"
        self.language = "en"
        self.location_paths = ["/data/Movies"]
        self.uuid = f"uuid-{section_id}"


class _FakePlex:
    def __init__(self, sections):
        self._sections = sections

    def discover_sections(self):
        return self._sections


def test_refresh_sections_include_matches_case_insensitively(tmp_path):
    from app.core.db import init_db
    from app.core.sections import refresh_sections

    db_path = tmp_path / "m.db"
    init_db(db_path)
    plex = _FakePlex([_FakeSection("1", "Movies")])

    sections = refresh_sections(
        db_path, plex,
        excluded_titles=set(),
        included_titles={"movies"},  # lowercase vs Plex's "Movies"
    )

    assert len(sections) == 1
    assert sections[0]["included"] == 1, (
        "v0.50.89: include matching must be case-insensitive"
    )


def test_refresh_sections_exclude_matches_case_insensitively(tmp_path):
    from app.core.db import init_db
    from app.core.sections import refresh_sections

    db_path = tmp_path / "m.db"
    init_db(db_path)
    plex = _FakePlex([_FakeSection("1", "Movies"), _FakeSection("2", "TV Shows")])

    sections = refresh_sections(
        db_path, plex,
        excluded_titles={"MOVIES"},  # uppercase vs Plex's "Movies"
        included_titles=set(),
    )

    by_id = {s["section_id"]: s for s in sections}
    assert by_id["1"]["included"] == 0
    assert by_id["2"]["included"] == 1


def test_refresh_sections_zero_match_include_logs_warning(tmp_path, caplog):
    from app.core.db import init_db
    from app.core.sections import refresh_sections

    db_path = tmp_path / "m.db"
    init_db(db_path)
    plex = _FakePlex([_FakeSection("1", "Movies")])

    with caplog.at_level(logging.WARNING):
        sections = refresh_sections(
            db_path, plex,
            excluded_titles=set(),
            included_titles={"Mooovies"},  # typo — matches nothing
        )

    assert sections[0]["included"] == 0
    assert any(
        "matched none" in rec.message for rec in caplog.records
    ), "a non-empty include list matching zero sections must log a warning"


def test_refresh_sections_include_match_does_not_warn(tmp_path, caplog):
    from app.core.db import init_db
    from app.core.sections import refresh_sections

    db_path = tmp_path / "m.db"
    init_db(db_path)
    plex = _FakePlex([_FakeSection("1", "Movies")])

    with caplog.at_level(logging.WARNING):
        refresh_sections(
            db_path, plex,
            excluded_titles=set(),
            included_titles={"Movies"},
        )

    assert not any("matched none" in rec.message for rec in caplog.records)
