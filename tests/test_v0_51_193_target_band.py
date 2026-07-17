"""v0.51.193 — code-review follow-ups on the loudness UI arc.

The load-bearing one: the level target had TWO disagreeing ranges. config validate
accepts -70..0 (a typo-guard), but the usable hover band is -31..-6, and only some
paths clamped. So a configured target outside [-31,-6] (which config still accepts)
made the download-conditioner level to the raw value while normalize-one clamped it —
the exact 'same op, two answers' mirror-drift v0.51.191 set out to remove, quietly
reopened. Fix: clamp at the ONE functional read point, settings.loudness_target_lufs,
via shared constants, so worker / normalize-one / the card's stepper all agree.

Also here: the app.js CEIL literal must track plex.THEME_UPLOAD_CEILING_BYTES (it was
a bare duplicate with nothing linking them).
"""
from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest

from app.core.config_file import LOUDNESS_TARGET_FLOOR, LOUDNESS_TARGET_CEIL
from app.core.plex import THEME_UPLOAD_CEILING_BYTES

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def _settings(target: str):
    d = Path(tempfile.mkdtemp())
    os.environ["MOTIF_CONFIG_DIR"] = str(d)
    os.environ["MOTIF_DATA_DIR"] = str(d / "data")
    os.environ["MOTIF_LOUDNESS_TARGET"] = target
    from app.config import Settings
    return Settings(config_dir=d, data_dir=d / "data")


def test_band_is_the_stepper_clamp():
    """The Python band constants must equal the client's LOUD_MIN/LOUD_MAX literals."""
    assert LOUDNESS_TARGET_FLOOR == -31.0 and LOUDNESS_TARGET_CEIL == -6.0
    assert "const LOUD_MIN = -31.0, LOUD_MAX = -6.0;" in APP_JS


@pytest.mark.parametrize("cfg,expect", [
    ("-20", -20.0),    # in band → passthrough
    ("-18", -18.0),    # the default
    ("-35", -31.0),    # below the floor (config still accepts it) → clamp up
    ("-3", -6.0),      # above the ceil → clamp down
    ("-31", -31.0),    # exact floor
    ("-6", -6.0),      # exact ceil
])
def test_settings_property_clamps_to_the_band(cfg, expect):
    """This is the single functional read point worker + normalize-one + api_item share.
    Pre-fix it returned the raw config value, so an out-of-band config diverged per-path.
    """
    s = _settings(cfg)
    assert s.loudness_target_lufs == expect
    # the raw config is untouched — config validate's -70..0 typo-guard still applies to it
    if cfg not in ("-20", "-18", "-31", "-6"):
        assert s._cfg.loudness.target_lufs != expect, "clamp must not mutate stored config"


def test_worker_and_normalize_one_no_longer_diverge_out_of_band():
    """The concrete regression: at config target -35, the download-conditioner (which
    reads the property) and normalize-one (which also resolves the property, then
    re-clamps) must agree. Both resolve to the floor, -31."""
    s = _settings("-35")
    conditioner_target = s.loudness_target_lufs                      # worker.py path
    normalize_default = s.loudness_target_lufs                       # normalize-one default
    clamped = max(LOUDNESS_TARGET_FLOOR, min(LOUDNESS_TARGET_CEIL, normalize_default))
    assert conditioner_target == clamped == -31.0


def test_app_js_ceil_matches_the_python_source_of_truth():
    """The card's over-ceiling gate hardcodes the byte count; pin it to plex.py so a
    change there can't silently drift the two apart."""
    assert f"const CEIL = {THEME_UPLOAD_CEILING_BYTES};" in APP_JS


def test_stepper_seed_is_clamped_to_the_band():
    """The card must not display/audition a target the server will clamp away."""
    assert "Math.max(LOUD_MIN, Math.min(LOUD_MAX," in APP_JS
    # and normalize-one's own clamp uses the shared constants, not bare literals
    api_py = (REPO / "app" / "web" / "api.py").read_text()
    assert "max(LOUDNESS_TARGET_FLOOR, min(LOUDNESS_TARGET_CEIL, target))" in api_py


def test_null_tmdb_row_is_guarded_before_the_post():
    """A row with data-id='' would POST tmdb_id: Number('') === 0; guard it with a
    clear message instead."""
    assert "if (!btn.dataset.id || !Number.isFinite(_idNum))" in APP_JS
    assert "tmdb_id: _idNum," in APP_JS


def test_preview_play_rejection_is_caught():
    """audio.play() rejects asynchronously; the sync try/catch can't see it."""
    assert "audio.play().catch(" in APP_JS
