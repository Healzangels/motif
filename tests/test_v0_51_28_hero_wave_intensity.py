"""v0.51.28 — hero wave ramps harder per running job (the user).

Per-job energy step and the energy->visual gain both increased so each extra
running job visibly lifts + brightens the hero wave instead of nudging it a few
percent.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def test_per_job_energy_step_increased():
    # the busy-score -> energy step is now 0.22 (was 0.145).
    assert "(_busyScore - 1) * 0.22" in APP_JS, (
        "v0.51.28: the per-job hero-wave energy step must be 0.22")
    assert "(_busyScore - 1) * 0.145" not in APP_JS, (
        "the old 0.145 step must not survive")


def test_energy_visual_gain_increased():
    # primary wave (.hero::after): bigger height + brightness gain.
    assert "scaleY(calc(1 + 0.44 * var(--hero-wave-energy, 0)))" in APP_CSS
    assert "brightness(calc(1 + 0.75 * var(--hero-wave-energy, 0)))" in APP_CSS
    # the old, subtler gains must be gone.
    assert "scaleY(calc(1 + 0.32 * var(--hero-wave-energy, 0)))" not in APP_CSS
    assert "brightness(calc(1 + 0.55 * var(--hero-wave-energy, 0)))" not in APP_CSS
