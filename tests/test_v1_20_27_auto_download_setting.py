"""v1.20.27 — surface auto_download_new_themes_for_unthemed_rows in the UI.

the user: "where did we put the setting to auto download new themes for
rows we don't have a theme for?" It was config-file-only
(sync.auto_download_new_themes_for_unthemed_rows, default False). This
adds a Settings → SCHEDULE → SYNC TIMING checkbox + the
MOTIF_AUTO_DOWNLOAD_NEW_THEMES env override, matching the sibling
sync.auto_enum_after_sync toggle.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── env override behavioral ──────────────────────────────────


def test_env_override_enables_auto_download(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_AUTO_DOWNLOAD_NEW_THEMES", "true")
    from app.config import Settings
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    assert (
        s._cfg.sync.auto_download_new_themes_for_unthemed_rows is True
    ), "MOTIF_AUTO_DOWNLOAD_NEW_THEMES=true must flip the sync flag"


def test_default_is_false(tmp_path):
    from app.config import Settings
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    assert (
        s._cfg.sync.auto_download_new_themes_for_unthemed_rows is False
    ), "default stays False (explicit-prompt-over-silent, per v1.19.70)"


# ── wiring source pins ───────────────────────────────────────


def test_env_map_has_binding():
    cf = (REPO / "app" / "core" / "config_file.py").read_text()
    assert '"MOTIF_AUTO_DOWNLOAD_NEW_THEMES"' in cf
    assert "sync.auto_download_new_themes_for_unthemed_rows" in cf


def test_settings_html_has_schedule_checkbox():
    html = (REPO / "app" / "web" / "templates" / "settings.html").read_text()
    # the toggle exists, bound to the sync field.
    assert ('data-cfg-field="sync.'
            'auto_download_new_themes_for_unthemed_rows"') in html
    # v1.21.12: moved into the SCHEDULE → AUTOMATION block (grouped with
    # the other automation toggles; saved via data-save="sync placement"
    # which PATCHes both the sync.* and placement.* sections).
    auto = html[html.index('// AUTOMATION'):]
    auto = auto[:auto.index('data-save="sync placement"') + 100]
    assert "auto_download_new_themes_for_unthemed_rows" in auto, (
        "the checkbox must sit in the SCHEDULE → AUTOMATION block"
    )


def test_v1_20_27_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
