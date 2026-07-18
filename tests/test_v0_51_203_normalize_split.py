"""v0.51.203 — Phase 3: split the on-arrival normalize gate into two toggles.

normalize_on_download was one switch covering every download. Now it's SPLIT by who
triggered the download: motif-initiated auto-picks (reason 'new' / 'url_changed', the
sync/enum auto-download) go through normalize_auto_added; user-triggered downloads
(// DOWNLOAD FROM TDB, bulk) go through normalize_on_download; an explicit per-job
payload.normalize (SET URL) still overrides both. UPLOAD MP3 / SET URL are unaffected —
they carry their own per-theme checkbox.

The gate decision lives in _should_condition_download (pure, no I/O) so the 6 branches
are unit-tested directly rather than through a real download.
"""
from __future__ import annotations

from pathlib import Path

from app.core.worker import (
    _should_condition_download,
    _AUTO_ADDED_DOWNLOAD_REASONS,
)

REPO = Path(__file__).resolve().parent.parent


class _S:
    """Minimal settings double — only the two toggles the gate reads."""
    def __init__(self, *, manual: bool, auto: bool):
        self.normalize_on_download = manual
        self.normalize_auto_added = auto


# ── the gate decision: auto vs manual vs explicit ────────────────────────────


def test_auto_pick_uses_the_auto_toggle_not_the_manual_one():
    """A 'new' auto-download conditions iff normalize_auto_added — INDEPENDENT of the
    manual toggle (this is the whole point of the split)."""
    for reason in _AUTO_ADDED_DOWNLOAD_REASONS:
        # auto on, manual off → auto-pick conditions
        assert _should_condition_download(_S(manual=False, auto=True), {"reason": reason}) is True
        # auto off, manual on → auto-pick does NOT condition
        assert _should_condition_download(_S(manual=True, auto=False), {"reason": reason}) is False


def test_manual_download_uses_the_manual_toggle_not_the_auto_one():
    """A user-triggered reason conditions iff normalize_on_download — independent of auto."""
    for reason in ("manual", "bulk_select", "bulk_backup", "upstream_update_accepted", "override"):
        assert _should_condition_download(_S(manual=True, auto=False), {"reason": reason}) is True
        assert _should_condition_download(_S(manual=False, auto=True), {"reason": reason}) is False


def test_no_reason_falls_back_to_the_manual_toggle():
    """A download with no reason key is treated as user-triggered (the conservative
    default — auto-picks always stamp a reason)."""
    assert _should_condition_download(_S(manual=True, auto=False), {}) is True
    assert _should_condition_download(_S(manual=False, auto=True), {}) is False


def test_explicit_payload_normalize_overrides_both_toggles():
    """SET URL's payload.normalize wins regardless of reason or toggles, both ways."""
    both_off = _S(manual=False, auto=False)
    both_on = _S(manual=True, auto=True)
    # explicit True conditions even with both toggles off, on any reason
    assert _should_condition_download(both_off, {"normalize": True, "reason": "new"}) is True
    assert _should_condition_download(both_off, {"normalize": True, "reason": "manual"}) is True
    # explicit False stays raw even with both toggles on
    assert _should_condition_download(both_on, {"normalize": False, "reason": "new"}) is False
    assert _should_condition_download(both_on, {"normalize": False, "reason": "manual"}) is False


def test_the_two_toggles_are_independent():
    """All four (manual, auto) combinations produce the expected auto/manual decision."""
    auto_reason = {"reason": "new"}
    manual_reason = {"reason": "manual"}
    for manual in (False, True):
        for auto in (False, True):
            s = _S(manual=manual, auto=auto)
            assert _should_condition_download(s, auto_reason) is auto
            assert _should_condition_download(s, manual_reason) is manual


def test_auto_reason_set_is_exactly_the_two_sync_download_reasons():
    """The auto bucket is the sync/enum auto-download reasons — 'new' (unthemed
    auto-acquire) + 'url_changed' (re-fetch on upstream URL change). Anything else is
    a user action. Pins the set so a new manual reason can't silently join it."""
    assert _AUTO_ADDED_DOWNLOAD_REASONS == frozenset({"new", "url_changed"})


# ── config surface ───────────────────────────────────────────────────────────


def test_normalize_auto_added_defaults_off():
    from app.core.config_file import MotifConfig
    assert MotifConfig().loudness.normalize_auto_added is False


def test_env_override_maps_the_new_field():
    cf = (REPO / "app" / "core" / "config_file.py").read_text()
    assert '("MOTIF_NORMALIZE_AUTO_ADDED", "loudness.normalize_auto_added"' in cf


def test_settings_property_exposes_both_toggles():
    cfg_py = (REPO / "app" / "config.py").read_text()
    assert "def normalize_auto_added" in cfg_py
    assert "def normalize_on_download" in cfg_py


# ── Settings UI (both checkboxes, saved by the downloads-loudness button) ─────


def test_settings_has_both_normalize_checkboxes():
    html = (REPO / "app" / "web" / "templates" / "settings.html").read_text()
    assert 'data-cfg-field="loudness.normalize_on_download"' in html
    assert 'data-cfg-field="loudness.normalize_auto_added"' in html
    # both are loudness.* fields, so the existing SAVE DOWNLOADS (data-save="downloads
    # loudness") button collects them — no new save wiring needed.
    assert 'data-save="downloads loudness"' in html
