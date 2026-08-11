"""v0.51.256 — the no-recovery theme-lost tier reaches the operator by default.

v1.18.90 shipped `plex_theme_lost` OFF-by-default on two grounds: the event
fires per-row, and a bulk Plex change could burst it. Both are now answered,
and by MEASUREMENT rather than argument:

  Burst   — bursts coalesce as of v0.51.254/255 (the bulk flag now reaches the
            client-side loops and both automated sweeps), on top of the
            pre-existing notify_dedupe per-row keys + 24h rate-limit.

  Noise   — the kind proved ACCURATE in the field. Five lifetime firings on the
            operator's install, five correct: ER (left the library entirely,
            zero plex_items rows) plus four 2026-08-09 disk-dropout casualties
            that were genuinely stranded — live row, has_theme=0, no local_files
            backup, and a ThemerrDB URL dead upstream (failure_kind=
            video_removed). No false positives at all.

The asymmetry that motivated the flip: `plex_theme_lost` is the ONLY tier that
means "no automatic recovery exists — a human must supply a URL", yet it was
the one tier muted, while the two RECOVERABLE tiers (backup_ready /
sidecar_available) alerted immediately. The cheap cases were loud and the
expensive one was silent. The four stranded titles sat unseen in the drawer for
two days.

NOTE for the operator's own install: `_serialize` writes the FULL events dict
to motif.yaml via `dataclasses.asdict`, and `_hydrate_dataclass` merges with
YAML winning — so an existing config carrying `plex_theme_lost: false` is
UNAFFECTED by this default. Changing the shipped default fixes fresh installs;
an existing install needs the Settings → NOTIFICATIONS toggle.
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

REPO = Path(__file__).resolve().parent.parent


def test_no_recovery_tier_defaults_on():
    from app.core.config_file import _DEFAULT_NOTIFY_EVENTS
    assert _DEFAULT_NOTIFY_EVENTS.get("plex_theme_lost") is True


def test_all_four_lost_tiers_now_alert_by_default():
    """The point of the tag stated as one property: no theme-loss tier is
    silent. Tiers 3 and 4 share the plex_theme_lost kind (the tier drives the
    BODY text, not the routing), so these three keys cover all four tiers."""
    from app.core.config_file import _DEFAULT_NOTIFY_EVENTS
    for kind in ("theme_lost_backup_ready", "theme_lost_sidecar_available",
                 "plex_theme_lost"):
        assert _DEFAULT_NOTIFY_EVENTS.get(kind) is True, kind


def test_dispatch_reaches_the_send_path_with_default_config(tmp_path):
    """Behavioral, not source-text (v1.18.81): a default-config dispatch must
    get past BOTH the unknown-kind gate and the enabled gate. Pinning only the
    constant would pass even if the gate dropped it downstream."""
    from app.core import notify
    from app.core.config_file import NotificationsConfig
    cfg = NotificationsConfig(apprise_urls=["json://localhost/stub"])
    with patch.object(notify, "_get_pool") as pool:
        notify.dispatch(
            tmp_path / "motif.db", cfg,
            event_kind="plex_theme_lost", title="t", body="b",
        )
    assert pool.return_value.submit.called, (
        "plex_theme_lost must reach the send path with DEFAULT config — the "
        "no-recovery tier is the one alert nothing else can substitute for")


def test_an_explicit_opt_out_still_wins():
    """The flip changes the DEFAULT, not the operator's authority. Anyone who
    deliberately turned this off keeps it off — and on this very install, the
    saved motif.yaml is exactly such an explicit value."""
    from app.core.config_file import NotificationsConfig
    cfg = NotificationsConfig()
    cfg.events = {**cfg.events, "plex_theme_lost": False}
    assert cfg.events["plex_theme_lost"] is False


def test_settings_ui_exposes_the_toggle():
    """An existing install can only change this in the UI (YAML wins over the
    default on load), so the toggle is load-bearing, not decorative."""
    html = (REPO / "app" / "web" / "templates" / "settings.html").read_text()
    assert 'data-cfg-field="notifications.events.plex_theme_lost"' in html


# ── the stale-comment correction ─────────────────────────────────────────

def test_inbox_kinds_note_does_not_claim_arrived_themed_is_dormant():
    """`plex_item_arrived_themed` DOES fire (3 rows on the operator's install),
    but the note above INBOX_EVENT_KINDS still called it forward-readiness that
    "doesn't fire until a later tag". Same class as the `rk=` log label fixed in
    v0.51.253: a stale claim costs a reader a wrong assumption."""
    src = (REPO / "app" / "core" / "notify_inbox.py").read_text()
    # The comment-trap, one more time: the CORRECTION quotes the false claim, so
    # `"doesn't fire until" not in src` fails against the fix itself (it did —
    # that is how this assertion got written). Assert the retraction is PRESENT
    # rather than that the phrase is absent.
    assert "DOES fire" in src, (
        "the dormancy note is unretracted — a reader will assume the event is "
        "inert and skip it when tracing a missing inbox row")
    from app.core.notify_inbox import INBOX_EVENT_KINDS
    assert "plex_item_arrived_themed" in INBOX_EVENT_KINDS
