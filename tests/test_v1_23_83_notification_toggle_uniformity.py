"""v1.23.83 — notification SETTINGS toggle uniformity.

The notification family content went uniform in v1.23.81/82; this does
the same to the CONFIGURATION surface (the per-event toggle list in
/settings → NOTIFICATIONS). Before, the muted suffix conveyed "default
on/off" four different ways (or not at all) — "(on)" / "(opt-in)" /
"(OFF by default)" / nothing — mixed with character notes
(noisy / rare / per-row). Now:

  - every toggle ends with a uniform muted (on)/(off) chip reflecting
    its registry default; character notes live in the hint text;
  - every toggle label carries its notification's emoji, so the config
    list visually mirrors the Discord messages it controls;
  - the three theme-lost toggles are parallel — "— BACKUP READY" /
    "— STILL PLAYING" / "— NO FALLBACK"; the sidecar one was realigned
    to match its v1.23.82 notification title ("💔 Theme lost — still
    playing", was "SIDECAR AVAILABLE").
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
HTML = (REPO / "app" / "web" / "templates" / "settings.html").read_text()


def _toggle_label(key: str, *, prefix: str = "notifications.events") -> str:
    """The form-label-text span for one <prefix>.<key> toggle.

    v0.51.215: prefix is a parameter because this lookup was hardcoded to
    notifications.events. — so the v0.51.210 IN-APP INBOX block, which repeats the same
    event kinds under notifications.inbox_events.*, was invisible to every guard here.
    That is precisely why label drift shipped in it."""
    i = HTML.index(f'data-cfg-field="{prefix}.{key}"')
    start = HTML.index('<span class="form-label-text">', i)
    end = HTML.index("</span></span>", start) + len("</span></span>")
    return HTML[start:end]


def _label_name(label: str) -> str:
    """The label's NAME — emoji + words, without the trailing (on)/(off) chip."""
    inner = label[label.index(">") + 1:]
    cut = inner.find('<span class="muted')
    return (inner[:cut] if cut != -1 else inner).strip()


# ── the registry ↔ chip guard (the load-bearing test) ───────

def test_every_toggle_chip_matches_its_registry_default():
    """Each toggle's muted (on)/(off) chip must reflect the event's
    _DEFAULT_NOTIFY_EVENTS default — so the UI can't drift from the
    actual default, and the indicator is uniform across all 18."""
    from app.core.config_file import _DEFAULT_NOTIFY_EVENTS
    for key, default in _DEFAULT_NOTIFY_EVENTS.items():
        label = _toggle_label(key)
        want = '<span class="muted small">(on)</span>' if default \
            else '<span class="muted small">(off)</span>'
        assert want in label, (
            f"{key} (default={default}) must show the uniform "
            f"{'(on)' if default else '(off)'} chip — got: {label!r}"
        )


def test_no_legacy_default_state_tags_remain():
    """The inconsistent pre-v1.23.83 default tags must be gone from the
    toggle LABELS (the muted-span form). 'opt in here' may still appear
    in hint prose — this checks the label span only."""
    from app.core.config_file import _DEFAULT_NOTIFY_EVENTS
    for key in _DEFAULT_NOTIFY_EVENTS:
        label = _toggle_label(key)
        assert "(opt-in" not in label, f"{key} label kept a legacy (opt-in) tag"
        assert "OFF by default" not in label
        assert "(rare, actionable)" not in label


# ── emoji on the theme_* labels mirrors the notification ────

def test_theme_labels_carry_their_notification_emoji():
    cases = {
        "theme_added":             "🎵",
        "theme_pushed":            "📤",
        "theme_backed_up":         "💾",
        "theme_deleted":           "🗑️",
        "new_tdb_theme_available": "✨",
        "backup_ready_to_deploy":  "🎯",
        "theme_lost_backup_ready": "💔",
        "theme_lost_sidecar_available": "💔",
        "plex_theme_lost":         "💔",
    }
    for key, emoji in cases.items():
        label = _toggle_label(key)
        assert emoji in label, f"{key} toggle label must carry {emoji}"


# ── theme-lost family labels are parallel + match notifications ──

def test_theme_lost_toggles_are_parallel():
    backup = _toggle_label("theme_lost_backup_ready")
    sidecar = _toggle_label("theme_lost_sidecar_available")
    nofb = _toggle_label("plex_theme_lost")
    assert "THEME LOST — BACKUP READY" in backup
    # v1.23.82 retitled this notification "💔 Theme lost — still playing";
    # the toggle now matches (was "SIDECAR AVAILABLE").
    assert "THEME LOST — STILL PLAYING" in sidecar
    assert "SIDECAR AVAILABLE" not in sidecar
    assert "THEME LOST — NO FALLBACK" in nofb
    assert "(PLEX)" not in nofb


# ── the same kind, named the same way in both blocks ─────────

def test_inbox_block_labels_match_the_events_block_exactly():
    """v0.51.215: the IN-APP INBOX block repeats the same event kinds, so a kind must be
    NAMED identically in both — same emoji (variation selector included) and same
    qualifier. Two had drifted: a bare U+1F6E0 without VS16 (rendering with different
    glyph presentation than its twin on the same page), and 'THEME LOST' missing its
    '— NO FALLBACK', which left it reading as an ambiguous fourth sibling three lines
    under '— STILL PLAYING' and '— BACKUP READY'.

    The (on)/(off) chip legitimately differs — the two registries have separate defaults.
    Only the name is pinned."""
    from app.core.config_file import _DEFAULT_INBOX_EVENTS, _DEFAULT_NOTIFY_EVENTS
    shared = set(_DEFAULT_INBOX_EVENTS) & set(_DEFAULT_NOTIFY_EVENTS)
    assert shared, "the registries must overlap or this guard is vacuous"
    for key in sorted(shared):
        ev = _label_name(_toggle_label(key))
        ib = _label_name(_toggle_label(key, prefix="notifications.inbox_events"))
        assert ev == ib, (
            f"{key} is named differently in the two blocks:\n"
            f"  events.*       {ev!r}\n  inbox_events.* {ib!r}")


def test_no_block_claims_the_inbox_is_unconditional():
    """v0.51.210 made the in-app INBOX per-kind toggleable, which falsified the standing
    promise that a kind 'always lands in the INBOX regardless of this toggle'. A user who
    switched a kind OFF and then read that sentence would conclude the new toggle was
    broken."""
    for claim in ("regardless of this toggle", "always records to the in-app INBOX",
                  "unconditionally of this toggle"):
        assert claim not in HTML, f"stale pre-v0.51.210 copy still present: {claim!r}"
    from pathlib import Path as _P
    cfg = (REPO / "app" / "core" / "config_file.py").read_text()
    assert "unconditionally of this toggle" not in cfg


# ── version pin ──────────────────────────────────────────────

def test_v1_23_83_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
