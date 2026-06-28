"""v1.23.84 — resolve the 💾 notification-emoji collision.

The v1.23.82 emoji-distinctness guard only covered the 7 per-item THEME
events. Mapping emoji onto the Settings toggles (v1.23.83) surfaced a
SYSTEM-event collision the guard didn't watch: disk_low and
theme_backed_up both led with 💾. theme_backed_up legitimately owns 💾
(the "saved/staged" glyph); disk_low moves to 🗄️ (storage), so the two
are distinct in both the notification AND the Settings toggle list.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
WORKER_PY = (REPO / "app" / "core" / "worker.py").read_text()
SETTINGS_HTML = (REPO / "app" / "web" / "templates" / "settings.html").read_text()


def test_disk_low_notification_uses_cabinet_glyph():
    assert 'title=f"🗄️ Low disk space — {free_mb}MB free"' in WORKER_PY
    assert '💾 Low disk space' not in WORKER_PY  # old colliding glyph gone


def test_disk_low_settings_toggle_uses_cabinet_glyph():
    i = SETTINGS_HTML.index('data-cfg-field="notifications.events.disk_low"')
    label = SETTINGS_HTML[i:i + 200]
    assert "🗄️ DISK SPACE LOW" in label
    assert "💾 DISK SPACE LOW" not in label


def test_theme_backed_up_still_owns_the_floppy_glyph():
    """The collision is resolved by moving disk_low, NOT by changing
    theme_backed_up — confirm 💾 still belongs to the backup event."""
    from app.core.notify_content import format_theme_backed_up_title
    assert format_theme_backed_up_title({"display_title": "Foo"}) \
        == "💾 Theme backed up — Foo"
    i = SETTINGS_HTML.index(
        'data-cfg-field="notifications.events.theme_backed_up"')
    assert "💾 THEME BACKED UP" in SETTINGS_HTML[i:i + 200]


def test_v1_23_84_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
