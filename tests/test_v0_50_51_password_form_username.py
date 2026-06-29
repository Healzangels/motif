"""v0.50.51 — the change-password form carries a hidden username field.

Chrome's console flagged "Password forms should have (optionally hidden) username
fields for accessibility" on #password-form (CURRENT + NEW password, no username).
Add a hidden autocomplete=username input so password managers can associate the
credential and the a11y warning clears. The /api/admin/password endpoint reads
only current_password / new_password as typed Form params, so the extra field is
ignored by FastAPI (no behavior change).
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SETTINGS = (REPO / "app" / "web" / "templates" / "settings.html").read_text()


def test_password_form_has_hidden_username_field_before_the_password_inputs():
    form_start = SETTINGS.index('<form id="password-form"')
    form = SETTINGS[form_start:SETTINGS.index("</form>", form_start)]
    assert 'autocomplete="username"' in form
    assert "hidden" in form
    # the username field precedes the password inputs (the order managers expect)
    assert form.index('autocomplete="username"') < form.index('name="current_password"')
