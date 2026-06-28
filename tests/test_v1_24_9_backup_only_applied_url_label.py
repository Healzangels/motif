"""v1.24.9 — INFO card mislabeled a backup-only row's URL as "applied".

the user's SpongeBob SquarePants repro: the row is SRC=P (Plex serving its own
theme) with a TB ThemerrDB backup downloaded but NOT placed (motif stamped
local_files.last_place_attempt_reason='backup_only' and deferred to Plex). The
INFO card nonetheless rendered the downloaded TDB URL under the **"applied url"**
label, reading as if that theme were playing.

Root cause: `currentUrl` (the value the "applied url" line shows) is derived
purely from `isUrlSourced = lfSource === 'themerrdb' || 'url'` — it never
consulted `last_place_attempt_reason`. So any URL-sourced backup advertised
itself as applied. This is the same class as the v1.22.11 upload fix ("an
uploaded MP3 isn't an applied URL"): a *backup* isn't an applied URL either.

Fix (JS-only, label change): openInfoDialog derives `lfIsBackupOnly` from the
reason and relabels the line to **"backup url"** for backup-only rows. The URL,
thumbnail, video id and 0:30 audio preview all stay (they correctly previewed
the staged backup). The label returns to "applied url" the instant PROMOTE TO
ACTIVE flips the reason off 'backup_only'. Covers TB (themerrdb backup) and UB
(user-url backup); B/AB (plex_cloud/adopt) backups aren't URL-sourced, so they
already showed "applied url: —" and are unaffected.

These are source-text guards (the label is computed in app.js, no DOM here) plus
a server-contract pin: api_item MUST keep returning last_place_attempt_reason or
the JS gate reads undefined and silently regresses to "applied url".
"""
from __future__ import annotations

from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()


def test_backup_only_flag_derived_from_place_reason():
    """The label gate reads local_files.last_place_attempt_reason — the
    exact flag the worker stamps when it downloads but defers to Plex."""
    assert "const lfReason = lf?.last_place_attempt_reason || null;" in APP_JS
    assert "const lfIsBackupOnly = lfReason === 'backup_only';" in APP_JS


def test_applied_url_label_relabels_to_backup_url_when_backup_only():
    """`appliedUrlLabel` resolves to 'backup url' for a backup-only row that
    carries a URL, and falls back to 'applied url' otherwise."""
    assert "const appliedUrlLabel = (lfIsBackupOnly && currentUrl)" in APP_JS
    assert "? 'backup url'" in APP_JS
    assert ": 'applied url';" in APP_JS


def test_dt_renders_the_label_variable_not_a_hardcoded_string():
    """The <dt> must render ${appliedUrlLabel}; the pre-v1.24.9 hardcoded
    literal must be gone so a regression that re-pins 'applied url' fails."""
    assert "<dt>${appliedUrlLabel} ${currentUrl ?" in APP_JS
    assert "<dt>applied url ${currentUrl ?" not in APP_JS, (
        "re-hardcoding 'applied url' would resurrect the backup-only mislabel"
    )


def test_label_var_defined_after_currentUrl_is_in_scope():
    """appliedUrlLabel reads currentUrl, so it must appear AFTER currentUrl is
    bound (both const — referencing it earlier would be a TDZ ReferenceError)."""
    i_cur = APP_JS.index("const currentUrl = isUrlSourced")
    i_lbl = APP_JS.index("const appliedUrlLabel = (lfIsBackupOnly")
    assert i_cur < i_lbl, "appliedUrlLabel must be declared after currentUrl"


def test_api_item_still_returns_last_place_attempt_reason():
    """Server contract: the JS gate is only correct if api_item surfaces
    last_place_attempt_reason on the payload. api_item builds data.local_file
    from `SELECT * FROM local_files`, so last_place_attempt_reason — a native
    local_files column — is present. If that SELECT ever narrows to named
    columns, lfReason goes undefined → every backup row silently reverts to
    'applied url'. (v1.24.11: pin the real data path, not a string that happens
    to live in the unrelated _library_main_query.)"""
    assert "SELECT * FROM local_files" in API_PY


def test_app_js_still_parses():
    """The label edit must not break the parse (brace balance won't catch a
    malformed template literal)."""
    quickjs = pytest.importorskip("quickjs")
    ctx = quickjs.Context()
    try:
        ctx.eval(APP_JS)
    except quickjs.JSException as e:
        assert "SyntaxError" not in str(e), f"app.js failed to parse: {e}"
