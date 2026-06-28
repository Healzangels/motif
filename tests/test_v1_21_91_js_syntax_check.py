"""v1.21.91 — app.js must PARSE, not just balance its braces.

The v1.21.88 edition-scoping cascade added `ratingKey` as a parameter to
`hydrateRecoveryOptions`, but the function body already declared
`const ratingKey = data.rating_key || null` — a duplicate binding. That is
a JS *early error* (SyntaxError: invalid redefinition of parameter name),
so the ENTIRE app.js failed to parse: on v1.21.90 the library hung on
"loading…" and every button was inert.

The full suite stayed green through three tags (.88/.89/.90) because the
JS guards are source-text greps + a brace/paren/backtick balance delta —
none of which EXECUTE the file. A duplicate parameter keeps the braces
balanced; only a real parser catches it.

This test runs app.js through a real modern-JS parser (quickjs) and fails
on any SyntaxError. A runtime ReferenceError (no DOM in quickjs — e.g.
`document`/`window`) means the file parsed clean, which is all we assert.

quickjs is a local/dev safety net (it's a C extension not in the CI image,
so the test importorskips when absent). The suite is run locally before
every tag — which is exactly where this guard needs to fire.
"""
from __future__ import annotations

from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
APP_JS = REPO / "app" / "web" / "static" / "app.js"


def test_app_js_has_no_syntax_error():
    """Parse the whole file with a real JS engine. Duplicate params,
    stray tokens, malformed template literals — all caught here, none
    caught by the brace-balance delta."""
    quickjs = pytest.importorskip("quickjs")
    src = APP_JS.read_text()
    ctx = quickjs.Context()
    try:
        ctx.eval(src)
    except quickjs.JSException as e:
        msg = str(e)
        assert "SyntaxError" not in msg, (
            "app.js failed to PARSE — this breaks the entire UI "
            "(library hangs on 'loading…', buttons inert). "
            f"Parser said: {msg}"
        )
        # A non-syntax error (ReferenceError: 'document' is not defined,
        # etc.) means the file parsed fully and only tripped on the
        # missing DOM at runtime — exactly what we expect.


def test_hydrate_recovery_options_has_no_duplicate_rating_key_param():
    """Targeted regression pin for the v1.21.88 break: the function
    derives ratingKey from `data.rating_key` in its body, so it must NOT
    also take it as a parameter (a duplicate binding = SyntaxError)."""
    src = APP_JS.read_text()
    sig_start = src.index("function hydrateRecoveryOptions(")
    sig = src[sig_start:src.index(")", sig_start) + 1]
    assert "ratingKey" not in sig, (
        "hydrateRecoveryOptions must not take ratingKey as a param — its "
        "body already declares `const ratingKey = data.rating_key`. "
        f"Signature was: {sig}"
    )
    # The body must still source ratingKey from the API response so the
    # v1.21.88 /intent edition-scoping survives the param removal.
    body = src[sig_start:sig_start + 30000]
    assert "const ratingKey = data.rating_key" in body, (
        "the /intent POST relies on this const for rating_key->edition "
        "resolution — removing it would silently un-scope PROMOTE TO ACTIVE"
    )


def test_v1_21_91_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
