"""v1.18.72 — class-9 silent-failure breadcrumb sweep.

Audit rollover of the class-9 "silent-defensive-catch" pattern
(CLAUDE.md § 9): try/except blocks that absorb a failure mode
without a log breadcrumb are indistinguishable from a successful
no-op, so state drift accumulates and surfaces weeks later as
"the deploy looked fine but nothing happened."

v1.17.1 / v1.17.9 / v1.17.11 / v1.17.13 / v1.17.17 each closed
out subsets of this class. v1.18.72's audit found two remaining
MED-severity sites in `app/web/api.py`:

  1. `_oEmbed_for` (line ~9151) — link preview cache fetch.
     Caught network/JSON/import failures + returned None silently.
     Cold path (24h cache), but operator debugging "why are
     SoundCloud previews missing?" had no breadcrumb.

  2. `api_replace_item` body parse (line ~12544) — optional
     `kind` body field. Caught JSON parse errors + `pass`-ed
     silently. Legacy callers send empty body (intended path),
     but a malformed body would also slip through with no trace.

Both get `log.debug(...)` breadcrumbs — debug level is right for
cold paths so legacy-quiet stays quiet, but malformed inputs
have a thread to pull on.

The 15 LOW findings are benign defensive catches (known-empty
getters, parse failures on untrusted input where failure means
"fall back to default with comment"). Left as-is; the audit
report catalogues them for future passes.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


def _api_py() -> str:
    return (REPO / "app" / "web" / "api.py").read_text()


# ── Site 1: _oEmbed_for fetch failure ───────────────────────


def test_oembed_fetch_logs_on_http_non_2xx():
    """A non-2xx response from the oEmbed provider must log a
    debug breadcrumb naming the source + status + url BEFORE the
    return None. Pre-fix the `if resp.status_code != 200: return
    None` early-bailed with no trace — operator debugging "why
    do SoundCloud previews fail?" had no signal."""
    src = _api_py()
    # Find the oEmbed fetch block.
    fetch_idx = src.index("provider = _OEMBED_PROVIDERS[source]")
    fetch_end = src.index("# Trim to fields", fetch_idx)
    block = src[fetch_idx:fetch_end]
    # log.debug call must reference source + status_code + url.
    assert "log.debug(" in block, (
        "v1.18.72: non-2xx oEmbed path must log a debug "
        "breadcrumb (was silent return None)"
    )
    import re
    block_flat = re.sub(r'"\s*"', "", block)
    assert "returned HTTP" in block_flat, (
        "v1.18.72: log message names the HTTP status"
    )


def test_oembed_fetch_logs_on_exception():
    """A network / JSON / import exception during the fetch must
    log a debug breadcrumb naming the source + url + exception
    BEFORE the return None. Pre-fix `except Exception: return
    None` swallowed every failure mode."""
    src = _api_py()
    fetch_idx = src.index("provider = _OEMBED_PROVIDERS[source]")
    fetch_end = src.index("# Trim to fields", fetch_idx)
    block = src[fetch_idx:fetch_end]
    # The except block captures the exception (was `except Exception:`).
    assert "except Exception as e:" in block, (
        "v1.18.72: exception must be captured in `as e` so the "
        "breadcrumb can include the error detail"
    )
    # And log.debug uses %s formatting with e.
    import re
    block_flat = re.sub(r'"\s*"', "", block)
    assert "fetch failed for source=" in block_flat
    assert "%s" in block_flat  # format placeholder for the exception


def test_oembed_v1_18_72_marker_present():
    """v1.18.72 marker required in the oEmbed fetch block so a
    future refactor reading the area sees WHY the log call
    exists."""
    src = _api_py()
    fetch_idx = src.index("provider = _OEMBED_PROVIDERS[source]")
    fetch_end = src.index("# Trim to fields", fetch_idx)
    block = src[fetch_idx:fetch_end]
    assert "v1.18.72" in block
    # Class-9 reference.
    assert "class 9" in block.lower() or "class-9" in block.lower()


# ── Site 2: api_replace_item body parse ─────────────────────


def test_replace_body_parse_logs_on_failure():
    """The optional `kind` body parse must log a debug breadcrumb
    on JSON parse failure (or any other exception). Pre-fix bare
    `except Exception: pass` swallowed both the legacy-empty-body
    case (intended) AND malformed-body cases (unintended) with
    no way to distinguish."""
    src = _api_py()
    # Anchor on the api_replace_item function + the body-parse
    # try block.
    fn_idx = src.index("async def api_replace_item(")
    fn_end = src.index("with get_conn(db) as conn", fn_idx)
    block = src[fn_idx:fn_end]
    assert "log.debug(" in block, (
        "v1.18.72: body parse failure path must log a debug "
        "breadcrumb"
    )
    # The exception is captured as `e`.
    assert "except Exception as e:" in block, (
        "v1.18.72: exception captured so the log message can "
        "include the error detail"
    )
    # The breadcrumb mentions the function context.
    import re
    block_flat = re.sub(r'"\s*"', "", block)
    assert "api_replace_item: body parse failed" in block_flat


def test_replace_body_parse_v1_18_72_marker():
    """v1.18.72 marker explains the legacy-empty vs malformed
    distinction so a future refactor understands why the catch
    is intentional (legacy) but the log is required (malformed
    debugging)."""
    src = _api_py()
    fn_idx = src.index("async def api_replace_item(")
    fn_end = src.index("with get_conn(db) as conn", fn_idx)
    block = src[fn_idx:fn_end]
    assert "v1.18.72" in block
    block_flat = " ".join(block.split())
    assert ("legacy" in block_flat.lower()
            and "malformed" in block_flat.lower()), (
        "v1.18.72: marker must distinguish legacy-empty (intended) "
        "from malformed-body (debug breadcrumb worth keeping)"
    )


# ── Both sites use debug-level (cold paths) ─────────────────


def test_both_sites_use_debug_level_not_warn():
    """Both fixes use log.debug (not log.warning or log.error).
    These are cold paths — link previews are 24h-cached
    enrichment; replace body parse falls back to a global
    default. Debug level keeps the legacy-quiet case quiet while
    leaving a thread to pull on for the malformed case.

    Pin the level to prevent a future "promote to warn" change
    that would spam logs with the legacy-empty-body case."""
    src = _api_py()
    # Site 1
    fetch_idx = src.index("provider = _OEMBED_PROVIDERS[source]")
    fetch_end = src.index("# Trim to fields", fetch_idx)
    fetch_block = src[fetch_idx:fetch_end]
    assert "log.warning" not in fetch_block, (
        "v1.18.72: oEmbed fetch is a cold path; use log.debug "
        "not warn"
    )
    # Site 2
    fn_idx = src.index("async def api_replace_item(")
    fn_end = src.index("with get_conn(db) as conn", fn_idx)
    repl_block = src[fn_idx:fn_end]
    # The body-parse block specifically — find the except line.
    parse_idx = repl_block.index("except Exception as e:")
    parse_block = repl_block[parse_idx:parse_idx + 800]
    assert "log.warning" not in parse_block, (
        "v1.18.72: body parse is a cold path (legacy callers send "
        "empty body); use log.debug not warn — otherwise every "
        "legacy call spams the log"
    )
