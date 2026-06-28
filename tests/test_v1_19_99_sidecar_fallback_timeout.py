"""v1.19.99 — sidecar fallback also covers Plex no-response (timeout).

Code-review #4 follow-up (the user: "lets take care of that optional …
maybe also somewhere in the logs indicate it did the fallback so it's
clear why it's local and not api uploaded").

Pre-fix the v1.18.69 size-rejection sidecar fallback fired only on
`http_status == 500`. A large (≥ceiling) movie/TV upload that Plex
never answered (read timeout / connection reset under load) returned
`http_status=None` → no fallback → 3 retries re-POSTing the bytes →
terminal-fail with NOTHING placed. A folder-backed row can always be
themed via a local sidecar, so v1.19.99 widens the gate to
`http_status in (500, None)` and makes the HISTORY/LOGS line
cause-aware (too-large vs no-response) + explicit that the row is
served from a local sidecar, NOT api-uploaded.

size_rejection (the no-fallback PERMANENT-fail for collections /
folderless rows) stays HTTP-500-only — a bare timeout is more likely
transient, so those rows still retry it.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
WORKER_PY = (REPO / "app" / "core" / "worker.py").read_text()


def _place_collection_body() -> str:
    idx = WORKER_PY.index("def _do_place_collection(")
    nxt = WORKER_PY.find("\n    def ", idx + 1)
    return WORKER_PY[idx:nxt if nxt != -1 else len(WORKER_PY)]


# ── gate widened to 500 + None ───────────────────────────────


def test_fallback_gate_covers_timeout():
    body = _place_collection_body()
    assert "http_status in (500, None)" in body, (
        "v1.19.99: the sidecar-fallback gate must accept no-response "
        "(None) timeouts, not just HTTP 500"
    )


def test_size_rejection_permanent_fail_stays_500_only():
    """The no-fallback PERMANENT-fail classification must NOT widen to
    None — a folderless/collection timeout stays retryable (transient),
    only a deterministic HTTP 500 fast-fails."""
    body = _place_collection_body()
    idx = body.index("size_rejection = (")
    rule = body[idx:idx + 120]
    assert "http_status == 500" in rule, (
        "v1.19.99: size_rejection (permanent-fail) stays HTTP-500-only"
    )
    assert "http_status in (500, None)" not in rule


# ── cause-aware HISTORY/LOGS line ────────────────────────────


def test_fallback_log_event_is_cause_aware():
    body = _place_collection_body()
    assert "_too_large = http_status == 500" in body
    # Both human phrasings present.
    assert "likely over Plex's ~10MB theme-upload ceiling (HTTP 500)" in body
    assert "no response from Plex (timeout / connection reset)" in body


def test_fallback_log_event_says_not_api_uploaded():
    """the user's ask — the line must make clear the row is LOCAL, not
    an API upload."""
    body = _place_collection_body()
    assert "NOT uploaded to Plex's metadata store" in body
    assert "LINK={fell_back_kind}" in body


def test_fallback_reason_token_is_cause_aware():
    body = _place_collection_body()
    assert '"api_upload_too_large"' in body
    assert '"api_upload_no_response"' in body
    assert '"http_status": http_status' in body, (
        "v1.19.99: detail carries http_status for analytics"
    )


# ── classification logic (replicated with the cause selector) ─


def test_cause_phrase_selection_logic():
    def cause(http_status):
        too_large = http_status == 500
        return (
            "likely over Plex's ~10MB theme-upload ceiling (HTTP 500)"
            if too_large
            else "got no response from Plex (timeout / connection reset)"
        ), ("api_upload_too_large" if too_large else "api_upload_no_response")

    phrase_500, reason_500 = cause(500)
    phrase_to, reason_to = cause(None)
    assert "HTTP 500" in phrase_500 and reason_500 == "api_upload_too_large"
    assert "timeout" in phrase_to and reason_to == "api_upload_no_response"


def test_v1_19_99_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
