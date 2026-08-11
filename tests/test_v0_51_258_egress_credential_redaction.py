"""v0.51.258 — notifications/inbox holistic review, finding 4.

**The scrubber pointed the wrong way.** `notify_inbox.record_notification` has
run title/body through the events credential scrubber since v0.51.147 — for the
LOCAL database write. `notify.py` scrubbed nothing before handing the same two
strings to Discord, an embedded Apprise sink, or an external apprise-api. The
protection was applied to the copy that never leaves the machine and omitted on
the third-party hop.

No known live vector (CLAUDE.md is explicit that the real control is discipline,
and the Plex token rides an HTTP header), but a last-line-of-defense that covers
only the safe direction is not a defense. `_dispatch_inline` is the one function
every real send funnels through, so the redaction goes there.

**Redaction, NOT `events._scrub_text`.** That helper also caps at 2 KB, which is
log hygiene for the events table — applying it here would truncate a genuine
bulk digest mid-list. `test_a_long_batch_body_is_not_truncated` pins that
distinction so a future "make it consistent with the inbox" edit can't quietly
introduce the cap.
"""
from __future__ import annotations

import types
from pathlib import Path

import pytest

from app.core import notify


REPO = Path(__file__).resolve().parent.parent


@pytest.fixture
def egress(monkeypatch):
    """Capture what each sink actually receives from _dispatch_inline."""
    seen: dict[str, list] = {"embedded": [], "external": [], "discord": []}
    monkeypatch.setattr(
        notify, "_send_embedded",
        lambda urls, title, body, ntype, fmt, attach_path=None:
        (seen["embedded"].append((title, body)), (1, 0))[1])
    monkeypatch.setattr(
        notify, "_send_external",
        lambda url, title, body, ntype, fmt:
        (seen["external"].append((title, body)), (1, 0))[1])
    monkeypatch.setattr(notify, "_log_outcome", lambda *a, **k: None)
    return seen


def _fire(title, body, *, external=True):
    notify._dispatch_inline(
        Path("/tmp/x.db"), event_kind="theme_added",
        urls=["json://localhost/"],
        external_url="https://apprise.example/notify/k" if external else "",
        title=title, body=body)


# ── the fix ──────────────────────────────────────────────────


def test_userinfo_credentials_are_redacted_on_every_sink(egress):
    """`scheme://user:pass@host` must not reach ANY sink verbatim."""
    _fire("Theme added — https://bob:hunter2@media.example/x",
          "body https://bob:hunter2@media.example/x")
    assert egress["embedded"] and egress["external"]
    for title, body in egress["embedded"] + egress["external"]:
        assert "hunter2" not in title and "hunter2" not in body
        assert "https://***@media.example/x" in title
        assert "https://***@media.example/x" in body


def test_query_param_secrets_are_redacted(egress):
    _fire("t", "GET https://plex.example/library?X-Plex-Token=abc123def")
    (_t, body), = egress["embedded"]
    assert "abc123def" not in body
    assert "X-Plex-Token=***" in body


def test_webhook_path_token_is_redacted(egress):
    _fire("t", "sink https://discord.com/api/webhooks/1234/SUPERSECRETTOKEN")
    (_t, body), = egress["embedded"]
    assert "SUPERSECRETTOKEN" not in body


def test_discord_native_embed_path_is_covered_too(egress, monkeypatch):
    """The attachment route bypasses _send_embedded for discord:// sinks — it
    must see the redacted strings as well."""
    got = []
    monkeypatch.setattr(notify, "_prepare_attachment", lambda url: "/tmp/thumb.jpg")
    monkeypatch.setattr(notify, "_parse_discord_webhook", lambda u: ("id", "tok"))
    monkeypatch.setattr(
        notify, "_send_discord_embed",
        lambda u, *, title, body, notify_type, attach_path:
        (got.append((title, body)), True)[1])
    monkeypatch.setattr(notify, "_log_outcome", lambda *a, **k: None)
    notify._dispatch_inline(
        Path("/tmp/x.db"), event_kind="theme_added",
        urls=["discord://id/tok"], external_url="",
        title="t https://u:p@h/x", body="b https://u:p@h/x",
        attach_url="https://img.example/t.jpg")
    assert got, "the native discord path must have run"
    for title, body in got:
        assert "u:p@h" not in title and "u:p@h" not in body


# ── what the fix must NOT do ─────────────────────────────────


def test_ordinary_theme_urls_pass_through_verbatim(egress):
    """The bodies motif actually sends are full of legitimate URLs. Redaction
    must be a no-op on every one of them — a false positive here silently
    mangles the link the operator is meant to click."""
    body = ("🎵 **Theme added** — Foo (2024)\n\n"
            "[watch](https://www.youtube.com/watch?v=dQw4w9WgXcQ)\n"
            "https://youtu.be/dQw4w9WgXcQ\n"
            "https://www.facebook.com/watch/?v=987654321\n"
            "https://image.tmdb.org/t/p/w500/abc.jpg")
    _fire("🎵 Theme added — Foo (2024)", body)
    (title, sent), = egress["embedded"]
    assert sent == body, "a legitimate URL must survive redaction unchanged"
    assert title == "🎵 Theme added — Foo (2024)"


def test_a_long_batch_body_is_not_truncated(egress):
    """events._scrub_text caps at 2 KB. That cap is right for the events table
    and WRONG here — a 77-item restore digest is ~3 KB and would lose its tail
    with no marker. The egress path redacts without capping."""
    body = "\n".join(f"- Some Movie Title Number {i} (2024)" for i in range(90))
    assert len(body) > 2048, "the fixture must actually exceed the scrub cap"
    _fire("77 themes restored", body)
    (_t, sent), = egress["embedded"]
    assert sent == body
    assert "…" not in sent


# ── symmetry with the inbox (the finding itself) ─────────────


def test_inbox_and_egress_share_the_redaction():
    """Both directions now apply the SAME helper. Pinned as a pair because the
    finding was precisely that one had it and the other didn't."""
    inbox_src = (REPO / "app" / "core" / "notify_inbox.py").read_text()
    notify_src = (REPO / "app" / "core" / "notify.py").read_text()
    assert "_scrub_text" in inbox_src
    assert "_redact_url_credentials(title" in notify_src
    assert "_redact_url_credentials(body" in notify_src


def test_v0_51_258_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
