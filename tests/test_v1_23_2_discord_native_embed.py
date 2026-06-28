"""v1.23.2 — native Discord embed card for thumbnail notifications.

the user's v1.23.1 side-by-side: the FB attachment matched the YouTube
preview's image size exactly, but the YT image sits INSIDE Discord's
embed card (indent + border + header) while the attachment sat bare.
Apprise structurally can't produce the card look — its attachment
POST strips the embed, so the image always dangles below the card
(verified against the installed NotifyDiscord source).

Fix: for Discord sinks, a notification carrying a thumbnail is
sent NATIVELY — motif builds the webhook payload itself with
embeds[0].image = attachment://<file> + a multipart upload, so the
title, source line, and image all render inside one colored card.
Non-discord sinks and attachment-less notifications keep the apprise
path untouched.
"""
from __future__ import annotations

import json
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── webhook URL parsing ──────────────────────────────────────


def test_parse_discord_webhook_forms():
    from app.core.notify import _parse_discord_webhook
    assert _parse_discord_webhook(
        "discord://1234567890/abcDEF-token_123") == (
        "1234567890", "abcDEF-token_123")
    # apprise's optional botname@ prefix + query params.
    assert _parse_discord_webhook(
        "discord://motif@1234567890/tok?avatar=no") == (
        "1234567890", "tok")
    # v1.23.3: the raw webhook form apprise ALSO accepts natively —
    # the user's config carries this shape, and the v1.23.2
    # scheme-only check silently fell back to apprise (no embed
    # card, no breadcrumb).
    assert _parse_discord_webhook(
        "https://discord.com/api/webhooks/1234567890/tok-X_1"
    ) == ("1234567890", "tok-X_1")
    assert _parse_discord_webhook(
        "https://discordapp.com/api/webhooks/99/t?wait=true"
    ) == ("99", "t")
    # non-discord sinks + unrecognized shapes → None (apprise path).
    assert _parse_discord_webhook("tgram://bottoken/chatid") is None
    assert _parse_discord_webhook("discord://notdigits/tok") is None
    assert _parse_discord_webhook("discord://1234567890/") is None
    # lookalike host stays on the apprise path (no token exfil risk).
    assert _parse_discord_webhook(
        "https://discord.com.evil.com/api/webhooks/1/t") is None
    assert _parse_discord_webhook(
        "https://example.com/api/webhooks/1/t") is None


# ── native send payload shape ────────────────────────────────


def test_send_discord_embed_payload(monkeypatch, tmp_path):
    import httpx
    captured = {}

    class _FakeResp:
        status_code = 204
        text = ""

    class _FakeClient:
        def __init__(self, **kw): ...
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def post(self, url, data=None, files=None):
            captured["url"] = url
            captured["payload"] = json.loads(data["payload_json"])
            captured["files"] = files
            return _FakeResp()

    monkeypatch.setattr(httpx, "Client", _FakeClient)
    thumb = tmp_path / "thumb.jpg"
    thumb.write_bytes(b"\xff\xd8jpegbytes")
    from app.core.notify import _send_discord_embed
    ok = _send_discord_embed(
        "discord://1234567890/tok",
        title="\U0001f3b5 Theme added — Castle Impossible (2025)",
        body="Source: User URL · Facebook\n<https://fb.example/v/1>",
        notify_type="info", attach_path=str(thumb))
    assert ok
    assert captured["url"] == (
        "https://discord.com/api/webhooks/1234567890/tok")
    # v1.23.4: text rides as plain CONTENT above the card (the
    # YouTube-notification anatomy); the embed is image-only.
    content = captured["payload"]["content"]
    assert content.splitlines()[0].endswith("Castle Impossible (2025)")
    assert "Facebook" in content
    emb = captured["payload"]["embeds"][0]
    assert "title" not in emb and "description" not in emb
    # the image rides INSIDE the embed via the attachment:// ref,
    # matching the uploaded multipart filename exactly.
    assert emb["image"]["url"] == "attachment://thumb.jpg"
    assert "files[0]" in captured["files"]
    assert captured["files"]["files[0]"][0] == "thumb.jpg"
    # v1.23.5: Facebook brand blue — source identity, like the red
    # bar on YouTube's own preview cards.
    assert emb["color"] == 0x1877F2


def test_send_discord_embed_failure_is_false_not_raise(monkeypatch,
                                                       tmp_path):
    import httpx

    class _FakeResp:
        status_code = 401
        text = "Invalid Webhook Token"

    class _FakeClient:
        def __init__(self, **kw): ...
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def post(self, *a, **kw): return _FakeResp()

    monkeypatch.setattr(httpx, "Client", _FakeClient)
    thumb = tmp_path / "t.jpg"
    thumb.write_bytes(b"x")
    from app.core.notify import _send_discord_embed
    assert _send_discord_embed(
        "discord://1/tok", title="t", body="b",
        notify_type="info", attach_path=str(thumb)) is False


# ── dispatch partition ───────────────────────────────────────


def test_dispatch_inline_partitions_discord_when_attached(
        monkeypatch, tmp_path):
    """With a thumbnail: discord sinks → native embed; the rest →
    apprise WITH the attachment. Without: everything → apprise."""
    from app.core import notify as n
    thumb = tmp_path / "t.jpg"
    thumb.write_bytes(b"x")
    calls = {"native": [], "apprise": []}
    monkeypatch.setattr(n, "_prepare_attachment",
                        lambda url: str(thumb))
    monkeypatch.setattr(
        n, "_send_discord_embed",
        lambda u, **kw: calls["native"].append(u) or True)
    monkeypatch.setattr(
        n, "_send_embedded",
        lambda urls, *a, **kw: calls["apprise"].append(list(urls))
        or (len(urls), 0))
    monkeypatch.setattr(n, "_log_outcome", lambda *a, **kw: None)

    n._dispatch_inline(
        tmp_path / "db", event_kind="theme_added",
        urls=["discord://123/tok", "tgram://bt/cid"],
        external_url="", title="t", body="b",
        notify_type="info", body_format="markdown",
        attach_url="https://scontent.xx.fbcdn.net/x.jpg")
    assert calls["native"] == ["discord://123/tok"]
    assert calls["apprise"] == [["tgram://bt/cid"]]

    calls["native"].clear()
    calls["apprise"].clear()
    n._dispatch_inline(
        tmp_path / "db", event_kind="theme_added",
        urls=["discord://123/tok", "tgram://bt/cid"],
        external_url="", title="t", body="b",
        notify_type="info", body_format="markdown")
    assert calls["native"] == [], (
        "attachment-less notifications keep the apprise path — the "
        "YouTube preview cards depend on plain-content unfurls"
    )
    assert calls["apprise"] == [["discord://123/tok",
                                 "tgram://bt/cid"]]
