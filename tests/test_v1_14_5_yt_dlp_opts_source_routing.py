"""v1.14.5 — yt-dlp opts routed by source (skip YT-only opts when SC).

Pre-fix _opts() unconditionally set:
  - js_runtimes: nodejs runtime startup for YT cipher signature
    derivation (1-2s wall clock per download).
  - remote_components: ['ejs:github'] — yt-dlp fetches the EJS
    helper bundle from GitHub at extraction time so node can solve
    YouTube's signature + n challenges.
  - extractor_args.youtube.player_client = ['default','android',
    'ios','mweb'] — YT-extractor-specific fallback chain.

None of those apply to SoundCloud. The EJS fetch + node startup
were costing ~1-3s on every SC download for zero benefit, and the
extractor_args.youtube block was confusing yt-dlp's extractor
selection on some 2025+ builds.

v1.14.5 introduces a `source` kwarg on _opts() and gates the
YT-only block on source=='youtube'. download_theme computes the
source from the URL via a local _source_for() helper (mirrors
sync.url_source — kept local so this module's import graph stays
narrow).

Tests pin the routing behavior directly by calling _opts() with
both sources and asserting the YT-only keys are present /
absent.
"""
from __future__ import annotations

from pathlib import Path

from app.core.downloader import _opts, _source_for


# ── _source_for helper ────────────────────────────────────────


def test_source_for_recognizes_youtube_watch_url():
    assert _source_for("https://www.youtube.com/watch?v=abc123XYZ_-") == "youtube"


def test_source_for_recognizes_youtube_short_url():
    assert _source_for("https://youtu.be/abc123XYZ_-") == "youtube"


def test_source_for_recognizes_soundcloud_url():
    assert _source_for("https://soundcloud.com/artist/track-slug") == "soundcloud"


def test_source_for_unknown_host_returns_unknown():
    assert _source_for("https://example.com/whatever") == "unknown"


def test_source_for_empty_url_returns_unknown():
    assert _source_for("") == "unknown"
    assert _source_for(None) == "unknown"  # type: ignore[arg-type]


# ── _opts default keeps YT opts (no behavior change for YT) ───


def test_opts_defaults_to_youtube_for_back_compat():
    """_opts() with no `source` kwarg must behave exactly like
    pre-v1.14.5 — defaults to source='youtube' so any direct
    caller (none today, but keeps the door open) doesn't lose
    the YT optimisations silently."""
    opts = _opts(output_path=Path("/tmp/theme"), cookies_file=None)
    assert "js_runtimes" in opts
    assert opts["js_runtimes"] == {"node": {}}
    assert "remote_components" in opts
    assert opts["remote_components"] == ["ejs:github"]
    assert "extractor_args" in opts
    assert "youtube" in opts["extractor_args"]


def test_opts_explicit_youtube_source_keeps_all_yt_opts():
    """source='youtube' is the explicit flavor of the default. All
    three YT-only opts must be present."""
    opts = _opts(
        output_path=Path("/tmp/theme"),
        cookies_file=None,
        source="youtube",
    )
    assert "js_runtimes" in opts
    assert "remote_components" in opts
    assert "extractor_args" in opts


# ── _opts with SC source skips YT-only opts ───────────────────


def test_opts_soundcloud_source_skips_js_runtimes():
    """js_runtimes loads node, which costs ~1-2s on every download
    even when nothing in the SC extraction needs JS."""
    opts = _opts(
        output_path=Path("/tmp/theme"),
        cookies_file=None,
        source="soundcloud",
    )
    assert "js_runtimes" not in opts


def test_opts_soundcloud_source_skips_remote_components():
    """remote_components: ['ejs:github'] triggers a remote fetch of
    the EJS YouTube signature solver. Pure waste on SC downloads."""
    opts = _opts(
        output_path=Path("/tmp/theme"),
        cookies_file=None,
        source="soundcloud",
    )
    assert "remote_components" not in opts


def test_opts_soundcloud_source_skips_extractor_args():
    """extractor_args.youtube.player_client is YouTube-extractor
    specific. Including it on a SC download is silently wrong —
    yt-dlp ignores it but it confuses the extractor-selection
    logic on some 2025+ builds (logged as 'unrecognized extractor
    argument', noise in the failure debug surface)."""
    opts = _opts(
        output_path=Path("/tmp/theme"),
        cookies_file=None,
        source="soundcloud",
    )
    assert "extractor_args" not in opts


def test_opts_soundcloud_source_keeps_universal_opts():
    """The non-YT-specific opts (format, output, postprocessor,
    retries, ratelimit) must still be present on SC downloads."""
    opts = _opts(
        output_path=Path("/tmp/theme"),
        cookies_file=None,
        source="soundcloud",
    )
    assert opts["format"] == "bestaudio/best"
    assert "outtmpl" in opts
    assert opts["postprocessors"][0]["key"] == "FFmpegExtractAudio"
    assert opts["retries"] == 2
    assert opts["ratelimit"] == 5_000_000
    assert opts["noplaylist"] is True


def test_opts_unknown_source_treated_as_non_youtube():
    """Defensive: an unknown source string (extractor we haven't
    catalogued yet) must NOT pick up the YT-only opts. Pre-fix
    every download got them; the safe default for an unknown
    source is the minimal opt set."""
    opts = _opts(
        output_path=Path("/tmp/theme"),
        cookies_file=None,
        source="unknown",
    )
    assert "js_runtimes" not in opts
    assert "remote_components" not in opts
    assert "extractor_args" not in opts


# ── Universal options still flow through on both sources ──────


def test_cookies_attached_regardless_of_source(tmp_path):
    """cookies.txt is the user's auth file — relevant to both YT
    (age-gated, premium-required) and SC (Go+ subscription
    tracks). Must attach for both sources.

    v1.22.77: the attached path is now a per-thread throwaway COPY
    (yt-dlp's close() truncate-rewrites its cookiefile; the shared
    jar raced under concurrent downloads) — assert content parity
    instead of path identity."""
    cookies = tmp_path / "cookies.txt"
    cookies.write_text("# netscape\n")
    for source in ("youtube", "soundcloud", "unknown"):
        opts = _opts(
            output_path=Path("/tmp/theme"),
            cookies_file=cookies,
            source=source,
        )
        attached = opts.get("cookiefile")
        assert attached, (
            f"source={source} dropped the cookiefile attachment"
        )
        assert attached != str(cookies), (
            "v1.22.77: must attach the throwaway copy, not the jar"
        )
        assert Path(attached).read_text() == cookies.read_text()


def test_geo_bypass_attached_regardless_of_source():
    """geo_bypass / geo_bypass_country / proxy_url are about routing
    around regional content blocks — apply to any source whose
    content might be region-locked. Must propagate for both."""
    for source in ("youtube", "soundcloud"):
        opts = _opts(
            output_path=Path("/tmp/theme"),
            cookies_file=None,
            geo_bypass=True,
            geo_bypass_country="US",
            proxy_url="socks5://127.0.0.1:1080",
            source=source,
        )
        assert opts.get("geo_bypass") is True
        assert opts.get("geo_bypass_country") == "US"
        assert opts.get("proxy") == "socks5://127.0.0.1:1080"


# ── download_theme wires source from URL ──────────────────────


def test_download_theme_passes_source_into_opts():
    """The download_theme function is the live call site for _opts.
    Pin via static-text guard that it threads _source_for(url)
    through to the source kwarg — without this, the SC download
    path silently gets the YT-only opts and we've regressed the
    whole point of v1.14.5."""
    src = (Path(__file__).resolve().parent.parent
           / "app" / "core" / "downloader.py").read_text()
    fn_anchor = src.index("def download_theme(")
    body = src[fn_anchor:fn_anchor + 5000]
    assert "source=_source_for(youtube_url)" in body


def test_opts_youtube_only_block_is_gated_in_source():
    """Static-text guard: the YT-only opts must live inside an
    `if source == \"youtube\":` block. Pre-fix they were unconditional
    keys in the dict literal — the v1.14.5 refactor moved them
    behind the gate. A regression that flattens the dict back
    would re-add the SC perf cost without any test catching it
    directly (the SC opts test would still pass on a YT call)."""
    src = (Path(__file__).resolve().parent.parent
           / "app" / "core" / "downloader.py").read_text()
    fn_anchor = src.index("def _opts(")
    body = src[fn_anchor:fn_anchor + 6000]
    assert 'if source == "youtube":' in body
    # The three opt assignments must live under that branch — i.e.
    # appear AFTER the gate, not as keys in the initial dict literal.
    gate_pos = body.index('if source == "youtube":')
    # Find the first opts = { … } literal end ('}'+ blank-ish lines).
    # Cheaper proxy: the YT-only assignments use opts[...] = … syntax
    # (post-init mutation). Verify those specific lines exist after
    # the gate.
    after_gate = body[gate_pos:]
    assert 'opts["js_runtimes"] = {"node": {}}' in after_gate
    assert 'opts["remote_components"] = ["ejs:github"]' in after_gate
    assert 'opts["extractor_args"] = {' in after_gate
