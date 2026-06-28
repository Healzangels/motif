"""v1.22.11 — user-uploaded MP3 INFO cards stop posing as URL-sourced.

the user's repro (LotR "Sam Takes a Step" edition, green TDB pill, playing a
user-uploaded MP3): the // MOTIF INFO card showed
  - applied url (youtube): <TDB URL> themerrdb
  - video id: <TDB video id>
  - a YouTube thumbnail + "▸ click to watch on YouTube" link
  - revert: "REVERT would just re-apply the current URL (no-op)"
…all implying the theme came from that YouTube URL. It didn't — the theme is a
user-uploaded FILE.

Root cause: `isUrlSourced` grouped 'upload' with 'themerrdb'/'url', so when no
override existed `currentUrl` fell back to `t.youtube_url` (the TDB URL).
`currentUrl` then drove the applied-url row, the video-id row, the thumbnail +
watch link (ytUrl/ytId derive from it), and the revert no-op comparison.

Fix: 'upload' is file-sourced like M/A — `currentUrl` stays '' (applied-url '—',
no thumbnail/watch link, video-id '—'), and an upload-specific revert hint
replaces the URL-centric no-op wording. The "themerrdb url" row + the
"downloaded … source_video_id=" provenance row still surface what TDB offers and
where the uploaded bytes came from.

These are app.js source pins — the whole surface (currentUrl derivation, the
v1.14.20/v1.14.18 sibling guards) is tested this way; the card is computed
client-side from /api/items data.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def _isurlsourced_block() -> str:
    i = APP_JS.index("const isUrlSourced =")
    return APP_JS[i:i + 200]


def test_upload_excluded_from_is_url_sourced():
    """The single source of truth for "show an applied URL": 'upload' must NOT
    be in it. Pre-fix it was, so upload rows fell back to the TDB URL."""
    block = _isurlsourced_block()
    assert "lfSource === 'themerrdb'" in block
    assert "lfSource === 'url'" in block
    assert "lfSource === 'upload'" not in block, (
        "v1.22.11: 'upload' is file-sourced — it must NOT be in isUrlSourced, "
        "else currentUrl falls back to t.youtube_url and the card poses the "
        "uploaded MP3 as a YouTube-sourced theme"
    )


def test_v1_22_11_marker_explains_why():
    """A v1.22.11 marker must guard the isUrlSourced line so a future edit
    doesn't 'helpfully' re-add 'upload' (it reads symmetric with themerrdb/url
    at a glance)."""
    i = APP_JS.index("const isUrlSourced =")
    preceding = APP_JS[max(0, i - 900):i]
    assert "v1.22.11" in preceding
    assert "upload" in preceding and "file" in preceding.lower()


def test_currentUrl_only_url_sources_fall_back_to_tdb():
    """currentUrl must keep the isUrlSourced gate on the t.youtube_url
    fallback — that's what keeps upload/M/A/P rows from borrowing the TDB URL
    as 'applied'."""
    i = APP_JS.index("const currentUrl = isUrlSourced")
    block = APP_JS[i:i + 160]
    assert "ovr?.youtube_url || t.youtube_url" in block, (
        "v1.22.11: the TDB-url fallback must stay gated behind isUrlSourced"
    )


def test_filesourced_revert_branch_present_and_first():
    """v1.22.14: the revert branch must fire BEFORE the URL-centric branches and
    gate on isFileSourced (upload/adopt/plex_cloud) — NOT the literal 'upload' —
    so adopt/plex_cloud rows stop hitting 'REVERT would re-apply the current URL'
    too. It must carry the live-revert guard (suppress the file hint when a
    user-kind previous URL genuinely differs from applied, so the card doesn't
    contradict a working REVERT in the SOURCE menu)."""
    # isFileSourced gate covers the three motif-owned non-URL kinds.
    idx = APP_JS.index("const isFileSourced =")
    region = APP_JS[max(0, idx - 480):idx + 220]
    assert "v1.22.14" in region
    for kind in ("'upload'", "'adopt'", "'plex_cloud'"):
        assert f"lfSource === {kind}" in region, (
            f"v1.22.14: isFileSourced must include {kind}"
        )

    anchor = "let revertHint = '';"
    i = APP_JS.index(anchor)
    chain = APP_JS[i:i + 2400]
    branch_pos = chain.index("if (isFileSourced)")
    accepted_pos = chain.index("!previousUrl && pu && pu.decision === 'accepted'")
    noop_pos = chain.index("previousUrl === currentCanonical")
    assert branch_pos < accepted_pos < noop_pos, (
        "v1.22.14: the file-sourced revert branch must precede the URL-centric "
        "revert branches"
    )
    branch = chain[branch_pos:noop_pos]
    # Live-revert guard so the card doesn't contradict a working REVERT.
    assert "liveRevert" in branch
    assert "previousKind === 'user'" in branch
    # Per-kind wording + still points at UPLOAD MP3. (Substrings chosen
    # contiguous in source — the hint string is split across `+` concat lines.)
    assert "user-uploaded MP3" in branch
    assert "adopted sidecar" in branch
    assert "UPLOAD MP3" in branch


def test_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
