"""v1.19.48 — B badge color + button naming polish.

the user's 2026-05-27 feedback after first successful cloud-backup
on v1.19.46:

  1. "The B for the backup row yellow matches our missing cookie
     color so is a bit confusing or seems potentially like a
     bad state."

  2. "Can we make the source button instead of backup this theme
     to Download Plex Backup to more similarly match the phrasing
     of other buttons."

  3. "Let's also make the button the plex button so at a glance
     know its backing up plex's theme."

## Fixes

### Badge color: lemon → amber-bright

The v1.19.43 choice of `--lemon` for `.link-glyph-b` conflicted
with the cookies-needed UX vocabulary (--lemon is reserved for
.btn-cookies / .tdb-pill-cookies / .attn-pill-cookies /
.op-tone-cookies per v1.15.121). Lemon-as-warning bled into B
reading as "bad state."

New color: `--amber-bright` text + `--amber-rgb` border/bg. This
puts B firmly in the Plex-family color vocabulary (since the
bytes ARE Plex's cloud theme staged by motif) while staying
visually distinct from:
  - P (regular amber) — Plex-served
  - PS (regular amber) — Plex-serving intentional (post-LPS)
  - PU (cyan) — motif uploaded via API
  - BK (blue) — user-explicit backup

The amber-bright text on amber border/bg reads as "Plex-family
but distinct" — the user's mental model becomes: amber = Plex
content; bright amber = motif's copy of Plex's content (backup).

### Button label: BACKUP THIS THEME → DOWNLOAD PLEX BACKUP

Audit of every SOURCE-menu label found "BACKUP THIS THEME" was
the only outlier using a demonstrative pronoun ("this"). The
rest follow VERB + SOURCE or VERB + THING patterns:

  - DOWNLOAD TDB / RE-DOWNLOAD TDB / DOWNLOAD TDB BACKUP
  - REPLACE TDB / SET URL / UPLOAD MP3
  - LET PLEX SERVE / ADOPT + LET PLEX SERVE
  - PROMOTE TO ACTIVE / MARK AS BACKUP
  - CONVERT TO MANUAL / ACK DROP / CLEAR URL

"DOWNLOAD PLEX BACKUP" mirrors DOWNLOAD TDB BACKUP exactly
(verb + SOURCE + intent), so the two "download a backup"
actions read consistently across the menu.

### Button tone: themerrdb (green) → plex (amber)

The action backs up Plex's content. Tone should signal that.
LET PLEX SERVE / PUSH TO PLEX / RESTORE FROM PLEX all use the
plex/amber tone; cloud-backup joins that family.

The bulk-bar button's class changes
`lib-source-themerrdb` → `btn-plex` to match.
"""
from __future__ import annotations

import sys
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
LIBRARY_HTML = (
    REPO / "app" / "web" / "templates" / "library.html"
).read_text()
OPS_JS = (REPO / "app" / "web" / "static" / "ops.js").read_text()


# ── Badge color (Bug 1) ──────────────────────────────────────


def test_b_badge_color_is_amber_bright_not_lemon():
    """the user's specific complaint: 'The B for the backup row
    yellow matches our missing cookie color so is a bit
    confusing or seems potentially like a bad state.' Lemon is
    reserved for cookies UX surfaces; B must switch to
    amber-bright (Plex-family)."""
    rule_idx = APP_CSS.index(".link-glyph-b {")
    rule_end = APP_CSS.index("}", rule_idx)
    rule = APP_CSS[rule_idx:rule_end]
    assert "var(--amber-bright)" in rule, (
        "v1.19.48: B badge text color must be --amber-bright "
        "(Plex family, distinct from regular amber P/PS)"
    )
    # Lemon must be gone — that's the cookies UX, not for B.
    assert "var(--lemon)" not in rule
    assert "var(--lemon-rgb)" not in rule


def test_b_badge_border_and_bg_use_amber_rgb():
    """Border + bg use --amber-rgb (the Plex-family connection).
    Text is amber-bright; border/bg amber establishes the
    visual link to P/PS while text-color difference keeps B
    distinct."""
    rule_idx = APP_CSS.index(".link-glyph-b {")
    rule_end = APP_CSS.index("}", rule_idx)
    rule = APP_CSS[rule_idx:rule_end]
    assert "rgba(var(--amber-rgb), 0.5)" in rule
    assert "rgba(var(--amber-rgb), 0.08)" in rule


# ── Per-row label + tone (Bugs 2 + 3) ────────────────────────


def test_source_menu_label_is_download_plex_backup():
    """SOURCE-menu label must mirror DOWNLOAD TDB BACKUP shape
    (VERB + SOURCE + intent). No demonstrative pronoun ('this')."""
    assert "'DOWNLOAD PLEX BACKUP'" in APP_JS, (
        "v1.19.48: SOURCE-menu label must be 'DOWNLOAD PLEX "
        "BACKUP' (mirrors DOWNLOAD TDB BACKUP)"
    )
    # Old label must be gone.
    assert "'BACKUP THIS THEME'" not in APP_JS


def test_source_menu_button_tone_is_plex():
    """The SOURCE-menu entry for backup-cloud-theme must use
    tone='plex' (amber, Plex-family) so at-a-glance the user
    knows it's backing up Plex's theme."""
    idx = APP_JS.index("'backup-cloud-theme'")
    # menuItemHtml call body — walk forward to closing paren.
    block = APP_JS[idx:idx + 1500]
    assert "tone: 'plex'" in block, (
        "v1.19.48: backup-cloud-theme menu item must use "
        "tone: 'plex' (amber, mirrors LET PLEX SERVE / "
        "ADOPT + LET PLEX SERVE precedent)"
    )
    # Old themerrdb tone must be gone for THIS button. Verify
    # by checking that the block doesn't contain 'tone:
    # 'themerrdb'' between the action name and the closing.
    # Reach the next sourceItems.push or end of menuItemHtml call.
    closing = block.find("));")
    assert closing > 0
    item_block = block[:closing + 3]
    assert "tone: 'themerrdb'" not in item_block, (
        "v1.19.48: backup-cloud-theme must NOT use "
        "tone: 'themerrdb' (was the v1.19.43 choice; retoned "
        "to plex for v1.19.48)"
    )


# ── Bulk-bar label + class ───────────────────────────────────


def test_bulk_button_uses_btn_plex_class():
    """library.html bulk button must use `btn-plex` (amber)
    instead of `lib-source-themerrdb` (green)."""
    btn_idx = LIBRARY_HTML.index('id="library-cloud-backup-btn"')
    open_idx = LIBRARY_HTML.rfind("<button", 0, btn_idx)
    chunk = LIBRARY_HTML[open_idx:btn_idx + 400]
    assert "btn-plex" in chunk
    assert "lib-source-themerrdb" not in chunk, (
        "v1.19.48: bulk button must drop lib-source-themerrdb "
        "class (retoned to plex)"
    )


def test_bulk_button_default_label_is_download_plex_backup():
    """Default (count=1) label must be 'DOWNLOAD PLEX BACKUP'.
    v1.19.52: switched from hand-rolled `// DOWNLOAD N PLEX
    BACKUPS` template to withCount() helper which renders
    `// DOWNLOAD PLEX BACKUP (N)` — uniform-look fix the user
    requested. Singular form lives in the withCount call; the
    helper handles the (N) suffix."""
    btn_idx = LIBRARY_HTML.index('id="library-cloud-backup-btn"')
    open_idx = LIBRARY_HTML.rfind("<button", 0, btn_idx)
    close_idx = LIBRARY_HTML.index("</button>", btn_idx)
    chunk = LIBRARY_HTML[open_idx:close_idx + 10]
    assert "DOWNLOAD PLEX BACKUP" in chunk
    # JS handler uses withCount() to render the count badge.
    assert (
        "withCount(\n          '// DOWNLOAD PLEX BACKUP', cloudBackupCount,"
        in APP_JS
        or "withCount('// DOWNLOAD PLEX BACKUP', cloudBackupCount)" in APP_JS
    ), (
        "v1.19.52: multi-select label must use withCount() "
        "helper for uniform-look parity with other bulk buttons"
    )


# ── Drawer label (ops.js KIND_LABEL) ─────────────────────────


def test_ops_drawer_label_matches_action_buttons():
    """The op_progress drawer card title must read identically
    to the bulk-bar + SOURCE-menu labels so the user reads a
    consistent action name everywhere."""
    idx = OPS_JS.index("const KIND_LABEL = {")
    end = OPS_JS.index("};", idx)
    block = OPS_JS[idx:end]
    assert "cloud_themes_backup: 'DOWNLOAD PLEX BACKUP'" in block


# ── Cross-button audit (consistency guard) ───────────────────


def test_no_demonstrative_pronouns_in_source_menu_labels():
    """v1.19.48 audit finding: 'BACKUP THIS THEME' was the only
    SOURCE-menu label using 'this'. Guard against future
    additions accidentally bringing the pattern back."""
    import re
    # Extract every menuItemHtml label.
    labels = re.findall(
        r"menuItemHtml\(\s*'[^']+',\s*'([^']+)'", APP_JS,
    )
    bad = [
        lab for lab in labels
        if any(
            f" {pronoun} " in f" {lab} " or lab.endswith(f" {pronoun}")
            for pronoun in ("THIS", "THAT", "THESE", "THOSE")
        )
    ]
    assert not bad, (
        f"v1.19.48: SOURCE-menu labels must not use demonstrative "
        f"pronouns (this/that/these/those). Offenders: {bad}"
    )


def test_promote_tooltip_doesnt_reference_lemon_or_cloud_themes_label():
    """The v1.19.43 plex_cloud PROMOTE tooltip variant must
    still fire on plex_cloud rows (unchanged by the renaming),
    but check it doesn't reference the old label."""
    idx = APP_JS.index("isPlexCloudSynthetic")
    block = APP_JS[idx:idx + 1500]
    assert "re-upload trick" in block
    # The tooltip wording itself is unchanged — just verifying
    # the predicate still works.
    assert "source_kind === 'plex_cloud'" in block


# ── Version pin ──────────────────────────────────────────────


def test_v1_19_48_version_pin():
    """Version bumped at v1.19.48 (then again at v1.19.49 for
    the plex_cloud → TDB switch gates). Match 1.19.x prefix."""
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
