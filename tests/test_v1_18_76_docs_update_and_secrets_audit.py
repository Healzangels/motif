"""v1.18.76 — documentation update + forward-looking secrets-leak guard.

the user's ask: "lets update all documentation, also lets make sure
we're not exposing any sensitive documentation or information that
got into the docs."

## Documentation updates

1. CLAUDE.md gains the v1.18.0 placement-kind alignment catalogue —
   four JS sites that must agree on the `placed = !!it.media_folder
   || it.placement_kind === 'plex_upload'` predicate. v1.18.75
   surfaced isPlexAgentRow as a fourth site the existing
   three-site catalogue was missing.

2. PROJECT_HISTORY.md gains § 21 covering v1.18.62 → v1.18.75
   (14 tags) — the pending TDB URL surfacing arc, destructive-
   before-confirm atomic teardowns, Plex upload size handling,
   audit rollovers, and UX polish.

## Sensitive-content audit

Manually walked every tracked doc (CLAUDE.md, README.md, docs/
PROJECT_HISTORY.md, docs/DESIGN_SYSTEM.md, docs/DIAGNOSTICS.md)
for:
  - Real IP addresses (the operator's homelab IP)
  - Plex tokens (32-char hex)
  - GitHub PATs (ghp_*, gho_*, ghs_*, ghu_*)
  - Discord webhook IDs
  - Personal home paths (the operator's home dir)
  - Personal email addresses

Zero hits. Only public references survive: `healzangels/motif`
Docker Hub repo (intentional — README documents the image),
`192.168.1.10` placeholder IPs in install examples, and the
`thmr_...` API token format documentation.

This test stays as a FORWARD-LOOKING guard: future doc updates
that accidentally include sensitive patterns trip the test before
the commit lands in a publishable file.
"""
from __future__ import annotations

import re
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
# v0.50.0: the operator's home-dir prefix, computed at runtime (was a hardcoded
# literal) so the leak-guard scans for the real home path without naming anyone.
_HOME = str(Path.home()) + "/"


def _personal_strings():
    """Private strings the leak-guard scans for (homelab IP, hostname, etc.),
    read from a gitignored local file (tests/personal_strings.local) so the repo
    itself names none of them. Returns [] when the file is absent (fresh clone /
    CI) — there's nothing machine-specific to scan for, so the guard is a no-op
    there but still protects the operator's working checkout."""
    p = REPO / "tests" / "personal_strings.local"
    if not p.exists():
        return []
    return [
        ln.strip() for ln in p.read_text().splitlines()
        if ln.strip() and not ln.strip().startswith("#")
    ]


# Tracked docs that ship publicly. Gitignored files (SESSION_JOURNAL,
# AUDIT_*.md, CODEBASE_AUDIT.md, INFO_CARD_AUDIT.md) are local-only
# and DELIBERATELY excluded — they may contain debugging notes,
# server IPs, etc. that aren't appropriate for a public repo but
# fine on the user's machine.
TRACKED_DOCS = [
    REPO / "CLAUDE.md",
    REPO / "README.md",
    REPO / "docs" / "PROJECT_HISTORY.md",
    REPO / "docs" / "DESIGN_SYSTEM.md",
    REPO / "docs" / "DIAGNOSTICS.md",
]


def _tracked_source_files() -> list[Path]:
    """v1.19.56: widen the scan to every tracked source file
    that ships in the public repo. The v1.18.76 audit covered
    Markdown docs only; this catches sensitive strings that
    might land in code comments, JS hardcoded URLs, HTML
    placeholders, etc.

    Walks the repo for .py / .js / .html / .css / .yaml /
    .yml / .toml / .txt files, skipping the .git directory,
    __pycache__, venv/.venv, node_modules, and the .gitignored
    docs (SESSION_JOURNAL.md, AUDIT_*.md, etc.) so we only
    audit what's actually committed."""
    SKIP_DIRS = {
        ".git", "__pycache__", "venv", ".venv", "node_modules",
        ".pytest_cache", ".tox", "dist", "build",
    }
    SKIP_FILES = {
        "SESSION_JOURNAL.md",
        "CODEBASE_AUDIT.md",
        "INFO_CARD_AUDIT.md",
    }
    EXTENSIONS = {
        ".py", ".js", ".html", ".css", ".md", ".yaml", ".yml",
        ".toml", ".txt",
    }
    out: list[Path] = []
    for p in REPO.rglob("*"):
        if not p.is_file():
            continue
        if any(part in SKIP_DIRS for part in p.parts):
            continue
        if p.name in SKIP_FILES:
            continue
        if p.name.startswith("AUDIT_"):
            continue
        if p.suffix not in EXTENSIONS:
            continue
        # Exclude this very file (it documents the strings
        # it's scanning for; legit self-reference).
        # v1.19.56: also exclude the v1.19.56 docs-catchup test
        # which references the same sensitive-pattern strings
        # in its docstring + assertions as documentation.
        if p.name in {
            "test_v1_18_76_docs_update_and_secrets_audit.py",
            "test_v1_19_56_docs_catchup.py",
        }:
            continue
        out.append(p)
    return out


# ── CLAUDE.md: 4-site catalogue documented ──────────────────


def test_claude_md_catalogues_four_placement_kind_sites():
    """The v1.18.0 placement-kind alignment section must list
    every JS site that uses the canonical `placed` predicate.
    v1.18.75 added isPlexAgentRow as a 4th site after the
    pre-existing 3-site catalogue missed it."""
    claude = (REPO / "CLAUDE.md").read_text()
    # v1.19.40 doc update renamed "Bulk-bar selection bucket" to
    # "`updateLibrarySelectionUi` selection bucket" (matches the
    # actual function name; the prior name pointed at a function
    # that no longer existed under that name). Match on
    # "selection bucket" which survives both forms.
    for site in (
        "computeSrcLetter",
        "renderLibraryRow",
        "selection bucket",
        "isPlexAgentRow",
    ):
        assert site in claude, (
            f"v1.18.76: CLAUDE.md placement-kind catalogue must "
            f"reference the {site!r} site so future authors who "
            f"touch the predicate find the alignment list"
        )


def test_claude_md_documents_canonical_placed_predicate():
    """The exact `placed` predicate shape must be inline in
    CLAUDE.md so future authors copying the predicate to a new
    site land on the canonical form."""
    claude = (REPO / "CLAUDE.md").read_text()
    assert (
        "!!it.media_folder || it.placement_kind === 'plex_upload'"
        in claude
    ), (
        "v1.18.76: canonical predicate shape must appear verbatim "
        "in CLAUDE.md for copy-paste discoverability"
    )


def test_claude_md_references_mirror_drift_guard_template():
    """The catalogue must point at the v1.18.75 test as the
    mirror-drift guard template. Future authors adding a 5th
    site should follow the same pattern."""
    claude = (REPO / "CLAUDE.md").read_text()
    assert "test_compute_src_letter_placed_logic_unchanged" in claude


# ── PROJECT_HISTORY.md: § 21 covers v1.18.62 → v1.18.75 ──────


def test_project_history_section_21_exists():
    """§ 21 must catalogue the v1.18.62 → v1.18.75 arc. Without
    it the public release history skips 14 tags after v1.18.61."""
    history = (REPO / "docs" / "PROJECT_HISTORY.md").read_text()
    assert "## 21." in history, (
        "v1.18.76: PROJECT_HISTORY § 21 required to cover the "
        "v1.18.62 → v1.18.75 tag arc"
    )


def test_project_history_section_21_covers_every_tag():
    """Each of v1.18.62 → v1.18.75 must be named in § 21. Missing
    tags leave gaps in the historical record."""
    history = (REPO / "docs" / "PROJECT_HISTORY.md").read_text()
    sec_idx = history.index("## 21.")
    # Walk to end of file (no § 22 yet).
    section = history[sec_idx:]
    for minor in range(62, 76):
        tag = f"v1.18.{minor}"
        assert tag in section, (
            f"v1.18.76: § 21 must reference {tag} — every shipped "
            f"tag in the arc gets a line item"
        )


def test_project_history_section_21_names_the_four_arcs():
    """The digest must group the tags into the five thematic arcs
    (pending TDB URL surfacing / destructive-before-confirm /
    Plex upload size / audit rollovers / UX polish) so a reader
    can scan the structure without parsing every tag."""
    history = (REPO / "docs" / "PROJECT_HISTORY.md").read_text()
    sec_idx = history.index("## 21.")
    section = history[sec_idx:]
    for arc in (
        "pending TDB URL surfacing",
        "destructive-before-confirm",
        "Plex upload size",
        "audit rollovers",
    ):
        # Case-insensitive substring check for the arc theme.
        assert arc.lower() in section.lower(), (
            f"v1.18.76: § 21 must group tags under the {arc!r} arc"
        )


# ── Forward-looking secrets-leak guard ──────────────────────


def test_no_homelab_ip_in_tracked_docs():
    """The operator's homelab IP (e.g. from docker logs / screenshots) must
    NOT leak into tracked docs (CLAUDE.md, README.md, docs/*.md). The needle is
    read from the gitignored personal-strings file so it's named nowhere public.
    The .gitignored SESSION_JOURNAL / AUDIT_*.md are exempt (local-only notes)."""
    needles = [s for s in _personal_strings() if s.replace(".", "").isdigit()]
    for path in TRACKED_DOCS:
        text = path.read_text()
        for ip in needles:
            assert ip not in text, (
                f"personal IP must not appear in {path.name} — move it to "
                f"the .gitignored SESSION_JOURNAL/AUDIT_*.md."
            )


def test_no_personal_paths_in_tracked_docs():
    """The operator's home-dir path shouldn't leak — it betrays the
    username + filesystem layout."""
    for path in TRACKED_DOCS:
        text = path.read_text()
        assert _HOME not in text, (
            f"v1.18.76: personal home path `{_HOME}` must not "
            f"appear in {path.name}"
        )


def test_no_github_pats_in_tracked_docs():
    """GitHub personal access tokens use the prefix `ghp_` /
    `gho_` / `ghs_` / `ghu_`. Check no such token slipped into
    a tracked doc (e.g. a copy-paste from a debug session)."""
    pat_re = re.compile(r"\bgh[posu]_[A-Za-z0-9]{36,}")
    for path in TRACKED_DOCS:
        text = path.read_text()
        matches = pat_re.findall(text)
        assert not matches, (
            f"v1.18.76: GitHub PAT pattern found in {path.name}: "
            f"{matches!r}"
        )


def test_no_discord_webhook_urls_in_tracked_docs():
    """Discord webhook URLs reveal a private channel + auth in
    one string. A leaked webhook lets anyone post as the bot."""
    webhook_re = re.compile(
        r"discord(?:app)?\.com/api/webhooks/\d+/[\w-]{40,}"
    )
    for path in TRACKED_DOCS:
        text = path.read_text()
        matches = webhook_re.findall(text)
        assert not matches, (
            f"v1.18.76: Discord webhook URL found in {path.name}: "
            f"{matches!r}"
        )


def test_no_plex_token_pattern_in_tracked_docs():
    """Plex tokens are 20-char alphanumeric. A naive scan
    catches: `X-Plex-Token=...` headers, `token=...` query
    strings, and bare 20-char hex strings adjacent to plex
    keywords. Lower confidence than the GitHub-PAT match (false
    positives on legitimate 20-char IDs) so we narrow to
    plex-context-adjacent occurrences."""
    plex_token_re = re.compile(
        r"(?:X-Plex-Token|plex.{0,5}token).{0,10}[=:][\s\"'`]*"
        r"([A-Za-z0-9_-]{18,32})",
        re.IGNORECASE,
    )
    for path in TRACKED_DOCS:
        text = path.read_text()
        matches = plex_token_re.findall(text)
        # Allow documented placeholder values that match the
        # pattern but are obviously not real tokens.
        real = [
            m for m in matches
            if m.lower() not in {
                "your-plex-token-here",
                "your_plex_token",
                "yourplextoken",
                "abc123",
                "xxxxxxxxxxxxxxxxxxxx",
                "yyyyyyyyyyyyyyyyyyyy",
                "kdz_xxxxxxxxxxxxx",  # docs placeholder
            }
        ]
        assert not real, (
            f"v1.18.76: probable Plex token pattern found in "
            f"{path.name}: {real!r}"
        )


def test_no_personal_email_in_tracked_docs():
    """The operator's personal email must not appear in tracked
    docs. The commit-trailer `Co-Authored-By: ... <noreply@
    anthropic.com>` is fine (a noreply alias)."""
    # Pattern: any @gmail / @outlook / @yahoo / @icloud / etc.
    # Whitelist the noreply addresses motif intentionally uses.
    WHITELIST = {
        "noreply@anthropic.com",
        "noreply@github.com",
        "your-email@example.com",  # placeholder example
    }
    email_re = re.compile(
        r"\b[A-Za-z0-9._%+-]+@(?:gmail|outlook|hotmail|yahoo|"
        r"protonmail|icloud|me|aol|fastmail)\.com\b",
        re.IGNORECASE,
    )
    for path in TRACKED_DOCS:
        text = path.read_text()
        matches = [m for m in email_re.findall(text)]
        for m in matches:
            # email_re returns the domain via group(0); rebuild
            # via finditer to get the full address.
            pass
        full = [
            m.group(0)
            for m in email_re.finditer(text)
            if m.group(0).lower() not in WHITELIST
        ]
        assert not full, (
            f"v1.18.76: personal email found in {path.name}: "
            f"{full!r}"
        )


# ── v1.19.56: widened scan across all tracked source files ──


def test_no_homelab_ip_in_any_tracked_source_file():
    """The operator's homelab IP must not appear in any tracked source file
    (.py / .js / .html / .css / .md / .yaml / .toml / .txt) — an easy slip if a
    docker-log line is pasted into a code comment. The needle is read from the
    gitignored personal-strings file (named nowhere public)."""
    needles = [s for s in _personal_strings() if s.replace(".", "").isdigit()]
    OFFENDERS = []
    for path in _tracked_source_files():
        try:
            text = path.read_text()
        except (UnicodeDecodeError, OSError):
            continue
        for ip in needles:
            if ip in text:
                OFFENDERS.append(str(path.relative_to(REPO)))
                break
    assert not OFFENDERS, (
        f"personal IP found in source file(s): {OFFENDERS}. Move to a "
        f"gitignored audit/journal file or use `192.168.1.10` (the placeholder)."
    )


def test_no_personal_hostnames_in_any_tracked_source_file():
    """The operator's hostname(s) must not appear in any tracked source file
    (case-insensitive). Hostnames betray operator identity + server topology.
    The needles are read from the gitignored personal-strings file (the repo
    names none of them); non-numeric entries are treated as hostnames/names."""
    needles = [
        s.lower() for s in _personal_strings()
        if not s.replace(".", "").isdigit()
    ]
    OFFENDERS = []
    for path in _tracked_source_files():
        try:
            text = path.read_text().lower()
        except (UnicodeDecodeError, OSError):
            continue
        for h in needles:
            if h in text:
                OFFENDERS.append(f"{path.relative_to(REPO)}: {h}")
                break
    assert not OFFENDERS, (
        f"v1.19.56: personal hostname(s) found in source "
        f"file(s): {OFFENDERS}"
    )


def test_no_personal_paths_in_any_tracked_source_file():
    """The operator's home-dir path must not appear in any tracked
    source file. v1.18.76 audited docs only; v1.19.56 widened
    to catch code-comment leaks too."""
    OFFENDERS = []
    for path in _tracked_source_files():
        try:
            text = path.read_text()
        except (UnicodeDecodeError, OSError):
            continue
        if _HOME in text:
            OFFENDERS.append(str(path.relative_to(REPO)))
    assert not OFFENDERS, (
        f"v1.19.56: personal home path `{_HOME}` found in "
        f"source file(s): {OFFENDERS}"
    )


def test_no_personal_email_in_any_tracked_source_file():
    """the user's personal email must not appear in any tracked
    source file. Wider than v1.18.76's doc-only scan."""
    WHITELIST = {
        "noreply@anthropic.com",
        "noreply@github.com",
        "your-email@example.com",
    }
    email_re = re.compile(
        r"\b[A-Za-z0-9._%+-]+@(?:gmail|outlook|hotmail|yahoo|"
        r"protonmail|icloud|me|aol|fastmail)\.com\b",
        re.IGNORECASE,
    )
    OFFENDERS = []
    for path in _tracked_source_files():
        try:
            text = path.read_text()
        except (UnicodeDecodeError, OSError):
            continue
        for m in email_re.finditer(text):
            if m.group(0).lower() not in WHITELIST:
                OFFENDERS.append(
                    f"{path.relative_to(REPO)}: {m.group(0)}"
                )
    assert not OFFENDERS, (
        f"v1.19.56: personal email(s) found in source "
        f"file(s): {OFFENDERS}"
    )
