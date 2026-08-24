"""v0.51.275 — the fan-out review's docs batch: every committed doc that
contradicted shipped reality, reconciled.

The docs reviewer's headline: the docs TOUCHED this week were accurate; the
drift was in what wasn't touched. Fixed here: README claimed /healthz was the
only public endpoint (false since /readyz, v0.51.268 — stated in three spots),
understated the healthz response, mis-stated the sync notification subject
(v1.19.55 moved ✅ into the body), presented the 7-kind failure table as
exhaustive (rate_limited shipped in v0.51.269), framed an 11-of-20 event list
as the set, and never mentioned the in-app INBOX at all; ci.yml said "Two
BLOCKING gates" then listed three (the .267 edit) and still gave the pre-gate
rationale for skipping tags; release.yml echoed "the two BLOCKING gates";
DESIGN_SYSTEM's v1.22.55 settings section contradicted its own .263 addendum
in three places; and CLAUDE.md sold PROJECT_HISTORY as "v1.4.0 → current"
while its last entry is v1.22.28 — an entire versioning scheme (372 tags) ago.

These pins hold the corrected claims to the code they describe, so the next
drift in EITHER direction goes red.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
README = (REPO / "README.md").read_text()
CI = (REPO / ".github" / "workflows" / "ci.yml").read_text()
REL = (REPO / ".github" / "workflows" / "release.yml").read_text()
DS = (REPO / "docs" / "DESIGN_SYSTEM.md").read_text()
CLAUDE = (REPO / "CLAUDE.md").read_text()


def test_readme_public_endpoints_match_auth():
    import sys
    sys.path.insert(0, str(REPO))
    from app.core.auth import PUBLIC_PATHS
    non_auth_pages = PUBLIC_PATHS - {"/login", "/logout", "/setup"}
    assert non_auth_pages == {"/healthz", "/readyz"}, (
        "the probe set changed — update README's public-endpoint prose AND "
        "this test together")
    assert "Only one endpoint is public" not in README
    assert "Only `/healthz` is public." not in README
    assert README.count("`/readyz`") >= 2, "both prose spots + the table row"
    assert "| GET    | `/readyz`" in README


def test_readme_failure_kinds_cover_the_enum():
    import sys
    sys.path.insert(0, str(REPO))
    from app.core.downloader import FailureKind
    for kind in FailureKind:
        assert f"`{kind.value}`" in README, (
            f"README's failure table is missing {kind.value} — v0.51.269's "
            f"rate_limited was absent for a tag; keep the table synced")


def test_readme_sync_subject_matches_v1_19_55():
    assert "| Sync completed | `✅ Sync complete` |" not in README, (
        "v1.19.55 moved the ✅ into the body; the subject is the neutral "
        "'Motif sync — …' stem (DESIGN_SYSTEM documents this correctly)")
    assert "Motif sync —" in README


def test_readme_names_the_inbox_and_per_row_read():
    assert "MARK ALL READ" in README
    assert "per-notification" in README or "marks that row read" in README, (
        "v0.51.266's read model had no README home — the gap was the finding")


def test_ci_header_counts_its_own_gates():
    blocking_steps = CI.count("(BLOCKING)")
    assert blocking_steps == 3, (
        f"ci.yml has {blocking_steps} BLOCKING steps — update the header "
        f"count AND this test together")
    assert "Three BLOCKING gates" in CI
    assert "Two BLOCKING gates" not in CI
    assert "follows a branch push that already ran this" not in CI, (
        "the pre-v0.51.267 rationale — release.yml gates itself now")


def test_release_gate_wording_is_countless():
    assert "the two\n  # BLOCKING gates" not in REL
    assert "BLOCKING gates (pytest, ruff correctness, pip-audit)" in REL


def test_design_system_settings_section_is_internally_consistent():
    assert "long hint paragraphs each" not in DS, (
        "contradicted the .263 one-line-hints addendum ten lines below")
    assert "INERT for spacing" in DS, (
        "the tight-split claim must acknowledge the class still appears on "
        "tab-panel markup (four grids), inert for gap")
    assert "16 sites at v0.51.263" in DS


def test_claude_md_states_project_history_real_coverage():
    assert "v1.4.0 → current" not in CLAUDE, (
        "PROJECT_HISTORY stops at v1.22.28 — an entire versioning scheme ago; "
        "'current' sent debuggers to a file that cannot help with v0.5x code")
    assert "v1.4.0 → v1.22.28" in CLAUDE
    hist = (REPO / "docs" / "PROJECT_HISTORY.md").read_text()
    assert "v1.4.0 → v1.22.28" in hist[:600], (
        "if the digest's own coverage line moved, update CLAUDE.md with it")


def test_v0_51_275_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
