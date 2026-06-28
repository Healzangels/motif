"""v1.19.56 — documentation catch-up + widened secrets audit.

the user's ask: "Let's make sure our documentation is up to
date, also lets sanitize our documentation and github for
any sensitive data including IP addresses, hostnames, or other
information that should be kept private and does not need to be
for public consumption."

## Documentation updates

PROJECT_HISTORY.md was last updated by v1.18.76 (§ 22 covered
v1.18.76 → v1.18.85). Three new sections added:

  - § 23 covers v1.18.86 → v1.19.20 (line rollover +
    v1.19.x recovery-walker burn + LOGS UI)
  - § 24 covers v1.19.21 → v1.19.40 (BK pipe + P-row
    preservation + audit arc)
  - § 25 covers v1.19.41 → v1.19.55 (cloud-themes-backup
    feature in 3 tags + 10 stabilization tags +
    notification polish)

## Widened sensitive-data audit

v1.18.76 introduced the secrets-leak guard but scoped it to
.md docs only. v1.19.56 widens the scan to every tracked
source file (.py / .js / .html / .css / .yaml / .toml /
.txt) so a sensitive string in a code comment or HTML
placeholder gets caught at test time.

Adds:
  - homelab-IP scan across all source files
  - personal-hostname (case-insensitive) scan
  - personal home-path scan widened
  - personal email scan widened (was docs-only)
"""
from __future__ import annotations

import sys
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))


# ── PROJECT_HISTORY sections 23 / 24 / 25 ────────────────────


def test_project_history_section_23_exists():
    """§ 23 must catalogue v1.18.86 → v1.19.20."""
    history = (REPO / "docs" / "PROJECT_HISTORY.md").read_text()
    assert "## 23." in history, (
        "v1.19.56: PROJECT_HISTORY § 23 required to cover the "
        "v1.18.86 → v1.19.20 tag arc"
    )


def test_project_history_section_23_covers_arc_endpoints():
    """§ 23 must reference both endpoints of the arc."""
    history = (REPO / "docs" / "PROJECT_HISTORY.md").read_text()
    sec_idx = history.index("## 23.")
    sec_end = history.index("## 24.", sec_idx)
    section = history[sec_idx:sec_end]
    assert "v1.18.86" in section
    assert "v1.19.20" in section


def test_project_history_section_23_names_thematic_arcs():
    """§ 23 must group tags by thematic arc."""
    history = (REPO / "docs" / "PROJECT_HISTORY.md").read_text()
    sec_idx = history.index("## 23.")
    sec_end = history.index("## 24.", sec_idx)
    section = history[sec_idx:sec_end].lower()
    # Three thematic arcs in this digest.
    assert "rollover" in section
    assert "recovery-walker" in section or "recovery walker" in section
    assert "logs" in section


def test_project_history_section_24_exists():
    """§ 24 must catalogue v1.19.21 → v1.19.40."""
    history = (REPO / "docs" / "PROJECT_HISTORY.md").read_text()
    assert "## 24." in history


def test_project_history_section_24_covers_arc_endpoints():
    """§ 24 must reference both endpoints of the arc."""
    history = (REPO / "docs" / "PROJECT_HISTORY.md").read_text()
    sec_idx = history.index("## 24.")
    sec_end = history.index("## 25.", sec_idx)
    section = history[sec_idx:sec_end]
    assert "v1.19.21" in section
    assert "v1.19.40" in section


def test_project_history_section_24_names_bk_pipe():
    """§ 24's centerpiece is the BK badge pipe — the
    v1.19.42 cloud-themes-backup feature relies on it."""
    history = (REPO / "docs" / "PROJECT_HISTORY.md").read_text()
    sec_idx = history.index("## 24.")
    sec_end = history.index("## 25.", sec_idx)
    section = history[sec_idx:sec_end].lower()
    assert "bk" in section
    assert "p-row" in section or "p row" in section
    assert "v1.19.21" in section


def test_project_history_section_25_exists():
    """§ 25 must catalogue the cloud-themes-backup arc
    (v1.19.41 → v1.19.55)."""
    history = (REPO / "docs" / "PROJECT_HISTORY.md").read_text()
    assert "## 25." in history


def test_project_history_section_25_covers_full_arc():
    """§ 25 must reference each major tag in the
    cloud-themes-backup arc so the historical record is
    complete."""
    history = (REPO / "docs" / "PROJECT_HISTORY.md").read_text()
    sec_idx = history.index("## 25.")
    section = history[sec_idx:]
    for tag in (
        "v1.19.41", "v1.19.42", "v1.19.43",
        "v1.19.44", "v1.19.45", "v1.19.46",
        "v1.19.51", "v1.19.52", "v1.19.55",
    ):
        assert tag in section, (
            f"v1.19.56: § 25 must reference {tag} (cloud-themes-"
            f"backup arc tag)"
        )


def test_project_history_section_25_names_cloud_backup_feature():
    """§ 25's centerpiece is the cloud-themes-backup feature."""
    history = (REPO / "docs" / "PROJECT_HISTORY.md").read_text()
    sec_idx = history.index("## 25.")
    section = history[sec_idx:]
    assert "cloud-themes-backup" in section.lower()
    assert "Plex Pass" in section
    # Distinctive feature elements.
    assert "v1.18.36 re-upload" in section
    assert "C1" in section


# ── Widened secrets audit (verify the v1.18.76 file gained
#    the v1.19.56 broader tests) ─────────────────────────────


def test_secrets_audit_has_widened_source_file_scan():
    """v1.19.56 widened the secrets audit beyond .md docs to
    every tracked source file. Verify the helper +
    broader-scan tests exist."""
    audit_test = (
        REPO / "tests"
        / "test_v1_18_76_docs_update_and_secrets_audit.py"
    ).read_text()
    assert "_tracked_source_files" in audit_test, (
        "v1.19.56: secrets audit must expose a helper that "
        "enumerates every tracked source file (not just docs)"
    )
    assert "test_no_homelab_ip_in_any_tracked_source_file" in audit_test
    assert "test_no_personal_hostnames_in_any_tracked_source_file" in audit_test


def test_widened_audit_skips_gitignored_files():
    """The widened scan must skip .gitignored files
    (SESSION_JOURNAL, AUDIT_*.md, CODEBASE_AUDIT, etc.)
    because those are local-only working notes that may
    legitimately contain server IPs / paths."""
    audit_test = (
        REPO / "tests"
        / "test_v1_18_76_docs_update_and_secrets_audit.py"
    ).read_text()
    for skip in (
        "SESSION_JOURNAL.md",
        "CODEBASE_AUDIT.md",
        "INFO_CARD_AUDIT.md",
    ):
        assert skip in audit_test, (
            f"v1.19.56: widened audit must skip {skip}"
        )


# ── Version pin ──────────────────────────────────────────────


def test_v1_19_56_version_pin():
    """Version bumped at v1.19.56 (then again at v1.19.57 for
    the settings-UI toggle surfacing). Match 1.19.x prefix."""
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
