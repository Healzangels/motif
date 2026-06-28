"""v1.24.42 — security-audit dependency floors + ffmpeg build guard.

A 2026-06-25 audit (pip-audit over the pinned set + an ffmpeg CVE review) found:
  - fastapi==0.115.* capped the transitive starlette at <0.47, below every fix
    for a cluster of starlette advisories → bumped to 0.138.* + an explicit
    starlette>=1.3.1 floor (the only fix a plain rebuild could NOT reach);
  - yt-dlp / dulwich / python-multipart pins already resolved to fixed versions
    on a rebuild, but the documented floors were stale → bumped explicitly;
  - the ffmpeg PixelSmash CVE (CVE-2026-8461) is fixed in Debian trixie but not
    bookworm → a Dockerfile guard fails the build on a pre-trixie ffmpeg.

These pins keep a future fresh resolve (or an accidental base-image downgrade)
from regressing below the patched minimums. For motif the underlying CVEs were
all admin-only-DoS / non-applicable / already-neutralized; this is hygiene.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
REQ = (REPO / "requirements.txt").read_text()
DOCKERFILE = (REPO / "Dockerfile").read_text()


def test_starlette_pinned_to_fixed_floor():
    # the one fix a rebuild can't reach on its own (fastapi 0.115 capped it)
    assert "starlette>=1.3.1" in REQ
    assert "fastapi==0.138.*" in REQ


def test_dependency_security_floors():
    assert "yt-dlp>=2026.6.9" in REQ
    assert "dulwich>=1.2.6" in REQ
    assert "python-multipart>=0.0.31" in REQ


def test_dockerfile_ffmpeg_version_guard():
    # fails the build on bookworm's unpatched ffmpeg 5.x (CVE-2026-8461)
    assert "CVE-2026-8461" in DOCKERFILE
    assert "ffmpeg -version" in DOCKERFILE
    assert "grep -qE 'version (7|8|9|[1-9][0-9])\\.'" in DOCKERFILE


def test_audit_breadcrumbs_present():
    # every bumped line carries the audit-dated rationale
    assert REQ.count("v1.24.42 (security audit 2026-06-25)") >= 4
