"""v1.15.100 — MOTIF_PATH_TRANSLATIONS env var for non-standard mounts.

Closes the latent Phantom-M class for Unraid setups with mount
layouts that the hardcoded `_PATH_PREFIX_TRANSLATIONS` doesn't
cover. The v1.15.90/.91/.92 path-mismatch fixes mitigate
symptoms (unplace + place worker + scanner use natural keys
instead of folder_path), but `plex_enum`'s sidecar stat still
relies on path translation — and on setups where translation
fails, stat returns indeterminate (None) and the existing
`pi.local_theme_file` value is preserved indefinitely.

## What v1.15.100 adds

Env var `MOTIF_PATH_TRANSLATIONS` accepts a comma-separated
list of `host=container` pairs:

  MOTIF_PATH_TRANSLATIONS="/mnt/user/movies=/data/media/movies,/mnt/user/tv=/data/media/tv"

User pairs are tried FIRST (most-specific to the user's
setup), then the hardcoded defaults as fallback. Backwards
compatible: an unset / empty env var means defaults-only,
same as pre-v1.15.100.

Normalization:
* Trailing slash forced on both sides (`/mnt/user/foo` →
  `/mnt/user/foo/`) so prefix matching doesn't false-match
  `/mnt/user/foobar/`.
* Leading slash required on both sides — relative paths are
  rejected (they can't match Plex's absolute paths).
* Malformed entries (no `=`, empty side, non-absolute) are
  silently skipped.

Startup logs surface the active user-translations so
operators can verify the env var is wired up.

## Why env var (not motif.yaml)

The configurability is for the CONTAINER's view of the
host filesystem — set once at container start, doesn't
change at runtime. Env var matches the existing
`MOTIF_CONFIG_DIR` / `MOTIF_DATA_DIR` pattern. motif.yaml
covers app-level config (themes_dir, plex_url, etc.);
path translations are infrastructure-level.
"""

from __future__ import annotations

import importlib
from unittest import mock


def _reload_plex_enum():
    """Re-import plex_enum so the module-level
    `_PATH_PREFIX_TRANSLATIONS` re-computes against the
    current env var state. Each test that mutates the env
    must reload to see the new value."""
    from app.core import plex_enum
    return importlib.reload(plex_enum)


# ── env var parsing ─────────────────────────────────────────


def test_empty_env_returns_defaults_only():
    """Pre-v1.15.100 behavior is preserved: with no env var
    set, `_PATH_PREFIX_TRANSLATIONS` equals the hardcoded
    defaults. Counter-guard against accidentally requiring
    the env var for existing deployments to keep working."""
    with mock.patch.dict("os.environ", {}, clear=False):
        # Remove the env var if it's somehow set.
        if "MOTIF_PATH_TRANSLATIONS" in __import__("os").environ:
            del __import__("os").environ["MOTIF_PATH_TRANSLATIONS"]
        pe = _reload_plex_enum()
        assert pe._PATH_PREFIX_TRANSLATIONS == pe._HARDCODED_PATH_PREFIX_TRANSLATIONS


def test_user_pairs_prepend_to_defaults():
    """A single user pair gets prepended (tried first) before
    the hardcoded defaults. Both sides get normalized with
    trailing slashes."""
    with mock.patch.dict(
        "os.environ",
        {"MOTIF_PATH_TRANSLATIONS":
         "/mnt/user/movies=/data/media/movies"},
    ):
        pe = _reload_plex_enum()
        assert pe._PATH_PREFIX_TRANSLATIONS[0] == (
            "/mnt/user/movies/", "/data/media/movies/"
        )
        # Hardcoded defaults still appended.
        assert pe._HARDCODED_PATH_PREFIX_TRANSLATIONS[0] in pe._PATH_PREFIX_TRANSLATIONS


def test_multiple_user_pairs_comma_separated():
    """Multiple pairs separated by commas — all get prepended."""
    with mock.patch.dict(
        "os.environ",
        {"MOTIF_PATH_TRANSLATIONS":
         "/mnt/user/movies=/data/media/movies,"
         "/mnt/user/tv=/data/media/tv"},
    ):
        pe = _reload_plex_enum()
        first_two = pe._PATH_PREFIX_TRANSLATIONS[:2]
        assert ("/mnt/user/movies/", "/data/media/movies/") in first_two
        assert ("/mnt/user/tv/", "/data/media/tv/") in first_two


def test_trailing_slashes_normalized():
    """Each side gets a trailing slash added if missing."""
    with mock.patch.dict(
        "os.environ",
        {"MOTIF_PATH_TRANSLATIONS":
         "/mnt/user/movies=/data/media/movies"},  # no trailing /
    ):
        pe = _reload_plex_enum()
        assert pe._PATH_PREFIX_TRANSLATIONS[0] == (
            "/mnt/user/movies/", "/data/media/movies/"
        )


def test_already_trailing_slashes_kept():
    """If the user already supplied trailing slashes, we
    don't add another."""
    with mock.patch.dict(
        "os.environ",
        {"MOTIF_PATH_TRANSLATIONS":
         "/mnt/user/movies/=/data/media/movies/"},
    ):
        pe = _reload_plex_enum()
        assert pe._PATH_PREFIX_TRANSLATIONS[0] == (
            "/mnt/user/movies/", "/data/media/movies/"
        )


def test_malformed_entries_silently_skipped():
    """Various malformed inputs are skipped rather than raising —
    a typo in the env var doesn't break startup. Good entries
    in the same string are still parsed."""
    with mock.patch.dict(
        "os.environ",
        {"MOTIF_PATH_TRANSLATIONS":
         # Good entry + 3 malformed (missing =, empty side, relative path)
         "/mnt/user/movies=/data/media/movies,"
         "no_equals_sign,"
         "/missing_right=,"
         "relative_left=/data/foo"},
    ):
        pe = _reload_plex_enum()
        # The only good entry should be present.
        assert ("/mnt/user/movies/", "/data/media/movies/") in pe._PATH_PREFIX_TRANSLATIONS
        # Malformed entries didn't crash the parser.
        assert pe._PATH_PREFIX_TRANSLATIONS  # non-empty


def test_relative_paths_rejected():
    """Relative paths (no leading `/`) on either side are
    rejected — they can't match Plex's absolute paths."""
    with mock.patch.dict(
        "os.environ",
        {"MOTIF_PATH_TRANSLATIONS":
         "relative/host=/data/movies,/host/abs=relative/container"},
    ):
        pe = _reload_plex_enum()
        # Neither bad entry should appear.
        user_only = pe._PATH_PREFIX_TRANSLATIONS[
            :-len(pe._HARDCODED_PATH_PREFIX_TRANSLATIONS)
        ]
        for pair in user_only:
            host, container = pair
            assert host.startswith("/"), (
                f"v1.15.100: rejected relative host slipped through: {pair}"
            )
            assert container.startswith("/"), (
                f"v1.15.100: rejected relative container: {pair}"
            )


# ── Integration: _candidate_local_paths uses the merged list ──


def test_candidate_local_paths_includes_user_translations(tmp_path):
    """The downstream consumer `_candidate_local_paths` iterates
    the merged translation list, so user pairs become reachable
    candidates."""
    with mock.patch.dict(
        "os.environ",
        {"MOTIF_PATH_TRANSLATIONS":
         "/mnt/user/movies=/data/media/movies"},
    ):
        pe = _reload_plex_enum()
        # For a folder_path that starts with the user prefix,
        # the translated form should appear in the candidates.
        candidates = list(pe._candidate_local_paths(
            "/mnt/user/movies/A Dog's Journey (2019)"
        ))
        candidate_strs = [str(c) for c in candidates]
        # Original path is always first.
        assert candidate_strs[0] == "/mnt/user/movies/A Dog's Journey (2019)"
        # Translated form: /data/media/movies/A Dog's Journey (2019)
        assert any(
            "/data/media/movies/A Dog's Journey (2019)" in c
            for c in candidate_strs
        ), (
            f"v1.15.100: user translation didn't reach "
            f"_candidate_local_paths. Got: {candidate_strs}"
        )
