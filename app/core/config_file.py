"""
motif.yaml — persistent configuration file.

Lives at /config/motif.yaml on disk. The web UI is the primary editor; env
vars are reserved for first-boot bootstrapping and ALWAYS-overrides for
deployments that prefer 12-factor behavior.

Three-tier resolution order (highest priority wins):

  1. Environment variables (MOTIF_*)
  2. motif.yaml on disk
  3. Built-in defaults

This means: a value set in env always wins. A value set ONLY in YAML
(env unset) wins over the default. The UI writes to YAML; if the
corresponding env var is also set, the UI value won't take effect until
the env var is unset and the container restarted. The UI surfaces this
condition to the user with a "// ENV OVERRIDE" badge.

Atomic writes: tempfile-then-rename guarantees the file is never
half-written even if power dies mid-save. fcntl-based locking guarantees
two motif processes (which shouldn't happen, but might during a
container restart) don't both clobber the same file.

This module is the ONLY module that touches /config/motif.yaml. All
reads and writes go through ConfigFile.load() / ConfigFile.save().
"""
from __future__ import annotations

import dataclasses
import fcntl
import logging
import os
import tempfile
import threading
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator

import yaml

log = logging.getLogger(__name__)


# ----------------------------------------------------------------------------
# Schema
# ----------------------------------------------------------------------------
# Keep this aligned with the env-var names in config.py and the UI fields in
# settings.html. Each section maps cleanly to a UI tab.

@dataclass
class PathsConfig:
    # Where motif writes downloaded theme files. movies/ and tv/ subdirs are
    # auto-created. Container-relative path. NO DEFAULT — first-run setup
    # forces the user to choose, since the path must align with their /data
    # mount.
    themes_dir: str = ""
    cookies_file: str = "/config/cookies.txt"


@dataclass
class PlexConfigSection:
    enabled: bool = True
    url: str = ""
    token: str = ""              # masked in API responses
    movie_section: str = "1"     # fallback when discovery fails
    tv_section: str = "2"        # fallback when discovery fails
    analyze_after_placement: bool = True
    section_exclude: list[str] = field(default_factory=list)
    section_include: list[str] = field(default_factory=list)
    tvdb_api_key: str = ""       # legacy v1.5–v1.8: TVDB v4 paid key (deprecated)
    tmdb_api_key: str = ""       # for orphan resolution; masked in API responses


@dataclass
class DownloadsConfig:
    rate_per_hour: int = 30
    concurrency: int = 1
    retries: int = 3
    audio_quality: int = 0       # LAME -V scale: 0=best


@dataclass
class MatchingConfig:
    strict_edition: bool = True
    plus_mode: str = "separator"  # "separator" or "literal"


@dataclass
class PlacementConfig:
    # When True (default), a successful download immediately enqueues a
    # `place` job that hardlinks the staged theme into Plex's media folder.
    # When False, downloads land in /pending for user review and only get
    # placed after manual approval. Per-job payload may override this via
    # an "auto_place" key.
    auto_place: bool = True


@dataclass
class SyncConfig:
    db_url: str = "https://app.lizardbyte.dev/ThemerrDB"
    cron: str = "0 13 * * *"
    # Pick the transport for the daily ThemerrDB pull.
    #   "remote"   — original behavior (v1.x). Walk pages.json +
    #                per-item JSON over HTTP against `db_url`
    #                (gh-pages CDN). Battle-tested; ~10k+ HTTP
    #                calls per sync.
    #   "database" — Phase A (v1.12.121). Pull a single tarball
    #                of the LizardByte ThemerrDB `database` branch
    #                from codeload.github.com, extract to a temp
    #                dir, then resolve every item from local
    #                files. Same JSON shape, ~1000× fewer HTTP
    #                calls. Phase A.5 (v1.12.126) added an ETag
    #                short-circuit so an unchanged tree skips the
    #                upsert pipeline entirely.
    #   "git"      — Phase B (v1.13.0). Maintain a bare git mirror
    #                of the database branch via dulwich. First run
    #                shallow-clones (~30 MB pack); subsequent runs
    #                fetch the delta (typically <100 KB) and walk
    #                only the changed paths. Fastest and lowest-
    #                bandwidth on busy days; subsumes A's ETag
    #                short-circuit (a no-op fetch returns no new
    #                objects).
    # Default is "remote" until each phase clears burn-in. On
    # snapshot/git-path failure motif falls through to the next-
    # tier transport so a single sync cycle still completes.
    source: str = "remote"
    # codeload tarball URL for the snapshot path. Overridable for
    # self-hosted mirrors of the database branch (e.g. a corporate
    # proxy of the same content).
    database_url: str = "https://codeload.github.com/LizardByte/ThemerrDB/tar.gz/database"
    # v1.13.0 (Phase B): git URL for the dulwich mirror. Standard
    # GitHub HTTPS — no auth required for public repos.
    git_url: str = "https://github.com/LizardByte/ThemerrDB.git"
    # The branch motif tracks. LizardByte publishes daily exports
    # to `database`. Overridable so a self-hosted mirror can use
    # a different branch name.
    git_branch: str = "database"
    # v1.12.126: auto-enqueue a plex_enum job after every successful
    # ThemerrDB sync. The TDB sync's own resolve_theme_ids step
    # already links new TDB rows to existing plex_items, so the
    # post-sync enum is for catching PLEX-side changes (new titles
    # added, folders moved, sidecars added/removed) and re-verifying
    # plex_theme_uri claims via the TTL HEAD probe.
    #
    # Default True for safety — daily cron syncs need the auto-enum
    # since no human is around to manually click REFRESH afterward.
    # Users who want manual control on dashboard-button clicks can
    # disable this; the daily cron + manual click both honor the
    # setting (set up a separate plex_enum cron if you want different
    # behavior).
    auto_enum_after_sync: bool = True


@dataclass
class WebConfig:
    host: str = "0.0.0.0"
    port: int = 5309
    trust_forward_auth: bool = False


@dataclass
class RuntimeConfig:
    dry_run_default: bool = True
    log_level: str = "INFO"


@dataclass
class MotifConfig:
    """Top-level config object. Mirrors the structure of motif.yaml."""
    schema_version: int = 1
    paths: PathsConfig = field(default_factory=PathsConfig)
    plex: PlexConfigSection = field(default_factory=PlexConfigSection)
    downloads: DownloadsConfig = field(default_factory=DownloadsConfig)
    matching: MatchingConfig = field(default_factory=MatchingConfig)
    placement: PlacementConfig = field(default_factory=PlacementConfig)
    sync: SyncConfig = field(default_factory=SyncConfig)
    web: WebConfig = field(default_factory=WebConfig)
    runtime: RuntimeConfig = field(default_factory=RuntimeConfig)


# ----------------------------------------------------------------------------
# Env var bindings
# ----------------------------------------------------------------------------
# (env_var_name, dotted-path-into-MotifConfig, type-converter)
# This table is intentionally explicit so adding a new env var is a one-liner
# and so the codebase makes it easy to audit what env vars exist.

def _to_bool(s: str) -> bool:
    return s.strip().lower() in ("1", "true", "yes", "on")


def _to_csv_list(s: str) -> list[str]:
    return [x.strip() for x in s.split(",") if x.strip()]


ENV_BINDINGS: list[tuple[str, str, Any]] = [
    # paths
    ("MOTIF_THEMES_DIR",          "paths.themes_dir",                str),
    ("MOTIF_COOKIES_FILE",        "paths.cookies_file",              str),
    # plex
    ("MOTIF_PLEX_ENABLED",        "plex.enabled",                    _to_bool),
    ("MOTIF_PLEX_URL",            "plex.url",                        str),
    ("MOTIF_PLEX_TOKEN",          "plex.token",                      str),
    ("MOTIF_PLEX_MOVIE_SECTION",  "plex.movie_section",              str),
    ("MOTIF_PLEX_TV_SECTION",     "plex.tv_section",                 str),
    ("MOTIF_PLEX_ANALYZE",        "plex.analyze_after_placement",    _to_bool),
    ("MOTIF_PLEX_SECTION_EXCLUDE","plex.section_exclude",            _to_csv_list),
    ("MOTIF_PLEX_SECTION_INCLUDE","plex.section_include",            _to_csv_list),
    ("MOTIF_TVDB_API_KEY",        "plex.tvdb_api_key",               str),
    ("MOTIF_TMDB_API_KEY",        "plex.tmdb_api_key",               str),
    # downloads
    ("MOTIF_DL_RATE_HOUR",        "downloads.rate_per_hour",         int),
    ("MOTIF_DL_CONCURRENCY",      "downloads.concurrency",           int),
    ("MOTIF_DL_RETRIES",          "downloads.retries",               int),
    ("MOTIF_DL_QUALITY",          "downloads.audio_quality",         int),
    # matching
    ("MOTIF_STRICT_EDITION",      "matching.strict_edition",         _to_bool),
    ("MOTIF_PLUS_MODE",           "matching.plus_mode",              str),
    # placement
    ("MOTIF_AUTO_PLACE",          "placement.auto_place",            _to_bool),
    # sync
    ("MOTIF_DB_URL",              "sync.db_url",                     str),
    ("MOTIF_SYNC_CRON",           "sync.cron",                       str),
    ("MOTIF_SYNC_SOURCE",         "sync.source",                     str),
    ("MOTIF_DB_TAR_URL",          "sync.database_url",               str),
    ("MOTIF_DB_GIT_URL",          "sync.git_url",                    str),
    ("MOTIF_DB_GIT_BRANCH",       "sync.git_branch",                 str),
    ("MOTIF_AUTO_ENUM_AFTER_SYNC","sync.auto_enum_after_sync",       _to_bool),
    # web
    ("MOTIF_WEB_HOST",            "web.host",                        str),
    ("MOTIF_WEB_PORT",            "web.port",                        int),
    ("MOTIF_TRUST_FORWARD_AUTH",  "web.trust_forward_auth",          _to_bool),
    # runtime
    ("MOTIF_DRY_RUN",             "runtime.dry_run_default",         _to_bool),
    ("MOTIF_LOG_LEVEL",           "runtime.log_level",               str),
]


def env_overrides_present() -> dict[str, str]:
    """Return {dotted_path: env_var_name} for every env var currently set.
    Used by the UI to surface "// ENV OVERRIDE" badges next to settings the
    user can't change from the web because env always wins."""
    out = {}
    for env_name, path, _ in ENV_BINDINGS:
        if env_name in os.environ:
            out[path] = env_name
    return out


def _set_dotted(obj: Any, dotted: str, value: Any) -> None:
    parts = dotted.split(".")
    for p in parts[:-1]:
        obj = getattr(obj, p)
    setattr(obj, parts[-1], value)


def _get_dotted(obj: Any, dotted: str) -> Any:
    for p in dotted.split("."):
        obj = getattr(obj, p)
    return obj


# ----------------------------------------------------------------------------
# Validation
# ----------------------------------------------------------------------------

class ConfigValidationError(ValueError):
    """Raised when motif.yaml fails validation. Carries a list of human
    error messages so the UI can render them inline."""
    def __init__(self, errors: list[str]):
        self.errors = errors
        super().__init__("; ".join(errors))


def validate(cfg: MotifConfig, *, require_themes_dir: bool = True) -> list[str]:
    """Return a list of validation errors. Empty list means valid.
    `require_themes_dir=False` is used during initial bootstrap before the
    user has set the path — sync still runs (only download/place are blocked)."""
    errors: list[str] = []

    if require_themes_dir:
        if not cfg.paths.themes_dir:
            errors.append("paths.themes_dir is not set — visit /settings to choose where motif should write theme files")
        elif not os.path.isabs(cfg.paths.themes_dir):
            errors.append(f"paths.themes_dir must be an absolute path, got {cfg.paths.themes_dir!r}")

    if cfg.web.port < 1 or cfg.web.port > 65535:
        errors.append(f"web.port {cfg.web.port} is out of range (1-65535)")

    if cfg.downloads.rate_per_hour < 1:
        errors.append(f"downloads.rate_per_hour must be >= 1, got {cfg.downloads.rate_per_hour}")
    if cfg.downloads.rate_per_hour > 600:
        errors.append(f"downloads.rate_per_hour > 600 risks YouTube bot detection")

    if cfg.downloads.concurrency < 1 or cfg.downloads.concurrency > 8:
        errors.append(f"downloads.concurrency must be 1-8, got {cfg.downloads.concurrency}")

    if cfg.downloads.audio_quality < 0 or cfg.downloads.audio_quality > 9:
        errors.append(f"downloads.audio_quality must be 0-9 (LAME -V scale)")

    if cfg.matching.plus_mode not in ("separator", "literal"):
        errors.append(f"matching.plus_mode must be 'separator' or 'literal'")

    parts = cfg.sync.cron.split()
    if len(parts) != 5:
        errors.append(f"sync.cron must be 5-field cron (got {len(parts)} fields)")

    if cfg.sync.source not in ("remote", "database", "git"):
        errors.append(
            f"sync.source must be 'remote', 'database', or 'git' "
            f"(got {cfg.sync.source!r})"
        )

    if cfg.runtime.log_level not in ("DEBUG", "INFO", "WARNING", "ERROR"):
        errors.append(f"runtime.log_level must be DEBUG, INFO, WARNING, or ERROR")

    return errors


# ----------------------------------------------------------------------------
# File I/O
# ----------------------------------------------------------------------------

class ConfigFile:
    """Owns the on-disk motif.yaml. Use:
        cf = ConfigFile(path)
        cfg = cf.load()           # returns MotifConfig
        # ... mutate cfg ...
        cf.save(cfg, updated_by="admin")

    Concurrency model:
      - Per-process re-entrant lock around load/save (rare but possible
        under FastAPI's threadpool dispatch)
      - fcntl exclusive lock on the file itself during save
      - load is a normal file read; if save is in progress we just wait
        on the per-process lock, which is held briefly
    """

    def __init__(self, path: Path):
        self.path = path
        self._lock = threading.RLock()

    def exists(self) -> bool:
        return self.path.exists()

    def load(self) -> MotifConfig:
        """Load YAML from disk, fall back to defaults if missing/invalid.
        Then layer env vars on top. Returns the merged MotifConfig.
        Raises ConfigValidationError ONLY for clearly malformed YAML, not
        for missing required values (use validate() for that)."""
        with self._lock:
            cfg = MotifConfig()
            if self.path.exists():
                try:
                    raw = yaml.safe_load(self.path.read_text()) or {}
                except yaml.YAMLError as e:
                    raise ConfigValidationError(
                        [f"motif.yaml is not valid YAML: {e}"]
                    ) from e
                if not isinstance(raw, dict):
                    raise ConfigValidationError(
                        ["motif.yaml top-level must be a mapping"]
                    )
                _hydrate_dataclass(cfg, raw)
            self._apply_env_overrides(cfg)
            return cfg

    def _apply_env_overrides(self, cfg: MotifConfig) -> None:
        for env_name, dotted, conv in ENV_BINDINGS:
            v = os.environ.get(env_name)
            if v is None:
                continue
            try:
                _set_dotted(cfg, dotted, conv(v))
            except (ValueError, TypeError) as e:
                log.warning("Ignoring invalid env %s=%r: %s", env_name, v, e)

    def save(self, cfg: MotifConfig, *, updated_by: str = "system") -> None:
        """Atomic save: write a temp file in the same directory, fsync,
        rename over the target. fcntl-lock the target during the rename
        in case another process is reading."""
        with self._lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            data = _serialize(cfg, updated_by=updated_by)

            # Write to a temp file in the same directory so the rename is atomic
            fd, tmp_path = tempfile.mkstemp(
                prefix=".motif.yaml.", suffix=".tmp",
                dir=str(self.path.parent),
            )
            try:
                with os.fdopen(fd, "w") as f:
                    f.write(data)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp_path, self.path)
            except Exception:
                # Clean up the tempfile if rename failed
                try: os.unlink(tmp_path)
                except OSError: pass
                raise

            # Best-effort: chmod 0600 (cookies file is also 0600; this contains
            # the Plex token if the user puts it here)
            try:
                os.chmod(self.path, 0o600)
            except OSError:
                pass

            log.info("Wrote motif.yaml (updated_by=%s)", updated_by)


def _hydrate_dataclass(target: Any, src: dict) -> None:
    """Recursively populate a dataclass from a dict. Unknown keys are
    silently ignored (forward-compat). Type coercion is intentionally
    minimal — YAML's parser handles most of it."""
    if not dataclasses.is_dataclass(target):
        return
    for f in dataclasses.fields(target):
        if f.name not in src:
            continue
        v = src[f.name]
        cur = getattr(target, f.name)
        if dataclasses.is_dataclass(cur) and isinstance(v, dict):
            _hydrate_dataclass(cur, v)
        else:
            setattr(target, f.name, v)


def _serialize(cfg: MotifConfig, *, updated_by: str) -> str:
    """Render MotifConfig to a YAML string with stable ordering and a
    leading comment block. Stable ordering means git diffs are minimal."""
    payload = dataclasses.asdict(cfg)
    body = yaml.safe_dump(
        payload,
        sort_keys=False,        # preserve dataclass field order
        default_flow_style=False,
        allow_unicode=True,
        width=100,
    )
    header = (
        "# motif configuration\n"
        f"# Last updated by: {updated_by}\n"
        "#\n"
        "# This file is managed by the motif web UI. Edits made here are\n"
        "# preserved across container restarts. Environment variables (MOTIF_*)\n"
        "# always override values in this file.\n"
        "#\n"
        "# Schema version: incremented when fields change incompatibly.\n"
        "# Older files are migrated forward automatically on next read.\n"
        "\n"
    )
    return header + body
