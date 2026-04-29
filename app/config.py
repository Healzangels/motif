"""
Settings — the application's view of configuration.

This module is the bridge between the on-disk `motif.yaml` (managed by
`config_file.py`) and the rest of the codebase. It preserves the existing
`Settings` interface so consumers don't need to change.

Hot reload semantics:
  - The `Settings` instance is mutable and shared across the app (worker,
    scheduler, web).
  - `Settings.reload()` re-reads `motif.yaml` (env still overrides) and
    swaps the underlying MotifConfig in-place.
  - The web layer triggers reload after every save.
  - The scheduler/worker read fresh values on each iteration; long-running
    operations like a download in flight see the snapshot they started with.

Path model (post-v1.4.0):
  - `/config` is the appdata mount (DB, cookies, motif.yaml, session key)
  - `/data` is the unified data mount (mirrors host /mnt/user/data)
  - Themes go into `paths.themes_dir` (configured in UI; must be set before
    sync runs). motif creates `<themes_dir>/movies/` and `<themes_dir>/tv/`
    automatically.
  - Plex section location_paths from /library/sections are now valid
    container paths (because /data mirrors what Plex sees), so the
    placement engine uses them directly.

There are NO MORE `MOTIF_MOVIES_ROOT`, `MOTIF_TV_ROOT` env vars or
`/media/movies`, `/media/tv` mounts. If you have a v1.3.x deployment,
the migration on first boot translates old env vars into a seed
motif.yaml, then the env vars become no-ops.
"""
from __future__ import annotations

import os
import secrets
import threading
from pathlib import Path

from .core.config_file import (
    ConfigFile, MotifConfig, validate, env_overrides_present,
)


_DEFAULT_CONFIG_DIR = Path(os.environ.get("MOTIF_CONFIG_DIR", "/config"))
_DEFAULT_DATA_DIR = Path(os.environ.get("MOTIF_DATA_DIR", "/data"))


class Settings:
    """Live, reloadable settings backed by motif.yaml + env overrides.

    Most properties pass through to the underlying MotifConfig with a
    Path() wrapper where needed. Read-only from the consumer's perspective:
    to change settings, write the underlying YAML and call reload().
    """

    def __init__(self, config_dir: Path | None = None,
                 data_dir: Path | None = None):
        self._config_dir = config_dir or _DEFAULT_CONFIG_DIR
        self._data_dir = data_dir or _DEFAULT_DATA_DIR
        self._config_file = ConfigFile(self._config_dir / "motif.yaml")
        self._lock = threading.RLock()
        self._cfg: MotifConfig = self._config_file.load()

    # ---- Config file passthrough ----

    @property
    def cfg(self) -> MotifConfig:
        with self._lock:
            return self._cfg

    @property
    def config_file(self) -> ConfigFile:
        return self._config_file

    def reload(self) -> MotifConfig:
        """Re-read motif.yaml and swap. Returns the new config."""
        new = self._config_file.load()
        with self._lock:
            self._cfg = new
        return new

    def save(self, cfg: MotifConfig, *, updated_by: str) -> None:
        """Persist the config and immediately swap it into place."""
        self._config_file.save(cfg, updated_by=updated_by)
        with self._lock:
            self._cfg = self._config_file.load()  # re-read so env overrides re-apply

    def env_overrides(self) -> dict[str, str]:
        return env_overrides_present()

    def validate_current(self, *, require_themes_dir: bool = True) -> list[str]:
        return validate(self._cfg, require_themes_dir=require_themes_dir)

    # ---- Container paths ----

    @property
    def config_dir(self) -> Path:
        return self._config_dir

    @property
    def data_dir(self) -> Path:
        return self._data_dir

    @property
    def db_path(self) -> Path:
        return self._config_dir / "motif.db"

    @property
    def session_key_file(self) -> Path:
        return self._config_dir / ".session_key"

    # ---- Paths section ----

    @property
    def themes_dir(self) -> Path | None:
        """The configured themes output directory, or None if not yet set
        on first run. Consumers should check `is_paths_ready()` before
        attempting any download/placement."""
        v = self._cfg.paths.themes_dir
        return Path(v) if v else None

    @property
    def movies_themes_dir(self) -> Path | None:
        td = self.themes_dir
        return (td / "movies") if td else None

    @property
    def tv_themes_dir(self) -> Path | None:
        td = self.themes_dir
        return (td / "tv") if td else None

    @property
    def cookies_file(self) -> Path:
        return Path(self._cfg.paths.cookies_file)

    def is_paths_ready(self) -> bool:
        """True iff themes_dir is configured. The worker/scheduler check
        this before download/place jobs run; sync runs regardless."""
        return bool(self._cfg.paths.themes_dir)

    # ---- Plex section ----

    @property
    def plex_enabled(self) -> bool:
        return self._cfg.plex.enabled

    @property
    def plex_url(self) -> str:
        return self._cfg.plex.url

    @property
    def plex_token(self) -> str:
        return self._cfg.plex.token

    @property
    def plex_movie_section(self) -> str:
        return self._cfg.plex.movie_section

    @property
    def plex_tv_section(self) -> str:
        return self._cfg.plex.tv_section

    @property
    def plex_analyze_after(self) -> bool:
        return self._cfg.plex.analyze_after_placement

    @property
    def plex_excluded_titles(self) -> set[str]:
        return set(self._cfg.plex.section_exclude)

    @property
    def plex_included_titles(self) -> set[str]:
        return set(self._cfg.plex.section_include)

    @property
    def tvdb_api_key(self) -> str:
        return self._cfg.plex.tvdb_api_key

    # ---- Downloads section ----

    @property
    def download_rate_per_hour(self) -> int:
        return self._cfg.downloads.rate_per_hour

    @property
    def download_concurrency(self) -> int:
        return self._cfg.downloads.concurrency

    @property
    def download_max_retries(self) -> int:
        return self._cfg.downloads.retries

    @property
    def download_audio_quality(self) -> str:
        return str(self._cfg.downloads.audio_quality)

    # ---- Matching section ----

    @property
    def strict_edition_match(self) -> bool:
        return self._cfg.matching.strict_edition

    @property
    def plus_equiv_mode(self) -> str:
        return self._cfg.matching.plus_mode

    # ---- Sync section ----

    @property
    def motifdb_base_url(self) -> str:
        return self._cfg.sync.db_url

    @property
    def sync_cron(self) -> str:
        return self._cfg.sync.cron

    # ---- Web section ----

    @property
    def web_host(self) -> str:
        return self._cfg.web.host

    @property
    def web_port(self) -> int:
        return self._cfg.web.port

    @property
    def trust_forward_auth(self) -> bool:
        return self._cfg.web.trust_forward_auth

    # ---- Runtime section ----

    @property
    def dry_run_default(self) -> bool:
        return self._cfg.runtime.dry_run_default

    @property
    def log_level(self) -> str:
        return self._cfg.runtime.log_level

    # ---- Session key ----

    def resolve_session_key(self) -> str:
        """Return a session key — generated and persisted to /config on first
        run. Lives outside motif.yaml so it never accidentally gets exposed
        in an exported config (and so it survives YAML rewrites)."""
        sk = self.session_key_file
        if sk.exists():
            return sk.read_text().strip()
        key = secrets.token_urlsafe(48)
        sk.parent.mkdir(parents=True, exist_ok=True)
        sk.write_text(key)
        try: sk.chmod(0o600)
        except OSError: pass
        return key


# Module-level singleton accessor. The first call constructs; subsequent
# calls return the same instance (so `reload()` actually affects everyone).
_settings_singleton: Settings | None = None
_settings_lock = threading.Lock()


def get_settings() -> Settings:
    global _settings_singleton
    with _settings_lock:
        if _settings_singleton is None:
            _settings_singleton = Settings()
        return _settings_singleton


def reset_settings_for_tests() -> None:
    """Test-only — drop the singleton so a fresh Settings reads from a
    clean env/disk state. Never call this from production code."""
    global _settings_singleton
    with _settings_lock:
        _settings_singleton = None
