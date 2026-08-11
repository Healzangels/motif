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

Atomic writes: tempfile-then-rename (os.replace) guarantees the file is
never half-written even if power dies mid-save — a reader sees either the
old or the new file, never a torn one. Writers within the process are
serialized by a re-entrant lock (see save()).

This module is the ONLY module that touches /config/motif.yaml. All
reads and writes go through ConfigFile.load() / ConfigFile.save().
"""
from __future__ import annotations

import dataclasses
import logging
import os
import tempfile
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

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
    # v1.13.11: pre-download free-space guard. If themes_dir's filesystem
    # has less than this many MB free, downloads fail transiently with a
    # "low disk" error and the topbar shows a warning pill. Set to 0 to
    # disable the guard entirely. Default of 500MB is comfortably above
    # any single theme.mp3 (~5MB) plus yt-dlp's working space.
    min_free_disk_mb: int = 500


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
    audio_quality: int = 0       # LAME -V scale: 0=best
    # v1.13.53: yt-dlp geo-bypass + proxy options for routing
    # downloads around regional restrictions. Three knobs, layered:
    #   geo_bypass: yt-dlp's built-in X-Forwarded-For trick. Cheap;
    #     YouTube generally ignores it for rights-holder blocks but
    #     helps on extractors that respect XFF.
    #   geo_bypass_country: pin a 2-letter ISO country code for the
    #     fake XFF lookup (e.g. "US"). Empty = yt-dlp picks one.
    #   proxy_url: a real HTTP/SOCKS5 proxy yt-dlp routes through
    #     (e.g. "socks5://user:pass@host:1080"). This is the option
    #     that actually solves YouTube content blocks since it
    #     changes the IP YouTube sees.
    geo_bypass: bool = False
    geo_bypass_country: str = ""
    proxy_url: str = ""


# v0.51.193: the usable hover band for the level target. config validate keeps the
# wider -70..0 typo-guard (silence to full-scale = "not a typo"), but the FEATURE's
# real range is this: mp3gain outside it either clamps global_gain or lands a level
# nothing on the hover surface wants. Every functional consumer — the settings
# loudness_target_lufs property, normalize-one, the card's stepper — clamps to these,
# so "level to target" gives ONE answer no matter which door you came through
# (the mirror-drift v0.51.191 set out to remove, which the two disagreeing bands had
# quietly reopened for any configured target outside this range).
LOUDNESS_TARGET_FLOOR = -31.0
LOUDNESS_TARGET_CEIL = -6.0


@dataclass
class LoudnessConfig:
    """v0.51.188: condition theme loudness as it arrives.

    Plex plays themes on hover, so mixed-source loudness is the loudest quality
    defect in the library — the operator's own measurement: a -14.5 LUFS median
    with outliers up to -5.2 (clipping at +2.9 dBTP).

    normalize_on_download conditions a theme BEFORE it is placed, which is the
    cheap half of the problem: Plex ingests theme.mp3 at scan time, so a theme
    normalized before its first placement needs no propagation at all — Plex
    only ever sees the conditioned copy. (Re-normalizing an ALREADY-placed theme
    is the expensive half and needs a re-upload; that's // NORMALIZE, v0.51.185.)

    Default OFF. This mutates downloaded audio, and nobody opts a homelab into
    that by accident.
    """
    # v0.51.203 (Phase 3): the on-arrival gate is SPLIT by who triggered the download.
    # normalize_on_download = downloads YOU trigger (// DOWNLOAD FROM TDB, bulk download).
    # normalize_auto_added = the sync/enum auto-picks motif fetches without a click
    # (reason 'new' / 'url_changed', gated by the sync auto-download-new-themes toggle).
    # Independent so you can auto-level the hands-off picks while keeping your own
    # downloads raw, or vice-versa. UPLOAD MP3 / SET URL ignore both — they carry their
    # own per-theme checkbox (v0.51.201). Both default OFF (this mutates downloaded audio).
    normalize_on_download: bool = False
    normalize_auto_added: bool = False
    # -18 LUFS: the operator's audit put the library median at -14.5, and -18 is
    # the ambient-hover lean the target-preview slider was built to settle.
    target_lufs: float = -18.0


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
    # v1.18.23: how motif realizes a placement on movies/TV rows.
    #   'file' (default) — hardlink the canonical to a theme.mp3
    #                       sidecar at the Plex media folder
    #                       (legacy behavior, themerr-plex parity).
    #   'api'             — POST audio bytes to
    #                       /library/metadata/{rk}/themes; Plex
    #                       stores the theme in its own metadata
    #                       bundle (no sidecar at the media folder).
    # Collections IGNORE this setting — they have no folder and
    # always use 'api' regardless. Per-job payload "kind" key
    # overrides this default. The PLACE menu's "PUSH TO PLEX
    # (FILE)" / "PUSH TO PLEX (API)" items also set the per-job
    # kind explicitly, so the global default only governs
    # auto-place (sync downloads + scheduled retries).
    default_method: str = "file"
    # v1.24.32: gate the hourly auto-restore sweep (_restore_lost_placements).
    # Both default ON (the v1.24.26/.29 behavior). Separate switches per
    # the user's call so file-based recovery and API re-uploads can be toggled
    # independently.
    #   auto_restore_sidecar   — re-hardlink a theme.mp3 whose Plex folder copy
    #                            went missing (delete+re-add / Radarr churn).
    #   auto_restore_plex_upload — re-POST a theme to Plex whose uploaded
    #                            rating_key was destroyed by a delete+re-add
    #                            (the v1.24.28 RP state). When OFF the row stays
    #                            RP for a manual PLACE; detection/badge unaffected.
    auto_restore_sidecar: bool = True
    auto_restore_plex_upload: bool = True


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
    # v1.21.19: cron-only sub-toggle for the post-sync enum. Only has an
    # effect when auto_enum_after_sync (above) is OFF — it re-enables the
    # post-sync Plex enumeration for the SCHEDULED (cron) sync ONLY, leaving
    # manually-kicked // SYNC THEMERRDB clicks enum-free. Lets a user keep
    # the daily cron self-healing (no human around to click REFRESH) while
    # NOT paying the enum cost on every manual sync. The decision is
    # do_enum = auto_enum_after_sync OR (trigger == 'cron' AND this). Sync
    # trigger is stamped into the job payload by both enqueue sites.
    auto_enum_after_cron_sync: bool = False
    # v1.19.71: when sync discovers a NEW TDB theme for a row
    # in the user's Plex library that has no motif/Plex theme
    # yet (SRC=— shape), should motif auto-download the theme
    # immediately?
    #
    # Default False per the user's v1.19.70/.71 review: motif
    # should prefer the explicit operator prompt (blue !UPD
    # glyph + pending_update row → ACCEPT / KEEP CURRENT) over
    # silent action. Pre-v1.19.71 the equivalent behavior
    # fired silently whenever `enqueue_downloads=True` (the
    # worker default) and the row was unthemed — the user had
    # no way to know motif had auto-acquired themes for new
    # SRC=— titles. Surfacing as !UPD lets the user see the
    # action coming + decide per-row.
    #
    # When True, sync's is_new branch enqueues the download
    # immediately (existing pre-v1.19.71 behavior preserved
    # for users who want it). When False (default), sync
    # writes a pending_updates row with kind='new_theme_
    # available' instead — the user sees the blue !UPD glyph
    # + can ACCEPT to download or KEEP CURRENT to dismiss.
    #
    # This setting ONLY affects the SRC=— case (unthemed
    # row, no Plex content, no motif content). U/A/M/P rows
    # always write pending_updates regardless of this flag
    # — those have existing content the user might want to
    # KEEP CURRENT instead of overwriting.
    auto_download_new_themes_for_unthemed_rows: bool = False


@dataclass
class WebConfig:
    host: str = "0.0.0.0"
    port: int = 5309
    trust_forward_auth: bool = False
    # v1.17.23: optional IP allowlist for the forward-auth path.
    # v1.24.12: Empty = FAIL-CLOSED — forward-auth is SKIPPED (NOT the old
    # "trust the header from any IP"; that pre-v1.24.12 permissive default was the
    # admin-bypass footgun). Non-empty = AuthMiddleware checks the client IP against
    # each entry before trusting X-Authentik-Username /
    # X-Forwarded-User. Entries can be IPs ("172.17.0.5") or
    # CIDRs ("172.17.0.0/16"). When the list is non-empty AND
    # the request comes from an IP NOT in the list, the
    # forward-auth branch is skipped — the request falls
    # through to API token / session cookie / anonymous.
    # Security audit v1.17.12 HIGH 1: pre-v1.17.23 anyone
    # reaching :5309 directly (LAN, misconfigured firewall,
    # exposed container) could spoof admin via the header. The
    # allowlist closes that — the user's deploy with a known NPM
    # container IP gets a single-entry allowlist.
    forward_auth_allowed_ips: list[str] = field(default_factory=list)
    # v0.51.76 (security audit — XFF-spoof): the immediate-peer proxy IP(s) uvicorn
    # trusts to set X-Forwarded-For, mapped to uvicorn's forwarded_allow_ips. Empty
    # (default) → "*" = uvicorn trusts XFF from ANY peer (historical behavior). With
    # "*", request.client.host becomes the attacker-controlled leftmost XFF token, so a
    # direct-to-:5309 attacker (off-path LAN host / sibling container) can spoof an
    # allowlisted IP + X-Authentik-Username and bypass forward-auth entirely. Set this
    # to NPM's real container IP/CIDR so uvicorn only honors XFF from NPM; a direct
    # attacker's real peer IP then can't satisfy forward_auth_allowed_ips. Legit
    # requests are unaffected (they arrive via NPM, whose XFF stays honored).
    forward_auth_trusted_proxies: list[str] = field(default_factory=list)
    # v1.21.17 (security audit): controls the session cookie's Secure flag.
    # "auto" (default) = infer from X-Forwarded-Proto / URL scheme — preserves
    # the historical behavior, but that header is spoofable when uvicorn runs
    # forwarded_allow_ips="*", so a request claiming http downgrades the
    # cookie. "on" forces Secure=True (the right setting for an HTTPS-only
    # deploy behind NPM/CF); "off" forces it off for a plain-HTTP LAN box.
    cookie_secure: str = "auto"


@dataclass
class RuntimeConfig:
    dry_run_default: bool = True
    log_level: str = "INFO"


# v1.23.16: scheduled on-disk database backups. Manual backups are
# always available via the // DATABASE settings card (v1.23.15); this
# section governs the OPTIONAL nightly cron + retention prune. `cron`
# is read once at scheduler boot (like sync.cron) so a schedule change
# needs a restart; `enabled` + `retention` are read fresh on each tick
# so toggling them takes effect immediately.
@dataclass
class DatabaseBackupConfig:
    enabled: bool = False          # opt-in — off by default
    cron: str = "0 4 * * *"        # 04:00 UTC daily (clear of the 03:xx prunes)
    retention: int = 7             # keep newest N; 0 = keep all


# v1.17.0: Apprise notification sinks + per-event toggles. Two
# sinks coexist (homelab user choice — embed Apprise lib OR call
# an external Apprise API container, OR both):
#   * `apprise_urls` — list of Apprise service URLs (discord://,
#     pushover://, tgram://, etc.) dispatched in-process via the
#     embedded `apprise` package
#   * `apprise_external_url` — single URL to an external Apprise
#     API service (e.g. http://apprise-api.local:8000/notify/motif)
#     that motif POSTs to with title + body
# Empty fields = no dispatch for that sink. Both can be populated
# in which case motif fires both paths. See app/core/notify.py
# for the dispatcher.
#
# `events` is a per-event-kind toggle map. Default state: critical
# / aggregate events ON (sync_completed, sync_failed,
# bulk_action_completed, cookies_needed, disk_low,
# worker_restarted) and per-row / chatty events OFF
# (theme_added, theme_deleted, themes_added_by_sync,
# new_tdb_theme_available, release_available). User can flip any
# bit in /settings → NOTIFICATIONS.
_DEFAULT_NOTIFY_EVENTS: dict[str, bool] = {
    # v1.17.0: phase-1 event set — 3 ON, 1 OFF (opt-in). The
    # dispatch sites for these are wired in this release.
    # ON-by-default (aggregate / actionable):
    "sync_completed":          True,
    "sync_failed":             True,
    "bulk_action_completed":   True,
    # OFF-by-default (opt-in — title list can be long).
    # v1.21.6: these two no longer fire SEPARATE messages — each
    # now folds a "🎵 New:" / "🔄 Updated:" titles section INTO
    # the single sync_completed message (gated by the toggle).
    # Pre-fix a changed sync produced up to 3 notifications that
    # repeated the same lists. Kept as distinct keys so the two
    # sections stay independently toggleable.
    "themes_added_by_sync":    False,
    "themes_updated_by_sync":  False,
    # v1.17.4: phase-2 event set wired this tag (6 hook sites +
    # `notify_dedupe` primitive). Settings UI surfaces a toggle
    # for each one.
    # v1.17.10: keys MUST live here — the api.py:218 closed-set
    # filter drops anything missing from this dict at PATCH time
    # (silent drop pre-fix; logs at DEBUG since v1.17.10). Before
    # the v1.17.10 fix the v1.17.4 ship landed the UI toggles but
    # forgot to extend this allowlist → SAVE EVENTS auto-unchecked
    # every one of these on click.
    # ON-by-default (critical / rare, all dedupe-rate-limited):
    "cookies_needed":          True,   # rate-limited 6h
    "disk_low":                True,   # rate-limited 12h
    "worker_restarted":        True,   # only fires on unclean shutdown
    # OFF-by-default (opt-in — per-row or new-release ping):
    "theme_added":             False,
    # v1.19.55: distinct from theme_added — fires when motif
    # re-deploys an EXISTING theme to Plex (PUSH TO PLEX,
    # REPLACE TDB, PROMOTE TO ACTIVE, force-place jobs). The
    # row already had the theme; this signals "Plex's serving
    # state changed" not "new theme appeared." Pre-v1.19.55
    # all these triggered theme_added, conflating "added" with
    # "pushed/replaced." the user's feedback: "we need to update
    # theme added to take into account theme source changed
    # like push via side car or swapping to via api."
    "theme_pushed":            False,
    "theme_deleted":           False,
    "release_available":       False,
    # v1.21.5: UI-surfaced + shipped. Fires after a plex refresh
    # when a NEW Plex item matches a TDB theme but has no theme in
    # Plex or motif (SRC=—) — "you could add a theme". OFF by
    # default (opt-in, like the other theme_* events). The v1.17.0
    # deferral was about hooking resolve_theme_ids' 500-row hot
    # loop; v1.21.5 sidesteps that by hooking the plex_enum side
    # (_maybe_notify_theme_available, post-resolve) which is sparse
    # and naturally rate-limited.
    "new_tdb_theme_available": False,
    # v0.51.150: OFF-by-default (Apprise). Fires when a GENUINELY-NEW Plex item
    # arrives ALREADY themed by Plex (has_theme=1) that motif doesn't own (no
    # placement / local file) — an FYI "new content showed up already themed,
    # nothing to do." Baseline-gated (only after a section's first enum) + per-
    # (media_type, tmdb) 30-day deduped, so it can't flood on the initial seed or
    # on Plex remove+re-add churn. OFF for Discord like new_tdb_theme_available —
    # the in-app INBOX is its primary surface (see notify_inbox.INBOX_EVENT_KINDS),
    # recorded independently of THIS toggle but gated by its own inbox_events entry
    # since v0.51.210 (it was unconditional before that).
    "plex_item_arrived_themed": False,
    # v1.18.80: ON-by-default. Fires when Plex's theme transitions
    # 1→0 on a row with user_overrides.intent='backup'. Rare
    # (Plex doesn't drop its own themes often) AND actionable
    # (the user's backup is now eligible to deploy via // PROMOTE
    # TO ACTIVE). Different shape from theme_added (per-row but
    # rare) — defaults ON so the user sees the signal without
    # opt-in.
    "backup_ready_to_deploy":  True,
    # v1.21.46: ON-by-default. Fires when a theme DOWNLOADS as a
    # backup (backup_only stamp) instead of being placed — P-row
    # bulk ACCEPT, DOWNLOAD TDB BACKUP, SET URL + KEEP AS BACKUP,
    # CSV import-as-backup. the user's report: a bulk ACCEPT on
    # newly-added TDB P-rows landed N backups (link state TB) with
    # NO notification — theme_added/theme_pushed are gated on
    # `outcome.placed` so backups never fired any of them. ON by
    # default because the landing was previously a total silence;
    # coalesced (60s window, see notify._COALESCE_WINDOW_BY_KIND)
    # so a bulk collapses to one summary while the first backup
    # still pings immediately via the leading edge.
    "theme_backed_up":         True,
    # v1.18.90: OFF-by-default, conservatively — the event fires per-row and a
    # bulk Plex change could burst it.
    # v0.51.256: flipped ON. Both halves of that rationale are now answered, and
    # MEASURED rather than argued. (1) Bursts coalesce as of v0.51.254/255, on
    # top of the existing notify_dedupe per-row keys + 24h rate-limit. (2) The
    # accuracy question settled on the operator's install: 5 lifetime firings,
    # 5 correct — ER (left the library) plus 4 disk-dropout casualties that were
    # genuinely stranded (has_theme=0, no local_files backup, and a TDB URL dead
    # upstream with failure_kind=video_removed). Zero false positives.
    # This is the ONLY tier that means "no automatic recovery exists — a human
    # must supply a URL", so muting it hid the four alerts that actually needed
    # action while the RECOVERABLE tiers (backup_ready / sidecar_available)
    # pinged immediately. The cheap tiers were loud and the expensive one was
    # silent — exactly backwards.
    "plex_theme_lost":         True,
    # v1.22.60 (audit round 2 #1): the v1.19.41 tier-1/tier-2 kinds
    # were dispatched by the reaper but NEVER added here — and
    # notify.dispatch drops unknown kinds — so the "🎯 deploy your
    # backup" / "ADOPT your sidecar" theme-lost notifications were
    # dead for 30+ tags on every install. ON-by-default per the
    # backup_ready_to_deploy precedent: rare, actionable, 24h
    # dedupe-limited (plex_theme_lost's OFF default is about the
    # NO-recovery tier's noise; these tiers exist precisely
    # because there IS a one-click recovery).
    "theme_lost_backup_ready":      True,
    "theme_lost_sidecar_available": True,
    # v1.24.26: ON-by-default. Fires when the hourly restore sweep
    # (_restore_lost_placements) re-deploys a motif-owned sidecar whose
    # Plex theme.mp3 went missing — the delete+re-add case the v1.18.90
    # reaper silently skipped as 'other_fallback' (the user's Avenue Q
    # repro). Self-heal IS the action, so this is a confirmation ping,
    # not a call-to-action; ON because the operator wants to know motif
    # touched Plex on their behalf. Rare (a sidecar only goes missing on
    # a Plex folder churn) + coalesced via the once-hourly sweep cadence.
    "theme_auto_restored":          True,
}

# v0.51.210: per-kind IN-APP INBOX toggle map — which event kinds land in the topbar
# INBOX drawer, INDEPENDENT of the Apprise `events` toggles above. All default ON (the
# pre-toggle behaviour: every INBOX_EVENT_KIND records unconditionally). Turning one OFF
# stops NEW inbox rows of that kind; notify.dispatch gates the inbox record on this map.
# The key set MUST equal notify_inbox.INBOX_EVENT_KINDS (guarded by test_v0_51_210); it's
# duplicated here rather than imported to keep this config module free of an app-core
# dependency (config_file is imported very early).
_DEFAULT_INBOX_EVENTS: dict[str, bool] = {
    "theme_added":                  True,
    # v0.51.259: theme_pushed's APPRISE default is off (per-row deploy chatter on
    # a noisy channel); its INBOX default is on. The two registries answer
    # different questions — "should this ping Discord" vs "should this be
    # findable in-app" — and a quiet local record is the point of the drawer.
    "theme_pushed":                 True,
    "theme_backed_up":              True,
    "plex_item_arrived_themed":     True,
    "theme_auto_restored":          True,
    "new_tdb_theme_available":      True,
    "backup_ready_to_deploy":       True,
    "theme_lost_backup_ready":      True,
    "theme_lost_sidecar_available": True,
    "plex_theme_lost":              True,
}


@dataclass
class NotificationsConfig:
    apprise_urls: list[str] = field(default_factory=list)
    apprise_external_url: str = ""
    events: dict[str, bool] = field(
        default_factory=lambda: dict(_DEFAULT_NOTIFY_EVENTS))
    # v0.51.210: the in-app INBOX allowlist toggles (see _DEFAULT_INBOX_EVENTS).
    inbox_events: dict[str, bool] = field(
        default_factory=lambda: dict(_DEFAULT_INBOX_EVENTS))


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
    # v1.17.0: Apprise notification sinks + per-event toggles.
    notifications: NotificationsConfig = field(default_factory=NotificationsConfig)
    # v1.23.16: scheduled database backups + retention.
    database_backup: DatabaseBackupConfig = field(default_factory=DatabaseBackupConfig)
    # v0.51.188: theme loudness conditioning.
    loudness: LoudnessConfig = field(default_factory=LoudnessConfig)


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
    ("MOTIF_MIN_FREE_DISK_MB",    "paths.min_free_disk_mb",          int),
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
    ("MOTIF_DL_QUALITY",          "downloads.audio_quality",         int),
    # v1.13.53: download bypass options
    ("MOTIF_DL_GEO_BYPASS",       "downloads.geo_bypass",            _to_bool),
    ("MOTIF_DL_GEO_COUNTRY",      "downloads.geo_bypass_country",    str),
    ("MOTIF_DL_PROXY",            "downloads.proxy_url",             str),
    # matching
    ("MOTIF_STRICT_EDITION",      "matching.strict_edition",         _to_bool),
    ("MOTIF_PLUS_MODE",           "matching.plus_mode",              str),
    # placement
    ("MOTIF_AUTO_PLACE",          "placement.auto_place",            _to_bool),
    # v1.18.23: default placement method for movie/TV auto-place
    # ('file' for sidecar hardlink, 'api' for /library/metadata
    # POST). Collections ignore (always 'api').
    ("MOTIF_DEFAULT_PLACEMENT_METHOD", "placement.default_method",   str),
    # sync
    ("MOTIF_DB_URL",              "sync.db_url",                     str),
    ("MOTIF_SYNC_CRON",           "sync.cron",                       str),
    ("MOTIF_SYNC_SOURCE",         "sync.source",                     str),
    ("MOTIF_DB_TAR_URL",          "sync.database_url",               str),
    ("MOTIF_DB_GIT_URL",          "sync.git_url",                    str),
    ("MOTIF_DB_GIT_BRANCH",       "sync.git_branch",                 str),
    ("MOTIF_AUTO_ENUM_AFTER_SYNC","sync.auto_enum_after_sync",       _to_bool),
    ("MOTIF_AUTO_ENUM_AFTER_CRON_SYNC","sync.auto_enum_after_cron_sync", _to_bool),
    # v1.20.27: surfaced in Settings → SCHEDULE → SYNC TIMING.
    ("MOTIF_AUTO_DOWNLOAD_NEW_THEMES",
     "sync.auto_download_new_themes_for_unthemed_rows",              _to_bool),
    # web
    ("MOTIF_WEB_HOST",            "web.host",                        str),
    ("MOTIF_WEB_PORT",            "web.port",                        int),
    ("MOTIF_TRUST_FORWARD_AUTH",  "web.trust_forward_auth",          _to_bool),
    # v1.17.23: comma-separated IPs/CIDRs allowed to use the forward-auth header.
    # v1.24.12: empty = FAIL-CLOSED (forward-auth skipped), not permissive.
    ("MOTIF_FORWARD_AUTH_ALLOWED_IPS", "web.forward_auth_allowed_ips", _to_csv_list),
    # v0.51.76: comma-separated proxy IPs uvicorn trusts to set X-Forwarded-For
    # (→ forwarded_allow_ips). Empty = "*"; set to NPM's IP to close the XFF spoof.
    ("MOTIF_FORWARD_AUTH_TRUSTED_PROXIES", "web.forward_auth_trusted_proxies", _to_csv_list),
    # v1.21.17: "auto" | "on" | "off" — force the session cookie Secure flag.
    ("MOTIF_COOKIE_SECURE",       "web.cookie_secure",               str),
    # runtime
    ("MOTIF_DRY_RUN",             "runtime.dry_run_default",         _to_bool),
    ("MOTIF_LOG_LEVEL",           "runtime.log_level",               str),
    # v1.23.16: scheduled database backups
    ("MOTIF_DB_BACKUP_ENABLED",   "database_backup.enabled",         _to_bool),
    ("MOTIF_DB_BACKUP_CRON",      "database_backup.cron",            str),
    ("MOTIF_DB_BACKUP_RETENTION", "database_backup.retention",       int),
    ("MOTIF_NORMALIZE_ON_DOWNLOAD", "loudness.normalize_on_download", _to_bool),
    ("MOTIF_NORMALIZE_AUTO_ADDED", "loudness.normalize_auto_added",  _to_bool),
    ("MOTIF_LOUDNESS_TARGET",     "loudness.target_lufs",            float),
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
    """v0.50.89: read-side mirror of _set_dotted, used by save()'s
    env-override-baking guard."""
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
    user has set the path — sync still runs (only download/place are blocked).

    v0.50.89 (audit MEDIUM): the checks below assume each field already has
    the type its dataclass declares, but `_hydrate_dataclass` does NOT
    coerce types on YAML load ("intentionally minimal", per its own
    docstring) — a hand-edited or corrupted motif.yaml can hand a numeric
    field a string (`rate_per_hour: abc`), which raised an unhandled
    TypeError here on a bare `<` comparison. main.py calls this with no
    surrounding try/except, so that crashed boot on every subsequent
    start instead of surfacing a clean validation error. Wrapped so any
    stray type mismatch — TypeError from a numeric compare, AttributeError
    from calling a str method on a non-str — becomes one more entry in the
    returned error list instead of an uncaught exception."""
    errors: list[str] = []
    try:
        if require_themes_dir:
            if not cfg.paths.themes_dir:
                errors.append("paths.themes_dir is not set — visit /settings to choose where motif should write theme files")
            elif not os.path.isabs(cfg.paths.themes_dir):
                errors.append(f"paths.themes_dir must be an absolute path, got {cfg.paths.themes_dir!r}")

        if cfg.paths.min_free_disk_mb < 0:
            errors.append(
                f"paths.min_free_disk_mb must be >= 0 (0 disables the guard), "
                f"got {cfg.paths.min_free_disk_mb}"
            )

        if cfg.web.port < 1 or cfg.web.port > 65535:
            errors.append(f"web.port {cfg.web.port} is out of range (1-65535)")

        if cfg.web.cookie_secure not in ("auto", "on", "off"):
            errors.append(
                f"web.cookie_secure must be 'auto', 'on', or 'off' "
                f"(got {cfg.web.cookie_secure!r})"
            )

        if cfg.downloads.rate_per_hour < 1:
            errors.append(f"downloads.rate_per_hour must be >= 1, got {cfg.downloads.rate_per_hour}")
        if cfg.downloads.rate_per_hour > 600:
            errors.append(f"downloads.rate_per_hour > 600 risks YouTube bot detection")

        if cfg.downloads.concurrency < 1 or cfg.downloads.concurrency > 8:
            errors.append(f"downloads.concurrency must be 1-8, got {cfg.downloads.concurrency}")

        if cfg.downloads.audio_quality < 0 or cfg.downloads.audio_quality > 9:
            errors.append(f"downloads.audio_quality must be 0-9 (LAME -V scale)")

        # v1.13.53: bypass option validation. geo_bypass_country is
        # optional; if provided, must be a 2-letter ISO country code.
        # proxy_url must look like a URL with a recognized scheme.
        gbc = (cfg.downloads.geo_bypass_country or "").strip()
        if gbc and (len(gbc) != 2 or not gbc.isalpha()):
            errors.append(
                f"downloads.geo_bypass_country must be a 2-letter ISO code "
                f"(e.g. 'US', 'GB'), got {cfg.downloads.geo_bypass_country!r}",
            )
        proxy = (cfg.downloads.proxy_url or "").strip()
        if proxy:
            valid_schemes = ("http://", "https://", "socks5://", "socks5h://", "socks4://")
            if not any(proxy.startswith(s) for s in valid_schemes):
                errors.append(
                    f"downloads.proxy_url must start with one of "
                    f"{valid_schemes} (got {proxy[:20]!r}…)",
                )

        # v1.21.15 (security audit MED): sync.database_url / git_url drive
        # outbound fetches (tarball stream in _DatabaseSnapshot, dulwich
        # clone). validate() checked proxy_url + apprise schemes but never
        # these — leaving an admin-set (or config-injected) file:// or
        # internal-host SSRF target unvalidated. Mirror the proxy_url check.
        # v1.23.62 (audit #6): db_url (the per-item-JSON remote source) is the third
        # outbound-fetch URL and was missed by this loop too — scheme-validate it.
        for _u_field in ("database_url", "git_url", "db_url"):
            _u_val = (getattr(cfg.sync, _u_field, "") or "").strip()
            if _u_val and not (_u_val.startswith("http://")
                               or _u_val.startswith("https://")):
                errors.append(
                    f"sync.{_u_field} must start with http:// or https:// "
                    f"(got {_u_val[:24]!r}…)"
                )

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

        # v1.23.16: scheduled-backup validation.
        if len(cfg.database_backup.cron.split()) != 5:
            errors.append(
                f"database_backup.cron must be 5-field cron "
                f"(got {len(cfg.database_backup.cron.split())} fields)"
            )
        if cfg.database_backup.retention < 0:
            errors.append(
                f"database_backup.retention must be >= 0 (0 keeps all), "
                f"got {cfg.database_backup.retention}"
            )
        # v0.51.188: the target drives `gain = target - measured`, so a nonsense
        # value drives a nonsense gain onto real audio. -70..0 LUFS spans silence
        # to full-scale; anything outside it is a typo, not an intent.
        if not (-70.0 <= cfg.loudness.target_lufs <= 0.0):
            errors.append(
                f"loudness.target_lufs must be between -70 and 0 LUFS, "
                f"got {cfg.loudness.target_lufs}"
            )

        # v1.17.1: notifications.apprise_external_url scheme enforcement.
        # Mirrors the downloads.proxy_url check above — the field is
        # admin-controlled (low blast radius since motif is single-tenant
        # behind Authentik), but an admin mistake here turns motif into
        # a POST gun aimed at whatever the URL points at. Restrict to
        # http/https to block file://, gopher://, ftp:// and the like;
        # operators wanting another transport can use the embedded
        # apprise_urls instead. RFC1918 / loopback isn't blocked — it's
        # the canonical setup for a same-host apprise-api sidecar.
        ext_url = (cfg.notifications.apprise_external_url or "").strip()
        if ext_url:
            if not ext_url.startswith(("http://", "https://")):
                errors.append(
                    f"notifications.apprise_external_url must start with "
                    f"http:// or https:// (got {ext_url[:20]!r}…)",
                )

        # v1.17.13: notifications.apprise_urls per-URL scheme validation.
        # apprise plugins accept a wide set of schemes (discord://,
        # tgram://, pushover://, slack://, mailto://, gotify://, …);
        # we don't enumerate the allowlist here — apprise's own plugin
        # registry is the source of truth for what's dispatchable —
        # but we DO reject obviously-dangerous schemes that should
        # never appear in a notification URL (file://, ftp://,
        # gopher://, javascript:, etc.). Without this guard a careless
        # paste could send notifications anywhere apprise will dial.
        # Security audit (v1.17.12) HIGH 2.
        for i, url in enumerate(cfg.notifications.apprise_urls or []):
            if not isinstance(url, str) or not url.strip():
                continue
            if not _validate_apprise_url_scheme(url.strip()):
                scheme = url.strip().split("://", 1)[0] if "://" in url else url.strip()
                errors.append(
                    f"notifications.apprise_urls[{i}]: scheme not allowed "
                    f"(got {scheme!r}). Use a known Apprise service URL."
                )
    except (TypeError, AttributeError, ValueError) as e:
        errors.append(
            f"config contains a value of an unexpected type — {e}. "
            f"Check motif.yaml for a hand-edited or corrupted field."
        )
    return errors


# v1.17.13: apprise URL scheme validation + masking helpers used by
# both validate() and api.py's PATCH handler. Lives here so the
# allowlist/denylist + mask format are a single source of truth.

# Deny-listed schemes — local-resource access, code injection, or
# non-network transports that should never appear in a notification
# URL. Anything else (including the wide tail of apprise plugin
# schemes) passes the allowlist. Apprise's own add() call still
# rejects truly unparseable URLs at dispatch time.
_APPRISE_DENIED_SCHEMES: frozenset[str] = frozenset({
    "file", "ftp", "sftp", "gopher", "data",
    "javascript", "vbscript", "about", "chrome",
})

_APPRISE_MASK = "***"


def _validate_apprise_url_scheme(url: str) -> bool:
    """v1.17.13: True if `url` has a `<scheme>://...` shape AND the
    scheme isn't in the deny-list. Lenient on the scheme name so
    new apprise plugins don't require a code change here — the
    deny-list captures the actual risk (local-resource +
    code-injection schemes)."""
    if not url or "://" not in url:
        return False
    scheme = url.split("://", 1)[0].strip().lower()
    if not scheme:
        return False
    # Scheme name must look like a URI scheme — letters / digits /
    # `+` / `-` / `.`. Anything else (whitespace, control chars,
    # injection attempts) fails.
    if not all(c.isalnum() or c in "+-." for c in scheme):
        return False
    if scheme in _APPRISE_DENIED_SCHEMES:
        return False
    return True


def mask_apprise_url(url: str) -> str:
    """v1.17.13: replace an apprise URL with `<scheme>://***` so the
    GET /api/config response doesn't expose embedded credentials.
    Mirrors the plex.token / tmdb_api_key / proxy_url masking
    pattern. Preserves the scheme so the operator can see WHICH
    services they have configured without revealing tokens."""
    if not url:
        return ""
    if "://" not in url:
        return _APPRISE_MASK
    scheme = url.split("://", 1)[0]
    return f"{scheme}://{_APPRISE_MASK}"


def _is_masked_apprise_url(url: str) -> bool:
    """v1.17.13: True if `url` matches the masked form
    `<scheme>://***`. PATCH handler uses this to detect 'keep
    existing at this position' entries in the round-trip."""
    if not url or "://" not in url:
        return False
    suffix = url.split("://", 1)[1]
    return suffix == _APPRISE_MASK


def mask_url_credentials(url: str) -> str:
    """v1.21.17 (security audit): redact ONLY the userinfo of a URL —
    `https://user:tok@host/path` → `https://***@host/path`. Unlike
    mask_apprise_url (whole-URL mask) the host+path of a sync URL is
    non-secret and shown as an editable settings field, so we hide only the
    credential segment. A credential-free URL (the default public ThemerrDB
    URL) passes through unchanged so the operator still sees it in full."""
    if not url or "://" not in url:
        return url or ""
    scheme, rest = url.split("://", 1)
    authority = rest.split("/", 1)[0]
    if "@" not in authority:
        return url
    host_and_path = rest.split("@", 1)[1]
    return f"{scheme}://***@{host_and_path}"


def _is_masked_url_credentials(url: str) -> bool:
    """v1.21.17: True if `url` carries the masked-userinfo marker
    `://***@`. PATCH treats a round-tripped value with this marker as
    'keep the stored credential' (mirrors plex.token '***' = keep)."""
    return bool(url) and "://***@" in url


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
      - the write itself is a tempfile + atomic os.replace, so a reader
        never sees a torn file even mid-save
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
        rename over the target (os.replace is atomic, so a concurrent
        reader sees either the old or the new file, never a torn one)."""
        with self._lock:
            # v0.50.89: don't let a save silently bake a currently-active env
            # override into motif.yaml. `cfg` came from `load()`, which always
            # applies env overrides on top of the on-disk values, so a field
            # the caller never touched still holds the ENV value here, not the
            # disk value. Serializing it as-is would freeze that env value
            # into the YAML — the next time the env var is unset, the config
            # would keep behaving as if it were still set. For every
            # env-overridden field that's UNCHANGED from what the env
            # mandates (a passive carryover, not an explicit edit), restore
            # whatever's genuinely on disk before writing. A field the caller
            # DID explicitly set to something else is left alone.
            overrides = env_overrides_present()
            if overrides and self.path.exists():
                # v0.51.15 (audit #31): both fault branches were silent AND
                # diverged: a YAMLError hydrated disk_cfg from pure dataclass
                # DEFAULTS (env-bound fields written back as defaults, losing
                # the operator's real disk values), while a non-dict document
                # silently skipped the guard entirely (baking the env value —
                # the exact bug this guard exists to prevent). Unify: on either
                # fault we cannot know the disk values, so WARN loudly and skip
                # the un-bake — the env value gets written this once (env still
                # wins at runtime; nothing changes until the env var is unset),
                # which is strictly less damaging than a silent reset-to-default.
                try:
                    raw = yaml.safe_load(self.path.read_text()) or {}
                except yaml.YAMLError as e:
                    raw = None
                    log.warning(
                        "config save: %s is not parseable YAML (%s) — "
                        "skipping the env-override un-bake; env-mandated "
                        "values will be written into the file this save. "
                        "Restore a valid motif.yaml to preserve disk values.",
                        self.path, e)
                if raw is not None and not isinstance(raw, dict):
                    log.warning(
                        "config save: %s parsed as %s, not a mapping — "
                        "skipping the env-override un-bake (env-mandated "
                        "values will be written into the file this save)",
                        self.path, type(raw).__name__)
                    raw = None
                if isinstance(raw, dict):
                    disk_cfg = MotifConfig()
                    _hydrate_dataclass(disk_cfg, raw)
                    for dotted, env_name in overrides.items():
                        conv = next((c for e, p, c in ENV_BINDINGS if p == dotted), None)
                        if conv is None:
                            continue
                        try:
                            env_value = conv(os.environ[env_name])
                        except (ValueError, TypeError):
                            continue
                        try:
                            if _get_dotted(cfg, dotted) == env_value:
                                _set_dotted(cfg, dotted, _get_dotted(disk_cfg, dotted))
                        except AttributeError:
                            continue

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
            # v1.17.9: log on chmod failure. motif.yaml may hold the
            # Plex token + apprise notification URLs (which can carry
            # service-side credentials). Silently leaving the file at
            # the umask default on a chmod failure could expose those
            # to other local users on a multi-tenant host. Hygiene
            # audit (class 9): bare `except: pass` masked the leak.
            try:
                os.chmod(self.path, 0o600)
            except OSError as e:
                log.warning(
                    "motif.yaml chmod 0600 failed (path=%s): %s — "
                    "file may be readable by other local users; check "
                    "the host's filesystem permissions",
                    self.path, e,
                )

            log.info("Wrote motif.yaml (updated_by=%s)", updated_by)


def _hydrate_dataclass(target: Any, src: dict) -> None:
    """Recursively populate a dataclass from a dict. Unknown keys are
    silently ignored (forward-compat). Type coercion is intentionally
    minimal — YAML's parser handles most of it.

    v1.17.1 (class P fix): when both `cur` and `v` are dicts (the
    only example today: `notifications.events`), MERGE rather than
    REPLACE. Pre-fix the else branch wholesale-set the dict, which
    on a v1.17.1+ deploy adding a new ON-by-default event kind
    silently OFF'd it on existing installs — the saved YAML didn't
    carry the new key, so the load result was `{old_keys_only}`,
    and `dispatch()`'s `events.get(kind, False)` fell through to
    False. The PATCH path (api.py:191-210) already merges; this
    line makes the load path mirror that contract."""
    if not dataclasses.is_dataclass(target):
        return
    for f in dataclasses.fields(target):
        if f.name not in src:
            continue
        v = src[f.name]
        cur = getattr(target, f.name)
        if dataclasses.is_dataclass(cur) and isinstance(v, dict):
            _hydrate_dataclass(cur, v)
        elif isinstance(cur, dict) and isinstance(v, dict):
            # v1.17.1: merge — preserve defaults for keys not in `v`.
            merged = dict(cur)
            merged.update(v)
            setattr(target, f.name, merged)
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
