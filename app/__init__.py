"""motif — automated theme orchestration for Plex via ThemerrDB."""
# Bump this BEFORE creating each git tag — it drives the topbar
# brand display + the GitHub release-check comparison.
# v1.13.79: bump-before-tag protocol added to CLAUDE.md (was
# silently drifting from the latest tag through v1.13.74-78).
# v1.15.0: rolled over from the v1.14.x line (closed at v1.14.99
# with 99 tags). v1.15.0 cut after a 3-agent code audit (silent
# failures, recent regressions, test-coverage gaps). The two
# real bugs found were fixed pre-cut: a missing index in the
# v46→v47 schema migration, and two silent PlexClient.close
# swallows in the reprobe handler. Both noted in the v1.15.0
# journal entry as the reason for the cut.
# v1.19.0: rolled over from the v1.18.x line (closed at v1.18.99
# with 99 tags). Same 3-agent audit protocol as v1.15.0. ZERO
# real bugs found this time — v1.18.97/98/99 had just closed
# the class-9 silent-fails catalog the night before the cut, so
# the audit was inheriting a freshly-audited tree. Every recent
# defensive surface (perJobBusy v1.18.93, plex_rejected lockout
# v1.18.94, op_progress synth v1.18.95, body fail-fast v1.18.96,
# the three breadcrumb fixes) carried both source pins and
# behavioral tests. Cleanest rollover this project has had.
# v1.20.0: rolled over from the v1.19.x line (closed at v1.19.99
# with 99 tags). Same 3-agent audit protocol (silent-fails /
# recent-regressions / coverage). ONE real bug found + fixed
# pre-cut: a v1.19.98 SRC-axis mirror-drift miss — selectedEligible
# Updates (app.js) still used the bare `SRC!='-'` predicate without
# the new_theme_available exception, hiding the bulk bar for selected
# SRC=— new-theme rows. Fixed at the altitude level: extracted
# pendingUpdateActionable() so the predicate can't drift across its
# 5 bulk-bar/accept sites again. Plus two coalescer hardenings
# (stuck-ACTIVE reset, _dispatch_batch breadcrumb) + the
# previously-only-source-pinned sidecar fallback got behavioral
# coverage. The accepted ceiling/timeout trade-offs (v1.19.94/99)
# were re-flagged + consciously kept.
# v1.21.0: rolled over from the v1.20.x line, cut EARLY (at v1.20.67, not
# the usual .99 close) to carry the recovery_v55 sunset the user flagged.
# The 18 one-shot boot recovery walkers (v1.18.0 FK-cascade data-loss
# recovery + the override / adopt / placement / sha256 backfills) logged
# 16 INFO "marker already set — skipping" lines on EVERY boot once their
# runtime_settings markers were set — pure noise on every current install.
# v1.21.0 DISCONNECTS them from the startup path (main.py) while RETAINING
# recovery_v55.py + its tests as an archived, callable safety net: re-wire
# a specific walker if a historical bug class is ever re-introduced; full
# file deletion is deferred to a real v1.21.x .99 line-close. No fresh
# 3-agent audit this cut — the v1.20.x tree was heavily audited the same
# session across the .59-.67 arc (deferred-audit batch + shelf triage).
# v1.22.0: rolled over at the v1.21.x .99 line-close after a fresh 3-agent
# audit (silent-fails / recent-regressions / coverage) of the v1.21.81-99
# per-edition theme-isolation arc. ONE real regression found + fixed pre-cut:
# SET URL on a TAGGED edition wrote the override at edition_key but enqueued an
# edition-less download, so the v1.21.92 edition-scoped override read resolved
# at '' → ignored the user's URL (api_manual_url now threads edition_key). Plus
# cold-path breadcrumbs (the v1.21.95 LPS shared-edition no-op, edition-diag
# OSError, edition_key_for_rating_key rk-miss) and the #1 coverage gap closed
# (worker._do_relink cross-edition source-pull, was source-pinned only).
# Remaining coverage follow-ups (bounded by the v1.21.94 lint + v1.21.99 unit
# test): behavioral tests for plex_enum.reconcile_placement_paths' edition JOIN
# and the plex_upload over-ceiling restore endpoint.
# v1.22.1: code-review follow-ups on the v1.22.0 cut (3 findings rolled in).
# (1) SET URL's pre-enqueue download-cancel was title-wide while the enqueue
# it precedes is per-edition (v1.22.0) — a SET URL on one edition/section
# cancelled a sibling's pending download. Scoped it to the row's section +
# edition with the same COALESCE(json_extract payload edition_key) shape the
# re-download cancel uses (v1.21.82) — the one site that arc missed. (2) The
# v1.22.0 edition_key_for_rating_key rk-miss WARNING was unconditional but the
# fn runs per-row in the bulk-download loop → warn-once-then-debug (v1.17.11
# hot-path rule). (3) the v1.22.0 test version-pin used the narrower "1.2
# prefix → widened to "1." to match every sibling pin (survives major rollover).
# v1.22.2: root-caused the recurring "LET PLEX SERVE does nothing" on Watchmen
# editions to a JS rating_key omission — NOT a backend edition-scoping bug (the
# in-container worklist matched the midnight placement when given the right rk).
# The purge-revert-to-plex SOURCE-menu item shipped WITHOUT rk: it.rating_key
# (its adopt-and-let-plex-serve sibling had it), so letPlexServeFlow re-derived
# the row via an ambiguous (mt,id,section) .find() → first edition → a sibling
# with no placement → placements_total:0. Added the rk to the menu item + flow,
# resolved the row by clicked rk, and edition-scoped the adopt+LPS unplace too.
# v1.22.3: DOWNLOAD PLEX BACKUP wrote the captured bytes with
# abs_path.write_bytes() — an in-place open('wb') that TRUNCATES the existing
# canonical theme.mp3, needing WRITE on that file. On the user's Unraid the
# themerrdb canonical isn't writable by the container user, so the overwrite
# EACCES'd (rk=417813/417795 PermissionError) — surfaced to the UI as the WRONG
# "Plex wouldn't serve those bytes" (Plex served them fine: http 200, 2.9MB).
# Now writes a temp sibling + os.replace (dir-write only), matching placement's
# atomic pattern; edition-scoped the sha256 dedup guard; reworded the JS error.
# v1.22.4: runtime PUID/PGID support + boot writability probe. The Watchmen/Hokum
# permission saga root-caused to motif being the only container pinned to uid 99
# (baked USER, no PUID handling) on a uid-1000 *arr/Plex stack — a template reset
# to --user 99:100 silently broke every write to the 1000-owned shfs share. New
# entrypoint (docker-entrypoint.sh) adopts PUID/PGID like linuxserver images +
# gosu-drops; Unraid template + README/compose switched from hardcoded --user to
# PUID/PGID env. main._probe_writability test-writes config_dir/themes_dir at boot
# and logs a loud uid-vs-owner WARNING instead of crash-looping a download.
# v1.22.5: entrypoint honors UMASK (default 022) alongside PUID/PGID, matching
# the Unraid/linuxserver convention. Unraid best practice is UMASK=002 (group-
# writable dirs 775 / files 664) so a 99:100 stack (Sonarr/Radarr/Plex) shares
# the /data tree WITHOUT 777. the user's stack is PUID 99 PGID 100 UMASK 002; this
# lets motif be a first-class citizen of that model with appdata staying 99:100.
# v1.22.6: entrypoint validates PUID/PGID are numeric. A template slip that
# passed the literal "PGID" into the value field made gosu fail ("unable to
# find group PGID") and crash-LOOP the container; now a non-numeric PUID/PGID
# falls back to the default (99/100) + a loud ERROR so motif still boots and the
# operator can fix the env var, instead of a dead container.
# v1.22.7: editions code-review fixes (3 verified data-loss/drift findings from
# the 5-agent audit). (1) INFO-card LET PLEX SERVE (purge-and-ack) built its
# unplace with section_id only → backend section-wide DELETE nuked EVERY
# edition's placement (the v1.22.2 bleed at the 3rd LPS site); now threads the
# clicked rating_key. (2) REVERT's pre-enqueue download-cancel was title-wide
# while its enqueue is per-edition+section → cancelled sibling editions' pending
# downloads; now section+edition scoped (mirrors v1.22.1 SET URL). (3) the
# api_item INFO '' read-fallback was ungated while the unplace WRITE gates on
# single-edition → card claimed "placed" on editions LPS withholds; now gated to
# match (the user's Watchmen "card says placed, LPS does 0/0").
# v1.22.8: sync clears undecided pending_updates that propose a TDB theme the
# upstream no longer has — the title is still in TDB but its youtube_url went
# NULL, so an 'upstream_changed'/'new_theme_available' pending points at a
# removed theme and lingers as an un-actionable NEEDS WORK / !UPD prompt
# (the user's anime: Witch Hat Atelier, The Beginning After the End). Complements
# the v1.20.16 dropped-TITLE cleanup; urls_match (the legit U==TDB convert
# prompt) is deliberately left alone. Found in the NEEDS-WORK DB dig.
# v1.22.9: urls_match blue TDB↑ pill now honors a GLOBAL ('' section)
# user override. _has_user_override_sql gated the urls_match no-op branch
# on a strict section match (uo.section_id = pi.section_id), but a user
# override can be global (section_id='') — the legacy/cross-section shape
# SRC=U already honors via its own COALESCE(per-section, '') fallback. So a
# row in section N with its override at '' rendered a GREEN TDB pill (the
# urls_match branch failed _has_user_override) while SRC said U and NEEDS
# WORK ranked it — sort/pill drift. the user's anime (Berserk: The Golden Age
# Arc tmdb 211057, Fate/Strange Fake 229858: plex_items+local+placement at
# section '3', override+urls_match pending at ''). Added the '' fallback to
# the shared helper so all 11 urls_match sites (row pill, tdb_pills=update /
# =tdb filters) surface the blue ↑ "you could swap U→T" accurately. the user:
# "if the user uploaded theme is the same as a possible themerrdb theme you
# could be using then it should be a blue pill tdb."
# v1.22.10: NEEDS WORK (attention) sort now mirrors the pill's actionable
# gate. the user's v1.22.9 follow-up: The Beginning After the End / Witch Hat
# Atelier ranked at the TOP of NEEDS WORK while showing a GREEN pill — their
# pending is a no-op (upstream_changed whose TDB theme lost its youtube_url:
# real_diff false → pill correctly green), but the attention sort's priority-2
# branch gated only on decision=pending + a-pending-exists + presence, NONE of
# the pill's actionability gate, so it ranked the green-pill row NEEDS WORK
# anyway (same sort-vs-pill drift class as v1.19.86 priority-3). Extracted the
# pill's gate into _pending_update_actionable_sql and pointed ALL 12 surfaces
# at it — the 2 pill columns, tdb_pills=update/=tdb filters, attn_pills=update
# filter, 7 count subqueries, AND the attention sort — so the sort can't drift
# from the pill again. Behavior-preserving refactor (the gate logic was already
# identical at every inline site); the only semantic change is the sort gaining
# the gate it never had. The url-less stale pendings ALSO get swept by the
# v1.22.8 sync sweep — this read-path fix makes the row honest the instant the
# pill goes green, independent of sync timing.
# v1.22.11: user-uploaded MP3 INFO cards stop posing as URL-sourced. the user's
# LotR "Sam Takes a Step" upload (green TDB pill, playing an uploaded file)
# showed the TDB URL as "applied url", the TDB video id, a YouTube thumbnail +
# "watch on YouTube" link, and a no-op REVERT hint — all because isUrlSourced
# grouped 'upload' with themerrdb/url, so currentUrl fell back to t.youtube_url
# when no override existed. currentUrl drives the applied-url row, the video-id
# row, the thumbnail/watch link (ytUrl/ytId), and the revert comparison. Now
# 'upload' is file-sourced like M/A (currentUrl '' → applied-url '—', no
# thumbnail/watch, video-id '—') + an upload-specific revert hint replaces the
# URL-centric no-op wording. The "themerrdb url" row + the "downloaded …
# source_video_id=" provenance row still show what TDB offers + where the
# uploaded bytes came from. Completes the v1.19.59 fix (that tag corrected only
# the playback-source LABEL, not the URL/thumbnail/revert fields).
# v1.22.12: stop flagging new_theme_available on pure SRC=P rows. the user (after
# a sync found ~38 new anime themes): a wall of Plex-served anime lit up the blue
# TDB↑ pill / !UPD glyph / UPD badge / NEEDS WORK. v1.19.71 surfaced "TDB just
# published a theme" across SRC=—/P/U/A/M so the operator could take over, but
# anime is ~100% Plex-served + TDB's catalog keeps growing → every sync floods
# the attention surfaces with rows motif doesn't manage, burying the ones it
# does. Now the new_theme_available branch of _pending_update_actionable_sql ANDs
# _not_p_row (read: silences pure-P rows everywhere — pill/glyph/UPD/NEEDS WORK/
# filters/counts — including ones detected by prior syncs) + sync skips WRITING
# new_theme for pure-P rows (detection: `elif plex_supplies and not has_content`).
# SRC=—/U/A/M still surface (they pass _not_p_row). Theme stays discoverable; the
# operator can DOWNLOAD TDB per-row from the SOURCE menu to take over.
# v1.22.13: center the lone "CURRENT = PROPOSED" tile in the // THEMERRDB MATCH
# INFO-card panel (the urls_match single-tile diff). .diff-tiles-single was
# `grid-template-columns: 1fr 1fr` — a 2-col grid holding one tile, so it parked
# in the LEFT column (the user's My Ribdiculous Reincarnation screenshot). Now a
# single capped track (minmax(0, 360px)) + justify-content:center. CSS-only.
# v1.22.14: two code-review fixes on the v1.22.9-12 work. (1) The NEEDS WORK
# attention sort priority-2 PRESENCE gate was missing the new_theme_available
# escape its four sibling surfaces carry — v1.22.10 unified the ACTIONABLE
# sub-gate onto the sort but left the PRESENCE sub-gate to drift, so a SRC=—
# row sync just found a new TDB theme for showed the blue ↑ pill + UPD count +
# both update filters yet sank to ELSE 7 (bottom of NEEDS WORK) instead of
# priority-2 — the exact sort-vs-pill class v1.22.10 set out to kill. Added the
# escape to the sort's presence gate. (2) v1.22.11 special-cased the revert hint
# on the literal lfSource==='upload'; generalized to isFileSourced (upload/
# adopt/plex_cloud) so adopt/plex_cloud rows stop hitting the URL-centric
# "re-apply the current URL" wording, AND added a live-revert guard so the card
# no longer claims "unavailable" when a user-kind previous URL genuinely differs
# (has_previous_url ignores source_kind → REVERT is live in the menu). Plus test
# hardening: a behavioral sort test for (1), an isFileSourced/live-revert pin for
# (2), and the two fixed-window helper-body pins re-anchored on the next def.
# v1.22.15: host→container path translation on two Unraid-broken filesystem
# checks the audit found. (#1) worker.py over-ceiling sidecar fallback did
# Path(folder_path)/"theme.mp3" + mkdir(parents=True) on Plex's RAW host path
# (/mnt/user/... on the user's Unraid) — mkdir CREATED a phantom container-local
# tree Plex never serves, then logged success, so the >10MB theme silently went
# nowhere. (#4) plex_enum.py theme-lost reaper Tier-2 sidecar_fs check used the
# same raw Path().exists() — ALWAYS False on Unraid — mis-tiering a recoverable
# on-disk sidecar to no_fallback and firing the wrong "theme lost, no recovery"
# alert. Both now route through _candidate_local_paths like every sibling
# sidecar check; if no container dir resolves, #1 logs + falls through to the
# loud terminal-fail instead of a silent phantom write.
# v1.22.16: action-layer audit fixes. (#2 DATA-LOSS) bulk ACCEPT ALL deleted
# user_overrides SECTION-WIDE — one edition's accept wiped EVERY edition's
# override for the title (the per-row api_accept_update was edition-scoped in
# v1.21.87; the bulk path was missed). Scoped the override fetch/DELETE + both
# enqueues to the row's edition, tracking the global-'' retarget like the per-
# row path. (#5 SILENT FAILURE) api_accept_update discarded _enqueue_download's
# return; a 0-enqueue (no included Plex section owns the item) left the row
# "accepted" with the override deleted + no download + no rollback, behind
# HTTP 200 — now captures the count, warns, and returns enqueued_sections.
# (#6 CONTRACT) backup_cloud_theme promised "never raises" but the section-
# subdir resolve caught only RuntimeError — a sqlite OperationalError
# ("database is locked") escaped and aborted the whole bulk-backup batch; now
# any resolve error lands in the return dict so the loop skips one + continues.
# v1.22.17: HAMA/anime theme_id-linkage (audit cluster B). Four sync DETECTION
# sites keyed strictly on plex_items.guid_tmdb — but HAMA/AniDB anime (the bulk
# of the user's library) resolve to TVDB GUIDs, so guid_tmdb stays NULL while
# resolve_theme_ids links plex_items.theme_id (the v1.15.142 _enqueue_download
# pattern). So every theme_id-only row was silently excluded from: in_plex
# (new_count + the new_theme_available prompt), has_sidecar (M-row content),
# _plex_supplies_theme (the v1.22.12 pure-P SUPPRESSION precondition), and the
# end-of-sync prune's plex-presence check (which would reap a live HAMA prompt).
# Each now ALSO matches via theme_id linkage as an OR (superset — the guid_tmdb
# path is preserved, so nothing that matched before regresses). Net: when
# theme_id is linked (post-resolve / orphan-promotion / re-detection) a HAMA
# M/U/A row finally gets its prompt, a HAMA pure-P row stays suppressed, and a
# live HAMA prompt survives the prune.
# v1.22.18: worker placement-path edition-scope cluster (audit MED #8-#11).
# (#8) _do_place's post-place plex_items "hint" UPDATE (local_theme_file=1 /
# has_theme on a skipped place) was section-scoped but NOT edition-scoped, so a
# skipped place on one edition stamped EVERY sibling edition's row in the
# section (same tmdb_id, distinct {edition-X} folders) — now edition-scoped when
# the job named an edition. (#9) _do_place's pre-place +P capture read the raw
# pi["has_theme"]=1, ignoring the stale-cache override that demotes
# cached_has_theme when verified_ok=0 (Plex's /theme 404s — the claim is a lie);
# it stamped plex_independent_theme=1 on a broken-theme row → a phantom +P dot.
# Now gates on cached_has_theme + a COALESCE(verified_ok,1)<>0 SQL guard. (#10)
# _do_relink's success UPDATE (placement_kind='hardlink') omitted edition_key
# though the sibling DELETE includes it — media_folder is per-edition distinct
# for sidecars but plex_upload editions share media_folder='' (PK now fully
# pinned). (#11) _do_place's successful-place mismatch-clear keyed on
# place_edition_key (REQUESTED) while the placements INSERT in the same txn used
# placed_edition_key (PHYSICAL landing) — they split when a place lands in a
# different edition's folder than asked; both now key on placed_edition_key.
# v1.22.19: INFO-card no-op pending suppression in the edition-first branch
# (audit MED #7, contract-drift class). v1.12.119 added a no-op suppression to
# api_item's pending resolution — when a pending's new_youtube_url already ==
# the row's applied URL, accepting would just re-download the same URL, so the
# card hides it (the row pill hides it too). v1.21.68 later added the EDITION-
# FIRST resolution branch (rating_key → _info_edition) but never carried the
# suppression, so for any edition-resolved row (the common path, rk sent) a
# no-op pending surfaced a PROPOSED CHANGE in the INFO card that the row pill
# suppresses — a card-vs-pill disagreement. Mirrored the suppression into the
# edition-first branch; 'urls_match' stays exempt (the U→T flip is meaningful
# even when URLs match). (The audit's clear_override "partial-args" item was
# skipped — that endpoint's UI surface was removed in v1.15.97, so it's
# unreachable defensive API surface, not a live bug.)
# v1.22.20: plex.py get_item_paths non-200 breadcrumb (audit class-9, the
# confirmable slice of the plex.py log-level findings). get_item_paths backs the
# placement pipeline's media-folder lookup; its early return on a non-200 status
# (`if r is None or r.status_code != 200: return []`) was SILENT — a Plex 404 /
# 500 / auth response lost the media folder with no log, so a placement quietly
# skipped the item. The adjacent malformed-JSON branch already got a WARNING in
# v1.17.9, and _get only logs TRANSPORT errors (None), never HTTP error
# STATUSES, so this was the last silent []-return in the folder lookup. Split
# the None branch (already logged by _get) from the non-200 branch and added a
# WARNING to the latter. (The audit's other LOW plex.py log-level hints didn't
# map to a concrete silent-swallow — the theme-mutation paths already WARN per
# the v1.15.107 sweep — and the cloud_theme_backup "force-backup" item is a
# feature-add, not a bug: backup_cloud_theme has no force param + the identical-
# bytes dedup-skip is correct. Both left for the original audit detail.)
# v1.22.21: download-failure rollback is edition-scoped (edition-audit data-
# loss). A 4-agent sweep of every mutation on the edition-PK tables surfaced
# this: ACCEPT UPDATE / REVERT eagerly delete the clicked edition's override +
# flip its pending decision, stamp a `rollback` recipe, and queue the download;
# on TERMINAL download failure worker._run_rollback_safe undoes the prep — but
# the recipe dropped edition_key, so the re-pend hit the section with no edition
# filter (resetting a sibling edition's decision) and the override-restore
# INSERT omitted the edition_key column → defaulted to '' → a NON-'' edition's
# user URL was deleted by the endpoint then RESTORED ONTO THE STANDARD row,
# clobbering its override and losing the real one. The endpoints
# (api_accept_update, api_accept_all_updates, api_revert) now stamp
# rollback['edition_key']; _run_rollback_safe threads it into both WHEREs + both
# INSERT column lists. Default '' keeps old in-flight jobs at standard scope.
# v1.22.22: three section-keyed-without-edition bleeds (edition audit). (#3)
# api_unplace_item's plex_items flag flip (local_theme_file=0 / verified_ok=NULL)
# used the section-wide pi_where while its placements DELETE + local_files UPDATE
# were already edition-scoped — LPS on one edition dropped a sibling edition's
# SRC pill (M/A→P/–) until the next plex_enum; now scoped to the clicked
# rating_key (the plex_items PK, exact even for orphans). (#1) api_manual_url
# (SET URL) urls_match path cleared + re-inserted pending_updates for the whole
# section, wiping a sibling edition's per-edition accepted/declined decision
# (v1.21.81); the DELETE + re-INSERT now carry _edition_key. (#4)
# scanner._classify_and_record's `theme_id = ?` disjunct is title-wide (themes
# keyed by media_type+tmdb_id), so a sidecar found in ONE edition's folder
# flipped local_theme_file=1 on EVERY edition's row; the theme_id branch is now
# edition-scoped to the scanned folder's edition (the folder_path branch was
# already edition-specific). #3/#4 self-heal on the next plex_enum but showed a
# transient wrong SRC pill; #1 is a genuine per-edition decision wipe.
# v1.22.23: _drop_motif_tracking spares sibling editions (edition audit LOW #9).
# PURGE/FORGET/UNMANAGE clear motif's per-title decisions (user_overrides +
# pending_updates) via _drop_motif_tracking when they reach the last file-
# bearing section. That helper deleted TITLE-GLOBALLY, so on a multi-edition
# title, purging the last file-bearing edition also wiped a sibling edition's
# OWN per-edition override/decision (e.g. a pure-P "Director's Cut" with a
# manual URL). Added an acted_edition param: when an edition-scoped action
# (rating_key present) calls it, scope to `edition_key = '' OR edition_key =
# <acted>` — still clearing the title-global '' decisions (the v1.12.57 clean-
# slate: a stale '' override re-applies on next download) + the acted edition's,
# but SPARING other editions. UNMANAGE + FORGET last_section pass their resolved
# edition; DELETE (whole-title nuke) + the legacy global branches keep the full
# title-wide wipe. (The other 4 LOW edition-audit items are non-actionable:
# #8 accept's no-section else-branch is the by-design context-less fan-out with
# no edition to scope to; #5/#6/#7 are one-shot marker/loss-pattern-gated
# recovery walkers that already ran or recover from the now-impossible v1.18.0
# cascade — fixing them churns dead code.)
# v1.22.24: quarterly dependency-floor review. apprise floor bumped 1.10.0 →
# 1.11.0 (latest stable, released 2026-05-29 — new service handlers +
# transport-URL evolution). yt-dlp floor left at 2026.3.17: re-verified against
# PyPI + GitHub releases on 2026-06-06, still the most recent stable (no newer
# build exists), so no bump. CLAUDE.md "Last bumped" notes refreshed for both.
# v1.22.25: edition-correctness hardening for two re-triggerable recovery
# walkers (edition-audit LOW #5/#7). Both are one-shot, runtime_settings-marker-
# gated (an operator can clear the marker to re-run); their placements joins were
# section-only, so on a multi-edition title they bled across siblings. #7
# maybe_clear_plex_upload_indep_flag cleared +P (plex_independent_theme) on EVERY
# edition's row when ANY edition had a plex_upload — now the join carries
# p.edition_key = pi.edition_key. #5 maybe_cleanup_duplicate_placements deleted a
# missing-file hardlink if a plex_upload existed in the same SECTION (even a
# different edition) — now the self-join carries pu.edition_key = p.edition_key.
# Unreachable on a normal install (marker already set) but correct if re-run on a
# multi-edition library. (#6 maybe_recover_post_v55_data_loss left as-is: gated
# by _detect_loss_pattern which only the now-impossible v1.18.0 cascade produces,
# so it can't fire even via manual re-run — hardening its multi-column rebuild
# edition-derivation is disproportionate for provably-dead code.)
# v1.22.27: revert v1.22.26 (the Phase 1 edition-backfill admin endpoint +
# editions.py find/apply helpers + tests). the user manually re-keyed the ~5
# LotR/Hobbit mis-keyed titles, so the backfill tool is moot — removed the extra
# admin surface rather than carry a dead-but-harmless endpoint. The v1.22.26 tag
# stays as history; the forward path (_do_place writes folder-derived
# edition_key, v1.21.55) and the v1.21.68/.93 '' fallback already keep new state
# correct, so nothing depended on the backfill.
# v1.22.28: close the last DEFERRED coverage follow-up noted in the v1.22.0
# rollover — a behavioral test for reconcile_placement_paths' v1.21.94 edition
# JOIN (it was only guarded by the v1.21.94 edition-blind lint + a v1.21.99 unit
# test). Pins end-to-end that a stale '' (standard) placement reconciles to ITS
# OWN edition's folder, not cross-producing onto a sibling edition's {edition-X}
# folder (the pre-fix wrong-edition write), + a control that a correctly-placed
# edition isn't spuriously "moved". Test-only; no production change. (The
# over-ceiling-restore half of that follow-up already had test_v1_21_99.)
# v1.22.29: full-codebase audit batch 1 — two HIGH silent-data-loss fixes from
# the 8-agent sweep. (plex.py) enumerate_section_items /
# enumerate_collections_for_section broke pagination on `container_size <
# page_size` BEFORE the authoritative total_size check, so a short page
# mid-section (Plex's X-Plex-Container-Size is advisory) truncated the section →
# the v1.18.89 reaper deleted the "missing" plex_items rows (moderate truncation
# slips under its 20% guard). total_size is now primary; the short-page break is
# the total_size-unavailable fallback. (sync.py) _fetch_index + the snapshot
# index counted ONLY transport/parse errors as a failed page — a page that 200s
# with `[]` (CDN stale-empty) or a non-list body passed silently →
# index_incomplete=False → the full-walk drop sweep mis-stamped live themes as
# TDB-dropped (the v1.21.38 class through the 200-with-bad-body door). Now a
# non-list/empty body counts as failed (errs SAFE).
# v1.22.30: full-codebase audit batch 2 — worker job-loop integrity (Tag B).
# (downloader) a pre-existing theme.mp3 was returned as a SUCCESSFUL download
# even when 0-byte (prior crash mid-ffmpeg/OOM) → broken theme hardlinked behind
# a green ✓; now size-checked + stale 0-byte removed/re-downloaded. (worker) the
# table-wide running→pending zombie-reclaim ran at the top of EVERY thread's
# run() — _supervised re-enters run() on any mid-life thread crash, so a
# restarting thread reset every live sibling's in-flight job → duplicate
# download/place race; moved to _reclaim_orphan_jobs, run ONCE pre-spawn (aged
# orphans still caught by the scheduler stuck-job sweep). (worker) TokenBucket
# rate=0 (env/yaml bypassing the /settings ≥1 check) → ZeroDivisionError killed
# the download thread on its 2nd acquire; _fill_rate floored at 1/period.
# (worker) an unknown job_type went through the retry ladder, burning the attempt
# budget on a job no handler can run; now terminal (no retry).
# v1.22.31: full-codebase audit batch 3 — backend hardening (Tags C/G/D).
# (api adopt-from-plex) atomic temp-then-os.replace so a double-FS-failure no
# longer leaves the row with no canonical file; + 409 instead of fake ok:True
# when 0 sections adopt. (api clear-override) SELECT+DELETE+audit wrapped in one
# transaction (was autocommit → a failed audit left a deleted override with no
# trail). (api delete-item) empty-dir rmdir swallow gets a breadcrumb. (api
# probe-themes) up to 10 synchronous Plex calls moved off the event loop via
# run_in_threadpool (v1.21.20 block class). (notify) _send_embedded/_send_external
# failure reasons logged WARNING not DEBUG so a silent channel is diagnosable.
# v1.22.32: full-codebase audit batch 4 — reaper false "theme lost" (Tag E).
# plex_enum's still_p survivor check (gating the plex_theme_lost notification)
# INNER JOIN'd themes on pi.theme_id, so a SURVIVING sibling Plex still themes
# (has_theme=1) but motif never linked (theme_id NULL — a multi-edition rk, or a
# HAMA match TDB didn't cover) was invisible → the reaper fired a FALSE "theme
# lost" while Plex was still serving the title on that sibling. Now mirrors the
# candidate set's COALESCE: also matches survivors by plex_items.guid_tmdb
# directly (theme_id link optional), with the show↔tv media_type swap.
# v1.22.33: full-codebase audit batch 5 — scheduler/orphan-scan resilience
# (Tag H, 2 reachable items). (scheduler) _enqueue_sync ran its "don't
# double-enqueue" SELECT + INSERT in autocommit → two enqueuers racing (cron vs
# manual /api/sync/now) could both pass + both insert; now one BEGIN IMMEDIATE
# txn. (orphan_scan) one rk's unexpected get_themes raise aborted the whole
# diagnostic sweep (every later placement unscanned); now a per-row guard +
# progress-cb guard. The other 3 Tag-H candidates were refuted (tmdb
# negative-cache already implemented; _stuck_job_sweep lexical ISO compare is
# correct; placement partial-temp self-heals via the pre-clean).
# v1.22.34: full-codebase audit batch 6 — frontend feedback (Tag I). (app.js)
# DOWNLOAD PLEX BACKUP's .then() handled only res.ok truthy → a 200 with
# {ok:false} left the optimistic placeholder hanging with no alert; now an else
# clears it + alerts. (app.js) a 0-enqueued bulk download showed only a fleeting
# "0 QUEUED" → now an explicit alert. The third candidate (bulk-LPS finishWatcher
# hidden-tab edge) left as-is — delicate stateful watcher with a 30-min timeout
# backstop; a timing fix risks premature button-reset under queue backlog.
# v1.22.35: settings design — uniform reading measure. Each section drifted
# between a full-width intro paragraph + save row and a 720px form (the user's
# "full page vs half page text"). New --measure-form token (720px) shared by
# .block-intro + .form-grid + the standard .form-actions save row, so every
# section is one tidy left-aligned column; wide-content (.block-body-flush)
# tables opt out. Nested-in-form save rows lose their double-gap; two inline
# margin overrides folded into shared rules. CSS-led, no markup churn beyond
# dropping the inline styles. Guard: test_v1_22_35_settings_measure.
# v1.22.36: library stuck-row reconciler. A download/place would finish but the
# row's DL/PL chip stayed frozen (pulsing) until a manual page refresh — the 2s
# rapid-poll AND the 30s background reload both skip on a text selection / open
# dialog (the v1.10.7 interaction guard), so a stray selection or open row-menu
# froze the row indefinitely. New 6s watcher fires loadLibrary ONLY when a
# rendered row still claims job_in_flight while the backend op-queue has gone
# idle (the exact stale-frontend condition), bypassing the guards; hash-skip
# makes it a no-op otherwise. No-op in normal operation.
# v1.22.37: status-bar progress bars now reach 100% at completion. Pre-fix a
# finished op left the running set the instant it hit the final count, so the
# card DROPPED its bar (showLiveSections=false) and the topbar mini-bar switched
# to idle — the 100% frame never rendered (the user: "bars jump from some % to
# done without ever hitting 100%"). Now pctOf returns 100 on done; finished
# real-bar cards render a full 100% bar; and the mini-bar holds a 100% DONE
# flash for ~1.5s (mirroring the ✓ DONE button flash) before going idle. The
# per-phase bar reset was left as-is per the user's scope choice.
# v1.22.38: holistic-audit batch — multi-edition unplace data loss (frontend).
# The per-row DEL ("Remove from Plex folder") + bulk ADOPT+LET-PLEX-SERVE sent
# /unplace with section_id only; the backend's "absent rating_key = section-wide
# fan-out" branch then physically unlinked EVERY edition's theme.mp3 in the
# section. DEL on Theatrical nuked Extended/Sam. Both app.js sites now thread the
# row's rating_key (same class the LPS sites fixed in v1.21.61/.93; these two
# were missed). unplaceTheme gained a ratingKey param.
# v1.22.39: holistic-audit batch — HAMA guid_tmdb-NULL theme_id-linkage misses
# (the 5th + 6th sites of the v1.22.17/.32 class). (sync) the url_changed
# has_sidecar check keyed on guid_tmdb ONLY → a HAMA/anime M-row (guid_tmdb NULL,
# theme_id-linked) re-downloaded TDB bytes OVER the user's manual sidecar every
# cron sync; now matches via theme_id linkage like the is_new twin. (plex_enum)
# the reaper Tier-2 sidecar_db check keyed on guid_tmdb ONLY → a HAMA survivor
# mis-tiered to no_fallback ("theme lost, no recovery"); now LEFT JOIN themes +
# OR theme_id, mirroring the v1.22.32 still_p fix.
# v1.22.40: holistic-audit batch — destroy-then-fail FS ordering (atomic).
# (worker _do_download) on a source_video_id change should_unlink unlinked the
# staged theme.mp3 BEFORE the re-download — which frequently FAILS (the new URL
# points at a dead/private video) — leaving the row with no canonical on disk.
# Now rename-aside + restore-on-failure + drop-on-success. (placement
# force_overwrite) unlinked the existing theme BEFORE the place, so a place I/O
# error left the folder themeless; now place atomically first (temp+os.replace),
# then remove any differently-named leftover.
# v1.22.41 (audit SECURITY): events.py scrubber didn't redact URL credentials
# buried in list/nested-list/list-string detail values — only top-level dict
# values + dict-in-list were covered. A `{"urls": ["https://user:pass@host"]}`
# or `{"a": [["https://user:pass@host"]]}` leaked creds into the events table.
# Extracted a shape-aware _scrub_value helper that recurses to any depth.
# v1.22.42 (audit): three async endpoints did synchronous Plex round-trips (and
# a 50MB read) directly on the event loop, freezing every concurrent request:
# api_set_override_intent PROMOTE (get_themes + read_bytes + upload, inside the
# txn), api_unplace_item LPS restore loop (per-placement get_themes/delete/
# re-upload), and the _teardown_plex_api_artifacts_for_placements helper called
# from forget + delete. All blocking calls now run via run_in_threadpool.
# v1.22.43 (audit): fake-success + transient-error-poisons-cache. api_override
# SET URL dropped _enqueue_download's count + returned ok:True even on a
# 0-enqueue (override saved, no theme fetched, green toast) — now surfaced.
# tmdb _lookup_by_tvdb/_lookup_by_imdb returned None on a transient non-200 →
# cached as a 7-day NEGATIVE, silently killing HAMA resolution for a week — now
# raise TMDBError like _search. notify.dispatch_coalesced dropped the
# leading-edge single if _arm_coalesce_timer raised — now falls through to send.
# v1.22.44 (audit, plausible-finding follow-ups): F2 ops.js cancel handlers reset
# the button on a failed (non-2xx) cancel with NO poll/message → silent stale
# button; now always force a reconciling poll. W6 scheduler stuck-job-sweep
# last_error claimed "will retry per backoff schedule" but sets status='failed'
# terminally — corrected. (W3/S5 refuted, S4 deferred — see journal.)
# v1.22.45: sync_completed notification groups the New + Updated lists under
# Plex-section sub-headers (Movies / 4K Movies / TV Shows / Anime / Anime
# Movies …) so the operator can tell which library each theme belongs to at a
# glance. updated_titles now carries media_type+tmdb_id; the New-list EXISTS
# gate widened to pi.theme_id so HAMA/anime new themes are listed, not just
# counted (guid_tmdb-blind class).
# v1.22.46 (Tires repro): is_new SRC=— auto-download never fired with the toggle
# ON. _enqueue_download's section-ownership query (v1.15.142 HAMA rewrite) matched
# ONLY via pi.theme_id linkage, but during a sync's batch processing a brand-new
# theme's pi.theme_id isn't linked yet (resolve_theme_ids runs after the batches)
# → 0 sections → silent no-enqueue; the post-sync enum then missed it too (the
# sync's own resolve pre-linked theme_id, so not "newly_linked"). Now matches
# guid_tmdb OR theme_id (the canonical pattern) so new themes enqueue before the
# link forms while HAMA/anime (guid_tmdb NULL) keep working.
# v1.22.47 (the user's orphan-with-imdb question): resolve_theme_ids now matches a
# REAL ThemerrDB theme by imdb_id. The only imdb pass filtered
# upstream_source='plex_orphan' — re-bonding to a synthetic orphan but never a
# real theme — so an imdb-only Plex row (guid_tmdb NULL) whose real theme existed
# fell to the fragile title+year fallback and orphaned on a title/year mismatch.
# New sql_imdb_real pass (real themes, imdb_id) runs after tmdb, before the orphan
# re-bond + title; gated theme_id IS NULL (only links unlinked rows).
# v1.22.48 (the user's orphan-with-imdb question, diagnose-first): read-only admin
# diagnostic GET /api/admin/diagnostics/orphan-imdb — counts plex_orphan themes
# carrying an imdb_id + bounded/cached TMDB /find probes to report how many would
# RESOLVE to a real tmdb_id (net-new vs would-merge) vs. titles TMDB lacks. Gates
# whether an imdb→tmdb de-orphan walker is worth building. No data changes.
# v1.22.49 (the user's prod diagnostic: ~98% of imdb-bearing orphans resolve): the
# de-orphan walker. app/core/deorphan.py resolves each plex_orphan theme's imdb →
# real tmdb via TMDB and RE-KEYS the synthetic negative tmdb_id to the real one
# across themes + local_files + placements + pending_updates + user_overrides
# (FK-deferred, mirroring sync._upsert_theme's promotion). Skips collisions; keeps
# upstream_source='plex_orphan' (theme is still manual, not from TDB). POST
# /api/admin/deorphan-imdb?dry_run= (default true). Idempotent + per-row safe.
# v1.22.50 (the user's prod run hit 1 persistent error on 'Hokum'): de-orphan
# walker hardening. (a) pre-delete FK-invalid leftover child rows at the target
# tmdb before the re-key (the exact UNIQUE-conflict the sync-promotion already
# guards against — clash2 confirms no real theme is there, so they're junk);
# (b) class-8 retry on `database is locked`; (c) record the REAL per-row outcome
# in samples + surface the error reason (the label was set optimistically pre-txn
# so an errored row mislabeled as "rekeyed").
# v1.22.51 (the user's 'Hokum' FK error): previous_urls is a 5TH table FK'd to
# themes(media_type,tmdb_id) that neither the de-orphan re-key NOR
# sync._upsert_theme's orphan promotion moved — an orphan carrying a REVERT/
# url-change history row failed the re-key with FOREIGN KEY constraint failed at
# COMMIT. Both paths now DELETE-at-target + UPDATE previous_urls alongside the
# other four child tables. Latent in the promotion since previous_urls landed.
# v1.22.52 (forward-fix + relabel): _create_orphan_theme resolves imdb→tmdb via
# TMDB BEFORE minting a synthetic negative id (549/555 of the user's orphans were
# real titles minted synthetic only because no resolver ran here) — new manual/
# adopted themes key to their real identity from birth; all failure modes fall
# back to the synthetic mint. Cloud-backup orphan mints now stamp guid_imdb so
# minted rows stay resolvable. INFO card renders upstream 'plex_orphan' as
# "local (manual / adopted — not from themerrdb)" instead of the internal jargon.
# v1.22.53 (the user: "the 8 collisions — should we fix?"): read-only diagnostic
# GET /api/admin/diagnostics/orphan-collisions — per skipped collision, reports
# what the orphan + target theme records each hold (children counts across the 5
# FK'd tables) and which record the library row links to, with a per-row hint
# (SPLIT TRACKING / empty duplicate / etc). Gates the merge decision. No writes.
# v1.22.54 (the user: "my override wins"): collision merge. merge_orphan_collisions
# consolidates each duplicate orphan into its real-tmdb record: user_overrides +
# local_files ORPHAN WINS on same-slot collision (the user's deliberate choice is
# authoritative — restores the U-row state; TDB stays one ACCEPT away via the
# normal prompt); placements latest-placed_at wins; pending_updates/previous_urls
# target wins. Moved rows get theme_id re-pointed; plex_items + scan_findings
# re-point BEFORE the husk delete (both FKs ON DELETE SET NULL). POST
# /api/admin/deorphan-merge-collisions?dry_run= (default true). Idempotent.
# v1.23.50: glossary + legend chip colors audited against the real row chips.
# the user: the HL (hardlink) icon showed grey in the // GLOSSARY + in-context
# LEGEND, but a hardlink on a row is GREEN. The audit found the hand-set
# gc-*/gg-* palette (v1.23.43) wrong in many places — HL grey (→green), SRC M
# red (→magenta), SRC P lemon (→amber), LINK M amber (→red), TB green-bright
# (→green-pale), C sharing HL's class, AB/PB/UB borrowing their primary's shade,
# and the ! / ↺ flags both cyan (→blue / amber). Every gc-/gd-/gg- color now
# mirrors its real .link-badge-* / .link-glyph-* / .state-pill-btn-* /
# .title-glyph-* counterpart EXACTLY; a drift guard (test_v1_23_50) pins each so
# they can't diverge again. CSS + glossary-markup only.
# v1.23.51: the // LEGEND toggle (next to NEEDS WORK) now mirrors its sibling
# .chip instead of being a green-tinted filled box with tighter padding —
# the user's deploy screenshot: it "looks off" rendering as a different species of
# control beside the transparent NEEDS WORK chip. Now transparent + dim +
# outlined (+ appearance:none to kill any native-button chrome), with the green
# accent reserved for the open/active state. Joins the shared :focus-visible
# keyboard-outline group too. CSS-only.
# v1.23.52: // HELP topbar toggle unifies on green + the IDLE/HELP/logout trio
# is evenly spaced. the user (deploy): HELP was cyan on hover but a green fill when
# active — "unify on one" → hover is now green too (cyan stays reserved for
# info/update semantics). And the trio was unevenly spaced (help margin-left 8px
# → 16px to IDLE; logout margin-left 14px → 22px to HELP); both ad-hoc margins
# dropped so the single .topbar-status gap spaces all three evenly + pulls
# help+logout closer. CSS-only.
# v1.23.53: the chip LEGEND toggle — robust render + always available. the user
# reported it as an unstyled white box for a 3rd deploy running: the cause is a
# stale/partial-cache app.css missing the new .library-legend-pill rule, so the
# button fell back to the native UA button (white) AND lost its help-mode display
# gate. Fix is cache-proof: the toggle now carries the long-standing .chip class
# in the markup, so it renders as a proper outlined chip from ANY cached
# stylesheet; the pill rule is slimmed to just the caret + open accent. Also
# un-gated from help mode entirely (toggle + panel) so the chip legend shows on
# every library page (the user: "should display if help is pressed or isn't").
# v1.23.54: code-review follow-ups on the v1.23.50-53 UI cluster (self-review,
# all low-severity, no functional/data bugs). (1) an OPEN legend toggle's green
# label reverted to dim on hover — .chip:hover (color:--fg) is equal-specificity
# and later in source than .library-legend-pill.open, so an explicit .open:hover
# rule now holds the green. (2) two stale comments (base.html help-toggle margin,
# library.html legend help-gate) corrected. (3) strengthened the v1.23.50 drift
# guard to also pin each chip's BORDER color token (was color-only — a wrong
# border -rgb token could have drifted silently). CSS + comments + test only.
# v1.23.55: legend/glossary SRC + LINK chips now mirror the real row chip's
# faint BACKGROUND tint too — the v1.23.50 audit pinned color + border but missed
# the fill, so the decode chips read as hollow outlines while the actual row
# chips (.link-badge-* / .link-glyph-*) are subtly filled (the user: "src colors
# don't match"). Added the exact rgba tint to gc-u/a/m/p + gc-hl/c/mm/pu/pb/tb/
# ab/ub (T + – stay fill-less, matching their rows). The drift guard now pins all
# three color axes — color, border, background.
# v1.23.56: STOP the parallel gc-* palette for the glossary/legend SRC + LINK
# chips — reuse the real row classes (.link-badge link-badge-* / .link-glyph-*)
# in the markup instead. The parallel palette drifted on every color axis (.50
# color, .54 border, .55 fill) AND its recolors were invisible behind a stale
# cached app.css, so the legend showed OLD colors while the years-stable
# link-badge-* did not (the user: "the T is not the same green"). Reuse = identical
# to the row by construction + immune to asset staleness. Dots/flags stay gc-.
# v1.23.57: collections filter panel reserves the ED row's height. Collections
# have no editions so the ED filter row is dropped, but that left the file-axis
# column (DL/PL/LINK/ED) one row shorter — the whole filter panel shrank and
# jumped size when switching between a library tab and collections (the user). A
# .pill-filter-spacer (min-height 22px = one ED chip row) holds the ED row's place
# on collections so both panels are the same height. CSS + one template branch.
# v1.23.58: align the glossary/legend chip-row definitions into a column. Since
# the decode chips reuse the real row classes (v1.23.56) they have varying widths,
# leaving the def text ragged. .help-gloss-grid is now a 2-col (auto 1fr) grid +
# the rows are display:contents, so the chip column sizes to the widest chip per
# section and every definition lines up — chips keep their natural row-identical
# size (the user's fixed-width-wrapper follow-up, done CSS-only). CSS only.
# v1.23.59: the v1.23.57 collections filter spacer was 22px (the standalone
# .ed-pill-btn height), but inside a .pill-filter-row every chip is 20px (the
# v1.12.48 shared height) — so collections rendered ~2px taller than the library
# filter panel (the user: "ever so slightly larger"). Spacer min-height 22px → 20px
# so the panels match exactly. CSS one-liner.
# v1.23.60: align the glossary/legend "no theme" – / "no placement" — dashes with
# the other SRC/LINK chips. They used help-gloss-chip gc-none whose box differs
# from the link-badge letter chips, so the dash glyph sat off the column (the user).
# Now they carry .link-badge (same box as T/U/A/M/P) + gc-none (muted color), so
# the dash centers in an identical box and lines up. Topbar DROP keeps help-gloss-
# chip (it aligns with the other topbar pills). Markup only.
# v1.23.61: code-review follow-up — the glossary/legend LINK chips reused only the
# .link-glyph-* MODIFIER, but the real rows use base+modifier
# (class="link-glyph link-glyph-*"). Inert today (the base only adds inherited
# font-family + an inert text-align) but it deviated from the "byte-identical to
# the row" invariant the v1.23.56 reuse rests on, and no test pinned it. Added the
# .link-glyph base to all LINK decode chips so they're truly identical to the rows;
# drift guard now pins base+modifier. (SRC chips already carried the base
# .link-badge.) Markup + test only.
# v1.23.62: holistic-audit safe-batch. (#1/#10) /api/tmdb/test +
# /api/admin/test-notification ran a synchronous network call (TMDB httpx /
# Apprise send) directly in the async body, freezing the event loop — now
# offloaded via run_in_threadpool + the v1.22.58 lint extended to catch the two
# method names (the PlexClient-only derivation missed them). (#6) GET /api/config
# + config validation masked/checked git_url + database_url but not db_url, the
# 3rd credential-capable sync URL → cleartext leak, now covered. (#5/#15) a
# user-cancelled sync (_JobCancelled) was caught by the worker's broad except →
# spurious "Sync failed" notification + held auto-downloads released and run
# anyway; now caught first + re-raised. (#8) three dispatched theme-loss/backup
# event kinds were missing from notify._EVENT_NOTIFY_TYPE → neutral 'info'; the
# two loss tiers now warn. (#18) the payload-parse warn-once was a process-wide
# bool muting every handler after the first corrupt payload — now per call-site.
# v1.23.63: holistic-audit cluster E — check-then-insert atomicity. Three enqueue
# paths ran a dedup-SELECT + INSERT in plain autocommit, so a manual action racing
# the cron (or two concurrent requests) could both pass the "already queued?" gate
# and double-insert. (#7) /api/sync/now now mirrors the cron _enqueue_sync (v1.22.33)
# one-BEGIN-IMMEDIATE guard. (#16) bulk download-batch + download-missing wrap each
# _enqueue_download in a PER-ITEM transaction (atomic dedup, short lock holds — not a
# whole-batch hold that would starve the worker across a large bulk). (#19) decline-all
# wraps each _set_pending_update_decision in a per-row transaction, consistent with
# accept-all. (#17 — accept-all's single BEGIN IMMEDIATE over its 167-line edition-
# scoped loop, a PLAUSIBLE lock-hold on a very large bulk — DEFERRED: narrowing it
# needs restructuring that heavily-audited loop into per-row txns; the regression risk
# to a real user data path outweighs a rare lock stall on a single-tenant homelab.)
# v1.23.64: holistic-audit cluster F — reaper/enumeration data-loss. (#2/#3,
# plex.py) an EMPTY Metadata page returned mid-section (transient under load /
# mid-rescan) while totalSize indicated more items broke the pagination walk
# "successfully" with a TRUNCATED list + NO error → the v1.18.89 reaper treated
# the short set as authoritative and DELETED the unseen plex_items rows (the
# v1.22.29 short-page class through the empty-page door). Now raises
# PlexParseError so plex_enum counts a section error + skips the reaper. (#12,
# plex_enum.py) the still_p survivor check that SUPPRESSES a theme-lost
# notification had no plex_sections.included gate, so a stale has_theme=1 row in
# a DISABLED section masked a genuine loss in a managed section — now joins
# plex_sections AND included=1. (#11, plex_enum.py) a transient Apprise dispatch
# failure on a theme-lost notification was unrecoverable (the reaper already
# DELETED the source row → no future enum re-detects the loss to retry); now also
# persists a durable events row so the loss is surfaced in the LOGS UI even when
# the push fails. (#14 — a snapshot tarball truncated to only the movies/ subtree
# passing _extract's any-subdir sanity check, then index('tv_shows') returning
# ([],0) as a fake-success — DEFERRED to its own tag: it's MEDIUM/fake-success
# (no rows deleted; the media_types_seen gate already blocks the catastrophic
# mass-drop), rare-trigger, self-heals on the next good sync, and the correct fix
# (require all three subtrees) entangles with broad snapshot-test-fixture churn —
# no fixture ships movie_collections — that would risk this clean reaper diff.)
# v1.23.65: holistic-audit cluster G — edition-scope bleeds (the v1.21.5x-9x /
# v1.22.x per-edition isolation arc's last three audit-found gaps). (#9, api.py)
# the shared _teardown_plex_api_artifacts_for_placements helper resolved the Plex
# rating_key by (theme_id|guid, section_id) edition-BLIND, so PURGE/DELETE on one
# edition of a section-sharing multi-edition title (LotR/Watchmen) ran
# delete_theme against an ARBITRARY sibling's rk → cleared Plex's serving theme
# for the wrong edition; now each placement resolves its rk by its own
# edition_key (both callers SELECT it; mirrors the inline v1.21.69 fix). (#13,
# plex_enum.py) the has_plex_upload lookup that clears the +P observation joined
# placements without edition_key — the "collection-only" note went stale
# (worker.py:3583 writes plex_upload for movie/TV on the API-upload path), so one
# edition's plex_upload cleared SRC=P on a sibling that IS independently Plex-
# served; now joins on p.edition_key = pi.edition_key. (#4, api.py) DOWNLOAD
# MISSING LEFT JOINed local_files edition-blind, so a multi-edition title whose
# standard edition is themed hid a themeless sibling from the missing-set; now
# carries pi.edition_key through the DISTINCT + join + per-edition enqueue.
# v1.23.66: INFO-card section uniformity. Three card sections (.diff-section /
# THEMERRDB MATCH, .recovery-section / TRY THIS NEXT, .history-section / HISTORY)
# hardcoded an 18px/14px divider + 10px head margin — pre-token drift the
# v1.15.114 migration skipped because the values don't match a gap token — so they
# sat ~2px tighter than the .dlg-section peers (--gap-5=20 / --gap-4=16 / --gap-2=8)
# stacked in the SAME card. Tokenized all three to match .dlg-section so every
# section shares one spacing rhythm. The per-section header color-coding (diff
# blue / recovery amber / history cyan) is intentional + preserved — spacing only.
# v1.23.67: snapshot-tarball truncation guard (deferred audit #14). A codeload
# tarball truncated to only movies/ (tv_shows/ + movie_collections/ absent — an
# interrupted/edge-corrupt download that still extracts without a TarError)
# passed _extract's any()-subdir sanity check, then index('tv_shows') hit
# `pages.json missing → return [], 0` (failed_pages=0) so the run reported SUCCESS
# having synced ZERO tv/collection themes (silent fake-success; the v1.22.*
# media_types_seen gate only stopped the catastrophic mass-drop, not the
# zero-sync). _extract now requires EVERY subtree's pages.json → a truncated
# snapshot raises _SnapshotError + run_sync falls through to the git/remote tier.
# The test fixture builder (_build_database_tarball) now backfills all three
# canonical subtrees so minimal fixtures still satisfy the gate.
# v1.23.68: glossary + legend gain the TDB axis + a fixed chip rail. (1) Added a
# // TDB section (TDB / TDB↑ / TDB⚠ / TDB✗ / TDB◌ / no TDB) to both the GLOSSARY
# (base.html) and the in-context LEGEND (library.html), reusing the real
# .tdb-pill tdb-pill-* row classes (v1.23.56 reuse-don't-mirror) so the decode
# chips are identical to the row. (2) .help-gloss-grid's first column was `auto`,
# so each section auto-sized its own chip rail — the 9px DL/PL dots left their
# definitions ~11px left of the wider SRC/LINK chips' definitions (the user: "the
# DL/PL dots are not lined up"). Pinned the rail to a fixed 26px so every
# section's definition column lands at the same x; the wide text-pill sections
# (TDB + the topbar UPD/FAIL/DROP/DISK pills) opt into .help-gloss-grid-wide. The
# row TDB pill's margin-left:6px is zeroed in the grid so it sits flush.
# v1.23.69: fresh silent-bug audit (multi-agent + hand-verified). (HIGH, api.py)
# the PATCH /api/config keep-on-`***`-marker guard covered git_url + database_url
# but not db_url — v1.23.62 added db_url to the GET userinfo MASK without the
# symmetric write guard, so a standard settings SAVE round-trip wrote the masked
# `https://***@host` literally over a real db_url credential (then remote-tier
# syncs 401'd). Added db_url to the guard tuple. (MED, api.py) two enqueue
# endpoints ran a jobs-table dedup SELECT + INSERT in autocommit (jobs has no
# UNIQUE) so two concurrent clicks double-enqueued — api_relink_all + the
# per-section api_libraries_section_refresh (whose twin api_library_refresh was
# already wrapped); both now BEGIN IMMEDIATE, mirroring v1.23.63. (The audit also
# swept class-9 silent-catches, async event-loop blocks, and edition-scope/data-
# loss — all CLEAN. The scan/decide-finding enqueues share the race shape but
# dedup against a worker-stamped field, not the jobs table, so a plain wrap won't
# serialize them — deferred as LOW + worker-re-checked.)
# v1.23.70: diagnostic timing on /api/library for the tab-switch-lag report
# (the user: switching between library tabs / collections sometimes feels slow).
# Diagnosed first rather than optimizing the regression-risky browse query blind:
# WAL is on (so it's NOT reader-vs-writer lock wait), and tab switching is a full
# page navigation (re-downloads/re-parses app.js + re-inits before the fetch even
# fires). _library_main_query now records its own duration — a WARNING when it
# crosses ~750ms, and a query_ms field echoed in the response so the per-request
# cost is visible in devtools' Network tab during a real tab switch. Read-only;
# comes back out once the bottleneck (query vs full-nav re-parse) is characterized.
# v1.23.71: client-side library tab switching. The nav tab links were <a href>
# full-page navigations, so every switch re-downloaded/re-parsed the ~18k-line
# app.js + re-ran all init before loadLibrary fired (the user's "~1s loading on tab
# switch"; the /api/library query itself is ≤160ms, measured v1.23.70).
# switchLibraryTab (app.js) now swaps the tab in place: fetch the new tab's
# server-rendered HTML, swap only the per-tab fragments (the toolbar chips +
# legend — the server stays the source of truth), re-bind the swapped chips (the
# chip bindings were extracted into a re-callable bindLibraryToolbarChips),
# re-hydrate libraryState, loadLibrary(). PROGRESSIVE ENHANCEMENT: the <a href>
# still works and ANY error falls back to a full navigation, so this can never
# regress nav. Back/forward handled via pushState + popstate (a deep-link target
# with query pills reloads for fidelity). Needs browser verification on deploy
# (no JS harness here). Known v1 limit: the filter drawer isn't swapped, so the
# non-collections-only ED filter row persists across a switch until a full reload.
# v1.23.72: close the two LOW enqueue races deferred from the v1.23.69 audit.
# api_trigger_scan + api_decide_finding(/bulk) ran a dedup check + INSERT INTO
# jobs in autocommit (jobs has no UNIQUE), and dedup'd against a WORKER-stamped
# field (scan_runs.status / scan_findings.adopted_at) the request hasn't changed
# yet — so even a plain transaction wrap couldn't serialize them (both still
# pass the check + insert). Added a JOBS-table dedup (existing pending/running
# job for the same target) inside a BEGIN IMMEDIATE at each site, mirroring
# api_sync_now / api_relink_all: scan now also 409s when a scan job is pending;
# decide single/bulk skip a finding that already has a pending/running 'adopt'
# job (matched via json_extract(payload,'$.finding_id')). The bulk uses a per-row
# transaction (short locks, mirrors v1.23.63 decline-all). The worker's _do_adopt
# already re-checks adopted_at, so this just stops the duplicate JOB being made.
# v1.23.90 (the user: "anime numbers don't match, 1,244 vs 1,341"): the dashboard
# classified anime-section FILMS (media_type='movie' in an is_anime=1 section —
# anime movies / OVAs) inconsistently with the library tabs. The library is a
# clean partition (movies tab = is_anime=0 movies; anime tab = is_anime=1 movie
# +show; v1.19.28), but three dashboard sources each split differently: the
# // ANIME THEMED card (/api/coverage/plex `anime` array) was media_type='show'
# ONLY (1,244), the // PLEX ANIME card (/api/sections/coverage section totals)
# counted EVERY row in anime sections incl. collections (1,341), and _stats_sync
# (SSR) was show-only for anime + un-gated for movies (anime films counted on
# the MOVIES card). Now all three match the library partition: movies aggregates
# gate is_anime=0, anime aggregates take media_type IN ('show','movie'), and the
# // PLEX ANIME card sources from /api/coverage/plex's anime array (which already
# returns the shape, v1.15.47) like its PLEX MOVIES/TV siblings instead of the
# collection-contaminated section-totals sum. Anime films move MOVIES→ANIME (the
# 97-row gap); the PLEX LIBRARY total is unchanged. renderPlexAnimeCard retired.
# v1.24.0: rolled over at the v1.23.x .99 line-close after a fresh 3-agent audit
# (silent-fails / recent-regressions / coverage) of the v1.23.89-99 arc (split-
# EXISTS sync flush, the anime-count library partition, the bulk-download
# notification-wording fix, and the silent-failure sweep — collections-enum
# empty-page truncation guard, fresh 0-byte download guard, two event-loop
# offloads, two edition-scoped reads, two LOW error-masks). Regressions + coverage
# came back CLEAN (the api_coverage_plex offload proven byte-identical, every
# session behavior behaviorally tested). ONE real bug found + fixed pre-cut:
# _detect_and_stamp_drops_git probed survivorship with `read_json(path) is not
# None`, but read_json returns None for BOTH a missing tree path AND a present-
# but-malformed blob — so a momentarily-corrupt ThemerrDB entry read as "gone" →
# false tdb_dropped_at stamp + pending_update deletion, and commit_sync_ok
# advances the git baseline so the next run diffs from the new head and it never
# self-corrects (data-loss class). New _GitMirror.path_exists_in_tree probes blob
# EXISTENCE regardless of JSON validity → a corrupt survivor counts as present,
# never dropped (fails SAFE). Plus the LOW fold-in: the broad drop-detection
# `except: "drop detection failed"` split into per-phase try/except (detection /
# baseline-advance / compaction / event-log) so a baseline or compaction failure
# isn't mislabeled — the v1.22.74 advance-only-after-detection ordering + v1.23.80
# compact-only-after-HEAD-retired gate preserved. Two LOW items consciously
# DEFERRED to v1.24.x: the first-ever-anime-theme in_plex notification-count timing
# (cosmetic — download side covered; delicate flush/resolve ordering) and the
# api_admin_hama_gap event-loop offload (admin-rare; clean run_in_threadpool wrap).
# v1.24.1: bugs/silent-failures review follow-ups (Tag A, 2 HIGH + 1 MED). (HIGH,
# class 9) adopt_finding swallowed any _do_adopt failure + UNCONDITIONALLY stamped
# adopted_at, so a transient blip (db locked / sidecar gone / ENOSPC) became a
# permanent fake-"adopted" the worker marked DONE and the idempotence + jobs-dedup
# guards then blocked from ever retrying — now re-raises into the worker's bounded
# _mark_failed path, leaving adopted_at NULL (re-decidable). (MED) _do_adopt's
# local_files/placements writes used INSERT OR REPLACE → a re-adopt silently NULLed
# the health columns (canonical_present / theme_present / *_health_checked_at) that
# drive the NEEDS WORK / DL / PL sorts; now ON CONFLICT DO UPDATE, the v1.23.99
# _do_keep fix applied to adopt. (HIGH, class 12) api_stats offloaded its heavy SQL
# but ran _disk_status_for_stats (shutil.disk_usage → statvfs) inline in the async
# body — a stalled Unraid/NFS mount would freeze the event loop; now off-loaded via
# run_in_threadpool (the AST lint can't see it: nested def). CSS token-cleanup is
# the separate Tag B.
# v1.24.2: review Tag B — CSS token discipline (no behavior change). The library
# anime role-pill (.lib-flag-pill-anime) hand-mirrored a parallel pink (#ff7ab8 /
# 255,92,168 / 255,122,184) that drifted from the canonical --magenta its sibling
# .stat-plex-anime card already uses — the gc-* palette-drift class; now on the
# token. Plus the LOW polish bundle: .apply-complete-banner raw lime
# rgba(0,255,0,.04)→var(--bg-tint-green); .lib-flag-pill-4k color #ffce6b→
# var(--amber-bright); .sync-hist-transport-database tint→rgba(var(--magenta-rgb));
# dead var(--bg,#000) fallback→var(--bg); and a new --grey-rgb token replacing ~7
# raw rgba(180,180,180,…) muted-metadata sites (app.css + ops.css).
# v1.24.3: orphan scan probes the placement's OWN plex_rating_key. the user's prod
# scan flagged 7 plex_upload rows (motif_entry_missing / no_plex_entries /
# rk_lookup_failed) that RE-PUSH never cleared. They're MULTI-EDITION titles
# (Amadeus +Director's Cut, Star Wars ×4 cuts, Avatar +Extended, LotR/Hobbit
# Theatrical+Extended) whose '' placement targets a NAMED edition's rk (verified
# serving, ok=1). scan_one_placement re-resolved the rk from plex_items by the
# placement's '' edition_key — probing the wrong edition, or none in that section
# (→ rk_lookup_failed) — instead of the rk the placement actually used. Now it
# probes placements.plex_rating_key directly; plex_items resolution stays as the
# folder_path source + the legacy fallback for rk-less placements. Diagnostic-only
# fix (read path); the deeper ''-as-falsy edition mis-keying that created those
# placements is the still-deferred multi-edition cleanup.
# v1.24.4: backfill (the user's pick over the riskier place-path fix) — re-key the
# mislabeled plex_upload placements. orphan_scan.rekey_mislabeled_placements re-labels
# each plex_upload placement whose edition_key disagrees with the edition of its own
# plex_rating_key (per plex_items) to the rk's true edition — the ''-as-falsy residue
# (placement keyed '' but rk is Amadeus's Director's Cut etc.). Tracking-only +
# non-destructive: never touches Plex or the on-disk theme, leaves the '' local_files
# row (reached via the IN(edition,'') fallback), skips re-keys whose target PK slot is
# occupied + rks not in plex_items. New admin endpoint POST
# /api/admin/placements/rekey-mislabeled (dry-run default; ?apply=true writes), off the
# event loop. The deeper place-path ''-scoping bug stays deferred (changes auto-theming
# on multi-edition titles — the HOLD'd arc).
# v1.24.5: /code-review follow-ups on the v1.24.3/.4 orphan-scan arc (6 findings).
# (MED) scan_one_placement's orphan_sidecar check + DELETE SIDECAR target now use
# the folder of the rk we actually PROBE (placement_rk's plex_items folder via a
# LEFT JOIN), not the edition-resolved rk_row — for a mislabeled placement they
# diverged and stated the wrong-edition folder (the live Amadeus symptom). (MED)
# rekey_mislabeled_placements dedups intra-batch target PKs into conflicts so two
# re-keys landing on one PK can't collide on the 2nd UPDATE + abort the txn. (LOW)
# log_event moved AFTER the write txn commits (its flusher writes on its own conn +
# no WAL → logging inside BEGIN IMMEDIATE could drop the audit batch). (LOW) the
# rekey endpoint catches IntegrityError/OperationalError → {ok:false,reason} instead
# of an opaque 500. (LOW) the placement_rk lookup asserts media_folder='' (the
# plex_upload PK invariant). Plus the two back-to-back edition guards merged.
# v1.24.6: api_admin_hama_gap event-loop offload (deferred from the v1.24.0 rollover).
# The /api/admin/diagnostics/hama-gap handler ran ~13 sync conn.execute (6 COUNT(*) +
# 6 sample SELECTs + a runtime read) directly in its async body — a class-12 block
# (sqlite isn't in the AST lint's tracked set, so it slipped the guard). The whole body
# is now a nested def offloaded via run_in_threadpool, mirroring api_coverage_plex
# (v1.23.96) + the v1.24.1 stats disk offload. Byte-identical behavior (the v1.15.143
# behavioral test is unchanged); _require_admin stays in the async body.
# v1.24.7: edition-audit finding (HIGH, latent crash). api_unplace_item's LET PLEX
# SERVE / UNPLACE restore loop reads pr["edition_key"] (the v1.22.76 per-edition
# local_files fix), but its three placement worklist SELECTs only fetched
# media_folder/placement_kind/section_id — never edition_key. With row_factory=Row,
# pr["edition_key"] raises IndexError, so the whole Plex-API restore loop threw on any
# plex_upload LPS/UNPLACE with Plex enabled (prod config). No test exercised the
# plex-enabled restore path, so the v1.22.76 source pins (which checked the READ)
# missed it. Added edition_key to all three SELECTs; pinned both sides of the contract.
# v1.24.8: edition-audit finding (MED) — the reaper's theme-lost fallback classifier
# was edition-blind. The lost-theme candidate set is per-edition (DISTINCT folder_path)
# and the notification names the specific lost edition, but the four-way tier classifier
# (backup_signal / sidecar_db / other_fallback in plex_enum.py) matched a fallback by
# (media_type, tmdb_id) with NO edition filter — so on a multi-edition title that fully
# lost its Plex theme, a SIBLING edition's backup/sidecar got advertised as the lost
# edition's (operator told to PROMOTE/ADOPT the wrong file). Each classifier now scopes
# to `edition_key IN (this_edition, '')` (prefer this edition, allow the shared ''
# standard so a named loss with only a '' backup still recovers, but EXCLUDE other named
# siblings). The dedupe key gained the edition so two editions losing their theme in one
# 24h window each notify. still_p (the title-wide "still themed anywhere?" suppressor)
# is unchanged by design (v1.22.32). Read-only/notification-only — no data mutation.
# v1.24.9: INFO-card honesty — a backup-only row's downloaded URL was rendered
# under the "applied url" label. the user's SpongeBob SquarePants repro: SRC=P (Plex
# serving its own theme) + a TB ThemerrDB backup, yet the card showed
# "applied url ... themerrdb" as if that theme were playing. The label was driven
# purely by source_kind (isUrlSourced = themerrdb/url) and never consulted
# last_place_attempt_reason='backup_only' — the very flag stamped when motif
# downloads but DEFERS to Plex (doesn't place). openInfoDialog now relabels that
# line to "backup url" for backup-only rows (covers TB + UB; the URL, thumbnail and
# 0.51.222: test-hygiene sweep (user-requested) — kill the fixed-window source-slice rot
# class that tripped ~8x across v0.51.212-221 (byte-window slices, arity pins, fixed-line
# lookbacks: each asserted incidental TEXT SHAPE, not the invariant, so a structural edit
# that preserved the invariant still failed). Measured the whole suite: 108 tight (<20%
# headroom) sites, 4 at literal 0% (next in-scope edit breaks them). Rather than churn all
# 108 (the _slice_helpers doc's own stance: the comfortable-headroom bulk is fine), migrated
# the 11 sites below a 2% hard floor to _slice_helpers.slice_to_next (anchor-bounded, grows
# with the code) and added test_v0_51_222 as the durable forward guard: (1) HARD FLOOR — no
# slice may sit <2% headroom (fix or widen), (2) RATCHET — the tight-site count may not
# exceed the recorded baseline (108->96 here), so the population can only hold or shrink.
# New source pins should use slice_to_next. Product code untouched; tests only.
# 0.51.221: loose-end cleanup — declare ItemContext.edition_key. v0.51.220 set
# ctx["edition_key"] on the ItemContext TypedDict without declaring the field (only the
# DISPLAY-label ctx["edition"] was declared). Runtime is fine (total=False), and CI's
# mypy is report-only so it never blocked a build — which is precisely how a contract gap
# accrues silently. The field is now persisted to notifications.edition_key and read by
# both notify.dispatch sites, so it belongs in the declared contract, not implied.
# 0.51.220: notifications carry the exact edition they fired for — the last surface in
# the edition-exact click-through arc. The `notifications` table had no edition_key column,
# so an inbox click-through on a multi-edition title fell to the v0.51.218 picker even when
# the notice was about ONE cut (a theme placed on the extended edition). Schema v78 adds a
# nullable notifications.edition_key (idempotent _add_column migration). enrich_item already
# accepted edition_key for its display label (v1.21.76) — it now also keeps the RAW key in
# the ItemContext, so the 5-of-7 dispatch sites that already pass edition_key persist it for
# free; the 2 title-level digest sites pass none → NULL → picker fallback, correct.
# notify.dispatch + dispatch_coalesced thread _ic.get("edition_key") into
# record_notification; list_notifications returns it; the inbox rowHtml emits data-edn and
# openNotifRow forwards it as info_edition. '' (untagged standard edition) is preserved as a
# real scope distinct from NULL "unknown" — `!= null` on write, `'edn' in dataset` on read.
# Schema pins bumped (test_v0_50_17, test_v1_24_93) + CLAUDE.md v77->v78.
# 0.51.219: the per-row deep-links carry their edition, so they skip the v0.51.218
# picker. Every row of the loudness-audit report and the canonical-health report IS one
# local_files edition (both already SELECT edition_key and ship it to the client), so
# passing info_edition on those links lands the card on THAT cut directly instead of
# asking. The picker is now only ever the fallback for links that genuinely can't know —
# a title-level notification digest, and the /queue reprobe OPEN ROW whose event is not
# edition-specific (audited: its dataset carries mt/id/section/fourk/title and no edition,
# so it correctly defers rather than fabricating a scope). Three client edits: the two
# producers append info_edition (guarded `!= null`, since '' is the real untagged-folder
# edition a truthiness check would drop back into the picker), and the deep-link parser
# reads it with sp.has() and threads it through openInfoDialog's v0.51.218 editionKey arg.
# test_v1_14_85's verbatim openInfoDialog-arglist pin broke on the added arity while its
# invariant held; relaxed to the call prefix (same class as v0.51.218's v1_23_19 fix).
# 0.51.218: the INFO card stops GUESSING which edition it shows. A Plex title can hold
# several cuts in one section and motif tracks a SEPARATE theme file per cut. api_item
# resolves the cut from the clicked row's rating_key (v1.21.68) — but only a LIBRARY ROW
# click carries one. Every deep-link (inbox click-through, canonical-health,
# loudness-audit, /queue OPEN ROW) sends media_type + tmdb_id + section_id and nothing
# else, so _info_edition was None, execution fell to the section-only branch, and the card
# rendered local_payloads[0] — an arbitrary cut, with no ORDER BY behind it. The loudness
# it displayed, and the file // LEVEL THIS THEME rewrites, then belonged to a cut the user
# never chose. Measured against the live DB (2026-07-21): 32 (mt, tmdb, section) groups
# hold >1 edition and 23 of those have cuts whose theme files genuinely differ — Fellowship
# carries three at -33.33 / -14.75 / -18.75 LUFS, so "whichever came first" is a ~19 dB
# difference in what mp3gain rewrites. api_item now accepts an explicit edition_key (which
# beats the rk resolution — a caller naming the cut is more authoritative than one inferred)
# and, when nobody named one and several exist, returns edition_choices +
# edition_ambiguous instead of quietly picking. The card renders ONLY a picker in that
# state — returning BEFORE any reading is derived from the arbitrary row, since showing a
# sibling's LUFS beside a picker is exactly what made the wrong cut look like the right one
# — and choosing re-opens scoped to that cut, the same path a library-row click takes.
# Single-edition titles (2,745 of 2,822 rows) are untouched: no ambiguity, no picker, and
# naming a cut skips the DISTINCT scan entirely. '' is a real edition (the untagged folder)
# so the URL builder tests `!= null`, not truthiness, or the standard cut would be dropped.
# 0.51.217: plex_items' media_type vocabulary is translated before it's compared —
# user-reported, from clicking an "arrived already themed" inbox row and getting a 422.
# plex_items.media_type is PLEX's string set (movie/show/collection); themes, local_files,
# placements and every /api/items path use motif's (movie/tv/collection). The codebase
# translates constantly and deliberately — worker.py 4x motif->Plex, plex_enum 3x
# Plex->motif — but _maybe_notify_arrived_themed used the raw value in three places, and
# the sibling _maybe_notify_theme_available in the SAME file gets it right, which is why
# it read as correct. Two live bugs: (1) `lf.media_type = pi.media_type` compared 'tv' to
# 'show' for every TV row, always false, so the NOT EXISTS meant to SUPPRESS this FYI when
# motif already owns a theme for the title was INERT for TV — the "nothing to do" notice
# fired for shows motif manages, and the user could not tell which; (2) the same raw value
# rode into item_ctx, so the inbox click-through emitted info_mt=show and
# /api/items/show/<id> answered 422 literal_error. A sweep found these were the ONLY two
# raw comparisons in app/ (recovery_v55's is the correct longhand form). Behavioural tests
# seed the exact shape and assert on the endpoint contract, not a string compare; a lint
# pins the CLASS so the next one fails at test time. Note: the 30-day dedupe key embeds
# media_type, so a TV title deduped under the old 'show' key can ping once more —
# self-limiting, since the now-working suppression drops the rows motif owns.
# 0.51.216: info-card semantics — the last three ultra-review findings, all in the
# v0.51.207 loudness card. (1) The controls block was a bare <div> intermixed with dt/dd
# siblings inside <dl class="dlg-grid">; the HTML content model allows a dl to hold EITHER
# dt/dd groups OR div children, never both. It laid out fine so nothing complained, but a
# screen reader walking the list reached the target stepper, // PREVIEW AT TARGET,
# // LEVEL THIS THEME and // UNDO LEVELING with no preceding term, and the over-ceiling
# explanation was orphaned from any label. Now a real <dt>action</dt> / <dt>cannot level</dt>
# plus a dd that keeps grid-column:1/-1, so the v0.51.207 de-squishing is preserved.
# accent-red moved to an inner span because `.dlg-grid dd` (0,1,1) out-specifies a bare
# `.accent-red` (0,1,0) and would have silently repainted the warning as ordinary text.
# (2) The card chip now carries the library's ▂▄▆ meter glyph. .tier-badge-loud is
# byte-identical to .tier-badge-4k, which both the JS and CSS comments justify ONLY because
# the library marker is a GLYPH ("distinct ... by shape (meter bars vs the letters 4K)") —
# and the chip renders as bare letters right beside the 4K badge in the same <h3>, i.e. two
# identical amber pills, adjacent. Fixed by SHAPE, not colour: the amber ENCODES the state
# and stays fixed across themes. (3) RAW stopped claiming a measurement that may not exist
# — _loudness_marker returns "raw" for a local file with NO measurement at all, so
# "measured but not yet leveled" was false for exactly the rows whose next step is
# // MEASURE NOW. The card branches on the value it has; the library tooltip drops the
# claim entirely, since v0.51.202 pops loudness_i from the row payload and that surface
# genuinely cannot tell the two apart.
# 0.51.215: the NOTIFICATIONS settings block tells the truth again, and the guard that
# should have caught it can now see the block it was missing. v0.51.210 made the in-app
# INBOX per-kind toggleable, which FALSIFIED a standing promise repeated in three places —
# two settings hints and the config_file registry comment all still said a kind "always
# lands in the in-app INBOX regardless of this toggle". A user who switched a kind OFF and
# then read that sentence would reasonably conclude the new toggle was broken. All three
# now say the INBOX is that kind's primary surface, toggled separately under // IN-APP
# INBOX. Also realigns two labels that drifted when the v0.51.210 block was written: a
# bare U+1F6E0 without VS16 (rendering with different glyph presentation than its twin on
# the same page) and a "THEME LOST" that had lost its "— NO FALLBACK" qualifier, leaving
# it as an ambiguous fourth sibling three lines under "— STILL PLAYING" and "— BACKUP
# READY". Root cause of the drift: test_v1_23_83 hardcoded the notifications.events.
# prefix, so the entire inbox_events.* block was invisible to every guard in it. The
# lookup is now prefix-parameterized and a new cross-block test asserts a kind is NAMED
# identically in both (the on/off chip may differ — separate registries, separate
# defaults). Verified non-vacuous: it compares 8 kinds and fails on both pre-fix labels.
# 0.51.214: the in-dialog loudness mutations now invalidate the info-card cache.
# _infoFetch serves a cached payload for 6000ms per URL and a cache HIT does NOT refresh
# its ts, so the window runs from the FIRST fetch. // LEVEL THIS THEME, // UNDO LEVELING
# and // RE-MEASURE each re-open the card 700-900ms later to re-read the row they just
# changed — squarely inside that window — so the re-open replayed the PRE-mutation payload
# and overwrote the result the handler had just painted: the correct "now -18.4 LUFS"
# showed for ~700ms and then reverted to the old number. Bites whenever the click lands
# within ~5s of opening the card, i.e. the natural post-re-download flow. The row-menu
# handler already clears on any non-info action; these buttons live inside the dialog and
# bypassed it. Clearing at mutation-SUCCESS rather than merely before the re-open means
# every later read is fresh too, and a failed request keeps its still-valid cache.
# 0.51.213: fix the two v0.51.209 regressions + finish the a11y pass it half-did, all
# from the ultra review. (1) v0.51.209 routed collection notifications to /collections
# but never added that path to the info_open auto-open gate, so the click-through
# navigated and then opened NOTHING — strictly worse than the /tv it replaced, where the
# gate passed. The canonical-health and loudness-audit deep-links emit the same URL and
# were dead too. (2) Its new keydown handler matched any Enter/Space inside
# .notif-group-head — including the group's own Dismiss-all ×, which is a CHILD of the
# head — so preventDefault ate that button and toggled the group instead; dismissGroup
# was keyboard-unreachable. The bail on a nested control now comes first, mirroring the
# click handler's ordering. (3) That pass made the group HEADER operable and left the
# drawer's PRIMARY action — the click-through rows — with no tabindex, no role and no
# keydown path, so a keyboard user could expand a group and then reach none of its
# children. (4) Worse, .notif-group-head was given tabindex with no entry in the
# ops.css focus-ring allow-list (app.css strips the UA outline app-wide), so keyboard
# focus landed on it INVISIBLY — focusable-but-unseeable is worse than not tabbable.
# Both drawer surfaces are on the list now. The row's role+tabindex sit on .notif-main,
# not the <li> — on the list item they would strip its listitem role and nest the dismiss
# <button> inside a role="button"; .notif-group-head is likewise a div inside its <li>.
# Click and keyboard share one openNotifRow so the routing table can't drift between them.
# Three more fixed-window test slices bounded by real anchors: v1_14_85 (68% consumed),
# v0_51_209's 400-byte window (which this tag's own comment overran), and v0_51_12's
# arming-gate test — that one ALSO assumed its regex matched exactly one gate, which
# stopped being true the moment the auto-open gate gained /collections, so it now
# identifies the arming gate by what it owns rather than by position.
# 0.51.212: data-integrity hardening for the v0.51.208 per-item loudness probe, all
# three holes found by the ultra review of v0.51.205..211. (1) The UPDATE was blind on
# the PK, so a LEVEL landing during the ~1s ffmpeg window was silently overwritten with
# PRE-level loudness — and since measured_sha256 then equalled file_sha256, the audit's
# staleness skip made that wrong number PERMANENT (row reads "leveled", reports raw).
# Now a compare-and-set re-asserts the (norm_state, file_sha256) pair it read; a 0-row
# result is reported, not faked. (2) file_sha256 was re-stamped without file_size, which
# re-qualified an over-ceiling row for a LEVEL whose ~12MB Plex re-upload 500s (the
# v0.51.177 gate reads file_size) — the two now always move together. (3) measure-then-
# hash was a TOCTOU that paired OLD-bytes loudness with NEW-bytes sha, a self-consistent
# pair every downstream staleness gate accepts; it now hashes before AND after and
# refuses on mismatch. Plus: on a normalized row a stored sha that disagrees with disk is
# the ONLY record that the leveled file was swapped out-of-band (undo anchors dead) —
# re-stamping it silently erased that, so it now logs + leaves an event breadcrumb.
# Test debt from the same review: test_v0_51_208's re-open assert used a FIXED 2000-byte
# slice already 87% consumed (the v0.51.141-143 trap) — now bounded by the next handler
# registration; and its "silent theme" case mocked a -inf that _parse_loudnorm_json makes
# unreachable, so the REAL None path is now covered too.
# 0.51.211: fix the RAW loudness filter to match the row marker — RAW was a SUPERSET
# ("everything not-yet-leveled") that ALSO matched amber-outlier-glyph rows, so the
# filter and the glyph disagreed (user report). RAW now = unleveled AND within margin
# of target (the dim raw marker exactly); raw/outliers PARTITION the unleveled set, so
# selecting both chips gives every unleveled row (the old superset, via multi-select).
# 0.51.210: in-app INBOX allowlist toggles — Settings → NOTIFICATIONS → IN-APP INBOX,
# a per-kind checkbox (notifications.inbox_events, default all ON) choosing which event
# kinds reach the INBOX drawer, INDEPENDENT of the Apprise/Discord send-toggles.
# notify.dispatch gates the inbox record on it; declarative data-cfg-field checkboxes
# ride the generic config load/save; closed-set PATCH filter drops unknown kinds.
# 0.51.209: notification-inbox review fixes — (A) the group header was
# role="button" tabindex="0" but keyboard-inoperable; Enter/Space now expand it via
# a shared toggleGroupHead the click path also calls. (B) drawer click-through routed
# every non-movie to /tv; a collection now lands on /collections. + stale count_unread
# comment corrected (the badge counts rows; grouping is client-side/display-only).
# 0.51.208: per-item loudness probe — a // RE-MEASURE (measured rows) / // MEASURE
# NOW (unmeasured) button on the INFO card, the loudness analogue of // PROBE TDB URL.
# New read-only POST /api/admin/loudness/measure-one re-reads the file's current
# loudness + re-stamps the edition-scoped measurement; the card re-opens so plays-at,
# the chip, and the stepper base re-seed. Never rewrites audio or touches Plex.
# 0.51.207: LOUDNESS info-card presentation pass — clearer "raw" copy (plays at its
# original recorded level, not "raw source loudness"); a (clipping) explainer tooltip;
# the squished target/audition/action controls moved into a full-width .loud-controls
# block; an at-a-glance loudness chip beside the 4K badge, driven by the SAME
# server-derived marker (_loudness_marker) as the library row glyphs.
# 0.51.206: cleanup — remove 6 dead local imports left by the v0.51.199 chokepoint
# extraction (the loudness endpoints delegate to _normalize_one_row/_undo_one_row/
# _push_theme_to_plex/_measure_plex_serving now, but kept their old normalize_file/
# undo_file/measure_loudness/now_iso/time imports). AST-verified unused; endpoints unchanged.
# 0.51.205: audit hardening — M1 normalize_file rejects non-finite loudness (a legacy
# -inf row was round(inf)->OverflowError, breaking "never raises"); M2 -t-cap the PCM
# decode so a low-bitrate file can't balloon memory; M3 bulk counts/run use EXISTS not a
# fanning placements JOIN; L1 undo-one refuses a Plex-pushed row when Plex is unconfigured;
# L2 breadcrumb when the pre-gain PCM hash fails; L3 audit doesn't count a zero-row measure.
# 0.51.204: audit H1 — the UNDO path now ENFORCES its audio-restored verdict. undo_file ok
# gates on audio_restored (not "mp3gain -u ran"); _undo_one_row's over-restore branch keeps the
# row leveled + its recovery refs, re-stamps only the measurement, and does NOT push to Plex —
# was: committed raw + wiped refs + re-pushed a degraded theme (silent, permanent).
# 0.51.203: loudness Phase 3 — SPLIT normalize_on_download into two independent toggles:
# manual downloads (user-triggered) vs auto-added (sync/enum auto-picks, reason new/url_changed).
# Worker gate → _should_condition_download helper (explicit payload wins → auto vs manual toggle).
# New normalize_auto_added config + env + Settings checkbox; both default OFF. UPLOAD/SET URL
# unaffected (per-theme checkbox). Completes the loudness feature (Phase 0→3).
# 0.51.202: loudness Phase-2 arc code-review followups — (1) correct the "SAME set as
# // LEVEL OUTLIERS" overclaims: the filter/marker use the same loudness DEFINITION but are
# NOT eligibility-gated, so a SUPERSET; (2) drop dead loudness_i from /api/library rows;
# (3) harden manual-url normalize coercion so a JSON string "false" can't enable leveling.
# 0.51.201: Phase 2 Tag 6 — per-theme LEVEL LOUDNESS checkbox on UPLOAD MP3 + SET URL
# (default UNCHECKED/raw). UPLOAD conditions the file inline before placement (+ clears stale
# norm on a raw re-upload); SET URL threads a `normalize` payload the worker honors over the
# global normalize_on_download toggle. Both share _cond_columns + condition_new_download.
# 0.51.200: Phase 2 Tag 5 — 3-state loudness title-cell marker (raw muted / outlier amber /
# leveled cyan), same ▂▄▆ glyph, colour ENCODES state (theme SPLIT). Derived SERVER-SIDE in
# _library_main_query so the marker + the LOUDNESS filter share ONE rule (shared _OUTLIER_MARGIN_DB).
# 0.51.199: Phase 2 Tag 4 — the safety net. schema v77 (op_progress.kind += bulk_normalize_undo)
# + a bulk_normalize_undo op that reverses a whole // LEVEL run (per-row _undo_one_row chokepoint,
# extracted from undo-one) + // UNDO ALL LEVELING button (shown off bulk.leveled, survives eligible=0).
# 0.51.198: Phase 2 Tag 3b — LOUDNESS library filter axis (LEVELED / RAW / OUTLIERS);
# 'outliers' matches // LEVEL OUTLIERS. Registered on every surface + legend + glossary.
# 0.51.197: Phase 2 Tag 3 — // LEVEL OUTLIERS / // LEVEL LIBRARY buttons on the audit
# report (eligible + outliers counts that match the bulk op's predicate + configured target).
# 0.51.196: the loudness audition gets its OWN dedicated preview player (no longer
# hijacks the FILE & PLACEMENT play bar) + LOUDNESS moves above FILE & PLACEMENT.
# 0.51.195: Phase 2 Tag 2 — schema v76 (op_progress.kind += bulk_normalize) + the
# bulk_normalize background op (serial, loudest-first, per-row _normalize_one_row) + start endpoint.
# 0.51.194: Phase 2 groundwork — extract the per-row normalize into a shared
# _normalize_one_row chokepoint so // NORMALIZE and the coming bulk op share ONE impl.
# 0.51.193: code-review follow-ups on the loudness UI — one hover-band for the target
# (worker/normalize-one/card agree via a clamped settings property), leveled-marker legend
# un-gated from collections-only, + smaller guards (null-tmdb, play() reject, CEIL pin).
# 0.51.192: library title-cell marker for loudness-leveled rows (▂▄▆, fixed cyan) +
# shorten the audition-preview note so no state wraps and jumps the row.
# 0.51.191: one configured level target, not two — normalize-one hardcoded -18 while
# download-conditioning used the configured value. Card gains a target stepper + audition.
# 0.51.190: the INFO card grows a LOUDNESS group — what a theme plays at, whether
# it was leveled and by how much, the raw level it came from, and LEVEL/UNDO in place.
# 0:30 preview stay — they correctly previewed the staged backup). Returns to
# "applied url" the moment PROMOTE TO ACTIVE flips the reason off backup_only.
# JS-only label change; no data path touched.
# v1.24.10: INFO-card consistency — synthesize a baseline "how it got here"
# HISTORY line. The per-row events table is pruned after 30 days
# (_prune_events), so an auto-themed row nobody has touched loses its HISTORY
# section entirely once its download/place events age out (the user's A Knight of
# the Seven Kingdoms diagnostic: themed May 20, events gone by ~June 21). Every
# row motif owns a local_files row for now renders a single durable origin line
# derived from stored provenance (source_kind via _humanSourceKind + provenance
# + the new downloaded_at, falling back to motif_added_at) — none of which is
# pruned. renderRowHistory gained a `baseline` param: it shows real events when
# present, else the synthesized row (no CLEAR — nothing stored to clear), else ''
# (pure-P / SRC=— rows with no motif file stay sectionless as before). Backup-only
# rows tag the line "· backup (deferring to Plex)" so it doesn't read as applied
# (mirrors v1.24.9). api_item now also returns local_files.downloaded_at. The
# history degrades gracefully: detailed timeline → one-line origin after prune.
# v1.24.11: self-review cleanup of the v1.24.9/.10 INFO-card arc.
#   1. (dead code) v1.24.10 added a `downloaded_at` column to
#      _library_main_query (the library LIST query) — but api_item builds
#      data.local_file from `SELECT * FROM local_files`, which already
#      surfaces downloaded_at (and last_place_attempt_reason) as native
#      columns. The explicit column was never read by the INFO card; removed.
#      Both contract tests now pin the REAL path (`SELECT * FROM local_files`)
#      instead of a string that happened to live in the unrelated list query.
#   2. (consistency) the synthesized ORIGIN row's date now formats via the
#      card-wide fmt.timeAuto (pre-formatted in openInfoDialog) so it reads
#      `MMM DD, YYYY, HH:MM` like the motif-added / themerrdb-added dates,
#      not renderRowHistory's bare toLocaleString.
#   3. (waste) renderRowHistory only builds the per-event rows when events
#      exist (`hasEvents ? events.map(...) : ''`) — the synthesized branch no
#      longer maps over an empty array.
#   Behavioral tests tightened (assert the ORIGIN date + the full summary
#   note). Pure JS/test/comment cleanup; no behavior change for users.
# v1.24.11: self-review cleanup of the v1.24.9/.10 INFO-card arc — see above.
# v1.24.12: full security review remediation (the user-selected items).
#   1. Forward-auth FAILS CLOSED: _resolve_principal no longer trusts the
#      X-Authentik-Username header on an EMPTY forward_auth_allowed_ips (the
#      old "legacy permissive" path was an admin-bypass footgun on a directly-
#      reachable port). Header trusted only when the allowlist is set AND the
#      client IP matches; else fall through to token/session/anonymous. Not
#      live in the deploy (trust_forward_auth=False) but removes the footgun.
#      Boot warning reworded to describe the fail-closed behavior. New
#      tests/conftest.py gives the suite an allowlisted 127.0.0.1 peer so the
#      162 forward-auth tests keep authenticating.
#   2. section_id (the /collections filter) gains an in-function isdigit()
#      guard — route-pinned to ^[0-9]*$ already, so defense-in-depth only.
#   3. events scrubber redacts `…/webhooks/<id>/<token>` (Discord/Slack path
#      secret) — unreachable today, last-line defense.
#   Deliberately NOT done (the user's call): CSP / X-Frame-Options headers.
# v1.24.13: holistic-review fixes — lock-scope / atomicity batch (Wave 1 of 4).
#   #2 api_unmanage_item held the BEGIN IMMEDIATE write lock across a per-row
#      folder_has_theme_sidecar() /data walk; the re-stat is now precomputed
#      off-loop (run_in_threadpool) before the txn, the in-txn loop only UPDATEs.
#   #4 _do_adopt's 4-statement write cluster (local_files+placements+acks+
#      plex_items) ran in autocommit → an inline-adopt crash mid-cluster left a
#      tracked canonical with no placement (no worker retry on that path). Now
#      wrapped in one transaction().
#   #6 _retry_pending_placements did a non-atomic dedup-SELECT-then-INSERT →
#      a concurrent enqueue raced it into a duplicate place job. Now one
#      BEGIN IMMEDIATE around check+insert (mirrors _enqueue_sync v1.22.33).
#   #7 api_admin_delete_orphan_sidecar ran find_theme_sidecar_path (/data scan)
#      on the event loop (v1.23.96 offloaded the unlink but missed the scan).
# v1.24.14: holistic-review sync data-loss (wave 2/4).
#   #3 git-differential sync advanced the MOTIF_LAST_SYNC baseline even when a
#      changed path's read_json() returned None (a `continue`+errors+=1, not a
#      raise, so it slipped the v1.22.74 advance-only-on-success guard) -> that
#      add/modify was permanently dropped (next run diffs from the new HEAD).
#      commit_sync_ok now also gates on stats.errors==0 (clean in pure-git mode)
#      + breadcrumbs when read failures hold the baseline back.
#   #5 remote-tier _fetch_index trusted a 200-with-bad-body pages.json -> a
#      silent green zero-theme sync of a whole media type. Now raises on a
#      non-dict body + warns on a zero-page catalog (snapshot tier got this
#      in v1.23.67).
# v1.24.15: holistic-review LOW cleanup (wave 3/4 — safe items).
#   #9 deorphan merge: unconditional non-dry breadcrumb + amplifier abort cap
#      (v1.18.10 class) on the DELETE-heavy collision-merge walker.
#   #10 sync drop stamp + companion pending_updates DELETE now in one txn.
#   #11 op_progress RUNTIME stuck-sweep (progress.sweep_stuck + 15-min job) —
#      a raised finish_progress left an op 409-locked until restart.
#   #12 corrected the inaccurate bulk cloud-backup gate comment.
#   Deferred (the user): #1 migration idempotency (17 boot-critical migrations,
#      weak suite coverage, not live on v67) + #8 PROMOTE lock decomposition.
# v1.24.16: sync summary headline scoped to "no CATALOG changes" (was bare
#   "no changes"). The headline reflects only the ThemerrDB catalog diff; the
#   post-sync plex_enum (separate jobs, run AFTER the summary dispatch) can
#   auto-theme a newly-seen UNTHEMED Plex row from a PRE-EXISTING catalog
#   theme -> a theme_added card lands right after a "no changes" summary,
#   reading as a contradiction (the user's DragonHeart repro). Wording-only.
# v1.24.17: edition sweep #2 — libKey (bulk-selection identity) collapsed a
#   multi-edition title's editions into ONE key (mt:theme_tmdb), so multi-
#   selecting a multi-edition title checked both rows but fed every bulk action
#   (DOWNLOAD/PUSH/REVERT/LPS/ADOPT/ACCEPT) only ONE edition (the last), silently
#   dropping the rest. Appended rating_key (unique per edition) to libKey.
#   Plus the read-only read-path bleeds from the same sweep: #3 coverage
#   plex_items->placements JOINs (per-section + collections) gained
#   `p.edition_key = pi.edition_key` (two-placed-edition titles double-counted);
#   #4 api_recovery_options' INFO-card `local` query now scopes to the clicked
#   rating_key's edition (prefer-then-'' like the audio endpoint) so "RESOLVED
#   VIA {source}" can't show a sibling's source; #5 /api/coverage/plex `placed`
#   EXISTS (movies/tv/anime/collections) gained edition_key so a placed sibling
#   no longer marks a genuinely-unthemed edition as placed (hiding it from
#   "ready to add"). (Held: #1 worker _do_place ''-scoping — separate confirm.)
# v1.24.18: edition sweep #1 (the held carve-out, now confirmed) — worker
#   cached_rk resolution gated edition scope on TRUTHINESS. _do_place (hardlink
#   FILE path) + _do_place_collection (API-upload, also movie/TV kind='api')
#   resolved the rk they operate on via `_ed_clause = " AND pi.edition_key = ?"
#   if place_edition_key else ""` — so a TAGGED edition scoped, but the STANDARD
#   '' edition fell to the UNSCOPED LIMIT 1 and grabbed an arbitrary sibling
#   (theme into the wrong folder + wrong-rk refresh + split-brain re-enqueue).
#   '' is a REAL edition: scope by PRESENCE, always. Plus a ''-ONLY unscoped
#   fallback (the carve-out) so a default-'' payload on a TAGGED-only title
#   still resolves rather than no-op'ing; TAGGED payloads stay strict (no
#   sibling grab, v1.21.66). Behavioral discriminators on both helpers + source
#   pins on both gates.
# v1.24.19: plex_enum (Plex section refresh) status-bar accuracy, from the
#   status-bar review. (1) The LIVE OPS drawer card head was the generic
#   "// PLEX REFRESH" for every refresh — full scan, single /movies, and
#   collections-only all read identically. run_plex_enum now stamps
#   detail.scope_label ("All libraries" / "Movies" / "Movies (items only)" /
#   "Movies collections") and ops.js appends it to the card kind so the head
#   says WHAT is refreshing (per-section stage_label still names the live
#   section). (2) When Plex returns no totalSize the fetch bar stays
#   indeterminate AND the N/N counter is hidden, so the climbing received count
#   was invisible and the fetch read as stuck — the fetch callback now pushes
#   the running count onto the activity feed when total is unknown.
# v1.24.20: heartbeat-aware fast-reclaim of orphaned sync/plex_enum jobs.
#   A hung/dead sync left its jobs row in status='running' until the 120min
#   runtime backstop, pinning themerrdb_sync_in_flight>0 and wedging the
#   dashboard SYNC button disabled — with "no ops running", because the
#   op_progress stale-sweep had already emptied the LIVE OPS drawer (the user's
#   repro: button stuck at "// SYNCING THEMERRDB…" long after the sync finished,
#   up to 2h). _stuck_job_sweep now reclaims a running sync/plex_enum job within
#   minutes once its op_progress heartbeat is stale/terminal/missing; a healthy
#   long sync keeps its heartbeat fresh and is untouched (120min backstop intact).
# v1.24.21: operability (holistic-review Thread 2). (1) /healthz was an
#   unconditional 200 — the Docker healthcheck meant nothing. Now a real
#   LIVENESS check: DB reachable + (when main.py wires them onto app.state)
#   worker threads + scheduler alive → 200, else 503. Deliberately does NOT
#   ping Plex (external dep down must not make Docker kill us). Sync handler →
#   threadpool, so the blocking DB read is safe + lint-exempt. (2) Plex TEST
#   CONNECTION button on /settings → PLEX: connects with the saved URL+token and
#   surfaces the server name/version (or a clean error) so bad config fails fast
#   at save time, not hours later in a sync. New PlexClient.get_server_info +
#   /api/admin/test-plex, mirroring TEST NOTIFICATION/COOKIES.
# v1.24.22: migration-chain structural guards (holistic-review Thread 1, the
#   verified-real gap). The 66 forward-only _migrate_vN_to_vM had no end-to-end
#   test. Added STRUCTURE-only guards (zero migration-internal edits — the
#   catastrophic-risk zone stays untouched): fresh init lands at v67 with the
#   core tables; the dispatch chain is contiguous 1->66 + matches
#   CURRENT_SCHEMA_VERSION + every defined migration is wired (catches a
#   forgotten version bump / unwired migration); init_db is idempotent (re-run
#   is a clean no-op, no duplicate version row). Deliberately NO full-chain
#   replay test — old migrations rebuild tables to their THEN-current shape, so
#   replaying on a v67 DB is a false crash, not a bug.
# v1.24.23: code-review fixes for the v1.24.18-21 batch (self-review, recall).
#   (#1 HIGH regression in v1.24.20) the heartbeat reaper false-killed a HEALTHY
#   slow git clone: a first-run/oversized-mirror dulwich clone is one blocking
#   call that emits no op_progress heartbeat, so a >15min clone tripped the
#   stale-heartbeat reclaim mid-flight. Now the reaper only reclaims a sync/
#   plex_enum job when its op is TERMINAL or MISSING (the actual orphaned-job
#   signal — the user's "no ops running" repro); a still-running op is left alone
#   regardless of heartbeat freshness. (#3) /healthz now uses get_conn (closes +
#   busy_timeout) instead of a raw sqlite3.connect(timeout=2.0) that leaked the
#   connection and could false-503 under a heavy-write window.
# v1.24.24: code-review finding #2 — the v1.24.18 STANDARD ('') place fallback
#   grabbed an ARBITRARY tagged sibling (unscoped LIMIT 1) when no '' row existed
#   but 2+ tagged editions did (LotR: Theatrical/Extended/Sam) → theme hardlinked
#   into the wrong folder + wrong-rk refresh (cached_folder_path bypasses
#   find_target_folder). Now the fallback fires ONLY when there's EXACTLY ONE
#   candidate; when ambiguous it leaves cached_rk/cached_folder_path None so
#   find_target_folder cleanly no-matches the tagged folders under strict_edition.
#   The single-tagged-folder case still lands (v1.24.18 intent preserved). Both
#   _do_place + _do_place_collection gated; behavioral guards verified to fail on
#   the old arbitrary-pick.
# v1.24.25: resolve_theme_ids re-links a re-added orphan-backed movie by title.
#   A non-TDB movie themed via SET URL gets a synthetic plex_orphan theme; when
#   Plex deletes+re-adds the item (new rating_key) the new plex_items row could
#   only re-bond via guid_imdb (the sole orphan pass), so a guid-less item (a
#   stage musical — the user's "Avenue Q") matched NO pass (sql_title excludes
#   orphans) → theme_id stayed NULL → the surviving canonical + user_override
#   went invisible (SRC=—, no DL to restore). Added a LAST resolve pass
#   (sql_title_orphan): title_norm+year → plex_orphan when theme_id IS NULL,
#   after every real-theme pass so real matches always win. A plex_enum refresh
#   now re-links it; the canonical (in /config/themes, untouched by the Plex
#   delete) is then PUSH-able again.
# v1.24.26: auto-restore a motif-owned sidecar whose Plex theme.mp3 went missing
#   + a theme_auto_restored notification. The v1.25-era follow-up to v1.24.25:
#   once a re-added title re-links, motif should also REDEPLOY its theme without
#   the operator noticing the gap. New hourly scheduler sweep
#   (_restore_lost_placements) finds placements stamped theme_present=0 (the
#   sidecar is CONFIRMED gone by verify_placement_health) that are motif-owned
#   non-plex_upload sidecars with a surviving local_files canonical + a LIVE
#   plex_items folder + no in-flight place job + no permanent-skip reason,
#   RE-STATs each media_folder/theme.mp3 for authority (a stale stamp or mount
#   blip can't trigger a spurious re-place), re-enqueues a place job from the
#   backup, and fires theme_auto_restored (ON by default, warning-level) so the
#   operator knows motif self-healed the delete+re-add case the v1.18.90 reaper
#   silently skipped as 'other_fallback'.
# v1.24.27: year-LESS orphan re-link (Avenue Q, the data-confirmed sequel to
#   v1.24.25). the user's prod diagnostic showed the re-added Avenue Q came back
#   from Plex with year='' (folder "Avenue Q ()") — so v1.24.25's title+year
#   orphan pass couldn't match the surviving plex_orphan theme's year='2003'
#   (and the row had no guids) → stranded at theme_id=NULL, canonical+override
#   invisible (SRC=—, no DL). New resolve pass sql_title_orphan_yearless: when a
#   row has NO usable year (NULL or '') AND exactly ONE plex_orphan theme shares
#   its title_norm, bond by title alone (the v1.24.18 exactly-one gate keeps
#   remakes safe; a row that DOES carry a year still requires the year to match
#   via sql_title_orphan, preserving the Wonka 1971-vs-2023 guard). Runs LAST,
#   after the year-matching orphan pass, so a precise match always wins first.
# v1.24.28: stale plex_upload detection + honest "RE-PUSH" display. A theme
#   deployed via plex_upload (POST to a Plex rating_key's metadata) showed as
#   placed (PL/PU) forever even after a Plex delete+re-add destroyed that rk —
#   verify_placement_health excluded plex_upload from its sidecar stat, so
#   theme_present stayed NULL (the user's Avenue Q: uploaded to rk 660896, re-added
#   as 714864, nothing serving, yet PL/PU). New health pass stamps plex_upload
#   theme_present=1 if plex_rating_key is still a live plex_items row else 0
#   (guarded on plex_rating_key IS NOT NULL; self-healing on re-push). The
#   library read nulls media_folder + placement_kind for a stale plex_upload (so
#   every placed/SRC/PL read treats it not-placed → SRC='-') + sets needs_repush,
#   driving a distinct orange RP LINK badge + filter chip. Re-push stays manual
#   (the user's call); auto-restore for plex_upload is a planned follow-up.
# v1.24.29: auto re-PUSH a stale plex_upload (the v1.24.28 follow-up the user chose
#   to build). _restore_lost_placements (the v1.24.26 sidecar auto-restore sweep)
#   gains a plex_upload branch: a placement stamped theme_present=0 whose uploaded
#   rating_key is re-verified dead AND whose theme now has a LIVE bonded
#   plex_items row (post-v1.24.27 re-link, a current rk to upload to) re-pushes
#   via the same place-job + theme_auto_restored path. Same skip-reason +
#   per-edition in-flight dedup gating as the sidecar branch; no FS stat (the SQL
#   rk-liveness gate is the authority). Closes the Avenue Q arc: a delete+re-added
#   plex_upload now self-heals end-to-end (re-link → honest RP state → auto
#   re-push) within an hour.
# v1.24.30: backfill themes.title_norm so the whole orphan cohort can re-link.
#   The v1.24.27 prod diagnostic found ~340 plex_orphan themes with
#   title_norm=NULL — 4 of the 5 orphan-creation paths (adopt ×2, bulk import,
#   the collection SET-URL) never stamped it (only the v1.24.x movie SET-URL did,
#   which is why Avenue Q had it). The v1.24.25/.27 title re-link passes match
#   t.title_norm = plex_items.title_norm, and NULL = anything is FALSE in SQL, so
#   those orphans could NEVER self-recover on a Plex delete+re-add. resolve_theme
#   _ids now backfills title_norm = normalize_title(title) on any theme missing it
#   BEFORE its title-match passes (the single chokepoint that consumes title_norm)
#   — every refresh self-heals the whole cohort + any future NULL, no restart,
#   idempotent. Closes the orphan-recovery gap the Avenue Q arc surfaced.
# v1.24.31: uniform YouTube notification thumbnails. The Discord theme
#   notifications showed variable-size cards (the user: "some large some small")
#   because Discord auto-embedded the raw YouTube watch URL — its rich card
#   sizes to the source (landscape for videos, tall portrait for Shorts). Gave
#   YouTube the v1.22.94 Facebook treatment: _render_url_lines wraps the URL in
#   <...> to suppress the auto-embed, attachment_thumb_url returns the
#   deterministic mqdefault.jpg, and notify._prepare_attachment normalizes it to
#   the uniform 400x224 canvas (ytimg.com added to the attachment CDN allowlist).
# v1.24.32: independent toggles for the two auto-restore branches. the user
#   wanted settings switches to enable/disable the auto-recovery behaviors;
#   chose separate sidecar vs plex_upload re-push. New PlacementConfig fields
#   auto_restore_sidecar / auto_restore_plex_upload (both default ON = the
#   v1.24.26/.29 behavior) gate the two branches of _restore_lost_placements
#   (getattr-default True so an older yaml stays always-on). Two settings
#   toggles + the existing save()+reload() path makes them live without a
#   restart. Detection + the RP badge are unaffected when a toggle is OFF.
# v1.24.33: RP rows get a distinct, sortable re-push attention glyph. A stale
#   plex_upload (RP) reads as downloaded-but-unplaced, so it tripped the generic
#   amber "!" awaiting-placement glyph — looked like an ATTN icon + no way to
#   find them (the user). New orange ⟳ title glyph (matching the RP LINK badge),
#   checked before awaitingApproval, that LINKS to link_pills=rp so one click
#   surfaces every row needing a re-push. Unambiguous + findable.
# v1.24.34: ensure-coverage backfill for historical edition_key='' canonicals.
#   the user's Two Towers 409 ("no local file to replace from"): a pre-edition-
#   aware theme has its canonical at '' while the live Plex rows are tagged
#   (theatrical/extended), so the edition-scoped push + v1.24.28-29 RP
#   auto-recovery can't find a canonical for the tagged edition. Prod diagnostic:
#   31 such titles (LotR, SW fan-edits, Watchmen, director's cuts). the user chose
#   ensure-coverage (the theme is identical across editions): new marker-gated
#   one-shot walker maybe_backfill_edition_canonicals creates a per-edition
#   local_files row sharing the '' canonical for every live non-'' edition that
#   lacks one, so each becomes push-able + auto-restorable; the normal retry sweep
#   then places them. Idempotent, amplifier-sweep guarded, marker-once (respects a
#   later UNMANAGE), re-wired into boot.
# v1.24.35: code-review fixes #1-#3 (the v1.24.25-34 review). #1: the
#   auto-restore sweep enqueued place jobs with no `kind`, so a stale
#   plex_upload re-deployed as a SIDECAR (worker fell to default_method='file')
#   instead of re-uploading; now the payload carries kind='api' for plex_upload
#   and 'file' for sidecar. #2: neither branch checked the canonical exists —
#   a CONFIRMED-missing canonical (canonical_present=0) raised in the worker
#   without a skip-reason stamp → a doomed place re-enqueued + a false
#   "restored" notification every hour; now gated on canonical_present!=0. #3:
#   YouTube notifications suppressed the auto-embed unconditionally, so a row
#   with no parseable video_id (no thumb) showed NO image; now only suppress
#   when a thumb is actually available.
# v1.24.36: code-review fix #4 (sort-vs-render drift for RP rows). A stale
#   plex_upload (RP) renders PL='await' (amber) + the orange RP LINK badge
#   because the read path nulls media_folder/placement_kind — but the PL/LINK
#   ORDER-BY CASEs read the RAW placement_kind/theme_present, so it sorted as
#   red-'broken' (PL) / lumped with PU (LINK), contradicting the chip it paints
#   (the v1.23.24/.25 "rank by the chip you paint" invariant). Now _LIB_STALE_PU_SQL
#   pins the stale state to the await rank (PL=3) and a dedicated first rank (LINK=-1).
# v1.24.37: code-review fix #8 (perf). The v1.24.30 title_norm backfill pre-pass
#   probed `SELECT ... FROM themes WHERE title_norm IS NULL` at the end of EVERY
#   enum + sync — a full ~50K-row themes SCAN, since idx_themes_title_norm is
#   partial (WHERE title_norm IS NOT NULL) and excludes the NULL rows. The whole
#   NULL cohort is plex_orphan (real themes get title_norm from sync), so scoping
#   the probe to upstream_source='plex_orphan' rides idx_themes_orphan: SCAN →
#   SEARCH. Future-NULL self-heal unchanged.
# v1.24.38: code-review fix #6. theme_auto_restored fired at ENQUEUE in the
#   scheduler's _restore_lost_placements sweep — claiming "restored" before the
#   place jobs ran, so a place that later failed still pinged success. Now the
#   sweep only enqueues (payload tagged reason='auto_restore'); the worker fires
#   theme_auto_restored from _do_place / _do_place_collection on outcome.placed
#   (the place actually landing), coalesced so a Plex-re-add burst still
#   collapses to one summary. New coalesced formatters in notify_content.py.
# v1.24.39: code-review fix #5 (defense-in-depth). verify_placement_health's
#   plex_upload staleness pass UPDATE'd theme_present=0 for every uploaded rk not
#   in plex_items — unbounded. A failed/aborted enum that left plex_items EMPTY
#   would false-flip EVERY upload to RP → a mass v1.24.29 auto re-push storm. Now
#   the 0-stamp is skipped when plex_items is empty (a real library with uploads
#   always has items; a genuine mass re-add leaves plex_items FULL). The
#   confirmed-present 1-stamp is always applied. Partial under-population stays
#   bounded upstream by the v1.18.89 reaper's seen_rks gate + 20%/50-row cap.
# v1.24.40: RP (re-push needed) discoverability + accuracy. (a) Tightened
#   _LIB_STALE_PU_SQL to require the uploaded rk is genuinely DEAD (no live
#   plex_items row) — a re-linked/self-healed stamp (the user's Two Towers, rk
#   714913 live + has_theme=1) no longer false-reads RP. (b) New ⟳ ATTN filter
#   chip (attn_pills=repush) reusing the SAME predicate as link_pills=rp — one
#   source of truth, two entry points. (c) New topbar RE-PUSH count badge
#   (mirrors FAIL/UPD/DROP) so a genuine re-push surfaces without hunting.
#   Legend + glossary updated.
# v1.24.41: code-review of v1.24.40 — RE-PUSH badge count/render parity. (#1)
#   _REPUSH_COUNT_SQL counted FROM placements while the library renders FROM
#   plex_items, so a stale plex_upload whose item was REMOVED (no live plex_items
#   row) inflated the badge but never rendered (un-clickable). Rebuilt the count
#   as a mini-render (plex_items-anchored, WHERE _LIB_STALE_PU_SQL) so count ==
#   rendered RP rows. (#2) added a tab_hint so the badge routes to the owning tab
#   (collections — the likely RP source — instead of a static empty /movies).
#   (#3) the attention/NEEDS WORK sort bucketed bare theme_present=0 as urgent
#   with no rk-liveness gate — a live-rk stale plex_upload topped NEEDS WORK
#   looking placed; now reuses _LIB_STALE_PU_SQL. (#4) added the
#   plex_rating_key IS NOT NULL guard to _LIB_STALE_PU_SQL (matches the count).
# v1.24.42: security audit (2026-06-25, pip-audit + ffmpeg CVE review). Bumped
#   dependency floors to patched minimums: fastapi 0.115.*→0.138.* + explicit
#   starlette>=1.3.1 (0.115 capped starlette below every fix); yt-dlp→2026.6.9;
#   dulwich→>=1.2.6; python-multipart→>=0.0.31. Added a Dockerfile build-time
#   ffmpeg-version guard (fail on a pre-trixie/bookworm ffmpeg unpatched for
#   CVE-2026-8461 "PixelSmash"). For motif all the flagged CVEs were admin-only
#   DoS behind Authentik, non-applicable (Windows-only / server-side / read-only
#   fetch client), or already neutralized (fixed yt-dlp outtmpl, no external
#   downloaders/--exec) — this is hygiene + makes the patched minimums explicit.
# v1.24.43: AWAIT topbar badge. the user wanted downloaded-but-not-placed (!P)
#   rows — e.g. the v1.24.34 edition-coverage backups — surfaced proactively like
#   FAIL instead of having to filter to ATTN=!P. Mirrors the v1.24.40 RE-PUSH
#   badge: a count (_AWAIT_COUNT_SQL) + tab_hint routing to the owning tab, hidden
#   at 0. _LIB_AWAIT_SQL is the single predicate shared by the attn_pills=await
#   FILTER and the badge COUNT, so they can't drift (the v1.24.41 lesson).
# v1.24.44: topbar badges cycle through EVERY impacted tab (incl. collections).
#   the user: a "7 AWAIT" badge (6 movies + 1 collection) always landed on /movies
#   — the collection was unreachable. RE-PUSH + AWAIT carried only a single
#   tab_hint (LIMIT 1); DROP had a breakdown but no cycle binding. Added per-tab
#   breakdown queries (_REPUSH/_AWAIT_TAB_BREAKDOWN_SQL) + a shared bindBadgeCycle
#   JS binder for DROP/RE-PUSH/AWAIT, and widened the cycle regex to include
#   /collections so being on the collections tab finds its position. FAIL/UPD
#   already cycled.
# v1.24.45: over-ceiling collection theme — auto-downscale + surface. the user's
#   Middle-Earth Collection theme (12.5MB) exceeded Plex's ~10MB upload ceiling →
#   HTTP 500, and collections have NO sidecar fallback (no folder), so it
#   terminal-failed silently. _do_place_collection now re-encodes an over-ceiling
#   theme down to fit (ffmpeg, duration-aware bitrate) before upload so it
#   deploys; if it still can't fit (theme too long / no ffmpeg) it stamps a
#   distinct plex_rejected:over_ceiling reason (kept under the plex_rejected:
#   prefix so the scheduler retry-skip gate is unchanged) + a red ⊘ row glyph.
# v1.24.46: code-review follow-ups on the v1.24.43-45 arc.
#   (#1) The AWAIT badge counted over-ceiling collection rows that v1.24.45
#   simultaneously marks ⊘ "too large, can't place" — one doomed row got two
#   contradictory surfaces, and clicking AWAIT→PLACE re-enqueued the doomed
#   upload. _LIB_AWAIT_SQL now excludes plex_rejected:over_ceiling (NULL-safe IS
#   NOT), so the actionable count/filter agrees with the glyph.
#   (#2) _do_place_collection accepted a downscale that was smaller than the
#   ORIGINAL but still over the ceiling, then uploaded it for a guaranteed 500.
#   The gate now requires the re-encode to actually FIT the ceiling; a measured
#   still-over re-encode short-circuits to over_ceiling without the doomed POST.
#   (#6) _downscale_audio_to_fit's multi-line docstring → # comments (CLAUDE.md).
# v1.24.47: LOW cleanup pass — remaining v1.24.43-45 review findings.
#   (#7) The topbar cycle badges' breakdown (GROUP BY type,is_anime,is_4k,
#   media_type) could emit two rows that collapse to the same {tab, fourk} — an
#   anime-section AND a movie-section collection both route to 'collections' — so
#   the cycle saw a duplicate destination and a click "advancing to the next"
#   landed on the URL you were already on (a dead click). New shared
#   _breakdown_tabs() dedups by (tab, fourk); _breakdown_tab_hint() derives the
#   owning tab from breakdown[0]. Applied to the bindBadgeCycle family
#   (drops / repush / await), replacing 3 hand-mirrored comprehensions.
#   (#5) Those helpers fold the separate per-badge LIMIT-1 tab_hint queries
#   (_REPUSH/_AWAIT_TAB_HINT_SQL) into the breakdown the badge already runs —
#   2 redundant plex_items-anchored scans off the /api/stats + SSR hot path.
#   (#8) Marker: an over-ceiling collection's UPLOADED bytes intentionally differ
#   from the canonical on disk (file_sha256 unchanged) — for future reconcile.
# v1.24.48: review #9 — converge the topbar badge cycle binders. FAIL + UPD kept
#   their own bindFailureBadgeCycle / bindUpdatesBadgeCycle copies while DROP /
#   RE-PUSH / AWAIT used the shared bindBadgeCycle, so the cycle logic + the
#   /collections tab regex lived in THREE places — the next library-tab axis
#   change would have to touch all three or a badge silently fails to cycle to the
#   new tab (the v1.24.44 bug class). FAIL + UPD now call the one shared
#   bindBadgeCycle too; behavior unchanged, one implementation. ALSO measured the
#   review #4 SSR concern on the prod DB: the full 5-subquery topbar frame is
#   ~37ms (repush+await ~14ms+~11ms) — NOT the historical 20s — so left it as-is
#   (a TTL cache would shave an imperceptible 37ms onto a perf-sensitive path).
# v1.24.49: stamp the running version on the first boot log line ("motif vX.Y.Z
#   starting") so a pasted `docker logs` snippet carries it — the version showed
#   only in the UI topbar before, so logs alone couldn't tell which build emitted
#   them (the user's ask while diagnosing the worker-startup lines).
# v1.24.50: migration crash-loop hardening (additive class). init_db stamps
#   schema_version per-step but executescript autocommits, so a crash (kill -9 /
#   OOM / power-loss) between a column's commit and the version stamp left the
#   column present + version behind → boot re-ran the migration → a bare ALTER ADD
#   COLUMN raised "duplicate column name" forever. New idempotent _add_column
#   helper (PRAGMA table_info guard) now backs all 15 reachable (v21+) ADD COLUMN
#   migrations; v27→v28 audit_events CREATE gained IF NOT EXISTS. New
#   test_v1_24_50 migration-chain guard: fresh→v67, recent-chain re-run, per-
#   migration additive-crash-loop check (future-proofs a new bare ADD COLUMN), the
#   v17-21 fresh-start wall, + the helper. Destructive/table-rebuild migrations
#   (DROP COLUMN / CREATE-new→DROP→RENAME) at ancient versions (v26-31, no live DB)
#   are a separate harder class, deferred; the practically-reachable recent window
#   is verified clean.
# v1.24.51: migration crash-loop hardening (destructive class — completes the
#   arc, the user: "do both"). Audited every reachable (v21+) destructive/rebuild
#   migration: all but ONE were already crash-safe — v26→v27 + the v30+ inline
#   table rebuilds are BEGIN/COMMIT-atomic (v1.22.66), v54→v55/v57→v58/v59→v60 use
#   the idempotent _widen_check_constraint helper (v1.19.73/74), v52→v53's DROP
#   COLUMN is table_info-guarded. The gap: v29→v30 backfilled previous_urls from
#   themes.previous_youtube_url then DROPped it in one executescript (autocommits
#   each statement) → a crash after the DROP re-ran the INSERT...SELECT against the
#   dropped column → "no such column" crash-loop. Now gated on the source column
#   (mirrors v52→v53), each DROP independent so a crash between the two completes.
#   test_v1_24_51: behavioral forward + crash-loop re-run + between-drops, plus a
#   structural guard that the inline rebuilds keep their BEGIN/COMMIT atomicity.
# v1.24.52: dashboard RECENTLY ADDED carousel (the user liked the sibling
#   Missing-Trailer-Downloader dashboard). A poster strip of the titles motif
#   most recently placed a theme on → click a poster opens the same INFO card a
#   library row does. Two new endpoints: GET /api/recently-placed (distinct
#   recently-placed titles, newest first, plex_items 'show'→'tv') and GET
#   /api/plex/art/{rk} — a same-origin Plex poster proxy (token in the
#   X-Plex-Token header server-side, never the URL; rating_key must be all-digits
#   to block path-injection; off the event loop via run_in_threadpool). Frontend:
#   loadRecentlyAdded() builds cards via DOM APIs (XSS-safe) with a content-hash
#   to skip the 30s-poll rebuild; CRT-palette CSS on motif tokens. First of a
#   dashboard-enhancement arc (services panel + stats table + 3 donuts to follow).
# v1.24.53: dashboard SERVICES panel (dashboard-enhancement arc, #2 of 4). A
#   pinned bottom strip of status cards for motif's external dependencies: Plex
#   (online + round-trip latency + friendly name/version via a short-timeout GET /
#   probe — the token in the X-Plex-Token header) and yt-dlp (running version).
#   New GET /api/services (off the event loop via run_in_threadpool); loadServices()
#   renders DOM-API cards on motif tokens (green/red/mute status dot). Remaining
#   arc: general-statistics table + 3 coverage donuts.
# v1.24.54: carousel polish (the user feedback after v1.24.52/53 landed live).
#   (bug) Clicking a COLLECTION poster opened a broken INFO card —
#   /api/recently-placed returned tmdb_id from pi.guid_tmdb, which is NULL for
#   collections (no real TMDB id), so openInfoDialog called
#   /api/items/collection/null → 422. Now reads the synthetic placements.tmdb_id
#   (the id the /collections row + info card use); movies/shows unaffected.
#   (feat) RECENTLY ADDED gained an // auto-scroll toggle (the reference's "Auto
#   Scroll"): slowly advances the strip, pauses on hover, loops, persisted in
#   localStorage. Off by default.
# v1.24.55: SOURCE BREAKDOWN → a 3-up donut row (Total / Movies / TV), replacing
#   the prior single + collections pies (the user: "replace our existing 2 with the
#   three new ones · keep the SRC distribution, just 3-up"). Same T/A/U/M/P/– SRC
#   buckets; each donut scopes the same /api/stats theme_sources feed by
#   media_type and the three SHARE one legend-toggle (hide a letter → hidden on
#   all three). TV folds anime in (theme_sources carries no anime split); the
#   standalone collections source donut is dropped. _renderSourcePie reused;
#   renderTotal/Movies/Tv + renderAllSourcePies replace renderTheme/Collections.
# v1.24.56: // GENERAL STATISTICS — per-library source-split table adopted from
#   the Missing-Trailer-Downloader dashboard the user asked to mirror ("general
#   Statistics better" + clarified the panel he wanted was the stats table, not
#   services). Each library (Total / Movies / TV / Anime / Collections) × [LOCAL
#   (T/A/U/M, motif owns the theme file) | PLEX (P, Plex-Pass served) | MISSING
#   (–) | COVERAGE %], on motif's SRC axis — no genre-skip column (motif has no
#   genre-skip concept). Pivots the SAME /api/stats theme_sources feed the donuts
#   use; theme_sources now carries ps.is_anime so the ANIME row splits from TV
#   (plex_items.media_type is 'show' for both — the split lives on the section).
#   The donuts ignore is_anime (aggregate by letter), so the feed change is
#   additive. renderGeneralStats() pivots client-side; JS-revealed on data.
# v1.24.57: RECENTLY ADDED carousel polish (the user). (1) media-type glyph
#   (▶/▭/▦, reused from the coverage cards) ahead of the year in each card's
#   meta line. (2) .recent-title gets a fixed 2-line min-height so 1-line and
#   2-line titles occupy the same height — the year/date row then aligns across
#   every card. (3) auto-scroll defaults ON (unset localStorage reads as enabled;
#   only an explicit uncheck persists '0') AND the "not scrolling" bug fixed:
#   .recent-strip's scroll-snap-type re-snapped every 1px increment back to
#   offset 0, so the strip never moved with the toggle on — scroll-snap removed.
# v1.24.59: SOURCE BREAKDOWN donut toggles independent again (the user: "make the
#   toggle independent of one another"). v1.24.55 shared one hidden-set across
#   Total / Movies / TV (a legend click hid the letter on all three); now each
#   donut owns its own Set + localStorage key and a click hides the letter on
#   JUST that donut. Movies/TV seed once from the legacy shared key so the deploy
#   doesn't visually jump, then diverge. Click delegate discriminates by the
#   enclosing .source-pie-col id and re-renders only the clicked donut.
# v1.24.58: carousel meta layout follow-ups (the user). Title + info centered
#   under the poster (was left-aligned). Year rendered in (brackets) like a
#   normal movie/show year; year and placed-date on their OWN lines (dropped the
#   "·" separator). Title forced to a single ellipsised line (the mixed
#   1-line/2-line look read ragged) — which also aligns the meta without the
#   v1.24.57 2-line reserve. Bigger posters (116→150px) + wider gap (gap-5) so
#   fewer show at once with more spacing.
# v1.24.60: code-review cleanup of the v1.24.55-59 donut churn (no behavior
#   change). Dropped dead plumbing the shared→independent refactor left: the
#   _pieState.{total,movies,tv}_rows fields + the lastRowsKey stash (written,
#   never read — the click handler re-renders from all_rows); the summaryId/sumEl
#   summary branch in _renderSourcePie + its unitNoun opts (no renderer passed
#   summaryId once the -summary spans were dropped). Refreshed the stale
#   collections-pie WHY-comment at the render call site.
# v1.24.61: carousel auto-scroll fixes (the user). (1) Still off by default for him
#   — his prefs carried an explicit '0' persisted during the v1.24.54-56
#   broken-scroll phase, which beat the v1.24.57 default-ON. Versioned the
#   localStorage key (motif:recentAutoScroll → ...2) so that stale '0' is ignored
#   → unset → ON. (2) Hide the horizontal scrollbar while auto-scrolling via a
#   toggled .recent-strip-autoscroll class (scrollbar-width:none + ::-webkit-
#   scrollbar display:none); overflow-x stays auto so scrollLeft still drives it.
# v1.24.62: make the RECENTLY ADDED carousel + SERVICES panel hideable via the
#   existing // customize layout feature (the user: "the option to hide a section").
#   They sit OUTSIDE #dash-sections as fixed strips, so they were the only
#   non-hideable sections. Marked data-dash-pinned → a hide-only toggle (no
#   reorder; they keep their fixed top/bottom position) that shares the same
#   dashboard_layout persistence. dashboard-customize.js: pinnedSections/isPinnedId
#   helpers; applyLayout keeps + applies pinned hidden state; rebuildLayoutFromDOM
#   preserves pinned entries; inject/removePinnedControls + onPinnedToggleClick.
#   CSS reveals a hidden pinned section (dimmed) in customize mode so it can be
#   toggled back on.
# v1.24.63: carousel media-type icons → Feather-style inline SVG line icons
#   (film for movies, tv-with-antenna for TV, stacked-cards for collections),
#   replacing the ▶/▭/▦ glyphs (the user preferred these). The film + tv icons match
#   the Missing-Trailer-Downloader carousel; rendered in currentColor so they pick
#   up the muted meta tone. _recentTypeGlyph → _recentTypeIcon (returns a fixed
#   SVG constant, set via innerHTML — injection-safe). Dashboard coverage-card
#   glyphs (▶/▭/✦/▦) are unchanged — different surface, not asked for.
# v1.24.64: carousel auto-scroll also pauses while a modal dialog is open (the
#   INFO card, or any confirm) — the strip shouldn't drift behind it (the user).
#   The tick() guard adds `document.querySelector('dialog[open]')` (the codebase
#   idiom for "a native modal is showing") alongside the existing hover-pause;
#   the 30ms poll resumes scrolling once the dialog closes.
# v1.24.65: customizable per-library accent colors (the user). The PLEX dashboard
#   card colors (movies/tv/anime/collections) AND the active nav-tab underline
#   now resolve from 4 CSS vars (--dash-{movies,tv,anime,collections}-color)
#   defaulting to the existing tokens. // customize layout grows a // LIBRARY
#   COLORS panel (4 color pickers + // RESET COLORS); overrides persist in
#   localStorage 'motif:dashColors' and apply pre-paint on EVERY page via a
#   base.html head script (so the nav underline is colored everywhere, no flash).
#   The active library tab's underline now matches its card color (anime pink,
#   collections red, …) — DASH/LOGS/SETTINGS keep the default green.
# v1.24.66: (1) dashboard stat-card glyphs (MOVIES/TV/ANIME/COLLECTIONS THEMED +
#   PLEX rows) → Feather-style SVG line icons matching the carousel (film / tv /
#   sparkle-for-anime / stacked-cards), via a new media_glyph() Jinja macro;
#   replaces the ▶/▭/✦/▦ unicode set. (2) Carousel horizontal scrollbar is now
#   visibly styled (slim, on-palette) when auto-scroll is OFF — so manual scroll
#   is obviously available (the user) — and still hidden while auto-scrolling.
# v1.24.67: PER-SECTION COVERAGE + GENERAL STATISTICS now sit side by side as a
#   2-up .dash-pair flex row (the user: "double wide instead of 1 row each") —
#   two compact tables that each waste horizontal space at full width. The
#   wrapper is a single customize unit (keeps the section-coverage layout key so
#   an existing saved layout positions the pair); each inner .block keeps its id
#   for JS reveal + flexes to fill the row if its sibling is hidden, and wraps to
#   stacked below ~700px. The standalone GENERAL STATISTICS section was removed.
# v1.24.68: SOURCE BREAKDOWN restyled to match the reference's TRAILER COVERAGE
#   (the user: "their own little block instead of one large bar · same sort of
#   selectors"). Each donut is now its OWN card (.source-pie-col: border + grid,
#   donut beside its legend); the outer section is borderless (plain label + the
#   3 cards, cards wrap via auto-fit minmax(320px)). Legend → one selector per
#   row with the value group right-aligned as "count (pct%)" + 1-decimal pct,
#   mirroring "Local Trailer  4271 (29.0%)". Stale "hide it on all three"
#   subtitle fixed (toggles are per-chart since v1.24.59).
# v1.24.69: the STATISTICS pair (PER-SECTION COVERAGE + GENERAL STATISTICS) now
#   stretches to equal height (.dash-pair align-items: stretch) so the two cards
#   are the same size even when one table has more rows (the user) — the shorter
#   table sits at the top of its card.
# v1.24.70: (1) SWITCH PLACEMENT notifications no longer read as new themes —
#   reason='user_switch_placement' now gets its own title ("📤 Placement
#   switched — …") + "via SWITCH PLACEMENT" body label, instead of the generic
#   "Theme pushed to Plex" (the user: switch-to-API actions looked like new
#   themes). (2) The auto-scroll checkbox tick is muted (accent-color
#   --green-deep) — the bright --green stood out too much for a minor toggle.
# v1.24.71: don't offer REPLACE TDB when ThemerrDB has no theme video. A title
#   can be TDB-TRACKED (upstream_source set) yet have themes.youtube_url empty
#   (TDB catalogs it but has no theme yet — the user's Daredevil: Born Again).
#   REPLACE TDB was gated on isThemerrDb but not it.youtube_url, so it appeared +
#   409'd "ThemerrDB record has no youtube_url". Added the same it.youtube_url
#   gate DOWNLOAD TDB BACKUP already uses. Backend still validates (defense).
# v1.24.72: REPLACE TDB now survives a STALE accepted_update on overridden rows.
#   accepted_update is a sticky historical flag (an upstream TDB update was
#   accepted once). The gate's bare !accepted_update kept suppressing REPLACE TDB
#   even after the user later overrode the accepted theme with a user URL (SRC=U)
#   — the user's Super Mario Galaxy had no REPLACE TDB despite a valid TDB url. The
#   no-op concern only holds while the accepted TDB is still active (SRC=T), which
#   the placement clause already excludes; relaxed to
#   (!accepted_update || srcLetter !== 'T'). Mirror + node harness kept in sync.
# v1.24.73: distinct TDB pill for a tracked-but-themeless record. The green TDB
#   pill is the row-render FALLTHROUGH — it fired whenever upstream_source was set,
#   even when themes.youtube_url is empty (TDB lists the title but has no theme
#   video). That read as "healthy theme available" while v1.24.71 correctly hid
#   REPLACE TDB on the same row — the user: confusing. Added a muted "TDB ∅" pill
#   gated on the SAME !it.youtube_url the REPLACE TDB gate uses, so pill + action
#   agree. New .tdb-pill-empty CSS (dashed/muted) + legend gloss in both decoders.
# v1.24.74: re-sync the TEST-ONLY menu-actions mirror (lib/menu-actions.js) with
#   the live app.js renderLibraryRow gates — it's not loaded by any template, so
#   drift silently under-tested the real UI. REPLACE TDB had lost 3 clauses
#   (it.youtube_url v1.24.71, tdbActionPendingOk v1.20.2, !lpsHasCanonical
#   v1.14.46); the audit also caught stale redl (!isOrphan v1.17.17), revert
#   (src='M' RESTORE branch v1.12.81) and purge (orphanHasPurgeableState v1.18.8)
#   gates. Rejected delegating app.js → computeMenuActions (the mirror is a
#   deliberate subset; full superset rewrite would be high-risk on the row menus)
#   in favor of a clause-set drift lint (test_v1_24_74_menu_actions_mirror_drift)
#   + new node-harness behavioral cases.
# v1.24.75: TDB ∅ filter chip — surface tracked-but-themeless rows. Even though
#   only ~6 rows are in this state (verified on prod), the user wants every TDB pill
#   state filterable. New `empty` bucket across all layers (template chip, JS
#   computeTdbPill + ALL set + URL parser, server _pset + SQL branch). The green
#   `tdb` filter is tightened to REQUIRE a non-empty youtube_url so green ⊎ empty
#   is a clean partition (matches the v1.24.73 pill render). Daredevil now filters
#   under TDB ∅ instead of green TDB.
# v1.24.76: TDB legend/glossary up-to-date pass. Audit of the row-pill render vs
#   the // GLOSSARY (base.html) + in-context LEGEND (library.html) + filter chips
#   found the cookies state still decoded as the OLD "TDB ⚠" in all 3 surfaces
#   while the pill itself renders "TDB ⚿" (the v1.15.17 squared-key glyph) —
#   synced them to ⚿. New render↔decode glyph-consistency lint guards the whole
#   TDB axis. (∅ already covered v1.24.73/75; ATTN ⊘ toobig is render-only by
#   design; LINK condensed-legend subset left as-is.)
# v1.24.77: dashboard customize-mode layout fixes + GENERAL STATISTICS fill.
#   (1) // LIBRARY COLORS panel detached from the PLEX cards on reorder — it's a
#   standalone sibling injected before [data-dash-section="plex-coverage"];
#   repositionColorPanel() now re-pins it before the plex section from
#   rebuildLayoutFromDOM (the post-reorder chokepoint). (2) The STATISTICS
#   .dash-pair rendered as "a single large row and a small row" in customize mode
#   — the injected .dash-section-controls bar was a flex sibling of the two
#   columns; it now gets flex:0 0 100% so it owns row 1 and the cols wrap below
#   side-by-side. (3) GENERAL STATISTICS left bottom whitespace in the equal-height
#   pair — .dash-pair-col is now a flex column with the table flex:1 so the shorter
#   table's rows distribute the surplus height and fill the card.
# v1.24.78: dashboard customize audit follow-ups (review after v1.24.77).
#   (1) Grid analogue of the v1.24.77 dash-pair fix: the stat-card .grid sections
#   (top-stats/plex-coverage/operations/activity/storage) carry data-dash-section
#   on the .grid + keep display:grid while editing, so the injected controls bar
#   landed in the first grid CELL and shoved the cards into a ragged row (the user's
#   PLEX LIBRARY control box wedged left of the cards). Added grid-column:1/-1 so
#   it spans full width like the dash-pair flex:0 0 100%. (2) saveLayout() nulled
#   SAVE_TIMER without clearTimeout → exitCustomize's immediate flush during an
#   armed 300ms debounce fired a redundant duplicate PUT; now clears it first.
#   Audit also swept every sibling-walk + the save/sanitize/restore round-trip —
#   no other live bugs (onArrowMove was the only single-step walk; persistence is
#   sound; the `copies` card-vs-section id overlap is benign separate namespaces).
# v1.24.79: dashboard data fixes (from the dashboard audit). (1) ADDED TODAY /
#   ADDED THIS WEEK + the 30-day download-insight queries string-compared an
#   ISO-'T' timestamp (placed_at/created_at = 'YYYY-MM-DDTHH:MM:SS+00:00') against
#   datetime('now',...) ('YYYY-MM-DD HH:MM:SS', space sep); 'T'>' ' over-counted
#   any same-date-earlier-time row (prod ADDED THIS WEEK 15 vs true 14). Wrapped
#   the column in datetime() at all 6 sites. (2) PER-SECTION COVERAGE labeled the
#   Collections row 'MOVIES / STD' (no collections branch in typeLabel) → now
#   'COLLECTIONS' with no suffix. Bigger audit findings (per-section totals fold
#   collections in / FAILURE BREAKDOWN drill hardcodes /movies) reported separately.
# v1.24.80: PER-SECTION COVERAGE excludes collection items (the user chose this).
#   Collections live as media_type='collection' plex_items INSIDE the movie/show
#   sections but have their own tab + a synthetic Collections row; counting them
#   in each section's TOTAL inflated it (Movies 11.5k vs the /movies tab's 10.5k)
#   AND double-counted them against the Collections row (per-section grand total >
#   general-stats total). The per-section query now joins `AND pi.media_type !=
#   'collection'`, so each section row matches its drill-down tab + the axis no
#   longer double-counts.
# v1.24.81: FAILURE BREAKDOWN bars drill into the owning tab. Pre-fix every
#   failure-kind bar hardcoded /movies?status=failures, but the library's
#   status=failures filter is media-type-scoped to its tab, so an anime/TV-only
#   failure kind landed on an EMPTY movies view (same class as the v1.12.11 topbar
#   FAIL fix). /api/dashboard/insights now buckets failures by (kind, tab) and
#   returns, per kind, the cross-library total + the tab owning the most; the JS
#   routes the bar to r.tab. (4K-section edge stays at the tab's STD default.)
# v1.24.82: dashboard LOW-severity polish (audit follow-up, last of the 3 the user
#   OK'd). (1) Carousel autoscroll tick bails on document.hidden (no idle-tab
#   scroll churn) + reads hover live via :hover instead of a `paused` flag that
#   could stick true if a 30s poll re-rendered the strip mid-hover. (2) PER-SECTION
#   COVERAGE THEMED/UNTHEMED cells are now click-through filters (status=has_theme
#   / untracked), mirroring the failures+pending cells. SERVICES-panel re-paint
#   left as-is (latency changes every poll — documented no-hash by design).
# v1.24.83: movie/show poster on the // MOTIF INFO card (the user — MTDP-style
#   flare). A poster hero sits beside the title/scope block, sourced from the
#   existing /api/plex/art/{rk} Plex proxy the dashboard carousel already uses.
#   posterRk prefers the clicked edition's rating_key, else a placement's
#   plex_rating_key (for the 2-arg openInfoDialog callers). The <img> is removed
#   on 404 / non-digit rk so the hero collapses to just the meta (no broken box).
# v1.24.84: code-review cleanup of v1.24.83 — drop the redundant `width: 120px`
#   on .info-poster (flex: 0 0 120px already pins the box; the width was dead).
#   No visual change. The xhigh /code-review found no real bugs in the poster
#   feature (no race — listener attaches before the async error event; posterRk
#   /^\d+$/-gated + encodeURIComponent'd, no XSS; nesting breaks no post-render
#   query). This was its one cosmetic finding.
# v1.24.85: FIX the v1.24.84 regression — the poster blew up to full card width.
#   A flex item's min-width defaults to `auto` = the <img>'s intrinsic width
#   (~1000px for a full-res Plex poster), which OVERRIDES flex-basis. The v1.24.84
#   "redundant width" code-review cleanup was WRONG: width was capping that
#   auto-min. Restored width:120px + added min-width:0 (kills the intrinsic
#   floor) so the poster is pinned to 120px regardless of image size. Comment +
#   test now mark both as load-bearing so a future cleanup can't drop them again.
# v1.24.86: INFO card — moved the metadata grid INTO the poster hero column.
#   the user: the hero had wasted space beside the poster + below the title (the
#   .dlg-grid rendered full-width BELOW the hero). Nested .dlg-grid inside
#   .info-hero-meta so the fields fill the column beside the poster — the
#   standard poster+info-column media-detail layout (Plex/Radarr-style). The
#   theme thumbnail + recovery/history sections stay full-width below the hero.
#   (motif has no TMDB overview/genres to fill it MTDP-style — raw_json is empty.)
# v1.24.87: move the poster to the RIGHT of the metadata grid. v1.24.86 put it
#   left of the grid, but the poster is short (120px / 2:3) and the grid is tall,
#   so a tall empty strip stranded under the poster (the user: "lots of white space
#   in the left column — move the poster to the right where the original design
#   already had white space"). The dlg-grid is `140px 1fr` with short values, so
#   the right of every row was already empty; making the poster the LAST hero
#   child fills that existing dead space instead of creating a new gap.
# v1.24.88: SAVED FILTERS popup — fix the lingering :hover highlight. The popup
#   is an overflow:hidden + radius + box-shadow absolute overlay dropping over
#   the results table, and its row-hover fill is semi-transparent (rgba cyan
#   0.08). Without its own compositing layer Chrome left stale alpha tiles — the
#   highlight "lasted long after hovering away, sometimes appearing after already
#   hovering away" (the user). Promoted the popup with transform:translateZ(0) so
#   its paint is self-contained and mouseleave clears the fill cleanly.
# v1.24.89: INFO card cover layout, option A (the user picked from rendered
#   mockups). v1.24.86/.87 put the metadata grid beside the poster (left, then
#   right); both stranded a strip next to the fixed-size cover because the grid
#   height swings ~5→~20 rows. Option A = the "first version" geometry: cover
#   top-LEFT, title + scope chip + a one-line playback-source HEADLINE beside it
#   (fills the strip so it isn't bare), full detail grid full-width BELOW the
#   hero as a sibling. The playback line moved out of the grid into the headline.
# v1.24.90: INFO card cover layout, option B (the user wants to A/B compare vs A).
#   A bigger cover (180px, was 120) on the LEFT with the metadata as a column
#   beside it — title + scope chip + playback headline + the full .dlg-grid all
#   nested in .info-hero-meta (grid is no longer the full-width sibling A used).
#   The Plex / Letterboxd "media detail" look; the bigger cover reads as art so
#   leftover whitespace feels intentional. Option A is preserved at tag v1.24.89
#   (image healzangels/motif:v1.24.89) for a one-line image-tag swap-back.
# v1.24.91: INFO card — Esc-close no longer leaves a focus ring on the opener.
#   A native <dialog> restores focus to whatever opened it (a dashboard carousel
#   card is a <button>); Esc is a keyboard interaction so :focus-visible matches
#   on the restored element and it kept a clicked-looking outline. The X-button
#   path is a mouse interaction (no ring). The info-dlg `close` listener now
#   blurs the restored opener (rAF, skipped if focus moved into another dialog)
#   so Esc matches the X. Layout-independent of the v1.24.89/90 A/B comparison.
# v1.24.92: INFO card cover layout back to option A — the user compared A (v1.24.89)
#   vs B (v1.24.90) live and kept A. Reverted the cover/grid to A's geometry:
#   cover top-LEFT (120px), title + scope chip + playback headline beside it,
#   the full .dlg-grid full-width BELOW the hero as a sibling. Option B (bigger
#   180px cover + grid nested in the meta column) is preserved at tag v1.24.90.
#   The v1.24.91 Esc-close focus fix is layout-independent and carries forward.
# v1.24.93: finish the HAMA → TVDB rename (the user audit). v1.16.2 had renamed
#   only the USER-FACING label to "TVDB BRIDGE" (the "HAMA bridge" name was a
#   misnomer — 99.7% of stranded rows are TheTVDB-scraper TV, not the HAMA anime
#   agent; PROJECT_HISTORY §L), but kept the internal ids "for stability".
#   v1.24.93 renames the internals everywhere they're active: op kind
#   hama_bridge → tvdb_bridge (schema v68 migration rebuilds op_progress's CHECK
#   + re-keys stored kind/op_id + the runtime_settings timestamp), op_id
#   hama-bridge → tvdb-bridge, routes /api/admin/hama-bridge/rebuild →
#   /tvdb-bridge/rebuild and /diagnostics/hama-gap → /diagnostics/tvdb-gap,
#   functions, the ops.js/app.js maps, comments, CLAUDE.md, and 6 test files.
#   Remaining "hama" is intentional archaeology: historical db.py migrations +
#   the v68 rename source, this changelog, and tests asserting HAMA's ABSENCE.
# v1.24.94: dashboard sync line no longer shifts the page on async load. The
#   "Next sync … · Last run …" line (#dash-sync-line) populates from /api/stats;
#   it was display:none until then, so revealing it pushed the whole dashboard
#   down (the user: "it shifts all the dashboards down which looks weird"). Now
#   visibility:hidden + min-height:1lh reserves the row from first paint; JS
#   flips visibility:visible once filled — no layout shift.
# v1.24.95: record/music design animations (the user — aesthetic pass). The motif
#   icon is already a vinyl record, so: a spinning record-spinner loader replaces
#   the bare "loading…" in the INFO card (reusable recordLoaderHtml helper); the
#   login screen gets a slow concentric groove-ripple background behind the card;
#   and the topbar ▰▰▰ brand-mark becomes a 3-bar VU-meter equalizer. All
#   subtle/slow + infinite, so the existing prefers-reduced-motion rule disables
#   them; bases chosen for sane reduced-motion rest states. Bonus flourishes
#   (needle-drop on PLACE, spindle play-button, login spin-up) deferred to a
#   fast-follow.
# 0.50.0: fresh release baseline. The git history was consolidated to a single
#   commit and the operator's name/email scrubbed from the public repo (prior
#   internal version was 1.24.95). Versioning restarts at 0.50.0; the entries
#   above are kept as in-code archaeology (also in docs/PROJECT_HISTORY.md).
# 0.50.1: login screen — centre the auth-card + spinning-record backdrop (the
#   user). The card was top-pinned (margin:60px) against a record graphic that
#   radiates from screen-centre — top-heavy + unbalanced. base.html now stamps
#   body.auth-page when unauthenticated; that scope makes .content a full-height
#   flex centre (card dead-centre, footer pinned bottom) — standard pages
#   untouched. The login backdrop gains the motif record icon itself: a large,
#   faint, slowly-spinning vinyl (reuses the v1.24.95 .record-spinner .rec-*
#   icon; .login-record only rescales/dims/slows it to a 16s turntable idle)
#   behind the kept v1.24.95 groove-ripples — "since we don't see [the record]
#   very much". prefers-reduced-motion rests it static; CSS + template only.
# 0.50.2: topbar brand-mark → 5-band synth spectrum analyzer (the user — "make
#   the bars look more like a synth and move in a more synth like manner"). The
#   v1.24.95 mark was 3 bars all tracing ONE sine wave (brand-eq), phase-shifted
#   — too uniform/VU-meter. Now five bands, each its own irregular multi-peak
#   keyframe (brand-eq-1..5) at its own duration (0.7–1.5s), so they never sync
#   up — the lively independent bounce of a graphic-EQ. Amber kept (brand
#   accent); bases stay full-height so the reduced-motion clamp rests them as
#   full bars. CSS + base.html template only.
# 0.50.3: tactile micro-interactions (the user — design-flavor pass 1/4). Modal
#   <dialog>s now EASE IN (fade + slight rise/scale via @starting-style; degrades
#   to instant on older engines) instead of popping; filter/toggle .chip:hover
#   gains a 1px lift + faint glow. The .btn:active press already existed
#   (translateY(1px), kept). All brief user-initiated TRANSITIONS, so the
#   v1.15.134 reduced-motion policy (keeps transitions, kills only infinite
#   animations) leaves them intact by design. CSS-only.
# 0.50.4: reactive brand-mark EQ (the user — flavor pass 2/4). The v0.50.2 synth
#   bars ran at one lively rate always, which "feels way too fast" at rest. Now
#   the bands drift SLOWLY when idle (2.4-4.4s) and snap to the lively rate
#   (0.7-1.5s) + brighter amber while motif is working — an ambient "now playing"
#   activity indicator. CSS adds .brand-mark.is-active overrides (tempo + color
#   only; brand-eq-1..5 keyframes + infinite iteration inherit, so reduced-motion
#   still rests full bars); app.js refreshTopbarStatus toggles .is-active off the
#   existing anyMutatingOpActive signal (no-op on the chrome-less login page).
# 0.50.5: dashboard SOURCE BREAKDOWN donut draw-in (the user — flavor pass 3/4,
#   donut half). On a pie's FIRST render the slices sweep + scale + fade into
#   place; app.js _renderSourcePie adds .pie-drawin to the persistent slices <g>
#   exactly once (gated on _pieState[lastKeyKey] still being its initial ''), so
#   poll re-renders + legend toggles don't restart it. No fill-mode → the
#   reduced-motion clamp rests it at the natural state. The count-up half was
#   split out: the stat numbers are SSR-baked, so an on-load count-up reads as a
#   brief value-then-reset flash unless handled specially — deferred pending the
#   user's call on the tradeoff. CSS + app.js.
# 0.50.6: dashboard count-up (the user — flavor pass 3/4, count-up half;
#   GLITCH-FREE variant the user chose). The 4 coverage % numbers are SSR-baked,
#   so a naive count-up flashes the real value then resets to 0. Fix: a parser-
#   blocking inline script in dashboard.html stashes each % in data-countup +
#   resets it to 0% BEFORE first paint; app.js dashCountUp() then climbs 0→value
#   (easeOutCubic, 1.1s) and clears data-countup. setCov defers its pct write
#   while data-countup is pending so the 1s coverage poll can't clobber the
#   climb. reduced-motion users skip the inline reset (keep the static SSR value
#   — nothing animates). data-countup presence is the once-gate. app.js + template.
# 0.50.7: CRT power-on flicker (the user — flavor pass 4/4, CRT half). A quick,
#   gentle "screen turns on" sweep (~0.55s, softened from the preview — no
#   strobe) played ONCE per tab session. base.html renders a full-viewport
#   .crt-power-on overlay (pointer-events:none, rests opacity:0) + a tiny inline
#   script that adds .playing only when sessionStorage hasn't seen it — so it
#   fires on the first load, NOT on every full-page nav. The prefers-reduced-
#   motion clamp shortens it to near-instant (no flash). CSS + base.html.
# 0.50.8: needle-drop on PLACE (the user — flavor pass 4/4, vinyl half; FINAL of
#   the flavor sequence). A tonearm drops onto a spinning record over the clicked
#   PUSH TO PLEX button when a place succeeds. app.js needleDropAt(btn) spawns a
#   floating, self-cleaning overlay anchored to the button's viewport rect (NOT
#   the row DOM, which re-renders on the next poll) and removes it after the
#   animation via one timeout. Fired from replaceTheme's success branch. Skipped
#   entirely under reduced-motion (the overlay is never created). CSS + app.js.
# 0.50.9: ORPHAN SCAN drift labels show without underscores (the user —
#   MOTIF_ENTRY_MISSING → "MOTIF ENTRY MISSING", PLEX_FETCH_FAILED → "PLEX FETCH
#   FAILED"). New driftLabel() display helper in orphans.html applied at the
#   chip, DRIFT-cell, filter-status, and empty-state render sites. Display-only —
#   the raw drift_type still drives filtering, the DRIFT_TONE map, and the
#   per-type action set. (The underlying re-push false-flag + LotR dead-rk are
#   separate fixes, planned next.) Template-only.
# 0.50.10: Fix A — ORPHAN SCAN no longer false-flags over-ceiling re-encoded
#   uploads (the user's '10' 24MB + '10 Cloverfield Lane' 10.7MB themes). They're
#   re-encoded to ~8.5MB to fit Plex's ~10MB upload ceiling, so the uploaded blob's
#   hash differs from the canonical's BY DESIGN (v1.24.47) and the canonical hash
#   is never a Plex entry → perpetual motif_entry_missing that RE-PUSH can't clear.
#   Scoped much smaller than the original plan once the data showed
#   verify_placement_health is rk-based + EXCLUDES plex_upload (so the LIBRARY was
#   already correct) — the false-flag lived ONLY in the read-only scanner. Fix:
#   scan_one_placement classifies a row OK when motif's canonical is over the
#   ceiling AND Plex has a SELECTED upload:// entry (Plex serves the re-encode).
#   No schema / worker / verify change; no AWAIT/RE-PUSH SQL touch (the co-display
#   source is the dead-rk LotR, handled by Fix B next). orphan_scan.py only.
# 0.50.11: Fix B — ORPHAN SCAN // CLEAN UP for dead-rk plex_fetch_failed rows
#   (the user's LotR Collection, rk 579643: Plex removed + re-added the collection
#   under a new rk, so /themes 404s and PROBE can only re-404). New admin endpoint
#   POST /api/admin/orphan-scan/cleanup-dead-rk re-verifies the rk is GENUINELY
#   gone (no live plex_items row AND /themes still fails — refuses 409 if it came
#   back, off the event loop) then drops ONLY motif's stale placement row: no Plex
#   write, no canonical deletion, so re-placing fresh later works. orphans.html
#   adds the // CLEAN UP button + handler on plex_fetch_failed rows. the user
#   chose "just clean it up". api.py + orphans.html.
# 0.50.12: library hero <h1> title now updates on client-side tab switch (the
#   user: the title read ANIME while on the TV tab, only correcting on a hard
#   refresh). switchLibraryTab swapped document.title + the subtitle + the
#   REFRESH button label from the fetched page but never the visible
#   `.hero h1.title`, so it kept the originally-loaded tab's name. One-line swap
#   mirroring the adjacent subtitle swap. app.js only.
# 0.50.13: ORPHAN SCAN // CLEAN UP no longer refuses a dead-rk row that still has
#   a stale plex_items sibling (the user's LotR Collection rk 579643 → HTTP 409
#   "still a live Plex item"). The v0.50.11 guard had TWO refusals — (a) any live
#   plex_items row, (b) /themes still serving. But rk 579643's plex_items row is
#   STALE (enumerated once, never reaped) while its /themes genuinely 404s, so (a)
#   blocked the exact row it should clean. Dropped (a): the live /themes re-check
#   (b) is the authoritative + sufficient gate (refuses only if Plex now serves
#   the theme). cleanup-dead-rk endpoint only.
# 0.50.14: the COLLECTIONS // REFRESH button no longer re-enables mid-scan (the
#   user: it became clickable while RECONCILING PLACEMENT PATHS + queued jobs were
#   still running). plex_enum_active only has movies/tv/anime keys — a collections
#   refresh enumerates the underlying movie/tv sections (mapped to THOSE tabs), so
#   the collections tab's myTabBusy was ALWAYS false and the lock fell back to
#   globalEnumPipeline, which drops once the per-section enums finish but the
#   GLOBAL reconcile_placement_paths post-phase (+ queued) still runs. Fix: on the
#   collections tab, lock on plexEnumBusy (any plex_enum in flight) — true through
#   reconcile + queue — at both myTabBusy sites (refreshTopbarStatus +
#   updateLibraryRefreshBtnLabel, the latter via a stashed flag). app.js only.
# 0.50.15: needle-drop flourish now fires on a placement TRANSITION, not only
#   PUSH TO PLEX. the user: "not seeing the animation when placing a new row" —
#   the v0.50.8 fire was wired solely to replaceTheme (PUSH/re-push of an
#   already-placed row, so no first-time transition). New flashPlacedTransitions()
#   runs after each library render: any row that went not-placed → placed (the
#   v1.18.0 PL predicate: media_folder set OR plex_upload) drops a needle on the
#   row. Fires when the theme actually LANDS (the poll sees the placement), not
#   on enqueue — correct for DOWNLOAD, whose job can fail after the click. Covers
#   DOWNLOAD / UPLOAD MP3 / ADOPT / RESTORE / worker-initiated places uniformly.
#   Seeds silently on first sight (no flash-storm on load/tab-switch); viewport
#   check + per-render cap (5) keep a bulk place from flooding the screen. Adds
#   data-rk to each library <tr> as the anchor; needleDropAt gains a 'row' align
#   mode. PUSH keeps its own immediate fire. app.js only.
# 0.50.16: removed the needle-drop entirely (the user, after seeing it live:
#   "I don't know about the animation now seeing it live" → chose remove). Pulls
#   BOTH fires — the v0.50.8 PUSH-button drop and the v0.50.15 placement-
#   transition drop — plus needleDropAt + flashPlacedTransitions, the .needle-drop
#   CSS + nd-* keyframes, the <tr> data-rk anchor, and the two needle test files.
#   The other v0.50.x flavor animations (synth EQ, donut draw-in, count-up, CRT
#   power-on, tactile) all stay. app.js + app.css.
# 0.50.17: surface Plex's METADATA edition (editionTitle) in the library ED column
#   (the user: Alien (1979) shows "Directors Cut" in Plex but motif's ED was blank).
#   motif's ED/edition_key derive only from the {edition-X} FOLDER tag; Alien's
#   edition is a Plex metadata field on an untagged folder, so nothing showed.
#   Now schema v69 adds plex_items.plex_edition_title; plex_enum captures
#   editionTitle from enumerate_section_items (INSERT + both UPDATE paths);
#   /api/library selects it; the ED column falls back to it (dashed "meta" pill +
#   tooltip) when the folder has no {edition-X} tag. DISPLAY-ONLY — deliberately
#   NOT folded into edition_key, so the v1.21.x folder-based per-edition theme
#   scoping (placements/local_files PK) is untouched and a metadata-only edition
#   never drives placement. db.py + plex.py + plex_enum.py + api.py + app.js/css.
# 0.50.18: round the login AUTHENTICATE window into a vinyl record LABEL (the
#   user: "make it round or look like the center dot of the record"). .auth-card
#   becomes a circle (border-radius:50%, aspect-ratio:1, width min(460px,92vw))
#   centred over the spinning-record backdrop; the old rectangular border-left:3px
#   green accent → a circular ::after ring; ::before sheen recentred; new
#   .auth-card-inner clamps the form to the circle's safe centre zone so the round
#   edge never clips it. login.html + app.css only; the v0.50.1 centring + record
#   backdrop are untouched.
# 0.50.19: library-chrome polish (3 the-user asks). (1) LEGEND caret moved to the
#   END of the label (was at the front) — it's ► collapsed / ▾ open (the existing
#   .open rotate), matching the status-bar op-card-caret. (2) GLOSSARY help-bar
#   button gets the same trailing caret; app.js toggles .open on it on the
#   <dialog> open/close so it rotates ►→▾ in step. (3) the open LEGEND panel now
#   OVERLAYS the table instead of pushing it down — .library-block is a
#   positioning context + .library-legend-panel.open is position:absolute (auto
#   top keeps its static spot, removed from flow → no reflow) with bg/border/
#   shadow like .row-menu-panel. (4) #library-subtitle gets margin-top so the
#   /collections section selectors (which wrap to a 2nd line) no longer almost-
#   touch the subtitle. library.html + base.html + app.js + app.css.
# 0.50.20: login backdrop centring + scale (the user: "the center isn't center on
#   the spinning background … make the spinning logo and pulses larger"). .login-bg
#   is now position:absolute inside a position:relative body.auth-page .content, so
#   the record centres on the SAME box the card centres in (content area = viewport
#   minus footer) instead of the full viewport — fixes the record sitting low.
#   .login-record 78vmin → 118vmin (the ~460px card now reads as a record's ~1/3
#   centre label) + .login-ripple 42vmin → 64vmin to match. app.css only.
# 0.50.21: legend/caret corrections to v0.50.19 (the user). (1) Reverted the
#   absolute LEGEND-panel overlay back to the in-flow dropdown — the overlay hid
#   the top rows; the old push-down behaviour was preferred (dropped .library-block
#   + the absolute rule; restored <section class="block">). (2) Unified the caret
#   gap: it's now one margin-left on the shared .library-legend-caret class (no
#   literal HTML space on LEGEND, no glossary-only +2px) so LEGEND + GLOSSARY sit
#   the same distance from their labels. Both still ► closed / ▾ open. library.html
#   + base.html + app.css.
# 0.50.22: two the-user UI fixes. (1) login record was clipped after v0.50.20's
#   118vmin enlarge (overflowed the content height) → 88vmin so the WHOLE record
#   is in frame in any orientation, still larger than the old 78vmin; ripples
#   64→52vmin to match. (2) SAVED FILTERS dropdown rows: hover is now JUST the
#   cyan text colour (dropped the rgba bg tint that the user saw "flashing" as the
#   cursor crossed rows). app.css only.
# 0.50.23: sticky footer (the user: the healthz bar "jumps way up in page" on
#   short sections like ORPHAN SCAN — want it locked to the screen bottom like on
#   movies). body is now a flex column (it already had min-height:100vh) and
#   .content is flex:1 0 auto, so content grows to fill and .footer pins to the
#   viewport bottom on short pages while still sitting below the content on long
#   ones. The login page already proved the pattern (body.auth-page); the sticky
#   topbar still works inside the flex column. app.css only.
# 0.50.24: dashboard + glossary polish (the user). (1) DASH → DASHBOARD (nav
#   link + page <h1>). (2) glossary/legend chips: every SRC/LINK/DL/PL/FLAGS
#   indicator now CENTRES in its 26px rail (justify-self:center on the col-1 item;
#   defs keep justify-items:start so they still line up) + the grid vertically
#   centres them (align-items baseline→center); reverses v1.23.86's left-aligned
#   FLAGS glyphs. (3) // SOURCE BREAKDOWN header gets the standard framed .block-head
#   bar back (the bare title threw off inter-section spacing) while the section
#   stays borderless so the 3 donuts remain their own cards (the user chose "header
#   bar only"). base.html + dashboard.html + app.css.
# 0.50.25: cross-tab content-top alignment (the user: the search bar / RECENTLY
#   ADDED should start at the same Y on every tab). .hero gets min-height:150px so
#   every page's header reserves the same height (title + up to 2 subtitle lines)
#   and the first content block lines up tabbing between dash/movies/collections/
#   logs/settings. min-height (not height) so a hero that grows (backfill banner)
#   still expands. The exact px is eyeballed — may want a small nudge. app.css.
# 0.50.26: login record a touch larger again (the user) — 88 → 94vmin
#   (margin -47vmin). Still keeps the whole circle in frame with a thin margin.
# 0.50.27: the SAVED FILTERS popover blinked while open (the user). The 600ms
#   drift-detection setInterval(_updatePresetActiveState) re-toggled .is-active on
#   the list rows every tick; now it early-returns while the <details> menu is
#   open (drift detection only needs the CLOSED bookmark icon). app.js only.
# 0.50.28: flowing soundwave across the hero's lower band (the user: "cool to add
#   a wave style music animation across the width" in the v0.50.25 empty space). A
#   .hero::after sine wave — SVG sine path as a CSS mask, var(--green) as the masked
#   colour (token, no hardcoded hex), scrolling via mask-position. z-index:-1 keeps
#   it behind the title/subtitle; reduced-motion freezes it static. app.css only.
# 0.50.29: make the CRT power-on actually visible + add a power-OFF (the user
#   never saw it — the v0.50.7 once-per-session sessionStorage gate fired once then
#   never again). Now: power-ON fires right after a login (login.html stamps
#   motif:just-logged-in on submit; base.html plays it on the authenticated landing
#   page) and on a manual refresh (Navigation Timing type==reload). New power-OFF
#   tube-collapse flicker plays on the logout ⏻ click, then navigates (skipped +
#   instant under reduced-motion). base.html + login.html + app.css.
# 0.50.30: // FILTERS dropdown caret now rests ► (closed) and rotates 90°→▾ (open),
#   matching the LEGEND/GLOSSARY carets (the user: closed-state caret "is still
#   downward instead of >"). Was a ▾ base rotating 180°→▴ — wrong direction +
#   wrong glyph. Swapped the glyph to ► and the open rotation 180°→90°, added a
#   transition so it eases like the legend caret. library.html + app.css.
# 0.50.31: SAVED FILTERS rows go blue on hover/press only — dropped the persistent
#   ".is-active" cyan that painted the currently-applied preset the instant the
#   popover opened (the user: "only be blue when selected or on hover but they are
#   when you first open"). The match was honest — the applied filter is restored
#   from the cross-tab snapshot on load, so its row was correctly cyan — but the
#   auto-paint read as a confusing pre-selection; the bookmark star already signals
#   an active saved filter. Kept :hover, added :active for the click-moment
#   "selected" flash. JS still toggles .is-active (drives _activePresetId + the
#   star), it just no longer paints the row. app.css only.
# 0.50.32: hero descriptions line up on Y across every tab + the dashboard (the
#   user: "descriptions on the tabs are not lined up on Y"). The subtitle's Y
#   diverged two ways: only #library-subtitle had an explicit margin-top, and the
#   title's margin-bottom ADDED inside the flex .hero-row (dashboard/library) but
#   COLLAPSED in the plain-block heroes (logs/settings/orphans). Fix: .title
#   margin-bottom→0 + a single shared .hero-sub class (margin-top gap-5, == the old
#   library look gap-2+gap-3) on every page's description, so the title→subtitle
#   gap is one value everywhere. app.css + the 5 hero templates.
# 0.50.33: the APPLIED saved-filter row stays highlighted as "selected" (the user:
#   "when a filter is selected it should remain highlighted to indicate it's
#   selected"). v0.50.31 had stripped the persistent row paint because its
#   cyan-text-only form was indistinguishable from :hover and read as a stray hover
#   on open; restored here but made DISTINCT — static bg tint + left accent bar vs
#   hover's text-only cyan. JS already toggled .is-active off the live filter match
#   (only the genuinely-applied preset), so just the CSS came back. app.css.
# 0.50.34: remove the topbar AWAIT badge (the user: it "appears briefly while they
#   place" + is "redundant to re-push"). It flickered in during the download→place
#   handoff — a row is transiently downloaded-but-not-placed mid-place — and
#   duplicated the adjacent RE-PUSH badge. Removed the badge + glossary chip
#   (base.html), its JS render/cache/cycle (app.js), its tone/chip CSS (ops.css +
#   app.css), and the badge-only count machinery (_AWAIT_COUNT_SQL / _AWAIT_COUNT_
#   FROM / _AWAIT_TAB_BREAKDOWN_SQL / SSR awaiting_count / stats.awaiting). KEPT:
#   the attn_pills=await FILTER + the PL=await row state (both off _LIB_AWAIT_SQL),
#   so staged-but-not-placed themes are still findable + filterable.
# 0.50.35: INFO card — (1) a dead ThemerrDB URL (failure_kind ∈ video_removed/
#   private/age_restricted/geo_blocked) renders RED + a "dead" tag so it reads as
#   dead at a glance (the user); reverts to green when a reprobe clears the failure;
#   a pending NEW TDB URL is never "dead". (2) a compact "✓ theme.mp3 on disk"
#   backup line on every row that has a local file but isn't already a backup-only
#   row (which relabels applied→backup) — so a recoverable backup is visible on HL/
#   placed/dead-url rows too, with the YouTube source URL. app.js only.
# 0.50.36: Live Ops RUN INSIGHT shows sane peak/s + avg/s (the user: "400000 peak/s
#   · 207301 avg/s" on a 10,514-item/77s run — really ~136/s). progress.py samples
#   throughput as delta/dt with a 0.001s dt floor, so a fast upsert burst divides by
#   ~0 + the rate explodes; the old STATS took max(rate)+mean(rate), both inflated.
#   New _throughputStats(op): avg/s = processed / wall-clock elapsed; peak/s = the
#   busiest whole-second window (items/sample = rate×dt, bucketed by second). Both
#   the STATS readout + the THROUGHPUT chart header use it. ops.js only.
# 0.50.37: notification Source line carries a per-platform colour dot (the user:
#   "make the color on all youtube added videos red as blue is supposed to be
#   facebook"). Apprise's Discord embed bar is severity-coloured (INFO=blue for
#   every theme-add) + not a per-source knob, and the external apprise-api path
#   can't set a colour at all — so the colour lives in the body where it renders on
#   every transport: red YouTube, blue Facebook, orange SoundCloud, purple Instagram.
#   notify_content.py _format_provenance_line.
# 0.50.38: the dashboard ANIME stat-card glyph now matches the TV glyph (the user:
#   "make the Anime icon the same as the TV icon in both sections" — the // ANIME
#   THEMED coverage card + the // PLEX ANIME card). Folded anime into the tv branch
#   of the media_glyph() macro; the old 4-point sparkle is retired. dashboard.html.
# 0.50.39: post-code-review hardening of the v0.50.34-38 batch. (1) THROUGHPUT root
#   cause: progress.py floored the rate sample's dt at 0.001s, so a sub-second upsert
#   burst produced rate≈400000/s that leaked into RUN INSIGHT peak/avg, the live
#   items/sec pill, the ETA + the sparkline; floor at 1.0s instead so `rate` is a sane
#   items/sec everywhere, and simplify ops.js _throughputStats peak back to max(rate)
#   (the buggy renderer-side bucket reconstruction — wrong on the first sample +
#   sparse gaps — is gone). (2) INFO card: only link a backup YouTube URL for a real
#   11-char video id (not the 'recovered' sentinel / sha-hash / fb-ig-sc ids → dead
#   links); gate the red "dead" tag on a non-empty themerrdb url; reword the backup
#   tooltip so it's accurate on already-placed rows (no PROMOTE-TO-ACTIVE no-op).
#   progress.py + ops.js + app.js.
# 0.50.40: Live Ops drawer-display accuracy (audit, frontend half). (1) A cancelled/
#   failed real-bar op no longer snaps its progress bar to 100% — it freezes at its
#   last % (download stopped at 40%, enum failed at 12/200) so a stopped op doesn't
#   read as complete; only status==='done' → 100%. (2) The RUN INSIGHT done_summary
#   STATS block is skipped on a CANCELLED op (cloud_themes_backup stamps it pre-cancel,
#   so it read as complete while the headline said "Cancelled"). (3) peak/s + avg/s
#   are comma-grouped (fmtNum) to match done_summary (was "10,514 items · 207301
#   avg/s"). (4) the live RATE pill is magnitude-aware (fmtRate: 1 decimal under 10/s,
#   grouped integer above). ops.js only. (Backend data-accuracy half → next tag.)
# 0.50.41: Live Ops drawer accuracy (audit, backend half). (1) reset_stale_on_boot +
#   sweep_stuck flipped a stale op to 'failed' with a raw bulk UPDATE that never
#   closed the in-flight stage timing — so a crashed op's final (often longest) stage
#   was dropped from the RUN INSIGHT waterfall + the internal _stage_* keys leaked
#   into the finished detail. Both now go per-row through a shared _finalize_stale_
#   detail (mirrors finish_progress). (2) bulk_lps reset processed_total to u_done at
#   the unplace stage, so the op-wide 'items processed' counter (+ avg/s) jumped
#   backward n_to_probe→0 at the probe→unplace boundary; now carries it forward.
#   progress.py + api.py. (Deferred, noted: cloud_themes_backup walk→download counter,
#   sync remote-fetch boundary, plex_enum waterfall finer segmentation.)
# 0.50.42: Live Ops drawer clarity (audit, soft-clarity trio). (1) ETA over an hour
#   buckets to ">1h" (new fmtEta) — a projection that far out is rate-noise, so a
#   precise "7h 23m" was false precision; measured ELAPSED/RAN keep fmtDuration.
#   (2) the live RATE pill (recent ~10s smoothed, drives ETA) and STATS avg/s
#   (whole-run) legitimately differ; tooltips on each + on peak/s say so, so they
#   no longer read as a contradiction. (3) THROUGHPUT chart-header peak comma-grouped
#   (fmtNum) to match STATS peak/s — was max.toFixed(0) ("12703") beside "12,703".
#   ops.js only. (Deferred unchanged: cloud_themes_backup + sync counters,
#   plex_enum waterfall segmentation → next.)
# 0.50.43: plex_enum RUN INSIGHT waterfall — break out a 'health' stage (audit D4).
#   The post-loop tail ran reconcile_placement_paths + both stat-every-theme.mp3
#   health passes (placement + canonical) under one 'reconcile' bar; on a slow
#   Unraid/NFS mount the health stats can dominate the tail yet read as folder-
#   rename time. Now transitions enumerate → reconcile → health (post-loop,
#   forward-only — no per-section strip bouncing). plex_enum.py stage transition +
#   ops.js STAGE_TIMELINE health step. (Deferred unchanged: cloud_themes_backup +
#   sync remote-fetch counters — genuine-accuracy-with-risk, parked.)
# 0.50.44: dashboard column gutters align across every row. The PER-SECTION
#   COVERAGE | GENERAL STATISTICS .dash-pair used --gap-6 (24px) while every
#   grid-stats row (4-card PLEX LIBRARY above, 2-card ADDED below) uses the .grid
#   default --gap-4 (16px), so the pair's 2-column split sat 4px off the column
#   boundaries it stacks between — the dashboard "centering looks off" (the user).
#   .dash-pair gutter + .dash-pair-col flex basis now use --gap-4 so every row's
#   center boundary lands at the same x. app.css only.
# 0.50.45: drawer processed_total monotonic across stage boundaries (audit D2+D3,
#   same class as v0.50.41 bulk_lps). (D2) cloud_themes_backup: the walk stage
#   climbed processed_total to the candidate count (~3,883) then the download stage
#   reset it to 0; now the walk count is carried forward (walked_count[0] + i).
#   (D3) sync remote/snapshot per-item fetch: the per-media-type base was
#   stats.X_seen (FLUSHED, excl fetch errors) while the stamp added `completed`
#   (fetched, incl errors), so each movie→tv→collection boundary jumped backward by
#   the prior type's error count; now the base is a running `completed` total
#   (_remote_done). api.py + sync.py. Closes the drawer-numbers audit.
# 0.50.46: dashboard COVERAGE bar-fill no longer lags the % count-up. The bar was
#   only written by setCov when the async coverage poll landed, so it filled
#   visibly late after the % had already climbed (the user). Now mirrors the
#   v0.50.6 % pattern: SSR-bake the bar width inline (reduced-motion = already
#   displayed), pre-paint reset to 0 for motion users, dashCountUp drives the bar
#   in lockstep with the % (transition off during the rAF, restored after), and
#   setCov defers the bar write while the climb is in flight. dashboard.html +
#   app.js (+ ssr_pct_width macro).
# 0.50.47: code-review fixes for the v0.50.41 stale-sweep rewrite. (A1) the reaped
#   op's in-flight stage was closed at the REAP time, so a sweep_stuck reap (90 min
#   after a stall) recorded the final stage as ~90 min of idle and dwarfed the RUN
#   INSIGHT waterfall; now closed at the row's updated_at (last progress). (B1) the
#   per-row UPDATE matched op_id only — sweep_stuck runs while workers are LIVE, so a
#   row finishing between the SELECT and its UPDATE got stomped back to 'failed'; the
#   UPDATE now re-guards the non-terminal status. (B2) the per-row loop is wrapped in
#   one transaction so the sweep stays all-or-nothing like the old bulk UPDATE.
#   progress.py + ops.js (stale renderTimeline comment 'reconcile'→'health').
# 0.50.48: mobile foundation pass (#1 of an iterative mobile arc). The UI was
#   desktop-first with no phone tier, so a ~390px viewport overflowed. (a) the
#   fixed-width results table (~1000px of columns) is now in a .table-scroll
#   horizontal-scroll wrapper so the ACTIONS cell is swipeable instead of clipped
#   by body's overflow-x:hidden; (b) the 64px hero .title now clamps (clamp(34px,
#   12vw, 64px) — desktop unchanged ≥533px); (c) the existing @media(max-width:600px)
#   block gained phone overrides: topbar padding/gap shrink + nav horizontal-scroll,
#   refresh button + search go full-width, dash-pair-col drops its 340px floor.
#   library.html + app.css. Per-surface refinement (table columns, drawers, charts,
#   settings) iterates from screenshots.
# 0.50.49: topbar mobile fixes (the user, ~927px). (1) the IDLE / // HELP / logout
#   status cluster spilled off the clipped right edge — the topbar grid (brand |
#   nav | status) was forced wider than the viewport by the nav's 7-tab min-content;
#   .nav now flex-wraps within its 1fr column + min-width:0 so it shrinks instead of
#   shoving the status off. (2) "TV SHOWS" wrapped to two lines, which top-aligned
#   the single-line tabs so they floated up; .nav a is now white-space:nowrap (each
#   tab one line, the nav wraps as a group). Phone (≤600px) keeps a single-row
#   horizontal-scroll nav (flex-wrap:nowrap) instead of a tall multi-row stack.
#   app.css only.
# 0.50.50: mobile library + dialog fixes (the user, 480px). (1) row TITLES were
#   blank on every row (and any window < ~1440px): #library-table is
#   table-layout:fixed and .col-title is the only no-width column, so once the fixed
#   columns summed past the available width col-title collapsed to 0. Added a table
#   min-width:1440px — it keeps full width + scrolls in .table-scroll, col-title
#   always has room; desktop unchanged (width:100% wins ≥1440px so col-title still
#   absorbs slack). (2) the // RESULTS pager spilled next/last off the edge → the
#   block-head + block-head-actions + pager flex-wrap on a phone. (3) the row INFO
#   card + other .dlg modals go full-screen (100vw/100dvh) on a phone. app.css only.
# 0.50.51: settings // CHANGE PASSWORD form gains a hidden autocomplete=username
#   field — clears Chrome's "password forms should have a (hidden) username field"
#   a11y console warning + lets a password manager associate the credential. The
#   /api/admin/password endpoint reads only current_password/new_password (typed
#   Form params) so the extra field is ignored. (The other 3 console "password
#   field not in a form" warnings are masked CONFIG secrets — plex token / tmdb key
#   / proxy URL — intentionally not credential forms; left as-is.) settings.html.
# 0.50.52: settings tab bar (13 tabs) scrolls as a single compact row on a phone
#   instead of wrapping into a ragged multi-row block that read as "one big list
#   stacked on itself" (the user) — same single-row horizontal-scroll treatment as
#   the topbar nav. app.css only.
# 0.50.53: REGRESSION FIX — the v0.50.50 `#library-table { min-width: 1440px }`
#   was a BASE rule, so it forced the results table to >=1440px on the DESKTOP too,
#   overflowing .table-scroll and clipping the ACTIONS column on any window/panel
#   narrower than 1440px (the user). Scoped the min-width to the @media(max-width:
#   600px) phone block — desktop is back to width:100% (fits its container, no
#   horizontal scroll, col-title absorbs slack); phone still gets the min-width so
#   titles stay visible while swiping. app.css only.
# 0.50.54: results-table column-priority for narrow viewports. Below full-desktop
#   width the fixed columns (ACTIONS 320 + ED 160 + IMDB 120 + …) summed past the
#   viewport and squeezed the no-width TITLE column (the squish/blank-title band).
#   ≤1280px hides the two lowest-value columns — ED (an empty pill for ~all rows)
#   and IMDB (raw tt id) — so TITLE keeps its room with NO scroll down to ~1080px;
#   ≤1080px pins #library-table min-width:1080 so TITLE holds ~150px while the table
#   swipes (replaces the v0.50.53 phone min-width:1440 — ~25% less phone scroll).
#   Desktop >1280px is unchanged. app.css only. Scoped to #library-table.
# 0.50.55: legend + glossary completeness audit. Every chip the row renders is now
#   decoded. // GLOSSARY (full reference) gains the rest of the ATTN axis — !M
#   mismatch, !P awaiting-placement, ↩ restorable-snapshot (distinct from ↺), ⊘
#   too-big — plus the "Plex also serves" composite SRC corner-dot
#   (link-badge-also-plex). The in-context LEGEND (kept lean) gains LINK C / M / AB
#   and the !M / !P attention chips. New gg-mismatch/await/restore/toobig mirror the
#   real row hues (pinned by test_v1_23_50 FLAG_PAIRS, the v1.23.56 anti-drift rule);
#   the // FLAGS headers + gg-* pattern are unchanged (no restyle). app.css + 2
#   templates + tests only.
# 0.50.56: v0.50.55 code-review follow-ups (2 self-found gaps). (1) Glossary now
#   decodes the faded link-badge-themerrdb-only "T" — a ThemerrDB-tracked title NOT
#   in your Plex library (TDB-only browse / not-in-Plex filter); the v0.50.55 "decode
#   every chip" pass had missed this de-emphasized SRC variant. (2) The legend
#   completeness guards were non-guarding for LINK C/M/AB — those class strings also
#   live in the filter-chip buttons above the table, so the unscoped `in file` check
#   passed regardless of the legend rows; both tests now scope to the legend body.
#   base.html + 2 tests only.
# 0.50.57: full CRT motion — removed the prefers-reduced-motion gating entirely (the
#   user's call). On Windows (which commonly reports reduce) the v1.15.134 universal
#   CSS clamp had frozen the brand-mark equalizer + every looping animation to a static
#   frame ("the wave on top not doing anything"). Removed all three gates: the @media
#   clamp in app.css, the CRT power-off flourish skip in base.html, and the dashboard
#   count-up skip in dashboard.html. The full vibe (equalizer / pulses / ripples / hero
#   wave / power on-off / donut draw-in / count-up) now plays for everyone regardless of
#   the OS setting. New guard test pins the removal; old test_v1_15_134 deleted.
# 0.50.58: mobile-arc (v0.50.48..54) silent-bug audit follow-ups (3-finder sweep).
#   (1) CONFIRMED desktop regression: .table-scroll { overflow-x: auto } as a BASE
#   rule made it a dual-axis clip box at every width (overflow-x:auto forces
#   overflow-y:auto), clipping the per-row // SOURCE/PLACE/REMOVE dropdowns
#   (position:absolute, inline in the cell) on the bottom rows on DESKTOP. Scoped
#   the overflow to <=1080 (where min-width:1080 actually forces the table past the
#   viewport); above that the wrapper is a plain div + menus overlay freely.
#   (2)+(3) mobile nav + settings-tabs scroll strips hid the active tab off-screen
#   right — both now scrollIntoView the active item (no-op on desktop). Cleared by
#   the audit: column display:none (no JS cell-indexing), selector over-match,
#   wrapper-div selectors, hidden username input, .title clamp (--t-huge==64px).
# 0.50.59: hover/confirm text uniformity audit. Two-agent sweep of all ~247 title=
#   tooltips + ~51 confirm() prompts against the de-facto convention. Both were
#   already largely uniform; normalized the outliers (the user picked the 2 taste
#   calls): add the missing terminal period to prose tooltips (match the 73%
#   majority); brand "motif" lowercase mid-sentence, "Motif" sentence-initial;
#   IMDB→IMDb; config-key themes_dir→/themes + drop redundant "immediately" in the
#   dry-run confirm. ~39 strings across app.js + 4 templates. Text-only.
# 0.50.60: Settings → VISUALS — per-browser CRT-effect toggles. New settings tab
#   with 5 live-apply switches (no SAVE; localStorage 'motif:visuals', same model as
#   the dashboard LIBRARY COLORS panel): CRT power-on flash, power-off flash, hero
#   wave bar, scanline overlay, topbar equalizer. base.html stamps html.viz-no-<fx>
#   pre-paint so a disabled effect never flashes; CSS hides each + collapses the hero
#   min-height when the wave is off (content shifts up, no white band); the power
#   on/off JS gates honour the opt-out. Tab registered across the 4 SSR sites
#   (base allowlist + settings + app.css panel/underline rules).
# 0.50.61: two v0.50.60 follow-ups. (1) HERO WAVE off no longer breaks cross-tab Y
#   alignment: min-height:0 let each hero size to its own content, so the dashboard's
#   2-line subtitle pushed // RECENTLY ADDED lower than the library search bar. Now a
#   tighter-but-consistent min-height:130px (>= the tallest hero) keeps every tab's
#   first block on the same Y while still shifting up ~20px from the 150px shown state.
#   (2) Settings tabs: the 14th tab (VISUALS) wrapped to its own row — tightened tab
#   padding (18→12px) so all 14 fit one line, and made .tabs a single nowrap row
#   (scroll if a narrow window can't fit them) so a tab never orphans below again.
# 0.50.62: scanline overlay was imperceptible (the user) — both dark color stops sat
#   at 3px (zero-width band), so the line was a 1px gradient sliver at 0.18a×0.4opacity,
#   multiply-blended into nothing on the dark UI. Now a SOLID 1px dark line per 3px at
#   0.5 alpha / 0.6 opacity so the CRT scanlines actually read on bright content
#   (text/posters) and the VISUALS toggle has a visible effect. CSS-only.
# 0.50.63: code-review follow-ups for the VISUALS arc. (1) CONFIRMED regression: the
#   v0.50.61 .tabs { overflow-x:auto } reintroduced the dual-axis clip trap (the
#   v0.50.58 .table-scroll lesson — overflow-x:auto forces overflow-y:auto), clipping
#   the settings tabs' keyboard :focus-visible ring (outline-offset:2px, .tabs has no
#   vertical padding). Fixed: the tab ring draws INSET (#settings-tabs .tab:focus-visible
#   outline-offset:-2px) so it stays inside the clip box. (2) Added a drift-guard test:
#   the VISUALS field->class map is hand-duplicated in base.html / app.js / settings.html,
#   so pin that the 3 maps AGREE (phantom-guard, v1.18.81), not just that each exists.
# 0.50.64: universal row INFO card. Pre-tag only THEMED rows (theme_tmdb present) got
#   an ⓘ → the full ThemerrDB record card. the user: "give every row an info button so
#   users can see more info about ANY row." Now every row renders an ⓘ. Themed rows keep
#   the full api_item card; rows with NO theme have no record to fetch, so they open a
#   BARE card built entirely CLIENT-SIDE from the cached /api/library row (title/year/
#   section/edition/imdb+tmdb ids/folder/rating_key — all already on the row payload, no
#   backend round-trip, no new endpoint, no async surface). The click handler routes on
#   presence of data-id: present → openInfoDialog (full), absent → openBareInfoDialog
#   (looks the row up by rating_key in libraryState.items, renders renderBareInfoCard
#   synchronously). Hover-prefetch skips bare buttons (no data-id → nothing to fetch).
#   Bare card reuses the full card's .info-hero/.dlg-grid/.dlg-section classes so a
#   themed and an untemed row read as the same surface; ends with a "no theme yet — add
#   one from the SOURCE menu" call to action. JS-only.
# 0.50.65: code-review follow-ups for v0.50.64. (1) CONFIRMED race (found by 2 finders):
#   openBareInfoDialog painted #info-dlg-body synchronously but never bumped
#   openInfoDialog._seq, so an in-flight full-card fetch (from a prior themed-row ⓘ click)
#   passed its own `_seq !== _myToken` guard and clobbered the bare card with the WRONG
#   row — exactly the v1.17.20 audit-race-#7 the seq guard exists to prevent, sidestepped
#   by the new synchronous path. Fixed: bump openInfoDialog._seq at the top of
#   openBareInfoDialog so any pending fetch self-aborts. (2) LOW cleanup: dropped the dead
#   `|| ''` from `const mt = it.plex_media_type` in renderBareInfoCard (the ===-checks and
#   the `mt || '—'` render fallback behave identically on undefined). JS-only.
# 0.50.66: bare INFO card now shows the Plex poster like a themed item. the user:
#   "could we include the content's poster from Plex like a themed item." The full card
#   (openInfoDialog) already builds a poster-left hero from `/api/plex/art/{rk}` (numeric-
#   rk-guarded same-origin proxy, v1.24.52); the bare card already carries the row's
#   rating_key, so renderBareInfoCard builds the IDENTICAL <img class="info-poster"> as
#   the first hero child — fully client-side, no new data/round-trip. openBareInfoDialog
#   attaches the same post-paint error handler (remove the img on 404/non-art → hero
#   collapses to just the meta), so a row whose Plex art is missing degrades gracefully.
#   JS-only.
# 0.50.67: hero-wave + mobile-nav pass (4 the-user asks). (1) Mobile: the top nav
#   wrapped LOGS/SETTINGS to a 2nd row below ~927px — now a horizontal-scroll strip
#   like the settings tabs (.nav nowrap+overflow-x scoped to <=1080px; inset focus
#   ring for the dual-axis clip). (2) Mobile: the hero wave overlapped the
#   Collections subtitle — .hero now RESERVES the wave's clearance as padding-bottom
#   (border-box → no desktop growth, but guaranteed even when content overflows
#   min-height), so the wave is uniform + non-overlapping across every tab/width.
#   (3) Richer wave: a 2nd broader/slower sine scrolling the OTHER way (.hero::before)
#   so the band cross-weaves instead of one obvious repeat. (4) The hero wave is now
#   motif's "now working" indicator (bigger + more visible than the upper-left brand
#   EQ): html.motif-busy (toggled from anyMutatingOpActive) speeds it up + brightens
#   (green→green-bright) + grows it (scaleY), eased via transition. The brand EQ's
#   reactivity (.brand-mark.is-active) was RETIRED → calm constant ambient drift now.
#   CSS + 1 JS toggle line.
# 0.50.68: hero-wave reactivity follow-ups (2 the-user asks). (1) INSTANT START: the
#   wave lagged ~1.1s — it waited for refreshTopbarStatus to see the enqueued job past
#   the /api/stats 1s cache. ops.js now flips motif-busy the instant of the click (in
#   setOptimisticPlaceholder) + exposes hasOptimistic(); refreshTopbarStatus unions it
#   into heroBusy so it can't strip the wave off in the gap before the real op lands.
#   (2) INTENSITY BY QUEUE DEPTH: data-busy-level (1..3, CAPPED at 3 so it never looks
#   messy) scales the wave with how much is stacked — score = distinct active op-kinds
#   (tdb sync + plex refresh + bulk) + total queued per-row jobs (download_in_flight
#   etc. count pending+running, so a deep download queue pushes it up). 3 CSS levels
#   escalate opacity↑/duration↓/scaleY↑ monotonically; L3 scaleY 1.30 stays inside the
#   38px reserved band. CSS + ops.js + app.js.
# 0.50.69: durable hero-alignment RULE (the user — "keeps coming up"). The v0.50.67
#   wave padding (border-box) shrank the hero content-area to 150-38=112px, SHORTER
#   than the dashboard's ~119px 2-line-subtitle content, so the dashboard hero grew
#   past the floor while the shorter library/settings heroes stayed at 150 → the first
#   content block (search bar / RECENTLY ADDED / settings form) + the wave landed at
#   DIFFERENT Y per tab. Fix: hero geometry now derives from TWO :root tokens —
#   --hero-content-zone (126px, >= tallest hero content) + --hero-wave-band (38px).
#   wave-on min-height = calc(zone + band), padding-bottom = band → content-area == zone
#   >= tallest content, so EVERY tab renders the hero at one height. wave-off min-height
#   = zone (same token) → uniform + consistent when hidden too. Both states derive from
#   one token so they can't drift again. New test_v0_50_69 guards the rule (2 token-
#   derived min-heights, zone >= 120). CSS-only.
# 0.50.70: hero wave no longer visibly RESETS when changing sections (the user —
#   dashboard/logs/settings full-page reloads restarted the CSS animation at phase 0
#   while the library tabs looked continuous → flicker). base.html sets negative
#   animation-delay vars (--hero-wave-delay / -2) from Date.now() % duration, pre-paint,
#   so each wave layer resumes its continuous wall-clock phase on every load instead of
#   snapping to 0. .hero::after (9s) + .hero::before (14s) each read their own var (0s
#   fallback if JS off). Deterministic phase across all pages → nothing jumps; the two
#   layers' cross-weave stays continuous too. base.html + CSS only.
# 0.50.71: two hero-wave follow-ups. (1) NO idle→busy FLIP on nav: starting a refresh
#   on /tv then going to /dashboard briefly painted the SLOW wave, then the poll flipped
#   it fast (the user — "the change in how the wave looked"). Now app.js persists the
#   busy level to sessionStorage (per-tab, v1.18.84 scope) and base.html restores
#   motif-busy + data-busy-level PRE-PAINT, so a page loaded during active work paints
#   busy immediately; the poll keeps it accurate + clears it (self-corrects if the op
#   ended mid-nav). (2) RICHER LEVEL 4 (the user — "richer another level as a sync AND
#   refresh is going on"): score >= 6 → L4; its richness comes mostly from the SECOND
#   wave layer swelling (::before opacity 0.30→0.42) so the cross-weave reads fuller,
#   not just faster. Still capped (no L5). base.html + app.js + CSS.
# 0.50.72: two small clarity fixes (the user). (1) Dashboard RECENT ACTIVITY
#   "// all events →" hit bare /queue → the JOBS default, not the Events Log the
#   section shows. Link now carries ?view=events; both the /queue route (SSR) and
#   bindQueue (client re-apply) honor an explicit ?view=events|jobs so it lands on
#   the event stream flash-free. (2) The "no catalog changes (N checked)" sync
#   notification named no unit — the creeping N read as a library count. Now
#   "no catalog changes (6,201 ThemerrDB themes checked)" so it's clear the number
#   grows with ThemerrDB, not with matches in the motif library. api.py + app.js +
#   dashboard.html + worker.py.
# 0.50.73: hero-wave intensity now RAMPS instead of spiking (the user — "should have a
#   natural increase ... right now it jumps or spikes ... jarring when kicking off a few
#   jobs"). refreshTopbarStatus sets a TARGET level; _rampWaveLevel steps the DISPLAYED
#   level toward it one at a time (~380ms/step), so idle→L4 swells over ~1.5s + winds
#   down the same way, instead of snapping. _waveDisplayed inits from the DOM so a nav-
#   restored level (v0.50.71) still snaps (no ramp-from-idle on load). app.js only.
# 0.50.74: SOURCE BREAKDOWN — 4th ANIME donut + dynamic fill-the-arena layout + no-theme
#   off by default (the user, 3 asks). ANIME splits off TV via is_anime (theme_sources
#   carries it since v1.24.56); Movies + TV now EXCLUDE anime so it can't double-count.
#   The 4 donuts are table-driven (_SOURCE_DONUTS, each with its own scope predicate +
#   hidden set). renderAllSourcePies hides a donut whose scope has 0 items + sets
#   .source-pie-row[data-visible=N]; CSS makes the survivors FILL the row (4→1 across
#   desktop, resizing down; mobile 2-per-row, the lone odd chart full-width). '–' (No
#   theme) is seeded into every donut's hidden set once (idempotent marker) so it's
#   unchecked by default without resetting other toggles. app.js + dashboard.html + CSS.
# 0.50.75: code-review fixes on the 0.50.72-74 batch (xhigh). (1) CONFIRMED bug — the
#   hero wave could get STUCK busy forever: ops.js adds `motif-busy` out-of-band on an
#   optimistic click (v0.50.68) without touching _waveDisplayed, so if that click never
#   became a real op, refreshTopbar's _rampWaveLevel(0) hit the `displayed === target`
#   early-return (both already 0) and skipped the classList.remove — pre-v0.50.73's
#   unconditional toggle('motif-busy', heroBusy) had cleared it. Fix: the terminal branch
#   now AUTHORITATIVELY clears the class + attr when target<=0 even if already at 0. New
#   behavioral test locks it (idle-reconcile clears the stuck class). (2) Cleanups: folded
#   the parallel _keyById lookup into _SOURCE_DONUTS (each spec now carries its storage
#   `key` — one source of truth, no drift); refreshed two stale comments referencing the
#   removed renderTotal/Movies/Tv wrappers. app.js + test_v0_50_73.
# 0.50.76: CONTINUOUS-VELOCITY hero wave (the user — "like pressing the gas pedal it
#   smoothly starts increasing in speed, not suddenly at the new speed ... same when a
#   job completes it's like gently pressing the brake"). The v0.50.68-73 model scaled
#   the wave across 4 discrete data-busy-level CSS steps that swapped animation-duration;
#   that property ISN'T animatable, so each step re-timed the keyframe and the wave
#   JOLTED even stepping one level at a time. Replaced with a single rAF loop
#   (_heroWaveTick): it advances a MONOTONIC phase (a speed change alters the velocity,
#   never the position — so it can't jump) while easing a 0..1 energy toward a target
#   (exponential smoothing, ~0.75s time-constant). energy drives BOTH the phase velocity
#   (idle 9s/14s → full ~2.5s/3.8s) and the CSS intensity (opacity/scaleY/brightness via
#   calc() off --hero-wave-energy), so speed + intensity ramp together, continuously.
#   refreshTopbarStatus maps the busy score → a continuous energy target (floored so one
#   job reads, saturated at 1 so heavy stacking can't get frantic). ops.js kicks the wave
#   on click via window.__motifHeroWaveBump (instant gas response). base.html seeds the
#   wall-clock phase + restored energy pre-paint (no nav flicker; replaces the v0.50.70
#   animation-delay + v0.50.71 class/attr restore). Removed the 8 discrete level rules +
#   2 @keyframes + the motif-busy/data-busy-level class machinery. app.js + app.css +
#   base.html + ops.js; rewrote the 6 wave test files for the continuous model.
# 0.50.77: SOURCE BREAKDOWN — per-chart // HIDE + anime-folds-into-TV (the user). (1)
#   Each donut card has its own // HIDE button; hiding one/two/three drops them from the
#   arena and the survivors resize (the existing data-visible grid). The LAST visible
#   chart can't be hidden; hidden charts get restore chips in the section header. Hidden
#   set persisted per-browser (motif:dash:src-charts-hidden). (2) When the ANIME chart
#   isn't shown (user-hid it or no anime library) its data folds INTO the TV chart (scope
#   show||is_anime) and the card relabels "// TV + ANIME"; when anime IS shown each chart
#   is just its own library (TV = show && !is_anime). _scopeFor makes only the TV scope
#   dynamic; renderAllSourcePies + the render-skip key handle the re-render. app.js +
#   dashboard.html + app.css + new test_v0_50_77.
# 0.50.78: refine the anime fold (the user) — when the ANIME chart is hidden, anime folds
#   back into its NATIVE chart by media_type: anime MOVIES → Movies, anime SHOWS → TV
#   (v0.50.77 folded ALL anime into TV). No "+ ANIME" label on either card — it's just
#   assumed. _scopeFor now drops the `&& !is_anime` filter on BOTH Movies + TV when anime
#   isn't shown (reusing each donut's static scope for the shown case); the TV relabel is
#   gone. app.js + dashboard.html (tooltip) + test_v0_50_77.
# 0.50.79: REORDER the SOURCE BREAKDOWN pie charts (the user — "like the plex library
#   items"). Each donut card gets ◀ ▶ buttons that swap it with the adjacent VISIBLE chart;
#   the display order is persisted per-browser (motif:dash:src-charts-order) and applied by
#   physically reordering the .source-pie-col elements (DOM order == visual order, so the
#   is-last-visible / data-visible arena logic stays correct). renderAllSourcePies iterates
#   _orderedDonuts() now. _loadChartOrder sanitizes stored order (dedup, drop unknown, append
#   missing canonical); _reorderIds is the pure swap. app.js + dashboard.html + app.css +
#   new test_v0_50_79.
# 0.50.80: MATCHING/PLUS-MODE cleanup (the user asked what it still does). It's fully live
#   — plus_mode threads config → normalize_title, which decides folder↔theme title matches;
#   it only matters for a title containing a literal '+'. Dropped the never-reachable third
#   "word" mode (+ → " plus "): it was absent from the settings dropdown AND rejected by the
#   config validator, so only "separator" (default) + "literal" ever ran. Trimmed the enum +
#   normalize branch, rewrote the settings help copy to say WHEN literal matters (so 'C++' /
#   'Disney+' isn't collapsed to 'C' / 'Disney'). No behavior change for any reachable mode.
#   normalize.py + settings.html + test_v1_13_82 (dropped word test, added enum guard).
# 0.50.81: RUNTIME → DRY-RUN MODE status pill no longer shifts the row between states (the
#   user). The live label was "LIVE — real downloads + placements" — wide enough that, at
#   the 720px settings reading measure, the pill + toggle buttons overflowed and flex-wrap
#   dropped the buttons to a 2nd line (only when live), so the label sat "weirdly above".
#   Tightened it to "LIVE — real action" (parallels "DRY-RUN — no real action") so BOTH
#   states fit on one row identically; the downloads+placements detail already lives in the
#   hint right below. app.js one-liner + guard in test_v1_22_55.
# 0.50.82: LIVE OPS drawer — persistent expand hint (the user: the ▸ affordance on each
#   op card wasn't obvious). A muted "// click an op to expand its run detail" line sits
#   between the drawer head and the scrolling body (stays at the top); ops.js hides it when
#   there are no active/finished cards to expand. base.html + ops.css + ops.js + guard in
#   test_v1_20_22.
# 0.50.83: bulk SWITCH TO API (the user — filter LINK=HL, select, switch all to API push in
#   one pass instead of the per-row SOURCE menu). New // SWITCH TO API bulk button, visible
#   when the selection holds ≥1 themed FILE-sidecar row (hardlink/copy: media_folder set, not
#   the plex_upload sentinel ''), gated off the same browse views as PUSH. The click handler
#   mirrors bulk PUSH and loops the EXISTING per-row /switch-placement endpoint (edition-
#   scoped via rating_key) — no new backend, reuses the tested per-row teardown/upload path.
#   Only file rows are candidates (the endpoint FLIPS file↔api, so a plex_upload row would
#   flip back). library.html + app.js (bucket + visibility + handler) + new test_v0_50_83.
# 0.50.84: recolor the // SWITCH TO API bulk button cyan (the user — match the PLACE row).
#   Was amber btn-plex; now lib-source-place_api (cyan), the same class the per-row SWITCH
#   TO API menu item + the PU LINK chip use — the placement kind the action produces. Mirrors
#   the bulk ADOPT button's lib-source-adopt idiom. library.html one class swap + test.
# 0.50.85: bulk SWITCH TO API now offers SRC=U (and any HL/copy) rows (the user — testing
#   found U rows weren't presented/switchable). Cause: the v0.50.83 eligibility used
#   themedPred, whose `upstream_source !== 'plex_orphan'` arm (a PUSH-specific exclusion)
#   dropped SRC=U rows sitting on a plex_orphan theme (api.py:590 — an orphan row can carry
#   a user URL). Fix: gate on the SAME condition the per-row SWITCH uses (theme_tmdb + theme_
#   media_type present) — any SRC (T/U/A/M) that's HL/copy is now switchable. Count bucket +
#   handler predicate both updated (kept identical) + test_v0_50_83 (orphan row now eligible).
# 0.50.86: batch of mobile optimizations (the user, on-device testing). (1) LOGIN — an
#   "Invalid username or password" error pushed the auth-card's content past what a strict
#   circle can hold on mobile, clipping PASSWORD against the ring; aspect-ratio:1/1 → min-
#   height, so it stays a circle when content fits and grows into an oval instead of clipping
#   when it doesn't. (2) DASHBOARD SYNC HISTORY table had no scroll affordance and spilled off
#   the card's edge; gave it its own horizontal-scroll context (mirrors #library-table
#   .table-scroll) + a min-width. (3) LIBRARY FILTERBAR — // CLEAR ALL stranded itself on a 3rd
#   line alone, because margin-left:auto lived on .library-presets-menu ALONE and ate all
#   remaining room on ITS wrapped line before CLEAR ALL (next in source order) could share it;
#   grouped both under one .library-toolgroup wrapper so they wrap together. (4) SOURCE
#   BREAKDOWN pie cards — legend text forced cards wider than a mobile 2-per-row column
#   affords (the fixed 110px donut ALONE already exceeds the available column width beside a
#   legend); min-width:0 + ellipsis on the legend name (general fix, harmless on desktop) +
#   a mobile-only stack (donut above legend, not beside it) so the legend gets the full card
#   width. app.css (4 spots) + library.html (toolgroup wrapper) + new test_v0_50_86.
# 0.50.87: settings mobile overflow batch 2 + drop the eager sync-probe (the user, more
#   on-device testing). (1) API TOKENS' 6-column table + (4) LIBRARY SECTIONS' two 7-column
#   tables had no scroll context — spilled off-screen on mobile (LIBRARY SECTIONS taking the
#   MGD/ROLE checkboxes with it). Both wrapped in .table-scroll — the SAME reusable class
#   #library-table already uses, so zero new CSS, just two more consumers. (2) .form-actions
#   (used by EVERY settings save-row with a second button — SAVE+TEST NOTIFICATION, SAVE+PROBE
#   TRANSPORT, etc.) had no flex-wrap, so on a phone the combined width spilled past the card
#   edge instead of dropping to a 2nd line; added flex-wrap:wrap once at the base rule, fixing
#   every such row at once (harmless on desktop, always room there). (3) FUNCTIONAL BUG:
#   // PROBE TRANSPORT auto-fired 800ms after EVERY settings-page load, not just when viewing
#   // SYNC TRANSPORT — sync-probe-btn lives in the DOM for every tab (inactive panels are just
#   display:none). No sibling probe (TEST COOKIES/PROBE PLEX THEMES/TEST NOTIFICATION/TEST
#   PLEX) auto-fires; dropped the v1.13.2 auto-probe so it's click-to-run like the others.
#   settings.html (3 table-scroll wraps) + app.css (.form-actions) + app.js (bindSyncProbe) +
#   new test_v0_50_87.
# 0.50.89: holistic-audit fix sweep (Sonnet-5 full-codebase audit, 32 findings triaged into 7
#   batches; every fix verified against real code before writing — Sonnet's raw finding list
#   over-flagged, so ~6 "findings" were confirmed non-bugs on fresh scrutiny). BATCH 1 (edition-
#   scope bleeds): colon-folder migration, _do_adopt/_verify_adopt_state, _is_p_row_for_section,
#   recovery_v55 walker all had writes/reads missing edition_key → sibling-edition bleed; scoped
#   each. BATCH 2 (atomicity): cloud_theme_backup DB write now try/except-wrapped (honors its
#   never-raises contract after os.replace); _detect_and_stamp_drops_git wrapped in a txn;
#   _restore_lost_placements re-checks the in-flight dedup INSIDE the txn; get_runtime_bool seeds
#   via ON CONFLICT DO NOTHING. BATCH 3 (races): scanner re-stats after hashing + corrected stale
#   docstring (scan/place DO run concurrently since v1.20.40); cookies snapshot chmod 0600;
#   config_write_lock serializes PATCH /api/config read-modify-write. BATCH 4 (security): events
#   scrubber redacts compound query-param secret names (session_token=) + Slack webhook tokens.
#   BATCH 5 (config): validate() TypeError guard on corrupt YAML; save() no longer bakes active
#   env overrides into motif.yaml; refresh_sections matches include/exclude case-insensitively +
#   warns on zero-match. BATCH 6: _topbar_ssr_state now covers all 7 op_progress kinds (bulk_lps/
#   tvdb_bridge/cloud_themes_backup were falling to the generic ELSE tier). BATCH 7: bulk-ADOPT
#   click-handler predicate widened for plex_upload; api_decide_finding(_bulk) return 400 (not
#   500) on malformed JSON; scheduler job_defaults set misfire_grace_time=3600+coalesce so a cron
#   straddled by a restart still runs; downloader truncation + tmdb search-confidence scoped with
#   documenting comments (deliberate tradeoffs); orphaned /scans client JS surface (354 lines)
#   removed. New test files test_v0_50_89_audit_batch{1..7}; several stale source-pins updated.
# 0.50.90: auto-resolve imdb-bearing orphans when a TMDB key is present (the user:
#   "my key is valid + TMDB has the movie but the row still shows tmdb: orphan").
#   Root cause: the v1.22.49 de-orphan re-key walker was manual-only (a hidden
#   admin POST defaulting to dry_run), so orphans minted BEFORE a key was
#   configured never got re-keyed — a valid key didn't fix them retroactively.
#   New deorphan.resolve_orphans_in_background() fires the NON-destructive re-key
#   walker on a daemon thread (single-flight lock, no-ops fast when nothing to
#   resolve) from three triggers: boot (key present), a config save that sets the
#   TMDB key, and a TEST KEY that validates. The destructive collision-merge
#   stays manual. settings.html hint now says orphans re-resolve automatically.
#   NOTE: a specific row only resolves if TMDB's /find cross-references its IMDB
#   id — rows TMDB doesn't link stay orphan (logged as no_tmdb_match). New
#   test_v0_50_90_deorphan_autoresolve.
# 0.50.91: mobile polish + CRT turn-on rebuild (on-device report). (1) Touch
#   devices latch :hover after a tap, so buttons stayed "selected" — a @media
#   (hover: none) block suppresses the stuck fill/lift/glow/brighten (real
#   selected state uses .*-active classes, untouched; transform preserved for
#   .library-search-clear's base centering). (2) The round login card's stacked
#   fields clipped through the ::after ring on narrow screens — a @media
#   (max-width:560px) gentle oval (min-height 135vw) + trimmed inner safe-zone
#   (64%) keeps the ring a continuous loop with the form in its fat middle
#   (verified in-browser at 375px). (3) CRT power-ON rebuilt as the exact
#   inverse of power-OFF (scanline blooms from a centre point + veil lifts)
#   replacing the green flash. (4) The running-job op-mini strip wrapped onto
#   its own line in the cramped mobile topbar and floated there half-width,
#   disconnected — now pulled out of the status flow and pinned as a full-width
#   bar along the bottom edge of the topbar (:has reserves the row only while a
#   job is live). Verified in-browser at 375px. (5) The first-run SETUP form
#   (lede + 3 fields + hint + forward-auth foot) overflowed the round vinyl card
#   — it lacked login's .auth-card-inner column wrapper, so its children laid
#   out as flex-ROW columns and the foot spilled off-screen; it now wraps in
#   .auth-card-inner and the card degrades to the rounded-rect .auth-card-setup
#   "sleeve" that sizes to content (login stays a circle; scoped to the
#   modifier). New test_v0_50_91_mobile_and_crt.
# 0.50.92: kill the STANDARD-chip flash on library tab switch (the user: nav to
#   a tab last viewed in 4K briefly shows STANDARD before flipping to 4K). Root
#   cause: switchLibraryTab fetches the bare nav href /movies (no ?fourk), so
#   the server renders that fragment with STANDARD active; curChips.replaceWith
#   paints it, THEN hydrateLibraryStateForTab reads motif:variant:<tab>='fourk'
#   and the 4K chip lights up — one frame of STANDARD. Fix: pre-apply the
#   persisted variant to the fetched chips fragment BEFORE swapping it in
#   (respecting availability via the server's display:none), so the first paint
#   is already the right variant. app.js only. New test_v0_50_92_variant_flash.
# 0.50.93: fix the v0.50.91 hover regressions (self code-review). The
#   @media(hover:none) RESET was leaky: (a) `background: transparent` on the
#   TDB/ATTN filter pills stripped their tinted variant-class bg on touch
#   (regression); (b) `text-shadow: none` on .title-glyph stripped its
#   state-variant glow on touch (regression); (c) color-only hovers (.tab,
#   .dlg-close, .source-pie-restore-chip) were never reset so they stayed
#   latched — incl. .tab, the primary mobile nav (the whole symptom); (d)
#   .lib-flag-pill's real hover is :hover:not(:disabled), a specificity the
#   reset missed. Replaced the reset with GATING: the ~29 control-primitive
#   :hover rules are now wrapped in @media (hover: hover) at their own sites, so
#   a touch device (hover:none) simply gets no hover — base + variant styling
#   shows untouched, nothing latches. Correct by construction, no base-value
#   coupling. Also (code review): .auth-card-setup → the compound selector
#   .auth-card.auth-card-setup so it wins by specificity not source order; and
#   removed the now-dead v0.50.88 ops.css op-mini caps (the #op-mini strip
#   override supersedes them). CSS only. test_v0_50_91 hover tests rewritten to
#   assert the gating. Touch behavior to be confirmed on-device.
# 0.50.94: fix a v0.50.93 hover-gating regression on the // LEGEND toggle (the
#   user: "legend feels off ... should have the clicked highlight displayed"). The
#   open (active) green was a TWO-LINE selector list — `.library-legend-pill.open,`
#   / `.library-legend-pill.open:hover { … }` — and v0.50.93 wrapped the (hover:
#   hover) gate around only the `{`-bearing line, leaving `.open,` dangling before
#   the @media at-rule → invalid selector list → the browser dropped the WHOLE rule
#   → an OPEN legend showed no active accent (brace-balanced, so the CSS check
#   passed; the rule was just semantically dead). Split into two standalone rules:
#   `.open` UNGATED (active green on every device, touch included) + `.open:hover`
#   gated (only fights .chip:hover where a cursor can hover). Restores the exact
#   pre-v0.50.93 look; desktop hover + closed state unchanged. test_v1_23_51 green-
#   accent guard strengthened from a substring check to a real standalone-rule +
#   not-swallowed-by-@media assertion (the old guard passed on the broken CSS). CSS
#   + test only.
# 0.50.95: self-host the web fonts; drop external Google Fonts. Investigating the
#   user's "navigating to LOGS I notice a filter on the top row + a size change in
#   the top bar" (desktop, no job): rendered the real pages + measured — the topbar
#   AND hero are pixel-identical across pages (no per-page layout change). The real
#   cause is a FONT FOUT: all 3 families loaded from fonts.googleapis.com render-
#   blocking + display=swap, so on any cold-cache nav (e.g. DevTools "Disable cache",
#   which the user had on) the top row painted in the monospace fallback then
#   REFLOWED when VT323/JetBrains Mono swapped in — measured 30-85px horizontally
#   (hero title 242px VT323 vs 327px fallback; heights unchanged, which is why it
#   read as subtle). Fix: bundle the latin woff2 (VT323 400; JetBrains Mono 400/500/
#   600/700 — 300 requested-but-unused, dropped) in /static/fonts, @font-face in
#   app.css, preload the 2 first-paint fonts in base.html (href byte-matches the src
#   so no double-fetch), remove the Google <link>+preconnects. Same-origin+preloaded
#   → branded font on the first frame, no swap reflow; also kills the external
#   dependency (privacy/offline) for a self-hosted Authentik-gated app. New
#   test_v0_50_95. base.html + app.css + 5 woff2, no JS.
# 0.50.96: two on-device UI bugs. (1) LEGEND: the "full reference in // GLOSSARY"
#   button inside the library legend went dead after any client-side library-tab
#   switch (the user). switchLibraryTab innerHTML-swaps .library-legend-body, which
#   replaced the #library-legend-gloss node and dropped the direct click listener
#   initHelpMode attached once. Fixed via event delegation on the STABLE
#   #library-legend panel (survives the body swap). (2) LOGIN: on a narrow mobile
#   viewport the username/password fields spilled past the vinyl-label oval (the
#   user). The global .input min-width:240px floor forced them wider than the
#   trimmed .auth-card-inner (64% ≈ 217px at 368px); scoped .auth-card .input
#   { min-width: 0 } into the ≤560px media query so the fields shrink to the inner.
#   app.js + app.css. New test_v0_50_96.
# 0.50.97: glossary chips line up across ALL sections (the user: "the TDB chips at
#   the top and the TOPBAR at the bottom are not in line with the other glossary
#   items"). The 4 narrow sections shared a fixed 26px chip rail (defs aligned at
#   x=71), but TDB + TOPBAR used .help-gloss-grid-wide with an `auto` rail sized to
#   their wider text pills, so their chips + defs sat ~25-33px right of the rest
#   (measured: defLeft 96 / 104 vs 71). Fix: ONE uniform 60px rail on every section
#   (fits the widest chip, RE-PUSH/NO TDB ≈ 58px) + LEFT-align the chips
#   (justify-self:center → start), and dropped the .help-gloss-grid-wide exception
#   (CSS rule + the class on the TDB/TOPBAR divs). Verified in-browser: every chip
#   left edge + every def now land at one x across all 6 sections (chipLeft 29 /
#   defLeft 101 uniformly). app.css + base.html; test_v1_23_58 updated.
# 0.50.98: CRT power-ON rebuilt as a "vertical unfold" (the user picked variant A
#   from the mockup; the v0.50.91 inverse-of-power-off wasn't quite "a CRT turning
#   on"). The raster snaps to a bright centre line then the picture unfolds top→bottom
#   to fill the tube, the fold-line blooming then fading. Mechanism: two black shutters
#   (.crt-power-on::before top / ::after bottom) retract from the centre to the poles
#   revealing the live page; a SEPARATE child span (.crt-on-line, added in base.html)
#   carries the bright --fg scanline + glow so its flash doesn't ride the container
#   opacity (which would wash out the shutters). Dropped the crt-power-on-bg veil +
#   scaleX bloom. Verified via freeze-frames at 0.10/0.30/0.50s (centre-line flash →
#   unfolding → settled). Fires once after login / manual refresh, same trigger + the
#   VISUALS opt-out (html.viz-no-crt-on) as before. app.css + base.html; test_v0_50_91
#   §3 rewritten, test_v0_50_7 structure-agnostic (still green).
# 0.50.99: centre the glossary chips in the uniform rail (the user preferred centred
#   over the v0.50.97 left-align). One-line flip: justify-self start → center on the
#   col-1 chip indicator. The 60px uniform rail from v0.50.97 still aligns the defs
#   across sections; this just re-centres the chips within it — all chip centres + all
#   defs line up. CSS only; test_v1_23_58 flipped back to assert center.
# 0.51.0: rolled over from the 0.50.x line (closed at 0.50.99). Cut to land the
#   /code-review fixes for the v0.50.96-99 UI thread (3 confirmed findings):
#   (1) CRT fold-line glow was clipped below the seam — .crt-on-line paints between
#   ::before and ::after, so the bottom shutter overpainted its downward glow;
#   gave it z-index:1 above both shutters → symmetric bloom (verified via freeze-
#   frame). (2) The inline library legend's TDB section still carried the dead
#   .help-gloss-grid-wide class (rule deleted in v0.50.97, base.html stripped, but
#   library.html:813 missed) — removed it; the section already used the 60px rail so
#   no visual change. (3) test_no_wide_rail_exception only scanned base.html, letting
#   the stale library.html class pass green — now scans every template. Two altitude
#   design-smells (60px magic rail; the recurring .input min-width:240px floor)
#   deferred as backlog. app.css + library.html + test.
# 0.51.1: code-review altitude fix #1 — the global .input min-width:240px floor
#   (sized for full-width dialog inputs) kept overflowing narrower containers and
#   needing per-container min-width:0 escapes (.input-tiny; the v0.50.96 .auth-card
#   one). Changed the base floor to min(240px, 100%): it self-limits to the
#   container's own width, so it can never force the input wider than its parent,
#   while still holding at 240px wherever there's room. Retired the redundant
#   .auth-card .input escape (verified the login still doesn't spill at 368px). CSS
#   only; test_v0_50_96 login tests reworked to assert the self-limiting floor.
# 0.51.2: code-review altitude #2 — named the glossary chip rail --gloss-chip-rail
#   (was a bare 60px literal). Investigating the "auto-size the rail" suggestion: it
#   isn't feasible — the modal STACKS its glossary sections (one shared fixed rail
#   aligns their defs) while the inline library legend lays sections in a multi-column
#   grid (independent), and both share .help-gloss-grid, so no single content-sized
#   column spans both. So the fixed rail is correct; the fix here is to name it (one
#   documented adjustment point) + document the constraint (rail ≥ widest chip ≈ 58px)
#   inline. No auto-size, no overflow char-guard (char-count ≠ rendered width across
#   the chip fonts = a phantom guard). CSS + test only; no visual change.
# 0.51.3: mobile audit A1 (P0) — the nav gets its own full-width second row on a
#   phone. Pre-fix the brand | nav | status single-row grid crushed the 7-tab nav
#   into a ~45px horizontal-scroll sliver (measured; only "DAS…" of DASHBOARD
#   showed, the other 6 tabs off-screen with no scroll affordance) — mobile
#   navigation was effectively broken. Reworked the ≤600px topbar into a 2-row
#   grid-template-areas layout: brand + status share row 1 (they fit ~330px <
#   375px once the nav leaves), nav spans the full width on row 2 (~351px) as a
#   horizontal-scroll strip keeping desktop tab order, now with a thin scroll
#   track as the "more →" affordance (was hidden) + ~44px tap targets. brand got a
#   min-width:0 shrink floor (A9). The v0.50.91 op-mini job strip still pins to
#   the topbar bottom via :has(), below the nav row (verified: no overlap). CSS
#   only + test_v0_51_3; v0.50.88 grid-floor asserts updated for the supersession.
# 0.51.4: mobile audit B1 (P1) — hero action buttons reflow consistently across
#   pages on a phone. Pre-fix the hero groups reflowed THREE ways: desktop docks
#   them right; on mobile the library's .sync-actions went full-width below the
#   title but the dashboard's .hero-actions had NO mobile rule, so under .hero-row's
#   space-between it wrapped to a 2nd line and stayed LEFT-aligned at natural width.
#   Swiping tabs, the primary button jumped full-width → left-aligned. The user
#   chose one treatment for all: full-width below the title. Added .hero-actions {
#   width:100% } + .hero-actions .btn { flex:1 1 100% } to the ≤600px block so the
#   dashboard pair spans the row like the library (single SYNC THEMERRDB usually;
#   +REFRESH PLEX when auto-enum is off → stacked full-width). Verified in-browser
#   (dashboard button 190px left-aligned → 319px full content width). CSS + test.
# 0.51.5: mobile audit A4 (P1) — the Settings > IMPORT preview table swipes on a
#   phone instead of crushing. It's table-layout:fixed width:100% with fixed columns
#   summing ~736px + a fluid TITLE, wrapped in the generic .table-scroll (overflow-x
#   at ≤1080px) but with NO min-width floor — so at 375px its columns crushed to
#   ~281px (the Apply <select> + URL cells unreadable), unlike the sync-history /
#   jobs sibling tables which pin a floor. Added #import-preview-table {
#   min-width: 880px } in the ≤1080px block (co-located with the .table-scroll swipe
#   context) so the table holds its readable width and the wrapper swipes. Its only
#   popover is the Apply ▼ native <select> (browser top layer) — no dual-axis clip
#   like #library-table's custom .row-menu-panel. Verified in-browser (table 880px,
#   wrapper swipes, page stays 375). CSS + test. NOTE: library-table row actions
#   (audit A2/A3) intentionally SKIPPED — the fragile client-rendered core table
#   can't be verified in-harness; the user chose to leave it (actions still reachable
#   via h-scroll, not a regression).
# 0.51.6: mobile audit A6 (P2) — tap targets bumped to ~30px on a phone. Several
#   controls sat below the 24px WCAG-AA target / a comfortable thumb: the topbar `?`
#   help + `⏻` logout glyphs (~22px), the filter-drawer DL/PL/LINK pills (20px —
#   deliberately dense to fit six rows on desktop), and the SOURCE BREAKDOWN
#   per-donut ◀ ▶ / // HIDE controls (~10-14px, customize mode). Bumped all to 30px
#   in the ≤600px block ONLY; the desktop densities (topbar trio, six-row drawer)
#   are untouched. The drawer pill bump is uniform across pill types so the rows
#   stay aligned, and scoped to .pill-filter-drawer so inline rows keep density.
#   Verified in-browser at 375px (help 30×30, logout 30h, link-glyph + state-pill
#   30h, no overflow). CSS + test.
# 0.51.7: mobile audit B2 (P2) — settings action rows left-align their buttons after
#   wrapping on a phone. The .control-row groups (DRY-RUN, TVDB BRIDGE, REPROBE,
#   PROBE, ORPHAN SCAN) use .control-actions { margin-left: auto } to dock buttons
#   RIGHT on desktop; on mobile the flex-wrap row drops them to their own line but
#   margin-left:auto kept them right-anchored + detached from their label. Added
#   .control-row .control-actions { margin-left: 0 } in the ≤600px block. The
#   .control-row prefix (0,2,0) is REQUIRED — the base .control-actions (0,1,0) is
#   defined LATER in the file than the ≤600px block, so a bare-selector override
#   there would lose on source order (the mobile-override-before-base-rule trap;
#   caught it in-browser — first attempt showed margin-left still 110px). Verified:
#   marginLeft 0, actions left-aligned under the label, wrapped. CSS + test.
# 0.51.8: mobile audit A5 + A8 (P2/P3) — LOGS polish. A5: the .jobs-grid scrolls
#   horizontally on a phone (min-width:760px) so the ACTION column (cancel / // ACK)
#   sits off the right edge, but the native touch overlay scrollbar is invisible
#   until you scroll → the column read as absent. Added a thin persistent scroll
#   track (same treatment as the topbar nav) as the affordance. A8: the LOGS
#   .block-head (// JOBS + // EVENT STREAM chips + `● live`) is space-between +
#   flex-wrap on mobile; chips (~238px) + live (~45px) exceeded the ~281px content
#   width by a hair, so `live` wrapped to its own line, detached from the // EVENT
#   STREAM chip. Trimmed the block-head 18px side padding to 12px on a phone (a
#   sensible density gain for every section header) — recovers enough room for live
#   to stay on the chip row. Verified in-browser at 375px (live inline, block-head
#   back to 50px; jobs-grid scrollbar-width:thin, still scrolls, no overflow). CSS +
#   test.
# 0.51.9: mobile audit A7 (P3) — TDB coverage card foot wraps instead of clipping.
#   The COVERAGE cards (MOVIES/TV/ANIME/COLLECTIONS THEMED) render two foot spans
#   ("N of M themed" + "Z ready to add") in the base .stat-foot flex row with no
#   wrap; 4-5 digit counts overflowed the card and clipped against its
#   overflow:hidden (worst on a narrow 4-up desktop card or a full-width phone
#   card). The PLEX cards already stack their foot (v1.21.4 column); coverage had no
#   relief. Added flex-wrap:wrap to the base .stat-foot so the 2nd span drops to a
#   new line only when it must — harmless for the plex column layout + single-span
#   clickable cards. Verified in-browser at 375px with injected big counts (2nd span
#   wrapped, foot within card, no page overflow). Final tag of the mobile-audit
#   sweep (v0.51.3-9); library-table row actions (A2/A3) were skipped per the user.
#   CSS + test.
# 0.51.10: CRT power-ON reveal polish (the user: centre flash good, but the reveal
#   "off and clunky and not good looking"). Two root causes: (1) the shutters eased
#   IN (cubic-bezier 0.4,0,… slow start) after the hold, so the picture crawled open;
#   (2) they retracted as bare black edges — no phosphor, reading as curtains parting.
#   Fix: the shutters now SLIDE apart (translateY, was scaleY) on an ease-OUT
#   (cubic-bezier 0.32,0.62,0.4,1) so the tube springs open, and each shutter carries
#   a soft green bloom on its reveal edge (box-shadow) — because translateY leaves the
#   shadow un-squished (scaleY would flatten it), a glowing green scanline sweeps
#   outward WITH the picture, the authentic warm-up. Centre fold-line flash kept
#   untouched; veil + line timings unchanged (all 0.62s, synced). Verified via
#   freeze-frames (0.11/0.19/0.32s): black→glowing-seam→edges sweeping out. app.css +
#   test (v0.50.91 §3 rewritten scaleY-unfold → translateY-slide+glow).
# 0.51.11: round-4 holistic audit — Batch A (data integrity). #1 (HIGH): CANCEL of
#   an ACCEPT UPDATE / REVERT download now runs the rollback recipe. Extracted
#   Worker._run_rollback_safe → module-level apply_job_rollback (self-guards, opens
#   no conn for recipe-less jobs) and wired it into the worker _JobCancelled +
#   pre-yt-dlp checkpoint AND the API pending-cancel / bulk cancel-pending paths —
#   all four skipped it, stranding the half-applied override/decision (no theme, no
#   !UPD retry). #9: sections.migrate_themes_subdirs_inplace substr off-by-one wrote
#   double-slash paths (+1 → +2). #8: deorphan re-key walker (both resolve loop +
#   merge_orphan_collisions) omitted the jobs table → a pending download at the
#   synthetic id died when the id was promoted mid-window; now re-keys pending/
#   running jobs like sync.py v1.22.87. #3: place_theme mis-read motif's OWN
#   just-placed hardlink as a foreign M sidecar when the placements-row write had
#   failed; samefile() guard falls through to (idempotent) re-place. worker/api/
#   sections/deorphan/placement + behavioral tests.
# 0.51.12: round-4 audit — Batch B, the v1.23.71 client-tab-switch cluster. The
#   in-place (pushState) switcher skipped three things full navigation did
#   implicitly. #14/#18: the 30s library poll + v1.22.36 stuck-row reconciler armed
#   once at DOMContentLoaded and the gate omitted '/collections' — that tab never
#   got either, and a session landing there then client-switching to /movies ran
#   with neither for its whole life (stuck DL/PL chips, the exact v1.22.36
#   pathology); '/collections' added — arming on ANY library landing covers every
#   in-family switch. #15: the filterbar is server-rendered per tab (SRC A/M, LINK
#   HL/C, ED gated on tab != 'collections') and NOT swapped (pill handlers bound
#   once — a swap would drop them, the v0.50.96 class); post-switch the stale
#   drawer offered impossible pills / lost real ones and SRC ALL (DOM-derived)
#   silently excluded A/M rows. Fix at the boundary: collections-crossing switches
#   fall back to FULL navigation; movies/tv/anime (identical filterbar — test-
#   pinned) keep the fast path. #16: selection survived the switch (bulk bar armed
#   with the previous tab's off-screen rows; bulk actions fired at them) —
#   hydrateLibraryStateForTab now clears selected/selectedRows + hides the bar;
#   pill filters/q/sort persistence (v1.23.71 intent) unchanged. app.js + tests.
# 0.51.13: round-4 audit — Batch C (API robustness). #10: cleanup-dead-rk now
#   requires a DEFINITIVE 404 before deleting the placement row — get_themes
#   returns ok=False with http_status=None on transport errors, and the old
#   ok-only gate let a CLEAN UP click during a Plex outage delete LIVE plex_upload
#   placements' rows (orphan_scan classifies any not-ok as plex_fetch_failed).
#   #11: api_upload_theme reads the multipart body in 4MiB chunks with a running
#   50MiB cap (the v1.23.18 OOM-before-cap fix, missed at this endpoint — an
#   accidental multi-GB pick materialized fully in RAM before the cap check).
#   #12: the PURGE/UNPLACE/UNMANAGE/DELETE sidecar+canonical unlink loops are
#   offloaded via run_in_threadpool (class 12 — they hit the /data + themes
#   mounts; a spun-down Unraid disk blocked each syscall serially, freezing the
#   event loop for every concurrent request; invisible to the v1.22.58 AST lint
#   which covers network/subprocess only). All four loops run BEFORE their
#   tracking txns, so no transaction spans the await. #27: PUT
#   /api/dashboard/layout guards its JSON parse (400, was raw 500 — the last
#   bare await request.json() in the file, v0.50.89 class). api.py + tests.
# 0.51.14: round-4 audit — Batch D (Plex/sync correctness). #4: a git first-run/
#   baseline-reset walk (old head None → whole tree 'added', removed=[]) made the
#   changeset drop detector structurally blind to every removal across the reset
#   window; run_sync now routes reset runs to the last_seen-based FULL-WALK
#   detector (its precondition — all survivors' last_seen just refreshed — holds
#   exactly then), excluding failed reads per-item via errored_by_mt, which the
#   git read loop now populates (unresolvable failures skip detection; the
#   errors!=0 no-advance gate keeps the run a reset so it retries). #5: chronic-
#   pin escape — one persistently-malformed upstream blob pinned the baseline
#   forever (errors!=0 → never advance → ever-growing delta); if the SAME
#   baseline yields the SAME failed-path set two runs straight, advance anyway
#   (runtime key git_chronic_read_failures) with a WARNING. #6: the in-place
#   has_theme 1→0 backup-ready detector also fires for walker-staged cloud/TDB
#   backups (local_files source_kind='plex_cloud' / reason='backup_only', no
#   override row) — the Plex-Pass-lapse mode the v1.19.42 pipe was built for;
#   pre-fix only the reaper (DELETE path) matched them. #7: the reaper's tier-2
#   find_theme_sidecar_path ran INSIDE the reap's BEGIN IMMEDIATE txn — a stalled
#   /data mount held the writer lock indefinitely ('database is locked'
#   everywhere); now deadline-bounded (v1.22.65 pattern, 30s no-progress) with
#   abandon-without-join + skip-remaining-after-first-stall. sync/plex_enum +
#   tests (incl. a behavioral dulwich baseline-reset test).
# 0.51.15: round-4 audit — Batch E (silent failures, round 3 — class 9). #21: the
#   v55 recovery walker's pi sidecar stat coerced OSError to False ("file
#   absent") — now None (indeterminate, consumer's .get(rk, False) gate stays
#   conservative) + an INFO breadcrumb + an indeterminate count in the summary
#   line (the v1.21.42 M2 indeterminate-vs-False class). #20: its canonical-stat
#   OSError was a bare continue — a present-but-unstatable file left a permanent
#   silent gap in the one-shot walker; now breadcrumbed (v1.18.7 cold-path rule).
#   #30: apply_pending_restore swallowed ALL OSErrors on the stale WAL/-shm
#   unlink and proceeded to os.replace — directly beneath its own comment that a
#   leftover WAL would replay onto the restored file and corrupt it. Now
#   ENOENT-tolerant; any REAL unlink failure ABORTS the swap (pending snapshot
#   kept for retry, mirroring the safety-backup branch). #31: config save()'s
#   env-un-bake guard had two silent, DIVERGING fault branches — corrupt YAML
#   reset env-bound fields to dataclass DEFAULTS, a non-mapping document skipped
#   the guard (baking the env value, the exact bug it prevents). Both now WARN
#   and uniformly skip the un-bake (env wins this save — deterministic,
#   least-damaging; load() already fails loudly on the same faults).
#   recovery_v55/db_backup/config_file + behavioral tests.
# 0.51.16 — round-4 audit Batch F1: backend security + behavior.
#   #19 auth.py: password rotation revokes every OTHER session (a stolen
#   motif_sess cookie survived rotation for its full 30-day TTL); the
#   caller's own session is kept via keep_session_id, API tokens untouched.
#   #2 worker.py: the v1.24.45 collection downscale had no media_type gate —
#   an over-ceiling movie/TV API push was re-encoded to lower bitrate and
#   uploaded, making the v1.18.69 full-quality sidecar fallback unreachable;
#   movie/TV now skips the downscale AND the doomed POST (v1.21.99 pattern).
#   #23 worker.py: _do_place created its PlexClient ~160 lines before the
#   only finally that closed it — the pre-place cancel checkpoint + any DB
#   raise in the pi-resolution span leaked the client; creation moved to
#   just before the place_theme try. #24 worker.py: _safe_mark's final
#   attempt re-raised into the crash handler (re-pending completed jobs) or
#   killed the worker thread; locked-on-final now logs ERROR + gives up.
#   #25 scheduler.py: sync hour 0 wrapped section_refresh to 23:00 the
#   previous day — with restricted dom/dow it fired ~23h AFTER the sync;
#   hour-0 + restricted days now drops dom/dow (daily at 23:mm).
#   #26 cloud_theme_backup.py: force-walk mints stranded linked-but-empty
#   plex_orphan rows when the download failed/cancelled; targets carry a
#   'minted' flag + unmint_stale_orphans compensates at the run's exits.
# 0.51.17 — round-4 audit Batch F2: frontend behavior.
#   #17 dashboard-customize.js: fetchLayout's silent empty-layout default on
#   a failed GET + exitCustomize's unconditional flush overwrote the user's
#   saved dashboard layout with template defaults (v1.17.13 class; v1.21.43
#   fixed the PUT side only). Load failure now latches LAYOUT_LOAD_FAILED;
#   saveLayout refuses to PUT while latched; enterCustomize retries the GET
#   and refuses entry if it still fails. #13 ops.js: poll() scheduled its
#   successor without clearing the pending timer — every direct
#   motifOps.refresh (visibilitychange, opsHidden kick) forked one more
#   permanent /api/progress loop; poll() now cancels the pending successor
#   at entry + coalesces re-entrant calls (pollInFlight). #28 app.js: six
#   post-action sites called refreshTopbarStatus() immediately — the
#   /api/stats 1s cache + hash-skip froze pre-action topbar state (FAIL
#   count after ACK, DRY-RUN banner after disable) up to 10s; all six now
#   use the class-7 setTimeout(..., 1100). #29 app.js: bulk DOWNLOAD / bulk
#   TDB BACKUP error paths + bulk ADOPT's three early exits stranded their
#   optimistic '// QUEUING …' placeholder after failure (v1.15.35
#   convention); all clear it now.
# 0.51.18 — round-4 audit Batch G: recent-delta CSS + test hygiene.
#   #22 app.css: the v0.50.88/91 running-job topbar shrink protections were
#   ≤600px only — at 601-780px (half-snapped 1440px desktop, iPad portrait)
#   a running job still overflowed the topbar past the viewport; new disjoint
#   601-780px block applies the status-column shrink floor + wrap and the
#   op-mini full-width bottom strip WITHOUT the phone block's two-row
#   re-layout. #33 app.css: the v0.51.10 shutter reveal-edge glow (5px+22px
#   box-shadow) bled ~27px back into the viewport at a plain translateY(±100%)
#   park — a static green band at both poles for ~110ms before the veil fade;
#   shutters now park past the poles (calc(±100% ± 30px)) so the bloom exits
#   with them (the user opted to fix). #32 test_v0_51_6: the desktop-density
#   guard's bare substring asserts matched four unrelated rules (v1.18.81
#   phantom-guard class) — now scoped rule-body extraction.
# 0.51.19 — CRT power-on: the PICTURE unfolds, not just the mask. the user on
#   the v0.51.10-18 reveal: "still feels a bit too much like curtains ... you
#   can really feel like it's something being pulled across" — the page behind
#   the shutters was fully-formed and static, so the moving edge read as a
#   drape (worst over the empty side margins). A real CRT's vertical
#   deflection ramps up: the image starts as a compressed line and STRETCHES
#   open. New style-free #crt-tube wrapper (base.html, banners → footer)
#   squashes to scaleY(0.004) at the fold-line, holds through the centre-line
#   beat, then expands on the shutters' exact duration + bezier, with a
#   white-hot brightness/saturation bloom that settles as the raster opens.
#   Fully inert at rest (:has-gated on .playing); overlays stay outside the
#   wrapper. Verified live: per-frame computed-transform samples 0.004 →
#   0.10 → 0.88 → 1.0, brightness 2.6 → 1.0, transform/filter none at rest.
# 0.51.20 — CRT power-on rebuilt as the exact time-reverse of the power-OFF the
#   user calls "perfect". Iterating on the reveal (v0.51.10-19), the user: the
#   shutters felt "too much like curtains ... something being pulled across"; the
#   v0.51.19 tube-stretch felt like "a conveyor belt" AND its squash shrank the
#   scroll region so Chrome parked the page mid-way, not at the top. The power-off
#   nails it because NOTHING TRAVELS — a black veil fades in while a bright beam
#   blooms then collapses to a dot; pure luminance, zero page motion. Power-on now
#   mirrors it reversed: the veil (.crt-power-on black bg) HOLDS then fades AWAY to
#   reveal the untouched page, and the beam (.crt-on-line) plays power-off's line
#   keyframes backwards — a collapsed dot (scaleX 0) streaks out horizontally into
#   a thin bright line, blooms tall as the picture floods in, then fades. The page
#   never transforms: no scroll jank, no travel, same language as power-off. The
#   v0.50.98/v0.51.10-18 shutters + v0.51.19 #crt-tube wrapper are removed. Verified
#   live (Web Animations sampling): veil 1→hold→0, beam scaleX 0→1 streak, scaleY
#   blooms to 2.6, body transform `none` + scroll untouched throughout.
# 0.51.21 — the // ALL library chip + a wider RECENTLY ADDED carousel (the user).
#   ALL button: on Movies/TV/Anime a new resolution mode that unions the
#   standard AND 4K sections into one view (shown only when BOTH exist), and on
#   Collections combines every managed section's collections at once — reversing
#   the v1.18.18 removal of the collections ALL chip at the user's request. Opt-in
#   (STANDARD / first-section stays the default; choice persists per tab).
#   Backend: new `all_res` param on /api/library + _library_main_query SKIPS the
#   is_4k narrowing when set (collections force section_id=''); _library_resolution
#   _state gains has_both; /api/library/refresh's all_res branch re-enumerates BOTH
#   sections (scope <tab>-all). Carousel: _recently_placed_sync LIMIT 24 -> 40.
#   Verified live: standard=5 / 4K=3 / ALL=8 combined; chip order ALL·STANDARD·4K
#   with STANDARD active by default; click ALL -> 8 rows, "REFRESH ALL MOVIES",
#   persisted 'all', survives reload; collections ALL combines both sections.
# 0.51.22 — // ALL is now the DEFAULT library view (the user: "lets make all
#   be the default"). When ALL is available (both a standard AND a 4K section,
#   or >=2 collection sections) a fresh visit lands on ALL instead of STANDARD /
#   the first section. An explicit ?all_res=/?fourk=/?section_id= URL param
#   still wins, and a persisted per-tab choice (localStorage) overrides for
#   return visits — so picking STANDARD/4K/a-section still sticks. SSR: new
#   _default_all_res(request, tab) resolves the first-paint state (checks
#   has_both / >=2 sections); JS hydrate + init-load + switchLibraryTab
#   pre-apply mirror it via _allChipAvailable, defaulting ALL only when no
#   per-tab choice was ever saved.
# 0.51.23 — amber 4K tier badge next to the title (the user: after // ALL became
#   the default, "if a movie exists in both 4k and standard its impossible to
#   tell them apart at a glance, can we make a amber 4k symbol similar to in the
#   setting section chip that sits next to the title of the 4k library version
#   and in the info card"). /api/library now returns section_is_4k per row
#   (ps.is_4k); renderLibraryRow draws a non-interactive <span.tier-badge
#   .tier-badge-4k>4K</span> after the title name whenever section_is_4k — so a
#   4K row is distinguishable at a glance in the combined // ALL view AND the
#   dedicated 4K section. Both INFO-card render paths draw the same badge: the
#   full card from section_context.is_4k (api_item), the bare no-theme card from
#   the cached row's section_is_4k. .tier-badge-4k reuses the settings
#   .lib-flag-pill-4k amber "premium tier" palette (v1.11.9) with no hover/cursor.
# 0.51.24 — mobile card overflow (the user: "the per section coverage and the
#   general statistics in mobile view are spilling out of bounds of their
#   sections, can we also do an audit to make sure no other sections in mobile
#   view are doing this"). The v1.24.66 .dash-pair 2-up kept both cards flex at
#   ~152px each on a phone, so each 6-7-col compact table (476-599px of nowrap
#   cells) spilled hundreds of px past its card — dropping min-width alone never
#   actually stacked them (a flex item won't shrink below its content's
#   min-content). Fix (@media <=600px): plain block flow so each card is one
#   full-width row + the card becomes its own overflow-x scroll context so the
#   wide table scrolls INSIDE it (the v0.50.86 SYNC HISTORY swipe pattern) +
#   a table min-width floor for readability. The audit also caught SOURCE
#   BREAKDOWN: the v0.51.6 30px touch bump widened the per-donut controls to
#   ~122px, overflowing the ~150px 2-up donut column by ~25px — the label now
#   wraps the controls under the name. All scoped to the phone tier; desktop
#   keeps the flex 2-up equal-height layout (verified live at 375 + 1280). Every
#   other page (movies/tv/anime/collections/queue/settings) audited clean at 375.
# 0.51.25 — dashboard // REFRESH PLEX button locks instantly on click (the user:
#   "Plex refresh button is not locking when clicking on the dashboard and is
#   queued up, it takes a few seconds to show refreshing"). refreshTopbarStatus
#   (the 10s poll that owns the button) set .disabled/.textContent from
#   plexEnumBusy alone, and plexEnumBusy comes from /api/stats which is cached
#   ~750ms-1s + hash-skipped — so for ~1-2s after a click the poll served the
#   STALE pre-click snapshot (plex_enum_in_flight still 0) and UNLOCKED the
#   button the click handler had just locked; it only re-locked once the enqueued
#   enum finally showed up in stats. Fix: the poll now unions the button's OWN
#   click-time optimistic placeholder — plexRefreshing = plexEnumBusy ||
#   hasOptimistic('plex_enum') — the same bridge the hero wave already uses
#   (v0.50.68). hasOptimistic gained an optional kind filter so the union is
#   plex-specific, preserving the v1.14.62 invariant (this button never locks on
#   a TDB sync). Verified live: hasOptimistic('plex_enum')=true /
#   ('tdb_sync')=false. Only the dashboard button touched.
# 0.51.26 — two mobile display fixes (the user). (1) The EXISTING BACKUPS rows
#   are a flex row [name | DOWNLOAD RESTORE DELETE]; the 3 flex-shrink:0 buttons
#   ate the width and crushed the min-width:0 name to ~one char, so
#   word-break:break-all stacked the filename VERTICALLY one letter per line —
#   unreadable. Stack the row on a phone (flex-direction:column) so the name gets
#   the full width above its actions. (2) The horizontal-scroll data regions
#   (v0.51.24 dash-pair cards, the library .table-scroll, sync-history) fell
#   through to the global 10px muted-gray ::-webkit-scrollbar, which read as
#   foreign next to the green header + the nav's thin-green scroll strip (the
#   user: the dash scroll bar "is a different color and doesn't match the rest of
#   the header"). Give them the nav's thin-green treatment so every mobile scroll
#   strip is one visual language. Verified live at 375: name = full 319px row
#   width / one line; dash-pair scrollbar-color = green-deep.
# 0.51.27 — centre the library toolbar's button groups + de-jumble the filter
#   drawer on a phone (the user: centre "all standard 4k and the favorite filters
#   and clear all ... any other that don't fit naturally", and "our filters look a
#   bit jumbled"). @media <=600px: .filterbar + .chips justify-content:center; the
#   resolution + collections-section .chips get width:100% so they centre ON
#   SCREEN not inside a left-hugging shrink box; the preset-star + // CLEAR ALL
#   toolgroup drops its desktop margin-left:auto right-pin. Filter drawer: each
#   axis label (.pill-filter-label) drops its 72px right-aligned column for its
#   OWN centred full-width line (flex-basis:100%, text-align:center) with the
#   chips centred beneath (.pill-filter-row justify-content:center) — no more
#   ragged wrap. Verified live at 375: resolution chips centred (28-347), filter
#   labels centred full-width. .chips is only used in library.html (3 groups), so
#   no collateral. Desktop untouched.
# 0.51.28 — hero wave ramps harder per running job (the user: "increase the hero
#   wave bar intensity a bit more per job running, right now ... the increase [is]
#   pretty subtle"). Two levers: app.js per-job energy step 0.145 -> 0.22 (each
#   extra job lifts --hero-wave-energy more; full energy by ~score 4 not ~6), and
#   app.css energy->visual gain (.hero::after height 0.32->0.44, brightness
#   0.55->0.75, opacity 0.37->0.44; .hero::before cross-weave height 0.26->0.36,
#   opacity 0.32->0.40). Verified live: per-job scaleY delta ~0.04 -> ~0.088, per-
#   job brightness ~+0.08 -> ~+0.15. Still eased (not snapped) + saturates at 1.
# 0.51.29 — info-card audio playback: easier volume + drop the redundant ↓ (the
#   user: "the volume control bar on our info card playback is very hard to see
#   and control, also lets get ride of the little downwards arrow to download
#   since the vertical tripple dots also includes a download option"). app.js:
#   removed the sibling <a download>↓</a> next to the <audio> — the native
#   controls' ⋮ overflow menu already has Download. app.css: with the ↓ gone the
#   <audio> owns the whole play row — .info-audio max-width 480px->none +
#   width:100% + height 32px->40px, so the native volume slider + seek bar get
#   real width/height (the 32px squish was cramping them). Dead .info-play-row>a
#   rule removed. Verified live: play row has one child; .info-audio 40px, fills.
# 0.51.30 — collections chips: // ALL + a section could be highlighted at once
#   (the user: "in collections both All and other sections can become clicked at
#   the same time"). On a full page load bindLibrary's chip hydration toggled each
#   axis piecemeal, but SSR marks // ALL active BY DEFAULT (the server can't read
#   the client's persisted section / STANDARD-4K pick, so a bare /collections or
#   /movies SSRs ALL). When the client then resolved to a specific section (or
#   STANDARD/4K from localStorage), nothing cleared the SSR-ALL chip — so ALL AND
#   the section/resolution chip both showed .chip-active. Fix: an authoritative
#   chip reconcile at the end of the hydration (same shape hydrateLibraryStateFor
#   Tab uses on the in-place tab switch) so exactly ONE chip is active for the
#   resolved allRes/fourk/section_id. Also fixes a latent movies/tv/anime variant
#   where a persisted STANDARD left NO resolution chip lit. Verified live at the
#   resolution chips: default→ALL only, persisted fourk→4K only, standard→STANDARD
#   only (each exactly one active).
# 0.51.31 — dashboard de-dup + SERVICES enrichment (the user: "reducing some of
#   the charts on the dashboard, seems a lot of them now have a lot of duplicate
#   information ... COVERAGE COMPARISON ... any other services worth tracking
#   that motif is using or built off of lets add that to // SERVICES"). Removed
#   the // COVERAGE COMPARISON block — renderCoverageComparison fed it the SAME
#   /api/sections/coverage per-section themed/unthemed split the // PER-SECTION
#   COVERAGE table already shows, so it was pure duplicate clutter. Deleted the
#   template section, the JS fn + call, and the now-dead .coverage-row/.coverage-
#   bar CSS (their sole builder); the surviving PER-SECTION table uses
#   .section-coverage-row and is untouched. Reconciled the historical dead-CSS /
#   motion-token guards (v1.14.61 flip to co-removed; v1.20.47 drop from LIVE).
#   SERVICES gained ThemerrDB (the upstream catalogue — always shown, probes the
#   configured git source's smart-HTTP refs endpoint for reachability + names the
#   active transport) and TMDB (orphan id resolution — carded only when an api
#   key is configured, probed against /3/configuration). Both mirror the Plex
#   short-timeout probe, run off the event loop, and keep their credentials
#   server-side. Audited the other dashboard cards: COVERAGE vs PLEX LIBRARY
#   (themed% vs TDB-reach) + GENERAL STATISTICS vs PER-SECTION COVERAGE (distinct
#   columns) are intentionally different, not dups — left as-is.
# 0.51.32 — INFO card → LEFT overlay drawer (the user: "make the info card into a
#   left drawer instead of what it is currently so it's more in theme with the
#   status bar drawer, would like it have everything it current has in it but a
#   drawer instead"). The // MOTIF INFO card stays a native <dialog> — showModal()
#   keeps the modal scrim, Esc-to-close, focus trap, and ALL the v1.12.x open/close
#   + audio-teardown machinery intact — so only the LAYOUT + entry animation
#   change. A new `dlg-drawer-left` modifier class (base.html) restyles it into a
#   left-pinned, full-height sliding panel that mirrors the right-pinned
#   .ops-drawer: inner-edge (right) green-deep border + page-ward drop shadow,
#   slide in from -X reusing the proven .dlg @starting-style pattern (transform +
#   opacity over the ops-drawer's 0.28s cubic-bezier), a sticky ops-drawer-style
#   head, a scrolling body, and an ops-drawer-matched scrim (::backdrop rgba black
#   0.45, no blur). `display:flex` is gated on [open] so the UA hides it when
#   closed. Added scrim-click-to-close (routed through closeInfoDialog for the
#   audio/focus teardown) to match the ops-drawer. Scoped to .dlg-drawer-left so
#   every OTHER .dlg (glossary, upload, manual-URL) stays a centered modal. The
#   allow-discrete slide-OUT was dropped (it froze the entry transform mid-flight
#   in the target browser); close is instant like every other .dlg. Live-verified:
#   info-dlg computes position:fixed left/top 0, 440px full-height, green-deep
#   border-right, display:flex, translateX(0) on-screen; sticky head at y0, body
#   scrolls; the other dialogs unaffected.
# 0.51.33 — code-review follow-ups on the v0.51.31/32 pair (10-angle review; no
#   crashers/data-loss found — all in the two just-shipped tags). (1) The
#   // SERVICES ThemerrDB card was honest-ified: it probes the GIT SOURCE repo's
#   smart-HTTP refs endpoint specifically, but labeled the card "via {sync.source}"
#   as if it health-checked whichever transport (remote→db_url / database→
#   database_url each hit a different host) — misleading whenever source != git.
#   Now the status says "git source · Nms" (what's actually probed) and the
#   transport shows as a separate "sync: X" note. (2) Non-http(s) git_url (git@ /
#   file://, valid for dulwich, settable via MOTIF_DB_GIT_URL bypassing the
#   config-file scheme validator) → new `probeable` flag → card shows "git source ·
#   not probed" instead of a false "unreachable". (3) Dropped follow_redirects from
#   the probe (Plex probe it mirrors lacks it; avoids trailing an operator git_url's
#   30x to an arbitrary host). (4) INFO drawer goes FULL-SCREEN on ≤600px again
#   (the user's pick) — the drawer's .dlg.dlg-drawer-left (0,2,0) had silently
#   out-specificity'd the v0.50.50 mobile full-screen .dlg rule (0,1,0), so a phone
#   got a cramped 92vw drawer; re-stated at drawer specificity in a mobile @media.
#   (5) CSS cleanup: deleted a duplicate @starting-style block (two-step-edit
#   debris), dropped a redundant border-bottom re-declaration on the drawer head,
#   and extracted a shared --drawer-scrim token so the ops-drawer + info-drawer
#   scrims stay in sync by reference not by two copies. Serial-probe latency (~15s
#   worst case, flagged) left as-is: acceptable for a 30s dashboard poll where 0-1
#   deps are usually down; revisit with concurrency if it bites.
# 0.51.34 — widen the INFO left drawer back to 720px (the user: the 440px drawer
#   "feels a bit narrow"). v0.51.32 shipped the drawer at 440px, but the info
#   card's content (poster hero + `140px 1fr` diff/detail grid + sections) was
#   laid out for the 720px it had as a centered modal (base .dlg max-width), so
#   440px cramped it. Restored width:720px (max-width:92vw still caps narrower
#   desktops; mobile stays 100vw full-screen per v0.51.33). CSS one-liner.
# 0.51.35 — ops/status drawer no longer unsticks the topbar (the user: with a
#   drawer open "you can see the top that's usually behind the bar ... you can
#   suddenly see buttons"). The v1.21.30 LIVE-OPS background scroll-lock
#   (`html.ops-drawer-locked { overflow: hidden }`) made <html> a clamped scroll
#   container, which broke the position:sticky .topbar — it fell to its flow
#   position off the top of the viewport, exposing the page hero + its buttons
#   behind the open drawer. Removed the lock (class add/remove in ops.js + the CSS
#   rule); kept the v1.21.30 `scrollbar-gutter:stable` anti-shift halves, which
#   alone cover the Windows double-scrollbar case (the page scrollbar hides behind
#   the right-pinned drawer panel anyway). The INFO drawer (a native <dialog>
#   modal) never used this lock, so it was already unaffected — verified the
#   sticky topbar stays put with either drawer open. CSS/JS one-liners + guard
#   test flipped (test_v1_21_30).
# 0.51.36 — a backup-only (TB) row no longer paints as "awaiting placement" (the
#   user: DOWNLOAD TDB BACKUP "leaves it an amber state PL row thinking that it
#   needs to be placed when it was just downloading the backup"). Contract-drift
#   (bug class 9): the v1.19.21 `backup_only` terminal state was added to the WRITE
#   side (worker stamps last_place_attempt_reason='backup_only') + the retry-sweep
#   skip-list, but the READ-side "awaiting placement" predicates were never taught
#   to exclude it, so a finished backup row still lit the amber PL dot + amber "!"
#   glyph + matched the AWAIT/attention filters. Excluded backup_only at all FOUR
#   drift sites (mirror-drift): JS awaitingApproval (app.js — the visible dot +
#   glyph), _LIB_AWAIT_SQL (api.py — attn filter + count), the pl_pills=await SQL
#   branch, and the _row_matches_attn await branch. Display/filter only — no data
#   mutation. Behavioral test seeds a backup_only row + a genuine-await row and
#   asserts only the latter matches pl=await / attn=await. (Diagnosed from the
#   user's prod row via a 3-agent read-only trace; the SRC T→P-on-UNMANAGE +
#   "DOWNLOAD PLEX BACKUP no-ops" + info-card items from the same report are
#   plex_independent_theme / force-capture-UX questions, flagged separately — not
#   touched here to avoid the phantom-P edition-scope minefield.)
# 0.51.37 — follow-ups on the v0.51.36 confusing-row report (the user OK'd both).
#   (1) INFO card no longer says "(none — row has no theme staged)" for a row
#   where Plex IS serving a theme motif no longer manages — e.g. right after
#   UNMANAGE of a plex_upload row (motif untracks it but Plex keeps serving the
#   uploaded theme; landing at P is by-design since v1.20.57). The card's
#   Plex-serving branch only fired on plex_independent_theme=1, which plex_enum
#   sets on its NEXT cycle, so the post-UNMANAGE window fell to "(none)". api_item
#   now also surfaces plex_items.has_theme (plex_has_theme) so the card names the
#   real state + the recovery paths ("Plex is serving a theme motif no longer
#   manages — SOURCE → RE-DOWNLOAD TDB to take it over, or REMOVE → PURGE to clear
#   it"). NOTE: the SRC=P (vs the user's expected "T +P") is UNMANAGE working as
#   designed — it removes motif's theme, so there's no T; plex_independent_theme
#   self-heals to a formal +P on the next plex_enum. Display-only, no data
#   mutation. (2) DOWNLOAD PLEX BACKUP force-capture confirm leads with WHY the
#   strict run found nothing (Plex serving a motif upload, not a Plex Pass cloud
#   theme) so it doesn't read as "the action did nothing" (the user).
# 0.51.38 — centre the // RESULTS action group on a phone (the user: "get last on
#   the same row as the other options or if not do centering similar to what we
#   did in the filter section above ... same for legend and needs work"). The
#   LEGEND / NEEDS WORK / pager group now spans full width + centres on-screen and
#   the pager centres its own buttons, so a `last »` that can't share the row
#   centres as its own line instead of stranding left. Mirrors the v0.51.27
#   filterbar treatment; scoped to @media (max-width: 600px); .block-head-actions
#   is library-only so LOGS/queue block-heads are untouched. CSS-only.
# 0.51.39 — mobile: DB-backup + DRY-RUN button rows no longer overflow (the user:
#   "the database recovery buttons spill off the screen to the left ... stack them
#   centered ... audit for other places"). (1) .backup-row-actions (DOWNLOAD /
#   RESTORE / DELETE) had `justify-content: flex-end` + NO flex-wrap, so at ~361px
#   it overflowed and — right-anchored — spilled off the LEFT (`// DOWNLOAD` clipped
#   to `/ DOWNLOAD`). Now wraps + centres like the v0.51.38 pager. (2) Audit found
#   one sibling: the DRY-RUN .control-actions (ENABLE DRY-RUN + DISABLE) was nowrap,
#   its ~335px min-content forcing the whole settings page to ≥373px wide (a
#   horizontal page overflow < that). Added flex-wrap; kept left-aligned under its
#   label (v0.51.7), NOT centred, since centring would detach it from the label.
#   Every other action row already wraps/centres (v0.51.7/.27/.38) or scroll-
#   contains (library .row-actions in the min-width table). CSS-only.
# 0.51.40 — the // MOTIF INFO left drawer slides AWAY on close (the user: "the
#   info card drawer doesn't have the same slide away or retract effect like our
#   status bar drawer can we add that so it matches"). v0.51.32 slid it IN but
#   close()d instantly (the allow-discrete slide-OUT froze mid-transition). Now it
#   mirrors ops.js closeDrawer: closeInfoDialog adds .is-closing (transform
#   slide-out + scrim fade), then delays dlg.close() by the 0.28s transition.
#   Reopening mid-retract cancels the pending close (showModalNoFocusRing) so
#   showModal() never hits an [open] dialog; Esc routes through the same slide via
#   a cancel-event interceptor. Audio/focus teardown unchanged.
# 0.51.41 — the library RESULTS loading state uses the branded record-spinner (the
#   user: "when there is a loading screen for the results can we have the spinning
#   logo loading screen we used for our info card"). Both placeholders — the JS
#   re-fetch one (loadLibrary, gated on lastHash==null) + the template first-paint
#   one (#library-body) — now render recordLoaderHtml's .record-spinner instead of
#   a bare "loading…" (colspan 10→11 so it centres under the full table). The SVG
#   is inlined in the template for SSR first paint; test_v0_51_41 pins it identical
#   to the JS helper so they can't drift.
# 0.51.42 — code-review fix on v0.51.37: api_item resolved plex_independent_theme
#   + has_theme with a COUPLED gate (`_pi_independent is None AND _pi_has_theme is
#   None`). has_theme is NOT NULL, so a tier-1 rating_key hit always set it, which
#   short-circuited the section/global MAX fallback for a NULL plex_independent_
#   theme (a sidecar row) — the info-card +P label regressed 1 → (none) for a
#   multi-edition title whose queried edition's flag was NULL but a sibling's was
#   1. Now each tier runs if EITHER flag is unresolved + fills only the column
#   still None, so independent resolves off siblings again while a resolved rk
#   value is never clobbered. Behavioral test + a decouple source guard.
# 0.51.43 — the mobile section-nav scroll track no longer hides the active tab's
#   colored underline (the user: scrolling DASHBOARD/MOVIES/TV/ANIME/COLLECTIONS
#   "you can't see the colored bar since the scroll bar covers it"). The ≤600px
#   nav is a horizontal-scroll strip with a 3px green track (v0.51.3); the active
#   tab underline is a 2px border-bottom at the same bottom edge, and macOS overlay
#   scrollbars draw OVER the box (no reserved space) so the track covered it. Added
#   padding-bottom:6px to the mobile nav so the underline sits above the track —
#   both visible. CSS-only, scoped to ≤600px (desktop nav unchanged).
# 0.51.44 — the login card head shows the topbar MOTIF wordmark + synth equalizer
#   (the user: "instead of the top bar saying authenticate ... make it say MOTIF
#   with the synth like in the top bar"). login.html's .auth-card-head reuses the
#   real .brand-mark bars (the 5 animated .brand-bar synth bands) + a MOTIF
#   wordmark instead of ▰ + "AUTHENTICATE"; .auth-brand-mark scales the EQ to the
#   26px title + .auth-title uppercases the name. Login-only (setup.html keeps
#   ▰ FIRST RUN). Template + CSS only.
# 0.51.45 — // SYNC PERFORMANCE + // DOWNLOAD ACTIVITY sit side by side as a 2-up
#   pair (half width each) instead of a full-width row each (the user). Wrapped in
#   a .dash-pair, mirroring the v1.24.66 STATISTICS pair — pure template change,
#   reuses all the existing .dash-pair / .dash-pair-col CSS (2-up desktop, stacks
#   on mobile, customize-mode flex). The wrapper is the single customize unit
#   (reuses the insight-syncs layout key); each inner .block keeps its id for the
#   JS chart render + its own SSR display gate.
# 0.51.46 — topbar mini-bar: SYNC THEMERRDB now outranks REFRESH PLEX in the single
#   contended slot (the user: clicking both back-to-back "showed plex is refreshing
#   then bumps to themerrdb" — the quick refresh grabbed the slot at priority 2,
#   then handed off to the longer sync when it finished). Swapped tdb_sync (now 2)
#   above plex_enum (now 3) in ops.js OP_MINI_PRIORITY + the api.py _topbar_ssr_state
#   CASE (lockstep). Download still tops both. Reverses the v1.15.82 refresh>sync
#   order for this pair only.
# 0.51.47 — INFO-card audit fixes (4-agent read-only sweep). (1+2) EDITION BLEED:
#   the recovery-options RE-DOWNLOAD + REVERT handlers dropped rating_key (passed
#   only section_id), so on a multi-edition-in-one-section title they hit the
#   legacy section-wide path instead of the clicked edition — now thread rating_key
#   like the row SOURCE-menu equivalents (v1.21.64/.73) + the recovery-PURGE. (3)
#   COLLECTIONS: api_recovery_options mapped media_type='collection' → plex_mt
#   'movie', so all 3 rk-pick queries (incl. the clicked-rk one) missed the
#   collection's plex_items row → rating_key None → SET URL / UPLOAD MP3 recovery
#   buttons couldn't wire up; now `else media_type` like api_item. (4) consistency:
#   api_item's edition-label folder_path query used the same hardcoded-movie map →
#   aligned to _info_plex_type. Rest of the audit (open/render, lifecycle) clean.
# 0.51.48 — collection-safe plex_items media_type maps: the sequel to the v0.51.47
#   INFO-card audit. Eight more handlers derived a plex_items filter as `"show" if
#   media_type == "tv" else "movie"`, so a themed COLLECTION (media_type=
#   'collection', placed via plex_upload) matched media_type='movie' — an empty set
#   — and its plex_items rows were silently missed. Six are live-reachable (PURGE/
#   forget, DELETE, DEL-unplace, UNMANAGE restat, clear-failure owner-sections,
#   post-PURGE plex refresh); two are the api→file placement-transition branches
#   (REPLACE + SWITCH), movie/tv-only in practice (SWITCH 400s on collection, no
#   sidecar for a folderless collection) but aligned for uniformity. All eight now
#   `else media_type` like _info_plex_type. Source guard (test_v0_51_48) pins the
#   media_type-keyed hardcoded-movie shape out of api.py + each swept handler.
# 0.51.49 — the 9th collection→movie plex_items miss, the mt-keyed sibling of the
#   v0.51.48 sweep. The bulk LET PLEX SERVE helper (_bulk_lps_run) mapped
#   `"show" if mt == "tv" else "movie"` (mt = a theme row's media_type, so it
#   reaches 'collection') into a plex_items (guid_tmdb, media_type) lookup — so a
#   themed collection in a bulk-LPS selection matched media_type='movie', an empty
#   set, and its local_theme_file/has_theme flag-clear silently no-op'd (stale
#   SRC/PL after the op). v0.51.48's enumeration was media_type-keyed, so this
#   mt-keyed one slipped through. Now `else mt`. Source guard gains an mt-keyed
#   global tripwire + a _bulk_lps_run pin; the tab-keyed plex_sections.type map
#   (~6926) stays `else "movie"` on purpose (no 'collection' library section).
# 0.51.50 — RECAPTURE FROM PLEX: an honest one-click action for a row where Plex
#   is serving a theme motif itself uploaded. Post-UNMANAGE (the user's repro) the
#   row lands at bare P with Plex still serving motif's own upload:// entry.
#   DOWNLOAD PLEX BACKUP only captures Plex-Pass metadata:// CLOUD themes, so its
#   strict walker found nothing and dropped into a confusing "found nothing →
#   capture anyway?" confirm (the user: "you get the option but it never
#   downloads"). Now the /api/library payload carries plex_items.plex_theme_uri;
#   the SOURCE menu offers RECAPTURE FROM PLEX for an upload:// serving theme,
#   going STRAIGHT to the one-step force-capture (positive-leading confirm) that
#   actually stores the bytes. A metadata:// (or NULL not-yet-enumerated) uri keeps
#   DOWNLOAD PLEX BACKUP. Behavioral test pins plex_theme_uri into the payload.
# 0.51.51 — straight-to-capture for the per-row DOWNLOAD PLEX BACKUP; reverts the
#   dead v0.51.50 RECAPTURE gate. A prod-DB audit (the user's UNMANAGE/upload://
#   row) showed v0.51.50 gated on plex_theme_uri.startsWith('upload://'), but
#   plex_items.plex_theme_uri is ALWAYS the /library/metadata/{rk}/theme/{ver}
#   association url — never the upload://<sha> entry key (6,379/6,379 rows; no
#   scheme column exists at all), so RECAPTURE never rendered. The scheme is only
#   knowable via a per-row /themes call motif doesn't persist. So instead of a
#   render-time relabel, the per-row DOWNLOAD PLEX BACKUP click now goes STRAIGHT
#   to force-capture (capture whatever Plex serves — upload:// or metadata://
#   alike), non-destructive for a bare-P row, confirming only on the swap case
#   (a non-plex_cloud local file that would be REPLACED). The strict-then-"found
#   nothing → capture anyway?" two-step is gone. Bulk DOWNLOAD PLEX BACKUP stays
#   the strict C1-only Plex-Pass-loss sweep. plex_theme_uri reverted out of the
#   /api/library payload (nothing reads it now).
# 0.51.52 — RECENTLY ADDED carousel posters eager-load instead of lazy. the user:
#   they "sometimes don't load right away ... load in late, looks a bit off". Each
#   of the ~40 posters was loading='lazy', so off-screen tiles didn't fetch until
#   scrolled near — and with auto-scroll ON by default the strip cycles through
#   all of them anyway, so lazy just spread the SAME art-proxy fetches across the
#   scroll (the pop-in). Now eager + decoding='async', with the first ~8 (initial
#   viewport) at high fetch priority; the art proxy's 1-day Cache-Control makes it
#   a one-time cost per browser. No layout shift — the tiles already reserve the
#   2:3 poster box with a placeholder background.
# 0.51.53 — reverse-direction plex_items.media_type -> motif collection fix (code
#   review follow-up). The v0.51.47-49 forward sweep + guard only covered the
#   FORWARD map (motif -> plex_items); the review found 5 REVERSE sites
#   (plex_items -> motif) with the identical collection->movie collapse in a
#   spelling the forward grep couldn't see. The LIVE one: api_upload_theme minted
#   a themeless collection's themes row at media_type='movie' (NULL guid_tmdb
#   skips the theme-match branch -> hits the mint), orphaning it from the
#   /api/items/collection endpoints (INFO card / REVERT / PURGE). The other 4
#   409 before the ternary for collections (defensive). All 6 reverse conversions
#   now route through a single _motif_media_type helper (show->tv, movie/
#   collection identity); the v0.51.48 guard gains a reverse tripwire so the
#   'gone everywhere' claim is finally true.
# 0.51.54 — notification Source line colours its dot by SOURCE, not platform.
#   the user: "colour the dot by the source — purple for a user url/upload, green
#   for themerrdb, cyan for adopted ... at a glance show where it came from." Was
#   per-platform (YouTube-red) since v0.50.37. The dot now sits next to the
#   provenance label and follows the SRC pill palette (T green / U violet / A
#   cyan->blue / M magenta->brown / P amber — Discord ships only solid colour
#   circles, so cyan/magenta fall back to the nearest); the platform reads as
#   plain text + renders its real preview card from the URL. The dot also shows
#   on url-less sources (adopt / manual / upload) now, since it's a SOURCE signal.
# 0.51.55 — the branded record-spinner (the library "results section" loader) now
#   shows on the JOBS log + EVENT STREAM first load too (the user: "anywhere we
#   say loading, include the spinning animation"). loadQueue paints
#   recordLoaderHtml into #jobs-body + #event-stream-full on the empty state
#   (lastHash null) instead of the SSR "loading…" text / empty list; the events
#   loading row spans the 4-col grid via .event-stream-loading so it centres.
# 0.51.56 — code-review follow-ups. (1) worker: the plex_upload place log said
#   "Uploaded collection theme" but that API path serves movies/TV too (v1.23.65)
#   — reworded to "Uploaded theme to Plex (API)" (media_type field already carries
#   the type). (2) RECENTLY ADDED carousel: dropped fetchPriority='high' on the
#   first 8 posters — at default image priority they don't contend with the
#   dashboard's data XHRs (/api/stats, /api/insights); still eager-loaded. (3)
#   cloudBackupForceCapture: clear the optimistic '// QUEUING PLEX BACKUP'
#   placeholder when waitForOp is unavailable, so a successful start can't leave
#   it hanging with nothing to poll it to done.
# 0.51.57 — LET PLEX SERVE confirm honesty for PU rows (the user). The SOURCE-menu
#   LPS confirm always said "DELETE motif's theme.mp3 from the Plex folder", but a
#   plex_upload (PU/cyan) row has NO sidecar file in the media folder — motif POSTed
#   the theme to Plex's own theme list (v1.18.36). The line is only true for a
#   hardlink/copy (HL/CP) row. letPlexServeFlow now branches the removal bullet on
#   the row's placement_kind (same signal as the LINK badge): PU → "REMOVE motif's
#   uploaded theme from Plex … no file in the media folder"; HL/CP → the original
#   folder-delete line. Mirrors the honest state-branched copy v1.20.57 already gave
#   DEL/UNMANAGE/PURGE. Confirm-text only; no handler behavior changed.
# 0.51.58 — INFO card layout cleanup (the user: the play bar + text "going so far to
#   the right", the thumbnail "feels off centered", "everything feels askew"). Two CSS
#   changes, no DOM/JS: (1) .info-audio capped at max-width:340px (was max-width:none /
#   full-bleed) so the native player is a tidy control in the value column instead of a
#   bar stretching the whole 1fr — width:100%+flex still fill up to the cap and shrink on
#   phones; (2) .info-source-thumb-wrap left-aligned (margin:0, was 0 auto) + trimmed
#   480->360, and .info-thumb-caption text-align:left — so the SOURCE PREVIEW header,
#   thumbnail + caption all hang off the card's left content edge like the grid above
#   (one alignment axis, not a third centered one). 4:3 aspect + YT/SC parity unchanged.
# 0.51.59 — INFO card thumbnail re-centered (the user, after seeing v0.51.58: "I don't
#   mind the smaller thumbnail but I prefer it centered"). Keeps v0.51.58's play-bar cap
#   (.info-audio 340px) + the trimmed 360 preview, but restores .info-source-thumb-wrap
#   margin:0 auto + .info-thumb-caption text-align:center — so the source preview is the
#   original centered look, just smaller/less dominant. The v0.51.58 left-align lived one
#   tag. CSS-only.
# 0.51.60 — INFO card play bar matched to the thumbnail length (the user: "make it so the
#   playback is the same length as the thumbnail"). .info-audio max-width 340->360 so the
#   player cap equals .info-source-thumb-wrap's max-width. Bonus: at 360 the value-column
#   player (x~174) and the centered 360 thumbnail (x~180) also land at nearly the same
#   left edge, so they read as aligned too. CSS-only; keep the two caps in lockstep.
# 0.51.61 — INFO card detail polish (the user, 3 bundled tweaks). (1) .info-audio
#   color-scheme:dark + accent-color green so the native player renders as a dark
#   control, not a bright-white pill clashing with the CRT card. (2) .dlg-grid dd
#   overflow-wrap:anywhere (was word-break:break-all) so paths/URLs break at cleaner
#   points ("theme.mp3" stays whole vs "theme.mp"+"3"). (3) dropped the redundant
#   (youtube)/(soundcloud) platform tag from the themerrdb/applied/previous url labels
#   (repeated 3x, wrapped each to 2 lines; the value shows the full URL). The (pending)
#   / (was:) / dead tags + value-side src provenance are kept. Reverses v1.14.20's M2
#   parens-harmonization (its test updated).
# 0.51.62 — INFO card detail rows grouped into labeled sections (the user, chose
#   IDS / LINKS / TIMELINE / ON DISK). renderInfoCard's flat ~17-row <dl> is split into
#   four .dlg-section.info-group blocks reusing the .dlg-section + <h4> primitive (same
#   as SOURCE PREVIEW). Rows built as consts (_idsRows/_linksRows/_timelineRows/
#   _onDiskRows — same nesting depth as the old grid, conditional sub-templates
#   unchanged) + a _grp(title,rows) helper that renders a group only when non-empty, so
#   a metadata-only row's empty ON DISK leaves no dangling header. Row order within each
#   group unchanged. CSS: .info-hero + .info-group drops the first group's redundant
#   divider (hero already separates it). CSS+JS only, brace/paren delta 0.
# 0.51.63 — a library-section refresh no longer locks the /collections REFRESH button
#   (the user). The button gated on the global plex_enum_in_flight (ANY enum), so a
#   plain movies/tv/anime section refresh locked it. Fix: /api/stats surfaces a
#   COLLECTIONS-scoped count (plex_enum jobs tagged scope='collections' — a section
#   refresh tags scope='movies'/'tv'/'anime'), and the button's myTabBusy gates on THAT
#   at both JS sites (refreshTopbarStatus + updateLibraryRefreshBtnLabel). Stays true
#   through the collection refresh's reconcile stage (reconcile is a stage inside the
#   plex_enum job) so it doesn't re-enable mid-scan (the v0.50.14 concern); a global
#   scan_all/cascade still locks it via pipelineBusy. Removed the now-dead
#   __motif_plex_enum_busy stash.
# 0.51.64 — INFO card group labels reverted to the original demo names (the user:
#   "I actually like the other section labels ... the ones we first had shown in the
#   demo"). IDS/LINKS/TIMELINE/ON DISK → IDENTITY/SOURCE/HISTORY/FILE & PLACEMENT
#   (the 4 _grp() render-call titles). The row-const names + membership + order are
#   unchanged. _grp now htmlEscape's the title so the '&' in "file & placement"
#   renders as a literal ampersand. Label-only.
# 0.51.65 — code-review follow-up to v0.51.63: the body-absent /api/library/refresh
#   (legacy "scan everything", incl. collections) enqueued an UNTAGGED plex_enum
#   (payload '{}'), which matched neither the collections-scoped count nor the
#   scan_all/cascade pipeline signal — so a raw-API global refresh left the
#   /collections REFRESH button clickable mid-scan. Now tagged scope='scan_all' (a
#   full scan, like /api/libraries/refresh), so the existing pipeline lock covers it.
#   Not UI-reachable (the button always sends a concrete tab) — closes the
#   supported-endpoint hole generically. Backend one-liner + behavioral test.
# 0.51.66 — collections/library polish sweep: fix stale empty-state copy that named
#   a button that no longer exists. Two user-facing messages told the user to "click
#   REFRESH FROM PLEX" — but the library/collections refresh button was renamed to
#   "// REFRESH COLLECTIONS"/"// REFRESH MOVIES"/… back in v1.13.63/v1.18.4 ("REFRESH
#   FROM PLEX" survives only as an internal comment shorthand). The library-table
#   empty-state (loadLibrary, renders on /collections when a section has no rows) now
#   reads "click the // REFRESH button above"; the jobs-queue empty-state names both
#   buttons by their real "// "-prefixed labels (// SYNC THEMERRDB, // REFRESH). Copy-
#   only, no test pinned the old strings. Static-text guard added.
# 0.51.67 — code-review follow-up to v0.51.66: the v1.11.27 comment above the
#   library empty-state still quoted the "click REFRESH FROM PLEX" instruction that
#   v0.51.66 removed from the else-branch below it (the enum-active branch shows a
#   'scanning now' cue instead of that static prompt). Updated the quote to "the
#   static // REFRESH instruction" so the comment matches the code it describes.
#   Comment-prose only.
# 0.51.68 — complexity/regression audit fix #1 (the dominant "mirror-drift" class):
#   the AWAIT predicate ("downloaded canonical, no placement, awaiting a PLACE") lives
#   in FOUR copies that must stay byte-equivalent but drifted. _LIB_AWAIT_SQL (the
#   canonical) excludes BOTH terminal reasons (backup_only v0.51.36 + over_ceiling
#   v1.24.46), but: the attn post-stat fallback _row_matches_attn had only backup_only
#   (LIVE bug — an over-ceiling row re-appeared as AWAIT + re-enqueued a doomed upload
#   whenever attn=await+broken were multi-selected, the path the v1.24.46 guard never
#   hit); the pl=await SQL branch had only backup_only; _row_matches_pl had NEITHER.
#   Added the missing exclusion STRINGS to all three (no refactor into a shared helper
#   — the audit's low-risk guidance + CLAUDE.md "no premature abstraction"). New
#   behavioral guard exercises all four surfaces (proven to fail without the fix).
# 0.51.69 — complexity/regression audit fix #2: additive completeness guards for two
#   unguarded "mirror" surfaces (defense against the fix-one-break-another class — pure
#   test additions, no runtime change, currently in-sync). (a) notify._EVENT_NOTIFY_TYPE
#   severity map — assert every dispatchable config_file._DEFAULT_NOTIFY_EVENTS kind has
#   an explicit severity (it drifted twice, v1.23.62/v1.24.26, rendering warnings as
#   neutral info); runtime-import check. (b) server _pset pill whitelist vs JS pillAxes
#   — the tdb/dl/pl/link/ed axes had no client<->server set-equality guard (only attn
#   did, test_v1_15_23), so a UI token forgotten in _pset silently shows ALL rows; regex
#   on stable literals, per-axis set-equality, proven to catch a dropped token. Robust
#   checks, NOT brittle char-window source-pins.
# 0.51.70 — complexity/regression audit fix #3: two safe cleanups. (a) COLSPAN — the
#   library fetch-error row (loadLibrary catch) used colspan=10 against the 11-column
#   header; its loading + empty siblings were 11. The drifted third row left a trailing
#   empty cell on the red error banner. Fixed + extended test_v1_18_86's colspan guard.
#   (b) DEAD CSS — deleted the ~79-line Scans-page block (.kind-*/.scan-filter*/.kpi*/
#   bare .status-*/.scan-summary). Its emitting UI was removed v0.50.89; verified ZERO
#   template/JS emitters (and orphans.html — a DIFFERENT live subsystem — never used
#   .kpi, so test_v1_18_61's "/admin/orphans" docstring was stale). v1.18.61 had wasted
#   a token-remap commit maintaining it. Removed the two now-obsolete CSS-text-pin tests
#   that indexed the deleted selectors; left a breadcrumb + the live mismatch guards.
# 0.51.71 — complexity/regression audit fix #4: additive parity guard for the SRC-letter
#   source-video-id prefix family (mirror-drift). The "user-URL not adopt" test keys on a
#   GROWING prefix family (sc- v1.14.0, ig- v1.20.42, fb- v1.22.90) that must be mirrored
#   in _src_letter_sql (SQL) AND all three computeSrcLetter svid-classification blocks
#   (JS); the v1.22.90 FB add had to touch both sides by hand, with no guard. New test
#   extracts the family from the stable motif-specific literals (SQL LIKE 'xx-%', JS
#   svid.startsWith('xx-')) and asserts SQL == every JS OR-chain. Robust (specific
#   anchors, tokens found nowhere else), proven to catch a one-sided add. Test-only.
# 0.51.72 — real bug found while wiring the CI lint gate (ruff F821). api_sync_probe's
#   nested _probe_remote/_probe_database call httpx.Client(...) but httpx was never
#   imported in that function or module-level (it's imported LOCALLY in 6 OTHER api.py
#   funcs). So the settings-page "test connection" for the remote + database sync
#   transports raised NameError, swallowed by the call-site `except Exception` into a
#   bogus "NameError: name 'httpx' is not defined" — the probe reported the transport
#   unreachable even when fine (CLAUDE.md silent-defensive-catch class). git transport
#   was unaffected (imports dulwich locally). Fix: import httpx in api_sync_probe.
#   Behavioral test (probe a dead port → connection error, NOT NameError) proven to fail
#   on remote+database without the fix. This is exactly why the CI gate (next) matters.
# 0.51.73 — CI quality gate (the "would a staff engineer approve" ask). Pre-fix the only
#   GitHub Actions workflow (release.yml) just built the Docker image on tag — the 7,000+
#   tests, ruff, mypy, pip-audit ran NOWHERE automatically. New .github/workflows/ci.yml
#   runs on every push/PR (py3.12, matching the Dockerfile) with TWO blocking gates: the
#   full pytest suite + ruff's pyflakes-correctness rules (F minus 4 noise rules —
#   clean today, the family that caught the v0.51.72 httpx bug) and THREE report-only
#   signals (full ruff, mypy, pip-audit CVE scan). + requirements-dev.txt (test/tooling
#   deps, kept out of the runtime image) + ruff.toml (target py3.12). Every CI command
#   validated locally (ruff gate clean, pip-audit = 0 CVEs, yaml valid); GHA itself can't
#   be run here so the first live run may need a tweak. Guard test pins the structure.
# 0.51.74 — security audit fix (SSRF, medium). ThemerrDB (public, community-editable)
#   supplies youtube_theme_url values motif stores verbatim + feeds to yt-dlp; the
#   video-id gate used the UNANCHORED _YT_VID_RE, so http://169.254.169.254/embed/<11>
#   minted a fake id and reached yt-dlp's generic extractor → blind SSRF (server GETs to
#   cloud-metadata/LAN/localhost, unattended on cron sync). Added is_fetchable_theme_url()
#   — an http(s)+host-allowlist gate on the 4 supported platforms — at BOTH yt-dlp
#   chokepoints (probe_youtube_url + download_theme). Additive; does NOT touch the fragile
#   _YT_VID_RE/url_source/app.js-mirror path, and the 4 platforms are the only sources the
#   app supports so no legitimate URL is rejected. Unit + behavioral tests (probe/download
#   reject the SSRF URL without touching the network).
# 0.51.75 — security audit fix (credential leak, medium). A credential-bearing sync
#   URL (git PAT / basic-auth pw in the userinfo of a private ThemerrDB mirror) leaked
#   unscrubbed into op_progress activity strings (sync.py GET/git-clone/git-fetch →
#   progress.py), readable via GET /api/progress by a READ-scoped token (only
#   is_authenticated — unlike admin-gated /api/events; read tokens are handed to
#   Homepage dashboards). Scrub URL creds via events._redact_url_credentials on BOTH
#   progress write paths (start_progress + update_progress — robust catch-all) + at the
#   3 sync.py call-sites (belt-and-suspenders). /api/progress auth level deliberately
#   UNCHANGED (would break external dashboards). Behavioral test proven to fail without
#   the write-path scrub.
# 0.51.76 — security audit fix (XFF-spoof forward-auth bypass, HIGH — code side). motif
#   ran uvicorn with a HARDCODED forwarded_allow_ips="*" when trust_forward_auth was on,
#   so uvicorn overwrote request.client.host with the attacker-controlled leftmost
#   X-Forwarded-For token → a direct-to-:5309 attacker (off-path LAN/container) could
#   spoof an allowlisted IP + X-Authentik-Username and get full unauth admin. The full
#   close needs the operator's NPM topology (deferred to the user); this adds the CODE
#   side: web.forward_auth_trusted_proxies (env MOTIF_FORWARD_AUTH_TRUSTED_PROXIES) →
#   uvicorn's forwarded_allow_ips. Empty default = "*" (backward-compat, no behavior
#   change); operator sets it to NPM's container IP to close the hole. Also fixed 3 stale
#   comments that still called an empty forward_auth_allowed_ips "legacy permissive"
#   (v1.24.12 made it FAIL-CLOSED). Additive knob, default unchanged; config-roundtrip test.
# 0.51.77 — security audit cheap hardening: digit-validate `year` at the canonical path
#   chokepoint (path-traversal DiD). canonical_theme_subdir built f"{title} ({year})"
#   with title + edition sanitized but `year` RAW; a malicious Plex server supplying a
#   crafted year ("2020)/../../etc/(x") could escape themes_dir. Coerce any non-4-digit
#   year to no-year (ThemerrDB already caps to 4 chars). Not real-world exploitable
#   (trusted-Plex, constrained fixed-name write, no RCE) — a cheap one-line close.
#   NOTE: the logout-CSRF hardening (also cheap-hardening) was deliberately NOT done —
#   it's LOW (forced re-login only) and every fix variant (POST-form UI conversion /
#   Origin-Referer check) carries UI-styling or reverse-proxy-Host risk that can't be
#   verified in this environment; deferred to a change where the topbar button + NPM
#   Host behavior can be eyeballed. (SESSION_JOURNAL + memory record it.)
# 0.51.78 — CSS+login audit: two REAL login bugs. (1) authenticate_password's no-admin
#   timing-equalizer ran bcrypt.checkpw(b"x", b"$2b$12$"+b"x"*53) — a MALFORMED salt →
#   ValueError in microseconds (timing defense inert + INVERTED) and the unhandled error
#   500'd POST /login on a fresh/wiped-admin install (also exempting it from the rate
#   limit). Now verify_password("x", real-precomputed-hash) — real timing, swallows any
#   error. (2) login_get redirected away on `trust_forward_auth` ALONE, but forward-auth
#   is fail-closed with an empty allowlist (v1.24.12) → flip trust on without the IP list
#   and GET /login looped /login->'/'->/login, locking out the local-login fallback.
#   Removed the redundant redirect; the existing is_authenticated check (middleware always
#   resolves the principal) is the correct gate. Behavioral tests, proven fail-without.
# 0.51.79: CSS+login audit — CSS pass. (a) a11y: the universal :focus{outline:none}
#   stripped keyboard focus rings; the :focus-visible allow-list missed the primary nav
#   links, settings checkboxes/radios, and the topbar help/logout glyphs → invisible
#   focus. Repainted them; the glyph-only logout link also got an aria-label. (b) dead
#   CSS: four rename/redesign-residue selector clusters with ZERO live emitters
#   (.attn-pill-cookies [STATUS chip gone v1.19.67], .chip-info [blue lives on .btn-info],
#   #import-preview-table .url-cell [→ .url-link v1.15.77], .src-key-clear [→
#   .pill-filter-clear]) deleted + their now-stale JS/source-pin tests updated. (c)
#   ops.css: exact-match hardcoded sizes/radii swapped to the app.css :root tokens
#   (zero visual change). Static guards so the rings can't vanish + dead CSS can't creep.
# 0.51.80: CSS+login audit — login half. Session ids are hashed at rest:
#   sessions.id now stores sha256(cookie) instead of the raw motif_sess value, so a
#   DB read (SQLi foothold elsewhere, a leaked motif.db backup, /config filesystem
#   access) yields only a digest — useless for replay, since the raw token is 256 bits
#   of secrets.token_urlsafe entropy. create/lookup/destroy_session + the password-
#   rotation keep_session_id all hash consistently; the cookie stays raw. Deterministic
#   unsalted sha256 keeps the per-request lookup a plain indexed WHERE (no writer-lock
#   cost — cf. v1.11.37). No schema change; pre-existing raw-id rows stop matching and
#   expire (one forced re-login). Behavioral tests prove leak-can't-replay + keep-alive.
# 0.51.81: CSS+login audit — global brute-force ceiling on /login. The v1.21.18
#   per-IP throttle is defeated by an attacker rotating source IPs (trivial while
#   forwarded_allow_ips="*"). motif has ONE admin, so a high failure volume across ALL
#   IPs is unambiguously an attack: a second in-memory rolling-window counter caps total
#   failures across every IP (LOGIN_GLOBAL_MAX_FAILURES=50/900s) — once reached the
#   endpoint 429s everyone (no password check, no bcrypt-CPU burn) until it ages out. A
#   blank IP still counts; a success clears only the per-IP bucket (never the global one).
#   Folded into login_rate_limited/record_login_failure so /login is unchanged. An autouse
#   conftest fixture resets the process-level counters per test (the 900s window doesn't
#   age out within a suite run). Behavioral tests: fresh-IP blocked at ceiling + no
#   false-positive lockout below it.
# 0.51.82: RECENTLY ADDED carousel posters load through a bounded-concurrency queue.
#   v0.51.52 eager-loaded all ~40 posters (good — no lazy pop-in), but assigning all
#   ~40 img.src at once saturates the browser's ~6-connection-per-host cap; the long
#   pending image queue is then deprioritized to a near-stall while the WINDOW IS
#   UNFOCUSED (the user: "while autoscrolling and not clicked on the window the posters
#   aren't loading") until a click refocuses it. Now each URL is staged on data-src and
#   loaded via _loadCarouselPosters — only ~5 requests in flight, so no deprioritizable
#   backlog; still eager (every tile loads on arrival, leftmost first). Browser-timing
#   behavior; pinned by source-shape tests (can't be exercised headless).
# 0.51.83: UI audit pass (status drawer / info drawer / library CSS) — the info drawer's
#   recovery re-hydrate was edition-blind. hydrateRecoveryOptions takes a 5th rowRk arg
#   (v1.22.71) that pins /recovery-options to the clicked edition; the initial call passed
#   it but the two in-place re-hydrations after mark-alive + clear-failure (ACK FAILURE)
#   dropped it → server LIMIT 1 fallback redrew an arbitrary sibling edition's resolved/
#   acked annotation on multi-edition titles. Pass rowRk at both sites. (Status drawer +
#   library CSS audited clean — recurring traps all already guarded.)
# 0.51.84: UI audit a11y — the #info-dlg + #help-glossary modal dialogs had no
#   accessible name. A native <dialog> opened with showModal() does NOT derive its
#   name from a descendant <h2>, so a screen reader announced an unnamed dialog. Added
#   aria-labelledby on each, pointing at its (now id'd) // MOTIF INFO / // GLOSSARY title.
# 0.51.85: UI audit polish — the #op-mini-overflow pill was SSR'd op-tone-warn, but
#   renderTopbar always sets its computed tone (plex/tdb/queue) before it un-hides and no
#   op-mini renders warn anymore → a misleading dead default. Dropped to a neutral bare
#   .op-pill (the disk badge keeps its legit warn). NOTE: the .info-scope-row 6px/14px the
#   audit flagged as token-drift was DELIBERATELY LEFT — no --gap token matches (scale is
#   4/8/12/16…), so a swap would change spacing; off-scale literals stay (v0.51.79 policy).
# 0.51.86: DOWNLOAD PLEX BACKUP message — first-capture vs re-capture. The success alert
#   always claimed "Plex was serving a theme different from your existing backup", but the
#   "captured" branch fires on a FIRST-ever backup too (the common NO-TDB anime case) where
#   there was no existing backup to differ from (the user's confusion). The worker already
#   knows (its existing-local_files lookup): backup_cloud_theme now returns replaced_prior,
#   the op stamps backup_outcome.replaced, and the UI branches "Updated — re-captured" vs
#   "Captured — first backup". Old worker leaves replaced undefined → neutral "Captured".
#   Behavioral (first-capture + swap) + copy-guard tests.
# 0.51.87: RECENTLY ADDED carousel — the posters going blank while the window is unfocused
#   is a PAINT throttle, not a fetch stall. A live console probe proved all 40 posters were
#   fetched+decoded the whole time unfocused (loaded=40, fetching=0) — so v0.51.82's paced
#   loader fixed a non-issue (left in; harmless). The real cause: the per-30ms scrollLeft
#   auto-scroll churn outran the unfocused window's throttled compositing (console: "Forced
#   reflow" ×60), so scrolled-in tiles showed a stale blank frame until a click refocused.
#   Fix: the tick already bailed on document.hidden (hidden TAB); now also bails on
#   !document.hasFocus() (unfocused WINDOW), and a window blur listener nudges one repaint so
#   the frozen strip settles on its loaded posters. Source-guard tests (paint timing of a
#   real unfocused window — can't be exercised headless).
# 0.51.88: CSS+login audit (last hardening item) — idle-session timeout. Sessions died only
#   at the 30-day ABSOLUTE TTL; now a session untouched for >14 days (SESSION_IDLE_TIMEOUT_
#   SECONDS) is rejected too, bounding how long a lingering/stolen cookie stays live on an
#   idle account. The careful part (v1.11.37: the per-request last_seen_at UPDATE softlocked
#   the UI on the writer lock during long syncs): the refresh is COARSE — only when
#   last_seen_at is already >1h stale, so ~99% of requests write nothing — AND fail-fast +
#   best-effort (250ms busy_timeout, lock swallowed + warned-once). cleanup_expired_sessions
#   also reaps idle rows off the hot path (boot + scheduler sweep). Behavioral tests.
# 0.51.89: settings audit — the // CLEAR KEY (tmdb_api_key) + // CLEAR PROXY buttons were
#   dead. The clear-secret handler hard-returned for anything but plex.token, and the backend
#   keep-on-empty contract means a blank save keeps the value — so there was NO way to wipe the
#   TMDB key or proxy URL (the proxy hint even says "Click CLEAR PROXY to wipe"). Generalized
#   the handler to PATCH {section:{field:null}} for any data-cfg-clear path (the backend already
#   clears those on null). config save/load pipeline itself audited clean (silent-data-loss,
#   masked-secret roundtrip, closed-set, partial-clobber all guarded).
# 0.51.90: dashboard // DOWNLOAD ACTIVITY date alignment. The 30-bar axis was built with
#   LOCAL date math but keyed as UTC (toISOString), while the server buckets on UTC
#   DATE(created_at) — so in any negative-UTC zone (US) in the evening the last slot landed
#   on TOMORROW's UTC date, mislabeling/dropping the "today" bar. Client now iterates in UTC
#   (setUTCDate/getUTCDate); server window aligned to exactly the 30 UTC calendar dates the
#   client renders (was a rolling 30x24h window spanning a 31st partial date with no client
#   bucket → dropped). Also corrects a v0.51.89 rider: renaming the "// Clear-token button"
#   comment orphaned test_v1_17_5's bindConfigSaves slice-anchor (green-gate slip — the tag
#   shipped with that one stale-anchor test red; code was correct).
# 0.51.91: dashboard "hide section" never actually hid a section (RECENT ACTIVITY / SYNC
#   HISTORY stayed visible after // done editing). Root cause was a single malformed CSS
#   comment (app.css line ~6812): the v0.51.70 dead-Scans note listed removed selectors as
#   ".kind-*/.scan-filter*/.kpi*/..." — those '/' separators formed spurious '*/' sequences,
#   the FIRST of which closed the comment early. The CSS parser then hit invalid tokens and
#   its error-recovery swallowed the very next rule, ".dash-user-hidden { display:none
#   !important }", so the class that the customize toggle applies (and correctly persists via
#   /api/dashboard/layout — the PUT body was right all along) had no CSS backing it at
#   runtime. Reproduced + fixed + confirmed in a real browser (rule now parses; hidden
#   sections compute display:none outside customize, grid/block @ opacity .45 inside). Fix:
#   comma separators so the comment holds no '*/'. Guard test asserts no CSS comment closes
#   early (a comment-stripper leaves zero dangling '*/') and the .dash-user-hidden rule survives.
# 0.51.92: collections // ALL view — a per-library dot on the row + info card.
#   the user: several libraries can each own a collection with the SAME name
#   ("Action" ×3, one per library) and nothing told them apart. Adds a small dot
#   keyed to the owning library's TYPE (movie→amber / show→blue / anime→magenta,
#   reusing the dashboard per-library accents) with the library name on hover, on
#   the collection row (renderLibraryRow, gated on plex_media_type==='collection')
#   AND the info card's section chip. The /api/library SELECT now aliases ps.type
#   + ps.is_anime so the JS can pick the accent; section_title (already emitted)
#   is the tooltip / chip text. Colors verified in a real browser against the
#   resolved accent tokens; collections-only so the single-type tabs stay clean.
#   Also documents both the new library dot AND the v0.51.23 4K chip (never
#   decoded before) in a new "// TITLE" section of the // GLOSSARY + the
#   in-context library LEGEND — reusing the real tier-badge-4k / lib-dot-* row
#   classes (reuse-don't-mirror); the dot rows are gated to the collections tab.
# 0.51.93: the user disliked the v0.51.92 type-colored dot — replaced it with the
#   owning library's NAME trailing the collection title (a neutral .lib-tag pill
#   modeled on .edition-pill; natural case so "4K Movies" vs "Movies" also read
#   apart, which a type-color couldn't). Row: renderLibraryRow renders libTag
#   AFTER the name (collection rows only). Info card: dropped the extra dot — the
#   section chip already carries the library name. Removed the now-dead
#   ps.type/ps.is_anime SELECT aliases (only the dot color used them). GLOSSARY +
#   LEGEND updated to the name pill (4K chip decode unchanged). Verified in a real
#   browser; the 4K + neutral pill sit beside the name without breaking the flex.
# 0.51.94: tidy — removed the dead `sectionLabel` const in renderLibraryRow (built
#   a [Movies]/[Anime] title tag but UNUSED since v1.15.53 dropped it from the TDB
#   cell; v0.51.93's libTag now owns the collections library label) + de-referenced
#   the dead name in the v1.15.53 layout comment. No behavior change.
# 0.51.95: dashboard "ready to add" SSR count is now per-EDITION. The plex_*_ready
#   aggregates checked placements by media_type+tmdb_id+section_id only, while
#   /api/coverage/plex (the reference the SSR mirrors) scopes its `placed` flag by
#   `p.edition_key = pi.edition_key` (the v1.21.5x edition arc). So a placement on
#   ONE edition made every sibling edition of the same title+section read
#   "not ready" → the dashboard undershot the coverage page + the library's
#   per-row unthemed (SRC=—) view for multi-edition titles (mirror-drift). Added
#   the edition_key clause to all four SSR NOT EXISTS subqueries. Behavioral test
#   (two editions, one placed → SSR ready counts the unplaced sibling) + a
#   mirror-drift guard pinning all four subqueries + the coverage reference.
# 0.51.96: a11y — the login + first-run setup error banners (<div class="alert
#   alert-error">, server-rendered under {% if error %}) had no ARIA role, so a
#   screen reader never announced a failed login / setup error. Added role="alert"
#   to both (the standard live-region role for a validation error). Markup-only.
# 0.51.97: LOGS event-stream ?level=/?component= deep-link filter (v1.22.79) was
#   applied INVISIBLY — the SINCE chips still read ALL and there was no way to clear
#   it short of a full nav back to /queue. Now renders a dismissable amber pill per
#   active filter (renderEventFilters); clicking it clears that var, drops the URL
#   param (so a refresh doesn't re-apply it), and reloads. Verified render+clear in a
#   real browser; the .chip.ev-filter-clear (0,2,0) amber beats the later .chip color.
# 0.51.98: dashboard SYNC THEMERRDB / REFRESH PLEX hero buttons flashed clickable for
#   ~1s on nav while a sync/refresh was already running — the SSR rendered them plain
#   and only the first refreshTopbarStatus poll (/api/stats 1s TTL) locked them. Now
#   _dashboard_ssr_state bakes two busy flags (sync_btn_busy / refresh_plex_btn_busy)
#   and dashboard.html paints disabled + the busy label on first paint. The SYNC lock
#   mirrors app.js bindDashboard's page-load probe EXACTLY (tdb>0 OR autoEnum·enum>0 —
#   the init-path authority that arms the watcher + blocks the unlock); labels match
#   origLabel/busyLabel so the first poll is idempotent (flush >label</button>, no churn).
# 0.51.99: swept the rest of the "SSR button renders clickable, a poll locks it ~1s
#   later" class after the v0.51.98 dashboard fix. Only one more genuine flash: the
#   orphan-scan // RUN SCAN button (orphans.html) — orphans.html's own pollStatus()
#   locked it on status==='running' only after the init poll landed. /admin/orphans now
#   passes orphan_scan_running (from _ORPHAN_SCAN_STATE) so the button paints disabled +
#   the sibling span seeds "scanning…". All settings probe/reprobe buttons are SAFE
#   (click-only lock, no poll to flash to); library REFRESH already SSR-locks via
#   pipeline_in_flight (residual per-tab-scope gap noted, deferred).
# 0.51.100: closed that residual. The // REFRESH <NAME> button's SSR lock was
#   _pipeline_in_flight (cascade/scan_all only) — a per-tab scope=<tab> refresh, or
#   the pre-cascade tdb-sync phase, slipped through and flashed clickable on re-nav.
#   New _lib_refresh_in_flight(db, tab, fourk) is the FULL app.js libRefreshBusy mirror
#   (myTabBusy per tab+variant from section is_anime/type/is_4k + globalEnumPipeline:
#   pipeline OR tdb-sync·auto_enum OR ≥2 tabs running); template var renamed
#   pipeline_in_flight → refresh_in_flight. Behavioral tests cover per-tab lock +
#   cross-variant/cross-tab independence + collections + cascade back-compat.
# 0.51.101: plex-refresh perf (Tier-1 #1) — the three end-of-enum passes
#   (reconcile_placement_paths + the two FS-stat health passes) ran table-wide,
#   serial-FS, on EVERY enum, even a single-section REFRESH or an all-delta-
#   skipped run — thousands of theme.mp3 stats over the network mount for
#   nothing. Now run_plex_enum tracks the walked sections and SKIPS all three on
#   a no-work run / SCOPES them to the walked-or-targeted section(s) via a new
#   optional section_ids param (default None = table-wide, unchanged for the
#   nightly all-sections cron). placements/local_files carry section_id in the
#   PK so scoping is clean; the global pure-DB plex_upload staleness pass stays
#   table-wide. O(16K)→O(section) on the common REFRESH; ~0 on a stable cron.
# 0.51.102: plex-refresh perf (Tier-1 #3) — the per-show folder-path fallback
#   (get_item_paths_bulk, fires every enum on builds that ignore
#   includeLocations=1) fetched the FULL metadata tree per item just to read
#   Location.path. Now sends the same excludeElements trim the /all listing uses
#   (~5-10× smaller batches; Location/Media/Part kept so folder discovery is
#   unaffected — advisory per Plex spec) + bumps max_concurrent_batches 4→8.
# 0.51.103: plex-refresh perf (Tier-1 #2 + #4). #2: verify_placement_health +
#   verify_canonical_health now stat every theme.mp3 across a 16-worker thread
#   pool (the cost on a network mount) — the bucketing/accounting stays serial
#   so present/missing/skipped/prune + the mount-fault cap are byte-for-byte
#   unchanged. #4: _UPSERT_BATCH 200→400 + inter-batch sleep 150→100ms (simple
#   per-row writes → short lock-holds; halves the fixed sleep tax on a large
#   section). The resolve chunk_size/sleep are left as-is (deliberately tuned
#   against documented writer-starvation — their 7-UPDATE chunks hold ~1-2s).
# 0.51.104: stale-.motif-tmp sweep. _safe_link_or_copy stages a placement as
#   theme.mp3.motif-tmp then os.replace()s it to theme.mp3; a crash/restart in
#   that window orphans the temp, and it's only cleaned on the NEXT placement to
#   that folder — a folder that never gets re-placed (edition drift, a switch to
#   plex_upload) keeps it forever (the user found one in a LotR folder). New
#   sweep_stale_placement_temps walks distinct plex_items.folder_path media
#   folders (host→container translated), unlinks any theme.mp3.motif-tmp older
#   than 1h; a daily scheduler job (03:20 UTC) runs it. No-op when none exist.
# 0.51.105: RE-PUSH topbar badge lingered ~10s after the row it counted was
#   resolved. refreshTopbarStatus (owns RE-PUSH/FAIL/UPD badges) polled a FIXED
#   10s setInterval; a per-row `place` job (a re-push) emits no op_progress row →
#   no motif:ops-state-changed force-refresh → the badge waited for the next 10s
#   tick while libraryRapidPoll (~2s) had already resolved the row. Made the
#   topbar poll ADAPTIVE (2s while any mutating op is active, 10s idle) off the
#   anyMutatingOpActive signal refreshTopbarStatus already stashes — badges now
#   track rows during ops.
# 0.51.106: code-review corrective batch (findings from the v0.51.98-105 diff).
#   #1 (CONFIRMED): verify_placement_health's mount-fault cap is an ABSOLUTE
#   floor (max(50, total//4)). v0.51.101 made the pass runnable against a SINGLE
#   scoped section — a /data blip that reads every sidecar of a small (<200-row)
#   section missing has suspect < the 50 floor, so the guard never fired and the
#   whole section false-stamped broken. A count/ratio gate can't fix it (a small
#   mount-blip section is identical by count to a small library of genuinely-
#   broken rows, which v1.23.30 requires to surface). The distinguisher is FOLDER
#   LIVENESS: a mount fault takes the containing folder down too, a real theme.mp3
#   deletion doesn't. Now trips (below the floor) only when ~all examined read
#   missing-AND-folder-gone. verify_canonical_health untouched — its themes_dir
#   root probe is already section-count-independent. #5: plex.py get_item_paths_
#   bulk docstring still said "4" concurrent batches after v0.51.102 raised it to
#   8. + behavioral tests: the folder-liveness signal (dead-folder mount fault
#   suppressed; live-folder missing files + a few dead folders still surface) and
#   the _lib_refresh_in_flight term-3 (≥2 running tab enums lock a third, enum-
#   less tab — #4 had no coverage).
# 0.51.107: code-review findings #2 + #3. v0.51.101 moved BOTH verify_*_health
#   passes inside run_plex_enum's per-section scope, so (#2) the plex_upload
#   staleness 0-stamp — the RE-PUSH detector, documented to "run unconditionally"
#   — is skipped entirely on a no-work enum (every section delta-gated), and (#3)
#   with both auto_enum toggles off no enum fires at all → canonical + placement
#   health never re-stamp and a broken row lags indefinitely. Added
#   _daily_health_passes_job: a table-wide (section_ids=None) run of both passes
#   on a daily 03:25-UTC scheduler slot, decoupled from enum config — restores
#   the pre-v0.51.101 unconditional coverage. Best-effort (a pass failure never
#   crashes the scheduler); skips canonical when themes_dir is unset. + behavioral
#   tests (stale plex_upload rk → RE-PUSH; broken sidecar + canonical re-stamp;
#   no-themes_dir clean skip; scheduler registration + table-wide guard).
# 0.51.108: theme presets Tag 1 — accent decouple (invisible refactor, Fallout
#   renders pixel-identical). --green* was overloaded across ~155 CSS sites as
#   BOTH the brand/chrome accent AND the fixed semantic green (SRC=T source
#   identity + healthy/ok/present status). Split them so a future preset can
#   retheme the accent while pills/chips keep their meaning: new --accent* is the
#   themeable chrome accent (defaults to green); --green* are now ALIASES of
#   --accent* so the ~125 chrome sites follow it with no mass-rename; the ~30
#   semantic sites repoint to new FIXED tokens --src-t*/--ok* (literal green,
#   never themed). --green-pale (TB chip) stays fixed. Updated the semantic-token
#   pins in 6 tests; chrome pins untouched. No behavior change — purely the
#   token foundation for the // THEME picker + presets landing next.
# 0.51.109: theme presets Tag 2 — the // THEME picker + Fallout/Plex/Dracula/Nord.
#   window.MOTIF_THEMES (base.html) is the single source of truth: each preset
#   overrides the canvas (--bg/--fg/--line families) + the --accent family and
#   NOTHING else, so the semantic SRC/LINK pills+chips stay fixed. Applied
#   pre-paint on every page from localStorage 'motif:theme' (mirrors the v1.24.65
#   dashColors head script; Fallout/unset = the :root green defaults, no flash).
#   A // THEME select in the customize-layout panel (sibling of // LIBRARY COLORS)
#   live-applies + persists; the picker reads the shared bundle (no duplicate
#   hexes). Tests guard the full canvas/accent set per preset AND that no preset
#   ever touches a semantic token.
# 0.51.110: theme presets — feedback pass. (1) Relocated the // THEME picker out
#   of the dashboard customize-layout panel (where it read as tied to the library
#   colours) to Settings → VISUALS, its natural home beside the CRT-effect
#   toggles. New bindThemePicker() in app.js populates the <select> from the
#   shared window.MOTIF_THEMES + live-applies + persists; removed the picker from
#   dashboard-customize.js (the base.html pre-paint still themes every page). (2)
#   The Plex preset was too warm/brown + too dark; real Plex is a NEUTRAL near-
#   black charcoal (#1d1d1f) with gold (#e5a00d) — re-tuned the canvas to match.
# 0.51.111: theme presets — action buttons follow the accent (the user). The
#   generic amber action tone .btn-warn (SAVE / TEST / CLEAR / REBUILD) was stuck
#   amber on a Plex/Dracula/Nord canvas; repointed it from --amber to
#   --accent-bright + --accent (+ accent-rgb hover) so it themes, staying bolder
#   than a plain .btn. Genuinely-semantic tones stay fixed: .btn-danger (red),
#   .btn-plex (Plex amber), the SRC .lib-source-*/promote families, ENV-OVERRIDE.
# 0.51.112: theme presets Tag 3 — added Gruvbox (warm orange), Tokyo Night
#   (indigo blue), Synthwave (neon pink), and Mono (grayscale) to
#   window.MOTIF_THEMES. Additive: same machinery, each preset overrides only the
#   canvas + --accent family. tokyonight gets a two-word 'TOKYO NIGHT' picker
#   label. (The settings action buttons already theme as of v0.51.111 — the only
#   fixed one is the red destructive button, which stays red by design.)
# 0.51.113: theme flourishes + carousel dwell (the user). The topbar equalizer
#   bars (.brand-mark .brand-bar) and the hero-title glyph (.title::before) were
#   fixed --amber, reading Plex-ish on a non-green canvas; both now follow the
#   --accent so they match every theme. Also: the dashboard RECENTLY ADDED
#   carousel dwells 3s at the end before snapping back to the start (was instant).
# 0.51.114: theme "Split" decision (the user) + a fixed severity tone. The user
#   chose SPLIT — the row-table SRC/LINK pills, chips + status dots stay FIXED
#   (scannable data grid), while the peripheral SOURCE chrome (source-menu
#   buttons, URL links, success msgs) FOLLOWS the accent; the glossary/legend +
#   text labels anchor the meaning, so that divergence is aesthetic, not
#   confusing. The one carve-out: the SEVERITY scale (import CONFLICT +
#   orphan-drift warn) is a SAFETY signal, not a source color — a CONFLICT that
#   reads the same green as CLEAN could be applied over an existing theme
#   unnoticed. New fixed-amber .btn-tone-warn carries those; the ok tier rides
#   fixed-green .btn-tone-ok; .btn-warn stays the THEMED action tone (v0.51.111).
#   Plus a preset hex==rgb drift guard so a re-tuned accent can't half-theme.
# 0.51.115: render the FULL severity scale (code-review follow-up). The
#   orphan-drift "needs attention" tier (motif_not_selected / nothing_selected)
#   used lib-source-user, whose only CSS rule is .btn.lib-source-user — but the
#   drift chip is a .btn-less .chip, so it rendered COLORLESS (v0.51.114 fixing
#   the ok tier to actually paint made it the lone blank tier). New bare-selector
#   .btn-tone-attn (fixed violet) carries it. Also: btn-tone-warn border → new
#   --amber-deep so the btn-tone-* family shares one color+deep-border shape
#   (added --amber-deep + --violet-deep). New guard asserts every DRIFT_TONE +
#   import tone value resolves to a BARE css rule, so a .btn-gated class can't
#   silently fail to paint again.
# 0.51.116: SOURCE/PLACE action-button colors stay FIXED across themes (the
#   user). The menu-item color encodes the action's SOURCE (RE-DOWNLOAD TDB green
#   = ThemerrDB, SWITCH TO SIDECAR green = HL placement) — it mirrors the FIXED
#   SRC/LINK pill of the state the action lands the row in. lib-source-themerrdb
#   + lib-source-place_file were the only two still riding --green (→ accent), so
#   on a non-Fallout theme the same source read as two colors (gold button beside
#   a green pill). Repointed to the fixed --src-t / --ok tokens; the rest of the
#   family (user=violet, adopt/PU=cyan, plex=amber) was already fixed. Guard locks
#   the whole .btn.lib-source-* family off the themeable aliases. NOTE: this
#   supersedes the v0.51.114 "keep source-menu buttons themed" call.
# 0.51.117: library REFRESH button no longer flashes clickable on a fast section
#   switch mid-refresh (the user). switchLibraryTab re-applied the lock via
#   updateLibraryRefreshBtnLabel() (SYNC, reads the possibly-stale global-pipeline
#   stash) BEFORE the authoritative refreshTopbarStatus() (after await
#   loadLibrary) — a full refresh's startup / poll-lag window read not-busy, so the
#   button unlocked for the loadLibrary duration. Added a `tightenOnly` mode used
#   only by the section switch: it may LOCK but never optimistically UNLOCK a
#   disabled button; refreshTopbarStatus (fresh /api/stats) owns the unlock.
#   Variant/status-chip toggle callers keep instant unlock. Proven by a quickjs
#   harness driving the real fn (bug repro + fix).
# 0.51.118: dedup the theme-apply logic (code-review debt). Pre-paint (base.html)
#   + the // THEME picker (app.js bindThemePicker) applied a theme bundle via two
#   different loops — mirror-drift risk (a reload and an in-app switch could
#   diverge). Extracted ONE window.MOTIF_APPLY_THEME(name) in base.html (defined
#   before the pre-paint so both reach it): clear every preset token, then apply
#   the chosen bundle (fallout/unknown = clear only; the clear is a no-op on the
#   fresh pre-paint load). Both callers now delegate; dropped the picker's
#   allTokens set. Behaviorally proven by a quickjs harness (apply / switch-clears-
#   stale / fallout-clears-to-defaults).
# 0.51.119: two code-review follow-ups. (1) v0.51.117's tightenOnly blanket-held
#   the button on a section switch, over-locking a switch AWAY from a per-tab
#   refresh to an idle tab (spurious // REFRESHING…). Root cause: the button-lock
#   stashes are written after refreshTopbarStatus's 803 supersession bail, so
#   continuous fast-switching starves them stale. Fix: stamp window.__motif_enum_
#   stash_ts at the stash write; tightenOnly now holds ONLY when the stash is STALE
#   (fast-switch → can't rule out a refresh → hold, no flash) and TRUSTS a FRESH
#   stash (normal single switch → unlock, no over-lock). (2) MOTIF_APPLY_THEME
#   clears the deduped UNION of preset token keys (seen-set → 14 removeProperty
#   calls, not 7× per shared key). Both proven by the quickjs harnesses.
# 0.51.120: root-cause reorder (the user) — write the library REFRESH button's
#   lock stashes BEFORE refreshTopbarStatus's supersession bail. v0.51.119's
#   freshness gate was a heuristic; the real cause is that the stashes
#   (__motif_global_enum_pipeline, __motif_enum_active/pending, collections) were
#   written only AFTER the v1.17.22 bail, so continuous fast section-switching
#   supersedes every poll and starves them stale. Extracted a pure
#   _deriveEnumStashes(q) and write the stashes early (even a superseded poll now
#   keeps them fresh); the freshness gate is now an always-fresh backstop. Any
#   minor out-of-order among recent snapshots self-corrects on the next poll — a
#   starved stash did not. Guarded by test_v0_51_120 (writes-before-bail source
#   order + a drift lock pinning the helper's globalEnumPipeline to the main
#   block's identical formula + behavioral derivation).
# 0.51.121: swap the two top dashboard stat rows' metrics in place (the user, on
#   a screenshot: "swap the info in the themerrdb cards and the plex cards" →
#   chose "swap the metrics in place"). The top-stats section (top row) now
#   renders the PLEX LIBRARY reach cards (total + in/not-in-ThemerrDB) and the
#   plex-coverage section (bottom row) renders the COVERAGE % cards. The
#   data-dash-section shells keep their ids + DOM order — dashboard-customize.js
#   reorders #dash-sections BY id against a saved layout, so only the card BODIES
#   swap (each travels whole: tone class, anime/collections display:none gating,
#   JS-target ids, plex-foot CSS). The parser-blocking count-up moved below the
#   COVERAGE section (its cov-*-pct targets swapped down there), and the //
#   LIBRARY COLORS panel re-anchored onto top-stats (the colored --dash-*-color
#   cards it controls moved up). Guarded by test_v0_51_121.
# 0.51.122: redo the v0.51.121 swap the ROBUST way (the user's saved dashboard
#   customize layout undid v0.51.121 — applyLayout moves cards into sections BY
#   their data-dash-card id, so moving the card bodies between sections got
#   reverted on load while the section-label swap stuck → mismatched labels vs
#   cards). Fix: swap only the NUMBERS in place. The // …THEMED (tdb-*) cards keep
#   their titles/tone/section/id but now render the ThemerrDB REACH (total +
#   in/not-in-ThemerrDB); the // PLEX (plex-*) cards keep theirs but render the
#   COVERAGE % + bar + ready. Cards never move by id, so the saved layout can't
#   undo it. Reverted v0.51.121's section-label + color-panel changes (the user:
#   "names + color control back where they were"). The anime/collections hide-gate
#   + #plex-*-card reveal id moved onto the reach cards (renderPlexCoverage toggles
#   them by presence); the wide-reach foot column-stack moved from
#   [data-dash-card^="plex-"] to an explicit .stat-foot-stack class; the count-up
#   moved below the PLEX section (cov-*-pct swapped down there). Guarded by
#   test_v0_51_122.
# 0.51.123: "💔 Theme lost —" notifications now name the actual content instead
#   of the bare "tv/4656" id (the user, on a Discord screenshot). Root cause:
#   enrich_item derives display_title only from the `themes` table, so a lost
#   P-row with no ThemerrDB match (the exact "no backup configured" case) had no
#   title and _safe_display_title fell back to media_type/tmdb_id. The plex_enum
#   lost-theme candidate already carries cand["title"] (plex_items.title) — added
#   a fallback_title param to enrich_item (fills the gap only; a real themes title
#   still wins) and pass it at the live dispatch + the test-trigger endpoint.
#   Guarded by test_v0_51_123.
# 0.51.124: broaden v0.51.123 so EVERY titleless-row notification names the
#   content — most importantly "💾 Theme backed up" for cloud-backed P-rows,
#   which flows through enrich_item (worker.py) with no explicit fallback_title
#   (the user: "Theme backed up lets do that too"). enrich_item now looks up
#   plex_items.title (+year) by (guid_tmdb, plex media_type, section_id) when
#   neither the themes lookup nor fallback_title supplied a title — Plex always
#   knows the row's title. A real themes title still wins; the bare mt/tmdb
#   shape only shows when Plex has no matching row either (synthetic-tmdb rows).
#   Guarded by test_v0_51_124.
# 0.51.125: the library search now matches a bare TMDB id (the user: "having
#   trouble locating the rows that were removed" — the 💔 Theme lost alerts named
#   rows only as "tv/4656", and the search matched title + IMDb id only, so the
#   tmdb id was un-searchable). _library_main_query ORs pi.guid_tmdb = ? into the
#   search clause for all-digit queries (gated so a text search can't collide
#   with a stray tmdb; guid_tmdb is INTEGER, the str bind coerces via affinity).
#   The clause is identical in where_extra + where_pi_only so the leading
#   q-params stay aligned across every count path. Guarded by test_v0_51_125.
# 0.51.126: locate/title theme-linked rows by the theme's tmdb id. A 💔 Theme
#   lost alert names a row by COALESCE(themes.tmdb_id, guid_tmdb) (plex_enum.py:
#   2427), so a theme-linked row with a NULL guid_tmdb (anime / HAMA agents)
#   shows the THEMES tmdb_id — which v0.51.125's search + v0.51.124's title
#   fallback (both keyed on guid_tmdb only) missed (the user searched 31991 in
#   every section, no results). Both now ALSO match via pi.theme_id →
#   themes.tmdb_id. Guarded by test_v0_51_126.
# 0.51.127: reframe the no-backup 💔 Theme lost alert copy. The plex_theme_lost
#   (no_fallback) event fires from the v1.18.90 reaper — the item is gone from
#   Plex's listing (removed, or re-added under a new rating_key) so motif deleted
#   the row — yet the body told the user to "open the row's INFO card to restore"
#   (no row to open; the user searched a reaped row, found nothing). Reworded to
#   frame the item as removed from Plex + the conditional path if Plex re-added
#   it (re-detects on the next // REFRESH PLEX). Guarded by test_v0_51_127.
# 0.51.128: reaper consecutive-miss guard (grace before deleting + alerting). The
#   v1.18.90 reaper DELETEd a plex_items row + fired 💔 Theme lost the instant
#   Plex's section listing stopped returning it — so a transient Plex glitch (a
#   partial catalog, an API hiccup, a re-add under a new rating_key mid-enum)
#   could false-delete a live row + fire a false alert. Schema v70 adds
#   plex_items.consecutive_missing; the reaper now increments it per miss, resets
#   it the moment a row reappears, and reaps only at >= _REAP_MISS_THRESHOLD (2)
#   consecutive full enums. enumerate_section_items already raises on partial
#   fetches (v1.23.64), so this counter is the last line between a real removal
#   and a blip. Guarded by test_v0_51_128; the 8 pre-existing reaper tier tests
#   pre-age rows one below the threshold via _prime_reaper_grace.
# 0.51.129: code-review follow-ups to v0.51.128. (1) The reaper miss-counter only
#   advances on enums that WALK a section, but the contentChangedAt-skip short-
#   circuits before the reaper — so after a removal a genuine reap was deferred
#   ~24h (to the overdue bypass) and manual REFRESH couldn't force it, re-opening
#   phantom-P (SRC=P for removed content) for that window. Now a user REFRESH
#   stamps force=true → run_plex_enum bypasses the skip (two refreshes reap a
#   removed item + clear phantom-P on demand); cron/post-sync stay unforced.
#   (2) Made the test_v0_51_128 "no false alert" assertions load-bearing (the
#   STALE row is now themed so a reap CAN dispatch 💔). (3) Defer-log fires on
#   mixed enums + drops the hardcoded "1". (4) Cap consecutive_missing at the
#   threshold so a mass-guard-aborted bulk removal can't grow it unbounded.
#   Guarded by test_v0_51_129 + test_v1_14_74::test_force_bypasses_skip.
# 0.51.130: Settings → VISUALS THEME dropdown alignment. The .theme-select was
#   inline-block, so it sat BESIDE the THEME label with an off horizontal gap,
#   breaking the form's left edge (every other field's control sits below its
#   label). Made it display:block so the dropdown drops onto its own line, left-
#   aligned with the rest of the form; width stays intrinsic (compact).
# 0.51.131: (1) dashboard card titles retuned to match the v0.51.122 swapped
#   content — the top-stats cards are now plain (// MOVIES / TV / ANIME /
#   COLLECTIONS, the library total + ThemerrDB reach), the plex-coverage cards
#   are // … THEMED (the coverage %). The old // … THEMED (top) + // PLEX …
#   (bottom) titles no longer matched their numbers (the user). Display TEXT
#   only — ids/sections/positions untouched, so customize stays a no-op.
#   (2) THEME dropdown gap: dropped .theme-select's anomalous margin-top:gap-1
#   (which .input fields don't have and which floated the dropdown down toward
#   the hint) so it groups tight under its label like every other field.
#   Guarded by test_v0_51_131; ~8 dashboard test files remapped their title
#   anchors.
# 0.51.132: log cleanups from the user's boot log. (1) recovery_v55's v1.24.34
#   edition-coverage walker (the last recovery walker still boot-wired after
#   v1.21.0 retired the other 15) logged "marker already set — skipping" on the
#   INFO boot log every restart — downgraded to DEBUG. (2) resolve_theme_ids
#   logged "scanned N plex_items rows" where N is the cumulative rowcount across
#   its ~7 idempotent match-UPDATE passes (routinely >> row count, read like a
#   runaway scan) — reworded to report rows-processed + link-writes separately.
#   Guarded by test_v0_51_132. (No functional change; NOT the per-section
#   double-resolve — that's a hot-path perf item left for a deliberate pass.)
# 0.51.133: media_type-scope resolve_theme_ids (the per-section double-resolve
#   from v0.51.132's note). plex_enum ran the resolve twice per section — after
#   the items upsert AND after the collections upsert — each re-walking the WHOLE
#   section, so a 10.5K-movie section re-resolved all its items on the collections
#   pass. resolve_theme_ids gains a `media_type` param that narrows the scope; the
#   plex_enum call passes the pass's media_type ('movie'/'show' vs 'collection'),
#   so each pass only walks the rows it upserted (~2× less resolve work on a full
#   refresh). Purely narrows the rating_key set — per-row match logic unchanged,
#   so every row resolves identically (test_v0_51_133 proves two scoped passes ==
#   one unscoped). sync + recovery callers pass media_type=None (stay global).
# 0.51.134: CSS-audit T1 (P0) — the orphans /orphans FINDINGS table was wrapped
#   in .jobs-scroll, a dead class (its .jobs-scroll-x ancestor was removed in
#   v1.22.56) that gives NO scroll context. With body{overflow-x:hidden} the
#   7-col table spilled its right edge under the clip below ~760px and the ACTION
#   column (// RE-PUSH / // LET PLEX SERVE / × PURGE / // PROBE) was unreachable
#   on a phone. Swapped to the shared .table-scroll wrapper (overflow-x:auto at
#   ≤1080px, the #library-table pattern). Harness-proven at a 375px layout
#   viewport: PROBE button reachable-after-scroll false→true. Guarded by
#   test_v0_51_134. No min-width floor — the auto-layout table's button-driven
#   min-content already exceeds phone width, so the swipe context alone suffices.
# 0.51.135: CSS-audit T3 (P1/2) — the dashboard // STORAGE WASTE table had no
#   scroll wrapper, so its long DESTINATION container-path cell pushed the 5-col
#   table ~486px wide and the per-row // RELINK off-screen right under
#   body{overflow-x:hidden} below ~500px (also spilling the whole page wide).
#   Wrapped it in the shared .table-scroll (dashboard.html) like #library-table.
#   Harness-proven at 375px: page horizontal overflow gone (scrollWidth 375) +
#   // RELINK reachable-after-swipe true. (// RE-LINK ALL in the header is
#   always visible regardless.) Guarded by test_v0_51_135.
# 0.51.136: CSS-audit T2 (P1) — the dashboard STATISTICS .dash-pair (PER-SECTION
#   COVERAGE | GENERAL STATISTICS) sat 2-up, but each 6-7-col compact table's
#   min-content is ~532px so below ~1132px the flex cols (min-width:340) couldn't
#   give each table its width and the wider COVERAGE table spilled ~115px past its
#   card — hidden by body{overflow-x:hidden}. The v0.51.24 stack fix only covered
#   ≤600px, leaving the whole 601-1130px band broken. Moved the dash-pair
#   stack+swipe rules OUT of the ≤600 block into a new @media (max-width:1200px)
#   block (≤600 ⊂ ≤1200, phone unchanged). Harness-proven: 900px & 1080px went
#   from 2-up-with-spill to clean stack (no page overflow); 1250px keeps the
#   v1.24.66/69/77 equal-height 2-up; 375px still stacks + T3 RELINK swipes.
#   Breakpoint = measured ~1132px 2-up-fit threshold + margin for long Plex
#   library names; ≥1280 desktops keep 2-up. Guarded by test_v0_51_136.
# 0.51.137: CSS-audit T4 (P2) — the library filter drawer had two desktop pill
#   drifts from the v1.12.48 shrink-to-20px pass. (1) The LINK row was 2px taller
#   than DL/PL/SRC/ED/TDB: the v1.12.48 group set `.pill-filter-row .link-glyph
#   { height: 20px }` but the older v1.12.25 rule still set height:22px on the
#   SAME selector (equal specificity, later in source → the stale 22px won).
#   Dropped it → the 20px group governs, all six rows scan as one grid. (2) The
#   round DL/PL filter dots were 22w×20h ovals: v1.12.48 shrank filter-pill
#   HEIGHT but never the .state-pill-btn base WIDTH. Base 22→20px (state-pill-btn
#   only ever renders inside .pill-filter-row) → 20×20 circles. Harness-proven at
#   1400px: LINK row 22→20px, dots oval→circle; phone tier (30px touch pills)
#   unchanged. Guarded by test_v0_51_137.
# 0.51.138: CSS-audit T5 — removed 4 provably-dead library rules. `.form-label-row`
#   (v1.22.55 settings redesign retired the wrapper; no live markup uses it —
#   test_v1_22_55/test_v1_17_8 already assert its absence). `.state-pill.warn`,
#   `.state-pill.bad`, `.state-pill.mismatch` — the DL/PL dot suffix only ever
#   computes on/broken/pushed/await/'' (dl=broken|on|''; pl=broken|pushed|on|
#   await|''); 'mismatch' was retired as a dot state in v1.12.81, warn/bad never
#   stamped, so all three styled nothing. Also corrected test_v1_15_43, which
#   pinned .state-pill.mismatch's colour + mislabelled it a LIVE surface — the
#   real M surfaces (.link-glyph-mismatch, .attn-pill-mismatch) are untouched.
#   Guarded by test_v0_51_138. Per the user (thorough over dead-but-pinned).
# 0.51.139: CSS-audit T6 — token hygiene (all value-preserving, no visual change).
#   `border-radius: 2px` ×15 → var(--radius); `font-size: 11px` ×3 → var(--t-tiny);
#   the ≤600 .block-head `padding-left/right: 12px` → var(--gap-3). Each literal
#   equals its token, so rendering is unchanged — this just follows the design-
#   system rule (use tokens, never inline the value). test_v0_51_8's two pins
#   updated to the tokenized forms. DELIBERATELY SKIPPED: minting --motion-drawer
#   (the ops-drawer motion-fast/transform-only vs dialog motion-normal/opacity+
#   transform aren't the same tuple — a mint here would be the premature
#   abstraction CLAUDE.md forbids); .dlg-close 22px (test_v1_15_115 is a purpose-
#   built "must remain 22px" guard); .missing-banner→--bg-tint-amber (its 0.05 is
#   intentional, pinned by 2 tests — the unused --bg-tint-amber 0.04 is a dead
#   token, a T7 item). Guarded by test_v0_51_139.
# 0.51.140: scope the v0.51.136 dash-pair stack to the STATISTICS tables pair. A
#   /code-review found v0.51.136 restacked BOTH .dash-pair blocks at ≤1200px, but
#   only the STATISTICS pair (COVERAGE | GENERAL) has the table-overflow that
#   motivated it — the SYNC & DOWNLOADS insight charts are width:100% SVGs with no
#   overflow, and v0.51.45 deliberately put those two short sparklines 2-up. So the
#   ≤1200 rule was over-broad. Marked the STATISTICS wrapper .dash-pair-tables,
#   scoped the ≤1200 stack to it, and gave the insight pair
#   (.dash-pair:not(.dash-pair-tables)) its 2-up back down to 600px (stacks at the
#   phone tier like every card). Harness-verified: at 900px the tables stack while
#   the insight charts stay 2-up; at 375px both stack. Guarded by test_v0_51_140.
# 0.51.141: theme-upload UX for reverse-proxy body limits (the user hit a CrowdSec
#   AppSec 10 MiB body cap that 403'd the upload with a full HTML ban page before it
#   reached motif). (A) the upload fetch handler now detects an HTML/non-JSON error
#   body (a proxy/WAF block, not motif) and shows an actionable one-line message
#   instead of dumping the page; it also surfaces motif's own JSON `detail` (e.g.
#   413 "file > 50 MiB") rather than raw JSON. (B) a pre-flight size hint on file
#   pick: >50 MiB → will be rejected (motif's cap); >9 MiB → warn it may be
#   proxy-blocked + nudge to trim/re-encode (themes are short loops). Sibling
#   database-restore upload (via api()) left for a follow-up. Guarded by
#   test_v0_51_141.
# 0.51.142: code-review follow-ups on v0.51.141's theme-upload UX (3 message/UX
#   fixes, no behavior/data change). (1) the proxy-block error no longer hard-blames
#   request size — it branches on r.status so a 502 during a redeploy ("motif is
#   unreachable — retry"), a 401 SSO timeout ("session expired — sign in again"), a
#   403 WAF block, and a 413 body-limit each get the right prose instead of "trim
#   the theme." (2) the pre-flight >9 MiB proxy-cap warning gets a .warn (amber)
#   class so it reads as a warning, not the same dim tone as the plain info line.
#   (3) the >50 MiB reject ceils the shown size so it can't read "50.0 MB exceeds
#   50 MB" at the boundary. Guarded by test_v0_51_142.
# 0.51.143: close the code-review #4 false-success (deferred from v0.51.142). The
#   theme-upload used a direct fetch and declared "✓ uploaded" on r.ok alone — a
#   reverse proxy that intercepts the POST and answers a 302→200 HTML page (e.g. an
#   SSO login page) is also r.ok, so a theme that never reached motif reported
#   success. Now the success path confirms motif's JSON body ({ok:true}) before
#   declaring success. Sibling: the database-restore upload goes through api() (which
#   already throws on a non-JSON 200, so no false success there) — reframed its catch
#   so a proxy HTML 200 / network drop shows "could not reach motif…" instead of a
#   cryptic "Unexpected token '<'". Guarded by test_v0_51_143.
# 0.51.144: reverse-proxy audit — correctness batch (all three-tag audit, tag 1/3).
#   (1) New shared `describeProxyOrHttpError(status, ct, body)` / `proxyStatusHint(status)`
#   decoder next to api() — one home for the proxy/WAF/SSO messaging that v0.51.141/142
#   inlined; adds a 429 (rate-limited) branch; theme-upload now delegates to it.
#   (2) Three false-success guards: PUSH/REPLACE and SWITCH PLACEMENT set the button to
#   "QUEUED" on r.ok alone, and PURGE (/forget) passed on !r.ok — a proxy 302→200 SSO
#   page is r.ok, so each claimed success though nothing reached motif. They now confirm
#   motif's JSON content-type (PURGE still allows the legit 204) before claiming success.
#   (3) Fixes the v0.51.143 DB-restore discriminator: api() sets e.status even for a proxy
#   413/403 HTML page (e.detail is null there), so the status-only check dumped the raw
#   page for the MOST likely restore failure. Now keys on e.detail (motif error) →
#   e.status (proxy) → neither (network/SSO). Guarded by test_v0_51_144.
# 0.51.145: reverse-proxy audit tag 2/3 — pre-flight size hints on the other two
#   uploads (theme upload already had one). (1) DB restore (500 MiB cap, a real DB
#   nearly always exceeds a proxy body cap): file-pick hint → >500 MB reject / >9 MB
#   "very likely blocked by a proxy, restore on your LAN" amber warn. (2) Import-preview
#   (5 MB cap, direct fetch, previously zero proxy handling): file-pick size hint (>5 MB
#   reject + disable / >1 MB proxy warn) AND routes its error path through the shared
#   describeProxyOrHttpError decoder + a non-JSON-200 guard (kills the raw-HTML slice /
#   "Unexpected token '<'"). Guarded by test_v0_51_145.
# 0.51.146: reverse-proxy audit tag 3/3 — gateway-timeout messaging (429 already
#   landed via the shared decoder in tag 1). The slow DB-admin endpoints (backup
#   create, restore-from-backup, restore-upload) run in the threadpool and usually
#   FINISH even when a reverse proxy 502/503/504-times-out the client — but the client
#   read that as a hard failure and dumped the proxy's HTML page. New gatewayTimeoutNote()
#   reframes 502-504 as "motif may still be finishing — verify before retrying" and the
#   three catches now refresh the backup list / restore-pending banner so a completed-
#   but-timed-out action surfaces instead of tempting a needless retry. Guarded by
#   test_v0_51_146.
# 0.51.147: in-app notification center — backend foundation (build phase 1 of the
#   planned INBOX drawer). New schema v71 `notifications` table + `notify_inbox.py`
#   module (record + list/dismiss/dismiss-all/seen/count helpers). notify.dispatch and
#   dispatch_coalesced record an inbox row for the Additions+FYI event kinds
#   (INBOX_EVENT_KINDS) UNCONDITIONALLY of the per-event Apprise send-toggle — so the
#   inbox surfaces auto-added themes even when that kind's Discord toggle is off; a
#   `_record_inbox` flag on the coalescer's own sends prevents double-counting a bulk
#   burst. Endpoints GET /api/notifications + POST .../{id}/dismiss + .../dismiss-all +
#   .../seen; `notifications_unread` added to /api/stats for the topbar badge. The
#   drawer UI + the plex_item_arrived_themed event are later tags. Guarded by
#   test_v0_51_147.
# 0.51.148: in-app notification center — the INBOX pill + drawer UI (build phase 2).
#   Turns the v0.51.147 plumbing into something you can see: an always-visible INBOX
#   op-pill in the topbar that lights green + shows the unread count (from /api/stats
#   notifications_unread) whenever there are unread notifications, and a slide-in
#   NOTIFICATIONS drawer (reuses the LIVE-OPS drawer shell) listing each notification
#   with a fixed tier stripe (green add · cyan available · amber loss/FYI), emoji, kind
#   phrase, relative time, and per-row × dismiss, plus a CLEAR ALL. Opening the drawer
#   marks the unread set seen (badge → 0). New: bindNotifInbox() in app.js + the .notif-*
#   / .op-notif CSS in ops.css. Grouping (batch_id) + click-through are later tags.
#   Guarded by test_v0_51_148.
# 0.51.149: notification drawer renders stored titles verbatim (fix v0.51.148 double-
#   render). Every inbox notification title already carries its own emoji + descriptor
#   (notify_content formatters: "🎵 Theme added — <item>", "💔 Theme lost — <item>",
#   etc.), so the v0.51.148 drawer's own KIND emoji + phrase sub-line double-rendered
#   against real data (the harness used bare sample titles so it never showed). Now the
#   KIND map is tier-only (event_kind → stripe class), the row is [title][time+×] on a
#   1fr/auto grid, and the title renders verbatim. Dropped the dead .notif-emoji /
#   .notif-sub CSS. A clean bare-title field is the later enrichment tag. Guard
#   test_v0_51_149.
# 0.51.150: in-app notification center — the plex_item_arrived_themed event (build
#   phase 3). New FYI: fires when a GENUINELY-NEW Plex item arrives ALREADY themed by
#   Plex (has_theme=1) that motif doesn't own (no placement / local file). Hooked in
#   plex_enum._upsert_items after the theme-available push, over the rks inserted THIS
#   enum, BASELINE-GATED on updated>0 so a section's first enum (all inserts) stays
#   silent instead of flooding. Per-(media_type, tmdb) 30-day deduped. New
#   notify_content.format_arrived_themed_{title,body,batch_*}; config event
#   plex_item_arrived_themed (default OFF for Discord) + settings toggle +
#   _EVENT_NOTIFY_TYPE entry. OFF for Apprise but records to the in-app INBOX
#   unconditionally (the primary surface). Guard test_v0_51_150.
# 0.51.151: in-app notification center — drawer click-through to the INFO card (build
#   phase 4, enrichment). Per-item notifications now carry the item's identity
#   (media_type / tmdb_id / section_id) so a drawer row clicks through to that row's
#   INFO card via the ?info_open= deep-link (the /queue REPROBE OPEN ROW mechanism);
#   batch digests carry no identity and stay non-clickable. Threaded via a new item_ctx
#   param on notify.dispatch + single_item_ctx on dispatch_coalesced (the ItemContext
#   dict already has the ids), passed to record_notification which stores them on the
#   (already-nullable) notifications columns. Wired at all 8 per-item inbox dispatch
#   sites (4 worker coalesced + 4 plex_enum single). list_notifications returns the ids;
#   app.js row-body click builds the deep-link (× still dismisses). Guard test_v0_51_151;
#   click-through verified end-to-end in a browser harness. Smart batch grouping + a
#   prune job are the remaining Phase-4 items.
# 0.51.152: in-app notification center — inbox retention prune (build phase 4). The
#   `notifications` table was record-only (grew forever). New notify_inbox.
#   prune_notifications deletes dismissed rows >7 days old (the drawer only shows
#   UNDISMISSED rows, so dismissed = pure DB weight) + any row >30 days old (rotation
#   cap, mirrors events_prune). Wired as a daily scheduler job _prune_notifications at
#   03:12 UTC (between events_prune 03:10 and prune_history 03:15). Guard test_v0_51_152.
# 0.51.153: topbar — the INBOX pill gets its OWN grid column (the user: keep it visible
#   when a sync/refresh op-mini inflates the status cluster). Moved the pill out of
#   .topbar-status to a direct .topbar grid child before the cluster; desktop grid 3→4
#   cols (brand · nav(1fr) · INBOX · status), gap gap-7→gap-5 so the 4th column nets the
#   nav ~4px MORE room (no extra wrap). Mobile (≤600px) gives INBOX its own row-1 area
#   ("brand inbox status" / "nav nav nav"). Verified in a topbar harness at desktop width
#   (INBOX always visible with op-mini running, no horizontal overflow). Guard test_v0_51_153.
# 0.51.154: notification center — smart batch grouping in the drawer (build phase 4,
#   final). A burst of same-kind notifications (a bulk sync's adds, a mass theme-lost)
#   collapses into ONE expandable group row so the drawer isn't flooded — client-side
#   in bindNotifInbox (groupRows: adjacent same-event_kind runs within 10 min, length
#   >=3), so no backend change + it PRESERVES v0.51.151 click-through (children stay
#   individually clickable + dismissable when expanded). Group header = tier stripe +
#   emoji + "N <noun>" + caret + dismiss-all ×; per-child × ticks the count, group ×
#   clears the unit. Runs of 1-2 stay individual. .notif-group* CSS in ops.css.
#   Verified in a browser harness (burst→group, expand, child/group dismiss). Guard
#   test_v0_51_154. Completes the notification center (see the plan memory).
# 0.51.155: display / UI scaling (the user: motif renders small on a 2560×1440 panel;
#   125% browser zoom "feels normal" + de-pixelates posters). The size scale is px, so
#   the UI doesn't scale with the display, and the poster sharpness is an image-
#   rasterization effect only browser-zoom-style scaling fixes (rem would miss images).
#   Fix = a root `zoom` (scales text + layout + re-rasterizes posters): app.css AUTO
#   ladder (--ui-zoom media queries: ≥2400px→1.25, ≥3600px→1.5; laptops/1080p/OS-scaled
#   stay 1×) + a MANUAL override in Settings→VISUALS (Auto/100/110/125/150%) that sets
#   an inline html.zoom winning over the ladder, persisted localStorage 'motif:uiScale',
#   applied pre-paint (base.html MOTIF_APPLY_UI_SCALE) + wired by app.js bindUiScale.
#   Verified in a harness that zoom keeps sticky/fixed/vw correct + no overflow. Guard
#   test_v0_51_155.
# 0.51.156: theme loudness — the read-only MEASUREMENT primitive (loudness feature,
#   Phase 0 / tag A). New app/core/loudness.py: measure_loudness() shells out to
#   ffmpeg's EBU R128 loudnorm filter in ANALYSIS mode (print_format=json -f null,
#   no re-encode / no file written) + _parse_loudnorm_json() reads the integrated
#   loudness / true-peak / loudness-range from ffmpeg's stderr JSON. Best-effort
#   (missing ffmpeg / non-audio / timeout → None, never raises; class-9 warn-once).
#   The leaf everything else builds on — the LOUDNESS AUDIT op (stores per local_files
#   row) + eventual normalize/undo (engine TBD post-audit: mp3gain vs loudnorm) are
#   later tags. ffmpeg is container-only, so the parser is unit-tested vs a real
#   loudnorm sample + the subprocess stubbed. Guard test_v0_51_156. Motivation: the
#   download→transcode pipeline does ZERO loudness conditioning → mixed-source hover
#   volume is the #1 perceived-quality defect (see motif_loudness_normalization_plan).
# 0.51.157: theme loudness — schema v72 audit STORE (loudness feature, Phase 0 / tag B).
#   local_files gains 5 additive columns (loudness_i / loudness_tp / loudness_lra +
#   loudness_measured_at / loudness_measured_sha256) to persist the read-only ffmpeg
#   measurement. sha256 pins WHICH bytes were measured so a re-download/replace makes
#   the row's measurement stale (sha mismatch) + re-measurable. Migration _migrate_v71
#   _to_v72 is purely additive (idempotent via _add_column); existing rows read NULL
#   (never measured) until the audit op (tag C) runs. Nothing here touches a file.
#   Guard test_v0_51_157. Next: tag C = the loudness_audit background op (iterate
#   local-bytes rows, measure, store) + Diagnostics trigger + ops.js maps.
# 0.51.158: theme loudness — the LOUDNESS AUDIT op + runner + trigger (Phase 0 / tag C).
#   New app/core/loudness_audit.py walks every local-bytes theme (every local_files
#   row IS motif's canonical file → T/U/A/M + plex_cloud backups; pure-P has no
#   local_files row → excluded for free), measures each via loudness.measure_loudness
#   (ffmpeg, no re-encode, no file written) over a ThreadPoolExecutor, and stores the
#   v72 columns. sha256-keyed/incremental (skip rows whose loudness_measured_sha256 ==
#   file_sha256); edition-scoped per-row store. Wired as an in-memory page-scoped
#   background op (_LOUDNESS_AUDIT_STATE + POST/GET /api/admin/loudness-audit/
#   {start,status}) on the ORPHAN-SCAN template — deliberately NOT an op_progress kind,
#   so NO op_progress.kind CHECK-widen migration (CLAUDE.md's most dangerous op, the
#   v1.18.0 data-loss class) + no ops.js maps + no row-refresh exclusion (mutates no
#   library-row chip state). Trigger = // LOUDNESS AUDIT block in Settings→Diagnostics
#   (bindLoudnessAudit polls status → live "measuring X / N…" + summary). Guard
#   test_v0_51_158 (module logic + endpoint contract). Next: tag D = the report view
#   (histogram + outliers + target-preview slider) reading the stored columns.
# 0.51.159: theme loudness — the LOUDNESS AUDIT report view (Phase 0 / tag D, finishes
#   Phase 0). loudness_audit.build_report() aggregates the stored v72 measurements into
#   distribution stats + a fixed-bin histogram + loudest/quietest-40 outliers (title/year
#   + PK) + a compact [loudness_i, true_peak] array; GET /api/admin/loudness-report serves
#   it (pure read). New /admin/loudness dashboard (loudness.html, mirrors /admin/orphans)
#   hosts the RUN AUDIT trigger + renders: stat tiles (median/spread/min/max), a CSS-bar
#   histogram (median bin highlighted), a CLIENT-SIDE target-preview slider (drag a target
#   LUFS → live louder/quieter/within-±1dB/peak-limited counts, a pure dry-run modelling a
#   true-peak ceiling — zero files touched), and quietest/loudest outlier tables deep-
#   linking to INFO cards (info_open param). bindLoudnessReport in app.js; the Settings→
#   Diagnostics block becomes a link-out (// OPEN LOUDNESS AUDIT ▸). Themed neutral chrome
#   (--accent, not source-encoded — theme-SPLIT). Guard test_v0_51_159. Engine choice
#   (mp3gain vs loudnorm) + Phase 1 normalize/undo stay a post-Phase-0 gate.
# 0.51.160: loudness audit — real-world-testing follow-ups (the user ran the audit against
#   the full prod library). (1) LIVE report: the /admin/loudness distribution / histogram /
#   outliers only refreshed on completion — now bindLoudnessAudit's poll loop refreshes the
#   report every ~3s while measuring (throttled — the report query scans every measured row),
#   and bindLoudnessReport seeds the target slider to the median ONCE so a live refresh can't
#   yank it off the operator's drag. (2) MEASURE robustness: many prod theme.mp3s failed with
#   ffmpeg rc=254 / "no measurement parsed" (Star Wars, Amadeus, Empire, …) — added `-vn` to
#   the loudnorm command to drop an embedded cover-art / mjpeg stream (the classic
#   loudnorm-on-downloaded-mp3 abort; a no-op on audio-only files so no regression), + the
#   parse-failure debug log now includes ffmpeg's last stderr line so any remaining failure
#   is diagnosable. Guard test_v0_51_160.
# 0.51.161: topbar — move the INBOX pill to between the IDLE status pill and // HELP (the
#   user: "between the idle status pill and the //HELP with proper spacing"). Moved the
#   button back INTO .topbar-status (right after IDLE, before help-toggle; spaced by the
#   single cluster flex gap), reverting the v0.51.153 own-column. Still safe from op-mini
#   push-out — a running-job op-mini renders to INBOX's LEFT (before IDLE) on desktop and
#   drops to a bottom strip on mobile, so it grows away from INBOX, never over it. Desktop
#   grid back to 3 cols (auto 1fr auto); mobile row-1 back to "brand status" (INBOX rides
#   the cluster, its own grid-area removed). Updated test_v0_50_88 / test_v0_51_3 /
#   test_v0_51_153 pins.
# 0.51.162: loudness report — a RECOMMENDED target LUFS (the user: "create a recommended
#   loudness"). loudness_audit.build_report() now returns `recommended` = the library
#   median clamped to a comfortable ambient-hover band [-23, -18] + rounded to 0.5. For a
#   loud/clipping library (median > -18, e.g. the user's ~-15.9) it lands at -18:
#   attenuation is lossless-safe + pulls hot peaks back under 0 dBTP, without boosting the
#   quiet tail into its peak ceiling. The /admin/loudness report leads the stat strip with
#   a // RECOMMENDED tile (bright accent), SEEDS the target-preview slider there (not the
#   raw median), names it in the slider hint, + adds a // USE RECOMMENDED snap-back button.
#   bindLoudnessReport in app.js. Guard test_v0_51_162. The slider still lets the operator
#   override — the recommendation is only where the dry-run starts.
# 0.51.163: loudness report — FIX: the distribution never appeared during a real prod scan
#   (the user: "still not seeing the breakdown"). Root cause (reproduced): a SILENT theme
#   measures as -inf LUFS (ffmpeg loudnorm "input_i":"-inf"), and -inf is poison —
#   build_report crashed on math.floor(-inf) (OverflowError → the /api/admin/loudness-report
#   500'd) AND -inf serialises to "-Infinity" (invalid JSON the browser rejects). Either
#   killed the report; data-dependent, so it only bit after a silent theme got measured
#   (worked at 484 rows, broke later). Three layers: (1) loudness._parse_loudnorm_json
#   rejects any non-finite figure → -inf is a measurement GAP, never stored; (2) build_report
#   filters non-finite loudness_i from the scan/histogram/outliers + nulls a non-finite
#   true_peak in the values array → robust to -inf ALREADY in the DB, valid strict JSON;
#   (3) bindLoudnessReport no longer swallows a failed fetch silently (class-9) — it reveals
#   the section + "retrying…" + console.error, and the live poll recovers. Guard
#   test_v0_51_163 (reproduces the -inf crash + the strict-JSON + parse-reject).
# 0.51.164: theme loudness NORMALIZATION begins (Phase 1/1) — the mp3gain apply/undo
#   primitive + a reversibility PROBE. Engine = mp3gain (the user picked it off the real
#   spread: median -14.5 LUFS, wide, lots of +dBTP clipping → attenuation-dominant, which
#   mp3gain does losslessly + reversibly). Dockerfile adds mp3gain (tolerant install so a
#   repo hiccup can't brick the image). app/core/loudness_apply.py: gain_steps_for_target
#   (LUFS delta → mp3gain -g steps, boost capped by true-peak headroom so it never clips;
#   we use ffmpeg's LUFS/peak, NOT mp3gain's ReplayGain analysis), apply_gain/undo_via_tag,
#   + probe_mp3gain (copies ONE real theme to a temp file, proves apply->undo is BIT-EXACT
#   via sha256 without touching the real file). POST /api/admin/mp3gain-probe (threadpool)
#   + a // PROBE MP3GAIN button in Settings->Diagnostics. mp3gain is container-only so
#   nothing is trusted blind. Guard test_v0_51_164. NEXT: per-item // NORMALIZE / // UNDO
#   on the info card once the probe passes.
# 0.51.165: loudness probe v2 — FIX a false-negative in the v0.51.164 probe. On the user's
#   real files it read ok=false with inverse_g/undo_tag NOT bit-exact — but the criterion
#   was WRONG: mp3gain appends an APEv2 undo tag, so a restored FILE always differs from the
#   original by that tag even when every audio sample is restored. The safety layer is the
#   decoded PCM, not the container bytes. probe_mp3gain now decodes both files with ffmpeg
#   and compares the SAMPLES; tests the ATTENUATE direction production uses (target below the
#   loud median → global_gain moves away from the clamp ceiling a boost can hit) and keys the
#   verdict on that path; surfaces boost reversibility separately + corroborates at the byte
#   layer that the file diff is tag-only. app.js/settings.html verdict + copy follow.
#   Guard test_v0_51_165 (faithful stub models mp3gain's global_gain-clamp irreversibility +
#   ffmpeg decode). Still read-only wrt every real theme; re-run // PROBE MP3GAIN to re-gate.
# 0.51.167: CANONICAL HEALTH — find + repair themes missing from motif's storage. The loudness
#   audit surfaced ~14 items whose canonical theme.mp3 was GONE from disk (ffmpeg rc=254 "No
#   such file"); a vanished/0-byte canonical is a storage-health problem, not loudness. (1)
#   verify_canonical_health + _annotate_canonical_state now treat a 0-BYTE theme.mp3 as missing
#   (a failed download that left a stub is functionally gone — the downloader itself removes +
#   re-downloads one), so both the stored canonical_present flag and the live red DL dot /
#   dl_pills=broken filter flag it. (2) app/core/canonical_health.py classifies each
#   canonical_present=0 row by the download worker's own URL resolution: RE-DOWNLOADABLE (a
#   recorded ThemerrDB / user-override URL → one-click re-fetch) vs CANONICAL MISSING (upload /
#   adopt / plex_cloud — no re-fetchable URL, so re-downloading TDB would SWAP the operator's
#   content; surface for manual re-place). Edition-scoped throughout. (3) /admin/canonical-health
#   dashboard (Diagnostics card, mirrors LOUDNESS AUDIT): RUN CHECK re-stats fresh, REPAIR ALL
#   re-downloads the re-downloadable set (force-place, same as /redownload), the missing set
#   deep-links to each INFO card (SET URL / UPLOAD MP3 / RESTORE FROM PLEX — a surviving Plex
#   copy is hinted). Endpoints /api/admin/canonical-health/{report,check,repair};
#   bindCanonicalHealth in app.js. Guard test_v0_51_167.
# 0.51.168: theme loudness NORMALIZE / UNDO — Phase 1's first tag that MUTATES a theme, on
#   the smallest safe surface. // PROBE MP3GAIN passed on the real library (attenuate AND
#   boost both restore the AUDIO bit-exactly; the only byte diff is mp3gain's own undo tag),
#   so the engine is proven and the audition can start. Schema v72->v73: five additive
#   local_files columns (norm_state / norm_gain_db / norm_target / norm_at +
#   norm_orig_sha256 — the pre-normalize sha, so UNDO can PROVE it restored the original
#   bytes rather than assume it). loudness_apply.normalize_file (gain -> re-measure, steps==0
#   is a clean no-op) + undo_file (mp3gain -u -> re-measure, verifies bit-exact vs
#   norm_orig_sha256). Endpoints /api/admin/loudness/{normalize-one,undo-one} — threadpool
#   (class-12), edition-scoped writes on the full local_files PK so a normalize never bleeds
#   onto a sibling edition; a failed re-measure NULLs measured_sha (audit re-measures) rather
#   than storing a gap as truth (class-9). // AUDITION NORMALIZE in Settings->Diagnostics
#   auto-picks the loudest measured, un-normalized, HARDLINK-placed row: a hardlink shares the
#   canonical inode with the Plex sidecar, so Plex plays the normalized theme immediately and
#   the undo is equally live — an honest "hear it, then undo it". One theme at a time (the
#   button flips to // UNDO). Guard test_v0_51_168. NEXT: per-item INFO card, then bulk.
# 0.51.169: code-review follow-ups on the v0.51.168 normalize path, before the first real
#   audition. (1) STALE-MEASUREMENT: normalize-one never checked that the loudness it
#   derives gain from was measured at the CURRENT bytes — a re-download since the audit
#   would drive the gain off a stale number (worst case a big boost onto an already-loud
#   file). The auto-pick now carries the same `loudness_measured_sha256 = file_sha256` key
#   rows_needing_measure uses, a body-named row gets the same gate in Python, and
#   normalize_file takes expect_sha + REFUSES if the bytes on disk don't hash to it.
#   (2) RELOAD STRANDED A NORMALIZED THEME: the undo target lived only in a JS variable, so
#   reloading Settings hid // UNDO while the theme stayed normalized — no UI path back,
#   which breaks the audition's whole promise. New GET /api/admin/loudness/normalized +
#   refreshNormalizedState() re-arms // UNDO from the DB on load (and after a lost race).
#   (3) RACE: the norm_state guard was read on a since-closed connection; the UPDATE now
#   carries `AND norm_state IS NULL` so the WRITE is the guard — the first normalize keeps
#   the true norm_orig_sha256 (undo still verifies bit-exact, since mp3gain's undo tag
#   accumulates), and the loser reports honestly instead of returning wrong numbers.
#   (4) a non-dict JSON body 500'd on .get → isinstance guard. (5) normalize_file promised
#   "never raises" but a None measured_i raised TypeError → guarded at the leaf.
#   (6) one now_iso() per operation instead of two. Guard test_v0_51_169 (behavioral:
#   stale rows excluded, expect_sha refusal, race guard, normalized lookup).
# 0.51.170: undo verified the WRONG LAYER — a correct restore reported "not bit-exact" on
#   the first real audition. The operator normalized a tv theme (-5.15 -> -18.7 LUFS, -9
#   steps), pressed // UNDO, and got bit_exact=false even though the fresh re-measure came
#   back -5.15 / +2.6 dBTP — identical to the original, to the decimal. Cause: undo_file
#   compared the whole-FILE sha256 against norm_orig_sha256, but mp3gain leaves its APE tag
#   behind, so a restored file can NEVER byte-match the pre-normalize file. v0.51.165's
#   probe had already MEASURED this on these very files (restored_file_bit_exact=false +
#   restored_diff_is_tag_only=true) — the same layer mistake v0.51.164 made, repeated on the
#   undo path after being fixed in the probe. Fix: schema v74 adds
#   local_files.norm_orig_pcm_sha256 (the DECODED-PCM hash of the original, taken before
#   gain is applied); undo_file compares the restored samples against it and reports
#   `audio_restored` as the verdict, with file_bit_exact demoted to informational (expected
#   false). Legacy rows read NULL -> audio_restored=None (unknown), never a false alarm.
#   ALSO: the probe only ever tested ±2 steps while the first real normalize applied -9 —
#   magnitude is exactly what decides global_gain clamping, so `ok` now additionally
#   requires attenuate_deep_reversible_audio (-9 steps ≈ -13.5 dB, the production case).
#   Guard test_v0_51_170. RE-RUN // PROBE MP3GAIN: it now proves the deep path.
# 0.51.171: MEASURE what Plex actually serves — the audition's core claim was never
#   verified. // PROBE MP3GAIN came back fully green on the real library
#   (attenuate_deep_reversible_audio=true, so the -9-step path IS reversible;
#   audio_restored=true on undo), but the operator normalized -13.5 dB and could not hear a
#   difference in Plex. -13.5 dB is a 4-5x loudness drop, so the likely explanation is that
#   Plex never played the new bytes: v0.51.168 claimed "hardlink -> Plex plays it
#   immediately", which is right about the inode but assumes Plex reads theme.mp3 at
#   PLAYBACK. It doesn't — Local Media Assets INGESTS the sidecar into Plex's own store at
#   scan time (hence the metadata://themes/<sha1> entries keyed by CONTENT hash), so
#   mutating the sidecar changes nothing Plex plays until a refresh re-runs the agent.
#   Rather than crank the gain and judge by ear (same conclusion, more mutation, subjective
#   call), measure it: PlexClient.fetch_theme_bytes (full GET, no Range — the existing probe
#   caps at 4KB) + POST /api/admin/loudness/plex-serving resolves the edition-scoped
#   rating_key, GETs the SELECTED theme entry's bytes, measures them with ffmpeg, and
#   compares against the canonical's stored loudness. // WHAT IS PLEX SERVING? in the
#   audition block. Read-only wrt Plex and the theme; threadpool (class-12). This also
#   answers the placement question Phase 2 bulk depends on. Guard test_v0_51_171.
# 0.51.172: CANONICAL HEALTH — centre the SRC column (the user: "can we make the SRC list
#   centered in it's column"). The headers were already centred (.table thead th), so only
#   the VALUES were left-aligned against a centred label. New .col-src primitive follows the
#   documented col-X convention (DESIGN_SYSTEM: "Each declares width + text-align"), sized
#   110px to the longest value (THEMERRDB) so URL centres against the same budget. Applied
#   to both canonical-health tables (re-downloadable + canonical-missing), th + td.
#   .table td sets no text-align, so the single-class rule wins on the cells uncontested.
# 0.51.173: CONFIRMED by measurement, and the propagation probe. // WHAT IS PLEX SERVING?
#   on the real library: canonical -18.7 LUFS, Plex serving -5.15, serving_normalized=false,
#   entry_uri=metadata://themes/<sha1>, entries=1. So Plex plays its INGESTED copy, not the
#   file — mutating a hardlinked sidecar changes nothing it serves, and v0.51.168's "hardlink
#   -> Plex plays it immediately" was false. The operator's ears were right; three tags of
#   green checks weren't. Remaining unknown = how to propagate. Two candidates: REFRESH
#   (native, keeps the row a sidecar row, UNPROVEN) vs RE-UPLOAD (v1.18.35/36 PROVED Plex
#   content-dedupes by SHA-1 + auto-selects, but makes it an upload:// entry). Try the native
#   one and MEASURE: POST /api/admin/loudness/plex-reread refreshes the rk then POLLS the
#   measurement (refresh is async) and reports whether Plex actually flipped to -18.7.
#   // MAKE PLEX RE-READ IT in the audition block. The v0.51.171 fetch+measure block is now a
#   shared _measure_plex_serving helper both probes call. Guard test_v0_51_173.
# 0.51.174: REFRESH IS OUT — measured. // MAKE PLEX RE-READ IT on the real library:
#   refreshed=true, waited 20s, and re-checked minutes later — Plex STILL -5.15 vs a -18.7
#   canonical, SAME metadata://themes/<sha1> entry, entries=1. plex.refresh already sends
#   PUT /refresh?force=1 with an /analyze fallback (the strongest native primitive motif
#   has), and its own docstring quotes Plex: it's for "Added local media assets". That's the
#   mechanism — Plex's agent ADDS assets it lacks; it will not replace a theme entry it
#   already holds. So a changed sidecar is a dead letter and the sidecar can never carry an
#   update. Remaining path = the PROVEN one: POST the bytes to /themes; Plex content-dedupes
#   by SHA-1, so NEW bytes make a new entry and auto-select it (v1.18.35 probe / v1.18.36
#   production). Bonus: UNDO falls out free — re-uploading the ORIGINAL bytes hashes back to
#   the EXISTING metadata:// entry, so Plex re-selects it instead of accumulating junk.
#   POST /api/admin/loudness/plex-push uploads the normalized canonical + POLLS the
#   measurement (never trusts the 2xx). // PUSH NORMALIZED TO PLEX. Still a probe: it proves
#   the propagation step the per-item card + bulk will both depend on. Guard test_v0_51_174.
# 0.51.175: the push 500'd — and it's a KNOWN ceiling my new code didn't guard. Plex 500s
#   on a theme POST over ~10MB; motif has known this since v1.21.99 (the operator's Watchmen
#   re-upload 500'd and LPS "looked like it did nothing") and guarded it at THREE sites:
#   worker._PLEX_THEME_UPLOAD_CEILING_MB, orphan_scan._UPLOAD_CEILING_BYTES, and an inline
#   copy in set_active_theme_via_reupload. v0.51.174's loudness push was a FOURTH upload
#   path written without the check — CLAUDE.md's mirror-drift class exactly (a rule at N
#   sites; the new site misses it). Fixed at the ALTITUDE instead of adding a 4th copy: one
#   module-level plex.THEME_UPLOAD_CEILING_BYTES, enforced inside upload_collection_theme
#   (the chokepoint every caller shares) so it refuses the doomed POST with a clear
#   over_ceiling message; the inline copy is retired to the constant. ALSO: v0.51.174 only
#   reported bytes_sent on SUCCESS, so a real 500 arrived missing the one number that
#   diagnoses it — plex-push now reports bytes_sent + ceiling_bytes + over_ceiling on EVERY
#   path, and distinguishes over-ceiling from a 500 that is NOT the size cap.
#   Guard test_v0_51_175.
# 0.51.176: SIZE THE PROPAGATION CONSTRAINT before designing around it. The audition's
#   first target was 10.5MB and blew Plex's ~10MB upload ceiling — and re-upload is one of
#   only TWO ways to tell Plex a theme's bytes changed, the other (refresh) being measured
#   dead. So the ceiling decides the design, and guessing its blast radius is exactly the
#   mistake that cost the last four tags. loudness_audit.upload_ceiling_counts (read-only,
#   off local_files.file_size) counts over/under/unknown + the largest theme; build_report
#   ships it as `upload_ceiling`; the report shows an "over 10MB (un-pushable)" tile.
#   unknown_size is surfaced separately, NOT folded into under — a NULL size is an unknown,
#   not a small file (class-9). ALSO reframed by the operator's catch: normalize-at-download
#   only avoids propagation for the FIRST delivery — every later UNDO / re-apply of a
#   download-normalized theme still has to tell Plex, so propagation is a permanent
#   first-class primitive, not a one-off backfill chore. (The revert-to-source ask needs no
#   new machinery: mp3gain's undo tag already carries the original IN the file — the reason
#   it beat loudnorm — and v0.51.170 proved the restore is audio-bit-exact.)
#   Guard test_v0_51_176.
# 0.51.177: probe the last propagation candidate — delete the theme selection, unlock the field the delete bolts shut, then refresh so Local Media Assets re-ingests the changed sidecar; verdict off a re-measurement + the selected flag, and only ever on an under-ceiling theme re-upload could recover.
# 0.51.178: read Plex's theme-field lock flag instead of inferring it from a 200 — v0.51.177's unlock may have been a silent no-op, and if sidecar rows are locked by default that lock explains v0.51.173's dead refresh and puts a ceiling-free unlock+refresh back on the table.
# 0.51.179: fix the control picker — it took ONE candidate ordered by tmdb_id, and synthetic orphan ids are NEGATIVE, so it always landed on an orphan with no plex_items row and reported "no control" on a library full of them; now resolves the rating_key in SQL, skips orphans, and samples several rows.
# 0.51.180: propagation settled — delete+re-detect and refresh are MEASURED dead and are removed (the delete only cleared the selection, stranding the item and buying nothing); re-upload is the mechanism. Chases the lead the lock probe exposed: motif locks the theme field on every delete and never unlocks it, and LET PLEX SERVE / SWITCH api-to-file both delete then rely on an agent writing.
# 0.51.181: stop the lock probe asserting causation it never measured — one locked+themeless row is a CANDIDATE, not a cause (rk 3487 was a broken canonical AND a stale-upload suspect); size the lead with a local library-wide count, carry enough per row to triage it, and settle it per row with an unlock INTERVENTION.
# 0.51.182: settle the lock lead on the case that matters — an AGENT-SERVED row proves Plex has a theme to give, so delete it (the LPS operation) and refresh locked-then-unlocked to isolate the lock as the only variable; bytes captured first. Also refuses an unlock subject that already HAS a theme (v0.51.181 accepted rk 497736 and answered an unanswerable question).
# 0.51.183: close the lock arc (measured dead: the agent restored nothing locked OR unlocked) and retire its probes; ask the last question the v0.51.182 result raised — Plex's agent never re-selects after a delete, so does Plex play an UNSELECTED collection entry (LPS always worked) or nothing (LPS strands items)? Reads the serving association, not the selected flag.
# 0.51.184: v0.51.183 printed "REAL BUG" off ONE row whose entry it never checked was alive — a dead entry serves nothing regardless of the flag, which is the same symptom with an innocent cause. Verify the entry has playable bytes before it counts, sample the whole cohort instead of 6, and say n=1 out loud.
# 0.51.185: normalize now PROPAGATES. Plex plays its own ingested copy, so a gain applied to the file was inaudible until you clicked PUSH by hand — the operator normalized -13.5dB and heard nothing. Re-upload is the only step that works (v0.51.176-183), so normalize does it and verifies by re-measuring. Schema v75 records the pre-normalize entry so UNDO can re-select it instead of minting a new one every cycle.
# 0.51.186: UNDO now puts Plex back too, by re-selecting the entry recorded at normalize time (pushing the restored file would mint a new entry every cycle — the APE tag changes its hash). Before this, undo reverted the FILE and left Plex serving the normalized upload: the diverged state rk 261711 was actually found in. Status lines lead with whether Plex got it; the two answered probes retire.
# 0.51.187: undo SELF-CORRECTS. Re-selecting "what Plex served before" is only right if Plex matched the FILE back then — rk 261711 proved it might not: its recorded entry was itself a normalized upload, so undo restored Plex to -18.75 while the file went to -5.2, and the loudest-raw auto-pick grabbed it straight back. Detecting that without fixing it is half a fix; now it pushes the restored file when the re-select does not match.
# 0.51.188: normalize-at-download. Condition a theme BEFORE it is placed — the cheap half, because Plex has never seen it, so the only copy it ever ingests is the conditioned one and no propagation is needed. Default OFF; a loudness step never fails a download; a silent theme (-inf) is left raw rather than gained by infinity.
# 0.51.189: surface normalize-at-download in Settings (it was YAML/env-only). Two wiring traps caught by reading rather than shipping: `loudness` had to join _ALLOWED_TOP_LEVEL or every save 400s (the v1.13.26 placement bug), and the SAVE button had to name the section or the controls render and never save. Both now have standing lints that walk the config + the template.
# 0.51.223: ultra-review correctness fixes on the edition-exact arc. (#1) enrich_item defaulted edition_key to '' and stamped it unconditionally, so title-level notices (new_tdb_theme_available / plex_item_arrived_themed) recorded '' not NULL and their multi-edition click-through scoped to the standard cut instead of the v0.51.218 picker — now stamps only when the caller names a cut. (#2) the level/undo + measure re-open dropped editionKey, so a card reached by edition bounced to the picker on re-open (pick→act→re-pick) — now threads it. (#3) the picker guarded only the loudness rows; the hero LUFS chip, file & placement block, playback headline and edition scope-chip still showed an arbitrary sibling cut — all now suppressed when the cut is ambiguous.
# 0.51.224: close ultra-review #4/#5 (test-hygiene). #4: the v0.51.222 dashCountUp migration used 2-space end-anchors on a 4-space-nested function that is the LAST of its kind, so slice_to_next fell through to end-of-file (7685 chars) — the exact vacuous-window the sweep exists to kill, shipped inside the sweep; its four asserts passed only by file-unique luck. Re-anchored on the 4-space `dashCountUp();` invocation (1473 chars, tight). #5: the v0.51.222 ratchet's _measure only sees fixed `SRC[X:X+N]` windows, so a slice_to_next EOF fall-through is invisible to it — added an AST guard that resolves every static slice_to_next call against its real source and fails on any whose end-anchors never match (runs to EOF).
# 0.51.225: ultra-review LOW cleanups. #6: api_item ran a DISTINCT edition_key scan that duplicated the section-scoped local_files fetch it does anyway — now the ambiguity picker's choices are derived from that already-read list (sorted+deduped), one query instead of two on every rk-less info open. #10: added a mirror-drift lint pinning that notify.dispatch and notify.dispatch_coalesced pass the SAME item-identity kwargs to record_notification (media_type/tmdb_id/section_id/edition_key), so a future field added to one path but not the other fails at test time (v0.51.220's edition_key had to go into both by hand). #8 (drop the derived edition_ambiguous field) was assessed and declined: the server already computes it for its own edition-label suppression, so it is the single source of the >1 threshold — dropping it would duplicate that threshold across Python and JS, not remove redundancy.
# 0.51.226: explain the blank loudness meter on Plex-served (P) rows. The row meter is derived from motif's LOCAL copy of the theme, so a Plex-served row with no backup has no reading and the cell is blank (the operator asked why Predators / The Predator show none). Rather than mark the ~3,883-row P majority — the v0.51.192 "presence IS the signal" rule bans marking the boring bulk — enrich the SRC=P pill's own hover text to say so + point at // BACKUP THIS THEME. Gated on !it.loudness_marker so a BACKED-UP P-row (TB/BK/plex_cloud, which does have a local file and does show a meter) keeps the plain label.
# 0.51.227: ultra-review of the v223-226 diff found ONE defect — a phantom guard I shipped in v0.51.225. test_v0_51_218's section-scope pin sliced `slice_between(API_PY, "elif section_id:", "else:")`, but that anchor's first occurrence is the WORD inside a `# … the `elif section_id:` branch …` comment BEFORE the code, so the slice spanned an unrelated branch and passed even if the real branch's scope were gutted (proven by mutation). Replaced the source-pin with a behavioral two-section check (a cut only in section 2 must not surface in section 1's picker) that fails when the derivation drops its section scope. Also hardened the v0.51.222 slice ratchet with a new lint that flags any slice_between/slice_to_next start anchor resolving into a comment-only line — catches this wrong-occurrence class mechanically (verified it flags the old anchor).
# 0.51.228: holistic 8-lane audit fix wave (local ultrareview --fix). SEVEN confirmed defects, each verified against source first. (1) app.js bulk ADOPT+LPS: the EIGHTH SRC-axis drift site — a bare `!it.media_folder` reads `!''` as TRUE so plex_upload rows were ADOPTed then UNPLACEd, tearing down motif's own API upload; the v1.19.38 lint only walks awaitingApproval declarations so a plain .filter() was invisible to it. (2) adoptLpsCount counted rows the handler drops (it needs the theme linkage for the /unplace URL) — the badge said (N) and the click did fewer. (3+4) plex.py: the THIRD enumeration-truncation door — v1.22.29 closed short-page and v1.23.64/.95 empty-page by RAISING, but `container_size <= 0` still broke silently, returning a truncated walk with no error so the v1.18.89 reaper DELETEd live rows; collections were worse (its >50-row mass-abort can never trip on a typical section). Now raises when totalSize contradicts the short walk. (5) adopt.py destroy-then-fail: unlink() then os.link(), so if the link AND its copy2 fallback both failed the live canonical was already gone while local_files still pointed at it — now stages into a temp + atomic os.replace like worker.py/placement.py. (6) sync.py: the withheld-URL branch (user override wins) still advanced tdb_content_fingerprint, the per-item cursor the fast path skips on, so the stale TDB url was pinned FOREVER — baseline-advance class on a per-row cursor; reproduced by execution. (7) api.py DELETE SIDECAR resolved the folder edition-BLIND, unlinking an arbitrary sibling edition's LIVE theme.mp3 while reporting deleted:true — now edition-scoped and refuses (409) rather than guess.
# 0.51.229: audit wave 2 — data-integrity. (1) sync.py's two destructive `DELETE ... WHERE NOT EXISTS` sweeps (_sweep_orphan_user_overrides, _prune_stale_pending_updates) had NO abort cap and run on EVERY sync incl. no-change ones. This is the v1.18.10 class BY NAME: v1.18.0 wiped local_files+placements, the next sync's orphan sweep found 98 user_overrides whose presence-EXISTS all failed and DELETEd those too. Every sibling sweep got a guard; these two never did, and pending_updates has NO recovery walker at all so its mass-prune is unrecoverable. Both now DELETE inside a transaction and ROLL BACK when the kill is both >50 rows and >20% of the table (the reaper's idiom) — measuring the DELETE's own rowcount, so the guard and the delete can never drift onto different predicates. Routine cleanup is untouched (verified). (2) api_replace_item never DECLARED section_id, so FastAPI silently dropped the ?section_id=... app.js has always sent (with a comment saying it scopes the PUSH) — the worklist was edition-scoped but SECTION-WIDE, fanning a force-place across every section holding a local_files row and silently re-installing a sidecar on a row left in LET PLEX SERVE. Now scoped at the worklist SELECT, the existing-placements read and the placements DELETE; unscoped callers are byte-identical.
# 0.51.230: audit wave 3 — edition-blind READ joins. Five sites joined placements->local_files on (media_type, tmdb_id, section_id) only, but edition_key is in BOTH PKs (schema v63), so every placement fanned across every edition's local_files row and SUM/COUNT multiplied — the v1.14.36 cartesian bug re-opened on the edition axis (the inline comment there still documented only the v1.14.36 SECTION-axis fix). Measured on a 2-edition title with two 100B copies: 400 bytes / 4 rows before, 200 / 2 after. Hits the dashboard STORAGE copies KPI, /api/public/stats (Homepage widget) and /api/storage/copies — whose entire purpose is telling the operator how many bytes reorganising the share would reclaim, so the number they act on was wrong. Also: sql_missing_count joined local_files TITLE-wide while the DOWNLOAD MISSING action it mirrors is section+edition scoped (v1.11.0/v1.23.65), so a title downloaded only in the standard section reported 0 missing on the 4K tab while the button would enqueue it — now scoped to match.
# 0.51.231: audit wave 4 — the silent-failure cluster. (1) login_post ran bcrypt (rounds=12, ~235ms of pure CPU) INLINE in an async handler, freezing the single asyncio loop so every concurrent /api/stats, /api/library, /api/progress and /healthz stalled with it — and an attacker could drive it to the rate-limit ceiling; now run_in_threadpool (class 12; the standing lint can't see it, its blocklist is PlexClient methods, nothing CPU-bound). (2) notify._prepare_attachment leaked its mkstemp'd jpeg on any post-download failure (ffmpeg TimeoutExpired, getsize FileNotFoundError) and logged only at DEBUG — a bulk theme_added burst leaked one per item; now unlinks both temps and warns (the un-fixed twin of the v1.23.0 class-9 fix ten lines above). (3) progress._synthesize_queue_ops used `del` on module dicts while /api/progress runs on the threadpool — two concurrent cache misses raced to KeyError -> 500 -> the ops drawer blanked mid-burst; now .pop(k, None), matching every sibling line (`del` was the odd one out). (4) scheduler's release_available compared tags for INEQUALITY, so with nightly running ahead of the release branch it pushed an upgrade ping pointing BACKWARDS, once per release, never self-correcting; api.py's sibling compared parsed tuples — mirror drift. New app/core/versioning.py is_newer() now backs BOTH.
# 0.51.232: audit wave 5. (1) ops.js fetchProgress did `return r.json()` inside a try — in an ASYNC function a bare `return promise` resolves the outer promise WITH it, so the rejection escapes the try/catch. poll() sets pollInFlight=true and CLEARS pollTimer before the await and only resets after, so one non-JSON 200 (behind NPM+Authentik an expired session redirects to an HTML login page, and fetch follows redirects so r.ok is true) left the latch stuck true with no timer armed — LIVE OPS mini-bar, drawer and ops-state events dead until a full page reload, with nothing surfaced. `return await` makes poll()'s own v0.51.17 'fetchProgress never throws' comment TRUE rather than assumed. (2) deorphan re-keys the theme + 5 FK'd children but never the two newer (media_type, tmdb_id) tables — notifications + section_failure_acks. Not FK'd, so nothing cascaded and nothing complained; the rows just kept pointing at the retired synthetic id, leaving a clickable INBOX row that deep-links to a tmdb with no theme (killing the v0.51.220 edition-exact click-through) and a dismissed failure ack that stops matching so the banner reappears. Now re-keyed on BOTH the promote and merge paths — and deliberately NOT added to the pre-delete loop, which clears FK-invalid junk at the target and would otherwise destroy live inbox rows.
# 0.51.233: audit wave 6. (1) reconcile_placement_paths built its "Plex still reports this folder" skip set from RAW plex_items.folder_path (HOST paths) but compares it to placements.media_folder, which place_theme resolved through _candidate_local_paths to the CONTAINER path — so on any install needing a translation (the hardcoded /mnt/user/data/ -> /data/ pair or MOTIF_PATH_TRANSLATIONS) the skip could NEVER fire: every placement looked moved on every enum, cancelling the in-flight place, rewriting media_folder to the host path and re-enqueueing a forced place that resolved back to the container path and INSERTed a SECOND placements row (media_folder is in the PK), which the next enum deleted and re-enqueued — unbounded churn (v1.18.49 class). The skip set now indexes both dialects; adding candidates can only ADD skips, so it strictly reduces false moves and is a verified no-op where paths already match. (2) _record_local_file's ON CONFLICT unconditionally overwrote all 11 loudness/normalize columns on the v0.51.188 premise that "a re-download REPLACES the bytes" — true for a real download, FALSE for the two paths that return without replacing anything (download_theme short-circuits when the expected mp3 exists, e.g. DOWNLOAD TDB BACKUP with normalize-on-download off; the sibling-hardlink branch passes no `conditioned`). A leveled row was silently reported raw while the file on disk was still gained and still carried mp3gain's APEv2 undo tag, and // UNDO refuses unless norm_state='normalized' — the original audio was no longer restorable through motif. Now gated on the sha: identical bytes keep their state, new bytes still clear it.
# 0.51.234: audit wave 7 — three fixes, each with a test proven to fail against the pre-fix code. (1) sync._fetch_item caught only httpx.HTTPError, but r.json() raises json.JSONDecodeError (a ValueError): a 200 carrying a non-JSON body — a CDN/proxy error page, a truncated response — escaped the handler, propagated out of _do_fetch, and the un-try'd fut.result() in the remote-tier ThreadPoolExecutor loop turned ONE bad item into an aborted sync. Raising past `continue` also abandoned the candidate loop, so a bad imdb/ body meant the good themoviedb/ record was never tried. Now falls through to the next candidate and ultimately returns None into the existing record-is-None path that logs the tmdb_id and counts stats.errors. (2) plex._head_or_get_status fell back to a plain .get() on a non-200/401/403 HEAD; both callers pass a /theme path, so that read an entire theme MP3 into memory purely to obtain a status code. Inert while Plex answers HEAD, but a 405/redirect would silently make every verify a full-body download (~1-10MB x 2,800 themes on a bulk pass). stream() yields the same status without consuming the body. (3) idx_sync_runs_started was created only by the v36->v37 migration, so migrated installs had it and fresh installs sorted the dashboard sparkline with a temp b-tree — the same fresh-vs-migrated index divergence _migrate_v9_to_v10 documents having caused a soft-lock once. Declared in SCHEMA so both paths converge.
# 0.51.235: the git-apply loop's `if classification is None: continue` treated a per-item record it FAILED to parse exactly like a README edit — no error. So a ThemerrDB database-branch layout change would report 0 errors / 0 new / 0 updated (indistinguishable from a quiet upstream day apart from v1.21.43's warn-once line) and, because the baseline advance gates on stats.errors == 0, the changeset was consumed and those adds/modifies were never re-seen — the exact v1.22.74 failure mode that gate exists to prevent. An item-SHAPED path (3 parts, .json leaf, known top dir) that won't classify now counts: stats.errors blocks the advance for one run, failed_paths feeds the v0.51.14 chronic-pin escape so it can NEVER pin forever, and unresolved_failures makes a baseline-reset run skip drop detection rather than false-drop. The counting is deliberately narrow — the index artifacts (movies/all_page_N.json, movie_collections/pages.json) are TWO-part paths and so are never item-shaped; had they been, every sync would carry a non-zero error count and the baseline would pin permanently. That is pinned by a test. Top-dir membership moved to _GIT_ITEM_TOP_DIRS so the new predicate cannot drift from the classifier.
# 0.51.236: get_conn is isolation_level=None (AUTOCOMMIT), so a bare `with get_conn(...)` block commits every statement on its own — two adopt paths did several DEPENDENT writes that way. replace_with_themerrdb cancels in-flight jobs, captures previous_urls, DELETEs the user override, then INSERTs the download jobs; if that last INSERT failed (lock timeout, ENOSPC) the destructive half had ALREADY committed, leaving the operator with no override, no replacement download, and their in-flight download cancelled — the DB twin of the destroy-then-fail ordering v1.22.40 fixed for the filesystem. _maybe_restore_url_history's A->U promotion is likewise a local_files source_kind flip PLUS a user_overrides INSERT plus a themes UPDATE; applied partially the row's SRC letter disagrees with its override state and no walker reconciles that. Both now hold ONE transaction. Also: _GitMirror.read_json is typed dict|None but returned whatever json.loads produced, so a valid-JSON NON-object blob (an array, a bare string) came back as a list/str and every consumer calls record.get(...) — the AttributeError escaped the apply loop, where nothing catches it, aborting the entire sync. It now routes into the existing malformed-blob handler: warn-once + None = a counted read failure that holds the baseline instead of killing the run.
# 0.51.237: a ThemerrDB index entry with no `id` caused a FALSE tdb_dropped_at stamp on a LIVE title — DEMONSTRATED, not theorised: the new behavioural test drives run_sync with one id-less entry and, against the pre-fix code, the live row comes back stamped. sync has three signals for an untrustworthy catalog — index_incomplete (a whole page failed) suppresses the WHOLE sweep, errored_by_mt (per-item failure, id known) excludes just those ids, and the id-less entry hit NEITHER. _do_fetch returned a bare None, so no stats.errors (the errors gate passed) and no errored_by_mt entry (nothing excluded), while the item's last_seen_sync_at was never refreshed — and the full-walk sweep keys on exactly that column. The row flipped to SRC=- with the gray TDB circle pill and its pending_updates row was deleted. That is the v1.21.38 false-drop class coming through the one door v1.21.44's per-id exclusion cannot cover: with no id there is nothing to exclude, so the only safe move is to stand the sweep down for the run — exactly what git_unresolved already does on the baseline-reset path. Conservative by design: a real removal simply waits for the next clean run, because under-stamping is recoverable and over-stamping destroys pending_updates. Suppression is conditional (a clean run still stamps real drops) so this cannot become the v1.21.38 starvation bug, and the skip log now names WHICH signal fired.
# 0.51.238: filtering the library by the PU chip HID rows that visibly paint PU. v1.24.28 split stale plex_uploads out of PU into the RP chip and wrote the PU filter as `theme_present IS NOT 0` — correct then, because theme_present=0 WAS the whole definition of stale. v1.24.40 then added rk-liveness (a theme_present=0 row whose stored rk is LIVE self-corrects to healthy) and v1.24.41 added a non-NULL-rk guard (a legacy upload that never stored an rk has nothing to re-push TO), both ONLY inside _LIB_STALE_PU_SQL — which drives the render via needs_repush and the sort. The filter never caught up, so those two shapes rendered PU, sorted into the PU bucket, and matched NEITHER link_pills=pu NOR link_pills=rp. Demonstrated through the real endpoint: 4 seeded plex_upload shapes, 2 fell through both filters. The PU branch now derives from NOT the same constant, so PU and RP are an exact partition of plex_upload and cannot drift apart again — the contract-drift class (v1.17.10), where a later change moves the contract under an earlier hand-rolled test. NOT COALESCE(...,0) mirrors the render's CASE WHEN, which sends a NULL theme_present down the not-stale branch. The tests assert the PARTITION property rather than SQL text, so they keep holding if staleness is refined a fourth time.
# 0.51.239: the cloud-backup candidate query's three NOT EXISTS gates compared bare `pi.guid_tmdb`, but the FORCE path deliberately DROPS the `guid_tmdb IS NOT NULL` filter so a no-TDB row (a collection — the operator's A24 Films repro) can be captured. So for exactly the rows force exists to serve, every gate evaluated `= NULL` and was vacuously true. The one that matters is the allow_existing_local branch, whose stated purpose is 'exclude rows that ALREADY have plex_cloud local_files ... so the click is a no-op': for a NULL-guid collection that no-op could never fire, so DOWNLOAD PLEX BACKUP re-fetched the bytes from Plex and rewrote local_files on EVERY click. The gates now resolve the id the row will actually be keyed to via _CTB_EFFECTIVE_TMDB, mirroring _resolve_or_mint_tmdb_id's precedence (real guid wins, else the orphan id reachable through theme_id) — and a test pins that the SQL and the Python resolver agree, since disagreeing would exclude under one id and write under another. On the non-force walk guid_tmdb is NOT NULL by construction, so the COALESCE reduces to the old expression and that path is unchanged.
# 0.51.240: the de-orphan kind guard was a TWO-class test over a THREE-class space. _resolve_real_tmdb refuses a re-key when TMDB's kind disagrees with the orphan's media_type — 'otherwise we'd re-key into the wrong identity space' — but expressed it as (cand_kind == 'tv') != (media_type == 'tv'), which only separates tv from not-tv. themes.media_type is movie/tv/COLLECTION and _lookup_by_imdb only ever yields 'movie' or 'tv' (it reads movie_results then tv_results, else None), so a COLLECTION orphan resolving to a movie evaluated False != False, passed, and the row was re-keyed onto a MOVIE's tmdb_id while still carrying media_type='collection' — exactly the corruption the guard exists to prevent. The path is open because the v1.22.52 mint stamps plex_items.guid_imdb onto a minted orphan whatever its media_type, and this walker's candidate SELECT filters upstream_source / tmdb_id / imdb_id but NOT media_type. Whether a Plex collection actually carries an imdb guid in practice is unverified here (it needs the live DB), so this may be latent rather than firing — but equality is a strict tightening: byte-identical behaviour for movie and tv, and it refuses collection, which TMDB's /find cannot resolve an identity for anyway.
# 0.51.241: maybe_cleanup_duplicate_placements deleted a hardlink/copy placement whenever its sidecar was MISSING — and 'missing' is exactly what an unmounted /data looks like from inside the container (on Unraid the array may not be up when the container starts). One mount outage would therefore drop the hardlink half of EVERY dual placement in a single pass: the v1.18.10 amplifier-sweep class, where a defensive cleanup multiplies damage from an unrelated failure. Deliberately NOT fixed with a count cap: the legitimate case is ALSO 'every candidate's sidecar is missing' (the operator's real v1.19.17 run had 10 duplicates, all genuinely stale), so any threshold either blocks the real cleanup or lets the outage through. The FOLDER is the signal that separates them — a genuinely stale row has its media folder present with theme.mp3 gone, an unreachable path has no folder either. Now checked per row, so a mixed run still cleans the reachable stale rows instead of standing the whole sweep down, and rows skipped as unreachable are re-examined next boot (the one-shot marker is only stamped on a completed run).
# 0.51.242: the audit item 'op_progress rebuild shadow-table pre-clean' turned out to be a FALSE POSITIVE, and measuring it found a real one next door. The rebuild idiom is CREATE TABLE X_new -> INSERT SELECT -> DROP X -> RENAME; if a kill lands between the CREATE and the RENAME and that shadow COMMITS, the retry on the next boot dies on 'table X_new already exists' — init_db raises, so the container fails to start on EVERY restart until someone drops the table by hand (verified end to end). Whether it commits depends purely on BEGIN/COMMIT, which was measured rather than assumed: all TEN op_progress rebuilds are BEGIN-wrapped and roll back cleanly, so they needed nothing. THREE older blocks are not — themes_new + jobs_new in _migrate_v4_to_v5 and jobs__new in _migrate_v6_to_v7 — and those genuinely leaked a shadow. They now pre-clean, mirroring the v1.19.73 pre-clean in _widen_check_constraint. Only reachable on a DB still below schema v7 (current is v78), so almost certainly latent, but a boot-blocking crash-loop is worth three free lines. A lint now requires every rebuild to be either transactional or pre-cleaned, so a future one can't reopen this.
# 0.51.243: loudness controls sit in the value column, so the INFO card keeps one alignment.
# 0.51.244: re-opening the info card you're already on updates in place, keeping your scroll.
# 0.51.245: quarterly floors — yt-dlp 2026.7.4, apprise 1.12.0.
# 0.51.246: auth path stops blocking the event loop (30s writer-lock freeze + bcrypt).
# 0.51.247: 4 sidecar checks on plex_items.folder_path now translate host->container.
# 0.51.248: regression guards for the v246/v247 rewrites (parity + end-to-end).
# 0.51.249: drop 4 dead defs whose comments claimed protections that never existed.
# 0.51.250: folder-move reconcile sees theme_id-linked (anime) placements — 3 were invisible
#   (the 32 first measured included 29 plex_upload rows reconcile rightly excludes).
# 0.51.251: dead-code batch. The /override POST+DELETE pair removed (no caller; the DELETE
#   fan-out was a cross-edition override wipe for any token holder), ParsedFolder.has_year
#   removed with its mirror assert, saved_reason marked forensic-only at both schema sites.
#   Removing the pair exposed a PHANTOM GUARD: the v1.15.66 import-apply section_id='' test
#   had matched only the dead handler's INSERT since v1.21.34 added the intent column —
#   re-anchored inside the apply handler and mutation-verified against the live INSERT.
# 0.51.252: LOW batch closing the 2026-08-01 review backlog. /api/services WAN probes
#   (ThemerrDB git host + TMDB) cached 300s (~5,760 → ~576 outbound req/day per open
#   dashboard; Plex stays fresh every call), loadDashboard's five GETs fire concurrently
#   (first paint pays the slowest round-trip, not the sum), boot Plex section discovery
#   moved to a daemon thread (a down Plex no longer holds the UI hostage at start),
#   dead renderMissing + #*-missing-body writes removed, and bulk-bar labels write
#   through setBulkLabel so the selection poll can't stamp the resting count label
#   over a running handler's // PUSHING i/N progress (bug-class 5 ping-pong).
# 0.51.253: INCIDENT FIX — the place resolve targets the LIVE rating key. A disk
#   dropped off the array; Plex deleted the items and re-added them with new rating
#   keys on reconnect. The v0.51.128 reaper holds a missing row for 2 enums (correct
#   anti-glitch grace), so plex_items legitimately held a dead row AND a live row per
#   title — resolve_theme_ids had linked BOTH to the theme, so the JOIN matched both
#   and a bare LIMIT 1 returned the older, DEAD one. Every bulk PUSH upload 404'd
#   against a key Plex had already deleted (~80 movies). All three resolves in BOTH
#   _do_place_collection (the API/upload path the incident took) and _do_place (the
#   file path) now ORDER BY consecutive_missing ASC, last_seen_at DESC, so a dropout
#   self-corrects on the first enum instead of needing two refreshes. The v1.24.24
#   ambiguity guard now counts LIVE candidates only (a dead+live pair was being
#   refused as ambiguous). Also: the state log labeled job["id"] as `rk=`, which read
#   as a second rating key next to cached_rk while debugging the incident.
# 0.51.254: a client-side bulk loop is ONE action + Discord 429s retry. Exposed by
#   .253: once the 72-row recovery push actually SUCCEEDED it fired 72 Discord messages
#   (a failed place never notifies, so both defects had been hidden). v1.23.46 made
#   coalescing depend on an explicit `bulk` flag, but bulk PUSH / bulk SWITCH TO API are
#   CLIENT-SIDE loops over the single-row /replace + /switch-placement endpoints, so
#   nothing told the backend those N calls were one action. Both now accept ?bulk=1 and
#   stamp it into the place-job payload the worker already reads. /restore-canonical and
#   /adopt-sidecar deliberately untouched — MEASURED: neither enqueues a place job nor
#   dispatches, so neither can flood. Separately, _send_discord_embed treated HTTP 429 as
#   a generic failure and DISCARDED the message despite Discord naming retry_after —
#   those notifications were lost, not delayed. Now one retry, capped at 5s.
# 0.51.255: the last two unmarked notification sweeps. Sweeping the whole class after
#   .254 rather than only the loop the operator hit: of 12 place/download enqueue sites,
#   5 already stamp bulk, 3 are genuinely one-action-per-call (UPLOAD MP3 / SET URL /
#   PROMOTE — no bulk button exists), auto_restore coalesces via a hardcoded branch, and
#   TWO were unmarked AND automated. _retry_pending_placements (hourly, LIMIT 500) fired
#   theme_added per re-placed row — a 500-message ceiling from a cron tick nobody pressed.
#   reconcile_placement_paths (every enum, UNCAPPED) fires theme_pushed per relocated
#   row, and its trigger is a mass folder rename. Both are sweeps by construction, never
#   one user action per row, so both now stamp bulk unconditionally.
# 0.51.256: the no-recovery theme-lost tier alerts by default. v1.18.90 shipped
#   plex_theme_lost OFF (per-row event, burst risk); both grounds are now answered by
#   MEASUREMENT — bursts coalesce as of .254/.255, and the kind proved accurate in the
#   field: 5 lifetime firings, 5 correct, 0 false positives (ER left the library; 4
#   disk-dropout casualties with has_theme=0, no backup, and a TDB url dead upstream at
#   failure_kind=video_removed). It is the ONLY tier meaning "no automatic recovery
#   exists", so muting it silenced exactly the alerts needing a human while the two
#   RECOVERABLE tiers pinged instantly — backwards. NOTE: yaml wins over defaults on
#   load, so an existing install keeps its saved value; the Settings toggle is the fix
#   there. Also retracts a stale notify_inbox note calling plex_item_arrived_themed
#   dormant — it fires (3 rows on the operator's install).
# 0.51.257: the shutdown drain follows the BUFFER, not the timers. flush_all_coalesced
#   iterated _COALESCE_TIMERS and recovered (db_path, notifications) from timer.args, so
#   a kind holding buffered items with NO timer was invisible to it — and that is exactly
#   what an _arm_coalesce_timer failure leaves behind, the case v1.20.0's except handler
#   exists for. The batch vanished at exit with no breadcrumb, in the function whose whole
#   job is preventing that. Config now lives in _COALESCE_CFG beside the buffer, written
#   BEFORE the append, which also retires the fragile timer.args coupling. Second half:
#   _COALESCE_ACTIVE is gone — write-only since v1.23.46 replaced leading-edge inference
#   with the explicit bulk= flag, and the only thing keeping it alive was a v1.20.0 guard
#   that was itself a phantom (its fixed 800-char window reached past the except it named
#   into _flush_coalesced's copy of the line, so it would have passed with the handler
#   deleted). That guard is retargeted at the invariant the handler actually owes.
# 0.51.258: the credential scrubber pointed the wrong way. notify_inbox has run
#   title/body through the events scrubber since v0.51.147 — for the LOCAL db write —
#   while notify.py handed the same two strings to Discord / Apprise / an external
#   apprise-api completely raw. The protection was on the copy that never leaves the
#   machine and absent on the third-party hop. _dispatch_inline is the single function
#   every real send funnels through, so the redaction lands there and covers all three
#   sinks including the native discord-embed route. Redaction ONLY, not
#   events._scrub_text: that helper also caps at 2 KB, which is log hygiene for the
#   events table and would truncate a real 77-item digest mid-list. No known live
#   vector (the Plex token rides a header) — a last line of defense that covers only
#   the safe direction just is not one.
# 0.51.259: the inbox stops lying about how much it is showing, and covers two kinds
#   it never did. (a) The drawer fetched with no limit — the endpoint's 50-row
#   default — and the group head rendered children.length as if it were the total.
#   After the 77-row restore burst it read "50 themes restored" with 27 rows
#   invisible and nothing saying so. /api/notifications now returns `total` (every
#   UNDISMISSED row — the same scope the drawer renders, so a dismissed row cannot
#   inflate it into claiming hidden rows that dismissing will never reveal), the
#   drawer asks for the 200-row ceiling since burst SIZE is set by Plex not by us,
#   and it renders "showing N of M" only when actually truncated. (b) theme_pushed +
#   theme_backed_up join INBOX_EVENT_KINDS — theme_added's siblings had no in-app
#   trace at all, so the 72-item bulk PUSH left one coalesced Discord line and zero
#   rows in motif. theme_pushed keeps its Apprise-OFF default and gets an inbox-ON
#   default: the registries answer different questions, and the inbox records BEFORE
#   the Apprise gate, so the quiet local record lands for muted operators too.
#   Also adds the guard that was missing: app.js TIER/GROUP are a FOURTH event_kind
#   registry nothing checked — a new kind without entries silently rendered with no
#   tier stripe and a generic "N notifications" group label.
# 0.51.260: align every write-once writer's lock-wait budget. Measuring the standing
#   deferral #8 (api_set_override_intent holds the write lock across a Plex upload)
#   found its recorded consequence — "no data loss, worst case a few-sec writer
#   stall" — is only half right. A probe holding a real BEGIN IMMEDIATE for 40s:
#   transaction() writers waited 7.5s and SUCCEEDED (budget 30s x 5 attempts + 7.5s
#   backoff ~= 157.5s), but the events flusher DROPPED its batch at 32.6s and the
#   row was gone — it and notify_inbox.record_notification own their connections and
#   had 3 x 10s + 3s ~= 33s. So the gap belongs to the WRITERS, not to the one
#   caller that surfaced it: both now match db.LOCK_WAIT_S / LOCK_RETRY_DELAYS,
#   hoisted for a cross-module lint (they cannot import db — events.py is the
#   logging substrate and a db-layer fault must not take the audit log with it).
#   Deliberately NOT widened for the inbox READERS or dismiss/seen: those fail
#   visibly and are retried, and a 30s-per-attempt hang in a request handler is
#   worse than a fast error. Deferral #8 now degrades to what its record claims.
#   Two corrections while here: sqlite3.connect(timeout=N) ALREADY installs
#   busy_timeout=N*1000 (measured — a fresh conn reads the pragma back), so the
#   v1.13.50 comment calling the pragma the only mechanism was wrong; and the
#   flusher's drop message hardcoded "after 3 attempts", now the real count.
# 0.51.261: ratchet against NEW fixed-width test windows. An audit for silent
#   failures came back clean on the code — class-9 swallows all carry a fallback or
#   a documented cosmetic-callback reason; the class-12 async lint's own documented
#   blind spot (it walks only the DIRECT body of an async def, which is how the
#   v0.51.246 auth freeze survived) was extended one level into same-module sync
#   helpers and found ZERO. What it did find is in the SUITE: 1513 guards shaped
#   `src[a:a + N]`, which fail when unrelated code GROWS rather than when the thing
#   they protect breaks. Four went red in one session; v1.20.0's overshot the
#   `except` it named into the next function and would have passed with that handler
#   deleted. The everyday cost is worse than the phantom: a gate that manufactures a
#   red every few tags trains you to read gate failures as noise. New guards are now
#   blocked by an exact-count ratchet that only moves DOWN, and the worst single
#   offender is converted — refreshTopbarStatus's window, bumped FOUR times
#   (50000->60000->64000->66000->68000) and sitting 485 chars from failing again,
#   now ends at the next sibling function declaration, which its own bump comments
#   had named as the right boundary without using it. NOTE: this ratchets growth; it
#   does NOT claim the other 1512 are sound. A detector that tried produced 72 false
#   positives, then 4, all of them a brace-matcher tripping on an `extras = {}`
#   default parameter — so no overshoot check ships here.
# 0.51.262: a persistent rotating log at /config/logs/motif.log. stdout was the
#   ONLY place motif's Python logs ever went, and an Unraid Force Update RECREATES
#   the container and discards its JSON log — so every deploy erased all history.
#   Not hypothetical: on 2026-08-13 two forensic questions were put to prod and BOTH
#   were unanswerable for exactly this reason — had the events flusher ever logged
#   "DROPPING batch" (the consequence .260 fixed), and how slow does
#   upload_collection_theme actually get. The probe ran against a 35-line,
#   six-minute-old log, so its 0 was an artifact rather than an answer, and the
#   history is gone for good. The `events` table cannot substitute: the flusher IS
#   the events writer, so it cannot record its own failure there, and rows prune at
#   30 days. /config survives container replacement; stdout does not. Same format on
#   both sinks (one shared constant, so they correlate line-for-line), 10 MB x 5
#   backups for a ~60 MB ceiling on the appdata share, and a failure to open the
#   file WARNS and boots anyway — stdout still works, so the cost is persistence,
#   not logs. Raises the stakes on the v1.23.92 apprise clamp, which is what keeps
#   webhook tokens out of DEBUG and therefore off the disk.
# 0.51.263: NOTIFICATIONS settings relayout. The panel was one flat column of ~30
#   checkboxes carrying 1,319 words of hint prose (avg 38/toggle, max 132) — the
#   operator: "one long list ... a lot of text". Now: the 20 EVENTS toggles grouped
#   under five .form-subhead family headers (the v1.13.49 PLEX-panel pattern), the
#   two sync fold-in toggles NESTED under SYNC COMPLETED via .form-checkbox-sub
#   (v1.21.19) so the dependency is shown not narrated, every hint cut to one line
#   (440 words total, max 45 — the WHY lives in PROJECT_HISTORY, not the control
#   surface), and IN-APP INBOX collapsed to a hint-less two-column grid (new
#   .form-grid-cols2, 760px collapse) since its ten kinds are named identically to
#   their EVENTS twins (v1.23.83 pins the parity). Nothing load-bearing moved: all
#   data-cfg-fields, labels, chips, and the pinned hint phrases (reason-branch
#   titles, the v0.51.256 default rationale, action paths) survive — the 237
#   pre-existing tests over this panel pass unmodified. A word-budget ratchet
#   (<=50/hint, <=600 total) stops the wall of text growing back.
# 0.51.264: an override row stops re-notifying "1 updated" every sync, and stops
#   having its own !UPD prompt swept away. The operator kept getting "Motif sync
#   — 1 updated / Updated: Anime · Vampire in the Garden (2022)" for a row
#   showing TDB ∅, and one run log carried both halves of it: `updated:1` plus
#   `cleared 1 stale pending update(s) — upstream theme removed`. Two defects,
#   one wrong assumption — that themes.youtube_url is upstream's answer.
#   (1) v1.22.8's url-less sweep reads `youtube_url IS NULL` as "TDB removed its
#   theme". On a U-row the operator set BECAUSE TDB had nothing, v0.51.228
#   deliberately WITHHOLDS that write when TDB later publishes one, so the column
#   stays NULL and the sweep deleted the prompt the same run had just written —
#   nothing to accept or decline, and the log blamed upstream for motif's own
#   withheld write. The sweep now asks TDB's record (raw_json, which the withheld
#   branch rewrites every sync) for override rows; json_valid guards legacy rows
#   into the conservative no-delete answer (v1.18.10 amplifier rule).
#   (2) stats.updated_count fired on every url_changed RE-detection. The withheld
#   branch also leaves tdb_content_fingerprint stale by design, so url_changed
#   re-fires forever for the same video; a KEEP CURRENT didn't silence it either
#   (v1.20.14 holds the row 'declined', the count never read the decision). It
#   now counts only when the UPSERT below would actually arm a prompt — no row
#   yet, or a genuinely different video (the same IS-NOT test v1.20.14 re-arms
#   on). Repro'd against a real DB first: 3 syncs → 3 notifications + 3 swept
#   prompts pre-fix, 1 notification + a surviving prompt after.
# 0.51.265: document the contract v0.51.264's bug was reading wrong — and pin it
#   as behaviour, not prose. db.py said "themes.youtube_url — TDB's current URL",
#   flatly, which is what made v1.22.8's sweep author reasonable in treating
#   `youtube_url IS NULL` as "upstream removed the theme". It isn't: v0.51.228
#   WITHHOLDS that write while a user_override exists, and adopt.py backfills a
#   restored (non-TDB) url when the column is blank. So the column is "the TDB
#   url motif last COMMITTED", and upstream's live answer is
#   raw_json.youtube_theme_url. Corrected at the three sites that state or
#   violate the meaning: the themes schema comment, the migration's safety
#   analysis, and adopt.py's backfill. Correcting the comment ALONE would have
#   been a phantom guard (v1.18.81 — prose nothing executes), so the exception
#   ships as a behavioural test: an override row is synced against a NEW
#   upstream video and asserted to keep its committed url + stale fingerprint,
#   while raw_json carries the new one; the no-override base case asserts the
#   rule the exception is an exception TO. Disabling the withheld branch turns
#   two of them red (mutation-verified). No behaviour change in this tag.
# 0.51.266: reading ONE notification stops clearing unread on all of them, and
#   Escape stops leaving a ring on the INBOX pill. (1) `mark_seen()` was the only
#   seen-writer and is unconditional (WHERE dismissed_at IS NULL AND seen_at IS
#   NULL — no id), fired by the drawer's load() on every OPEN. There was no
#   per-row read state at all: rows held an unread highlight for that one viewing
#   and came back seen, so clicking one and returning looked like it had cleared
#   the lot. New `mark_seen_one` + POST /api/notifications/{id}/seen; a click marks
#   THAT row (every row, not just the .notif-clickable ones — a row with no item to
#   open would otherwise never clear); opening marks nothing; `// MARK ALL READ`
#   makes the old bulk gesture deliberate instead of a side effect of looking. It
#   reuses the CLEAR ALL primitive with one token hover modifier (design-system
#   rule: put the existing class on the new surface). (2) A mouse click focuses the
#   pill without painting a ring, but the Escape keypress flips Chrome's
#   :focus-visible heuristic to "keyboard", so ops.css's .op-pill:focus-visible
#   outline painted as the drawer left and stuck — the stuck-highlight class
#   v1.15.131 fixed globally, resurfacing for the pills that later JOINED the
#   focus-visible allow-list. Escape now blurs the pill when it holds focus; the
#   rule stays so Tab users still see it. All three mutation-verified.
# 0.51.267: three fixes from an external code-review validation brief (all 11 of
#   its findings were factually accurate; these are the three cheap ones — the
#   two HIGHs it named are inert on this deployment, and two more were declined
#   with reasons, recorded in ~/motif-review-validation-2026-08-20.md).
#   (#2) release.yml published images with NO test gate: it ran no pytest and
#   depended on no CI result, resting on the comment "a branch push already ran
#   this". ci.yml deliberately skips tags and workflow_dispatch can publish any
#   commit, so a red tree could reach :nightly — the channel deployments track.
#   New `gate` job re-runs ci.yml's BLOCKING checks against the exact tagged
#   source; build-and-push `needs: gate`. (#8) pip-audit was `|| true`, so a new
#   dependency CVE scrolled past while the job went green; measured clean at this
#   commit, so the ratchet cost nothing and it is blocking now (ruff-full + mypy
#   stay report-only — ~300 pre-existing findings). (#10) the Docker Hub
#   description step was gated on `steps.tags.outputs.is_stable`, retired at
#   0.50.0 and never emitted since, so it had been silently skipped on EVERY
#   release. Removed, and the general class is now linted: a workflow step that
#   reads an output nothing writes fails the suite. My own drift guard caught
#   that the new gate had omitted the newly-blocking pip-audit; the output lint
#   first flagged its own removal comment, so it now reads parsed YAML values
#   rather than raw text. All three mutation-verified.
# 0.51.268: GET /readyz — local operational readiness, separate from liveness.
#   Milestone 1 of the feature-implementation brief, scoped to readiness only
#   (no /metrics — nothing scrapes it here, and the brief's own guardrails make
#   an unscraped exporter pure cost). The same gap is #9 in the code-review
#   validation brief, reached independently. NOT hypothetical: a PUID mismatch
#   on a permission-enforcing share (v1.22.4) denied every write while Docker
#   reported the container healthy, and the only breadcrumb was one boot log
#   line — it surfaced a week later as crash-looping downloads. /healthz stays
#   LIVENESS and the Docker healthcheck deliberately stays pointed at it (a
#   probe that restart-loops on a permissions problem fixes nothing); /readyz
#   answers "can motif actually do its job here", 503s when it cannot, and NAMES
#   the failing check — never the path, since it is public like /healthz. The
#   write probe is TTL-cached (30s) so a poll cannot write a probe file per
#   scrape. config.probe_dir_writable is now the single definition of writable,
#   called by BOTH the boot probe and the endpoint, so they cannot drift; the
#   loud uid/owner diagnostic stays at boot where an operator can act on it.
#   Verified boundary, documented rather than papered over: the auth middleware
#   calls setup_complete(db_path) on EVERY request before the public-path
#   branch, so a vanished DB 500s before either probe runs — the db check covers
#   the transient/busy case. Three mutations verified red.
# 0.51.269: throttling becomes its own FailureKind. Feature D of the
#   implementation brief, first step only — the brief asks for an adaptive rate
#   controller but misses the prerequisite: motif could not SEE throttling as
#   distinct from any other network fault. YouTube's rate-limit prose AND a
#   literal HTTP 429 both landed in NETWORK_ERROR — safe (transient, and
#   correctly so since v1.15.12) but indistinguishable, so no controller could
#   tell "the provider is throttling me" from "the network hiccuped", and
#   neither could a human reading the log. A controller built without this
#   signal would be guessing, so the split ships on its own.
#   RATE_LIMITED behaves EXACTLY like NETWORK_ERROR everywhere that matters —
#   the hazard here is historical and specific: v1.15.12, a throttle classified
#   into a DEAD kind red-pilled 2005 of 2507 probed rows. "Transient" was
#   expressed three ways (worker needs_manual_override, a literal
#   indeterminate_set in api_probe, bulk-probe's err-is-not-None). The literal
#   set was the trap — a new kind could silently miss it and paint a red ✗ — so
#   it moved onto the enum as is_indeterminate, where a kind inherits an answer
#   instead of needing to be remembered. Throttle tokens are deliberately
#   specific ("http error 429" / "too many requests", never a bare "429"): the
#   classified message includes the URL, and a video ID like abc429xyz must stay
#   VIDEO_REMOVED. No migration — failure_kind carries no CHECK. Three mutations
#   verified red, including omitting the kind from is_indeterminate.
# 0.51.270: five findings from reviewing v0.51.263-269, four of them in code
#   shipped earlier the same day. (F1) renderEmpty hid CLEAR ALL but not MARK ALL
#   READ — the button postdates it (v0.51.266) — so an emptied inbox kept
#   offering an action with nothing to act on. (F2) markRead decremented the
#   unread badge and dismiss did not, so dismissing an unread row left the topbar
#   over-reporting until the next /api/stats poll; a v0.51.266 regression, since
#   before it the drawer-open zeroed the badge and the paths could not drift.
#   Fixed on BOTH the single-row and group paths, reading the class before the
#   row leaves the DOM. (F3) the topbar comment still claimed "opening the drawer
#   marks the set seen, so the next poll lands here with 0" — untrue since
#   v0.51.266, and precisely the lying-contract class that caused v0.51.264 and
#   that v0.51.265 shipped to fix. (F4) /readyz cached settings-derived answers
#   on a wall-clock TTL only, so after the operator saved the setting that makes
#   it ready it kept reporting NOT ready for up to 30s — reported broken at the
#   moment it was fixed. Now keyed on Settings.revision, which exists for exactly
#   this and which FolderIndex already invalidates against. (F5) the cache wrote
#   its validity keys before its payload, letting a concurrent reader pair a
#   fresh timestamp with the previous snapshot; payload is published first now.
#   Three mutation-verified. Findings 1-3 were mine from .266, 4-5 from .268.
# 0.51.271: an edition replaced by a SIBLING edition is no longer a lost theme.
#   The operator: pull the extended cut, put the theatrical cut in its place, and
#   motif fires "💔 Theme lost … no backup configured" for a title whose theme
#   never went anywhere. Two facts reframe it. (1) The canonical lives in motif's
#   OWN store (themes_dir/…/Title (Year) {edition-<key>}/theme.mp3), not the media
#   folder — only the placement and the edition-keyed association broke, so the
#   old message was not merely annoying, it was false. (2) Edition separation
#   exists to resolve AMBIGUITY (the edition-sibling bleed class), and ambiguity
#   needs two editions; with exactly one survivor the edition key is a
#   distinction without a difference, and there is nothing to bleed into.
#   New app/core/edition_swap.py carries the theme to the survivor under four
#   guards, each mutation-verified: exactly ONE surviving edition row for
#   (media_type, tmdb_id, section_id) — two or more is the ambiguous case motif
#   must not guess at; a genuinely different edition_key; a survivor with NO
#   theme of its own (an operator's per-edition pick always wins); and the
#   replacement PRESENT rather than the old row merely absent — a positive
#   condition, because a transient enumeration gap shows only the absence and
#   reading an absence as a removal is the shape v1.22.8 got wrong (v0.51.264).
#   The canonical moves BEFORE any row is touched and outside the transaction
#   (no write lock across filesystem work); a failed or blocked move aborts with
#   nothing changed, so the reaper's existing loss path still runs. Idempotent by
#   construction: after a carry-over the survivor HAS a theme, so guard 3 stops a
#   second pass and reconciliation cannot loop.
# 0.51.272: the edition carry-over finishes the job — four findings from
#   reviewing v0.51.271 the day after it shipped (the one module written after
#   that day's self-review; all four latent, the census proved the path had
#   never fired in prod). (F1 HIGH) the resolver re-keyed the dead placement and
#   never re-placed: the row's media_folder/rating-key died with the edition, so
#   the library read PLACED (`!!media_folder`) while Plex played nothing, until
#   the next enum stamped theme_present=0 and the row surfaced in NEEDS WORK as
#   a generic broken placement with no connection to the swap. Now the dead
#   placement is DELETED and a place job enqueued (v1.21.78 payload shape); the
#   worker resolves the survivor's folder/rk edition-scoped and refreshes Plex.
#   (F2 MED) the canonical moved BEFORE the row txn with no compensation — a row
#   failure left the file at the new path while file_path pointed at the old
#   one, plus a loss notification that never mentioned the half-move. The file
#   now moves back on any row failure, making "a failed run leaves the old
#   edition intact" true in both halves. (F3 MED) the user_overrides re-key was
#   not section-scoped — a swap in section 1 re-keyed a 4K section's override
#   off an edition that still exists there (class-2 bleed); now `section_id IN
#   (?, '')` on both the UPDATE and guard 3's override arm, so a foreign
#   section's survivor-key override also stops vetoing this section's carry.
#   (F4 LOW) the emptied {edition-X} folder is rmdir'd — rmdir refuses non-empty,
#   which is the emptiness test. All four mutation-verified red.
# 0.51.273: the edition carry-over is REACHABLE — the fan-out review's top
#   finding, hand-verified. The tier classifier's other_fallback arm matches the
#   reaped row's OWN local_files and dead placement (edition-scoped IN (?, '')),
#   and tier-3 `continue`d before candidacy — so a motif-downloaded, PLACED
#   theme whose edition was swapped never reached the v0.51.271 resolver at all;
#   the carry-over only ever fired for tier-1 (backup) / tier-2 (sidecar) rows.
#   It survived two tags because the only wiring test was a source-text pin
#   while every behavioral test called the resolver directly — verbatim the
#   v1.18.81 phantom-fix sub-pattern. Fix: tier-3 becomes a candidate, the
#   dispatch loop attempts the swap first, and then restores tier-3's historical
#   silence either way (resolved → carried, silent; unresolved → silent skip,
#   exactly pre-.273). The load-bearing test now drives the REAL pipe:
#   _upsert_items twice (grace threshold 2) → reap → tier → dispatch → resolver,
#   with a notification spy proving the silence held. Also from the review:
#   a malformed local_files.file_path (absolute / <3 components) is refused
#   before anything is derived from it — pathlib DISCARDS the left side when
#   joining an absolute right side, so a corrupted row would have sent every
#   exists/mkdir/replace into a foreign tree (the first draft of THIS tag's own
#   test seeded no file at the bad path and the mutation stayed green — rewritten
#   to put the file there); the survivor lookup counts LIVE rows only
#   (consecutive_missing == 0, the _do_place v0.51.253 idiom) so a mid-grace row
#   neither vetoes as a phantom sibling nor receives a theme right before its
#   reap; and the '' title-global override stays keyed at the lost edition while
#   ANOTHER section still carries it (the one-row residual of .272's F3). Five
#   mutations verified red.
# 0.51.274: the fan-out review's drawer batch. (1) Keyboard parity: v0.51.266
#   wired markRead into the CLICK handler only — the third instance of the
#   v0.51.213 mouse-only class in this one handler's history (found
#   independently by two reviewers, one driving real Tab keypresses). Enter on
#   an unread row navigated and left it unread forever, and non-clickable rows
#   had no tabindex at all — the exact state markRead's own comment forbids.
#   keydown now pairs the click path (markRead first, then navigate; Space
#   preventDefault'd so it can't scroll), and EVERY row is tabbable. (2) The
#   drawer-head text buttons were hover-bearing with NO focus-visible ring
#   (measured: Tab reached them, nothing painted) — added to the ops.css ring
#   block; and with MARK ALL READ present the fixed-width head wrapped every
#   label to 2-3 lines at ALL viewports (measured) — labels are nowrap now and
#   the actions ROW drops below the title as one unit (the documented mobile
#   idiom; LIVE OPS shares the head class, wrap is inert there). (3) Bug class
#   #7: markRead/markAllRead/dismiss/dismissGroup were the only mutating POSTs
#   in the file missing setTimeout(refreshTopbarStatus, 1100) — the 2s
#   ops-cadence poll re-read the 1s-TTL stats cache and resurrected the old
#   badge count for a full poll gap. (4) Tidiness: dismiss re-entry guard
#   (double-click double-decremented; markRead honors the same flag), markRead
#   keepalive (its POST raced its OWN click's navigation and an aborted send
#   left the row unread server-side forever), a group head now dims when its
#   last unread child is dismissed, MARK ALL READ hides once nothing unread
#   remains (the .270 renderEmpty class, one gap over), and a hidden badge
#   reads as zero (stale digits resurrected on a dim pill in the two-tab
#   case). Seven mutations verified red.
# 0.51.275: the fan-out review's docs batch — every committed doc that
#   contradicted shipped reality, reconciled. The reviewer's headline held: the
#   docs TOUCHED this week were accurate; the drift was in what wasn't touched.
#   README: "only /healthz is public" was FALSE in three spots since /readyz
#   (v0.51.268) — both prose sites + the endpoint table now carry /readyz, and
#   the healthz response shape is stated honestly; the sync notification row
#   claimed a `✅ Sync complete` SUBJECT (v1.19.55 moved the ✅ into the body —
#   DESIGN_SYSTEM had it right, README contradicted it); the failure-kind table
#   presented 7 kinds as exhaustive (rate_limited, v0.51.269, was absent) and
#   now enumerates the enum with a guard test that walks FailureKind; the
#   event-kind list is framed as a sample of the 20 with the loss/recovery
#   kinds named; and the in-app INBOX + per-row read (v0.51.266) finally have a
#   README home. ci.yml: "Two BLOCKING gates" listed three (the .267 edit never
#   updated the count) and still gave the pre-gate "a branch push already ran
#   this" rationale for skipping tags — release.yml gates itself now and both
#   headers say so; release.yml's "the two BLOCKING gates" echo fixed the same
#   way (names, not counts). DESIGN_SYSTEM's v1.22.55 settings section
#   contradicted its own .263 addendum three ways (long-hints claim, the
#   tight-split-is-gone claim — the class is INERT for gap but still appears on
#   four tab-panel grids, now said so — and "5 sites" vs 16 form-subheads).
#   CLAUDE.md sold PROJECT_HISTORY as "v1.4.0 → current" while its last entry
#   is v1.22.28 — an entire versioning scheme (372 tags) ago; it now states the
#   real coverage and points v0.5x archaeology at the per-tag changelog here.
#   Regenerating the digest itself is its own project, deliberately NOT this
#   tag. Guard tests hold the corrected claims to the code they describe
#   (PUBLIC_PATHS walk, FailureKind walk, BLOCKING-step count) so the next
#   drift in EITHER direction goes red.
# 0.51.276: feature-brief E, first release — ONE reconciliation run. The
#   pre-coding review found ~60% of E already shipped as scattered detectors
#   (canonical health v1.23.37, placement health v1.23.25, the hourly
#   missing-placement retry sweep, stale-temp/stuck-op sweeps, boot deorphan);
#   what was missing was the brief's unifying concept: run them NOW as one
#   action with a dry-run and a single classified summary. So this tag is
#   deliberately a WRAPPER, not a subsystem. New app/core/reconcile.py
#   run_reconciliation(db, themes_dir, dry_run) verifies canonical + placement
#   health, runs the retry sweep (REUSED via a new dry_run param on
#   _retry_pending_placements — its skip semantics took eleven tags to mature
#   and are not re-implemented; the no-arg cron call is byte-identical), and
#   reports the rest: broken canonicals, broken placements, and orphaned
#   canonicals via BOTH plex_items linkages (guid_tmdb OR theme_id — the
#   guid-only join over-reported orphans 10x in the 2026-08-22 census).
#   Repair scope is exactly the sweep's existing class — every no-placement
#   local_files row without a permanent-skip reason enqueues and the place
#   WORKER applies its per-row rules; deletion and content-overwrite stay out
#   of automation per the brief. One events breadcrumb per run, including the
#   nothing-to-do run (v1.18.5). POST /api/admin/reconcile, admin-only,
#   ?dry_run=true DEFAULT (conservative by default), threadpool-offloaded.
#   Behavioral matrix test: repairable / orphan / theme_id-linked (must NOT
#   count as orphan) / broken-canonical / permanent-skip rows; idempotent
#   second run; endpoint auth + plumbing. Three mutations red — including one
#   I wrote inverted first (`or` short-circuits the wrong way for a falsy
#   guard) and redid.
# 0.51.277: feature-brief B — theme revision history (backend). Schema v79:
#   theme_revisions (deliberately NO foreign keys — history must outlive
#   section/theme churn), retained binaries under themes_dir/.revisions/
#   (inside themes_dir so moves stay same-filesystem; dot-prefixed so the
#   orphan scan and Plex never see them). Retention per the operator's
#   2026-08-24 decision: FULL metadata history, last 2 retained binaries per
#   (media_type, tmdb_id, section_id, edition_key); rotation unlinks the
#   oldest retained file and NULLs retained_path — the row survives
#   metadata-only and the API says restorable=0 instead of promising an
#   impossible rollback (the brief's rule). Recording seams are the
#   replacement chokepoints, BEFORE the old bytes are destroyed: the worker's
#   download-success path used to unlink() its stale-stash of the old
#   canonical (v1.22.40) — that inode IS the outgoing revision, so it is
#   MOVED into the store instead (zero copies; the failure paths that restore
#   the stash are untouched, and a byte-identical redownload records nothing
#   via the incoming-sha dedupe); UPLOAD MP3 captures by COPY before the new
#   bytes land (the active file must keep serving, and the non-mismatch
#   placement may share its inode — the v1.11.99 hardlink-break unlink is
#   unchanged). restore_revision captures the OUTGOING current first (restore
#   is a transition), refuses metadata-only / already-active / missing-file
#   with operator-readable reasons, updates local_files, and enqueues a place
#   job — the v0.51.272 lesson applied: a carry that stops at the DB row
#   leaves Plex playing nothing. GET /api/items/{mt}/{id}/revisions (read,
#   restorable flag per row) + POST /api/revisions/{id}/restore (admin, 409s
#   with the refusal reason). Capture is never fatal to its caller (logged
#   warning + the old unlink fallback). The upload chokepoint is tested
#   END-TO-END over HTTP (two uploads → one revision holding the FIRST
#   upload's bytes); five mutations red (retention, sha-dedupe, the
#   restore-is-a-transition capture, the place enqueue, the upload hook). UI
#   (history tab / RESTORE button) is the next tag.
# 0.51.278: feature-brief B, UI — the INFO card's revisions section. Revisions
#   ride api_item's single fetch (the card is ONE request by design — v1.23.19
#   caches one promise; a second round-trip per open is the wrong shape). The
#   section renders between loudness and file&placement, ONLY when history
#   exists (a fresh row gets no empty shell); each row shows when / source /
#   size / reason; a retained row gets // RESTORE (the MEASURE NOW button
#   shape — btn btn-tiny btn-info + adjacent status span, per the reuse rule)
#   and a rotated row says "metadata only" with the retention explanation in
#   its title instead of offering a dead button. RESTORE renders the
#   endpoint's operator-readable 409 detail inline (already-active /
#   metadata-only), disables itself against double-clicks, lands past the
#   stats TTL (bug class #7), and kicks libraryRapidPoll — the enqueued place
#   job carries the per-row marker so the chips follow. The v1.18.81 rule is
#   the test's spine: the pipe is proven at the ENDPOINT (api_item really
#   carries the rows; restore-through-the-UI-contract runs the exact call the
#   button makes end to end, including the 409 path), not just pinned in JS.
#   Three mutations red.
# 0.51.279: feature-brief A completion — dry-run on ACCEPT UPDATE. The brief's
#   remaining A criterion verbatim: "Dry-run shows proposed behavior without
#   downloading, replacing, or refreshing Plex." POST /api/updates/{mt}/{id}/
#   accept?dry_run=true returns the plan and writes NOTHING — the branch sits
#   at the handler's exact read/write boundary (everything above is the
#   pending-row fetch + the override fetch; everything below writes), so
#   returning inside the transaction commits nothing. The plan names each side
#   effect verbatim: delete_override, apply_url, edition_key, sections, and an
#   HONEST enqueue_download — it mirrors the fan-out's real gate (owning Plex
#   rows in scope) rather than claiming True unconditionally; the first draft
#   claimed True and the mutation only went red once a not-in-Plex case
#   existed to discriminate it. A's OTHER criterion (preview-both) was already
#   met by shipped code — renderPendingUpdateDiff draws thumbnail + oembed
#   title + click-through on BOTH tiles since v1.14.3/v1.19.60 — verified and
#   recorded as MET with a pin rather than rebuilt. Default behavior without
#   the param is byte-identical (pinned). Two mutations red.
# 0.51.280: feature-brief D — per-provider download health + adaptive rate.
#   The brief's own hard requirement leads: FIXED MODE BEHAVIOR IS UNCHANGED —
#   'fixed' is the default, the worker's cooldown gate sits behind an explicit
#   == "adaptive" check (mutation-verified: forcing the gate on breaks the
#   fixed-mode pin), and health is OBSERVED in both modes so the operator has
#   real evidence (GET /api/admin/provider-health) before ever opting in — the
#   reason this tag was sequenced AFTER v0.51.269's RATE_LIMITED split, which
#   is the signal it runs on. New app/core/provider_health.py: five providers
#   (url-derived), state persisted per provider in runtime_settings (the
#   brief's guardrail — a cooldown lost across a restart resumes hammering a
#   throttled provider), transitions logged to events. RATE_LIMITED → COOLDOWN
#   with 5min·2^streak backoff capped 6h + rate halves floored at adaptive_min;
#   success → GOOD + 25%-per-success recovery toward adaptive_max (gradual,
#   never a jump — pinned); NETWORK/UNKNOWN → DEGRADED only on a 3-streak;
#   VIDEO_* kinds have ZERO health impact (one dead URL must never cool a
#   provider); COOKIES_EXPIRED records last_error_class='auth' and does NOT
#   pause — a deliberate, recorded deviation from the brief (per-job failure +
#   cookies_needed already surface it, and one misclassified error must not
#   halt every download). Adaptive mode defers a cooling provider's jobs via
#   the v1.14.54 _mark_transient seam (attempt-free — cooldowns must not eat
#   retry budget) with ±20% jitter. Config: downloads.rate_mode /
#   adaptive_min_per_hour / adaptive_max_per_hour (dataclass fields, so the
#   v1.17.10 closed-set admits them — proven by a PATCH round-trip test, not
#   assumed), validation bounds, settings UI selector + min/max fields. Three
#   mutations red.
# 0.51.281: feature-brief C — trim/fade editing (backend). The design blocker
#   this feature was parked on dissolved by our own construction: a trim is a
#   lossy ffmpeg re-encode, which used to mean destroying the only copy — but
#   revision history (v0.51.277) retains the pre-edit bytes as a restorable
#   revision, so an edit is non-destructive the same way every replacement is
#   (the save's revision reason is 'replaced_by_edit', and a test asserts the
#   retained bytes are the ORIGINAL). Deliberate recorded deviations from the
#   brief's §7.2: NO loudness normalization in the editor — motif's mp3gain
#   pipeline is lossless/undoable and a second re-encode would break its undo
#   anchors, so trim/fade here and // LEVEL LOUDNESS for leveling; and
#   silence-detection deferred (the brief marks it optional). New
#   app/core/audio_edit.py: render_candidate (ffmpeg ARG ARRAY, never a shell;
#   -ss/-to after -i for sample-accurate trims; fades computed on the OUTPUT
#   timeline; server-side bounds validation; candidates under
#   themes_dir/.edit-candidates — dot-prefixed like .revisions, same FS so the
#   save is an atomic replace; TTL sweep rides each render), candidate_path
#   (32-hex traversal-safe), save_edit (NO ffmpeg needed: optimistic base_sha
#   lock — the brief's concurrent-edit guard, no locks held across ffmpeg
#   work; capture the outgoing revision; MOVE the candidate into place; clear
#   the 11 mp3gain norm columns via the worker's own _cond_columns so undo
#   anchors can't dangle; enqueue the re-place — the v0.51.272 lesson).
#   Endpoints: POST edit-theme (preview render), GET edit-candidate/{cid}.mp3
#   (range-capable stream, mirroring v1.12.90), POST /save (409 + reason on a
#   stale base or spent candidate), POST /cancel. Four mutations red. The
#   render tests skip without ffmpeg (this dev Mac); ubuntu runners carry it,
#   so CI + the release gate exercise them — verify the tag's CI skip count.
#   UI dialog is the next tag.
# 0.51.282: feature-brief C, UI — the // EDIT AUDIO dialog. THE FINAL ITEM OF
#   THE ENTIRE FEATURE BRIEF. The brief's own acceptable v1 shape: numeric
#   start/end + fade fields in the canonical dlg shell (v1.17.8; the footer is
#   the REAL .form-actions primitive — the first draft invented .dlg-actions
#   and was corrected against the upload dialog), // PREVIEW renders a server
#   candidate and plays it in an <audio> (range-capable, the .281 stream
#   endpoint), // SAVE AS NEW REVISION says exactly what happens — the
#   outgoing audio becomes a restorable revision. TRIM END prefills from the
#   card's own player duration when known. The candidate lifecycle is airtight
#   from the UI side: a re-preview discards its predecessor, ×/CANCEL/Esc all
#   discard (Esc via the <dialog> native cancel event), a successful save
#   CONSUMES the candidate so the close that follows cannot cancel a file the
#   server already moved, and the server TTL sweep backstops anything a closed
#   tab leaves behind. A save 409 re-enables SAVE with the operator-readable
#   reason visible (dialog-submit errors never auto-dismiss — the
#   design-system rule); success runs the 1100ms stats-TTL convention + rapid
#   poll so the re-place's chips follow. The card offers // EDIT AUDIO only in
#   the same conditional branch as the player (no canonical, no editor) and
#   the button carries the CURRENT sha — the save's optimistic lock. The
#   loudness hint points at // LEVEL LOUDNESS (the recorded deviation: the
#   lossless mp3gain pipeline levels; the editor only trims and fades). Three
#   mutations red.
# 0.51.283: the drawer head is a deterministic two-row layout. The operator,
#   with a prod screenshot of the freshly-deployed .274: "the mark all as read
#   and the clear all are off centered or pushed all the way to the right
#   which makes the alignment look off." The .274 fix stopped the
#   label-shredding but parked the wrapped actions row right-hugging under a
#   left-aligned title — technically wrapped, visually broken. Now, per the
#   labelled-rows-sit-LEFT idiom: title + × on row one (× moved out of the
#   actions div to a direct header child, pushed right by margin-left:auto),
#   the two text buttons LEFT-aligned on their own always-present second row
#   (flex-basis:100%, order-driven, no sometimes-wrap). Scoped to
#   #notif-drawer throughout — the LIVE OPS head (one short title + ×) is
#   untouched, and a property-matched guard pins that the ordering rules never
#   leak unscoped (property-matched because "border:" CONTAINS "order:" — the
#   test's own first draft flagged an innocent border rule, the v0.51.269
#   substring trap again). The .274 halves that were right survive: labels
#   still never break, the actions row still moves as a unit. Two mutations
#   red.
# 0.51.284: the render tests cannot silently skip in CI. v0.51.281 shipped the
#   trim/fade render path with skip-without-ffmpeg tests on the assumption
#   ubuntu runners ship ffmpeg. VERIFIED WRONG on the tag's own CI run: 4
#   skipped, identical to the dev Mac — the render path had never executed
#   anywhere, and nothing would ever have said so (a skip hides inside a green
#   run; the phantom class again, this time in test INFRASTRUCTURE rather than
#   a test). Two-part fix, each half guarding the other: both workflows
#   install ffmpeg before pytest, and both set MOTIF_REQUIRE_FFMPEG=1 — the
#   .281 test module now raises at IMPORT under the flag when ffmpeg is
#   missing, so dropping the install step turns CI red instead of quietly
#   skipping again (proven locally: the flag on this ffmpeg-less Mac errors
#   collection). The guard test parses both workflows' step ORDER (install
#   before pytest) and both pytest invocations' flag; mutation-verified by
#   dropping the install step.
# 0.51.285: dashboard carousel de-hitched (the user: "hitch every few
#   seconds"). Two independent causes, both fixed. (1) The auto-scroll ran on
#   setInterval(30ms) — not frame-aligned, so under load Chrome's timer
#   coalescing landed two ticks in one 60Hz frame and none in the next,
#   advancing the strip in 2px/0px bursts. Now a requestAnimationFrame loop
#   scaled by the measured frame gap: same 33.3px/s, one even step per painted
#   frame, dt clamped at 100ms so resuming from a throttled stretch steps
#   instead of teleporting; all four freeze guards (hidden / unfocused /
#   hover / dialog[open]) and the 3s end-dwell survive verbatim. (2) The art
#   proxy served FULL-RES Plex posters into 150px tiles — every tile scrolling
#   into view rasterized a multi-megapixel bitmap, one tile every ~5s at
#   scroll speed, which IS the reported "every few seconds" beat.
#   /api/plex/art/{rk} gains opt-in ?w= (60–1200, 422 outside): routed through
#   Plex's photo transcoder at 2:3, any refusal falls back to the full thumb
#   (a PMS without the endpoint behaves exactly as before). Only the carousel
#   passes w=300 (150 CSS px @2x); the INFO-card heroes keep full-res URLs
#   untouched.
__version__ = "0.51.285"
# 0.50.88: mobile bug batch round 3 — a much bigger sweep from on-device
#   testing. (1) TOPBAR: the op-mini job-progress pill's 220px label cap +
#   90px bar (~370px alone) plus .topbar-status having no shrink floor pushed
#   the WHOLE topbar (nav, ?/logout) past the viewport whenever a job ran —
#   shrunk the mobile op-mini caps (ops.css), gave .topbar-status min-width:0
#   + flex-wrap (crowded status cluster drops to a 2nd row instead of off-
#   screen), and floored the nav grid column at 24px so it never fully
#   vanishes. (2) LOGS: JOBS' fixed 180px ACTION column left ~9-27px per
#   fluid column — unreadable; gave rows a 760px min-width + one shared
#   horizontal scroll on .jobs-grid (header+body scroll together, no sticky
#   involved so this doesn't resurrect the v1.22.56 sticky-thead bug). EVENT
#   STREAM's fixed TIME/LEVEL/COMPONENT columns (~264px) left the message
#   column a sliver that broke one character per line — stacked the message
#   onto its own full-width row below a compact meta line instead. (3) LOGIN:
#   the v0.50.86 min-height fix only grows .auth-card when content exceeds
#   the floor, which no-error content never does — so the SAME circle with
#   the error banner added (~74px more content) had its rectangle's corners
#   (the SIGN IN button) poke past the chord and clip. New .auth-card-has-
#   error modifier (login.html) grows ONLY the height (not width, so the
#   error text's wrap — and thus how tall it is — doesn't change) to
#   min(600px,150vw); verified via an ellipse-corner-containment check across
#   320-600px viewports. (4) GLOSSARY: re-clicking // GLOSSARY while open
#   called showModal() on an already-open <dialog>, which throws — the
#   exception aborted before close() ran, leaving the button permanently
#   .open/highlighted. Now toggles: open→close(), closed→showModal().
#   (5) INFO CARD: .info-audio's bare width:100% claimed the whole <dd> row,
#   wrapping the ↓ download link onto its own flush-left line below the play
#   bar. .info-play-row flexes the two together; flex-basis:0 (not auto) on
#   .info-audio so flex-wrap's line-assignment doesn't treat the native
#   <audio controls>'s ~300px intrinsic width as unshrinkable and banish the
#   icon to line 2 by itself. (6) SETTINGS: an audit swept every tab for the
#   same class of bug fixed in v0.50.87 — found one more, the IMPORT preview
#   table (7 columns, several fixed-px), missed by that pass; wrapped in the
#   same .table-scroll. The PLEX-tab overflow reported this round matches
#   what v0.50.87 already fixed (every screenshot this round still reads
#   v0.50.86 — tested against the pre-fix build). (7) ACCESSIBILITY: closing
#   over #ops-drawer's static aria-hidden="true" (base.html) was never
#   cleared on open, so focusing any descendant (most commonly × close)
#   while it was visible tripped Chrome's aria-hidden/focus-retention block.
#   openDrawer/closeDrawer (ops.js) now toggle it in lockstep, blurring focus
#   out first on close to avoid the same violation in reverse. Also reviewed:
#   the settings "password field not in a form" DOM hints are benign (AJAX-
#   saved, not native forms — no fix needed); the requestAnimationFrame/
#   forced-reflow violations trace to bootstrap-autofill-overlay.js, a
#   browser extension, not motif's code.
