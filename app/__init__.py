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
__version__ = "0.50.97"
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
