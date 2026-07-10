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
__version__ = "0.51.120"
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
