# AnimeThemes coverage — 2026-09-09, after the tag-3 bridge fix (v0.51.317)

Same July snapshot and method as 2026-09-09-baseline.md; the bridge now falls back
to season.tmdb when season.tvdb is absent (70 TV entries) and sorts TV before
MOVIE/OVA within a season. Delta vs the baseline: clean 1,059 -> 1,082 (+23),
glance 47 -> 44, has-OP 1,117 -> 1,120; nothing lost. Name search was ON for this run
(the baseline probe's rescue count was 14/154; here 15 rows resolve by name only).

Input: `motif.db` (anime shows: 1280). Bridge: 15958 entries with an AniDB id. API requests: 243.

| bucket | rows | bridged | AT any audio | AT has OP | clean | glance | name |
|---|---|---|---|---|---|---|---|
| no_theme | 376 | 339 (90%) | 311 (82%) | 301 (80%) | 300 | 5 | 6 |
| plex_served_unlinked | 328 | 322 (98%) | 306 (93%) | 304 (92%) | 293 | 12 | 1 |
| tdb_linked | 419 | 405 (96%) | 399 (95%) | 395 (94%) | 372 | 24 | 3 |
| user_set | 157 | 144 (91%) | 125 (79%) | 120 (76%) | 117 | 3 | 5 |
| all | 1280 | 1210 (94%) | 1141 (89%) | 1120 (87%) | 1082 | 44 | 15 |

