# AnimeThemes coverage — 2026-09-11, series close-out (v0.51.328)

Same July snapshot and method as 2026-09-09-tag3-bridge-fix.md, run on the
shipped code after tags 3b–5 (v0.51.318–.327) and the v0.51.328 TMDB-movie
index. Name search ON (the sweep runs it OFF; the picker runs it on a click).
Result: identical to the tag-3 report — clean 1,082 / glance 44 / name 15 of
1,280; 1,120 (87 %) have an opening with audio — so nothing between .318 and
.327 moved the resolver, as intended (those tags changed the picker, the
review page and the polish, not `resolve`). The movie index cannot show here:
this library's anime section holds 1,280 shows and no films.

The numbers spec §6 decision 3 (a SRC letter) waited on, read from the table:
a full APPLY SELECTED would source **300 no-theme rows** (80 % of that bucket)
and, as backups, **293 Plex-served rows** — up to ~593 of 1,280 anime rows
(46 % of the tab) carrying AnimeThemes provenance while reading `U` in the SRC
column, indistinguishable from a hand-typed URL. That is the case for a
letter; the case against is the six-site cost and that a cherry-picked
handful never needs one.

Input: `motif.db` (anime shows: 1280). Bridge: 15958 entries with an AniDB id. API requests: 243.

| bucket | rows | bridged | AT any audio | AT has OP | clean | glance | name |
|---|---|---|---|---|---|---|---|
| no_theme | 376 | 339 (90%) | 311 (82%) | 301 (80%) | 300 | 5 | 6 |
| plex_served_unlinked | 328 | 322 (98%) | 306 (93%) | 304 (92%) | 293 | 12 | 1 |
| tdb_linked | 419 | 405 (96%) | 399 (95%) | 395 (94%) | 372 | 24 | 3 |
| user_set | 157 | 144 (91%) | 125 (79%) | 120 (76%) | 117 | 3 | 5 |
| all | 1280 | 1210 (94%) | 1141 (89%) | 1120 (87%) | 1082 | 44 | 15 |

Default-pick audio for the no-theme bucket: ~869 MB.

## misses — no_theme (65)

- Action Monster Big Movie (2026) — no bridge entry for this row's guids
- Angel Sanctuary (2000) — bridged but no AnimeThemes entry with audio
- Crowned in a Hundred Days (2026) — no bridge entry for this row's guids
- GHOST CONCERT : missing Songs (2026) — bridged but no AnimeThemes entry with audio
- Gamera: Rebirth (2023) — no bridge entry for this row's guids
- Genocyber (1994) — bridged but no AnimeThemes entry with audio
- Goddamn (1990) — bridged but no AnimeThemes entry with audio
- Gundam Breaker: Battlogue (2021) — bridged but no AnimeThemes entry with audio
- Gundam: Requiem for Vengeance (2024) — bridged but no AnimeThemes entry with audio
- Hakugei: Legend of the Moby Dick (1997) — bridged but no AnimeThemes entry with audio
- Heaven Official's Blessing (2020) — bridged but no AnimeThemes entry with audio
- Hello! Sandybell (1981) — bridged but no AnimeThemes entry with audio
- Hero Return (2020) — bridged but no AnimeThemes entry with audio
- Idaten Jump (2005) — no bridge entry for this row's guids
- Jentry Chau vs. the Underworld (2024) — no bridge entry for this row's guids
- Journal of the Mysterious Creatures (2019) — no bridge entry for this row's guids
- Junji Ito Maniac: Japanese Tales of the Macabre (2023) — bridged but no AnimeThemes entry with audio
- Left-Hand Layup! (2023) — no bridge entry for this row's guids
- Leviathan (2025) — bridged but no AnimeThemes entry with audio
- Ling Cage (2019) — no bridge entry for this row's guids
- Love Through a Prism (2026) — bridged but no AnimeThemes entry with audio
- Marvel's Future Avengers (2017) — bridged but no AnimeThemes entry with audio
- Mega Man Star Force (2006) — bridged but no AnimeThemes entry with audio
- Meiji Gekken: 1874 (2024) — bridged but no AnimeThemes entry with audio
- Monster But Wild (2023) — no bridge entry for this row's guids
- My Daemon (2023) — no bridge entry for this row's guids
- Mysteria Friends (2019) — bridged but no AnimeThemes entry with audio
- Naruto - The Ocean Cut (2022) — no bridge entry for this row's guids
- One Pace (2020) — no bridge entry for this row's guids
- One Piece Log: Fish-Man Island Saga (2024) — no bridge entry for this row's guids
- Onimusha (2023) — bridged but no AnimeThemes entry with audio
- Onmyoji (2023) — no bridge entry for this row's guids
- Phoenix: Eden17 (2023) — bridged but no AnimeThemes entry with audio
- Pokémon: Hisuian Snow (2022) — bridged but no AnimeThemes entry with audio
- Pokémon: Paldean Winds (2023) — bridged but no AnimeThemes entry with audio
- Pokémon: Path to the Peak (2023) — no bridge entry for this row's guids
- Pucca: Love Recipe (2018) — no bridge entry for this row's guids
- Pyramid Game (2025) — no bridge entry for this row's guids
- Room Camp (2020) — no bridge entry for this row's guids
- Shin Captain Tsubasa (1989) — no bridge entry for this row's guids

## misses — plex_served_unlinked (22)

- Appleseed XIII (2011) — bridged but no AnimeThemes entry with audio
- Beyblade: Metal Fight Beyblade (2009) — bridged but no AnimeThemes entry with audio
- Blade (2011) — bridged but no AnimeThemes entry with audio
- Blades of the Guardians (2023) — no bridge entry for this row's guids
- Blood of Zeus (2020) — no bridge entry for this row's guids
- Chibi Devi! (2011) — bridged but no AnimeThemes entry with audio
- Digimon Fusion (2010) — bridged but no AnimeThemes entry with audio
- Guyver: The Bioboosted Armor (2005) — bridged but no AnimeThemes entry with audio
- Hamtaro (2000) — bridged but no AnimeThemes entry with audio
- Hellsing Ultimate (2006) — bridged but no AnimeThemes entry with audio
- Helluva Boss (2019) — no bridge entry for this row's guids
- Iron Man (2010) — bridged but no AnimeThemes entry with audio
- Megazone 23 (1989) — bridged but no AnimeThemes entry with audio
- Powerpuff Girls Z (2006) — bridged but no AnimeThemes entry with audio
- RESIDENT EVIL: Infinite Darkness (2021) — bridged but no AnimeThemes entry with audio
- Samurai Jack (2001) — no bridge entry for this row's guids
- Scott Pilgrim Takes Off (2023) — bridged but no AnimeThemes entry with audio
- Star Wars: Visions (2021) — bridged but no AnimeThemes entry with audio
- The Garden of Sinners (2007) — no bridge entry for this row's guids
- Transformers: Armada (2002) — bridged but no AnimeThemes entry with audio
- Wolverine (2011) — bridged but no AnimeThemes entry with audio
- Æon Flux (1991) — no bridge entry for this row's guids

## misses — tdb_linked (20)

- Arcane (2021) — no bridge entry for this row's guids
- Avatar: The Last Airbender (2005) — no bridge entry for this row's guids
- Beyblade (2001) — bridged but no AnimeThemes entry with audio
- Blue Eye Samurai (2023) — no bridge entry for this row's guids
- Captain Laserhawk: A Blood Dragon Remix (2023) — no bridge entry for this row's guids
- Castlevania (2017) — no bridge entry for this row's guids
- Castlevania: Nocturne (2023) — no bridge entry for this row's guids
- DOTA: Dragon's Blood (2021) — no bridge entry for this row's guids
- Devil May Cry (2025) (2025) — no bridge entry for this row's guids
- Dragon Raja -The Blazing Dawn- (2022) — bridged but no AnimeThemes entry with audio
- Final Fantasy: Legend of the Crystals (1994) — bridged but no AnimeThemes entry with audio
- Fog Hill of Five Elements (2020) — no bridge entry for this row's guids
- GOOD NIGHT WORLD (2023) — bridged but no AnimeThemes entry with audio
- Gundam Build Metaverse (2023) — bridged but no AnimeThemes entry with audio
- PLUTO (2023) — bridged but no AnimeThemes entry with audio
- Pacific Rim: The Black (2021) — no bridge entry for this row's guids
- Sand Land: The Series (2024) — bridged but no AnimeThemes entry with audio
- Spare Me, Great Lord! (2021) — bridged but no AnimeThemes entry with audio
- The Legend of Korra (2012) — no bridge entry for this row's guids
- Zoids: Fuzors (2003) — bridged but no AnimeThemes entry with audio

## misses — user_set (32)

- 3x3 Eyes (1991) — bridged but no AnimeThemes entry with audio
- 4 CUT HERO (2023) — no bridge entry for this row's guids
- A Will Eternal (2020) — no bridge entry for this row's guids
- AMAIM: Warrior at the Borderline UltraSteel Ogre-Gear (2023) — bridged but no AnimeThemes entry with audio
- Akuma Kun (2023) — no bridge entry for this row's guids
- Another Journey to the West (2024) — no bridge entry for this row's guids
- BAKI-DOU: The Invincible Samurai (2026) — no bridge entry for this row's guids
- Babel II: Beyond Infinity (2001) — bridged but no AnimeThemes entry with audio
- Beast Wars II: Super Lifeform Transformers (1998) — bridged but no AnimeThemes entry with audio
- Beast Wars Neo: Super Lifeform Transformers (1999) — bridged but no AnimeThemes entry with audio
- Birdy the Mighty (1996) — bridged but no AnimeThemes entry with audio
- Blue Submarine No. 6 (1998) — bridged but no AnimeThemes entry with audio
- Brotherhood: Final Fantasy XV (2016) — bridged but no AnimeThemes entry with audio
- Bureau of Paranormal Investigation (2020) — bridged but no AnimeThemes entry with audio
- Dallos (1983) — bridged but no AnimeThemes entry with audio
- Dandelion (2026) — bridged but no AnimeThemes entry with audio
- Demon Spirit Seed Manual (2020) — no bridge entry for this row's guids
- Eden (2021) — bridged but no AnimeThemes entry with audio
- Eiken (2003) — bridged but no AnimeThemes entry with audio
- Exception (2022) — bridged but no AnimeThemes entry with audio
- Fights Break Sphere (2017) — bridged but no AnimeThemes entry with audio
- Fist of the North Star: HOKUTO NO KEN (2026) — bridged but no AnimeThemes entry with audio
- Hi Hi Puffy AmiYumi (2004) — no bridge entry for this row's guids
- Pokémon Concierge (2023) — bridged but no AnimeThemes entry with audio
- Pokémon Evolutions (2021) — bridged but no AnimeThemes entry with audio
- Pokémon: Origins (2013) — bridged but no AnimeThemes entry with audio
- Pokémon: The Arceus Chronicles (2022) — bridged but no AnimeThemes entry with audio
- Pokémon: Twilight Wings (2020) — bridged but no AnimeThemes entry with audio
- Power Stone (1999) — bridged but no AnimeThemes entry with audio
- Pucca (2006) — bridged but no AnimeThemes entry with audio
- Rakshasa Street (2016) — no bridge entry for this row's guids
- Yu-Gi-Oh! Card Game: The Chronicles (2025) — no bridge entry for this row's guids
