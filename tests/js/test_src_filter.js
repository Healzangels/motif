// v0.51.338: behavioural pins for the SRC-letter prune rule. The module under
// test is the LIVE code — base.html loads app/web/static/lib/src-filter.js
// before app.js, and loadLibrary() + the SRC ALL button both route through it.
//
// Run: `node --test tests/js/test_src_filter.js`
// (wrapped for the pytest gate by tests/test_v0_51_338_library_ui.py)
"use strict";
const test = require("node:test");
const assert = require("node:assert/strict");
const { keepOfferedLetters } = require("../../app/web/static/lib/src-filter.js");

// The chip shapes a library page can render: every letter plain, AT tab-only.
function chips(overrides) {
  const base = [
    { letter: "T", tabOnly: "" },
    { letter: "U", tabOnly: "" },
    { letter: "AT", tabOnly: "anime" },
    { letter: "A", tabOnly: "" },
    { letter: "M", tabOnly: "" },
    { letter: "P", tabOnly: "" },
    { letter: "Pp", tabOnly: "" },
    { letter: "-", tabOnly: "" },
    { letter: "", tabOnly: "" }, // the CLEAR button carries data-src-filter=""
  ];
  return overrides ? overrides(base) : base;
}
const withoutAM = (base) => base.filter((c) => c.letter !== "A" && c.letter !== "M");
const EVERY = ["T", "U", "AT", "A", "M", "P", "Pp", "-"];

test("a tab-only letter is kept on its own tab", () => {
  assert.deepEqual(keepOfferedLetters(["AT"], chips(), "anime"), ["AT"]);
});

test("a tab-only letter is dropped on every other tab", () => {
  for (const tab of ["movies", "tv", "collections"]) {
    assert.deepEqual(keepOfferedLetters(["AT"], chips(), tab), [], tab);
  }
});

test("dropping the hidden letter leaves the visible ones untouched, in order", () => {
  assert.deepEqual(keepOfferedLetters(["T", "AT", "Pp"], chips(), "tv"), ["T", "Pp"]);
  assert.deepEqual(keepOfferedLetters(["T", "AT", "Pp"], chips(), "anime"), ["T", "AT", "Pp"]);
});

test("a letter with no chip on the page is dropped (A / M on collections)", () => {
  assert.deepEqual(keepOfferedLetters(["A", "M", "P"], chips(withoutAM), "collections"), ["P"]);
});

test("a letter nobody renders a chip for is dropped on every tab", () => {
  for (const tab of ["movies", "tv", "anime", "collections"]) {
    assert.deepEqual(keepOfferedLetters(["ZZ", "T"], chips(), tab), ["T"], tab);
  }
});

test("the CLEAR button's empty token never becomes a letter", () => {
  assert.deepEqual(keepOfferedLetters(["", "U"], chips(), "movies"), ["U"]);
});

test("every plain-chip letter survives on every tab that renders its chip", () => {
  const plain = EVERY.filter((l) => l !== "AT");
  for (const tab of ["movies", "tv", "anime"]) {
    assert.deepEqual(keepOfferedLetters(plain, chips(), tab), plain, tab);
  }
});

test("SRC ALL on movies (the ALL branch feeds every chip letter) excludes AT", () => {
  const all = chips().map((c) => c.letter).filter(Boolean);
  const kept = keepOfferedLetters(all, chips(), "movies");
  assert.ok(!kept.includes("AT"), kept.join(","));
  assert.ok(kept.includes("T") && kept.includes("-"), kept.join(","));
});

test("SRC ALL on anime includes AT", () => {
  const all = chips().map((c) => c.letter).filter(Boolean);
  assert.ok(keepOfferedLetters(all, chips(), "anime").includes("AT"));
});

test("a Set input (libraryState.srcFilter) works and duplicates collapse", () => {
  assert.deepEqual(keepOfferedLetters(new Set(["U", "AT"]), chips(), "anime"), ["U", "AT"]);
  assert.deepEqual(keepOfferedLetters(["U", "U"], chips(), "tv"), ["U"]);
});

test("a letter offered by any chip visible on this tab is kept", () => {
  const twin = (base) => base.concat([{ letter: "AT", tabOnly: "" }]);
  assert.deepEqual(keepOfferedLetters(["AT"], chips(twin), "movies"), ["AT"]);
});

test("no chips, no letters, or no input never throw", () => {
  assert.deepEqual(keepOfferedLetters(["T"], [], "movies"), []);
  assert.deepEqual(keepOfferedLetters([], chips(), "movies"), []);
  assert.deepEqual(keepOfferedLetters(null, null, "movies"), []);
  assert.deepEqual(keepOfferedLetters(["constructor", "toString"], chips(), "movies"), []);
});
