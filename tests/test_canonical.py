"""Regression tests for the canonical-theme-subdir helper."""
import unittest

from app.core.canonical import canonical_theme_subdir, sanitize_for_filesystem


class TestSanitizeForFilesystem(unittest.TestCase):
    def test_clean_passthrough(self):
        self.assertEqual(sanitize_for_filesystem("Inception"), "Inception")

    def test_replaces_colon(self):
        # v1.10.23 swapped the replacement char from `_` to `-` to
        # match Plex's own "Title - Year" folder convention.
        self.assertEqual(
            sanitize_for_filesystem("Star Wars: A New Hope"),
            "Star Wars- A New Hope",
        )

    def test_replaces_slash(self):
        self.assertEqual(sanitize_for_filesystem("Face/Off"), "Face-Off")

    def test_replaces_all_bad_chars(self):
        # Cover every char in the bad set.
        self.assertEqual(
            sanitize_for_filesystem('a/b\\c:d*e?f"g<h>i|j'),
            "a-b-c-d-e-f-g-h-i-j",
        )

    def test_collapses_whitespace(self):
        self.assertEqual(sanitize_for_filesystem("Spaces    everywhere"),
                         "Spaces everywhere")

    def test_strips_trailing_dots(self):
        # Windows hates trailing dots/spaces in dir names.
        self.assertEqual(sanitize_for_filesystem("Trouble..."), "Trouble")
        self.assertEqual(sanitize_for_filesystem("Trouble.   "), "Trouble")

    def test_empty_input(self):
        self.assertEqual(sanitize_for_filesystem(""), "untitled")
        self.assertEqual(sanitize_for_filesystem("   "), "untitled")
        self.assertEqual(sanitize_for_filesystem("..."), "untitled")


class TestCanonicalThemeSubdir(unittest.TestCase):
    def test_with_year(self):
        self.assertEqual(
            canonical_theme_subdir("Inception", "2010"),
            "Inception (2010)",
        )

    def test_without_year(self):
        self.assertEqual(canonical_theme_subdir("Some Movie", None),
                         "Some Movie")
        self.assertEqual(canonical_theme_subdir("Some Movie", ""),
                         "Some Movie")

    def test_paren_prefix_preserved(self):
        # User's reported case
        self.assertEqual(
            canonical_theme_subdir("(500) Days of Summer", "2009"),
            "(500) Days of Summer (2009)",
        )

    def test_year_pure_title(self):
        # Title that's just digits — our reported scanner regression
        self.assertEqual(canonical_theme_subdir("2010", "1984"), "2010 (1984)")

    def test_unsafe_chars_replaced(self):
        self.assertEqual(
            canonical_theme_subdir("Mission: Impossible", "1996"),
            "Mission- Impossible (1996)",
        )

    def test_empty_title_falls_back(self):
        self.assertEqual(canonical_theme_subdir("", "2020"),
                         "untitled (2020)")
        self.assertEqual(canonical_theme_subdir(None, None), "untitled")


if __name__ == "__main__":
    unittest.main()
