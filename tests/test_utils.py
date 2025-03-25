"""Utilities for testing."""


def assert_actual_is_expected(actual, expected) -> None:
    """
    Custom assertion function to compare actual and expected results.
    Prints both values in case of failure for better debugging.
    """
    if actual != expected:
        print("\n=== ACTUAL ===\n", actual)
        print("\n=== EXPECTED ===\n", expected)
    assert actual == expected
