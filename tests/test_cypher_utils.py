"""Tests for cypher utilities."""

from bento_meta.objects import Property

from bento_mdb_updates.cypher_utils import escape_quotes_in_attr


def test_escape_quotes_in_attr() -> None:
    prop = Property(
        {"handle": "Quote's Handle", "desc": """quote's quote\'s "quotes\""""},
    )
    escape_quotes_in_attr(prop)

    print(prop.handle)
    print(r"""Quote\'s Handle""")
    assert prop.handle == r"""Quote\'s Handle"""
    assert prop.desc == r"""quote\'s quote\'s \"quotes\""""
