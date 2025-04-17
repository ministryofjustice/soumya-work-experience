"""Tests for the pipeline module."""
from src.pipeline import (
    redact,
)


class TestRedact:
    """Tests for the redact function."""

    def test_with_redaction(self):
        """Test the expected functionality."""
        row = {
            "first_name": "John",
            "last_name": "Smith",
            "favourite_food": "rhubard",
        }

        columns_to_redact = ["first_name", "last_name"]

        actual = redact(
            row=row,
            columns=columns_to_redact,
        )

        expected = {
            "first_name": "***",
            "last_name": "***",
            "favourite_food": "rhubard",
        }

        assert actual == expected

    def test_no_redaction(self):
        """Test the expected functionality."""
        row = {
            "first_name": "John",
            "last_name": "Smith",
            "favourite_food": "rhubard",
        }

        columns_to_redact = []

        actual = redact(
            row=row,
            columns=columns_to_redact,
        )

        expected = {
            "first_name": "John",
            "last_name": "Smith",
            "favourite_food": "rhubard",
        }

        assert actual == expected
