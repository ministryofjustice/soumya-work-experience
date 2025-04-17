"""Tests for the pipeline module."""
from src.pipeline import (
    redact, convert_to_datetime ,
)
from datetime import datetime
import pandas as pd
from pathlib import Path

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
            columns=columns_to_redact
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
            columns=columns_to_redact
        )

        expected = {
            "first_name": "John",
            "last_name": "Smith",
            "favourite_food": "rhubard",
        }

        assert actual == expected

class TestConvertToDatetime:
    def __validate_TimeStamps(self, row):
        try:
        # Format: year-month-day hour-minute-second
            datetime.strptime(row['event_timestamp'], "%Y-%m-%d %H:%M:%S")
            return True

        except ValueError:
            return False
        
    def test_valid_input(self):
        
        row = {"user_id":431,"event_id":2551,"event_name":"alert","event_date":"2024-08-06","event_time":"02:48:41","event_location":["51.39323","0.47713"],"event_description":"Numquam rerum repellat. Harum ad a ducimus. Qui illum hic consequatur aliquam distinctio numquam. Beatae aspernatur tenetur dolor consequatur.\nFacere corrupti aut. Qui libero nihil tenetur ipsum."}
        converted_row = convert_to_datetime(row)
        if not self.__validate_TimeStamps(converted_row):
                assert False
        assert True

        
       