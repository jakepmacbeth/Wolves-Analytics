"""
Unit tests for parsing utilities.

Tests the safe parsing functions that handle inconsistent NBA API data types.
"""
import pytest
from src.etl.parsing_utils import parse_int, parse_float, parse_bool, parse_string


class TestParseInt:
    """Test parse_int function."""
    
    def test_parse_valid_int(self):
        """Should parse valid integers."""
        assert parse_int(42) == 42
        assert parse_int("42") == 42
        assert parse_int(42.7) == 42
        assert parse_int("123") == 123
    
    def test_parse_none(self):
        """Should return None for None input."""
        assert parse_int(None) is None
    
    def test_parse_invalid(self):
        """Should return None for invalid inputs."""
        assert parse_int("invalid") is None
        assert parse_int("") is None
        assert parse_int("12.5.6") is None
        assert parse_int([1, 2, 3]) is None
        assert parse_int({"value": 42}) is None
    
    def test_parse_zero(self):
        """Should handle zero correctly."""
        assert parse_int(0) == 0
        assert parse_int("0") == 0
        assert parse_int(0.0) == 0
    
    def test_parse_negative(self):
        """Should handle negative numbers."""
        assert parse_int(-42) == -42
        assert parse_int("-42") == -42
        assert parse_int(-42.7) == -42


class TestParseFloat:
    """Test parse_float function."""
    
    def test_parse_valid_float(self):
        """Should parse valid floats."""
        assert parse_float(42.5) == 42.5
        assert parse_float("42.5") == 42.5
        assert parse_float(42) == 42.0
        assert parse_float("123.456") == 123.456
    
    def test_parse_none(self):
        """Should return None for None input."""
        assert parse_float(None) is None
    
    def test_parse_invalid(self):
        """Should return None for invalid inputs."""
        assert parse_float("invalid") is None
        assert parse_float("") is None
        assert parse_float([1.5, 2.5]) is None
    
    def test_parse_zero(self):
        """Should handle zero correctly."""
        assert parse_float(0) == 0.0
        assert parse_float(0.0) == 0.0
        assert parse_float("0.0") == 0.0
    
    def test_parse_negative(self):
        """Should handle negative numbers."""
        assert parse_float(-42.5) == -42.5
        assert parse_float("-42.5") == -42.5


class TestParseBool:
    """Test parse_bool function."""
    
    @pytest.mark.parametrize("input_val,expected", [
        (True, True),
        (False, False),
        ("true", True),
        ("TRUE", True),
        ("false", False),
        ("FALSE", False),
        ("t", True),
        ("f", False),
        ("yes", True),
        ("no", False),
        ("y", True),
        ("n", False),
        ("1", True),
        ("0", False),
        (1, True),
        (0, False),
        (42, True),
        (-1, True),
        (0.0, False),
        (1.0, True),
    ])
    def test_parse_bool_valid(self, input_val, expected):
        """Should parse various boolean representations."""
        assert parse_bool(input_val) == expected
    
    def test_parse_bool_invalid(self):
        """Should return None for invalid inputs."""
        assert parse_bool("maybe") is None
        assert parse_bool(None) is None
        assert parse_bool("") is None
        assert parse_bool([True]) is None
    
    def test_parse_bool_whitespace(self):
        """Should handle whitespace in strings."""
        assert parse_bool("  true  ") == True
        assert parse_bool("  false  ") == False


class TestParseString:
    """Test parse_string function."""
    
    def test_parse_valid_string(self):
        """Should return strings as-is."""
        assert parse_string("hello") == "hello"
        assert parse_string("Hello World") == "Hello World"
    
    def test_parse_numbers(self):
        """Should convert numbers to strings."""
        assert parse_string(42) == "42"
        assert parse_string(42.5) == "42.5"
    
    def test_parse_none(self):
        """Should return None for None input."""
        assert parse_string(None) is None
    
    def test_parse_empty(self):
        """Should return None for empty strings."""
        assert parse_string("") is None
        assert parse_string("   ") is None
    
    def test_parse_whitespace(self):
        """Should strip whitespace."""
        assert parse_string("  hello  ") == "hello"
        assert parse_string("\thello\n") == "hello"