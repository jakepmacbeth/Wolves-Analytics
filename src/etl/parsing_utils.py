"""
Shared parsing utilities for NBA API data.

These functions safely parse values from NBA API responses which can have
inconsistent types (None, strings, numbers, etc.) into strongly-typed values.
"""
from typing import Any, Optional


def parse_int(x: Any) -> Optional[int]:
    """
    Safely parse value to integer.
    
    Args:
        x: Value to parse (can be None, int, float, str)
    
    Returns:
        int if parseable, None otherwise
    
    Examples:
        >>> parse_int(42)
        42
        >>> parse_int("42")
        42
        >>> parse_int(42.7)
        42
        >>> parse_int(None)
        None
        >>> parse_int("invalid")
        None
    """
    if x is None:
        return None
    try:
        return int(x)
    except (ValueError, TypeError):
        return None


def parse_float(x: Any) -> Optional[float]:
    """
    Safely parse value to float.
    
    Args:
        x: Value to parse (can be None, int, float, str)
    
    Returns:
        float if parseable, None otherwise
    
    Examples:
        >>> parse_float(42.5)
        42.5
        >>> parse_float("42.5")
        42.5
        >>> parse_float(42)
        42.0
        >>> parse_float(None)
        None
        >>> parse_float("invalid")
        None
    """
    if x is None:
        return None
    try:
        return float(x)
    except (ValueError, TypeError):
        return None


def parse_bool(x: Any) -> Optional[bool]:
    """
    Safely parse value to boolean.
    
    Handles multiple representations of true/false:
    - Python booleans: True, False
    - Numbers: 1 (True), 0 (False), any non-zero (True)
    - Strings: "true", "t", "yes", "y", "1" (True, case-insensitive)
              "false", "f", "no", "n", "0" (False, case-insensitive)
    
    Args:
        x: Value to parse
    
    Returns:
        bool if parseable, None otherwise
    
    Examples:
        >>> parse_bool(True)
        True
        >>> parse_bool("yes")
        True
        >>> parse_bool(1)
        True
        >>> parse_bool("FALSE")
        False
        >>> parse_bool(0)
        False
        >>> parse_bool(None)
        None
        >>> parse_bool("maybe")
        None
    """
    if x is None:
        return None
    
    # Already a boolean
    if isinstance(x, bool):
        return x
    
    # Numeric values
    if isinstance(x, (int, float)):
        return bool(x)
    
    # String values
    if isinstance(x, str):
        lower = x.lower().strip()
        if lower in ("true", "t", "1", "yes", "y"):
            return True
        if lower in ("false", "f", "0", "no", "n"):
            return False
    
    return None


def parse_string(x: Any) -> Optional[str]:
    """
    Safely convert value to string.
    
    Args:
        x: Value to convert
    
    Returns:
        str if value exists, None for None/empty strings
    
    Examples:
        >>> parse_string("hello")
        'hello'
        >>> parse_string(42)
        '42'
        >>> parse_string(None)
        None
        >>> parse_string("")
        None
    """
    if x is None:
        return None
    
    s = str(x).strip()
    return s if s else None