"""
Shared parsing utilities for API data.

These functions safely parse values from NBA API responses which can have
inconsistent types (None, strings, numbers, etc.) 
"""
from typing import Any, Optional


def parse_int(x: Any) -> Optional[int]:

    if x is None:
        return None
    try:
        return int(x)
    except (ValueError, TypeError):
        return None


def parse_float(x: Any) -> Optional[float]:

    if x is None:
        return None
    try:
        return float(x)
    except (ValueError, TypeError):
        return None


def parse_bool(x: Any) -> Optional[bool]:

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
    if x is None:
        return None
    
    s = str(x).strip()
    return s if s else None