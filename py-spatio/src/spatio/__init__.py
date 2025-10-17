"""
Spatio: A blazingly fast spatial database library

Spatio is a high-performance, embedded spatio-temporal database designed for
applications that need to store and query location-based data efficiently.

Example usage:
    >>> import spatio
    >>>
    >>> # Create an in-memory database
    >>> db = spatio.Spatio.memory()
    >>>
    >>> # Store a simple key-value pair
    >>> db.insert(b"user:123", b"John Doe")
    >>>
    >>> # Store a geographic point
    >>> nyc = spatio.Point(40.7128, -74.0060)
    >>> db.insert_point("cities", nyc, b"New York City")
    >>>
    >>> # Find nearby points within 100km
    >>> nearby = db.find_nearby("cities", nyc, 100_000.0, 10)
    >>> print(f"Found {len(nearby)} cities nearby")
"""

from __future__ import annotations

# Import the compiled Rust extension
from spatio._spatio import Config as _Config
from spatio._spatio import Point as _Point
from spatio._spatio import SetOptions as _SetOptions
from spatio._spatio import Spatio as _Spatio
from spatio._spatio import __version__

# Re-export main classes
__all__ = [
    "Config",
    "Point",
    "SetOptions",
    "Spatio",
    "__version__",
]

# Type aliases for better API
Spatio = _Spatio
Point = _Point
SetOptions = _SetOptions
Config = _Config

# Package metadata
__author__ = "Petro Kvartsianyi"
__email__ = "pkvartsianyi@example.com"
__license__ = "MIT"

# Version validation
try:
    # Ensure the version is accessible
    _ = __version__
except AttributeError:
    __version__ = "unknown"
