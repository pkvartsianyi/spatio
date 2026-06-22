"""
Spatio: a high-performance, embedded spatio-temporal database.

Store and query the locations of moving objects efficiently.

Example usage:
    >>> import spatio
    >>>
    >>> # Create an in-memory database
    >>> db = spatio.Spatio.memory()
    >>>
    >>> # Upsert an object's location (longitude, latitude)
    >>> nyc = spatio.Point(-74.0060, 40.7128)
    >>> db.upsert("cities", "nyc", nyc, {"population": 8_000_000})
    >>>
    >>> # Find objects within 100km, newest first
    >>> nearby = db.query_radius("cities", nyc, 100_000.0, limit=10)
    >>> print(f"Found {len(nearby)} cities nearby")
"""

from __future__ import annotations

# Import the compiled Rust extension
from spatio._spatio import Config as _Config
from spatio._spatio import DistanceMetric as _DistanceMetric
from spatio._spatio import Point as _Point
from spatio._spatio import Polygon as _Polygon
from spatio._spatio import SetOptions as _SetOptions
from spatio._spatio import Spatio as _Spatio
from spatio._spatio import TemporalPoint as _TemporalPoint
from spatio._spatio import __version__

# Re-export main classes
__all__ = [
    "Config",
    "DistanceMetric",
    "Point",
    "Polygon",
    "SetOptions",
    "Spatio",
    "TemporalPoint",
    "__version__",
]

# Type aliases for better API
Spatio = _Spatio
Point = _Point
TemporalPoint = _TemporalPoint
SetOptions = _SetOptions
Config = _Config
Polygon = _Polygon
DistanceMetric = _DistanceMetric

# Package metadata
__author__ = "Petro Kvartsianyi"
__license__ = "MIT"
