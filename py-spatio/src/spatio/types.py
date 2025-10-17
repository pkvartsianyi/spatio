"""
Type definitions and utilities for Spatio.

This module provides type aliases, protocols, and utility types
for better type safety and documentation.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from typing import Dict
from typing import List
from typing import Protocol
from typing import Tuple
from typing import Union

if TYPE_CHECKING:
    from spatio._spatio import Point

# Type aliases for common data types
KeyType = Union[bytes, str]
ValueType = Union[bytes, str]
TimestampType = Union[int, float]
DistanceType = float
CoordinateType = float

# Geographic coordinate bounds
MIN_LATITUDE = -90.0
MAX_LATITUDE = 90.0
MIN_LONGITUDE = -180.0
MAX_LONGITUDE = 180.0

# Trajectory type: list of (Point, timestamp) tuples
TrajectoryPoint = Tuple["Point", TimestampType]
Trajectory = List[TrajectoryPoint]


# Exception types
class SpatioError(Exception):
    """Base exception for Spatio errors."""

    pass


class InvalidCoordinateError(SpatioError):
    """Raised when coordinates are invalid."""

    pass


class DatabaseClosedError(SpatioError):
    """Raised when operating on a closed database."""

    pass


class ConfigurationError(SpatioError):
    """Raised when configuration is invalid."""

    pass


# Utility functions for validation
def validate_latitude(lat: float) -> None:
    """Validate latitude coordinate."""
    if not (MIN_LATITUDE <= lat <= MAX_LATITUDE):
        raise InvalidCoordinateError(
            f"Latitude must be between {MIN_LATITUDE} and {MAX_LATITUDE}, got {lat}"
        )


def validate_longitude(lon: float) -> None:
    """Validate longitude coordinate."""
    if not (MIN_LONGITUDE <= lon <= MAX_LONGITUDE):
        raise InvalidCoordinateError(
            f"Longitude must be between {MIN_LONGITUDE} and {MAX_LONGITUDE}, got {lon}"
        )


def validate_coordinates(lat: float, lon: float) -> None:
    """Validate both latitude and longitude."""
    validate_latitude(lat)
    validate_longitude(lon)


# Constants for common operations
DEFAULT_QUERY_LIMIT = 100
DEFAULT_GEOHASH_PRECISION = 8
DEFAULT_SEARCH_RADIUS_METERS = 1000.0

# Earth radius in meters (for distance calculations)
EARTH_RADIUS_METERS = 6371000.0

# Common distance constants (in meters)
KILOMETER = 1000.0
MILE = 1609.34
NAUTICAL_MILE = 1852.0
