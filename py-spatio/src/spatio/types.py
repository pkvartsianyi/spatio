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

# Spatial query results
SpatialResult = Tuple["Point", bytes, DistanceType]  # (point, value, distance)
SpatialResults = List[SpatialResult]

# Bounding box query results
BoundingBoxResult = Tuple["Point", bytes]  # (point, value)
BoundingBoxResults = List[BoundingBoxResult]

# Database statistics
DatabaseStats = Dict[str, Union[int, float]]


class PointProtocol(Protocol):
    """Protocol for point-like objects."""

    @property
    def lat(self) -> float:
        """Latitude coordinate."""
        ...

    @property
    def lon(self) -> float:
        """Longitude coordinate."""
        ...

    def distance_to(self, other: PointProtocol) -> float:
        """Calculate distance to another point."""
        ...


class DatabaseProtocol(Protocol):
    """Protocol for database-like objects."""

    def insert(
        self,
        key: KeyType,
        value: ValueType,
        options: SetOptionsProtocol | None = None,
    ) -> None:
        """Insert a key-value pair."""
        ...

    def get(self, key: KeyType) -> bytes | None:
        """Get a value by key."""
        ...

    def delete(self, key: KeyType) -> bytes | None:
        """Delete a key and return the old value."""
        ...

    def insert_point(
        self,
        prefix: str,
        point: PointProtocol,
        value: ValueType,
        options: SetOptionsProtocol | None = None,
    ) -> None:
        """Insert a geographic point."""
        ...

    def find_nearby(
        self,
        prefix: str,
        center: PointProtocol,
        radius_meters: DistanceType,
        limit: int,
    ) -> SpatialResults:
        """Find nearby points."""
        ...


class SetOptionsProtocol(Protocol):
    """Protocol for set options."""

    @classmethod
    def with_ttl(cls, ttl_seconds: float) -> SetOptionsProtocol:
        """Create options with TTL."""
        ...

    @classmethod
    def with_expiration(cls, timestamp: float) -> SetOptionsProtocol:
        """Create options with expiration timestamp."""
        ...


class ConfigProtocol(Protocol):
    """Protocol for database configuration."""

    @property
    def geohash_precision(self) -> int:
        """Geohash precision level."""
        ...

    @geohash_precision.setter
    def geohash_precision(self, value: int) -> None:
        """Set geohash precision level."""
        ...

    @classmethod
    def with_geohash_precision(cls, precision: int) -> ConfigProtocol:
        """Create config with custom geohash precision."""
        ...


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


def validate_geohash_precision(precision: int) -> None:
    """Validate geohash precision."""
    if not (1 <= precision <= 12):
        raise ConfigurationError(
            f"Geohash precision must be between 1 and 12, got {precision}"
        )


def validate_ttl(ttl_seconds: float) -> None:
    """Validate TTL value."""
    if ttl_seconds <= 0:
        raise ConfigurationError(f"TTL must be positive, got {ttl_seconds}")


def validate_distance(distance: float) -> None:
    """Validate distance value."""
    if distance < 0:
        raise ValueError(f"Distance must be non-negative, got {distance}")


def validate_limit(limit: int) -> None:
    """Validate query limit."""
    if limit <= 0:
        raise ValueError(f"Limit must be positive, got {limit}")


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

# Time constants (in seconds)
MINUTE = 60
HOUR = 3600
DAY = 86400
WEEK = 604800
