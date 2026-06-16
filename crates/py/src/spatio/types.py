"""Coordinate validation helpers and constants for Spatio."""

from __future__ import annotations

# Geographic coordinate bounds
MIN_LATITUDE = -90.0
MAX_LATITUDE = 90.0
MIN_LONGITUDE = -180.0
MAX_LONGITUDE = 180.0


def validate_latitude(lat: float) -> None:
    """Validate a latitude coordinate, raising ValueError if out of range."""
    if not (MIN_LATITUDE <= lat <= MAX_LATITUDE):
        raise ValueError(
            f"Latitude must be between {MIN_LATITUDE} and {MAX_LATITUDE}, got {lat}"
        )


def validate_longitude(lon: float) -> None:
    """Validate a longitude coordinate, raising ValueError if out of range."""
    if not (MIN_LONGITUDE <= lon <= MAX_LONGITUDE):
        raise ValueError(
            f"Longitude must be between {MIN_LONGITUDE} and {MAX_LONGITUDE}, got {lon}"
        )


def validate_coordinates(lat: float, lon: float) -> None:
    """Validate both latitude and longitude."""
    validate_latitude(lat)
    validate_longitude(lon)


# Common defaults and distance constants (meters)
DEFAULT_QUERY_LIMIT = 100
DEFAULT_SEARCH_RADIUS_METERS = 1000.0
EARTH_RADIUS_METERS = 6371000.0
KILOMETER = 1000.0
MILE = 1609.34
NAUTICAL_MILE = 1852.0
