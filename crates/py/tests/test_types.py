import pytest

from spatio.types import (
    DEFAULT_QUERY_LIMIT,
    DEFAULT_SEARCH_RADIUS_METERS,
    EARTH_RADIUS_METERS,
    KILOMETER,
    MAX_LATITUDE,
    MAX_LONGITUDE,
    MILE,
    MIN_LATITUDE,
    MIN_LONGITUDE,
    NAUTICAL_MILE,
    validate_coordinates,
    validate_latitude,
    validate_longitude,
)


@pytest.mark.parametrize("lat", [0.0, MIN_LATITUDE, MAX_LATITUDE, 45.0, -45.0])
def test_validate_latitude_accepts_in_range(lat):
    # Boundary values are inclusive and must not raise.
    validate_latitude(lat)


@pytest.mark.parametrize("lat", [-90.1, 90.1, 1000.0, -1000.0])
def test_validate_latitude_rejects_out_of_range(lat):
    with pytest.raises(ValueError, match="Latitude"):
        validate_latitude(lat)


@pytest.mark.parametrize("lon", [0.0, MIN_LONGITUDE, MAX_LONGITUDE, 120.0, -120.0])
def test_validate_longitude_accepts_in_range(lon):
    validate_longitude(lon)


@pytest.mark.parametrize("lon", [-180.1, 180.1, 1000.0, -1000.0])
def test_validate_longitude_rejects_out_of_range(lon):
    with pytest.raises(ValueError, match="Longitude"):
        validate_longitude(lon)


def test_validate_coordinates_ok():
    validate_coordinates(40.7128, -74.0060)


def test_validate_coordinates_rejects_bad_latitude():
    with pytest.raises(ValueError, match="Latitude"):
        validate_coordinates(95.0, 0.0)


def test_validate_coordinates_rejects_bad_longitude():
    with pytest.raises(ValueError, match="Longitude"):
        validate_coordinates(0.0, 200.0)


def test_constants_have_expected_values():
    assert (MIN_LATITUDE, MAX_LATITUDE) == (-90.0, 90.0)
    assert (MIN_LONGITUDE, MAX_LONGITUDE) == (-180.0, 180.0)
    assert DEFAULT_QUERY_LIMIT == 100
    assert DEFAULT_SEARCH_RADIUS_METERS == 1000.0
    assert EARTH_RADIUS_METERS == 6371000.0
    assert (KILOMETER, MILE, NAUTICAL_MILE) == (1000.0, 1609.34, 1852.0)
