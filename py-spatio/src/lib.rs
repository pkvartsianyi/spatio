//! Python bindings for Spatio
//!
//! This module provides Python bindings for the Spatio spatio-temporal database using PyO3.
//! It exposes the core functionality including database operations, spatio-temporal queries,
//! and trajectory tracking.

use geo::{Distance, Haversine, Polygon as GeoPolygon};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList, PyTuple};
use spatio::DistanceMetric as RustDistanceMetric;
use spatio::Point as RustPoint;
use spatio::{
    config::{Config as RustConfig, SetOptions as RustSetOptions},
    db::DB as RustDB,
    error::Result as RustResult,
};
use std::time::{Duration, UNIX_EPOCH};

/// Convert Rust Result to Python Result
fn handle_error<T>(result: RustResult<T>) -> PyResult<T> {
    result.map_err(|e| PyRuntimeError::new_err(e.to_string()))
}

/// Python wrapper for geographic Point
#[pyclass(name = "Point")]
#[derive(Clone, Debug)]
pub struct PyPoint {
    inner: RustPoint,
}

#[pymethods]
impl PyPoint {
    /// Create a new Point with latitude and longitude.
    ///
    /// # Note
    /// Altitude is currently not supported. The `alt` parameter is accepted for
    /// API compatibility but will be ignored, as the underlying geo::Point is 2D.
    ///
    /// # Args
    ///     lat: Latitude in degrees (-90 to 90)
    ///     lon: Longitude in degrees (-180 to 180)
    ///     alt: Optional altitude (currently ignored - see note above)
    ///
    /// # Returns
    ///     A new Point instance
    ///
    /// # Raises
    ///     ValueError: If latitude or longitude are out of valid range
    #[new]
    #[pyo3(signature = (lat, lon, alt=None))]
    fn new(lat: f64, lon: f64, alt: Option<f64>) -> PyResult<Self> {
        if !(-90.0..=90.0).contains(&lat) {
            return Err(PyValueError::new_err("Latitude must be between -90 and 90"));
        }
        if !(-180.0..=180.0).contains(&lon) {
            return Err(PyValueError::new_err(
                "Longitude must be between -180 and 180",
            ));
        }

        let point = RustPoint::new(lon, lat);
        if alt.is_some() {
            // Note: geo::Point doesn't support altitude, parameter ignored
        }

        Ok(PyPoint { inner: point })
    }

    #[getter]
    fn lat(&self) -> f64 {
        self.inner.y()
    }

    #[getter]
    fn lon(&self) -> f64 {
        self.inner.x()
    }

    /// Get the altitude of the point.
    ///
    /// # Note
    /// Always returns None as altitude is not currently supported.
    #[getter]
    fn alt(&self) -> Option<f64> {
        None // geo::Point doesn't support altitude in current version
    }

    fn __repr__(&self) -> String {
        if let Some(alt) = self.alt() {
            format!("Point(lat={}, lon={}, alt={})", self.lat(), self.lon(), alt)
        } else {
            format!("Point(lat={}, lon={})", self.lat(), self.lon())
        }
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }

    /// Calculate distance to another point in meters using Haversine formula
    fn distance_to(&self, other: &PyPoint) -> f64 {
        Haversine.distance(self.inner, other.inner)
    }
}

/// Python wrapper for distance metrics
#[pyclass(name = "DistanceMetric")]
#[derive(Clone, Debug)]
pub struct PyDistanceMetric {
    inner: RustDistanceMetric,
}

#[pymethods]
impl PyDistanceMetric {
    #[classattr]
    const HAVERSINE: &'static str = "haversine";
    #[classattr]
    const GEODESIC: &'static str = "geodesic";
    #[classattr]
    const RHUMB: &'static str = "rhumb";
    #[classattr]
    const EUCLIDEAN: &'static str = "euclidean";

    #[new]
    fn new(metric: &str) -> PyResult<Self> {
        let inner = match metric.to_lowercase().as_str() {
            "haversine" => RustDistanceMetric::Haversine,
            "geodesic" => RustDistanceMetric::Geodesic,
            "rhumb" => RustDistanceMetric::Rhumb,
            "euclidean" => RustDistanceMetric::Euclidean,
            _ => {
                return Err(PyValueError::new_err(
                    "Invalid metric. Use 'haversine', 'geodesic', 'rhumb', or 'euclidean'",
                ));
            }
        };
        Ok(PyDistanceMetric { inner })
    }

    fn __repr__(&self) -> String {
        format!("DistanceMetric({:?})", self.inner)
    }
}

/// Python wrapper for SetOptions
#[pyclass(name = "SetOptions")]
#[derive(Clone, Debug)]
pub struct PySetOptions {
    inner: RustSetOptions,
}

#[pymethods]
impl PySetOptions {
    #[new]
    fn new() -> Self {
        PySetOptions {
            inner: RustSetOptions::default(),
        }
    }

    /// Create SetOptions with TTL in seconds
    #[staticmethod]
    fn with_ttl(ttl_seconds: f64) -> PyResult<Self> {
        if !ttl_seconds.is_finite() {
            return Err(PyValueError::new_err(
                "TTL must be finite (not NaN or infinity)",
            ));
        }
        if ttl_seconds <= 0.0 {
            return Err(PyValueError::new_err("TTL must be positive"));
        }
        if ttl_seconds > u64::MAX as f64 {
            return Err(PyValueError::new_err("TTL is too large"));
        }

        let duration = Duration::from_secs_f64(ttl_seconds);
        Ok(PySetOptions {
            inner: RustSetOptions::with_ttl(duration),
        })
    }

    /// Create SetOptions with absolute expiration timestamp
    #[staticmethod]
    fn with_expiration(timestamp: f64) -> PyResult<Self> {
        if !timestamp.is_finite() {
            return Err(PyValueError::new_err(
                "Timestamp must be finite (not NaN or infinity)",
            ));
        }
        if timestamp < 0.0 {
            return Err(PyValueError::new_err("Timestamp must be non-negative"));
        }
        if timestamp > u64::MAX as f64 {
            return Err(PyValueError::new_err("Timestamp is too large"));
        }

        let system_time = UNIX_EPOCH + Duration::from_secs_f64(timestamp);
        Ok(PySetOptions {
            inner: RustSetOptions::with_expiration(system_time),
        })
    }
}

/// Python wrapper for database Config
#[pyclass(name = "Config")]
#[derive(Clone, Debug)]
pub struct PyConfig {
    inner: RustConfig,
}

#[pymethods]
impl PyConfig {
    #[new]
    fn new() -> Self {
        PyConfig {
            inner: RustConfig::default(),
        }
    }

    /// Create config with custom geohash precision (1-12)
    #[staticmethod]
    fn with_geohash_precision(precision: usize) -> PyResult<Self> {
        if !(1..=12).contains(&precision) {
            return Err(PyValueError::new_err(
                "Geohash precision must be between 1 and 12",
            ));
        }

        Ok(PyConfig {
            inner: RustConfig::with_geohash_precision(precision),
        })
    }

    #[getter]
    fn geohash_precision(&self) -> usize {
        self.inner.geohash_precision
    }

    #[setter]
    fn set_geohash_precision(&mut self, precision: usize) -> PyResult<()> {
        if !(1..=12).contains(&precision) {
            return Err(PyValueError::new_err(
                "Geohash precision must be between 1 and 12",
            ));
        }
        self.inner.geohash_precision = precision;
        Ok(())
    }
}

/// Main Spatio database class
#[pyclass(name = "Spatio")]
pub struct PySpatio {
    db: RustDB,
}

#[pymethods]
impl PySpatio {
    /// Create an in-memory Spatio database
    #[staticmethod]
    fn memory() -> PyResult<Self> {
        let db = handle_error(RustDB::memory())?;
        Ok(PySpatio { db })
    }

    /// Create an in-memory database with custom configuration
    #[staticmethod]
    fn memory_with_config(config: &PyConfig) -> PyResult<Self> {
        let db = handle_error(RustDB::memory_with_config(config.inner.clone()))?;
        Ok(PySpatio { db })
    }

    /// Open a persistent Spatio database from file
    #[staticmethod]
    fn open(path: &str) -> PyResult<Self> {
        let db = handle_error(RustDB::open(path))?;
        Ok(PySpatio { db })
    }

    /// Open a persistent database with custom configuration
    #[staticmethod]
    fn open_with_config(path: &str, config: &PyConfig) -> PyResult<Self> {
        let db = handle_error(RustDB::open_with_config(path, config.inner.clone()))?;
        Ok(PySpatio { db })
    }

    /// Insert a key-value pair
    #[pyo3(signature = (key, value, options=None))]
    fn insert(
        &self,
        key: &Bound<'_, PyBytes>,
        value: &Bound<'_, PyBytes>,
        options: Option<&PySetOptions>,
    ) -> PyResult<()> {
        let key_bytes = key.as_bytes();
        let value_bytes = value.as_bytes();
        let opts = options.map(|o| o.inner.clone());

        handle_error(self.db.insert(key_bytes, value_bytes, opts))?;
        Ok(())
    }

    /// Get a value by key, returns None if not found
    fn get(&self, key: &Bound<'_, PyBytes>) -> PyResult<Option<PyObject>> {
        let key_bytes = key.as_bytes();
        let result = handle_error(self.db.get(key_bytes))?;

        Python::with_gil(|py| match result {
            Some(bytes) => Ok(Some(PyBytes::new(py, &bytes).into())),
            None => Ok(None),
        })
    }

    /// Delete a key, returns the old value if it existed
    fn delete(&self, key: &Bound<'_, PyBytes>) -> PyResult<Option<PyObject>> {
        let key_bytes = key.as_bytes();
        let result = handle_error(self.db.delete(key_bytes))?;

        Python::with_gil(|py| match result {
            Some(bytes) => Ok(Some(PyBytes::new(py, &bytes).into())),
            None => Ok(None),
        })
    }

    /// Insert a geographic point with automatic spatial indexing
    #[pyo3(signature = (prefix, point, value, options=None))]
    fn insert_point(
        &self,
        prefix: &str,
        point: &PyPoint,
        value: &Bound<'_, PyBytes>,
        options: Option<&PySetOptions>,
    ) -> PyResult<()> {
        let value_bytes = value.as_bytes();
        let opts = options.map(|o| o.inner.clone());

        handle_error(
            self.db
                .insert_point(prefix, &point.inner, value_bytes, opts),
        )
    }

    /// Find nearby points within a radius
    fn query_within_radius(
        &self,
        prefix: &str,
        center: &PyPoint,
        radius_meters: f64,
        limit: usize,
    ) -> PyResult<PyObject> {
        let results =
            handle_error(
                self.db
                    .query_within_radius(prefix, &center.inner, radius_meters, limit),
            )?;

        Python::with_gil(|py| {
            let py_list = PyList::empty(py);
            for (point, value) in results {
                let py_point = PyPoint { inner: point };
                let py_value = PyBytes::new(py, &value);
                let distance = Haversine.distance(center.inner, point);
                let tuple = (py_point, py_value, distance).into_pyobject(py)?;
                py_list.append(tuple)?;
            }
            Ok(py_list.into())
        })
    }

    /// Insert trajectory data for an object
    #[pyo3(signature = (object_id, trajectory, options=None))]
    fn insert_trajectory(
        &self,
        object_id: &str,
        trajectory: &Bound<'_, PyList>,
        options: Option<&PySetOptions>,
    ) -> PyResult<()> {
        let mut rust_trajectory = Vec::new();

        for item in trajectory.iter() {
            let tuple = item.downcast::<PyTuple>()?;
            if tuple.len() != 2 {
                return Err(PyValueError::new_err(
                    "Trajectory items must be (Point, timestamp) tuples",
                ));
            }

            let point_ref: PyRef<PyPoint> = tuple.get_item(0)?.extract()?;
            let point = point_ref.clone();
            let timestamp_f64: f64 = tuple.get_item(1)?.extract()?;
            let timestamp = UNIX_EPOCH + Duration::from_secs(timestamp_f64 as u64);

            rust_trajectory.push(spatio::config::TemporalPoint {
                point: point.inner,
                timestamp,
            });
        }

        let opts = options.map(|o| o.inner.clone());
        handle_error(self.db.insert_trajectory(object_id, &rust_trajectory, opts))
    }

    /// Query trajectory data for a time range
    fn query_trajectory(
        &self,
        object_id: &str,
        start_time: f64,
        end_time: f64,
    ) -> PyResult<PyObject> {
        let results = handle_error(self.db.query_trajectory(
            object_id,
            start_time as u64,
            end_time as u64,
        ))?;

        Python::with_gil(|py| {
            let py_list = PyList::empty(py);
            for temporal_point in results {
                let py_point = PyPoint {
                    inner: temporal_point.point,
                };
                let timestamp_f64 = temporal_point
                    .timestamp
                    .duration_since(UNIX_EPOCH)
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?
                    .as_secs_f64();
                let tuple = (py_point, timestamp_f64).into_pyobject(py)?;
                py_list.append(tuple)?;
            }
            Ok(py_list.into())
        })
    }

    /// Check if any points exist within a radius
    fn contains_point(&self, prefix: &str, center: &PyPoint, radius_meters: f64) -> PyResult<bool> {
        handle_error(self.db.contains_point(prefix, &center.inner, radius_meters))
    }

    /// Count points within a distance
    fn count_within_radius(
        &self,
        prefix: &str,
        center: &PyPoint,
        radius_meters: f64,
    ) -> PyResult<usize> {
        handle_error(
            self.db
                .count_within_radius(prefix, &center.inner, radius_meters),
        )
    }

    /// Check if any points exist within a bounding box
    fn intersects_bounds(
        &self,
        prefix: &str,
        min_lat: f64,
        min_lon: f64,
        max_lat: f64,
        max_lon: f64,
    ) -> PyResult<bool> {
        handle_error(
            self.db
                .intersects_bounds(prefix, min_lat, min_lon, max_lat, max_lon),
        )
    }

    /// Find all points within a bounding box
    fn find_within_bounds(
        &self,
        prefix: &str,
        min_lat: f64,
        min_lon: f64,
        max_lat: f64,
        max_lon: f64,
        limit: usize,
    ) -> PyResult<PyObject> {
        let results = handle_error(
            self.db
                .find_within_bounds(prefix, min_lat, min_lon, max_lat, max_lon, limit),
        )?;

        Python::with_gil(|py| {
            let py_list = PyList::empty(py);
            for (point, value) in results {
                let py_point = PyPoint { inner: point };
                let py_value = PyBytes::new(py, &value);
                let tuple = (py_point, py_value).into_pyobject(py)?;
                py_list.append(tuple)?;
            }
            Ok(py_list.into())
        })
    }

    /// Calculate distance between two points using a specified metric
    fn distance_between(
        &self,
        point1: &PyPoint,
        point2: &PyPoint,
        metric: &PyDistanceMetric,
    ) -> PyResult<f64> {
        handle_error(
            self.db
                .distance_between(&point1.inner, &point2.inner, metric.inner),
        )
    }

    /// Find K nearest neighbors to a query point
    fn knn(
        &self,
        prefix: &str,
        center: &PyPoint,
        k: usize,
        max_radius: f64,
        metric: &PyDistanceMetric,
    ) -> PyResult<PyObject> {
        let results =
            handle_error(
                self.db
                    .knn(prefix, &center.inner, k, max_radius, metric.inner),
            )?;

        Python::with_gil(|py| {
            let py_list = PyList::empty(py);
            for (point, value, distance) in results {
                let py_point = PyPoint { inner: point };
                let py_value = PyBytes::new(py, &value);
                let tuple = (py_point, py_value, distance).into_pyobject(py)?;
                py_list.append(tuple)?;
            }
            Ok(py_list.into())
        })
    }

    /// Query points within a polygon boundary
    fn query_within_polygon(
        &self,
        prefix: &str,
        polygon_coords: &Bound<'_, PyList>,
        limit: usize,
    ) -> PyResult<PyObject> {
        // Parse polygon coordinates from list of (lon, lat) tuples
        let mut coords = Vec::new();
        for item in polygon_coords.iter() {
            let tuple = item.downcast::<PyTuple>()?;
            if tuple.len() != 2 {
                return Err(PyValueError::new_err(
                    "Polygon coordinates must be (lon, lat) tuples",
                ));
            }
            let lon: f64 = tuple.get_item(0)?.extract()?;
            let lat: f64 = tuple.get_item(1)?.extract()?;
            coords.push(geo::coord! { x: lon, y: lat });
        }

        if coords.len() < 3 {
            return Err(PyValueError::new_err(
                "Polygon must have at least 3 coordinates",
            ));
        }

        // Create polygon
        let polygon = GeoPolygon::new(geo::LineString::from(coords), vec![]);

        let results = handle_error(self.db.query_within_polygon(prefix, &polygon, limit))?;

        Python::with_gil(|py| {
            let py_list = PyList::empty(py);
            for (point, value) in results {
                let py_point = PyPoint { inner: point };
                let py_value = PyBytes::new(py, &value);
                let tuple = (py_point, py_value).into_pyobject(py)?;
                py_list.append(tuple)?;
            }
            Ok(py_list.into())
        })
    }

    /// Force sync to disk
    fn sync(&self) -> PyResult<()> {
        handle_error(self.db.sync())
    }

    /// Get database statistics
    fn stats(&self) -> PyResult<PyObject> {
        let stats = handle_error(self.db.stats())?;

        Python::with_gil(|py| {
            let dict = pyo3::types::PyDict::new(py);
            dict.set_item("key_count", stats.key_count)?;
            dict.set_item("expired_count", stats.expired_count)?;
            dict.set_item("operations_count", stats.operations_count)?;
            Ok(dict.into())
        })
    }

    /// Close the database
    fn close(&mut self) -> PyResult<()> {
        // For now, this is a no-op since DB doesn't implement mutable close
        Ok(())
    }

    fn __repr__(&self) -> String {
        "Spatio(database)".to_string()
    }
}

/// Python module definition
#[pymodule]
fn _spatio(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PySpatio>()?;
    m.add_class::<PyPoint>()?;
    m.add_class::<PySetOptions>()?;
    m.add_class::<PyConfig>()?;
    m.add_class::<PyDistanceMetric>()?;

    // Add version
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;

    Ok(())
}
