//! Python bindings for Spatio
//!
//! This module provides Python bindings for the Spatio spatio-temporal database using PyO3.
//! It exposes the core functionality including database operations, spatio-temporal queries,
//! and trajectory tracking.

// All geo types are now accessed through spatio wrappers
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList, PyTuple};
use spatio::DistanceMetric as RustDistanceMetric;
use spatio::Point as RustPoint;
use spatio::Polygon as RustPolygon;
use spatio::{
    SyncDB as RustDB,
    config::{Config as RustConfig, SetOptions as RustSetOptions},
    error::Result as RustResult,
};
use std::time::{Duration, UNIX_EPOCH};

/// Convert Rust Result to Python Result
fn handle_error<T>(result: RustResult<T>) -> PyResult<T> {
    result.map_err(|e| PyRuntimeError::new_err(e.to_string()))
}

/// Python wrapper for geographic Point (2D only - altitude not currently supported)
///
/// Note: The `alt` parameter is accepted for API compatibility but ignored,
/// as the underlying geo::Point type is 2D.
#[pyclass(name = "Point")]
#[derive(Clone, Debug)]
pub struct PyPoint {
    inner: RustPoint,
}

#[pymethods]
impl PyPoint {
    /// Create a new Point with latitude and longitude.
    ///
    /// # Args
    ///     lon: Longitude in degrees (-180 to 180) - x-coordinate
    ///     lat: Latitude in degrees (-90 to 90) - y-coordinate
    ///     alt: Optional altitude (ignored - see struct documentation)
    ///
    /// # Note
    ///     Uses (longitude, latitude) order to match GeoJSON standard and the Rust API.
    ///     This is the mathematical (x, y) convention used by most GIS libraries.
    ///
    /// # Returns
    ///     A new Point instance
    ///
    /// # Raises
    ///     ValueError: If latitude or longitude are out of valid range
    #[new]
    #[pyo3(signature = (lon, lat, alt=None))]
    fn new(lon: f64, lat: f64, alt: Option<f64>) -> PyResult<Self> {
        if !(-90.0..=90.0).contains(&lat) {
            return Err(PyValueError::new_err("Latitude must be between -90 and 90"));
        }
        if !(-180.0..=180.0).contains(&lon) {
            return Err(PyValueError::new_err(
                "Longitude must be between -180 and 180",
            ));
        }

        let point = RustPoint::new(lon, lat);
        // Altitude parameter is silently ignored (see struct documentation)
        let _ = alt;

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
    /// Always returns None (altitude not supported).
    #[getter]
    fn alt(&self) -> Option<f64> {
        None
    }

    fn __repr__(&self) -> String {
        if let Some(alt) = self.alt() {
            format!("Point(lon={}, lat={}, alt={})", self.lon(), self.lat(), alt)
        } else {
            format!("Point(lon={}, lat={})", self.lon(), self.lat())
        }
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }

    /// Calculate distance to another point in meters using Haversine formula
    fn distance_to(&self, other: &PyPoint) -> f64 {
        self.inner.haversine_distance(&other.inner)
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

    /// Update an object's location
    #[pyo3(signature = (namespace, object_id, point, metadata=None))]
    fn update_location(
        &self,
        namespace: &str,
        object_id: &str,
        point: &PyPoint,
        metadata: Option<&Bound<'_, PyBytes>>,
    ) -> PyResult<()> {
        let meta_bytes = metadata.map(|b| b.as_bytes()).unwrap_or(&[]);
        let pos = spatio::Point3d::new(point.lon(), point.lat(), point.alt().unwrap_or(0.0));

        handle_error(
            self.db
                .update_location(namespace, object_id, pos, meta_bytes),
        )
    }

    /// Query current locations within radius
    #[pyo3(signature = (namespace, center, radius, limit=100))]
    fn query_current_within_radius(
        &self,
        namespace: &str,
        center: &PyPoint,
        radius: f64,
        limit: usize,
    ) -> PyResult<Py<PyList>> {
        let center_pos =
            spatio::Point3d::new(center.lon(), center.lat(), center.alt().unwrap_or(0.0));
        let results = handle_error(self.db.query_current_within_radius(
            namespace,
            &center_pos,
            radius,
            limit,
        ))?;

        Python::attach(|py| {
            let py_list = PyList::empty(py);
            for loc in results {
                let py_point = PyPoint {
                    inner: RustPoint::new(loc.position.x, loc.position.y), // TODO: Support 3D in PyPoint
                };
                let py_meta = PyBytes::new(py, &loc.metadata);
                // (object_id, point, metadata)
                let tuple = (loc.object_id, py_point, py_meta).into_pyobject(py)?;
                py_list.append(tuple)?;
            }
            Ok(py_list.unbind())
        })
    }

    /// Query objects near another object
    #[pyo3(signature = (namespace, object_id, radius, limit=100))]
    fn query_near_object(
        &self,
        namespace: &str,
        object_id: &str,
        radius: f64,
        limit: usize,
    ) -> PyResult<Py<PyList>> {
        let results = handle_error(
            self.db
                .query_near_object(namespace, object_id, radius, limit),
        )?;

        Python::attach(|py| {
            let py_list = PyList::empty(py);
            for loc in results {
                let py_point = PyPoint {
                    inner: RustPoint::new(loc.position.x, loc.position.y),
                };
                let py_meta = PyBytes::new(py, &loc.metadata);
                let tuple = (loc.object_id, py_point, py_meta).into_pyobject(py)?;
                py_list.append(tuple)?;
            }
            Ok(py_list.unbind())
        })
    }

    /// Query trajectory
    #[pyo3(signature = (namespace, object_id, start_time, end_time, limit=100))]
    fn query_trajectory(
        &self,
        namespace: &str,
        object_id: &str,
        start_time: f64,
        end_time: f64,
        limit: usize,
    ) -> PyResult<Py<PyList>> {
        let start = UNIX_EPOCH + Duration::from_secs_f64(start_time);
        let end = UNIX_EPOCH + Duration::from_secs_f64(end_time);

        let results = handle_error(
            self.db
                .query_trajectory(namespace, object_id, start, end, limit),
        )?;

        Python::attach(|py| {
            let py_list = PyList::empty(py);
            for update in results {
                let py_point = PyPoint {
                    inner: RustPoint::new(update.position.x, update.position.y),
                };
                let py_meta = PyBytes::new(py, &update.metadata);
                let ts = update
                    .timestamp
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs_f64();

                // (point, metadata, timestamp)
                let tuple = (py_point, py_meta, ts).into_pyobject(py)?;
                py_list.append(tuple)?;
            }
            Ok(py_list.unbind())
        })
    }

    /// Get database statistics
    fn stats(&self) -> PyResult<Py<PyAny>> {
        let stats = self.db.stats();

        Python::attach(|py| {
            let dict = pyo3::types::PyDict::new(py);
            // TODO: Update stats fields when DbStats is updated
            dict.set_item("key_count", stats.key_count)?;
            dict.set_item("expired_count", stats.expired_count)?;
            dict.set_item("operations_count", stats.operations_count)?;
            Ok(dict.into_any().unbind())
        })
    }

    /// Close the database
    fn close(&self) -> PyResult<()> {
        handle_error(self.db.close())
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
