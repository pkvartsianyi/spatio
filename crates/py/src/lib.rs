//! Python bindings for Spatio
//!
//! This module provides Python bindings for the Spatio spatio-temporal database using PyO3.
//! It exposes the core functionality including database operations, spatio-temporal queries,
//! and trajectory tracking.

// All geo types are now accessed through spatio wrappers
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList};
use spatio::DistanceMetric as RustDistanceMetric;
use spatio::{Point3d, Spatio};
use spatio::{config::Config as RustConfig, error::Result as RustResult};
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

/// Convert Rust Result to Python Result
fn handle_error<T>(result: RustResult<T>) -> PyResult<T> {
    result.map_err(|e| PyRuntimeError::new_err(e.to_string()))
}

/// Python wrapper for geographic Point (3D)
#[pyclass(name = "Point")]
#[derive(Clone, Debug)]
pub struct PyPoint {
    inner: Point3d,
}

#[pymethods]
impl PyPoint {
    /// Create a new Point with x, y, and optional z coordinates.
    ///
    /// # Args
    ///     x: X coordinate (Longitude)
    ///     y: Y coordinate (Latitude)
    ///     z: Z coordinate (Altitude), defaults to 0.0
    #[new]
    #[pyo3(signature = (x, y, z=None))]
    fn new(x: f64, y: f64, z: Option<f64>) -> PyResult<Self> {
        Ok(PyPoint {
            inner: Point3d::new(x, y, z.unwrap_or(0.0)),
        })
    }

    #[getter]
    fn x(&self) -> f64 {
        self.inner.x()
    }

    #[getter]
    fn y(&self) -> f64 {
        self.inner.y()
    }

    #[getter]
    fn z(&self) -> f64 {
        self.inner.z()
    }

    /// Alias for x (Longitude)
    #[getter]
    fn lon(&self) -> f64 {
        self.inner.x()
    }

    /// Alias for y (Latitude)
    #[getter]
    fn lat(&self) -> f64 {
        self.inner.y()
    }

    /// Alias for z (Altitude)
    #[getter]
    fn alt(&self) -> f64 {
        self.inner.z()
    }

    fn __repr__(&self) -> String {
        format!(
            "Point(x={:.4}, y={:.4}, z={:.4})",
            self.inner.x(),
            self.inner.y(),
            self.inner.z()
        )
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }

    /// Calculate distance to another point in meters using Haversine formula
    fn distance_to(&self, other: &PyPoint) -> f64 {
        let r = 6371000.0; // Earth radius in meters
        let d_lat = (other.lat() - self.lat()).to_radians();
        let d_lon = (other.lon() - self.lon()).to_radians();
        let lat1 = self.lat().to_radians();
        let lat2 = other.lat().to_radians();

        let a = (d_lat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (d_lon / 2.0).sin().powi(2);
        let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());

        r * c
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
    db: Arc<Spatio>,
}

#[pymethods]
impl PySpatio {
    /// Create an in-memory Spatio database
    #[staticmethod]
    fn memory() -> PyResult<Self> {
        let db = Spatio::builder()
            .build()
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(PySpatio { db: Arc::new(db) })
    }

    /// Create an in-memory database with custom configuration
    #[staticmethod]
    fn memory_with_config(config: &PyConfig) -> PyResult<Self> {
        let db = Spatio::builder()
            .config(config.inner.clone())
            .build()
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(PySpatio { db: Arc::new(db) })
    }

    /// Open a persistent Spatio database from file
    #[staticmethod]
    fn open(path: &str) -> PyResult<Self> {
        let db = Spatio::builder()
            .path(path)
            .build()
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(PySpatio { db: Arc::new(db) })
    }

    /// Open a persistent database with custom configuration
    #[staticmethod]
    fn open_with_config(path: &str, config: &PyConfig) -> PyResult<Self> {
        let db = Spatio::builder()
            .path(path)
            .config(config.inner.clone())
            .build()
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(PySpatio { db: Arc::new(db) })
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
        let pos = point.inner.clone();

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
        let center_pos = center.inner.clone();
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
                    inner: loc.position,
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
                    inner: loc.position,
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
                    inner: update.position,
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
    m.add_class::<PyConfig>()?;
    m.add_class::<PyDistanceMetric>()?;

    // Add version
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;

    Ok(())
}
