//! Python bindings for Spatio
//!
//! This module provides Python bindings for the Spatio spatio-temporal database using PyO3.
//! It exposes the core functionality including database operations, spatio-temporal queries,
//! and trajectory tracking.

// All geo types are now accessed through spatio wrappers
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyList;
use spatio::{DistanceMetric as RustDistanceMetric, Point3d, Spatio};
use spatio::{config::Config as RustConfig, error::Result as RustResult};
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use futures::{SinkExt, StreamExt};
use once_cell::sync::Lazy;
use spatio_server::SBPClientCodec;
use spatio_server::protocol::{Command, ResponsePayload, ResponseStatus};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

static RUNTIME: Lazy<tokio::runtime::Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create Tokio runtime")
});

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

/// Python wrapper for TemporalPoint
#[pyclass(name = "TemporalPoint")]
#[derive(Clone, Debug)]
pub struct PyTemporalPoint {
    #[pyo3(get, set)]
    pub point: PyPoint,
    #[pyo3(get, set)]
    pub timestamp: f64,
}

#[pymethods]
impl PyTemporalPoint {
    #[new]
    fn new(point: PyPoint, timestamp: f64) -> Self {
        PyTemporalPoint { point, timestamp }
    }

    fn __repr__(&self) -> String {
        format!(
            "TemporalPoint(point={:?}, timestamp={})",
            self.point, self.timestamp
        )
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

    /// Connect to a remote Spatio server
    #[staticmethod]
    #[pyo3(signature = (host="127.0.0.1", port=3000))]
    fn server(host: &str, port: u16) -> PyResult<PySpatioClient> {
        PySpatioClient::new(host, port)
    }

    /// Upsert an object's location
    #[pyo3(signature = (namespace, object_id, point, metadata=None))]
    fn upsert(
        &self,
        namespace: &str,
        object_id: &str,
        point: &PyPoint,
        metadata: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        let pos = point.inner.clone();

        let metadata_value = if let Some(meta) = metadata {
            pythonize::depythonize(meta).map_err(|e| PyValueError::new_err(e.to_string()))?
        } else {
            serde_json::Value::Null
        };

        handle_error(
            self.db
                .upsert(namespace, object_id, pos, metadata_value, None),
        )
    }

    /// Alias for upsert for backward compatibility
    #[pyo3(signature = (namespace, object_id, point, metadata=None))]
    fn update_location(
        &self,
        namespace: &str,
        object_id: &str,
        point: &PyPoint,
        metadata: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        self.upsert(namespace, object_id, point, metadata)
    }

    /// Insert a trajectory (sequence of points)
    #[pyo3(signature = (namespace, object_id, trajectory))]
    fn insert_trajectory(
        &self,
        namespace: &str,
        object_id: &str,
        trajectory: Vec<PyTemporalPoint>,
    ) -> PyResult<()> {
        let mut core_trajectory = Vec::with_capacity(trajectory.len());
        for tp in trajectory {
            if !tp.timestamp.is_finite() || tp.timestamp < 0.0 {
                return Err(PyValueError::new_err(
                    "Timestamp must be a finite, non-negative value",
                ));
            }
            core_trajectory.push(spatio::TemporalPoint {
                point: spatio::Point::new(tp.point.inner.x(), tp.point.inner.y()),
                timestamp: UNIX_EPOCH + Duration::from_secs_f64(tp.timestamp),
            });
        }

        handle_error(
            self.db
                .insert_trajectory(namespace, object_id, &core_trajectory),
        )
    }

    /// Query current locations within radius
    #[pyo3(signature = (namespace, center, radius, limit=100))]
    fn query_radius(
        &self,
        namespace: &str,
        center: &PyPoint,
        radius: f64,
        limit: usize,
    ) -> PyResult<Py<PyList>> {
        let center_pos = center.inner.clone();
        let results = handle_error(self.db.query_radius(namespace, &center_pos, radius, limit))?;

        Python::attach(|py| {
            let py_list = PyList::empty(py);
            for (loc, dist) in results {
                let py_point = PyPoint {
                    inner: loc.position,
                };
                let py_meta = pythonize::pythonize(py, &loc.metadata)?;
                // (object_id, point, metadata, distance)
                let tuple = (loc.object_id, py_point, py_meta, dist).into_pyobject(py)?;
                py_list.append(tuple)?;
            }
            Ok(py_list.unbind())
        })
    }

    /// Query objects near another object
    #[pyo3(signature = (namespace, object_id, radius, limit=100))]
    fn query_near(
        &self,
        namespace: &str,
        object_id: &str,
        radius: f64,
        limit: usize,
    ) -> PyResult<Py<PyList>> {
        let results = handle_error(self.db.query_near(namespace, object_id, radius, limit))?;

        Python::attach(|py| {
            let py_list = PyList::empty(py);
            for (loc, dist) in results {
                let py_point = PyPoint {
                    inner: loc.position,
                };
                let py_meta = pythonize::pythonize(py, &loc.metadata)?;
                // (object_id, point, metadata, distance)
                let tuple = (loc.object_id, py_point, py_meta, dist).into_pyobject(py)?;
                py_list.append(tuple)?;
            }
            Ok(py_list.unbind())
        })
    }

    /// Find k nearest neighbors in 3D
    #[pyo3(signature = (namespace, center, k))]
    fn knn(&self, namespace: &str, center: &PyPoint, k: usize) -> PyResult<Py<PyList>> {
        let center_pos = center.inner.clone();
        let results = handle_error(self.db.knn(namespace, &center_pos, k))?;

        Python::attach(|py| {
            let py_list = PyList::empty(py);
            for (loc, dist) in results {
                let py_point = PyPoint {
                    inner: loc.position,
                };
                let py_meta = pythonize::pythonize(py, &loc.metadata)?;
                // (object_id, point, metadata, distance)
                let tuple = (loc.object_id, py_point, py_meta, dist).into_pyobject(py)?;
                py_list.append(tuple)?;
            }
            Ok(py_list.unbind())
        })
    }

    /// Find k nearest neighbors near an object
    #[pyo3(signature = (namespace, object_id, k))]
    fn knn_near_object(&self, namespace: &str, object_id: &str, k: usize) -> PyResult<Py<PyList>> {
        let results = handle_error(self.db.knn_near_object(namespace, object_id, k))?;

        Python::attach(|py| {
            let py_list = PyList::empty(py);
            for (loc, dist) in results {
                let py_point = PyPoint {
                    inner: loc.position,
                };
                let py_meta = pythonize::pythonize(py, &loc.metadata)?;
                // (object_id, point, metadata, distance)
                let tuple = (loc.object_id, py_point, py_meta, dist).into_pyobject(py)?;
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
                let py_meta = pythonize::pythonize(py, &update.metadata)?;
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

    /// Query objects within a 2D bounding box
    #[pyo3(signature = (namespace, min_x, min_y, max_x, max_y, limit=100))]
    fn query_bbox(
        &self,
        namespace: &str,
        min_x: f64,
        min_y: f64,
        max_x: f64,
        max_y: f64,
        limit: usize,
    ) -> PyResult<Py<PyList>> {
        let results = handle_error(
            self.db
                .query_bbox(namespace, min_x, min_y, max_x, max_y, limit),
        )?;

        Python::attach(|py| {
            let py_list = PyList::empty(py);
            for loc in results {
                let py_point = PyPoint {
                    inner: loc.position,
                };
                let py_meta = pythonize::pythonize(py, &loc.metadata)?;
                // (object_id, point, metadata) - no distance for bbox
                let tuple = (loc.object_id, py_point, py_meta).into_pyobject(py)?;
                py_list.append(tuple)?;
            }
            Ok(py_list.unbind())
        })
    }

    /// Query objects within a cylindrical volume
    #[pyo3(signature = (namespace, center, min_z, max_z, radius, limit=100))]
    fn query_within_cylinder(
        &self,
        namespace: &str,
        center: &PyPoint,
        min_z: f64,
        max_z: f64,
        radius: f64,
        limit: usize,
    ) -> PyResult<Py<PyList>> {
        let center_geo = spatio::Point::new(center.inner.x(), center.inner.y());
        let results = handle_error(
            self.db
                .query_within_cylinder(namespace, center_geo, min_z, max_z, radius, limit),
        )?;

        Python::attach(|py| {
            let py_list = PyList::empty(py);
            for (loc, dist) in results {
                let py_point = PyPoint {
                    inner: loc.position,
                };
                let py_meta = pythonize::pythonize(py, &loc.metadata)?;
                // (object_id, point, metadata, distance)
                let tuple = (loc.object_id, py_point, py_meta, dist).into_pyobject(py)?;
                py_list.append(tuple)?;
            }
            Ok(py_list.unbind())
        })
    }

    /// Query objects within a 3D bounding box
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (namespace, min_x, min_y, min_z, max_x, max_y, max_z, limit=100))]
    fn query_within_bbox_3d(
        &self,
        namespace: &str,
        min_x: f64,
        min_y: f64,
        min_z: f64,
        max_x: f64,
        max_y: f64,
        max_z: f64,
        limit: usize,
    ) -> PyResult<Py<PyList>> {
        let results = handle_error(
            self.db
                .query_within_bbox_3d(namespace, min_x, min_y, min_z, max_x, max_y, max_z, limit),
        )?;

        Python::attach(|py| {
            let py_list = PyList::empty(py);
            for loc in results {
                let py_point = PyPoint {
                    inner: loc.position,
                };
                let py_meta = pythonize::pythonize(py, &loc.metadata)?;
                // (object_id, point, metadata)
                let tuple = (loc.object_id, py_point, py_meta).into_pyobject(py)?;
                py_list.append(tuple)?;
            }
            Ok(py_list.unbind())
        })
    }

    /// Query objects within a bounding box relative to another object
    #[pyo3(signature = (namespace, object_id, width, height, limit=100))]
    fn query_bbox_near_object(
        &self,
        namespace: &str,
        object_id: &str,
        width: f64,
        height: f64,
        limit: usize,
    ) -> PyResult<Py<PyList>> {
        let results = handle_error(
            self.db
                .query_bbox_near_object(namespace, object_id, width, height, limit),
        )?;

        Python::attach(|py| {
            let py_list = PyList::empty(py);
            for loc in results {
                let py_point = PyPoint {
                    inner: loc.position,
                };
                let py_meta = pythonize::pythonize(py, &loc.metadata)?;
                // (object_id, point, metadata)
                let tuple = (loc.object_id, py_point, py_meta).into_pyobject(py)?;
                py_list.append(tuple)?;
            }
            Ok(py_list.unbind())
        })
    }

    /// Query objects within a cylindrical volume relative to another object
    #[pyo3(signature = (namespace, object_id, min_z, max_z, radius, limit=100))]
    fn query_cylinder_near_object(
        &self,
        namespace: &str,
        object_id: &str,
        min_z: f64,
        max_z: f64,
        radius: f64,
        limit: usize,
    ) -> PyResult<Py<PyList>> {
        let results = handle_error(
            self.db
                .query_cylinder_near_object(namespace, object_id, min_z, max_z, radius, limit),
        )?;

        Python::attach(|py| {
            let py_list = PyList::empty(py);
            for (loc, dist) in results {
                let py_point = PyPoint {
                    inner: loc.position,
                };
                let py_meta = pythonize::pythonize(py, &loc.metadata)?;
                // (object_id, point, metadata, distance)
                let tuple = (loc.object_id, py_point, py_meta, dist).into_pyobject(py)?;
                py_list.append(tuple)?;
            }
            Ok(py_list.unbind())
        })
    }

    /// Query objects within a 3D bounding box relative to another object
    #[pyo3(signature = (namespace, object_id, width, height, depth, limit=100))]
    fn query_bbox_3d_near_object(
        &self,
        namespace: &str,
        object_id: &str,
        width: f64,
        height: f64,
        depth: f64,
        limit: usize,
    ) -> PyResult<Py<PyList>> {
        let results = handle_error(
            self.db
                .query_bbox_3d_near_object(namespace, object_id, width, height, depth, limit),
        )?;

        Python::attach(|py| {
            let py_list = PyList::empty(py);
            for loc in results {
                let py_point = PyPoint {
                    inner: loc.position,
                };
                let py_meta = pythonize::pythonize(py, &loc.metadata)?;
                // (object_id, point, metadata)
                let tuple = (loc.object_id, py_point, py_meta).into_pyobject(py)?;
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
            dict.set_item("expired_count", stats.expired_count)?;
            dict.set_item("operations_count", stats.operations_count)?;
            dict.set_item("size_bytes", stats.size_bytes)?;
            dict.set_item("hot_state_objects", stats.hot_state_objects)?;
            dict.set_item("cold_state_trajectories", stats.cold_state_trajectories)?;
            dict.set_item("cold_state_buffer_bytes", stats.cold_state_buffer_bytes)?;
            dict.set_item("memory_usage_bytes", stats.memory_usage_bytes)?;
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

/// RPC Client for Spatio server
#[pyclass(name = "SpatioClient")]
#[derive(Clone)]
pub struct PySpatioClient {
    host: String,
    port: u16,
    inner: Arc<tokio::sync::Mutex<Option<Framed<TcpStream, SBPClientCodec>>>>,
}

impl PySpatioClient {
    /// Internal helper to send a command and receive a response
    fn call(&self, cmd: Command) -> PyResult<(ResponseStatus, ResponsePayload)> {
        RUNTIME.block_on(async {
            let mut inner_opt = self.inner.lock().await;

            // Ensure connected
            if inner_opt.is_none() {
                let addr = format!("{}:{}", self.host, self.port);
                let stream = tokio::net::TcpStream::connect(&addr).await.map_err(|e| {
                    PyRuntimeError::new_err(format!("Failed to reconnect to {}: {}", addr, e))
                })?;
                *inner_opt = Some(Framed::new(stream, SBPClientCodec));
            }

            let inner = inner_opt.as_mut().unwrap();

            let fut = async {
                inner
                    .send(cmd)
                    .await
                    .map_err(|e| PyRuntimeError::new_err(format!("Send error: {}", e)))?;

                let resp = inner.next().await.ok_or_else(|| {
                    PyRuntimeError::new_err("Connection closed by server".to_string())
                })?;

                resp.map_err(|e| PyRuntimeError::new_err(format!("Receive error: {}", e)))
            };

            match tokio::time::timeout(tokio::time::Duration::from_secs(10), fut).await {
                Ok(res) => {
                    if res.is_err() {
                        // On error, clear the connection so next call retries
                        *inner_opt = None;
                    }
                    res
                }
                Err(_) => {
                    *inner_opt = None;
                    Err(PyRuntimeError::new_err("RPC request timed out after 10s"))
                }
            }
        })
    }
}

#[pymethods]
impl PySpatioClient {
    #[new]
    #[pyo3(signature = (host="127.0.0.1", port=3000))]
    fn new(host: &str, port: u16) -> PyResult<Self> {
        Ok(PySpatioClient {
            host: host.to_string(),
            port,
            inner: Arc::new(tokio::sync::Mutex::new(None)),
        })
    }

    #[pyo3(signature = (namespace, object_id, point, metadata=None))]
    fn upsert(
        &self,
        namespace: String,
        object_id: String,
        point: &PyPoint,
        metadata: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        let meta_vec = if let Some(meta) = metadata {
            let val: serde_json::Value =
                pythonize::depythonize(meta).map_err(|e| PyValueError::new_err(e.to_string()))?;
            serde_json::to_vec(&val).unwrap_or_default()
        } else {
            vec![]
        };

        let cmd = Command::Upsert {
            namespace,
            id: object_id,
            point: point.inner.clone(),
            metadata: meta_vec,
            opts: None,
        };

        let (status, payload) = self.call(cmd)?;
        match status {
            ResponseStatus::Ok => Ok(()),
            ResponseStatus::Error => {
                if let ResponsePayload::Error(e) = payload {
                    Err(PyRuntimeError::new_err(e))
                } else {
                    Err(PyRuntimeError::new_err("Unknown server error"))
                }
            }
        }
    }

    fn get(&self, namespace: String, id: String) -> PyResult<Option<(String, PyPoint, Py<PyAny>)>> {
        let cmd = Command::Get { namespace, id };
        let (status, payload) = self.call(cmd)?;

        if matches!(status, ResponseStatus::Error)
            && let ResponsePayload::Error(e) = payload
        {
            if e == "Not found" {
                return Ok(None);
            }
            return Err(PyRuntimeError::new_err(e));
        }

        if let ResponsePayload::Object {
            id,
            point,
            metadata,
        } = payload
        {
            Python::attach(|py| {
                let py_point = PyPoint { inner: point };
                let meta_json: serde_json::Value =
                    serde_json::from_slice(&metadata).unwrap_or_default();
                let py_meta = pythonize::pythonize(py, &meta_json)?;
                Ok(Some((id, py_point, py_meta.unbind())))
            })
        } else {
            Err(PyRuntimeError::new_err("Unexpected response payload"))
        }
    }

    fn delete(&self, namespace: String, id: String) -> PyResult<()> {
        let cmd = Command::Delete { namespace, id };
        let (status, payload) = self.call(cmd)?;
        match status {
            ResponseStatus::Ok => Ok(()),
            ResponseStatus::Error => {
                if let ResponsePayload::Error(e) = payload {
                    Err(PyRuntimeError::new_err(e))
                } else {
                    Err(PyRuntimeError::new_err("Unknown server error"))
                }
            }
        }
    }

    #[pyo3(signature = (namespace, center, radius, limit=100))]
    fn query_radius(
        &self,
        namespace: String,
        center: &PyPoint,
        radius: f64,
        limit: usize,
    ) -> PyResult<Py<PyList>> {
        let cmd = Command::QueryRadius {
            namespace,
            center: center.inner.clone(),
            radius,
            limit,
        };

        let (status, payload) = self.call(cmd)?;
        if matches!(status, ResponseStatus::Error)
            && let ResponsePayload::Error(e) = payload
        {
            return Err(PyRuntimeError::new_err(e));
        }

        if let ResponsePayload::Objects(results) = payload {
            Python::attach(|py| {
                let py_list = PyList::empty(py);
                for (id, pos, meta_bytes, dist) in results {
                    let py_point = PyPoint { inner: pos };
                    let meta_json: serde_json::Value =
                        serde_json::from_slice(&meta_bytes).unwrap_or_default();
                    let py_meta = pythonize::pythonize(py, &meta_json)?;
                    let tuple = pyo3::types::PyTuple::new(
                        py,
                        [
                            id.into_pyobject(py)?.into_any(),
                            py_point.into_pyobject(py)?.into_any(),
                            py_meta.into_any(),
                            dist.into_pyobject(py)?.into_any(),
                        ],
                    )?;
                    py_list.append(tuple)?;
                }
                Ok(py_list.unbind())
            })
        } else {
            Err(PyRuntimeError::new_err("Unexpected response payload"))
        }
    }

    fn stats(&self) -> PyResult<Py<PyAny>> {
        let (status, payload) = self.call(Command::Stats)?;
        if matches!(status, ResponseStatus::Error)
            && let ResponsePayload::Error(e) = payload
        {
            return Err(PyRuntimeError::new_err(e));
        }

        if let ResponsePayload::Stats(stats) = payload {
            Python::attach(|py| {
                let dict = pyo3::types::PyDict::new(py);
                dict.set_item("expired_count", stats.expired_count)?;
                dict.set_item("operations_count", stats.operations_count)?;
                dict.set_item("size_bytes", stats.size_bytes)?;
                dict.set_item("hot_state_objects", stats.hot_state_objects)?;
                dict.set_item("cold_state_trajectories", stats.cold_state_trajectories)?;
                dict.set_item("cold_state_buffer_bytes", stats.cold_state_buffer_bytes)?;
                dict.set_item("memory_usage_bytes", stats.memory_usage_bytes)?;
                Ok(dict.into_any().unbind())
            })
        } else {
            Err(PyRuntimeError::new_err("Unexpected response payload"))
        }
    }

    fn close(&self) -> PyResult<()> {
        let (status, payload) = self.call(Command::Close)?;
        match status {
            ResponseStatus::Ok => Ok(()),
            ResponseStatus::Error => {
                if let ResponsePayload::Error(e) = payload {
                    Err(PyRuntimeError::new_err(e))
                } else {
                    Err(PyRuntimeError::new_err("Unknown server error"))
                }
            }
        }
    }

    #[pyo3(signature = (namespace, min_x, min_y, max_x, max_y, limit=100))]
    fn query_bbox(
        &self,
        namespace: String,
        min_x: f64,
        min_y: f64,
        max_x: f64,
        max_y: f64,
        limit: usize,
    ) -> PyResult<Py<PyList>> {
        let cmd = Command::QueryBbox {
            namespace,
            min_x,
            min_y,
            max_x,
            max_y,
            limit,
        };
        let (status, payload) = self.call(cmd)?;
        if matches!(status, ResponseStatus::Error)
            && let ResponsePayload::Error(e) = payload
        {
            return Err(PyRuntimeError::new_err(e));
        }

        if let ResponsePayload::ObjectList(results) = payload {
            Python::attach(|py| {
                let py_list = PyList::empty(py);
                for (id, pos, meta_bytes) in results {
                    let py_point = PyPoint { inner: pos };
                    let meta_json: serde_json::Value =
                        serde_json::from_slice(&meta_bytes).unwrap_or_default();
                    let py_meta = pythonize::pythonize(py, &meta_json)?;
                    let tuple = pyo3::types::PyTuple::new(
                        py,
                        [
                            id.into_pyobject(py)?.into_any(),
                            py_point.into_pyobject(py)?.into_any(),
                            py_meta.into_any(),
                        ],
                    )?;
                    py_list.append(tuple)?;
                }
                Ok(py_list.unbind())
            })
        } else {
            Err(PyRuntimeError::new_err("Unexpected response payload"))
        }
    }

    #[pyo3(signature = (namespace, center, min_z, max_z, radius, limit=100))]
    fn query_within_cylinder(
        &self,
        namespace: String,
        center: &PyPoint,
        min_z: f64,
        max_z: f64,
        radius: f64,
        limit: usize,
    ) -> PyResult<Py<PyList>> {
        let cmd = Command::QueryCylinder {
            namespace,
            center_x: center.inner.x(),
            center_y: center.inner.y(),
            min_z,
            max_z,
            radius,
            limit,
        };
        let (status, payload) = self.call(cmd)?;
        if matches!(status, ResponseStatus::Error)
            && let ResponsePayload::Error(e) = payload
        {
            return Err(PyRuntimeError::new_err(e));
        }

        if let ResponsePayload::Objects(results) = payload {
            Python::attach(|py| {
                let py_list = PyList::empty(py);
                for (id, pos, meta_bytes, dist) in results {
                    let py_point = PyPoint { inner: pos };
                    let meta_json: serde_json::Value =
                        serde_json::from_slice(&meta_bytes).unwrap_or_default();
                    let py_meta = pythonize::pythonize(py, &meta_json)?;
                    let tuple = pyo3::types::PyTuple::new(
                        py,
                        [
                            id.into_pyobject(py)?.into_any(),
                            py_point.into_pyobject(py)?.into_any(),
                            py_meta.into_any(),
                            dist.into_pyobject(py)?.into_any(),
                        ],
                    )?;
                    py_list.append(tuple)?;
                }
                Ok(py_list.unbind())
            })
        } else {
            Err(PyRuntimeError::new_err("Unexpected response payload"))
        }
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (namespace, min_x, min_y, min_z, max_x, max_y, max_z, limit=100))]
    fn query_within_bbox_3d(
        &self,
        namespace: String,
        min_x: f64,
        min_y: f64,
        min_z: f64,
        max_x: f64,
        max_y: f64,
        max_z: f64,
        limit: usize,
    ) -> PyResult<Py<PyList>> {
        let cmd = Command::QueryBbox3d {
            namespace,
            min_x,
            min_y,
            min_z,
            max_x,
            max_y,
            max_z,
            limit,
        };
        let (status, payload) = self.call(cmd)?;
        if matches!(status, ResponseStatus::Error)
            && let ResponsePayload::Error(e) = payload
        {
            return Err(PyRuntimeError::new_err(e));
        }

        if let ResponsePayload::ObjectList(results) = payload {
            Python::attach(|py| {
                let py_list = PyList::empty(py);
                for (id, pos, meta_bytes) in results {
                    let py_point = PyPoint { inner: pos };
                    let meta_json: serde_json::Value =
                        serde_json::from_slice(&meta_bytes).unwrap_or_default();
                    let py_meta = pythonize::pythonize(py, &meta_json)?;
                    let tuple = pyo3::types::PyTuple::new(
                        py,
                        [
                            id.into_pyobject(py)?.into_any(),
                            py_point.into_pyobject(py)?.into_any(),
                            py_meta.into_any(),
                        ],
                    )?;
                    py_list.append(tuple)?;
                }
                Ok(py_list.unbind())
            })
        } else {
            Err(PyRuntimeError::new_err("Unexpected response payload"))
        }
    }

    #[pyo3(signature = (namespace, object_id, trajectory))]
    fn insert_trajectory(
        &self,
        namespace: String,
        object_id: String,
        trajectory: Vec<PyTemporalPoint>,
    ) -> PyResult<()> {
        let mut core_trajectory = Vec::with_capacity(trajectory.len());
        for tp in trajectory {
            core_trajectory.push(spatio_types::point::TemporalPoint {
                point: spatio::Point::new(tp.point.inner.x(), tp.point.inner.y()),
                timestamp: UNIX_EPOCH + Duration::from_secs_f64(tp.timestamp),
            });
        }

        let cmd = Command::InsertTrajectory {
            namespace,
            id: object_id,
            trajectory: core_trajectory,
        };

        let (status, payload) = self.call(cmd)?;
        match status {
            ResponseStatus::Ok => Ok(()),
            ResponseStatus::Error => {
                if let ResponsePayload::Error(e) = payload {
                    Err(PyRuntimeError::new_err(e))
                } else {
                    Err(PyRuntimeError::new_err("Unknown server error"))
                }
            }
        }
    }

    #[pyo3(signature = (namespace, id, start_time, end_time, limit=100))]
    fn query_trajectory(
        &self,
        namespace: String,
        id: String,
        start_time: f64,
        end_time: f64,
        limit: usize,
    ) -> PyResult<Py<PyList>> {
        let cmd = Command::QueryTrajectory {
            namespace,
            id,
            start_time: UNIX_EPOCH + Duration::from_secs_f64(start_time),
            end_time: UNIX_EPOCH + Duration::from_secs_f64(end_time),
            limit,
        };

        let (status, payload) = self.call(cmd)?;
        if matches!(status, ResponseStatus::Error)
            && let ResponsePayload::Error(e) = payload
        {
            return Err(PyRuntimeError::new_err(e));
        }

        if let ResponsePayload::Trajectory(updates) = payload {
            Python::attach(|py| {
                let py_list = PyList::empty(py);
                for update in updates {
                    let py_point = PyPoint {
                        inner: update.position,
                    };
                    let meta_json: serde_json::Value =
                        serde_json::from_slice(&update.metadata).unwrap_or_default();
                    let py_meta = pythonize::pythonize(py, &meta_json)?;
                    let ts = update
                        .timestamp
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs_f64();
                    let tuple = pyo3::types::PyTuple::new(
                        py,
                        [
                            py_point.into_pyobject(py)?.into_any(),
                            py_meta.into_any(),
                            ts.into_pyobject(py)?.into_any(),
                        ],
                    )?;
                    py_list.append(tuple)?;
                }
                Ok(py_list.unbind())
            })
        } else {
            Err(PyRuntimeError::new_err("Unexpected response payload"))
        }
    }

    #[pyo3(signature = (namespace, object_id, radius, limit=100))]
    fn query_near(
        &self,
        namespace: String,
        object_id: String,
        radius: f64,
        limit: usize,
    ) -> PyResult<Py<PyList>> {
        let cmd = Command::QueryNear {
            namespace,
            id: object_id,
            radius,
            limit,
        };

        let (status, payload) = self.call(cmd)?;
        if matches!(status, ResponseStatus::Error)
            && let ResponsePayload::Error(e) = payload
        {
            return Err(PyRuntimeError::new_err(e));
        }

        if let ResponsePayload::Objects(results) = payload {
            Python::attach(|py| {
                let py_list = PyList::empty(py);
                for (id, pos, meta_bytes, dist) in results {
                    let py_point = PyPoint { inner: pos };
                    let meta_json: serde_json::Value =
                        serde_json::from_slice(&meta_bytes).unwrap_or_default();
                    let py_meta = pythonize::pythonize(py, &meta_json)?;
                    let tuple = pyo3::types::PyTuple::new(
                        py,
                        [
                            id.into_pyobject(py)?.into_any(),
                            py_point.into_pyobject(py)?.into_any(),
                            py_meta.into_any(),
                            dist.into_pyobject(py)?.into_any(),
                        ],
                    )?;
                    py_list.append(tuple)?;
                }
                Ok(py_list.unbind())
            })
        } else {
            Err(PyRuntimeError::new_err("Unexpected response payload"))
        }
    }
}

/// Python wrapper for SetOptions
#[pyclass(name = "SetOptions")]
#[derive(Clone, Debug)]
pub struct PySetOptions {
    #[allow(dead_code)]
    inner: spatio::config::SetOptions,
}

#[pymethods]
impl PySetOptions {
    #[new]
    fn new() -> Self {
        PySetOptions {
            inner: spatio::config::SetOptions::default(),
        }
    }
}

/// Python module definition
#[pymodule]
fn _spatio(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PySpatio>()?;
    m.add_class::<PySpatioClient>()?;
    m.add_class::<PyPoint>()?;
    m.add_class::<PyConfig>()?;
    m.add_class::<PyDistanceMetric>()?;
    m.add_class::<PyTemporalPoint>()?;
    m.add_class::<PySetOptions>()?;

    // Add version
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;

    Ok(())
}
