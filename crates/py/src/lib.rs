//! Python bindings for Spatio
//!
//! This module provides Python bindings for the Spatio spatio-temporal database using PyO3.
//! It exposes the core functionality including database operations, spatio-temporal queries,
//! and trajectory tracking.

// All geo types are now accessed through spatio wrappers
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyList;
use spatio::{DistanceMetric as RustDistanceMetric, Point3d, Polygon as RustPolygon, Spatio};
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

/// Python wrapper for Polygon
#[pyclass(name = "Polygon")]
#[derive(Clone, Debug)]
pub struct PyPolygon {
    inner: RustPolygon,
}

#[pymethods]
impl PyPolygon {
    #[new]
    #[pyo3(signature = (exterior, interiors=None))]
    fn new(exterior: Vec<(f64, f64)>, interiors: Option<Vec<Vec<(f64, f64)>>>) -> PyResult<Self> {
        let interior_coords = interiors.unwrap_or_default();
        Ok(PyPolygon {
            inner: RustPolygon::from_coords(&exterior, interior_coords),
        })
    }

    #[getter]
    fn exterior(&self) -> Vec<(f64, f64)> {
        self.inner.exterior().coords().map(|c| (c.x, c.y)).collect()
    }

    #[getter]
    fn interiors(&self) -> Vec<Vec<(f64, f64)>> {
        self.inner
            .interiors()
            .iter()
            .map(|ring| ring.coords().map(|c| (c.x, c.y)).collect())
            .collect()
    }

    fn to_geojson(&self) -> PyResult<String> {
        self.inner
            .to_geojson()
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }

    #[staticmethod]
    fn from_geojson(json: &str) -> PyResult<Self> {
        let inner =
            RustPolygon::from_geojson(json).map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(PyPolygon { inner })
    }

    fn __repr__(&self) -> String {
        format!(
            "Polygon(exterior_points={}, interiors={})",
            self.inner.exterior().coords().count(),
            self.inner.interiors().len()
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

    /// Get current location of an object
    #[pyo3(signature = (namespace, object_id))]
    fn get(&self, namespace: &str, object_id: &str) -> PyResult<Option<Py<PyAny>>> {
        let result = handle_error(self.db.get(namespace, object_id))?;

        if let Some(loc) = result {
            Python::attach(|py| {
                let py_point = PyPoint {
                    inner: loc.position,
                };
                let py_meta = pythonize::pythonize(py, &loc.metadata)?;
                let ts = loc
                    .timestamp
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs_f64();

                // (point, metadata, timestamp)
                let tuple = (py_point, py_meta, ts).into_pyobject(py)?;
                Ok(Some(tuple.unbind().into_any()))
            })
        } else {
            Ok(None)
        }
    }

    /// Delete an object
    #[pyo3(signature = (namespace, object_id))]
    fn delete(&self, namespace: &str, object_id: &str) -> PyResult<()> {
        handle_error(self.db.delete(namespace, object_id))
    }

    /// Query objects within a polygon
    #[pyo3(signature = (namespace, polygon, limit=100))]
    fn query_polygon(
        &self,
        namespace: &str,
        polygon: &PyPolygon,
        limit: usize,
    ) -> PyResult<Py<PyList>> {
        let results = handle_error(self.db.query_polygon(namespace, &polygon.inner, limit))?;

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

    /// Calculate distance between two objects
    #[pyo3(signature = (namespace, id1, id2, metric))]
    fn distance_between(
        &self,
        namespace: &str,
        id1: &str,
        id2: &str,
        metric: &PyDistanceMetric,
    ) -> PyResult<Option<f64>> {
        handle_error(self.db.distance_between(namespace, id1, id2, metric.inner))
    }

    /// Calculate distance from object to point
    #[pyo3(signature = (namespace, id, point, metric))]
    fn distance_to(
        &self,
        namespace: &str,
        id: &str,
        point: &PyPoint,
        metric: &PyDistanceMetric,
    ) -> PyResult<Option<f64>> {
        let p = spatio::Point::new(point.inner.x(), point.inner.y());
        handle_error(self.db.distance_to(namespace, id, &p, metric.inner))
    }

    /// Compute convex hull
    #[pyo3(signature = (namespace))]
    fn convex_hull(&self, namespace: &str) -> PyResult<Option<PyPolygon>> {
        let result = handle_error(self.db.convex_hull(namespace))?;
        Ok(result.map(|inner| PyPolygon { inner }))
    }

    /// Compute bounding box
    #[pyo3(signature = (namespace))]
    fn bounding_box(&self, namespace: &str) -> PyResult<Option<(f64, f64, f64, f64)>> {
        let result = handle_error(self.db.bounding_box(namespace))?;
        Ok(result.map(|rect| (rect.min().x, rect.min().y, rect.max().x, rect.max().y)))
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
    m.add_class::<PyPolygon>()?;
    m.add_class::<PyPoint>()?;
    m.add_class::<PyConfig>()?;
    m.add_class::<PyDistanceMetric>()?;
    m.add_class::<PyTemporalPoint>()?;
    m.add_class::<PySetOptions>()?;

    // Add version
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;

    Ok(())
}
