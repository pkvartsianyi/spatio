//! Node.js bindings for Spatio, built with napi-rs.
//!
//! Mirrors the Python bindings, with JS naming: camelCase methods, plain objects
//! instead of tuples, and an options object instead of a `Config` class.

#![deny(clippy::all)]

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use napi::Error;
use napi::bindgen_prelude::FromNapiValue;
use napi::sys::{napi_env, napi_value};
use napi_derive::napi;
use serde_json::Value;
use spatio::config::{Config as CoreConfig, SetOptions as CoreSetOptions};
use spatio::db::CurrentLocation;
use spatio::error::SpatioError;
use spatio::{DistanceMetric as CoreMetric, Point3d, Polygon as CorePolygon};

const DEFAULT_LIMIT: usize = 100;

type Result<T> = napi::Result<T, &'static str>;

fn to_js_err(e: SpatioError) -> Error<&'static str> {
    let code = match e {
        SpatioError::InvalidInput(_)
        | SpatioError::InvalidTimestamp
        | SpatioError::SerializationError
        | SpatioError::SerializationErrorWithContext(_) => "InvalidInput",
        SpatioError::ObjectNotFound => "ObjectNotFound",
        SpatioError::DatabaseClosed => "DatabaseClosed",
        SpatioError::Io(_) => "IoError",
        _ => "GenericFailure",
    };
    Error::new(code, e.to_string())
}

fn invalid_input(msg: impl Into<String>) -> Error<&'static str> {
    Error::new("InvalidInput", msg.into())
}

fn time_from_secs(secs: f64) -> Result<SystemTime> {
    spatio_types::time::system_time_from_secs(secs).map_err(invalid_input)
}

fn secs_from_time(time: SystemTime) -> f64 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

/// JS numbers are doubles, so reject what a `usize` can't hold instead of wrapping.
fn count(value: f64, name: &str) -> Result<usize> {
    if value < 0.0 || value.fract() != 0.0 || value > u32::MAX as f64 || value.is_nan() {
        return Err(invalid_input(format!(
            "{name} must be a non-negative integer, got {value}"
        )));
    }
    Ok(value as usize)
}

fn limit_or_default(limit: Option<f64>) -> Result<usize> {
    limit.map_or(Ok(DEFAULT_LIMIT), |limit| count(limit, "limit"))
}

/// A 3D point: x is longitude, y is latitude, z is altitude.
#[napi]
#[derive(Clone)]
pub struct Point {
    inner: Point3d,
}

#[napi]
impl Point {
    #[napi(constructor)]
    pub fn new(x: f64, y: f64, z: Option<f64>) -> Self {
        Self {
            inner: Point3d::new(x, y, z.unwrap_or(0.0)),
        }
    }

    #[napi(getter)]
    pub fn x(&self) -> f64 {
        self.inner.x()
    }

    #[napi(getter)]
    pub fn y(&self) -> f64 {
        self.inner.y()
    }

    #[napi(getter)]
    pub fn z(&self) -> f64 {
        self.inner.z()
    }

    #[napi(getter)]
    pub fn lon(&self) -> f64 {
        self.inner.x()
    }

    #[napi(getter)]
    pub fn lat(&self) -> f64 {
        self.inner.y()
    }

    #[napi(getter)]
    pub fn alt(&self) -> f64 {
        self.inner.z()
    }

    /// Horizontal (2D) Haversine distance to another point, in meters.
    #[napi]
    pub fn distance_to(&self, other: &Point) -> f64 {
        self.inner.haversine_2d(&other.inner)
    }

    #[napi(js_name = "toString")]
    pub fn to_js_string(&self) -> String {
        format!(
            "Point(x={:.4}, y={:.4}, z={:.4})",
            self.inner.x(),
            self.inner.y(),
            self.inner.z()
        )
    }
}

impl FromNapiValue for Point {
    unsafe fn from_napi_value(env: napi_env, value: napi_value) -> napi::Result<Self> {
        let point = unsafe { <&Point>::from_napi_value(env, value) }?;
        Ok(point.clone())
    }
}

/// A polygon as `[x, y]` rings, with optional holes.
#[napi]
#[derive(Clone)]
pub struct Polygon {
    inner: CorePolygon,
}

#[napi]
impl Polygon {
    #[napi(constructor)]
    pub fn new(exterior: Vec<(f64, f64)>, interiors: Option<Vec<Vec<(f64, f64)>>>) -> Self {
        Self {
            inner: CorePolygon::from_coords(&exterior, interiors.unwrap_or_default()),
        }
    }

    #[napi(getter)]
    pub fn exterior(&self) -> Vec<(f64, f64)> {
        self.inner.exterior().coords().map(|c| (c.x, c.y)).collect()
    }

    #[napi(getter)]
    pub fn interiors(&self) -> Vec<Vec<(f64, f64)>> {
        self.inner
            .interiors()
            .iter()
            .map(|ring| ring.coords().map(|c| (c.x, c.y)).collect())
            .collect()
    }

    #[napi]
    pub fn to_geojson(&self) -> Result<String> {
        self.inner
            .to_geojson()
            .map_err(|e| invalid_input(e.to_string()))
    }

    #[napi(factory)]
    pub fn from_geojson(json: String) -> Result<Self> {
        let inner = CorePolygon::from_geojson(&json).map_err(|e| invalid_input(e.to_string()))?;
        Ok(Self { inner })
    }
}

#[napi(string_enum = "lowercase")]
pub enum DistanceMetric {
    Haversine,
    Geodesic,
    Rhumb,
    Euclidean,
}

impl From<DistanceMetric> for CoreMetric {
    fn from(metric: DistanceMetric) -> Self {
        match metric {
            DistanceMetric::Haversine => CoreMetric::Haversine,
            DistanceMetric::Geodesic => CoreMetric::Geodesic,
            DistanceMetric::Rhumb => CoreMetric::Rhumb,
            DistanceMetric::Euclidean => CoreMetric::Euclidean,
        }
    }
}

#[napi(object)]
pub struct Config {
    /// Recent-history buffer size per object.
    pub buffer_capacity: Option<f64>,
    /// Writes buffered before flushing to the OS.
    pub persistence_buffer_size: Option<f64>,
}

impl TryFrom<Config> for CoreConfig {
    type Error = Error<&'static str>;

    fn try_from(config: Config) -> Result<Self> {
        let mut core = CoreConfig::default();
        if let Some(capacity) = config.buffer_capacity {
            let capacity = count(capacity, "bufferCapacity")?;
            if capacity == 0 {
                return Err(invalid_input("bufferCapacity must be greater than zero"));
            }
            core.buffer_capacity = capacity;
        }
        if let Some(size) = config.persistence_buffer_size {
            core.persistence.buffer_size = count(size, "persistenceBufferSize")?;
        }
        Ok(core)
    }
}

#[napi(object)]
pub struct SetOptions {
    /// Seconds since the Unix epoch.
    pub timestamp: Option<f64>,
}

#[napi(object)]
pub struct TemporalPoint {
    pub point: Point,
    /// Seconds since the Unix epoch.
    pub timestamp: f64,
}

#[napi(object)]
pub struct Location {
    pub object_id: String,
    pub point: Point,
    pub metadata: Value,
}

impl From<Arc<CurrentLocation>> for Location {
    fn from(loc: Arc<CurrentLocation>) -> Self {
        Self {
            object_id: loc.object_id.clone(),
            point: Point {
                inner: loc.position.clone(),
            },
            metadata: loc.metadata.clone(),
        }
    }
}

#[napi(object)]
pub struct Neighbor {
    pub object_id: String,
    pub point: Point,
    pub metadata: Value,
    /// Distance from the query center, in meters.
    pub distance: f64,
}

impl From<(Arc<CurrentLocation>, f64)> for Neighbor {
    fn from((loc, distance): (Arc<CurrentLocation>, f64)) -> Self {
        Self {
            object_id: loc.object_id.clone(),
            point: Point {
                inner: loc.position.clone(),
            },
            metadata: loc.metadata.clone(),
            distance,
        }
    }
}

#[napi(object)]
pub struct Snapshot {
    pub point: Point,
    pub metadata: Value,
    /// Seconds since the Unix epoch.
    pub timestamp: f64,
}

#[napi(object)]
pub struct BoundingBox {
    pub min_x: f64,
    pub min_y: f64,
    pub max_x: f64,
    pub max_y: f64,
}

#[napi(object)]
pub struct Stats {
    pub operations_count: f64,
    pub hot_state_objects: f64,
    pub cold_state_trajectories: f64,
    pub cold_state_buffer_bytes: f64,
    pub memory_usage_bytes: f64,
}

fn neighbors(
    results: spatio::error::Result<Vec<(Arc<CurrentLocation>, f64)>>,
) -> Result<Vec<Neighbor>> {
    let results = results.map_err(to_js_err)?;
    Ok(results.into_iter().map(Neighbor::from).collect())
}

fn locations(results: spatio::error::Result<Vec<Arc<CurrentLocation>>>) -> Result<Vec<Location>> {
    let results = results.map_err(to_js_err)?;
    Ok(results.into_iter().map(Location::from).collect())
}

fn open_db(builder: spatio::DBBuilder, config: Option<Config>) -> Result<Spatio> {
    let builder = match config {
        Some(config) => builder.config(config.try_into()?),
        None => builder,
    };
    let db = builder.build().map_err(to_js_err)?;
    Ok(Spatio { db })
}

#[napi]
pub struct Spatio {
    db: spatio::Spatio,
}

#[napi]
impl Spatio {
    /// Create an in-memory database.
    #[napi(factory)]
    pub fn memory(config: Option<Config>) -> Result<Self> {
        open_db(spatio::Spatio::builder(), config)
    }

    /// Open a persistent database, creating it if needed.
    #[napi(factory)]
    pub fn open(path: String, config: Option<Config>) -> Result<Self> {
        open_db(spatio::Spatio::builder().path(path), config)
    }

    #[napi]
    pub fn upsert(
        &self,
        namespace: String,
        object_id: String,
        point: &Point,
        metadata: Option<Value>,
        opts: Option<SetOptions>,
    ) -> Result<()> {
        let opts = match opts.and_then(|o| o.timestamp) {
            Some(secs) => Some(CoreSetOptions::with_timestamp(time_from_secs(secs)?)),
            None => None,
        };
        self.db
            .upsert(
                &namespace,
                &object_id,
                point.inner.clone(),
                metadata.unwrap_or(Value::Null),
                opts,
            )
            .map_err(to_js_err)
    }

    /// Insert a trajectory. Trajectories are stored in 2D, so a point with a
    /// non-zero z is rejected.
    #[napi]
    pub fn insert_trajectory(
        &self,
        namespace: String,
        object_id: String,
        trajectory: Vec<TemporalPoint>,
    ) -> Result<()> {
        let mut points = Vec::with_capacity(trajectory.len());
        for tp in trajectory {
            if tp.point.z() != 0.0 {
                return Err(invalid_input(
                    "trajectories are stored in 2D; point z must be 0",
                ));
            }
            points.push(spatio::TemporalPoint {
                point: spatio::Point::new(tp.point.x(), tp.point.y()),
                timestamp: time_from_secs(tp.timestamp)?,
            });
        }
        self.db
            .insert_trajectory(&namespace, &object_id, &points)
            .map_err(to_js_err)
    }

    #[napi]
    pub fn get(&self, namespace: String, object_id: String) -> Result<Option<Snapshot>> {
        let loc = self.db.get(&namespace, &object_id).map_err(to_js_err)?;
        Ok(loc.map(|loc| Snapshot {
            point: Point {
                inner: loc.position.clone(),
            },
            metadata: loc.metadata.clone(),
            timestamp: secs_from_time(loc.timestamp),
        }))
    }

    #[napi]
    pub fn delete(&self, namespace: String, object_id: String) -> Result<()> {
        self.db.delete(&namespace, &object_id).map_err(to_js_err)
    }

    #[napi]
    pub fn query_radius(
        &self,
        namespace: String,
        center: &Point,
        radius: f64,
        limit: Option<f64>,
    ) -> Result<Vec<Neighbor>> {
        neighbors(
            self.db
                .query_radius(&namespace, &center.inner, radius, limit_or_default(limit)?),
        )
    }

    #[napi]
    pub fn query_near(
        &self,
        namespace: String,
        object_id: String,
        radius: f64,
        limit: Option<f64>,
    ) -> Result<Vec<Neighbor>> {
        neighbors(
            self.db
                .query_near(&namespace, &object_id, radius, limit_or_default(limit)?),
        )
    }

    #[napi]
    pub fn knn(&self, namespace: String, center: &Point, k: f64) -> Result<Vec<Neighbor>> {
        neighbors(self.db.knn(&namespace, &center.inner, count(k, "k")?))
    }

    #[napi]
    pub fn knn_near_object(
        &self,
        namespace: String,
        object_id: String,
        k: f64,
    ) -> Result<Vec<Neighbor>> {
        neighbors(
            self.db
                .knn_near_object(&namespace, &object_id, count(k, "k")?),
        )
    }

    #[napi]
    pub fn query_trajectory(
        &self,
        namespace: String,
        object_id: String,
        start_time: f64,
        end_time: f64,
        limit: Option<f64>,
    ) -> Result<Vec<Snapshot>> {
        let updates = self
            .db
            .query_trajectory(
                &namespace,
                &object_id,
                time_from_secs(start_time)?,
                time_from_secs(end_time)?,
                limit_or_default(limit)?,
            )
            .map_err(to_js_err)?;
        Ok(updates
            .into_iter()
            .map(|update| Snapshot {
                point: Point {
                    inner: update.position,
                },
                metadata: update.metadata,
                timestamp: secs_from_time(update.timestamp),
            })
            .collect())
    }

    #[napi]
    pub fn query_bbox(
        &self,
        namespace: String,
        min_x: f64,
        min_y: f64,
        max_x: f64,
        max_y: f64,
        limit: Option<f64>,
    ) -> Result<Vec<Location>> {
        locations(self.db.query_bbox(
            &namespace,
            min_x,
            min_y,
            max_x,
            max_y,
            limit_or_default(limit)?,
        ))
    }

    #[napi]
    pub fn query_within_cylinder(
        &self,
        namespace: String,
        center: &Point,
        min_z: f64,
        max_z: f64,
        radius: f64,
        limit: Option<f64>,
    ) -> Result<Vec<Neighbor>> {
        let center = spatio::Point::new(center.x(), center.y());
        neighbors(self.db.query_within_cylinder(
            &namespace,
            center,
            min_z,
            max_z,
            radius,
            limit_or_default(limit)?,
        ))
    }

    #[napi]
    #[allow(clippy::too_many_arguments)]
    pub fn query_within_bbox3d(
        &self,
        namespace: String,
        min_x: f64,
        min_y: f64,
        min_z: f64,
        max_x: f64,
        max_y: f64,
        max_z: f64,
        limit: Option<f64>,
    ) -> Result<Vec<Location>> {
        locations(self.db.query_within_bbox_3d(
            &namespace,
            min_x,
            min_y,
            min_z,
            max_x,
            max_y,
            max_z,
            limit_or_default(limit)?,
        ))
    }

    #[napi]
    pub fn query_bbox_near_object(
        &self,
        namespace: String,
        object_id: String,
        width: f64,
        height: f64,
        limit: Option<f64>,
    ) -> Result<Vec<Location>> {
        locations(self.db.query_bbox_near_object(
            &namespace,
            &object_id,
            width,
            height,
            limit_or_default(limit)?,
        ))
    }

    #[napi]
    pub fn query_cylinder_near_object(
        &self,
        namespace: String,
        object_id: String,
        min_z: f64,
        max_z: f64,
        radius: f64,
        limit: Option<f64>,
    ) -> Result<Vec<Neighbor>> {
        neighbors(self.db.query_cylinder_near_object(
            &namespace,
            &object_id,
            min_z,
            max_z,
            radius,
            limit_or_default(limit)?,
        ))
    }

    #[napi]
    pub fn query_bbox3d_near_object(
        &self,
        namespace: String,
        object_id: String,
        width: f64,
        height: f64,
        depth: f64,
        limit: Option<f64>,
    ) -> Result<Vec<Location>> {
        locations(self.db.query_bbox_3d_near_object(
            &namespace,
            &object_id,
            width,
            height,
            depth,
            limit_or_default(limit)?,
        ))
    }

    #[napi]
    pub fn query_polygon(
        &self,
        namespace: String,
        polygon: &Polygon,
        limit: Option<f64>,
    ) -> Result<Vec<Location>> {
        locations(
            self.db
                .query_polygon(&namespace, &polygon.inner, limit_or_default(limit)?),
        )
    }

    #[napi]
    pub fn distance_between(
        &self,
        namespace: String,
        id1: String,
        id2: String,
        metric: DistanceMetric,
    ) -> Result<Option<f64>> {
        self.db
            .distance_between(&namespace, &id1, &id2, metric.into())
            .map_err(to_js_err)
    }

    #[napi]
    pub fn distance_to(
        &self,
        namespace: String,
        id: String,
        point: &Point,
        metric: DistanceMetric,
    ) -> Result<Option<f64>> {
        let point = spatio::Point::new(point.x(), point.y());
        self.db
            .distance_to(&namespace, &id, &point, metric.into())
            .map_err(to_js_err)
    }

    #[napi]
    pub fn convex_hull(&self, namespace: String) -> Result<Option<Polygon>> {
        let hull = self.db.convex_hull(&namespace).map_err(to_js_err)?;
        Ok(hull.map(|inner| Polygon { inner }))
    }

    #[napi]
    pub fn bounding_box(&self, namespace: String) -> Result<Option<BoundingBox>> {
        let rect = self.db.bounding_box(&namespace).map_err(to_js_err)?;
        Ok(rect.map(|rect| BoundingBox {
            min_x: rect.min().x,
            min_y: rect.min().y,
            max_x: rect.max().x,
            max_y: rect.max().y,
        }))
    }

    #[napi]
    pub fn stats(&self) -> Stats {
        let stats = self.db.stats();
        Stats {
            operations_count: stats.operations_count as f64,
            hot_state_objects: stats.hot_state_objects as f64,
            cold_state_trajectories: stats.cold_state_trajectories as f64,
            cold_state_buffer_bytes: stats.cold_state_buffer_bytes as f64,
            memory_usage_bytes: stats.memory_usage_bytes as f64,
        }
    }

    /// Flush buffered writes and release the file lock.
    #[napi]
    pub fn close(&self) -> Result<()> {
        self.db.close().map_err(to_js_err)
    }
}
