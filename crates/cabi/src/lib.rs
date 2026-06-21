//! C ABI for Spatio.
//!
//! This crate exposes a flat, `extern "C"` surface over the embedded
//! [`spatio`] database so non-Rust languages can drive it in-process. It is the
//! foundation for the Go bindings (called via `purego`, no cgo).
//!
//! ## Conventions
//! - **Handle:** an opaque `*mut c_void` returned by `spatio_open*`, freed by
//!   `spatio_close`. It wraps a boxed [`spatio::Spatio`] (cheaply `Clone`,
//!   `Send + Sync`).
//! - **Status:** every fallible function returns `i32` (`0` = OK; see the
//!   `SPATIO_ERR_*` constants in [`ffi`]).
//! - **Errors:** human-readable detail is written to an `err: *mut *mut c_char`
//!   out-param; the caller frees it with `spatio_string_free`.
//! - **Points:** passed as scalar `f64 x, y, z`.
//! - **Result sets:** returned as a packed little-endian binary buffer
//!   (`out_ptr`/`out_len`) the caller frees with `spatio_buffer_free`. See
//!   [`wire`] for the layout. Per-record metadata is an opaque length-prefixed
//!   blob (JSON), decoded lazily by the caller.
//! - **Polygons / convex hull:** GeoJSON strings.
//! - **Timestamps:** `f64` seconds since the unix epoch.
//!
//! The boundary functions are written to never panic on caller input: the
//! workspace release profile uses `panic = "abort"`, so an unwind across the
//! ABI would abort the host process.

// The boundary functions take raw pointers but null-check and validate them, so
// they are exported as safe-to-call from C rather than marked `unsafe`.
#![allow(clippy::not_unsafe_ptr_arg_deref)]

mod ffi;
mod wire;

use ffi::*;
use serde::Deserialize;
use spatio::config::{Config, SetOptions};
use spatio::db::CurrentLocation;
use spatio::{DistanceMetric, Point, Point3d, Polygon, Spatio, TemporalPoint};
use spatio_types::time::system_time_from_secs;
use std::ffi::{CString, c_char, c_void};
use std::sync::Arc;

/// Unwrap a `Result<_, i32>` from an `ffi` helper, reporting a standard message
/// for the error code and returning it from the current function on `Err`.
macro_rules! tri {
    ($expr:expr, $err:expr) => {
        match $expr {
            Ok(v) => v,
            Err(code) => return unsafe { arg_err($err, code) },
        }
    };
}

/// Set a stock message for a non-`SpatioError` status code and return it.
///
/// # Safety
/// `err` must be null or a valid, writable `*mut *mut c_char`.
unsafe fn arg_err(err: *mut *mut c_char, code: i32) -> i32 {
    let msg = match code {
        SPATIO_ERR_NULL_ARG => "null pointer argument",
        SPATIO_ERR_UTF8 => "argument is not valid UTF-8",
        SPATIO_ERR_INVALID_INPUT => "invalid input",
        SPATIO_ERR_INVALID_TIMESTAMP => "invalid timestamp",
        SPATIO_ERR_SERIALIZATION => "serialization error",
        _ => "error",
    };
    unsafe { set_err(err, msg) };
    code
}

static VERSION_C: &[u8] = concat!(env!("CARGO_PKG_VERSION"), "\0").as_bytes();

/// Return the cabi crate version as a static, null-terminated C string. Never
/// freed by the caller.
#[unsafe(no_mangle)]
pub extern "C" fn spatio_version() -> *const c_char {
    VERSION_C.as_ptr() as *const c_char
}

/// Free a string previously returned by this library (error messages, GeoJSON).
/// Null is ignored.
#[unsafe(no_mangle)]
pub extern "C" fn spatio_string_free(s: *mut c_char) {
    if !s.is_null() {
        unsafe { drop(CString::from_raw(s)) };
    }
}

/// Free a result buffer previously returned through `out_ptr`/`out_len`. Null is
/// ignored.
#[unsafe(no_mangle)]
pub extern "C" fn spatio_buffer_free(ptr: *mut u8, len: usize) {
    if ptr.is_null() {
        return;
    }
    unsafe {
        drop(Box::from_raw(std::ptr::slice_from_raw_parts_mut(ptr, len)));
    }
}

// Input parsing helpers

#[derive(Deserialize)]
struct ConfigDto {
    buffer_capacity: Option<usize>,
    persistence_buffer_size: Option<usize>,
}

/// Build a [`Config`] from optional JSON (`{"buffer_capacity":N,
/// "persistence_buffer_size":N}`). Mirrors the fields the Python binding exposes.
fn build_config(json: Option<&str>) -> Result<Config, i32> {
    let mut cfg = Config::default();
    if let Some(s) = json {
        let dto: ConfigDto = serde_json::from_str(s).map_err(|_| SPATIO_ERR_INVALID_INPUT)?;
        if let Some(cap) = dto.buffer_capacity {
            if cap == 0 {
                return Err(SPATIO_ERR_INVALID_INPUT);
            }
            cfg.buffer_capacity = cap;
        }
        if let Some(size) = dto.persistence_buffer_size {
            cfg.persistence.buffer_size = size;
        }
    }
    Ok(cfg)
}

#[derive(Deserialize)]
struct OptsDto {
    timestamp: Option<f64>,
}

/// Build optional [`SetOptions`] from optional JSON (`{"timestamp":secs}`).
fn build_opts(json: Option<&str>) -> Result<Option<SetOptions>, i32> {
    let Some(s) = json else { return Ok(None) };
    let dto: OptsDto = serde_json::from_str(s).map_err(|_| SPATIO_ERR_INVALID_INPUT)?;
    match dto.timestamp {
        Some(secs) => {
            let ts = system_time_from_secs(secs).map_err(|_| SPATIO_ERR_INVALID_TIMESTAMP)?;
            Ok(Some(SetOptions::with_timestamp(ts)))
        }
        None => Ok(Some(SetOptions::default())),
    }
}

/// Parse metadata JSON; a null pointer means no metadata (`Null`).
fn parse_metadata(json: Option<&str>) -> Result<serde_json::Value, i32> {
    match json {
        Some(s) => serde_json::from_str(s).map_err(|_| SPATIO_ERR_INVALID_INPUT),
        None => Ok(serde_json::Value::Null),
    }
}

fn parse_metric(s: &str) -> Result<DistanceMetric, i32> {
    match s.to_ascii_lowercase().as_str() {
        "haversine" => Ok(DistanceMetric::Haversine),
        "geodesic" => Ok(DistanceMetric::Geodesic),
        "rhumb" => Ok(DistanceMetric::Rhumb),
        "euclidean" => Ok(DistanceMetric::Euclidean),
        _ => Err(SPATIO_ERR_INVALID_INPUT),
    }
}

/// Emit a `(location, distance)` result list as a binary buffer.
unsafe fn emit_neighbors(
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
    results: Vec<(Arc<CurrentLocation>, f64)>,
) -> i32 {
    unsafe { emit_buffer(out_ptr, out_len, wire::encode_neighbors(&results)) }
}

/// Emit a location result list as a binary buffer.
unsafe fn emit_locations(
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
    results: Vec<Arc<CurrentLocation>>,
) -> i32 {
    unsafe { emit_buffer(out_ptr, out_len, wire::encode_locations(&results)) }
}

// Lifecycle

/// Create an in-memory database. `config_json` may be null for defaults.
#[unsafe(no_mangle)]
pub extern "C" fn spatio_open_memory(
    config_json: *const c_char,
    out_handle: *mut *mut c_void,
    err: *mut *mut c_char,
) -> i32 {
    if out_handle.is_null() {
        return unsafe { arg_err(err, SPATIO_ERR_NULL_ARG) };
    }
    let cfg_json = tri!(unsafe { cstr_opt(config_json) }, err);
    let cfg = tri!(build_config(cfg_json), err);
    match Spatio::memory_with_config(cfg) {
        Ok(db) => {
            unsafe { *out_handle = Box::into_raw(Box::new(db)) as *mut c_void };
            SPATIO_OK
        }
        Err(e) => unsafe { report(err, &e) },
    }
}

/// Open (or create) a persistent database at `path`. `config_json` may be null.
#[unsafe(no_mangle)]
pub extern "C" fn spatio_open(
    path: *const c_char,
    config_json: *const c_char,
    out_handle: *mut *mut c_void,
    err: *mut *mut c_char,
) -> i32 {
    if out_handle.is_null() {
        return unsafe { arg_err(err, SPATIO_ERR_NULL_ARG) };
    }
    let path = tri!(unsafe { cstr(path) }, err);
    let cfg_json = tri!(unsafe { cstr_opt(config_json) }, err);
    let cfg = tri!(build_config(cfg_json), err);
    match Spatio::open_with_config(path, cfg) {
        Ok(db) => {
            unsafe { *out_handle = Box::into_raw(Box::new(db)) as *mut c_void };
            SPATIO_OK
        }
        Err(e) => unsafe { report(err, &e) },
    }
}

/// Flush buffered writes and free the handle. The handle must not be used again.
#[unsafe(no_mangle)]
pub extern "C" fn spatio_close(handle_ptr: *mut c_void, err: *mut *mut c_char) -> i32 {
    if handle_ptr.is_null() {
        return unsafe { arg_err(err, SPATIO_ERR_NULL_ARG) };
    }
    let db = unsafe { Box::from_raw(handle_ptr as *mut Spatio) };
    match db.close() {
        Ok(()) => SPATIO_OK,
        Err(e) => unsafe { report(err, &e) },
    }
}

// Writes

/// Upsert an object's location. `metadata_json` and `opts_json` may be null.
#[unsafe(no_mangle)]
pub extern "C" fn spatio_upsert(
    handle_ptr: *mut c_void,
    namespace: *const c_char,
    object_id: *const c_char,
    x: f64,
    y: f64,
    z: f64,
    metadata_json: *const c_char,
    opts_json: *const c_char,
    err: *mut *mut c_char,
) -> i32 {
    let db = tri!(unsafe { handle(handle_ptr) }, err);
    let ns = tri!(unsafe { cstr(namespace) }, err);
    let id = tri!(unsafe { cstr(object_id) }, err);
    let meta = tri!(
        parse_metadata(tri!(unsafe { cstr_opt(metadata_json) }, err)),
        err
    );
    let opts = tri!(build_opts(tri!(unsafe { cstr_opt(opts_json) }, err)), err);
    match db.upsert(ns, id, Point3d::new(x, y, z), meta, opts) {
        Ok(()) => SPATIO_OK,
        Err(e) => unsafe { report(err, &e) },
    }
}

/// Delete an object.
#[unsafe(no_mangle)]
pub extern "C" fn spatio_delete(
    handle_ptr: *mut c_void,
    namespace: *const c_char,
    object_id: *const c_char,
    err: *mut *mut c_char,
) -> i32 {
    let db = tri!(unsafe { handle(handle_ptr) }, err);
    let ns = tri!(unsafe { cstr(namespace) }, err);
    let id = tri!(unsafe { cstr(object_id) }, err);
    match db.delete(ns, id) {
        Ok(()) => SPATIO_OK,
        Err(e) => unsafe { report(err, &e) },
    }
}

#[derive(Deserialize)]
struct TrajInputPoint {
    x: f64,
    y: f64,
    t: f64,
}

/// Insert a trajectory from JSON: `[{"x":..,"y":..,"t":secs}, ...]`.
#[unsafe(no_mangle)]
pub extern "C" fn spatio_insert_trajectory(
    handle_ptr: *mut c_void,
    namespace: *const c_char,
    object_id: *const c_char,
    trajectory_json: *const c_char,
    err: *mut *mut c_char,
) -> i32 {
    let db = tri!(unsafe { handle(handle_ptr) }, err);
    let ns = tri!(unsafe { cstr(namespace) }, err);
    let id = tri!(unsafe { cstr(object_id) }, err);
    let json = tri!(unsafe { cstr(trajectory_json) }, err);
    let points: Vec<TrajInputPoint> = tri!(
        serde_json::from_str(json).map_err(|_| SPATIO_ERR_INVALID_INPUT),
        err
    );

    let mut trajectory = Vec::with_capacity(points.len());
    for p in points {
        let ts = tri!(
            system_time_from_secs(p.t).map_err(|_| SPATIO_ERR_INVALID_TIMESTAMP),
            err
        );
        trajectory.push(TemporalPoint {
            point: Point::new(p.x, p.y),
            timestamp: ts,
        });
    }
    match db.insert_trajectory(ns, id, &trajectory) {
        Ok(()) => SPATIO_OK,
        Err(e) => unsafe { report(err, &e) },
    }
}

// Reads

/// Get an object's current location as a binary buffer of 0 or 1 location
/// records.
#[unsafe(no_mangle)]
pub extern "C" fn spatio_get(
    handle_ptr: *mut c_void,
    namespace: *const c_char,
    object_id: *const c_char,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
    err: *mut *mut c_char,
) -> i32 {
    let db = tri!(unsafe { handle(handle_ptr) }, err);
    let ns = tri!(unsafe { cstr(namespace) }, err);
    let id = tri!(unsafe { cstr(object_id) }, err);
    match db.get(ns, id) {
        Ok(opt) => unsafe {
            emit_buffer(out_ptr, out_len, wire::encode_location_opt(opt.as_deref()))
        },
        Err(e) => unsafe { report(err, &e) },
    }
}

/// Database statistics, written as 7 `u64` values into `out` (an array of at
/// least 7): expired_count, operations_count, size_bytes, hot_state_objects,
/// cold_state_trajectories, cold_state_buffer_bytes, memory_usage_bytes.
#[unsafe(no_mangle)]
pub extern "C" fn spatio_stats(
    handle_ptr: *mut c_void,
    out: *mut u64,
    err: *mut *mut c_char,
) -> i32 {
    let db = tri!(unsafe { handle(handle_ptr) }, err);
    if out.is_null() {
        return unsafe { arg_err(err, SPATIO_ERR_NULL_ARG) };
    }
    let s = db.stats();
    unsafe {
        let a = std::slice::from_raw_parts_mut(out, 7);
        a[0] = s.expired_count;
        a[1] = s.operations_count;
        a[2] = s.size_bytes as u64;
        a[3] = s.hot_state_objects as u64;
        a[4] = s.cold_state_trajectories as u64;
        a[5] = s.cold_state_buffer_bytes as u64;
        a[6] = s.memory_usage_bytes as u64;
    }
    SPATIO_OK
}

// Point / volume queries (return binary buffers)

/// Objects within `radius` of a point, with distances.
#[unsafe(no_mangle)]
pub extern "C" fn spatio_query_radius(
    handle_ptr: *mut c_void,
    namespace: *const c_char,
    x: f64,
    y: f64,
    z: f64,
    radius: f64,
    limit: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
    err: *mut *mut c_char,
) -> i32 {
    let db = tri!(unsafe { handle(handle_ptr) }, err);
    let ns = tri!(unsafe { cstr(namespace) }, err);
    match db.query_radius(ns, &Point3d::new(x, y, z), radius, limit) {
        Ok(results) => unsafe { emit_neighbors(out_ptr, out_len, results) },
        Err(e) => unsafe { report(err, &e) },
    }
}

/// Objects near another object, with distances.
#[unsafe(no_mangle)]
pub extern "C" fn spatio_query_near(
    handle_ptr: *mut c_void,
    namespace: *const c_char,
    object_id: *const c_char,
    radius: f64,
    limit: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
    err: *mut *mut c_char,
) -> i32 {
    let db = tri!(unsafe { handle(handle_ptr) }, err);
    let ns = tri!(unsafe { cstr(namespace) }, err);
    let id = tri!(unsafe { cstr(object_id) }, err);
    match db.query_near(ns, id, radius, limit) {
        Ok(results) => unsafe { emit_neighbors(out_ptr, out_len, results) },
        Err(e) => unsafe { report(err, &e) },
    }
}

/// k nearest neighbors of a point.
#[unsafe(no_mangle)]
pub extern "C" fn spatio_knn(
    handle_ptr: *mut c_void,
    namespace: *const c_char,
    x: f64,
    y: f64,
    z: f64,
    k: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
    err: *mut *mut c_char,
) -> i32 {
    let db = tri!(unsafe { handle(handle_ptr) }, err);
    let ns = tri!(unsafe { cstr(namespace) }, err);
    match db.knn(ns, &Point3d::new(x, y, z), k) {
        Ok(results) => unsafe { emit_neighbors(out_ptr, out_len, results) },
        Err(e) => unsafe { report(err, &e) },
    }
}

/// k nearest neighbors of another object.
#[unsafe(no_mangle)]
pub extern "C" fn spatio_knn_near_object(
    handle_ptr: *mut c_void,
    namespace: *const c_char,
    object_id: *const c_char,
    k: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
    err: *mut *mut c_char,
) -> i32 {
    let db = tri!(unsafe { handle(handle_ptr) }, err);
    let ns = tri!(unsafe { cstr(namespace) }, err);
    let id = tri!(unsafe { cstr(object_id) }, err);
    match db.knn_near_object(ns, id, k) {
        Ok(results) => unsafe { emit_neighbors(out_ptr, out_len, results) },
        Err(e) => unsafe { report(err, &e) },
    }
}

/// Objects within a 2D bounding box.
#[unsafe(no_mangle)]
pub extern "C" fn spatio_query_bbox(
    handle_ptr: *mut c_void,
    namespace: *const c_char,
    min_x: f64,
    min_y: f64,
    max_x: f64,
    max_y: f64,
    limit: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
    err: *mut *mut c_char,
) -> i32 {
    let db = tri!(unsafe { handle(handle_ptr) }, err);
    let ns = tri!(unsafe { cstr(namespace) }, err);
    match db.query_bbox(ns, min_x, min_y, max_x, max_y, limit) {
        Ok(results) => unsafe { emit_locations(out_ptr, out_len, results) },
        Err(e) => unsafe { report(err, &e) },
    }
}

/// Objects within a cylindrical volume, with distances.
#[unsafe(no_mangle)]
pub extern "C" fn spatio_query_within_cylinder(
    handle_ptr: *mut c_void,
    namespace: *const c_char,
    x: f64,
    y: f64,
    min_z: f64,
    max_z: f64,
    radius: f64,
    limit: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
    err: *mut *mut c_char,
) -> i32 {
    let db = tri!(unsafe { handle(handle_ptr) }, err);
    let ns = tri!(unsafe { cstr(namespace) }, err);
    match db.query_within_cylinder(ns, Point::new(x, y), min_z, max_z, radius, limit) {
        Ok(results) => unsafe { emit_neighbors(out_ptr, out_len, results) },
        Err(e) => unsafe { report(err, &e) },
    }
}

/// Objects within a 3D bounding box.
#[unsafe(no_mangle)]
pub extern "C" fn spatio_query_within_bbox_3d(
    handle_ptr: *mut c_void,
    namespace: *const c_char,
    min_x: f64,
    min_y: f64,
    min_z: f64,
    max_x: f64,
    max_y: f64,
    max_z: f64,
    limit: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
    err: *mut *mut c_char,
) -> i32 {
    let db = tri!(unsafe { handle(handle_ptr) }, err);
    let ns = tri!(unsafe { cstr(namespace) }, err);
    match db.query_within_bbox_3d(ns, min_x, min_y, min_z, max_x, max_y, max_z, limit) {
        Ok(results) => unsafe { emit_locations(out_ptr, out_len, results) },
        Err(e) => unsafe { report(err, &e) },
    }
}

/// Objects within a bounding box centered on another object.
#[unsafe(no_mangle)]
pub extern "C" fn spatio_query_bbox_near_object(
    handle_ptr: *mut c_void,
    namespace: *const c_char,
    object_id: *const c_char,
    width: f64,
    height: f64,
    limit: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
    err: *mut *mut c_char,
) -> i32 {
    let db = tri!(unsafe { handle(handle_ptr) }, err);
    let ns = tri!(unsafe { cstr(namespace) }, err);
    let id = tri!(unsafe { cstr(object_id) }, err);
    match db.query_bbox_near_object(ns, id, width, height, limit) {
        Ok(results) => unsafe { emit_locations(out_ptr, out_len, results) },
        Err(e) => unsafe { report(err, &e) },
    }
}

/// Objects within a cylinder centered on another object, with distances.
#[unsafe(no_mangle)]
pub extern "C" fn spatio_query_cylinder_near_object(
    handle_ptr: *mut c_void,
    namespace: *const c_char,
    object_id: *const c_char,
    min_z: f64,
    max_z: f64,
    radius: f64,
    limit: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
    err: *mut *mut c_char,
) -> i32 {
    let db = tri!(unsafe { handle(handle_ptr) }, err);
    let ns = tri!(unsafe { cstr(namespace) }, err);
    let id = tri!(unsafe { cstr(object_id) }, err);
    match db.query_cylinder_near_object(ns, id, min_z, max_z, radius, limit) {
        Ok(results) => unsafe { emit_neighbors(out_ptr, out_len, results) },
        Err(e) => unsafe { report(err, &e) },
    }
}

/// Objects within a 3D bounding box centered on another object.
#[unsafe(no_mangle)]
pub extern "C" fn spatio_query_bbox_3d_near_object(
    handle_ptr: *mut c_void,
    namespace: *const c_char,
    object_id: *const c_char,
    width: f64,
    height: f64,
    depth: f64,
    limit: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
    err: *mut *mut c_char,
) -> i32 {
    let db = tri!(unsafe { handle(handle_ptr) }, err);
    let ns = tri!(unsafe { cstr(namespace) }, err);
    let id = tri!(unsafe { cstr(object_id) }, err);
    match db.query_bbox_3d_near_object(ns, id, width, height, depth, limit) {
        Ok(results) => unsafe { emit_locations(out_ptr, out_len, results) },
        Err(e) => unsafe { report(err, &e) },
    }
}

/// Objects whose location falls within a polygon (supplied as GeoJSON).
#[unsafe(no_mangle)]
pub extern "C" fn spatio_query_polygon(
    handle_ptr: *mut c_void,
    namespace: *const c_char,
    polygon_geojson: *const c_char,
    limit: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
    err: *mut *mut c_char,
) -> i32 {
    let db = tri!(unsafe { handle(handle_ptr) }, err);
    let ns = tri!(unsafe { cstr(namespace) }, err);
    let geojson = tri!(unsafe { cstr(polygon_geojson) }, err);
    let polygon = tri!(
        Polygon::from_geojson(geojson).map_err(|_| SPATIO_ERR_INVALID_INPUT),
        err
    );
    match db.query_polygon(ns, &polygon, limit) {
        Ok(results) => unsafe { emit_locations(out_ptr, out_len, results) },
        Err(e) => unsafe { report(err, &e) },
    }
}

/// Historical trajectory between two timestamps (unix seconds), as a binary
/// buffer of trajectory records.
#[unsafe(no_mangle)]
pub extern "C" fn spatio_query_trajectory(
    handle_ptr: *mut c_void,
    namespace: *const c_char,
    object_id: *const c_char,
    start_secs: f64,
    end_secs: f64,
    limit: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
    err: *mut *mut c_char,
) -> i32 {
    let db = tri!(unsafe { handle(handle_ptr) }, err);
    let ns = tri!(unsafe { cstr(namespace) }, err);
    let id = tri!(unsafe { cstr(object_id) }, err);
    let start = tri!(
        system_time_from_secs(start_secs).map_err(|_| SPATIO_ERR_INVALID_TIMESTAMP),
        err
    );
    let end = tri!(
        system_time_from_secs(end_secs).map_err(|_| SPATIO_ERR_INVALID_TIMESTAMP),
        err
    );
    match db.query_trajectory(ns, id, start, end, limit) {
        Ok(updates) => unsafe { emit_buffer(out_ptr, out_len, wire::encode_trajectory(&updates)) },
        Err(e) => unsafe { report(err, &e) },
    }
}

// Distance & geometry

/// Distance between two objects under `metric`. `*out_found` is false if either
/// object is missing.
#[unsafe(no_mangle)]
pub extern "C" fn spatio_distance_between(
    handle_ptr: *mut c_void,
    namespace: *const c_char,
    id1: *const c_char,
    id2: *const c_char,
    metric: *const c_char,
    out_distance: *mut f64,
    out_found: *mut bool,
    err: *mut *mut c_char,
) -> i32 {
    let db = tri!(unsafe { handle(handle_ptr) }, err);
    let ns = tri!(unsafe { cstr(namespace) }, err);
    let a = tri!(unsafe { cstr(id1) }, err);
    let b = tri!(unsafe { cstr(id2) }, err);
    let m = tri!(parse_metric(tri!(unsafe { cstr(metric) }, err)), err);
    match db.distance_between(ns, a, b, m) {
        Ok(opt) => unsafe { write_optional_f64(out_distance, out_found, opt) },
        Err(e) => unsafe { report(err, &e) },
    }
}

/// Distance from an object to a point under `metric`. `*out_found` is false if
/// the object is missing.
#[unsafe(no_mangle)]
pub extern "C" fn spatio_distance_to(
    handle_ptr: *mut c_void,
    namespace: *const c_char,
    object_id: *const c_char,
    x: f64,
    y: f64,
    metric: *const c_char,
    out_distance: *mut f64,
    out_found: *mut bool,
    err: *mut *mut c_char,
) -> i32 {
    let db = tri!(unsafe { handle(handle_ptr) }, err);
    let ns = tri!(unsafe { cstr(namespace) }, err);
    let id = tri!(unsafe { cstr(object_id) }, err);
    let m = tri!(parse_metric(tri!(unsafe { cstr(metric) }, err)), err);
    match db.distance_to(ns, id, &Point::new(x, y), m) {
        Ok(opt) => unsafe { write_optional_f64(out_distance, out_found, opt) },
        Err(e) => unsafe { report(err, &e) },
    }
}

/// Helper for the `Option<f64>` return shape shared by the distance functions.
///
/// # Safety
/// `out_distance`/`out_found`, if non-null, must be valid writable pointers.
unsafe fn write_optional_f64(
    out_distance: *mut f64,
    out_found: *mut bool,
    value: Option<f64>,
) -> i32 {
    if !out_found.is_null() {
        unsafe { *out_found = value.is_some() };
    }
    if let Some(d) = value
        && !out_distance.is_null()
    {
        unsafe { *out_distance = d };
    }
    SPATIO_OK
}

/// Convex hull of all objects in a namespace, as GeoJSON, or null if fewer than
/// three points.
#[unsafe(no_mangle)]
pub extern "C" fn spatio_convex_hull(
    handle_ptr: *mut c_void,
    namespace: *const c_char,
    out_geojson: *mut *mut c_char,
    err: *mut *mut c_char,
) -> i32 {
    if out_geojson.is_null() {
        return unsafe { arg_err(err, SPATIO_ERR_NULL_ARG) };
    }
    let db = tri!(unsafe { handle(handle_ptr) }, err);
    let ns = tri!(unsafe { cstr(namespace) }, err);
    match db.convex_hull(ns) {
        Ok(Some(poly)) => match poly.to_geojson() {
            Ok(s) => unsafe { out_string(out_geojson, s) },
            Err(_) => SPATIO_ERR_SERIALIZATION,
        },
        Ok(None) => {
            unsafe { *out_geojson = std::ptr::null_mut() };
            SPATIO_OK
        }
        Err(e) => unsafe { report(err, &e) },
    }
}

/// Axis-aligned 2D bounding box of all objects in a namespace. `*out_found` is
/// false for an empty namespace.
#[unsafe(no_mangle)]
pub extern "C" fn spatio_bounding_box(
    handle_ptr: *mut c_void,
    namespace: *const c_char,
    out_min_x: *mut f64,
    out_min_y: *mut f64,
    out_max_x: *mut f64,
    out_max_y: *mut f64,
    out_found: *mut bool,
    err: *mut *mut c_char,
) -> i32 {
    let db = tri!(unsafe { handle(handle_ptr) }, err);
    let ns = tri!(unsafe { cstr(namespace) }, err);
    match db.bounding_box(ns) {
        Ok(Some(rect)) => {
            unsafe {
                if !out_found.is_null() {
                    *out_found = true;
                }
                if !out_min_x.is_null() {
                    *out_min_x = rect.min().x;
                }
                if !out_min_y.is_null() {
                    *out_min_y = rect.min().y;
                }
                if !out_max_x.is_null() {
                    *out_max_x = rect.max().x;
                }
                if !out_max_y.is_null() {
                    *out_max_y = rect.max().y;
                }
            }
            SPATIO_OK
        }
        Ok(None) => {
            if !out_found.is_null() {
                unsafe { *out_found = false };
            }
            SPATIO_OK
        }
        Err(e) => unsafe { report(err, &e) },
    }
}

#[cfg(test)]
mod tests;
