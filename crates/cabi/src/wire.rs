//! Binary wire format for result sets crossing the C ABI.
//!
//! Results are packed little-endian into a single owned buffer to avoid the
//! per-record float formatting, envelope overhead, and reflection that a JSON
//! boundary incurs. The Go side reads the buffer directly with `unsafe.Slice`.
//!
//! ## Layout
//! ```text
//! u32  count
//! count × record
//! ```
//! Field encodings:
//! - `f64`   little-endian IEEE-754
//! - `str`   u32 length, then UTF-8 bytes
//! - `bytes` u32 length, then raw bytes (metadata: JSON, or empty for null)
//!
//! Record shapes:
//! - **neighbor**   `x y z timestamp distance` (5×f64), `id:str`, `meta:bytes`
//! - **location**   `x y z timestamp` (4×f64), `id:str`, `meta:bytes`
//! - **trajectory** `x y z timestamp` (4×f64), `meta:bytes`
//!
//! `get` reuses the **location** shape with a count of 0 or 1.

use spatio::db::{CurrentLocation, LocationUpdate};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Layout version, exported as `spatio_wire_version`.
pub const WIRE_VERSION: u32 = 2;

fn unix_secs(t: SystemTime) -> f64 {
    t.duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

/// Little-endian buffer builder.
struct Writer {
    buf: Vec<u8>,
}

impl Writer {
    fn with_count(count: usize) -> Self {
        let mut buf = Vec::with_capacity(4 + count * 48);
        buf.extend_from_slice(&(count as u32).to_le_bytes());
        Writer { buf }
    }

    #[inline]
    fn f64(&mut self, v: f64) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    #[inline]
    fn bytes(&mut self, b: &[u8]) {
        self.buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
        self.buf.extend_from_slice(b);
    }

    /// Metadata as JSON written in place; `Null` collapses to an empty blob.
    fn meta(&mut self, v: &serde_json::Value) {
        let at = self.buf.len();
        self.buf.extend_from_slice(&0u32.to_le_bytes());
        if !v.is_null() && serde_json::to_writer(&mut self.buf, v).is_err() {
            self.buf.truncate(at + 4);
        }
        let len = (self.buf.len() - at - 4) as u32;
        self.buf[at..at + 4].copy_from_slice(&len.to_le_bytes());
    }

    #[inline]
    fn str(&mut self, s: &str) {
        self.bytes(s.as_bytes());
    }

    fn into_box(self) -> Box<[u8]> {
        self.buf.into_boxed_slice()
    }
}

#[inline]
fn write_location(w: &mut Writer, loc: &CurrentLocation) {
    w.f64(loc.position.x());
    w.f64(loc.position.y());
    w.f64(loc.position.z());
    w.f64(unix_secs(loc.timestamp));
    w.str(&loc.object_id);
    w.meta(&loc.metadata);
}

/// Encode `(location, distance)` results (radius/knn/cylinder queries).
pub fn encode_neighbors(results: &[(Arc<CurrentLocation>, f64)]) -> Box<[u8]> {
    let mut w = Writer::with_count(results.len());
    for (loc, dist) in results {
        w.f64(loc.position.x());
        w.f64(loc.position.y());
        w.f64(loc.position.z());
        w.f64(unix_secs(loc.timestamp));
        w.f64(*dist);
        w.str(&loc.object_id);
        w.meta(&loc.metadata);
    }
    w.into_box()
}

/// Encode location-only results (bbox/polygon queries).
pub fn encode_locations(results: &[Arc<CurrentLocation>]) -> Box<[u8]> {
    let mut w = Writer::with_count(results.len());
    for loc in results {
        write_location(&mut w, loc);
    }
    w.into_box()
}

/// Encode a single optional location (`get`), as 0 or 1 location records.
pub fn encode_location_opt(loc: Option<&CurrentLocation>) -> Box<[u8]> {
    let mut w = Writer::with_count(loc.is_some() as usize);
    if let Some(loc) = loc {
        write_location(&mut w, loc);
    }
    w.into_box()
}

/// Encode historical trajectory samples.
pub fn encode_trajectory(updates: &[LocationUpdate]) -> Box<[u8]> {
    let mut w = Writer::with_count(updates.len());
    for u in updates {
        w.f64(u.position.x());
        w.f64(u.position.y());
        w.f64(u.position.z());
        w.f64(unix_secs(u.timestamp));
        w.meta(&u.metadata);
    }
    w.into_box()
}
