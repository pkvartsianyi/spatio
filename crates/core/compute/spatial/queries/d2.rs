//! 2D spatial operations for geographic queries.

use crate::compute::spatial::{DistanceMetric, distance_between, point_in_polygon};
use crate::compute::validation::validate_geographic_point;
use crate::config::{BoundingBox2D, SetOptions};
use crate::db::{DB, DBInner};
use crate::error::{Result, SpatioError};
use bytes::Bytes;
use spatio_types::geo::{Point, Polygon};
use std::cmp::Ordering;

impl DB {
    /// Insert a geographic point with automatic spatial indexing.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{Spatio, Point};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut db = Spatio::memory()?;
    /// let nyc = Point::new(-74.0060, 40.7128);
    ///
    /// db.insert_point("cities", &nyc, b"New York City", None)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn insert_point(
        &mut self,
        prefix: &str,
        point: &Point,
        value: &[u8],
        opts: Option<SetOptions>,
    ) -> Result<()> {
        // Validate geographic coordinates first
        validate_geographic_point(point)?;

        let data_ref = Bytes::copy_from_slice(value);
        let item = crate::config::DbItem::from_options(data_ref, opts.as_ref());
        let created_at = item.created_at;

        // Clone data only once for spatial index
        let data_for_index = item.value.clone();

        DBInner::validate_timestamp(created_at)?;
        let key = DBInner::generate_spatial_key(prefix, point.x(), point.y(), 0.0, created_at)?;
        let key_bytes = Bytes::copy_from_slice(key.as_bytes());

        self.inner.insert_item(key_bytes.clone(), item);

        self.inner.spatial_index.insert_point_2d(
            prefix,
            point.x(),
            point.y(),
            key.clone(),
            data_for_index,
        );

        self.inner
            .write_to_aof_if_needed(&key_bytes, value, opts.as_ref(), created_at)?;
        Ok(())
    }

    /// Insert or update a geographic point using a stable object ID.
    ///
    /// Unlike `insert_point`, this uses a deterministic key based on `object_id`,
    /// so updates replace previous positions. Ideal for tracking current state
    /// of moving objects without accumulating historical data.
    ///
    /// Returns the previous data if the object existed.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{Spatio, Point};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut db = Spatio::memory()?;
    ///
    /// let pos = Point::new(-74.0060, 40.7128);
    /// db.upsert_point("drones", "drone_123", &pos, b"active", None)?;
    ///
    /// let new_pos = Point::new(-74.0070, 40.7138);
    /// let old = db.upsert_point("drones", "drone_123", &new_pos, b"active", None)?;
    /// assert!(old.is_some());
    /// # Ok(())
    /// # }
    /// ```
    pub fn upsert_point(
        &mut self,
        prefix: &str,
        object_id: &str,
        point: &Point,
        value: &[u8],
        opts: Option<SetOptions>,
    ) -> Result<Option<Bytes>> {
        // Validate geographic coordinates first
        validate_geographic_point(point)?;

        let data_ref = Bytes::copy_from_slice(value);
        let item = crate::config::DbItem::from_options(data_ref, opts.as_ref());
        let created_at = item.created_at;

        // Clone data only once for spatial index
        let data_for_index = item.value.clone();

        let key = format!("{}:{}", prefix, object_id);
        let key_bytes = Bytes::copy_from_slice(key.as_bytes());

        // Remove old spatial index entry if exists
        if let Some(old_item) = self.inner.keys.get(&key_bytes)
            && !old_item.is_expired()
        {
            let _ = self.inner.spatial_index.remove_entry(prefix, &key);
        }

        // Insert/update main storage
        let old_value = self.inner.insert_item(key_bytes.clone(), item);

        // Insert into spatial index
        self.inner.spatial_index.insert_point_2d(
            prefix,
            point.x(),
            point.y(),
            key.clone(),
            data_for_index,
        );

        self.inner
            .write_to_aof_if_needed(&key_bytes, value, opts.as_ref(), created_at)?;

        Ok(old_value.map(|item| item.value))
    }

    /// Find nearby points within a radius.
    ///
    /// Uses spatial indexing for efficient queries. Results are ordered
    /// by distance from the query point.
    ///
    /// # Arguments
    ///
    /// * `prefix` - Namespace to search in
    /// * `center` - Center point for the search
    /// * `radius_meters` - Search radius in meters
    /// * `limit` - Maximum number of results to return
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{Spatio, Point};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut db = Spatio::memory()?;
    /// let center = Point::new(-74.0060, 40.7128);
    ///
    /// // Find up to 10 points within 1km
    /// let nearby = db.query_within_radius("cities", &center, 1000.0, 10)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn query_within_radius(
        &self,
        prefix: &str,
        center: &Point,
        radius_meters: f64,
        limit: usize,
    ) -> Result<Vec<(Point, Bytes)>> {
        validate_geographic_point(center)?;
        crate::compute::validation::validate_radius(radius_meters)?;

        let results = self.inner.spatial_index.query_within_radius_2d(
            prefix,
            center.x(),
            center.y(),
            radius_meters,
            limit,
        );

        let points: Vec<(Point, Bytes)> = results
            .into_iter()
            .map(|(x, y, _key, data, _distance)| (Point::new(x, y), data))
            .collect();

        Ok(points)
    }

    /// Check if any points exist within a circular radius.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{Spatio, Point};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut db = Spatio::memory()?;
    /// let center = Point::new(-74.0060, 40.7128);
    /// let has_nearby = db.contains_point("cities", &center, 50_000.0)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn contains_point(&self, prefix: &str, center: &Point, radius_meters: f64) -> Result<bool> {
        validate_geographic_point(center)?;
        crate::compute::validation::validate_radius(radius_meters)?;

        Ok(self.inner.spatial_index.contains_point_2d(
            prefix,
            center.x(),
            center.y(),
            radius_meters,
        ))
    }

    /// Check if any points exist within a bounding box.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{Spatio, Point};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut db = Spatio::memory()?;
    /// let has_points = db.intersects_bounds("sensors", 40.7, -74.1, 40.8, -73.9)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn intersects_bounds(
        &self,
        prefix: &str,
        min_lat: f64,
        min_lon: f64,
        max_lat: f64,
        max_lon: f64,
    ) -> Result<bool> {
        crate::compute::validation::validate_bbox(min_lon, min_lat, max_lon, max_lat)?;

        let results = self
            .inner
            .spatial_index
            .query_within_bbox_2d(prefix, min_lon, min_lat, max_lon, max_lat);
        Ok(!results.is_empty())
    }

    /// Count points within a circular radius.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{Spatio, Point};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut db = Spatio::memory()?;
    /// let center = Point::new(-74.0060, 40.7128);
    /// let count = db.count_within_radius("sensors", &center, 1000.0)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn count_within_radius(
        &self,
        prefix: &str,
        center: &Point,
        radius_meters: f64,
    ) -> Result<usize> {
        validate_geographic_point(center)?;
        crate::compute::validation::validate_radius(radius_meters)?;

        Ok(self.inner.spatial_index.count_within_radius_2d(
            prefix,
            center.x(),
            center.y(),
            radius_meters,
        ))
    }

    /// Find all points within a bounding box.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{Spatio, Point};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut db = Spatio::memory()?;
    /// let points = db.find_within_bounds("sensors", 40.7, -74.1, 40.8, -73.9, 100)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn find_within_bounds(
        &self,
        prefix: &str,
        min_lat: f64,
        min_lon: f64,
        max_lat: f64,
        max_lon: f64,
        limit: usize,
    ) -> Result<Vec<(Point, Bytes)>> {
        let results = self
            .inner
            .spatial_index
            .query_within_bbox_2d(prefix, min_lon, min_lat, max_lon, max_lat);

        let mut points = Vec::new();
        for (key, data) in results.into_iter().take(limit) {
            if let Some(tree) = self.inner.spatial_index.indexes.get(prefix)
                && let Some(indexed_point) = tree.iter().find(|p| p.key == key)
            {
                let point = Point::new(indexed_point.x, indexed_point.y);
                points.push((point, data));
            }
        }
        Ok(points)
    }

    /// Calculate distance between two points (meters).
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{Spatio, Point, DistanceMetric};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut db = Spatio::memory()?;
    /// let nyc = Point::new(-74.0060, 40.7128);
    /// let la = Point::new(-118.2437, 34.0522);
    /// let distance = db.distance_between(&nyc, &la, DistanceMetric::Haversine)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn distance_between(
        &self,
        point1: &Point,
        point2: &Point,
        metric: DistanceMetric,
    ) -> Result<f64> {
        Ok(distance_between(point1, point2, metric))
    }

    /// Find the K nearest points to a query point within a namespace.
    ///
    /// This performs a K-nearest-neighbor search using the spatial index.
    /// It first queries a radius, then refines to the K nearest points.
    ///
    /// # Arguments
    ///
    /// * `prefix` - Namespace to search in
    /// * `center` - Query point
    /// * `k` - Number of nearest neighbors to return
    /// * `max_radius` - Maximum search radius in meters
    /// * `metric` - Distance metric to use
    ///
    /// # Returns
    ///
    /// Vector of (Point, Bytes, distance) tuples sorted by distance (nearest first)
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{Spatio, Point, DistanceMetric};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut db = Spatio::memory()?;
    ///
    /// let nyc = Point::new(-74.0060, 40.7128);
    /// db.insert_point("cities", &nyc, b"New York", None)?;
    ///
    /// let query = Point::new(-74.0, 40.7);
    /// let nearest = db.knn("cities", &query, 5, 100_000.0, DistanceMetric::Haversine)?;
    ///
    /// for (point, data, distance) in nearest {
    ///     println!("Found city at {}m", distance);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn knn(
        &self,
        prefix: &str,
        center: &Point,
        k: usize,
        max_radius: f64,
        metric: DistanceMetric,
    ) -> Result<Vec<(Point, Bytes, f64)>> {
        let results = self.inner.spatial_index.knn_2d_with_max_distance(
            prefix,
            center.x(),
            center.y(),
            k * 2,
            Some(max_radius),
        );

        let mut candidates: Vec<_> = results
            .into_iter()
            .map(|(x, y, _key, data, _)| {
                let point = Point::new(x, y);
                let dist = distance_between(center, &point, metric);
                (point, data, dist)
            })
            .collect();

        candidates.sort_by(|a, b| a.2.partial_cmp(&b.2).unwrap_or(Ordering::Equal));
        candidates.truncate(k);
        Ok(candidates)
    }

    /// Query points within a polygon boundary.
    ///
    /// This finds all points that are contained within the given polygon.
    /// It uses the polygon's bounding box for initial filtering via the
    /// spatial index, then performs precise point-in-polygon tests.
    ///
    /// # Arguments
    ///
    /// * `prefix` - Namespace to search in
    /// * `polygon` - The polygon boundary
    /// * `limit` - Maximum number of results to return
    ///
    /// # Returns
    ///
    /// Vector of (Point, Bytes) tuples for points within the polygon
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{Spatio, Point, Polygon};
    /// use geo::polygon;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut db = Spatio::memory()?;
    ///
    /// let poly = polygon![
    ///     (x: -74.0, y: 40.7),
    ///     (x: -73.9, y: 40.7),
    ///     (x: -73.9, y: 40.8),
    ///     (x: -74.0, y: 40.8),
    /// ];
    /// let poly: Polygon = poly.into();
    ///
    /// let results = db.query_within_polygon("cities", &poly, 100)?;
    /// println!("Found {} cities in polygon", results.len());
    /// # Ok(())
    /// # }
    /// ```
    pub fn query_within_polygon(
        &self,
        prefix: &str,
        polygon: &Polygon,
        limit: usize,
    ) -> Result<Vec<(Point, Bytes)>> {
        use geo::BoundingRect;

        // Validate polygon coordinates
        crate::compute::validation::validate_polygon(polygon)?;

        let bbox = polygon
            .inner()
            .bounding_rect()
            .ok_or_else(|| SpatioError::InvalidInput("Polygon has no bounding box".to_string()))?;

        let candidates = self.find_within_bounds(
            prefix,
            bbox.min().y,
            bbox.min().x,
            bbox.max().y,
            bbox.max().x,
            usize::MAX,
        )?;

        let mut results = Vec::new();
        for (point, data) in candidates {
            if point_in_polygon(polygon, &point) {
                results.push((point, data));
                if results.len() >= limit {
                    break;
                }
            }
        }

        Ok(results)
    }

    /// Query all points within a bounding box.
    ///
    /// Returns all spatial points that fall within the specified 2D bounding box,
    /// ordered by their distance from the box's center.
    ///
    /// # Arguments
    ///
    /// * `prefix` - Optional key prefix to filter results
    /// * `bbox` - The bounding box to search within
    /// * `limit` - Maximum number of results to return
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{Spatio, Point, BoundingBox2D};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut db = Spatio::memory()?;
    ///
    /// // Store some points
    /// db.insert_point("poi", &Point::new(-73.9855, 40.7580), b"times_square", None)?;
    /// db.insert_point("poi", &Point::new(-73.9665, 40.7829), b"central_park", None)?;
    /// db.insert_point("poi", &Point::new(-73.9442, 40.6782), b"brooklyn", None)?;
    ///
    /// // Query points in Manhattan
    /// let manhattan = BoundingBox2D::new(-74.0479, 40.6829, -73.9067, 40.8820);
    /// let results = db.query_within_bbox("poi", &manhattan, 100)?;
    ///
    /// println!("Found {} points in Manhattan", results.len());
    /// # Ok(())
    /// # }
    /// ```
    pub fn query_within_bbox(
        &self,
        prefix: &str,
        bbox: &BoundingBox2D,
        limit: usize,
    ) -> Result<Vec<(Point, Bytes)>> {
        let center = bbox.center();

        let dx = bbox.width() / 2.0;
        let dy = bbox.height() / 2.0;
        let radius_deg = (dx * dx + dy * dy).sqrt();
        let radius_meters = radius_deg * 111_000.0;

        let candidates = self.query_within_radius(prefix, &center, radius_meters, limit * 2)?;

        let mut results = Vec::new();
        for (point, data) in candidates {
            if bbox.contains_point(&point) {
                results.push((point, data));
                if results.len() >= limit {
                    break;
                }
            }
        }

        Ok(results)
    }

    /// Store a bounding box with a key.
    ///
    /// Serializes and stores a bounding box, making it retrievable later.
    /// Useful for storing geographic regions, service areas, or zones.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to store the bounding box under
    /// * `bbox` - The bounding box to store
    /// * `opts` - Optional settings like TTL
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{Spatio, BoundingBox2D};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut db = Spatio::memory()?;
    ///
    /// let manhattan = BoundingBox2D::new(-74.0479, 40.6829, -73.9067, 40.8820);
    /// db.insert_bbox("zones:manhattan", &manhattan, None)?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub fn insert_bbox(
        &mut self,
        key: impl AsRef<[u8]>,
        bbox: &BoundingBox2D,
        opts: Option<SetOptions>,
    ) -> Result<()> {
        let serialized = bincode::serialize(bbox)
            .map_err(|e| SpatioError::SerializationErrorWithContext(e.to_string()))?;
        self.insert(key, serialized, opts)?;
        Ok(())
    }

    /// Retrieve a bounding box by key.
    ///
    /// Deserializes and returns a previously stored bounding box.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to retrieve
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{Spatio, BoundingBox2D};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut db = Spatio::memory()?;
    ///
    /// let manhattan = BoundingBox2D::new(-74.0479, 40.6829, -73.9067, 40.8820);
    /// db.insert_bbox("zones:manhattan", &manhattan, None)?;
    ///
    /// if let Some(bbox) = db.get_bbox("zones:manhattan")? {
    ///     println!("Manhattan area: {}°×{}°", bbox.width(), bbox.height());
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn get_bbox(&self, key: impl AsRef<[u8]>) -> Result<Option<BoundingBox2D>> {
        match self.get(key)? {
            Some(data) => {
                let bbox = bincode::deserialize(&data)
                    .map_err(|e| SpatioError::SerializationErrorWithContext(e.to_string()))?;
                Ok(Some(bbox))
            }
            None => Ok(None),
        }
    }

    /// Find all bounding boxes that intersect with a given bounding box.
    ///
    /// Returns all stored bounding boxes (with the specified prefix) that
    /// intersect with the query bounding box.
    ///
    /// # Arguments
    ///
    /// * `prefix` - Key prefix to filter results (e.g., "zones:")
    /// * `bbox` - The bounding box to check for intersections
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{Spatio, BoundingBox2D};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut db = Spatio::memory()?;
    ///
    /// db.insert_bbox("zones:manhattan", &BoundingBox2D::new(-74.0479, 40.6829, -73.9067, 40.8820), None)?;
    /// db.insert_bbox("zones:brooklyn", &BoundingBox2D::new(-74.0421, 40.5707, -73.8333, 40.7395), None)?;
    ///
    /// let query = BoundingBox2D::new(-74.01, 40.70, -73.95, 40.75);
    /// let intersecting = db.find_intersecting_bboxes("zones:", &query)?;
    ///
    /// println!("Found {} intersecting zones", intersecting.len());
    /// # Ok(())
    /// # }
    /// ```
    pub fn find_intersecting_bboxes(
        &self,
        prefix: &str,
        bbox: &BoundingBox2D,
    ) -> Result<Vec<(String, BoundingBox2D)>> {
        let prefix_bytes = Bytes::from(prefix.to_owned());
        let mut results = Vec::new();

        for (key, item) in self.inner.keys.range(prefix_bytes.clone()..) {
            if !key.starts_with(prefix.as_bytes()) {
                break;
            }

            if item.is_expired() {
                continue;
            }

            if let Ok(stored_bbox) = bincode::deserialize::<BoundingBox2D>(&item.value)
                && stored_bbox.intersects(bbox)
            {
                let key_str = String::from_utf8_lossy(key).to_string();
                results.push((key_str, stored_bbox));
            }
        }

        Ok(results)
    }
}
