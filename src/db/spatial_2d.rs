//! 2D spatial operations for geographic queries.

use super::{DB, DBInner};
use crate::config::{BoundingBox2D, SetOptions};
use crate::error::{Result, SpatioError};
use bytes::Bytes;
use geo::Point;

impl DB {
    /// Insert a geographic point with automatic spatial indexing.
    ///
    /// Points are automatically indexed for spatial queries. The system
    /// chooses the optimal indexing strategy based on data patterns.
    ///
    /// # Arguments
    ///
    /// * `prefix` - Namespace for the point (e.g., "cities", "sensors")
    /// * `point` - Geographic coordinates
    /// * `data` - Associated data to store with the point
    /// * `opts` - Optional settings like TTL
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{Spatio, Point};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let db = Spatio::memory()?;
    /// let nyc = Point::new(-74.0060, 40.7128);
    ///
    /// db.insert_point("cities", &nyc, b"New York City", None)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn insert_point(
        &self,
        prefix: &str,
        point: &Point,
        value: &[u8],
        opts: Option<SetOptions>,
    ) -> Result<()> {
        let data_ref = Bytes::copy_from_slice(value);

        let mut inner = self.write()?;

        let item = match opts {
            Some(SetOptions { ttl: Some(ttl), .. }) => {
                crate::config::DbItem::with_ttl(data_ref.clone(), ttl)
            }
            Some(SetOptions {
                expires_at: Some(expires_at),
                ..
            }) => crate::config::DbItem::with_expiration(data_ref.clone(), expires_at),
            _ => crate::config::DbItem::new(data_ref.clone()),
        };
        let created_at = item.created_at;

        DBInner::validate_timestamp(created_at)?;
        let key = DBInner::generate_spatial_key(prefix, point.x(), point.y(), 0.0, created_at)?;
        let key_bytes = Bytes::copy_from_slice(key.as_bytes());

        inner.insert_item(key_bytes.clone(), item);

        inner.spatial_index.insert_point_2d(
            prefix,
            point.x(),
            point.y(),
            key.clone(),
            data_ref.clone(),
        );

        inner.write_to_aof_if_needed(&key_bytes, value, opts.as_ref(), created_at)?;
        Ok(())
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
    /// let db = Spatio::memory()?;
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
        let inner = self.read()?;

        let results = inner.spatial_index.query_within_radius_2d(
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

    /// Check if there are any points within a circular region.
    ///
    /// This method checks if any points exist within the specified distance
    /// from a center point in the given namespace.
    ///
    /// # Arguments
    ///
    /// * `prefix` - Namespace to search in
    /// * `center` - Center point of the circular region
    /// * `radius_meters` - Radius in meters
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{Spatio, Point};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let db = Spatio::memory()?;
    /// let center = Point::new(-74.0060, 40.7128);
    ///
    /// // Check if there are any cities within 50km
    /// let has_nearby = db.contains_point("cities", &center, 50_000.0)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn contains_point(&self, prefix: &str, center: &Point, radius_meters: f64) -> Result<bool> {
        let inner = self.read()?;
        Ok(inner
            .spatial_index
            .contains_point_2d(prefix, center.x(), center.y(), radius_meters))
    }

    /// Check if there are any points within a bounding box.
    ///
    /// This method checks if any points exist within the specified
    /// rectangular region in the given namespace.
    ///
    /// # Arguments
    ///
    /// * `prefix` - Namespace to search in
    /// * `min_lat` - Minimum latitude of bounding box
    /// * `min_lon` - Minimum longitude of bounding box
    /// * `max_lat` - Maximum latitude of bounding box
    /// * `max_lon` - Maximum longitude of bounding box
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{Spatio, Point};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let db = Spatio::memory()?;
    ///
    /// // Check if there are any points in Manhattan area
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
        let inner = self.read()?;
        let results = inner
            .spatial_index
            .query_within_bbox_2d(prefix, min_lon, min_lat, max_lon, max_lat);
        Ok(!results.is_empty())
    }

    /// Count points within a distance from a center point.
    ///
    /// This method counts how many points exist within the specified
    /// distance from a center point without returning the actual points.
    /// More efficient than `query_within_radius` when you only need the count.
    ///
    /// # Arguments
    ///
    /// * `prefix` - Namespace to search in
    /// * `center` - Center point for the search
    /// * `radius_meters` - Search radius in meters
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{Spatio, Point};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let db = Spatio::memory()?;
    /// let center = Point::new(-74.0060, 40.7128);
    ///
    /// // Count how many sensors are within 1km
    /// let count = db.count_within_radius("sensors", &center, 1000.0)?;
    /// println!("Found {} sensors within 1km", count);
    /// # Ok(())
    /// # }
    /// ```
    pub fn count_within_radius(
        &self,
        prefix: &str,
        center: &Point,
        radius_meters: f64,
    ) -> Result<usize> {
        let inner = self.read()?;
        Ok(inner.spatial_index.count_within_radius_2d(
            prefix,
            center.x(),
            center.y(),
            radius_meters,
        ))
    }

    /// Find all points within a bounding box.
    ///
    /// This method returns all points that fall within the specified
    /// rectangular region, up to the specified limit.
    ///
    /// # Arguments
    ///
    /// * `prefix` - Namespace to search in
    /// * `min_lat` - Minimum latitude of bounding box
    /// * `min_lon` - Minimum longitude of bounding box
    /// * `max_lat` - Maximum latitude of bounding box
    /// * `max_lon` - Maximum longitude of bounding box
    /// * `limit` - Maximum number of results to return
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{Spatio, Point};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let db = Spatio::memory()?;
    ///
    /// // Find all sensors in Manhattan area
    /// let points = db.find_within_bounds("sensors", 40.7, -74.1, 40.8, -73.9, 100)?;
    /// println!("Found {} sensors in Manhattan", points.len());
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
        let inner = self.read()?;
        let results = inner
            .spatial_index
            .query_within_bbox_2d(prefix, min_lon, min_lat, max_lon, max_lat);

        let mut points = Vec::new();
        for (key, data) in results.into_iter().take(limit) {
            if let Some(tree) = inner.spatial_index.indexes.get(prefix)
                && let Some(indexed_point) = tree.iter().find(|p| p.key == key)
            {
                let point = Point::new(indexed_point.x, indexed_point.y);
                points.push((point, data));
            }
        }
        Ok(points)
    }

    /// Calculate the distance between two points using a specified metric.
    ///
    /// This is a convenience method that wraps geo crate distance calculations.
    /// For most lon/lat use cases, Haversine is recommended.
    ///
    /// # Arguments
    ///
    /// * `point1` - First point
    /// * `point2` - Second point
    /// * `metric` - Distance metric (Haversine, Geodesic, Rhumb, or Euclidean)
    ///
    /// # Returns
    ///
    /// Distance in meters
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{Spatio, Point, spatial::DistanceMetric};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let db = Spatio::memory()?;
    ///
    /// let nyc = Point::new(-74.0060, 40.7128);
    /// let la = Point::new(-118.2437, 34.0522);
    ///
    /// let distance = db.distance_between(&nyc, &la, DistanceMetric::Haversine)?;
    /// println!("Distance: {} meters", distance);
    /// # Ok(())
    /// # }
    /// ```
    pub fn distance_between(
        &self,
        point1: &Point,
        point2: &Point,
        metric: crate::spatial::DistanceMetric,
    ) -> Result<f64> {
        Ok(crate::spatial::distance_between(point1, point2, metric))
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
    /// use spatio::{Spatio, Point, spatial::DistanceMetric};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let db = Spatio::memory()?;
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
        metric: crate::spatial::DistanceMetric,
    ) -> Result<Vec<(Point, Bytes, f64)>> {
        let inner = self.read()?;

        let results = inner.spatial_index.knn_2d_with_max_distance(
            prefix,
            center.x(),
            center.y(),
            k,
            Some(max_radius),
        );

        let mut filtered: Vec<(Point, Bytes, f64)> = results
            .into_iter()
            .map(|(x, y, _key, data, dist)| (Point::new(x, y), data, dist))
            .collect();

        if metric != crate::spatial::DistanceMetric::Haversine {
            for (point, _, dist) in &mut filtered {
                *dist = crate::spatial::distance_between(center, point, metric);
            }
            filtered.sort_by(|a, b| a.2.partial_cmp(&b.2).unwrap_or(std::cmp::Ordering::Equal));
            filtered.truncate(k);
        }

        Ok(filtered)
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
    /// use spatio::{Spatio, Point};
    /// use geo::{polygon, Polygon};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let db = Spatio::memory()?;
    ///
    /// let poly: Polygon = polygon![
    ///     (x: -74.0, y: 40.7),
    ///     (x: -73.9, y: 40.7),
    ///     (x: -73.9, y: 40.8),
    ///     (x: -74.0, y: 40.8),
    /// ];
    ///
    /// let results = db.query_within_polygon("cities", &poly, 100)?;
    /// println!("Found {} cities in polygon", results.len());
    /// # Ok(())
    /// # }
    /// ```
    pub fn query_within_polygon(
        &self,
        prefix: &str,
        polygon: &geo::Polygon,
        limit: usize,
    ) -> Result<Vec<(Point, Bytes)>> {
        use geo::BoundingRect;

        let bbox = polygon
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
            if crate::spatial::point_in_polygon(polygon, &point) {
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
    /// let db = Spatio::memory()?;
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
    /// let db = Spatio::memory()?;
    ///
    /// let manhattan = BoundingBox2D::new(-74.0479, 40.6829, -73.9067, 40.8820);
    /// db.insert_bbox("zones:manhattan", &manhattan, None)?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub fn insert_bbox(
        &self,
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
    /// let db = Spatio::memory()?;
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
    /// let db = Spatio::memory()?;
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
        let inner = self.read()?;
        let prefix_bytes = Bytes::from(prefix.to_owned());
        let mut results = Vec::new();

        for (key, item) in inner.keys.range(prefix_bytes.clone()..) {
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
