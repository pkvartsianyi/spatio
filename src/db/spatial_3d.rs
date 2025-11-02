//! 3D spatial operations for altitude-aware geographic queries.

use super::{DB, DBInner};
use crate::config::{BoundingBox3D, Point3d, SetOptions};
use crate::error::Result;
use crate::spatial_index::{BBoxQuery, CylinderQuery};
use bytes::Bytes;

impl DB {
    /// Insert a 3D point (with altitude) into the database.
    ///
    /// This method stores a 3D geographic point with altitude/elevation information
    /// and automatically adds it to the 3D spatial index for altitude-aware queries.
    ///
    /// # Arguments
    ///
    /// * `prefix` - The prefix/namespace for organizing related points
    /// * `point` - The 3D point with x, y, z coordinates
    /// * `value` - The data to associate with this point
    /// * `options` - Optional TTL and other storage options
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{Spatio, Point3d};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let db = Spatio::memory()?;
    ///
    /// // Track a drone at 100 meters altitude
    /// let drone_pos = Point3d::new(-74.0060, 40.7128, 100.0);
    /// db.insert_point_3d("drones", &drone_pos, b"Drone-001", None)?;
    ///
    /// // Track an aircraft at 10,000 meters
    /// let aircraft_pos = Point3d::new(-74.0070, 40.7138, 10000.0);
    /// db.insert_point_3d("aircraft", &aircraft_pos, b"Flight-AA123", None)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn insert_point_3d(
        &self,
        prefix: &str,
        point: &Point3d,
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
        let key =
            DBInner::generate_spatial_key(prefix, point.x(), point.y(), point.z(), created_at)?;
        let key_bytes = Bytes::copy_from_slice(key.as_bytes());

        inner.insert_item(key_bytes.clone(), item);

        inner.spatial_index.insert_point(
            prefix,
            point.x(),
            point.y(),
            point.z(),
            key.clone(),
            data_ref.clone(),
        );

        inner.write_to_aof_if_needed(&key_bytes, value, opts.as_ref(), created_at)?;
        Ok(())
    }

    /// Query points within a 3D spherical radius.
    ///
    /// Finds all points within a spherical distance from the center point,
    /// taking altitude differences into account using 3D distance calculation.
    ///
    /// # Arguments
    ///
    /// * `prefix` - The prefix/namespace to search
    /// * `center` - The center point for the search
    /// * `radius` - Radius in meters (3D distance)
    /// * `limit` - Maximum number of results to return
    ///
    /// # Returns
    ///
    /// Vector of (point, data, distance) tuples sorted by distance.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{Spatio, Point3d};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let db = Spatio::memory()?;
    ///
    /// let drone1 = Point3d::new(-74.0060, 40.7128, 100.0);
    /// let drone2 = Point3d::new(-74.0070, 40.7138, 150.0);
    ///
    /// db.insert_point_3d("drones", &drone1, b"Drone-1", None)?;
    /// db.insert_point_3d("drones", &drone2, b"Drone-2", None)?;
    ///
    /// // Find drones within 500m radius (3D)
    /// let center = Point3d::new(-74.0065, 40.7133, 125.0);
    /// let nearby = db.query_within_sphere_3d("drones", &center, 500.0, 10)?;
    ///
    /// for (point, data, distance) in nearby {
    ///     println!("Found drone at {}m distance", distance);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn query_within_sphere_3d(
        &self,
        prefix: &str,
        center: &Point3d,
        radius: f64,
        limit: usize,
    ) -> Result<Vec<(Point3d, Bytes, f64)>> {
        let inner = self.read()?;

        let results = inner.spatial_index.query_within_sphere(
            prefix,
            center.x(),
            center.y(),
            center.z(),
            radius,
            limit,
        );

        Ok(Self::results_to_point3d_with_distance(
            &inner, prefix, results,
        ))
    }

    /// Query points within a 3D bounding box.
    ///
    /// # Arguments
    ///
    /// * `prefix` - The prefix/namespace to search
    /// * `bbox` - The 3D bounding box to search within
    /// * `limit` - Maximum number of results to return
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{Spatio, Point3d, BoundingBox3D};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let db = Spatio::memory()?;
    ///
    /// let drone = Point3d::new(-74.0060, 40.7128, 100.0);
    /// db.insert_point_3d("drones", &drone, b"Drone-1", None)?;
    ///
    /// // Search in a 3D box
    /// let bbox = BoundingBox3D::new(-74.01, 40.71, 50.0, -74.00, 40.72, 150.0);
    /// let results = db.query_within_bbox_3d("drones", &bbox, 100)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn query_within_bbox_3d(
        &self,
        prefix: &str,
        bbox: &BoundingBox3D,
        limit: usize,
    ) -> Result<Vec<(Point3d, Bytes)>> {
        let inner = self.read()?;

        let results = inner.spatial_index.query_within_bbox(
            prefix,
            BBoxQuery {
                min_x: bbox.min_x,
                min_y: bbox.min_y,
                min_z: bbox.min_z,
                max_x: bbox.max_x,
                max_y: bbox.max_y,
                max_z: bbox.max_z,
            },
        );

        let limited_results: Vec<(String, Bytes)> = results.into_iter().take(limit).collect();
        Ok(Self::results_to_point3d(&inner, prefix, limited_results))
    }

    /// Query points within a cylindrical volume.
    ///
    /// This is useful for altitude-constrained radius queries, such as finding
    /// all aircraft within a certain horizontal distance and altitude range.
    ///
    /// # Arguments
    ///
    /// * `prefix` - The prefix/namespace to search
    /// * `center` - The center point (only x, y used for horizontal center)
    /// * `min_altitude` - Minimum altitude/z coordinate
    /// * `max_altitude` - Maximum altitude/z coordinate
    /// * `horizontal_radius` - Horizontal radius in meters
    /// * `limit` - Maximum number of results
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{Spatio, Point3d};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let db = Spatio::memory()?;
    ///
    /// let aircraft1 = Point3d::new(-74.0060, 40.7128, 5000.0);
    /// let aircraft2 = Point3d::new(-74.0070, 40.7138, 10000.0);
    ///
    /// db.insert_point_3d("aircraft", &aircraft1, b"Flight-1", None)?;
    /// db.insert_point_3d("aircraft", &aircraft2, b"Flight-2", None)?;
    ///
    /// // Find aircraft between 3000m and 7000m altitude within 10km horizontal
    /// let center = Point3d::new(-74.0065, 40.7133, 0.0);
    /// let results = db.query_within_cylinder_3d(
    ///     "aircraft",
    ///     &center,
    ///     3000.0,
    ///     7000.0,
    ///     10000.0,
    ///     100
    /// )?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn query_within_cylinder_3d(
        &self,
        prefix: &str,
        center: &Point3d,
        min_altitude: f64,
        max_altitude: f64,
        horizontal_radius: f64,
        limit: usize,
    ) -> Result<Vec<(Point3d, Bytes, f64)>> {
        let inner = self.read()?;

        let results = inner.spatial_index.query_within_cylinder(
            prefix,
            CylinderQuery {
                center_x: center.x(),
                center_y: center.y(),
                min_z: min_altitude,
                max_z: max_altitude,
                radius: horizontal_radius,
            },
            limit,
        );

        Ok(Self::results_to_point3d_with_distance(
            &inner, prefix, results,
        ))
    }

    /// Find the k nearest neighbors in 3D space.
    ///
    /// # Arguments
    ///
    /// * `prefix` - The prefix/namespace to search
    /// * `point` - The query point
    /// * `k` - Number of nearest neighbors to find
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{Spatio, Point3d};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let db = Spatio::memory()?;
    ///
    /// db.insert_point_3d("drones", &Point3d::new(-74.00, 40.71, 100.0), b"D1", None)?;
    /// db.insert_point_3d("drones", &Point3d::new(-74.01, 40.72, 200.0), b"D2", None)?;
    /// db.insert_point_3d("drones", &Point3d::new(-74.02, 40.73, 300.0), b"D3", None)?;
    ///
    /// // Find 2 nearest drones in 3D space
    /// let query = Point3d::new(-74.005, 40.715, 150.0);
    /// let nearest = db.knn_3d("drones", &query, 2)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn knn_3d(
        &self,
        prefix: &str,
        point: &Point3d,
        k: usize,
    ) -> Result<Vec<(Point3d, Bytes, f64)>> {
        let inner = self.read()?;

        let results = inner
            .spatial_index
            .knn_3d(prefix, point.x(), point.y(), point.z(), k);

        Ok(Self::results_to_point3d_with_distance(
            &inner, prefix, results,
        ))
    }

    /// Calculate the 3D distance between two points.
    ///
    /// Uses haversine formula for horizontal distance and incorporates
    /// altitude difference using the Pythagorean theorem.
    ///
    /// # Returns
    ///
    /// Distance in meters.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{Spatio, Point3d};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let db = Spatio::memory()?;
    ///
    /// let p1 = Point3d::new(-74.0060, 40.7128, 0.0);
    /// let p2 = Point3d::new(-74.0070, 40.7138, 100.0);
    ///
    /// let distance = db.distance_between_3d(&p1, &p2)?;
    /// println!("3D distance: {} meters", distance);
    /// # Ok(())
    /// # }
    /// ```
    pub fn distance_between_3d(&self, p1: &Point3d, p2: &Point3d) -> Result<f64> {
        Ok(p1.haversine_3d(p2))
    }

    /// Helper to convert spatial index results to Point3d with data and distance
    pub(super) fn results_to_point3d_with_distance(
        inner: &DBInner,
        prefix: &str,
        results: Vec<(String, Bytes, f64)>,
    ) -> Vec<(Point3d, Bytes, f64)> {
        results
            .into_iter()
            .filter_map(|(key, data, distance)| {
                inner
                    .spatial_index
                    .indexes
                    .get(prefix)?
                    .iter()
                    .find(|p| p.key == key)
                    .map(|indexed_point| {
                        (
                            Point3d::new(indexed_point.x, indexed_point.y, indexed_point.z),
                            data,
                            distance,
                        )
                    })
            })
            .collect()
    }

    /// Helper to convert spatial index results to Point3d with data (no distance)
    pub(super) fn results_to_point3d(
        inner: &DBInner,
        prefix: &str,
        results: Vec<(String, Bytes)>,
    ) -> Vec<(Point3d, Bytes)> {
        results
            .into_iter()
            .filter_map(|(key, data)| {
                inner
                    .spatial_index
                    .indexes
                    .get(prefix)?
                    .iter()
                    .find(|p| p.key == key)
                    .map(|indexed_point| {
                        (
                            Point3d::new(indexed_point.x, indexed_point.y, indexed_point.z),
                            data,
                        )
                    })
            })
            .collect()
    }
}
