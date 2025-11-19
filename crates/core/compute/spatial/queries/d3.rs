//! 3D spatial operations for altitude-aware geographic queries.

use crate::compute::spatial::rtree::{BBoxQuery, CylinderQuery};
use crate::compute::validation::validate_geographic_point_3d;
use crate::config::{BoundingBox3D, Point3d, SetOptions};
use crate::db::{DB, DBInner};
use crate::error::Result;
use bytes::Bytes;
use spatio_types::geo::Point as GeoPoint;

impl DB {
    /// Insert a 3D point with altitude into the database.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{Spatio, Point3d};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut db = Spatio::memory()?;
    /// let pos = Point3d::new(-74.0060, 40.7128, 100.0);
    /// db.insert_point_3d("drones", &pos, b"Drone-001", None)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn insert_point_3d(
        &mut self,
        prefix: &str,
        point: &Point3d,
        value: &[u8],
        opts: Option<SetOptions>,
    ) -> Result<()> {
        validate_geographic_point_3d(point)?;

        let data_ref = Bytes::copy_from_slice(value);
        let item = crate::config::DbItem::from_options(data_ref, opts.as_ref());
        let created_at = item.created_at;
        let data_for_index = item.value.clone();

        DBInner::validate_timestamp(created_at)?;
        let key =
            DBInner::generate_spatial_key(prefix, point.x(), point.y(), point.z(), created_at)?;
        let key_bytes = Bytes::copy_from_slice(key.as_bytes());

        self.inner.insert_item(key_bytes.clone(), item);

        self.inner.spatial_index.insert_point(
            prefix,
            point.x(),
            point.y(),
            point.z(),
            key.clone(),
            data_for_index,
        );

        self.inner
            .write_to_aof_if_needed(&key_bytes, value, opts.as_ref(), created_at)?;
        Ok(())
    }

    /// Query points within a 3D spherical radius (altitude-aware).
    ///
    /// Returns (point, data, distance) tuples sorted by distance.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{Spatio, Point3d};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut db = Spatio::memory()?;
    /// let pos = Point3d::new(-74.0060, 40.7128, 100.0);
    /// db.insert_point_3d("drones", &pos, b"Drone-1", None)?;
    ///
    /// let center = Point3d::new(-74.0065, 40.7133, 125.0);
    /// let nearby = db.query_within_sphere_3d("drones", &center, 500.0, 10)?;
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
        validate_geographic_point_3d(center)?;
        crate::compute::validation::validate_radius(radius)?;

        let results = self
            .inner
            .spatial_index
            .query_within_sphere(prefix, center, radius, limit);

        Ok(Self::results_to_point3d_with_distance(
            &self.inner,
            prefix,
            results,
        ))
    }

    /// Query points within a 3D bounding box.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{Spatio, Point3d, BoundingBox3D};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut db = Spatio::memory()?;
    /// let pos = Point3d::new(-74.005, 40.715, 100.0);
    /// db.insert_point_3d("drones", &pos, b"Drone-1", None)?;
    ///
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
        crate::compute::validation::validate_bbox_3d(
            bbox.min_x, bbox.min_y, bbox.min_z, bbox.max_x, bbox.max_y, bbox.max_z,
        )?;

        let results = self.inner.spatial_index.query_within_bbox(
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
        Ok(Self::results_to_point3d(
            &self.inner,
            prefix,
            limited_results,
        ))
    }

    /// Query points within a cylindrical volume (altitude-constrained radius).
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{Spatio, Point3d};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut db = Spatio::memory()?;
    /// let pos = Point3d::new(-74.0060, 40.7128, 5000.0);
    /// db.insert_point_3d("aircraft", &pos, b"Flight-1", None)?;
    ///
    /// let center = Point3d::new(-74.0065, 40.7133, 0.0);
    /// let results = db.query_within_cylinder_3d(
    ///     "aircraft",
    ///     &center,
    ///     3000.0,  // min altitude
    ///     7000.0,  // max altitude
    ///     10000.0, // horizontal radius
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
        let results = self.inner.spatial_index.query_within_cylinder(
            prefix,
            CylinderQuery {
                center: GeoPoint::new(center.x(), center.y()),
                min_z: min_altitude,
                max_z: max_altitude,
                radius: horizontal_radius,
            },
            limit,
        );

        Ok(Self::results_to_point3d_with_distance(
            &self.inner,
            prefix,
            results,
        ))
    }

    /// Find the k nearest neighbors in 3D space.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{Spatio, Point3d};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut db = Spatio::memory()?;
    /// db.insert_point_3d("drones", &Point3d::new(-74.00, 40.71, 100.0), b"D1", None)?;
    /// db.insert_point_3d("drones", &Point3d::new(-74.01, 40.72, 200.0), b"D2", None)?;
    ///
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
        let results = self.inner.spatial_index.knn_3d(prefix, point, k);

        Ok(Self::results_to_point3d_with_distance(
            &self.inner,
            prefix,
            results,
        ))
    }

    /// Calculate 3D distance between two points (meters).
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{Spatio, Point3d};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut db = Spatio::memory()?;
    /// let p1 = Point3d::new(-74.0060, 40.7128, 0.0);
    /// let p2 = Point3d::new(-74.0070, 40.7138, 100.0);
    /// let distance = db.distance_between_3d(&p1, &p2)?;
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
