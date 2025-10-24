//! Spatial index manager powering geospatial queries.
//!
//! This module maintains geohash-backed indexes used by the database to
//! execute nearby, bounds, and distance-based lookups efficiently.

use crate::error::Result;
use crate::spatial::Point;
use crate::types::Config;
use bytes::Bytes;
use geohash;
use rustc_hash::{FxHashMap, FxHashSet};
use std::cmp::Ordering;

/// Threshold for large search radius in meters
const LARGE_RADIUS_THRESHOLD: f64 = 100_000.0;

/// Threshold for small dataset size
const SMALL_DATASET_THRESHOLD: usize = 1000;

/// Default geohash precision for spatial indexing
pub const DEFAULT_GEOHASH_PRECISION: usize = 8;

/// Default geohash precisions for neighbor search
pub const DEFAULT_SEARCH_PRECISIONS: &[usize] = &[6, 7, 8];

/// Simplified index manager focused on spatial operations only.
///
/// This manages spatial indexes for efficient geographic queries.
/// It automatically handles geohash-based indexing for points.
pub struct IndexManager {
    /// Spatial indexes organized by prefix
    spatial_indexes: FxHashMap<String, SpatialIndex>,
    /// Geohash precisions to use for neighbor search
    search_precisions: Vec<usize>,
}

/// A spatial index for a specific prefix/namespace
struct SpatialIndex {
    /// Buckets of points grouped by geohash string
    buckets: FxHashMap<String, FxHashMap<Bytes, IndexedPoint>>,
    /// Total number of indexed points
    len: usize,
}

struct IndexedPoint {
    point: Point,
    data: Bytes,
}

impl IndexManager {
    /// Create a new index manager with default configuration
    pub fn new() -> Self {
        Self {
            spatial_indexes: FxHashMap::default(),
            search_precisions: DEFAULT_SEARCH_PRECISIONS.to_vec(),
        }
    }

    /// Create a new index manager with custom configuration
    pub fn with_config(config: &Config) -> Self {
        // Generate search precisions with proper bounds and deduplication
        let mut search_precisions = Vec::new();

        // Add precision-2 if valid and different
        let p_minus_2 = config.geohash_precision.saturating_sub(2).max(1);
        if p_minus_2 >= 1 {
            search_precisions.push(p_minus_2);
        }

        // Add precision-1 if valid and different from previous
        let p_minus_1 = config.geohash_precision.saturating_sub(1).max(1);
        if p_minus_1 >= 1 && !search_precisions.contains(&p_minus_1) {
            search_precisions.push(p_minus_1);
        }

        // Add the main precision if different from previous
        let main_precision = config.geohash_precision.min(12);
        if !search_precisions.contains(&main_precision) {
            search_precisions.push(main_precision);
        }

        // Ensure we have at least one precision
        if search_precisions.is_empty() {
            search_precisions.push(1);
        }

        Self {
            spatial_indexes: FxHashMap::default(),
            search_precisions,
        }
    }

    #[cfg(test)]
    fn primary_precision(&self) -> usize {
        *self
            .search_precisions
            .last()
            .unwrap_or(&DEFAULT_GEOHASH_PRECISION)
    }

    /// Helper method to determine if we should use full scan vs geohash optimization
    fn should_use_full_scan(&self, prefix: &str, radius_meters: f64) -> bool {
        let index = match self.spatial_indexes.get(prefix) {
            Some(index) => index,
            None => return true, // No index means no optimization possible
        };

        radius_meters > LARGE_RADIUS_THRESHOLD || index.len() < SMALL_DATASET_THRESHOLD
    }

    /// Insert a point into the spatial index
    pub fn insert_point(
        &mut self,
        prefix: &str,
        geohash: &str,
        key: &Bytes,
        point: &Point,
        data: &Bytes,
    ) -> Result<()> {
        let index = self
            .spatial_indexes
            .entry(prefix.to_string())
            .or_insert_with(SpatialIndex::new);

        index.insert(geohash, key, point, data);
        Ok(())
    }

    /// Find nearby points within a radius
    pub fn find_nearby(
        &self,
        prefix: &str,
        center: &Point,
        radius_meters: f64,
        limit: usize,
    ) -> Result<Vec<(Point, Bytes)>> {
        let index = match self.spatial_indexes.get(prefix) {
            Some(index) => index,
            None => return Ok(Vec::new()),
        };

        if limit == 0 {
            return Ok(Vec::new());
        }

        let mut results = if self.should_use_full_scan(prefix, radius_meters) {
            self.collect_full_scan(index, center, radius_meters, limit)
        } else {
            let candidates = self.collect_geohash_candidates(center);
            let matches = self.collect_geohash_matches(index, &candidates, center, radius_meters);

            if matches.is_empty() {
                self.collect_full_scan(index, center, radius_meters, limit)
            } else {
                self.dedupe_matches(matches, limit)
            }
        };

        self.sort_and_limit(&mut results, center, limit);
        Ok(results)
    }

    /// Find all points within a bounding box
    pub fn find_within_bounds(
        &self,
        prefix: &str,
        min_lat: f64,
        min_lon: f64,
        max_lat: f64,
        max_lon: f64,
        limit: usize,
    ) -> Result<Vec<(Point, Bytes)>> {
        let index = match self.spatial_indexes.get(prefix) {
            Some(index) => index,
            None => return Ok(Vec::new()),
        };

        let mut results = Vec::new();

        // Check all points in the index
        for entry in index.entries() {
            if entry
                .point
                .within_bounds(min_lat, min_lon, max_lat, max_lon)
            {
                results.push((entry.point, entry.data.clone()));
                if results.len() >= limit {
                    break;
                }
            }
        }

        Ok(results)
    }

    /// Check if there are any points within a circular region
    pub fn contains_point(&self, prefix: &str, center: &Point, radius_meters: f64) -> Result<bool> {
        let index = match self.spatial_indexes.get(prefix) {
            Some(index) => index,
            None => return Ok(false),
        };

        if self.should_use_full_scan(prefix, radius_meters) {
            return Ok(!self
                .collect_full_scan(index, center, radius_meters, 1)
                .is_empty());
        }

        let candidates = self.collect_geohash_candidates(center);
        let matches = self.collect_geohash_matches(index, &candidates, center, radius_meters);

        if matches.is_empty() {
            Ok(!self
                .collect_full_scan(index, center, radius_meters, 1)
                .is_empty())
        } else {
            Ok(true)
        }
    }

    /// Check if there are any points within a bounding box
    pub fn intersects_bounds(
        &self,
        prefix: &str,
        min_lat: f64,
        min_lon: f64,
        max_lat: f64,
        max_lon: f64,
    ) -> Result<bool> {
        let index = match self.spatial_indexes.get(prefix) {
            Some(index) => index,
            None => return Ok(false),
        };

        // Check if any point intersects with the bounding box
        for entry in index.entries() {
            if entry
                .point
                .within_bounds(min_lat, min_lon, max_lat, max_lon)
            {
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Count points within a distance from a center point
    pub fn count_within_distance(
        &self,
        prefix: &str,
        center: &Point,
        radius_meters: f64,
    ) -> Result<usize> {
        let index = match self.spatial_indexes.get(prefix) {
            Some(index) => index,
            None => return Ok(0),
        };

        if self.should_use_full_scan(prefix, radius_meters) {
            return Ok(self
                .collect_full_scan(index, center, radius_meters, usize::MAX)
                .len());
        }

        self.traverse_nearby(center, radius_meters, |_, _| true)
    }

    fn traverse_nearby<F>(&self, center: &Point, radius_meters: f64, mut visit: F) -> Result<usize>
    where
        F: FnMut(&Bytes, &Point) -> bool,
    {
        let candidates = self.collect_geohash_candidates(center);
        let mut count = 0;

        for index in self.spatial_indexes.values() {
            let matches = self.collect_geohash_matches(index, &candidates, center, radius_meters);
            for (_distance, point, data) in matches {
                if visit(&data, &point) {
                    count += 1;
                }
            }
        }

        Ok(count)
    }

    /// Remove a specific entry from the spatial index
    pub fn remove_entry(&mut self, prefix: &str, geohash: &str, key: &Bytes) -> Result<()> {
        if let Some(index) = self.spatial_indexes.get_mut(prefix) {
            let removed = index.remove(geohash, key);
            if removed && index.is_empty() {
                self.spatial_indexes.remove(prefix);
            }
        }
        Ok(())
    }

    fn collect_full_scan(
        &self,
        index: &SpatialIndex,
        center: &Point,
        radius_meters: f64,
        limit: usize,
    ) -> Vec<(Point, Bytes)> {
        if limit == 0 {
            return Vec::new();
        }

        let mut results = Vec::with_capacity(limit.min(1000));
        for entry in index.entries() {
            if results.len() >= limit {
                break;
            }

            if center.distance_to(&entry.point) <= radius_meters {
                results.push((entry.point, entry.data.clone()));
            }
        }
        results
    }

    fn collect_geohash_candidates(&self, center: &Point) -> FxHashSet<String> {
        let mut candidates = FxHashSet::default();
        candidates.reserve(self.search_precisions.len() * 9);

        for precision in &self.search_precisions {
            if let Ok(center_geohash) = center.to_geohash(*precision) {
                candidates.insert(center_geohash.clone());

                for direction in &[
                    geohash::Direction::N,
                    geohash::Direction::S,
                    geohash::Direction::E,
                    geohash::Direction::W,
                    geohash::Direction::NE,
                    geohash::Direction::NW,
                    geohash::Direction::SE,
                    geohash::Direction::SW,
                ] {
                    if let Ok(neighbor) = geohash::neighbor(&center_geohash, *direction) {
                        candidates.insert(neighbor);
                    }
                }
            }
        }

        candidates
    }

    fn collect_geohash_matches(
        &self,
        index: &SpatialIndex,
        candidates: &FxHashSet<String>,
        center: &Point,
        radius_meters: f64,
    ) -> Vec<(f64, Point, Bytes)> {
        let mut matches = Vec::new();

        for (stored_geohash, bucket) in &index.buckets {
            if !Self::geohash_matches_any(stored_geohash, candidates) {
                continue;
            }

            for entry in bucket.values() {
                let distance = center.distance_to(&entry.point);
                if distance <= radius_meters {
                    matches.push((distance, entry.point, entry.data.clone()));
                }
            }
        }

        matches
    }

    fn dedupe_matches(
        &self,
        mut matches: Vec<(f64, Point, Bytes)>,
        limit: usize,
    ) -> Vec<(Point, Bytes)> {
        if matches.is_empty() || limit == 0 {
            return Vec::new();
        }

        matches.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal));

        let mut seen_points = FxHashSet::default();
        let mut results = Vec::with_capacity(matches.len().min(limit));

        for (_, point, data) in matches {
            let point_key = (point.lat.to_bits(), point.lon.to_bits());
            if seen_points.insert(point_key) {
                results.push((point, data));
                if results.len() >= limit {
                    break;
                }
            }
        }

        results
    }

    fn sort_and_limit(&self, results: &mut Vec<(Point, Bytes)>, center: &Point, limit: usize) {
        if results.len() <= 1 {
            if results.len() > limit {
                results.truncate(limit);
            }
            return;
        }

        results.sort_by(|a, b| {
            let dist_a = center.distance_to(&a.0);
            let dist_b = center.distance_to(&b.0);
            dist_a.partial_cmp(&dist_b).unwrap_or(Ordering::Equal)
        });

        results.truncate(limit);
    }

    fn geohash_matches_any(stored_geohash: &str, candidates: &FxHashSet<String>) -> bool {
        candidates.iter().any(|candidate| {
            stored_geohash.starts_with(candidate.as_str()) || candidate.starts_with(stored_geohash)
        })
    }

    /// Get statistics about spatial indexes
    pub fn stats(&self) -> IndexStats {
        let mut total_points = 0;
        let index_count = self.spatial_indexes.len();

        for index in self.spatial_indexes.values() {
            total_points += index.len();
        }

        IndexStats {
            index_count,
            total_points,
        }
    }
}

impl SpatialIndex {
    fn new() -> Self {
        Self {
            buckets: FxHashMap::default(),
            len: 0,
        }
    }

    fn len(&self) -> usize {
        self.len
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn insert(&mut self, geohash: &str, key: &Bytes, point: &Point, data: &Bytes) {
        let bucket = self.buckets.entry(geohash.to_string()).or_default();

        let entry = IndexedPoint {
            point: *point,
            data: data.clone(),
        };

        if bucket.insert(key.clone(), entry).is_none() {
            self.len += 1;
        }
    }

    fn remove(&mut self, geohash: &str, key: &Bytes) -> bool {
        let mut removed = false;

        if let Some(bucket) = self.buckets.get_mut(geohash) {
            if bucket.remove(key).is_some() {
                self.len = self.len.saturating_sub(1);
                removed = true;
            }

            if bucket.is_empty() {
                self.buckets.remove(geohash);
            }
        }

        removed
    }

    fn entries(&self) -> impl Iterator<Item = &IndexedPoint> + '_ {
        self.buckets.values().flat_map(|bucket| bucket.values())
    }
}

/// Statistics about the index manager
#[derive(Debug)]
pub struct IndexStats {
    pub index_count: usize,
    pub total_points: usize,
}

impl Default for IndexManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spatial::{Point, SpatialKey};
    use bytes::Bytes;
    use rustc_hash::FxHashSet;
    use std::time::{Duration, SystemTime};

    #[test]
    fn test_default_geohash_precision() {
        let manager = IndexManager::new();
        assert_eq!(manager.primary_precision(), DEFAULT_GEOHASH_PRECISION);
        assert_eq!(manager.search_precisions, DEFAULT_SEARCH_PRECISIONS);
    }

    #[test]
    fn test_custom_geohash_precision() {
        let config = Config::with_geohash_precision(10);

        let manager = IndexManager::with_config(&config);
        assert_eq!(manager.primary_precision(), 10);
        assert_eq!(manager.search_precisions, vec![8, 9, 10]);
    }

    #[test]
    fn test_insert_and_remove_with_custom_precision() -> Result<()> {
        let config = Config::with_geohash_precision(6); // Lower precision for testing

        let mut manager = IndexManager::with_config(&config);
        let point = Point::new(40.7128, -74.0060);
        let data = Bytes::from("test_data");

        // Insert point
        let geohash = point.to_geohash(manager.primary_precision()).unwrap();
        let storage_key = Bytes::from(
            SpatialKey::geohash_unique("test", &geohash, &point, SystemTime::now()).into_bytes(),
        );

        manager.insert_point("test", &geohash, &storage_key, &point, &data)?;

        // Verify it exists
        let nearby = manager.find_nearby("test", &point, 1000.0, 10)?;
        assert_eq!(nearby.len(), 1);

        // Remove point
        manager.remove_entry("test", &geohash, &storage_key)?;

        // Verify it's gone
        let nearby_after = manager.find_nearby("test", &point, 1000.0, 10)?;
        assert_eq!(nearby_after.len(), 0);

        Ok(())
    }

    #[test]
    fn test_search_with_different_precisions() -> Result<()> {
        // Test with single precision
        let config1 = Config::with_geohash_precision(7);
        let mut manager1 = IndexManager::with_config(&config1);

        // Test with multiple precisions
        let config2 = Config::with_geohash_precision(8);
        let mut manager2 = IndexManager::with_config(&config2);

        let point = Point::new(40.7128, -74.0060);
        let data = Bytes::from("test_data");

        // Insert into both managers
        let geohash1 = point.to_geohash(manager1.primary_precision()).unwrap();
        let geohash2 = point.to_geohash(manager2.primary_precision()).unwrap();
        let storage_key1 = Bytes::from(
            SpatialKey::geohash_unique("test", &geohash1, &point, SystemTime::now()).into_bytes(),
        );
        let storage_key2 = Bytes::from(
            SpatialKey::geohash_unique(
                "test",
                &geohash2,
                &point,
                SystemTime::now() + Duration::from_nanos(1),
            )
            .into_bytes(),
        );

        manager1.insert_point("test", &geohash1, &storage_key1, &point, &data)?;
        manager2.insert_point("test", &geohash2, &storage_key2, &point, &data)?;

        // Both should find the point
        let results1 = manager1.find_nearby("test", &point, 1000.0, 10)?;
        let results2 = manager2.find_nearby("test", &point, 1000.0, 10)?;

        assert_eq!(results1.len(), 1);
        assert_eq!(results2.len(), 1);

        Ok(())
    }

    #[test]
    fn test_multiple_points_same_geohash() -> Result<()> {
        let config = Config::with_geohash_precision(6);
        let mut manager = IndexManager::with_config(&config);

        let point_a = Point::new(40.7128, -74.0060);
        let point_b = Point::new(40.7129, -74.0061);

        let geohash_a = point_a.to_geohash(config.geohash_precision).unwrap();
        let geohash_b = point_b.to_geohash(config.geohash_precision).unwrap();
        assert_eq!(geohash_a, geohash_b);

        let key_a = Bytes::from(
            SpatialKey::geohash_unique("test", &geohash_a, &point_a, SystemTime::now())
                .into_bytes(),
        );
        let key_b = Bytes::from(
            SpatialKey::geohash_unique(
                "test",
                &geohash_b,
                &point_b,
                SystemTime::now() + Duration::from_nanos(1),
            )
            .into_bytes(),
        );

        manager.insert_point(
            "test",
            &geohash_a,
            &key_a,
            &point_a,
            &Bytes::from_static(b"A"),
        )?;
        manager.insert_point(
            "test",
            &geohash_b,
            &key_b,
            &point_b,
            &Bytes::from_static(b"B"),
        )?;

        let nearby = manager.find_nearby("test", &point_a, 100.0, 10)?;
        assert_eq!(nearby.len(), 2);

        let values: FxHashSet<_> = nearby.into_iter().map(|(_, data)| data).collect();
        assert!(values.contains(&Bytes::from_static(b"A")));
        assert!(values.contains(&Bytes::from_static(b"B")));

        Ok(())
    }

    #[test]
    fn test_search_precisions_exact_values() {
        // Test precision 1: should be [1]
        let config = Config::with_geohash_precision(1);
        let manager = IndexManager::with_config(&config);
        assert_eq!(manager.search_precisions, vec![1]);

        // Test precision 2: should be [1, 2]
        let config = Config::with_geohash_precision(2);
        let manager = IndexManager::with_config(&config);
        assert_eq!(manager.search_precisions, vec![1, 2]);

        // Test precision 3: should be [1, 2, 3]
        let config = Config::with_geohash_precision(3);
        let manager = IndexManager::with_config(&config);
        assert_eq!(manager.search_precisions, vec![1, 2, 3]);

        // Test normal case: precision 8 should be [6, 7, 8]
        let config = Config::with_geohash_precision(8);
        let manager = IndexManager::with_config(&config);
        assert_eq!(manager.search_precisions, vec![6, 7, 8]);

        // Test high precision: 12 should be [10, 11, 12]
        let config = Config::with_geohash_precision(12);
        let manager = IndexManager::with_config(&config);
        assert_eq!(manager.search_precisions, vec![10, 11, 12]);
    }

    #[test]
    fn test_search_precisions_edge_cases() {
        // Test precision 1 (should not produce 0 or negative values)
        let config = Config::with_geohash_precision(1);
        let manager = IndexManager::with_config(&config);
        assert!(
            manager
                .search_precisions
                .iter()
                .all(|&p| (1..=12).contains(&p))
        );
        assert!(manager.search_precisions.contains(&1));

        // Test precision 2 (should not produce 0)
        let config = Config::with_geohash_precision(2);
        let manager = IndexManager::with_config(&config);
        assert!(
            manager
                .search_precisions
                .iter()
                .all(|&p| (1..=12).contains(&p))
        );
        assert!(manager.search_precisions.contains(&2));

        // Test precision 12 (upper bound)
        let config = Config::with_geohash_precision(12);
        let manager = IndexManager::with_config(&config);
        assert!(
            manager
                .search_precisions
                .iter()
                .all(|&p| (1..=12).contains(&p))
        );
        assert!(manager.search_precisions.contains(&12));

        // Test that search precisions are unique and sorted
        let config = Config::with_geohash_precision(5);
        let manager = IndexManager::with_config(&config);
        let mut sorted_precisions = manager.search_precisions.clone();
        sorted_precisions.sort();
        sorted_precisions.dedup();
        assert_eq!(manager.search_precisions.len(), sorted_precisions.len());

        // Test that main precision is always included
        for precision in 1..=12 {
            let config = Config::with_geohash_precision(precision);
            let manager = IndexManager::with_config(&config);
            assert!(manager.search_precisions.contains(&precision.min(12)));
        }
    }

    #[test]
    fn test_constants_are_reasonable() {
        assert!((1..=12).contains(&DEFAULT_GEOHASH_PRECISION));

        for &precision in DEFAULT_SEARCH_PRECISIONS {
            assert!((1..=12).contains(&precision));
        }

        assert!(
            DEFAULT_SEARCH_PRECISIONS.contains(&DEFAULT_GEOHASH_PRECISION)
                || DEFAULT_SEARCH_PRECISIONS
                    .iter()
                    .any(|&p| (p as i32 - DEFAULT_GEOHASH_PRECISION as i32).abs() <= 1)
        );
    }
}
