use crate::error::{Result, SpatioError};
use crate::spatial::Point;
use crate::types::Config;
use bytes::Bytes;
use geohash;
use rustc_hash::{FxHashMap, FxHashSet};

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
    /// Geohash precision for indexing
    geohash_precision: usize,
    /// Geohash precisions to use for neighbor search
    search_precisions: Vec<usize>,
}

/// A spatial index for a specific prefix/namespace
struct SpatialIndex {
    /// Points stored with their geohash keys
    points: FxHashMap<String, (Point, Bytes)>,
}

impl IndexManager {
    /// Create a new index manager with default configuration
    pub fn new() -> Self {
        Self {
            spatial_indexes: FxHashMap::default(),
            geohash_precision: DEFAULT_GEOHASH_PRECISION,
            search_precisions: DEFAULT_SEARCH_PRECISIONS.to_vec(),
        }
    }

    /// Create a new index manager with custom configuration
    pub fn with_config(config: &Config) -> Self {
        Self {
            spatial_indexes: FxHashMap::default(),
            geohash_precision: config.geohash_precision,
            search_precisions: config.geohash_search_precisions.clone(),
        }
    }

    /// Helper method to determine if we should use full scan vs geohash optimization
    fn should_use_full_scan(&self, prefix: &str, radius_meters: f64) -> bool {
        let index = match self.spatial_indexes.get(prefix) {
            Some(index) => index,
            None => return true, // No index means no optimization possible
        };

        radius_meters > LARGE_RADIUS_THRESHOLD || index.points.len() < SMALL_DATASET_THRESHOLD
    }

    /// Insert a point into the spatial index
    pub fn insert_point(&mut self, prefix: &str, point: &Point, data: &Bytes) -> Result<()> {
        let index = self
            .spatial_indexes
            .entry(prefix.to_string())
            .or_insert_with(SpatialIndex::new);

        let geohash = point
            .to_geohash(self.geohash_precision)
            .map_err(|_| SpatioError::InvalidGeohash)?;

        index.points.insert(geohash, (*point, data.clone()));
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

        let mut results = Vec::with_capacity(limit.min(1000));

        // For large search radii or small datasets, use full scan instead of geohash optimization
        if self.should_use_full_scan(prefix, radius_meters) {
            // Check all points in the index
            for (point, data) in index.points.values() {
                if results.len() >= limit {
                    break;
                }
                let distance = center.distance_to(point);
                if distance <= radius_meters {
                    results.push((*point, data.clone()));
                }
            }
        } else {
            // Use geohash-based search for efficiency
            let mut candidates = FxHashSet::default();
            candidates.reserve(27); // 9 directions * 3 precisions

            // Try multiple precision levels for better coverage
            for precision in &self.search_precisions {
                if let Ok(center_geohash) = center.to_geohash(*precision) {
                    candidates.insert(center_geohash.clone());

                    // Add neighbors at this precision
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

            // Collect candidates with distances for sorting
            let mut candidates_with_distance = Vec::new();

            // Check all candidate geohashes
            for geohash in &candidates {
                // Check if any point starts with this geohash prefix
                for (stored_geohash, (point, data)) in &index.points {
                    if stored_geohash.starts_with(geohash) || geohash.starts_with(stored_geohash) {
                        let distance = center.distance_to(point);
                        if distance <= radius_meters {
                            candidates_with_distance.push((distance, *point, data.clone()));
                        }
                    }
                }
            }

            // Sort by distance and take closest results, naturally handling duplicates
            candidates_with_distance
                .sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));

            // Take unique points (deduplicate by point coordinates) up to limit
            let mut seen_points = FxHashSet::default();
            for (_, point, data) in candidates_with_distance {
                let point_key = (point.lat.to_bits(), point.lon.to_bits());
                if seen_points.insert(point_key) {
                    results.push((point, data));
                    if results.len() >= limit {
                        break;
                    }
                }
            }

            // If we didn't find enough results, fall back to full scan
            if results.is_empty() {
                for (point, data) in index.points.values() {
                    let distance = center.distance_to(point);
                    if distance <= radius_meters {
                        results.push((*point, data.clone()));
                    }
                }
            }
        }

        // Sort by distance and limit results
        results.sort_by(|a, b| {
            let dist_a = center.distance_to(&a.0);
            let dist_b = center.distance_to(&b.0);
            dist_a
                .partial_cmp(&dist_b)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        results.truncate(limit);
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
        for (point, data) in index.points.values() {
            if point.within_bounds(min_lat, min_lon, max_lat, max_lon) {
                results.push((*point, data.clone()));
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

        // For small datasets or large radii, just check all points
        if self.should_use_full_scan(prefix, radius_meters) {
            for (point, _) in index.points.values() {
                if center.distance_to(point) <= radius_meters {
                    return Ok(true);
                }
            }
            return Ok(false);
        }

        // Use geohash-based search for efficiency
        let mut candidates = std::collections::HashSet::new();

        // Try multiple precision levels for better coverage
        for precision in &self.search_precisions {
            if let Ok(center_geohash) = center.to_geohash(*precision) {
                candidates.insert(center_geohash.clone());

                // Add neighbors at this precision
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

        // Check all candidate geohashes
        for geohash in candidates {
            // Check if any point starts with this geohash prefix
            for (stored_geohash, (point, _)) in &index.points {
                if (stored_geohash.starts_with(&geohash) || geohash.starts_with(stored_geohash))
                    && center.distance_to(point) <= radius_meters
                {
                    return Ok(true);
                }
            }
        }

        // If geohash search didn't find anything, fall back to full scan
        for (point, _) in index.points.values() {
            if center.distance_to(point) <= radius_meters {
                return Ok(true);
            }
        }

        Ok(false)
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
        for (point, _) in index.points.values() {
            if point.within_bounds(min_lat, min_lon, max_lat, max_lon) {
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

        let mut count = 0;

        // For small datasets or large radii, just check all points
        if self.should_use_full_scan(prefix, radius_meters) {
            for (point, _) in index.points.values() {
                if center.distance_to(point) <= radius_meters {
                    count += 1;
                }
            }
            return Ok(count);
        }

        // Use geohash-based search for efficiency
        let mut candidates = std::collections::HashSet::new();

        // Try multiple precision levels for better coverage
        for precision in &self.search_precisions {
            if let Ok(center_geohash) = center.to_geohash(*precision) {
                candidates.insert(center_geohash.clone());

                // Add neighbors at this precision
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

        // Check all candidate geohashes
        let mut found_points = std::collections::HashSet::new();
        for geohash in candidates {
            // Check if any point starts with this geohash prefix
            for (stored_geohash, (point, _)) in &index.points {
                if (stored_geohash.starts_with(&geohash) || geohash.starts_with(stored_geohash))
                    && center.distance_to(point) <= radius_meters
                {
                    // Use point coordinates as key to avoid double counting
                    found_points.insert((point.lat.to_bits(), point.lon.to_bits()));
                }
            }
        }

        count = found_points.len();

        // If geohash search didn't find anything, fall back to full scan
        if count == 0 {
            for (point, _) in index.points.values() {
                if center.distance_to(point) <= radius_meters {
                    count += 1;
                }
            }
        }

        Ok(count)
    }

    /// Remove a point from the spatial index
    pub fn remove_point(&mut self, prefix: &str, point: &Point) -> Result<()> {
        if let Some(index) = self.spatial_indexes.get_mut(prefix) {
            let geohash = point
                .to_geohash(self.geohash_precision)
                .map_err(|_| SpatioError::InvalidGeohash)?;
            index.points.remove(&geohash);
        }
        Ok(())
    }

    /// Get statistics about spatial indexes
    pub fn stats(&self) -> IndexStats {
        let mut total_points = 0;
        let index_count = self.spatial_indexes.len();

        for index in self.spatial_indexes.values() {
            total_points += index.points.len();
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
            points: FxHashMap::default(),
        }
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
    use crate::spatial::Point;
    use bytes::Bytes;

    #[test]
    fn test_default_geohash_precision() {
        let manager = IndexManager::new();
        assert_eq!(manager.geohash_precision, DEFAULT_GEOHASH_PRECISION);
        assert_eq!(manager.search_precisions, DEFAULT_SEARCH_PRECISIONS);
    }

    #[test]
    fn test_custom_geohash_precision() {
        let config = Config {
            geohash_precision: 10,
            geohash_search_precisions: vec![8, 9, 10],
            ..Default::default()
        };

        let manager = IndexManager::with_config(&config);
        assert_eq!(manager.geohash_precision, 10);
        assert_eq!(manager.search_precisions, vec![8, 9, 10]);
    }

    #[test]
    fn test_insert_and_remove_with_custom_precision() -> Result<()> {
        let config = Config {
            geohash_precision: 6, // Lower precision for testing
            ..Default::default()
        };

        let mut manager = IndexManager::with_config(&config);
        let point = Point::new(40.7128, -74.0060);
        let data = Bytes::from("test_data");

        // Insert point
        manager.insert_point("test", &point, &data)?;

        // Verify it exists
        let nearby = manager.find_nearby("test", &point, 1000.0, 10)?;
        assert_eq!(nearby.len(), 1);

        // Remove point
        manager.remove_point("test", &point)?;

        // Verify it's gone
        let nearby_after = manager.find_nearby("test", &point, 1000.0, 10)?;
        assert_eq!(nearby_after.len(), 0);

        Ok(())
    }

    #[test]
    fn test_search_with_different_precisions() -> Result<()> {
        // Test with single precision
        let config1 = Config {
            geohash_search_precisions: vec![7],
            ..Default::default()
        };
        let mut manager1 = IndexManager::with_config(&config1);

        // Test with multiple precisions
        let config2 = Config {
            geohash_search_precisions: vec![6, 7, 8, 9],
            ..Default::default()
        };
        let mut manager2 = IndexManager::with_config(&config2);

        let point = Point::new(40.7128, -74.0060);
        let data = Bytes::from("test_data");

        // Insert into both managers
        manager1.insert_point("test", &point, &data)?;
        manager2.insert_point("test", &point, &data)?;

        // Both should find the point
        let results1 = manager1.find_nearby("test", &point, 1000.0, 10)?;
        let results2 = manager2.find_nearby("test", &point, 1000.0, 10)?;

        assert_eq!(results1.len(), 1);
        assert_eq!(results2.len(), 1);

        Ok(())
    }

    #[test]
    fn test_constants_are_reasonable() {
        // Ensure constants are within valid geohash precision range
        assert!((1..=12).contains(&DEFAULT_GEOHASH_PRECISION));

        for &precision in DEFAULT_SEARCH_PRECISIONS {
            assert!((1..=12).contains(&precision));
        }

        // Ensure search precisions include the default precision or are around it
        assert!(
            DEFAULT_SEARCH_PRECISIONS.contains(&DEFAULT_GEOHASH_PRECISION)
                || DEFAULT_SEARCH_PRECISIONS
                    .iter()
                    .any(|&p| (p as i32 - DEFAULT_GEOHASH_PRECISION as i32).abs() <= 1)
        );
    }
}
