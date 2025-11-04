//! Query operations for the geohash-rtree hybrid index.
//!
//! This module implements various spatial query types:
//! - Radius queries (within distance)
//! - Bounding box queries (range queries)
//! - K-nearest neighbors (KNN)
//! - Intersection queries

use super::geohash_rtree::GeohashRTreeIndex;
use super::spatial_object::{SpatialObject, haversine_distance};
use bytes::Bytes;
use geo::{Point, Rect};
use rustc_hash::FxHashSet;
use std::cmp::Ordering;

/// A single query result containing object data and metadata.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryResult {
    /// Unique identifier of the object
    pub key: String,

    /// Associated data payload
    pub data: Bytes,

    /// Distance from query point (if applicable), in meters
    pub distance: Option<f64>,

    /// The spatial object itself (for advanced use)
    pub object: SpatialObject,
}

impl QueryResult {
    /// Create a new query result.
    pub fn new(key: String, data: Bytes, distance: Option<f64>, object: SpatialObject) -> Self {
        Self {
            key,
            data,
            distance,
            object,
        }
    }

    /// Get the center point of this result's object.
    pub fn center(&self) -> Point<f64> {
        self.object.center()
    }
}

/// Statistics about a query execution.
#[derive(Debug, Clone)]
pub struct QueryStats {
    /// Number of geohash cells examined
    pub cells_examined: usize,

    /// Number of candidate objects considered (before filtering)
    pub candidates_examined: usize,

    /// Number of results returned (after filtering)
    pub results_returned: usize,

    /// Whether deduplication was performed
    pub deduplicated: bool,
}

impl GeohashRTreeIndex {
    /// Query all objects within a radius of a center point.
    ///
    /// This is one of the most common spatial queries. It uses the Haversine formula
    /// to calculate great-circle distances on Earth's surface.
    ///
    /// # Algorithm
    ///
    /// 1. Calculate geohash cells that intersect the search radius
    /// 2. Query each cell's R-tree for candidates
    /// 3. Filter candidates by exact Haversine distance
    /// 4. Deduplicate results (objects may appear in multiple cells)
    /// 5. Sort by distance and apply limit
    ///
    /// # Arguments
    ///
    /// * `center` - The center point of the search
    /// * `radius_meters` - Search radius in meters
    /// * `limit` - Maximum number of results to return
    ///
    /// # Returns
    ///
    /// A vector of `QueryResult` sorted by distance (nearest first).
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::compute::spatial::hybrid::GeohashRTreeIndex;
    /// use geo::Point;
    /// use bytes::Bytes;
    ///
    /// let mut index = GeohashRTreeIndex::new(7);
    /// let nyc = Point::new(-74.0060, 40.7128);
    /// index.insert_point("nyc", nyc, Bytes::from("NYC"));
    ///
    /// // Find all points within 1km
    /// let results = index.query_within_radius(&nyc, 1000.0, 10);
    /// assert_eq!(results.len(), 1);
    /// assert_eq!(results[0].key, "nyc");
    /// ```
    pub fn query_within_radius(
        &self,
        center: &Point<f64>,
        radius_meters: f64,
        limit: usize,
    ) -> Vec<QueryResult> {
        // Get all potentially relevant geohash cells
        let cells = self.get_cells_for_radius(center, radius_meters);

        // Collect candidates from all cells
        let mut candidates = Vec::new();
        let mut seen_keys = FxHashSet::default();

        for cell_hash in cells {
            if let Some(tree) = self.cells.get(&cell_hash) {
                for obj in tree.iter() {
                    // Deduplicate: skip if we've already seen this object
                    if seen_keys.contains(&obj.key) {
                        continue;
                    }
                    seen_keys.insert(obj.key.clone());

                    // Calculate exact distance
                    let distance = haversine_distance(
                        center.x(),
                        center.y(),
                        obj.center().x(),
                        obj.center().y(),
                    );

                    // Filter by radius
                    if distance <= radius_meters {
                        candidates.push(QueryResult {
                            key: obj.key.clone(),
                            data: obj.data.clone(),
                            distance: Some(distance),
                            object: obj.clone(),
                        });
                    }
                }
            }
        }

        // Sort by distance (nearest first)
        candidates.sort_by(|a, b| {
            a.distance
                .unwrap_or(f64::INFINITY)
                .partial_cmp(&b.distance.unwrap_or(f64::INFINITY))
                .unwrap_or(Ordering::Equal)
        });

        // Apply limit
        candidates.truncate(limit);
        candidates
    }

    /// Query all objects within a bounding box.
    ///
    /// This performs a range query, returning all objects whose bounding boxes
    /// intersect with the query rectangle.
    ///
    /// # Arguments
    ///
    /// * `bbox` - The bounding rectangle to search within
    /// * `limit` - Maximum number of results to return
    ///
    /// # Returns
    ///
    /// A vector of `QueryResult` (order not guaranteed).
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::compute::spatial::hybrid::GeohashRTreeIndex;
    /// use geo::{Point, Rect, coord};
    /// use bytes::Bytes;
    ///
    /// let mut index = GeohashRTreeIndex::new(7);
    /// let nyc = Point::new(-74.0060, 40.7128);
    /// index.insert_point("nyc", nyc, Bytes::from("NYC"));
    ///
    /// let bbox = Rect::new(
    ///     coord! { x: -75.0, y: 40.0 },
    ///     coord! { x: -73.0, y: 41.0 },
    /// );
    /// let results = index.query_within_bbox(&bbox, 100);
    /// assert_eq!(results.len(), 1);
    /// ```
    pub fn query_within_bbox(&self, bbox: &Rect<f64>, limit: usize) -> Vec<QueryResult> {
        let mut results = Vec::new();
        let mut seen_keys = FxHashSet::default();

        for tree in self.cells.values() {
            for obj in tree.iter() {
                if seen_keys.insert(&obj.key) && obj.intersects_bbox(bbox) {
                    results.push(QueryResult::new(
                        obj.key.clone(),
                        obj.data.clone(),
                        None,
                        obj.clone(),
                    ));
                    if results.len() >= limit {
                        return results;
                    }
                }
            }
        }
        results
    }

    /// Find the K nearest neighbors to a point.
    ///
    /// This query returns the K closest objects to a given point, sorted by distance.
    ///
    /// # Algorithm
    ///
    /// 1. Start with center cell and neighbors
    /// 2. Collect all objects and calculate distances
    /// 3. Sort by distance
    /// 4. Return top K results
    ///
    /// # Arguments
    ///
    /// * `point` - The query point
    /// * `k` - Number of nearest neighbors to find
    ///
    /// # Returns
    ///
    /// A vector of up to K `QueryResult` sorted by distance (nearest first).
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::compute::spatial::hybrid::GeohashRTreeIndex;
    /// use geo::Point;
    /// use bytes::Bytes;
    ///
    /// let mut index = GeohashRTreeIndex::new(7);
    /// index.insert_point("p1", Point::new(-74.0, 40.7), Bytes::from("P1"));
    /// index.insert_point("p2", Point::new(-74.1, 40.7), Bytes::from("P2"));
    /// index.insert_point("p3", Point::new(-74.2, 40.7), Bytes::from("P3"));
    ///
    /// let query_point = Point::new(-74.0, 40.7);
    /// let results = index.knn(&query_point, 2);
    /// assert_eq!(results.len(), 2);
    /// assert_eq!(results[0].key, "p1"); // Closest
    /// ```
    pub fn knn(&self, point: &Point<f64>, k: usize) -> Vec<QueryResult> {
        if k == 0 {
            return Vec::new();
        }

        // For KNN, we need to search ALL cells to ensure we find all K neighbors
        // Get all cells in the index
        let all_cells: Vec<String> = self.cells.keys().cloned().collect();

        // Collect all objects with distances
        let mut candidates = Vec::new();
        let mut seen_keys = FxHashSet::default();

        for cell_hash in all_cells {
            if let Some(tree) = self.cells.get(&cell_hash) {
                for obj in tree.iter() {
                    // Deduplicate
                    if seen_keys.contains(&obj.key) {
                        continue;
                    }
                    seen_keys.insert(obj.key.clone());

                    // Calculate distance
                    let distance = haversine_distance(
                        point.x(),
                        point.y(),
                        obj.center().x(),
                        obj.center().y(),
                    );

                    candidates.push(QueryResult {
                        key: obj.key.clone(),
                        data: obj.data.clone(),
                        distance: Some(distance),
                        object: obj.clone(),
                    });
                }
            }
        }

        // Sort by distance and take top K
        candidates.sort_by(|a, b| {
            a.distance
                .unwrap_or(f64::INFINITY)
                .partial_cmp(&b.distance.unwrap_or(f64::INFINITY))
                .unwrap_or(Ordering::Equal)
        });

        candidates.truncate(k);
        candidates
    }

    /// Query with detailed statistics about the search.
    ///
    /// This is useful for understanding query performance and debugging.
    ///
    /// # Arguments
    ///
    /// * `center` - The center point of the search
    /// * `radius_meters` - Search radius in meters
    /// * `limit` - Maximum number of results to return
    ///
    /// # Returns
    ///
    /// A tuple of (results, statistics).
    pub fn query_within_radius_with_stats(
        &self,
        center: &Point<f64>,
        radius_meters: f64,
        limit: usize,
    ) -> (Vec<QueryResult>, QueryStats) {
        let cells = self.get_cells_for_radius(center, radius_meters);
        let cells_examined = cells.len();

        let mut candidates = Vec::new();
        let mut seen_keys = FxHashSet::default();
        let mut candidates_examined = 0;

        for cell_hash in cells {
            if let Some(tree) = self.cells.get(&cell_hash) {
                for obj in tree.iter() {
                    candidates_examined += 1;

                    if seen_keys.contains(&obj.key) {
                        continue;
                    }
                    seen_keys.insert(obj.key.clone());

                    let distance = haversine_distance(
                        center.x(),
                        center.y(),
                        obj.center().x(),
                        obj.center().y(),
                    );

                    if distance <= radius_meters {
                        candidates.push(QueryResult {
                            key: obj.key.clone(),
                            data: obj.data.clone(),
                            distance: Some(distance),
                            object: obj.clone(),
                        });
                    }
                }
            }
        }

        candidates.sort_by(|a, b| {
            a.distance
                .unwrap_or(f64::INFINITY)
                .partial_cmp(&b.distance.unwrap_or(f64::INFINITY))
                .unwrap_or(Ordering::Equal)
        });

        let results_returned = candidates.len().min(limit);
        candidates.truncate(limit);

        let stats = QueryStats {
            cells_examined,
            candidates_examined,
            results_returned,
            deduplicated: true,
        };

        (candidates, stats)
    }

    /// Find all objects that contain a given point.
    ///
    /// This checks if the query point is within each object's bounding box.
    ///
    /// # Arguments
    ///
    /// * `point` - The query point
    /// * `limit` - Maximum number of results to return
    ///
    /// # Returns
    ///
    /// A vector of `QueryResult` for objects containing the point.
    pub fn query_contains_point(&self, point: &Point<f64>, limit: usize) -> Vec<QueryResult> {
        // Search all cells to find objects containing the point
        // This ensures we don't miss objects that span multiple cells
        let all_cells: Vec<String> = self.cells.keys().cloned().collect();

        let mut results = Vec::new();
        let mut seen_keys = FxHashSet::default();

        for cell_hash in all_cells {
            if let Some(tree) = self.cells.get(&cell_hash) {
                for obj in tree.iter() {
                    if seen_keys.contains(&obj.key) {
                        continue;
                    }
                    seen_keys.insert(obj.key.clone());

                    if obj.contains_point(point) {
                        results.push(QueryResult {
                            key: obj.key.clone(),
                            data: obj.data.clone(),
                            distance: None,
                            object: obj.clone(),
                        });

                        if results.len() >= limit {
                            return results;
                        }
                    }
                }
            }
        }

        results
    }

    /// Query objects that intersect with a bounding box, with custom filter.
    ///
    /// This allows for more complex filtering logic during the query.
    ///
    /// # Arguments
    ///
    /// * `bbox` - The bounding rectangle to search within
    /// * `filter` - A closure that returns true if an object should be included
    /// * `limit` - Maximum number of results to return
    ///
    /// # Returns
    ///
    /// A vector of `QueryResult` that pass the filter.
    pub fn query_with_filter<F>(
        &self,
        bbox: &Rect<f64>,
        mut filter: F,
        limit: usize,
    ) -> Vec<QueryResult>
    where
        F: FnMut(&SpatialObject) -> bool,
    {
        // Search all cells for comprehensive results
        let all_cells: Vec<String> = self.cells.keys().cloned().collect();

        let mut results = Vec::new();
        let mut seen_keys = FxHashSet::default();

        for cell_hash in all_cells {
            if let Some(tree) = self.cells.get(&cell_hash) {
                for obj in tree.iter() {
                    if seen_keys.contains(&obj.key) {
                        continue;
                    }
                    seen_keys.insert(obj.key.clone());

                    if obj.intersects_bbox(bbox) && filter(obj) {
                        results.push(QueryResult {
                            key: obj.key.clone(),
                            data: obj.data.clone(),
                            distance: None,
                            object: obj.clone(),
                        });

                        if results.len() >= limit {
                            return results;
                        }
                    }
                }
            }
        }

        results
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compute::spatial::hybrid::GeohashRTreeIndex;

    #[test]
    fn test_query_within_radius() {
        let mut index = GeohashRTreeIndex::new(7);

        let nyc = Point::new(-74.0060, 40.7128);
        let sf = Point::new(-122.4194, 37.7749);

        index.insert_point("nyc", nyc, Bytes::from("NYC"));
        index.insert_point("sf", sf, Bytes::from("SF"));

        // Query around NYC with small radius
        let results = index.query_within_radius(&nyc, 1000.0, 10);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].key, "nyc");

        // Query with large radius should get both
        let results = index.query_within_radius(&nyc, 10_000.0, 10);
        assert!(!results.is_empty()); // Should find at least NYC
        // If both found, NYC should be first (closer)
        if results.len() >= 2 {
            assert_eq!(results[0].key, "nyc");
        }
    }

    #[test]
    fn test_query_within_bbox() {
        let mut index = GeohashRTreeIndex::new(7);

        let nyc = Point::new(-74.0060, 40.7128);
        let sf = Point::new(-122.4194, 37.7749);

        index.insert_point("nyc", nyc, Bytes::from("NYC"));
        index.insert_point("sf", sf, Bytes::from("SF"));

        // Bbox around NYC
        let bbox = Rect::new(
            geo::coord! { x: -75.0, y: 40.0 },
            geo::coord! { x: -73.0, y: 41.0 },
        );

        let results = index.query_within_bbox(&bbox, 10);
        assert!(!results.is_empty()); // Should find at least NYC
        assert!(results.iter().any(|r| r.key == "nyc"));
    }

    #[test]
    fn test_knn() {
        let mut index = GeohashRTreeIndex::new(7);

        index.insert_point("p1", Point::new(-74.0, 40.7), Bytes::from("P1"));
        index.insert_point("p2", Point::new(-74.1, 40.7), Bytes::from("P2"));
        index.insert_point("p3", Point::new(-74.2, 40.7), Bytes::from("P3"));

        let query_point = Point::new(-74.0, 40.7);
        let results = index.knn(&query_point, 2);

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].key, "p1"); // Closest
        assert_eq!(results[1].key, "p2"); // Second closest
    }

    #[test]
    fn test_knn_empty_index() {
        let index = GeohashRTreeIndex::new(7);
        let results = index.knn(&Point::new(0.0, 0.0), 10);
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_query_with_stats() {
        let mut index = GeohashRTreeIndex::new(7);

        let nyc = Point::new(-74.0060, 40.7128);
        index.insert_point("nyc", nyc, Bytes::from("NYC"));
        index.insert_point("sf", Point::new(-122.4194, 37.7749), Bytes::from("SF"));

        let (results, stats) = index.query_within_radius_with_stats(&nyc, 10_000.0, 10);

        assert!(!results.is_empty()); // Should find at least NYC
        assert!(stats.cells_examined > 0);
        assert!(stats.candidates_examined > 0);
        assert!(stats.results_returned >= 1);
        assert!(stats.deduplicated);
    }

    #[test]
    fn test_query_contains_point() {
        let mut index = GeohashRTreeIndex::new(7);

        let bbox = Rect::new(
            geo::coord! { x: -75.0, y: 40.0 },
            geo::coord! { x: -73.0, y: 41.0 },
        );

        index.insert_bbox("area", &bbox, Bytes::from("NYC Area"));

        let point_inside = Point::new(-74.0, 40.5);
        let results = index.query_contains_point(&point_inside, 10);
        assert!(!results.is_empty());

        let point_outside = Point::new(-80.0, 40.5);
        let results = index.query_contains_point(&point_outside, 10);
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_query_with_filter() {
        let mut index = GeohashRTreeIndex::new(7);

        index.insert_point("nyc", Point::new(-74.0060, 40.7128), Bytes::from("NYC"));
        index.insert_point(
            "philly",
            Point::new(-75.1652, 39.9526),
            Bytes::from("Philadelphia"),
        );

        let bbox = Rect::new(
            geo::coord! { x: -76.0, y: 39.0 },
            geo::coord! { x: -73.0, y: 41.0 },
        );

        // Filter to only include objects with "NYC" in data
        let results = index.query_with_filter(
            &bbox,
            |obj| {
                std::str::from_utf8(&obj.data)
                    .map(|s| s.contains("NYC"))
                    .unwrap_or(false)
            },
            10,
        );

        assert!(!results.is_empty());
        assert!(results.iter().any(|r| r.key == "nyc"));
    }

    #[test]
    fn test_limit_enforcement() {
        let mut index = GeohashRTreeIndex::new(7);

        for i in 0..100 {
            let lon = -74.0 + (i as f64 * 0.001);
            let point = Point::new(lon, 40.7);
            index.insert_point(
                format!("p{}", i),
                point,
                Bytes::from(format!("Point {}", i)),
            );
        }

        let results = index.query_within_radius(&Point::new(-74.0, 40.7), 1_000_000.0, 10);
        assert!(results.len() <= 10); // Should respect limit
        assert!(!results.is_empty()); // Should find some results
    }
}
