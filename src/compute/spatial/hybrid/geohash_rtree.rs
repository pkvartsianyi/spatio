//! Core implementation of the Geohash-RTree hybrid spatial index.
//!
//! This module provides the main index structure that combines geohash partitioning
//! with R-tree indexing for efficient spatial queries at scale.

use super::spatial_object::SpatialObject;
use bytes::Bytes;
use geo::{Point, Polygon, Rect};
use geohash::{encode, neighbors};
use rstar::RTree;
use rustc_hash::FxHashMap;
use std::collections::HashSet;

/// Statistics for a single geohash cell.
#[derive(Debug, Clone)]
pub struct CellStats {
    /// The geohash string for this cell
    pub geohash: String,
    /// Number of objects in this cell's R-tree
    pub object_count: usize,
    /// Memory estimate for this cell's R-tree in bytes
    pub estimated_memory: usize,
}

/// Overall statistics for the geohash-rtree index.
#[derive(Debug, Clone)]
pub struct GeohashStats {
    /// Total number of geohash cells
    pub cell_count: usize,
    /// Total number of objects across all cells (may include duplicates)
    pub total_objects: usize,
    /// Number of unique objects (deduplicated count)
    pub unique_objects: usize,
    /// Average objects per cell
    pub avg_objects_per_cell: f64,
    /// Geohash precision level
    pub precision: usize,
    /// Per-cell statistics
    pub cells: Vec<CellStats>,
}

/// The main geohash-rtree hybrid spatial index.
///
/// # Architecture
///
/// This index maintains a two-level hierarchy:
/// 1. Top level: HashMap<Geohash, RTree>
/// 2. Bottom level: RTree containing SpatialObjects
///
/// # Thread Safety
///
/// This structure is not thread-safe by default. Wrap in `Arc<Mutex<>>` or
/// `Arc<RwLock<>>` for concurrent access.
///
/// # Memory Layout
///
/// ```text
/// GeohashRTreeIndex
/// ├─ cells: HashMap<String, RTree<SpatialObject>>
/// │  ├─ "dr5regw" -> RTree [obj1, obj2, obj3]
/// │  ├─ "dr5regu" -> RTree [obj4, obj5]
/// │  └─ "dr5regv" -> RTree [obj6]
/// ├─ object_to_cells: HashMap<String, HashSet<String>>
/// │  ├─ "obj1" -> {"dr5regw"}
/// │  ├─ "obj4" -> {"dr5regu", "dr5regv"}  // spans multiple cells
/// │  └─ ...
/// └─ precision: 7
/// ```
///
/// # Examples
///
/// ```rust
/// use spatio::compute::spatial::hybrid::GeohashRTreeIndex;
/// use geo::Point;
/// use bytes::Bytes;
///
/// let mut index = GeohashRTreeIndex::new(7);
///
/// // Insert a point
/// let nyc = Point::new(-74.0060, 40.7128);
/// index.insert_point("nyc", nyc, Bytes::from("New York City"));
///
/// // Query nearby points
/// let results = index.query_within_radius(&nyc, 1000.0, 10);
/// assert_eq!(results.len(), 1);
/// ```
pub struct GeohashRTreeIndex {
    /// Map of geohash -> R-tree for that cell
    pub(crate) cells: FxHashMap<String, RTree<SpatialObject>>,

    /// Reverse index: object key -> set of geohash cells it belongs to
    /// This enables efficient deletion and prevents duplicates
    object_to_cells: FxHashMap<String, HashSet<String>>,

    /// Geohash precision (1-12)
    precision: usize,
}

impl GeohashRTreeIndex {
    /// Create a new geohash-rtree hybrid index.
    ///
    /// # Arguments
    ///
    /// * `precision` - Geohash precision level (1-12)
    ///   - Higher = smaller cells, more granular partitioning
    ///   - Lower = larger cells, fewer partitions
    ///   - Recommended: 6-8 for most applications
    ///
    /// # Panics
    ///
    /// Panics if precision is not in range 1-12.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::compute::spatial::hybrid::GeohashRTreeIndex;
    ///
    /// // City-level precision
    /// let index = GeohashRTreeIndex::new(5);
    ///
    /// // Street-level precision (recommended)
    /// let index = GeohashRTreeIndex::new(7);
    ///
    /// // Building-level precision
    /// let index = GeohashRTreeIndex::new(8);
    /// ```
    pub fn new(precision: usize) -> Self {
        assert!(
            (1..=12).contains(&precision),
            "Geohash precision must be between 1 and 12"
        );

        Self {
            cells: FxHashMap::default(),
            object_to_cells: FxHashMap::default(),
            precision,
        }
    }

    /// Get the geohash precision used by this index.
    pub fn precision(&self) -> usize {
        self.precision
    }

    /// Get the number of geohash cells in use.
    pub fn cell_count(&self) -> usize {
        self.cells.len()
    }

    /// Get the total number of unique objects in the index.
    pub fn object_count(&self) -> usize {
        self.object_to_cells.len()
    }

    /// Insert a 2D point into the index.
    ///
    /// # Arguments
    ///
    /// * `key` - Unique identifier for this point
    /// * `point` - The geographic point to index
    /// * `data` - Associated data payload
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::compute::spatial::hybrid::GeohashRTreeIndex;
    /// use geo::Point;
    /// use bytes::Bytes;
    ///
    /// let mut index = GeohashRTreeIndex::new(7);
    /// let point = Point::new(-122.4194, 37.7749);
    /// index.insert_point("sf", point, Bytes::from("San Francisco"));
    /// ```
    pub fn insert_point<K: Into<String>, D: Into<Bytes>>(
        &mut self,
        key: K,
        point: Point<f64>,
        data: D,
    ) {
        let key = key.into();
        let data = data.into();

        // Calculate geohash for this point
        let coord = geohash::Coord {
            x: point.x(),
            y: point.y(),
        };
        let hash = encode(coord, self.precision).unwrap_or_else(|_| {
            // Fallback for invalid coordinates
            "invalid".to_string()
        });

        // Create spatial object
        let obj = SpatialObject::from_point(key.clone(), point, data);

        // Insert into the appropriate cell
        self.insert_object_into_cell(key, obj, hash);
    }

    /// Insert a 3D point into the index.
    ///
    /// # Arguments
    ///
    /// * `key` - Unique identifier for this point
    /// * `x` - Longitude
    /// * `y` - Latitude
    /// * `z` - Altitude/elevation in meters
    /// * `data` - Associated data payload
    pub fn insert_point_3d<K: Into<String>, D: Into<Bytes>>(
        &mut self,
        key: K,
        x: f64,
        y: f64,
        z: f64,
        data: D,
    ) {
        let key = key.into();
        let data = data.into();

        // Calculate geohash (only uses x, y)
        let coord = geohash::Coord { x, y };
        let hash = encode(coord, self.precision).unwrap_or_else(|_| "invalid".to_string());

        // Create 3D spatial object
        let obj = SpatialObject::from_point_3d(key.clone(), x, y, z, data);

        // Insert into the appropriate cell
        self.insert_object_into_cell(key, obj, hash);
    }

    /// Insert a polygon into the index.
    ///
    /// Polygons may span multiple geohash cells. The polygon will be stored
    /// in all cells that its bounding box intersects.
    ///
    /// # Arguments
    ///
    /// * `key` - Unique identifier for this polygon
    /// * `polygon` - The polygon geometry
    /// * `data` - Associated data payload
    pub fn insert_polygon<K: Into<String>, D: Into<Bytes>>(
        &mut self,
        key: K,
        polygon: &Polygon<f64>,
        data: D,
    ) {
        let key = key.into();
        let data = data.into();

        // Get bounding box of polygon
        use geo::BoundingRect;
        let Some(bbox) = polygon.bounding_rect() else {
            return; // Empty polygon
        };

        // Create spatial object
        let obj = SpatialObject::from_polygon(key.clone(), polygon, data);

        // Get all geohash cells that intersect the bounding box
        let cells = self.get_cells_for_bbox(&bbox);

        // Insert into all intersecting cells
        self.insert_object_into_cells(key, obj, cells);
    }

    /// Insert a bounding box into the index.
    ///
    /// # Arguments
    ///
    /// * `key` - Unique identifier for this bbox
    /// * `bbox` - The bounding rectangle
    /// * `data` - Associated data payload
    pub fn insert_bbox<K: Into<String>, D: Into<Bytes>>(
        &mut self,
        key: K,
        bbox: &Rect<f64>,
        data: D,
    ) {
        let key = key.into();
        let data = data.into();

        // Create spatial object
        let obj = SpatialObject::from_bbox(key.clone(), bbox, data);

        // Get all geohash cells that intersect the bounding box
        let cells = self.get_cells_for_bbox(bbox);

        // Insert into all intersecting cells
        self.insert_object_into_cells(key, obj, cells);
    }

    /// Remove an object from the index by key.
    ///
    /// # Arguments
    ///
    /// * `key` - The unique identifier of the object to remove
    ///
    /// # Returns
    ///
    /// `true` if the object was found and removed, `false` otherwise.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::compute::spatial::hybrid::GeohashRTreeIndex;
    /// use geo::Point;
    /// use bytes::Bytes;
    ///
    /// let mut index = GeohashRTreeIndex::new(7);
    /// let point = Point::new(-122.4194, 37.7749);
    /// index.insert_point("sf", point, Bytes::from("San Francisco"));
    ///
    /// assert!(index.remove("sf"));
    /// assert!(!index.remove("sf")); // Already removed
    /// ```
    pub fn remove(&mut self, key: &str) -> bool {
        // Get all cells this object is in
        let Some(cell_hashes) = self.object_to_cells.remove(key) else {
            return false;
        };

        // Remove from each cell's R-tree
        for hash in cell_hashes {
            if let Some(tree) = self.cells.get_mut(&hash) {
                // Remove objects with matching key
                let new_tree = RTree::bulk_load(
                    tree.iter()
                        .filter(|obj| obj.key != key)
                        .cloned()
                        .collect::<Vec<_>>(),
                );

                // Replace the tree
                *tree = new_tree;

                // Clean up empty cells to save memory
                if tree.size() == 0 {
                    self.cells.remove(&hash);
                }
            }
        }

        true
    }

    /// Clear all objects from the index.
    pub fn clear(&mut self) {
        self.cells.clear();
        self.object_to_cells.clear();
    }

    /// Check if an object with the given key exists in the index.
    pub fn contains_key(&self, key: &str) -> bool {
        self.object_to_cells.contains_key(key)
    }

    /// Get comprehensive statistics about the index.
    ///
    /// # Returns
    ///
    /// `GeohashStats` containing cell counts, object counts, and per-cell details.
    pub fn stats(&self) -> GeohashStats {
        let cell_count = self.cells.len();
        let unique_objects = self.object_to_cells.len();

        // Calculate total objects (with duplicates across cells)
        let total_objects: usize = self.cells.values().map(|tree| tree.size()).sum();

        let avg_objects_per_cell = if cell_count > 0 {
            total_objects as f64 / cell_count as f64
        } else {
            0.0
        };

        // Collect per-cell statistics
        let mut cells: Vec<CellStats> = self
            .cells
            .iter()
            .map(|(hash, tree)| {
                let object_count = tree.size();
                // Rough memory estimate: each object ~200 bytes + tree overhead
                let estimated_memory = object_count * 200 + 1024;

                CellStats {
                    geohash: hash.clone(),
                    object_count,
                    estimated_memory,
                }
            })
            .collect();

        // Sort by object count descending
        cells.sort_by(|a, b| b.object_count.cmp(&a.object_count));

        GeohashStats {
            cell_count,
            total_objects,
            unique_objects,
            avg_objects_per_cell,
            precision: self.precision,
            cells,
        }
    }

    // ========================================================================
    // Private helper methods
    // ========================================================================

    /// Insert an object into a single geohash cell.
    fn insert_object_into_cell(&mut self, key: String, obj: SpatialObject, hash: String) {
        // Remove old version if it exists
        if self.object_to_cells.contains_key(&key) {
            self.remove(&key);
        }

        // Get or create the R-tree for this cell
        let tree = self.cells.entry(hash.clone()).or_default();

        // Insert into R-tree (bulk load for efficiency)
        let mut objects: Vec<SpatialObject> = tree.iter().cloned().collect();
        objects.push(obj);
        *tree = RTree::bulk_load(objects);

        // Update reverse index
        let mut cell_set = HashSet::new();
        cell_set.insert(hash);
        self.object_to_cells.insert(key, cell_set);
    }

    /// Insert an object into multiple geohash cells.
    fn insert_object_into_cells(&mut self, key: String, obj: SpatialObject, hashes: Vec<String>) {
        if hashes.is_empty() {
            return;
        }

        // Remove old version if it exists
        if self.object_to_cells.contains_key(&key) {
            self.remove(&key);
        }

        // Insert into each cell
        for hash in &hashes {
            let tree = self.cells.entry(hash.clone()).or_default();

            // Add to existing tree
            let mut objects: Vec<SpatialObject> = tree.iter().cloned().collect();
            objects.push(obj.clone());
            *tree = RTree::bulk_load(objects);
        }

        // Update reverse index
        self.object_to_cells
            .insert(key, hashes.into_iter().collect());
    }

    /// Get all geohash cells that intersect a bounding box.
    pub(crate) fn get_cells_for_bbox(&self, bbox: &Rect<f64>) -> Vec<String> {
        let mut cells = HashSet::new();

        // Get corners of bbox
        let min_coord = geohash::Coord {
            x: bbox.min().x,
            y: bbox.min().y,
        };
        let max_coord = geohash::Coord {
            x: bbox.max().x,
            y: bbox.max().y,
        };

        // Get geohashes for all four corners
        let min_hash = encode(min_coord, self.precision).unwrap_or_default();
        let max_hash = encode(max_coord, self.precision).unwrap_or_default();

        let top_left = encode(
            geohash::Coord {
                x: bbox.min().x,
                y: bbox.max().y,
            },
            self.precision,
        )
        .unwrap_or_default();
        let bottom_right = encode(
            geohash::Coord {
                x: bbox.max().x,
                y: bbox.min().y,
            },
            self.precision,
        )
        .unwrap_or_default();

        cells.insert(min_hash.clone());
        cells.insert(max_hash.clone());
        cells.insert(top_left.clone());
        cells.insert(bottom_right.clone());

        // Add neighbors for all corners to ensure complete coverage
        for corner in &[min_hash, max_hash, top_left, bottom_right] {
            if let Ok(nb) = neighbors(corner) {
                cells.insert(nb.n);
                cells.insert(nb.ne);
                cells.insert(nb.e);
                cells.insert(nb.se);
                cells.insert(nb.s);
                cells.insert(nb.sw);
                cells.insert(nb.w);
                cells.insert(nb.nw);
            }
        }

        cells.into_iter().collect()
    }

    /// Get all geohash cells within a radius of a point.
    pub(crate) fn get_cells_for_radius(
        &self,
        center: &Point<f64>,
        radius_meters: f64,
    ) -> Vec<String> {
        // For very large radii (>1000km), search all cells
        // This is more efficient than trying to calculate exact cells
        if radius_meters > 1_000_000.0 {
            return self.cells.keys().cloned().collect();
        }

        // Conservative approach: get center cell and multiple rings of neighbors
        let coord = geohash::Coord {
            x: center.x(),
            y: center.y(),
        };
        let center_hash = encode(coord, self.precision).unwrap_or_default();

        let mut cells = HashSet::new();
        cells.insert(center_hash.clone());

        // Add all immediate neighbors (ring 1)
        if let Ok(nb) = neighbors(&center_hash) {
            cells.insert(nb.n.clone());
            cells.insert(nb.ne.clone());
            cells.insert(nb.e.clone());
            cells.insert(nb.se.clone());
            cells.insert(nb.s.clone());
            cells.insert(nb.sw.clone());
            cells.insert(nb.w.clone());
            cells.insert(nb.nw.clone());

            // Add ring 2 neighbors (neighbors of neighbors) for better coverage
            let ring1 = vec![nb.n, nb.ne, nb.e, nb.se, nb.s, nb.sw, nb.w, nb.nw];
            for cell in ring1 {
                if let Ok(nb2) = neighbors(&cell) {
                    cells.insert(nb2.n);
                    cells.insert(nb2.ne);
                    cells.insert(nb2.e);
                    cells.insert(nb2.se);
                    cells.insert(nb2.s);
                    cells.insert(nb2.sw);
                    cells.insert(nb2.w);
                    cells.insert(nb2.nw);
                }
            }
        }

        cells.into_iter().collect()
    }
}

impl Default for GeohashRTreeIndex {
    fn default() -> Self {
        Self::new(7) // Street-level precision by default
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_index() {
        let index = GeohashRTreeIndex::new(7);
        assert_eq!(index.precision(), 7);
        assert_eq!(index.cell_count(), 0);
        assert_eq!(index.object_count(), 0);
    }

    #[test]
    #[should_panic]
    fn test_invalid_precision() {
        GeohashRTreeIndex::new(0);
    }

    #[test]
    fn test_insert_point() {
        let mut index = GeohashRTreeIndex::new(7);
        let point = Point::new(-74.0060, 40.7128);

        index.insert_point("nyc", point, Bytes::from("New York"));

        assert_eq!(index.object_count(), 1);
        assert_eq!(index.cell_count(), 1);
        assert!(index.contains_key("nyc"));
    }

    #[test]
    fn test_insert_multiple_points() {
        let mut index = GeohashRTreeIndex::new(7);

        index.insert_point("nyc", Point::new(-74.0060, 40.7128), Bytes::from("NYC"));
        index.insert_point("sf", Point::new(-122.4194, 37.7749), Bytes::from("SF"));
        index.insert_point("la", Point::new(-118.2437, 34.0522), Bytes::from("LA"));

        assert_eq!(index.object_count(), 3);
        assert!(index.cell_count() >= 2); // At least 2 different cells
    }

    #[test]
    fn test_remove() {
        let mut index = GeohashRTreeIndex::new(7);
        let point = Point::new(-74.0060, 40.7128);

        index.insert_point("nyc", point, Bytes::from("New York"));
        assert!(index.contains_key("nyc"));

        assert!(index.remove("nyc"));
        assert!(!index.contains_key("nyc"));
        assert_eq!(index.object_count(), 0);

        // Removing again should return false
        assert!(!index.remove("nyc"));
    }

    #[test]
    fn test_update_point() {
        let mut index = GeohashRTreeIndex::new(7);

        index.insert_point("city", Point::new(-74.0060, 40.7128), Bytes::from("NYC"));
        assert_eq!(index.object_count(), 1);

        // Update with new location
        index.insert_point("city", Point::new(-122.4194, 37.7749), Bytes::from("SF"));
        assert_eq!(index.object_count(), 1); // Still just one object
    }

    #[test]
    fn test_clear() {
        let mut index = GeohashRTreeIndex::new(7);

        index.insert_point("nyc", Point::new(-74.0060, 40.7128), Bytes::from("NYC"));
        index.insert_point("sf", Point::new(-122.4194, 37.7749), Bytes::from("SF"));

        index.clear();

        assert_eq!(index.object_count(), 0);
        assert_eq!(index.cell_count(), 0);
    }

    #[test]
    fn test_stats() {
        let mut index = GeohashRTreeIndex::new(7);

        index.insert_point("nyc", Point::new(-74.0060, 40.7128), Bytes::from("NYC"));
        index.insert_point("sf", Point::new(-122.4194, 37.7749), Bytes::from("SF"));

        let stats = index.stats();
        assert_eq!(stats.unique_objects, 2);
        assert_eq!(stats.precision, 7);
        assert!(stats.cell_count >= 1);
    }

    #[test]
    fn test_insert_polygon() {
        use geo::polygon;

        let mut index = GeohashRTreeIndex::new(7);

        let poly = polygon![
            (x: -74.0, y: 40.7),
            (x: -74.0, y: 40.8),
            (x: -73.9, y: 40.8),
            (x: -73.9, y: 40.7),
            (x: -74.0, y: 40.7),
        ];

        index.insert_polygon("area", &poly, Bytes::from("Manhattan"));

        assert_eq!(index.object_count(), 1);
        // Polygon may span multiple cells
        assert!(index.cell_count() >= 1);
    }
}
