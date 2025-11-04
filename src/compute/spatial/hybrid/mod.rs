//! # Two-Level Spatial Indexing: Geohash + RTree Hybrid
//!
//! This module implements a sophisticated two-level spatial indexing system inspired by
//! [Tile38](https://tile38.com/), combining the scalability of geohash partitioning with
//! the precision of R-tree spatial queries.
//!
//! ## Architecture Overview
//!
//! The hybrid index uses a two-tier approach:
//!
//! 1. **Level 1 - Geohash Partitioning (Coarse-grained)**
//!    - Geographic space is divided into hierarchical grid cells using geohash encoding
//!    - Each geohash cell represents a rectangular region at a specific precision level
//!    - Acts as a spatial "bucket" to distribute data across multiple sub-indexes
//!
//! 2. **Level 2 - RTree Indexing (Fine-grained)**
//!    - Each geohash cell maintains its own independent R-tree index
//!    - R-trees provide exact geometric queries within their cell
//!    - Supports points, polygons, bounding boxes, and complex shapes
//!
//! ## How It Works
//!
//! ### Insertion Flow
//! ```text
//! 1. Object arrives (point, polygon, etc.)
//! 2. Calculate geohash(es) covering the object's extent
//! 3. For each geohash cell:
//!    - Get or create the RTree for that cell
//!    - Insert object into the RTree
//! ```
//!
//! ### Query Flow
//! ```text
//! 1. Query arrives (radius search, bbox, etc.)
//! 2. Calculate geohash cells that intersect query region
//! 3. For each candidate geohash cell:
//!    - Retrieve the RTree for that cell
//!    - Execute precise geometric query on the RTree
//! 4. Merge and deduplicate results from all cells
//! 5. Apply final filters and limits
//! ```
//!
//! ## Key Benefits
//!
//! ### 1. **Scalability**
//! - Spatial data is partitioned across many small R-trees instead of one giant tree
//! - Each R-tree has logarithmic complexity: O(log n) where n is points per cell
//! - Total complexity: O(k * log(n/k)) where k is number of cells intersected
//! - Significantly faster than O(log N) for a single tree with N total points
//!
//! ### 2. **Faster Candidate Filtering**
//! - Geohash prefix matching quickly eliminates irrelevant regions
//! - Only R-trees in relevant cells are queried
//! - Reduces search space from entire dataset to local neighborhoods
//!
//! ### 3. **Reduced Index Search Space**
//! - Small R-trees have better cache locality
//! - Less memory pressure during queries
//! - Better CPU cache utilization
//!
//! ### 4. **Distribution-Friendly**
//! - Geohash cells can be distributed across multiple servers
//! - Natural sharding key for horizontal scaling
//! - Each cell's R-tree can be queried independently
//!
//! ### 5. **Flexible Precision**
//! - Adjustable geohash precision balances granularity vs. overhead
//! - Higher precision = more cells, smaller R-trees, faster queries
//! - Lower precision = fewer cells, larger R-trees, less overhead
//!
//! ### 6. **Exact Geometric Queries**
//! - R-tree provides accurate distance calculations
//! - Supports complex spatial predicates (intersects, contains, within)
//! - No false positives unlike pure geohash approximations
//!
//! ## Performance Characteristics
//!
//! | Operation | Single RTree | Geohash Only | Hybrid (This) |
//! |-----------|--------------|--------------|---------------|
//! | Insert    | O(log N)     | O(1)         | O(log(N/k))   |
//! | Query     | O(log N + m) | O(m) + FP    | O(k·log(N/k) + m) |
//! | Memory    | O(N)         | O(N)         | O(N + k)      |
//!
//! Where:
//! - N = total number of objects
//! - k = number of geohash cells touched
//! - m = number of results
//! - FP = false positives (geohash approximation errors)
//!
//! ## Precision Guidelines
//!
//! Geohash precision determines cell size:
//!
//! | Precision | Cell Size (approx) | Use Case |
//! |-----------|-------------------|----------|
//! | 4         | ~20km × 20km      | Continental queries |
//! | 5         | ~4.9km × 4.9km    | City-level queries |
//! | 6         | ~1.2km × 0.6km    | Neighborhood queries |
//! | 7         | ~153m × 153m      | Street-level queries |
//! | 8         | ~38m × 19m        | Building-level queries |
//! | 9         | ~4.8m × 4.8m      | Room-level queries |
//!
//! **Recommendation**: Precision 6-8 for most applications
//!
//! ## Examples
//!
//! ```rust
//! use spatio::compute::spatial::hybrid::{GeohashRTreeIndex, SpatialObject};
//! use geo::Point;
//!
//! // Create index with precision 7 (street-level)
//! let mut index = GeohashRTreeIndex::new(7);
//!
//! // Insert points
//! let nyc = Point::new(-74.0060, 40.7128);
//! index.insert_point("nyc", nyc, &b"New York City"[..]);
//!
//! let sf = Point::new(-122.4194, 37.7749);
//! index.insert_point("sf", sf, &b"San Francisco"[..]);
//!
//! // Query within 1000m radius
//! let results = index.query_within_radius(&nyc, 1000.0, 10);
//!
//! // Range query
//! let bbox = geo::Rect::new(
//!     geo::coord! { x: -74.1, y: 40.6 },
//!     geo::coord! { x: -73.9, y: 40.8 },
//! );
//! let results = index.query_within_bbox(&bbox, 100);
//! ```
//!
//! ## Comparison to Alternatives
//!
//! ### vs. Single R-tree
//! - **Pros**: Better scalability, faster queries on large datasets, distribution-friendly
//! - **Cons**: Slight overhead for small datasets, objects may be stored in multiple cells
//!
//! ### vs. Pure Geohash
//! - **Pros**: Exact geometric queries, supports complex shapes, no false positives
//! - **Cons**: More memory usage, slightly more complex implementation
//!
//! ### vs. Quadtree/KD-tree
//! - **Pros**: Better for non-uniform distributions, handles overlapping objects
//! - **Cons**: More complex to distribute, less intuitive spatial keys
//!
//! ## Implementation Notes
//!
//! - Objects spanning multiple geohash cells are stored in each cell
//! - Deduplication is performed during query result merging
//! - Empty geohash cells consume minimal memory (lazy initialization)
//! - Thread-safe with appropriate locking (when feature enabled)

pub mod geohash_rtree;
pub mod query;
pub mod spatial_object;

pub use geohash_rtree::{CellStats, GeohashRTreeIndex, GeohashStats};
pub use query::{QueryResult, QueryStats};
pub use spatial_object::{ObjectType, SpatialObject};

#[cfg(test)]
mod tests;
