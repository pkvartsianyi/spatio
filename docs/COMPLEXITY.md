# Operation Complexity

Time complexity of each public `Spatio`/`DB` operation, with an efficiency tier.
Grounded in the current implementation (`crates/core/db/{mod,hot_state,cold_state}.rs`,
`crates/core/compute/spatial/rtree.rs`).

## Symbols

| Symbol | Meaning |
|---|---|
| `N` | objects in the target namespace |
| `K` | objects matching a query's bounding envelope (`K ≤ N`) |
| `L` | total records in the on-disk trajectory log |
| `B` | per-object recent-buffer capacity (`buffer_capacity`) |
| `M` | points in a trajectory insert |
| `V` | polygon vertex count |
| `P` | objects with a recent buffer |

## Tiers

| Tier | Class | Meaning |
|---|---|---|
| 🟢 T1 | `O(1)` / `O(log N)` | constant or logarithmic |
| 🟡 T2 | `O(log N + K)` | output-sensitive index query |
| 🟠 T3 | `O(N)` | per-namespace scan |
| 🔴 T4 | `O(L)` | full-log / unbounded scan |

## Operations

| Operation | Complexity | Tier | Notes |
|---|---|---|---|
| **Writes** | | | |
| `upsert` | `O(log N)` (`O(1)` if unmoved) | 🟡 T2 | R\*-tree remove+reinsert on move; skips the index entirely if the position is unchanged. Holds the **global** index write-lock, so writers serialize across all namespaces. |
| `delete` | `O(log N)` | 🟡 T2 | tombstone append `O(1)` + DashMap remove + index remove (coords supplied → fast path). |
| `insert_trajectory` | `O(M log N)` | 🟡 T2 | M sequential `upsert`s. |
| **Point reads** | | | |
| `get` | `O(1)` | 🟢 T1 | DashMap lookup. |
| `distance_between` | `O(1)` | 🟢 T1 | two lookups + math. |
| `distance_to` | `O(1)` | 🟢 T1 | one lookup + math. |
| `bounding_box` | `O(1)` | 🟢 T1 | reads the R\*-tree root envelope. |
| **Spatial queries** | | | |
| `query_radius` | `O(log N + K)` | 🟡 T2 | envelope prune + per-candidate haversine + top-k heap. |
| `query_bbox` (2D) | `O(log N + K)` | 🟡 T2 | |
| `query_within_bbox_3d` | `O(log N + K)` | 🟡 T2 | |
| `query_within_cylinder` | `O(log N + K)` | 🟡 T2 | envelope + altitude/horizontal filter. |
| `knn` | `O(k log N)` | 🟡 T2 | R\*-tree nearest-neighbor iterator, take k. |
| `query_polygon` | `O(log N + K·V)` | 🟡→🟠 | bbox broad-phase, then point-in-polygon per candidate; degrades with V. |
| `query_near`, all `*_near_object` | `O(1)` + delegate | 🟡 T2 | `O(1)` target lookup, then the matching query above. |
| **Namespace-wide** | | | |
| `convex_hull` | `O(N log N)` | 🟠 T3 | materializes all namespace points, then hull. |
| `stats` | `O(P)` | 🟠 T3 | hot part `O(1)`; cold part iterates recent buffers. |
| **Trajectory (cold)** | | | |
| `query_trajectory` (buffer hit) | `O(B log B)` | 🟢/🟡 | served from the recent buffer when `buffer.len() < capacity`. |
| `query_trajectory` (log fallback) | `O(L)` | 🔴 T4 | full scan of the stable log prefix; **grows unbounded with history**. |
| **Lifecycle** | | | |
| `open` / recovery | `O(live + tail)` | 🟡/🟠 | checkpoint snapshot load + replay of the post-snapshot tail. |

## Caveats

1. **`K` can collapse to `N`.** The 🟡 spatial queries are output-sensitive — efficient
   only when the envelope is selective. A very large radius/bbox drives `K → N`, making
   them effectively `O(N)`. The tier assumes a reasonable query area.

2. **The performance cliffs to know about:**
   - `query_trajectory`'s log fallback is `O(L)` over *all history ever written* — the
     one operation that degrades without bound. It is avoided when the per-object buffer
     covers the requested window.
   - Every write takes one **global** index `RwLock`, so write throughput does not scale
     across namespaces despite the per-op `O(log N)`. Per-namespace lock sharding is the
     lever to change this.
