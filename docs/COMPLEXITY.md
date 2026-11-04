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
| `live` | objects surviving in the recovery snapshot |
| `tail` | log records appended since the last checkpoint |

## Tiers

| Tier | Class | Meaning |
|---|---|---|
| 🟢 T1 | `O(1)` / `O(log N)` | constant or logarithmic |
| 🟡 T2 | `O(log N + K)` | output-sensitive index query |
| 🟠 T3 | `O(N)` | per-namespace scan |
| 🔴 T4 | `O(L)` | full-log / unbounded scan |

**⚠️ = serializing write.** All writes acquire one **global** spatial-index
`RwLock`, so they are mutually exclusive across *every* namespace regardless of
their `O(log N)` cost. This is the dominant write-throughput limit, not the
algorithmic complexity.

## Operations

| Operation | Complexity | Tier | Notes |
|---|---|---|---|
| **Writes** | | | |
| `upsert` ⚠️ | `O(log N)` (`O(1)` if unmoved) | 🟢 T1 | R\*-tree remove+reinsert on move; skips the index entirely if the position is unchanged. No output, so no `K` term. |
| `delete` ⚠️ | `O(log N)` | 🟢 T1 | tombstone append `O(1)` + DashMap remove + index remove (coords supplied → fast path). |
| `insert_trajectory` ⚠️ | `O(M log N)` | 🟢 T1 ×M | M sequential `upsert`s; scales linearly in the input size M. |
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
| `knn` | `O(log N + K)` | 🟡 T2 | grows a sphere from 1 m until it holds `k` points, so `K` is what that sphere covers, at most 4x wider than needed. Tightly clustered points raise `K`. |
| `query_polygon` | `O(log N + K·V)` | 🟡 T2 | bbox broad-phase, then point-in-polygon per candidate. **The only op with a two-axis degradation path** — cost grows with both `K` (envelope selectivity) *and* `V` (vertex count), so `K·V` is worse than the plain `K→N` of the other queries. |
| `query_near`, all `*_near_object` | `O(1)` + delegate | 🟡 T2 | `O(1)` target lookup, then one delegate query; inherits that query's cost — `query_near`→`query_radius`, `knn_near_object`→`knn` (`O(k log N)`), etc. |
| **Namespace-wide** | | | |
| `convex_hull` | `O(N log N)` | 🟠 T3 | materializes all namespace points, then hull. |
| `stats` | `O(P)` | 🟠 T3 | hot part `O(1)`; cold part iterates recent buffers. |
| **Trajectory (cold)** | | | |
| `query_trajectory` (buffer hit) | `O(B log B)` | 🟢/🟡 | Always taken for `:memory:` databases, which keep only the buffer. Otherwise taken when the query starts after the horizon (everything not in a buffer is older), or when `limit` is met by updates newer than it. Deletes, restarts and buffer evictions raise the horizon, so older windows use the log. |
| `query_trajectory` (log fallback) | `O(L)` | 🔴 T4 | full scan of the stable log prefix, holding at most `limit` results; shrink `L` with `compact`. |
| **Lifecycle** | | | |
| `flush` | `O(1)` + fsync | 🟢 T1 | writes out the log buffer. |
| `compact` | `O(L)` | 🔴 T4 | two passes over the log; writes wait until it finishes. |
| `open` / recovery | `O(live + tail)` | 🟡/🟠 | load the checkpoint snapshot (`live` objects) + replay the post-snapshot `tail`. Without a checkpoint this degrades to a full `O(L)` replay. |

## Caveats

1. **`K` can collapse to `N`.** The 🟡 spatial queries are output-sensitive — efficient
   only when the envelope is selective. A very large radius/bbox drives `K → N`, making
   them effectively `O(N)`. The tier assumes a reasonable query area.

2. **The performance cliffs to know about:**
   - `query_trajectory`'s log fallback is `O(L)` over *all history in the log* — the
     one operation that degrades without bound. It is avoided for recent windows
     (see the buffer-hit row), and `compact` bounds `L`.
   - The global write lock (⚠️ rows) caps write throughput regardless of the per-op
     `O(log N)`. Per-namespace lock sharding is the lever to change this.
