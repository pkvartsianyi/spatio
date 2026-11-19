# Spatio Architecture

Spatio is a high-performance, spatio-temporal database designed for tracking moving objects. It is organized as a Cargo workspace with multiple crates, each serving a specific role in the ecosystem.

## Workspace Structure

The project is divided into the following crates:

### Core Layer

- **`spatio` (`crates/core`)**:
  - The core database engine.
  - Implements the "Hot/Cold" architecture.
  - **Hot State**: In-memory `DashMap` + R*-tree for real-time spatial queries.
  - **Cold State**: Append-only log files for historical trajectory persistence.
  - **Shapes**: Polygons, circles and corridors per namespace in their own R*-tree behind their own lock, saved to `<db>.shapes`.
  - No network dependencies; purely embedded.

- **`spatio-types` (`crates/types`)**:
  - Shared primitive types used across the ecosystem.
  - Defines `Point3d`, `BoundingBox`, `Polygon`, `LineString`, `Shape`, etc.
  - Great-circle line geometry: point-to-line distance and line intersection.
  - Serializaton logic (Serde, GeoJSON).
  - Dependency for all other crates to ensure type compatibility.

### Application Layer

- **`spatio-server` (`crates/server`)**:
  - A lightweight TCP server wrapper around `spatio`.
  - Exposes the database API as JSON-RPC over TCP (using `tarpc`).
  - Handles concurrent connections and offloads blocking DB operations to a thread pool.
  - Intended for remote access or multi-process architectures.

- **`spatio-client` (`crates/client`)**:
  - Native Rust RPC client for `spatio-server`.
  - Provides a strongly-typed async API to interact with a remote Spatio instance.
  - Uses a TCP transport.

### Interface Layer

- **`spatio-py` (`bindings/python`)**:
  - Python bindings for the core `spatio` engine using `PyO3`.
  - Allows using Spatio as a native Python library (`import spatio`).
  - Bypasses the server/client layer for maximum performance in Python applications.

- **`spatio-node` (`bindings/typescript`)**:
  - Node.js and TypeScript bindings for the core `spatio` engine using `napi-rs`.
  - Published to npm as `spatio-db`, with a prebuilt addon per platform.

## Component Interaction Diagram

```mermaid
graph TD
    subgraph "Clients"
        RustApp[Rust App]
        PyApp[Python App]
        RemoteApp[Remote Client]
    end

    subgraph "Interface / Network"
        ClientLib[spatio-client]
        PyBindings[spatio-py]
        TcpServer[spatio-server]
    end

    subgraph "Core Engine"
        Core["spatio Core"]
        Types[spatio-types]
    end

    subgraph "Storage"
        HotState["Hot State - RAM"]
        ColdState["Cold State - Disk"]
    end

    %% Flows & Dependencies
    Core --> Types
    ClientLib --> Types
    PyBindings --> Types
    TcpServer --> Types
    RustApp --> ClientLib
    ClientLib -- "TCP RPC" --> TcpServer
    TcpServer --> Core
    PyApp --> PyBindings
    PyBindings -- "FFI" --> Core
    Core --> HotState
    Core --> ColdState
```

## Data Flow

### Write Path (Upsert)
1. **Request**: A location update is received (via embedded call, Python call, or RPC).
2. **Hot State Update**: The current position is updated in the in-memory `DashMap` and Spatial Index. `DashMap` uses sharded locks; the spatial index sits behind a single `RwLock`, so index writes are serialized.
3. **Cold State Append**: The update is buffered in an in-memory write buffer.
   - **Buffering**: Writes are not immediately flushed to disk. They are stored in a count-based buffer (default: 512 records).
   - **Flushing**: When the buffer is full (`pending_writes >= buffer_size`), the data is explicitly flushed to the log file.
   - **Configuration**: Controlled via `PersistenceConfig` (`buffer_size`).

### Write Path (Shapes)
1. **Validate**: Coordinates, vertex counts, radius and width are checked.
2. **Persist**: A CRC-checked record is appended to `<db>.shapes` and synced before the shape becomes visible. Shapes change rarely, so each write is synced.
3. **Index**: The shape's padded bounding box goes into the namespace's shape R*-tree. This lock is separate from the point index, so shape writes never block location updates.
4. **Recovery**: On open the file is replayed; torn or corrupt records are skipped, and the file is rewritten when it ends mid-record or holds mostly superseded records. `compact` also rewrites it.

### Read Path (Spatial Query)
1. **Hot Query**: Radius/BBox queries scan the in-memory R*-tree.
3. **Return**: Current locations are returned immediately.

### Read Path (Shape Query)
1. **Broad phase**: The shape R*-tree returns shapes whose padded bounding box covers the point (or the point's radius).
2. **Exact test**: Polygons use point-in-polygon (edges straight in lon/lat, as in GeoJSON); circles and corridors use great-circle distance.

### Read Path (Trajectory Query)
1. **Buffer Check**: The in-memory buffer in `ColdState` answers the query if it can prove nothing newer is missing. `ColdState` tracks a horizon: every update not in a buffer is older than it. The answer comes from the buffer or the log, never a merge of the two.
2. **Disk Scan**: If more history is needed, the append-only log is scanned efficiently (or via index if available). `:memory:` databases have no log, so the buffer is their whole history: the last `buffer_capacity` updates per object, removed on `delete`.

## Threading Model

- **Core**: Thread-safe. Reads and writes take sharded `DashMap` locks, and spatial queries and index updates share one `RwLock`.
- **Server**: Async-first (Tokio). Blocking database operations (disk I/O, heavy spatial calcs) are offloaded to `spawn_blocking` to keep the event loop responsive.
