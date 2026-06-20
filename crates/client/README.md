# Spatio Client

A lightweight, native Rust RPC client for connecting to remote Spatio database servers.

## Overview

`spatio-client` provides an async client for interacting with `spatio-server` instances over TCP, using `tarpc` over a JSON-serialized transport.

## Features

- **Async/Await**: Built on `tokio` for non-blocking I/O
- **Full API Coverage**: Supports all Spatio database operations

## Usage

```rust
use spatio_client::SpatioClient;
use spatio_types::point::Point3d;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Connect to a remote server (requests use a fixed 30s deadline)
    let client = SpatioClient::connect("127.0.0.1:3000".parse()?).await?;

    // Perform operations
    let nyc = Point3d::new(-74.0060, 40.7128, 0.0);
    client.upsert("cities", "nyc", nyc, serde_json::json!({"pop": 8000000}), None).await?;

    // Query
    let nearby = client.query_radius("cities", &nyc, 100_000.0, 10).await?;
    println!("Found {} nearby cities", nearby.len());

    Ok(())
}
```

## API

The client mirrors the embedded database API:

- `upsert(namespace, object_id, position, metadata, options)`
- `get(namespace, object_id)`
- `delete(namespace, object_id)`
- `query_radius(namespace, center, radius, limit)`
- `query_bbox(namespace, min_x, min_y, max_x, max_y, limit)`
- `knn(namespace, center, k)`
- `stats()`
- And more...

## Performance

The client uses `tarpc` over a length-delimited, JSON-serialized transport. Typical latency for local connections is sub-millisecond.

## License

MIT - see [LICENSE](../../LICENSE)
