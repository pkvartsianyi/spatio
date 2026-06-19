# Spatio Server

A lightweight TCP server for remote access to the Spatio spatio-temporal database.

## Overview

The Spatio Server provides a TCP interface for interacting with a Spatio database instance from multiple processes or remote machines. It exposes the full Spatio API as JSON-RPC (via `tarpc`) over TCP.

## Running the Server

### Using Cargo
```bash
cargo run --package spatio-server -- --host 127.0.0.1 --port 3000 --data-dir ./data
```

### Options
- `--host`: Bind address (default: `127.0.0.1`)
- `--port`: Port to listen on (default: `3000`)
- `--data-dir`: Directory for the persistent database. If omitted, the server runs in-memory.

## Client Access

Use the Rust [`spatio-client`](../client) crate:
```rust
let client = spatio_client::SpatioClient::connect("127.0.0.1:3000".parse()?).await?;
```

## Security Note

The current version of Spatio Server does not include TLS or authentication. It is intended for use in trusted private networks or behind a secure proxy.

## License

MIT - see [LICENSE](../../LICENSE)
