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
- `--auth-token` / `SPATIO_AUTH_TOKEN`: Token clients must present. Required when binding a non-loopback address unless `--insecure` is passed.
- `--tls-cert`, `--tls-key` / `SPATIO_TLS_CERT`, `SPATIO_TLS_KEY`: PEM certificate chain and key; enables TLS.

Docker binds `0.0.0.0`, so it needs `-e SPATIO_AUTH_TOKEN=...` (and ideally TLS files mounted into the container).

## Client Access

Use the Rust [`spatio-client`](../client) crate:
```rust
use spatio_client::{ClientOptions, SpatioClient};

let client = SpatioClient::connect_with(
    "127.0.0.1:3000".parse()?,
    ClientOptions { auth_token: Some(token), tls: None },
).await?;
```

For TLS, set `tls` to a `rustls::ClientConfig` (re-exported as `spatio_client::rustls`) and the server name to verify.

## Limits

Per request: 64 KiB of metadata, 10,000 trajectory points, 256-byte namespace/id, 10,000 query results, 8 MiB frames.

## License

MIT - see [LICENSE](../../LICENSE)
