# Spatio Server

A lightweight TCP server for remote access to the Spatio spatio-temporal database.

## Overview

The Spatio Server provides a high-performance, framed TCP interface for interacting with a Spatio database instance from multiple processes or remote machines. It supports the full Spatio API via a custom binary protocol (SBP).

## Running the Server

### Using Cargo
```bash
cargo run --package spatio-server -- --host 127.0.0.1 --port 3000
```

### Options
- `--host`: Bind address (default: `127.0.0.1`)
- `--port`: Port to listen on (default: `3000`)
- `--path`: Path to the persistent database file (default: `:memory:`)
- `--buffer-capacity`: Size of the in-memory trajectory buffer (default: `1000`)

## Client Access

### Python
Use the `SpatioClient` from the `spatio` package:
```python
import spatio
client = spatio.Spatio.server(host="127.0.0.1", port=3000)
```

## Security Note

The current version of Spatio Server does not include TLS or authentication. It is intended for use in trusted private networks or behind a secure proxy.

## License

MIT - see [LICENSE](../../LICENSE)
