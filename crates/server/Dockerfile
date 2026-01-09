FROM rustlang/rust:nightly AS builder

WORKDIR /app
COPY . .

# Build the server binary
RUN cargo build --release --bin spatio-server

# Runtime stage
FROM debian:bookworm-slim

# Install OpenSSL for dependencies if needed (though pure rust deps might not need it, it's safer for networking)
RUN apt-get update && apt-get install -y ca-certificates libssl3 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/spatio-server /usr/local/bin/

# Expose the default port
EXPOSE 3000

# Run the server binding to all interfaces
ENTRYPOINT ["spatio-server", "--host", "0.0.0.0"]
