# Builder stage
FROM rustlang/rust:nightly AS builder

WORKDIR /app
COPY . .

# Build the server binary
RUN cargo build --release --bin spatio-server

# TzData stage
FROM cgr.dev/chainguard/wolfi-base AS tzdata
RUN apk add --no-cache tzdata

# Setup stage (for permissions)
FROM cgr.dev/chainguard/wolfi-base AS setup
RUN mkdir /data \
    && chown 65532:65532 /data \
    && chmod 777 /data

# Final stage
FROM cgr.dev/chainguard/glibc-dynamic:latest

# Copy timezone data
COPY --from=tzdata /usr/share/zoneinfo /usr/share/zoneinfo

# Copy data directory with permissions
COPY --from=setup --chown=65532:65532 /data /data

# Copy binary
COPY --from=builder /app/target/release/spatio-server /usr/local/bin/spatio-server

# User nonroot (65532)
USER 65532:65532

VOLUME ["/data"]

ENTRYPOINT ["spatio-server", "--host", "0.0.0.0"]
CMD ["--data-dir", "/data/spatio.db"]
