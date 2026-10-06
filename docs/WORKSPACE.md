# Spatio Workspace Structure

Spatio is a Cargo workspace:

```
spatio/
├── Cargo.toml              # Workspace root
├── crates/
│   ├── core/               # spatio: the embedded database
│   ├── types/              # spatio-types: shared spatial/temporal types
│   ├── server/             # spatio-server: TCP (tarpc) server
│   ├── client/             # spatio-client: Rust client for spatio-server
│   ├── cabi/               # spatio-cabi: C ABI used by the Go bindings
│   └── benchmarks/         # spatio-benchmarks: bench_core / bench_server
├── bindings/
│   ├── python/             # spatio-py, published to PyPI as `spatio`
│   └── go/                 # Go bindings (purego) over spatio-cabi
├── tests/                  # spatio-integration-tests
├── docs/
└── scripts/
```

Each crate has its own `version` in its `Cargo.toml`; `edition`, `rust-version`,
`license` and `repository` are inherited from `[workspace.package]`. Internal
crates are referenced through `[workspace.dependencies]` in the root `Cargo.toml`.
The Go bindings version lives in `bindings/go/VERSION`.

## Common Commands

```bash
cargo fmt --all
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features --exclude spatio-py

cargo run -p spatio --example getting_started   # see crates/core/examples/
just bench-core                                 # core benchmark
```

`spatio-py` is excluded from `cargo test` because PyO3's `extension-module`
test binaries don't link; test it from Python instead:

```bash
cd bindings/python
maturin develop
pytest
```

The `justfile` wraps these (`just test`, `just lint`, `just py-test`, `just go-test`).

## Troubleshooting

**PyO3 rejects a newer Python than it supports:**

```bash
export PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1
```
