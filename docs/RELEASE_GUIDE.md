# Release Guide

Releases are automated by `.github/workflows/auto-release.yml`. On every push to
`main` that touches a crate's `Cargo.toml` (or `bindings/go/VERSION`), the workflow
waits for CI to pass, then releases each component whose version has no tag yet.

| Component | Version source | Tag | Published to |
|-----------|----------------|-----|--------------|
| spatio-types | `crates/types/Cargo.toml` | `types-v<ver>` | crates.io |
| spatio | `crates/core/Cargo.toml` | `core-v<ver>` | crates.io + GitHub Release |
| spatio-server | `crates/server/Cargo.toml` | `server-v<ver>` | crates.io + `ghcr.io` image |
| spatio-client | `crates/client/Cargo.toml` | `client-v<ver>` | crates.io |
| spatio-cabi | `crates/cabi/Cargo.toml` | `cabi-v<ver>` | crates.io |
| spatio-py | `bindings/python/Cargo.toml` | `python-v<ver>` | PyPI (`spatio`) |
| Go bindings | `bindings/go/VERSION` | `bindings/go/v<ver>` | GitHub Release (native libs) |

Crates are published in dependency order (types → core → server → client;
cabi after core). Publishing is idempotent: a version already on crates.io is
skipped and the tag is created only if missing, so a failed run can be re-run.

## Bumping Versions

Requires [cargo-edit](https://github.com/killercup/cargo-edit) (`cargo install cargo-edit`).

```bash
just bump spatio 0.3.10   # cargo set-version -p spatio 0.3.10 (+ release benchmark)
just bump-go 0.1.1
just patch-all            # patch-bump every crate and the Go bindings
```

`cargo set-version` also updates `[workspace.dependencies]` and `Cargo.lock`.
Commit the result and merge to `main`.

The Python package version comes from `bindings/python/Cargo.toml`
(`dynamic = ["version"]` in `pyproject.toml`).

## Core Release Benchmarks

Bumping `spatio` runs `scripts/bench-release.sh <version>`, which runs `bench_core`
and writes `crates/benchmarks/results/core-v<version>.{json,md}` with a comparison
to the previous version. Commit both files; CI uses the `.md` as the body of the
`core-v<version>` GitHub Release. Run it standalone with `just bench-release <version>`.

## Configuration

- `CRATES_IO_TOKEN` secret: crates.io API token.
- PyPI uses trusted publishing (OIDC); no token secret is needed.
