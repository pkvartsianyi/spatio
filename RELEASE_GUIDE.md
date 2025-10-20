# Release Guide for SpatioLite

This guide explains how to create new releases for both the Rust core library and the Python bindings.

## Overview

SpatioLite uses an automated release workflow (`.github/workflows/auto-release.yml`) that:
- Automatically detects version changes
- Creates GitHub releases
- Publishes to crates.io (Rust) and PyPI (Python)
- Only triggers when a new version tag doesn't exist remotely

## Prerequisites

Before releasing, ensure:
- [ ] All tests pass locally: `cargo test --all`
- [ ] Python tests pass: `cd py-spatio && python -m pytest`
- [ ] Lints pass: `cargo clippy --all-targets -- -D warnings`
- [ ] Code is formatted: `cargo fmt --all -- --check`
- [ ] Documentation builds: `cargo doc --no-deps`
- [ ] CHANGELOG is updated (if applicable)

## Releasing the Rust Core Library

### 1. Update Version

Edit `Cargo.toml`:
```toml
[package]
version = "0.1.0-alpha.11"  # Bump from current version
```

### 2. Commit and Push

```bash
git add Cargo.toml
git commit -m "chore: bump version to 0.1.0-alpha.11"
git push origin main
```

### 3. Automated Release

The CI workflow will:
1. Detect that `rust-v0.1.0-alpha.11` doesn't exist
2. Run tests on Linux, macOS, and Windows
3. Create a Git tag `rust-v0.1.0-alpha.11`
4. Create a GitHub release
5. Publish to crates.io (if `CRATES_IO_TOKEN` is configured)

### 4. Verify Release

Check:
- GitHub Actions workflow completed successfully
- GitHub release is created: https://github.com/USERNAME/SpatioLite/releases
- Package appears on crates.io: https://crates.io/crates/spatiolite

## Releasing the Python Package

### 1. Update Version

Edit `py-spatio/Cargo.toml`:
```toml
[package]
version = "0.1.0-alpha.11"  # Bump from current version
```

### 2. Update Python Package Metadata (Optional)

If you need to update Python-specific metadata, edit `py-spatio/pyproject.toml`:
```toml
[project]
version = "0.1.0-alpha.11"  # Should match Cargo.toml
```

### 3. Commit and Push

```bash
git add py-spatio/Cargo.toml py-spatio/pyproject.toml
git commit -m "chore(python): bump version to 0.1.0-alpha.11"
git push origin main
```

### 4. Automated Release

The CI workflow will:
1. Detect that `python-v0.1.0-alpha.11` doesn't exist
2. Run tests on multiple OS/Python combinations
3. Create a Git tag `python-v0.1.0-alpha.11`
4. Build Python wheels using `maturin`
5. Create a GitHub release
6. Publish to PyPI (if `PYPI_API_TOKEN` is configured)

### 5. Verify Release

Check:
- GitHub Actions workflow completed successfully
- GitHub release is created: https://github.com/USERNAME/SpatioLite/releases
- Package appears on PyPI: https://pypi.org/project/spatio/

## Versioning Strategy

SpatioLite follows [Semantic Versioning](https://semver.org/):

- **Major version (X.0.0)**: Breaking changes
- **Minor version (0.X.0)**: New features, backward compatible
- **Patch version (0.0.X)**: Bug fixes, backward compatible

### Pre-release Tags

During early development:
- `0.1.0-alpha.X`: Alpha releases (unstable API)
- `0.1.0-beta.X`: Beta releases (API stabilizing)
- `0.1.0-rc.X`: Release candidates (production-ready testing)

Once stable:
- `1.0.0`: First stable release

## Troubleshooting

### Release Didn't Trigger

**Problem**: You pushed a commit but the release didn't trigger.

**Solution**: Check that:
1. The version in `Cargo.toml` was actually changed
2. The corresponding tag doesn't already exist: `git ls-remote --tags origin | grep <tag-name>`
3. You pushed to the `main` branch
4. The workflow file path trigger matches the file you changed

### Tag Already Exists Error

**Problem**: Workflow fails saying "Tag already exists remotely."

**Solution**: This is expected behavior! The workflow detected that the version hasn't changed. To create a new release:
1. Bump the version number to a new value
2. Commit and push again

### Publish Failed

**Problem**: Tests passed but publishing failed.

**Solution**: Check:
1. `CRATES_IO_TOKEN` secret is configured (for Rust)
2. `PYPI_API_TOKEN` secret is configured (for Python)
3. You have permission to publish to the package
4. The package name isn't already taken

## Manual Release (Emergency)

If automated release fails, you can release manually:

### Manual Rust Release

```bash
# Create and push tag
git tag rust-v0.1.0-alpha.11
git push origin rust-v0.1.0-alpha.11

# Publish to crates.io
cargo publish
```

### Manual Python Release

```bash
# Create and push tag
git tag python-v0.1.0-alpha.11
git push origin python-v0.1.0-alpha.11

# Build and publish
cd py-spatio
maturin build --release
maturin publish
```

## Release Checklist

Before each release:

- [ ] Update version in appropriate `Cargo.toml`
- [ ] Run full test suite
- [ ] Update CHANGELOG (if maintained)
- [ ] Review and update documentation
- [ ] Commit with descriptive message
- [ ] Push to main branch
- [ ] Monitor CI workflow
- [ ] Verify release artifacts
- [ ] Test installation from registry
- [ ] Announce release (if applicable)

## Configuration

### Required Secrets

For automated publishing, configure these secrets in GitHub:
- `CRATES_IO_TOKEN`: Token from https://crates.io/settings/tokens
- `PYPI_API_TOKEN`: Token from https://pypi.org/manage/account/token/

### Optional Variables

- `DRY_RUN`: Set to `true` to test release workflow without publishing

## Related Documentation

- [CI Workflow Fix](CI_WORKFLOW_FIX.md) - Details on tag-based version detection
- [GitHub Actions Workflow](.github/workflows/auto-release.yml) - Full workflow definition
- [Cargo Documentation](https://doc.rust-lang.org/cargo/reference/publishing.html) - Publishing Rust crates
- [Maturin Guide](https://www.maturin.rs/) - Building and publishing Python packages from Rust