# Version Management Scripts

This directory contains scripts for managing versions and releases in the SpatioLite project.

## Scripts

### `bump-version.sh`

Main script for updating package versions.

**Usage:**
```bash
./scripts/bump-version.sh <package> <new_version> [options]
```

**Arguments:**
- `<package>`: Which package to bump (`rust`, `python`, or `both`)
- `<new_version>`: The new version (e.g., `0.1.1`, `0.2.0-alpha.1`)

**Options:**
- `--dry-run`: Show what would change without making changes
- `--no-commit`: Update files but don't commit
- `--help`: Show help message

**Examples:**
```bash
# Bump Rust crate version
./scripts/bump-version.sh rust 0.2.1

# Bump Python package version
./scripts/bump-version.sh python 0.1.5

# Bump both to same version
./scripts/bump-version.sh both 1.0.0

# Preview changes without making them
./scripts/bump-version.sh rust 0.3.0 --dry-run
```

### `check-version.sh`

Script for checking current version status.

**Usage:**
```bash
./scripts/check-version.sh
```

Shows:
- Current Rust crate version
- Current Python package version
- Latest release tags for each package
- Version status and readiness for release

## Justfile Integration

For convenience, these scripts are also available as `just` commands:

```bash
# Check versions
just check-version

# Bump versions
just bump-rust 0.2.1
just bump-python 0.1.5
just bump-both 1.0.0

# Dry run (preview changes)
just bump-rust-dry 0.2.1
just bump-python-dry 0.1.5
just bump-both-dry 1.0.0

# Update without committing
just bump-rust-no-commit 0.2.1
```

## Automatic Releases

When you bump a version and push to `main`, GitHub Actions automatically:

1. **Detects version changes** in `Cargo.toml` files
2. **Runs tests** for changed packages
3. **Creates releases** with appropriate tags
4. **Publishes packages** to registries

**Tag Format:**
- Rust changes: `rust-v1.2.3`
- Python changes: `python-v0.5.1`

## Version Format

Both packages follow [Semantic Versioning](https://semver.org/):

- `MAJOR.MINOR.PATCH` (e.g., `1.0.0`)
- `MAJOR.MINOR.PATCH-prerelease` (e.g., `1.0.0-alpha.1`)

**Supported prerelease identifiers:**
- `alpha` - Early development
- `beta` - Feature complete, testing
- `rc` - Release candidate

## Workflow

### Basic Version Update

1. **Check current status:**
   ```bash
   just check-version
   ```

2. **Update version:**
   ```bash
   just bump-rust 0.2.1  # or bump-python, bump-both
   ```

3. **Push to trigger release:**
   ```bash
   git push origin main
   ```

### Independent Versioning

Rust and Python packages can have different versions:

```bash
# Rust at v0.3.0, Python at v0.1.2
just bump-rust 0.3.1      # Only Rust updated
just bump-python 0.1.3    # Only Python updated
```

### Synchronized Versioning

For major releases, use same version:

```bash
just bump-both 1.0.0      # Both packages updated to same version
```

## Files Updated

The scripts automatically update:

- **Rust package**: `Cargo.toml`, `Cargo.lock`
- **Python package**: `py-spatio/Cargo.toml`, `py-spatio/Cargo.lock`
- **Git**: Commit with version change message

## Requirements

- **Git**: For version control operations
- **Rust/Cargo**: For updating lock files
- **Bash**: Scripts are written in Bash

## Troubleshooting

### Version Format Errors

Ensure versions follow semantic versioning:
```bash
# Valid formats
0.1.0
1.2.3
2.0.0-alpha.1
1.0.0-beta.2
1.0.0-rc.1

# Invalid formats
v0.1.0        # No 'v' prefix
0.1           # Missing patch version
1.0.0-ALPHA   # Uppercase prerelease
```

### Uncommitted Changes

Scripts require a clean working directory:
```bash
# Commit or stash changes first
git add .
git commit -m "your changes"

# Then run version script
just bump-rust 0.2.1
```

### Missing Dependencies

If scripts fail, ensure you have:
```bash
# Check git
git --version

# Check cargo
cargo --version

# Make scripts executable
chmod +x scripts/*.sh
```
