# Version Management

This project uses **version-driven releases** with **independent versioning** for the Rust crate and Python package. Each package can have its own version number and release cycle.

## Overview

- **Rust crate** (`spatio`): Core spatial database library
- **Python package** (`spatio`): Python bindings for the Rust library
- **Independent versions**: Each can be released separately with different version numbers
- **Automatic releases**: GitHub Actions detects version changes and creates releases automatically

## How It Works

1. **Update version** using the bump script
2. **Commit and push** to main branch
3. **GitHub Actions detects** version changes
4. **Automatic release** is created and published

No manual tag creation needed!

## Version Management Scripts

### Check Version Status

```bash
# Check current versions and tag status
just check-version
# or
./scripts/check-version.sh
```

This shows:
- Current Rust crate version
- Current Python package version  
- Latest tags for each package
- Version status and readiness for release

### Bump Versions

#### Rust Crate Only
```bash
# Bump Rust crate version
just bump-rust 0.2.1
./scripts/bump-version.sh rust 0.2.1

# Preview changes (dry run)
just bump-rust-dry 0.2.1
```

#### Python Package Only
```bash
# Bump Python package version
just bump-python 0.1.5
./scripts/bump-version.sh python 0.1.5

# Preview changes (dry run)
just bump-python-dry 0.1.5
```

#### Both Packages (Same Version)
```bash
# Bump both to same version
just bump-both 1.0.0
./scripts/bump-version.sh both 1.0.0

# Preview changes (dry run)
just bump-both-dry 1.0.0
```

## Automatic Release Strategy

GitHub Actions automatically creates tags and releases when version changes are detected:

- `rust-v1.2.3` - Automatically created for Rust crate releases
- `python-v0.5.1` - Automatically created for Python package releases  

## Typical Workflows

### Independent Development

Most of the time, you'll release packages independently:

```bash
# Rust library gets new features
just bump-rust 0.3.0
git push origin main
# → GitHub Actions automatically creates rust-v0.3.0 release

# Later, Python bindings get updates
just bump-python 0.2.0  
git push origin main
# → GitHub Actions automatically creates python-v0.2.0 release
```

### Synchronized Release

For major releases, you might want to synchronize:

```bash
# Release both with same version
just bump-both 1.0.0
git push origin main
# → GitHub Actions creates both rust-v1.0.0 and python-v1.0.0 releases
```

### Pre-release Versions

Support for alpha, beta, and rc versions:

```bash
# Rust pre-release
just bump-rust 0.4.0-alpha.1

# Python pre-release  
just bump-python 0.3.0-beta.2

# Combined pre-release
just bump-both 2.0.0-rc.1
```

## Automatic Publishing

The GitHub workflow automatically detects version changes and publishes accordingly:

| Version Change | Rust Publish | Python Publish | GitHub Release |
|----------------|--------------|----------------|----------------|
| `Cargo.toml`   | ✅           | ❌             | ✅ (rust-v*)   |
| `py-spatio/Cargo.toml` | ❌   | ✅             | ✅ (python-v*) |
| Both files     | ✅           | ✅             | ✅ (both tags) |

## Manual Workflow Trigger

You can also trigger the workflow manually via GitHub Actions:

1. Go to **Actions** → **Auto Release on Version Change**
2. Click **Run workflow**
3. Select branch: `main`
4. Run workflow (it will detect current version changes)

## Version File Locations

The scripts automatically update versions in:

- **Rust crate**: `Cargo.toml` 
- **Python package**: `py-spatio/Cargo.toml`
- **Lock files**: Both `Cargo.lock` files are updated

## Best Practices

### When to Release Independently

- **Rust only**: Core library changes, performance improvements, new spatial algorithms
- **Python only**: Binding improvements, Python-specific features, documentation updates
- **Both**: Breaking changes, major feature releases, synchronized version numbers

### Semantic Versioning

Both packages follow [Semantic Versioning](https://semver.org/):

- `MAJOR.MINOR.PATCH`
- `MAJOR.MINOR.PATCH-prerelease`

Example progression:
```
Rust:   0.1.0 → 0.1.1 → 0.2.0 → 0.2.1 → 1.0.0
Python: 0.1.0 → 0.1.1 → 0.1.2 → 0.2.0 → 1.0.0
```

### Version Compatibility

The Python package depends on the Rust crate via `path = ".."`, so:

- Python version can be ahead of Rust (new bindings for existing features)
- Python version can be behind Rust (not all features exposed yet)
- Major version bumps should generally be coordinated

## Troubleshooting

### Version Mismatch Errors

If you see version mismatch errors during CI:
```bash
# Check current status
just check-version

# Fix by updating the appropriate package
just bump-rust 0.2.1  # or bump-python, bump-both
```

### Tag Already Exists

If a tag already exists:
```bash
# Check existing tags
git tag -l | grep -E "(rust-v|python-v|^v)"

# Use a different version number
just bump-rust 0.2.2  # instead of 0.2.1
```

### Build Failures

For maturin/Python build issues:
```bash
# Test Python build locally
cd py-spatio
just build

# Check for file conflicts
just py-clean
just build
```

## Examples

### Typical Development Cycle

```bash
# 1. Check current status
just check-version

# 2. Make changes to Rust code
# ... edit src/ files ...

# 3. Release new Rust version
just bump-rust-dry 0.2.1  # preview
just bump-rust 0.2.1      # actual bump
git push origin main
# → Auto-release triggered, rust-v0.2.1 created and published

# 4. Later: update Python bindings
# ... edit py-spatio/src/ files ...

# 5. Release new Python version
just bump-python 0.1.3
git push origin main
# → Auto-release triggered, python-v0.1.3 created and published
```

### Coordinated Release

```bash
# Major release with breaking changes
just bump-both-dry 1.0.0  # preview
just bump-both 1.0.0      # actual bump  
git push origin main
# → Auto-release triggered, both packages released
```

### Version-Driven Workflow Benefits

- **No manual tag management** - Tags are created automatically
- **Consistent releases** - Every version change triggers a release
- **Failed releases don't create tags** - Only successful builds get released
- **Clear audit trail** - Easy to see what triggered each release
- **Prevents forgotten releases** - Can't forget to create a release after version bump

This version-driven approach gives you maximum flexibility while maintaining clear separation between the Rust library and Python bindings release cycles. The automatic detection ensures that every version change results in a proper release without manual intervention.