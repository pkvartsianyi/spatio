# Scripts

Helper scripts for version management.

## bump-version.sh

Update package versions.

```bash
./scripts/bump-version.sh <package> <version>
```

**Packages:**
- `rust` - Main Rust crate
- `python` - Python bindings
- `types` - Spatio-types crate
- `all` - Bump everything to same version

**Options:**
- `--dry-run` - Preview changes
- `--no-commit` - Update files without committing

**Examples:**
```bash
# Bump Rust to 0.2.1
./scripts/bump-version.sh rust 0.2.1

# Preview Python bump
./scripts/bump-version.sh python 0.1.5 --dry-run

# Bump everything to 1.0.0
./scripts/bump-version.sh all 1.0.0
```

## check-version.sh

Check current versions and release status.

```bash
./scripts/check-version.sh
```

## Using with just

```bash
just check-version
just bump-rust 0.2.1
just bump-python 0.1.5
just bump-all 1.0.0
```

## Auto-Release

When you bump a version and push to `main`, GitHub Actions:
1. Detects version changes
2. Runs tests
3. Creates release with tag (`rust-v1.2.3` or `python-v0.5.1`)
4. Publishes to crates.io/PyPI

## Version Format

Use semantic versioning: `MAJOR.MINOR.PATCH`

```bash
# Valid
0.1.0
1.2.3
2.0.0-alpha.1
1.0.0-rc.1

# Invalid
v0.1.0        # No 'v' prefix
0.1           # Need patch version
```
