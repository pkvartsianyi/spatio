# GitHub Actions Workflows

This directory contains the CI/CD workflows for SpatioLite, implementing a pragmatic testing and release strategy.

## Workflows Overview

### `ci.yml` - Daily Development CI
**Triggers:** Every push and pull request  
**Purpose:** Fast feedback for development  
**Runs on:** Linux only (Ubuntu)  
**Duration:** ~5-10 minutes  

**What it does:**
- Rust: Format, lint, test, documentation checks
- Uses stable Rust toolchain only
- Optimized for speed and quick feedback

### `auto-release.yml` - Version-Driven Releases
**Triggers:** Version changes in `Cargo.toml` files  
**Purpose:** Automatic releases when versions are bumped  
**Runs on:** Cross-platform (Linux, Windows, macOS)  
**Duration:** ~30-45 minutes  

**What it does:**
1. **Detects version changes** in Rust/Python packages
2. **Cross-platform testing** - ensures releases work everywhere
3. **Creates GitHub releases** with auto-generated changelogs
4. **Publishes packages** to crates.io and PyPI
5. **Only releases if all tests pass**

### `python.yml` - Python Package CI
**Triggers:** Changes to Python package files  
**Purpose:** Fast Python-specific testing  
**Runs on:** Linux only (Ubuntu)  
**Duration:** ~10-15 minutes  

**What it does:**
- Tests Python bindings across Python 3.9-3.13
- Builds wheels and runs pytest
- Code quality checks (ruff, mypy)
- Performance benchmarks

### `docs.yml` - Documentation
**Triggers:** Documentation changes  
**Purpose:** Build and deploy documentation  
**Runs on:** Linux only (Ubuntu)  

## CI Strategy Philosophy

### Development Speed vs Release Quality

We use a **two-tier approach**:

#### Tier 1: Fast Daily CI
- **Linux-only testing** for speed
- **Catches 95% of issues** in development
- **Quick feedback** to keep developers productive
- **Low cost** in CI minutes

#### Tier 2: Comprehensive Release CI
- **Cross-platform testing** before releases
- **Multiple Python versions** across platforms
- **Thorough validation** ensures release quality
- **Higher cost** but only runs when releasing

### Benefits

**Fast development cycle** - Quick CI feedback  
**High release confidence** - Thorough pre-release testing  
**Cost efficient** - Expensive tests only when needed  
**Automatic releases** - No manual release process  
**Platform coverage** - Windows/macOS issues caught before release

## Usage Examples

### Normal Development
```bash
# Make changes, commit, push
git add .
git commit -m "feat: add new feature"
git push origin feature-branch
# → Fast Linux CI runs (~5 minutes)
```

### Creating a Release
```bash
# Bump version using script
just bump-rust 0.2.1
# → Commits version change

git push origin main
# → Auto-release workflow triggers:
#   1. Cross-platform testing
#   2. GitHub release creation
#   3. Package publication
```

### Manual Testing
```bash
# Run comprehensive tests locally
just security-audit
just benchmarks
just coverage
just test-examples
```

## Workflow Dependencies

```mermaid
graph TD
    A[Version Bump] --> B[Push to main]
    B --> C[Detect Changes]
    C --> D[Cross-Platform Tests]
    D --> E[Create Release]
    E --> F[Publish Packages]
    
    G[Daily Development] --> H[Fast Linux CI]
    H --> I[Merge/Deploy]
```

## Configuration

### Secrets Required
- `CARGO_REGISTRY_TOKEN` - For crates.io publishing
- `PYPI_API_TOKEN` - For PyPI publishing

### Repository Settings
- Branch protection on `main`
- Require PR reviews
- Require status checks to pass

## Monitoring

### Success Indicators
- Green CI badges on README
- Successful releases in GitHub Releases
- Packages published to registries
- No failed deployments

### Common Issues
- **Cross-platform test failures** - Usually Windows/macOS specific
- **Publishing failures** - Often token or permission issues
- **Version detection issues** - Check Cargo.toml format

## Maintenance

### Regular Tasks
- Update action versions monthly
- Review and update Python version matrix
- Monitor CI performance and costs
- Update documentation as workflows evolve

### Cost Optimization
- Linux-only daily CI keeps costs low
- Cross-platform testing only on releases
- Efficient caching strategies
- Fail-fast on obvious issues

This strategy balances developer productivity with release quality while keeping CI costs reasonable.