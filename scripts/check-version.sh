#!/bin/bash

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Function to print colored output
print_info() {
    echo -e "${BLUE}INFO:${NC} $1"
}

print_success() {
    echo -e "${GREEN}SUCCESS:${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}WARNING:${NC} $1"
}

print_error() {
    echo -e "${RED}ERROR:${NC} $1"
}

# Change to root directory
cd "$ROOT_DIR"

# Check if we're in a git repository
if ! git rev-parse --git-dir > /dev/null 2>&1; then
    print_error "Not in a git repository"
    exit 1
fi

# Get current versions
if [[ ! -f "Cargo.toml" ]]; then
    print_error "Cargo.toml not found"
    exit 1
fi

if [[ ! -f "py-spatio/Cargo.toml" ]]; then
    print_error "py-spatio/Cargo.toml not found"
    exit 1
fi

RUST_VERSION=$(grep '^version = ' Cargo.toml | head -1 | sed 's/version = "\(.*\)"/\1/')
PYTHON_VERSION=$(grep '^version = ' py-spatio/Cargo.toml | head -1 | sed 's/version = "\(.*\)"/\1/')

# Get latest git tag
LATEST_TAG=$(git describe --tags --abbrev=0 2>/dev/null || echo "none")
LATEST_TAG_VERSION=${LATEST_TAG#v}  # Remove 'v' prefix

print_info "Version Check Report"
print_info "==================="
print_info ""
print_info "Rust crate version:    $RUST_VERSION"
print_info "Python package version: $PYTHON_VERSION"
print_info "Latest git tag:        $LATEST_TAG"
print_info ""

# Check version consistency
VERSIONS_CONSISTENT=true

if [[ "$RUST_VERSION" != "$PYTHON_VERSION" ]]; then
    print_error "Version mismatch between Rust and Python packages!"
    print_error "  Rust:   $RUST_VERSION"
    print_error "  Python: $PYTHON_VERSION"
    VERSIONS_CONSISTENT=false
fi

if [[ "$LATEST_TAG" != "none" && "$RUST_VERSION" != "$LATEST_TAG_VERSION" ]]; then
    if [[ "$RUST_VERSION" > "$LATEST_TAG_VERSION" ]]; then
        print_info "Rust version ($RUST_VERSION) is newer than latest tag ($LATEST_TAG_VERSION)"
        print_info "This is normal if you're preparing a new release"
    else
        print_warning "Rust version ($RUST_VERSION) is older than latest tag ($LATEST_TAG_VERSION)"
        print_warning "This might indicate a problem"
    fi
fi

# Check for uncommitted changes
if ! git diff --quiet || ! git diff --cached --quiet; then
    print_info "Uncommitted changes detected:"
    git status --short
    print_info ""
fi

# Summary
if [[ "$VERSIONS_CONSISTENT" == true ]]; then
    print_success "All versions are consistent!"

    if [[ "$LATEST_TAG" == "none" ]]; then
        print_info "No tags found. Ready to create first release with version $RUST_VERSION"
    elif [[ "$RUST_VERSION" == "$LATEST_TAG_VERSION" ]]; then
        print_info "Current version matches latest tag. No release needed unless changes were made."
    else
        print_info "Ready to create new release with version $RUST_VERSION"
    fi
else
    print_error "Version inconsistencies found! Please fix before releasing."
    exit 1
fi

print_info ""
print_info "To bump version: ./scripts/bump-version.sh <new_version>"
print_info "To see what would change: ./scripts/bump-version.sh <new_version> --dry-run"
