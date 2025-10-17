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

# Get latest git tags
LATEST_RUST_TAG=$(git tag -l "rust-v*" | sort -V | tail -1 2>/dev/null || echo "none")
LATEST_PYTHON_TAG=$(git tag -l "python-v*" | sort -V | tail -1 2>/dev/null || echo "none")
LATEST_COMBINED_TAG=$(git tag -l "v*" | grep -v "rust-v" | grep -v "python-v" | sort -V | tail -1 2>/dev/null || echo "none")

LATEST_RUST_TAG_VERSION=${LATEST_RUST_TAG#rust-v}
LATEST_PYTHON_TAG_VERSION=${LATEST_PYTHON_TAG#python-v}
LATEST_COMBINED_TAG_VERSION=${LATEST_COMBINED_TAG#v}

print_info "Version Check Report"
print_info "==================="
print_info ""
print_info "Rust crate version:     $RUST_VERSION"
print_info "Python package version: $PYTHON_VERSION"
print_info ""
print_info "Latest Rust tag:        $LATEST_RUST_TAG"
print_info "Latest Python tag:      $LATEST_PYTHON_TAG"
print_info "Latest combined tag:    $LATEST_COMBINED_TAG"
print_info ""

# Check version consistency - packages can have different versions
VERSIONS_CONSISTENT=true

print_info "Version Status:"
print_info "--------------"

# Check Rust version against its tag
if [[ "$LATEST_RUST_TAG" != "none" ]]; then
    if [[ "$RUST_VERSION" == "$LATEST_RUST_TAG_VERSION" ]]; then
        print_success "Rust version matches latest Rust tag"
    elif [[ "$RUST_VERSION" > "$LATEST_RUST_TAG_VERSION" ]]; then
        print_info "Rust version ($RUST_VERSION) is newer than latest Rust tag ($LATEST_RUST_TAG_VERSION)"
        print_info "Ready for new Rust release"
    else
        print_warning "Rust version ($RUST_VERSION) is older than latest Rust tag ($LATEST_RUST_TAG_VERSION)"
    fi
else
    print_info "No Rust-specific tags found. Ready for first Rust release."
fi

# Check Python version against its tag
if [[ "$LATEST_PYTHON_TAG" != "none" ]]; then
    if [[ "$PYTHON_VERSION" == "$LATEST_PYTHON_TAG_VERSION" ]]; then
        print_success "Python version matches latest Python tag"
    elif [[ "$PYTHON_VERSION" > "$LATEST_PYTHON_TAG_VERSION" ]]; then
        print_info "Python version ($PYTHON_VERSION) is newer than latest Python tag ($LATEST_PYTHON_TAG_VERSION)"
        print_info "Ready for new Python release"
    else
        print_warning "Python version ($PYTHON_VERSION) is older than latest Python tag ($LATEST_PYTHON_TAG_VERSION)"
    fi
else
    print_info "No Python-specific tags found. Ready for first Python release."
fi

# Check for uncommitted changes
if ! git diff --quiet || ! git diff --cached --quiet; then
    print_info "Uncommitted changes detected:"
    git status --short
    print_info ""
fi

# Summary
print_success "Version check completed!"
print_info ""
print_info "Available commands:"
print_info "  Rust only:   ./scripts/bump-version.sh rust <version>"
print_info "  Python only: ./scripts/bump-version.sh python <version>"
print_info "  Both:        ./scripts/bump-version.sh both <version>"
print_info ""
print_info "Dry run:       ./scripts/bump-version.sh <package> <version> --dry-run"
