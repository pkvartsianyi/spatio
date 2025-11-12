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

# Get current versions from individual crates
if [[ ! -f "crates/core/Cargo.toml" ]]; then
    print_error "crates/core/Cargo.toml not found"
    exit 1
fi

if [[ ! -f "crates/py/Cargo.toml" ]]; then
    print_error "crates/py/Cargo.toml not found"
    exit 1
fi

if [[ ! -f "crates/types/Cargo.toml" ]]; then
    print_error "crates/types/Cargo.toml not found"
    exit 1
fi

CORE_VERSION=$(grep '^version = ' crates/core/Cargo.toml | head -1 | sed 's/version = "\(.*\)"/\1/')
PYTHON_VERSION=$(grep '^version = ' crates/py/Cargo.toml | head -1 | sed 's/version = "\(.*\)"/\1/')
TYPES_VERSION=$(grep '^version = ' crates/types/Cargo.toml | head -1 | sed 's/version = "\(.*\)"/\1/')

# Get latest git tags
LATEST_CORE_TAG=$(git tag -l "core-v*" | sort -V | tail -1 2>/dev/null || echo "none")
LATEST_PYTHON_TAG=$(git tag -l "python-v*" | sort -V | tail -1 2>/dev/null || echo "none")
LATEST_TYPES_TAG=$(git tag -l "types-v*" | sort -V | tail -1 2>/dev/null || echo "none")

LATEST_CORE_TAG_VERSION=${LATEST_CORE_TAG#core-v}
LATEST_PYTHON_TAG_VERSION=${LATEST_PYTHON_TAG#python-v}
LATEST_TYPES_TAG_VERSION=${LATEST_TYPES_TAG#types-v}

print_info "Version Check Report"
print_info "==================="
print_info ""
print_info "spatio (core) version:  $CORE_VERSION"
print_info "spatio-py version:      $PYTHON_VERSION"
print_info "spatio-types version:   $TYPES_VERSION"
print_info ""
print_info "Latest core tag:        $LATEST_CORE_TAG"
print_info "Latest Python tag:      $LATEST_PYTHON_TAG"
print_info "Latest types tag:       $LATEST_TYPES_TAG"
print_info ""

print_info "Version Status:"
print_info "--------------"

# Check core version against its tag
if [[ "$LATEST_CORE_TAG" != "none" ]]; then
    if [[ "$CORE_VERSION" == "$LATEST_CORE_TAG_VERSION" ]]; then
        print_success "spatio (core) version matches latest tag"
    elif [[ "$CORE_VERSION" > "$LATEST_CORE_TAG_VERSION" ]]; then
        print_info "spatio (core) version ($CORE_VERSION) is newer than latest tag ($LATEST_CORE_TAG_VERSION)"
        print_info "✓ Ready for new core release"
    else
        print_warning "spatio (core) version ($CORE_VERSION) is older than latest tag ($LATEST_CORE_TAG_VERSION)"
    fi
else
    print_info "No core-specific tags found. Ready for first core release."
fi

# Check Python version against its tag
if [[ "$LATEST_PYTHON_TAG" != "none" ]]; then
    if [[ "$PYTHON_VERSION" == "$LATEST_PYTHON_TAG_VERSION" ]]; then
        print_success "spatio-py version matches latest tag"
    elif [[ "$PYTHON_VERSION" > "$LATEST_PYTHON_TAG_VERSION" ]]; then
        print_info "spatio-py version ($PYTHON_VERSION) is newer than latest tag ($LATEST_PYTHON_TAG_VERSION)"
        print_info "✓ Ready for new Python release"
    else
        print_warning "spatio-py version ($PYTHON_VERSION) is older than latest tag ($LATEST_PYTHON_TAG_VERSION)"
    fi
else
    print_info "No Python-specific tags found. Ready for first Python release."
fi

# Check Types version against its tag
if [[ "$LATEST_TYPES_TAG" != "none" ]]; then
    if [[ "$TYPES_VERSION" == "$LATEST_TYPES_TAG_VERSION" ]]; then
        print_success "spatio-types version matches latest tag"
    elif [[ "$TYPES_VERSION" > "$LATEST_TYPES_TAG_VERSION" ]]; then
        print_info "spatio-types version ($TYPES_VERSION) is newer than latest tag ($LATEST_TYPES_TAG_VERSION)"
        print_info "✓ Ready for new types release"
    else
        print_warning "spatio-types version ($TYPES_VERSION) is older than latest tag ($LATEST_TYPES_TAG_VERSION)"
    fi
else
    print_info "No types-specific tags found. Ready for first types release."
fi

# Check workspace dependency consistency
print_info ""
print_info "Workspace Dependencies:"
print_info "----------------------"

WORKSPACE_TYPES_VERSION=$(grep '^spatio-types = ' Cargo.toml | sed 's/.*version = "\([^"]*\)".*/\1/')
if [[ "$WORKSPACE_TYPES_VERSION" == "$TYPES_VERSION" ]]; then
    print_success "Workspace spatio-types dependency matches crate version ($TYPES_VERSION)"
else
    print_warning "Workspace spatio-types dependency ($WORKSPACE_TYPES_VERSION) differs from crate version ($TYPES_VERSION)"
    print_warning "Run: ./scripts/bump-version.sh types $TYPES_VERSION"
fi

# Check for uncommitted changes
print_info ""
if ! git diff --quiet || ! git diff --cached --quiet; then
    print_info "Uncommitted changes detected:"
    git status --short
    print_info ""
fi

# Summary
print_success "Version check completed!"
print_info ""
print_info "Available commands:"
print_info "  Core only:   ./scripts/bump-version.sh core <version>"
print_info "  Python only: ./scripts/bump-version.sh python <version>"
print_info "  Types only:  ./scripts/bump-version.sh types <version>"
print_info "  All:         ./scripts/bump-version.sh all <version>"
print_info ""
print_info "Dry run:       ./scripts/bump-version.sh <package> <version> --dry-run"
