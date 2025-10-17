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

# Function to show usage
show_usage() {
    cat << EOF
Usage: $0 <new_version> [options]

ARGUMENTS:
    <new_version>    The new version to set (e.g., 0.1.1, 0.2.0-alpha.1, 1.0.0-beta.2)

OPTIONS:
    --dry-run       Show what would be changed without making actual changes
    --no-tag        Update versions but don't create git tag
    --no-commit     Update versions but don't commit changes
    --help, -h      Show this help message

EXAMPLES:
    $0 0.1.1                    # Bump to version 0.1.1 and create tag
    $0 0.2.0-alpha.1 --dry-run # Show what would change for pre-release
    $0 0.1.2 --no-tag          # Update versions but don't create tag

The script will update versions in:
    - Cargo.toml (main project)
    - py-spatio/Cargo.toml (Python bindings)
    - Any other version references found

EOF
}

# Parse command line arguments
NEW_VERSION=""
DRY_RUN=false
NO_TAG=false
NO_COMMIT=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --no-tag)
            NO_TAG=true
            shift
            ;;
        --no-commit)
            NO_COMMIT=true
            shift
            ;;
        --help|-h)
            show_usage
            exit 0
            ;;
        -*)
            print_error "Unknown option: $1"
            show_usage
            exit 1
            ;;
        *)
            if [[ -z "$NEW_VERSION" ]]; then
                NEW_VERSION="$1"
            else
                print_error "Too many arguments. Version already set to '$NEW_VERSION'"
                show_usage
                exit 1
            fi
            shift
            ;;
    esac
done

# Validate arguments
if [[ -z "$NEW_VERSION" ]]; then
    print_error "New version is required"
    show_usage
    exit 1
fi

# Validate version format
if ! [[ "$NEW_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9]+(\.[0-9]+)?)?$ ]]; then
    print_error "Invalid version format: $NEW_VERSION"
    print_error "Expected format: X.Y.Z or X.Y.Z-prerelease (e.g., 1.0.0, 1.0.0-alpha.1)"
    exit 1
fi

# Change to root directory
cd "$ROOT_DIR"

# Check if we're in a git repository
if ! git rev-parse --git-dir > /dev/null 2>&1; then
    print_error "Not in a git repository"
    exit 1
fi

# Check for uncommitted changes
if [[ "$DRY_RUN" == false && "$NO_COMMIT" == false ]]; then
    if ! git diff --quiet || ! git diff --cached --quiet; then
        print_error "You have uncommitted changes. Please commit or stash them first."
        git status --short
        exit 1
    fi
fi

# Get current versions
CURRENT_RUST_VERSION=$(grep '^version = ' Cargo.toml | head -1 | sed 's/version = "\(.*\)"/\1/')
CURRENT_PYTHON_VERSION=$(grep '^version = ' py-spatio/Cargo.toml | head -1 | sed 's/version = "\(.*\)"/\1/')

print_info "Current versions:"
print_info "  Rust crate: $CURRENT_RUST_VERSION"
print_info "  Python package: $CURRENT_PYTHON_VERSION"
print_info ""
print_info "New version: $NEW_VERSION"

# Check if tag already exists
if git tag -l | grep -q "^v${NEW_VERSION}$"; then
    print_error "Tag v${NEW_VERSION} already exists"
    exit 1
fi

# Files to update
declare -a FILES_TO_UPDATE=(
    "Cargo.toml"
    "py-spatio/Cargo.toml"
)

# Function to update version in file
update_version_in_file() {
    local file="$1"
    local new_version="$2"

    if [[ ! -f "$file" ]]; then
        print_warning "File not found: $file"
        return 1
    fi

    local current_version=$(grep '^version = ' "$file" | head -1 | sed 's/version = "\(.*\)"/\1/')

    if [[ "$DRY_RUN" == true ]]; then
        print_info "Would update $file: $current_version -> $new_version"
    else
        print_info "Updating $file: $current_version -> $new_version"

        # Create backup
        cp "$file" "$file.backup"

        # Update version
        if sed -i.tmp "s/^version = \".*\"/version = \"$new_version\"/" "$file"; then
            rm "$file.tmp" 2>/dev/null || true
            rm "$file.backup"
        else
            print_error "Failed to update $file"
            mv "$file.backup" "$file" 2>/dev/null || true
            return 1
        fi
    fi
}

# Update versions in all files
print_info "Updating version files..."
for file in "${FILES_TO_UPDATE[@]}"; do
    update_version_in_file "$file" "$NEW_VERSION"
done

# Update Cargo.lock files
if [[ "$DRY_RUN" == false ]]; then
    print_info "Updating Cargo.lock files..."

    # Update main Cargo.lock
    if cargo update --workspace --quiet; then
        print_success "Updated main Cargo.lock"
    else
        print_warning "Failed to update main Cargo.lock"
    fi

    # Update Python Cargo.lock
    if (cd py-spatio && cargo update --quiet); then
        print_success "Updated py-spatio/Cargo.lock"
    else
        print_warning "Failed to update py-spatio/Cargo.lock"
    fi
fi

# Commit changes
if [[ "$DRY_RUN" == false && "$NO_COMMIT" == false ]]; then
    print_info "Committing version changes..."

    git add Cargo.toml Cargo.lock py-spatio/Cargo.toml py-spatio/Cargo.lock

    if git commit -m "bump version to $NEW_VERSION"; then
        print_success "Committed version changes"
    else
        print_error "Failed to commit changes"
        exit 1
    fi
fi

# Create git tag
if [[ "$DRY_RUN" == false && "$NO_TAG" == false ]]; then
    print_info "Creating git tag v$NEW_VERSION..."

    if git tag "v$NEW_VERSION" -m "Release version $NEW_VERSION"; then
        print_success "Created tag v$NEW_VERSION"
    else
        print_error "Failed to create tag"
        exit 1
    fi
fi

# Summary
print_info ""
print_success "Version bump completed!"
print_info "Version: $NEW_VERSION"

if [[ "$DRY_RUN" == true ]]; then
    print_info "This was a dry run. No files were actually modified."
elif [[ "$NO_COMMIT" == true ]]; then
    print_warning "Files updated but not committed. Don't forget to commit your changes!"
elif [[ "$NO_TAG" == true ]]; then
    print_warning "Changes committed but no tag created."
    print_info "To create the tag manually: git tag v$NEW_VERSION"
else
    print_info "Changes committed and tag created."
    print_info ""
    print_info "Next steps:"
    print_info "  1. Push changes: git push origin main"
    print_info "  2. Push tag: git push origin v$NEW_VERSION"
    print_info "  3. Or push both: git push origin main v$NEW_VERSION"
fi

print_info ""
print_info "The release workflow will automatically:"
print_info "  - Create GitHub release"
print_info "  - Publish Rust crate to crates.io"
print_info "  - Publish Python package to PyPI"
