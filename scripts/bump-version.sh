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
Usage: $0 <package> <new_version> [options]

ARGUMENTS:
    <package>        Which package to bump: 'rust', 'python', or 'both'
    <new_version>    The new version to set (e.g., 0.1.1, 0.2.0-alpha.1, 1.0.0-beta.2)

OPTIONS:
    --dry-run       Show what would be changed without making actual changes
    --no-commit     Update versions but don't commit changes
    --help, -h      Show this help message

EXAMPLES:
    $0 rust 0.1.1                    # Bump Rust crate to 0.1.1
    $0 python 0.2.0                  # Bump Python package to 0.2.0
    $0 both 0.1.5                    # Bump both to same version
    $0 python 0.2.0-alpha.1 --dry-run # Show what would change for Python pre-release

The script will update versions in:
    - rust: Cargo.toml (main project)
    - python: py-spatio/Cargo.toml (Python bindings)
    - both: Both Cargo.toml files (same version)

Note: GitHub Actions will automatically detect version changes and create releases.

EOF
}

# Parse command line arguments
PACKAGE=""
NEW_VERSION=""
DRY_RUN=false
NO_COMMIT=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --dry-run)
            DRY_RUN=true
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
            if [[ -z "$PACKAGE" ]]; then
                PACKAGE="$1"
            elif [[ -z "$NEW_VERSION" ]]; then
                NEW_VERSION="$1"
            else
                print_error "Too many arguments"
                show_usage
                exit 1
            fi
            shift
            ;;
    esac
done

# Validate arguments
if [[ -z "$PACKAGE" ]]; then
    print_error "Package is required (rust, python, or both)"
    show_usage
    exit 1
fi

if [[ -z "$NEW_VERSION" ]]; then
    print_error "New version is required"
    show_usage
    exit 1
fi

# Validate package argument
if [[ "$PACKAGE" != "rust" && "$PACKAGE" != "python" && "$PACKAGE" != "both" ]]; then
    print_error "Invalid package: $PACKAGE. Must be 'rust', 'python', or 'both'"
    show_usage
    exit 1
fi

# Remove 'v' prefix if present
NEW_VERSION=${NEW_VERSION#v}

# Validate version format
if ! [[ "$NEW_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9]+(\.[0-9]+)?)?$ ]]; then
    print_error "Invalid version format: $NEW_VERSION"
    print_error "Expected format: X.Y.Z or X.Y.Z-prerelease (e.g., 1.0.0, 1.0.0-alpha.1)"
    print_error "Note: 'v' prefix is automatically removed if present"
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
print_info "Updating: $PACKAGE"
print_info "New version: $NEW_VERSION"

# Files to update based on package
declare -a FILES_TO_UPDATE=()
case "$PACKAGE" in
    "rust")
        FILES_TO_UPDATE=("Cargo.toml")
        ;;
    "python")
        FILES_TO_UPDATE=("py-spatio/Cargo.toml")
        ;;
    "both")
        FILES_TO_UPDATE=("Cargo.toml" "py-spatio/Cargo.toml")
        ;;
esac

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
            rm "${file}.tmp" 2>/dev/null || true
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

    case "$PACKAGE" in
        "rust"|"both")
            if cargo update --workspace --quiet; then
                print_success "Updated main Cargo.lock"
            else
                print_warning "Failed to update main Cargo.lock"
            fi
            ;;
    esac

    case "$PACKAGE" in
        "python"|"both")
            if (cd py-spatio && cargo update --quiet); then
                print_success "Updated py-spatio/Cargo.lock"
            else
                print_warning "Failed to update py-spatio/Cargo.lock"
            fi
            ;;
    esac
fi

# Commit changes
if [[ "$DRY_RUN" == false && "$NO_COMMIT" == false ]]; then
    print_info "Committing version changes..."

    # Add files based on what was updated
    declare -a FILES_TO_ADD=()
    case "$PACKAGE" in
        "rust")
            FILES_TO_ADD=("Cargo.toml" "Cargo.lock")
            ;;
        "python")
            FILES_TO_ADD=("py-spatio/Cargo.toml" "py-spatio/Cargo.lock")
            ;;
        "both")
            FILES_TO_ADD=("Cargo.toml" "Cargo.lock" "py-spatio/Cargo.toml" "py-spatio/Cargo.lock")
            ;;
    esac

    # Add files, using -f for potentially ignored lock files
    for file in "${FILES_TO_ADD[@]}"; do
        if [[ "$file" == *"Cargo.lock" ]]; then
            git add -f "$file" 2>/dev/null || print_warning "Could not add $file (might be ignored)"
        else
            git add "$file"
        fi
    done

    COMMIT_MSG="bump $PACKAGE version to $NEW_VERSION"
    if git commit -m "$COMMIT_MSG"; then
        print_success "Committed version changes"
    else
        print_error "Failed to commit changes"
        exit 1
    fi
fi

# Summary
print_info ""
print_success "Version bump completed!"
print_info "Package: $PACKAGE"
print_info "Version: $NEW_VERSION"

if [[ "$DRY_RUN" == true ]]; then
    print_info "This was a dry run. No files were actually modified."
elif [[ "$NO_COMMIT" == true ]]; then
    print_warning "Files updated but not committed. Don't forget to commit your changes!"
else
    print_info "Changes committed."
    print_info ""
    print_info "Next step: Merge changes to trigger auto-release"
fi

print_info ""
print_info "GitHub Actions will automatically detect the version change and:"
case "$PACKAGE" in
    "rust")
        print_info "  - Create GitHub release with rust-v$NEW_VERSION tag"
        print_info "  - Publish Rust crate to crates.io"
        ;;
    "python")
        print_info "  - Create GitHub release with python-v$NEW_VERSION tag"
        print_info "  - Publish Python package to PyPI"
        ;;
    "both")
        print_info "  - Create GitHub releases for both packages"
        print_info "  - Publish Rust crate to crates.io"
        print_info "  - Publish Python package to PyPI"
        ;;
esac
