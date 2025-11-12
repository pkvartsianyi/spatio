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
    <package>        Which package to bump: 'rust', 'python', 'types', or 'all'
    <new_version>    The new version to set (e.g., 0.1.1, 0.2.0-alpha.1, 1.0.0-beta.2)

OPTIONS:
    --dry-run       Show what would be changed without making actual changes
    --no-commit     Update versions but don't commit changes
    --help, -h      Show this help message

EXAMPLES:
    $0 rust 0.1.1                    # Bump Rust crate to 0.1.1
    $0 python 0.2.0                  # Bump Python package to 0.2.0
    $0 types 0.1.0                   # Bump spatio-types to 0.1.0
    $0 all 0.1.5                     # Bump all packages to same version
    $0 python 0.2.0-alpha.1 --dry-run # Show what would change for Python pre-release

The script will update versions in:
    - rust: Cargo.toml (workspace root)
    - python: crates/py/Cargo.toml (Python bindings)
    - types: crates/types/Cargo.toml (Core types)
    - all: All Cargo.toml files (same version)

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
    print_error "Package is required (rust, python, types, or all)"
    show_usage
    exit 1
fi

if [[ -z "$NEW_VERSION" ]]; then
    print_error "New version is required"
    show_usage
    exit 1
fi

# Validate package argument
if [[ "$PACKAGE" != "rust" && "$PACKAGE" != "python" && "$PACKAGE" != "types" && "$PACKAGE" != "all" ]]; then
    print_error "Invalid package: $PACKAGE. Must be 'rust', 'python', 'types', or 'all'"
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

# Get current versions from workspace root
CURRENT_RUST_VERSION=$(grep '^\[workspace.package\]' -A 10 Cargo.toml | grep '^version = ' | head -1 | sed 's/version = "\(.*\)"/\1/')
CURRENT_PYTHON_VERSION="$CURRENT_RUST_VERSION"  # All use workspace version
CURRENT_TYPES_VERSION="$CURRENT_RUST_VERSION"   # All use workspace version

print_info "Current versions:"
print_info "  Rust crate: $CURRENT_RUST_VERSION"
print_info "  Python package: $CURRENT_PYTHON_VERSION"
print_info "  Types package: $CURRENT_TYPES_VERSION"
print_info ""
print_info "Updating: $PACKAGE"
print_info "New version: $NEW_VERSION"

# Files to update based on package
declare -a FILES_TO_UPDATE=()
case "$PACKAGE" in
    "rust"|"python"|"types"|"all")
        # All versions managed in workspace root now
        FILES_TO_UPDATE=("Cargo.toml")
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

    local current_version=$(grep '^\[workspace.package\]' -A 10 "$file" | grep '^version = ' | head -1 | sed 's/version = "\(.*\)"/\1/')

    if [[ "$DRY_RUN" == true ]]; then
        print_info "Would update $file (workspace version): $current_version -> $new_version"
    else
        print_info "Updating $file (workspace version): $current_version -> $new_version"

        # Create backup
        cp "$file" "$file.backup"

        # Update version in [workspace.package] section
        if awk -v new_ver="$new_version" '
            /^\[workspace\.package\]/ { in_workspace=1 }
            /^\[/ && !/^\[workspace\.package\]/ { in_workspace=0 }
            in_workspace && /^version = / { print "version = \"" new_ver "\""; next }
            { print }
        ' "$file" > "${file}.tmp"; then
            mv "${file}.tmp" "$file"
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

# Function to check if version is a prerelease
is_prerelease() {
    [[ "$1" =~ -[a-zA-Z0-9]+(\.[0-9]+)?$ ]]
}

# Function to update dependency version in Cargo.toml
update_dependency_version() {
    local file="$1"
    local dep_name="$2"
    local new_version="$3"

    if [[ ! -f "$file" ]]; then
        return 1
    fi

    # Create backup
    cp "$file" "$file.backup"

    # Update dependency version (macOS and Linux compatible)
    if awk -v dep="$dep_name" -v ver="$new_version" '
        /^[[:space:]]*spatio-types = \{/ {
            sub(/version = "[^"]*"/, "version = \"" ver "\"")
        }
        { print }
    ' "$file" > "$file.tmp"; then
        mv "$file.tmp" "$file"
        rm "$file.backup"
        return 0
    else
        mv "$file.backup" "$file" 2>/dev/null || true
        return 1
    fi
}

# For prerelease versions, update dependency specifications to use exact version
if [[ "$DRY_RUN" == false ]] && is_prerelease "$NEW_VERSION"; then
    print_info "Prerelease version detected - updating dependency specifications..."

    case "$PACKAGE" in
        "rust"|"all")
            if update_dependency_version "crates/core/Cargo.toml" "spatio-types" "=$NEW_VERSION"; then
                print_success "Updated spatio-types dependency in crates/core/Cargo.toml to =$NEW_VERSION"
            else
                print_error "Failed to update spatio-types dependency specification"
                exit 1
            fi
            ;;
    esac
fi

# Update Cargo.lock files
UPDATE_FAILED=false
if [[ "$DRY_RUN" == false ]]; then
    print_info "Updating Cargo.lock files..."

    case "$PACKAGE" in
        "rust"|"all")
            if cargo update --workspace --quiet 2>&1; then
                print_success "Updated main Cargo.lock"
            else
                print_error "Failed to update main Cargo.lock"
                UPDATE_FAILED=true
            fi
            ;;
    esac

    case "$PACKAGE" in
        "python"|"all")
            if (cd crates/py && cargo update --quiet 2>&1); then
                print_success "Updated crates/py/Cargo.lock"
            else
                print_error "Failed to update crates/py/Cargo.lock"
                UPDATE_FAILED=true
            fi
            ;;
    esac

    case "$PACKAGE" in
        "types"|"all")
            if (cd crates/types && cargo update --quiet 2>/dev/null); then
                print_success "Updated crates/types/Cargo.lock"
            else
                print_info "crates/types has no Cargo.lock (workspace member)"
            fi
            ;;
    esac

    # Abort if any critical update failed
    if [[ "$UPDATE_FAILED" == true ]]; then
        print_error "Cargo update failed. Aborting without committing."
        print_error ""
        print_error "For prerelease versions, dependencies need exact version specifications."
        print_error "The dependency updates may have been applied. You may need to:"
        print_error "  git checkout Cargo.toml crates/core/Cargo.toml"
        exit 1
    fi
fi

# --- CHANGELOG GENERATION ----------------------------------------------------

# --- CHANGELOG GENERATION ----------------------------------------------------

update_changelog() {
    print_info "Rebuilding CHANGELOG.md from latest release..."

    local changelog_file="CHANGELOG.md"
    local temp_file
    temp_file=$(mktemp)

    {
        echo "# Changelog"
        echo ""
        echo "All notable changes since the last release are documented below."
        echo ""
    } > "$temp_file"

    git fetch --tags --quiet || true

    # Get the most recent tag (sorted by version)
    local last_tag
    last_tag=$(git describe --tags --abbrev=0 2>/dev/null || echo "")

    if [[ -z "$last_tag" ]]; then
        print_warning "No previous tag found — using entire commit history."
        last_tag=$(git rev-list --max-parents=0 HEAD)
    fi

    local date
    date=$(date +%Y-%m-%d)
    echo "## [$NEW_VERSION] - $date" >> "$temp_file"

    print_info "Generating changelog since tag: ${last_tag}"

    # Collect commits between last tag and HEAD
    local commits
    commits=$(git log "${last_tag}"..HEAD --pretty=format:"%s" || true)

    if [[ -z "$commits" ]]; then
        echo "(no new commits since ${last_tag})" >> "$temp_file"
        mv "$temp_file" "$changelog_file"
        print_warning "No new commits to include in changelog."
        return
    fi

    local added changed fixed
    added=""; changed=""; fixed=""

    while IFS= read -r commit; do
        msg="${commit#*: }"
        msg="${msg# }"
        case "$commit" in
            feat:*|feature:*) added="${added}\n- ${msg}" ;;
            fix:*|bugfix:*) fixed="${fixed}\n- ${msg}" ;;
            refactor:*|chore:*|style:*) changed="${changed}\n- ${msg}" ;;
            bump*version*) ;; # skip version bumps
            *) changed="${changed}\n- ${commit}" ;;
        esac
    done <<< "$commits"

    [[ -n "$added" ]] && echo -e "\n### Added${added}" >> "$temp_file"
    [[ -n "$changed" ]] && echo -e "\n### Changed${changed}" >> "$temp_file"
    [[ -n "$fixed" ]] && echo -e "\n### Fixed${fixed}" >> "$temp_file"

    echo "" >> "$temp_file"

    mv "$temp_file" "$changelog_file"
    print_success "CHANGELOG.md regenerated for commits since ${last_tag}"
}

# Only update changelog for 'all' package bumps (combined releases)
if [[ "$DRY_RUN" == false && "$PACKAGE" == "all" ]]; then
    print_info "Generating CHANGELOG.md from latest commits..."
    update_changelog
fi

# ------------------------------------------------------------------------------
if [[ "$DRY_RUN" == false && "$NO_COMMIT" == false ]]; then
    print_info "Committing version changes..."

    # Add files based on what was updated
    declare -a FILES_TO_ADD=()
    case "$PACKAGE" in
        "rust"|"types")
            FILES_TO_ADD=("Cargo.toml" "Cargo.lock")
            ;;
        "python")
            FILES_TO_ADD=("Cargo.toml" "Cargo.lock" "crates/py/Cargo.lock")
            ;;
        "all")
            FILES_TO_ADD=("Cargo.toml" "Cargo.lock" "crates/py/Cargo.lock" "crates/types/Cargo.lock" "CHANGELOG.md")
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
    "types")
        print_info "  - Create GitHub release with types-v$NEW_VERSION tag"
        print_info "  - Publish spatio-types to crates.io"
        ;;
    "all")
        print_info "  - Create GitHub releases for all packages"
        print_info "  - Publish Rust crate to crates.io"
        print_info "  - Publish Python package to PyPI"
        print_info "  - Publish spatio-types to crates.io"
        ;;
esac
