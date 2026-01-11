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
    <package>        Which package to bump: 'core', 'python', 'types', 'server', 'client', or 'all'
    <new_version>    The new version to set (e.g., 0.1.1, 0.2.0-alpha.1, 1.0.0-beta.2)

OPTIONS:
    --dry-run       Show what would be changed without making actual changes
    --no-commit     Update versions but don't commit changes
    --help, -h      Show this help message

EXAMPLES:
    $0 core 0.1.9                    # Bump spatio core to 0.1.9
    $0 python 0.2.0                  # Bump Python package to 0.2.0
    $0 types 0.1.9                   # Bump spatio-types to 0.1.9
    $0 server 0.1.0                  # Bump spatio-server to 0.1.0
    $0 client 0.1.0                  # Bump spatio-client to 0.1.0
    $0 all 0.1.9                     # Bump all packages to same version
    $0 python 0.2.0-alpha.1 --dry-run # Show what would change for Python pre-release

The script will update versions in:
    - core: crates/core/Cargo.toml (spatio core crate)
    - python: crates/py/Cargo.toml (Python bindings)
    - types: crates/types/Cargo.toml (Core types)
    - server: crates/server/Cargo.toml (RPC server)
    - client: crates/client/Cargo.toml (RPC client)
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
    print_error "Package is required (core, python, types, server, client, or all)"
    show_usage
    exit 1
fi

if [[ -z "$NEW_VERSION" ]]; then
    print_error "New version is required"
    show_usage
    exit 1
fi

# Validate package argument
if [[ "$PACKAGE" != "core" && "$PACKAGE" != "python" && "$PACKAGE" != "types" && "$PACKAGE" != "server" && "$PACKAGE" != "client" && "$PACKAGE" != "all" ]]; then
    print_error "Invalid package: $PACKAGE. Must be 'core', 'python', 'types', 'server', 'client', or 'all'"
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

# Get current versions using cargo metadata for robust parsing
CURRENT_CORE_VERSION=$(cargo metadata --format-version 1 --no-deps 2>/dev/null | \
    grep -o '"name":"spatio","version":"[^"]*"' | head -1 | cut -d'"' -f8)
CURRENT_PYTHON_VERSION=$(cargo metadata --format-version 1 --no-deps 2>/dev/null | \
    grep -o '"name":"spatio-py","version":"[^"]*"' | head -1 | cut -d'"' -f8)
CURRENT_TYPES_VERSION=$(cargo metadata --format-version 1 --no-deps 2>/dev/null | \
    grep -o '"name":"spatio-types","version":"[^"]*"' | head -1 | cut -d'"' -f8)
CURRENT_SERVER_VERSION=$(cargo metadata --format-version 1 --no-deps 2>/dev/null | \
    grep -o '"name":"spatio-server","version":"[^"]*"' | head -1 | cut -d'"' -f8)
CURRENT_CLIENT_VERSION=$(cargo metadata --format-version 1 --no-deps 2>/dev/null | \
    grep -o '"name":"spatio-client","version":"[^"]*"' | head -1 | cut -d'"' -f8)

# Fallback to awk if cargo metadata fails
if [[ -z "$CURRENT_CORE_VERSION" ]]; then
    CURRENT_CORE_VERSION=$(awk -F'[" ]+' '/^version[[:space:]]*=/ {print $3; exit}' crates/core/Cargo.toml)
fi
if [[ -z "$CURRENT_PYTHON_VERSION" ]]; then
    CURRENT_PYTHON_VERSION=$(awk -F'[" ]+' '/^version[[:space:]]*=/ {print $3; exit}' crates/py/Cargo.toml)
fi
if [[ -z "$CURRENT_TYPES_VERSION" ]]; then
    CURRENT_TYPES_VERSION=$(awk -F'[" ]+' '/^version[[:space:]]*=/ {print $3; exit}' crates/types/Cargo.toml)
fi
if [[ -z "$CURRENT_SERVER_VERSION" ]]; then
    CURRENT_SERVER_VERSION=$(awk -F'[" ]+' '/^version[[:space:]]*=/ {print $3; exit}' crates/server/Cargo.toml)
fi
if [[ -z "$CURRENT_CLIENT_VERSION" ]]; then
    CURRENT_CLIENT_VERSION=$(awk -F'[" ]+' '/^version[[:space:]]*=/ {print $3; exit}' crates/client/Cargo.toml)
fi

print_info "Current versions:"
print_info "  spatio (core): $CURRENT_CORE_VERSION"
print_info "  spatio-py: $CURRENT_PYTHON_VERSION"
print_info "  spatio-types: $CURRENT_TYPES_VERSION"
print_info "  spatio-server: $CURRENT_SERVER_VERSION"
print_info "  spatio-client: $CURRENT_CLIENT_VERSION"
print_info ""
print_info "Updating: $PACKAGE"
print_info "New version: $NEW_VERSION"

# Files to update based on package
declare -a FILES_TO_UPDATE=()
case "$PACKAGE" in
    "core")
        FILES_TO_UPDATE=("crates/core/Cargo.toml")
        ;;
    "python")
        FILES_TO_UPDATE=("crates/py/Cargo.toml")
        ;;
    "types")
        FILES_TO_UPDATE=("crates/types/Cargo.toml")
        ;;
    "server")
        FILES_TO_UPDATE=("crates/server/Cargo.toml")
        ;;
    "client")
        FILES_TO_UPDATE=("crates/client/Cargo.toml")
        ;;
    "all")
        FILES_TO_UPDATE=("crates/core/Cargo.toml" "crates/py/Cargo.toml" "crates/types/Cargo.toml" "crates/server/Cargo.toml" "crates/client/Cargo.toml")
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

    local current_version=$(awk -F'[" ]+' '/^version[[:space:]]*=/ {print $3; exit}' "$file")

    if [[ "$DRY_RUN" == true ]]; then
        print_info "Would update $file: $current_version -> $new_version"
    else
        print_info "Updating $file: $current_version -> $new_version"

        # Create backup
        cp "$file" "$file.backup"

        # Update version using awk (BSD sed compatible)
        awk -v new_ver="$new_version" '
            /^version[[:space:]]*=/ && !done {
                print "version = \"" new_ver "\""
                done=1
                next
            }
            { print }
        ' "$file" > "$file.tmp"

        # Check if awk succeeded
        if [[ $? -ne 0 ]]; then
            print_error "Failed to update $file (awk command failed)"
            mv "$file.backup" "$file" 2>/dev/null || true
            rm -f "$file.tmp" 2>/dev/null || true
            return 1
        fi

        # Move the temp file to the original
        mv "$file.tmp" "$file"

        # Verify the version was actually changed
        local updated_version=$(awk -F'[" ]+' '/^version[[:space:]]*=/ {print $3; exit}' "$file")
        if [[ "$updated_version" != "$new_version" ]]; then
            print_error "Failed to update $file (version verification failed: expected $new_version, got $updated_version)"
            mv "$file.backup" "$file" 2>/dev/null || true
            rm -f "$file.tmp" 2>/dev/null || true
            return 1
        fi

        # Success - cleanup
        rm -f "$file.backup" 2>/dev/null || true
    fi
}

# Update versions in all files
print_info "Updating version files..."
for file in "${FILES_TO_UPDATE[@]}"; do
    update_version_in_file "$file" "$NEW_VERSION"
done

# Update workspace dependency for spatio-types if needed
if [[ "$PACKAGE" == "types" || "$PACKAGE" == "all" ]] && [[ "$DRY_RUN" == false ]]; then
    print_info "Updating spatio-types workspace dependency..."
    WORKSPACE_CARGO="Cargo.toml"

    if [[ -f "$WORKSPACE_CARGO" ]]; then
        # Update spatio-types version in workspace dependencies
        cp "$WORKSPACE_CARGO" "$WORKSPACE_CARGO.backup"

        # Use awk for BSD sed compatibility
        awk -v new_ver="$NEW_VERSION" '
            /^spatio-types = { version = / {
                sub(/version = "[^"]*"/, "version = \"" new_ver "\"")
            }
            { print }
        ' "$WORKSPACE_CARGO" > "$WORKSPACE_CARGO.tmp"

        # Check if awk succeeded
        if [[ $? -ne 0 ]]; then
            print_error "Failed to update workspace dependency (awk command failed)"
            mv "$WORKSPACE_CARGO.backup" "$WORKSPACE_CARGO" 2>/dev/null || true
            rm -f "$WORKSPACE_CARGO.tmp" 2>/dev/null || true
        else
            mv "$WORKSPACE_CARGO.tmp" "$WORKSPACE_CARGO"
            rm -f "$WORKSPACE_CARGO.backup" 2>/dev/null || true
            print_success "Updated workspace dependency for spatio-types"
        fi
    fi
fi

# Update workspace dependency for spatio (core) if needed
if [[ "$PACKAGE" == "core" || "$PACKAGE" == "all" ]] && [[ "$DRY_RUN" == false ]]; then
    print_info "Updating spatio workspace dependency..."
    WORKSPACE_CARGO="Cargo.toml"

    if [[ -f "$WORKSPACE_CARGO" ]]; then
        # Update spatio version in workspace dependencies
        cp "$WORKSPACE_CARGO" "$WORKSPACE_CARGO.backup"

        # Use awk for BSD sed compatibility
        awk -v new_ver="$NEW_VERSION" '
            /^spatio = { version = / {
                sub(/version = "[^"]*"/, "version = \"" new_ver "\"")
            }
            { print }
        ' "$WORKSPACE_CARGO" > "$WORKSPACE_CARGO.tmp"

        # Check if awk succeeded
        if [[ $? -ne 0 ]]; then
            print_error "Failed to update workspace dependency (awk command failed)"
            mv "$WORKSPACE_CARGO.backup" "$WORKSPACE_CARGO" 2>/dev/null || true
            rm -f "$WORKSPACE_CARGO.tmp" 2>/dev/null || true
        else
            mv "$WORKSPACE_CARGO.tmp" "$WORKSPACE_CARGO"
            rm -f "$WORKSPACE_CARGO.backup" 2>/dev/null || true
            print_success "Updated workspace dependency for spatio"
        fi
    fi
fi

# Update workspace dependency for spatio-server if needed
if [[ "$PACKAGE" == "server" || "$PACKAGE" == "all" ]] && [[ "$DRY_RUN" == false ]]; then
    print_info "Updating spatio-server workspace dependency..."
    WORKSPACE_CARGO="Cargo.toml"

    if [[ -f "$WORKSPACE_CARGO" ]]; then
        cp "$WORKSPACE_CARGO" "$WORKSPACE_CARGO.backup"

        awk -v new_ver="$NEW_VERSION" '
            /^spatio-server = { version = / {
                sub(/version = "[^"]*"/, "version = \"" new_ver "\"")
            }
            { print }
        ' "$WORKSPACE_CARGO" > "$WORKSPACE_CARGO.tmp"

        if [[ $? -ne 0 ]]; then
            print_error "Failed to update workspace dependency (awk command failed)"
            mv "$WORKSPACE_CARGO.backup" "$WORKSPACE_CARGO" 2>/dev/null || true
            rm -f "$WORKSPACE_CARGO.tmp" 2>/dev/null || true
        else
            mv "$WORKSPACE_CARGO.tmp" "$WORKSPACE_CARGO"
            rm -f "$WORKSPACE_CARGO.backup" 2>/dev/null || true
            print_success "Updated workspace dependency for spatio-server"
        fi
    fi
fi

# Update workspace dependency for spatio-client if needed
if [[ "$PACKAGE" == "client" || "$PACKAGE" == "all" ]] && [[ "$DRY_RUN" == false ]]; then
    print_info "Updating spatio-client workspace dependency..."
    WORKSPACE_CARGO="Cargo.toml"

    if [[ -f "$WORKSPACE_CARGO" ]]; then
        cp "$WORKSPACE_CARGO" "$WORKSPACE_CARGO.backup"

        awk -v new_ver="$NEW_VERSION" '
            /^spatio-client = { version = / {
                sub(/version = "[^"]*"/, "version = \"" new_ver "\"")
            }
            { print }
        ' "$WORKSPACE_CARGO" > "$WORKSPACE_CARGO.tmp"

        if [[ $? -ne 0 ]]; then
            print_error "Failed to update workspace dependency (awk command failed)"
            mv "$WORKSPACE_CARGO.backup" "$WORKSPACE_CARGO" 2>/dev/null || true
            rm -f "$WORKSPACE_CARGO.tmp" 2>/dev/null || true
        else
            mv "$WORKSPACE_CARGO.tmp" "$WORKSPACE_CARGO"
            rm -f "$WORKSPACE_CARGO.backup" 2>/dev/null || true
            print_success "Updated workspace dependency for spatio-client"
        fi
    fi
fi

# Update Cargo.lock files
UPDATE_FAILED=false
if [[ "$DRY_RUN" == false ]]; then
    print_info "Updating Cargo.lock files..."

    if cargo update --workspace --quiet 2>&1; then
        print_success "Updated Cargo.lock"
    else
        print_error "Failed to update Cargo.lock"
        UPDATE_FAILED=true
    fi

    # Abort if update failed
    if [[ "$UPDATE_FAILED" == true ]]; then
        print_error "Cargo update failed. Aborting without committing."
        print_error ""
        print_error "Run 'git status' to see what was changed."
        print_error "You may need to restore files with: git checkout ."
        exit 1
    fi
fi

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
        "core")
            FILES_TO_ADD=("crates/core/Cargo.toml" "Cargo.toml" "Cargo.lock")
            ;;
        "python")
            FILES_TO_ADD=("crates/py/Cargo.toml" "Cargo.lock")
            ;;
        "types")
            FILES_TO_ADD=("crates/types/Cargo.toml" "Cargo.toml" "Cargo.lock")
            ;;
        "server")
            FILES_TO_ADD=("crates/server/Cargo.toml" "Cargo.toml" "Cargo.lock")
            ;;
        "client")
            FILES_TO_ADD=("crates/client/Cargo.toml" "Cargo.toml" "Cargo.lock")
            ;;
        "all")
            FILES_TO_ADD=("crates/core/Cargo.toml" "crates/py/Cargo.toml" "crates/types/Cargo.toml" "crates/server/Cargo.toml" "crates/client/Cargo.toml" "Cargo.toml" "Cargo.lock" "CHANGELOG.md")
            ;;
    esac

    # Add files, using -f for potentially ignored lock files
    for file in "${FILES_TO_ADD[@]}"; do
        if [[ "$file" == *"Cargo.lock" ]]; then
            git add -f "$file" 2>/dev/null || print_warning "Could not add $file (might be ignored)"
        else
            git add "$file" 2>/dev/null || print_warning "Could not add $file"
        fi
    done

    COMMIT_MSG="bump $PACKAGE version to $NEW_VERSION"
    
    # Check if there are actually changes to commit
    if git diff --cached --quiet; then
        print_warning "No changes to commit. Version was likely already at $NEW_VERSION."
    else
        if git commit -m "$COMMIT_MSG"; then
            print_success "Committed version changes"
        else
            print_error "Failed to commit changes"
            exit 1
        fi
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
    print_info "Next step: Push to main to trigger auto-release"
fi

print_info ""
print_info "GitHub Actions will automatically detect the version change and:"
case "$PACKAGE" in
    "core")
        print_info "  - Create GitHub release with core-v$NEW_VERSION tag"
        print_info "  - Publish spatio to crates.io"
        ;;
    "python")
        print_info "  - Create GitHub release with python-v$NEW_VERSION tag"
        print_info "  - Publish spatio-py to PyPI"
        ;;
    "types")
        print_info "  - Create GitHub release with types-v$NEW_VERSION tag"
        print_info "  - Publish spatio-types to crates.io"
        ;;
    "server")
        print_info "  - Create GitHub release with server-v$NEW_VERSION tag"
        print_info "  - Publish spatio-server to crates.io"
        ;;
    "client")
        print_info "  - Create GitHub release with client-v$NEW_VERSION tag"
        print_info "  - Publish spatio-client to crates.io"
        ;;
    "all")
        print_info "  - Create GitHub releases for all packages"
        print_info "  - Publish spatio to crates.io"
        print_info "  - Publish spatio-py to PyPI"
        print_info "  - Publish spatio-types to crates.io"
        print_info "  - Publish spatio-server to crates.io"
        print_info "  - Publish spatio-client to crates.io"
        ;;
esac
