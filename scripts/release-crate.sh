#!/usr/bin/env bash
# Usage: release-crate.sh <crate> <tag-prefix> <version>. Safe to re-run.
set -euo pipefail

crate=$1 tag=$2$3 version=$3

if curl -sf -A "spatio-release (https://github.com/pkvartsianyi/spatio)" \
    "https://crates.io/api/v1/crates/$crate/$version" >/dev/null; then
    echo "$crate $version already on crates.io"
else
    cargo publish -p "$crate"
fi

if ! git ls-remote --exit-code --tags origin "refs/tags/$tag" >/dev/null; then
    git -c user.name="github-actions[bot]" \
        -c user.email="41898282+github-actions[bot]@users.noreply.github.com" \
        tag "$tag" -m "Release $crate $version"
    git push origin "$tag"
fi
