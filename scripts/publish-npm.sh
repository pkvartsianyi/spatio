#!/usr/bin/env bash
# Usage: publish-npm.sh <version>. Expects the built .node files in
# bindings/typescript/artifacts. Safe to re-run from the same commit.
set -euo pipefail

version=$1
commit=$(git rev-parse HEAD)

cd bindings/typescript

npm version "$version" --no-git-tag-version --allow-same-version
npx napi create-npm-dirs
npx napi artifacts --output-dir artifacts
npx napi pre-publish --no-gh-release --skip-optional-publish

published_commit() {
    local name
    name=$(node -p "require('./package.json').name")
    if npm view "$name@$version" version 2>/dev/null | grep -q .; then
        npm view "$name@$version" gitHead 2>/dev/null | grep . || echo unknown
    fi
}

# Published versions are immutable, so finishing a partial release from another
# commit would ship addons and a loader from different builds.
check() {
    local name published
    name=$(node -p "require('./package.json').name")
    published=$(published_commit)
    if [ -n "$published" ] && [ "$published" != "$commit" ]; then
        echo "$name $version was published from $published, not $commit." >&2
        echo "Bump the version in bindings/typescript/Cargo.toml to release this commit." >&2
        exit 1
    fi
}

publish() {
    local name
    name=$(node -p "require('./package.json').name")
    if [ -n "$(published_commit)" ]; then
        echo "$name $version already on npm"
    else
        npm publish --access public ${NPM_DRY_RUN:+--dry-run}
    fi
}

for dir in npm/*/; do
    (cd "$dir" && check)
done
check

# Platform packages go first so the main package never points at a missing one.
for dir in npm/*/; do
    (cd "$dir" && publish)
done
publish
