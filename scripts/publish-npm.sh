#!/usr/bin/env bash
# Usage: publish-npm.sh <version>. Expects the built .node files in
# bindings/typescript/artifacts. Safe to re-run.
set -euo pipefail

version=$1

cd bindings/typescript

npm version "$version" --no-git-tag-version --allow-same-version
npx napi create-npm-dirs
npx napi artifacts --output-dir artifacts
npx napi pre-publish --no-gh-release --skip-optional-publish

publish() {
    local name
    name=$(node -p "require('./package.json').name")
    if npm view "$name@$version" version 2>/dev/null | grep -q .; then
        echo "$name $version already on npm"
    else
        npm publish --access public ${NPM_DRY_RUN:+--dry-run}
    fi
}

# Platform packages go first so the main package never points at a missing one.
for dir in npm/*/; do
    (cd "$dir" && publish)
done
publish
