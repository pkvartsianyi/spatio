# Release benchmark results

Each core release stores a `bench_core` run here, named by version:

- `core-v<version>.json` — raw metrics (all runs), produced by `bench_core --json`.
- `core-v<version>.md` — human-readable summary plus a comparison against the
  previous version's results.

These files are generated and committed automatically by
`scripts/bench-release.sh`, which `scripts/bump-version.sh core <version>` runs
as part of cutting a core release. The `.md` file is also used as the GitHub
Release body for the `core-v<version>` tag.

To (re)generate for a version without bumping:

```bash
./scripts/bench-release.sh <version>
```

Comparisons are only meaningful when run on consistent hardware, so the
canonical results are produced locally at release time rather than in CI.
