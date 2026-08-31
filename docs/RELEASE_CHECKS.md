# Release checks

Run the deterministic release gate from the repository root:

```bash
bash scripts/release-check.sh
```

The gate requires the Bun version pinned by `client/package.json` and performs a
frozen install. It then runs the complete client lint, typecheck, and unit-test
suites; all deterministic backend tests; Python and Bun lock consistency;
syntax checks for every tracked shell script; the public-tree validator; and an
isolated production build that must match committed `static/` byte-for-byte.

Tests that need live credentials or infrastructure must carry the `external`
and `integration` pytest markers. Unmarked tests cannot open internet sockets,
and every backend test setup, call, and teardown phase has a deadline. The gate
also applies a 180-second process-tree deadline to every command, reports
progress every 30 seconds, and self-tests descendant cleanup before running the
suites. To run the deterministic gate followed by the opt-in live tests:

```bash
bash scripts/release-check.sh --with-external
```

The external suite currently includes the live SQL warehouse comparison in
`scripts/test_parallel.py`. It requires valid Databricks credentials and is not
run in normal CI.

The release gate deliberately fails on any lint error, dirty or stale static
artifact, lock drift, timeout, unmarked network access, or secret/private
identifier found in the derived public tree. Fix the source issue; do not
reduce the checked file set or bypass the public-mirror policy.
