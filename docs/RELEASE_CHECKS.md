# Release checks

## Fast publish/deploy gate

The hot path runs a bounded allowlist of release-critical checks:

```bash
bash scripts/release-check.sh --fast
```

It verifies export anonymization and secret handling, deployment manifests and
authorization boundaries, shell syntax, the customer-safe public tree, and an
isolated frontend build that must match committed `static/` byte-for-byte.
`sync-mirror.sh` uses this tier and skips its working-tree public scan because
the mirror immediately validates the exact public stage derived from
`origin/main`. `dba_deploy.sh` runs the same fast tier once, before any platform
call, through `scripts/deploy-preflight.sh`.

## Full audit gate

Run the complete deterministic audit from the repository root:

```bash
bash scripts/release-check.sh --full
```

`--full` is the default for compatibility. The full gate requires the Bun
version pinned by `client/package.json` and performs a
frozen install. It then runs the complete client lint, typecheck, and unit-test
suites; Python lint and all deterministic backend tests; Python and Bun lock consistency;
syntax checks for every tracked shell script; the public-tree validator; and an
isolated production build that must match committed `static/` byte-for-byte.

Tests that need live credentials or infrastructure must carry the `external`
and `integration` pytest markers. Unmarked tests cannot open internet sockets,
and every backend test setup, call, and teardown phase has a deadline. The gate
also applies a 180-second process-tree deadline to every command, reports
progress every 30 seconds, and self-tests descendant cleanup before running the
suites. To run the deterministic gate followed by the opt-in live tests:

```bash
bash scripts/release-check.sh --full --with-external
```

The external suite currently includes the live SQL warehouse comparison in
`scripts/test_parallel.py`. It requires valid Databricks credentials and is not
run in normal CI.

Use the full tier in CI, nightly runs, and manual release audits. The fast tier
does not replace that coverage; it removes broad suites from repetitive mirror
and deploy operations while preserving checks that can make publishing unsafe.
