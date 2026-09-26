# CYC-20260926T092050957424Z-repository-renewal

## Decision and scope

Branch: **scenario development on the unchanged lock**, alongside the requested repository renewal. At 2026-09-26 09:20 UTC, upstream `main` remained `3a6c7e361a1613a7d75288a4628058e2b3af0e67` and all six split references matched the previous baseline. [upstream.json](upstream.json) retains exact previous/remote/locked refs and check time. Alignment is inherited from the [prior partial baseline](../../../../baselines/BASE-20260926-main.json); no new implementation inspection was used. Dependencies were not refreshed.

The user requested a structure that can support hundreds of agent investigations. This cycle separated definitions, executions, findings and pending questions; retained historical bytes; and exercised the recording path with both passing and failing desired behavior. [Migration decisions](../../../../docs/migration-2026-09-26.md) document preservation, adopted scope and remaining work.

## User expectations and execution

1. **Isolated fixture lifecycle:** create, update and delete a fixture without reading or altering a physical seed; a fresh wrapper starts empty. [Expectation and public basis](../../../../spec/expectations/SPEC-isolated-fixture-lifecycle.md). New workflow passes 1 test / 10 assertions, and the adopted basic CRUD suite passes 9 tests / 18 assertions. The physical seed remains exactly unchanged. No library problem found in this tested workflow.
2. **Prepared column order:** positional values follow the explicit INSERT column list, and a later primary-key read returns the fixture. [Expectation and native basis](../../../../spec/expectations/SPEC-prepared-column-order.md). Native returns `id=10, name=PrepItem, price=19.99, category=null`. ZTD returns `id=19, name="10", price=0, category=null`, and lookup by key 10 returns `[]`. Native writes its physical control table; ZTD leaves its physical table empty as intended. The desired value/lookup expectation fails.

| Execution | Retained metadata and output | Result |
| --- | --- | --- |
| Fixture lifecycle / CRUD | [Run](runs/20260926T092528699261Z-SCN-sqlite-fixture-lifecycle/run.json), [new workflow](runs/20260926T092528699261Z-SCN-sqlite-fixture-lifecycle/01-output.txt), [basic CRUD](runs/20260926T092528699261Z-SCN-sqlite-fixture-lifecycle/02-output.txt) | Exit 0 / 0; 10 tests, 28 assertions |
| Native/ZTD standalone control / existing failing test | [Run](runs/20260926T092623785666Z-SCN-sqlite-insert-column-order/run.json), [control output](runs/20260926T092623785666Z-SCN-sqlite-insert-column-order/01-output.txt), [PHPUnit failure](runs/20260926T092623785666Z-SCN-sqlite-insert-column-order/02-output.txt) | Exit 1 / 1; known unmet expectation |

Both were also executed before a recording-tool improvement; those earlier run directories remain intact. The later runs verify the updated recording path, including normalized database version metadata. No library behavior changed between these runs. Exact argv, original revision `9153d5fc1ef118c518dbbaba794c211cbe25b00e`, source archives containing local changes, full lock snapshots, all package refs and raw output hashes are in each run record.

Common environment: PHP 8.5.8, PDO/pdo_sqlite 8.5.8, SQLite client/server 3.53.3, PHPUnit 10.5.65, Darwin arm64. `sqlite::memory:`, PDO exception error mode, default ZTD configuration, no external services. Native controls use a fresh in-memory database per case. Runtime tooling is the complete locked dependency set.

Commands from repository root:

```bash
composer install
python3 scripts/lab.py start repository-renewal
python3 scripts/lab.py run SCN-sqlite-fixture-lifecycle --cycle CYC-20260926T092050957424Z-repository-renewal
python3 scripts/lab.py run SCN-sqlite-insert-column-order --cycle CYC-20260926T092050957424Z-repository-renewal
```

The last command returns 1 while the problem persists. Future executions create a new cycle; do not reopen/overwrite this completed one. Each PHP test or reproduction contains schema, seeds and operation order.

## Classification and upstream disposition

- Fixture lifecycle: desired behavior met in the scope above. Native physical reads verify isolation; native and ZTD are intentionally not expected to expose identical physical contents.
- Column order: **existing problem**, already described by [#465](https://github.com/k-kinzal/ztd-query-php/issues/465) on the same lock/runtime. No regression introduction point is claimed. [Finding and reproduction](../../../../findings/FND-sqlite-insert-column-order/README.md); [open/closed search](../../../../findings/FND-sqlite-insert-column-order/issue-search.md). The issue was open and its body matches this run. No duplicate report or redundant comment was submitted.
- Observation infrastructure: previous version-log merging could mix classes from different executions. Now each process replaces its own metadata, and curated runs use unique paths. This is a repository tooling correction, not an upstream library issue.
- All other historical failures remain unclassified by this cycle. Existing bug assertions and broken setups are explicit work items.

## Tooling and replay verification

[tooling-checks.txt](tooling-checks.txt) retains 12 tooling tests, Composer validation/platform requirements and two successful PHP replays. The source archive and lock from the lifecycle run were extracted into `build/replay-renewal`, dependencies installed there with `composer install --no-interaction --no-progress`, then both PHP test files were executed there. This checks that local uncommitted source changes are actually replayable. The disposable directory is supplementary; the archive/lock and essential output are retained in repository records.

Additional checks: record/reference/hash validation, PHP and Python syntax, shell syntax and `git diff --check`. Historical migration hashes verify all 20 moved/copied source artifacts. Existing test source files were preserved; the new workflow is additive.

## Remaining work and baseline

`WRK-refresh-structure` is complete. The baseline pointer still identifies the original **partial** package baseline; this run adds scoped evidence without claiming full supported-matrix verification.

Next investigations: `WRK-historical-assertions`, `WRK-scenario-setup`, `WRK-pdo-result-types`, `WRK-legacy-black-box`, `WRK-supported-matrix`, and `WRK-session-created-schema`. Prioritize a bounded user question; do not mass-promote old status labels. PHP 8.1–8.4, other SQLite releases, MySQL/PDO/MySQLi, PostgreSQL, alternate binding styles and PDO options are untested in this cycle. The full legacy suite was not rerun.
