# CYC-20260926T200050738102Z-mysqli-native-parity

## Decision and scope

The [upstream check](upstream.json) at 2026-09-26 20:00:50 UTC found monorepo
`main` moved from `aac51e9d99121f79c98720fec3f8ddc03bf7c59b` to
`249944eaed3ea200ed235fb6473296c7e301608a` while all six split packages kept
the references locked in `BASE-20260926-main-aac51e9`. The check therefore
selected the regression branch with alignment `unverified`.

Alignment evidence gathered afterwards (GitHub API, 2026-09-26 20:01–20:03 UTC):

- The compare lists ten commits from PRs #466 (sql-fixture), #470
  (sql-semantics writer fidelity, 6410 files) and #353 (Renovate). The PR file
  lists for #466 and #353 contain no `packages/ztd-query-*` path; #470 exceeds
  the API's 3000-file listing cap.
- The Git tree SHAs of every `packages/ztd-query-*` directory are identical at
  both commits (core `6b53c5a7…`, mysql `298a95b8…`, mysqli-adapter
  `a77f46b1…`, pdo-adapter `cdb9ecfc…`, postgres `037fc904…`, sqlite
  `54c7e172…`), so no ZTD package content changed. The changed `sql-*`
  packages are not dependencies of the locked ZTD packages (`composer.lock`
  lists no `k-kinzal/sql-*` requirement).
- `composer update 'k-kinzal/ztd-query-*' --with-all-dependencies
  --minimal-changes` reported "Nothing to modify in lock file"; the lock SHA-256
  `5cf40ce7…` is unchanged.

Regression verification was still executed on the unchanged lock (six runs
below) and produced identical outcomes. Because no package content changed, the
cycle then developed the pending priority-1 question `WRK-mysqli-native-parity`:
where does `ZtdMysqli` diverge from native `mysqli` for application code that
was written against the extension? The
[expectation](../../../../spec/expectations/SPEC-mysqli-native-parity.md) was
written before execution from the adapter README, the ztd-query-mysql spec,
upstream PR statements (#219 identities, #225 transactions, #213 exception
translation) and native mysqlnd behavior, with a native control run inside the
scenario so that a control mismatch marks a scenario defect rather than a ZTD
problem.

## Execution evidence

Lock: `baselines/locks/5cf40ce75ff397286bc40f7646df41e852e84ae6dfade4efe23d2e1573beec92.lock`
(unchanged). Working tree revision `8ec02fd` plus the uncommitted scenario,
expectation and finding files; the runner recorded `dirty=true` and snapshots
the sources. MySQL for every MySQL run: container `ztd-parity-mysql-20260926`,
image `mysql@sha256:7dcddc01f13bab2f15cde676d44d01f61fc9f99fe7785e86196dfc07d358ae2b`,
server 8.0.46, `--performance-schema=OFF --innodb-buffer-pool-size=32M
--innodb-log-buffer-size=8M --max-connections=20`, database `ztd_types`, host
port 55706. PHP 8.1 runs used the retained image
`sha256:3415e059fb3ae1bf23c52f053d6db09554eac9fe7d7d58329b8d726686bafe1b`
(PHP 8.1.34, Linux aarch64, mysqlnd 8.1.34) joined to the container network;
host runs used PHP 8.5.8 (Darwin arm64, mysqlnd 8.5.8, SQLite 3.53.3).

### Regression reruns on the unchanged lock

| Scenario | Previous run (BASE-20260926-main-aac51e9) | This cycle | Result |
| --- | --- | --- | --- |
| SCN-sqlite-fixture-lifecycle, PHP 8.5.8 | [142322](../CYC-20260926T142139580320Z-upstream-regression/runs/20260926T142322706363Z-SCN-sqlite-fixture-lifecycle/run.json) | [200600](runs/20260926T200600783062Z-SCN-sqlite-fixture-lifecycle/run.json) exit 0,0 | unchanged (only PHPUnit timing line differs) |
| SCN-sqlite-insert-column-order, PHP 8.5.8 | [142328](../CYC-20260926T142139580320Z-upstream-regression/runs/20260926T142328492503Z-SCN-sqlite-insert-column-order/run.json) | [200601](runs/20260926T200601289941Z-SCN-sqlite-insert-column-order/run.json) exit 1,1 | unchanged; #465 still reproduces |
| SCN-mysql-product-types, PHP 8.5.8 | [142402](../CYC-20260926T142139580320Z-upstream-regression/runs/20260926T142402163041Z-SCN-mysql-product-types/run.json) | [200601](runs/20260926T200601642725Z-SCN-mysql-product-types/run.json) exit 0 | identical after normalising table names |
| SCN-mysql-product-types, PHP 8.1.34 | [142531](../CYC-20260926T142139580320Z-upstream-regression/runs/20260926T142531051949Z-SCN-mysql-product-types/run.json) | [200602](runs/20260926T200602805229Z-SCN-mysql-product-types/run.json) exit 0 | identical |
| SCN-mysql-session-schema, PHP 8.5.8 | [142939](../CYC-20260926T142139580320Z-upstream-regression/runs/20260926T142939589356Z-SCN-mysql-session-schema/run.json) | [200602](runs/20260926T200602464374Z-SCN-mysql-session-schema/run.json) exit 1 | identical; same four unmet MySQLi checks (#471) |
| SCN-mysql-session-schema, PHP 8.1.34 | [142950](../CYC-20260926T142139580320Z-upstream-regression/runs/20260926T142950478363Z-SCN-mysql-session-schema/run.json) | [200604](runs/20260926T200604519847Z-SCN-mysql-session-schema/run.json) exit 1 | identical |

`lab.py compare` reported the same step exit codes for every pair
(`requires-review`); the review found no outcome transition.

```sh
python3 scripts/lab.py run SCN-sqlite-fixture-lifecycle --cycle CYC-20260926T200050738102Z-mysqli-native-parity
python3 scripts/lab.py run SCN-sqlite-insert-column-order --cycle CYC-20260926T200050738102Z-mysqli-native-parity
ZTD_TYPES_DSN='mysql:host=127.0.0.1;port=55706;dbname=ztd_types;charset=utf8mb4' \
  python3 scripts/lab.py run SCN-mysql-product-types --cycle CYC-20260926T200050738102Z-mysqli-native-parity
ZTD_MYSQL_PORT=55706 python3 scripts/lab.py run SCN-mysql-session-schema --cycle CYC-20260926T200050738102Z-mysqli-native-parity
ZTD_TYPES_NETWORK=container:ztd-parity-mysql-20260926 ZTD_TYPES_DSN='mysql:host=127.0.0.1;port=3306;dbname=ztd_types;charset=utf8mb4' \
  python3 scripts/lab.py run SCN-mysql-product-types --cycle CYC-20260926T200050738102Z-mysqli-native-parity --php scenarios/types/SCN-mysql-product-types/php81
ZTD_MYSQL_NETWORK=container:ztd-parity-mysql-20260926 ZTD_MYSQL_PORT=3306 \
  python3 scripts/lab.py run SCN-mysql-session-schema --cycle CYC-20260926T200050738102Z-mysqli-native-parity --php scenarios/schema/SCN-mysql-session-schema/php81
```

### New scenario: SCN-mysqli-native-parity

[Manifest](../../../../scenarios/parity/SCN-mysqli-native-parity/scenario.json),
[script](../../../../scenarios/parity/SCN-mysqli-native-parity/scenario.php).
The script runs one fixed sequence of mysqli operations on a native `mysqli`
connection with a physical table (dropped afterwards) and on a `ZtdMysqli`
with a session-only table, then evaluates 64 explicit expectations for both.

| Runtime | Run | Exit | Unmet (new) | Unmet (known #471/#472) | Control mismatches |
| --- | --- | --- | --- | --- | --- |
| PHP 8.5.8 / MySQL 8.0.46 | [201422](runs/20260926T201422531791Z-SCN-mysqli-native-parity/run.json) | 1 | 39 | 4 | 0 |
| PHP 8.1.34 / MySQL 8.0.46 | [201423](runs/20260926T201423986360Z-SCN-mysqli-native-parity/run.json) | 1 | 39 | 4 | 5 (scenario defect, see below) |
| PHP 8.5.8 / MySQL 8.0.46 | [201657](runs/20260926T201657950162Z-SCN-mysqli-native-parity/run.json), [output](runs/20260926T201657950162Z-SCN-mysqli-native-parity/01-output.txt) | 1 | 39 | 4 | 0 |
| PHP 8.1.34 / MySQL 8.0.46 | [201659](runs/20260926T201659423693Z-SCN-mysqli-native-parity/run.json), [output](runs/20260926T201659423693Z-SCN-mysqli-native-parity/01-output.txt) | 1 | 39 | 4 | 0 |

The first PHP 8.1 run had five control mismatches because native
`mysqli::execute_query()` only exists since PHP 8.2; the missing insert shifted
later row counts. The scenario was corrected to insert that row with `query()`
when the method is absent (`execute_query_available` records which path ran),
and both runtimes were re-recorded. ZTD outcomes were identical before and
after the correction and between PHP versions (only random table names differ).

Met on both runtimes: `affected` counts via `lastAffectedRows()`, prepared
`execute()`, `real_query()` return values, `query($sql, MYSQLI_USE_RESULT)`,
`multi_query('SELECT 1; SELECT 2')`, `fetch_fields()` names, `num_rows`,
`query()` returning `false` for an unknown table with reporting off, all five
transaction checks (`begin_transaction`/`rollback`/`commit`, `autocommit(false)`,
SQL `START TRANSACTION`/`ROLLBACK`, `savepoint()` + `ROLLBACK TO SAVEPOINT`),
`stmt->store_result()`, `bind_result()`/`fetch()` buffered and unbuffered,
`get_result()` typing, physical absence, `fromMysqli()` session table,
`fromMysqli()` isolation from the underlying handle, and
`MYSQLI_OPT_INT_AND_FLOAT_NATIVE` through `fromMysqli()`.

Unmet, grouped by finding (each reduced to a self-contained script in
`findings/` and run on both runtimes, outputs in [repro/](repro/)):

| Finding | Unmet checks | Repro exit PHP 8.5 / 8.1 |
| --- | --- | --- |
| [FND-mysqli-insert-id](../../../../findings/FND-mysqli-insert-id/README.md) | `insert_id_*` (8), `stmt_insert_id`, `last_insert_id_sql`, `fromMysqli.insert_id` | [1](repro/insert-id-php85-output.txt) / [1](repro/insert-id-php81-output.txt) |
| [FND-mysqli-state-properties](../../../../findings/FND-mysqli-state-properties/README.md) | `stmt_affected_rows`, `field_count_after_*`, `stmt_num_rows`, `stmt_field_count`, `off_unknown_table_errno/error/sqlstate`, `off_*_errno` | [1](repro/state-properties-php85-output.txt) / [1](repro/state-properties-php81-output.txt) |
| [FND-mysqli-error-reporting](../../../../findings/FND-mysqli-error-reporting/README.md) | `off_syntax_return`, `off_duplicate_return`, `off_not_null_return`, `strict_duplicate`, `strict_not_null`, `strict_syntax`, `strict_stmt_duplicate` | [1](repro/error-reporting-php85-output.txt) / [1](repro/error-reporting-php81-output.txt) |
| [FND-mysqli-stmt-init](../../../../findings/FND-mysqli-stmt-init/README.md) | `stmt_init_prepare` | [1](repro/stmt-init-php85-output.txt) / [1](repro/stmt-init-php81-output.txt) |
| [FND-mysqli-real-query-results](../../../../findings/FND-mysqli-real-query-results/README.md) | `store_result_after_select`, `use_result` | [1](repro/real-query-results-php85-output.txt) / [1](repro/real-query-results-php81-output.txt) |
| [FND-mysqli-multi-query](../../../../findings/FND-mysqli-multi-query/README.md) | `multi_query_dml` | [1](repro/multi-query-php85-output.txt) / [1](repro/multi-query-php81-output.txt) |
| [FND-mysqli-result-metadata](../../../../findings/FND-mysqli-result-metadata/README.md) | `fetch_fields_types`, `fetch_fields_orgtable`, `fetch_fields_flags` | [1](repro/result-metadata-php85-output.txt) / [1](repro/result-metadata-php81-output.txt) |
| [FND-mysqli-unconnected-construct](../../../../findings/FND-mysqli-unconnected-construct/README.md) | `unconnected.construct`, `option_native_types.real_connect` | [1](repro/unconnected-construct-php85-output.txt) / [1](repro/unconnected-construct-php81-output.txt) |
| known #471 | `create_table`, `insert_first`, `execute_query_insert` | – |
| known #472 | `query_default_types` | – |

```sh
ZTD_MYSQL_PORT=55706 python3 scripts/lab.py run SCN-mysqli-native-parity --cycle CYC-20260926T200050738102Z-mysqli-native-parity
ZTD_MYSQL_NETWORK=container:ztd-parity-mysql-20260926 ZTD_MYSQL_PORT=3306 \
  python3 scripts/lab.py run SCN-mysqli-native-parity --cycle CYC-20260926T200050738102Z-mysqli-native-parity \
  --php scenarios/parity/SCN-mysqli-native-parity/php81 --timeout 600
# reductions, host PHP 8.5.8 and PHP 8.1 image respectively
ZTD_MYSQL_PORT=55706 php findings/FND-mysqli-<name>/repro.php
ZTD_MYSQL_NETWORK=container:ztd-parity-mysql-20260926 ZTD_MYSQL_PORT=3306 \
  scenarios/parity/SCN-mysqli-native-parity/php81 findings/FND-mysqli-<name>/repro.php
```

Observation only (documented limitation): reading `$ztd->affected_rows` throws
`Error: Property access is not allowed yet`; the README says the property is
unavailable, so this is not counted.

## Classification and upstream disposition

- **Upstream advanced without ZTD package change.** Monorepo `main` moved to
  `249944e`; tree SHAs prove every `packages/ztd-query-*` directory is
  unchanged and the lock did not move. The six regression reruns show no
  outcome change. A new baseline record
  [BASE-20260926-main-249944e](../../../../baselines/BASE-20260926-main-249944e.json)
  adopts the same lock with this alignment evidence.
- **No regression.** Every curated scenario reproduced its previous outcome
  under comparable runtimes.
- **Scenario defect (corrected):** the PHP 8.1 control used
  `execute_query()`, absent before PHP 8.2; corrected and re-recorded. The
  initial runs are retained.
- **Existing problems, reported (eight new issues, all reproduced on the exact
  lock on PHP 8.5.8 and 8.1.34 with MySQL 8.0.46; issue searches in each
  finding's `issue-search.md` found no prior report):**
  - [#483](https://github.com/k-kinzal/ztd-query-php/issues/483) generated
    keys unreadable (`insert_id` throws, `LAST_INSERT_ID()` returns 0) —
    FND-mysqli-insert-id.
  - [#484](https://github.com/k-kinzal/ztd-query-php/issues/484) connection and
    statement properties throw; `errno`/`error` empty after a failed query —
    FND-mysqli-state-properties.
  - [#485](https://github.com/k-kinzal/ztd-query-php/issues/485) constraint
    violations and syntax errors raise `ZtdMysqliException` code 0 instead of
    `mysqli_sql_exception` 1062/1364/1064, also under `MYSQLI_REPORT_OFF` —
    FND-mysqli-error-reporting.
  - [#486](https://github.com/k-kinzal/ztd-query-php/issues/486) `stmt_init()`
    bypasses the session — FND-mysqli-stmt-init.
  - [#487](https://github.com/k-kinzal/ztd-query-php/issues/487) `real_query()`
    results unreadable through `store_result()`/`use_result()` —
    FND-mysqli-real-query-results.
  - [#488](https://github.com/k-kinzal/ztd-query-php/issues/488) `multi_query()`
    not applied to session tables — FND-mysqli-multi-query.
  - [#489](https://github.com/k-kinzal/ztd-query-php/issues/489) `fetch_fields()`
    metadata differs — FND-mysqli-result-metadata (the issue notes that
    documenting the behavior would be acceptable if intended).
  - [#490](https://github.com/k-kinzal/ztd-query-php/issues/490)
    `new ZtdMysqli()` without arguments connects immediately —
    FND-mysqli-unconnected-construct.
- **Existing problem, additional evidence pending:** `execute_query()` for an
  INSERT also returns `mysqli_result` (#471). Posting a comment failed twice
  with HTTP 403 `Resource not accessible by integration` (the active `gh`
  login is the installation token `quuu824016[bot]`, which can create issues
  but not comment). The comment text is retained in
  [comment-execute-query.md](../../../../findings/FND-mysqli-query-return-type/comment-execute-query.md).
- **Met expectations:** transactions through the API and SQL (basis PR #225),
  `fromMysqli()` isolation, `bind_result()` paths and `get_result()` typing all
  behaved as native `mysqli` in this scope.

The basis for every expectation is the native `mysqli` contract plus the README
statement that `ZtdMysqli` can be passed wherever the application expects a
mysqli connection; the native control inside the scenario satisfied every
expectation on both runtimes (`control_mismatches: 0` in the corrected runs),
so the expectations themselves are validated against the real driver.

## Remaining work

Baseline: [BASE-20260926-main-249944e](../../../../baselines/BASE-20260926-main-249944e.json)
is now current (same lock, upstream `249944e`, alignment via tree SHAs). Scope
remains partial: PostgreSQL, MySQL 8.4/9.x, PHP 8.2–8.4 and the wider legacy
suites were not run.

Work items: `WRK-mysqli-native-parity` stays ready for the remaining gaps
(async queries, `change_user`/`select_db`/`set_charset`, `get_warnings()`),
for posting the pending #471 comment once a token with issue-write access is
available, and for re-verification when upstream responds. New
`WRK-pdo-error-reporting` (priority 1) records that an ad hoc probe during this
cycle saw `ZtdPdo` raise `ZtdPdoException` code 0 for a duplicate key where
PDO raises `PDOException` SQLSTATE 23000; that observation was not retained as
a run and needs its own expectation and scenario. `WRK-supported-matrix`
carries this cycle's runtime coverage.

Validation: `python3 scripts/lab.py validate` and
`python3 -m unittest discover -s scripts/tests`. Cleanup after the cycle:
`docker rm -fv ztd-parity-mysql-20260926`.
