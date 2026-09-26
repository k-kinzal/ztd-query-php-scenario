# CYC-20260926T142139580320Z-upstream-regression

## Decision and scope

The [upstream check](upstream.json) at 2026-09-26 14:21:39 UTC found that
monorepo `main` moved from `3a6c7e361a1613a7d75288a4628058e2b3af0e67` to
`aac51e9d99121f79c98720fec3f8ddc03bf7c59b` and that four split packages
advanced: ztd-query-mysql (`bce93c33…` → `969eb5d9…`), ztd-query-mysqli-adapter
(`0faf44e7…` → `3fe31d57…`), ztd-query-pdo-adapter (`7f50ca47…` → `3ed255a4…`)
and ztd-query-postgres (`e25aa7ee…` → `02bcf6de…`). ztd-query-core and
ztd-query-sqlite are unchanged. The regression branch was therefore selected.

Alignment: the GitHub compare of the two monorepo commits lists nine commits
from PRs #467 and #468. Each advanced split package's head commit is titled
"Merge pull request #468 …" (2026-09-26T12:22–12:24Z). PR #467 touched no
`packages/ztd-query-*` path, and PR #468 changed source only in
`packages/ztd-query-mysqli-adapter/src` (two files), otherwise tests, fuzz and
bench files. That metadata is the only basis for saying the split set
corresponds to `aac51e9`; no implementation was inspected.

Regression verification reran the three curated scenarios on the new lock and
compared them with the retained runs on `BASE-20260926-main`. Because the only
source change is in the MySQLi adapter and no curated scenario used MySQLi, the
cycle also executed the pending `WRK-session-created-schema` question, the
documented no-migration workflow, through both PDO and MySQLi on MySQL. The
[expectation](../../../../spec/expectations/SPEC-session-created-schema.md) was
written before execution from the adapter READMEs and native driver behavior.

## Execution evidence

Lock before: `baselines/locks/1250f878…lock` (previous baseline, preserved).
Lock after: `baselines/locks/5cf40ce7…lock`, produced by
`composer update 'k-kinzal/ztd-query-*' --with-all-dependencies --minimal-changes`;
only the four ZTD packages changed. All run records carry the complete lock,
source snapshot, package references and runtime metadata. Working tree revision
`fb2b44d` plus the uncommitted scenario files; the runner recorded `dirty=true`.

MySQL service for every MySQL run: container `ztd-regress-mysql-20260926`,
image `mysql@sha256:7dcddc01f13bab2f15cde676d44d01f61fc9f99fe7785e86196dfc07d358ae2b`,
server 8.0.46, `--performance-schema=OFF --innodb-buffer-pool-size=32M
--innodb-log-buffer-size=8M --max-connections=20`, database `ztd_types`, host
port 63249. PHP 8.1 runs used the retained image
`sha256:3415e059fb3ae1bf23c52f053d6db09554eac9fe7d7d58329b8d726686bafe1b`
(PHP 8.1.34, Linux aarch64, mysqlnd 8.1.34, SQLite 3.46.1) joined to the
container network; host runs used PHP 8.5.8 (Darwin arm64, mysqlnd 8.5.8,
SQLite 3.53.3). Docker 29.7.2, Composer 2.9.3, PHPUnit 10.5.65.

### Existing scenarios, old lock versus new lock

| Scenario | Old run (BASE-20260926-main) | New run | Result |
| --- | --- | --- | --- |
| SCN-sqlite-fixture-lifecycle, PHP 8.5.8 | [092528](../CYC-20260926T092050957424Z-repository-renewal/runs/20260926T092528699261Z-SCN-sqlite-fixture-lifecycle/run.json) exit 0,0 | [142322](runs/20260926T142322706363Z-SCN-sqlite-fixture-lifecycle/run.json) exit 0,0 | unchanged, expectation met |
| SCN-sqlite-insert-column-order, PHP 8.5.8 | [092623](../CYC-20260926T092050957424Z-repository-renewal/runs/20260926T092623785666Z-SCN-sqlite-insert-column-order/run.json) exit 1,1 | [142328](runs/20260926T142328492503Z-SCN-sqlite-insert-column-order/run.json) exit 1,1, [output](runs/20260926T142328492503Z-SCN-sqlite-insert-column-order/01-output.txt) | unchanged; #465 still reproduces (`id=19, name="10", price=0`) |
| SCN-mysql-product-types, PHP 8.5.8 / MySQL 8.0.46 | [094607](../CYC-20260926T093729409229Z-pdo-result-types/runs/20260926T094607042313Z-SCN-mysql-product-types/run.json) exit 0 | [142402](runs/20260926T142402163041Z-SCN-mysql-product-types/run.json) exit 0 | unchanged; 90 output lines identical after normalising table names and versions |
| SCN-mysql-product-types, PHP 8.1.34 / MySQL 8.0.46 | [094551](../CYC-20260926T093729409229Z-pdo-result-types/runs/20260926T094551697374Z-SCN-mysql-product-types/run.json) exit 0 | [142531](runs/20260926T142531051949Z-SCN-mysql-product-types/run.json) exit 0 | unchanged; identical observations |

`lab.py compare` reported the same step exit codes for every pair and
`requires-review`; the review above found no outcome transition. Commands:

```sh
python3 scripts/lab.py run SCN-sqlite-fixture-lifecycle --cycle CYC-20260926T142139580320Z-upstream-regression
python3 scripts/lab.py run SCN-sqlite-insert-column-order --cycle CYC-20260926T142139580320Z-upstream-regression
ZTD_TYPES_DSN='mysql:host=127.0.0.1;port=63249;dbname=ztd_types;charset=utf8mb4' \
  python3 scripts/lab.py run SCN-mysql-product-types --cycle CYC-20260926T142139580320Z-upstream-regression
ZTD_TYPES_NETWORK=container:ztd-regress-mysql-20260926 \
ZTD_TYPES_DSN='mysql:host=127.0.0.1;port=3306;dbname=ztd_types;charset=utf8mb4' \
  python3 scripts/lab.py run SCN-mysql-product-types --cycle CYC-20260926T142139580320Z-upstream-regression \
  --php scenarios/types/SCN-mysql-product-types/php81
```

Environment note: the first PHP 8.1 attempt of SCN-mysql-product-types timed
out in the runner's 60 s interpreter probe while Docker started the image cold;
no run record was created. The wrapper then answered in about one second and
the retry above completed.

### New scenario: SCN-mysql-session-schema

[Manifest](../../../../scenarios/schema/SCN-mysql-session-schema/scenario.json),
[script](../../../../scenarios/schema/SCN-mysql-session-schema/scenario.php).
Random table names per adapter; no physical table is ever created by ZTD.
The native control reads `information_schema.TABLES` and a native `mysqli`
connection records the return value of `query()` for DDL/DML.

| Runtime | Run | Exit | Met | Unmet |
| --- | --- | --- | --- | --- |
| PHP 8.5.8 / MySQL 8.0.46 | [142939](runs/20260926T142939589356Z-SCN-mysql-session-schema/run.json), [output](runs/20260926T142939589356Z-SCN-mysql-session-schema/01-output.txt) | 1 | 23 | mysqli.create_table, mysqli.insert_affected, mysqli.update_affected, mysqli.delete_affected |
| PHP 8.1.34 / MySQL 8.0.46 | [142950](runs/20260926T142950478363Z-SCN-mysql-session-schema/run.json), [output](runs/20260926T142950478363Z-SCN-mysql-session-schema/01-output.txt) | 1 | 23 | same four |

Met on both runtimes: all thirteen PDO checks (session `CREATE TABLE`, `exec()`
returning 2/1/1 affected rows, the README query returning `['Alice']`, typed
lookup `id` int / `name` string / `active` int, update visibility, count after
delete, no physical table before and after writes, fresh `ZtdPdo` and
`disableZtd()` both raising SQLSTATE 42S02 / 1146, `enableZtd()` restoring the
session) and the corresponding MySQLi checks for `lastAffectedRows()`,
`execute_query()` (also on PHP 8.1), `prepare()/bind_param()/get_result()`,
typed lookup, visibility, physical absence, fresh session, disable/enable.

Unmet: `ZtdMysqli::query()` returned a `mysqli_result` object for `CREATE
TABLE`, `INSERT`, `UPDATE` and `DELETE`, while the native control returned
`true` for the same statements. Affected-row counts were correct.

```sh
ZTD_MYSQL_PORT=63249 python3 scripts/lab.py run SCN-mysql-session-schema --cycle CYC-20260926T142139580320Z-upstream-regression
ZTD_MYSQL_NETWORK=container:ztd-regress-mysql-20260926 ZTD_MYSQL_PORT=3306 \
  python3 scripts/lab.py run SCN-mysql-session-schema --cycle CYC-20260926T142139580320Z-upstream-regression \
  --php scenarios/schema/SCN-mysql-session-schema/php81
```

### Reductions and old-lock replay

Two self-contained reproductions were run with the new lock on both PHP
versions and, from a disposable checkout installed from the previous lock
([old-lock-replay/README.md](old-lock-replay/README.md)), on the same two
runtimes and database:

| Reproduction | New lock PHP 8.5 | New lock PHP 8.1 | Old lock PHP 8.5 | Old lock PHP 8.1 |
| --- | --- | --- | --- | --- |
| [FND-mysqli-query-return-type/repro.php](../../../../findings/FND-mysqli-query-return-type/repro.php) | [exit 1](repro-php85-output.txt) | [exit 1](repro-php81-output.txt) | [exit 1](old-lock-replay/repro-php85-output.txt) | [exit 1](old-lock-replay/repro-php81-output.txt) |
| [FND-mysqli-query-native-types/repro.php](../../../../findings/FND-mysqli-query-native-types/repro.php) | [exit 1](repro-types-php85-output.txt) | [exit 1](repro-types-php81-output.txt) | [exit 1](old-lock-replay/repro-types-php85-output.txt) | [exit 1](old-lock-replay/repro-types-php81-output.txt) |
| SCN-mysql-session-schema script | run 142939 above | run 142950 above | [exit 1, same four unmet](old-lock-replay/php85-output.txt) | [exit 1, same four unmet](old-lock-replay/php81-output.txt) |

The first reproduction shows native `true` versus ZTD `mysqli_result` for
CREATE TABLE, INSERT, UPDATE, DELETE and DROP TABLE, and the idiom
`$mysqli->query($ddl) === true` evaluating to `false` under ZTD.

Its SELECT control exposed a second divergence: with default options native
`mysqli::query()` returns every column as a string, whereas `ZtdMysqli::query()`
returns `int`/`float` for INT, BIGINT, DOUBLE and BOOLEAN columns. The second
reproduction confirms this against three controls: native default (strings),
native with `MYSQLI_OPT_INT_AND_FLOAT_NATIVE` (native types, identical to ZTD)
and `prepare()->get_result()` (native types under both, matching).

## Classification and upstream disposition

- **No regression.** Every curated scenario produced the same outcomes and
  observations on the new lock as on `BASE-20260926-main` under comparable
  runtimes. #465 remains an existing problem (the finding record is unchanged;
  this cycle's run is additional evidence, not a new report).
- **Existing problem, reported:** `ZtdMysqli::query()` returns `mysqli_result`
  for statements without a result set. Reproduces on both locks and both PHP
  versions. [FND-mysqli-query-return-type](../../../../findings/FND-mysqli-query-return-type/README.md),
  [issue search](../../../../findings/FND-mysqli-query-return-type/issue-search.md)
  found no prior report; filed as
  [#471](https://github.com/k-kinzal/ztd-query-php/issues/471).
- **Existing problem, reported:** `ZtdMysqli::query()` result columns carry
  native int/float types where native mysqli returns strings. Reproduces on both
  locks and both PHP versions. [FND-mysqli-query-native-types](../../../../findings/FND-mysqli-query-native-types/README.md),
  [issue search](../../../../findings/FND-mysqli-query-native-types/issue-search.md)
  found no prior report; filed as
  [#472](https://github.com/k-kinzal/ztd-query-php/issues/472). The issue notes
  that documenting the behavior would be an acceptable alternative if intended.
- **Environment failure (no record):** the cold-start probe timeout described
  above; resolved by retrying.
- The PDO adapter met every session-schema expectation on both runtimes; no
  PDO problem was found in this scope.

Basis for both expectations is the native `mysqli` contract plus the README
statement that `ZtdMysqli` can be passed wherever the application expects a
mysqli connection; neither README documents a different return value or
result typing. Neither finding is attributed to the PR #468 source change,
because the previous lock behaves identically.

## Remaining work

Baseline: [BASE-20260926-main-aac51e9](../../../../baselines/BASE-20260926-main-aac51e9.json)
is now current, with the lock snapshot and the tested scope stated in the
record. The scope is partial: PostgreSQL, MySQL 8.4/9.x, PHP 8.2–8.4, MySQLi
beyond the session-schema workflow, and the wider legacy suites were not run.

Work items: `WRK-session-created-schema` stays ready with PostgreSQL/SQLite,
`unknownSchemaBehavior: Exception` and ALTER/DROP as the next steps;
`WRK-supported-matrix` carries this cycle's runtime coverage; new
`WRK-mysqli-native-parity` (priority 1) covers plain `query()` typing with an
explicit expectation, `insert_id`, `multi_query`, `store_result` and
`fromMysqli()`.

Validation: `python3 scripts/lab.py validate` and
`python3 -m unittest discover -s scripts/tests`. Cleanup after the cycle:
`docker rm -fv ztd-regress-mysql-20260926`; the ignored `build/oldlock/`
checkout may be deleted.
