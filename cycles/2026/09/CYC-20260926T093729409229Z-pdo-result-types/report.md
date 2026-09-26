# CYC-20260926T093729409229Z-pdo-result-types

## Decision and scope

No library problem was found in the selected MySQL PDO product workflow on
PHP 8.1.34 or 8.5.8 with MySQL 8.0.46. Exact values, PHP types, decimal rounding,
NULL handling, fetch-option changes, and physical isolation met the expectation.

The [upstream check](upstream.json) at 2026-09-26 09:37:29 UTC found no change in
upstream `main` (`3a6c7e361a1613a7d75288a4628058e2b3af0e67`), all six split
references, or the lock relative to `BASE-20260926-main`. This is the
scenario-development branch. Package alignment is inherited from that baseline;
no implementation was inspected. Dependencies and the baseline pointer remain
unchanged. Exact package references are in the check and every run record.

Selected work: `WRK-pdo-result-types`. An application saves product fixtures and
reads them into strictly typed code or JSON. The
[expectation](../../../../spec/expectations/SPEC-pdo-product-types.md) was written
before execution. The [scenario](../../../../scenarios/types/SCN-mysql-product-types/scenario.json)
uses only PDO and ZtdPdo public APIs. Its basis is the public PDO replacement and
isolation contract, PHP's PDO attribute documentation, and clean native controls.

Legacy discovery used `lab.py catalog pdo --legacy`, `catalog SPEC-13 --legacy`,
and selective reads of `MysqlTypeRoundtripTest.php`, SPEC-12.4/12.5 and
SPEC-13.1/13.4. The older numeric tests usually cast results before asserting
them. This scenario retains exact fetched types. The work item's old references
to SPEC-12.1/12.6 concern error/null modes; emulation/stringification are actually
SPEC-12.4/12.5. Frozen legacy files were not changed.

## Execution evidence

### Final results

| Runtime | Retained run | Result |
| --- | --- | --- |
| PHP/mysqlnd 8.1.34, Linux aarch64; MySQL 8.0.46 | [metadata](runs/20260926T094551697374Z-SCN-mysql-product-types/run.json), [output](runs/20260926T094551697374Z-SCN-mysql-product-types/01-output.txt) | Exit 0, all desired outcomes met |
| PHP/mysqlnd 8.5.8, Darwin arm64; MySQL 8.0.46 | [metadata](runs/20260926T094607042313Z-SCN-mysql-product-types/run.json), [output](runs/20260926T094607042313Z-SCN-mysql-product-types/01-output.txt) | Exit 0, all desired outcomes met |

Each runtime exercised eight combinations: native/emulated prepares,
stringification initially off/on, and execute-array/explicit bindValue binding.
Each combination independently created native and ZTD tables and inserted two
products. Both query and prepare reads ran with stringification off and on.
That is 64 exact row observations per runtime, including both controls and ZTD.
All eight native/ZTD comparisons and all physical isolation checks passed on
each runtime. These are observations of the selected workflow, not a claim
about the wider historical suite.

With stringification disabled, expected and actual rows on both runtimes were:

| Column | Product 1 | Product 2 (convertible inputs) | PHP type |
| --- | --- | --- | --- |
| id INT | 1 | 2 | int |
| stock INT | 7 | 42, from input `'42'` | int |
| external_id BIGINT | 9000000001 | 9000000002, from string input | int |
| weight DOUBLE | 1.25 | 2.75, from string input | float |
| price DECIMAL(10,2) | `'12.50'` | `'100.00'`, from `'99.999'` | string |
| sku VARCHAR | `'SKU-A'` | `'1203'`, from integer input | string |
| active BOOLEAN | 1 | 0 | int |
| note VARCHAR NULL | NULL | empty string | null / string |

With stringification enabled every non-NULL value became its expected string;
NULL stayed NULL. Both writes returned true. Native physical rows contained the
products, while each ZTD physical table remained empty. The script records the
value and `get_debug_type()` of every fetched cell without coercing observations.

Every run retains the complete lock, exact source archive including local
changes, Git revision (`a0549dbfa7270a27fc578f33b8873b68f890d55f`), runtime and
driver versions, commands, raw output, and hashes. Both final runs contain the
same corrected scenario source. PHPUnit 10.5.65 is installed and recorded, but
these standalone PHP steps do not invoke PHPUnit. Composer is 2.9.3 and Docker
client/server are 29.7.2. Service images and startup options are in
[environment.json](environment.json). Both final runs used the same smaller
MySQL instance, utf8mb4, and the SQL mode emitted in their output.

### Reproduction

The schema, inputs, binding types, operation order, and cleanup are all in
[scenario.php](../../../../scenarios/types/SCN-mysql-product-types/scenario.php).
No fixtures or PHPUnit helpers are required after `composer install`.
Only random tables created by this script are dropped.

```sh
composer install
docker run -d --name ztd-pdo-types-small-20260926 \
  -e MYSQL_ROOT_PASSWORD=root -e MYSQL_DATABASE=ztd_types \
  -p 127.0.0.1::3306 \
  mysql@sha256:7dcddc01f13bab2f15cde676d44d01f61fc9f99fe7785e86196dfc07d358ae2b \
  --performance-schema=OFF --innodb-buffer-pool-size=32M \
  --innodb-log-buffer-size=8M --max-connections=20
docker logs ztd-pdo-types-small-20260926
# Wait for the final server's ready-for-connections message.
docker port ztd-pdo-types-small-20260926
# Substitute the printed host port; this run used 54247.
ZTD_TYPES_DSN='mysql:host=127.0.0.1;port=54247;dbname=ztd_types;charset=utf8mb4' \
  php scenarios/types/SCN-mysql-product-types/scenario.php
```

Retained run commands, in addition to the connection setup above:

```sh
ZTD_TYPES_DSN='mysql:host=127.0.0.1;port=54247;dbname=ztd_types;charset=utf8mb4' \
  python3 scripts/lab.py run SCN-mysql-product-types \
  --cycle CYC-20260926T093729409229Z-pdo-result-types

ZTD_TYPES_NETWORK=container:ztd-pdo-types-small-20260926 \
ZTD_TYPES_DSN='mysql:host=127.0.0.1;port=3306;dbname=ztd_types;charset=utf8mb4' \
  python3 scripts/lab.py run SCN-mysql-product-types \
  --cycle CYC-20260926T093729409229Z-pdo-result-types \
  --php scenarios/types/SCN-mysql-product-types/php81
```

The PHP 8.1 wrapper pins the locally retained image
`sha256:3415e059fb3ae1bf23c52f053d6db09554eac9fe7d7d58329b8d726686bafe1b`,
mounting this checkout and its locked vendor directory. It joins the database
container's network so its DSN uses port 3306. If that image is unavailable,
`docker build --build-arg PHP_VERSION=8.1 -t ztd-types-php81 .` and
`ZTD_TYPES_PHP_IMAGE=ztd-types-php81` select a newly built image. Record its actual
versions as a new run; the moving base tag does not guarantee an exact replay.

Cleanup after verification:

```sh
docker rm -fv ztd-pdo-types-small-20260926 ztd-pdo-types-20260926
```

## Classification and upstream disposition

No confirmed library issue or regression was found. No upstream report was
filed. [Open and closed issue searches](issue-search.json) retained the terms
`type`, `STRINGIFY_FETCHES`, and `EMULATE_PREPARES`, commands, timestamps, and
results. These searches are context only; issue titles or closure states were
not used as behavioral evidence.

Earlier runs remain immutable:

- [Initial PHP 8.5 run](runs/20260926T094118781205Z-SCN-mysql-product-types/run.json)
  passed with the original scenario and default MySQL startup options. Its
  [output](runs/20260926T094118781205Z-SCN-mysql-product-types/01-output.txt)
  contains the same successful product observations.
- **Environment failure:** [first PHP 8.1 run](runs/20260926T094215657006Z-SCN-mysql-product-types/run.json)
  exited 255 while connecting through `host.docker.internal` with
  [Network is unreachable](runs/20260926T094215657006Z-SCN-mysql-product-types/01-output.txt).
  No product operations ran. Subsequent Docker inspection found the original
  MySQL container exited 137 with `OOMKilled=true`; its finish time was after
  the completed PHP 8.5 run. A retry joining that stopped container failed in
  the interpreter probe before a run record could be created. Both details
  are retained in environment.json. Creating a smaller MySQL instance and
  sharing its network allowed both final executions to finish.
- **Scenario defect:** [second PHP 8.1 run](runs/20260926T094435364750Z-SCN-mysql-product-types/run.json)
  connected but both native and ZTD cases failed before writing because the
  scenario read `ATTR_STRINGIFY_FETCHES` via `getAttribute()`. The
  [native-only probe](attribute-probe.php) and [output](attribute-probe-output.txt)
  show that this driver rejects that readback with IM001 while accepting
  `setAttribute()` and returning strings. The correction removes that optional
  readback, checks setter success, and preserves exact output expectations.
  Both final runs passed after the correction. This was not a ZTD defect.

Native probe command (exit 0): use the PHP 8.1 environment above and execute
`scenarios/types/SCN-mysql-product-types/php81 cycles/2026/09/CYC-20260926T093729409229Z-pdo-result-types/attribute-probe.php`.

## Remaining work

`WRK-pdo-result-types` remains ready with these results attached. The next
bounded question is exact fetched types after convertible inserts on SQLite
and PostgreSQL, using expectations appropriate to each database. The current
passing MySQL scenario does not establish their behavior.

Still untested: PHP 8.2–8.4, MySQL 8.4/9.1, MySQLi, PostgreSQL, SQLite,
nonconvertible input and overflow, larger unsigned integers, UPDATE, alternate
null/error modes, session-only DDL, precision-sensitive floating values, and
existing physical rows. `WRK-supported-matrix` remains open for the broader
runtime matrix. The baseline pointer and dependency lock were not refreshed.

Validation: `python3 scripts/lab.py validate` and
`python3 -m unittest discover -s scripts/tests` (12 tests). The cycle is complete
for its bounded MySQL question, including classification of failed attempts;
the wider repository goal and remaining work are still open.
