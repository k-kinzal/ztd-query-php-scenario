# Docker preparation for supported versions

## Task and expected outcome

The user asked for Docker environments that can exercise supported PHP,
MySQL, PostgreSQL and SQLite versions. The bounded scope is a usable version
selector with actual runtime checks and retained evidence. Expected behavior:
selected runtimes build/start, native drivers can create and edit temporary
fixtures, requested versions match the running processes, the committed lock
is installed, optional scenario commands retain their exit codes, and each
cell cleans up its own databases. A configured selection alone is not proof
of library support.

The existing SQLite fixture expectation was also executed through the new
runner: [SPEC-isolated-fixture-lifecycle](../../../../spec/expectations/SPEC-isolated-fixture-lifecycle.md).
It expects session CRUD to leave physical seeds unchanged and a second ZTD
session empty. Infrastructure probes are explicitly native-driver checks,
not assertions about ZTD correctness.

## Upstream and regression branch

[upstream.json](upstream.json) checked all references at 2026-09-27 04:46:31 UTC.
Monorepo main advanced from `249944eaed3ea200ed235fb6473296c7e301608a` to
`e489d7daf1ea545ff3c03ccc216757ad85952bc4`. All six split-package refs stayed
unchanged. [Composer refresh](composer-update.txt) found nothing to modify.
The lock remains SHA-256
`5cf40ce75ff397286bc40f7646df41e852e84ae6dfade4efe23d2e1573beec92`.
The old lock is already retained in `baselines/locks/` and each curated run
links it. **Alignment with the new monorepo commit is unverified**; this cycle
checks the explicitly locked package set and does not refresh the baseline or
claim to verify latest main.

Before executing the new Docker runner, the five existing curated scenarios were rerun on host
PHP 8.5.8. SQLite was 3.53.3; MySQL was 8.0.46/mysqlnd 8.5.8, with the same SQL
mode and adapter settings as the previous PHP 8.5 baseline. The fresh MySQL
service identity/options are in [regression-service.json](regression-service.json).
It was removed after the runs.

- SQLite fixture lifecycle: both steps exit 0; desired expectations met.
- SQLite insert column order: both steps exit 1; existing issue #465 unchanged.
- MySQL product types: exit 0, zero failed checks, native/ZTD values identical.
- MySQL session schema: exit 1, the same four unmet MySQLi checks (#471).
- MySQLi parity: exit 1, the same 39 failed checks, four known unmet checks,
  zero native control mismatches (#471/#472/#483–#490).

[Runner comparisons](regression-comparison.txt) retain each pair's results.
The MySQL runner comparison flags non-comparable recorded conditions because
the source snapshot includes a changed `Dockerfile`. That file was not used
by these host-PHP regression runs; the scenario/helper source, PHP/driver/DB
runtime records and adapter options are identical. [Output review](regression-output-comparison.json)
found identical product-type and session-schema outputs after normalizing
random table names. Parity differs only in native connection `thread_id`
after the same normalization. No user expectation changed outcome.

[Known issue status check](known-issues.json) queried both open/closed states;
all the existing reports remain open. Repeating the same evidence does not
justify duplicate issues/comments. No new ZTD problem was found in this scope.
The infrastructure implementation errors described below are local setup
problems, not upstream findings.

## Implementation and execution

- [Dockerfile](../../../../Dockerfile): selectable PHP base, locked Composer
  install, all database drivers, selectable SQLite shared library with source
  SHA-256 and actual PHP-side version checks.
- [Compose configuration](../../../../docker/compose.matrix.yml): disposable
  MySQL/PostgreSQL services, readiness checks, no host ports, all needed
  adapter/scenario connection variables, selectable CPU architecture.
- [Matrix runner](../../../../scripts/docker-matrix.py) and
  [native probe](../../../../scripts/matrix-probe.php): source snapshots, exact
  commands, logs, actual versions, image IDs, lock refs, nonzero failures and
  cleanup. Each build uses the frozen source copy.
- [Selections](../../../../docker/matrix.json) and
  [usage/replay guide](../../../../docs/docker-matrix.md). The
  [65-cell plan](matrix-plan.json) is configuration, not executed coverage.

Execution evidence lives under [environment/](environment/). Each timestamped
invocation has `source.tar.gz`, source hashes, a `summary.json` and per-cell
`result.json`, runtime/command output and build/service/cleanup logs. Source
snapshots include local uncommitted edits on top of repository revision
`12a0cfd`; exact input bytes and Composer references are retained. Optional
PHPUnit runs write JUnit to `/evidence/junit.xml` and the configured version
recorder writes `/evidence/versions.json`.

Initial development attempts are preserved, including a build failure while
trying to rebuild the already compiled SQLite extensions (`sqlite3` has
`config0.m4`), and interrupted builds. The final approach uses the official
image's dynamically linked extensions, installs the chosen shared library,
sets loader precedence, and compares PDO/sqlite3 versions. SQLite builds only
the shared library, with column metadata enabled; the unnecessary second
SQLite compilation for the CLI was removed. These are corrected environment
setup defects. Interrupted cells provide no successful runtime evidence.

## Remaining scope

The supported behavior baseline remains `BASE-20260926-main-249944e`.
PostgreSQL ZTD workflows, MySQL variants beyond the retained scenario runs,
and the full 65-cell Cartesian product still need expectation-level evidence.
Historical legacy suite assertions have not been promoted to desired behavior.
`WRK-supported-matrix` remains ready for that work. Runtime image tags and
Debian packages can move; retained image IDs/digests and source/lock snapshots
identify these executions. Full byte-identical runtime replay needs the built
image, as described in the guide.


## Completed environment executions

| PHP | Actual database | Scope/result | Evidence |
| --- | --- | --- | --- |
| 8.1.34 | sqlite 3.40.1 | native drivers + ZTD fixture: 1 test / 10 assertions | [result](environment/20260927T045351856096Z/php8.1-sqlitesystem/result.json) |
| 8.2.34 | sqlite 3.40.1 | native drivers + ZTD fixture: 1 test / 10 assertions | [result](environment/20260927T045351856096Z/php8.2-sqlitesystem/result.json) |
| 8.3.35 | sqlite 3.40.1 | native drivers + ZTD fixture: 1 test / 10 assertions | [result](environment/20260927T045351856096Z/php8.3-sqlitesystem/result.json) |
| 8.4.26 | sqlite 3.40.1 | native drivers + ZTD fixture: 1 test / 10 assertions | [result](environment/20260927T045351856096Z/php8.4-sqlitesystem/result.json) |
| 8.5.11 | sqlite 3.40.1 | native drivers + ZTD fixture: 1 test / 10 assertions | [result](environment/20260927T045351856096Z/php8.5-sqlitesystem/result.json) |
| 8.5.11 | mysql 8.0.11 | native drivers passed | [result](environment/20260927T045523925003Z/php8.5-mysql8.0.11/result.json) |
| 8.5.11 | mysql 8.4.11 | native drivers passed | [result](environment/20260927T045523925003Z/php8.5-mysql8.4/result.json) |
| 8.5.11 | mysql 9.1.0 | native drivers passed | [result](environment/20260927T045523925003Z/php8.5-mysql9.1/result.json) |
| 8.5.11 | postgres 16.15 (Debian 16.15-1.pgdg13+2) | native drivers passed | [result](environment/20260927T045523925003Z/php8.5-postgres16/result.json) |
| 8.5.11 | postgres 17.11 (Debian 17.11-1.pgdg13+2) | native drivers passed | [result](environment/20260927T045523925003Z/php8.5-postgres17/result.json) |
| 8.1.34 | sqlite 3.46.1 | native drivers + ZTD fixture: 1 test / 10 assertions | [result](environment/20260927T045734458885Z/php8.1-sqlite3.46.1/result.json) |
| 8.5.11 | mysql 8.0.46 | native drivers + ZTD product types: 0 failed checks | [result](environment/20260927T045922612784Z/php8.5-mysql8.0/result.json) |
| 8.5.11 | sqlite 3.40.1 | native drivers passed | [result](environment/20260927T045955650946Z/php8.5-sqlite3.40.1/result.json) |

All 13 completed environment cells passed, and all their cleanup commands
returned zero. Six cells also passed the reviewed ZTD fixture test (PHP
8.1.34/8.2.34/8.3.35/8.4.26/8.5.11 with system SQLite 3.40.1, plus PHP 8.1.34
with custom SQLite 3.46.1). PHP 8.5.11/MySQL 8.0.46 passed the existing product
type scenario, with zero failed checks. These are partial expectation results;
PostgreSQL and the other MySQL versions above have native infrastructure
coverage only. The MySQL 8.0.11 cell used amd64 emulation; the PHP clients and
other servers ran on arm64. Both custom SQLite builds were also verified through
PDO and SQLite3; their source IDs and compile options are retained.

Not executed: native MySQL 8.1/8.2/8.3/9.0 cells, other PHP/database crosses,
and the remaining ZTD workflows. The configured 65-cell plan is not a claim
that all 65 passed.

## Final checks

- [Tooling tests](tooling-tests.txt): 16 passed, including failed-build cleanup,
  failed log collection, preserved command errors, cleanup failures and frozen
  source copies.
- [Composer validation](composer-validate.txt): strict validation passed.
- [Configuration and negative checks](checks.json): both Compose files parsed;
  requesting PHP 9 from an 8.1 image failed with exit 1; selecting unsupported
  PHP 8.0 failed with exit 2. Direct Compose interpolation with `PHP_VERSION=8.1`
  selected the matching image, and MySQL/PostgreSQL clients selected the proper
  database probes.
- [Deliberate command failure](negative-check/20260927T050251795283Z/php8.5-sqlitesystem/result.json):
  native checks passed, `php -r 'exit(7);'` retained exit 7, the runner returned
  1, and cleanup returned 0. This is an expected harness check, not a finding.
- `php -l scripts/matrix-probe.php`, `sh -n docker/install-sqlite.sh`, Python
  compilation and `git diff --check` passed. Archive member hashes and every
  successful probe's installed lock identity were verified.
- `python3 scripts/lab.py validate`: records and historical evidence valid.
  No task-owned containers remain. Built images remain for reuse.
