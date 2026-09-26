# SPEC-session-created-schema

## User task

A test creates the tables it needs through the ZTD connection instead of running migrations, fills them, and runs the application's queries. The database has no such table before or after the test. Other tests must be able to run the same code in parallel against the same empty database.

## Expectation

With a MySQL database that does not contain the table, using only the connection returned by the public adapter constructors (`new ZtdPdo(...)`, `new ZtdMysqli(...)`):

1. `CREATE TABLE <users> (id INT PRIMARY KEY, name VARCHAR(255) NOT NULL, active BOOLEAN NOT NULL)` succeeds in the session.
2. A single `INSERT ... VALUES (1, 'Alice', TRUE), (2, 'Bob', FALSE)` reports two affected rows: `PDO::exec()` returns `2`; `ZtdMysqli::query()` returns `true` and `lastAffectedRows()` returns `2`.
3. The documented example query `SELECT name FROM <users> WHERE active = ? ORDER BY id` with parameter `1` returns exactly `['Alice']` (PDO `FETCH_COLUMN`) and `[['name' => 'Alice']]` (`mysqli_result::fetch_all(MYSQLI_ASSOC)`). Under MySQLi the README uses `execute_query()`; the same result is expected from that call on every supported PHP version, and from `prepare()`/`bind_param()`/`get_result()`.
4. A prepared lookup by `id = 2` returns `['id' => 2, 'name' => 'Bob', 'active' => 0]` with `id` and `active` as PHP `int` and `name` as `string`.
5. `UPDATE <users> SET name = 'Robert' WHERE id = 2` affects one row and the next read returns `Robert`. `DELETE FROM <users> WHERE id = 1` affects one row and a count query returns `1`.
6. No physical table exists at any point: a separate native connection finds nothing in `information_schema.TABLES` for the table name, before and after the session writes.
7. A second, fresh ZTD connection to the same database does not see the table: selecting from it fails with the database's unknown-table error (MySQL 1146 / SQLSTATE 42S02), because the table exists only in the first session and unknown tables pass through to the physical database by default.
8. `disableZtd()` on the first connection exposes the physical database, so the same select fails with the unknown-table error; `enableZtd()` restores the session and the count query returns `1` again.

## Basis

- The adapter READMEs ([PDO](https://github.com/k-kinzal/ztd-query-php/blob/aac51e9d99121f79c98720fec3f8ddc03bf7c59b/packages/ztd-query-pdo-adapter/README.md), [MySQLi](https://github.com/k-kinzal/ztd-query-php/blob/aac51e9d99121f79c98720fec3f8ddc03bf7c59b/packages/ztd-query-mysqli-adapter/README.md)) state that tables are created and filled through the same connection and exist only in the session, that tests need no migrations, seeding or cleanup, and that they can run in parallel against one empty database. Items 1–3 are the documented example verbatim except for the table name.
- `lastAffectedRows()`, `disableZtd()`/`enableZtd()` and the default `unknownSchemaBehavior: Passthrough` are documented in the same READMEs.
- Affected-row counts, fetched PHP types and the unknown-table error are native PDO/MySQLi behavior on MySQL 8 with mysqlnd; PHP 8.1+ returns native integer types for both emulated and native prepares.
- Both adapters require PHP 8.1+, so the documented calls are expected to work on PHP 8.1.

## Scope

MySQL through PDO and MySQLi with default ZTD configuration and exception error reporting. PostgreSQL and SQLite, non-default `unknownSchemaBehavior`, ALTER/DROP, transactions, and concurrent sessions in separate processes are not covered by this definition. Runtime coverage is recorded by each execution.

## Links

- [Scenario](../../scenarios/schema/SCN-mysql-session-schema/scenario.json)
- Work item: `WRK-session-created-schema`
