# ZTD Query Adapter Specifications (EARS Notation)

Version: ztd-query-mysqli-adapter v0.1.1, ztd-query-pdo-adapter v0.1.1

## 1. Connection

### 1.1 New Connection (mysqli)
When a user creates a new `ZtdMysqli` instance with valid connection parameters, the system shall establish a connection and enable ZTD mode by default.

### 1.2 New Connection (PDO)
When a user creates a new `ZtdPdo` instance with a valid DSN and credentials, the system shall establish a connection and enable ZTD mode by default.

### 1.3 Wrap Existing Connection (mysqli)
When a user calls `ZtdMysqli::fromMysqli()` with an existing `mysqli` instance, the system shall create a ZTD-enabled wrapper that delegates to the inner connection.

### 1.4 Wrap Existing Connection (PDO)
When a user calls `ZtdPdo::fromPdo()` with an existing `PDO` instance, the system shall create a ZTD-enabled wrapper that delegates to the inner connection.

### 1.5 Auto-detection (PDO)
When a user creates a `ZtdPdo` without specifying a `SessionFactory`, the system shall detect the appropriate factory from the PDO driver name (mysql, pgsql, sqlite).

If the PDO driver is not supported, the system shall throw a `RuntimeException`.

If the required platform package is not installed, the system shall throw a `RuntimeException` with installation instructions.

### 1.6 Schema Reflection
When a ZTD adapter is constructed, the system shall reflect all existing table schemas from the physical database via `SchemaReflector::reflectAll()`.

The ZTD adapter MUST be constructed AFTER the physical tables exist; otherwise, schema-dependent operations (UPDATE, DELETE) will fail with `RuntimeException` ("UPDATE simulation requires primary keys" / "DELETE simulation requires primary keys").

INSERT and SELECT operations on unreflected tables may still work, as they do not require primary key information.

### 1.7 Supported Platforms
The PDO adapter supports the following platforms via auto-detection:
- MySQL (driver: `mysql`, package: `k-kinzal/ztd-query-mysql`)
- PostgreSQL (driver: `pgsql`, package: `k-kinzal/ztd-query-postgres`)
- SQLite (driver: `sqlite`, package: `k-kinzal/ztd-query-sqlite`)

## 2. ZTD Mode

### 2.1 Enable/Disable
When ZTD mode is enabled, the system shall rewrite SQL queries using CTE (Common Table Expression) shadowing.

When ZTD mode is disabled, the system shall pass queries directly to the underlying connection without rewriting.

### 2.2 Isolation
While ZTD mode is enabled, all write operations (INSERT, UPDATE, DELETE) shall be tracked in an in-memory shadow store and shall NOT modify the physical database.

While ZTD mode is enabled, SELECT queries shall read from the shadow store via CTE rewriting. The shadow store replaces the physical table entirely; data present only in the physical table is NOT visible through ZTD-enabled SELECT queries. When ZTD mode is disabled, SELECT queries read directly from the physical table.

### 2.3 Toggle
The system shall provide `enableZtd()`, `disableZtd()`, and `isZtdEnabled()` methods to control and inspect ZTD mode.

### 2.4 Session State
Each ZTD adapter instance maintains its own session state. Shadow data is not shared between instances and is not persisted across instance lifecycle.

## 3. Read Operations

### 3.1 SELECT
When ZTD is enabled and a SELECT query is executed, the system shall return results from the shadow store only. Physical table data is not included; the CTE replaces the table reference with shadow store contents.

When ZTD is enabled and the result set is empty, the system shall return an empty result set (not false or null).

### 3.2 Prepared SELECT
When a prepared SELECT statement with bound parameters is executed, the system shall rewrite the query and return correct results.

Prepared statements support the following binding methods:
- **PDO**: `bindValue()` for value binding, `bindParam()` for by-reference binding, and `execute($params)` with positional or named parameter arrays.
- **MySQLi**: `bind_param()` with type string and by-reference variables, `execute()` for execution, and `execute_query()` (PHP 8.2+) as a shortcut.

Query rewriting occurs at **prepare time**, not execute time. If ZTD mode is toggled between `prepare()` and `execute()`, the prepared query retains its original rewritten form.

### 3.3 Complex Queries
When ZTD is enabled, the CTE rewriting shall correctly handle:
- **JOINs** (INNER JOIN, LEFT JOIN) across multiple shadow tables.
- **Self-JOINs** where the same shadow table is referenced with different aliases.
- **Aggregations** (COUNT, SUM, MIN, MAX) with GROUP BY and HAVING clauses.
- **Subqueries** in WHERE clauses (e.g., `WHERE id IN (SELECT ...)`).
- **Correlated subqueries** in SELECT list (e.g., `(SELECT COUNT(*) FROM t WHERE t.fk = u.id)`).
- **UNION** queries combining results from shadow tables.
- **ORDER BY** with LIMIT and OFFSET.
- **DISTINCT** selection.

UPDATE and DELETE statements with subqueries referencing other shadow tables shall also be correctly rewritten.

### 3.4 Fetch Methods
When ZTD is enabled, the following fetch methods shall return correct results from the shadow store:
- `fetchAll()` with `FETCH_ASSOC`, `FETCH_NUM`, `FETCH_BOTH` modes.
- `fetch()` for row-by-row iteration (returns `false` when no more rows).
- `fetchColumn()` for retrieving a single column value.
- `fetchObject()` for retrieving rows as `stdClass` objects.
- `columnCount()` shall return the correct number of columns in the result set.

Re-executing a prepared statement (calling `execute()` multiple times with different parameters) shall work correctly with ZTD-enabled queries.

## 4. Write Operations

### 4.1 INSERT
When an INSERT is executed with ZTD enabled, the system shall track the inserted rows in the shadow store without modifying the physical table.

Subsequent SELECT queries shall include the inserted rows.

### 4.2 UPDATE
When an UPDATE is executed with ZTD enabled, the system shall track the updated rows in the shadow store without modifying the physical table.

Subsequent SELECT queries shall reflect the updated values.

UPDATE operations require the table schema (including primary keys) to be known. If the schema was not reflected at session creation time, the behavior depends on `unknownSchemaBehavior`:
- `Passthrough`: The UPDATE passes through to the physical database (breaking ZTD isolation).
- `Exception`: A `ZtdMysqliException`/`ZtdPdoException` ("Unknown table") is thrown.

### 4.3 DELETE
When a DELETE is executed with ZTD enabled, the system shall track the deletion in the shadow store without modifying the physical table.

Subsequent SELECT queries shall not include the deleted rows.

DELETE operations on unreflected tables follow the same `unknownSchemaBehavior` rules as UPDATE (see 4.2).

### 4.4 Affected Row Count
After a write operation via `ZtdMysqli::query()`, `lastAffectedRows()` shall return the number of rows affected by the ZTD-simulated operation.

After a write operation via `ZtdPdo::exec()`, the return value shall be the number of affected rows.

After a write operation via `ZtdPdoStatement::execute()`, `rowCount()` shall return the number of affected rows.

After a write operation via `ZtdMysqliStatement::execute()`, `ztdAffectedRows()` shall return the number of affected rows. Note: The `$stmt->affected_rows` property may not be available ("Property access is not allowed yet"); `ztdAffectedRows()` is the reliable accessor.

### 4.5 Write Result Sets
When a write operation (INSERT/UPDATE/DELETE) is executed with ZTD enabled, the affected row data is consumed internally for shadow processing.

For `ZtdMysqli::query()`, the returned `mysqli_result` object from a write operation has an exhausted cursor; calling `fetch_assoc()` returns `null`.

For `ZtdPdoStatement`, calling `fetchAll()` after a write operation returns an empty array because `hasResultSet()` is `false` for `WRITE_SIMULATED` queries.

### 4.6 real_query (mysqli)
When `real_query()` is called with ZTD enabled, write operations (INSERT, UPDATE, DELETE) shall be tracked in the shadow store, and `real_query()` shall return `true`.

When `real_query()` is called with ZTD enabled for a SELECT query, `real_query()` shall return `true`, but `store_result()` shall return `false`. The CTE-rewritten query result is consumed internally and not available via `store_result()`. Use `query()` instead of `real_query()` + `store_result()` for SELECT queries in ZTD mode.

### 4.7 insert_id (mysqli)
When ZTD is enabled, accessing the `$mysqli->insert_id` property after an INSERT throws an `Error` ("Property access is not allowed yet") because the INSERT was simulated in the shadow store and never executed on the physical database.

### 4.8 Transactions
Transaction control methods (`begin_transaction()` / `beginTransaction()`, `commit()`, `rollBack()` / `rollback()`) are delegated directly to the underlying connection. They do not affect the shadow store.

For `ZtdMysqli`, `savepoint()` and `release_savepoint()` are also delegated to the underlying connection.

For `ZtdPdo`, `inTransaction()` reflects the state of the underlying connection.

Shadow data remains visible after `commit()` or `rollBack()` because it is stored independently of the physical transaction state.

### 4.9 Utility Methods
`real_escape_string()` (mysqli) and `quote()` (PDO) are delegated to the underlying connection and work correctly in ZTD mode.

`lastInsertId()` (PDO) is delegated to the underlying connection. Its value may not reflect shadow-simulated inserts.

## 5. DDL Operations

### 5.1 CREATE TABLE
When a CREATE TABLE statement is executed with ZTD enabled and the table already exists physically, the system shall throw a `ZtdMysqliException`/`ZtdPdoException` with "Table already exists" error.

When a CREATE TABLE statement is executed with ZTD enabled and the table does NOT exist physically, the system shall track the table schema in the shadow store. Subsequent INSERT/SELECT operations on the shadow-created table shall work.

### 5.2 DROP TABLE
When a DROP TABLE statement is executed with ZTD enabled, the system shall clear the shadow data for the table.

After DROP TABLE, subsequent queries on that table shall fall through to the physical database (the physical table is not dropped).

### 5.3 TRUNCATE
When a TRUNCATE statement is executed with ZTD enabled, the system shall clear all shadowed data for the table.

## 6. Unsupported SQL

### 6.1 Default Behavior
If unsupported SQL is executed and `unsupportedBehavior` is `Exception` (the default), the system shall throw a `ZtdMysqliException` or `ZtdPdoException`.

If unsupported SQL is executed and `unsupportedBehavior` is `Ignore`, the system shall silently skip the statement and return `false` (mysqli) or `0` (PDO exec).

If unsupported SQL is executed and `unsupportedBehavior` is `Notice`, the system shall emit a user notice/warning and skip the statement.

### 6.2 Behavior Rules
When `behaviorRules` are configured in `ZtdConfig`, the system shall apply the first matching rule's behavior for unsupported SQL, overriding the default.

Rules support two pattern types:
- Prefix match (case-insensitive): e.g., `'SET'` matches any SQL starting with "SET".
- Regex match: e.g., `'/^SET\s+/i'` matches SQL matching the regex. Patterns starting with `/` are treated as regex.

### 6.3 Transaction Statements
Transaction control statements (BEGIN, COMMIT, ROLLBACK, SAVEPOINT) are not rewritten and shall be delegated directly to the underlying connection.

For `ZtdMysqli`, transaction control should use the dedicated methods (`begin_transaction()`, `commit()`, `rollback()`) rather than SQL strings via `query()`.

## 7. Unknown Schema

### 7.1 Passthrough (default)
If `unknownSchemaBehavior` is `Passthrough` (the default) and a write operation (UPDATE, DELETE) references an unreflected table, the system shall pass the operation directly to the underlying connection (breaking ZTD isolation for that operation).

**Platform note:** This passthrough behavior is verified on MySQL (both adapters). On PostgreSQL and SQLite, UPDATE on unreflected tables throws `RuntimeException` ("UPDATE simulation requires primary keys") instead of passing through (see 10.3). DELETE on unreflected tables passes through on MySQL and SQLite, but throws `RuntimeException` on PostgreSQL.

SELECT and INSERT operations on unreflected tables pass through to the physical database or shadow store respectively, regardless of this setting.

### 7.2 Exception
If `unknownSchemaBehavior` is `Exception` and a write operation (UPDATE, DELETE) references an unreflected table, the system shall throw a `ZtdMysqliException`/`ZtdPdoException` ("Unknown table").

**Platform note:** On MySQL, the exception type is `ZtdMysqliException`/`ZtdPdoException` with "Unknown table" message. On PostgreSQL and SQLite, UPDATE operations throw `RuntimeException` ("UPDATE simulation requires primary keys") regardless of the `unknownSchemaBehavior` setting. DELETE operations on PostgreSQL/SQLite throw `RuntimeException` ("Unknown table") rather than `ZtdPdoException`.

### 7.3 EmptyResult
If `unknownSchemaBehavior` is `EmptyResult` and a write operation (UPDATE, DELETE) references an unreflected table, the system shall return an empty result without modifying the physical database and without throwing an exception.

Verified on MySQL (PDO adapter): the physical table remains unchanged after UPDATE/DELETE operations.

### 7.4 Notice
If `unknownSchemaBehavior` is `Notice` and a write operation (UPDATE, DELETE) references an unreflected table, the system shall emit a user notice/warning and return an empty result without modifying the physical database.

Verified on MySQL (both adapters): a `E_USER_NOTICE` or `E_USER_WARNING` is triggered.

## 8. Constraint Enforcement

### 8.1 Shadow Store Constraints
The shadow store does NOT enforce database constraints. The following constraints are not checked during ZTD-simulated write operations:

- **PRIMARY KEY**: Duplicate primary key values are accepted in the shadow store.
- **UNIQUE**: Duplicate values in unique columns are accepted.
- **NOT NULL**: NULL values are accepted even for NOT NULL columns.
- **FOREIGN KEY**: References to non-existent parent rows are accepted.

This is by design - the shadow store is an in-memory simulation layer, not a full database engine. Constraint enforcement is deferred to the physical database when changes are eventually applied.

## 9. Configuration

### 9.1 ZtdConfig
The `ZtdConfig` class accepts three parameters:
- `unsupportedBehavior` (`UnsupportedSqlBehavior`): Default `Exception`. Controls handling of unsupported SQL.
- `unknownSchemaBehavior` (`UnknownSchemaBehavior`): Default `Passthrough`. Controls handling of queries on unreflected tables.
- `behaviorRules` (`array<string, UnsupportedSqlBehavior>`): Pattern-to-behavior mapping for fine-grained unsupported SQL control.

### 9.2 Default Configuration
`ZtdConfig::default()` creates a config with `Exception` unsupported behavior and `Passthrough` unknown schema behavior.

## 10. Platform Behavior

### 10.1 Cross-Platform Consistency
The following behaviors are verified as consistent across MySQL, PostgreSQL, and SQLite:
- Basic CRUD operations (INSERT, UPDATE, DELETE, SELECT).
- Shadow store isolation (ZTD-enabled writes do not modify the physical database).
- Session isolation (shadow data is not shared between instances or persisted across lifecycle).
- DDL operations (CREATE TABLE on existing table throws; CREATE TABLE on non-existent table creates in shadow; DROP TABLE clears shadow data).
- Write result sets (exec() returns affected count; fetchAll() after write returns empty array).
- Constraint non-enforcement (PRIMARY KEY, NOT NULL, UNIQUE, FOREIGN KEY not enforced in shadow store).
- Prepared statement parameter binding (bindValue, bindParam, execute with params array, re-execute, execute_query).
- Fetch methods (fetch, fetchAll with FETCH_ASSOC/FETCH_NUM/FETCH_BOTH, fetchColumn, fetchObject, columnCount).
- Schema reflection (adapter constructed after table reflects schema; adapter constructed before table fails UPDATE/DELETE with "requires primary keys").
- Auto-detection of PDO driver (mysql, pgsql, sqlite all verified).
- Complex queries: JOINs (INNER, LEFT), self-JOINs, aggregations (COUNT, SUM, MIN, MAX), GROUP BY/HAVING, subqueries, correlated subqueries, UNION, ORDER BY/LIMIT/OFFSET, DISTINCT.
- UPDATE/DELETE with subqueries referencing other shadow tables.
- Unsupported SQL handling (Exception, Ignore, Notice modes; behavior rules with prefix and regex patterns; transaction statement passthrough).

### 10.2 Platform-Specific Notes
- **TRUNCATE**: Verified on MySQL and PostgreSQL. SQLite does not have native TRUNCATE TABLE syntax; `DELETE FROM table` (DML) is the equivalent but follows regular DELETE processing through ZTD.
- **FOREIGN KEY constraints**: The foreign key constraint scenario uses a parent-child table relationship on MySQL and PostgreSQL. SQLite does not include the foreign key test because SQLite requires `PRAGMA foreign_keys = ON` to enforce them, which is outside ZTD scope.
- **Unsupported SQL**: Platform-specific unsupported SQL examples: MySQL uses `SET @var = 1`, PostgreSQL uses `SET search_path TO public`, SQLite uses `PRAGMA journal_mode=WAL`. All three platforms support behavior rules with prefix and regex patterns.
- **Unknown Schema**: Behavior rules and unknown schema handling are verified on all platforms. See 10.3 for cross-platform inconsistencies.

### 10.3 Cross-Platform Inconsistencies
The following behaviors differ across platforms and may indicate areas for improvement:

- **Unknown schema UPDATE (Passthrough mode)**: On MySQL (both adapters), UPDATE on unreflected tables passes through to the physical database as documented. On PostgreSQL and SQLite (PDO adapter), the same operation throws `RuntimeException` ("UPDATE simulation requires primary keys") instead of passing through. This means the `unknownSchemaBehavior: Passthrough` setting does not take effect for UPDATE operations on PostgreSQL and SQLite.

- **Unknown schema UPDATE (Exception mode)**: On MySQL, UPDATE on unreflected tables throws `ZtdPdoException` ("Unknown table"). On PostgreSQL and SQLite, it throws `RuntimeException` ("UPDATE simulation requires primary keys") — the exception type and message differ from MySQL.

- **Unknown schema DELETE**: On MySQL and SQLite, DELETE on unreflected tables in Passthrough mode passes through to the physical database. On PostgreSQL, DELETE in Exception mode throws `RuntimeException` ("Unknown table") rather than `ZtdPdoException`. On SQLite, DELETE in Exception mode also throws `RuntimeException` ("Unknown table") rather than `ZtdPdoException`.
