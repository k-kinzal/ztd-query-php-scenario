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

### 1.4a Static Factory (PDO)
When a user calls `ZtdPdo::connect()` with a valid DSN and credentials, the system shall create a ZTD-enabled wrapper equivalent to `ZtdPdo::fromPdo(PDO::connect(...))`.

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
- **MySQLi**: `bind_param()` with type string and by-reference variables, `execute()` for execution, `execute_query()` (PHP 8.2+) as a shortcut, and `bind_result()` + `fetch()` for bound-variable result retrieval.

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
- **CASE** expressions (in SELECT and UPDATE SET clauses).
- **LIKE** with wildcard patterns.
- **IN** with literal value lists.
- **BETWEEN** range conditions.
- **EXISTS** / **NOT EXISTS** correlated subqueries.
- **COALESCE** and other SQL functions.
- **Window functions** (ROW_NUMBER, SUM OVER PARTITION BY).

UPDATE and DELETE statements with subqueries referencing other shadow tables shall also be correctly rewritten.

User-written CTE (WITH) queries shall work correctly alongside ZTD's internal CTE shadowing on MySQL and SQLite. On PostgreSQL, table references inside user CTEs are not rewritten, causing the inner CTE to read from the physical table rather than the shadow store (see 10.3).

### 3.4 Fetch Methods
When ZTD is enabled, the following fetch methods shall return correct results from the shadow store:
- `fetchAll()` with `FETCH_ASSOC`, `FETCH_NUM`, `FETCH_BOTH` modes.
- `fetch()` for row-by-row iteration (returns `false` when no more rows).
- `fetchColumn()` for retrieving a single column value.
- `fetchObject()` for retrieving rows as `stdClass` objects.
- `columnCount()` shall return the correct number of columns in the result set.
- `getIterator()` / `foreach` iteration over a `ZtdPdoStatement` shall yield rows correctly.

Re-executing a prepared statement (calling `execute()` multiple times with different parameters) shall work correctly with ZTD-enabled queries.

## 4. Write Operations

### 4.1 INSERT
When an INSERT is executed with ZTD enabled, the system shall track the inserted rows in the shadow store without modifying the physical table.

Subsequent SELECT queries shall include the inserted rows.

Multi-row INSERT (e.g., `INSERT INTO t VALUES (1, 'a'), (2, 'b')`) is supported. The affected row count reflects the total number of inserted rows.

INSERT with NULL values is supported. NULL values are correctly stored in the shadow store and queryable via `IS NULL` / `IS NOT NULL`.

### 4.1a INSERT ... SELECT
When an `INSERT ... SELECT` is executed with ZTD enabled, the system shall insert rows from the SELECT result (which reads from the shadow store) into the target table's shadow store.

On MySQL, `INSERT ... SELECT` requires explicit column lists on both sides (e.g., `INSERT INTO t (a, b) SELECT a, b FROM s`). Using `SELECT *` throws `RuntimeException` ("INSERT column count does not match SELECT column count") because the MySQL InsertTransformer counts `*` as 1 column instead of expanding it. On SQLite, `INSERT ... SELECT *` works correctly (see 10.3).

### 4.2 UPDATE
When an UPDATE is executed with ZTD enabled, the system shall track the updated rows in the shadow store without modifying the physical table.

Subsequent SELECT queries shall reflect the updated values.

UPDATE operations require the table schema (including primary keys) to be known. If the schema was not reflected at session creation time, the behavior depends on `unknownSchemaBehavior`:
- `Passthrough`: The UPDATE passes through to the physical database (breaking ZTD isolation).
- `Exception`: A `ZtdMysqliException`/`ZtdPdoException` ("Unknown table") is thrown.

### 4.2a UPSERT
When an `INSERT ... ON DUPLICATE KEY UPDATE` (MySQL) or `INSERT ... ON CONFLICT DO UPDATE` (PostgreSQL) is executed with ZTD enabled, the system shall:
- Insert the row if no duplicate primary key exists in the shadow store.
- Update the matching row if a duplicate primary key exists in the shadow store.

When `INSERT ... ON CONFLICT DO NOTHING` (PostgreSQL) is executed and a duplicate exists, the insert is silently ignored.

### 4.2b REPLACE
When a `REPLACE INTO` statement (MySQL) is executed with ZTD enabled, the system shall delete any existing row with matching primary key and insert the new row in the shadow store.

### 4.2c Multi-Table UPDATE
When a multi-table UPDATE statement is executed with ZTD enabled, the system shall update the target table rows in the shadow store based on the JOIN condition, without modifying the physical database.

Only rows matching the JOIN and WHERE conditions shall be updated; other rows remain unchanged.

Platform-specific syntax:
- **MySQL**: `UPDATE users u JOIN orders o ON u.id = o.user_id SET u.active = 0 WHERE o.amount > 150`
- **PostgreSQL**: `UPDATE users SET active = 0 FROM orders WHERE users.id = orders.user_id AND orders.amount > 150`

### 4.2d Multi-Table DELETE
When a multi-table DELETE statement is executed with ZTD enabled, the system shall delete the specified rows from the shadow store based on the JOIN condition, without modifying the physical database.

Platform-specific syntax:
- **MySQL**: `DELETE o FROM orders o JOIN users u ON o.user_id = u.id WHERE u.name = 'Bob'`
- **PostgreSQL**: `DELETE FROM orders USING users WHERE orders.user_id = users.id AND users.name = 'Bob'`

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

### 4.7 Property Access (mysqli)
When ZTD is enabled, accessing mysqli properties (e.g., `$mysqli->insert_id`, `$mysqli->server_version`, `$mysqli->affected_rows`) via the `__get` magic method throws an `Error` ("Property access is not allowed yet") because the C-extension property handler takes precedence over `__get`. This applies to both `new ZtdMysqli(...)` and `ZtdMysqli::fromMysqli(...)`.

Use the dedicated methods instead: `lastAffectedRows()` for affected row count; delegated methods like `get_server_info()`, `character_set_name()`, `stat()` for connection information.

### 4.8 Transactions
Transaction control methods (`begin_transaction()` / `beginTransaction()`, `commit()`, `rollBack()` / `rollback()`) are delegated directly to the underlying connection. They do not affect the shadow store.

For `ZtdMysqli`, `savepoint()` and `release_savepoint()` are also delegated to the underlying connection.

For `ZtdPdo`, `inTransaction()` reflects the state of the underlying connection.

Shadow data remains visible after `commit()` or `rollBack()` because it is stored independently of the physical transaction state.

### 4.9 Utility Methods
`real_escape_string()` (mysqli) and `quote()` (PDO) are delegated to the underlying connection and work correctly in ZTD mode.

`lastInsertId()` (PDO) is delegated to the underlying connection. Its value may not reflect shadow-simulated inserts.

`escape_string()` (mysqli) is an alias for `real_escape_string()` and is delegated to the underlying connection.

The following methods are delegated directly to the underlying connection without ZTD interception: `multi_query()`, `more_results()`, `next_result()`, `autocommit()`, `set_charset()`, `character_set_name()`, `get_charset()`, `select_db()`, `ping()`, `stat()`, `get_server_info()`, `get_connection_stats()`.

`multi_query()` bypasses ZTD entirely — queries executed via `multi_query()` operate directly on the physical database.

For PDO, the following methods are delegated: `setAttribute()`, `getAttribute()`, `errorCode()`, `errorInfo()`.

For `ZtdPdoStatement`, the following methods are delegated: `closeCursor()`, `setFetchMode()`, `bindColumn()`, `getColumnMeta()`, `errorCode()`, `errorInfo()`.

## 5. DDL Operations

### 5.1 CREATE TABLE
When a CREATE TABLE statement is executed with ZTD enabled and the table already exists physically, the system shall throw a `ZtdMysqliException`/`ZtdPdoException` with "Table already exists" error.

When a CREATE TABLE statement is executed with ZTD enabled and the table does NOT exist physically, the system shall track the table schema in the shadow store. Subsequent INSERT/SELECT/UPDATE/DELETE operations on the shadow-created table shall work correctly.

### 5.1a ALTER TABLE
When an `ALTER TABLE` statement is executed with ZTD enabled, the system shall modify the table definition in the shadow store's schema registry without modifying the physical table.

**MySQL** — Fully supported. ALTER TABLE operations:
- **ADD COLUMN**: Adds a new column to the shadow schema. Subsequent inserts/selects may use the new column.
- **DROP COLUMN**: Removes a column from the shadow schema and from any existing shadow data rows.
- **MODIFY COLUMN**: Changes the column type definition in the shadow schema.
- **CHANGE COLUMN**: Renames and/or changes the type of a column. Existing shadow data rows are updated with the new column name.
- **RENAME COLUMN ... TO**: Renames a column in the shadow schema and existing data.
- **RENAME TO**: Renames the table in the shadow store and schema registry.
- **ADD PRIMARY KEY / DROP PRIMARY KEY**: Modifies the primary key definition in the shadow schema.
- **ADD/DROP FOREIGN KEY**: No-op (foreign keys are metadata-only in ZTD).
- Unsupported ALTER TABLE operations (e.g., ADD INDEX, ADD SPATIAL INDEX, PARTITION) throw `UnsupportedSqlException`.

**SQLite** — Partially supported. The mutation resolver accepts ALTER TABLE (ADD/DROP/RENAME COLUMN, RENAME TO) without throwing exceptions, but the CTE rewriter does NOT reflect schema changes in query results:
- ADD COLUMN: new column is silently dropped from SELECT results (not included in CTE)
- DROP COLUMN: column still appears in SELECT results (CTE uses original schema)
- RENAME COLUMN: old column name is still used in SELECT results
- Original columns continue to work normally after ALTER TABLE

**PostgreSQL** — Not supported. ALTER TABLE throws `ZtdPdoException` ("ALTER TABLE not yet supported for PostgreSQL").

### 5.1b CREATE TABLE LIKE
When a `CREATE TABLE ... LIKE` statement is executed with ZTD enabled, the system shall create a shadow table with the same schema as the source table. Subsequent CRUD operations on the new table shall work correctly.

Supported on MySQL, PostgreSQL, and SQLite. PostgreSQL uses `CREATE TABLE t (LIKE source)` syntax.

### 5.1c CREATE TABLE AS SELECT
When a `CREATE TABLE ... AS SELECT` statement is executed with ZTD enabled, the system shall create a shadow table with the columns from the SELECT result and populate it with the selected data.

**MySQL and PostgreSQL**: Fully supported. Subsequent SELECT on the created table returns the data.

**SQLite**: Partially supported. The exec succeeds and the shadow table is created, but:
- SELECT immediately after CTAS fails with "no such table" (CTE rewriter cannot build CTE without physical table)
- INSERT into the created table works
- After INSERT, SELECT works but returns only INSERTed rows — the original CTAS data is lost

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

**Platform note:** On PostgreSQL and SQLite, EmptyResult mode works correctly for DELETE operations (physical table unchanged). However, UPDATE operations throw `RuntimeException` ("UPDATE simulation requires primary keys") regardless of EmptyResult mode — the error occurs before the unknown schema behavior check.

### 7.4 Notice
If `unknownSchemaBehavior` is `Notice` and a write operation (UPDATE, DELETE) references an unreflected table, the system shall emit a user notice/warning and return an empty result without modifying the physical database.

Verified on MySQL (both adapters): a `E_USER_NOTICE` or `E_USER_WARNING` is triggered.

**Platform note:** On PostgreSQL and SQLite, Notice mode works correctly for DELETE operations (notice emitted, physical table unchanged). However, UPDATE operations throw `RuntimeException` ("UPDATE simulation requires primary keys") regardless of Notice mode — the error occurs before the unknown schema behavior check.

## 8. Constraint Enforcement

### 8.1 Shadow Store Constraints
The shadow store does NOT enforce database constraints. The following constraints are not checked during ZTD-simulated write operations:

- **PRIMARY KEY**: Duplicate primary key values are accepted in the shadow store.
- **UNIQUE**: Duplicate values in unique columns are accepted.
- **NOT NULL**: NULL values are accepted even for NOT NULL columns.
- **FOREIGN KEY**: References to non-existent parent rows are accepted.

This is by design - the shadow store is an in-memory simulation layer, not a full database engine. Constraint enforcement is deferred to the physical database when changes are eventually applied.

## 8.5 Error Recovery

### 8.5a Transformer Errors
When malformed SQL is executed with ZTD enabled, the ztd-query transformer may throw `RuntimeException` before the query reaches the database engine. This differs from standard PDO/mysqli error propagation where `PDOException` or `mysqli_sql_exception` would be thrown.

### 8.5b Shadow Store Consistency After Errors
When a SQL error occurs (either from the transformer or the database engine), the shadow store shall remain consistent. Previously inserted/updated/deleted shadow data is not rolled back or corrupted by a subsequent error.

Subsequent valid operations after an error shall execute correctly against the intact shadow store.

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
- Prepared statement parameter binding (bindValue with PARAM_INT/PARAM_STR/PARAM_NULL types, bindParam by-reference with re-execute, execute with positional params array, execute with named params, re-execute with different params, execute_query with NULL).
- Fetch methods (fetch, fetchAll with FETCH_ASSOC/FETCH_NUM/FETCH_BOTH/FETCH_OBJ, fetchColumn, fetchColumn with index, columnCount, rowCount after UPDATE, foreach iteration, setFetchMode, query() with fetch mode argument, fetch returns false when exhausted).
- Schema reflection (adapter constructed after table reflects schema; adapter constructed before table fails UPDATE/DELETE with "requires primary keys").
- Auto-detection of PDO driver (mysql, pgsql, sqlite all verified).
- Complex queries: JOINs (INNER, LEFT), self-JOINs, aggregations (COUNT, SUM, MIN, MAX), GROUP BY/HAVING, subqueries, correlated subqueries, UNION, ORDER BY/LIMIT/OFFSET, DISTINCT.
- UPDATE/DELETE with subqueries referencing other shadow tables.
- Unsupported SQL handling (Exception, Ignore, Notice modes; behavior rules with prefix and regex patterns; transaction statement passthrough).
- Transaction control (beginTransaction/commit, beginTransaction/rollback, quote).
- DDL shadow-created table operations (INSERT/UPDATE/DELETE on shadow-created tables).
- Statement methods (closeCursor, setFetchMode, bindColumn, columnCount, getIterator/foreach).
- UPSERT operations (INSERT ... ON DUPLICATE KEY UPDATE on MySQL; INSERT ... ON CONFLICT on PostgreSQL).
- Multi-table UPDATE/DELETE operations (UPDATE ... JOIN, DELETE ... JOIN on MySQL; UPDATE ... FROM, DELETE ... USING on PostgreSQL).
- User-written CTE (WITH) queries work alongside ZTD CTE shadowing (MySQL and SQLite; see 10.3 for PostgreSQL limitation).
- INSERT ... SELECT with explicit column lists.
- Multi-row INSERT and NULL value handling.
- Configuration (ZtdConfig, unsupported behavior, behavior rules, ZTD toggle cycles).
- Advanced query patterns: CASE expressions, LIKE, IN, BETWEEN, EXISTS/NOT EXISTS, COALESCE, window functions (ROW_NUMBER, SUM OVER).
- Error recovery: shadow store remains consistent after transformer errors, SQL errors, and constraint violations; subsequent operations succeed.
- CREATE TABLE LIKE (all platforms).
- CREATE TABLE AS SELECT (MySQL and PostgreSQL; see 10.3 for SQLite limitation).
- Query rewriting at prepare time (toggling ZTD between prepare/execute retains rewritten query).
- DDL edge cases: CREATE TABLE IF NOT EXISTS, DROP TABLE IF EXISTS, TRUNCATE then INSERT cycles.
- Data type handling: DATE, DATETIME/TIMESTAMP, DECIMAL/NUMERIC, INTEGER, and NULL values preserved correctly in shadow store.
- Special character handling: quotes, newlines, Unicode, emoji, empty strings preserved correctly in shadow store (see 10.3 for MySQL backslash exception).
- ZTD lifecycle: shadow data persists across enable/disable toggle cycles; physical data inserted while ZTD is off is not visible when ZTD is re-enabled.
- Transaction isolation: shadow data persists after rollback (shadow store is independent of physical transaction state).
- Query edge cases: COUNT(*) vs COUNT(col) with NULLs, SUM ignoring NULLs, ORDER BY with NULLs, LIMIT 0, LIMIT with OFFSET, self-referencing UPDATE (score = score + 10), string concatenation in UPDATE, DISTINCT with NULLs, GROUP BY HAVING, MIN/MAX on strings, multiple sequential UPDATEs to same row, insert-delete-insert same ID cycle.
- Stress testing: 50 sequential INSERTs, bulk UPDATE, bulk DELETE with correct counts; verified on all platforms.

### 10.2 Platform-Specific Notes
- **TRUNCATE**: Verified on MySQL and PostgreSQL. SQLite does not have native TRUNCATE TABLE syntax and attempting `TRUNCATE TABLE` throws an exception; `DELETE FROM table` (DML) is the equivalent but follows regular DELETE processing through ZTD.
- **multi_query() bypass**: Verified on MySQL (MySQLi). `multi_query()` bypasses ZTD entirely even when ZTD is enabled — writes go directly to the physical database and reads bypass the shadow store.
- **FOREIGN KEY constraints**: The foreign key constraint scenario uses a parent-child table relationship on MySQL and PostgreSQL. SQLite does not include the foreign key test because SQLite requires `PRAGMA foreign_keys = ON` to enforce them, which is outside ZTD scope.
- **Unsupported SQL**: Platform-specific unsupported SQL examples: MySQL uses `SET @var = 1`, PostgreSQL uses `SET search_path TO public`, SQLite uses `PRAGMA journal_mode=WAL`. All three platforms support behavior rules with prefix and regex patterns.
- **ALTER TABLE**: Fully supported on MySQL (via `AlterTableMutation`). SQLite accepts ALTER TABLE without error but CTE rewriter ignores schema changes (see 5.1a). PostgreSQL throws `ZtdPdoException` for ALTER TABLE.
- **CREATE TABLE LIKE**: Verified on MySQL, PostgreSQL, and SQLite. PostgreSQL uses `CREATE TABLE t (LIKE source)` syntax.
- **CREATE TABLE AS SELECT**: Fully supported on MySQL and PostgreSQL. SQLite has limitations (see 5.1c).
- **Unknown Schema**: Behavior rules and unknown schema handling are verified on all platforms. See 10.3 for cross-platform inconsistencies.

### 10.3 Cross-Platform Inconsistencies
The following behaviors differ across platforms and may indicate areas for improvement:

- **Unknown schema UPDATE (Passthrough mode)**: On MySQL (MySQLi adapter and PDO adapter via `new ZtdPdo()` constructor), UPDATE on unreflected tables passes through to the physical database as documented. On MySQL via `ZtdPdo::fromPdo()`, PostgreSQL, and SQLite, the same operation throws `RuntimeException` ("UPDATE simulation requires primary keys") instead of passing through. This means the `unknownSchemaBehavior: Passthrough` setting does not take effect for UPDATE operations when using `fromPdo()` or on PostgreSQL/SQLite.

- **Unknown schema UPDATE (Exception mode)**: On MySQL, UPDATE on unreflected tables throws `ZtdPdoException` ("Unknown table"). On PostgreSQL and SQLite, it throws `RuntimeException` ("UPDATE simulation requires primary keys") — the exception type and message differ from MySQL.

- **Unknown schema DELETE**: On MySQL and SQLite, DELETE on unreflected tables in Passthrough mode passes through to the physical database. On PostgreSQL, DELETE in Exception mode throws `RuntimeException` ("Unknown table") rather than `ZtdPdoException`. On SQLite, DELETE in Exception mode also throws `RuntimeException` ("Unknown table") rather than `ZtdPdoException`.

- **INSERT ... ON CONFLICT DO NOTHING (SQLite)**: On PostgreSQL, `ON CONFLICT DO NOTHING` correctly ignores duplicate inserts in the shadow store. On SQLite, the same syntax inserts both rows into the shadow store (the DO NOTHING clause is not processed, and the shadow store does not enforce PK constraints). `ON CONFLICT DO UPDATE` works correctly on both platforms.

- **INSERT ... SELECT * (MySQL only)**: On MySQL, `INSERT INTO t SELECT * FROM s` throws `RuntimeException` ("INSERT column count does not match SELECT column count") because the MySQL InsertTransformer counts `SELECT *` as 1 column instead of expanding it. The workaround is to use explicit column lists: `INSERT INTO t (a, b) SELECT a, b FROM s`. On SQLite and PostgreSQL, `INSERT ... SELECT *` works correctly.

- **User-written CTEs (PostgreSQL)**: On MySQL and SQLite, user-written CTE queries (e.g., `WITH cte AS (SELECT * FROM t) SELECT * FROM cte`) work correctly — table references inside the user's CTE are rewritten to read from the shadow store. On PostgreSQL, table references inside user CTEs are NOT rewritten, so the inner CTE reads from the physical table (empty) instead of the shadow store, returning 0 rows.

- **Multi-table UPDATE/DELETE syntax**: MySQL uses `UPDATE t1 JOIN t2 ON ... SET ...` and `DELETE t1 FROM t1 JOIN t2 ON ...`. PostgreSQL uses `UPDATE t1 SET ... FROM t2 WHERE ...` and `DELETE FROM t1 USING t2 WHERE ...`. Both syntaxes are supported by their respective platform adapters.

- **ALTER TABLE schema propagation (SQLite)**: On MySQL, ALTER TABLE fully updates the shadow schema and CTE rewriting reflects changes. On SQLite, ALTER TABLE is accepted without error but the CTE rewriter continues to use the original reflected schema — added columns are silently dropped from query results, dropped columns still appear, and renamed columns keep their old name.

- **CREATE TABLE AS SELECT (SQLite)**: On MySQL and PostgreSQL, CTAS correctly creates and populates the shadow table for subsequent queries. On SQLite, CTAS creates the shadow table but SELECT immediately fails with "no such table". After a subsequent INSERT, SELECT works but only returns the INSERTed rows — the original CTAS data is lost.

- **Backslash corruption in MySQL shadow store**: On MySQL (both MySQLi and PDO adapters), backslash characters in string values inserted via prepared statements are corrupted in the shadow store. The CTE rewriter embeds values as string literals without escaping backslashes, causing MySQL to interpret escape sequences: `\t` → tab, `\n` → newline, `\b` → backspace, `\r` → carriage return, `\0` → null byte, `\\` → single backslash. Unrecognized sequences like `\f` drop the backslash. This does NOT affect SQLite or PostgreSQL, which handle backslashes correctly. **This is a potential issue for users storing file paths, regular expressions, or other backslash-containing data.**

- **PostgreSQL BOOLEAN false casting**: On PostgreSQL, inserting PHP `false` into a BOOLEAN column via prepared statement succeeds, but subsequent SELECT fails because the CTE rewriter generates `CAST('' AS BOOLEAN)`, which is invalid PostgreSQL syntax. `true` works correctly. MySQL is unaffected (uses TINYINT). SQLite is unaffected (typeless).

- **PostgreSQL BIGINT overflow**: On PostgreSQL, inserting large integers (> 2,147,483,647) into BIGINT columns via prepared statement succeeds, but subsequent SELECT fails because the CTE rewriter generates `CAST(value AS integer)` instead of `CAST(value AS bigint)`, causing numeric overflow. MySQL and SQLite handle BIGINT values correctly.

- **DELETE without WHERE clause (SQLite)** (Issue #7): On MySQL and PostgreSQL, `DELETE FROM table` (without WHERE clause) correctly clears the shadow store. On SQLite, `DELETE FROM table` without a WHERE clause is silently ignored — the shadow store retains all rows. The workaround is to use `DELETE FROM table WHERE 1=1` which works correctly on all platforms.

- **NULL sort order in ORDER BY**: MySQL and SQLite sort NULLs first in ASC order. PostgreSQL sorts NULLs last in ASC order. This is standard SQL behavior (not a ZTD issue), but tests should account for the difference.

- **Unknown schema EmptyResult/Notice modes (PostgreSQL/SQLite)**: On MySQL, all four `unknownSchemaBehavior` modes (Passthrough, Exception, EmptyResult, Notice) work correctly for both UPDATE and DELETE on unreflected tables. On PostgreSQL and SQLite, only DELETE operations respect EmptyResult and Notice modes. UPDATE operations throw `RuntimeException` ("UPDATE simulation requires primary keys") regardless of the configured behavior, because the error occurs in the ShadowStore before the unknown schema behavior check is applied.
