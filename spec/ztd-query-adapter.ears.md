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

While ZTD mode is enabled, SELECT queries on **reflected** tables shall read from the shadow store via CTE rewriting. The shadow store replaces the physical table entirely; data present only in the physical table is NOT visible through ZTD-enabled SELECT queries. When ZTD mode is disabled, SELECT queries read directly from the physical table.

**Note:** SELECT queries on unreflected tables, views, and derived table subqueries may pass through to the physical database (see 3.3a, 3.3b).

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
- **MySQLi**: `bind_param()` with type string and by-reference variables, `execute()` for execution, `execute_query()` (PHP 8.2+) as a shortcut for SELECT/INSERT/UPDATE/DELETE, and `bind_result()` + `fetch()` for bound-variable result retrieval. **Note:** `execute_query()` correctly handles UPDATE and DELETE with parameters, but has limitations with UPSERT and REPLACE (see 4.2a, 4.2b).

Query rewriting occurs at **prepare time**, not execute time. If ZTD mode is toggled between `prepare()` and `execute()`, the prepared query retains its original rewritten form.

**Data snapshotting**: The CTE VALUES clause (shadow store data) is also captured at prepare time. A prepared SELECT will NOT see data inserted after it was prepared — even if the INSERT occurs before `execute()`. To see post-INSERT data, the SELECT must be prepared (or re-prepared) after the INSERT. Similarly, re-executing a prepared DELETE or UPDATE operates on the frozen CTE snapshot — previously deleted rows are still "visible" in the snapshot, so `rowCount()` / `ztdAffectedRows()` may report affected-row counts for rows that were already removed by a prior execution of the same statement.

### 3.3 Complex Queries
When ZTD is enabled, the CTE rewriting shall correctly handle:
- **JOINs** (INNER JOIN, LEFT JOIN, RIGHT JOIN, FULL OUTER JOIN on PostgreSQL) across multiple shadow tables.
- **Self-JOINs** where the same shadow table is referenced with different aliases.
- **Aggregations** (COUNT, SUM, MIN, MAX) with GROUP BY and HAVING clauses.
- **Subqueries** in WHERE clauses (e.g., `WHERE id IN (SELECT ...)`).
- **Correlated subqueries** in SELECT list (e.g., `(SELECT COUNT(*) FROM t WHERE t.fk = u.id)`).
- **UNION** queries combining results from shadow tables.
- **EXCEPT** / **INTERSECT** set operations (SQLite and PostgreSQL only; see 3.3d).
- **ORDER BY** with LIMIT and OFFSET.
- **DISTINCT** selection.
- **CASE** expressions (in SELECT and UPDATE SET clauses).
- **LIKE** with wildcard patterns.
- **IN** / **NOT IN** with literal value lists and subqueries.
- **BETWEEN** range conditions.
- **EXISTS** / **NOT EXISTS** correlated subqueries.
- **COALESCE** and other SQL functions.
- **String functions** (UPPER, LOWER, LENGTH, SUBSTR/SUBSTRING, REPLACE, TRIM, GROUP_CONCAT/STRING_AGG, CONCAT).
- **Math functions** (ABS, ROUND, CEIL, FLOOR).
- **JOIN ... USING** syntax (alternative to ON).
- **Multiple user CTEs** (`WITH cte1 AS (...), cte2 AS (...) SELECT ...`).
- **Window functions** (ROW_NUMBER, SUM OVER PARTITION BY, AVG/SUM with ROWS/RANGE BETWEEN frames, LAG, LEAD, RANK, DENSE_RANK).

UPDATE and DELETE statements with subqueries referencing other shadow tables shall also be correctly rewritten.

User-written CTE (WITH) queries shall work correctly alongside ZTD's internal CTE shadowing on MySQL and SQLite. On PostgreSQL, table references inside user CTEs are not rewritten, causing the inner CTE to read from the physical table rather than the shadow store (see 10.3).

### 3.3e CTE-based DML (WITH ... INSERT/UPDATE/DELETE)
`WITH ... INSERT INTO ... SELECT`, `WITH ... UPDATE`, and `WITH ... DELETE` (CTE-based DML) are NOT supported on any platform:

- **MySQL (MySQLi and PDO)**: The `MySqlQueryGuard::classifyWithFallback()` correctly classifies CTE DML as `WRITE_SIMULATED`, but the mutation resolver receives a `WithStatement` (not an `InsertStatement`/`UpdateStatement`/`DeleteStatement`) and throws `RuntimeException` ("Missing shadow mutation for write simulation").
- **SQLite**: The CTE rewriter prepends shadow CTEs, which causes the user's CTE name to become invisible in the DML part of the statement. This produces "no such table: cte_name" errors.
- **PostgreSQL**: The CTE rewriter produces invalid SQL — it does not properly handle user CTEs combined with DML statements. Errors include syntax errors and "relation does not exist".

The shadow store is not corrupted by CTE DML failures — previously inserted data remains intact after the error. Users needing CTE-based DML should either disable ZTD for those queries or rewrite the query as a standard DML with subqueries.

### 3.3a Derived Tables (Subqueries in FROM)
Derived tables (subqueries in the FROM clause) are NOT fully supported by the CTE rewriter. Table references inside derived subqueries are generally not rewritten:

- **MySQL and PostgreSQL**: Derived tables always return empty results because inner table references read from the physical database. This applies both when the derived table is the sole FROM source and when it is JOINed with a regular table.
- **SQLite**: Derived tables as sole FROM source return empty. However, when a derived table is JOINed with a regular table, table references inside the derived subquery ARE rewritten and return shadow data correctly. Mutations in the shadow store are reflected through derived table JOINs on SQLite.

### 3.3b Views
Database views are NOT rewritten by the CTE rewriter. Querying a view through ZTD returns empty results because the view's underlying query reads from physical tables, not the shadow store. This applies to all platforms.

### 3.3c Recursive CTEs
`WITH RECURSIVE` queries that do NOT reference shadow tables (e.g., number series generation) work correctly on all platforms.

`WITH RECURSIVE` queries that reference shadow tables are NOT supported:
- **MySQL**: The CTE rewriter prepends its own `WITH` clause before the `RECURSIVE` keyword, producing invalid SQL (syntax error). This applies to both MySQLi and PDO adapters.
- **SQLite**: The query executes but returns empty results — table references inside the recursive CTE are not rewritten, so the query reads from the physical table (empty).
- **PostgreSQL**: Same behavior as non-recursive user CTEs — returns empty results because table references inside CTEs are not rewritten (see 10.3).

### 3.3d Set Operations (EXCEPT / INTERSECT)
`EXCEPT` and `INTERSECT` set operations work correctly on **SQLite** and **PostgreSQL** — table references in both sides are rewritten to read from the shadow store.

On **MySQL** (both MySQLi and PDO adapters), `EXCEPT` and `INTERSECT` throw `UnsupportedSqlException` ("Multi-statement SQL statement"). The MySQL CTE rewriter incorrectly parses these as multi-statement SQL. UNION works correctly on all platforms.

### 3.4 Fetch Methods

#### PDO Adapter
When ZTD is enabled, the following PDO fetch methods shall return correct results from the shadow store:
- `fetchAll()` with `FETCH_ASSOC`, `FETCH_NUM`, `FETCH_BOTH`, `FETCH_OBJ`, `FETCH_COLUMN`, `FETCH_CLASS`, `FETCH_FUNC` modes.
- `fetchAll()` with combined modes: `FETCH_GROUP|FETCH_ASSOC`, `FETCH_UNIQUE|FETCH_ASSOC`, `FETCH_GROUP|FETCH_COLUMN`, `FETCH_KEY_PAIR`.
- `fetchAll(PDO::FETCH_FUNC, callable)` invokes the callback for each row and returns transformed results.
- `fetch()` for row-by-row iteration (returns `false` when no more rows).
- `fetch(PDO::FETCH_BOUND)` with `bindColumn()` populates bound variables from shadow store data.
- `fetchColumn()` for retrieving a single column value (supports column index argument).
- `fetchObject()` for retrieving rows as `stdClass` objects.
- `fetch(PDO::FETCH_LAZY)` for lazy row objects with property access to column values.
- `setFetchMode(PDO::FETCH_CLASS, ClassName)` and `setFetchMode(PDO::FETCH_INTO, $obj)` — see section 4.10.
- `columnCount()` shall return the correct number of columns in the result set.
- `getColumnMeta($index)` returns column metadata (name, table, etc.) from the shadow store result set.
- `getIterator()` / `foreach` iteration over a `ZtdPdoStatement` shall yield rows correctly.
- `query($sql, $mode, $arg)` accepts a fetch mode as second argument (e.g., `query($sql, PDO::FETCH_COLUMN, 0)`).

**PDO Attribute Interactions:**
- `ATTR_DEFAULT_FETCH_MODE`: Connection-level default fetch mode is respected by ZTD statements. `setFetchMode()` on a statement overrides the connection-level default.
- `ATTR_EMULATE_PREPARES`: Both `true` and `false` work correctly with ZTD shadow store on all platforms. ZTD performs its own query rewriting regardless of this setting.
- `ATTR_STRINGIFY_FETCHES`: Affects type coercion of values returned from shadow store queries. When `true`, numeric values are returned as strings.
- `ATTR_CASE`: Column name case is preserved through CTE rewriting (`CASE_NATURAL`).

#### MySQLi Adapter
When ZTD is enabled, the following MySQLi result methods shall return correct results from the shadow store:
- `fetch_assoc()` returns an associative array (or `null` when no more rows).
- `fetch_row()` returns a numeric array.
- `fetch_object()` returns an `stdClass` object.
- `fetch_array(MYSQLI_BOTH|MYSQLI_ASSOC|MYSQLI_NUM)` returns an array in the requested mode.
- `fetch_all(MYSQLI_ASSOC|MYSQLI_NUM)` returns all rows at once.
- `num_rows` property returns the number of rows in the result set.
- `data_seek($offset)` repositions the result cursor, allowing re-reading of previously fetched rows.
- `bind_result()` + `fetch()` returns `true` for each row and `null` when exhausted.

Re-executing a prepared statement (calling `execute()` multiple times with different parameters) shall work correctly with ZTD-enabled queries.

## 4. Write Operations

### 4.1 INSERT
When an INSERT is executed with ZTD enabled, the system shall track the inserted rows in the shadow store without modifying the physical table.

Subsequent SELECT queries shall include the inserted rows.

Multi-row INSERT (e.g., `INSERT INTO t VALUES (1, 'a'), (2, 'b')`) is supported. The affected row count reflects the total number of inserted rows.

INSERT with NULL values is supported. NULL values are correctly stored in the shadow store and queryable via `IS NULL` / `IS NOT NULL`.

**Limitation (PDO prepared INSERT):** On the PDO adapter, rows inserted via prepared statements (`prepare()` + `execute()`) cannot be subsequently updated or deleted — UPDATE/DELETE operations report affected rows but the shadow store data remains unchanged. Rows inserted via `exec()` work correctly. The MySQLi adapter is NOT affected. See 10.3 for details.

### 4.1a INSERT ... SELECT
When an `INSERT ... SELECT` is executed with ZTD enabled, the system shall insert rows from the SELECT result (which reads from the shadow store) into the target table's shadow store.

On MySQL, `INSERT ... SELECT` requires explicit column lists on both sides (e.g., `INSERT INTO t (a, b) SELECT a, b FROM s`). Using `SELECT *` throws `RuntimeException` ("INSERT column count does not match SELECT column count") because the MySQL InsertTransformer counts `*` as 1 column instead of expanding it. On SQLite, `INSERT ... SELECT *` works correctly (see 10.3).

**Limitation (SQLite and PostgreSQL)**: `INSERT ... SELECT` with computed columns (e.g., `price * 2`), aggregate functions (COUNT, AVG, SUM), or GROUP BY does NOT correctly transfer values. The rows are inserted with correct count, but computed/aggregated column values become NULL. This applies to expressions, arithmetic, and aggregation functions in the SELECT list — only direct column references are transferred correctly. On MySQL (both MySQLi and PDO), all of these work correctly. The workaround on SQLite/PostgreSQL is to SELECT first, then INSERT the results manually in application code.

`INSERT ... SELECT` with LEFT JOIN + GROUP BY + aggregation is a specific case of this limitation (see 10.3). On MySQL with a LEFT JOIN, it throws a column-not-found error instead.

`INSERT ... SELECT WHERE NOT EXISTS` (conditional insert) works correctly on all platforms.

`INSERT ... SELECT` with UNION ALL works correctly on all platforms.

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

When `INSERT ... ON CONFLICT DO NOTHING` (PostgreSQL) is executed and a duplicate exists, the insert is silently ignored. **Note**: On SQLite, `ON CONFLICT DO NOTHING` inserts both rows because the shadow store does not enforce PK constraints (see 10.3). Use `INSERT OR IGNORE` instead on SQLite.

**Limitation (PDO prepared statements):** On the PDO adapter, `INSERT ... ON CONFLICT DO UPDATE` via `prepare()` + `execute()` does NOT update existing rows in the shadow store — the old row is retained unchanged. The same operation works correctly via `exec()`. Users should use `exec()` for upsert operations, or execute SELECT + conditional INSERT/UPDATE in application code (see 10.3). Verified on all 3 PDO platforms (MySQL, PostgreSQL, SQLite). **Note:** The MySQLi adapter handles prepared UPSERT (`ON DUPLICATE KEY UPDATE`) correctly via `prepare()` + `bind_param()` + `execute()` — the existing row is updated as expected. However, MySQLi `execute_query()` (PHP 8.2+) has the same limitation as PDO prepared statements — upsert does NOT update existing rows.

### 4.2b REPLACE
When a `REPLACE INTO` statement (MySQL) is executed with ZTD enabled, the system shall delete any existing row with matching primary key and insert the new row in the shadow store.

**Limitation (PDO prepared statements):** On the PDO adapter, `REPLACE INTO` via `prepare()` + `execute()` does NOT replace existing rows in the shadow store — the old row is retained unchanged. The same operation works correctly via `exec()` (see 10.3). Verified on all PDO platforms supporting REPLACE (MySQL, SQLite). **Note:** The MySQLi adapter handles prepared `REPLACE INTO` correctly via `prepare()` + `bind_param()` + `execute()` — the existing row is replaced as expected. However, MySQLi `execute_query()` (PHP 8.2+) has the same limitation — replace does NOT replace existing rows.

SQLite additionally supports `INSERT OR REPLACE INTO` syntax as a synonym for `REPLACE INTO`. The same exec/prepared statement behavior applies.

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

### 4.2e INSERT IGNORE / Ignore-Duplicate Syntax
When an `INSERT IGNORE INTO` (MySQL), `INSERT OR IGNORE INTO` (SQLite), or `INSERT ... ON CONFLICT (col) DO NOTHING` (PostgreSQL) statement is executed with ZTD enabled, the system shall silently skip rows that would cause a duplicate primary key violation in the shadow store. Non-duplicate rows are inserted normally.

Batch INSERT IGNORE with mixed duplicate and non-duplicate rows correctly inserts only non-duplicate rows while skipping duplicates.

Prepared ignore-duplicate INSERT via `prepare()` + `execute()` also correctly skips duplicates on all platforms.

Ignore-duplicate INSERT does not affect subsequent INSERT operations — a normal INSERT after an ignore-duplicate INSERT works as expected.

**Note**: Each platform uses different syntax: MySQL uses `INSERT IGNORE INTO`, SQLite uses `INSERT OR IGNORE INTO`, and PostgreSQL uses `INSERT ... ON CONFLICT (col) DO NOTHING`. On SQLite, the standard SQL `INSERT ... ON CONFLICT DO NOTHING` does NOT correctly skip duplicates (see 10.3); use `INSERT OR IGNORE` instead.

### 4.3 DELETE
When a DELETE is executed with ZTD enabled, the system shall track the deletion in the shadow store without modifying the physical table.

Subsequent SELECT queries shall not include the deleted rows.

DELETE operations on unreflected tables follow the same `unknownSchemaBehavior` rules as UPDATE (see 4.2).

**Limitation (SQLite)**: `DELETE FROM table` without a WHERE clause is silently ignored on SQLite — the shadow store retains all rows. The workaround is to use `DELETE FROM table WHERE 1=1` which works correctly on all platforms (see 10.3).

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

`lastInsertId()` (PDO) is delegated to the underlying connection. Its value may not reflect shadow-simulated inserts. On PostgreSQL, calling `lastInsertId($sequenceName)` after a shadow INSERT throws `PDOException` ("currval of sequence is not yet defined in this session") because the physical sequence is never advanced. On MySQL and SQLite, `lastInsertId()` returns a value (typically '0') without throwing.

`escape_string()` (mysqli) is an alias for `real_escape_string()` and is delegated to the underlying connection.

The following methods are delegated directly to the underlying connection without ZTD interception: `multi_query()`, `more_results()`, `next_result()`, `autocommit()`, `set_charset()`, `character_set_name()`, `get_charset()`, `select_db()`, `ping()`, `stat()`, `get_server_info()`, `get_connection_stats()`, `stmt_init()`, `store_result()`, `use_result()`.

`multi_query()` bypasses ZTD entirely — queries executed via `multi_query()` operate directly on the physical database.

`select_db()` changes the physical connection's active database but does NOT update the ZTD session's schema reflection or shadow store. Shadow data from the original database persists after `select_db()`.

`stmt_init()` returns a raw `mysqli_stmt` (NOT a `ZtdMysqliStatement`), meaning queries prepared via `stmt_init()` bypass ZTD entirely — writes go to the physical database and reads come from the physical database. Users should use `ZtdMysqli::prepare()` instead.

`insert_id` on ZtdMysqli throws `Error` ("Property access is not allowed yet") because the parent constructor is not called. There is no `lastInsertId()` method equivalent on the MySQLi adapter. On ZtdMysqliStatement, `insert_id` delegates to the inner stmt and returns 0 for shadow INSERTs.

For PDO, the following methods are delegated: `setAttribute()`, `getAttribute()`, `errorCode()`, `errorInfo()`.

For `ZtdPdoStatement`, the following methods are delegated: `closeCursor()`, `setFetchMode()`, `bindColumn()`, `getColumnMeta()`, `errorCode()`, `errorInfo()`, `debugDumpParams()`.

**debugDumpParams() note**: Because ZTD rewrites queries at prepare time, `debugDumpParams()` outputs the rewritten SQL, not the original user SQL. INSERT statements appear as `SELECT ? AS "col", ...`, UPDATE/DELETE statements appear as `WITH "table" AS (...) SELECT ...`. This is correct behavior — it reflects what the database engine actually receives.

### 4.10 FETCH_CLASS / FETCH_INTO
When `setFetchMode(PDO::FETCH_CLASS, ClassName)` is called on a `ZtdPdoStatement`, subsequent `fetch()` calls return instances of the specified class with properties populated from the shadow store result.

`fetchAll(PDO::FETCH_CLASS, ClassName)` returns an array of instances of the specified class.

`setFetchMode(PDO::FETCH_CLASS, ClassName, [$constructorArgs])` passes constructor arguments to each instantiated object.

`PDO::FETCH_CLASS | PDO::FETCH_PROPS_LATE` calls the constructor before setting properties (properties are set after constructor).

`setFetchMode(PDO::FETCH_INTO, $object)` populates an existing object's properties from each fetched row.

All FETCH_CLASS modes work correctly with shadow store data, including after shadow INSERT, UPDATE, and DELETE operations. Verified on all 3 PDO platforms.

### 4.11 PDO Error Modes
When `ATTR_ERRMODE` is set to `ERRMODE_EXCEPTION` (the default in most ZTD test scenarios), invalid SQL shall throw `PDOException`.

When `ATTR_ERRMODE` is set to `ERRMODE_SILENT`, invalid SQL (e.g., querying a non-existent table) shall return `false` from `query()` instead of throwing. The shadow store remains intact — subsequent valid queries succeed.

When `ATTR_ERRMODE` is set to `ERRMODE_WARNING`, invalid SQL shall emit a PHP warning and return `false`.

Normal ZTD operations (INSERT, UPDATE, DELETE, SELECT on shadow tables) work correctly in all three error modes. Shadow data is not affected by the error mode setting.

When `setAttribute(PDO::ATTR_ERRMODE, ...)` is called mid-session to switch error modes, the new mode takes effect immediately for subsequent operations. For example, switching from `ERRMODE_SILENT` to `ERRMODE_EXCEPTION` causes subsequent invalid queries to throw `PDOException` instead of returning `false`.

Prepared statements also respect the current error mode — a prepared statement `execute()` on an invalid query in `ERRMODE_SILENT` returns `false` without throwing.

Verified on all 3 PDO platforms.

### 4.12 Statement Property Access (MySQLi)
When a prepared statement is created via `ZtdMysqli::prepare()`, the ZTD adapter rewrites and executes the query internally. Accessing statement-level properties like `param_count` on the `ZtdMysqliStatement` throws `Error` ("ZtdMysqliStatement object is already closed") because the underlying statement lifecycle is managed internally.

Use `ztdAffectedRows()` for affected row counts. `store_result()` works on prepared SELECT statements after `execute()`. `field_count` and `num_rows` work correctly on `mysqli_result` objects returned by `query()`.

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

Advanced ALTER TABLE operations (RENAME TABLE, CHANGE COLUMN with existing shadow data, MODIFY COLUMN with existing data, DROP COLUMN removing shadow data, multiple sequential ALTER operations) are verified on both MySQL PDO and MySQLi adapters. Physical table isolation is confirmed — ALTER TABLE changes do not leak to the physical database.

**Error handling (MySQL)**: ALTER TABLE error scenarios are properly handled:
- **ADD COLUMN** with a column name that already exists throws `ColumnAlreadyExistsException`.
- **DROP COLUMN** / **MODIFY COLUMN** / **CHANGE COLUMN** / **RENAME COLUMN** on a nonexistent column throws `ColumnNotFoundException`.
- The shadow store remains intact after ALTER TABLE errors — previously inserted data is not lost.
- After a successful ALTER TABLE ADD COLUMN followed by a failed duplicate ADD, subsequent INSERT must include the new column (schema has been updated).

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

PostgreSQL-specific: `DROP TABLE ... CASCADE` and `DROP TABLE ... RESTRICT` are both supported — the CASCADE/RESTRICT modifiers are parsed but have no effect on shadow store behavior (no FK dependencies in shadow). `DROP TABLE IF EXISTS ... CASCADE` also works correctly. Verified on PostgreSQL PDO.

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

**Platform note:** This passthrough behavior is verified on MySQL via `new ZtdMysqli(...)` and `new ZtdPdo(...)` constructors. On MySQL via `ZtdPdo::fromPdo()`, PostgreSQL, and SQLite, UPDATE on unreflected tables throws `RuntimeException` ("UPDATE simulation requires primary keys") instead of passing through — meaning `unknownSchemaBehavior: Passthrough` does NOT take effect for UPDATE operations with these constructors/platforms (see 10.3). DELETE on unreflected tables passes through on MySQL and SQLite, but throws `RuntimeException` on PostgreSQL.

**Nuance (operation history):** On MySQL via `fromPdo()`, if no prior shadow operations have touched the unreflected table, Passthrough mode DOES pass UPDATE through to the physical database. However, once a shadow INSERT is executed on the table, the shadow store "knows" the table but lacks PK schema, causing subsequent UPDATE to throw `RuntimeException`. The behavior depends on operation history, not just configuration.

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
- **DEFAULT**: Column default values are NOT applied. When INSERT omits columns with DEFAULT values, the shadow store inserts NULL (not the default). Users must explicitly provide all column values in INSERT statements when using ZTD mode.

This is by design - the shadow store is an in-memory simulation layer, not a full database engine. Constraint enforcement is deferred to the physical database when changes are eventually applied.

### 8.2 Error Recovery

### 8.2a Transformer Errors
When malformed SQL is executed with ZTD enabled, the ztd-query transformer may throw `RuntimeException` before the query reaches the database engine. This differs from standard PDO/mysqli error propagation where `PDOException` or `mysqli_sql_exception` would be thrown.

### 8.2b Shadow Store Consistency After Errors
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
- Constraint non-enforcement (PRIMARY KEY, NOT NULL, UNIQUE, FOREIGN KEY, DEFAULT not enforced in shadow store).
- Prepared statement parameter binding (bindValue with PARAM_INT/PARAM_STR/PARAM_NULL types, bindParam by-reference with re-execute, execute with positional params array, execute with named params, re-execute with different params, execute_query with NULL, execute_query with UPDATE/DELETE params; execute_query with UPSERT/REPLACE does NOT update existing rows — see 4.2a, 4.2b).
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
- Insert-ignore operations (INSERT IGNORE on MySQL; INSERT OR IGNORE on SQLite; ON CONFLICT DO NOTHING on PostgreSQL).
- Multi-table UPDATE/DELETE operations (UPDATE ... JOIN, DELETE ... JOIN on MySQL; UPDATE ... FROM, DELETE ... USING on PostgreSQL).
- User-written CTE (WITH) queries work alongside ZTD CTE shadowing (MySQL and SQLite; see 10.3 for PostgreSQL limitation).
- INSERT ... SELECT with explicit column lists.
- Multi-row INSERT and NULL value handling.
- Configuration (ZtdConfig, unsupported behavior, behavior rules, ZTD toggle cycles).
- Advanced query patterns: CASE expressions, LIKE, IN, BETWEEN, EXISTS/NOT EXISTS, COALESCE, window functions (ROW_NUMBER, SUM OVER, AVG/SUM with ROWS BETWEEN/RANGE BETWEEN frames, LAG, LEAD, RANK, DENSE_RANK).
- Error recovery: shadow store remains consistent after transformer errors, SQL errors, and constraint violations; subsequent operations succeed.
- CREATE TABLE LIKE (all platforms).
- CREATE TABLE AS SELECT (MySQL and PostgreSQL; see 10.3 for SQLite limitation).
- Query rewriting at prepare time (toggling ZTD between prepare/execute retains rewritten query).
- DDL edge cases: CREATE TABLE IF NOT EXISTS, DROP TABLE IF EXISTS, TRUNCATE then INSERT cycles.
- Data type handling: DATE, DATETIME/TIMESTAMP, TIME, DECIMAL/NUMERIC, INTEGER, FLOAT/DOUBLE, and NULL values preserved correctly in shadow store. TIME columns support comparisons, BETWEEN, and UPDATE operations. See 10.3 for BLOB/BINARY and PostgreSQL BOOLEAN/BIGINT limitations.
- Special character handling: quotes, newlines, Unicode, emoji, empty strings preserved correctly in shadow store (see 10.3 for MySQL backslash exception).
- ZTD lifecycle: shadow data persists across enable/disable toggle cycles; physical data inserted while ZTD is off is not visible when ZTD is re-enabled.
- Transaction isolation: shadow data persists after rollback (shadow store is independent of physical transaction state).
- Query edge cases: COUNT(*) vs COUNT(col) with NULLs, SUM ignoring NULLs, ORDER BY with NULLs, LIMIT 0, LIMIT with OFFSET, self-referencing UPDATE (score = score + 10), string concatenation in UPDATE, DISTINCT with NULLs, GROUP BY HAVING, MIN/MAX on strings, multiple sequential UPDATEs to same row, insert-delete-insert same ID cycle.
- Stress testing: 50 sequential INSERTs, bulk UPDATE, bulk DELETE with correct counts; 200-row prepared INSERT with aggregation, 200-row bulk INSERT with GROUP BY category counts, interleaved INSERT/UPDATE/DELETE with intermediate reads at scale; verified on all platforms.
- Utility methods: getAvailableDrivers(), lastInsertId(), errorCode(), errorInfo(), setAttribute()/getAttribute(), quote(); verified on all PDO platforms.
- Realistic multi-step workflows: e-commerce order processing (create customer/products, add order items, calculate totals, update stock, complete order), user registration with tier upgrade, inventory reporting with LEFT JOINs and aggregations, order cancellation with stock restoration and item cleanup; verified on all 4 adapters (MySQLi, MySQL PDO, PostgreSQL PDO, SQLite PDO) with ZTD isolation confirmed (no data leaks to physical DB).
- Advanced subquery patterns: nested subqueries (3 levels deep), scalar subqueries in SELECT, CASE in WHERE clause, EXISTS/NOT EXISTS correlated subqueries, UNION vs UNION ALL, 3-table JOINs; verified on all PDO platforms.
- Prepared statements with complex queries: prepared JOINs with params, prepared IN/NOT IN clauses with params, prepared CASE WHEN expressions with params, prepared aggregation with GROUP BY re-execute, prepared subqueries with params, prepared UPDATE/DELETE with params, prepared INSERT then query, named params in JOIN; verified on all 4 adapters (MySQLi, MySQL PDO, PostgreSQL PDO, SQLite PDO).
- Composite primary keys: tables with 2-column and 3-column composite PKs correctly support INSERT, UPDATE, DELETE, prepared statements, aggregations, self-JOINs, and partial PK match (WHERE on subset of PK columns); verified on all 4 adapters.
- Derived tables (subqueries in FROM): CTE rewriter does not fully rewrite table references inside derived subqueries; returns empty on MySQL/PostgreSQL. SQLite partially supports derived tables in JOIN context (see 3.3a, 10.3).
- Views: not rewritten by CTE rewriter; querying views through ZTD returns empty results on all platforms.
- INSERT DEFAULT VALUES: not supported on any platform under ZTD (Issue #31). `INSERT INTO t DEFAULT VALUES` throws on SQLite ("Insert statement has no values to project"). `INSERT INTO t (col) VALUES (DEFAULT)` fails on all platforms because InsertTransformer converts to `SELECT DEFAULT AS col` which is invalid SQL. INSERT with partial columns (omitting columns with defaults) works. See 10.3 for details.
- JSON data: INSERT/SELECT/UPDATE with JSON data (text column or native JSON/JSONB type), JSON functions (json_extract on SQLite, JSON_EXTRACT/JSON_UNQUOTE on MySQL, ->> on PostgreSQL), JSON in WHERE clauses, prepared statements with JSON; verified on all 4 adapters.
- CROSS JOIN: explicit CROSS JOIN and implicit cross join (comma-separated FROM) correctly produce cartesian product from shadow tables; mutations (DELETE) correctly reduce CROSS JOIN result set; verified on all 4 adapters.
- FULL OUTER JOIN: correctly handles NULL-extended rows from both sides of the join; works with prepared statements; verified on PostgreSQL (not available on MySQL/SQLite).
- RIGHT JOIN: correctly preserves unmatched rows from the right table with NULLs on the left; verified on all 4 adapters.
- Recursive CTEs: `WITH RECURSIVE` without shadow table references works on all platforms. `WITH RECURSIVE` referencing shadow tables fails with syntax error on MySQL, returns empty on SQLite/PostgreSQL (see 3.3c, 10.3).
- MySQLi statement methods: `ztdAffectedRows()` returns correct affected row counts for INSERT/UPDATE/DELETE, `get_result()` + `fetch_all()` for SELECT, `bind_result()` + `fetch()` for bound variable retrieval, `reset()` clears ZTD result and allows re-execute, `free_result()` allows re-execute; verified on MySQLi.
- Window function FRAME clauses: ROWS BETWEEN (1 PRECEDING AND 1 FOLLOWING), ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW, RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW produce correct rolling averages, cumulative sums, and partitioned results from shadow store data; LAG/LEAD return correct previous/next values with NULLs at boundaries; RANK/DENSE_RANK produce correct rankings; window functions correctly reflect shadow mutations (UPDATE/DELETE); verified on all 4 adapters.
- INSERT ON CONFLICT behavior: `INSERT ... ON CONFLICT(col) DO UPDATE SET ...` works correctly on SQLite and PostgreSQL; `INSERT ... ON CONFLICT(col) DO NOTHING` works on PostgreSQL but inserts both rows on SQLite (shadow store does not enforce PK constraints, see 10.3); `INSERT OR IGNORE` (SQLite-specific shorthand) correctly ignores duplicates.
- Set operations: UNION works on all platforms. EXCEPT and INTERSECT work on SQLite and PostgreSQL (see 3.3d, 10.3 for MySQL limitation).
- String functions: UPPER, LOWER, LENGTH, SUBSTR/SUBSTRING, REPLACE, TRIM work on all platforms. GROUP_CONCAT (MySQL/SQLite), STRING_AGG (PostgreSQL), CONCAT (MySQL), `||` concatenation (PostgreSQL/SQLite) all work correctly with shadow data.
- Math functions: ABS, ROUND, CEIL/CEILING, FLOOR work on all platforms with shadow data.
- JOIN USING syntax: `JOIN ... USING (column)` works correctly alongside ZTD CTE rewriting on all 4 adapters.
- Multiple user CTEs: `WITH cte1 AS (...), cte2 AS (...) SELECT ...` works on MySQL and SQLite.
- NOT IN: `WHERE col NOT IN (literal_list)` and `WHERE col NOT IN (SELECT ...)` work on all platforms.
- Aggregates on empty sets: COUNT returns 0, SUM/AVG/MIN/MAX return NULL on empty shadow result sets; verified on all 4 adapters.
- Zero-match UPDATE/DELETE: operations matching no shadow rows return 0 affected rows without error; verified on all PDO platforms.
- Date/time functions: DATE(), strftime() (SQLite), YEAR()/MONTH()/DATE_ADD()/DATE_FORMAT() (MySQL), TO_CHAR()/INTERVAL arithmetic (PostgreSQL) work correctly with shadow-stored date values. GROUP BY date parts works. Date comparisons in WHERE work. See 10.3 for PostgreSQL EXTRACT limitation.
- Advanced window functions: NTILE, FIRST_VALUE, LAST_VALUE with ROWS BETWEEN UNBOUNDED frames, PARTITION BY with ROW_NUMBER and SUM OVER all work correctly from shadow store data; mutations (UPDATE category) correctly change partition groupings; verified on all 4 adapters.
- NATURAL JOIN: works correctly with shadow data on SQLite; matches on shared column names.
- PostgreSQL-specific features: ILIKE (case-insensitive LIKE), `::` type casting, `||` string concatenation, POSITION(), GENERATE_SERIES (when not referencing shadow tables) — all work correctly with ZTD shadow data. Note: INSERT/UPDATE/DELETE RETURNING clause is **not supported** (see 10.3, Issue #32).
- SQL comments: single-line (`--`) and block (`/* */`) comments preserved through CTE rewriting; verified on all 4 adapters.
- INSERT without column list: `INSERT INTO t VALUES (...)` works correctly on all platforms.
- Multi-table JOINs: 4-table and 5-table JOINs work correctly with shadow data; mutations on joined tables correctly affect JOIN results; verified on all 4 adapters.
- MySQL-specific functions: IF(), IFNULL(), FIND_IN_SET(), CONCAT_WS(), REVERSE(), LPAD() all work correctly with shadow data.
- != operator: works identically to <> on all platforms.
- HAVING without GROUP BY: aggregate conditions without GROUP BY (e.g., `HAVING COUNT(*) > N`) work correctly on all platforms.
- SQLite-specific functions: typeof(), INSTR(), IIF(), printf(), HEX(), NULLIF(), CAST() all work correctly with shadow data.
- Parameterized LIMIT/OFFSET: prepared statements with `LIMIT ? OFFSET ?` work on all platforms. On MySQL, params must use `PARAM_INT` (string-typed params cause syntax error "near '2' OFFSET '0'"). SQLite and PostgreSQL accept both string and integer-typed params.
- Expression-based GROUP BY: `GROUP BY CASE WHEN ... THEN ... END`, `GROUP BY LENGTH(col)`, `GROUP BY SUBSTR(col, ...)` all work correctly from shadow store data; verified on all 4 adapters.
- INSERT...SELECT with WHERE filtering: `INSERT INTO t SELECT ... FROM s WHERE condition` correctly filters shadow data before inserting; `INSERT INTO t SELECT ... FROM s WHERE col > (SELECT AVG(col) FROM s)` works on all platforms.
- Correlated HAVING: `GROUP BY col HAVING COUNT(*) >= (SELECT min_count FROM t WHERE t.cat = p.cat)` works on SQLite.
- Pagination after mutations: parameterized LIMIT/OFFSET correctly reflects INSERT/DELETE mutations in shadow store.
- Conditional aggregation: `SUM(CASE WHEN ... THEN 1 ELSE 0 END)` and `SUM(CASE WHEN ... THEN amount ELSE 0 END)` work correctly from shadow data for status counting and revenue breakdown; mutations correctly update conditional aggregation results; verified on all platforms.
- PostgreSQL FILTER clause: `COUNT(*) FILTER (WHERE ...)` and `SUM(amount) FILTER (WHERE ...)` work correctly with shadow data.
- COUNT DISTINCT: `COUNT(DISTINCT col)` works correctly from shadow data on all platforms.
- Multi-column ORDER BY: `ORDER BY col1 ASC, col2 DESC` works correctly from shadow data on all platforms.
- ORDER BY expression: `ORDER BY ABS(col - value)` and similar expressions work correctly.
- NULL handling edge cases: UPDATE SET NULL, IS NULL / IS NOT NULL after mutations, COALESCE chains (including all-NULL fallback), NULL in CASE expressions, prepared statements with NULL parameter binding (both UPDATE and INSERT), IFNULL() (MySQL), NULLS FIRST / NULLS LAST ordering (PostgreSQL); verified on all 4 adapters.
- WHERE clause operators with mutations: LIKE with `%` and `_` wildcards, NOT LIKE, BETWEEN, NOT BETWEEN, EXISTS / NOT EXISTS correlated subqueries, IN with subquery, comparison operators (`>`, `>=`, `<`, `<=`, `<>`) — all correctly reflect shadow store mutations (INSERT/UPDATE/DELETE); prepared LIKE and BETWEEN with parameters work; verified on all 4 adapters.
- Data migration workflow: SELECT with LEFT JOIN + GROUP BY + aggregation (COUNT, SUM, MAX) works correctly for reading shadow data; manual INSERT of computed stats works; UPDATE with IN (subquery on stats table) works; subquery inside CASE in SELECT list works; complex WHERE with IN subquery + HAVING + AND works; verified on all 4 adapters.
- Column type edge cases: TIME values with comparisons, BETWEEN, and UPDATE work correctly in shadow store; mixed-type arithmetic (INT + FLOAT, CAST) works; CASE with mixed return types works; UPDATE with arithmetic expressions (e.g., `price * 1.1`) works; BOOLEAN toggle via UPDATE works (MySQL TINYINT, PostgreSQL SMALLINT); aggregate functions on mixed types work; verified on all 4 adapters.
- Complex UPDATE patterns: CASE expressions in UPDATE SET (grade reassignment based on salary thresholds), conditional arithmetic (different percentage raises by grade), string concatenation in SET (`name || ' (suffix)'`), prepared UPDATE with CASE and parameters, multiple sequential mutations (UPDATE+UPDATE+UPDATE chains, UPDATE+DELETE+count, INSERT+UPDATE+query, DELETE+INSERT same ID); all correctly accumulate in shadow store; verified on all 4 adapters.
- Physical data replacement (not overlay): when a table has pre-existing physical data, the CTE shadow REPLACES the physical table entirely. Physical data is NOT visible through ZTD queries — the shadow store starts empty. UPDATE/DELETE on physical-only rows match 0 rows. INSERT with IDs overlapping physical IDs works (shadow doesn't check physical PKs). Aggregates on empty shadow return correct empty-set values. Disabling ZTD reveals physical data unchanged. Shadow data persists across enable/disable toggle cycles. Verified on all 4 adapters.
- Prepare-time rewriting persistence: query rewriting occurs at `prepare()` time, not `execute()` time. A statement prepared with ZTD enabled retains its CTE-rewritten form even if ZTD is disabled before `execute()` (reads shadow data). Conversely, a statement prepared with ZTD disabled reads physical data even if ZTD is enabled before `execute()`. Two prepared statements (one with ZTD on, one with ZTD off) coexist and retain their respective behaviors. Re-executing across multiple toggle cycles always uses the prepare-time rewriting. Verified on all 4 adapters.
- Transaction-shadow independence: the shadow store is completely independent of physical transaction state. Shadow data (INSERT, UPDATE, DELETE) persists after `rollBack()` — the shadow store is NOT rolled back. `commit()` does not flush shadow data to the physical database. Multiple transaction cycles (commit + rollback + commit) accumulate shadow data from all cycles. `inTransaction()` reflects the physical connection state, not the shadow state. Prepared statements executed within a rolled-back transaction retain their shadow effects. Verified on all 4 adapters.
- INSERT...SELECT subquery patterns: `INSERT ... SELECT` with WHERE filtering works on all platforms. `INSERT ... SELECT WHERE NOT EXISTS` (conditional insert) works on all platforms. `INSERT ... SELECT` with UNION ALL works on all platforms. `INSERT ... SELECT` after mutations reflects post-mutation shadow state. `INSERT ... SELECT` with computed columns and GROUP BY aggregation works on MySQL but produces NULLs on SQLite/PostgreSQL (see 4.1a, 10.3).
- Shadow-created table interoperability: tables created via `CREATE TABLE` in ZTD mode (shadow-created) interoperate with reflected physical tables. JOINs, LEFT JOINs with aggregation, INSERT...SELECT, NOT IN subqueries, EXISTS subqueries, UPDATE with IN subquery, and DELETE with EXISTS subquery all work correctly between shadow-created and reflected tables on SQLite and MySQL. On PostgreSQL, UPDATE with IN subquery referencing a shadow-created table throws a syntax error (CTE rewriter produces invalid SQL). Verified on all 4 adapters.
- Quoted identifiers with SQL reserved words: column names that are SQL reserved words (`order`, `group`, `key`, `value`, `select`) work correctly when quoted with backticks (MySQL) or double quotes (SQLite, PostgreSQL). INSERT, SELECT, UPDATE, DELETE, GROUP BY on column named `group`, ORDER BY on column named `order`, prepared statements with reserved-word columns, JOINs on reserved-word columns, and table names that are reserved words (e.g., table named `order`) all work correctly through ZTD CTE rewriting. Verified on all 4 adapters.
- `ZtdPdo::connect()` static factory (PHP 8.4+): creates a fully functional ZTD-enabled adapter equivalent to `fromPdo(PDO::connect(...))`. INSERT, SELECT, UPDATE, DELETE, prepared statements, JOINs, and ZTD toggle all work correctly. Verified on all 3 PDO platforms (SQLite, MySQL, PostgreSQL).
- `ZtdPdo::fromPdo()` on MySQL and PostgreSQL: reflects existing table schemas, supports full CRUD, prepared statements, JOINs, and shadow isolation (physical data not visible through ZTD). On MySQL, UPDATE on unreflected tables throws `RuntimeException` if the table has had prior shadow operations (e.g., INSERT), but passes through to physical DB if no shadow operations preceded. Verified on MySQL and PostgreSQL.
- Subqueries in various positions: scalar subqueries in SELECT list, subqueries in ORDER BY, subqueries in HAVING (with nested derived tables), nested WHERE with combined AND/OR/IN operators, CASE with subqueries, EXISTS/NOT EXISTS combined, multiple subqueries in WHERE. Mutations (DELETE) correctly reflected through subqueries. Verified on all 4 adapters.
- Table aliasing: simple aliases (`t`), AS keyword (`AS t`), aliased JOINs, self-JOINs with aliases, aliased aggregations with LEFT JOIN, aliased subqueries, aliased prepared statements with re-execute, and aliased UPDATEs reflected in aliased SELECTs. Verified on all 4 adapters.
- Column aliasing: AS in SELECT (simple and expression aliases), aggregate aliases (COUNT, SUM, AVG), alias in ORDER BY and HAVING, CASE expression aliases, COALESCE aliases with NULL fallback, multi-alias JOINs. Verified on all 4 adapters.
- Batch processing workflow: multi-step shadow accumulation — seeding, conditional UPDATE with IN subquery, JOIN-based filtering, iterative balance updates (self-referencing arithmetic), cross-table aggregation after mutations, prepared statement batch updates with 20+ rows. Verified on all 4 adapters.
- Shadow store DEFAULT non-enforcement: column DEFAULT values are NOT applied by the shadow store. INSERT with omitted columns receives NULL (not the default). Filed as issue #21.
- UNION/EXCEPT/INTERSECT mutation reflection: UNION ALL, UNION (distinct), EXCEPT, and INTERSECT correctly reflect shadow store mutations (INSERT, UPDATE, DELETE). UNION with prepared statements and aggregation in UNION branches work correctly. Verified on all 4 adapters (EXCEPT/INTERSECT on SQLite and PostgreSQL only).
- rowCount()/ztdAffectedRows() accuracy: correct affected row counts after INSERT (1), UPDATE single/multiple/zero rows, DELETE single/multiple/zero rows, exec() return value, re-execute with frozen CTE snapshot (see 3.2 note). Verified on all 4 adapters.
- Concurrent ZTD instances: multiple ZtdPdo/ZtdMysqli instances connected to the same physical database maintain fully independent shadow stores. Interleaved INSERT/UPDATE/DELETE operations are isolated — mutations in one instance are invisible to the other. Disabling ZTD on one instance does not affect the other. Verified on all 4 adapters.
- Prepared HAVING/GROUP BY with parameters: `GROUP BY ... HAVING aggregate >= ?` with bound parameters works correctly on MySQL and PostgreSQL (both PDO and MySQLi). On SQLite, HAVING with prepared params returns empty results (issue #22); HAVING with literal values works. `GROUP BY` with `WHERE` params (no HAVING) works on all platforms.
- Prepared IN/NOT IN/CASE WHEN with parameters: `WHERE id IN (?, ?)`, `WHERE id NOT IN (?, ?)`, `WHERE category IN (:cat1, :cat2)`, `IN (SELECT ... WHERE price > ?)`, and `CASE WHEN price > ? THEN ... ELSE ... END` all work correctly with bound parameters; re-execute with different params works. Verified on all 4 adapters.
- Prepared multi-table UPDATE/DELETE with parameters: MySQL `UPDATE ... JOIN ... SET ... WHERE col > ?` and `DELETE o FROM ... JOIN ... WHERE col = ?` with prepared params work correctly. PostgreSQL `UPDATE ... FROM ... WHERE col > ?` and `DELETE FROM ... USING ... WHERE col = ?` work correctly. JOIN SELECT with GROUP BY + HAVING + prepared params works on MySQL and PostgreSQL. Verified on all 4 adapters (MySQLi, MySQL PDO, PostgreSQL PDO).
- debugDumpParams(): outputs rewritten SQL (not original user SQL) because ZTD rewrites at prepare time. INSERT shows as `SELECT ? AS "col", ...`, UPDATE/DELETE show as `WITH "table" AS (...) SELECT ...`. SELECT shows CTE-prepended form. Parameter metadata (Params count, position, type) is correctly reported. Works before and after execute, and after re-execution. Verified on all 3 PDO platforms.
- FETCH_CLASS with custom classes: `setFetchMode(PDO::FETCH_CLASS, ClassName)`, `fetchAll(PDO::FETCH_CLASS, ClassName)`, constructor args, FETCH_PROPS_LATE, and FETCH_INTO all work correctly with shadow store data. Class instances are populated with correct column values from CTE-rewritten queries. Works with JOINs, prepared statements, and after shadow mutations. Verified on all 3 PDO platforms.
- MySQLi statement introspection: `param_count` on `ZtdMysqliStatement` throws "already closed" because ZTD manages statement lifecycle internally. `affected_rows` and `insert_id` on `ZtdMysqli` throw "Property access is not allowed yet" (use `ztdAffectedRows()` instead). `field_count` and `num_rows` on `mysqli_result` work correctly. `errno`/`error` report 0/empty after success. `store_result()` on prepared SELECT works. Verified on MySQLi.
- bindColumn() and FETCH_BOUND: `bindColumn()` by column number and by column name, with `fetch(PDO::FETCH_BOUND)` iteration, works correctly with shadow store data. bindColumn with type hint (PDO::PARAM_INT, PDO::PARAM_STR) works. After shadow INSERT and UPDATE, bound variables reflect current shadow state. Verified on all 3 PDO platforms.
- FETCH_FUNC callback: `fetchAll(PDO::FETCH_FUNC, callable)` correctly transforms each row via the callback, including after shadow mutations and with prepared statements. FETCH_GROUP | FETCH_COLUMN, FETCH_KEY_PAIR after mutations, and FETCH_COLUMN with GROUP BY aggregation also work. Verified on all 3 PDO platforms.
- PDO attribute interactions: `ATTR_EMULATE_PREPARES` (both true/false), `ATTR_STRINGIFY_FETCHES`, `ATTR_DEFAULT_FETCH_MODE`, `ATTR_CASE` all work correctly with ZTD shadow store. Attribute changes mid-session take effect for subsequent queries. Verified on all 3 PDO platforms.
- exec() return values and rowCount(): `exec()` returns correct affected row counts for INSERT (1 or multi-row), UPDATE (matched rows), DELETE (deleted rows), and zero-match operations. `rowCount()` on prepared statements returns correct counts. Sequential operations return independent counts. Consistent across all 4 adapters.
- ORM-style statement reuse: prepare-once/execute-many with batch INSERT, SELECT with different params, bindValue/bindParam reuse, paginated queries with LIMIT/OFFSET reuse, find-by-ID pattern, multiple coexisting prepared statements used interleaved. Verified on all 3 PDO platforms.
- DDL mid-session lifecycle: DROP TABLE clears shadow data. On SQLite (shadow-created tables), querying after DROP throws "no such table" and INSERT throws "Cannot determine columns". On MySQL and PostgreSQL (physically existing tables), querying after DROP falls through to physical DB (0 rows). DROP+CREATE cycle works on all platforms but behavior differs. Cross-table shadow isolation preserved after DROP. Verified on all 3 PDO platforms.
- Error boundary resilience: Shadow store remains intact after invalid SQL, missing table queries, prepare errors, and multiple consecutive errors. Write operations succeed after errors. Mid-workflow error recovery (successful op → error → successful op) maintains shadow consistency. Error code cleared after successful recovery. Verified on all 3 PDO platforms.
- Prepare-time mutation visibility: Prepared SELECT does not see post-prepare INSERT/UPDATE/DELETE (CTE snapshot frozen). New prepare() after mutation sees latest state. Two prepared statements hold different snapshots (before and after mutation). JOIN prepared before mutation uses frozen snapshot. Re-executed DELETE uses frozen snapshot (reports affected rows for already-deleted rows). query() always sees latest state. Verified on all 3 PDO platforms.
- ALTER TABLE ADD COLUMN after data: On MySQL, ALTER TABLE modifies the physical table and new columns are queryable (physical fallback). On SQLite, ALTER TABLE is accepted but CTE rewriter ignores new columns ("no such column"). On PostgreSQL, ALTER TABLE throws `ZtdPdoException` ("ALTER TABLE not yet supported"). Shadow store remains intact after ALTER TABLE errors on all platforms. Verified on all 3 PDO platforms.
- fetchColumn(), closeCursor(), fetchObject(), FETCH_KEY_PAIR, FETCH_COLUMN: all work correctly with shadow data across platforms. fetchColumn with column index, iteration via fetchColumn loop, fetchColumn returns false when exhausted. Verified on all 3 PDO platforms.
- Iterator and default fetch mode: foreach/getIterator, ATTR_DEFAULT_FETCH_MODE (NUM/ASSOC/OBJ), setFetchMode override, query() with fetch mode argument. Verified on all 3 PDO platforms.
- Advanced fetch modes: FETCH_BOTH, FETCH_NUM, FETCH_OBJ, FETCH_LAZY, fetchAll with FETCH_GROUP, FETCH_UNIQUE, FETCH_COLUMN with column index, setFetchMode persistence across fetches. Verified on all 3 PDO platforms.
- nextRowset(): delegates to underlying PDO driver. MySQL returns false (no additional result sets from CTE queries). SQLite and PostgreSQL throw PDOException "Driver does not support this function". Verified on all 3 PDO platforms.
- MySQL backslash corruption: string values containing backslashes in shadow store are corrupted by CTE rewriter. `\t` → tab, `\n` → newline, `\b` → backspace, `\r` → carriage return. Double backslash `\\` also affected. Occurs with both exec() and prepared statements. SQLite and PostgreSQL not affected. Verified on MySQL PDO.
- PostgreSQL BOOLEAN/BIGINT edge cases: BOOLEAN `true` via prepared statement works, but `false` fails on SELECT (CTE generates invalid `CAST('' AS BOOLEAN)`). BIGINT within int32 range works, values exceeding int32 fail (CTE generates `CAST(value AS integer)` instead of `bigint`). Verified on PostgreSQL PDO.
- PDO error mode interactions: `ERRMODE_EXCEPTION` throws on invalid SQL, `ERRMODE_SILENT` returns false, `ERRMODE_WARNING` emits warning and returns false. Normal shadow operations work in all modes. Switching error modes mid-session takes effect immediately. Shadow store remains intact after errors in any mode. Verified on all 3 PDO platforms.
- CASE WHEN in ORDER BY: `ORDER BY CASE role WHEN 'admin' THEN 1 WHEN 'moderator' THEN 2 ELSE 3 END, name` correctly sorts shadow data by custom priority. Works with both `query()` and `prepare()`+`execute()`. Verified on all 3 PDO platforms.
- Interleaved prepared statements: multiple prepared statements on the same connection can be executed in interleaved fashion (prepare A, prepare B, execute A, execute B, re-execute A with different params). Each statement maintains its own CTE snapshot and result set independently. Verified on all 3 PDO platforms.
- FETCH_LAZY: `fetch(PDO::FETCH_LAZY)` returns a lazy row object with property access to column values. Works correctly with shadow data including after mutations. Verified on all 3 PDO platforms.
- exec() with SELECT: `exec()` on a SELECT query goes through ZTD rewriter and returns `rowCount()` (typically 0 on SQLite, may return actual row count on MySQL/PostgreSQL). Not an error. Verified on all 3 PDO platforms.
- Very long string values: string values of 10,000+ characters stored and retrieved correctly through shadow store via prepared statements. Verified on all 3 PDO platforms.
- Wide tables (20+ columns): tables with 20 columns correctly store and retrieve all column values through CTE rewriting. Verified on all 3 PDO platforms.
- Sequential mutations: insert-then-update, insert-then-delete, update-then-delete, multiple updates on same row (last write wins), delete-all-then-insert, interleaved inserts and deletes, bulk update then selective delete — all correctly accumulate in shadow store. Physical isolation confirmed (no data leaks). Verified on all 4 adapters (MySQLi, MySQL PDO, PostgreSQL PDO, SQLite PDO).
- MySQL DELETE/UPDATE with ORDER BY + LIMIT: `DELETE FROM t ORDER BY col LIMIT n` and `UPDATE t SET ... ORDER BY col LIMIT n` correctly select and affect only the first N rows in the specified order. Works with LIMIT only, ORDER BY + LIMIT, and WHERE + ORDER BY + LIMIT. Physical isolation confirmed. Verified on MySQL MySQLi and MySQL PDO.
- Multi-row REPLACE INTO: `REPLACE INTO t (cols) VALUES (...), (...), (...)` correctly inserts all rows when none exist, replaces matching rows on primary key, and handles partial overlap. Physical isolation confirmed. Verified on MySQL MySQLi and MySQL PDO.
- CTE-based DML (WITH ... INSERT/UPDATE/DELETE): NOT supported on any platform. MySQL throws RuntimeException ("Missing shadow mutation"); SQLite throws "no such table" for user CTE names; PostgreSQL produces syntax errors. Shadow store remains intact after failures. Verified on all 4 adapters.
- Empty string vs NULL: empty string (`''`) and NULL are correctly distinguished in shadow store — `IS NULL` does not match empty strings, `= ''` does not match NULL. Verified on all 3 PDO platforms.
- PARAM_BOOL binding: `bindValue()` with `PDO::PARAM_BOOL` works for boolean-to-integer column comparisons on SQLite and MySQL. On PostgreSQL, PARAM_BOOL sends 't'/'f' strings which fail for INT columns — use PARAM_INT instead. Verified on all 3 PDO platforms.
- prepare() with empty options array: `prepare($sql, [])` works identically to `prepare($sql)`. Verified on all 3 PDO platforms.
- closeCursor() then re-execute: calling `closeCursor()` mid-fetch then `execute()` with new params works correctly. Verified on all 3 PDO platforms.
- REST API workflow patterns: paginated listing, single-record fetch, create/update/delete, multi-table JOIN filtering, LIKE search, BETWEEN price range, conditional aggregation, soft-delete patterns all work correctly with ZTD shadow store. Verified on all 3 PDO platforms.
- ZTD toggle error resilience: errors during ZTD-enabled or ZTD-disabled operations do not corrupt the shadow store. Shadow data persists through toggle cycles even after errors. Prepared statements created with ZTD enabled survive toggle cycles (retain their CTE-rewritten form). Shadow-only tables (created via ZTD CREATE TABLE) are not accessible when ZTD is disabled (throw PDOException "no such table" on SQLite). On MySQL/PostgreSQL (where physical tables exist), shadow INSERT/UPDATE/DELETE changes are not visible when ZTD is disabled — physical DB retains original data. Multiple toggle cycles accumulate shadow data correctly. Verified on all 3 PDO platforms and MySQLi.
- Insert-ignore duplicate handling: `INSERT IGNORE INTO` (MySQL), `INSERT OR IGNORE INTO` (SQLite), and `INSERT ... ON CONFLICT (col) DO NOTHING` (PostgreSQL) all correctly skip duplicate PK rows in the shadow store. Non-duplicate rows are inserted normally. Batch INSERT IGNORE with mixed duplicates correctly skips only duplicate rows. Prepared insert-ignore works correctly. Subsequent normal INSERT operations are unaffected. Physical isolation confirmed (shadow data does not leak to physical DB). Verified on all 4 adapters (MySQLi, MySQL PDO, PostgreSQL PDO, SQLite PDO).
- PostgreSQL DISTINCT ON: `SELECT DISTINCT ON (col) ... ORDER BY col, ...` works correctly with shadow data, returning the first row per group. Verified on PostgreSQL PDO.
- PostgreSQL LATERAL JOIN: `FROM t, LATERAL (SELECT ... FROM t2 WHERE t2.fk = t.pk ...)` tested — may return empty results (similar to derived table limitation) but does not error. Verified on PostgreSQL PDO.
- PostgreSQL STRING_AGG with ORDER BY: `STRING_AGG(col, ', ' ORDER BY col)` correctly aggregates shadow data with ordering. Verified on PostgreSQL PDO.
- PostgreSQL GREATEST/LEAST: scalar functions work correctly with shadow data. Verified on PostgreSQL PDO.
- MySQL GROUP_CONCAT with ORDER BY: `GROUP_CONCAT(col ORDER BY col SEPARATOR ', ')` correctly aggregates shadow data. Verified on MySQL PDO.
- MySQL IF/IFNULL/CONCAT_WS: conditional and string functions work correctly with shadow data including NULL handling. Verified on MySQL PDO.
- MySQL REVERSE/LPAD: string manipulation functions work correctly with shadow data. Verified on MySQL PDO.
- Correlated scalar subqueries with COUNT/SUM: `(SELECT COUNT(*) FROM t2 WHERE t2.fk = t1.pk)` in SELECT list works correctly across shadow tables. Verified on MySQL PDO.
- ALTER TABLE advanced operations (MySQL): RENAME TABLE, CHANGE COLUMN with existing shadow data (column renamed in store), MODIFY COLUMN with existing data (type change, data preserved), DROP COLUMN removes data from shadow rows, ADD COLUMN then INSERT with new column, multiple sequential ALTER operations. Physical isolation confirmed (ALTER TABLE changes do not leak to physical table). ALTER TABLE error recovery: ColumnAlreadyExistsException for duplicate column, ColumnNotFoundException for nonexistent column, shadow store intact after errors. Verified on MySQL PDO and MySQLi.

### 10.2 Platform-Specific Notes
- **TRUNCATE**: Verified on MySQL and PostgreSQL. SQLite does not have native TRUNCATE TABLE syntax and attempting `TRUNCATE TABLE` throws an exception; `DELETE FROM table` (DML) is the equivalent but follows regular DELETE processing through ZTD. PostgreSQL supports various TRUNCATE options (`TRUNCATE TABLE`, `TRUNCATE` without TABLE keyword, `TRUNCATE ONLY`, `RESTART IDENTITY`, `CONTINUE IDENTITY`, `CASCADE`) — all options are accepted and result in the shadow store being cleared. The options have no additional effect in shadow mode.
- **multi_query() bypass**: Verified on MySQL (MySQLi). `multi_query()` bypasses ZTD entirely even when ZTD is enabled — writes go directly to the physical database and reads bypass the shadow store.
- **FOREIGN KEY constraints**: The foreign key constraint scenario uses a parent-child table relationship on MySQL and PostgreSQL. SQLite does not include the foreign key test because SQLite requires `PRAGMA foreign_keys = ON` to enforce them, which is outside ZTD scope.
- **Unsupported SQL**: Platform-specific unsupported SQL examples: MySQL uses `SET @var = 1`, PostgreSQL uses `SET search_path TO public`, SQLite uses `PRAGMA journal_mode=WAL`. All three platforms support behavior rules with prefix and regex patterns.
- **ALTER TABLE**: Fully supported on MySQL (via `AlterTableMutation`) for ADD/DROP/MODIFY/CHANGE/RENAME COLUMN, RENAME TABLE, ADD/DROP PRIMARY KEY, and ADD/DROP FOREIGN KEY (metadata-only no-ops). Unsupported operations that throw `UnsupportedSqlException`: ADD/DROP INDEX, ADD/DROP KEY, ADD/DROP UNIQUE KEY, ADD/DROP CONSTRAINT, RENAME INDEX/KEY, ALTER COLUMN SET/DROP DEFAULT, ORDER BY. SQLite accepts ALTER TABLE without error but CTE rewriter ignores schema changes (see 5.1a). PostgreSQL throws `ZtdPdoException` for ALTER TABLE.
- **TEMPORARY/UNLOGGED tables**: `CREATE TEMPORARY TABLE` (and `CREATE TEMP TABLE`) works correctly on all platforms — the shadow store creates the table regardless of the TEMPORARY modifier. PostgreSQL `CREATE UNLOGGED TABLE` also works. The `TEMPORARY`/`TEMP`/`UNLOGGED` keywords do not affect shadow store behavior. DROP TABLE on shadow-created temporary tables removes them from the shadow store. Verified on MySQL (MySQLi), PostgreSQL (PDO), and SQLite (PDO).
- **PostgreSQL ONLY keyword**: `UPDATE ONLY`, `DELETE FROM ONLY`, and `TRUNCATE ONLY` work correctly — the PgSqlParser regex patterns include `(?:ONLY\s+)?` to skip the keyword during table name extraction. In shadow mode, ONLY has no effect (no table inheritance in shadow store). Verified on PostgreSQL PDO.
- **CREATE TABLE LIKE**: Verified on MySQL, PostgreSQL, and SQLite. PostgreSQL uses `CREATE TABLE t (LIKE source)` syntax.
- **CREATE TABLE AS SELECT**: Fully supported on MySQL and PostgreSQL. SQLite has limitations (see 5.1c).
- **Unknown Schema**: Behavior rules and unknown schema handling are verified on all platforms. See 10.3 for cross-platform inconsistencies.
- **Insert-ignore syntax**: MySQL uses `INSERT IGNORE INTO`, SQLite uses `INSERT OR IGNORE INTO`, PostgreSQL uses `INSERT ... ON CONFLICT (col) DO NOTHING`. All three syntaxes correctly skip duplicate PK rows in the shadow store. On SQLite, the standard SQL `ON CONFLICT DO NOTHING` syntax (without the `OR IGNORE` shorthand) does NOT skip duplicates (see 10.3). Verified on all 4 adapters (MySQLi, MySQL PDO, PostgreSQL PDO, SQLite PDO).
- **SQLite conflict resolution syntax**: SQLite's `INSERT OR REPLACE INTO` syntax works correctly as a synonym for `REPLACE INTO` via `exec()`. Multiple replacements on the same PK correctly retain only the last value. Prepared `INSERT OR REPLACE` has the same limitation as prepared `REPLACE INTO` (old row retained, see 4.2b). Verified on SQLite PDO.
- **execute_query with UPDATE/DELETE**: MySQLi `execute_query()` (PHP 8.2+) correctly handles UPDATE and DELETE operations with parameters, including multi-row updates/deletes and affected row counts. Verified on MySQLi.
- **execute_query UPSERT/REPLACE limitation**: MySQLi `execute_query()` with UPSERT (`ON DUPLICATE KEY UPDATE`) and REPLACE does NOT update/replace existing rows — the old row is retained. This contrasts with `prepare()` + `bind_param()` + `execute()` which works correctly. New row inserts via execute_query UPSERT/REPLACE work as expected. Verified on MySQLi.
- **Prepared upsert limitation (cross-platform)**: PDO prepared `INSERT ... ON CONFLICT DO UPDATE` does NOT update existing rows on any platform (MySQL, PostgreSQL, SQLite). The `exec()` path works correctly on all platforms. Verified on all 3 PDO platforms.
- **Schema-qualified table names (PostgreSQL)**: See 10.3 for details. INSERT/UPDATE/DELETE work; SELECT returns empty due to CTE rewriter limitation. Verified on PostgreSQL PDO.
- **Doubled-quote escaping ('')**: MySQL and SQLite handle `''` escaping correctly. PostgreSQL has a parser bug (issue #25) — see 10.3 for details. Workaround: use prepared statements.
- **ALTER TABLE exception types and error handling**: See 10.3 for cross-platform differences. MySQL validates column existence and throws raw core exceptions (ColumnAlreadyExistsException, ColumnNotFoundException — NOT wrapped in adapter exceptions). SQLite silently ignores all validation errors.

### 10.3 Cross-Platform Inconsistencies
The following behaviors differ across platforms and may indicate areas for improvement:

- **Unknown schema UPDATE (Passthrough mode)**: On MySQL (MySQLi adapter and PDO adapter via `new ZtdPdo()` constructor), UPDATE on unreflected tables passes through to the physical database as documented. On MySQL via `ZtdPdo::fromPdo()`, PostgreSQL, and SQLite, the same operation throws `RuntimeException` ("UPDATE simulation requires primary keys") instead of passing through. This means the `unknownSchemaBehavior: Passthrough` setting does not take effect for UPDATE operations when using `fromPdo()` or on PostgreSQL/SQLite. **Nuance**: On MySQL via `fromPdo()`, if NO prior shadow operations have touched the unreflected table, Passthrough mode DOES pass UPDATE through to the physical database. However, if a shadow INSERT was previously executed on the table, the shadow store "knows" the table but lacks PK schema, causing UPDATE to throw `RuntimeException`. This means the behavior depends on operation history, not just configuration.

- **Unknown schema UPDATE (Exception mode)**: On MySQL, UPDATE on unreflected tables throws `ZtdPdoException` ("Unknown table"). On PostgreSQL and SQLite, it throws `RuntimeException` ("UPDATE simulation requires primary keys") — the exception type and message differ from MySQL.

- **Unknown schema DELETE**: On MySQL and SQLite, DELETE on unreflected tables in Passthrough mode passes through to the physical database. On PostgreSQL, DELETE in Exception mode throws `RuntimeException` ("Unknown table") rather than `ZtdPdoException`. On SQLite, DELETE in Exception mode also throws `RuntimeException` ("Unknown table") rather than `ZtdPdoException`.

- **INSERT ... ON CONFLICT DO NOTHING (SQLite)**: On PostgreSQL, `ON CONFLICT DO NOTHING` correctly ignores duplicate inserts in the shadow store. On SQLite, the same syntax inserts both rows into the shadow store (the DO NOTHING clause is not processed, and the shadow store does not enforce PK constraints). `ON CONFLICT DO UPDATE` works correctly on both platforms.

- **INSERT ... SELECT * (MySQL only)**: On MySQL, `INSERT INTO t SELECT * FROM s` throws `RuntimeException` ("INSERT column count does not match SELECT column count") because the MySQL InsertTransformer counts `SELECT *` as 1 column instead of expanding it. The workaround is to use explicit column lists: `INSERT INTO t (a, b) SELECT a, b FROM s`. On SQLite and PostgreSQL, `INSERT ... SELECT *` works correctly.

- **User-written CTEs (PostgreSQL)**: On MySQL and SQLite, user-written CTE queries (e.g., `WITH cte AS (SELECT * FROM t) SELECT * FROM cte`) work correctly — table references inside the user's CTE are rewritten to read from the shadow store. On PostgreSQL, table references inside user CTEs are NOT rewritten, so the inner CTE reads from the physical table (empty) instead of the shadow store, returning 0 rows.

- **Schema-qualified table names (PostgreSQL)**: When using `public.tablename` (or any `schema.tablename`) syntax, INSERT/UPDATE/DELETE work correctly — the mutation resolver strips the schema prefix and resolves to the unqualified table name in the shadow store. However, SELECT with schema-qualified names returns empty results because the CTE rewriter does not recognize `public.tablename` as a shadow table. The workaround is to use unqualified table names in SELECT queries. Mixed usage (INSERT via `public.tablename`, SELECT via `tablename`) works correctly.

- **execute_query vs prepare+bind_param for UPSERT/REPLACE (MySQLi)**: MySQLi `prepare()` + `bind_param()` + `execute()` correctly handles UPSERT (`ON DUPLICATE KEY UPDATE`) and `REPLACE INTO` — existing rows are updated/replaced as expected. However, `execute_query()` (PHP 8.2+), which internally calls `prepare()` + `execute($params)`, does NOT update/replace existing rows. This suggests the array-param `execute()` path differs from the `bind_param()` path for mutation processing. UPDATE and DELETE operations work correctly via both paths.

- **Multi-table UPDATE/DELETE syntax**: MySQL uses `UPDATE t1 JOIN t2 ON ... SET ...` and `DELETE t1 FROM t1 JOIN t2 ON ...`. PostgreSQL uses `UPDATE t1 SET ... FROM t2 WHERE ...` and `DELETE FROM t1 USING t2 WHERE ...`. Both syntaxes are supported by their respective platform adapters.

- **MySQL comma-syntax multi-table UPDATE**: MySQL's alternative comma syntax (`UPDATE t1, t2 SET ... WHERE ...`) is partially supported. The first table listed gets its columns updated, but the secondary table's columns may not be updated depending on the cross-table WHERE conditions. This contrasts with JOIN syntax (`UPDATE t1 JOIN t2 ON ... SET ...`) which works correctly. The `MultiUpdateMutation` processes the CTE-rewritten query and may not properly handle cross-table joins expressed via comma syntax. Users should prefer JOIN syntax for multi-table UPDATE operations.

- **MySQL multi-target DELETE only affects first table** (Issue #26): When using `DELETE t1, t2 FROM t1 JOIN t2 ON ... WHERE ...`, only the first listed table has matching rows deleted. The second table is not affected. This is the same limitation as comma-syntax multi-table UPDATE — the `MultiDeleteMutation` is created but the CTE rewriter only processes the deletion against the primary table. Single-target DELETE with JOIN (`DELETE t1 FROM t1 JOIN t2 ON ... WHERE ...`) works correctly. Verified on both MySQLi and PDO adapters.

- **SQLite ALTER TABLE RENAME TO drops shadow data** (Issue #27): On SQLite, `ALTER TABLE old_name RENAME TO new_name` drops the shadow for the old table name but does NOT create a new entry under the new name. After rename: (1) the old table name falls through to the physical DB, returning only physical rows; (2) the new table name is not accessible (throws PDOException); (3) any ZTD-inserted data is permanently lost. This is because `SqliteMutationResolver::resolveAlterRenameTable()` only creates a `DropTableMutation` for the old name. On MySQL, RENAME TABLE works correctly (shadow store is updated).

- **ALTER TABLE schema propagation (SQLite)**: On MySQL, ALTER TABLE fully updates the shadow schema and CTE rewriting reflects changes. On SQLite, ALTER TABLE is accepted without error but the CTE rewriter continues to use the original reflected schema — added columns are silently dropped from query results, dropped columns still appear, and renamed columns keep their old name.

- **CREATE TABLE AS SELECT (SQLite)**: On MySQL and PostgreSQL, CTAS correctly creates and populates the shadow table for subsequent queries. On SQLite, CTAS creates the shadow table but SELECT immediately fails with "no such table". After a subsequent INSERT, SELECT works but only returns the INSERTed rows — the original CTAS data is lost.

- **HAVING with prepared statement parameters (SQLite)**: On MySQL and PostgreSQL (both PDO and MySQLi), `GROUP BY ... HAVING aggregate >= ?` with bound parameters works correctly. On SQLite, HAVING with prepared params returns empty results — the HAVING clause filter evaluates incorrectly when the threshold is a bound parameter rather than a literal. HAVING with literal values works correctly on SQLite. Issue #22.

- **Backslash corruption in MySQL shadow store**: On MySQL (both MySQLi and PDO adapters), backslash characters in string values inserted via prepared statements are corrupted in the shadow store. The CTE rewriter embeds values as string literals without escaping backslashes, causing MySQL to interpret escape sequences: `\t` → tab, `\n` → newline, `\b` → backspace, `\r` → carriage return, `\0` → null byte, `\\` → single backslash. Unrecognized sequences like `\f` drop the backslash. This does NOT affect SQLite or PostgreSQL, which handle backslashes correctly. **This is a potential issue for users storing file paths, regular expressions, or other backslash-containing data.**

- **PostgreSQL BOOLEAN false casting**: On PostgreSQL, inserting PHP `false` into a BOOLEAN column via prepared statement succeeds, but subsequent SELECT fails because the CTE rewriter generates `CAST('' AS BOOLEAN)`, which is invalid PostgreSQL syntax. `true` works correctly. MySQL is unaffected (uses TINYINT). SQLite is unaffected (typeless).

- **PostgreSQL BIGINT overflow**: On PostgreSQL, inserting large integers (> 2,147,483,647) into BIGINT columns via prepared statement succeeds, but subsequent SELECT fails because the CTE rewriter generates `CAST(value AS integer)` instead of `CAST(value AS bigint)`, causing numeric overflow. MySQL and SQLite handle BIGINT values correctly.

- **UPDATE with IN (subquery GROUP BY HAVING)**: On MySQL (both MySQLi and PDO), `UPDATE t SET ... WHERE id IN (SELECT col FROM t2 GROUP BY col HAVING SUM(x) > N)` works correctly when the subquery references a **different** table. On SQLite, the CTE rewriter produces incomplete SQL, causing "incomplete input" error. On PostgreSQL, the CTE rewriter generates an incorrect cross join between tables, causing "ambiguous column" error. The SELECT version of the same subquery works correctly on all platforms. This is a potential issue for users who need to UPDATE based on aggregated subquery results on SQLite or PostgreSQL.

- **UPDATE with self-referencing IN subquery + GROUP BY HAVING**: On MySQL, `UPDATE t SET ... WHERE col IN (SELECT col FROM t GROUP BY col HAVING AVG(x) > N)` (where the subquery references the **same** table being updated) runs without error but incorrectly updates ALL rows instead of only matching ones. The CTE rewriter mishandles the self-referencing subquery.

- **UPDATE with correlated subquery in SET clause**: `UPDATE t1 SET col = (SELECT SUM(col2) FROM t2 WHERE t2.fk = t1.pk)` works on MySQL. On SQLite, the CTE rewriter produces "near FROM: syntax error". On PostgreSQL, the CTE rewriter produces "column must appear in GROUP BY clause" error.

- **DELETE without WHERE clause (SQLite)** (Issue #7): On MySQL and PostgreSQL, `DELETE FROM table` (without WHERE clause) correctly clears the shadow store. On SQLite, `DELETE FROM table` without a WHERE clause is silently ignored — the shadow store retains all rows. The workaround is to use `DELETE FROM table WHERE 1=1` which works correctly on all platforms.

- **CTE-based DML (WITH ... INSERT/UPDATE/DELETE)**: Not supported on any platform. On MySQL (both MySQLi and PDO), `classifyWithFallback()` correctly identifies WITH DML as `WRITE_SIMULATED`, but the mutation resolver receives a `WithStatement` and throws `RuntimeException` ("Missing shadow mutation for write simulation"). On SQLite, the CTE rewriter prepends shadow CTEs, making user CTE names invisible and producing "no such table" errors. On PostgreSQL, the CTE rewriter produces invalid SQL (syntax errors or "relation does not exist"). The shadow store is not corrupted by these failures. Users should rewrite CTE-based DML as standard DML with subqueries, or disable ZTD for those queries.

- **Recursive CTEs with shadow tables**: On MySQL, `WITH RECURSIVE` referencing a shadow table causes a syntax error because the CTE rewriter prepends `WITH ztd_shadow AS (...)` before the `RECURSIVE` keyword, producing `WITH ztd_shadow AS (...), RECURSIVE cat_tree AS (...)` — invalid SQL. On SQLite, the query executes but returns empty results (table references not rewritten). On PostgreSQL, same as SQLite (returns empty). Non-recursive `WITH` works on MySQL and SQLite but not PostgreSQL (documented separately). Users needing hierarchical queries with ZTD should use application-level recursion or disable ZTD for those queries.

- **Derived tables in JOIN (SQLite vs MySQL/PostgreSQL)**: On SQLite, derived tables JOINed with regular tables work correctly — the CTE rewriter rewrites table references inside the derived subquery, and shadow mutations are visible. On MySQL and PostgreSQL, derived tables always return empty results regardless of JOIN context, because the CTE rewriter does not rewrite table references inside derived subqueries. Users relying on derived tables with ZTD should use direct JOINs or CTEs instead.

- **EXCEPT / INTERSECT (MySQL)**: On MySQL (both MySQLi and PDO adapters), `EXCEPT` and `INTERSECT` throw `UnsupportedSqlException` ("Multi-statement SQL statement") because the MySQL CTE rewriter misparses the semicolon-free set operation as a multi-statement query. On SQLite and PostgreSQL, both operators work correctly with shadow data. Users needing EXCEPT/INTERSECT on MySQL should use NOT IN / IN subqueries instead, or disable ZTD for those queries.

- **PostgreSQL EXTRACT on shadow dates**: On PostgreSQL, `EXTRACT(YEAR FROM date_column)` returns 0 for DATE values stored in the shadow store. The CTE rewriter stores date values as strings, and PostgreSQL's `EXTRACT` function cannot parse them. MySQL `YEAR()`/`MONTH()`, SQLite `strftime()`, and PostgreSQL `TO_CHAR()` all work correctly on shadow-stored dates. Users should use `TO_CHAR(date_col, 'YYYY')` instead of `EXTRACT(YEAR FROM date_col)` on PostgreSQL.

- **NULL sort order in ORDER BY**: MySQL and SQLite sort NULLs first in ASC order. PostgreSQL sorts NULLs last in ASC order. This is standard SQL behavior (not a ZTD issue), but tests should account for the difference.

- **ON DUPLICATE KEY UPDATE self-referencing expression (MySQL)**: When using `INSERT ... ON DUPLICATE KEY UPDATE stock = stock + VALUES(stock)`, the shadow store loses the original row's column value. Instead of computing `old_value + new_value`, the `stock` reference in the SET clause does not resolve to the existing row's value. Simple replacement (e.g., `stock = VALUES(stock)`) works correctly. Users needing increment-style upserts should read the current value first and compute the new value in application code.

- **PDO prepared INSERT: rows cannot be updated/deleted**: On the **PDO adapter**, rows inserted via prepared statements (`prepare()` + `execute()`) cannot be subsequently updated or deleted. UPDATE/DELETE operations report affected rows, but the shadow store data remains unchanged. This affects all PDO platforms (SQLite, MySQL, PostgreSQL). Rows inserted via `exec()` work correctly. **Note:** The **MySQLi adapter** is NOT affected — prepared INSERT + UPDATE/DELETE works correctly. Users on the PDO adapter should use `exec()` for INSERT operations when subsequent UPDATE/DELETE is needed.

- **Upsert via PDO prepared statements (MySQL and PostgreSQL)**: On the **PDO adapter**, `REPLACE INTO` (MySQL) and `INSERT ... ON CONFLICT DO UPDATE` (PostgreSQL) via **prepared statements** (`prepare()` + `execute()`) do not update existing rows in the shadow store — the old row is retained unchanged. The same operations work correctly via `exec()` (non-prepared). Users should use `exec()` for upsert operations, or execute SELECT + conditional INSERT/UPDATE in application code. **Note:** The **MySQLi adapter** handles prepared UPSERT (`ON DUPLICATE KEY UPDATE`) and `REPLACE INTO` correctly — existing rows are updated/replaced as expected.

- **INSERT...SELECT with computed/aggregated columns (SQLite and PostgreSQL)**: On **SQLite and PostgreSQL**, `INSERT ... SELECT` with any non-simple column expression — computed columns (e.g., `price * 2`), aggregate functions (COUNT, AVG, SUM), or GROUP BY — inserts the correct number of rows but all computed/aggregated column values become NULL. Only direct column references are transferred correctly. On **MySQL** (both MySQLi and PDO), all of these work correctly: computed columns transfer computed values, and GROUP BY with aggregation transfers aggregate values. The LEFT JOIN + GROUP BY variant is a more severe case: on MySQL it throws a column-not-found error (lost JOIN alias references). The workaround on all platforms is to SELECT the data first, then INSERT the results manually in application code.

- **BLOB/BINARY data with binary bytes**: Inserting binary data (containing null bytes, high-byte values like `\xFF`) into BLOB/BYTEA columns via prepared statements succeeds, but subsequent SELECT fails because the CTE rewriter embeds the binary bytes as string literals, producing invalid SQL syntax (e.g., "unrecognized token" on SQLite). Text-only BLOB payloads work correctly. Users storing binary data should disable ZTD for those queries or encode binary data (e.g., base64) before inserting.

- **SAVEPOINT commands**: SAVEPOINT, RELEASE SAVEPOINT, and ROLLBACK TO SAVEPOINT are not supported in ZTD mode. Behavior is platform-specific:
  - **SQLite**: All three commands throw `UnsupportedSqlException` ("Statement type not supported").
  - **MySQL** (PDO and MySQLi): SAVEPOINT and RELEASE SAVEPOINT throw "Empty or unparseable SQL statement"; ROLLBACK TO SAVEPOINT throws "Statement type not supported."
  - **PostgreSQL**: All three commands silently pass through without error, but the shadow store does **not** participate in savepoints — shadow data persists regardless of savepoint rollback.
  Users requiring savepoint-like behavior should manage state in application code.

- **Unknown schema EmptyResult/Notice modes (PostgreSQL/SQLite)**: On MySQL, all four `unknownSchemaBehavior` modes (Passthrough, Exception, EmptyResult, Notice) work correctly for both UPDATE and DELETE on unreflected tables. On PostgreSQL and SQLite, only DELETE operations respect EmptyResult and Notice modes. UPDATE operations throw `RuntimeException` ("UPDATE simulation requires primary keys") regardless of the configured behavior, because the error occurs in the ShadowStore before the unknown schema behavior check is applied.

- **Doubled single-quote escaping in UPDATE/DELETE (PostgreSQL)**: On MySQL and SQLite (both adapters), SQL statements containing `''` (doubled single-quote) escaping in SET values work correctly — e.g., `UPDATE t SET body = 'it''s updated' WHERE id = 1`. On PostgreSQL, the PgSqlParser's `stripStringLiterals()` regex (`/E?'(?:[^'\\\\]|\\\\.)*'/`) does not handle `''` escaping. It treats `'it''s updated'` as two separate string literals (`'it'` and `'s updated'`), causing `mapStrippedOffsetToOriginal()` to compute an incorrect offset. The resulting WHERE clause extraction is off by one character (e.g., `WHERE id = 1` becomes `WHERE d = 1`), triggering "column does not exist" errors. Issue #25. Workaround: use prepared statements with parameter binding.

- **PostgreSQL multi-table TRUNCATE only truncates first table** (Issue #29): PostgreSQL supports `TRUNCATE table1, table2, table3` to truncate multiple tables in a single statement. However, `PgSqlParser::extractTruncateTable()` only captures the first table name from the regex, so only the first table is truncated in the shadow store — subsequent tables retain their data. Workaround: issue separate TRUNCATE statements for each table.

- **SELECT with locking clauses (FOR UPDATE, FOR SHARE) — no-op in ZTD**: Locking clauses (FOR UPDATE, FOR SHARE, LOCK IN SHARE MODE, FOR NO KEY UPDATE, FOR KEY SHARE) are preserved in the CTE-rewritten SQL and accepted by all databases. However, since the query reads from CTE-derived data (not physical table rows), **no actual row locks are acquired**. This is semantically correct for ZTD (shadow data doesn't need locking) but could be surprising for user code that relies on SELECT...FOR UPDATE for pessimistic concurrency control. The locking clause is effectively a no-op when ZTD is enabled.

- **MySQL INSERT ... SET syntax**: MySQL's alternative INSERT syntax (`INSERT INTO table SET col1 = val1, col2 = val2`) works correctly in ZTD. The `InsertTransformer::buildInsertSetSelect()` converts SET operations into a SELECT expression for CTE shadowing. Combined with ON DUPLICATE KEY UPDATE also works correctly. This is MySQL-specific — PostgreSQL and SQLite do not support this syntax.

- **INSERT...SELECT...ON DUPLICATE KEY UPDATE (MySQL)**: The combination of `INSERT...SELECT` with `ON DUPLICATE KEY UPDATE` works correctly in ZTD shadow mode for simple `VALUES(col)` references. The `InsertTransformer::buildInsertFromSelect()` wraps the inner SELECT in a subquery and the `UpsertMutation` handles conflict resolution. However, complex expressions in the UPDATE clause (e.g., `score = score + VALUES(score)`) are stored as literal strings rather than evaluated — same limitation as regular ON DUPLICATE KEY UPDATE expressions (Issue #16).

- **INSERT with DEFAULT keyword in VALUES fails under ZTD** (Issue #31): `INSERT INTO t (col) VALUES (DEFAULT)` and `INSERT INTO t DEFAULT VALUES` both fail under ZTD. The `InsertTransformer` converts VALUES clauses to SELECT expressions (e.g., `SELECT DEFAULT AS "col"`), but `DEFAULT` is only valid in INSERT VALUES context — not in SELECT. MySQL reports syntax error 1064, PostgreSQL reports "DEFAULT is not allowed in this context", SQLite reports similar errors. This affects all three platforms. Workaround: omit columns with DEFAULT from the INSERT column list, or supply the default value explicitly.

- **Prepared statement re-execution uses stale snapshot**: Prepared statement CTEs are built at `prepare()` time from the current shadow store snapshot. Re-executing the same prepared statement after shadow data changes (via `exec()` or other queries) does **NOT** reflect those changes — the CTE still contains the prepare-time snapshot. INSERT re-execution works correctly (mutations are additive and don't depend on CTE data), but SELECT/UPDATE/DELETE re-executions show stale data. To see updated shadow state, a new `prepare()` call is needed. This applies to all platforms (SQLite, MySQL, PostgreSQL) and both adapters (PDO, MySQLi).

- **ZtdPdo::query() with fetchMode arguments**: `ZtdPdo::query()` supports the optional `$fetchMode` and variadic `...$fetchModeArgs` parameters. When provided, the method calls `setFetchMode($fetchMode, ...$fetchModeArgs)` on the result statement. All standard fetch modes work correctly: FETCH_ASSOC, FETCH_NUM, FETCH_OBJ, FETCH_BOTH, FETCH_COLUMN (with column index), and FETCH_CLASS (with class name). This is consistent across all three database platforms.

- **SqlBehaviorRule ordering and matching**: Behavior rules in `ZtdConfig` are evaluated in order — the first matching rule determines the behavior for that SQL statement. Prefix matching is case-insensitive (normalizes SQL to uppercase). Regex patterns (starting with `/`) use `preg_match()`. When no rule matches, the default `unsupportedBehavior` applies. Rule ordering is critical: placing a broad prefix rule (e.g., `'SET'`) before a specific prefix rule (e.g., `'SET @special'`) will prevent the specific rule from ever matching.

- **PostgreSQL RETURNING clause not supported**: PostgreSQL's `RETURNING` clause on DML statements (`INSERT ... RETURNING`, `UPDATE ... RETURNING`, `DELETE ... RETURNING`, `INSERT ... ON CONFLICT ... RETURNING`) is not supported under ZTD. The CTE rewriter does not preserve the RETURNING clause, causing all such statements to fail. This affects all DML types. Workaround: execute the DML operation first, then run a separate SELECT to read the affected data.

- **TRUNCATE TABLE + re-insert workflow (MySQL)**: On MySQL, `TRUNCATE TABLE t` followed by new INSERTs works correctly in ZTD shadow mode. The `TruncateMutation` clears the shadow store, and subsequent inserts populate it cleanly. Multiple truncate-reinsert cycles work correctly, including re-using the same primary key values.

- **DELETE FROM table without WHERE is a no-op on shadow store (SQLite)**: On SQLite, `DELETE FROM table` without a WHERE clause does **not** delete rows from the shadow store (see also Section 4.3 Limitation). The CTE rewriter appears to require a WHERE clause to identify deletable rows. The statement returns 0 affected rows and the shadow data remains intact. Workaround: use `DELETE FROM table WHERE 1=1` which correctly matches and deletes all shadow rows.

- **stmt_init() bypasses ZTD (MySQLi)**: `ZtdMysqli::stmt_init()` returns a raw `mysqli_stmt` (NOT a `ZtdMysqliStatement`), meaning all queries prepared via `stmt_init()` bypass ZTD entirely and operate directly on the physical database. INSERT via `stmt_init()` writes to the physical DB, and the data is invisible through ZTD-enabled SELECT. SELECT via `stmt_init()` reads from the physical DB, not the shadow store. Users should always use `ZtdMysqli::prepare()` instead of `stmt_init()` when ZTD behavior is desired. Verified on MySQLi.

- **INSERT with SQL expressions in VALUES**: SQL expressions in VALUES clauses (arithmetic `40 + 50`, functions `UPPER('alice')`, `COALESCE(NULL, 'Fallback')`, `CASE WHEN`, concatenation `||` / `CONCAT()`, `ABS()`, `LENGTH()`, negative numbers, `GREATEST()`, `IF()`) are correctly handled by the InsertTransformer. The expressions are evaluated during the CTE SELECT and the computed values are stored in the shadow store. Verified on all 3 PDO platforms.

- **Self-referencing INSERT (INSERT INTO t SELECT FROM t)**: `INSERT INTO t ... SELECT ... FROM t` (where source and target are the same table) works correctly — the SELECT snapshot is taken before the INSERT starts, preventing infinite loops. On MySQL, both direct column references and computed expressions (e.g., `id + 100`) transfer correctly. On SQLite and PostgreSQL, computed expressions in SELECT become NULL (known limitation, see 4.1a), but direct column references transfer correctly. Self-referencing INSERT correctly reflects post-mutation state (UPDATE/DELETE before the INSERT). Verified on all 4 adapters.

- **lastAffectedRows() accuracy edge cases (MySQLi)**: `lastAffectedRows()` returns correct counts for all operation types: single INSERT (1), multi-row INSERT (N), single UPDATE (1), multi-row UPDATE (N), zero-match UPDATE (0), DELETE (N), zero-match DELETE (0). Sequential operations return independent counts — each operation resets the counter. Prepared statements expose counts via `ztdAffectedRows()`. Verified on MySQLi.

- **lastInsertId() / insert_id returns 0 after shadow INSERT**: Since ZTD rewrites INSERT into CTE-based SELECT queries, no physical INSERT occurs and the database's auto-increment counter is NOT updated. On **PDO** (all platforms), `lastInsertId()` returns `"0"` after shadow INSERT. On **PostgreSQL PDO**, calling `lastInsertId()` with a sequence name throws `PDOException` ("currval of sequence is not yet defined in this session") because the sequence was never advanced; calling without a sequence name also throws ("lastval is not yet defined"). On **MySQLi**, `$mysqli->insert_id` throws `Error` ("Property access is not allowed yet") because ZtdMysqli doesn't call the parent constructor, and there is NO `lastInsertId()` method available — making auto-increment ID retrieval impossible. **ZtdMysqliStatement** delegates `$stmt->insert_id` to the inner stmt, which returns 0 but may throw if the statement is already closed by ZTD. When ZTD is disabled, `lastInsertId()` / `insert_id` works correctly. This is a significant user pitfall: auto-increment ID workflows require either explicit IDs or disabling ZTD.

- **select_db() interaction with ZTD (MySQLi)**: `ZtdMysqli::select_db()` delegates directly to the inner `mysqli`, changing the physical connection's active database. The ZTD session's schema reflection and shadow store are NOT updated. Shadow data from the original database persists after `select_db()`. CTE-rewritten queries may still resolve because the shadow data is embedded in the CTE regardless of the active database. `select_db()` with a non-existent database throws `mysqli_sql_exception`. Verified on MySQLi.

- **execute_query() with INSERT...SELECT (MySQLi)**: `execute_query()` (PHP 8.2+) supports INSERT...SELECT patterns including cross-table copies, filtered copies with WHERE, parameterized WHERE clauses, and self-referencing INSERT (INSERT INTO t SELECT FROM t). execute_query() internally uses `prepare()` + `execute()`, so it benefits from the same CTE rewriting as the prepare path. Verified on MySQLi.

- **PostgreSQL array types (INTEGER[], TEXT[]) broken in shadow store**: INSERT with array literal values (`'{1,2,3}'`) succeeds, but any subsequent SELECT on the table fails because the CTE rewriter generates `CAST('{1,2,3}' AS INTEGER)` instead of `CAST('{1,2,3}' AS INTEGER[])`. The schema reflector correctly maps array types (`_int4` → `INT4[]`) but the `CastRenderer` emits the base type without the array suffix. TEXT[] columns are unaffected because `CAST('...' AS TEXT)` is compatible. NULL array values also work correctly. The `ARRAY[1,2,3]` constructor syntax is misinterpreted by the InsertTransformer (comma-separated values counted as multiple columns), causing "column count does not match" errors. Since the CTE builds shadow data with ALL table columns, even SELECTing non-array columns fails if the table has any INTEGER[] column with data. Workaround: avoid INTEGER[]/NUMERIC[] columns when using ZTD, or disable ZTD for queries involving array-typed tables.

- **Generated columns (STORED/VIRTUAL) in shadow store**: Tables with GENERATED ALWAYS AS columns work with ZTD when INSERT omits the generated columns from the column list. The non-generated columns are stored correctly in the shadow. However, the generated column values are typically NULL in shadow queries because the CTE rewriting doesn't trigger the database's expression computation (no physical row exists). Verified on MySQL (STORED and VIRTUAL), PostgreSQL (STORED only — PostgreSQL doesn't support VIRTUAL), and SQLite (STORED and VIRTUAL). Users should not rely on generated column values when ZTD is enabled.

- **CHECK constraints not enforced in shadow store**: Since ZTD rewrites INSERT/UPDATE to CTE-based operations that never physically modify the table, database-level CHECK constraints are NOT triggered. Invalid values (violating CHECK conditions) are stored in shadow without error. This applies to all platforms. The invalid data never reaches the physical table (physical isolation is maintained). Users relying on CHECK constraints for data validation should perform validation in application code when ZTD is enabled.

- **UNION/EXCEPT/INTERSECT with LIMIT/OFFSET**: UNION ALL and UNION (distinct) work correctly with LIMIT, OFFSET, and ORDER BY in shadow queries across all platforms. UNION reflects shadow mutations (INSERT/DELETE). However, EXCEPT and INTERSECT may return 0 rows with shadow data on some platforms because independently CTE-rewritten branches may have type/format mismatches that prevent row deduplication/matching. UNION ALL (which does not deduplicate) is not affected. Verified on SQLite and PostgreSQL.

- **Correlated subqueries reflect shadow mutations**: Scalar correlated subqueries (in SELECT list), EXISTS, NOT EXISTS, and COUNT-based correlated subqueries correctly reflect shadow mutations (INSERT, UPDATE, DELETE) in the CTE-rewritten queries. Both the outer and inner query branches see consistent shadow data. Verified on SQLite (PDO), MySQL (PDO and MySQLi).

- **UPDATE with self-referencing arithmetic (SET col = col + N)**: Self-referencing UPDATE expressions (increment, decrement, multiply, cross-column references like `SET balance = balance + counter`) work correctly in shadow store. Multiple sequential self-referencing updates on the same row accumulate correctly. Self-referencing updates after prior INSERT or DELETE correctly operate on the current shadow state. Multi-column self-referencing updates in a single statement work correctly. Verified on SQLite.

- **COLLATE clause in queries**: COLLATE in WHERE clauses (e.g., `WHERE name COLLATE utf8mb4_bin = 'value'`), ORDER BY, and LIKE patterns works correctly in CTE-rewritten shadow queries on MySQL. Case-sensitive comparisons via COLLATE correctly differentiate between case variants. COLLATE after mutations also works correctly. Verified on MySQL.

- **DELETE with correlated subqueries**: `DELETE ... WHERE EXISTS (SELECT ...)`, `DELETE ... WHERE NOT EXISTS (SELECT ...)`, `DELETE ... WHERE col IN (SELECT ...)`, and `DELETE ... WHERE col > (SELECT ...)` all work correctly in shadow store. The correlated subquery correctly reflects the current shadow state. Verified on SQLite and MySQL (MySQLi).

- **User-defined CTEs (WITH ... AS) overwritten by ZTD**: When a query contains a user-defined CTE (`WITH cte_name AS (...) SELECT ... FROM cte_name`), the ZTD CTE rewriter replaces the WITH clause with its own shadow CTE. This causes user-defined CTE names to become undefined references, resulting in errors. Inline subqueries (derived tables) and direct GROUP BY/aggregation queries should be used as alternatives. This applies to all platforms (SQLite, MySQL, PostgreSQL).

- **UPDATE SET col = (subquery) — platform-specific behavior**: Non-correlated scalar subqueries in UPDATE SET clauses (e.g., `SET price = (SELECT MAX(price) FROM t)`) work correctly on **MySQL** and **SQLite** but fail on **PostgreSQL** (duplicate alias error for self-referencing, grouping error for cross-table). Correlated subqueries (referencing the outer table being updated) fail on **SQLite** (syntax error) but may succeed on **MySQL**. On **PostgreSQL**, ALL subqueries in SET clauses fail. Workaround: compute the subquery value separately, then UPDATE with a literal value. Verified on all 3 platforms.

- **Large dataset (100+ rows) in shadow store**: CTE-based shadow store handles 100+ rows correctly for all operations: INSERT, SELECT with aggregation (SUM, AVG, MIN, MAX), GROUP BY, UPDATE subset, DELETE subset, ORDER BY with LIMIT/OFFSET. No performance degradation or correctness issues observed. Verified on SQLite, MySQL (PDO and MySQLi), and PostgreSQL.

- **Window functions with prepared statements**: Window functions (ROW_NUMBER, RANK, SUM OVER, PARTITION BY) work correctly when combined with prepared statements and parameter binding. Multiple window functions in a single query work. Window function results correctly reflect shadow mutations (INSERT). Verified on all 3 PDO platforms (SQLite, MySQL, PostgreSQL).

- **Transaction interaction with shadow store**: `beginTransaction()`/`begin_transaction()`, `commit()`, and `rollBack()`/`rollback()` work correctly when combined with ZTD shadow operations. Shadow INSERT within a transaction is visible after commit. `inTransaction()` returns correct state throughout the transaction lifecycle. Multiple operations within a single transaction (INSERT, UPDATE) work correctly. After `commit()`, physical isolation is maintained — `disableZtd()` shows 0 physical rows. After `rollBack()`, shadow data may or may not persist (implementation-dependent). Verified on SQLite, MySQL (PDO and MySQLi), and PostgreSQL.

- **PDO fetch modes with shadow data**: All standard PDO fetch modes work correctly with CTE-rewritten shadow queries: `FETCH_OBJ` (stdClass), `FETCH_NUM` (numeric array), `FETCH_BOTH` (both keys), `FETCH_COLUMN` (single column), `FETCH_KEY_PAIR` (key-value pairs), and `fetchObject()`. MySQLi fetch methods (`fetch_assoc`, `fetch_row`, `fetch_object`, `fetch_all` with MYSQLI_ASSOC/MYSQLI_NUM) also work correctly. Fetch modes work after shadow mutations. Verified on SQLite, MySQL (PDO and MySQLi), and PostgreSQL.

- **Named parameter binding (:param) with CTE rewriting**: PDO named parameters (`:param` syntax) work correctly with ZTD CTE rewriting in SELECT, INSERT, UPDATE, and DELETE statements. All three binding methods work: `execute([':name' => value])`, `bindValue(':name', value, type)`, and `bindParam(':name', $var, type)`. Multiple named parameters in a single query work correctly. Verified on SQLite.

- **Multiple ZtdPdo instances with independent shadow stores**: Each ZtdPdo instance maintains its own independent shadow store. Mutations (INSERT, UPDATE, DELETE) in one instance are NOT visible to another instance, even when both wrap connections to the same database schema. Disabling ZTD on one instance does not affect the other. Verified on SQLite.

- **ENUM column type in shadow store (MySQL)**: MySQL ENUM columns (`ENUM('active', 'inactive', 'pending')`) work correctly in ZTD shadow store. INSERT with valid ENUM values, UPDATE to different ENUM values, WHERE comparison against ENUM values, and NULL ENUM values all work correctly. Verified on MySQLi.
