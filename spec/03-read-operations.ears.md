# 3. Read Operations

## SPEC-3.1 SELECT
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tested versions:** ztd-query-mysqli-adapter v0.1.1, ztd-query-pdo-adapter v0.1.1, MySQL 8.0, PostgreSQL 16, SQLite 3.x, PHP 8.3
**Tests:** `Scenarios/BasicCrudScenario::testInsertAndSelect` (all platforms), `Scenarios/BasicCrudScenario::testSelectReturnsEmptyWhenNoRows`

When ZTD is enabled and a SELECT query is executed, the system shall return results from the shadow store only. Physical table data is not included; the CTE replaces the table reference with shadow store contents.

When ZTD is enabled and the result set is empty, the system shall return an empty result set (not false or null).

**Verified behavior:** Aggregates on empty sets return correct values (COUNT → 0, SUM/AVG/MIN/MAX → NULL). Zero-match UPDATE/DELETE returns 0 affected rows without error. Cursor-based (keyset) pagination (WHERE id > ? ORDER BY id LIMIT N) works correctly — page traversal, mid-dataset inserts/deletes, and descending cursor all return expected results. Offset-based pagination (LIMIT/OFFSET) works correctly across all platforms — page traversal, partial last page, empty result beyond data range, pagination after INSERT/DELETE all return expected results. MySQL PDO requires PARAM_INT binding for LIMIT/OFFSET prepared parameters (see [SPEC-10.2.17](10-platform-notes.ears.md)). DECIMAL precision is preserved through shadow store for financial calculations (SUM, arithmetic in UPDATE SET, comparison in WHERE). IS NULL / IS NOT NULL filtering on nullable columns (soft delete pattern) works correctly. Type roundtrip through shadow store preserves integers (including INT_MIN/INT_MAX), floats, decimals, strings (including empty strings and single quotes), NULLs, and booleans (MySQL/SQLite; PostgreSQL false has known issue [SPEC-11.PG-BOOLEAN-FALSE](11-known-issues.ears.md)). Prepared IN-list queries (WHERE id IN (?, ?, ?)) work correctly across all platforms for both positional and integer/string parameters. Dual-query pagination pattern (paginated data + total count in same session) works correctly — total count reflects INSERT/DELETE mutations, filtered counts work, keyset and offset pagination both work alongside COUNT queries (see [SPEC-10.2.52](10-platform-notes.ears.md)). Multi-column ORDER BY with expressions, CASE-based priority sorting, and NULL ordering all work correctly (see [SPEC-10.2.54](10-platform-notes.ears.md)).

## SPEC-3.2 Prepared SELECT
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tested versions:** ztd-query-mysqli-adapter v0.1.1, ztd-query-pdo-adapter v0.1.1, MySQL 8.0, PostgreSQL 16, SQLite 3.x, PHP 8.3
**Tests:** `Scenarios/BasicCrudScenario::testPreparedSelectWithBindValue` (all platforms), `Scenarios/PreparedStatementScenario` (all platforms), `Mysqli/PreparedStatementTest`, `Pdo/MysqlPreparedStatementTest`, `Pdo/PostgresPreparedStatementTest`, `Pdo/SqlitePreparedStatementTest`, `Pdo/PreparedStatementTest`

When a prepared SELECT statement with bound parameters is executed, the system shall rewrite the query and return correct results.

Prepared statements support the following binding methods:
- **PDO**: `bindValue()` for value binding, `bindParam()` for by-reference binding, and `execute($params)` with positional or named parameter arrays.
- **MySQLi**: `bind_param()` with type string and by-reference variables, `execute()` for execution, `execute_query()` (PHP 8.2+) as a shortcut for SELECT/INSERT/UPDATE/DELETE, and `bind_result()` + `fetch()` for bound-variable result retrieval. **Note:** `execute_query()` correctly handles UPDATE and DELETE with parameters, but has limitations with UPSERT and REPLACE (see [SPEC-4.2a](04-write-operations.ears.md), [SPEC-4.2b](04-write-operations.ears.md)).

Query rewriting occurs at **prepare time**, not execute time. If ZTD mode is toggled between `prepare()` and `execute()`, the prepared query retains its original rewritten form.

**Data snapshotting**: The CTE VALUES clause (shadow store data) is also captured at prepare time. A prepared SELECT will NOT see data inserted after it was prepared — even if the INSERT occurs before `execute()`. To see post-INSERT data, the SELECT must be prepared (or re-prepared) after the INSERT. Similarly, re-executing a prepared DELETE or UPDATE operates on the frozen CTE snapshot — previously deleted rows are still "visible" in the snapshot, so `rowCount()` / `ztdAffectedRows()` may report affected-row counts for rows that were already removed by a prior execution of the same statement.

**Verified behavior:** Prepare-time rewriting persistence confirmed. Named parameter binding (`:param` syntax) works. Interleaved prepared statements maintain independent snapshots. ORM-style statement reuse (prepare-once/execute-many) works. Parameterized LIMIT/OFFSET works (MySQL requires `PARAM_INT`). Dynamic WHERE clause building patterns (WHERE 1=1 AND optional filters, varying parameter counts across separate queries) work correctly — common PHP query builder idiom (see [SPEC-10.2.51](10-platform-notes.ears.md)).

## SPEC-3.3 Complex Queries
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tested versions:** ztd-query-mysqli-adapter v0.1.1, ztd-query-pdo-adapter v0.1.1, MySQL 8.0, PostgreSQL 16, SQLite 3.x, PHP 8.3
**Tests:** `Scenarios/JoinAndSubqueryScenario` (all platforms), `Mysqli/ComplexQueryTest`, `Mysqli/AdvancedQueryPatternsTest`, `Pdo/MysqlComplexQueryTest`, `Pdo/PostgresComplexQueryTest`, `Pdo/SqliteComplexQueryTest`, `Pdo/ComplexQueryTest`, `Pdo/AdvancedQueryPatternsTest`

When ZTD is enabled, the CTE rewriting shall correctly handle:
- **JOINs** (INNER JOIN, LEFT JOIN, RIGHT JOIN, FULL OUTER JOIN on PostgreSQL) across multiple shadow tables.
- **Self-JOINs** where the same shadow table is referenced with different aliases.
- **Aggregations** (COUNT, SUM, MIN, MAX) with GROUP BY and HAVING clauses.
- **Subqueries** in WHERE clauses (e.g., `WHERE id IN (SELECT ...)`).
- **Correlated subqueries** in SELECT list (e.g., `(SELECT COUNT(*) FROM t WHERE t.fk = u.id)`).
- **UNION** queries combining results from shadow tables.
- **EXCEPT** / **INTERSECT** set operations (SQLite and PostgreSQL only; see [SPEC-3.3d](#spec-33d-set-operations-except--intersect)).
- **ORDER BY** with LIMIT and OFFSET.
- **DISTINCT** selection.
- **CASE** expressions (in SELECT and UPDATE SET clauses).
- **LIKE** with wildcard patterns (including ESCAPE clause).
- **IN** / **NOT IN** with literal value lists and subqueries.
- **BETWEEN** range conditions.
- **EXISTS** / **NOT EXISTS** correlated subqueries.
- **COALESCE** and other SQL functions.
- **String functions** (UPPER, LOWER, LENGTH, SUBSTR/SUBSTRING, REPLACE, TRIM, GROUP_CONCAT/STRING_AGG, CONCAT).
- **Math functions** (ABS, ROUND, CEIL, FLOOR).
- **JOIN ... USING** syntax (alternative to ON).
- **Multiple user CTEs** (`WITH cte1 AS (...), cte2 AS (...) SELECT ...`).
- **Window functions** (ROW_NUMBER, SUM OVER PARTITION BY, AVG/SUM with ROWS/RANGE BETWEEN frames, LAG, LEAD, RANK, DENSE_RANK, NTILE, FIRST_VALUE, LAST_VALUE).
- **CROSS JOIN** and implicit cross join (comma-separated FROM).
- **NATURAL JOIN** (SQLite).
- **CASE WHEN in ORDER BY** for custom sort priority.
- **Expression-based GROUP BY** (GROUP BY CASE, GROUP BY LENGTH, GROUP BY SUBSTR).
- **Conditional aggregation** (SUM(CASE WHEN ...)).
- **COUNT DISTINCT**.
- **Multi-column ORDER BY** and ORDER BY expression.

UPDATE and DELETE statements with subqueries referencing other shadow tables shall also be correctly rewritten.

User-written CTE (WITH) queries shall work correctly alongside ZTD's internal CTE shadowing on MySQL and SQLite. On PostgreSQL, table references inside user CTEs are not rewritten, causing the inner CTE to read from the physical table rather than the shadow store (see [SPEC-11.PG-CTE](11-known-issues.ears.md)).

**Verified behavior:** Prepared statements with complex queries work (JOINs, IN/NOT IN, CASE WHEN, aggregation GROUP BY, subqueries). Advanced subquery patterns (3-level nesting, scalar subqueries, combined AND/OR/IN). 4-table and 5-table JOINs work. Platform-specific functions (MySQL: IF, IFNULL, FIND_IN_SET, CONCAT_WS, REVERSE, LPAD, GROUP_CONCAT ORDER BY; PostgreSQL: ILIKE, `::` casting, `||` concat, POSITION, FILTER clause, STRING_AGG ORDER BY, GREATEST/LEAST, DISTINCT ON; SQLite: typeof, INSTR, IIF, printf, HEX, NULLIF, CAST, GLOB).

## SPEC-3.3f Full-Text Search
**Status:** Known Issue
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tested versions:** ztd-query-mysqli-adapter v0.1.1, ztd-query-pdo-adapter v0.1.1, MySQL 8.0, PostgreSQL 16, SQLite 3.x, PHP 8.5
**Tests:** `Mysqli/FullTextSearchTest`, `Pdo/MysqlFullTextSearchTest`, `Pdo/PostgresFullTextSearchTest`, `Pdo/SqliteFullTextSearchTest`

Full-text search is NOT supported through ZTD CTE rewriting on any platform. Each platform fails for a different reason:

- **MySQL (MySQLi and PDO)**: `MATCH(col1, col2) AGAINST('terms')` throws `General error: 1214 The used table type doesn't support FULLTEXT indexes`. The CTE-derived temporary table does not carry the FULLTEXT index from the physical table, so the MATCH expression cannot execute.
- **PostgreSQL**: `to_tsvector()` / `@@` / `to_tsquery()` queries throw errors because the tsvector column type metadata is not correctly reproduced in the CTE cast. See [SPEC-11.FULLTEXT](11-known-issues.ears.md).
- **SQLite**: FTS5 virtual tables fail with `no such column: table_name` because the CTE rewriter does not recognize FTS5 virtual table references. FTS5 tables cannot be wrapped in CTEs.

All platforms: physical isolation is confirmed (shadow INSERTs do not reach the physical table). Shadow operations (regular CRUD) continue to work correctly after a full-text search attempt (whether it throws or is caught).

Workaround: disable ZTD for full-text search queries, or use LIKE-based pattern matching through ZTD instead.

## SPEC-3.3g User-Defined Functions in Queries
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO
**Tested versions:** ztd-query-mysqli-adapter v0.1.1, ztd-query-pdo-adapter v0.1.1, MySQL 8.0, PostgreSQL 16, PHP 8.5
**Tests:** `Mysqli/StoredProcedureTest`, `Pdo/MysqlStoredProcedureTest`, `Pdo/PostgresStoredFunctionTest`

User-defined functions (MySQL stored functions, PostgreSQL PL/pgSQL functions) called within SELECT, WHERE, and ORDER BY clauses work through the CTE-rewritten shadow queries. The function call is a scalar expression evaluated by the database engine on the CTE-derived data.

**Verified behavior:** Function in SELECT expression returns correct computed values. Function in WHERE clause correctly filters shadow data. Function in ORDER BY clause correctly sorts shadow data. Multiple functions in the same query work. Prepared statements with stored function calls work on MySQL (MySQLi and PDO).

**Limitation:** PostgreSQL prepared statements with user-defined functions in WHERE clauses may return incorrect results (empty result set) despite the same query working via `query()`. See [SPEC-11.PG-PREPARED-FUNCTION](11-known-issues.ears.md).

If a user-defined function internally reads from a table that is shadow-stored, the function reads from the physical table (empty), not the shadow store. This is a fundamental limitation: the CTE rewriter only rewrites table references in the outer query, not inside function bodies.

## SPEC-3.3e CTE-based DML (WITH ... INSERT/UPDATE/DELETE)
**Status:** Known Issue
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tested versions:** ztd-query-mysqli-adapter v0.1.1, ztd-query-pdo-adapter v0.1.1, MySQL 8.0, PostgreSQL 16, SQLite 3.x, PHP 8.3
**Tests:** `Mysqli/CteDmlTest`, `Pdo/MysqlCteDmlTest`, `Pdo/PostgresCteDmlTest`, `Pdo/SqliteCteDmlTest`

`WITH ... INSERT INTO ... SELECT`, `WITH ... UPDATE`, and `WITH ... DELETE` (CTE-based DML) are NOT supported on any platform:

- **MySQL (MySQLi and PDO)**: The `MySqlQueryGuard::classifyWithFallback()` correctly classifies CTE DML as `WRITE_SIMULATED`, but the mutation resolver receives a `WithStatement` (not an `InsertStatement`/`UpdateStatement`/`DeleteStatement`) and throws `RuntimeException` ("Missing shadow mutation for write simulation").
- **SQLite**: The CTE rewriter prepends shadow CTEs, which causes the user's CTE name to become invisible in the DML part of the statement. This produces "no such table: cte_name" errors.
- **PostgreSQL**: The CTE rewriter produces invalid SQL — it does not properly handle user CTEs combined with DML statements. Errors include syntax errors and "relation does not exist".

The shadow store is not corrupted by CTE DML failures — previously inserted data remains intact after the error. Users needing CTE-based DML should either disable ZTD for those queries or rewrite the query as a standard DML with subqueries.

## SPEC-3.3a Derived Tables (Subqueries in FROM)
**Status:** Known Issue
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tested versions:** ztd-query-mysqli-adapter v0.1.1, ztd-query-pdo-adapter v0.1.1, MySQL 8.0, PostgreSQL 16, SQLite 3.x, PHP 8.3
**Tests:** `Mysqli/DerivedTableAndViewTest`, `Pdo/MysqlDerivedTableAndViewTest`, `Pdo/PostgresDerivedTableAndViewTest`, `Pdo/SqliteDerivedTableAndViewTest`

Derived tables (subqueries in the FROM clause) are NOT fully supported by the CTE rewriter. Table references inside derived subqueries are generally not rewritten:

- **MySQL**: Derived tables always return empty results because inner table references read from the physical database. This applies both when the derived table is the sole FROM source and when it is JOINed with a regular table.
- **PostgreSQL**: Derived tables as sole FROM source work correctly — table references inside the subquery ARE rewritten. This includes `SELECT ... FROM (SELECT ... ROW_NUMBER() OVER (...) FROM table) sub WHERE ...` patterns (e.g., deduplication with ROW_NUMBER). JOINed derived tables also work.
- **SQLite**: Derived tables as sole FROM source return empty. However, when a derived table is JOINed with a regular table, table references inside the derived subquery ARE rewritten and return shadow data correctly. Mutations in the shadow store are reflected through derived table JOINs on SQLite.

## SPEC-3.3b Views
**Status:** Known Issue (By-Design)
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tested versions:** ztd-query-mysqli-adapter v0.1.1, ztd-query-pdo-adapter v0.1.1, MySQL 8.0, PostgreSQL 16, SQLite 3.x, PHP 8.3
**Tests:** `Mysqli/ViewThroughZtdTest`, `Pdo/MysqlViewThroughZtdTest`, `Pdo/PostgresViewThroughZtdTest`, `Pdo/SqliteViewThroughZtdTest`

Database views are NOT rewritten by the CTE rewriter. Querying a view through ZTD returns empty results because the view's underlying query reads from physical tables, not the shadow store. This applies to all platforms. Users should query base tables directly for shadow data visibility.

## SPEC-3.3c Recursive CTEs
**Status:** Known Issue
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tested versions:** ztd-query-mysqli-adapter v0.1.1, ztd-query-pdo-adapter v0.1.1, MySQL 8.0, PostgreSQL 16, SQLite 3.x, PHP 8.3
**Tests:** `Mysqli/RecursiveCteAndRightJoinTest`, `Pdo/MysqlRecursiveCteAndRightJoinTest`, `Pdo/PostgresRecursiveCteAndRightJoinTest`, `Pdo/SqliteRecursiveCteAndRightJoinTest`

`WITH RECURSIVE` queries that do NOT reference shadow tables (e.g., number series generation) work correctly on all platforms.

`WITH RECURSIVE` queries that reference shadow tables are NOT supported:
- **MySQL**: The CTE rewriter prepends its own `WITH` clause before the `RECURSIVE` keyword, producing invalid SQL (syntax error). This applies to both MySQLi and PDO adapters.
- **SQLite**: The query executes but returns empty results — table references inside the recursive CTE are not rewritten, so the query reads from the physical table (empty).
- **PostgreSQL**: Same behavior as non-recursive user CTEs — returns empty results because table references inside CTEs are not rewritten.

## SPEC-3.3d Set Operations (EXCEPT / INTERSECT)
**Status:** Partially Verified
**Platforms:** SQLite-PDO, PostgreSQL-PDO (verified); MySQLi, MySQL-PDO (not supported)
**Tested versions:** ztd-query-pdo-adapter v0.1.1, PostgreSQL 16, SQLite 3.x, PHP 8.3
**Tests:** `Pdo/SqliteSetOperationsAndFunctionsTest`, `Pdo/PostgresSetOperationsAndFunctionsTest`, `Mysqli/ExceptIntersectTest`, `Pdo/MysqlExceptIntersectTest`

`EXCEPT` and `INTERSECT` set operations work correctly on **SQLite** and **PostgreSQL** — table references in both sides are rewritten to read from the shadow store.

On **MySQL** (both MySQLi and PDO adapters), `EXCEPT` and `INTERSECT` throw `UnsupportedSqlException` ("Multi-statement SQL statement"). The MySQL CTE rewriter incorrectly parses these as multi-statement SQL. UNION works correctly on all platforms.

**Verified behavior:** UNION/EXCEPT/INTERSECT correctly reflect shadow store mutations (INSERT, UPDATE, DELETE). UNION with prepared statements and aggregation in UNION branches works.

## SPEC-3.5 JSON / JSONB Functions
**Status:** Partially Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/JsonFunctionsTest`, `Mysqli/JsonAndCrossJoinTest`, `Pdo/MysqlJsonFunctionsTest`, `Pdo/MysqlJsonAndCrossJoinTest`, `Pdo/PostgresJsonbFunctionsTest`, `Pdo/PostgresJsonbOperatorsTest`, `Pdo/SqliteJsonFunctionsTest`, `Pdo/SqliteJsonAndCrossJoinTest`

When ZTD is enabled, JSON/JSONB functions and operators shall work correctly on CTE-rewritten shadow data. JSON values stored via INSERT are preserved in the shadow store and queryable through platform-specific JSON functions.

Platform-specific support:

- **MySQL (MySQLi and PDO)**: `JSON_EXTRACT()`, `JSON_UNQUOTE()`, `->>` arrow operator, `JSON_CONTAINS()`, `JSON_LENGTH()`, `JSON_SET()` in UPDATE. JSON column type (`JSON`) is supported. JSON functions work in SELECT, WHERE, and ORDER BY clauses.
- **PostgreSQL**: JSONB operators (`->`, `->>`, `@>` containment, `#>` / `#>>` path extraction), `jsonb_extract_path_text()`, `jsonb_agg()`, `jsonb_object_agg()`, `jsonb_set()` in UPDATE, `||` merge operator. JSONB value comparison in WHERE and GROUP BY works. `COALESCE` with JSONB extraction works.
- **SQLite**: `json_extract()`, `->` / `->>` operators (3.38.0+), `json_type()`, `json_array_length()`, `json_group_array()`, `json_set()` in UPDATE. JSON functions work in SELECT, WHERE, and ORDER BY clauses. Prepared statements with `json_extract()` in WHERE work.

**Limitation (PostgreSQL prepared statements):** Prepared statements with JSONB operators (`->>`, `@>`) in WHERE clauses may return empty results through the CTE rewriter, similar to [SPEC-11.PG-PREPARED-FUNCTION](11-known-issues.ears.md). The `?` key-existence operator may conflict with prepared statement parameter placeholders.

## SPEC-3.6 Composite Primary Keys
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/CompositePrimaryKeyTest`, `Pdo/MysqlCompositePrimaryKeyTest`, `Pdo/PostgresCompositePrimaryKeyTest`, `Pdo/SqliteCompositePrimaryKeyTest`, `Mysqli/CompositePkUpsertTest`, `Pdo/PostgresCompositePkUpsertTest`, `*CompositePkEdgeCasesTest`

When ZTD is enabled, tables with composite (multi-column) primary keys shall support INSERT, UPDATE, DELETE, and SELECT correctly through the shadow store.

- INSERT with composite PK values stores rows correctly.
- UPDATE with full composite PK in WHERE updates only the matching row.
- UPDATE with partial composite PK match updates all matching rows.
- DELETE with composite PK in WHERE deletes only the matching row.
- Three-column composite PKs work correctly.
- Prepared statements with composite PK parameters work.
- Aggregation queries (GROUP BY, SUM, COUNT) on composite PK tables work.

**Verified behavior (extended):** UPDATE/DELETE with subquery WHERE on composite PK tables work (e.g., `WHERE order_id IN (SELECT ... FROM other_table WHERE ...)`). Cross-table JOINs between composite PK table and another table work. Aggregate across JOIN with composite PK table works. Prepared multi-execute with composite PK parameters works. DELETE then re-INSERT at same composite PK works. Correlated subqueries referencing composite PK tables work. Self-referencing arithmetic UPDATE on partial PK match works.

## SPEC-3.7 NULL Handling
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/NullHandlingEdgeCasesTest`, `Pdo/MysqlNullHandlingEdgeCasesTest`, `Pdo/PostgresNullHandlingEdgeCasesTest`, `Pdo/SqliteNullHandlingEdgeCasesTest`, `*NullInAggregatesTest`

When ZTD is enabled, NULL values shall be correctly handled through the shadow store:

- UPDATE SET column = NULL correctly stores NULL in the shadow store.
- IS NULL / IS NOT NULL filters correctly reflect mutations (UPDATE to NULL, UPDATE from NULL).
- COALESCE chains with multiple nullable columns work correctly.
- CASE expressions with IS NULL / IS NOT NULL conditions return correct results.
- Prepared statement INSERT with NULL-bound parameters stores NULL correctly.
- Platform-specific NULL functions (MySQL: IFNULL; PostgreSQL: COALESCE; SQLite: IFNULL, COALESCE) work with shadow data.

**Verified behavior (aggregates):** COUNT(*) counts all rows including NULLs; COUNT(column) excludes NULLs. COUNT(DISTINCT column) excludes NULLs. SUM/AVG of all-NULL groups returns NULL. MIN/MAX of all-NULL sets returns NULL. HAVING with COUNT(column) correctly filters groups with no non-NULL values. HAVING SUM(...) IS NOT NULL works. COALESCE inside aggregate (SUM(COALESCE(col, 0))) works. GROUP_CONCAT / STRING_AGG omits NULLs (returns NULL for all-NULL groups). NULL in arithmetic produces NULL (e.g., NULL + 10 = NULL). NULL excluded from BETWEEN. NULL = NULL is never true (standard SQL). NULL in CASE with conditional aggregation works correctly.

## SPEC-3.4 Fetch Methods
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tested versions:** ztd-query-mysqli-adapter v0.1.1, ztd-query-pdo-adapter v0.1.1, MySQL 8.0, PostgreSQL 16, SQLite 3.x, PHP 8.3
**Tests:** `Mysqli/FetchMethodsTest`, `Mysqli/FetchModesTest`, `Pdo/MysqlFetchModeTest`, `Pdo/MysqlFetchModesTest`, `Pdo/MysqlFetchModeAdvancedTest`, `Pdo/PostgresFetchModeTest`, `Pdo/PostgresFetchModesTest`, `Pdo/SqliteFetchModeTest`, `Pdo/SqliteFetchModesTest`

### PDO Adapter
When ZTD is enabled, the following PDO fetch methods shall return correct results from the shadow store:
- `fetchAll()` with `FETCH_ASSOC`, `FETCH_NUM`, `FETCH_BOTH`, `FETCH_OBJ`, `FETCH_COLUMN`, `FETCH_CLASS`, `FETCH_FUNC` modes.
- `fetchAll()` with combined modes: `FETCH_GROUP|FETCH_ASSOC`, `FETCH_UNIQUE|FETCH_ASSOC`, `FETCH_GROUP|FETCH_COLUMN`, `FETCH_KEY_PAIR`.
- `fetchAll(PDO::FETCH_FUNC, callable)` invokes the callback for each row and returns transformed results.
- `fetch()` for row-by-row iteration (returns `false` when no more rows).
- `fetch(PDO::FETCH_BOUND)` with `bindColumn()` populates bound variables from shadow store data.
- `fetchColumn()` for retrieving a single column value (supports column index argument).
- `fetchObject()` for retrieving rows as `stdClass` objects.
- `fetch(PDO::FETCH_LAZY)` for lazy row objects with property access to column values.
- `setFetchMode(PDO::FETCH_CLASS, ClassName)` and `setFetchMode(PDO::FETCH_INTO, $obj)` — see [SPEC-4.10](04-write-operations.ears.md).
- `columnCount()` shall return the correct number of columns in the result set.
- `getColumnMeta($index)` returns column metadata (name, table, etc.) from the shadow store result set.
- `getIterator()` / `foreach` iteration over a `ZtdPdoStatement` shall yield rows correctly.
- `query($sql, $mode, $arg)` accepts a fetch mode as second argument (e.g., `query($sql, PDO::FETCH_COLUMN, 0)`).

**PDO Attribute Interactions:**
- `ATTR_DEFAULT_FETCH_MODE`: Connection-level default fetch mode is respected by ZTD statements. `setFetchMode()` on a statement overrides the connection-level default.
- `ATTR_EMULATE_PREPARES`: Both `true` and `false` work correctly with ZTD shadow store on all platforms.
- `ATTR_STRINGIFY_FETCHES`: Affects type coercion of values returned from shadow store queries. When `true`, numeric values are returned as strings.
- `ATTR_CASE`: Column name case is preserved through CTE rewriting (`CASE_NATURAL`).

### MySQLi Adapter
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

**Verified behavior:** nextRowset() delegates to underlying PDO driver. MySQL returns false. SQLite and PostgreSQL throw PDOException. debugDumpParams() outputs rewritten SQL. FETCH_CLASS/FETCH_INTO work. bindColumn with type hints works.
