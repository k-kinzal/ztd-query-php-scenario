# 3. Read Operations

## SPEC-3.1 SELECT
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tested versions:** ztd-query-mysqli-adapter v0.1.1, ztd-query-pdo-adapter v0.1.1, MySQL 8.0, PostgreSQL 16, SQLite 3.x, PHP 8.3
**Tests:** `Scenarios/BasicCrudScenario::testInsertAndSelect` (all platforms), `Scenarios/BasicCrudScenario::testSelectReturnsEmptyWhenNoRows`

When ZTD is enabled and a SELECT query is executed, the system shall return results from the shadow store only. Physical table data is not included; the CTE replaces the table reference with shadow store contents.

When ZTD is enabled and the result set is empty, the system shall return an empty result set (not false or null).

**Verified behavior:** Aggregates on empty sets return correct values (COUNT → 0, SUM/AVG/MIN/MAX → NULL). Zero-match UPDATE/DELETE returns 0 affected rows without error. Cursor-based (keyset) pagination (WHERE id > ? ORDER BY id LIMIT N) works correctly — page traversal, mid-dataset inserts/deletes, and descending cursor all return expected results. Offset-based pagination (LIMIT/OFFSET) works correctly across all platforms — page traversal, partial last page, empty result beyond data range, pagination after INSERT/DELETE all return expected results. MySQL PDO requires PARAM_INT binding for LIMIT/OFFSET prepared parameters (see [SPEC-10.2.17](10-platform-notes.ears.md)). DECIMAL precision is preserved through shadow store for financial calculations (SUM, arithmetic in UPDATE SET, comparison in WHERE). IS NULL / IS NOT NULL filtering on nullable columns (soft delete pattern) works correctly. Type roundtrip through shadow store preserves integers (including INT_MIN/INT_MAX), floats, decimals, strings (including empty strings and single quotes), NULLs, and booleans (MySQL/SQLite; PostgreSQL false has known issue [SPEC-11.PG-BOOLEAN-FALSE](11-known-issues.ears.md)). Prepared IN-list queries (WHERE id IN (?, ?, ?)) work correctly across all platforms for both positional and integer/string parameters.

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

**Verified behavior:** Prepare-time rewriting persistence confirmed. Named parameter binding (`:param` syntax) works. Interleaved prepared statements maintain independent snapshots. ORM-style statement reuse (prepare-once/execute-many) works. Parameterized LIMIT/OFFSET works (MySQL requires `PARAM_INT`).

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
**Status:** Pending Verification
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/FullTextSearchTest`, `Pdo/MysqlFullTextSearchTest`, `Pdo/PostgresFullTextSearchTest`, `Pdo/SqliteFullTextSearchTest`

Full-text search is a common real-world pattern for search features. Each platform uses different syntax:

- **MySQL**: `MATCH(col1, col2) AGAINST('terms')` with NATURAL LANGUAGE MODE and BOOLEAN MODE. Requires FULLTEXT index. MATCH expression can appear in WHERE (for filtering) and SELECT (for relevance scoring).
- **PostgreSQL**: `to_tsvector('config', text) @@ to_tsquery('config', 'terms')`. Supports `plainto_tsquery()` for plain text input, `ts_rank()` for relevance scoring, `ts_headline()` for result highlighting, and boolean operators (`&`, `|`, `!`).
- **SQLite**: FTS5 virtual tables with `MATCH` operator, `bm25()` ranking, `highlight()`, and `snippet()` functions.

Scenarios verify: keyword matching, boolean operators (required/excluded terms), relevance scoring, highlight/snippet generation, no-match returns empty, search after INSERT reflects shadow data, and prepared statement parameters.

## SPEC-3.3g User-Defined Functions in Queries
**Status:** Pending Verification
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO
**Tests:** `Mysqli/StoredProcedureTest`, `Pdo/MysqlStoredProcedureTest`, `Pdo/PostgresStoredFunctionTest`

User-defined functions (MySQL stored functions, PostgreSQL PL/pgSQL functions) called within SELECT, WHERE, and ORDER BY clauses should work through the CTE-rewritten shadow queries because the function call is a scalar expression evaluated by the database engine on the CTE-derived data.

However, if a user-defined function internally reads from a table that is shadow-stored, the function reads from the physical table (empty), not the shadow store. This is a fundamental limitation: the CTE rewriter only rewrites table references in the outer query, not inside function bodies.

Scenarios verify: function in SELECT expression, function in WHERE clause, function in ORDER BY clause, multiple functions in same query, function that reads from shadow table (expected to read physical), and prepared statements with function parameters.

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

- **MySQL and PostgreSQL**: Derived tables always return empty results because inner table references read from the physical database. This applies both when the derived table is the sole FROM source and when it is JOINed with a regular table.
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
