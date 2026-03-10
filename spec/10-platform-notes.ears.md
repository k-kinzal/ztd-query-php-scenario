# 10. Platform-Specific Notes

## SPEC-10.2.1 TRUNCATE
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO

- Verified on MySQL and PostgreSQL. SQLite does not have native TRUNCATE TABLE syntax; `DELETE FROM table` is the equivalent.
- PostgreSQL supports various TRUNCATE options (`TRUNCATE TABLE`, `TRUNCATE` without TABLE keyword, `TRUNCATE ONLY`, `RESTART IDENTITY`, `CONTINUE IDENTITY`, `CASCADE`) — all options are accepted and result in the shadow store being cleared.

## SPEC-10.2.2 multi_query() bypass
**Status:** Verified (By-Design)
**Platforms:** MySQLi

`multi_query()` bypasses ZTD entirely even when ZTD is enabled — writes go directly to the physical database and reads bypass the shadow store. Verified on MySQL (MySQLi).

## SPEC-10.2.3 FOREIGN KEY constraints
**Status:** Verified (By-Design)
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO

The foreign key constraint scenario uses a parent-child table relationship on MySQL and PostgreSQL. SQLite does not include the foreign key test because SQLite requires `PRAGMA foreign_keys = ON` to enforce them, which is outside ZTD scope.

## SPEC-10.2.4 Unsupported SQL examples
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO

Platform-specific unsupported SQL: MySQL uses `SET @var = 1`, PostgreSQL uses `SET search_path TO public`, SQLite uses `PRAGMA journal_mode=WAL`. All three platforms support behavior rules with prefix and regex patterns.

## SPEC-10.2.5 ALTER TABLE platform differences
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO (full); SQLite-PDO (accepted, ineffective); PostgreSQL-PDO (not supported)

- **MySQL**: Fully supported via `AlterTableMutation` for ADD/DROP/MODIFY/CHANGE/RENAME COLUMN, RENAME TABLE, ADD/DROP PRIMARY KEY, and ADD/DROP FOREIGN KEY (metadata-only no-ops). Unsupported: ADD/DROP INDEX, ADD/DROP KEY, ADD/DROP UNIQUE KEY, ADD/DROP CONSTRAINT, RENAME INDEX/KEY, ALTER COLUMN SET/DROP DEFAULT, ORDER BY — throw `UnsupportedSqlException`.
- **SQLite**: Accepts ALTER TABLE without error but CTE rewriter ignores schema changes.
- **PostgreSQL**: Throws `ZtdPdoException` for ALTER TABLE.
- **MySQL error types**: ALTER TABLE throws raw core exceptions (ColumnAlreadyExistsException, ColumnNotFoundException — NOT wrapped in adapter exceptions). SQLite silently ignores all validation errors.

## SPEC-10.2.6 TEMPORARY/UNLOGGED tables
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO

`CREATE TEMPORARY TABLE` (and `CREATE TEMP TABLE`) works correctly on all platforms — the shadow store creates the table regardless of the TEMPORARY modifier. PostgreSQL `CREATE UNLOGGED TABLE` also works. DROP TABLE on shadow-created temporary tables removes them from the shadow store.

## SPEC-10.2.7 PostgreSQL ONLY keyword
**Status:** Verified
**Platforms:** PostgreSQL-PDO

`UPDATE ONLY`, `DELETE FROM ONLY`, and `TRUNCATE ONLY` work correctly — the PgSqlParser regex patterns include `(?:ONLY\s+)?` to skip the keyword. In shadow mode, ONLY has no effect.

## SPEC-10.2.8 execute_query with UPDATE/DELETE (MySQLi)
**Status:** Verified
**Platforms:** MySQLi

MySQLi `execute_query()` (PHP 8.2+) correctly handles UPDATE and DELETE operations with parameters, including multi-row updates/deletes and affected row counts.

## SPEC-10.2.9 execute_query UPSERT/REPLACE limitation (MySQLi)
**Status:** Known Issue
**Platforms:** MySQLi

MySQLi `execute_query()` with UPSERT (`ON DUPLICATE KEY UPDATE`) and REPLACE does NOT update/replace existing rows. This contrasts with `prepare()` + `bind_param()` + `execute()` which works correctly.

## SPEC-10.2.10 MySQL INSERT ... SET syntax
**Status:** Verified (By-Design)
**Platforms:** MySQLi, MySQL-PDO

MySQL's alternative INSERT syntax (`INSERT INTO table SET col1 = val1, col2 = val2`) works correctly in ZTD. Combined with ON DUPLICATE KEY UPDATE also works. MySQL-specific.

## SPEC-10.2.11 SELECT with locking clauses
**Status:** Verified (By-Design)
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO

Locking clauses (FOR UPDATE, FOR SHARE, LOCK IN SHARE MODE, FOR NO KEY UPDATE, FOR KEY SHARE) are preserved in the CTE-rewritten SQL. However, since the query reads from CTE-derived data, **no actual row locks are acquired**. The locking clause is effectively a no-op when ZTD is enabled.

## SPEC-10.2.12 SQLite conflict resolution syntax
**Status:** Verified
**Platforms:** SQLite-PDO

SQLite's `INSERT OR REPLACE INTO` syntax works correctly as a synonym for `REPLACE INTO` via `exec()`. Prepared `INSERT OR REPLACE` has the same limitation as prepared `REPLACE INTO`.

## SPEC-10.2.13 NULL sort order in ORDER BY
**Status:** Verified (By-Design)
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO

MySQL and SQLite sort NULLs first in ASC order. PostgreSQL sorts NULLs last in ASC order. This is standard SQL behavior, not a ZTD issue.

## SPEC-10.2.14 PostgreSQL-specific features
**Status:** Verified
**Platforms:** PostgreSQL-PDO

ILIKE (case-insensitive LIKE), `::` type casting, `||` string concatenation, POSITION(), GENERATE_SERIES (when not referencing shadow tables), DISTINCT ON, STRING_AGG with ORDER BY, GREATEST/LEAST, OFFSET...FETCH syntax — all work correctly with ZTD shadow data.

**Not supported:** INSERT/UPDATE/DELETE RETURNING clause ([Issue #32](11-known-issues.ears.md)).

## SPEC-10.2.15 MySQL-specific features
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO

IF(), IFNULL(), FIND_IN_SET(), CONCAT_WS(), REVERSE(), LPAD(), GROUP_CONCAT with ORDER BY — all work correctly with shadow data.

## SPEC-10.2.16 SQLite-specific features
**Status:** Verified
**Platforms:** SQLite-PDO

typeof(), INSTR(), IIF(), printf(), HEX(), NULLIF(), CAST(), GLOB operator — all work correctly with shadow data. NATURAL JOIN works.

## SPEC-10.2.17 Prepared LIMIT/OFFSET on MySQL PDO
**Status:** Verified
**Platforms:** MySQL-PDO

MySQL requires integer types for LIMIT and OFFSET parameters. When using `execute($params)` with positional arrays, PDO sends all values as strings, causing `Syntax error ... near ''3' OFFSET '0''`. Workaround: use `bindValue($pos, $val, PDO::PARAM_INT)` for LIMIT/OFFSET parameters. MySQLi, PostgreSQL, and SQLite are not affected.

## SPEC-10.2.18 Date/time functions through shadow store
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO

Platform-specific date/time functions work correctly with CTE shadow data:

- **MySQL**: DATE_FORMAT(), YEAR(), MONTH(), DAY(), HOUR(), MINUTE(), DATEDIFF(), CURDATE(). Date comparison with BETWEEN and `>=` works. GROUP BY with DATE_FORMAT() works.
- **PostgreSQL**: TO_CHAR() works correctly. EXTRACT(YEAR FROM ...) returns 0 for shadow dates (see [SPEC-11.PG-EXTRACT](11-known-issues.ears.md)). Date comparison and ordering work.
- **SQLite**: strftime(), date(), julianday(), date modifiers ('+1 month') all work correctly with shadow data. Date comparison and ordering work.

Date values survive INSERT, UPDATE, and SELECT roundtrip. Prepared statements with date range parameters work on all platforms.

## SPEC-10.2.19 PostgreSQL ENUM types
**Status:** Verified
**Platforms:** PostgreSQL-PDO
**Tested versions:** ztd-query-pdo-adapter v0.1.1, PostgreSQL 16, PHP 8.5
**Tests:** `Pdo/PostgresEnumTypeTest`

PostgreSQL native ENUM types (CREATE TYPE ... AS ENUM) work correctly through ZTD shadow store. INSERT, UPDATE, WHERE comparison, NULL values, filtering, ordering, and prepared statements with ENUM parameters all function correctly. Cross-platform parity with MySQL column-level ENUM (MySQLi/EnumTypeTest, Pdo/MysqlEnumTypeTest).

## SPEC-10.2.20 Schema introspection queries
**Status:** Verified
**Platforms:** MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tested versions:** ztd-query-pdo-adapter v0.1.1, MySQL 8.0, PostgreSQL 16, SQLite 3.x, PHP 8.5
**Tests:** `Pdo/MysqlInformationSchemaTest`, `Pdo/PostgresInformationSchemaTest`, `Pdo/SqliteSchemaIntrospectionTest`

Schema introspection queries read physical database metadata and may pass through or be treated as unsupported SQL depending on the adapter and query:

- **MySQL PDO**: `SELECT ... FROM INFORMATION_SCHEMA.TABLES` and `INFORMATION_SCHEMA.COLUMNS` execute correctly (pass through).
- **PostgreSQL PDO**: `SELECT ... FROM information_schema.tables` and `information_schema.columns` execute correctly (pass through).
- **SQLite PDO**: `SELECT ... FROM sqlite_master` and `PRAGMA table_info()` may execute or throw depending on adapter behavior.

**Verified behavior:** Shadow operations (INSERT, SELECT, UPDATE, DELETE) continue to work correctly alongside schema introspection queries on all platforms. The shadow store is not affected by schema queries.

## SPEC-10.2.21 Multi-tenant query patterns
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tested versions:** ztd-query-mysqli-adapter v0.1.1, ztd-query-pdo-adapter v0.1.1, MySQL 8.0, PostgreSQL 16, SQLite 3.x, PHP 8.5
**Tests:** `Mysqli/MultiTenantPatternTest`, `Pdo/MysqlMultiTenantPatternTest`, `Pdo/PostgresMultiTenantPatternTest`, `Pdo/SqliteMultiTenantPatternTest`

**Verified behavior:** Multi-tenant query patterns (WHERE tenant_id = ?) work correctly through ZTD shadow store for all CRUD operations. Tenant-filtered SELECT, INSERT, UPDATE, DELETE, prepared SELECT with tenant_id parameter, JOINs across tenant-filtered tables, and per-tenant aggregation (GROUP BY tenant_id) all return correct results. Mutations for one tenant do not affect other tenants' data. Physical isolation confirmed on all platforms.

## SPEC-10.2.22 Generated / computed columns
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/GeneratedColumnTest`, `Pdo/MysqlGeneratedColumnTest`, `Pdo/PostgresGeneratedColumnTest`, `Pdo/SqliteGeneratedColumnTest`, `*GeneratedColumnEdgeCasesTest`

Tables with generated columns (MySQL `GENERATED ALWAYS AS ... STORED/VIRTUAL`, PostgreSQL `GENERATED ALWAYS AS ... STORED`, SQLite `GENERATED ALWAYS AS ... STORED/VIRTUAL`) can be used with ZTD. INSERT omitting generated columns works. UPDATE on non-generated columns works.

**Generated column values are NULL in the shadow store.** The CTE does not re-evaluate generation expressions. Since INSERT omits generated columns (they are computed by the DB engine), the shadow store has NULL for all generated columns. This applies to both STORED and VIRTUAL generated columns. After UPDATE of source columns, the generated column remains NULL.

**Workaround:** Use the generation expression directly in SELECT, WHERE, GROUP BY, and ORDER BY clauses (e.g., `price * quantity` instead of referencing the `total` generated column). `COALESCE(generated_col, manual_expression)` also works. Non-prepared queries with manual expressions work correctly. Prepared statements with arithmetic expressions and bound parameters in WHERE may return empty results on SQLite.

## SPEC-10.2.23 Window functions
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/WindowFrameTest`, `Mysqli/WindowFunctionWithPreparedStmtTest`, `Mysqli/DateAndAdvancedWindowTest`, `Pdo/MysqlWindowFrameTest`, `Pdo/PostgresWindowFrameTest`, `Pdo/SqliteWindowFrameTest`, `*WindowFunctionEdgeCasesTest` (and platform variants)

Window functions (ROW_NUMBER, RANK, DENSE_RANK, NTILE, LAG, LEAD, FIRST_VALUE, LAST_VALUE, SUM/AVG OVER) work correctly on CTE-rewritten shadow data. Window frame specifications (ROWS BETWEEN, RANGE BETWEEN) and PARTITION BY clauses are preserved through the CTE rewriting. Prepared statements with window functions work.

**Verified behavior:** Multiple window functions with different PARTITION BY clauses in the same query work. NTILE distribution works. LAG/LEAD return NULL for out-of-frame rows. SUM OVER with NULL values skips NULLs (running total does not decrease). FIRST_VALUE/LAST_VALUE with ROWS BETWEEN frames work. DENSE_RANK correctly handles ties. Window functions reflect mutations (UPDATE visible in subsequent windowed SELECT).

## SPEC-10.2.24 Column aliasing in complex queries
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/ColumnAliasingTest`, `Pdo/MysqlColumnAliasingTest`, `Pdo/PostgresColumnAliasingTest`, `Pdo/SqliteColumnAliasingTest`, `*ColumnAliasingEdgeCasesTest`

Column aliases in SELECT expressions are preserved through CTE rewriting. Aliases on aggregate functions, CASE expressions, and subqueries in SELECT list are correctly propagated to the result set.

**Verified behavior:** UNION aliases come from the first SELECT (standard SQL behavior). Alias shadowing (using a column name as alias for a different expression, e.g., `UPPER(name) AS name`) works correctly. Window function aliases (ROW_NUMBER, SUM OVER) are preserved. Multiple aliases derived from the same source column work. CASE expression aliases in result set work. LEFT JOIN with aggregate aliases work. Prepared statements with aliased columns work.

## SPEC-10.2.25 SQL comments in queries
**Status:** Verified
**Tested versions:** ztd-query-pdo-adapter v0.1.1, SQLite 3.x, PHP 8.5
**Platforms:** SQLite-PDO
**Tests:** `Pdo/SqliteSqlCommentsTest`

SQL comments (`-- line comment` and `/* block comment */`) within queries do not interfere with CTE rewriting or query classification.

## SPEC-10.2.26 Reserved keyword identifiers
**Status:** Verified
**Tested versions:** ztd-query-pdo-adapter v0.1.1, SQLite 3.x, PHP 8.5
**Platforms:** SQLite-PDO
**Tests:** `Pdo/SqliteReservedKeywordIdentifierTest`

Tables and columns named with SQL reserved keywords (quoted with backticks or double quotes) work correctly through ZTD shadow store.

## SPEC-10.2.27 LATERAL subqueries (PostgreSQL)
**Status:** Known Issue
**Platforms:** PostgreSQL-PDO
**Tested versions:** ztd-query-pdo-adapter v0.1.1, PostgreSQL 16, PHP 8.5
**Tests:** `Pdo/PostgresLateralSubqueryTest`

PostgreSQL `LATERAL` subqueries (correlated subqueries in FROM clause) return empty results through ZTD CTE rewriting. The CTE rewriter does not rewrite table references inside LATERAL clauses — the inner query reads from the physical table (empty), so the outer query gets no rows.

Affected patterns: `LATERAL (SELECT ... FROM table WHERE correlated_condition)`, `LEFT JOIN LATERAL ... ON true`, `LATERAL` with LIMIT (top-N per group).

**Workarounds:**
- Use correlated subqueries in the SELECT list instead of LATERAL: `SELECT (SELECT SUM(x) FROM t WHERE t.fk = u.id) FROM u`.
- Use regular JOINs with GROUP BY subqueries: `JOIN (SELECT fk, SUM(x) FROM t WHERE 1=1 GROUP BY fk) sub ON sub.fk = u.id`.

See [SPEC-11.PG-LATERAL](11-known-issues.ears.md).

## SPEC-10.2.28 CTE MATERIALIZED hint (PostgreSQL)
**Status:** Verified (Known Limitation)
**Platforms:** PostgreSQL-PDO
**Tested versions:** ztd-query-pdo-adapter v0.1.1, PostgreSQL 16, PHP 8.5
**Tests:** `Pdo/PostgresCteMaterializedTest`

PostgreSQL CTE materialization hints (`WITH ... AS MATERIALIZED (...)` and `WITH ... AS NOT MATERIALIZED (...)`) are accepted without error but the user CTE is overwritten by ZTD's shadow CTE. The user CTE body reads from the physical table (empty), returning 0 rows. This is consistent with [SPEC-11.PG-CTE](11-known-issues.ears.md) — all user CTEs on PostgreSQL are affected, regardless of materialization hints. Regular queries and shadow mutations continue to work correctly.

## SPEC-10.2.29 Sequences (PostgreSQL)
**Status:** Verified
**Platforms:** PostgreSQL-PDO
**Tested versions:** ztd-query-pdo-adapter v0.1.1, PostgreSQL 16, PHP 8.5
**Tests:** `Pdo/PostgresSequenceTest`

PostgreSQL sequence operations work correctly through ZTD:

- `SELECT nextval('sequence_name')` returns incrementing values (sequences operate on physical database state, not shadow store).
- `INSERT ... VALUES (nextval('sequence'), ...)` works — sequence-generated IDs are stored in the shadow store.
- `SERIAL` / `GENERATED BY DEFAULT AS IDENTITY` columns work with ZTD — INSERT omitting the serial column uses the sequence default.

Physical isolation confirmed: shadow inserts using sequences do not persist to the physical table.

## SPEC-10.2.30 String quoting and escaping (PostgreSQL)
**Status:** Verified
**Platforms:** PostgreSQL-PDO
**Tested versions:** ztd-query-pdo-adapter v0.1.1, PostgreSQL 16, PHP 8.5
**Tests:** `Pdo/PostgresDollarQuotedStringTest`

String quoting and escaping through ZTD on PostgreSQL:

- INSERT with `''`-escaped single quotes works correctly (`'It''s a test'` → `It's a test`).
- Prepared statements with string parameters containing single quotes work correctly.
- Strings containing semicolons do not cause statement splitting.
- WHERE clause with `''`-escaped string comparison works.
- Backslash characters are preserved (`standard_conforming_strings = on` by default).
- Empty strings and NULL values are correctly stored and retrieved.
- Double quotes inside string values are preserved.

**Limitation:** UPDATE with `''`-escaped single quotes in SET value breaks WHERE clause parsing (existing [Issue #25](https://github.com/k-kinzal/ztd-query-php/issues/25)). Workaround: use prepared statements for UPDATE with special characters.

## SPEC-10.2.31 Many-to-many relationship queries
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/TagFilteringTest`, `Pdo/MysqlTagFilteringTest`, `Pdo/PostgresTagFilteringTest`, `Pdo/SqliteTagFilteringTest`

Many-to-many relationship query patterns (e.g., blog posts with tags) work correctly through ZTD shadow store. Verified patterns: filter by single tag (JOIN), filter by all tags (GROUP BY HAVING COUNT), filter by any tag (DISTINCT), tag count per entity (LEFT JOIN + COUNT), post count per tag, exclude by tag (NOT EXISTS), add/remove tags and requery, prepared tag parameter, physical isolation.

## SPEC-10.2.32 Hierarchical self-join queries
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/HierarchicalSelfJoinTest`, `Pdo/MysqlHierarchicalSelfJoinTest`, `Pdo/PostgresHierarchicalSelfJoinTest`, `Pdo/SqliteHierarchicalSelfJoinTest`

Self-join tree traversal patterns (org chart, category hierarchy) work correctly through ZTD shadow store on all platforms. Verified patterns: 1-level self-join (employee + manager), 2-level self-join (employee + manager + grandmanager), direct report count, team salary budget, salary comparison (employee vs manager), root/leaf node detection, department rank (window function + self-join), promote/move operation and re-verification, prepared direct reports lookup.

## SPEC-10.2.33 Inventory management workflow
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/InventoryWorkflowTest`, `Pdo/MysqlInventoryWorkflowTest`, `Pdo/PostgresInventoryWorkflowTest`, `Pdo/SqliteInventoryWorkflowTest`

Inventory management patterns work correctly through ZTD shadow store. Verified patterns: stock decrement (self-referencing UPDATE), conditional stock check (UPDATE WHERE stock >= N), restock (increment), low stock alert (WHERE stock < reorder_point), total inventory value (SUM aggregate), order fulfillment workflow (INSERT order + UPDATE stock + verify), bulk CASE adjustment, stock movement report (JOIN + conditional aggregation), prepared availability check, sequential decrements, physical isolation.

## SPEC-10.2.34 Pivot/cross-tab reporting
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/PivotReportTest`, `Pdo/MysqlPivotReportTest`, `Pdo/PostgresPivotReportTest`, `Pdo/SqlitePivotReportTest`, `*PivotMultiLevelTest`

Pivot-style reports using conditional aggregation (SUM/COUNT with CASE WHEN) work correctly on all platforms. Verified patterns: monthly category pivot, region category pivot, month-over-month comparison, COUNT-based pivot, pivot after INSERT/UPDATE/DELETE, prepared parameterized pivot.

**Verified behavior (extended):** Two-dimension pivot (region × month) works. Three-dimension pivot (region × category × month) works. Pivot with NULL categories handled via COALESCE. Pivot with HAVING filter works. COUNT-based pivot with CASE works. Pivot after interleaved INSERT + UPDATE mutations works. COUNT(DISTINCT) in pivot columns works. Prepared parameterized pivot with category filter works.

**Limitation:** Percentage-of-total calculations using scalar subqueries in SELECT (`SUM(x) / (SELECT SUM(x) FROM t)`) return empty results on all platforms — see [SPEC-11.BARE-SUBQUERY-REWRITE](11-known-issues.ears.md). The CROSS JOIN derived-table workaround works on SQLite (where derived tables in JOINs are rewritten per [SPEC-3.3a](03-read-operations.ears.md)) but NOT on MySQL or PostgreSQL (where derived tables always return empty).

## SPEC-10.2.35 Multi-step transformation workflow
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/MultiStepTransformationTest`, `Pdo/MysqlMultiStepTransformationTest`, `Pdo/PostgresMultiStepTransformationTest`, `Pdo/SqliteMultiStepTransformationTest`

ETL-like multi-step data transformation workflows work correctly through ZTD shadow store on all platforms. Verified patterns: full order processing pipeline (insert orders → update customer aggregates → recalculate tiers → verify), bulk insert then cross-table aggregate, sequential update-delete-select pipeline, conditional insert based on aggregate check, interleaved reads and writes (each mutation verified before proceeding), multi-table cleanup (delete + reset).

## SPEC-10.2.36 Period-over-period comparison queries
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/PeriodComparisonTest`, `Pdo/MysqlPeriodComparisonTest`, `Pdo/PostgresPeriodComparisonTest`, `Pdo/SqlitePeriodComparisonTest`

Period comparison analytics (year-over-year, quarter-over-quarter, month-over-month) work correctly through ZTD shadow store on all platforms. Verified patterns: YoY revenue comparison using conditional aggregation (SUM CASE WHEN year = ?), YoY growth percentage by region, monthly trend with LAG() window function for period deltas, quarterly aggregation with CASE-based grouping, prepared parameterized period comparison, period comparison after INSERT, cross-period region×product matrix with HAVING filter.

## SPEC-10.2.38 Deduplication query patterns
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tested versions:** ztd-query-mysqli-adapter v0.1.1, ztd-query-pdo-adapter v0.1.1, MySQL 8.0, PostgreSQL 16, SQLite 3.x, PHP 8.5
**Tests:** `Mysqli/DeduplicationPatternTest`, `Pdo/MysqlDeduplicationPatternTest`, `Pdo/PostgresDeduplicationPatternTest`, `Pdo/SqliteDeduplicationPatternTest`, `*DeduplicationEdgeCasesTest`

Deduplication query patterns (finding and removing duplicate rows) work through ZTD shadow store with platform-specific differences.

**Common patterns (all platforms):**
- `GROUP BY ... HAVING COUNT(*) > 1` for finding duplicate groups works.
- `COUNT(DISTINCT col)` for duplicate summary works.
- `WHERE col IN (SELECT col GROUP BY col HAVING COUNT(*) > 1)` for finding all duplicate rows works.
- `MIN(id)`/`MAX(id)` with GROUP BY for first/last occurrence works.
- Prepared duplicate lookup works.

**Verified behavior (extended):** Keep-highest-score dedup using JOIN with MAX aggregate subquery works. Keep-first-created dedup using JOIN with MIN(id) works. GROUP BY treats NULLs as equal (standard SQL). Composite-key dedup (GROUP BY col1, col2) works. Dedup summary stats (COUNT(*) - COUNT(DISTINCT col)) work. Multi-pass dedup (delete some duplicates, keep others) works via workaround. Dedup after INSERT correctly reflects new duplicates.

**ROW_NUMBER derived table for deduplication:**
- **PostgreSQL**: `SELECT ... FROM (SELECT ..., ROW_NUMBER() OVER (PARTITION BY ...) AS rn FROM table) sub WHERE rn = 1` works correctly. PostgreSQL also supports `DISTINCT ON (col)` as a simpler alternative.
- **MySQL and SQLite**: ROW_NUMBER derived table as sole FROM source returns empty (SPEC-3.3a). Workaround: use `GROUP BY` with `MIN(id)`/`MAX(id)` aggregation.

**DELETE duplicates:**
- DELETE with NOT IN + GROUP BY subquery causes "incomplete input" on SQLite (CTE rewriter truncates SQL with GROUP BY in subquery — extends known issue from [SPEC-11.UPDATE-AGGREGATE-SUBQUERY](11-known-issues.ears.md) to DELETE). Workaround: query IDs to keep first, then delete by explicit ID list.

## SPEC-10.2.39 Data reconciliation patterns
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tested versions:** ztd-query-mysqli-adapter v0.1.1, ztd-query-pdo-adapter v0.1.1, MySQL 8.0, PostgreSQL 16, SQLite 3.x, PHP 8.5
**Tests:** `Mysqli/DataReconciliationTest`, `Pdo/MysqlDataReconciliationTest`, `Pdo/PostgresDataReconciliationTest`, `Pdo/SqliteDataReconciliationTest`

Data reconciliation patterns (comparing two tables for missing, extra, and mismatched rows) work correctly through ZTD shadow store on all platforms. Verified patterns: LEFT JOIN anti-join (missing/extra rows), INNER JOIN with inequality (mismatched values), exact match detection, reconciliation summary with scalar subqueries, column-level difference detail using CASE, reconciliation after INSERT/UPDATE fixes, prepared parameterized reconciliation.

**PostgreSQL-specific:** `FULL OUTER JOIN` reconciliation with CASE-based status classification works correctly, providing a single-query view of all matches, mismatches, missing, and extra rows.

## SPEC-10.2.37 Event log / audit trail queries
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/EventLogTest`, `Pdo/MysqlEventLogTest`, `Pdo/PostgresEventLogTest`, `Pdo/SqliteEventLogTest`

Audit trail and event log query patterns work correctly through ZTD shadow store on all platforms. Verified patterns: time-range queries (BETWEEN), prepared time-range with parameter reuse, filter by type and entity, append new events and verify ordering, event count by type (GROUP BY), event log with user JOIN (who did what), activity per user (LEFT JOIN + COUNT), entity history (prepared parameterized), multiple COUNT DISTINCT in single query (COUNT(DISTINCT col1), COUNT(DISTINCT col2)), multiple COUNT DISTINCT with GROUP BY, physical isolation confirmation.

## SPEC-10.2.40 Financial ledger / running balance
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/LedgerRunningBalanceTest`, `Pdo/MysqlLedgerRunningBalanceTest`, `Pdo/PostgresLedgerRunningBalanceTest`, `Pdo/SqliteLedgerRunningBalanceTest`

Financial ledger patterns with running balance calculation via window functions work correctly through ZTD shadow store on all platforms. Verified patterns: `SUM() OVER (ORDER BY ... ROWS UNBOUNDED PRECEDING)` running total, running balance after new transaction inserts, self-referencing balance UPDATE (`balance = balance + N`), multiple window functions in same query (ROW_NUMBER + SUM + COUNT), `PARTITION BY account_id` running balance, credit/debit totals via CASE aggregation, full transaction workflow (insert + update balance + verify running total), prepared parameterized running balance, physical isolation.

## SPEC-10.2.41 Cascading delete across related tables
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/CascadeCleanupTest`, `Pdo/MysqlCascadeCleanupTest`, `Pdo/PostgresCascadeCleanupTest`, `Pdo/SqliteCascadeCleanupTest`

Cascading delete workflows (deleting a parent entity and all related child data across 4 tables) work correctly through ZTD shadow store on all platforms. Verified patterns: DELETE with 2-level nested subquery (`WHERE id IN (SELECT ... WHERE col IN (SELECT ...))`), DELETE with 3-level nested subquery, multi-step cascade (likes→comments→posts→users), orphan detection via LEFT JOIN after cascade, aggregate counts before/after, user activity summary with correlated subqueries in SELECT, prepared parameterized delete, physical isolation.

## SPEC-10.2.42 Bulk conditional upgrade with aggregate lookup
**Status:** Partially Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/BulkConditionalUpgradeTest`, `Pdo/MysqlBulkConditionalUpgradeTest`, `Pdo/PostgresBulkConditionalUpgradeTest`, `Pdo/SqliteBulkConditionalUpgradeTest`

Bulk conditional UPDATE based on cross-table aggregate lookups (e.g., CRM-style customer tier upgrades). The query-then-update workaround (SELECT eligible IDs, then UPDATE by explicit list) works on all platforms.

**Known Issue:** Direct `UPDATE ... WHERE id IN (SELECT ... GROUP BY ... HAVING SUM() >= N)` fails on SQLite with "incomplete input" — see [SPEC-11.UPDATE-AGGREGATE-SUBQUERY](11-known-issues.ears.md). Workaround: query eligible IDs via SELECT first, then UPDATE by explicit ID list.

Verified patterns (using workaround): sequential tier upgrades (bronze→silver→gold), upgrade after new order inserts, tier verification via LEFT JOIN with order aggregates, tier count summary (GROUP BY tier), misclassified customer detection (NOT IN with aggregate subquery in SELECT), UPDATE with simple non-aggregate subquery (works directly), order status change affecting eligibility, physical isolation.

## SPEC-10.2.43 RBAC permission checking (5-table JOIN)
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/RbacPermissionCheckTest`, `Pdo/MysqlRbacPermissionCheckTest`, `Pdo/PostgresRbacPermissionCheckTest`, `Pdo/SqliteRbacPermissionCheckTest`

Role-based access control query patterns with 5 junction tables (users, roles, permissions, user_roles, role_permissions) work correctly through ZTD shadow store on all platforms. Verified patterns: nested EXISTS with 3-table JOIN inside, 5-table JOIN listing all user-permission pairs, NOT EXISTS for users without roles, COUNT(DISTINCT) permission count per user, grant role via junction INSERT + immediate EXISTS re-check, revoke role via junction DELETE + verify, IN subquery for users with a given role, inactive user filtering, prepared permission check with resource/action parameters, physical isolation.

## SPEC-10.2.44 Job queue state machine processing
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/JobQueueProcessingTest`, `Pdo/MysqlJobQueueProcessingTest`, `Pdo/PostgresJobQueueProcessingTest`, `Pdo/SqliteJobQueueProcessingTest`

Job queue processing patterns (priority-based claiming, state transitions, retry logic) work correctly through ZTD shadow store on all platforms. Verified patterns: claim next job via UPDATE with subquery (ORDER BY priority DESC, created_at ASC LIMIT 1), complete job (status transition + attempt increment), fail job, retry failed jobs (WHERE attempts < max_attempts), job status dashboard (GROUP BY status), queue-specific conditional aggregation, delete completed jobs, full lifecycle (claim→complete→claim next), sequential state transitions on same row (each mutation visible to next query), insert new job and verify queue ordering, physical isolation.

## SPEC-10.2.45 Invoice/billing workflow
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/InvoiceWorkflowTest`, `Pdo/MysqlInvoiceWorkflowTest`, `Pdo/PostgresInvoiceWorkflowTest`, `Pdo/SqliteInvoiceWorkflowTest`

Invoice/billing workflow patterns (multi-table: customers, invoices, line items) work correctly through ZTD shadow store on all platforms. Verified patterns: line item subtotal calculation (quantity × unit_price), invoice total via JOIN + GROUP BY + SUM, discount percentage application (SUM * (1 - pct/100)), status transitions with conditional UPDATE (draft→sent→paid, guard against invalid transitions), 3-table customer spending summary (customer JOIN invoice JOIN line_items with COUNT DISTINCT + SUM), add/remove line items and verify total recalculation, prepared parameterized invoice lookup (re-execute for different customers), HAVING filter for high-value invoices, physical isolation.

## SPEC-10.2.46 Chained mutation visibility
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/ChainedMutationVisibilityTest`, `Pdo/MysqlChainedMutationVisibilityTest`, `Pdo/PostgresChainedMutationVisibilityTest`, `Pdo/SqliteChainedMutationVisibilityTest`

Interleaved INSERT/UPDATE/DELETE/SELECT chains correctly reflect each mutation step in subsequent reads through ZTD shadow store on all platforms. Verified patterns: INSERT→SELECT→UPDATE→SELECT→DELETE→SELECT chain with aggregate verification at each step, multiple sequential UPDATEs to the same row (arithmetic accumulation: quantity + 5 + 3 + 2 = original + 10), DELETE then re-INSERT at same PK (new data visible), cross-table mutation visibility (item mutations + log inserts reflected in JOINed queries), conditional UPDATE based on computed thresholds, physical isolation after full mutation chain.

## SPEC-10.2.47 Search filter and pagination patterns
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/SearchFilterPaginationTest`, `Pdo/MysqlSearchFilterPaginationTest`, `Pdo/PostgresSearchFilterPaginationTest`, `Pdo/SqliteSearchFilterPaginationTest`

Parameterized search with multiple filter conditions and pagination (common REST API pattern) works correctly through ZTD shadow store on all platforms. Verified patterns: LIKE pattern search with prepared parameter, category + price range (BETWEEN) combined filter, active + in-stock boolean filter, 4-condition combined filter (LIKE + equality + range + boolean), LIMIT/OFFSET pagination (page traversal), search results update correctly after INSERT/UPDATE mutations, category summary with GROUP BY + HAVING, PostgreSQL ILIKE case-insensitive search, MySQL LIMIT/OFFSET with PARAM_INT binding, physical isolation.

## SPEC-10.2.48 CAST and type conversion
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/CastAndTypeConversionTest`, `Pdo/MysqlCastAndTypeConversionTest`, `Pdo/PostgresCastAndTypeConversionTest`, `Pdo/SqliteCastAndTypeConversionTest`

CAST and type conversion expressions work correctly through ZTD shadow store on all platforms. Verified patterns: CAST text to integer/numeric in SELECT, WHERE, ORDER BY, and GROUP BY; computed columns with multiple CASTs; CAST with NULL values; prepared statements with CAST in WHERE; CAST after INSERT; aggregate with CAST.

**Platform-specific syntax:**
- **MySQL**: `CAST(x AS SIGNED)`, `CAST(x AS DECIMAL(10,2))`, `CONVERT(x, type)`.
- **PostgreSQL**: `CAST(x AS INTEGER)`, `CAST(x AS NUMERIC)`, `x::integer` (double-colon shorthand). The `::` notation works correctly through ZTD.
- **SQLite**: `CAST(x AS INTEGER)`, `CAST(x AS REAL)`. `typeof()` function works.

## SPEC-10.2.49 Large string / text field handling
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/LargeStringHandlingTest`, `Pdo/MysqlLargeStringHandlingTest`, `Pdo/PostgresLargeStringHandlingTest`, `Pdo/SqliteLargeStringHandlingTest`

Large text data (10KB – 500KB) survives INSERT, UPDATE, and SELECT roundtrip through ZTD shadow store on all platforms. Verified patterns: 10KB string via exec(), 100KB string, 500KB string via prepared statement, LIKE search on large text, LENGTH/SUBSTR aggregate functions, UPDATE large field, multiple large rows with independent data, physical isolation.

The CTE rewriter correctly embeds large string values as literals in generated CTEs. LONGTEXT (MySQL), TEXT (PostgreSQL/SQLite) columns work. No silent truncation observed up to 500KB.

## SPEC-10.2.50 Bitwise operations
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/BitwiseOperationsTest`, `Pdo/MysqlBitwiseOperationsTest`, `Pdo/PostgresBitwiseOperationsTest`, `Pdo/SqliteBitwiseOperationsTest`

Bitwise operations (AND, OR, NOT/complement) work correctly through ZTD shadow store on all platforms. Verified patterns: flag check via `(col & mask) = mask`, combine flags via `col | flag`, remove flag via `col & ~flag`, cross-table access matrix (user permissions × feature requirements), grant/revoke via UPDATE with bitwise operators, permission summary via conditional aggregation, physical isolation.

**Platform-specific differences:**
- **MySQL**: `^` for XOR, `BIT_COUNT()` function. Both work through ZTD.
- **PostgreSQL**: `#` for XOR (not `^`). Works through ZTD.
- **SQLite**: No XOR operator. `^` throws "unrecognized token". Workaround: `(a | b) - (a & b)`.

**Known limitation (SQLite):** Prepared statements with bitwise AND in WHERE clause (`(col & ?) = ?`) return empty results. Non-prepared queries with literal bitmask values work correctly. Workaround: use literal values or build the query dynamically.

## SPEC-10.2.51 Dynamic WHERE clause building (query builder pattern)
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/DynamicFilterBuildingTest`, `Pdo/MysqlDynamicFilterBuildingTest`, `Pdo/PostgresDynamicFilterBuildingTest`, `Pdo/SqliteDynamicFilterBuildingTest`

Dynamic WHERE clause construction patterns common in PHP query builders work correctly through ZTD shadow store on all platforms. Verified patterns: `WHERE 1=1 AND ...` base pattern, optional single/multiple filter application, dynamic ORDER BY (column + direction), dynamic LIMIT/OFFSET with filter, separate queries with varying parameter counts, IS NULL / IS NOT NULL filtering, boolean-like column filtering, physical isolation.

## SPEC-10.2.52 Pagination with total count (dual-query API pattern)
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/PaginationWithTotalCountTest`, `Pdo/MysqlPaginationWithTotalCountTest`, `Pdo/PostgresPaginationWithTotalCountTest`, `Pdo/SqlitePaginationWithTotalCountTest`

The common REST API pattern of fetching paginated results AND total count in the same session works correctly through ZTD shadow store on all platforms. Verified patterns: page data + total count (two separate queries), total count updates after INSERT, paginate with filters + filtered total, empty page (offset beyond data range), page size change mid-session, prepared COUNT + prepared paginated query, keyset (cursor) pagination with total count, physical isolation.

## SPEC-10.2.53 Date interval and arithmetic queries
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/DateIntervalTest`, `Pdo/MysqlDateIntervalTest`, `Pdo/PostgresDateIntervalTest`, `Pdo/SqliteDateIntervalTest`

Date arithmetic and interval query patterns work correctly through ZTD shadow store on all platforms using platform-specific syntax.

**Platform-specific date arithmetic:**
- **MySQL**: `DATE_ADD(col, INTERVAL 7 DAY)`, `DATE_SUB(col, INTERVAL 1 MONTH)`, `DATE_FORMAT(col, '%Y-%m')`, `DATEDIFF()`.
- **PostgreSQL**: `col + INTERVAL '7 days'`, `col - INTERVAL '1 month'`, `TO_CHAR(col, 'YYYY-MM')`, `date_trunc('month', col)`. Note: `EXTRACT()` returns 0 for shadow dates (see [SPEC-11.PG-EXTRACT](11-known-issues.ears.md)).
- **SQLite**: `date(col, '+7 days')`, `date(col, '-1 month')`, `strftime('%Y-%m', col)`.

Verified patterns: date range BETWEEN query, date arithmetic (+days, -months), GROUP BY month, overlapping date range detection, date comparison in UPDATE WHERE, INSERT then query by relative date range, prepared date range with bound parameters, physical isolation.

## SPEC-10.2.54 Multi-column sorting and ordering
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/MultiColumnSortingTest`, `Pdo/MysqlMultiColumnSortingTest`, `Pdo/PostgresMultiColumnSortingTest`, `Pdo/SqliteMultiColumnSortingTest`

Multi-column ORDER BY with expressions and custom sort logic works correctly through ZTD shadow store on all platforms. Verified patterns: two-column sort (category ASC, price DESC), three-column sort, CASE-based priority sorting (custom enum-like ordering), ORDER BY expression (LENGTH, COALESCE), ORDER BY with NULL handling, sort direction change after UPDATE, prepared statement with ORDER BY and LIMIT, physical isolation.

**Platform-specific NULL sort order:** MySQL and SQLite sort NULLs first in ASC order. PostgreSQL sorts NULLs last in ASC order (supports explicit `NULLS FIRST` / `NULLS LAST`).

## SPEC-10.2.55 Upsert workflow patterns
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/UpsertWorkflowTest`, `Pdo/MysqlUpsertWorkflowTest`, `Pdo/PostgresUpsertWorkflowTest`, `Pdo/SqliteUpsertWorkflowTest`

Realistic upsert workflow patterns (idempotent writes, skip-if-exists, read-modify-write) work correctly through ZTD shadow store on all platforms using platform-specific syntax.

## SPEC-10.2.56 UPDATE ... FROM join syntax (PostgreSQL)
**Status:** Verified
**Platforms:** PostgreSQL-PDO (works), SQLite-PDO (fails — see [SPEC-11.UPDATE-FROM](11-known-issues.ears.md))
**Tests:** `Pdo/PostgresUpdateFromJoinTest`, `Pdo/SqliteUpdateFromJoinTest`

PostgreSQL's `UPDATE t1 SET col = t2.col FROM t2 WHERE t1.id = t2.fk` syntax works through ZTD. The CTE rewriter handles the FROM clause in UPDATE statements correctly on PostgreSQL.

On SQLite, the same syntax (supported since SQLite 3.33) still fails with a syntax error through ZTD.

**Correlated subquery workaround limitation:** Using a correlated subquery in the SET clause (e.g., `SET col = (SELECT x FROM t2 WHERE t2.fk = t1.id)`) fails on PostgreSQL with a CTE rewriter syntax error when the outer table is referenced by fully-qualified name. See [SPEC-11.UPDATE-SUBQUERY-SET](11-known-issues.ears.md).

## SPEC-10.2.57 PostgreSQL ROUND function with double precision
**Status:** Verified
**Platforms:** PostgreSQL-PDO
**Tests:** `Pdo/PostgresPivotReportTest`

PostgreSQL's `ROUND(value, precision)` function with two arguments requires `numeric` type, not `double precision`. Queries like `ROUND(SUM(amount) * 100.0 / total, 1)` where the expression produces `double precision` will fail with "function round(double precision, integer) does not exist".

Workaround: wrap the expression in `CAST(... AS numeric)`: `ROUND(CAST(expr AS numeric), 1)`. This is a PostgreSQL-native limitation, not a ZTD issue, but it commonly appears when writing cross-platform percentage calculations.

**Platform-specific upsert syntax:**
- **MySQL**: `INSERT INTO ... ON DUPLICATE KEY UPDATE`, `INSERT IGNORE INTO`. MySQLi `prepare()+bind_param()+execute()` works correctly; PDO `exec()` works, PDO prepared UPSERT does not (see [SPEC-11.PDO-UPSERT](11-known-issues.ears.md)).
- **PostgreSQL**: `INSERT INTO ... ON CONFLICT (col) DO UPDATE SET col = EXCLUDED.col`, `INSERT INTO ... ON CONFLICT DO NOTHING`. PDO `exec()` works; PDO prepared UPSERT does not.
- **SQLite**: `INSERT OR REPLACE INTO`, `INSERT OR IGNORE INTO`. PDO `exec()` works; PDO prepared does not. Note: `ON CONFLICT DO NOTHING` inserts both rows on SQLite (see [SPEC-11.SQLITE-ON-CONFLICT](11-known-issues.ears.md)); use `INSERT OR IGNORE` instead.

Verified patterns: idempotent write (replace existing), skip-if-exists, check-then-insert pattern, upsert counter (read-modify-write increment), batch upsert (multiple sequential upserts), verify old data replaced not accumulated, read-modify-write preserving non-updated columns, physical isolation.

## SPEC-10.2.58 Reservation/booking system workflow
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/ReservationBookingTest`, `Pdo/MysqlReservationBookingTest`, `Pdo/PostgresReservationBookingTest`, `Pdo/SqliteReservationBookingTest`

A reservation/booking system workflow using venues, time slots, and reservations works correctly through ZTD shadow store on all platforms. The scenario exercises anti-join availability checking, status transition guards, and aggregate utilization reporting.

Verified patterns: LEFT JOIN anti-join to find available (unreserved) slots, prepared statement availability query with venue and time range filters, book-and-verify workflow (INSERT then confirm slot unavailable), status transitions with guards (pending → confirmed → cancelled, invalid transition rejected), cancel-and-rebook cycle (cancel frees slot, new booking takes it), venue utilization report (3-table JOIN with COUNT DISTINCT and CASE conditional aggregation), customer booking history via prepared 3-table JOIN, physical isolation.

## SPEC-10.2.59 Loyalty points system workflow
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/LoyaltyPointsTest`, `Pdo/MysqlLoyaltyPointsTest`, `Pdo/PostgresLoyaltyPointsTest`, `Pdo/SqliteLoyaltyPointsTest`

A loyalty points system using members, point transactions, and rewards catalog works correctly through ZTD shadow store on all platforms. The scenario exercises running balance calculations, tier promotion via aggregate thresholds, and earn/redeem transaction interleaving.

Verified patterns: SUM aggregate for member point balances, tier calculation using CASE expression on SUM aggregate, tier promotion (earn points then UPDATE tier), earn/redeem interleaving with balance verification after each transaction, HAVING-based qualification (find members qualifying for rewards by tier and balance), window function running total (SUM() OVER PARTITION BY ORDER BY), conditional aggregation earn-vs-redeem summary (CASE WHEN in SUM), DELETE transaction and verify balance recalculation, available rewards lookup via prepared statement with scalar subquery in WHERE, physical isolation.

## SPEC-10.2.60 Content versioning (draft/publish) workflow
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/ContentVersioningTest`, `Pdo/MysqlContentVersioningTest`, `Pdo/PostgresContentVersioningTest`, `Pdo/SqliteContentVersioningTest`

A content versioning system using articles and article versions works correctly through ZTD shadow store on all platforms. The scenario exercises latest-version lookup via correlated MAX subquery, status-based filtering, and version rollback.

Verified patterns: latest version per article using correlated MAX subquery in WHERE, status-based filtering (draft, published, archived), publish workflow (draft → published with guard), create new version and verify it becomes latest, version comparison (side-by-side query of specific versions), rollback (create new version from previous content), article summary report (JOIN with COUNT and MAX aggregate), prepared statement with correlated MAX subquery for author lookup, archive workflow (published → archived), physical isolation.

## SPEC-10.2.61 Shopping cart and checkout workflow
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/ShoppingCartCheckoutTest`, `Pdo/MysqlShoppingCartCheckoutTest`, `Pdo/PostgresShoppingCartCheckoutTest`, `Pdo/SqliteShoppingCartCheckoutTest`

A shopping cart and checkout workflow using products, cart items, orders, and order items works correctly through ZTD shadow store on all platforms. The scenario exercises multi-table JOIN with aggregate totals, multi-step checkout (calculate total, create order, transfer cart items, clear cart), stock management, and category-level revenue reporting.

Verified patterns: cart contents via JOIN with computed line totals (price × quantity), prepared statement cart total (SUM aggregate with JOIN), add-to-cart and verify updated total, multi-step checkout workflow (calculate → create order → create order items → delete cart), stock decrement via UPDATE after order, category revenue report (GROUP BY with multi-table JOIN), order status transitions with guards (pending → confirmed → shipped, invalid transition rejected), physical isolation.

## SPEC-10.2.62 Survey and poll results workflow
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/SurveyResultsTest`, `Pdo/MysqlSurveyResultsTest`, `Pdo/PostgresSurveyResultsTest`, `Pdo/SqliteSurveyResultsTest`

A survey and poll results workflow using surveys, questions, and responses works correctly through ZTD shadow store on all platforms. The scenario exercises response distribution analysis, percentage calculations via conditional aggregation, average ratings with platform-specific CAST, and correlated subqueries for completion statistics.

Verified patterns: response count per question (LEFT JOIN GROUP BY COUNT), response distribution (GROUP BY answer value), average rating with CAST (DECIMAL on MySQL, NUMERIC on PostgreSQL, REAL on SQLite), yes/no percentage via conditional aggregation (COUNT CASE WHEN / COUNT), survey completion rate with correlated subquery for question count, prepared statement filter by specific answer, submit response and verify in results, physical isolation.

## SPEC-10.2.63 Notification inbox workflow
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/NotificationInboxTest`, `Pdo/MysqlNotificationInboxTest`, `Pdo/PostgresNotificationInboxTest`, `Pdo/SqliteNotificationInboxTest`

A notification inbox workflow using users, notifications, and notification preferences works correctly through ZTD shadow store on all platforms. The scenario exercises batch UPDATE operations, unread count calculations, priority-based filtering, and multi-table JOIN with preference checks.

Verified patterns: unread count per user (LEFT JOIN with conditional COUNT), unread-by-priority breakdown (GROUP BY with conditional aggregation), mark single notification as read (UPDATE with guard), batch mark-all-as-read for user (UPDATE WHERE user_id AND is_read = 0), prepared statement notification lookup with priority filter, delete old read notifications (DELETE with date and status condition), user preference summary (JOIN with conditional aggregation on enabled/disabled), 3-table JOIN (notifications + users + preferences for push-enabled users with unread notifications), physical isolation.

## SPEC-10.2.64 Approval workflow with quorum
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/ApprovalWorkflowTest`, `Pdo/MysqlApprovalWorkflowTest`, `Pdo/PostgresApprovalWorkflowTest`, `Pdo/SqliteApprovalWorkflowTest`

A multi-step document approval workflow using documents, approvers, and approval rules works correctly through ZTD shadow store on all platforms. The scenario exercises quorum-based decision making, status transitions with guards, and multi-table JOIN verification.

Verified patterns: pending documents list (SELECT with LEFT JOIN approver COUNT), submit for approval (UPDATE draft→pending, INSERT approver assignments), approver decision recording (UPDATE approver decision, COUNT remaining pending), quorum check (3-table JOIN verifying approved_count >= min_approvals), document approval after quorum (UPDATE document status, 3-table JOIN verification), rejection override with require_unanimous flag (single rejection causes document rejection), physical isolation.

## SPEC-10.2.65 Leaderboard and ranking system
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/LeaderboardRankingTest`, `Pdo/MysqlLeaderboardRankingTest`, `Pdo/PostgresLeaderboardRankingTest`, `Pdo/SqliteLeaderboardRankingTest`

A gaming leaderboard with score tracking, ranking with ties, and position updates works correctly through ZTD shadow store on all platforms. The scenario exercises DENSE_RANK() window functions, score updates with history tracking, and tied ranking verification.

Verified patterns: leaderboard ranking via DENSE_RANK() OVER (ORDER BY score DESC), score update with history INSERT, top-N players via LIMIT with window function, player rank lookup via correlated subquery (COUNT DISTINCT higher scores), score history timeline (JOIN with ORDER BY date DESC), tied ranking verification (same score = same rank, consecutive next rank via DENSE_RANK), physical isolation.

**Note:** Prepared statements with derived tables containing window functions return empty results on MySQL (MySQLi, MySQL-PDO) and SQLite-PDO. PostgreSQL-PDO works correctly. Workaround: use correlated subqueries or `query()` instead.

## SPEC-10.2.66 Configuration cascade with priority overrides
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/ConfigurationCascadeTest`, `Pdo/MysqlConfigurationCascadeTest`, `Pdo/PostgresConfigurationCascadeTest`, `Pdo/SqliteConfigurationCascadeTest`

A hierarchical key-value configuration system with defaults, environment-specific overrides, and priority-based resolution works correctly through ZTD shadow store on all platforms. The scenario exercises LEFT JOIN with COALESCE for fallback resolution, correlated subqueries for highest-priority lookup, and dynamic override management.

Verified patterns: default config lookup (basic key-value retrieval), override resolution (LEFT JOIN + COALESCE picks override when present), environment-specific config (prepared statement with environment filter), priority-based override (correlated subquery ORDER BY priority DESC LIMIT 1), add override (INSERT and verify via JOIN+COALESCE), remove override restores default (DELETE and verify via LEFT JOIN), effective config report (all keys with resolved values using COALESCE + correlated subquery).

## SPEC-10.2.67 Audit trail with change history
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/AuditTrailTest`, `Pdo/MysqlAuditTrailTest`, `Pdo/PostgresAuditTrailTest`, `Pdo/SqliteAuditTrailTest`

An audit trail system with change logging, point-in-time state reconstruction, and revert capabilities works correctly through ZTD shadow store on all platforms. The scenario exercises INSERT-per-change logging, correlated MAX subquery for latest state, and aggregation-based reporting.

Verified patterns: log INSERT (product + audit record pair), log UPDATE with old/new values, change history for record (ORDER BY changed_at), latest state from audit log (correlated subquery with MAX(id)), count changes by action (GROUP BY action COUNT), revert to old value (read audit_log old_value, UPDATE product), physical isolation.

## SPEC-10.2.68 Waitlist queue with priority ordering
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/WaitlistQueueTest`, `Pdo/MysqlWaitlistQueueTest`, `Pdo/PostgresWaitlistQueueTest`, `Pdo/SqliteWaitlistQueueTest`

A waitlist queue system with priority ordering, promotion, capacity limits, and position calculations works correctly through ZTD shadow store on all platforms. The scenario exercises ROW_NUMBER() window functions with PARTITION BY, conditional UPDATE with capacity guards, and priority-based queue ordering.

Verified patterns: waitlist position by priority (ROW_NUMBER() OVER PARTITION BY event ORDER BY priority, joined_at), join waitlist (INSERT and verify position), promote from waitlist (UPDATE status to promoted, increment enrolled_count, verify capacity), cancel and shift positions (UPDATE to cancelled, verify remaining positions renumber), capacity guard (COUNT enrolled vs capacity prevents over-enrollment), priority queue ordering (ORDER BY priority ASC, joined_at ASC across multiple levels), physical isolation.

## SPEC-10.2.69 Coupon and discount system
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/CouponDiscountTest`, `Pdo/MysqlCouponDiscountTest`, `Pdo/PostgresCouponDiscountTest`, `Pdo/SqliteCouponDiscountTest`

A coupon code validation and discount system with usage tracking, expiry checking, and usage limits works correctly through ZTD shadow store on all platforms. The scenario exercises date-range filtering, LEFT JOIN with GROUP BY for usage counting, and conditional aggregation for discount reporting.

Verified patterns: valid coupon lookup (prepared statement with code + is_active filter), usage limit check (LEFT JOIN with GROUP BY, compare COUNT vs max_uses), apply coupon (INSERT usage record, verify discount calculation), expired coupon exclusion (date-range condition), usage summary per coupon (COUNT + SUM aggregate GROUP BY), deactivate coupon (UPDATE is_active = 0, verify exclusion), physical isolation.

## SPEC-10.2.70 Subscription billing cycle
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/SubscriptionBillingTest`, `Pdo/MysqlSubscriptionBillingTest`, `Pdo/PostgresSubscriptionBillingTest`, `Pdo/SqliteSubscriptionBillingTest`

A subscription billing workflow using subscriptions, billing records, and credits works correctly through ZTD shadow store on all platforms. The scenario exercises recurring billing record management, credit application, balance calculations via COALESCE with correlated subqueries, conditional aggregation for paid/unpaid tracking, and prepared statement date-range filtering.

Verified patterns: active subscriptions with billing summary (LEFT JOIN + GROUP BY with CASE-based paid/unpaid aggregation), billing record generation (INSERT + SUM verification), balance with credits (correlated subqueries with COALESCE for unpaid billing minus credits), credit application (INSERT + SUM verification), mark billing paid (UPDATE with conditional aggregation verification), billing history prepared statement (3 parameters: subscription_id + date range), physical isolation.

**Note:** Derived table approach (LEFT JOIN with subqueries in FROM) returns empty results on MySQL (MySQLi, MySQL-PDO). Workaround: use correlated subqueries in SELECT instead.

## SPEC-10.2.71 Financial ledger with reversals
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/FinancialLedgerTest`, `Pdo/MysqlFinancialLedgerTest`, `Pdo/PostgresFinancialLedgerTest`, `Pdo/SqliteFinancialLedgerTest`

A double-entry accounting ledger with debit/credit entries, balance calculations, reversal entries, and transaction summaries works correctly through ZTD shadow store on all platforms. The scenario exercises DECIMAL precision through the shadow store, SUM with CASE-based sign logic for debit/credit accounting, and paired entry validation.

Verified patterns: account balances via SUM with CASE debit/credit sign multiplier (LEFT JOIN + GROUP BY), paired transaction entry recording (INSERT pairs + COUNT/SUM verification), double-entry integrity check (SUM of debits equals SUM of credits), reversal entries with balance recalculation (INSERT reversal pair, verify adjusted balances), account statement prepared statement (date range filter), transaction-level summary (GROUP BY transaction_ref with COUNT/SUM/MIN), physical isolation.

## SPEC-10.2.72 Inventory allocation with reservations
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/InventoryAllocationTest`, `Pdo/MysqlInventoryAllocationTest`, `Pdo/PostgresInventoryAllocationTest`, `Pdo/SqliteInventoryAllocationTest`

An inventory management system with stock tracking, reservation management, sale conversion, and warehouse reporting works correctly through ZTD shadow store on all platforms. The scenario exercises self-referencing arithmetic (available = total - reserved - sold), coordinated multi-table mutations, and multi-table JOIN with aggregate reporting.

Verified patterns: available stock calculation (total_stock - reserved - sold expression), stock reservation (INSERT reservation + UPDATE reserved count), reservation-to-sale conversion (UPDATE reservation status + adjust reserved/sold counts, verify available unchanged), cancel reservation (UPDATE status + restore reserved count), warehouse stock report (LEFT JOIN with GROUP BY and SUM aggregates across warehouses), SKU lookup prepared statement (JOIN with computed available column), physical isolation.

## SPEC-10.2.73 Order fulfillment workflow
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/OrderFulfillmentTest`, `Pdo/MysqlOrderFulfillmentTest`, `Pdo/PostgresOrderFulfillmentTest`, `Pdo/SqliteOrderFulfillmentTest`

A multi-stage order processing system with line items and fulfillment tracking works correctly through ZTD shadow store on all platforms. The scenario exercises partial vs complete fulfillment detection, status transition guards, conditional aggregation for order summary reporting, and multi-table JOIN with computed order values.

Verified patterns: order summary list (LEFT JOIN + GROUP BY with SUM(quantity * unit_price)), single item fulfillment (INSERT fulfillment + UPDATE item status with affected-row check), partial fulfillment detection (CASE-based conditional COUNT for shipped/pending items), complete fulfillment with order status transition (all items shipped → UPDATE order status), cross-order fulfillment report (conditional aggregation across orders), order lookup prepared statement (customer name filter with JOIN + GROUP BY), physical isolation.

## SPEC-10.2.74 Sales reporting with pivot aggregation
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/SalesReportTest`, `Pdo/MysqlSalesReportTest`, `Pdo/PostgresSalesReportTest`, `Pdo/SqliteSalesReportTest`

A sales reporting system with regional breakdowns, product metrics, pivot-style quarterly reports, and net sales adjusted for returns works correctly through ZTD shadow store on all platforms. The scenario exercises CASE-based conditional SUM for pivot reports, GROUP BY with HAVING threshold filtering, and net quantity calculations with return adjustments.

Verified patterns: sales by region (LEFT JOIN + GROUP BY with CASE-filtered SUM), sales by product (JOIN + GROUP BY with ORDER BY computed revenue DESC), pivot by quarter (CASE WHEN quarter = 'Q1' THEN ... conditional SUM columns), net sales with returns (CASE WHEN sale_type = 'return' THEN negative quantity adjustment), region filter with HAVING threshold (GROUP BY HAVING SUM > threshold), region sales prepared statement (region_id + sale_type parameters), physical isolation.

## SPEC-10.2.75 Retry queue with job processing
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/RetryQueueTest`, `Pdo/MysqlRetryQueueTest`, `Pdo/PostgresRetryQueueTest`, `Pdo/SqliteRetryQueueTest`

A job processing queue with priority ordering, retry logic, completion logging, and metrics reporting works correctly through ZTD shadow store on all platforms. The scenario exercises state machine transitions (pending → processing → completed/failed), retry count management with self-referencing arithmetic, priority-based queue ordering, and multi-table JOIN for job history.

Verified patterns: pending jobs by priority (ORDER BY priority ASC, created_at ASC), start processing (UPDATE status with affected-row guard + INSERT log), complete job (UPDATE status + INSERT completion log with JOIN verification), fail and retry (UPDATE retry_count + 1, reset status to pending), max retries exceeded (mark as permanently failed), job metrics (GROUP BY status with COUNT/MAX aggregates), job history prepared statement (LEFT JOIN with GROUP BY for log count per job by type), physical isolation.

## SPEC-10.2.76 Employee shift scheduling with overlap detection
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/EmployeeSchedulingTest`, `Pdo/MysqlEmployeeSchedulingTest`, `Pdo/PostgresEmployeeSchedulingTest`, `Pdo/SqliteEmployeeSchedulingTest`

An employee scheduling system with shift assignments, overlap detection, shift swapping, and department coverage reporting works correctly through ZTD shadow store on all platforms. The scenario exercises prepared statements with date range BETWEEN for lookup, multi-condition WHERE for overlap detection, UPDATE of two rows for shift swapping, and GROUP BY with HAVING for coverage checks.

Verified patterns: list shifts by date range (prepared BETWEEN with JOIN), assign shift (INSERT + JOIN verification), schedule conflict detection (WHERE employee_id = ? AND shift_date = ? AND start_time < ? AND end_time > ?), swap shifts between employees (UPDATE two rows, verify both changed), department coverage report (COUNT GROUP BY department HAVING >= 1), cancel and reassign (DELETE + INSERT, verify count unchanged), physical isolation.

## SPEC-10.2.77 Gift card balance tracking with prepare-once/execute-many
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/GiftCardRedemptionTest`, `Pdo/MysqlGiftCardRedemptionTest`, `Pdo/PostgresGiftCardRedemptionTest`, `Pdo/SqliteGiftCardRedemptionTest`

A gift card system with balance tracking, redemption, reload, and expiry management works correctly through ZTD shadow store on all platforms. The scenario exercises prepared statement reuse (prepare once, execute for multiple cards), self-referencing arithmetic in UPDATE (balance = balance - amount), and CASE-based aggregation for balance ranges.

Verified patterns: balance check with prepared reuse (SELECT WHERE code = ? executed for multiple codes), card redemption (UPDATE balance = balance - amount WHERE balance >= amount + INSERT transaction), card reload (UPDATE balance = balance + amount + INSERT transaction), transaction history (JOIN + ORDER BY DESC with prepared param), expire unused cards (UPDATE WHERE current_balance = initial_balance AND status = 'active'), balance summary (SUM GROUP BY status with CASE ranges), physical isolation.

## SPEC-10.2.78 Product catalog with category hierarchy
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/ProductCatalogTest`, `Pdo/MysqlProductCatalogTest`, `Pdo/PostgresProductCatalogTest`, `Pdo/SqliteProductCatalogTest`

A product catalog with hierarchical categories, faceted counts, price filtering, and stock alerts works correctly through ZTD shadow store on all platforms. The scenario exercises self-JOIN for parent-child hierarchy, LEFT JOIN with GROUP BY for faceted counts, prepared BETWEEN for price range filtering, and multi-condition WHERE for low stock alerts.

Verified patterns: category tree self-join (LEFT JOIN categories c2 ON c1.id = c2.parent_id, COUNT children), products in category (prepared JOIN with category_id param), faceted counts (LEFT JOIN + GROUP BY for product count per category), price range filter (prepared BETWEEN with ORDER BY), low stock alert (WHERE stock_qty < threshold with JOIN for category context), update category name (verify JOIN reflects new name), physical isolation.

## SPEC-10.2.79 Email campaign delivery tracking
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/EmailCampaignTest`, `Pdo/MysqlEmailCampaignTest`, `Pdo/PostgresEmailCampaignTest`, `Pdo/SqliteEmailCampaignTest`

An email campaign system with delivery tracking, open rate calculations, bounce handling, and campaign comparison works correctly through ZTD shadow store on all platforms. The scenario exercises batch INSERT for recipients, CASE-based conditional COUNT for delivery metrics, percentage calculations via COUNT ratios, and LEFT JOIN for campaign comparison.

Verified patterns: campaign overview (COUNT with CASE for delivered/bounced/pending), send campaign (UPDATE status + batch INSERT recipients), track delivery (UPDATE delivery_status for multiple recipients), open rate calculation (COUNT(opened_at) / COUNT(*) via CASE GROUP BY campaign), bounce handling (UPDATE to bounced, verify metric changes), campaign comparison (LEFT JOIN with aggregate metrics side by side), physical isolation.

## SPEC-10.2.80 Time tracking with billable hours aggregation
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/TimeTrackingTest`, `Pdo/MysqlTimeTrackingTest`, `Pdo/PostgresTimeTrackingTest`, `Pdo/SqliteTimeTrackingTest`

A time tracking system with billable hours, client invoicing, budget tracking, and employee reporting works correctly through ZTD shadow store on all platforms. The scenario exercises SUM with GROUP BY across 3-table JOINs, HAVING for over-budget detection, prepared BETWEEN for date-range employee reports, and UPDATE for billable flag changes.

Verified patterns: billable hours by project (SUM WHERE billable=1 GROUP BY project with JOIN), client invoice summary (3-table JOIN clients+projects+time_entries, SUM(hours * rate_per_hour) GROUP BY client), over-budget detection (SUM(hours) HAVING > budget_hours via JOIN), employee weekly hours (prepared BETWEEN + GROUP BY employee), add time entry (INSERT + verify SUM changes), mark non-billable (UPDATE billable=0, verify SUM decreases), physical isolation.

## SPEC-10.2.81 Warranty claim state machine
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/WarrantyClaimTest`, `Pdo/MysqlWarrantyClaimTest`, `Pdo/PostgresWarrantyClaimTest`, `Pdo/SqliteWarrantyClaimTest`

A warranty claim system with product registration, validity checking, claim filing, and multi-step status transitions works correctly through ZTD shadow store on all platforms. The scenario exercises cross-table validation (JOIN purchase+product for warranty period check), platform-specific date arithmetic, and conditional UPDATE with status guards.

Verified patterns: file warranty claim (INSERT + 3-table JOIN verification), warranty validity check (date arithmetic: MySQL DATE_ADD, PostgreSQL interval, SQLite date() function), approve claim with guard (UPDATE WHERE status='filed', affected=1), reject claim (UPDATE status + set resolved_date), claim status report (COUNT CASE GROUP BY product), resolve claim (full lifecycle: filed → resolved with date), physical isolation.

## SPEC-10.2.82 Class enrollment with capacity and prerequisites
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/ClassEnrollmentTest`, `Pdo/MysqlClassEnrollmentTest`, `Pdo/PostgresClassEnrollmentTest`, `Pdo/SqliteClassEnrollmentTest`

A class enrollment system with capacity limits, prerequisite validation, waitlist management, and promotion works correctly through ZTD shadow store on all platforms. The scenario exercises COUNT for capacity checking, EXISTS subquery for prerequisite validation, ORDER BY with LIMIT subquery for waitlist promotion, and multi-condition WHERE for roster queries.

Verified patterns: enroll student (INSERT + JOIN verification), capacity check (COUNT WHERE status='enrolled' vs max_capacity), prerequisite validation (EXISTS subquery checking completed enrollment in prerequisite course), waitlist on full (INSERT with status='waitlisted'), promote from waitlist (UPDATE first waitlisted by enrolled_at ORDER BY), course roster (JOIN WHERE status='enrolled' ORDER BY name), physical isolation.

## SPEC-10.2.83 Property listing with faceted search
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/PropertyListingTest`, `Pdo/MysqlPropertyListingTest`, `Pdo/PostgresPropertyListingTest`, `Pdo/SqlitePropertyListingTest`

A property listing system with city-based search, price range filtering, multi-condition queries, pagination, and aggregate statistics works correctly through ZTD shadow store on all platforms. The scenario exercises prepared statements with multiple filter parameters, BETWEEN for price ranges, LIMIT/OFFSET for pagination, and GROUP BY with COUNT for listing statistics.

Verified patterns: search by city (prepared WHERE city = ?), price range filter (prepared BETWEEN with ORDER BY price), multi-filter search (WHERE city = ? AND bedrooms >= ? AND price <= ?), paginated results (LIMIT/OFFSET, verify page sizes), listing counts by city (COUNT GROUP BY city ORDER BY count DESC), update listing price (UPDATE + verify via SELECT), physical isolation.

## SPEC-10.2.84 Document tagging with many-to-many search
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/DocumentTaggingTest`, `Pdo/MysqlDocumentTaggingTest`, `Pdo/PostgresDocumentTaggingTest`, `Pdo/SqliteDocumentTaggingTest`

A document tagging system with many-to-many relationships, tag cloud aggregation, intersection search, and untagged document detection works correctly through ZTD shadow store on all platforms. The scenario exercises 3-table JOINs through junction tables, HAVING COUNT(DISTINCT) for "all tags" matching, LEFT JOIN with IS NULL for anti-join, and GROUP BY with ORDER BY for tag cloud.

Verified patterns: tag document (INSERT junction + JOIN verification), documents by tag (3-table JOIN with prepared tag name), tag cloud aggregation (COUNT GROUP BY tag ORDER BY count DESC), documents with all tags (HAVING COUNT(DISTINCT tag_id) = N for intersection), remove tag (DELETE junction + verify count), untagged documents (LEFT JOIN WHERE IS NULL), physical isolation.

## SPEC-10.2.85 Auction bidding with highest bid tracking
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/AuctionBiddingTest`, `Pdo/MysqlAuctionBiddingTest`, `Pdo/PostgresAuctionBiddingTest`, `Pdo/SqliteAuctionBiddingTest`

An auction bidding system with bid placement, highest bid tracking, auction closure, and bidder statistics works correctly through ZTD shadow store on all platforms. The scenario exercises MAX subquery for current highest bid, INSERT with UPDATE for price tracking, JOIN with ORDER BY DESC for bid history, and COUNT(DISTINCT) with HAVING for active bidder stats.

Verified patterns: place bid (INSERT bid + UPDATE current_price), highest bid (prepared SELECT MAX(bid_amount) WHERE auction_id = ?), bid history (JOIN ORDER BY bid_amount DESC with prepared param), auction summary (COUNT + MAX + MIN per auction via JOIN), close auction (UPDATE status, verify winner via MAX JOIN), active bidder stats (COUNT(DISTINCT bidder_name) GROUP BY auction HAVING > 1), physical isolation.

## SPEC-10.2.86 Recipe ingredient scaling and aggregation
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/RecipeIngredientTest`, `Pdo/MysqlRecipeIngredientTest`, `Pdo/PostgresRecipeIngredientTest`, `Pdo/SqliteRecipeIngredientTest`

A recipe system with ingredient management, quantity scaling, shopping list aggregation, and substitution lookup works correctly through ZTD shadow store on all platforms. The scenario exercises arithmetic expressions in SELECT (quantity * factor), SUM GROUP BY for shopping list aggregation across multiple recipes, and LEFT JOIN for optional substitution lookup.

Verified patterns: recipe ingredients (prepared JOIN with recipe_id), scaled quantities (SELECT quantity * 2 for double servings), shopping list aggregation (SUM(quantity) GROUP BY item across multiple recipes), available substitutions (LEFT JOIN substitutions, show items with/without alternatives), add ingredient (INSERT + verify count), ingredient count per recipe (COUNT GROUP BY recipe with LEFT JOIN), physical isolation.

## SPEC-10.2.87 Project milestone tracking with completion metrics
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/ProjectMilestoneTest`, `Pdo/MysqlProjectMilestoneTest`, `Pdo/PostgresProjectMilestoneTest`, `Pdo/SqliteProjectMilestoneTest`

A project milestone tracking system with completion percentages, overdue detection, deadline monitoring, and risk assessment works correctly through ZTD shadow store on all platforms. The scenario exercises conditional COUNT for completion percentage calculation, date comparison for overdue detection, prepared BETWEEN for deadline range queries, and HAVING with CASE for risk assessment.

Verified patterns: project overview (JOIN + COUNT total and completed milestones), completion percentage (COUNT CASE completed * 100 / COUNT(*) GROUP BY project), overdue milestones (WHERE due_date < current AND status != completed with JOIN), complete milestone (UPDATE status + set completed_date, verify percentage changes), upcoming deadlines (prepared BETWEEN date range ORDER BY due_date), project at risk (HAVING COUNT CASE overdue > 0), physical isolation.

## SPEC-10.2.88 Correlated UPDATE with scalar subquery
**Status:** Partially Verified
**Platforms:** MySQLi (V), MySQL-PDO (V), PostgreSQL-PDO (K), SQLite-PDO (K)
**Tests:** `Mysqli/CorrelatedUpdateTest`, `Pdo/MysqlCorrelatedUpdateTest`, `Pdo/PostgresCorrelatedUpdateTest`, `Pdo/SqliteCorrelatedUpdateTest`

Correlated subqueries in SELECT work on all platforms. However, correlated UPDATE with scalar subquery in SET clause (`UPDATE t1 SET col = (SELECT AGG(col) FROM t2 WHERE t2.fk = t1.id)`) works on MySQL but fails on PostgreSQL (CTE rewriter grouping error) and SQLite (CTE rewriter syntax error). Workaround: query the computed values first, then update each row with explicit values.

Verified patterns (all platforms): correlated subquery in SELECT, DELETE with NOT EXISTS correlated subquery, UPDATE WHERE IN (subquery), correlated COUNT in SELECT. Known issue (PostgreSQL, SQLite): UPDATE SET col = (correlated scalar subquery), chained correlated updates.

## SPEC-10.2.89 NULL handling and COALESCE chains
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/NullCoalescingTest`, `Pdo/MysqlNullCoalescingTest`, `Pdo/PostgresNullCoalescingTest`, `Pdo/SqliteNullCoalescingTest`

NULL handling through ZTD shadow store works correctly on all platforms. The scenario exercises COALESCE with multiple fallback values, IS NULL / IS NOT NULL comparisons, LEFT JOIN with NULL columns, COUNT(*) vs COUNT(column) difference with NULLs, UPDATE SET to NULL, and CASE WHEN for conditional NULL replacement.

Verified patterns: COALESCE chain (3-value fallback), NULL-safe comparison (IS NULL count), LEFT JOIN with NULL keys (right side no match), NULL in aggregation (COUNT(*) vs COUNT(col), AVG ignoring NULL), UPDATE to NULL (SET col = NULL, verify IS NULL), conditional NULL replacement (CASE WHEN equivalent of NULLIF), physical isolation.

## SPEC-10.2.90 Multi-step ETL with INSERT SELECT
**Status:** Partially Verified
**Platforms:** MySQLi (V), MySQL-PDO (V), PostgreSQL-PDO (P), SQLite-PDO (P)
**Tests:** `Mysqli/MultiStepEtlTest`, `Pdo/MysqlMultiStepEtlTest`, `Pdo/PostgresMultiStepEtlTest`, `Pdo/SqliteMultiStepEtlTest`

Multi-step ETL workflows using INSERT SELECT with GROUP BY aggregation work correctly on all platforms. Incremental loads, DELETE + recalculate, and cross-table consistency verification all work. However, correlated UPDATE for recalculation (`UPDATE summary SET total = (SELECT SUM(amount) FROM raw WHERE raw.region = summary.region)`) fails on PostgreSQL and SQLite (same limitation as SPEC-10.2.88). Workaround: query aggregates first, then update with explicit values.

Verified patterns: INSERT SELECT with GROUP BY (aggregate into summary table), verify summary data correctness, incremental load (additional INSERTs + new summary rows), delete and recalculate (DELETE raw + update summary), cross-table consistency (SUM raw = SUM summary). Known issue (PostgreSQL, SQLite): correlated UPDATE for recalculation.

## SPEC-10.2.91 Large IN lists and batch operations
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/LargeInListTest`, `Pdo/MysqlLargeInListTest`, `Pdo/PostgresLargeInListTest`, `Pdo/SqliteLargeInListTest`

SELECT, UPDATE, and DELETE with large IN lists (20+ items) work correctly through ZTD shadow store on all platforms. NOT IN exclusion and string-valued IN lists also work. Prepared statements with multiple parameters of different types function correctly.

Verified patterns: SELECT with 20-item IN list, UPDATE with IN list (8 items, verify affected count), DELETE with IN list (5 items, verify remaining count), NOT IN exclusion, IN list with string values (category names), prepared statement with 3 mixed-type parameters, physical isolation.

## SPEC-10.2.92 UNION and UNION ALL queries
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/UnionQueryTest`, `Pdo/MysqlUnionQueryTest`, `Pdo/PostgresUnionQueryTest`, `Pdo/SqliteUnionQueryTest`

UNION ALL and UNION (distinct) queries combining results from multiple shadow tables work correctly through ZTD on all platforms. UNION with WHERE filters, ORDER BY, and LIMIT all function correctly. Shadow mutations (INSERT, DELETE) are properly reflected in subsequent UNION queries.

Verified patterns: UNION ALL combining two tables (verify full row count), UNION distinct (deduplication of identical rows), UNION ALL with WHERE filter on each side, UNION ALL with ORDER BY and LIMIT, INSERT then verify UNION includes new row, DELETE then verify UNION excludes deleted row, physical isolation.

## SPEC-10.2.93 String manipulation functions through ZTD
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/StringManipulationTest`, `Pdo/MysqlStringManipulationTest`, `Pdo/PostgresStringManipulationTest`, `Pdo/SqliteStringManipulationTest`

String functions work correctly through ZTD CTE rewriter on all platforms. Concatenation uses platform-appropriate syntax (CONCAT() for MySQL/PostgreSQL, || for SQLite). UPPER(), LOWER(), LENGTH(), TRIM(), SUBSTR(), and REPLACE() all produce correct results on shadow-stored data.

Verified patterns: string concatenation (platform-specific: CONCAT or ||), UPPER/LOWER case conversion with prepared statement, LENGTH function with ORDER BY, TRIM leading/trailing spaces from shadow-inserted data, SUBSTR extraction, REPLACE character substitution, physical isolation.

## SPEC-10.2.94 Sequential INSERT batch with mixed DML visibility
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/BatchInsertPatternTest`, `Pdo/MysqlBatchInsertPatternTest`, `Pdo/PostgresBatchInsertPatternTest`, `Pdo/SqliteBatchInsertPatternTest`

Sequential INSERT operations followed by SELECT, UPDATE, DELETE, and aggregation all work correctly through ZTD shadow store on all platforms. The scenario tests starting from an empty table (no seed data in setUp), verifying that shadow-only data is immediately queryable, filterable, and mutable.

Verified patterns: sequential INSERTs (10 rows, verify COUNT), INSERT then filter (WHERE level = 'ERROR'), INSERT then aggregate (COUNT GROUP BY level), INSERT + UPDATE + read (verify both original and updated visible), INSERT + DELETE + read (verify remaining count), mixed DML sequence (INSERT 5, UPDATE 2, DELETE 1, INSERT 3 more — verify final state), physical isolation.

## SPEC-10.2.95 ZTD enable/disable toggle mid-session
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/ZtdTogglePatternTest`, `Pdo/MysqlZtdTogglePatternTest`, `Pdo/PostgresZtdTogglePatternTest`, `Pdo/SqliteZtdTogglePatternTest`

ZTD enable/disable toggle behavior is consistent across all platforms. Shadow inserts are visible when ZTD is enabled, invisible in physical queries when ZTD is disabled, and persist across disable/re-enable cycles. Physical data inserted with ZTD disabled is visible in physical queries but not through ZTD (since ZTD replaces the physical table view with the shadow store). Shadow updates survive toggle cycles.

Verified patterns: shadow insert visible in ZTD (not in physical table), physical data not visible through ZTD (shadow store replaces physical view), disable ZTD sees physical only (3 physical rows, not shadow), re-enable restores shadow data, physical insert visibility (visible physically, not through ZTD), shadow update survives toggle (UPDATE persists after disable+re-enable), physical isolation.

## SPEC-10.2.96 Chained user-written CTEs
**Status:** Partially Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO (Known Issue), SQLite-PDO (Partially)
**Tests:** `Mysqli/ChainedUserCteTest`, `Pdo/MysqlChainedUserCteTest`, `Pdo/PostgresChainedUserCteTest`, `Pdo/SqliteChainedUserCteTest`

Multiple user-written CTEs chained together (`WITH cte1 AS (...), cte2 AS (... FROM cte1) SELECT * FROM cte2`) work correctly on MySQL (MySQLi, MySQL-PDO) and SQLite when the outer SELECT only references user CTEs. Two-CTE chains, three-CTE chains, prepared statements with chained CTEs, and post-insert visibility all work.

**Known limitation (all platforms):** When the outer SELECT joins a user CTE back to the original physical table (`FROM table s JOIN user_cte t ON ...`), results may be empty. The CTE rewriter may conflict when the outer query references both a physical table and a user-defined CTE simultaneously. See [SPEC-11.CTE-JOIN-BACK](#spec-11cte-join-back).

**PostgreSQL:** All chained CTE tests are affected by [SPEC-11.PG-CTE](11-known-issues.ears.md) — user CTEs read from the physical table (empty), returning 0 rows.

Verified patterns: two-CTE chain (regional totals + ranking), three-CTE chain (product totals + filter + running sum), chained CTE with prepared statement and WHERE filter, chained CTE after INSERT sees new data, physical isolation.

## SPEC-10.2.97 Row value (tuple) comparisons
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/RowValueComparisonTest`, `Pdo/MysqlRowValueComparisonTest`, `Pdo/PostgresRowValueComparisonTest`, `Pdo/SqliteRowValueComparisonTest`

Row value comparisons for composite key lookups work correctly through ZTD on all platforms. `WHERE (col1, col2) IN ((v1, v2), (v3, v4))`, `WHERE (col1, col2) = (v1, v2)`, and `WHERE (col1, col2) > (v1, v2)` all produce correct results on shadow-stored data. Prepared statements with row value parameters also work.

Verified patterns: row value IN list with multiple tuples, single tuple equality, prepared params with tuple comparison, row value greater-than (lexicographic ordering), row value query after UPDATE reflects mutation, physical isolation.

## SPEC-10.2.98 SQL keywords in string data
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/SqlKeywordInDataTest`, `Pdo/MysqlSqlKeywordInDataTest`, `Pdo/PostgresSqlKeywordInDataTest`, `Pdo/SqliteSqlKeywordInDataTest`

String values containing SQL reserved keywords (SELECT, DROP, INSERT, UPDATE, DELETE, FROM, WHERE, GROUP BY, ORDER BY, TRUNCATE TABLE) are stored and retrieved correctly through ZTD shadow store. The CTE rewriter's SQL parser correctly distinguishes keywords inside string literals from actual SQL syntax.

Verified patterns: INSERT and SELECT with keyword-containing values, LIKE filter on keyword-containing data, prepared statement with keyword value, UPDATE to keyword-containing value, storing a complete SQL statement as a string literal, physical isolation.

## SPEC-10.2.99 Triple and quadruple self-joins
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/TripleSelfJoinTest`, `Pdo/MysqlTripleSelfJoinTest`, `Pdo/PostgresTripleSelfJoinTest`, `Pdo/SqliteTripleSelfJoinTest`

Self-joining the same table 3 or 4 times with different aliases works correctly through ZTD on all platforms. The CTE rewriter rewrites all table references regardless of how many times the same table appears. Correlated subqueries referencing the same table also work.

Verified patterns: triple self-join (employee → manager → grand-manager), quadruple self-join (4 hierarchy levels), self-join with GROUP BY and COUNT aggregate, correlated subquery counting direct reports from same table, self-join after INSERT mutation, physical isolation.

## SPEC-10.2.100 Empty string vs NULL distinction
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/EmptyStringVsNullTest`, `Pdo/MysqlEmptyStringVsNullTest`, `Pdo/PostgresEmptyStringVsNullTest`, `Pdo/SqliteEmptyStringVsNullTest`

The ZTD shadow store correctly preserves the distinction between empty strings (`''`) and NULL values on all platforms. `IS NULL` returns only true NULLs, `IS NOT NULL` includes empty strings, `LENGTH('')` returns 0 while `LENGTH(NULL)` returns NULL, and `COALESCE` falls through NULL but not empty strings.

Verified patterns: IS NOT NULL includes empty strings, IS NULL excludes empty strings, LENGTH/CHAR_LENGTH on empty vs NULL, COALESCE distinguishes empty from NULL, UPDATE from empty to NULL and back, prepared statement with empty string parameter, physical isolation.

## SPEC-10.2.101 Unicode and multi-byte character data
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/UnicodeDataTest`, `Pdo/MysqlUnicodeDataTest`, `Pdo/PostgresUnicodeDataTest`, `Pdo/SqliteUnicodeDataTest`

Multi-byte Unicode strings (German umlauts, Japanese CJK, Spanish accents, Irish apostrophes, Russian Cyrillic) are stored and retrieved correctly through ZTD shadow store on all platforms. The CTE rewriter's string literal embedding preserves multi-byte characters. MySQL requires `CHARACTER SET utf8mb4` on the table; PostgreSQL and SQLite handle UTF-8 natively.

Verified patterns: Unicode data INSERT and SELECT round-trip, WHERE equality with Unicode value, LIKE with Unicode substring, prepared statement with Unicode parameter, UPDATE to different Unicode text, CHAR_LENGTH/LENGTH counts characters not bytes for multi-byte strings, physical isolation.

## SPEC-10.2.102 Conditional aggregation without GROUP BY
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/ConditionalAggregateNoGroupByTest`, `Pdo/MysqlConditionalAggregateNoGroupByTest`, `Pdo/PostgresConditionalAggregateNoGroupByTest`, `Pdo/SqliteConditionalAggregateNoGroupByTest`

Whole-table aggregation without GROUP BY works correctly through ZTD on all platforms. COUNT(*), SUM(), AVG(), MIN(), MAX() all return correct results on shadow data. Conditional COUNT with CASE expressions, SUM with positive/negative CASE, and HAVING without GROUP BY (valid SQL for single-group filtering) all work.

Verified patterns: whole-table COUNT/SUM/AVG, conditional COUNT with CASE WHEN, SUM with CASE for balance calculation, HAVING without GROUP BY (match and no-match), multiple aggregate functions with WHERE filter, aggregate after INSERT sees new data, physical isolation.

## SPEC-10.2.103 Polymorphic comments with type discriminator
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/PolymorphicCommentTest`, `Pdo/MysqlPolymorphicCommentTest`, `Pdo/PostgresPolymorphicCommentTest`, `Pdo/SqlitePolymorphicCommentTest`

A polymorphic comment system using type+id discriminator pattern (common in Laravel/Eloquent ORMs) works correctly through ZTD shadow store on all platforms. The scenario uses a `comments` table with `commentable_type` and `commentable_id` columns referencing multiple parent entity types (posts, photos). This exercises LEFT JOIN with conditional type matching, CASE expression for parent entity resolution, conditional aggregation across entity types, anti-join for untagged entities, and cross-type commenter statistics.

Verified patterns: query comments by type+id (prepared statement), JOIN comments with parent entity via CASE on type (LEFT JOIN posts + LEFT JOIN photos), comment count by entity type (GROUP BY type), comment count per specific entity (LEFT JOIN with type filter), add comment (INSERT + JOIN verification), delete comment (DELETE + count verification), entities with no comments (anti-join via LEFT JOIN WHERE IS NULL), most active commenters with per-type breakdown (conditional COUNT with CASE), physical isolation.

## SPEC-10.2.104 Data archival workflow with INSERT SELECT
**Status:** Partially Verified
**Platforms:** MySQLi (V), MySQL-PDO (V), PostgreSQL-PDO (V), SQLite-PDO (P)
**Tests:** `Mysqli/DataArchivalTest`, `Pdo/MysqlDataArchivalTest`, `Pdo/PostgresDataArchivalTest`, `Pdo/SqliteDataArchivalTest`

A data archival workflow moving completed orders from an active table to an archive table using INSERT SELECT works on all platforms with known SQLite limitations. The scenario exercises INSERT SELECT for bulk archival, DELETE after archival, cross-table UNION ALL queries for combined reporting, and read-your-writes consistency across both tables.

**SQLite limitation:** Literal values in INSERT SELECT (e.g., `'2026-03-09'` as archived_at) become NULL in the shadow store ([SPEC-11.INSERT-SELECT-COMPUTED](11-known-issues.ears.md)). UNION ALL in derived tables also returns empty on SQLite ([SPEC-11.BARE-SUBQUERY-REWRITE](11-known-issues.ears.md)). Workaround: query each table separately and combine in application code.

Verified patterns: INSERT SELECT with literal column (archived_at), delete archived rows from active table, archive summary verification, customer history across active + archived tables (UNION ALL with prepared params), physical isolation. SQLite: separate queries as workaround for UNION ALL and NULL literal.

## SPEC-10.2.105 Social feed with multi-table relationships
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/SocialFeedTest`, `Pdo/MysqlSocialFeedTest`, `Pdo/PostgresSocialFeedTest`, `Pdo/SqliteSocialFeedTest`

A social feed system with users, friendships, posts, and reactions (4-table schema) works correctly through ZTD shadow store on all platforms. The scenario exercises IN subqueries for friend-based feed construction, LEFT JOIN with conditional aggregation for reaction counts, double IN subquery for mutual friend detection, friendship status transitions (pending → accepted), and feed expansion after relationship changes.

Verified patterns: friend feed construction (IN subquery on friendships), feed with per-reaction-type counts (LEFT JOIN reactions + CASE COUNT), mutual friend detection (two IN subqueries intersection), friend count per user (LEFT JOIN with status filter), add reaction (INSERT + verify count), accept friend request (UPDATE status + verify feed expansion), physical isolation.

## SPEC-10.2.106 Access log sessionization with window functions
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/AccessLogSessionTest`, `Pdo/MysqlAccessLogSessionTest`, `Pdo/PostgresAccessLogSessionTest`, `Pdo/SqliteAccessLogSessionTest`

Access log analysis using window functions for sessionization and funnel analysis works correctly through ZTD shadow store on all platforms. The scenario exercises LAG for previous-visit detection, ROW_NUMBER for visit sequencing, conditional COUNT(DISTINCT) for funnel step analysis, SUBSTR-based date grouping for daily active users, and aggregate summaries per user.

Verified patterns: page visit frequency (COUNT GROUP BY page), time between visits (LAG OVER PARTITION BY user ORDER BY time), visit sequence numbering (ROW_NUMBER OVER PARTITION BY user), funnel analysis (COUNT DISTINCT CASE per step), log new visit (INSERT + verify funnel update), user activity summary (COUNT + COUNT DISTINCT + MIN/MAX per user), daily active users (GROUP BY date substring), physical isolation.

## SPEC-10.2.107 Feature flag evaluation and A/B test analysis
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/FeatureFlagTest`, `Pdo/MysqlFeatureFlagTest`, `Pdo/PostgresFeatureFlagTest`, `Pdo/SqliteFeatureFlagTest`

A feature flag system with A/B test experiment tracking works correctly through ZTD shadow store on all platforms. The scenario exercises conditional aggregation for conversion rate calculation, ROUND with division for percentage metrics, CROSS JOIN for feature eligibility matrix, CASE expressions for multi-condition flag evaluation, and segment-based result grouping.

Verified patterns: list enabled features (WHERE enabled + ORDER BY), A/B test conversion rate (SUM/COUNT/AVG per variant), results by user segment (JOIN segments + GROUP BY segment/variant), toggle feature (UPDATE enabled/rollout_pct), add experiment results (INSERT + verify updated aggregates), feature eligibility matrix (CROSS JOIN features × users with CASE evaluation), physical isolation.

## SPEC-10.2.108 Product review/rating system
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/ReviewRatingTest`, `Pdo/MysqlReviewRatingTest`, `Pdo/PostgresReviewRatingTest`, `Pdo/SqliteReviewRatingTest`

A product review system with star ratings and helpful-vote counting works correctly through ZTD shadow store on all platforms. The scenario exercises AVG/COUNT aggregation per product, prepared-statement filtering by rating range (BETWEEN), LEFT JOIN with COALESCE for helpful-vote summation, and CASE WHEN conditional counts for star-level distribution histograms.

Verified patterns: average rating per product (AVG + COUNT GROUP BY product), filter reviews by rating range (prepared BETWEEN), most helpful reviews (LEFT JOIN votes + SUM helpful + ORDER BY helpfulness DESC), star distribution histogram (SUM CASE WHEN per rating level), add review and verify AVG updates, vote on review and verify helpful count, physical isolation.

## SPEC-10.2.109 Referral/affiliate tracking
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/ReferralTrackingTest`, `Pdo/MysqlReferralTrackingTest`, `Pdo/PostgresReferralTrackingTest`, `Pdo/SqliteReferralTrackingTest`

A referral tracking system with a self-referencing user table and multi-level attribution works correctly through ZTD shadow store on all platforms. The scenario exercises self-JOINs for direct referral counting, 2-level self-JOINs for referral chain traversal, triple JOINs (referrer → referred → purchases) for revenue attribution, HAVING filters on aggregated referral value, and LEFT JOIN IS NULL for leaf-node detection.

Verified patterns: direct referral count (self-JOIN + LEFT JOIN GROUP BY), referral chain depth two (2-level self-JOIN), referral revenue attribution (triple JOIN + SUM), top referrers by purchase value (HAVING SUM threshold), add new referral and verify chain, users with no referrals (LEFT JOIN WHERE IS NULL), physical isolation.

## SPEC-10.2.110 Content moderation queue
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/ContentModerationTest`, `Pdo/MysqlContentModerationTest`, `Pdo/PostgresContentModerationTest`, `Pdo/SqliteContentModerationTest`

A content moderation system with flag accumulation, escalation threshold, and decision recording works correctly through ZTD shadow store on all platforms. The scenario exercises LEFT JOIN with GROUP BY for flag counting per post, HAVING COUNT for escalation threshold detection, 3-table LEFT JOIN with IS NULL for unreviewed flagged posts, GROUP BY for flag reason distribution, and multi-step moderation workflows (record decision + update post status, dismiss flags after decision).

Verified patterns: flag count per post (LEFT JOIN + COUNT GROUP BY), posts exceeding threshold (HAVING COUNT >= N), unreviewed flagged posts (3-table JOIN + LEFT JOIN IS NULL), flag reason distribution (GROUP BY reason), record moderation decision (INSERT decision + UPDATE status), dismiss flags after decision (DELETE + verify counts), physical isolation.

## SPEC-10.2.111 Inventory hold/reservation with expiry
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/InventoryHoldTest`, `Pdo/MysqlInventoryHoldTest`, `Pdo/PostgresInventoryHoldTest`, `Pdo/SqliteInventoryHoldTest`

An inventory reservation system with time-windowed holds and hold-to-purchase conversion works correctly through ZTD shadow store on all platforms. The scenario exercises LEFT JOIN with conditional CASE/SUM for available-stock calculation (stock minus active held quantity), date-based filtering for active vs expired holds, multi-step workflows (confirm hold + create purchase), and bulk status updates for expired-hold cleanup.

Verified patterns: available stock (LEFT JOIN + SUM CASE active held), active holds per product (COUNT + SUM with status/date filter), expired holds (prepared statement with date comparison), confirm hold (UPDATE status + INSERT purchase), cleanup expired holds (UPDATE WHERE expired), hold-to-purchase full workflow (INSERT hold → confirm → INSERT purchase → verify stock), physical isolation.

## SPEC-10.2.112 Dashboard KPI compilation
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/DashboardKpiTest`, `Pdo/MysqlDashboardKpiTest`, `Pdo/PostgresDashboardKpiTest`, `Pdo/SqliteDashboardKpiTest`

A dashboard KPI compilation with multiple independent aggregations across 4 entity tables works correctly through ZTD shadow store on all platforms. The scenario exercises SUM/COUNT/AVG for revenue summaries, cross-entity JOINs with GROUP BY for revenue-by-segment breakdowns, priority-grouped ticket counts, per-customer health scores via multiple LEFT JOINs, SUBSTR-based date grouping for monthly order trends, and COUNT with ORDER BY/LIMIT for top-pages analysis.

Verified patterns: revenue summary (SUM + COUNT + AVG on completed orders), revenue by segment (JOIN customers + orders GROUP BY segment), open tickets by priority (COUNT WHERE status GROUP BY priority), customer health score (LEFT JOIN orders + tickets with COUNT/SUM per customer), monthly order trend (SUBSTR date grouping + COUNT/SUM), top pages by views (COUNT GROUP BY page ORDER BY DESC LIMIT), physical isolation.

## SPEC-10.2.113 Password reset token lifecycle
**Status:** Verified (SQLite-PDO)
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/PasswordResetTokenTest`, `Pdo/MysqlPasswordResetTokenTest`, `Pdo/PostgresPasswordResetTokenTest`, `Pdo/SqlitePasswordResetTokenTest`

A token-based password reset flow with expiry dates, one-time consumption, and expired-token cleanup works correctly through ZTD shadow store. The scenario exercises date-filtered SELECT for valid token lookup, UPDATE to mark tokens as consumed, DELETE with date comparison for expired-token cleanup, EXISTS subquery to find users with valid tokens, and JOIN with status filter to exclude locked users from token results.

Verified patterns: find valid token (SELECT WHERE consumed=0 AND expires_at >= date), expired token exclusion (date-filtered SELECT returns empty), consume token (UPDATE consumed=1 + verify remaining), delete expired tokens (DELETE WHERE expires_at < date + COUNT), users with valid tokens (EXISTS subquery), active-user token filter (JOIN users WHERE status='active'), physical isolation.

## SPEC-10.2.114 Multi-language content with fallback
**Status:** Verified (SQLite-PDO)
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/MultiLanguageContentTest`, `Pdo/MysqlMultiLanguageContentTest`, `Pdo/PostgresMultiLanguageContentTest`, `Pdo/SqliteMultiLanguageContentTest`

An internationalization content system with language-priority fallback using LEFT JOIN + COALESCE works correctly through ZTD shadow store. The scenario exercises direct translation lookup, double LEFT JOIN for requested-then-default language fallback via COALESCE, GROUP BY with COUNT for translation coverage statistics, and INSERT to add new translations with immediate availability.

Verified patterns: direct translation (SELECT WHERE lang_code), fallback to default (LEFT JOIN requested + LEFT JOIN default + COALESCE), missing-language fallback (COALESCE returns English default), translation coverage stats (COUNT GROUP BY slug), all content with language (COALESCE across 3 items), add translation (INSERT + verify direct lookup), physical isolation.

## SPEC-10.2.115 Split payment with partial refunds
**Status:** Verified (SQLite-PDO)
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/SplitPaymentTest`, `Pdo/MysqlSplitPaymentTest`, `Pdo/PostgresSplitPaymentTest`, `Pdo/SqliteSplitPaymentTest`

A payment splitting system with multi-method orders, SUM integrity validation, partial refunds, and percentage calculations works correctly through ZTD shadow store. The scenario exercises HAVING SUM for split-sum integrity checks, SUM GROUP BY for method breakdowns, arithmetic UPDATE for partial refunds, net-revenue computation (amount - refunded_amount), ROUND percentage calculations, and HAVING COUNT for multi-method order detection.

Verified patterns: split sum matches order total (JOIN + GROUP BY + HAVING SUM = total), payment method breakdown (SUM GROUP BY method), partial refund (UPDATE refunded_amount + status), net revenue per order (SUM of amount - refunded_amount), method percentage (ROUND price * 100.0 / total), add zero-amount split (INSERT + verify SUM unchanged), orders with multiple methods (HAVING COUNT > 1), physical isolation.

## SPEC-10.2.116 User activity streak detection via LAG
**Status:** Verified (SQLite-PDO)
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/UserActivityStreakTest`, `Pdo/MysqlUserActivityStreakTest`, `Pdo/PostgresUserActivityStreakTest`, `Pdo/SqliteUserActivityStreakTest`

A user activity streak detection system using LAG window function for previous-date comparison works correctly through ZTD shadow store. The scenario exercises COUNT DISTINCT for total activity days, LAG() OVER (PARTITION BY user ORDER BY date) for previous-date retrieval, gap detection by comparing LAG output in PHP, and MIN/MAX for activity date ranges.

Verified patterns: total activity days (COUNT DISTINCT activity_date GROUP BY user), LAG previous date (LAG window partitioned by user), gap detection via LAG (filter rows where prev_date shows non-consecutive gap), user with most activity (GROUP BY + ORDER BY DESC LIMIT 1), activity date range (MIN/MAX per user), physical isolation.

## SPEC-10.2.117 Data retention policy (GDPR-style anonymization)
**Status:** Verified (SQLite-PDO)
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/DataRetentionPolicyTest`, `Pdo/MysqlDataRetentionPolicyTest`, `Pdo/PostgresDataRetentionPolicyTest`, `Pdo/SqliteDataRetentionPolicyTest`

A GDPR-style data retention system with PII anonymization, session cleanup, and audit trail preservation works correctly through ZTD shadow store. The scenario exercises multi-column UPDATE for anonymization (masking email, name, phone), DELETE with date-range for old session cleanup, JOIN to find active users with recent sessions, audit log preservation after anonymization, bulk anonymization with string concatenation (CONCAT on MySQL, || on PostgreSQL/SQLite), and LEFT JOIN with IS NULL for inactive-user detection.

Verified patterns: baseline count (COUNT before retention), anonymize deleted user (multi-column UPDATE SET email/name/phone), delete old sessions (DELETE WHERE date < threshold + verify count), active users with recent sessions (JOIN + WHERE date + status), audit trail preserved (COUNT audit entries after anonymization), bulk anonymize (UPDATE with string concatenation WHERE created_at < date), users with no recent activity (LEFT JOIN + IS NULL), physical isolation.

## SPEC-10.2.118 Multi-jurisdiction tax calculation
**Status:** Verified (SQLite-PDO)
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/TaxCalculationTest`, `Pdo/MysqlTaxCalculationTest`, `Pdo/PostgresTaxCalculationTest`, `Pdo/SqliteTaxCalculationTest`

A multi-jurisdiction tax calculation system with category-based rates, per-item tax computation, and aggregate summaries works correctly through ZTD shadow store. The scenario exercises JOIN for matching items with applicable tax rules by jurisdiction and category, ROUND arithmetic for per-item tax computation, SUM GROUP BY for total tax by jurisdiction, WHERE with arithmetic expression for high-tax filtering, AVG GROUP BY for average rate by country, and COUNT + SUM GROUP BY for category revenue summaries.

Verified patterns: per-item tax (JOIN rules + ROUND arithmetic), total tax by jurisdiction (SUM + GROUP BY jurisdiction), tax-exempt items (WHERE rate = 0), price with tax (computed column + ORDER BY), average rate by country (AVG GROUP BY country), high-tax items (WHERE computed > threshold), category revenue summary (COUNT + SUM GROUP BY category), physical isolation.

## SPEC-10.2.119 Sliding window rate limiting
**Status:** Verified (SQLite-PDO)
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/SlidingWindowRateLimitTest`, `Pdo/MysqlSlidingWindowRateLimitTest`, `Pdo/PostgresSlidingWindowRateLimitTest`, `Pdo/SqliteSlidingWindowRateLimitTest`

A rolling time-window rate limiting system with tiered quotas and per-endpoint overrides works correctly through ZTD shadow store. The scenario exercises COUNT with BETWEEN on datetime strings for request counting within a sliding window, JOIN with HAVING for comparing request counts against per-client quotas, GROUP BY client + endpoint for top-endpoint analysis, LEFT JOIN with COALESCE for applying per-endpoint rate limit overrides over default tier limits, GROUP BY tier with AVG/MAX/COUNT for usage summary by client tier, and CASE expressions for burst detection within sub-windows.

Verified patterns: request count in window (COUNT + BETWEEN datetime), rate limit check (JOIN clients + GROUP BY + HAVING vs quota), top endpoints by client (GROUP BY client + endpoint + ORDER BY count), override applied (LEFT JOIN overrides + COALESCE for effective limit), tier usage summary (GROUP BY tier + AVG/MAX/COUNT), burst detection (CASE for sub-window counting), physical isolation.

## SPEC-10.2.120 Event sourcing projection
**Status:** Verified (SQLite-PDO)
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/EventSourcingProjectionTest`, `Pdo/MysqlEventSourcingProjectionTest`, `Pdo/PostgresEventSourcingProjectionTest`, `Pdo/SqliteEventSourcingProjectionTest`

An event-sourcing projection system that derives current state from an append-only event log works correctly through ZTD shadow store. The scenario exercises SUM with CASE WHEN for computing balances from credit/debit events, GROUP BY aggregate_id + event_type for event count distribution, correlated subquery with MAX(version) for latest-event-per-account lookup, SUM OVER (PARTITION BY ... ORDER BY) window function for running balance calculation, JOIN snapshots with SUM of delta events for snapshot-plus-delta balance reconstruction, and WHERE BETWEEN on version range for event replay.

Verified patterns: current balance from events (SUM + CASE credit/debit + GROUP BY), event count by type (GROUP BY aggregate + type + COUNT), latest event per account (JOIN with MAX version subquery), running balance with window (SUM CASE OVER PARTITION BY ORDER BY), snapshot plus delta (JOIN snapshot + SUM events after snapshot version), events between versions (WHERE version BETWEEN), physical isolation.

## SPEC-10.2.121 Closure table hierarchy (5-level tree)
**Status:** Verified (SQLite-PDO)
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/ClosureTableHierarchyTest`, `Pdo/MysqlClosureTableHierarchyTest`, `Pdo/PostgresClosureTableHierarchyTest`, `Pdo/SqliteClosureTableHierarchyTest`

A closure table pattern for deep hierarchy traversal (5 levels) with ancestor/descendant queries works correctly through ZTD shadow store. The scenario exercises JOIN with closure table for all-descendants query (depth > 0), reverse closure JOIN for all-ancestors query, depth = 1 filtering for direct children, depth <= N for subtree depth filtering, LEFT JOIN with IS NULL or NOT EXISTS for leaf node detection, GROUP BY ancestor with COUNT for subtree size aggregation, and INSERT of new closure entries for subtree insertion (move) with subsequent read verification.

Verified patterns: all descendants (closure JOIN depth > 0), all ancestors (reverse closure JOIN depth > 0 ORDER BY depth), direct children (depth = 1), subtree depth filter (depth <= 2), leaf nodes (LEFT JOIN IS NULL or NOT EXISTS), subtree count (GROUP BY ancestor + COUNT), move subtree (INSERT closure entries + verify new ancestry), physical isolation.

## SPEC-10.2.122 Temporal version lookup (effective date ranges)
**Status:** Verified (SQLite-PDO)
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/TemporalVersionLookupTest`, `Pdo/MysqlTemporalVersionLookupTest`, `Pdo/PostgresTemporalVersionLookupTest`, `Pdo/SqliteTemporalVersionLookupTest`

A temporal data pattern with effective date ranges for point-in-time lookups works correctly through ZTD shadow store. The scenario exercises JOIN with IS NULL for current-record lookup (effective_to IS NULL), WHERE with date range comparison and OR IS NULL for point-in-time query at arbitrary dates, ORDER BY effective_from for price history timeline, GROUP BY with COUNT for version count per product, self-join or correlated subquery for price increase percentage (comparing first vs current price), and multi-step DML (INSERT new price + UPDATE old current price's effective_to) for price version management.

Verified patterns: current prices (JOIN + WHERE effective_to IS NULL), price at date (WHERE effective_from <= date AND effective_to >= date OR IS NULL), price history (WHERE product + ORDER BY date), price change count (GROUP BY product + COUNT), price increase percentage (self-join first vs current + ROUND arithmetic), update current price (INSERT new + UPDATE old effective_to), physical isolation.

## SPEC-10.2.123 Incremental sync delta detection
**Status:** Verified (SQLite-PDO)
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/IncrementalSyncDeltaTest`, `Pdo/MysqlIncrementalSyncDeltaTest`, `Pdo/PostgresIncrementalSyncDeltaTest`, `Pdo/SqliteIncrementalSyncDeltaTest`

An incremental data synchronization system with watermark-based change detection works correctly through ZTD shadow store. The scenario exercises LEFT JOIN with IS NULL for detecting new (never-synced) records, JOIN with date comparison for detecting updated records since last sync, WHERE with soft-delete flag + date comparison for deleted record detection, subquery against watermark table for delta-since-watermark query, INSERT + GROUP BY for recording sync actions and counting by action type, and UPDATE on watermark table for advancing the sync position.

Verified patterns: detect new records (LEFT JOIN sync_log IS NULL), detect updated records (JOIN sync_log + WHERE updated_at > watermark), detect soft-deleted (WHERE is_deleted = 1 + updated_at > watermark), delta since watermark (subquery from watermark table), record sync actions (INSERT + GROUP BY action + COUNT), update watermark (UPDATE + verify read-back), physical isolation.

## SPEC-10.2.124 Cohort retention analysis
**Status:** Verified (SQLite-PDO)
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/CohortRetentionAnalysisTest`, `Pdo/MysqlCohortRetentionAnalysisTest`, `Pdo/PostgresCohortRetentionAnalysisTest`, `Pdo/SqliteCohortRetentionAnalysisTest`

A user cohort retention analysis system with churn detection and power-user identification works correctly through ZTD shadow store. The scenario exercises GROUP BY signup_month with COUNT for cohort sizing, JOIN users + activities with COUNT DISTINCT user_id per activity_month for retention tracking, GROUP BY signup_month with AVG(action_count) for cohort engagement metrics, COUNT DISTINCT user_id per activity_month for monthly active user counts, LEFT JOIN with IS NULL for churn detection (active in signup month but absent in following month), and SUM + HAVING for power-user identification.

Verified patterns: cohort size (GROUP BY signup_month + COUNT), retention by month (JOIN + COUNT DISTINCT user_id per month), cohort average activity (GROUP BY signup_month + AVG), active users per month (COUNT DISTINCT per activity_month), churn detection (LEFT JOIN next month IS NULL), power users (SUM action_count + HAVING threshold), physical isolation.

## SPEC-10.2.125 Customer RFM segmentation
**Status:** Verified (SQLite-PDO partial — derived table with window functions returns empty)
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/CustomerRfmSegmentationTest`, `Pdo/MysqlCustomerRfmSegmentationTest`, `Pdo/PostgresCustomerRfmSegmentationTest`, `Pdo/SqliteCustomerRfmSegmentationTest`

A customer RFM (Recency, Frequency, Monetary) segmentation system using NTILE window functions for quartile scoring works through ZTD shadow store. The scenario exercises MAX(order_date) for recency ranking, COUNT for frequency ranking, SUM for monetary ranking, NTILE(4) OVER with three independent orderings from a derived subquery for quartile scoring, HAVING with nested AVG subquery for high-value customer identification, and ROUND(SUM/COUNT) for average order value.

On SQLite, the RFM scoring test (derived table with NTILE window functions) returns empty — extends SPEC-11.DERIVED-TABLE-PREPARED to affect `query()` as well as `prepare()`.

Verified patterns: recency ranking (MAX + ORDER BY), frequency ranking (COUNT + ORDER BY), monetary ranking (SUM + ORDER BY), RFM quartile scoring (NTILE window function from derived subquery), high-value customers (HAVING with nested AVG subquery), average order value (ROUND SUM/COUNT), physical isolation.

## SPEC-10.2.126 Help desk SLA tracking
**Status:** Verified (SQLite-PDO)
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/HelpDeskSlaTest`, `Pdo/MysqlHelpDeskSlaTest`, `Pdo/PostgresHelpDeskSlaTest`, `Pdo/SqliteHelpDeskSlaTest`

A help desk SLA tracking system with first response time, workload analysis, and priority-based status reporting works correctly through ZTD shadow store. The scenario exercises MIN correlated subquery for first response time, CASE expression on MIN aggregate for response status evaluation, COUNT(DISTINCT) + COUNT for agent workload metrics, SUM(CASE) conditional aggregation with CASE-based ORDER BY for priority cross-tab, NOT EXISTS correlated subquery for unresponded ticket detection, and LEFT JOIN with IS NULL filter for excluding internal responses.

Verified patterns: first response time (MIN + LEFT JOIN), response status (CASE on MIN aggregate), agent workload (COUNT DISTINCT + COUNT), priority cross-tab (SUM CASE + CASE ORDER BY), unresponded tickets (NOT EXISTS), response count excluding internal (LEFT JOIN filter), physical isolation.

## SPEC-10.2.127 Meeting room booking
**Status:** Verified (SQLite-PDO)
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/MeetingRoomBookingTest`, `Pdo/MysqlMeetingRoomBookingTest`, `Pdo/PostgresMeetingRoomBookingTest`, `Pdo/SqliteMeetingRoomBookingTest`

A meeting room booking system with availability checking, overlap detection, and utilization tracking works correctly through ZTD shadow store. The scenario exercises NOT EXISTS with compound range overlap formula (start < end AND end > start) for room availability, LEFT JOIN + COUNT + GROUP BY for bookings per room, overlap detection for proposed booking conflicts, COUNT + GROUP BY for per-user booking counts, CASE WHEN on COUNT for utilization classification, and COUNT(DISTINCT) for floor-level aggregation.

Verified patterns: room availability (NOT EXISTS overlap), bookings per room (LEFT JOIN COUNT), overlap detection (compound range check), bookings per user (GROUP BY COUNT), room utilization (CASE on COUNT), floor summary (COUNT DISTINCT + COUNT), physical isolation.

## SPEC-10.2.128 Budget allocation with rollover
**Status:** Verified (SQLite-PDO)
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/BudgetRolloverTest`, `Pdo/MysqlBudgetRolloverTest`, `Pdo/PostgresBudgetRolloverTest`, `Pdo/SqliteBudgetRolloverTest`

A department budget tracking system with cumulative spending, variance analysis, and category breakdown works correctly through ZTD shadow store. The scenario exercises SUM(SUM()) OVER (PARTITION BY ... ORDER BY ...) for cumulative running totals (nested aggregate window function), ROUND(actual - budget/12) for monthly budget variance, HAVING with cross-table expression for over-budget detection, scalar subquery with WHERE 1=1 for category percentage of total, and RANK() OVER for department spending ranking.

Verified patterns: monthly spending (GROUP BY + SUM), cumulative spending (SUM of SUM OVER window), budget variance (arithmetic with annual_budget/12), over-budget months (HAVING > budget/12), category breakdown (scalar subquery percentage), department ranking (RANK OVER), physical isolation.

## SPEC-10.2.129 Gradebook weighted averages
**Status:** Verified (SQLite-PDO)
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/GradebookWeightedAvgTest`, `Pdo/MysqlGradebookWeightedAvgTest`, `Pdo/PostgresGradebookWeightedAvgTest`, `Pdo/SqliteGradebookWeightedAvgTest`

A gradebook system with weighted average calculation, letter grade assignment, and missing assignment detection works correctly through ZTD shadow store. The scenario exercises SUM(score/max_score * 100.0 * weight) / SUM(weight) for weighted average computation (complex arithmetic inside aggregates), CASE boundaries on aggregate result for letter grade assignment, AVG/MIN/MAX of computed percentages for class statistics, CROSS JOIN + LEFT JOIN IS NULL for missing assignment detection, and HAVING with nested correlated subquery for top performer per category identification.

Verified patterns: weighted average (SUM*weight/SUM weight), letter grades (CASE on aggregate), class statistics (AVG/MIN/MAX of expressions), category averages (GROUP BY category), missing assignments (CROSS JOIN + LEFT JOIN IS NULL), top performer per category (HAVING with nested subquery), physical isolation.

## SPEC-10.2.130 Fleet service tracking
**Status:** Verified (SQLite-PDO)
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/FleetServiceTrackingTest`, `Pdo/MysqlFleetServiceTrackingTest`, `Pdo/PostgresFleetServiceTrackingTest`, `Pdo/SqliteFleetServiceTrackingTest`

A fleet vehicle service tracking system with overdue detection, cost analysis, and mileage tracking works correctly through ZTD shadow store. The scenario exercises MAX date/mileage aggregation per vehicle, HAVING with string date comparison for overdue service detection, SUM/COUNT/ROUND for per-vehicle cost analysis, GROUP BY service type for aggregate statistics, LAG() OVER (PARTITION BY ... ORDER BY ...) for mileage between consecutive services, and COUNT DISTINCT/SUM/MAX/MIN for active fleet summary.

Verified patterns: last service per vehicle (MAX + GROUP BY), overdue services (HAVING date < cutoff), service cost analysis (SUM/COUNT/ROUND), service type summary (GROUP BY + aggregates), mileage between services (LAG window function), active fleet summary (COUNT DISTINCT + SUM + MAX + MIN), physical isolation.

## SPEC-10.2.131 Quota management
**Status:** Verified (SQLite-PDO)
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/QuotaManagementTest`, `Pdo/MysqlQuotaManagementTest`, `Pdo/PostgresQuotaManagementTest`, `Pdo/SqliteQuotaManagementTest`

A SaaS quota management system with utilization tracking, over-quota detection, and usage trend analysis works correctly through ZTD shadow store. The scenario exercises correlated MAX subquery in WHERE for latest usage snapshot, ROUND percentage calculations across multiple resource dimensions (storage, API calls, users), CASE-based threshold evaluation for over-quota flagging, SUM + GROUP BY for daily usage trends, compound OR conditions with arithmetic comparisons for high-risk account identification, and AVG with ROUND for daily usage averages.

Verified patterns: latest usage snapshot (correlated MAX subquery), quota utilization (percentage ROUND), over-quota detection (CASE + multi-column comparison), usage trend (SUM GROUP BY date), high-risk accounts (compound threshold filter), average daily usage (AVG + ROUND), physical isolation.

## SPEC-10.2.132 Shipping tracker
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/ShippingTrackerTest`, `Pdo/MysqlShippingTrackerTest`, `Pdo/PostgresShippingTrackerTest`, `Pdo/SqliteShippingTrackerTest`

An order-to-delivery tracking system with orders, shipments, and tracking events works correctly through ZTD shadow store. The scenario exercises multi-table JOIN for order-shipment summaries with NULL handling for undelivered shipments, double correlated MAX subquery with id tiebreaker for latest tracking event per shipment, GROUP BY with SUM(CASE) cross-tab for delivery rate by carrier with ROUND percentage, date string comparison for on-time delivery analysis, and GROUP BY with COUNT for event type distribution.

Verified patterns: order-shipment summary (multi-table JOIN + NULL), latest event per shipment (double correlated MAX subquery), delivery rate (SUM CASE cross-tab + ROUND), on-time delivery (date comparison + aggregate percentage), event type distribution (GROUP BY COUNT), physical isolation.

## SPEC-10.2.133 Return and refund processing
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/ReturnRefundTest`, `Pdo/MysqlReturnRefundTest`, `Pdo/PostgresReturnRefundTest`, `Pdo/SqliteReturnRefundTest`

A return authorization and refund processing system with orders, order items, and returns works correctly through ZTD shadow store. The scenario exercises double LEFT JOIN (orders → order_items → returns) with GROUP BY and COALESCE for per-order return summaries, SUM with prepared statement WHERE filter for approved refund totals, GROUP BY reason with COUNT and SUM for return reason breakdown, multi-table JOIN with DISTINCT for pending return detection, and arithmetic (unit_price - refund_amount) for restocking fee calculation.

Verified patterns: order return summary (double LEFT JOIN + GROUP BY + COALESCE), approved refund total (SUM + prepared statement), reason breakdown (GROUP BY + COUNT + SUM), pending returns (multi-table JOIN + DISTINCT), restocking fee (arithmetic expression), physical isolation.

## SPEC-10.2.134 Chat messaging
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/ChatMessagingTest`, `Pdo/MysqlChatMessagingTest`, `Pdo/PostgresChatMessagingTest`, `Pdo/SqliteChatMessagingTest`

A chat messaging system with users, conversations, messages, and read receipts works correctly through ZTD shadow store. The scenario exercises correlated MAX subquery with JOIN for latest message per conversation, NOT EXISTS anti-join for unread message counting per user per conversation, GROUP BY with COUNT for message counts, COUNT DISTINCT for participant counting, and multi-column GROUP BY with COUNT and MAX aggregation for per-user message statistics.

Verified patterns: latest message per conversation (correlated MAX subquery + JOIN), unread count (NOT EXISTS anti-join + GROUP BY), message count (GROUP BY COUNT), conversation participants (COUNT DISTINCT), user message stats (GROUP BY + COUNT + MAX), physical isolation.

## SPEC-10.2.135 Appointment scheduling
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/AppointmentSchedulingTest`, `Pdo/MysqlAppointmentSchedulingTest`, `Pdo/PostgresAppointmentSchedulingTest`, `Pdo/SqliteAppointmentSchedulingTest`

An appointment scheduling system with providers, time slots, and appointments works correctly through ZTD shadow store. The scenario exercises JOIN with WHERE filter on availability and provider for open slot lookup, GROUP BY with COUNT and SUM for provider schedule summaries (total/available/booked using integer flag arithmetic), multi-table JOIN (appointments → slots → providers) with status filter for active appointment listing, time range overlap detection (start_time < X AND end_time > Y) for booking conflict detection, and simple COUNT with WHERE for cancelled appointment tallying.

Verified patterns: available slots (JOIN + WHERE filter), provider schedule summary (GROUP BY + COUNT + SUM), active appointments (multi-table JOIN + status filter), booking conflict detection (range overlap), cancelled count (COUNT + WHERE), physical isolation.

## SPEC-10.2.136 Expense report workflow
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/ExpenseReportTest`, `Pdo/MysqlExpenseReportTest`, `Pdo/PostgresExpenseReportTest`, `Pdo/SqliteExpenseReportTest`

An employee expense reporting system with employees, expense reports, and expense items works correctly through ZTD shadow store. The scenario exercises multi-table JOIN with GROUP BY and SUM for verifying report item totals, 3-table JOIN with multi-column GROUP BY (department + category) for category breakdown by department, self-join on employees table for manager-based pending approval lookup, GROUP BY with SUM and WHERE filter for reimbursement summary per employee, HAVING with SUM aggregate threshold for high-value report detection, and scalar subquery with ROUND for category percentage of total across approved/reimbursed reports.

Verified patterns: report item totals (JOIN + GROUP BY SUM), category breakdown by department (3-table JOIN + multi-column GROUP BY), pending approvals (self-join + WHERE filter), reimbursement summary (GROUP BY + SUM + status filter), high-value reports (HAVING SUM threshold), category percentage (scalar subquery + ROUND), physical isolation.

## SPEC-10.2.137 Voting poll
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/VotingPollTest`, `Pdo/MysqlVotingPollTest`, `Pdo/PostgresVotingPollTest`, `Pdo/SqliteVotingPollTest`

A polling and voting system with polls, options, and votes works correctly through ZTD shadow store. The scenario exercises LEFT JOIN with GROUP BY and COUNT for vote tallying including zero-vote options, inline scalar subquery for percentage calculation (COUNT * 100.0 / (SELECT COUNT(*))), ORDER BY aggregate with LIMIT 1 for poll winner detection, COUNT DISTINCT for unique voter participation, HAVING with COUNT for minimum-vote threshold filtering, and NOT IN subquery for anti-join voter cross-participation analysis.

Verified patterns: poll results (LEFT JOIN + percentage scalar subquery), poll winner (ORDER BY aggregate + LIMIT 1), voter participation (COUNT DISTINCT), minimum votes (HAVING COUNT), non-voters (NOT IN subquery anti-join), physical isolation.

## SPEC-10.2.138 Kanban board task management
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/KanbanBoardTest`, `Pdo/MysqlKanbanBoardTest`, `Pdo/PostgresKanbanBoardTest`, `Pdo/SqliteKanbanBoardTest`

A task management kanban board system with boards, tasks, and task dependencies works correctly through ZTD shadow store. The scenario exercises GROUP BY with multi-column grouping (board + status) for task count per status, GROUP BY assignee with WHERE filter for active workload analysis, EXISTS correlated subquery on dependency table for blocked task detection (todo tasks with unfinished dependencies), date comparison with status filter for overdue task identification, CASE expression mapping priority numbers to labels with GROUP BY for priority distribution, and SUM(CASE)/COUNT with ROUND for per-board completion percentage.

Verified patterns: task count by status (GROUP BY + COUNT), assignee workload (GROUP BY + WHERE + NULL exclusion), blocked tasks (EXISTS correlated subquery on dependencies), overdue tasks (date comparison + status filter), priority distribution (CASE + GROUP BY), completion percentage (SUM CASE / COUNT + ROUND), physical isolation.

## SPEC-10.2.139 Prescription tracking
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/PrescriptionTrackingTest`, `Pdo/MysqlPrescriptionTrackingTest`, `Pdo/PostgresPrescriptionTrackingTest`, `Pdo/SqlitePrescriptionTrackingTest`

A medical prescription tracking system with patients, doctors, visits, and prescriptions works correctly through ZTD shadow store. The scenario exercises 4-table JOIN (patients JOIN visits JOIN prescriptions JOIN doctors) for patient prescription summaries, self-referencing UPDATE arithmetic for refill count decrement, date BETWEEN for active prescription filtering at different points in time, and GROUP BY with COUNT and COUNT DISTINCT for per-doctor prescription and patient statistics.

Verified patterns: 4-table JOIN with ORDER BY (patient summary), self-referencing UPDATE SET col = col - 1 (refill tracking), BETWEEN on date columns (active prescriptions), GROUP BY with COUNT + COUNT DISTINCT (doctor stats), physical isolation.

## SPEC-10.2.140 Playlist management
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/PlaylistManagementTest`, `Pdo/MysqlPlaylistManagementTest`, `Pdo/PostgresPlaylistManagementTest`, `Pdo/SqlitePlaylistManagementTest`

A playlist management system with playlists, songs, and playlist-song associations works correctly through ZTD shadow store. The scenario exercises 3-table JOIN for playlist contents with position ordering, UPDATE with arithmetic expressions for position reordering (shift positions down then move item), GROUP BY genre with COUNT for genre distribution across playlists, SUM of play counts across playlists per song for most-played ranking, and SUM of song durations per playlist for total playlist length.

Verified patterns: 3-table JOIN with ORDER BY position (playlist contents), UPDATE SET position = position + 1 with range WHERE (position shift), GROUP BY genre COUNT (distribution), SUM play_count GROUP BY song (most played), SUM duration JOIN (playlist duration), physical isolation.

## SPEC-10.2.141 Badge achievement system
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/BadgeAchievementTest`, `Pdo/MysqlBadgeAchievementTest`, `Pdo/PostgresBadgeAchievementTest`, `Pdo/SqliteBadgeAchievementTest`

A gamification badge achievement system with users, badges, and user-badge progress tracking works correctly through ZTD shadow store. The scenario exercises 3-table JOIN with CASE WHEN for unlock status display, GROUP BY badge with COUNT and ROUND percentage for rarity calculation, COUNT with WHERE IS NOT NULL filter for unlocked badge tallying, sequential UPDATE for progress increment and unlock timestamp, and WHERE IS NULL filter for in-progress badge listing.

Verified patterns: 3-table JOIN with CASE WHEN IS NOT NULL (progress display), GROUP BY with COUNT + ROUND percentage (rarity), COUNT WHERE IS NOT NULL (unlocked per user), UPDATE progress + UPDATE unlocked_at (progress tracking), WHERE IS NULL filter (in-progress badges), physical isolation.

## SPEC-10.2.142 Attendance tracker
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/AttendanceTrackerTest`, `Pdo/MysqlAttendanceTrackerTest`, `Pdo/PostgresAttendanceTrackerTest`, `Pdo/SqliteAttendanceTrackerTest`

An employee attendance tracking system with employees and daily attendance records works correctly through ZTD shadow store. The scenario exercises SUM CASE for status categorization (present/late/absent counts per employee), GROUP BY department with ROUND percentage for attendance rate, prepared statements with BETWEEN for date-range queries, LEFT JOIN anti-pattern for finding employees with no record on a given date, and HAVING with SUM CASE = 0 for perfect attendance detection.

Verified patterns: SUM CASE status categorization (attendance summary), GROUP BY department ROUND percentage (attendance rate), prepared statement with BETWEEN (date range), LEFT JOIN WHERE IS NULL (absence detection), HAVING SUM CASE = 0 (perfect attendance), physical isolation.

## SPEC-10.2.143 Dynamic pricing
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/DynamicPricingTest`, `Pdo/MysqlDynamicPricingTest`, `Pdo/PostgresDynamicPricingTest`, `Pdo/SqliteDynamicPricingTest`

A dynamic pricing system with products, price history, and competitor prices works correctly through ZTD shadow store. The scenario exercises correlated MAX subquery for effective-date price lookup (latest price per product), price change history with ORDER BY for audit trail, CASE expression for discount tier classification based on price ranges, JOIN with ROUND arithmetic for competitor price comparison and difference calculation, and derived table subquery with GROUP BY for category-level price range aggregation.

Verified patterns: correlated MAX subquery (current price lookup), ORDER BY effective_date (price history), CASE WHEN price ranges (tier classification), JOIN with ROUND(a - b, 2) (competitor comparison), derived table + GROUP BY MIN/MAX (category price range), physical isolation.

## SPEC-10.2.144 Warehouse transfer
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/WarehouseTransferTest`, `Pdo/MysqlWarehouseTransferTest`, `Pdo/PostgresWarehouseTransferTest`, `Pdo/SqliteWarehouseTransferTest`

An inter-warehouse stock transfer system with warehouses, products, stock levels, and transfer records works correctly through ZTD shadow store. The scenario exercises multi-table JOIN for stock summary with product names, GROUP BY SUM for total stock across warehouses, self-join on warehouses table for source/destination names on transfer records, GROUP BY with HAVING SUM threshold for completed transfer volume by route, HAVING filter for low-stock warehouse detection, prepared statement for transfer lookup by status, and sequential INSERT+UPDATE for recording transfers with stock level adjustments.

Verified patterns: 3-table JOIN (stock summary), GROUP BY SUM (total stock per product), self-join on same table (transfer source/destination), GROUP BY HAVING SUM >= threshold (transfer volume), HAVING SUM < threshold (low stock), prepared statement with status filter, self-referencing UPDATE arithmetic (stock adjustment), physical isolation.

## SPEC-10.2.145 Course prerequisite
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/CoursePrerequisiteTest`, `Pdo/MysqlCoursePrerequisiteTest`, `Pdo/PostgresCoursePrerequisiteTest`, `Pdo/SqliteCoursePrerequisiteTest`

An academic course enrollment system with courses, prerequisites, students, and completions works correctly through ZTD shadow store. The scenario exercises 3-table JOIN for student transcript display, SUM + COUNT aggregation for credits earned, double-nested NOT EXISTS for prerequisite eligibility checking (students who have completed all required courses), LEFT JOIN with IS NULL for missing prerequisite detection, COUNT DISTINCT with LEFT JOIN for prerequisite completion tracking, and prepared BETWEEN for date-range filtering.

Verified patterns: 3-table JOIN (transcript), SUM + COUNT GROUP BY (credits), double-nested NOT EXISTS (eligibility), LEFT JOIN WHERE IS NULL (missing prerequisites), COUNT DISTINCT with LEFT JOIN (completion count), prepared BETWEEN (date range), physical isolation.

## SPEC-10.2.146 Meal planning
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/MealPlanningTest`, `Pdo/MysqlMealPlanningTest`, `Pdo/PostgresMealPlanningTest`, `Pdo/SqliteMealPlanningTest`

A weekly meal planning system with meals, ingredients, weekly plan, and day reference tables works correctly through ZTD shadow store. The scenario exercises GROUP BY category with COUNT for meal categorization, multi-table JOIN for weekly plan overview with meal names, CROSS JOIN (via UNION ALL subquery) with LEFT JOIN and IS NULL for unassigned slot detection, SUM aggregation through plan-to-ingredients JOIN for shopping list quantity totals, WHERE IN dietary tag filter with GROUP BY HAVING for vegetarian/vegan meal listing, and prepared statement for meals-by-day lookup.

Verified patterns: GROUP BY COUNT (category), multi-table JOIN ORDER BY (plan overview), CROSS JOIN + LEFT JOIN WHERE IS NULL (unassigned slots), SUM aggregate through JOIN (shopping list), WHERE IN + GROUP BY HAVING (dietary filter), prepared statement (day lookup), physical isolation.

## SPEC-10.2.147 Insurance claims
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/InsuranceClaimTest`, `Pdo/MysqlInsuranceClaimTest`, `Pdo/PostgresInsuranceClaimTest`, `Pdo/SqliteInsuranceClaimTest`

An insurance claims processing system with policies, claims, and claim notes works correctly through ZTD shadow store. The scenario exercises multi-table JOIN with GROUP BY for per-policy claim summaries (count + total requested), GROUP BY status with COUNT and CASE for human-readable status distribution, SUM payout by policy type for paid claims, ROUND + COALESCE + SUM CASE for coverage utilization percentage, LEFT JOIN with COUNT for note count per claim, and prepared statement for claims filtered by policy type.

Verified patterns: JOIN + GROUP BY COUNT + SUM (claim summary), GROUP BY + CASE label (status distribution), SUM WHERE status filter (payouts by type), ROUND + COALESCE + SUM CASE / coverage_limit (utilization %), LEFT JOIN COUNT (note count), prepared statement (policy type filter), physical isolation.

## SPEC-10.2.148 API key management
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/ApiKeyManagementTest`, `Pdo/MysqlApiKeyManagementTest`, `Pdo/PostgresApiKeyManagementTest`, `Pdo/SqliteApiKeyManagementTest`

An API key lifecycle and usage quota tracking system with clients, keys, and usage records works correctly through ZTD shadow store. The scenario exercises JOIN with GROUP BY for key status summary per client, GROUP BY key+date with COUNT for daily usage counts, COUNT/quota percentage with ROUND for quota utilization, SUM CASE for error rate calculation by endpoint (response codes >= 400), AVG + ROUND grouped by tier for response time analysis, and prepared statement for usage lookup by key prefix.

Verified patterns: JOIN + GROUP BY COUNT (key status summary), GROUP BY key+date COUNT (daily usage), COUNT/quota ROUND percentage (quota utilization), SUM CASE >= 400 / COUNT (error rate), AVG ROUND GROUP BY (response time by tier), prepared statement with JOIN (key prefix lookup), physical isolation.

## SPEC-10.2.149 Content moderation queue
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/ContentModerationQueueTest`, `Pdo/MysqlContentModerationQueueTest`, `Pdo/PostgresContentModerationQueueTest`, `Pdo/SqliteContentModerationQueueTest`

A content moderation queue system with users, content, flags, and moderator actions works correctly through ZTD shadow store. The scenario exercises JOIN with COUNT GROUP BY for flag counts per flagged content, JOIN with COUNT GROUP BY for moderator action summaries, CASE expression with IN for status categorization (active/review_needed/removed) with GROUP BY on the CASE alias, NOT EXISTS correlated subquery for finding unflagged published content, COUNT GROUP BY for flag reason breakdown, and prepared statement with JOIN for moderator-specific content lookup.

Verified patterns: JOIN + COUNT GROUP BY + WHERE status filter (flagged content), JOIN + COUNT GROUP BY + ORDER BY (moderator summary), CASE WHEN IN + GROUP BY alias + COUNT (status distribution), NOT EXISTS correlated subquery (unflagged content), COUNT GROUP BY + ORDER BY count DESC (reason breakdown), prepared statement with JOIN (moderator lookup), physical isolation.

## SPEC-10.2.150 Classroom quiz scoring
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/ClassroomQuizScoringTest`, `Pdo/MysqlClassroomQuizScoringTest`, `Pdo/PostgresClassroomQuizScoringTest`, `Pdo/SqliteClassroomQuizScoringTest`

A classroom quiz scoring system with students, quizzes, questions, and answers works correctly through ZTD shadow store. The scenario exercises 4-table JOIN with CASE for answer verification (comparing given_answer to correct_answer), SUM CASE / COUNT with ROUND for score percentage calculation, GROUP BY with HAVING for pass/fail threshold filtering against a dynamic column (passing_score), NOT EXISTS for finding students who missed a quiz, derived table JOIN for class average calculation, per-question difficulty analysis with success rate percentage, and prepared statement with multiple parameters for student+quiz lookup.

Verified patterns: 4-table JOIN + CASE answer match + SUM/COUNT ROUND (score %), GROUP BY HAVING dynamic threshold (failing students), NOT EXISTS (missing submissions), derived table JOIN + AVG ROUND (class average), SUM CASE / COUNT per question (difficulty), prepared statement with 2 params (student quiz lookup), physical isolation.

## SPEC-10.2.151 Onboarding checklist
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/OnboardingChecklistTest`, `Pdo/MysqlOnboardingChecklistTest`, `Pdo/PostgresOnboardingChecklistTest`, `Pdo/SqliteOnboardingChecklistTest`

An employee onboarding checklist system with departments, employees, checklist items, and completions works correctly through ZTD shadow store. The scenario exercises LEFT JOIN with COUNT and scalar subquery for completion percentage per employee, NOT EXISTS for outstanding items, derived table JOIN with AVG ROUND for department-level progress summary, CASE with scalar subquery divisor for status labels (complete/in_progress/behind), INSERT followed by SELECT verification for multi-step workflow, LEFT JOIN with COUNT DISTINCT and COUNT GROUP BY category for cross-employee category completion, and prepared statement with scalar subquery for department-filtered lookup.

Verified patterns: LEFT JOIN COUNT + scalar subquery ROUND (completion %), NOT EXISTS (outstanding items), derived table JOIN + AVG ROUND (department progress), CASE + scalar subquery (status labels), INSERT + SELECT verification (multi-step), LEFT JOIN COUNT DISTINCT + GROUP BY category (category breakdown), prepared statement (department filter), physical isolation. SQLite requires `WHERE 1=1` workaround in bare scalar subqueries (SPEC-11.BARE-SUBQUERY-REWRITE).

## SPEC-10.2.152 Library book lending
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/LibraryLendingTest`, `Pdo/MysqlLibraryLendingTest`, `Pdo/PostgresLibraryLendingTest`, `Pdo/SqliteLibraryLendingTest`

A library book lending system with books, members, and loan records works correctly through ZTD shadow store. The scenario exercises 3-table JOIN for currently borrowed books (WHERE return_date IS NULL), CASE WHEN with date comparison for overdue detection, platform-specific date arithmetic for late fee calculation (JULIANDAY on SQLite, DATEDIFF on MySQL, date subtraction on PostgreSQL), LEFT JOIN with CASE for book availability status, LEFT JOIN with COUNT and SUM CASE for member borrowing statistics (total loans, distinct books, currently out), GROUP BY category with HAVING COUNT for category popularity filtering, and prepared statement with JOIN for member loan lookup.

Verified patterns: 3-table JOIN + IS NULL filter (currently borrowed), CASE WHEN date comparison (overdue detection), platform-specific date diff + ROUND arithmetic (late fees), LEFT JOIN + CASE (availability), LEFT JOIN + COUNT + COUNT DISTINCT + SUM CASE (member stats), GROUP BY + HAVING COUNT (category popularity), prepared JOIN (member loans), physical isolation.

## SPEC-10.2.153 Employee skill matrix
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/SkillMatrixTest`, `Pdo/MysqlSkillMatrixTest`, `Pdo/PostgresSkillMatrixTest`, `Pdo/SqliteSkillMatrixTest`

An employee skill matrix system with employees, skills, employee_skills (junction with level), and project requirements works correctly through ZTD shadow store. The scenario exercises 3-table JOIN with GROUP BY for skill summary per employee (COUNT, ROUND AVG, MIN), HAVING COUNT = (SELECT COUNT) for fully-qualified project matching (employees meeting all requirements), LEFT JOIN with COALESCE for skill gap analysis (missing or underqualified skills), SUM CASE cross-tab for skill level distribution (beginner/intermediate/expert), MIN with HAVING for minimum competency threshold, and prepared statement with JOIN and ORDER BY for employee skill lookup.

Verified patterns: 3-table JOIN + COUNT + ROUND AVG + MIN (skill summary), HAVING COUNT = scalar subquery (fully-qualified matching), LEFT JOIN + COALESCE + compound WHERE (skill gap), SUM CASE cross-tab (level distribution), MIN + HAVING (minimum competency), prepared JOIN + ORDER BY (skill lookup), physical isolation.

## SPEC-10.2.154 Parking garage
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/ParkingGarageTest`, `Pdo/MysqlParkingGarageTest`, `Pdo/PostgresParkingGarageTest`, `Pdo/SqliteParkingGarageTest`

A parking garage management system with levels (capacity), passes (monthly/annual), and entry/exit records works correctly through ZTD shadow store. The scenario exercises COUNT with GROUP BY for current occupancy per level, LEFT JOIN with ROUND percentage for capacity utilization, GROUP BY SUBSTR for daily entry statistics with COALESCE SUM, LEFT JOIN entries to passes with COUNT for pass holder activity, LEFT JOIN with SUM and COALESCE for revenue by level, LEFT JOIN with IS NULL for identifying non-pass vehicles still parked, and prepared statement with JOIN for vehicle history lookup.

Verified patterns: COUNT + GROUP BY + IS NULL filter (occupancy), LEFT JOIN + ROUND percentage (utilization), GROUP BY SUBSTR + COUNT + COALESCE SUM (daily stats), LEFT JOIN + COUNT (pass holder activity), LEFT JOIN + COALESCE SUM (revenue by level), double LEFT JOIN + IS NULL (non-pass vehicles), prepared JOIN (vehicle history), physical isolation.

## SPEC-10.2.155 Employee leave balance
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/LeaveBalanceTest`, `Pdo/MysqlLeaveBalanceTest`, `Pdo/PostgresLeaveBalanceTest`, `Pdo/SqliteLeaveBalanceTest`

An employee leave management system with employees, leave types (annual/sick/personal with quotas), and leave requests (with status workflow) works correctly through ZTD shadow store. The scenario exercises 3-table JOIN with SUM GROUP BY for approved leave totals, LEFT JOIN with COALESCE(SUM, 0) for remaining balance calculation per leave type, self-join overlap detection for conflicting date ranges, SUM CASE cross-tab for department-level status breakdown (approved/pending/rejected days), UPDATE status transition followed by aggregate verification, and prepared statement with date range BETWEEN filtering.

Verified patterns: 3-table JOIN + SUM GROUP BY (approved totals), LEFT JOIN + COALESCE SUM (remaining balance), self-join date overlap detection, SUM CASE cross-tab (department overview), UPDATE + aggregate verify (approve and recheck), prepared BETWEEN (date range search), physical isolation.

## SPEC-10.2.156 Tenant usage metering
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/UsageMeteringTest`, `Pdo/MysqlUsageMeteringTest`, `Pdo/PostgresUsageMeteringTest`, `Pdo/SqliteUsageMeteringTest`

A SaaS tenant usage metering system with tenants (plan + quota), usage records (per endpoint per day), and overage charges works correctly through ZTD shadow store. The scenario exercises GROUP BY with SUBSTR date truncation for monthly aggregation, GROUP BY with SUM + COUNT for endpoint breakdown, ROUND with CAST division for quota utilization percentage, CASE for over/under status, HAVING with cross-table threshold for over-quota detection, LEFT JOIN with COALESCE for overage charge lookup, and prepared statement with 3 parameters (tenant + date range).

Verified patterns: GROUP BY SUBSTR month + SUM (monthly summary), GROUP BY endpoint + SUM + COUNT (breakdown), ROUND CAST division percentage (utilization), CASE over/under (status label), HAVING SUM > quota (threshold), LEFT JOIN + COALESCE (overage charges), prepared 3-param (tenant + date range), physical isolation.

## SPEC-10.2.157 Document workflow pipeline
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/DocumentWorkflowTest`, `Pdo/MysqlDocumentWorkflowTest`, `Pdo/PostgresDocumentWorkflowTest`, `Pdo/SqliteDocumentWorkflowTest`

A document publishing workflow with documents (draft/review/approved/published status), reviewers (with expertise), and reviews (approve/reject/comment decisions) works correctly through ZTD shadow store. The scenario exercises GROUP BY status for document summary, LEFT JOIN with conditional filter and CASE for quorum check (>= 2 approvals), LEFT JOIN with SUM CASE for reviewer workload cross-tab (approve/reject/comment counts), WHERE + LEFT JOIN + HAVING for under-quorum documents in review, UPDATE status transition with subsequent verification, correlated MAX subquery in WHERE for latest review per document, and prepared statement for reviewer document lookup.

Verified patterns: GROUP BY COUNT (status summary), LEFT JOIN + CASE quorum (approval check), LEFT JOIN + SUM CASE cross-tab (reviewer workload), HAVING < threshold (awaiting review), UPDATE + verify (publish transition), correlated MAX subquery in WHERE (latest review), prepared JOIN (reviewer lookup), physical isolation.

## SPEC-10.2.158 Equipment maintenance scheduling
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/EquipmentMaintenanceTest`, `Pdo/MysqlEquipmentMaintenanceTest`, `Pdo/PostgresEquipmentMaintenanceTest`, `Pdo/SqliteEquipmentMaintenanceTest`

An equipment maintenance system with equipment (category, location, last service date, service interval), technicians (with specialties), and service records (type, date, cost, notes) works correctly through ZTD shadow store. The scenario exercises GROUP BY category COUNT for equipment inventory, platform-specific date arithmetic for overdue detection (DATEDIFF on MySQL, date subtraction on PostgreSQL, JULIANDAY on SQLite), LEFT JOIN with COUNT and SUM for technician workload and cost totals, correlated MAX subquery for most recent service per equipment, GROUP BY with SUM and ROUND(AVG, 2) for cost breakdown by category, and prepared statement with JOIN for technician service history lookup.

Verified patterns: GROUP BY COUNT (category inventory), date arithmetic overdue detection (platform-specific), LEFT JOIN + COUNT + SUM (technician workload), correlated MAX subquery in WHERE (latest service), GROUP BY + SUM + ROUND AVG (cost breakdown), prepared JOIN (technician history), physical isolation.

## SPEC-10.2.159 Hotel room management
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/HotelRoomManagementTest`, `Pdo/MysqlHotelRoomManagementTest`, `Pdo/PostgresHotelRoomManagementTest`, `Pdo/SqliteHotelRoomManagementTest`

A hotel management system with rooms (type, floor, rate, status), guests (VIP level), and stays (dates, charge, rating) works correctly through ZTD shadow store. The scenario exercises GROUP BY room type COUNT for inventory, WHERE status filter for occupied rooms, 2-table JOIN with SUM and ROUND(AVG, 2) for revenue by room type, LEFT JOIN with COUNT and SUM for guest stay history, 3-table JOIN with WHERE rating filter and multi-column ORDER BY for high-rated stays, and prepared statement for floor availability lookup.

Verified patterns: GROUP BY COUNT (room type inventory), WHERE status filter (occupied rooms), JOIN + SUM + ROUND AVG (revenue by type), LEFT JOIN + COUNT + SUM (guest history), 3-table JOIN + WHERE + ORDER BY (high rated stays), prepared WHERE + AND (floor availability), physical isolation.

## SPEC-10.2.160 IT incident management
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/IncidentManagementTest`, `Pdo/MysqlIncidentManagementTest`, `Pdo/PostgresIncidentManagementTest`, `Pdo/SqliteIncidentManagementTest`

An IT incident management system with incidents (severity, status, dates, reporter), agents (team, tier), and assignments (action, notes) works correctly through ZTD shadow store. The scenario exercises GROUP BY severity COUNT for incident summary, WHERE IN with CASE for priority labeling and CASE-based ORDER BY for severity sorting, LEFT JOIN with COUNT and COUNT DISTINCT for agent workload (total actions vs unique incidents), WHERE IS NOT NULL for resolved incident filtering, NOT EXISTS subquery for unassigned incident detection, and prepared DISTINCT JOIN for team-based incident lookup.

Verified patterns: GROUP BY COUNT (severity summary), WHERE IN + CASE + CASE ORDER BY (priority labeling), LEFT JOIN + COUNT + COUNT DISTINCT (agent workload), WHERE IS NOT NULL (resolved incidents), NOT EXISTS subquery (unassigned detection), prepared DISTINCT JOIN (team lookup), physical isolation.

## SPEC-10.2.161 Membership tier management
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/MembershipTierTest`, `Pdo/MysqlMembershipTierTest`, `Pdo/PostgresMembershipTierTest`, `Pdo/SqliteMembershipTierTest`

A loyalty program with tier rules (min spending thresholds, benefit percentages), members (current tier, join date), and purchases (amount, category, date) works correctly through ZTD shadow store. The scenario exercises GROUP BY member + category with SUM for spending breakdown, SUM with CASE for tier eligibility determination based on cumulative spending thresholds, LEFT JOIN COALESCE for current benefit percentage lookup, UPDATE for tier promotion with SELECT verification, and prepared BETWEEN + JOIN for date-range purchase history.

Verified patterns: GROUP BY + SUM (spending by category), SUM + CASE thresholds (tier eligibility), LEFT JOIN + COALESCE (benefit lookup), UPDATE + SELECT verify (tier promotion), prepared BETWEEN + JOIN (purchase history), physical isolation.

## SPEC-10.2.162 Customer feedback NPS
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/CustomerNpsTest`, `Pdo/MysqlCustomerNpsTest`, `Pdo/PostgresCustomerNpsTest`, `Pdo/SqliteCustomerNpsTest`

A Net Promoter Score system with customers (segment, signup date) and surveys (score 0-10, channel, comment) works correctly through ZTD shadow store. The scenario exercises CASE for NPS category classification (promoter/passive/detractor), ROUND with SUM CASE and COUNT for overall NPS percentage arithmetic, GROUP BY channel with SUM CASE for per-channel breakdown, LEFT JOIN IS NULL for anti-join detection of customers without feedback, and prepared BETWEEN for score range filtering with JOIN.

Verified patterns: CASE categories (NPS classification), ROUND + SUM CASE / COUNT (NPS percentage), GROUP BY + SUM CASE (channel breakdown), LEFT JOIN IS NULL (no-feedback anti-join), prepared BETWEEN + JOIN (score range), physical isolation.

## SPEC-10.2.163 Asset depreciation tracking
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/AssetDepreciationTest`, `Pdo/MysqlAssetDepreciationTest`, `Pdo/PostgresAssetDepreciationTest`, `Pdo/SqliteAssetDepreciationTest`

A fixed asset depreciation system with assets (category, purchase cost, useful life, salvage value, status) and depreciation entries (period, amount, cumulative, book value) works correctly through ZTD shadow store. The scenario exercises GROUP BY category with COUNT and SUM for asset portfolio summary, JOIN with correlated MAX subquery for latest depreciation entry per asset, ROUND arithmetic for depreciation percentage calculation, HAVING with aggregate threshold for fully depreciated asset detection, and prepared JOIN with category filter for asset lookup.

Verified patterns: GROUP BY + COUNT + SUM (category summary), JOIN + correlated MAX subquery (latest entry), ROUND arithmetic (depreciation %), HAVING aggregate threshold (fully depreciated), prepared JOIN + WHERE (category lookup), physical isolation.

## SPEC-10.2.164 Subscription renewal workflow
**Status:** Partially Verified
**Platforms:** MySQL-PDO, SQLite-PDO
**Tests:** `Pdo/MysqlSubscriptionRenewalTest`, `Pdo/SqliteSubscriptionRenewalTest`

A SaaS subscription renewal system with plans (trial/paid, monthly price), subscriptions (customer, status, date range), and invoices (amount, period, status) exercises DELETE WHERE IN (subquery with JOIN) for expired trial cleanup, INSERT ... SELECT with JOIN for renewal invoice generation, multiple correlated subqueries in a single SELECT list (total_spent, invoice_count), prepared HAVING with JOIN for high-value customer filter, and UPDATE with status verification.

DELETE WHERE IN, correlated subqueries in SELECT list, and UPDATE+verify work on all platforms. INSERT ... SELECT with JOIN has column value nullification on SQLite (SPEC-11.INSERT-SELECT-COMPUTED). Prepared HAVING returns empty on SQLite (SPEC-11.SQLITE-HAVING-PARAMS).

## SPEC-10.2.165 Student grade report
**Status:** Partially Verified
**Platforms:** MySQL-PDO, SQLite-PDO
**Tests:** `Pdo/MysqlStudentGradeReportTest`, `Pdo/SqliteStudentGradeReportTest`

A student grading system with students, weighted assignments (homework, midterm, final), and submissions (score, graded/draft status) exercises CROSS JOIN + LEFT JOIN with COALESCE for missing submissions scored as zero, multiple nested CASE WHEN for letter grade tiers (A through F), weighted average via SUM(score/max * weight), DELETE with EXISTS for draft cleanup, per-assignment AVG with LEFT JOIN, and prepared HAVING with complex aggregate expression.

LEFT JOIN COALESCE (missing=0), weighted CASE WHEN, DELETE EXISTS, and per-assignment AVG work on all platforms. Prepared HAVING with multi-table LEFT JOIN returns empty on SQLite (SPEC-11.SQLITE-HAVING-PARAMS).

## SPEC-10.2.166 Inventory snapshot with UNION ALL
**Status:** Partially Verified (New Finding)
**Platforms:** MySQL-PDO, SQLite-PDO
**Tests:** `Pdo/MysqlInventorySnapshotTest`, `Pdo/SqliteInventorySnapshotTest`

A warehouse inventory system with bins (product, location, base quantity), inbound movements, and outbound movements exercises UNION ALL in derived table for net movement calculation, INSERT ... SELECT with GROUP BY from UNION ALL for snapshot generation, HAVING on aggregated UNION ALL, product-level aggregation with double LEFT JOIN to subqueries, and prepared UNION ALL derived table with BETWEEN date filter.

**New Finding:** UNION ALL inside a derived table (subquery in FROM clause) returns empty results on SQLite and MySQL through ZTD. PostgreSQL works correctly. Top-level UNION ALL works correctly on all platforms — the CTE rewriter handles `SELECT ... UNION ALL SELECT ...` at the top level, but when wrapped in `(... UNION ALL ...) alias`, the rewriter does not rewrite table references inside the UNION branches on SQLite and MySQL. Product-level aggregation using LEFT JOIN to separate aggregate subqueries (no UNION ALL) works correctly on SQLite and PostgreSQL, but returns empty on MySQL (derived tables in LEFT JOIN not rewritten). See SPEC-11.UNION-ALL-DERIVED.

## SPEC-10.2.167 Sales commission with window functions
**Status:** Partially Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/SalesCommissionTest`, `Pdo/MysqlSalesCommissionTest`, `Pdo/PostgresSalesCommissionTest`, `Pdo/SqliteSalesCommissionTest`

A sales commission system with reps and deals exercises ROW_NUMBER() OVER (PARTITION BY rep ORDER BY date) for deal sequencing, SUM() OVER (PARTITION BY ... ROWS UNBOUNDED PRECEDING) for running totals, LAG() for comparing to previous deal amount, window function in derived table filtered by WHERE rn=1 for top deal per rep, prepared statement with window function, and physical isolation.

ROW_NUMBER, SUM OVER, LAG, and prepared window queries work correctly on all platforms. **Window function in derived table returns empty** on all platforms (MySQLi, MySQL-PDO, SQLite) — the CTE rewriter does not handle derived tables containing window functions. PostgreSQL passes window function queries but derived table pattern also returns empty. See SPEC-11.WINDOW-DERIVED.

## SPEC-10.2.168 Project timesheet with ROLLUP
**Status:** Partially Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/ProjectTimesheetTest`, `Pdo/MysqlProjectTimesheetTest`, `Pdo/PostgresProjectTimesheetTest`, `Pdo/SqliteProjectTimesheetTest`

An employee timesheet system with employees, projects, and time entries exercises GROUP BY ... WITH ROLLUP (MySQL) / GROUP BY ROLLUP(...) (PostgreSQL) for department+project subtotals and grand totals, COALESCE for ROLLUP NULL labels, conditional aggregation with SUM(CASE WHEN billable ...), HAVING SUM threshold, prepared GROUP BY with department filter, and physical isolation. SQLite uses UNION ALL approach for subtotals since ROLLUP is unsupported.

Conditional aggregation, HAVING threshold, and prepared GROUP BY work on all platforms. ROLLUP returns correct results on MySQL (MySQLi and PDO) and PostgreSQL. **SQLite UNION ALL subtotal approach in derived table returns only 1 row** instead of 9 — branches are lost, confirming SPEC-11.UNION-ALL-DERIVED.

## SPEC-10.2.169 Waitlist reservation with NOT EXISTS and CASE
**Status:** Partially Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/WaitlistReservationTest`, `Pdo/MysqlWaitlistReservationTest`, `Pdo/PostgresWaitlistReservationTest`, `Pdo/SqliteWaitlistReservationTest`

A restaurant reservation system with tables, reservations, and waitlist exercises NOT EXISTS anti-pattern for available table discovery, nested CASE in SELECT for fit-status classification, multiple correlated COUNT scalar subqueries per row, UPDATE SET with correlated scalar subquery containing nested NOT EXISTS, DELETE with status filter, CASE-as-boolean in WHERE with prepared parameters, and physical isolation.

NOT EXISTS, nested CASE in SELECT, scalar subqueries, correlated UPDATE, and DELETE all work correctly on all platforms. **CASE-as-boolean in WHERE with prepared parameters returns wrong row count** (3 instead of expected 2) on all platforms — the CASE expression used as a filter condition doesn't evaluate correctly when bound parameters are involved. See SPEC-11.CASE-WHERE-PARAMS.

## SPEC-10.2.170 Fleet vehicle tracking with prefix-overlapping table names
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/FleetVehicleTrackingTest`, `Pdo/MysqlFleetVehicleTrackingTest`, `Pdo/PostgresFleetVehicleTrackingTest`, `Pdo/SqliteFleetVehicleTrackingTest`

A fleet management system with tables `vehicle`, `vehicle_type`, and `vehicle_trip` — where "vehicle" is a prefix of both other table names — exercises the CTE rewriter's table reference detection with overlapping names. Tests verify 3-table JOIN across prefix-overlapping tables, GROUP BY SUM with LEFT JOIN for zero-trip vehicles, COUNT(DISTINCT vehicle_id) per type, self-referencing UPDATE arithmetic (mileage += distance), chained self-referencing UPDATEs, prepared BETWEEN date range, and querying only the "vehicle" table when all three tables have shadow data. All tests pass on all platforms — the CTE rewriter correctly distinguishes prefix-overlapping table names.

## SPEC-10.2.171 Donation campaign with self-referencing arithmetic and reordered INSERT
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/DonationCampaignTest`, `Pdo/MysqlDonationCampaignTest`, `Pdo/PostgresDonationCampaignTest`, `Pdo/SqliteDonationCampaignTest`

A donation campaign system with donors, campaigns, and donations exercises INSERT with explicit column list in non-DDL order, self-referencing UPDATE arithmetic (raised = raised + amount), chained self-referencing UPDATEs, COUNT(DISTINCT donor_id), COALESCE(SUM, 0) with LEFT JOIN for campaigns with zero donations, ROUND percentage calculation, DELETE + verify remaining count, and prepared 3-table JOIN by donor email. All tests pass on all platforms — the shadow store correctly handles reordered column inserts and chained self-referencing arithmetic updates.

## SPEC-10.2.178 Appointment scheduling with BETWEEN, EXISTS/NOT EXISTS, overlap detection
**Status:** Verified (SQLite-PDO)
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/AppointmentSchedulingTest`, `Pdo/MysqlAppointmentSchedulingTest`, `Pdo/PostgresAppointmentSchedulingTest`, `Pdo/SqliteAppointmentSchedulingTest`

An appointment scheduling system with rooms and bookings exercises BETWEEN for time range filtering, correlated EXISTS and NOT EXISTS subqueries for room availability, NOT EXISTS with overlap detection (new_start < existing_end AND new_end > existing_start), COALESCE for nullable notes, UPDATE WHERE BETWEEN to cancel morning bookings, COUNT with CASE for status summaries via LEFT JOIN GROUP BY, prepared statements with three BETWEEN/equality params, and IN list inside NOT EXISTS. SQLite-PDO: all tests pass.

## SPEC-10.2.179 Product catalog with LIKE, IN list, COALESCE chain, LIMIT/OFFSET, string functions
**Status:** Verified (SQLite-PDO)
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/ProductCatalogSearchTest`, `Pdo/MysqlProductCatalogSearchTest`, `Pdo/PostgresProductCatalogSearchTest`, `Pdo/SqliteProductCatalogSearchTest`

A product catalog with nullable brand and discount columns exercises LIKE with prefix and contains patterns, NOT LIKE, IN list for multi-category filtering, COALESCE chains for NULL brand and description, COALESCE(discount_price, price) for effective pricing, multi-column ORDER BY (category ASC, price DESC), LIMIT/OFFSET pagination, LENGTH/CHAR_LENGTH and UPPER string functions through the CTE shadow store, prepared LIKE with bound wildcard parameter, and UPDATE with IS NULL in WHERE. SQLite-PDO: all tests pass.

## SPEC-10.2.180 Audit trail versioning with sequential same-row updates, MIN/MAX, LIMIT/OFFSET
**Status:** Verified (SQLite-PDO)
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/AuditTrailVersioningTest`, `Pdo/MysqlAuditTrailVersioningTest`, `Pdo/PostgresAuditTrailVersioningTest`, `Pdo/SqliteAuditTrailVersioningTest`

An audit trail system with documents and log entries exercises three sequential UPDATE mutations on the same row (draft→review→approved→published) verifying shadow store consistency after each step, INSERT into a second table referencing the updated state, MAX(version) with GROUP BY, JOIN with COUNT and HAVING for filtering documents by log entry count, MIN/MAX on timestamps per group, LIMIT/OFFSET pagination of audit log, DELETE by timestamp range, and bulk UPDATE with status filter. SQLite-PDO: all tests pass.

## SPEC-10.2.181 JSON/JSONB column operations through CTE shadow store
**Status:** Verified (with known issue for JSONB `?` operators)
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/JsonColumnTest`, `Pdo/MysqlJsonColumnTest`, `Pdo/PostgresJsonbColumnTest`, `Pdo/SqliteJsonFunctionsTest`, `Pdo/PostgresJsonbOperatorConflictTest`

JSON/JSONB column operations work correctly through the CTE shadow store across all platforms. Verified: JSON_EXTRACT/json_extract(), ->> text extraction, -> JSON extraction, nested path access, JSON in WHERE clause, JSON_CONTAINS/jsonb containment (@>), JSON_LENGTH/jsonb_array_length(), JSON_SET/jsonb_set() in UPDATE, JSON_SEARCH, GROUP BY on JSON-extracted values, ORDER BY JSON-extracted numeric values, CAST JSON text to numeric, and prepared statements with JSON extraction in WHERE.

**PostgreSQL JSONB specifics:** The `->`, `->>`, `@>`, `<@` operators all work correctly. The `?` (key exists), `?|` (any key exists), and `?&` (all keys exist) operators fail — the CTE rewriter treats `?` as a parameter placeholder. See SPEC-11.PG-JSONB-QUESTION-MARK. Workaround: `jsonb_exists()`, `jsonb_exists_any()`, `jsonb_exists_all()`.

**MySQL specifics:** JSON_EXTRACT returns quoted strings (`"Acme"`) — use JSON_UNQUOTE or ->> for text values. JSON_CONTAINS, JSON_SEARCH, JSON_LENGTH all work correctly.

**SQLite specifics:** json_extract(), ->>, json_type(), json_array_length(), json_group_array() all work correctly (SQLite 3.38.0+).

## SPEC-10.2.182 Row value constructors (tuple comparisons)
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/RowValueConstructorTest`, `Pdo/MysqlRowValueConstructorTest`, `Pdo/PostgresRowValueConstructorTest`, `Pdo/SqliteRowValueConstructorTest`

Row value constructors — `WHERE (a, b) IN ((1, 'x'), (2, 'y'))` — work correctly through the CTE shadow store on all platforms. Verified: multi-column IN, multi-column NOT IN, row value equality `(a, b) = (val1, val2)`, row value greater-than comparison, row value IN with JOIN, row value IN with subquery, UPDATE/DELETE with row value WHERE, prepared statements with row value equality, and physical isolation. Composite primary key lookups using tuple syntax are correctly handled by the CTE rewriter.

## SPEC-10.2.183 DISTINCT inside aggregate functions
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/DistinctAggregateTest`, `Pdo/MysqlDistinctAggregateTest`, `Pdo/PostgresDistinctAggregateTest`, `Pdo/SqliteDistinctAggregateTest`

DISTINCT qualifiers inside aggregate functions work correctly through CTE shadow data on all platforms. Verified: COUNT(DISTINCT col), SUM(DISTINCT col), AVG(DISTINCT col), GROUP_CONCAT(DISTINCT col ORDER BY col) on MySQL/SQLite, STRING_AGG(DISTINCT col, sep ORDER BY col) on PostgreSQL. SUM(DISTINCT) correctly deduplicates values before summing (SUM vs SUM(DISTINCT) produce different results). COUNT(DISTINCT) with HAVING works correctly. Prepared statements with COUNT(DISTINCT) in JOINs work correctly.

## SPEC-10.2.184 Anti-join patterns (LEFT JOIN WHERE NULL, NOT EXISTS, NOT IN)
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/AntiJoinPatternTest`, `Pdo/MysqlAntiJoinPatternTest`, `Pdo/PostgresAntiJoinPatternTest`, `Pdo/SqliteAntiJoinPatternTest`

Anti-join patterns for finding rows without matching rows in related tables work correctly through CTE shadow data. All three equivalent anti-join forms produce identical correct results: LEFT JOIN WHERE IS NULL, NOT EXISTS correlated subquery, NOT IN subquery. Also verified: semi-join (EXISTS), chained anti-join (3-table anti-pattern), double NOT EXISTS (combined EXISTS + NOT EXISTS), anti-join mutation sensitivity (anti-join correctly reflects INSERT/DELETE mutations), prepared NOT EXISTS with bound threshold parameter, and physical isolation.

## SPEC-10.2.185 Multiple self-joins (same table aliased N times)
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/MultiSelfJoinTest`, `Pdo/MysqlMultiSelfJoinTest`, `Pdo/PostgresMultiSelfJoinTest`, `Pdo/SqliteMultiSelfJoinTest`

Self-joins where the same table is joined to itself up to 4 times with different aliases work correctly through CTE shadow data on all platforms. Verified: simple self-join (employee + manager via LEFT JOIN), two-level self-join (employee + manager + grand-manager), triple self-join (4 aliases for same table), self-join comparison (e.salary > m.salary), self-join for pair discovery (e1.dept = e2.dept AND e1.id < e2.id), self-join with aggregate (COUNT direct reports per manager with HAVING), NOT EXISTS with self-referencing subquery (top earner per department), self-join after INSERT mutation, and prepared self-join with department filter. The CTE rewriter correctly generates independent CTE references for each alias of the same table.

## SPEC-10.2.186 DELETE with subquery JOIN (orphan cleanup pattern)
**Status:** Verified
**Platforms:** MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Pdo/MysqlDeleteWithSubqueryJoinTest`, `Pdo/PostgresDeleteWithSubqueryJoinTest`, `Pdo/SqliteDeleteWithSubqueryJoinTest`

DELETE statements with WHERE clauses containing subqueries that JOIN other tables work correctly through CTE shadow data. Verified: DELETE WHERE IN (subquery filtering related table), DELETE WHERE NOT EXISTS (orphan detection), DELETE WHERE NOT IN (unordered items), DELETE with compound condition (discontinued AND NOT EXISTS), DELETE with multi-table subquery (subquery JOINs 2 tables), DELETE followed by JOIN verification of remaining data consistency, and prepared DELETE with subquery parameter. The CTE rewriter correctly handles the DELETE target table and subquery source tables simultaneously.

## SPEC-10.2.187 INSERT...SELECT with multi-table JOIN and aggregates
**Status:** Known Issue (see [SPEC-11.INSERT-SELECT-JOIN](11-known-issues.ears.md))
**Platforms:** MySQLi (error), MySQL-PDO (error), PostgreSQL-PDO (NULL columns), SQLite-PDO (NULL columns)
**Tests:** `Pdo/MysqlInsertSelectJoinAggregateTest`, `Pdo/PostgresInsertSelectJoinAggregateTest`, `Pdo/SqliteInsertSelectJoinAggregateTest`

INSERT...SELECT with multi-table JOINs and aggregate functions (COUNT, SUM) fails or produces incorrect results. On MySQL, the InsertTransformer throws "Unknown column 'alias.col' in 'field list'" because it cannot resolve column references from JOINed table aliases. On PostgreSQL and SQLite, rows are inserted but non-PK columns from JOINed tables and aggregate expressions become NULL — extending SPEC-11.INSERT-SELECT-COMPUTED to multi-table JOIN sources. Some source columns (e.g. `c.region`) may preserve values while others (e.g. `c.name`) do not. INSERT...SELECT from a single table (no JOINs) works on MySQL as a workaround.

## SPEC-10.2.188 LIKE pattern matching with ESCAPE clause
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/LikePatternTest`, `Pdo/MysqlLikePatternTest`, `Pdo/PostgresLikePatternTest`, `Pdo/SqliteLikePatternTest`

LIKE pattern matching works correctly through CTE shadow data on all platforms. Verified: `%` wildcard, `_` single-character wildcard, LIKE with ESCAPE clause for literal `%` matching, NOT LIKE, LIKE in UPDATE WHERE, LIKE in DELETE WHERE, LIKE with prepared statement parameters, case-insensitive LIKE (MySQL/SQLite default), and PostgreSQL ILIKE. The CTE rewriter correctly handles ESCAPE clauses and special characters in LIKE patterns.

## SPEC-10.2.189 Scalar subqueries in SELECT list
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/ScalarSubqueryTest`, `Pdo/MysqlScalarSubqueryTest`, `Pdo/PostgresScalarSubqueryTest`, `Pdo/SqliteScalarSubqueryTest`

Correlated scalar subqueries in SELECT lists work correctly through CTE shadow data on all platforms. Verified: single scalar subquery (COUNT), multiple scalar subqueries in same SELECT, correlated scalar subquery with aggregate (MAX), scalar subquery combined with WHERE filter, nested scalar subquery referencing multiple tables, scalar subquery returning NULL (empty result set), scalar subquery reflecting INSERT mutations, and prepared statements with scalar subqueries.

## SPEC-10.2.190 INTERSECT and EXCEPT set operations
**Status:** Partial (see [SPEC-11.MYSQL-EXCEPT-INTERSECT](11-known-issues.ears.md), [SPEC-11.SQLITE-MULTI-COL-INTERSECT](11-known-issues.ears.md))
**Platforms:** PostgreSQL-PDO (fully working), SQLite-PDO (partial), MySQL-PDO/MySQLi (rejected)
**Tests:** `Pdo/PostgresSetOperationTest`, `Pdo/SqliteSetOperationTest`, `Pdo/MysqlSetOperationTest`, `Mysqli/SetOperationTest`

INTERSECT and EXCEPT work correctly on PostgreSQL through CTE shadow data, including INTERSECT ALL, multi-column INTERSECT, EXCEPT with ORDER BY/LIMIT, combined UNION+EXCEPT+INTERSECT with correct precedence, mutation sensitivity, and prepared statements. On SQLite, single-column INTERSECT/EXCEPT work but multi-column INTERSECT and EXCEPT both return empty results (see [SPEC-11.SQLITE-MULTI-COL-SET-OP](11-known-issues.ears.md)). On MySQL (PDO and MySQLi), INTERSECT and EXCEPT are rejected as "Multi-statement SQL" by the CTE rewriter; UNION works correctly. Workarounds for MySQL: use IN/NOT IN subqueries instead.

## SPEC-10.2.191 UPDATE with subqueries in SET and WHERE clauses
**Status:** Partial (see [SPEC-11.UPDATE-SET-CORRELATED-SUBQUERY](11-known-issues.ears.md))
**Platforms:** SQLite-PDO, PostgreSQL-PDO
**Tests:** `Pdo/SqliteUpdateSubqueryTest`, `Pdo/PostgresUpdateSubqueryTest`

UPDATE with subqueries in WHERE clause (e.g., `WHERE category_id IN (SELECT ...)`) works correctly on all platforms. UPDATE with correlated subqueries in the SET clause (e.g., `SET price = price * (1 - (SELECT discount_pct FROM categories WHERE ...))`) fails on SQLite and PostgreSQL — the CTE rewriter produces syntax errors on SQLite and column resolution errors on PostgreSQL. MySQL is NOT affected (all correlated SET patterns work correctly). Self-referencing scalar subqueries in SET also fail on SQLite/PostgreSQL but work on MySQL. Zero-row updates with empty subquery WHERE work correctly on all platforms.

## SPEC-10.2.193 DELETE with correlated subqueries
**Status:** Verified
**Platforms:** SQLite-PDO, PostgreSQL-PDO
**Tests:** `Pdo/SqliteDeleteCorrelatedTest`, `Pdo/PostgresDeleteCorrelatedTest`

DELETE with correlated subqueries works correctly through CTE shadow data. Verified: DELETE WHERE EXISTS (correlated), DELETE with scalar self-referencing subquery (performance < AVG from same table), DELETE WHERE NOT EXISTS, prepared DELETE with correlated subquery and bound parameters. Unlike UPDATE SET with correlated subqueries (which fails on SQLite/PostgreSQL), DELETE handles correlation correctly on all platforms.

## SPEC-10.2.192 INSERT...SELECT with UNION source
**Status:** Verified
**Platforms:** SQLite-PDO
**Tests:** `Pdo/SqliteInsertSelectUnionTest`

INSERT...SELECT where the SELECT source is a UNION or UNION ALL query works correctly through the CTE shadow store on SQLite. Both UNION ALL (preserving duplicates) and UNION (deduplicating) produce correct row counts and values.

## SPEC-10.2.194 User-defined CTEs (WITH ... AS)
**Status:** Partial (see [SPEC-11.USER-CTE-CONFLICT](11-known-issues.ears.md))
**Platforms:** SQLite-PDO (partial), PostgreSQL-PDO (broken)
**Tests:** `Pdo/SqliteUserCteConflictTest`, `Pdo/PostgresUserCteConflictTest`

User-defined CTEs conflict with the CTE rewriter's own WITH clauses. On PostgreSQL, ALL user CTE patterns silently return 0 rows. On SQLite, simple CTEs and CTEs referencing other CTEs work correctly, but multiple CTEs JOINed together return 0 rows. CTE with INSERT...SELECT is unsupported (throws exception) on both platforms.

## SPEC-10.2.195 PostgreSQL RETURNING clause
**Status:** Known Issue (see [SPEC-11.PG-RETURNING](11-known-issues.ears.md))
**Platforms:** PostgreSQL-PDO (broken)
**Tests:** `Pdo/PostgresReturningClauseTest`

RETURNING clause on INSERT/UPDATE/DELETE silently returns 0 rows through the CTE shadow store. Mutations execute correctly but the RETURNING result set is always empty. Affects all RETURNING variants including prepared statements.

## SPEC-10.2.196 INSERT IGNORE / ON CONFLICT DO NOTHING with UNIQUE constraints
**Status:** Known Issue (see [SPEC-11.INSERT-IGNORE-UNIQUE](11-known-issues.ears.md))
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO (all confirmed)
**Tests:** `Mysqli/InsertIgnoreTest`, `Pdo/MysqlInsertIgnoreTest`

The shadow store only checks PRIMARY KEY for duplicate detection in INSERT IGNORE / INSERT OR IGNORE / ON CONFLICT DO NOTHING operations. Non-PK UNIQUE key constraints are silently ignored, allowing duplicate rows to be inserted. Affects all platforms.

## SPEC-10.2.197 MySQL ENUM type ordering and semantics
**Status:** Known Issue (see [SPEC-11.ENUM-ORDERING](11-known-issues.ears.md))
**Platforms:** MySQLi, MySQL-PDO (confirmed)
**Tests:** `Mysqli/EnumTypeTest`, `Pdo/MysqlEnumTypeTest`

ENUM columns in CTE shadow store lose internal index ordering. ORDER BY uses alphabetical order instead of definition order. Comparison operators (`>`, `<`) use alphabetical semantics. DEFAULT ENUM values are not applied. Basic INSERT/SELECT/WHERE equality for ENUM works correctly.

## SPEC-10.2.198 DELETE/UPDATE ORDER BY LIMIT
**Status:** Partial (see [SPEC-11.UPDATE-WHERE-ORDER-DESC-LIMIT](11-known-issues.ears.md))
**Platforms:** MySQLi (works), MySQL-PDO (partial)
**Tests:** `Mysqli/DeleteLimitTest`, `Pdo/MysqlDeleteLimitTest`

Basic DELETE/UPDATE with ORDER BY LIMIT works correctly on MySQLi. On MySQL-PDO, UPDATE with WHERE + ORDER BY DESC + LIMIT updates the wrong row (DESC ordering not respected). DELETE with ORDER BY LIMIT works correctly on both adapters.

## SPEC-10.2.199 Hex literal syntax
**Status:** Partial (see [SPEC-11.HEX-LITERAL-UPDATE](11-known-issues.ears.md))
**Platforms:** MySQLi (partial), MySQL-PDO (partial), SQLite-PDO (works)
**Tests:** `Mysqli/HexLiteralTest`, `Pdo/MysqlHexLiteralTest`, `Pdo/SqliteHexLiteralTest`

INSERT and SELECT with hex literals (`0x...` and `X'...'`) work correctly on all platforms. UPDATE SET with `X'...'` syntax fails on MySQL (parsed as column name 'X'). The `0x...` prefix syntax works in all contexts. SQLite handles both syntaxes correctly in all contexts.

## SPEC-10.2.200 Nested function expressions with prepared params
**Status:** Known Issue (see [SPEC-11.SQLITE-NESTED-FUNC-PARAMS](11-known-issues.ears.md))
**Platforms:** SQLite-PDO (broken); MySQL-PDO, PostgreSQL-PDO (works)
**Tests:** `Pdo/SqliteNestedFunctionWhereTest`

SQLite-only: WHERE clauses with nested function expressions (e.g., `LENGTH(REPLACE(col, 'a', '')) > ?`) return 0 rows when used with prepared statement parameters. The same queries work correctly on MySQL and PostgreSQL.

## SPEC-10.2.201 PostgreSQL GENERATED columns in CTE shadow store
**Status:** Partial
**Platforms:** PostgreSQL-PDO
**Tests:** `Pdo/PostgresGeneratedColumnTest`

PostgreSQL GENERATED ALWAYS AS (expression) STORED columns lose their computed-column semantics in the CTE shadow store. Computed values become TEXT type, causing aggregate functions (SUM, AVG) to fail with "function does not exist" type errors. GENERATED ALWAYS AS IDENTITY columns are also not supported — the shadow store does not generate identity values, resulting in duplicate/null IDs.

## SPEC-10.2.202 PostgreSQL ARRAY literal with ARRAY[] constructor
**Status:** Known Issue (extends [Issue #33](11-known-issues.ears.md))
**Platforms:** PostgreSQL-PDO
**Tests:** `Pdo/PostgresArrayFunctionsTest`

PostgreSQL ARRAY[] constructor syntax (`ARRAY['a','b','c']`) in INSERT causes "Insert values count does not match column count". The commas inside ARRAY[] are misinterpreted as value separators. This extends Issue #33 (array types broken) with a different failure mode — not just CAST issues, but INSERT parsing failures.

## SPEC-10.2.203 UPDATE without WHERE clause
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/UpdateWithoutWhereTest`, `Pdo/MysqlUpdateWithoutWhereTest`, `Pdo/PostgresUpdateWithoutWhereTest`, `Pdo/SqliteUpdateWithoutWhereTest`

`UPDATE table SET col = value` (no WHERE clause) correctly updates all rows in the shadow store on all platforms. This is different from DELETE without WHERE, which is silently ignored on SQLite ([Issue #7](11-known-issues.ears.md)). The `WHERE 1=1` workaround is not needed for UPDATE.

## SPEC-10.2.204 Scalar subquery in INSERT VALUES position
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/InsertValuesSubqueryTest`, `Pdo/MysqlInsertValuesSubqueryTest`, `Pdo/PostgresInsertValuesSubqueryTest`, `Pdo/SqliteInsertValuesSubqueryTest`

`INSERT INTO t (id, total) VALUES (1, (SELECT COUNT(*) FROM t2))` with scalar subqueries in the VALUES clause works correctly on all platforms. The CTE rewriter correctly rewrites table references inside scalar subqueries embedded in VALUES. COUNT, SUM, MAX, and filtered subqueries all produce correct values. Subqueries correctly see shadow data from prior mutations.

## SPEC-10.2.205 Table-less queries through ZTD
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/TablelessQueryTest`, `Pdo/MysqlTablelessQueryTest`, `Pdo/PostgresTablelessQueryTest`, `Pdo/SqliteTablelessQueryTest`

Queries that do not reference any table (system functions, arithmetic, literals) work correctly through ZTD on all platforms. Verified patterns: `SELECT 1`, `SELECT 1+1`, `SELECT 'hello'`, `SELECT CURRENT_TIMESTAMP`, `SELECT NULL`, platform-specific system functions (VERSION(), DATABASE(), sqlite_version(), current_database(), etc.), and prepared table-less queries.

## SPEC-10.2.206 CREATE TABLE mid-session then DML
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/DdlMidSessionDmlTest`, `Pdo/MysqlDdlMidSessionDmlTest`, `Pdo/PostgresDdlMidSessionDmlTest`, `Pdo/SqliteDdlMidSessionDmlTest`

When a table is created through ZTD mid-session (`CREATE TABLE` via exec/query), subsequent DML (INSERT, SELECT, UPDATE, DELETE) on the new table works correctly through the shadow store. On MySQL and PostgreSQL, the table is NOT physically created — the ZTD adapter maintains the schema and data entirely in the shadow store. On SQLite (fromPdo), the DDL passes through to the underlying in-memory connection.

## SPEC-10.2.207 Expression-based WHERE in DELETE/UPDATE
**Status:** Partially Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/ExpressionWhereClauseDmlTest`, `Pdo/MysqlExpressionWhereClauseDmlTest`, `Pdo/PostgresExpressionWhereClauseDmlTest`, `Pdo/SqliteExpressionWhereClauseDmlTest`

DELETE and UPDATE with function calls in WHERE (LENGTH, LOWER, ABS, CONCAT/||, arithmetic expressions) work correctly on all platforms. CASE expressions in DELETE/UPDATE WHERE fail on MySQL ([Issue #96](11-known-issues.ears.md)). Prepared DELETE with function WHERE fails on SQLite ([Issue #95](11-known-issues.ears.md)).

## SPEC-10.2.208 ALL/ANY/SOME subquery comparison operators
**Status:** Partially Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Pdo/MysqlAnyAllSubqueryComparisonTest`, `Pdo/PostgresAnyAllSubqueryComparisonTest`, `Pdo/SqliteAnyAllSubqueryComparisonTest`

SQL-standard `> ALL (SELECT ...)`, `= ANY (SELECT ...)`, and `>= SOME (SELECT ...)` comparison operators work correctly on MySQL through the CTE shadow store, including in SELECT, UPDATE, and DELETE contexts. Shadow mutations in both the main and subquery tables are correctly reflected. On SQLite, equivalent patterns using `> (SELECT MAX(...))`, `IN (SELECT ...)`, and `< (SELECT MIN(...))` work correctly. On PostgreSQL, SELECT-only ALL/ANY/SOME patterns work, but UPDATE with `= ANY(SELECT ...)` or `WHERE id IN (SELECT ... FROM other_table)` fails with syntax error — the CTE rewriter incorrectly adds the subquery's table to the outer FROM clause ([Issue #100](11-known-issues.ears.md)).

## SPEC-10.2.209 INSERT ... SELECT DISTINCT
**Status:** Known Issue ([Issue #99](11-known-issues.ears.md))
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/InsertSelectDistinctTest`, `Pdo/MysqlInsertSelectDistinctTest`, `Pdo/PostgresInsertSelectDistinctTest`, `Pdo/SqliteInsertSelectDistinctTest`

`INSERT INTO t2 SELECT DISTINCT col FROM t1` ignores the DISTINCT keyword on all platforms. On MySQL (PDO and MySQLi), all source rows are inserted without deduplication. On PostgreSQL and SQLite, correct row count but NULL values stored instead of actual values. COUNT(DISTINCT col) in standalone SELECT works correctly on all platforms.

## SPEC-10.2.210 Cross-table shadow DELETE/UPDATE with IN subquery
**Status:** Partially Verified
**Platforms:** MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Pdo/MysqlCrossTableShadowDeleteTest`, `Pdo/PostgresCrossTableShadowDeleteTest`, `Pdo/SqliteCrossTableShadowDeleteTest`

DELETE/UPDATE with WHERE IN (subquery on a different table) where BOTH tables have shadow data works correctly on MySQL and SQLite. On PostgreSQL, the CTE rewriter incorrectly adds the subquery's table to the outer FROM clause causing syntax errors ([Issue #100](11-known-issues.ears.md)). Additionally, PostgreSQL tables with BOOLEAN columns trigger CAST('' AS BOOLEAN) errors due to existing [Issue #6](11-known-issues.ears.md). Three-table chain operations (ban→update user→delete posts) work correctly on MySQL and SQLite.

## SPEC-10.2.211 GROUP BY HAVING with aggregate thresholds on shadow data
**Status:** Verified
**Platforms:** MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Pdo/MysqlHavingThresholdShadowTest`, `Pdo/PostgresHavingThresholdShadowTest`, `Pdo/SqliteHavingThresholdShadowTest`

GROUP BY with HAVING aggregate thresholds (COUNT >= N, SUM >= N, AVG > N) correctly filters shadow data on all platforms. Verified: HAVING COUNT, HAVING SUM, HAVING with multiple conditions (COUNT AND AVG), multi-column GROUP BY HAVING, HAVING with scalar subquery threshold, HAVING reflecting INSERT/DELETE/UPDATE mutations, conditional aggregation with SUM(CASE WHEN ...) in HAVING, PostgreSQL-specific FILTER clause with HAVING, and prepared HAVING with bound threshold parameter.

## SPEC-10.2.212 Prepared statements with expressions in SET and subqueries in WHERE
**Status:** Partially Verified
**Platforms:** MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Pdo/MysqlPreparedExpressionDmlTest`, `Pdo/PostgresPreparedExpressionDmlTest`, `Pdo/SqlitePreparedExpressionDmlTest`

Prepared UPDATE with arithmetic expressions in SET (`price = price * ?`), addition expressions (`quantity = quantity + ?`), re-execution with different parameters, CASE expressions with parameters in SET, and prepared SELECT with computed columns all work correctly on all platforms. Prepared DELETE with expression in WHERE (`price * quantity < ?`) works on MySQL and PostgreSQL but deletes ALL rows on SQLite ([Issue #101](11-known-issues.ears.md)). Prepared UPDATE CASE with RETURNING returns empty on PostgreSQL (existing RETURNING limitation, [Issue #53](11-known-issues.ears.md)).

## SPEC-10.2.213 Derived table with multi-table JOIN
**Status:** Partially Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/DerivedTableAndNestingTest`, `Pdo/MysqlDerivedTableAndNestingTest`, `Pdo/PostgresDerivedTableAndNestingTest`, `Pdo/SqliteDerivedTableAndNestingTest`

SELECT from a derived table containing a multi-table JOIN with GROUP BY returns empty on MySQL (MySQLi, MySQL-PDO) and SQLite. PostgreSQL handles correctly. Derived tables with simple WHERE (no JOIN) also return empty on MySQL/SQLite. CROSS JOIN, nested subqueries (2 and 3 levels deep), and multiple scalar subqueries in WHERE all work correctly on all platforms. This extends Issue #13 — the CTE rewriter does not rewrite table references inside derived tables containing JOINs on MySQL/SQLite.

## SPEC-10.2.214 Self-JOIN, NATURAL JOIN, implicit comma JOIN on shadow data
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/SelfJoinHierarchyTest`, `Pdo/MysqlSelfJoinHierarchyTest`, `Pdo/PostgresSelfJoinHierarchyTest`, `Pdo/SqliteSelfJoinHierarchyTest`, `Mysqli/NaturalJoinQueryTest`, `Pdo/MysqlNaturalJoinQueryTest`, `Pdo/PostgresNaturalJoinQueryTest`, `Pdo/SqliteNaturalJoinQueryTest`, `Mysqli/ImplicitJoinAndEdgeCaseTest`, `Pdo/MysqlImplicitJoinAndEdgeCaseTest`, `Pdo/PostgresImplicitJoinAndEdgeCaseTest`, `Pdo/SqliteImplicitJoinAndEdgeCaseTest`

Self-JOIN (employee→manager), NATURAL JOIN, NATURAL LEFT JOIN, and implicit comma JOIN (`FROM t1, t2 WHERE ...`) all work correctly on all platforms through the CTE shadow store. Self-JOINs correctly reflect INSERT, UPDATE, and DELETE mutations. NATURAL JOIN with aggregates works. Implicit comma joins with aggregates and after mutations work. BETWEEN, EXISTS, NOT EXISTS correlated subqueries, and conditional aggregation (SUM(CASE WHEN ...)) all work correctly.

## SPEC-10.2.215 Three-table JOIN chain on shadow data
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/ThreeTableJoinChainTest`, `Pdo/MysqlThreeTableJoinChainTest`, `Pdo/PostgresThreeTableJoinChainTest`, `Pdo/SqliteThreeTableJoinChainTest`

Three-table INNER JOIN chains, three-table LEFT JOIN chains, three-table JOINs with GROUP BY and aggregates (COUNT, AVG), three-table JOINs with HAVING, and three-table JOINs after mutations all work correctly on all platforms.

## SPEC-10.2.216 GROUP_CONCAT / STRING_AGG and COUNT(DISTINCT) on shadow data
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/GroupConcatAggregateTest`, `Pdo/MysqlGroupConcatAggregateTest`, `Pdo/PostgresStringAggTest`, `Pdo/SqliteGroupConcatAggregateTest`, `Mysqli/MultiDistinctAggregateTest`, `Pdo/MysqlMultiDistinctAggregateTest`, `Pdo/PostgresMultiDistinctAggregateTest`, `Pdo/SqliteMultiDistinctAggregateTest`

GROUP_CONCAT (MySQL), STRING_AGG (PostgreSQL), GROUP_CONCAT (SQLite) all work correctly on shadow data including custom separators, DISTINCT, ORDER BY within function, and after INSERT mutations. Multiple COUNT(DISTINCT col) columns in a single query, SUM(DISTINCT), COUNT(DISTINCT) with GROUP BY, and aggregates after mutations all work correctly on all platforms.

## SPEC-10.2.217 INSERT...SELECT with ORDER BY LIMIT on shadow data
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/InsertSelectLimitTest`, `Pdo/MysqlInsertSelectLimitTest`, `Pdo/PostgresInsertSelectLimitTest`, `Pdo/SqliteInsertSelectLimitTest`

INSERT...SELECT with ORDER BY and LIMIT (top-N copy pattern), INSERT...SELECT with WHERE and LIMIT, INSERT...SELECT LIMIT after mutations, and INSERT...SELECT with OFFSET all work correctly on all platforms. The CTE rewriter preserves ORDER BY and LIMIT clauses in INSERT...SELECT source queries.

## SPEC-10.2.218 Table aliases in UPDATE/DELETE (MySQL, PostgreSQL)
**Status:** Partially Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO
**Tests:** `Mysqli/AliasedTableDmlTest`, `Pdo/MysqlAliasedTableDmlTest`, `Pdo/PostgresAliasedTableDmlTest`

MySQL-style aliased UPDATE (`UPDATE t p SET p.col = ...`) and aliased DELETE (`DELETE p FROM t p WHERE ...`) work correctly on MySQLi and MySQL-PDO. PostgreSQL-style aliased UPDATE (`UPDATE t AS p SET col = p.col * 1.10 WHERE p.col = ...`) and aliased DELETE (`DELETE FROM t p WHERE p.col = 0`) work correctly. However, PostgreSQL aliased UPDATE with self-referencing subquery (`WHERE p.price > (SELECT AVG(price) FROM same_table)`) fails with syntax error due to existing Issue #74. SQLite does not support table aliases in UPDATE/DELETE statements natively.

## SPEC-10.2.219 Multi-table DML patterns on shadow data
**Status:** Partially Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/MultiTableDmlPatternsTest`, `Pdo/MysqlMultiTableDmlPatternsTest`, `Pdo/PostgresMultiTableDmlPatternsTest`, `Pdo/SqliteMultiTableDmlPatternsTest`

MySQL multi-table UPDATE JOIN (`UPDATE t1 JOIN t2 ON ... SET t1.col = ...`) works correctly on MySQLi and MySQL-PDO. PostgreSQL UPDATE FROM (`UPDATE t1 SET col = ... FROM t2 WHERE ...`) works correctly. SQLite UPDATE with IN subquery works correctly. DELETE with IN subquery from other table works on all platforms. DELETE with EXISTS correlated subquery works on all platforms. INSERT...SELECT from JOIN fails on all platforms (Issue #49) — MySQL produces "Unknown column" error, PostgreSQL/SQLite insert 0 rows.

## SPEC-10.2.220 Window function queries on shadow data
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/WindowFunctionQueryTest`, `Pdo/MysqlWindowFunctionQueryTest`, `Pdo/PostgresWindowFunctionQueryTest`, `Pdo/SqliteWindowFunctionQueryTest`

Window function queries (ROW_NUMBER, RANK, DENSE_RANK, SUM OVER, LAG, LEAD, NTILE) work correctly on all platforms through the CTE shadow store. Verified: PARTITION BY, ORDER BY within OVER, ties in RANK/DENSE_RANK, SUM OVER PARTITION, LAG/LEAD with NULLs, NTILE distribution. Window functions correctly reflect INSERT, UPDATE, and DELETE mutations. Prepared statements with WHERE parameters combined with window functions work correctly on all platforms (using ? placeholders; $N style affected by Issue #85 on PostgreSQL).

## SPEC-10.2.221 INSERT...SELECT with UNION/UNION ALL on shadow data
**Status:** Partially Verified (see [SPEC-11.MYSQL-INSERT-SELECT-UNION](11-known-issues.ears.md))
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/InsertFromUnionTest`, `Pdo/MysqlInsertFromUnionTest`, `Pdo/PostgresInsertFromUnionTest`, `Pdo/SqliteInsertFromUnionTest`

INSERT...SELECT with UNION ALL and UNION (distinct) from multiple source tables works correctly on SQLite and PostgreSQL. On MySQL (both MySQLi and MySQL-PDO), the CTE rewriter rejects these as "Multi-statement SQL" ([Issue #103](11-known-issues.ears.md)). PostgreSQL additionally handles INSERT from INTERSECT and EXCEPT correctly. Prepared statements with parameters in UNION branches work on SQLite and PostgreSQL. Aggregation after INSERT from UNION works correctly on all platforms that support the INSERT.

## SPEC-10.2.222 Set operations (UNION/INTERSECT/EXCEPT) on shadow data
**Status:** Partially Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Pdo/SqliteSetOperationsQueryTest`, `Pdo/PostgresInsertFromUnionTest`

UNION and UNION ALL work correctly on all platforms for SELECT queries. INTERSECT and EXCEPT work as single-column queries on SQLite, but multi-column INTERSECT/EXCEPT return empty on SQLite (Issue #50). PostgreSQL handles all set operations correctly. MySQL rejects EXCEPT/INTERSECT as multi-statement (Issue #14). Set operations correctly reflect mutations on all platforms where they are supported. Prepared UNION with parameters works on SQLite and PostgreSQL.

## SPEC-10.2.223 Prepared BETWEEN and CASE-in-HAVING on shadow data
**Status:** Partially Verified
**Platforms:** MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Pdo/SqlitePreparedBetweenAndCaseHavingTest`

Prepared SELECT/UPDATE/DELETE with BETWEEN work correctly on SQLite. Prepared BETWEEN with date ranges, NOT BETWEEN, and combined BETWEEN+AND conditions work. CASE in HAVING without parameters works correctly. CASE in HAVING with prepared parameters returns empty on SQLite (related to Issue #22 — HAVING with prepared params). Non-prepared CASE in HAVING, prepared BETWEEN for DML, and BETWEEN after mutations all work correctly.

## SPEC-10.2.224 GROUP_CONCAT and multi-step DML lifecycle on shadow data
**Status:** Verified
**Platforms:** SQLite-PDO
**Tests:** `Pdo/SqliteGroupConcatAndMultiDmlLifecycleTest`

GROUP_CONCAT with custom separator, GROUP_CONCAT after INSERT/DELETE, GROUP_CONCAT DISTINCT, and multi-step DML lifecycles (INSERT→UPDATE→SELECT, multiple UPDATEs on same row, INSERT→UPDATE→DELETE cycle, bulk UPDATE→partial DELETE, batch INSERT→aggregate) all work correctly on SQLite. GROUP_CONCAT with ORDER BY subquery returns empty (known derived table issue). The shadow store correctly maintains state across arbitrary sequences of mutations on the same rows.

## SPEC-10.2.225 Column order INSERT and multi-row INSERT VALUES
**Status:** Verified
**Platforms:** MySQL-PDO, PostgreSQL-PDO, MySQLi (all pass); SQLite-PDO (previously verified)
**Tests:** `Pdo/MysqlColumnOrderAndMultiRowInsertTest`, `Pdo/PostgresColumnOrderAndMultiRowInsertTest`, `Mysqli/ColumnOrderAndMultiRowInsertTest`

INSERT with columns in non-DDL order (reverse, partial, mixed) correctly maps values to columns on all platforms. Multi-row INSERT VALUES (3+ rows in single statement) works on all platforms. Combined multi-row + reordered columns works. Prepared INSERT with reordered columns works. UPDATE after reordered INSERT preserves column mappings. Multi-row INSERT followed by aggregate queries returns correct results. This extends `SqliteColumnOrderInsertTest` cross-platform coverage.

## SPEC-10.2.226 HAVING with prepared params and compound WHERE conditions
**Status:** Partially Verified
**Platforms:** MySQL-PDO (pass), PostgreSQL-PDO (pass), SQLite-PDO (HAVING with params fails — Issue #22)
**Tests:** `Pdo/MysqlHavingPreparedAndCompoundWhereTest`, `Pdo/PostgresHavingPreparedAndCompoundWhereTest`, `Pdo/SqliteHavingPreparedAndCompoundWhereTest`

HAVING COUNT/SUM with bound parameters works on MySQL PDO and PostgreSQL PDO. SQLite fails (confirms Issue #22). HAVING combined with WHERE and two bound parameters works on MySQL/PostgreSQL. HAVING after shadow INSERT/DELETE correctly reflects mutations on MySQL/PostgreSQL. Compound WHERE with OR/AND and parentheses works for UPDATE and DELETE on all platforms. Nested OR/AND in SELECT works. Prepared compound WHERE UPDATE works. Compound WHERE after multiple mutations (INSERT + UPDATE) works.

## SPEC-10.2.227 CASE in WHERE and UNION/UNION ALL SELECT
**Status:** Verified
**Platforms:** MySQL-PDO (pass), PostgreSQL-PDO (pass, 1 incomplete — nested CASE type mismatch), SQLite-PDO (pass)
**Tests:** `Pdo/MysqlCaseWhereAndUnionSelectTest`, `Pdo/PostgresCaseWhereAndUnionSelectTest`, `Pdo/SqliteCaseWhereAndUnionSelectTest`

CASE expressions in SELECT WHERE clause work on all platforms (simple CASE, CASE with prepared param, CASE after UPDATE, searched CASE). Nested CASE works on MySQL and SQLite; PostgreSQL requires boolean return type (integer return triggers type error). UNION ALL between two shadow tables works on all platforms. UNION DISTINCT works. UNION ALL after mutations (INSERT + DELETE) correctly reflects changes. UNION with prepared params works. UNION ALL on same table with different WHERE conditions works. String concat (|| on PostgreSQL/SQLite, CONCAT on MySQL) in UPDATE SET and WHERE works after mutations. Note: CASE in WHERE for DML (DELETE/UPDATE) still matches all rows on MySQL (Issue #96) — SELECT is not affected.

## SPEC-10.2.228 GROUP BY expression and INSERT with function calls
**Status:** Verified
**Platforms:** MySQL-PDO (pass), SQLite-PDO (pass)
**Tests:** `Pdo/MysqlGroupByExpressionAndInsertFunctionTest`, `Pdo/SqliteGroupByExpressionAndInsertFunctionTest`

GROUP BY with CASE expression works, including after INSERT mutations and with HAVING. GROUP BY with function expression (UPPER) works. GROUP BY with date extraction (MONTH on MySQL, SUBSTR on SQLite) works. GROUP BY expression with prepared params works on MySQL. INSERT with UPPER/LOWER, CONCAT/||, arithmetic expressions, NOW()/datetime() all produce correct shadow data. INSERT with function then UPDATE then query lifecycle works.

## SPEC-10.2.229 Prepared UPDATE CASE in SET and INSERT with scalar subquery in VALUES
**Status:** Verified
**Platforms:** MySQL-PDO (pass), PostgreSQL-PDO (pass)
**Tests:** `Pdo/MysqlPreparedCaseSetAndSubqueryInsertTest`, `Pdo/PostgresPreparedCaseSetAndSubqueryInsertTest`

Prepared UPDATE with CASE in SET using ? params works on both MySQL and PostgreSQL (note: Issue #61 is specific to $N params on PostgreSQL). Multiple CASE expressions in single SET clause with prepared params works. INSERT with scalar subquery in VALUES (single-row SELECT, MAX, COUNT*10, cross-table subquery) works on both platforms. DELETE with self-referencing AVG subquery (WHERE price < (SELECT AVG(price) FROM same_table)) works. String concat || in UPDATE SET and WHERE works on PostgreSQL.

## SPEC-10.2.230 Table name prefix isolation
**Status:** Verified
**Platforms:** SQLite-PDO (pass)
**Tests:** `Pdo/SqliteTableNamePrefixConfusionTest`

Tables with overlapping name prefixes (e.g., orders/order_items/order_archive) are correctly distinguished by the CTE rewriter. INSERT into one table does not affect queries on a table with a shared prefix. JOIN between prefix-sharing tables works. Mutating one or both tables then JOINing works correctly. All three tables in a single session maintain independent shadow stores.

## SPEC-10.2.231 PostgreSQL BOOLEAN FALSE in shadow store
**Status:** Known Issue (confirms Issue #6)
**Platforms:** PostgreSQL-PDO
**Tests:** `Pdo/PostgresBooleanColumnShadowTest`

PostgreSQL BOOLEAN TRUE values work correctly in the shadow store. BOOLEAN FALSE values cause `CAST('' AS BOOLEAN)` error because PHP serializes `false` as empty string. This affects SELECT, UPDATE, and DELETE on any table with BOOLEAN columns when any shadow row contains a FALSE value. Even selecting only non-boolean columns fails because the CTE rewriter includes all columns. Using INTEGER instead of BOOLEAN avoids the issue.

## SPEC-10.2.232 GROUP BY / ORDER BY position number
**Status:** Verified
**Platforms:** SQLite-PDO (pass), MySQL-PDO (pass), PostgreSQL-PDO (pass), MySQLi (pass)
**Tests:** `Pdo/SqliteGroupByPositionNumberTest`, `Pdo/MysqlGroupByPositionNumberTest`, `Pdo/PostgresGroupByPositionNumberTest`, `Mysqli/GroupByPositionNumberTest`

GROUP BY and ORDER BY using column position numbers (e.g., `GROUP BY 1, 2 ORDER BY 3 DESC`) work correctly through ZTD on all platforms. Tested: single-position GROUP BY, multi-position GROUP BY, ORDER BY position DESC, GROUP BY position with HAVING, and prepared statements with positional grouping + WHERE params. The CTE rewriter correctly preserves positional references when wrapping queries in CTEs.

## SPEC-10.2.233 Conditional aggregation with prepared params
**Status:** Verified
**Platforms:** SQLite-PDO (pass), MySQL-PDO (pass), MySQLi (pass), PostgreSQL-PDO (pass with ?)
**Tests:** `Pdo/SqliteConditionalAggregateWithParamsTest`, `Pdo/MysqlConditionalAggregateWithParamsTest`, `Pdo/PostgresConditionalAggregateWithParamsTest`, `Mysqli/ConditionalAggregateWithParamsTest`

`SUM(CASE WHEN status = ? THEN amount ELSE 0 END)` and `COUNT(CASE WHEN status = ? THEN 1 END)` with prepared params work correctly on all platforms. Multiple conditional aggregates with different params in the same query also work. Conditional aggregate in HAVING with param works on SQLite and MySQL (PostgreSQL not tested with HAVING variant due to possible #22 interaction).

## SPEC-10.2.234 Prepared UPDATE/DELETE with BETWEEN
**Status:** Verified
**Platforms:** SQLite-PDO (pass)
**Tests:** `Pdo/SqlitePreparedUpdateBetweenTest`

Prepared UPDATE and DELETE with `WHERE col BETWEEN ? AND ?` work correctly on SQLite. NOT BETWEEN also works. Chained prepared BETWEEN updates (first A range, then B range) correctly apply both modifications. The CTE rewriter preserves parameter binding positions for BETWEEN clauses in DML statements.

## SPEC-10.2.235 UPDATE with multiple subqueries in SET clause
**Status:** Partial (extends Issue #51)
**Platforms:** MySQL-PDO (pass), MySQLi (pass), SQLite-PDO (fails for correlated/WHERE), PostgreSQL-PDO (fails)
**Tests:** `Pdo/SqliteMultiSubqueryUpdateSetTest`, `Pdo/MysqlMultiSubqueryUpdateSetTest`, `Pdo/PostgresMultiSubqueryUpdateSetTest`, `Mysqli/MultiSubqueryUpdateSetTest`

UPDATE with multiple non-correlated subqueries without WHERE in SET (`SET col1 = (SELECT MIN(x) FROM t), col2 = (SELECT MAX(x) FROM t)`) works on all platforms. However, any subquery in SET that contains `FROM ... WHERE` fails on SQLite ("near FROM: syntax error") and PostgreSQL ("ambiguous column" or "must appear in GROUP BY"). Multiple correlated subqueries and prepared subqueries with params also fail on SQLite and PostgreSQL. MySQL handles all variants correctly.

## SPEC-10.2.236 MySQL UPDATE JOIN with derived table
**Status:** Known Issue (Issue #104)
**Platforms:** MySQLi (fails), MySQL-PDO (fails)
**Tests:** `Pdo/MysqlUpdateJoinPatternTest`, `Mysqli/MultiSubqueryUpdateSetTest`

MySQL UPDATE JOIN with a derived table (subquery in JOIN position) fails with "Identifier name is too long" — the CTE rewriter treats the subquery as a table identifier. UPDATE JOIN with direct tables works correctly. Workaround: use correlated subqueries in SET instead.

## SPEC-10.2.237 INSERT...SELECT with GROUP BY and prepared params
**Status:** Partial (extends Issue #20, #22)
**Platforms:** MySQL-PDO (pass), SQLite-PDO (fails — NULLs), PostgreSQL-PDO (fails — NULLs)
**Tests:** `Pdo/SqliteInsertSelectGroupByWithParamsTest`, `Pdo/MysqlInsertSelectGroupByWithParamsTest`, `Pdo/PostgresInsertSelectGroupByWithParamsTest`

INSERT...SELECT with GROUP BY and aggregate functions (COUNT, SUM) stores NULL aggregates on SQLite and PostgreSQL (extends Issue #20). Both exec() and prepared statement paths produce NULLs. Additionally, INSERT...SELECT with GROUP BY HAVING and prepared params returns 0 rows on SQLite (extends Issue #22). MySQL handles all variants correctly, including prepared params. INSERT OR REPLACE with computed expressions (UPPER, concatenation, arithmetic) in VALUES works correctly on SQLite.

## SPEC-10.2.238 INSERT OR REPLACE with computed expressions in VALUES
**Status:** Verified
**Platforms:** SQLite-PDO (pass)
**Tests:** `Pdo/SqliteInsertOrReplaceComputedTest`

INSERT OR REPLACE with function calls (UPPER, LOWER), string concatenation (||), and arithmetic expressions (1+1) in the VALUES clause works correctly on SQLite through ZTD. Both replacement (conflict) and insert (no conflict) paths produce correct computed values. No duplicate PKs created after multiple replaces.

## SPEC-10.2.239 UPSERT with subquery in SET expression
**Status:** Known Issue (Issue #105)
**Platforms:** MySQLi (fails), MySQL-PDO (fails), PostgreSQL-PDO (fails), SQLite-PDO (fails)
**Tests:** `Pdo/MysqlUpsertSubqueryInSetTest`, `Mysqli/UpsertSubqueryInSetTest`, `Pdo/PostgresUpsertSubqueryInSetTest`, `Pdo/SqliteUpsertSubqueryInSetTest`

INSERT with upsert (ON DUPLICATE KEY UPDATE / ON CONFLICT DO UPDATE) containing a subquery in the SET expression fails on all platforms. MySQL/SQLite: subquery evaluates to 0. PostgreSQL: CAST of subquery text as literal string. All platforms: prepared variant breaks parameter count/index. New row inserts (no conflict) and simple SET expressions work correctly.

## SPEC-10.2.240 PostgreSQL UPDATE...FROM with prepared $N params
**Status:** Known Issue (Issue #106)
**Platforms:** PostgreSQL-PDO (fails with $N, passes with ? and :name)
**Tests:** `Pdo/PostgresUpdateFromPreparedTest`

UPDATE...FROM with `$N` positional parameters does not apply the update — rows remain unchanged. The same query works with `?` placeholders and `:name` named parameters. UPDATE...FROM via exec() with literals also works.

## SPEC-10.2.241 Named parameters (:name style)
**Status:** Verified
**Platforms:** SQLite-PDO (pass, except HAVING — Issue #22), MySQL-PDO (pass), PostgreSQL-PDO (pass)
**Tests:** `Pdo/SqliteNamedParamTest`, `Pdo/MysqlNamedParamTest`, `Pdo/PostgresNamedParamTest`

Named parameters (:name style) work correctly for SELECT, INSERT, UPDATE, DELETE, and subquery WHERE patterns on all platforms. GROUP BY HAVING with named params works on MySQL and PostgreSQL but returns empty on SQLite (consistent with Issue #22). Notably, PostgreSQL GROUP BY HAVING with `:name` params works, while `$N` positional params fail (Issue #22), suggesting the issue is in `$N` parameter handling specifically.

## SPEC-10.2.242 EXPLAIN/DESCRIBE/SHOW diagnostic statements
**Status:** Known Issue (Issue #107)
**Platforms:** SQLite-PDO (blocked), MySQL-PDO (blocked), PostgreSQL-PDO (likely blocked)
**Tests:** `Pdo/SqliteExplainThroughZtdTest`, `Pdo/MysqlExplainThroughZtdTest`

EXPLAIN, EXPLAIN QUERY PLAN, DESCRIBE, and SHOW CREATE TABLE are all blocked by ZTD with "Statement type not supported SQL statement." These read-only diagnostic statements should pass through to the physical database.

## SPEC-10.2.243 MySQL DELETE with multi-table JOIN and prepared params
**Status:** Verified
**Platforms:** MySQL-PDO (pass)
**Tests:** `Pdo/MysqlDeleteMultiTablePreparedTest`

DELETE with JOIN and prepared parameters works correctly on MySQL PDO. Tested: JOIN with single param, multi-param, shadow-inserted data, and comma syntax. All variants produce correct results.

## SPEC-10.2.244 User-written multiple CTEs
**Status:** Partial (extends Issue #52)
**Platforms:** MySQL-PDO (pass), SQLite-PDO (partial — CROSS JOIN fails)
**Tests:** `Pdo/SqliteMultipleCteSelectTest`, `Pdo/MysqlMultipleCteSelectTest`

Single user-written CTEs, CTEs with shadow data, CTEs with prepared params, chained CTEs (referencing other CTEs), and multiple CTEs with scalar subqueries all work correctly on both MySQL and SQLite. However, multiple CTEs with CROSS JOIN returns 0 rows on SQLite (extends Issue #52). MySQL handles all CTE patterns correctly.

## SPEC-10.2.245 UPDATE CASE with correlated subquery in SET
**Status:** Partial (extends Issues #51, #61)
**Platforms:** MySQL-PDO (pass), SQLite-PDO (fails — syntax error), PostgreSQL-PDO (fails — no-op or grouping error)
**Tests:** `Pdo/SqliteUpdateCaseSubqueryTest`, `Pdo/MysqlUpdateCaseSubqueryTest`, `Pdo/PostgresUpdateCaseSubqueryTest`

UPDATE SET with CASE WHEN EXISTS(correlated subquery) and CASE WHEN (scalar correlated subquery > threshold) works correctly on MySQL (both exec and prepared). SQLite fails with "near FROM: syntax error" (extends Issue #51). PostgreSQL fails: CASE WHEN EXISTS is a no-op even via exec() (extends Issue #61 beyond prepared-only), CASE with scalar correlated subquery produces "must appear in GROUP BY" error.

## SPEC-10.2.246 INSERT...SELECT...ON DUPLICATE KEY UPDATE with prepared params
**Status:** Verified
**Platforms:** MySQL-PDO (pass)
**Tests:** `Pdo/MysqlInsertSelectOnDuplicateTest`

INSERT...SELECT...ON DUPLICATE KEY UPDATE works correctly on MySQL PDO, including with prepared params in the WHERE clause. Both conflict (update) and no-conflict (insert) paths produce correct results.

## SPEC-10.2.247 Prepared LIMIT ? OFFSET ? parameter handling
**Status:** Partial
**Platforms:** SQLite-PDO (pass), MySQL-PDO (partial — PARAM_INT workaround), PostgreSQL-PDO (partial — ?-style passes, $N fails)
**Tests:** `Pdo/SqlitePreparedLimitOffsetParamsTest`, `Pdo/MysqlPreparedLimitOffsetParamsTest`, `Pdo/PostgresPreparedLimitOffsetParamsTest`

Prepared SELECT with `LIMIT ? OFFSET ?` pagination: SQLite works for all variants (basic, WHERE+LIMIT+OFFSET, shadow data, re-execute, bindValue). MySQL fails with `execute([3, 2])` because PDO sends string values `'3'` (SPEC-10.2.17) — workaround `bindValue($pos, $val, PDO::PARAM_INT)` works. PostgreSQL works with `?` placeholders but fails with `$N` — `LIMIT $1 OFFSET $2` returns all rows (ignores LIMIT), and `WHERE $1 LIMIT $2 OFFSET $3` returns empty (extends Issue #106).

## SPEC-10.2.248 INSERT...SELECT WHERE NOT EXISTS (anti-join conditional insert)
**Status:** Partial
**Platforms:** SQLite-PDO (partial), MySQL-PDO (pass), PostgreSQL-PDO (partial)
**Tests:** `Pdo/SqliteInsertWhereNotExistsTest`, `Pdo/MysqlInsertWhereNotExistsTest`, `Pdo/PostgresInsertWhereNotExistsTest`

INSERT...SELECT WHERE NOT EXISTS (anti-join) works on all platforms for basic cross-table exec, prepared with WHERE param, and shadow-inserted data visibility. Self-referencing INSERT NOT EXISTS on same table (`INSERT INTO t SELECT ... FROM t WHERE NOT EXISTS(SELECT 1 FROM t ...)`) fails: SQLite produces rows with incorrect computed columns (extends Issue #20), MySQL works correctly. PostgreSQL $1 param variant inserts only 1 row instead of 2 (extends Issue #106).

## SPEC-10.2.249 UPDATE SET with multiple independent scalar subqueries
**Status:** Partial (extends Issues #51, #61)
**Platforms:** MySQL-PDO (pass), SQLite-PDO (fails — syntax error), PostgreSQL-PDO (fails — grouping error)
**Tests:** `Pdo/SqliteUpdateMultiSubquerySetTest`, `Pdo/MysqlUpdateMultiSubquerySetTest`, `Pdo/PostgresUpdateMultiSubquerySetTest`

UPDATE with 2-3 correlated scalar subqueries from different tables in SET (`SET a = (SELECT ... FROM t2), b = (SELECT ... FROM t3)`) works on MySQL for all variants: exec, prepared with WHERE param, and shadow data. SQLite fails with "near FROM: syntax error" (extends Issue #51 — the CTE rewriter truncates at FROM keyword in correlated subqueries within SET). PostgreSQL fails with "must appear in GROUP BY" (extends Issue #61 — the rewriter incorrectly combines subqueries, adding table references that require GROUP BY).

## SPEC-10.2.250 DELETE WHERE IN (SELECT ... GROUP BY HAVING)
**Status:** Partial
**Platforms:** MySQL-PDO (pass), SQLite-PDO (fails — incomplete input), PostgreSQL-PDO (partial — ? pass, $1 fails)
**Tests:** `Pdo/SqliteDeleteWithAggregatedInSubqueryTest`, `Pdo/MysqlDeleteWithAggregatedInSubqueryTest`, `Pdo/PostgresDeleteWithAggregatedInSubqueryTest`

DELETE WHERE col IN (SELECT ... GROUP BY HAVING) and DELETE WHERE col NOT IN (SELECT ... GROUP BY HAVING SUM()): MySQL passes all variants (exec, prepared, shadow data). SQLite fails with "incomplete input" — the CTE rewriter truncates the SQL when DELETE has a subquery with GROUP BY HAVING (already known). PostgreSQL passes with `?` params but `$1` prepared variant doesn't filter (extends Issue #106).

## SPEC-10.2.251 Sequential DML with subquery references to shadow data
**Status:** Partial
**Platforms:** SQLite-PDO (partial), MySQL-PDO (partial), PostgreSQL-PDO (partial)
**Tests:** `Pdo/SqliteSequentialDmlSubqueryVisibilityTest`, `Pdo/MysqlSequentialDmlSubqueryVisibilityTest`, `Pdo/PostgresSequentialDmlSubqueryVisibilityTest`

Sequential DML chains where each operation references data from the previous: INSERT→UPDATE (with subquery referencing shadow-inserted row) passes on SQLite and MySQL. INSERT→UPDATE→DELETE chain passes on all platforms. Cross-table INSERT...SELECT JOIN chain fails: SQLite produces NULL columns (extends Issue #20), MySQL fails with "Unknown column" for JOINed alias (extends Issue #49), PostgreSQL fails with "operator does not exist: text = integer" (CTE casts produce type mismatches). UPDATE based on AVG of shadow-inserted log entries fails on SQLite/PostgreSQL (extends Issues #51/#61).

## SPEC-10.2.252 INSERT...SELECT with partial column list (explicit INSERT column list)
**Status:** Partial (extends Issue #20)
**Platforms:** MySQL-PDO (pass), SQLite-PDO (fails — NULL columns), PostgreSQL-PDO (fails — NULL columns)
**Tests:** `Pdo/SqliteInsertSelectPartialColumnListTest`, `Pdo/MysqlInsertSelectPartialColumnListTest`, `Pdo/PostgresInsertSelectPartialColumnListTest`

INSERT...SELECT with explicit column list omitting AUTOINCREMENT/SERIAL PK (`INSERT INTO t (col1, col2) SELECT ...`) produces rows with all-NULL values on SQLite and PostgreSQL. MySQL works correctly. This extends Issue #20 beyond computed columns — even simple column references produce NULLs when the INSERT column list doesn't include all table columns. INSERT...SELECT * (matching schemas, no column list) works on SQLite but is blocked on MySQL/PostgreSQL with "Cannot determine columns SQL statement" (extends Issue #40 to PostgreSQL).

## SPEC-10.2.253 Three-table JOIN in DML (DELETE and UPDATE with subquery)
**Status:** Partial
**Platforms:** SQLite-PDO (partial), MySQL-PDO (pending), PostgreSQL-PDO (pending), MySQLi (pending)
**Tests:** `Pdo/SqliteThreeTableJoinDmlTest`, `Pdo/MysqlThreeTableJoinDmlTest`, `Pdo/PostgresThreeTableJoinDmlTest`, `Mysqli/ThreeTableJoinDmlTest`

Three-table JOIN SELECT works correctly on all platforms. DELETE with 3-table JOIN subquery (no GROUP BY HAVING) works correctly on all platforms via exec. Prepared DELETE with category param works on MySQL (PDO and MySQLi) and SQLite, but **fails on PostgreSQL** — `$1` parameter in a subquery WHERE clause inside DELETE does not filter rows (extends Issue #106 to 3-table JOIN context). UPDATE with 3-table JOIN subquery containing GROUP BY HAVING fails on SQLite with "incomplete input" and on PostgreSQL with syntax error; **works on MySQL** (both PDO and MySQLi). Prepared UPDATE with HAVING param also works on MySQL but fails on SQLite/PostgreSQL.

## SPEC-10.2.254 DELETE with chained EXISTS and NOT EXISTS conditions
**Status:** Verified (SQLite), pending (MySQL, PostgreSQL)
**Platforms:** SQLite-PDO (pass), MySQL-PDO (pending), PostgreSQL-PDO (pending)
**Tests:** `Pdo/SqliteDeleteChainedExistsTest`, `Pdo/MysqlDeleteChainedExistsTest`, `Pdo/PostgresDeleteChainedExistsTest`

DELETE with multiple correlated EXISTS / NOT EXISTS conditions referencing different shadow tables works correctly on SQLite and MySQL (exec and prepared). On PostgreSQL, the exec variant works, but the **prepared variant with `$1` fails** — `NOT EXISTS (... status = $1)` does not correctly bind the parameter, causing incorrect rows to be deleted (extends Issue #106 to chained EXISTS pattern). Tested: EXISTS + NOT EXISTS combined, double NOT EXISTS, EXISTS on shadow-inserted data.

## SPEC-10.2.255 COALESCE in DML operations
**Status:** Verified (SQLite exec), partial (SQLite prepared)
**Platforms:** SQLite-PDO
**Tests:** `Pdo/SqliteCoalesceInDmlTest`

COALESCE in UPDATE SET (literal and multi-column) works correctly on SQLite. Prepared UPDATE with COALESCE and bound default parameter works. Nested COALESCE (`COALESCE(discount, price, ?)`) works correctly with prepared params. However, DELETE WHERE with COALESCE and prepared params is affected by SQLite + PDO type affinity: `execute([0, 50])` sends values as strings, causing string comparison instead of numeric, which affects all comparison operators with `?` params on SQLite. This is a PDO behavior, not a ZTD issue. Workaround: use `bindValue()` with `PDO::PARAM_INT`.

## SPEC-10.2.256 CASE expression in INSERT VALUES
**Status:** Verified (exec), partial (prepared with params)
**Platforms:** SQLite-PDO
**Tests:** `Pdo/SqliteCaseInInsertValuesTest`

CASE expressions in direct INSERT VALUES work correctly on SQLite for literal conditions (exec path): simple CASE, multi-branch CASE, multi-row INSERT with CASE per row, and nested CASE. Prepared INSERT with CASE and `?` params is affected by SQLite type affinity: `CASE WHEN ? > 75` where `?` is bound as string '60' evaluates to true because SQLite TEXT > INTEGER type ordering. This is raw PDO behavior (confirmed independently), not ZTD-specific.

## SPEC-10.2.257 String functions in DML operations
**Status:** Verified (SQLite)
**Platforms:** SQLite-PDO
**Tests:** `Pdo/SqliteStringFunctionDmlTest`

String functions in UPDATE SET work correctly on SQLite and MySQL: REPLACE(), UPPER(), LOWER(), TRIM(), concatenation (|| on SQLite, CONCAT on MySQL), SUBSTR()/LEFT() with prepared params. Prepared UPDATE with REPLACE and bound search/replacement params works on SQLite and MySQL. On **PostgreSQL**, non-prepared variants (UPPER, LOWER, POSITION, ||) work, but prepared variants with `$N` params fail: `REPLACE(col, $1, $2)` stores NULL values; `col || $1` doesn't apply concatenation. [Issue #108] DELETE WHERE with LENGTH()/LOCATE() and prepared params works on SQLite and MySQL; SQLite DELETE with `LENGTH(col) > ?` is affected by type affinity (SPEC-10.2.255).

## SPEC-10.2.258 NULL parameter binding in DML
**Status:** Verified (SQLite)
**Platforms:** SQLite-PDO
**Tests:** `Pdo/SqliteNullParamDmlTest`

NULL parameter handling works correctly on SQLite: prepared INSERT with `PDO::PARAM_NULL`, UPDATE SET column to NULL via prepared statement, mixed NULL/non-NULL params in multi-column UPDATE, DELETE WHERE IS NULL, SELECT IS NULL on shadow-updated data, and DELETE IS NOT NULL with additional param. All correctly handled by the CTE rewriter.

## SPEC-10.2.259 Self-referencing DELETE and UPDATE (same table subquery, no GROUP BY)
**Status:** Verified (SQLite)
**Platforms:** SQLite-PDO
**Tests:** `Pdo/SqliteSelfReferencingDeleteInTest`

Self-referencing DML without GROUP BY HAVING works correctly on SQLite: DELETE WHERE IN (SELECT parent_id FROM same_table), DELETE WHERE priority > (SELECT AVG(priority) FROM same_table), prepared self-ref DELETE with status param, UPDATE with self-referencing scalar subquery, and self-ref DELETE on shadow-inserted data. All variants produce correct results. This contrasts with GROUP BY HAVING variants which fail with "incomplete input".

## SPEC-10.2.260 DELETE with multiple subquery conditions (IN + NOT IN + scalar)
**Status:** Verified (SQLite)
**Platforms:** SQLite-PDO
**Tests:** `Pdo/SqliteDeleteWithMultipleSubqueryConditionsTest`

DELETE combining multiple subquery patterns in one WHERE clause works correctly on SQLite: IN + NOT IN subqueries, scalar subquery comparison (price > SELECT AVG), prepared DELETE with multiple subquery conditions and bound param, and EXISTS + scalar subquery combined. The CTE rewriter correctly handles diverse subquery types in a single statement.

## SPEC-10.2.261 UPDATE SET with multiple non-correlated aggregate subqueries
**Status:** Verified (SQLite)
**Platforms:** SQLite-PDO
**Tests:** `Pdo/SqliteUpdateWithSubqueryInMultipleSetsTest`

UPDATE with multiple non-correlated aggregate subqueries in SET works on SQLite when all subqueries reference the **same** table: `SET min=(SELECT MIN...), max=(SELECT MAX...), avg=(SELECT AVG...)`. Also works: subquery arithmetic in SET, subqueries reflecting shadow data, prepared variant with WHERE param, and CASE with COUNT subquery in SET. This contrasts with correlated subqueries (containing FROM...WHERE) which fail with "near FROM: syntax error" on SQLite. On PostgreSQL, even non-correlated subqueries fail with "must appear in GROUP BY" when subqueries reference **different** tables (confirmed via PostgresMultiCorrelatedSetUpdateTest::testNonCorrelatedMultiSubquerySet). The distinction is: same-table non-correlated subqueries work everywhere; cross-table non-correlated subqueries fail on PostgreSQL (extends Issue #61).

## SPEC-10.2.262 JSON column DML operations (UPDATE SET, DELETE WHERE)
**Status:** Verified (with known issues for upsert and prepared comparison)
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/JsonDmlTest`, `Pdo/MysqlJsonDmlTest`, `Pdo/PostgresJsonDmlTest`, `Pdo/SqliteJsonDmlTest`

JSON DML operations work correctly through the CTE shadow store: UPDATE SET using JSON_SET/json_set/jsonb_set to modify fields, DELETE with JSON_EXTRACT/json_extract/->> in WHERE to filter by JSON values, combined UPDATE SET+WHERE with JSON functions, UPDATE with JSON_REMOVE/json_remove to remove keys, INSERT with json_object() in VALUES, UPDATE with JSONB || merge operator (PostgreSQL), DELETE with @> containment operator (PostgreSQL). Prepared UPDATE with JSON function + params works. **Known issues:** (1) Prepared SELECT with JSON function followed by comparison operator (> ?) returns empty — Issue #113; (2) Upsert SET with JSON function call (JSON_SET/jsonb_set in ON DUPLICATE/ON CONFLICT) produces invalid JSON — Issue #114.

## SPEC-10.2.263 Composite (multi-column) primary key DML
**Status:** Verified
**Platforms:** SQLite-PDO, PostgreSQL-PDO
**Tests:** `Pdo/SqliteCompositePkDmlTest`, `Pdo/PostgresCompositePkDmlTest`

Tables with composite primary keys (e.g., `PRIMARY KEY (student_id, course_id)`) work correctly through the ZTD shadow store: UPDATE by full composite PK, UPDATE by partial PK (one column — affects multiple rows), DELETE by full composite PK, DELETE by partial PK, prepared UPDATE with composite PK params, and SELECT with three-table JOIN on composite PK table. The shadow store correctly tracks and applies DML for multi-column PKs. Verified on SQLite and PostgreSQL.

## SPEC-10.2.264 INSERT with SQL function calls in VALUES
**Status:** Verified
**Platforms:** SQLite-PDO
**Tests:** `Pdo/SqliteInsertFunctionValuesTest`

INSERT with function calls as value expressions works correctly through the CTE shadow store on SQLite: CURRENT_TIMESTAMP, datetime('now'), arithmetic expressions (2+3*4), COALESCE(NULL, value), CASE WHEN expression, UPPER/LOWER string functions, nested functions (LENGTH(REPLACE(...))), subquery in VALUES ((SELECT MAX(...))), and prepared INSERT mixing function calls with ? params. All patterns insert correctly and the values are retrievable via shadow SELECT.

## SPEC-10.2.265 Multi-row INSERT and multi-row upsert
**Status:** Verified (with known issues)
**Platforms:** MySQLi, MySQL-PDO, SQLite-PDO
**Tests:** `Mysqli/MultiRowUpsertTest`, `Pdo/MysqlMultiRowUpsertTest`, `Pdo/SqliteMultiRowUpsertTest`

Multi-row INSERT (no conflict) works correctly on all platforms. Multi-row INSERT ON DUPLICATE KEY UPDATE with VALUES() references (direct replacement) works correctly on MySQL. Prepared multi-row INSERT ON DUPLICATE KEY UPDATE works correctly on MySQL. Multi-row INSERT IGNORE works correctly on MySQL. **Known issues:** (1) Multi-row upsert with self-referencing accumulate expression (table.qty + VALUES(qty)) evaluates to 0 — Issue #112; (2) SQLite multi-row ON CONFLICT DO NOTHING inserts duplicate PK rows — extends Issue #41; (3) SQLite prepared multi-row ON CONFLICT DO UPDATE inserts duplicates — extends Issue #41.

## SPEC-10.2.266 Row value constructor in DML WHERE
**Status:** Verified (with known issue on PostgreSQL)
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/RowValueConstructorDmlTest`, `Pdo/MysqlRowValueConstructorDmlTest`, `Pdo/PostgresRowValueConstructorDmlTest`, `Pdo/SqliteRowValueConstructorDmlTest`

Row value constructors (tuple comparisons) `WHERE (col1, col2) IN (SELECT ...)` in DELETE and UPDATE through ZTD shadow store. DELETE with `(a,b) IN (SELECT ...)` works on all platforms. UPDATE with `(a,b) IN (SELECT ...)` works on MySQL (MySQLi, PDO) and SQLite. Prepared DELETE with `(a,b) = (?, ?)` works on all platforms. DELETE with `(a,b) NOT IN (SELECT ...)` works on MySQL. **Known issues:** (1) UPDATE WHERE `(a,b) IN (SELECT ...)` produces syntax error on PostgreSQL — Issue #116; (2) Prepared DELETE WHERE `(a,b) = ($1, $2)` with `$N` params fails on PostgreSQL (extends Issue #106).

## SPEC-10.2.267 Window function in DML subquery
**Status:** Verified (with known issues)
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/WindowFunctionDmlTest`, `Pdo/MysqlWindowFunctionDmlTest`, `Pdo/PostgresWindowFunctionDmlTest`, `Pdo/SqliteWindowFunctionDmlTest`

Window functions (ROW_NUMBER, DENSE_RANK, RANK) in subqueries within DML. INSERT...SELECT with window functions works on all platforms. DELETE with ROW_NUMBER subquery works on MySQL and SQLite. **Known issues:** (1) DELETE with ROW_NUMBER subquery produces syntax error on PostgreSQL — Issue #115; (2) UPDATE JOIN with window function subquery treated as identifier on MySQL — extends Issue #104/Issue #115; (3) UPDATE with correlated subquery containing window function produces syntax error on SQLite — Issue #115.

## SPEC-10.2.268 DISTINCT in DML subquery and INSERT...SELECT with HAVING
**Status:** Verified (with known issues)
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/UpdateDistinctSubqueryTest`, `Pdo/MysqlUpdateDistinctSubqueryTest`, `Pdo/PostgresUpdateDistinctSubqueryTest`, `Pdo/SqliteUpdateDistinctSubqueryTest`

COUNT(DISTINCT) and AVG(DISTINCT) in correlated UPDATE SET subqueries work on MySQL (MySQLi, PDO). DELETE WHERE id IN (SELECT DISTINCT ...) works on all platforms. INSERT...SELECT with GROUP BY HAVING works on MySQL. **Known issues:** (1) UPDATE SET = (SELECT COUNT(DISTINCT ...)) produces syntax error on SQLite and GROUP BY error on PostgreSQL — extends Issue #10/Issue #115; (2) INSERT...SELECT with GROUP BY HAVING produces "no such column" error on SQLite and PostgreSQL — Issue #117; (3) Prepared INSERT...SELECT HAVING returns 0 rows on SQLite (extends Issue #22) and PostgreSQL with `$N` (extends Issue #106).

## SPEC-10.2.269 DELETE...USING (PostgreSQL)
**Status:** Verified (with known $N param issue)
**Platforms:** PostgreSQL-PDO
**Tests:** `Pdo/PostgresDeleteUsingTest`

PostgreSQL DELETE...USING (multi-table DELETE syntax) works through ZTD shadow store: simple USING join, USING with additional WHERE condition, USING on shadow-inserted data, and prepared DELETE USING with `?` placeholders all work correctly. **Known issue:** Prepared DELETE USING with `$N` parameters does not apply the delete (extends Issue #106).

## SPEC-10.2.270 SELECT DISTINCT ON in DML context (PostgreSQL)
**Status:** Verified (with known issues in DML subqueries)
**Platforms:** PostgreSQL-PDO
**Tests:** `Pdo/PostgresDistinctOnDmlTest`

PostgreSQL `DISTINCT ON` (get first row per group) works correctly through the ZTD shadow store for plain SELECT and INSERT...SELECT: SELECT DISTINCT ON (sensor_id) ... ORDER BY sensor_id, reading_time DESC returns correct latest-per-group results on shadow data. INSERT...SELECT DISTINCT ON materializes correctly. **Known issues:** (1) DELETE WHERE NOT IN (SELECT DISTINCT ON ...) produces syntax error — CTE rewriter truncates ORDER BY [Issue #132]; (2) UPDATE WHERE id IN (SELECT DISTINCT ON ...) produces syntax error [Issue #132]; (3) Prepared DISTINCT ON with $N param returns empty (extends Issue #106).

## SPEC-10.2.271 SELECT FOR UPDATE / FOR SHARE locking clauses
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/SelectForUpdateTest`, `Pdo/MysqlSelectForUpdateTest`, `Pdo/PostgresSelectForUpdateTest`, `Pdo/SqliteSelectForUpdateTest`

Locking clauses (FOR UPDATE, FOR SHARE) work correctly through the ZTD shadow store on MySQL and PostgreSQL: rows inserted via ZTD are visible through SELECT FOR UPDATE, SELECT FOR SHARE, and within transactions. PostgreSQL-specific variants FOR NO KEY UPDATE and FOR KEY SHARE also work. The CTE rewriter preserves the locking clause when rewriting SELECT queries. Full pessimistic-locking workflow (SELECT FOR UPDATE → UPDATE → COMMIT) works correctly. On SQLite (which does not support FOR UPDATE), the clause is silently accepted and returns correct results — SQLite ignores the locking hint.

## SPEC-10.2.272 INTERSECT/EXCEPT in DML subqueries
**Status:** Verified (with known issues on MySQL and PostgreSQL UPDATE)
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/IntersectExceptDmlTest`, `Pdo/MysqlIntersectExceptDmlTest`, `Pdo/PostgresIntersectExceptDmlTest`, `Pdo/SqliteIntersectExceptDmlTest`

Set operations (INTERSECT, EXCEPT) in DML context: DELETE WHERE IN (... INTERSECT ...) and UPDATE WHERE IN (... EXCEPT ...) work on MySQL and SQLite. INSERT...SELECT INTERSECT/EXCEPT works on PostgreSQL and SQLite. **Known issues:** (1) MySQL INSERT...SELECT INTERSECT/EXCEPT rejected as "multi-statement SQL" — extends Issue #14 [extends Issue #14]; (2) PostgreSQL UPDATE WHERE IN (EXCEPT subquery) produces syntax error [Issue #134]; (3) Physical isolation verified — set operation DML does not affect underlying tables.

## SPEC-10.2.273 Aggregate FILTER clause in DML
**Status:** Verified (with known issues)
**Platforms:** PostgreSQL-PDO, SQLite-PDO
**Tests:** `Pdo/PostgresAggregateFilterDmlTest`, `Pdo/SqliteAggregateFilterDmlTest`

SQL standard FILTER clause on aggregates: basic SELECT with FILTER works on both PostgreSQL and SQLite — COUNT(*) FILTER (WHERE ...) and multiple FILTER aggregates in one query return correct results on shadow data. DELETE WHERE with FILTER subquery works. **Known issues:** (1) INSERT...SELECT with FILTER loses column alias [Issue #131]; (2) UPDATE SET with FILTER subquery produces syntax error [Issue #131]; (3) Prepared SELECT with FILTER and $N returns empty on PostgreSQL (extends Issue #106).

## SPEC-10.2.274 Conditional upsert (ON CONFLICT DO UPDATE WHERE)
**Status:** Verified (with known issues)
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/ConditionalUpsertTest`, `Pdo/MysqlConditionalUpsertTest`, `Pdo/PostgresConditionalUpsertTest`, `Pdo/SqliteConditionalUpsertTest`

Conditional upsert behavior: basic ON DUPLICATE KEY UPDATE (MySQL) and ON CONFLICT DO UPDATE (PostgreSQL/SQLite) work for unconditional updates. The WHERE clause on DO UPDATE is ignored — updates happen unconditionally [extends Issue #30]. MySQL IF()/VALUES() conditional expressions in ON DUPLICATE KEY UPDATE evaluate to 0 instead of the conditional value [Issue #133]. Prepared upsert inserts duplicate PK rows on MySQL-PDO (extends Issue #17) and SQLite (extends Issue #41).

## SPEC-10.2.275 UPDATE/DELETE with ORDER BY LIMIT (MySQL)
**Status:** Known Issue
**Platforms:** MySQLi, MySQL-PDO
**Tests:** `Mysqli/UpdateOrderByLimitTest`, `Pdo/MysqlUpdateOrderByLimitTest`

MySQL-specific UPDATE/DELETE with ORDER BY and LIMIT clauses are completely non-functional through the shadow store. UPDATE ... ORDER BY ... LIMIT N does not update any rows; DELETE ... ORDER BY ... LIMIT N does not delete any rows. The CTE rewriter does not preserve ORDER BY and LIMIT on DML statements. This is a common MySQL pattern for queue processing, batch operations, and partial updates [Issue #130].

## SPEC-10.2.276 Writable CTEs (DML inside WITH clause)
**Status:** Known Issue
**Platforms:** PostgreSQL-PDO, SQLite-PDO
**Tests:** `Pdo/PostgresMultiCteDmlTest`, `Pdo/SqliteMultiCteDmlTest`

PostgreSQL writable CTEs (WITH ... AS (DELETE/UPDATE RETURNING *) ...) fail: CTE name is not recognized in outer query ("relation does not exist"). Multi-writable CTEs return empty results. SQLite correctly rejects writable CTEs (not supported by the engine) with clear error messages. Data integrity is preserved after failed writable CTE attempts on SQLite [extends Issue #28].

## SPEC-10.2.277 INSERT...SELECT with self-join
**Status:** Known Issue
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/InsertSelectSelfJoinTest`, `Pdo/MysqlInsertSelectSelfJoinTest`, `Pdo/PostgresInsertSelectSelfJoinTest`, `Pdo/SqliteInsertSelectSelfJoinTest`

INSERT...SELECT where the SELECT involves a self-join (same table with different aliases) fails on all platforms [Issue #135]. MySQL throws "Unknown column 'b.id' in 'field list'" — the CTE rewriter cannot resolve the second alias. PostgreSQL and SQLite silently insert 0 rows. Cross-table INSERT...SELECT (no self-join) works. SELECT with self-join (no INSERT) works on all platforms. Related to Issue #49 (INSERT...SELECT multi-table JOIN) but with distinct 0-row behavior on PostgreSQL/SQLite vs NULLs in #49.

## SPEC-10.2.278 UPDATE with string concatenation (self-referencing)
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO (exec only), SQLite-PDO
**Tests:** `Mysqli/UpdateConcatSelfRefTest`, `Pdo/MysqlUpdateConcatSelfRefTest`, `Pdo/PostgresUpdateConcatSelfRefTest`, `Pdo/SqliteUpdateConcatSelfRefTest`

UPDATE with string concatenation referencing the column being updated works correctly: append (`path = path || '/archived'` / `CONCAT(path, '/archived')`), prepend, bulk update wrapping (`label = '[' || label || ']'`), prepared with `?` params (MySQL, SQLite). Multi-column self-referencing concat (`SET label = label || '_old', path = path || '/' || label`) correctly uses pre-update values for all SET expressions (SQL standard). **Known issue:** Prepared UPDATE concat with `$N` params on PostgreSQL returns 0 rows (extends Issue #106).

## SPEC-10.2.279 Chained DML mutation consistency
**Status:** Verified
**Platforms:** SQLite-PDO
**Tests:** `Pdo/SqliteChainedDmlConsistencyTest`

Sequential DML operations maintain correct shadow store state: INSERT→UPDATE on shadow-inserted rows, INSERT→UPDATE→DELETE chains, physical seed→shadow UPDATE→shadow DELETE, double UPDATE on same row (accumulates correctly), DELETE→re-INSERT with same PK (returns new values), and aggregate queries after mutation chains all produce correct results. The shadow store correctly tracks and applies arbitrary sequences of mutations including quantity arithmetic (`quantity = quantity + 50`, `quantity = quantity * 2`).

## SPEC-10.2.280 UPDATE with mathematical expressions and subqueries
**Status:** Verified (with known issues for correlated subqueries on SQLite)
**Platforms:** SQLite-PDO
**Tests:** `Pdo/SqliteUpdateMathExpressionTest`

UPDATE with math expressions referencing columns works: percentage calculation (`balance * 1.05`), cross-column expressions (`credit_limit - balance`), CASE with math branches, prepared UPDATE with math and `?` params. **Known issues:** UPDATE SET with correlated subquery SUM in expression (`balance = balance + COALESCE((SELECT SUM(amount) FROM bonuses WHERE ...), 0.0)`) produces "near FROM: syntax error" on SQLite — extends Issue #51. Same applies to prepared variant.

## SPEC-10.2.281 Upsert with scalar subquery in SET clause
**Status:** Known Issue (extends #105)
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/UpsertScalarSubquerySetTest`, `Pdo/MysqlUpsertScalarSubquerySetTest`, `Pdo/PostgresUpsertScalarSubquerySetTest`, `Pdo/SqliteUpsertScalarSubquerySetTest`

Upsert with scalar subquery in SET using EXCLUDED/VALUES reference: `ON CONFLICT DO UPDATE SET discount = (SELECT discount_pct FROM categories WHERE id = EXCLUDED.category_id)` evaluates to 0 on SQLite and MySQL instead of the subquery result [extends Issue #105]. On PostgreSQL, the CTE rewriter wraps the subquery text as a CAST literal string: `CAST('(SELECT discount_pct ...' AS numeric)` which produces "invalid input syntax for type numeric". Prepared variants produce param count mismatch on MySQL/SQLite or column index out of range on SQLite. COALESCE wrapping the subquery produces 0.0 on all platforms (subquery not evaluated, COALESCE falls through to default). No-conflict fresh inserts work correctly across all platforms.

## SPEC-10.2.282 PostgreSQL OVERLAPS operator in DML
**Status:** Verified (non-prepared only)
**Platforms:** PostgreSQL-PDO
**Tests:** `Pdo/PostgresOverlapsAndRangeTest`

The SQL standard OVERLAPS operator for temporal range comparison works correctly through the CTE shadow store in non-prepared contexts: SELECT with OVERLAPS in WHERE, UPDATE with OVERLAPS condition, DELETE with OVERLAPS condition all return correct results on shadow data. INSERT...SELECT with OVERLAPS fails due to self-join requirement (Issue #135, not OVERLAPS itself). **Known issue:** Prepared SELECT/UPDATE with OVERLAPS and `$N` params returns 0 rows (extends Issue #106).

## SPEC-10.2.283 GLOB operator in DML (SQLite)
**Status:** Verified
**Platforms:** SQLite-PDO
**Tests:** `Pdo/SqliteRegexOperatorDmlTest`

SQLite's GLOB operator works correctly in UPDATE and DELETE WHERE clauses through the CTE shadow store. All patterns tested: `UPDATE ... WHERE code GLOB 'PRD-*'`, `DELETE ... WHERE code GLOB 'SVC-*'`, prepared UPDATE with GLOB and bound parameters, NOT GLOB, and SELECT GLOB on shadow-inserted data. GLOB is the only regex-family operator that works in DML contexts — MySQL REGEXP/RLIKE and PostgreSQL ~/~*/SIMILAR TO do NOT work in UPDATE/DELETE [Issue #136].

## SPEC-10.2.284 CASE expression in ORDER BY
**Status:** Verified
**Platforms:** SQLite-PDO
**Tests:** `Pdo/SqliteCaseOrderByDmlTest`

CASE expressions in ORDER BY clauses work correctly through the CTE shadow store on SQLite. Tested patterns: `ORDER BY CASE col WHEN 'critical' THEN 1 ... END`, prepared SELECT with CASE ORDER BY, INSERT...SELECT with CASE ORDER BY LIMIT, UPDATE with CASE in both SET and WHERE clauses. Combined CASE ORDER BY with NULLS LAST also works.

## SPEC-10.2.285 EXISTS in SELECT list
**Status:** Verified (SQLite only)
**Platforms:** SQLite-PDO (works); MySQL-PDO, PostgreSQL-PDO (broken, [Issue #137])
**Tests:** `Pdo/SqliteExistsInSelectListTest`, `Pdo/MysqlExistsInSelectListTest`, `Pdo/PostgresExistsInSelectListTest`

EXISTS and NOT EXISTS as boolean expressions in the SELECT list work correctly on SQLite through the CTE shadow store. The correlated EXISTS subquery correctly reads from shadow data: after shadow INSERT, EXISTS reflects the new row; after shadow DELETE, EXISTS correctly returns false. Multiple EXISTS in the same SELECT list and prepared statements with EXISTS also work.

MySQL and PostgreSQL are affected by [Issue #137] — EXISTS in SELECT list always returns 0 (MySQL) or causes type mismatch error (PostgreSQL).

## SPEC-10.2.286 NULLS FIRST / NULLS LAST in ORDER BY
**Status:** Verified
**Platforms:** SQLite-PDO, PostgreSQL-PDO (non-$N prepared)
**Tests:** `Pdo/SqliteNullsFirstLastDmlTest`, `Pdo/PostgresNullsFirstLastDmlTest`

NULLS FIRST and NULLS LAST in ORDER BY work correctly through the CTE shadow store on SQLite and PostgreSQL. Tested patterns: SELECT with NULLS FIRST, SELECT with NULLS LAST, INSERT...SELECT with NULLS LAST LIMIT (window function + ORDER BY NULLS LAST), prepared SELECT with NULLS FIRST (? params on both platforms). PostgreSQL FETCH FIRST N ROWS ONLY with NULLS LAST also works.

**Known issue:** Prepared SELECT with `$N` params and NULLS FIRST returns 0 rows on PostgreSQL (extends Issue #85). MySQL < 8.0.26 does not support NULLS FIRST/LAST syntax.

## SPEC-10.2.287 Quoted identifiers in DML
**Status:** Verified (SQLite); Known Issue (MySQL [Issue #139])
**Platforms:** SQLite-PDO (works), MySQLi (broken), MySQL-PDO (broken)
**Tests:** `Pdo/SqliteQuotedIdentifierDmlTest`, `Pdo/MysqlQuotedIdentifierDmlTest`, `Mysqli/QuotedIdentifierDmlTest`

SQLite double-quoted mixed-case identifiers (`"UserId"`, `"FirstName"`, `"sl_UserProfiles"`) work correctly in all DML contexts: INSERT, SELECT, UPDATE, DELETE, prepared statements, JOIN, and UPDATE SET with concatenation referencing quoted column names.

MySQL backtick-quoted identifiers (`\`UserId\``, `\`my_UserProfiles\``) work for INSERT and SELECT but fail for UPDATE and DELETE [Issue #139].

## SPEC-10.2.288 GROUPING SETS / ROLLUP / CUBE
**Status:** Verified
**Platforms:** PostgreSQL-PDO, MySQL-PDO (WITH ROLLUP only), SQLite-PDO (simulated via UNION ALL)
**Tests:** `Pdo/PostgresGroupingSetsTest`, `Pdo/MysqlGroupingSetsTest`, `Pdo/SqliteGroupingSetsCubeRollupTest`, `Mysqli/GroupingSetsTest`

Advanced GROUP BY extensions work correctly through the CTE shadow store:
- **PostgreSQL**: GROUPING SETS, ROLLUP, CUBE, and GROUPING() function all produce correct subtotals and grand totals from shadow data.
- **MySQL**: WITH ROLLUP syntax and GROUPING() function work correctly.
- **SQLite**: Simulated ROLLUP via UNION ALL with GROUP BY works correctly.

## SPEC-10.2.289 Numeric precision in shadow store
**Status:** Verified (partial)
**Platforms:** MySQLi
**Tests:** `Mysqli/NumericPrecisionTest`

DECIMAL(20,10) values are preserved correctly in the shadow store: high-precision values (3.1415926535), very small values (0.0000000001), and negative decimals. DOUBLE values with scientific-scale magnitudes (1e15, 1e-10) are approximately preserved.

**Known issue:** DECIMAL arithmetic in UPDATE SET (`dec_val = dec_val * 1.075`) does not compute the multiplication [Issue #140]. BIGINT boundary values are returned with correct values but may differ in PHP type representation (int vs string).

## SPEC-10.2.290 INSERT...SELECT with LIMIT/OFFSET
**Status:** Verified
**Platforms:** SQLite-PDO
**Tests:** `Pdo/SqliteInsertSelectLimitTest`

INSERT...SELECT with LIMIT, LIMIT OFFSET, ORDER BY DESC LIMIT (top-N), prepared INSERT...SELECT LIMIT, and INSERT...SELECT LIMIT on shadow-inserted data all work correctly through the CTE shadow store on SQLite. The LIMIT/OFFSET clauses are preserved in the CTE-rewritten INSERT...SELECT.

## SPEC-10.2.291 CASE expression in UPDATE SET
**Status:** Verified (SQLite); Known Issue (MySQL, PostgreSQL [Issue #142])
**Platforms:** SQLite-PDO (works), MySQL-PDO (partial), PostgreSQL-PDO (partial), MySQLi (partial)
**Tests:** `Pdo/SqliteCaseInUpdateSetTest`, `Pdo/MysqlCaseInUpdateSetTest`, `Pdo/PostgresCaseInUpdateSetTest`, `Mysqli/CaseInUpdateSetTest`

CASE expressions in UPDATE SET clause: Simple CASE without WHERE works on all platforms. Prepared CASE in SET works on all platforms. **Known issue:** CASE in SET + WHERE clause does not evaluate the CASE (columns keep original values) on MySQL, PostgreSQL, and MySQLi [Issue #142]. Multiple CASE with arithmetic branches (`balance * 1.05`) also does not compute. Nested CASE, searched CASE, and CASE + further mutations all work on SQLite.

## SPEC-10.2.292 COALESCE in DML operations
**Status:** Verified (MySQL, SQLite); Known Issue (PostgreSQL [Issue #144])
**Platforms:** MySQL-PDO (works), SQLite-PDO (works), PostgreSQL-PDO (partial)
**Tests:** `Pdo/MysqlCoalesceInDmlTest`, `Pdo/SqliteCoalesceInDmlTest`, `Pdo/PostgresCoalesceInDmlTest`

COALESCE in UPDATE SET and DELETE WHERE: All patterns work on MySQL and SQLite — single-column COALESCE default, prepared COALESCE with params, nested COALESCE, multi-column COALESCE, DELETE WHERE COALESCE, and SELECT COALESCE on shadow data.

PostgreSQL: Simple single-column `UPDATE SET price = COALESCE(price, 0.00)` works. **Known issues:** Multi-column COALESCE UPDATE does not evaluate (NULL stays NULL), nested COALESCE returns wrong argument, DELETE WHERE COALESCE has no effect and corrupts id column, prepared variants fail with type mismatch [Issue #144].

## SPEC-10.2.293 Anti-join query patterns
**Status:** Verified (SQLite); Known Issue (MySQL [Issue #143], PostgreSQL [Issue #143])
**Platforms:** SQLite-PDO (works), MySQL-PDO (broken), PostgreSQL-PDO (broken)
**Tests:** `Pdo/SqliteAntiJoinNullPatternTest`, `Pdo/MysqlAntiJoinNullPatternTest`, `Pdo/PostgresAntiJoinNullPatternTest`

Three equivalent anti-join patterns (LEFT JOIN IS NULL, NOT EXISTS, NOT IN) all return correct and identical results on SQLite — find unmatched rows, with additional filters, with prepared params, and DELETE using anti-join.

**Known issues:** On MySQL, LEFT JOIN IS NULL and NOT EXISTS return ALL rows (condition ignored), NOT IN returns EMPTY set. On PostgreSQL, all patterns fail with type mismatch (text = integer). DELETE with anti-join also broken on both platforms [Issue #143].
