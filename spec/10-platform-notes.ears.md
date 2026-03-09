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
