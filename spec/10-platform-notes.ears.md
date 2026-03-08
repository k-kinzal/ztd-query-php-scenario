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
