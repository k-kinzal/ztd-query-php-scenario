# 11. Known Issues and Cross-Platform Inconsistencies

## SPEC-11.UNKNOWN-UPDATE `[Issue #39]` Unknown schema UPDATE (Passthrough mode)
**Status:** Known Issue
**Platforms:** MySQL-PDO (fromPdo), PostgreSQL-PDO, SQLite-PDO
**Related specs:** [SPEC-7.1](07-unknown-schema.ears.md), [SPEC-7.2](07-unknown-schema.ears.md)
**Tests:** `Pdo/MysqlUnknownSchemaTest`, `Pdo/PostgresUnknownSchemaTest`, `Pdo/SqliteUnknownSchemaTest`

On MySQL via `ZtdPdo::fromPdo()`, PostgreSQL, and SQLite, UPDATE on unreflected tables throws `RuntimeException` ("UPDATE simulation requires primary keys") instead of passing through. The `unknownSchemaBehavior` setting does not take effect for UPDATE operations on these platforms. On MySQL via `fromPdo()`, behavior depends on operation history — if no prior shadow operations touched the table, Passthrough works; after a shadow INSERT, it fails.

## SPEC-11.UNKNOWN-EXCEPTION `[Issue #39]` Unknown schema UPDATE (Exception mode)
**Status:** Known Issue
**Platforms:** PostgreSQL-PDO, SQLite-PDO
**Related specs:** [SPEC-7.2](07-unknown-schema.ears.md)

On PostgreSQL and SQLite, UPDATE throws `RuntimeException` ("UPDATE simulation requires primary keys") instead of `ZtdPdoException` ("Unknown table"). Exception type and message differ from MySQL.

## SPEC-11.UNKNOWN-DELETE `[Issue #39]` Unknown schema DELETE inconsistency
**Status:** Known Issue
**Platforms:** PostgreSQL-PDO, SQLite-PDO
**Related specs:** [SPEC-7.1](07-unknown-schema.ears.md)

On PostgreSQL, DELETE in Exception mode throws `RuntimeException` rather than `ZtdPdoException`. On SQLite, same behavior.

## SPEC-11.SQLITE-ON-CONFLICT `[Issue #41]` INSERT ... ON CONFLICT DO NOTHING (SQLite)
**Status:** Known Issue
**Platforms:** SQLite-PDO
**Related specs:** [SPEC-4.2a](04-write-operations.ears.md), [SPEC-4.2e](04-write-operations.ears.md)

On SQLite, `INSERT ... ON CONFLICT DO NOTHING` inserts both rows (shadow store does not enforce PK constraints). Use `INSERT OR IGNORE` instead. PostgreSQL handles this correctly.

**Extended scope (2026-03-10):** The same issue affects multi-row INSERT with ON CONFLICT DO NOTHING: `INSERT INTO t VALUES (...), (...) ON CONFLICT(id) DO NOTHING` inserts all rows including duplicates. Prepared multi-row INSERT with ON CONFLICT also inserts duplicate PK rows. Tests: `Pdo/SqliteMultiRowUpsertTest`.

## SPEC-11.MYSQL-INSERT-SELECT-STAR `[Issue #40]` INSERT ... SELECT * (MySQL)
**Status:** Known Issue
**Platforms:** MySQLi, MySQL-PDO
**Related specs:** [SPEC-4.1a](04-write-operations.ears.md)

On MySQL, `INSERT INTO t SELECT * FROM s` throws `RuntimeException` because the InsertTransformer counts `SELECT *` as 1 column. Workaround: use explicit column lists. SQLite and PostgreSQL work correctly.

## SPEC-11.PG-CTE `[Issue #4]` User-written CTEs (PostgreSQL)
**Status:** Known Issue (By-Design)
**Platforms:** PostgreSQL-PDO
**Related specs:** [SPEC-3.3](03-read-operations.ears.md)
**Tests:** `Pdo/PostgresUserCteTest`

On PostgreSQL, table references inside user CTEs are NOT rewritten — the inner CTE reads from the physical table, returning 0 rows. MySQL and SQLite work correctly.

## SPEC-11.PG-SCHEMA-QUALIFIED `[Issue #24]` Schema-qualified table names (PostgreSQL)
**Status:** Known Issue
**Platforms:** PostgreSQL-PDO
**Tests:** `Pdo/PostgresSchemaQualifiedTest`

INSERT/UPDATE/DELETE with `public.tablename` work. SELECT returns empty (CTE rewriter doesn't recognize schema-qualified names). Workaround: use unqualified table names in SELECT.

## SPEC-11.EXECUTE-QUERY-UPSERT `[Issue #42]` execute_query vs prepare+bind_param for UPSERT/REPLACE (MySQLi)
**Status:** Known Issue
**Platforms:** MySQLi
**Related specs:** [SPEC-4.2a](04-write-operations.ears.md), [SPEC-4.2b](04-write-operations.ears.md)
**Tests:** `Mysqli/ExecuteQueryWriteOpsTest`

MySQLi `execute_query()` does NOT update/replace existing rows for UPSERT and REPLACE. `prepare()` + `bind_param()` + `execute()` works correctly. The array-param `execute()` path differs from the `bind_param()` path.

## SPEC-11.MYSQL-MULTI-TABLE-DELETE `[Issue #26]` Multi-target DELETE (MySQL)
**Status:** Known Issue
**Platforms:** MySQLi, MySQL-PDO
**Related specs:** [SPEC-4.2d](04-write-operations.ears.md)
**Tests:** `Mysqli/MultiTableDeleteTest`, `Pdo/MysqlMultiTableDeleteTest`

Multi-target DELETE (`DELETE t1, t2 FROM ...`) only deletes from the first table. Single-target DELETE with JOIN works correctly.

## SPEC-11.SQLITE-ALTER-RENAME `[Issue #27]` ALTER TABLE RENAME TO (SQLite)
**Status:** Known Issue
**Platforms:** SQLite-PDO
**Related specs:** [SPEC-5.1a](05-ddl-operations.ears.md)
**Tests:** `Pdo/SqliteAlterTableRenameTest`

ALTER TABLE RENAME TO drops shadow data without creating new entry. ZTD-inserted data is permanently lost.

## SPEC-11.PG-TRUNCATE-MULTI `[Issue #29]` PostgreSQL multi-table TRUNCATE
**Status:** Known Issue
**Platforms:** PostgreSQL-PDO
**Related specs:** [SPEC-5.3](05-ddl-operations.ears.md)
**Tests:** `Pdo/PostgresMultiTableTruncateTest`

`TRUNCATE table1, table2` only truncates the first table. Workaround: separate TRUNCATE per table.

## SPEC-11.PG-RETURNING `[Issue #32]` PostgreSQL RETURNING clause
**Status:** Known Issue
**Platforms:** PostgreSQL-PDO
**Tests:** `Pdo/PostgresReturningClauseTest`

INSERT/UPDATE/DELETE RETURNING clause is not supported. CTE rewriter does not preserve RETURNING. Workaround: separate SELECT after DML.

## SPEC-11.PDO-PREPARED-INSERT `[Issue #23]` PDO prepared INSERT cannot be updated/deleted
**Status:** Known Issue
**Platforms:** MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Related specs:** [SPEC-4.1](04-write-operations.ears.md)
**Tests:** `Pdo/MysqlPreparedInsertUpdateBugTest`, `Pdo/PostgresPreparedInsertUpdateBugTest`, `Pdo/SqlitePreparedInsertUpdateBugTest`

On PDO, rows inserted via `prepare()` + `execute()` cannot be subsequently updated or deleted. MySQLi is NOT affected. Use `exec()` for INSERT when subsequent UPDATE/DELETE is needed.

## SPEC-11.PDO-UPSERT `[Issue #17]` Upsert via PDO prepared statements
**Status:** Known Issue
**Platforms:** MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Related specs:** [SPEC-4.2a](04-write-operations.ears.md), [SPEC-4.2b](04-write-operations.ears.md)
**Tests:** `Pdo/MysqlPreparedUpsertTest`, `Pdo/PostgresPreparedUpsertTest`, `Pdo/SqlitePreparedUpsertTest`

PDO prepared REPLACE INTO and INSERT ... ON CONFLICT DO UPDATE do NOT update existing rows. Use `exec()` instead.

## SPEC-11.SQLITE-HAVING-PARAMS `[Issue #22]` HAVING with prepared params (SQLite, PostgreSQL)
**Status:** Known Issue
**Platforms:** SQLite-PDO (confirmed), PostgreSQL-PDO (confirmed for complex multi-table queries)
**Tests:** `Pdo/SqlitePreparedAggregateParamsTest`, `Pdo/SqliteSubscriptionRenewalTest`, `Pdo/SqliteStudentGradeReportTest`, `Pdo/PostgresSubscriptionRenewalTest`, `Pdo/PostgresStudentGradeReportTest`, `Pdo/SqliteInsertSelectGroupByWithParamsTest`

On SQLite, HAVING with bound parameters returns empty results. HAVING with literal values works. MySQL works correctly. PostgreSQL also returns empty for complex multi-table HAVING with `$N` params (e.g., `HAVING SUM(amount) >= $2` with JOINs), extending this issue beyond SQLite-only. Also affects INSERT...SELECT with GROUP BY HAVING and prepared params — returns 0 rows on SQLite.

## SPEC-11.MYSQL-BACKSLASH `[Issue #5]` Backslash corruption in MySQL shadow store
**Status:** Known Issue
**Platforms:** MySQLi, MySQL-PDO
**Tests:** `Mysqli/BackslashCorruptionTest`, `Pdo/MysqlBackslashCorruptionTest`

Backslash characters are corrupted in shadow store: `\t` → tab, `\n` → newline, etc. CTE rewriter embeds values without escaping backslashes. SQLite and PostgreSQL not affected.

## SPEC-11.PG-BOOLEAN-FALSE `[Issue #6]` PostgreSQL BOOLEAN false casting
**Status:** Known Issue
**Platforms:** PostgreSQL-PDO
**Tests:** `Pdo/PostgresTypeEdgeCaseTest`

Inserting `false` into BOOLEAN column via prepared statement succeeds, but SELECT fails (CTE generates `CAST('' AS BOOLEAN)`). `true` works correctly.

## SPEC-11.PG-BIGINT `[Issue #6]` PostgreSQL BIGINT overflow
**Status:** Known Issue
**Platforms:** PostgreSQL-PDO
**Tests:** `Pdo/PostgresTypeEdgeCaseTest`

Large integers (> 2^31) in BIGINT columns fail on SELECT (CTE generates `CAST(value AS integer)` instead of `bigint`). MySQL and SQLite handle BIGINT correctly.

## SPEC-11.SQLITE-DELETE-NO-WHERE `[Issue #7]` DELETE without WHERE clause (SQLite)
**Status:** Known Issue
**Platforms:** SQLite-PDO
**Related specs:** [SPEC-4.3](04-write-operations.ears.md)
**Tests:** `Pdo/SqliteDeleteWithoutWhereTest`

`DELETE FROM table` without WHERE is silently ignored on SQLite. Workaround: `DELETE FROM table WHERE 1=1`.

## SPEC-11.MYSQL-EXCEPT-INTERSECT `[Issue #14]` EXCEPT/INTERSECT on MySQL
**Status:** Known Issue
**Platforms:** MySQLi, MySQL-PDO
**Related specs:** [SPEC-3.3d](03-read-operations.ears.md)
**Tests:** `Mysqli/ExceptIntersectTest`, `Pdo/MysqlExceptIntersectTest`

EXCEPT and INTERSECT throw `UnsupportedSqlException` on MySQL. The CTE rewriter misparses them as multi-statement queries. Workaround: NOT IN / IN subqueries.

## SPEC-11.PG-EXTRACT `[Issue #15]` PostgreSQL EXTRACT on shadow dates
**Status:** Known Issue
**Platforms:** PostgreSQL-PDO
**Tests:** `Pdo/PostgresDateTimeFunctionsTest`

`EXTRACT(YEAR FROM date_column)` returns 0 for shadow-stored dates. Workaround: use `TO_CHAR(date_col, 'YYYY')`.

## SPEC-11.UPSERT-SELF-REF `[Issue #16]` ON DUPLICATE KEY UPDATE self-referencing expression
**Status:** Known Issue
**Platforms:** MySQLi, MySQL-PDO
**Tests:** `Mysqli/InsertSelectUpsertTest`, `Pdo/MysqlInsertSelectUpsertTest`

`INSERT ... ON DUPLICATE KEY UPDATE stock = stock + VALUES(stock)` loses original row's value. Simple replacement (`stock = VALUES(stock)`) works.

## SPEC-11.INSERT-DEFAULT `[Issue #31]` INSERT with DEFAULT keyword
**Status:** Known Issue
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/InsertDefaultValuesTest`, `Pdo/MysqlInsertDefaultValuesTest`, `Pdo/PostgresInsertDefaultValuesTest`, `Pdo/SqliteInsertDefaultValuesTest`

`INSERT INTO t (col) VALUES (DEFAULT)` and `INSERT INTO t DEFAULT VALUES` both fail under ZTD. The InsertTransformer converts to SELECT expressions where DEFAULT is invalid. Workaround: supply values explicitly.

## SPEC-11.PG-ARRAY-TYPE `[Issue #33]` PostgreSQL array types broken
**Status:** Known Issue
**Platforms:** PostgreSQL-PDO
**Tests:** `Pdo/PostgresArrayTypeTest`

INSERT with INTEGER[] array values succeeds, but SELECT fails (CastRenderer emits base type without array suffix). TEXT[] is unaffected. ARRAY constructor syntax causes column count errors.

## SPEC-11.BINARY-DATA `[Issue #19]` BLOB/BINARY data with binary bytes
**Status:** Known Issue
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO

Inserting binary data (null bytes, high-byte values) via prepared statements succeeds, but SELECT fails (CTE rewriter embeds binary bytes as string literals). Text-only BLOB payloads work. Workaround: base64 encode or disable ZTD.

## SPEC-11.PG-QUOTE-ESCAPE `[Issue #25]` Doubled single-quote escaping (PostgreSQL)
**Status:** Known Issue
**Platforms:** PostgreSQL-PDO
**Tests:** `Pdo/PostgresEscapedQuoteTest` (if exists)

PostgreSQL PgSqlParser's regex doesn't handle `''` escaping, causing incorrect WHERE clause extraction. Workaround: use prepared statements.

## SPEC-11.SQLITE-CTAS `[Issue #36]` CREATE TABLE AS SELECT (SQLite)
**Status:** Known Issue
**Platforms:** SQLite-PDO
**Related specs:** [SPEC-5.1c](05-ddl-operations.ears.md)
**Tests:** `Pdo/SqliteCtasEmptyResultTest`

SELECT immediately after CTAS fails with "no such table". After INSERT, original CTAS data is lost.

## SPEC-11.INSERT-SELECT-COMPUTED `[Issue #20]` INSERT...SELECT with computed columns (SQLite/PostgreSQL)
**Status:** Known Issue
**Platforms:** SQLite-PDO, PostgreSQL-PDO
**Related specs:** [SPEC-4.1a](04-write-operations.ears.md)
**Tests:** `Pdo/SqliteInsertSelectComputedColumnsTest`, `Pdo/SqliteInsertSelectAggregateTest`, `Pdo/SqliteInsertSelectGroupByWithParamsTest`, `Pdo/PostgresInsertSelectGroupByWithParamsTest`

Computed columns and aggregated values become NULL when using INSERT...SELECT on SQLite and PostgreSQL. MySQL works correctly. This extends to GROUP BY with aggregate functions (COUNT, SUM) — both exec() and prepared statement paths produce NULL aggregates on SQLite and PostgreSQL.

**Extended scope (2026-03-10):** The NULL column issue is broader than computed/aggregated columns. INSERT...SELECT with an explicit partial column list (`INSERT INTO t (col1, col2) SELECT ...`) that omits columns with defaults (AUTOINCREMENT/SERIAL PKs, nullable columns) produces all-NULL values even for simple column references on SQLite and PostgreSQL. This affects common patterns like `INSERT INTO log_table (ref_id, name, action) SELECT id, name, 'imported' FROM source_table`. MySQL handles this correctly. Tests: `Pdo/SqliteInsertSelectPartialColumnListTest`, `Pdo/PostgresInsertSelectPartialColumnListTest`.

## SPEC-11.MYSQL-COMMA-UPDATE `[Issue #44]` MySQL comma-syntax multi-table UPDATE
**Status:** Known Issue
**Platforms:** MySQLi, MySQL-PDO
**Related specs:** [SPEC-4.2c](04-write-operations.ears.md)

`UPDATE t1, t2 SET ... WHERE ...` is partially supported. Prefer JOIN syntax.

## SPEC-11.UPDATE-SUBQUERY-SET `[Issue #10]` UPDATE SET col = (subquery) platform differences
**Status:** Known Issue
**Platforms:** PostgreSQL-PDO, SQLite-PDO (fails); MySQLi, MySQL-PDO (works)

Non-correlated scalar subqueries in UPDATE SET work on MySQL and SQLite but fail on PostgreSQL. Correlated subqueries in SET fail on SQLite.

## SPEC-11.FULLTEXT `[Issue #43]` Full-text search not supported through ZTD
**Status:** Known Issue
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Related specs:** [SPEC-3.3f](03-read-operations.ears.md)
**Tests:** `Mysqli/FullTextSearchTest`, `Pdo/MysqlFullTextSearchTest`, `Pdo/PostgresFullTextSearchTest`, `Pdo/SqliteFullTextSearchTest`

Full-text search queries (MATCH...AGAINST on MySQL, tsvector/tsquery on PostgreSQL, FTS5 MATCH on SQLite) fail through ZTD CTE rewriting. On MySQL, the CTE-derived table does not carry FULLTEXT indexes, causing error 1214. On SQLite, FTS5 virtual table references are not recognized by the CTE rewriter. On PostgreSQL, tsvector column types are not correctly reproduced in CTE casts. Workaround: disable ZTD for full-text search queries.

## SPEC-11.PG-PREPARED-FUNCTION `[Issue #45]` PostgreSQL prepared statement with UDF in WHERE
**Status:** Known Issue
**Platforms:** PostgreSQL-PDO
**Related specs:** [SPEC-3.3g](03-read-operations.ears.md)
**Tests:** `Pdo/PostgresStoredFunctionTest`

On PostgreSQL, prepared statements with user-defined functions in WHERE clauses may return incorrect (empty) results, despite the same query working correctly via `query()`. The `$1` placeholder parameter combined with a UDF call in the WHERE condition does not filter correctly through the CTE-rewritten query.

## SPEC-11.UPDATE-FROM `[Issue #72]` UPDATE ... FROM syntax not supported (SQLite)
**Status:** Known Issue
**Platforms:** SQLite-PDO (confirmed)
**Related specs:** [SPEC-4.2](04-write-operations.ears.md), [SPEC-10.2.56](10-platform-notes.ears.md)
**Tests:** `Pdo/SqliteUpdateFromJoinTest`

`UPDATE t1 SET col = t2.col FROM t2 WHERE t1.id = t2.fk` (UPDATE FROM join syntax, SQLite 3.33+) throws syntax error through ZTD on SQLite. The CTE rewriter's SQL parser does not recognize the FROM clause in UPDATE statements and produces invalid SQL. This also applies to UPDATE FROM with derived tables and prepared UPDATE FROM.

**PostgreSQL:** UPDATE FROM now works correctly through ZTD (see [SPEC-10.2.56](10-platform-notes.ears.md)).

The shadow store is not corrupted by the failure. Workaround: use `WHERE id IN (SELECT ...)` subqueries instead of UPDATE FROM joins.

## SPEC-11.PG-LATERAL `[Issue #71]` LATERAL subqueries return empty (PostgreSQL)
**Status:** Known Issue
**Platforms:** PostgreSQL-PDO
**Related specs:** [SPEC-10.2.27](10-platform-notes.ears.md)
**Tests:** `Pdo/PostgresLateralSubqueryTest`

PostgreSQL `LATERAL` subqueries return empty results through ZTD. The CTE rewriter does not rewrite table references inside LATERAL clauses — the inner correlated subquery reads from the physical table (empty), so the outer query gets no rows. This affects all LATERAL patterns: `FROM table, LATERAL (SELECT ...)`, `LEFT JOIN LATERAL ... ON true`, and LATERAL with LIMIT (top-N per group).

Workarounds:
- Use correlated subqueries in the SELECT list: `SELECT (SELECT SUM(x) FROM t WHERE t.fk = u.id) FROM u`.
- Use regular JOINs with GROUP BY subqueries: `JOIN (SELECT fk, SUM(x) FROM t WHERE 1=1 GROUP BY fk) sub ON sub.fk = u.id`.
- For top-N per group, use window functions: `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...) = 1`.

## SPEC-11.BARE-SUBQUERY-REWRITE `[Issue #73]` Bare subquery table references not rewritten (SQLite)
**Status:** Known Issue
**Platforms:** SQLite-PDO (confirmed)
**Related specs:** [SPEC-3.3](03-read-operations.ears.md)
**Tests:** `Pdo/SqliteScalarSubqueryInSelectTest`, `Pdo/SqlitePivotReportTest`, `Pdo/SqliteSubqueryNestingTest`

On SQLite, the CTE rewriter does not rewrite table references inside subqueries that contain a bare `SELECT ... FROM table` without a WHERE or GROUP BY clause. This affects:

1. **Scalar subqueries in SELECT list**: `SELECT col, (SELECT SUM(x) FROM t) AS total FROM t` — the inner subquery reads from the physical table (empty), causing the entire outer query to return empty results.
2. **User CTEs without WHERE/GROUP BY**: `WITH cte AS (SELECT * FROM t) SELECT * FROM cte` — the CTE reads from the physical table, returning 0 rows.
3. **User CTE + CROSS JOIN**: Even with `WHERE 1=1` in the user CTE, combining a user CTE via CROSS JOIN with an outer query that also references a shadow table returns empty.
4. **Scalar subqueries in CASE WHEN**: `CASE WHEN col > (SELECT AVG(x) FROM t) THEN ...` — the inner subquery reads from the physical table (empty), causing AVG to return NULL and all comparisons to fail. The entire outer query returns empty.

**MySQL and PostgreSQL:** Scalar subqueries in SELECT and CASE WHEN now work correctly on MySQLi, MySQL-PDO, and PostgreSQL-PDO. The CTE rewriter rewrites table references inside scalar subqueries on these platforms.

Adding `WHERE 1=1` to the bare subquery forces the rewriter to recognize and rewrite the table reference, except in the CTE + CROSS JOIN case.

Workarounds:
- For scalar subqueries in SELECT: add `WHERE 1=1` to the inner subquery, or use CROSS JOIN with a derived table `(SELECT SUM(x) AS total FROM t) t`.
- For user CTEs: add `WHERE 1=1` to the CTE definition, or use GROUP BY.
- For percentage/ratio calculations: use CROSS JOIN with derived table instead of scalar subquery.

## SPEC-11.UPDATE-AGGREGATE-SUBQUERY `[Issue #9]` UPDATE with aggregate subquery in WHERE
**Status:** Known Issue
**Platforms:** SQLite-PDO (confirmed)
**Related specs:** [SPEC-4.2](04-write-operations.ears.md)
**Tests:** `Pdo/SqliteBulkConditionalUpgradeTest`

`UPDATE table SET col = value WHERE id IN (SELECT col FROM other_table GROUP BY col HAVING SUM(amount) >= N)` causes "incomplete input" on SQLite. The CTE rewriter truncates the SQL when processing UPDATE/DELETE statements whose WHERE clause contains a subquery with GROUP BY and HAVING aggregate expressions. The shadow store is not corrupted by the failure. This also affects DELETE statements: `DELETE FROM table WHERE id NOT IN (SELECT MIN(id) FROM table GROUP BY col)` fails with the same "incomplete input" error.

Workaround: query eligible IDs first via SELECT, then UPDATE by explicit ID list.

```sql
-- Fails on SQLite:
UPDATE customers SET tier = 'gold'
WHERE id IN (SELECT customer_id FROM orders GROUP BY customer_id HAVING SUM(amount) >= 1000);

-- Workaround (all platforms):
-- Step 1: SELECT eligible IDs
SELECT customer_id FROM orders WHERE status = 'completed' GROUP BY customer_id HAVING SUM(amount) >= 1000;
-- Step 2: UPDATE by explicit list
UPDATE customers SET tier = 'gold' WHERE id IN (1, 3, 7);
```

## SPEC-11.DERIVED-TABLE-PREPARED `[Issue #13]` Prepared statements with derived tables return empty
**Status:** Known Issue
**Platforms:** MySQLi, MySQL-PDO, SQLite-PDO (confirmed); PostgreSQL-PDO (works correctly)
**Related specs:** [SPEC-3.3](03-read-operations.ears.md), [SPEC-10.2.65](10-platform-notes.ears.md)
**Tests:** `Mysqli/LeaderboardRankingTest`, `Pdo/MysqlLeaderboardRankingTest`, `Pdo/SqliteLeaderboardRankingTest`

Prepared statements with derived tables (subquery in FROM) that contain window functions return empty results on MySQL (MySQLi, MySQL-PDO) and SQLite-PDO. The same query works correctly via `query()` on MySQL. PostgreSQL-PDO works correctly with both `prepare()` and `query()`.

```sql
-- Returns empty on MySQL and SQLite when used with prepare()+execute():
SELECT username, score, player_rank
FROM (
    SELECT username, score,
           DENSE_RANK() OVER (ORDER BY score DESC) AS player_rank
    FROM players
) ranked
WHERE username = ?
```

Workarounds:
- Use correlated subqueries instead of derived tables: `SELECT p.username, p.score, (SELECT COUNT(DISTINCT p2.score) FROM players p2 WHERE p2.score > p.score) + 1 AS player_rank FROM players p WHERE p.username = ?`.
- Use `query()` with escaped parameters instead of `prepare()`.

## SPEC-11.CTE-JOIN-BACK `[Issue #52]` User CTE joined back to original table returns empty
**Status:** Known Issue
**Platforms:** SQLite-PDO (confirmed)
**Related specs:** [SPEC-10.2.96](10-platform-notes.ears.md), [SPEC-3.3c](03-read-operations.ears.md)
**Tests:** `Pdo/SqliteChainedUserCteTest`, `Pdo/MysqlChainedUserCteTest`, `Mysqli/ChainedUserCteTest`

When the outer SELECT of a user CTE query joins a user-defined CTE back to the original table (`FROM table s JOIN user_cte t ON ...`), the query returns empty results. Chained CTEs work correctly when the outer SELECT only references user CTEs (not the original table). The CTE rewriter may conflict when the outer query references both a physical table and a user-defined CTE simultaneously.

```sql
-- Returns empty (SQLite confirmed):
WITH regional AS (
    SELECT region, SUM(amount) AS region_total
    FROM sales GROUP BY region
),
top_regions AS (
    SELECT region, region_total FROM regional WHERE region_total >= 450
)
SELECT s.id, s.product, t.region_total
FROM sales s
JOIN top_regions t ON s.region = t.region;

-- Workaround: avoid joining CTE back to original table.
-- Use a single CTE or subquery instead:
SELECT s.id, s.product, r.region_total
FROM sales s
JOIN (SELECT region, SUM(amount) AS region_total FROM sales GROUP BY region HAVING SUM(amount) >= 450) r
ON s.region = r.region;
```

## SPEC-11.CORRELATED-UPDATE-SET `[Issue #51]` Correlated scalar subquery in UPDATE SET clause
**Status:** Known Issue
**Platforms:** PostgreSQL-PDO, SQLite-PDO (confirmed); MySQLi, MySQL-PDO (works correctly)
**Related specs:** [SPEC-10.2.88](10-platform-notes.ears.md)
**Tests:** `Pdo/PostgresCorrelatedUpdateTest`, `Pdo/SqliteCorrelatedUpdateTest`

`UPDATE t1 SET col = (SELECT AGG(col) FROM t2 WHERE t2.fk = t1.id)` fails on PostgreSQL (CTE rewriter grouping error) and SQLite (CTE rewriter syntax error). MySQL handles this correctly via both MySQLi and PDO. Correlated subqueries in SELECT and in WHERE (UPDATE WHERE IN (subquery), DELETE WHERE NOT EXISTS) work on all platforms.

Workaround: query the computed values first via SELECT, then UPDATE each row with explicit values.

```sql
-- Fails on PostgreSQL/SQLite:
UPDATE departments SET avg_salary = (SELECT AVG(salary) FROM employees WHERE department_id = departments.id);

-- Workaround (all platforms):
-- Step 1: Query aggregates
SELECT department_id, AVG(salary) AS avg_sal FROM employees GROUP BY department_id;
-- Step 2: Update with explicit values
UPDATE departments SET avg_salary = 87500.00 WHERE id = 1;
UPDATE departments SET avg_salary = 57500.00 WHERE id = 2;
```

## SPEC-11.CHECK-COLUMN-NAME `[Issue #70]` Column names containing "check" cause INSERT failures
**Status:** Known Issue
**Platforms:** SQLite-PDO (confirmed), MySQLi, MySQL-PDO, PostgreSQL-PDO (likely affected)
**Related specs:** [SPEC-4.1](04-write-operations.ears.md)

Column names containing the substring `check` (e.g., `check_in`, `check_out`, `checkin_date`, `checkout_date`) cause `INSERT INTO table VALUES (...)` to fail with "Insert values count does not match column count". The SQL parser likely interprets `check` as the SQL `CHECK` constraint keyword during schema reflection, miscounting the columns.

Workarounds:
- Use explicit column lists in INSERT: `INSERT INTO table (col1, col2, ...) VALUES (...)`.
- Rename columns to avoid the `check` substring: use `arrival_date` / `departure_date` instead of `check_in` / `check_out`.
- Quote column names in DDL with double quotes (SQLite confirmed).

## SPEC-11.UNION-ALL-DERIVED `[Issue #13]` UNION ALL in derived table returns empty
**Status:** Known Issue
**Platforms:** SQLite-PDO (confirmed), MySQL-PDO (confirmed), PostgreSQL-PDO (works correctly)
**Related specs:** [SPEC-3.3](03-read-operations.ears.md), [SPEC-10.2.166](10-platform-notes.ears.md)
**Tests:** `Pdo/SqliteInventorySnapshotTest`, `Pdo/MysqlInventorySnapshotTest`, `Pdo/PostgresInventorySnapshotTest`

UNION ALL inside a derived table (subquery in FROM clause) returns empty results through ZTD on SQLite. Top-level UNION ALL works correctly — the CTE rewriter rewrites table references in `SELECT ... FROM t1 UNION ALL SELECT ... FROM t2`. However, when UNION ALL is wrapped in a derived table — `SELECT ... FROM (SELECT ... FROM t1 UNION ALL SELECT ... FROM t2) alias` — the rewriter does not rewrite the table references inside the UNION branches. Both branches read from the physical tables (empty), returning 0 rows.

This also affects INSERT ... SELECT with UNION ALL derived tables and prepared statements with UNION ALL derived tables.

```sql
-- Returns empty on SQLite:
SELECT bin_id, SUM(qty) AS net
FROM (
    SELECT bin_id, qty FROM inbound
    UNION ALL
    SELECT bin_id, -qty FROM outbound
) movements
GROUP BY bin_id;

-- Workaround: use separate aggregate subqueries with LEFT JOIN:
SELECT b.id,
       COALESCE(i.total_in, 0) - COALESCE(o.total_out, 0) AS net
FROM bins b
LEFT JOIN (SELECT bin_id, SUM(qty) AS total_in FROM inbound GROUP BY bin_id) i ON i.bin_id = b.id
LEFT JOIN (SELECT bin_id, SUM(qty) AS total_out FROM outbound GROUP BY bin_id) o ON o.bin_id = b.id;
```

## SPEC-11.WINDOW-DERIVED `[Issue #13]` Window function in derived table returns empty
**Status:** Known Issue
**Platforms:** MySQLi (confirmed), MySQL-PDO (confirmed), PostgreSQL-PDO (confirmed), SQLite-PDO (confirmed)
**Related specs:** [SPEC-3.3](03-read-operations.ears.md), [SPEC-10.2.167](10-platform-notes.ears.md)
**Tests:** `Mysqli/SalesCommissionTest`, `Pdo/MysqlSalesCommissionTest`, `Pdo/PostgresSalesCommissionTest`, `Pdo/SqliteSalesCommissionTest`

A SELECT from a derived table containing window functions returns empty results through ZTD on all platforms. The common "top-N per group" pattern — `SELECT * FROM (SELECT ..., ROW_NUMBER() OVER (...) AS rn FROM t) sub WHERE rn = 1` — returns 0 rows. Top-level window function queries work correctly (ROW_NUMBER, SUM OVER, LAG all return expected results). The CTE rewriter does not rewrite table references inside derived tables that contain window functions.

```sql
-- Returns empty on all platforms:
SELECT r.name, d.client, d.amount
FROM (
    SELECT d.*, ROW_NUMBER() OVER (PARTITION BY d.rep_id ORDER BY d.amount DESC) AS rn
    FROM deals d
) d
JOIN reps r ON r.id = d.rep_id
WHERE d.rn = 1;

-- Workaround: use correlated subquery with MAX instead:
SELECT r.name, d.client, d.amount
FROM deals d
JOIN reps r ON r.id = d.rep_id
WHERE d.amount = (SELECT MAX(d2.amount) FROM deals d2 WHERE d2.rep_id = d.rep_id);
```

## SPEC-11.CASE-WHERE-PARAMS `[Issue #75]` CASE-as-boolean in WHERE with prepared params returns wrong count
**Status:** Known Issue
**Platforms:** MySQLi (confirmed), MySQL-PDO (confirmed), PostgreSQL-PDO (confirmed), SQLite-PDO (confirmed)
**Related specs:** [SPEC-3.6](03-read-operations.ears.md), [SPEC-10.2.169](10-platform-notes.ears.md)
**Tests:** `Mysqli/WaitlistReservationTest`, `Pdo/MysqlWaitlistReservationTest`, `Pdo/PostgresWaitlistReservationTest`, `Pdo/SqliteWaitlistReservationTest`

Using a CASE expression as a boolean filter in a WHERE clause with prepared statement parameters returns incorrect row counts through ZTD on all platforms. A query with `WHERE status = ? AND CASE WHEN ? = 'value' THEN condition END` returns more rows than expected — the CASE filter appears to be ignored or always evaluate to true.

```sql
-- Returns 3 rows instead of expected 2 on all platforms:
SELECT w.guest_name, w.party_size, w.priority
FROM waitlist w
WHERE w.status = ?
  AND CASE WHEN ? = 'high' THEN w.priority = 1
           WHEN ? = 'medium' THEN w.priority <= 2
           ELSE 1=1
      END
ORDER BY w.priority, w.id;
-- Params: ['waiting', 'medium', 'medium']

-- Workaround: expand CASE conditions into explicit OR/AND logic in the application layer.
```

## SPEC-11.PG-SELF-REF-UPDATE `[Issue #74]` Self-referencing UPDATE WHERE IN/subquery (PostgreSQL)
**Status:** Known Issue
**Platforms:** PostgreSQL-PDO (confirmed); MySQLi, MySQL-PDO, SQLite-PDO (works correctly)
**Related specs:** [SPEC-10.2.173](10-platform-notes.ears.md)
**Tests:** `Pdo/PostgresDeleteReinsertCycleTest`

Three related failures when UPDATE references its own table in WHERE subqueries on PostgreSQL:

1. **UPDATE WHERE IN (SELECT from same table):** `UPDATE t SET status = 'x' WHERE id IN (SELECT id FROM t WHERE condition)` fails with "table name specified more than once". The CTE rewriter generates a duplicate table reference in the rewritten SQL.

2. **UPDATE WHERE = (scalar subquery from same table):** `UPDATE t SET price = price * 1.1 WHERE category = (SELECT category FROM t ORDER BY price DESC LIMIT 1)` fails with syntax error. The CTE rewriter incorrectly expands the table reference inside the scalar subquery.

3. **UPDATE WHERE IN (SELECT with JOIN+GROUP BY):** `UPDATE t SET status = 'x' WHERE id IN (SELECT i.id FROM t i JOIN (SELECT ... GROUP BY ...) p ON ... WHERE ...)` fails with "column reference 'id' is ambiguous". The CTE rewriter loses table qualification on the outer WHERE clause.

All three patterns work correctly on MySQL (both MySQLi and PDO) and SQLite-PDO. Only PostgreSQL is affected.

Workarounds:
- Query the IDs first via SELECT, then UPDATE by explicit ID list.
- Use application-side filtering instead of self-referencing subqueries.

```sql
-- All fail on PostgreSQL:
UPDATE products SET status = 'featured' WHERE id IN (SELECT id FROM products WHERE category = 'x');
UPDATE products SET price = price * 1.1 WHERE category = (SELECT category FROM products ORDER BY price DESC LIMIT 1);
UPDATE invoices SET status = 'paid' WHERE id IN (SELECT i.id FROM invoices i JOIN (...) p ON ... WHERE p.total >= i.amount);

-- Workaround (all platforms):
-- Step 1: Query IDs
SELECT id FROM products WHERE category = 'electronics';
-- Step 2: UPDATE by explicit list
UPDATE products SET status = 'featured' WHERE id IN (1, 2);
```

## SPEC-11.EXCEPTION-WRAPPING `[Issue #2]` RuntimeException instead of DatabaseException
**Status:** Known Issue
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Related specs:** [SPEC-7.1](07-unknown-schema.ears.md)
**Tests:** `Pdo/MysqlExceptionWrappingTest`, `Pdo/PostgresExceptionWrappingTest`, `Pdo/SqliteExceptionWrappingTest`, `Mysqli/ExceptionWrappingTest`

When UPDATE or DELETE targets a table whose schema was not reflected at adapter construction time, `ShadowStore` throws a raw `RuntimeException` instead of `DatabaseException`. The adapter-specific exception wrapping (`ZtdMysqli::query()` catches `DatabaseException`, `ZtdPdo::exec()` catches `DatabaseException`) does not catch `RuntimeException`, so it propagates unwrapped to user code. Users cannot rely on catching the adapter-specific exception type.

## SPEC-11.SELF-REF-UPDATE-HAVING `[Issue #11]` Self-referencing UPDATE with GROUP BY HAVING
**Status:** Known Issue
**Platforms:** MySQL-PDO, MySQLi (incorrect results); SQLite-PDO (syntax error); PostgreSQL-PDO (ambiguous column)
**Tests:** `Pdo/MysqlAdvancedSubqueryTest`

`UPDATE table SET col = val WHERE col IN (SELECT col FROM same_table GROUP BY col HAVING AGG(col) > N)` incorrectly updates ALL rows on MySQL instead of only the rows matching the HAVING condition. The CTE rewriter mishandles the self-referencing subquery with GROUP BY HAVING. On SQLite, the same query fails with "incomplete input". On PostgreSQL, it fails with "column reference is ambiguous".

## SPEC-11.RECURSIVE-CTE-SHADOW `[Issue #12]` WITH RECURSIVE + shadow table
**Status:** Known Issue
**Platforms:** MySQL-PDO, MySQLi (syntax error); SQLite-PDO (empty results); PostgreSQL-PDO (empty results)
**Tests:** `Pdo/MysqlRecursiveCteAndRightJoinTest`, `Pdo/PostgresRecursiveCteAndRightJoinTest`, `Pdo/SqliteRecursiveCteAndRightJoinTest`

The CTE rewriter prepends its own `WITH` clause before the user's `WITH RECURSIVE`, producing invalid SQL on MySQL (`WITH ztd_shadow AS (...), RECURSIVE cat_tree AS (...)`). On SQLite and PostgreSQL, the query executes but table references inside the recursive CTE are not rewritten, causing the recursive part to read from the physical table (empty) and return 0 rows.

## SPEC-11.CTE-DML `[Issue #28]` CTE-based DML not supported
**Status:** Known Issue (Feature Gap)
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Related specs:** [SPEC-4.1](04-write-operations.ears.md)
**Tests:** `Pdo/MysqlCteDmlTest`, `Pdo/PostgresCteDmlTest`, `Pdo/SqliteCteDmlTest`, `Mysqli/CteDmlTest`

CTE-based DML statements (`WITH ... INSERT`, `WITH ... UPDATE`, `WITH ... DELETE`) are not supported on any platform. On MySQL, the mutation resolver receives a `WithStatement` instead of an `InsertStatement`/`UpdateStatement`/`DeleteStatement`. On SQLite, CTE name collisions occur. On PostgreSQL, invalid SQL is produced. Workaround: rewrite as standard DML with subqueries.

## SPEC-11.PG-CONFLICT-WHERE `[Issue #30]` ON CONFLICT DO UPDATE WHERE clause ignored
**Status:** Known Issue
**Platforms:** PostgreSQL-PDO
**Related specs:** [SPEC-4.2a](04-write-operations.ears.md)
**Tests:** `Pdo/PostgresOnConflictWhereTest`

PostgreSQL conditional upserts with `ON CONFLICT DO UPDATE ... WHERE condition` have the WHERE clause stripped by `PgSqlParser::extractOnConflictUpdateColumns()`. The `UpsertMutation` always updates conflicting rows regardless of the WHERE condition. This produces incorrect results — rows are updated when they should be left unchanged because the WHERE condition was not met.

## SPEC-11.PG-CTAS-TEXT `[Issue #37]` CTAS column types default to TEXT (PostgreSQL)
**Status:** Known Issue
**Platforms:** PostgreSQL-PDO
**Related specs:** [SPEC-5.1c](05-ddl-operations.ears.md)
**Tests:** `Pdo/PostgresCtasTest`

When `CREATE TABLE AS SELECT` creates a shadow table on PostgreSQL, all column types default to TEXT (via `ColumnType::unknown()`). Subsequent queries with integer comparisons fail with "operator does not exist: text = integer". The fix would be to infer column types from the source table schema. Workaround: use explicit `CAST` or string comparisons in WHERE clauses.

## SPEC-11.INSERT-SELECT-CASE `[Issue #76]` INSERT...SELECT with CASE expression produces 0 rows (SQLite)
**Status:** Known Issue
**Platforms:** SQLite-PDO (confirmed)
**Related specs:** [SPEC-4.1a](04-write-operations.ears.md), [SPEC-10.2.175](10-platform-notes.ears.md)
**Tests:** `Pdo/SqlitePayrollDeductionTest`

`INSERT INTO t (cols) SELECT col, CASE WHEN condition THEN expr1 ELSE expr2 END, ... FROM source` produces 0 inserted rows on SQLite through ZTD. The INSERT appears to execute without error, but a subsequent SELECT on the target table shows no new rows were added. This extends SPEC-11.INSERT-SELECT-COMPUTED — not only do computed/aggregated values become NULL, but CASE expressions in INSERT...SELECT may cause the entire INSERT to produce no rows.

```sql
-- Produces 0 rows on SQLite (expected 4):
INSERT INTO payroll (id, employee_id, pay_period, gross_pay, net_pay, status)
SELECT e.id + 10, e.id, '2025-02',
       CASE WHEN e.department = 'Engineering' THEN e.base_salary * 1.10
            ELSE e.base_salary
       END,
       NULL, 'pending'
FROM employee e;
```

Workaround: query the source data first via SELECT, compute values in application code, then INSERT explicit rows.

## SPEC-11.PG-UPDATE-SET-FROM-KEYWORD `[Issue #47]` UPDATE SET with FROM-syntax functions (PostgreSQL)
**Status:** Known Issue
**Platforms:** PostgreSQL-PDO (confirmed)
**Related specs:** [SPEC-10.2.184](10-platform-notes.ears.md)
**Tests:** `Pdo/PostgresUpdateSetFromKeywordTest`, `Pdo/MysqlUpdateSetFromKeywordTest` (control), `Mysqli/UpdateSetFromKeywordTest` (control), `Pdo/SqliteUpdateSetFromKeywordTest` (control)

`PgSqlParser::extractUpdateSets` uses a regex that terminates SET clause extraction at the first `FROM` keyword. Standard SQL functions that use `FROM` as part of their syntax — `TRIM(BOTH ' ' FROM col)`, `SUBSTRING(col FROM n FOR m)`, `EXTRACT(field FROM source)` — are truncated, producing invalid SQL.

Affected patterns:
- `UPDATE t SET name = TRIM(BOTH ' ' FROM name) WHERE id = 1` — parsed as `SET name = TRIM(BOTH ' '`, producing `SELECT TRIM(BOTH ' ' AS "name"` → syntax error.
- `UPDATE t SET code = SUBSTRING(code FROM 1 FOR 3) WHERE id = 1` — parsed as `SET code = SUBSTRING(code`, truncated at `FROM`.
- `UPDATE t SET yr = EXTRACT(YEAR FROM created_at) WHERE id = 1` — parsed as `SET yr = EXTRACT(YEAR`, truncated at `FROM`.
- Multiple SET assignments with TRIM(... FROM ...) lose all subsequent assignments.

This is distinct from SPEC-11.PG-EXTRACT (which documents incorrect CTE casting of dates) and SPEC-11.UPDATE-SUBQUERY-SET (which documents subquery failures in SET). The root cause is the same regex (`/\bSET\s+(.+?)(?:\s+FROM\s+|...)/is`) but the trigger is standard SQL function syntax rather than subqueries or UPDATE...FROM joins.

MySQL (MySQLi, MySQL-PDO) and SQLite-PDO are NOT affected — MySQL uses phpMyAdmin SqlParser (proper parser, not regex), and SQLite's regex does not use `FROM` as a SET clause terminator.

Workarounds:
- Use function-specific alternatives: `TRIM(name)` instead of `TRIM(BOTH ' ' FROM name)`, `SUBSTR(code, 1, 3)` instead of `SUBSTRING(code FROM 1 FOR 3)`.
- Compute the value in a SELECT first, then UPDATE with the explicit result.

```sql
-- Fails on PostgreSQL:
UPDATE items SET name = TRIM(BOTH ' ' FROM name) WHERE id = 1;
UPDATE items SET code = SUBSTRING(code FROM 1 FOR 3) WHERE id = 1;

-- Workaround (all platforms):
UPDATE items SET name = TRIM(name) WHERE id = 1;
UPDATE items SET code = SUBSTR(code, 1, 3) WHERE id = 1;
```

## SPEC-11.PG-JSONB-QUESTION-MARK `[Issue #48]` JSONB ? / ?| / ?& operators conflict with parameter placeholder
**Status:** Known Issue
**Platforms:** PostgreSQL-PDO (confirmed)
**Related specs:** [SPEC-3.5](03-read-operations.ears.md)
**Tests:** `Pdo/PostgresJsonbOperatorConflictTest`, `Pdo/PostgresJsonbColumnTest`

PostgreSQL JSONB operators `?` (key exists), `?|` (any key exists), and `?&` (all keys exist) conflict with the CTE rewriter's parameter placeholder detection. The PgSqlParser treats `?` as a prepared-statement parameter marker and converts it to `$N`, producing invalid SQL such as `WHERE attributes $1 'material'`. This affects both `query()` and `prepare()` calls — the `?` is replaced during CTE rewriting, not during PDO parameter binding.

This is distinct from the well-known PDO limitation where `?` in prepared statements conflicts with the JSONB `?` operator. The ZTD CTE rewriter additionally converts `?` to `$N` even in non-prepared `query()` calls, making the operator unusable regardless of execution method.

Workarounds:
- Use `jsonb_exists(col, 'key')` instead of `col ? 'key'`
- Use `jsonb_exists_any(col, array['k1', 'k2'])` instead of `col ?| array['k1', 'k2']`
- Use `jsonb_exists_all(col, array['k1', 'k2'])` instead of `col ?& array['k1', 'k2']`

```sql
-- Fails on PostgreSQL through ZTD:
SELECT name FROM docs WHERE meta ? 'reviewed';
SELECT name FROM docs WHERE meta ?| array['reviewed', 'priority'];
SELECT name FROM docs WHERE meta ?& array['author', 'reviewed'];

-- Workaround (all PostgreSQL contexts):
SELECT name FROM docs WHERE jsonb_exists(meta, 'reviewed');
SELECT name FROM docs WHERE jsonb_exists_any(meta, array['reviewed', 'priority']);
SELECT name FROM docs WHERE jsonb_exists_all(meta, array['author', 'reviewed']);
```

## SPEC-11.SQLITE-MULTI-COL-SET-OP `[Issue #50]` Multi-column INTERSECT/EXCEPT returns empty results on SQLite
**Status:** Known Issue
**Platforms:** SQLite-PDO (confirmed)
**Related specs:** [SPEC-3.3d](03-read-operations.ears.md)
**Tests:** `Pdo/SqliteSetOperationTest::testIntersectMultiColumnReturnsEmptyOnSqlite`, `Pdo/SqliteMultiColumnExceptTest`

Single-column INTERSECT and EXCEPT work correctly on SQLite through the CTE shadow store, but multi-column variants return 0 rows instead of the expected results. For example, `SELECT department, skill FROM employees INTERSECT SELECT department, skill FROM contractors` returns empty when it should return 3 matching (department, skill) pairs. Both INTERSECT and EXCEPT are affected when the projection includes multiple non-PK columns. PostgreSQL handles these correctly.

## SPEC-11.UPDATE-SET-CORRELATED-SUBQUERY `[Issue #51]` UPDATE with subquery in SET clause produces errors
**Status:** Known Issue
**Platforms:** SQLite-PDO (confirmed), PostgreSQL-PDO (confirmed); MySQL-PDO, MySQLi NOT affected
**Related specs:** [SPEC-4.2](04-write-operations.ears.md)
**Tests:** `Pdo/SqliteUpdateSubqueryTest`, `Pdo/PostgresUpdateSubqueryTest`, `Pdo/MysqlUpdateSubqueryTest`, `Pdo/SqliteMultiSubqueryUpdateSetTest`, `Pdo/MysqlMultiSubqueryUpdateSetTest`, `Pdo/PostgresMultiSubqueryUpdateSetTest`

UPDATE statements with subqueries in the SET clause fail through the CTE rewriter on SQLite and PostgreSQL. The issue affects **both correlated and non-correlated** subqueries — any subquery in SET that contains a `FROM ... WHERE` clause triggers the failure. Non-correlated subqueries without a WHERE clause (`SET col = (SELECT AGG(x) FROM t)`) work correctly on all platforms.

**SQLite:** The rewriter produces `near "FROM": syntax error`. The SET clause extraction regex likely terminates at the first `FROM` keyword inside the subquery. This affects single and multiple subqueries in SET, correlated and non-correlated, with and without prepared params.

**PostgreSQL:** Multiple error variants:
- Correlated: `column "price" does not exist` — incorrect aliasing
- Multi-subquery correlated: `column reference "category" is ambiguous` — the CTE rewriter adds the subquery's target table to the outer FROM clause
- Non-correlated with WHERE: `column must appear in GROUP BY clause` — the rewriter wraps the SET subquery with the outer table reference, creating an invalid GROUP BY requirement

**MySQL:** NOT affected — all subquery-in-SET patterns work correctly (MySQLi and MySQL-PDO), including multiple correlated subqueries, non-correlated subqueries, and prepared variants.

Self-referencing scalar subqueries in SET (e.g., `SET price = (SELECT MAX(price) FROM same_table WHERE ...)`) also fail with different errors per platform. UPDATE with subqueries in WHERE clause works correctly on all platforms. DELETE with correlated subqueries also works correctly on all platforms.

## SPEC-11.USER-CTE-CONFLICT `[Issue #52]` User-defined CTEs silently return empty results
**Status:** Known Issue
**Platforms:** PostgreSQL-PDO (all CTE patterns), SQLite-PDO (multiple CTEs JOINed)
**Related specs:** [SPEC-3.3](03-read-operations.ears.md)
**Tests:** `Pdo/PostgresUserCteConflictTest`, `Pdo/SqliteUserCteConflictTest`

Queries containing user-defined CTEs (`WITH ... AS (...)`) silently return 0 rows through the CTE shadow store. On PostgreSQL, ALL user CTE patterns return empty — simple CTEs, multiple CTEs, CTEs referencing other CTEs, and CTEs with prepared statements. On SQLite, simple CTEs and CTE-referencing-CTE patterns work, but multiple CTEs JOINed together return 0 rows. The CTE rewriter adds its own WITH clauses, and these appear to conflict with or shadow user-defined CTEs. This is a high-severity silent data loss bug.

## SPEC-11.PG-RETURNING `[Issue #53]` PostgreSQL RETURNING clause silently drops result set
**Status:** Known Issue
**Platforms:** PostgreSQL-PDO (confirmed)
**Related specs:** [SPEC-4.1](04-write-operations.ears.md), [SPEC-4.2](04-write-operations.ears.md), [SPEC-4.3](04-write-operations.ears.md)
**Tests:** `Pdo/PostgresReturningClauseTest`

INSERT/UPDATE/DELETE with RETURNING clause silently return 0 rows through the CTE shadow store. The mutations themselves succeed (shadow store is updated correctly), but the RETURNING result set is always empty. Affects: `RETURNING *`, `RETURNING col1, col2`, prepared statements with RETURNING, and all three mutation types (INSERT, UPDATE, DELETE). This is a high-severity silent data loss bug — users relying on RETURNING for auto-generated IDs or confirmation data will get no data and no error.

## SPEC-11.INSERT-SELECT-JOIN `[Issue #49]` INSERT...SELECT with multi-table JOIN produces incorrect results
**Status:** Known Issue
**Platforms:** MySQL-PDO (error), MySQL-MySQLi (error), PostgreSQL-PDO (NULL columns), SQLite-PDO (NULL columns)
**Related specs:** [SPEC-4.1a](04-write-operations.ears.md), SPEC-11.INSERT-SELECT-COMPUTED
**Tests:** `Pdo/MysqlInsertSelectJoinAggregateTest`, `Pdo/PostgresInsertSelectJoinAggregateTest`, `Pdo/SqliteInsertSelectJoinAggregateTest`

INSERT...SELECT with multi-table JOINs and aggregates fails or produces incorrect results across all platforms. The InsertTransformer cannot properly resolve column references from JOINed table aliases.

**MySQL:** Throws `PDOException` with "Unknown column 'o.id' in 'field list'" — the InsertTransformer cannot resolve column references from JOINed table aliases at all. This is distinct from SPEC-11.MYSQL-INSERT-SELECT-STAR (which is about SELECT * column count mismatch).

**PostgreSQL / SQLite:** Rows are inserted but non-PK columns from JOINed tables and aggregate functions (COUNT, SUM) become NULL. Some source columns (e.g. `c.region`) may preserve values while others (e.g. `c.name`) become NULL. This extends SPEC-11.INSERT-SELECT-COMPUTED to multi-table JOIN sources.

```sql
-- Fails on MySQL (Unknown column 'o.id'):
INSERT INTO summary (id, customer_id, customer_name, total_orders, total_amount, region)
SELECT c.id, c.id, c.name, COUNT(o.id), SUM(o.amount), c.region
FROM customers c
JOIN orders o ON o.customer_id = c.id
GROUP BY c.id, c.name, c.region;

-- Workaround on MySQL: INSERT...SELECT from a single table (no JOINs)
-- No known workaround for correct aggregate values on PostgreSQL/SQLite
```

## SPEC-11.ALTER-ADD-COL-STALE-SCHEMA `[Issue #54]` ALTER TABLE ADD COLUMN: schema cache not invalidated
**Status:** Known Issue
**Platforms:** SQLite-PDO (confirmed), MySQL-PDO (partial)
**Related specs:** [SPEC-5.2](05-ddl-operations.ears.md)
**Tests:** `Pdo/SqliteAlterAddColumnDmlTest`, `Pdo/MysqlAlterAddColumnDmlTest`

After `ALTER TABLE ADD COLUMN`, the CTE rewriter's schema cache is not invalidated. INSERT and UPDATE referencing the new column succeed (shadow store accepts the data), but SELECT queries that reference the new column fail with "no such column" because the CTE is generated from the stale schema. **Data written to the new column is silently lost** — it enters the shadow store but can never be read back through ZTD.

On MySQL-PDO, INSERT/UPDATE/SELECT with the new column all work, but pre-existing shadow rows do not receive the DEFAULT value for the new column (they get 0/NULL instead).

Queries using only original columns continue to work correctly after ADD COLUMN on all platforms.

## SPEC-11.PDO-REPLACE-PREPARED `[Issue #55]` REPLACE/INSERT OR REPLACE via PDO prepared statement creates duplicate PK rows
**Status:** Known Issue
**Platforms:** MySQL-PDO (confirmed), SQLite-PDO (confirmed)
**Related specs:** [SPEC-4.4](04-write-operations.ears.md)
**Tests:** `Pdo/MysqlReplaceIntoPreparedTest`, `Pdo/SqliteInsertOrReplaceTest`

REPLACE INTO (MySQL) and INSERT OR REPLACE (SQLite) via PDO prepared statements do not delete the existing row in the shadow store. A new row is inserted alongside the original, creating **duplicate primary keys**. The `exec()` path handles REPLACE correctly.

| Method | Behavior |
|--------|----------|
| `exec("REPLACE INTO ...")` | Correct: deletes old, inserts new |
| `prepare("REPLACE INTO ...")->execute()` | **Bug**: inserts without deleting, duplicate PKs |

Related: Issue #42 (MySQLi execute_query REPLACE), Issue #17 (PDO prepared upsert).

## SPEC-11.PG-GENERATE-SERIES `[Issue #56]` PostgreSQL generate_series() LEFT JOIN with shadow table returns empty
**Status:** Known Issue
**Platforms:** PostgreSQL-PDO (confirmed)
**Related specs:** [SPEC-3.1](03-read-operations.ears.md)
**Tests:** `Pdo/PostgresGenerateSeriesTest`

When `generate_series()` with column alias (e.g., `AS d(day)`) is LEFT JOINed with a shadow-stored table, the CTE rewriter does not rewrite the table reference in the JOIN. The physical table (empty) is read, so all amounts are 0/NULL. NOT EXISTS subqueries against shadow tables in generate_series context also fail (return all rows instead of filtering).

Integer `generate_series()` in a derived table (`(SELECT generate_series(1,5) AS n) AS gs`) LEFT JOINed with shadow table **does work** — only the `AS alias(col)` form is affected.

Related: Issue #13 (derived tables not rewritten), SPEC-11.PG-LATERAL (LATERAL inner references not rewritten).

## SPEC-11.PG-IS-NOT-DISTINCT-FROM `[Issue #57]` PostgreSQL IS NOT DISTINCT FROM between columns returns empty
**Status:** Known Issue
**Platforms:** PostgreSQL-PDO (confirmed)
**Related specs:** [SPEC-3.1](03-read-operations.ears.md)
**Tests:** `Pdo/PostgresNullSafeComparisonTest`

`IS NOT DISTINCT FROM` between two columns (or a column and expression) returns empty results through the CTE rewriter when both values are NULL. Column-to-literal comparison (`col IS NOT DISTINCT FROM NULL`) works correctly. The bug is specifically in column-to-column or column-to-expression form.

| Pattern | Behavior |
|---------|----------|
| `WHERE a IS NOT DISTINCT FROM NULL` | Correct |
| `WHERE a IS DISTINCT FROM 'value'` | Correct |
| `WHERE a IS NOT DISTINCT FROM b` | **Bug**: returns empty when both NULL |
| `WHERE a IS NOT DISTINCT FROM expr::TYPE` | **Bug**: returns empty when both NULL |

## SPEC-11.SQLITE-DELETE-HAVING `[Issue #58]` SQLite DELETE with IN (subquery GROUP BY HAVING) produces incomplete SQL
**Status:** Known Issue
**Platforms:** SQLite-PDO (confirmed)
**Related specs:** [SPEC-4.3](04-write-operations.ears.md)
**Tests:** `Pdo/SqliteDeleteWithHavingSubqueryTest`

On SQLite, `DELETE FROM t WHERE id IN (SELECT ... GROUP BY ... HAVING ...)` throws "SQLSTATE[HY000]: General error: 1 incomplete input". The CTE rewriter generates truncated/incomplete SQL when processing DELETE with a subquery containing GROUP BY and HAVING clauses. Also affects `DELETE ... WHERE NOT EXISTS (SELECT ... GROUP BY ... HAVING ...)`.

Related: Issue #9 (UPDATE with IN subquery GROUP BY HAVING), Issue #22 (HAVING with prepared params).

## SPEC-11.MYSQL-DELETE-SELF-HAVING `[Issue #59]` MySQL self-referencing DELETE with IN (subquery GROUP BY HAVING) deletes all rows
**Status:** Known Issue
**Platforms:** MySQL-PDO (confirmed)
**Related specs:** [SPEC-4.3](04-write-operations.ears.md)
**Tests:** `Pdo/MysqlDeleteWithHavingSubqueryTest`

When DELETE references the same table in both the target and the IN subquery with GROUP BY HAVING, the CTE rewriter incorrectly deletes all rows instead of only those matching the HAVING filter. This is the DELETE equivalent of Issue #11 (UPDATE variant).

Related: Issue #11 (UPDATE self-referencing with GROUP BY HAVING).

## SPEC-11.PG-ROW-VALUE-DML `[Issue #60]` Row value constructor in UPDATE/DELETE WHERE produces syntax error
**Status:** Known Issue
**Platforms:** PostgreSQL-PDO (confirmed); MySQL-PDO, MySQLi, SQLite-PDO (works correctly)
**Related specs:** [SPEC-4.2](04-write-operations.ears.md), [SPEC-4.3](04-write-operations.ears.md)
**Tests:** `Pdo/PostgresRowValueInSubqueryTest`

`UPDATE table SET col = val WHERE (col1, col2, col3) IN (SELECT ...)` and `DELETE FROM table WHERE (col1, col2) IN (SELECT ...)` produce a syntax error on PostgreSQL through the CTE rewriter. SELECT with the same row value pattern works correctly. MySQL (both PDO and MySQLi) and SQLite handle row value constructors in UPDATE/DELETE WHERE correctly.

## SPEC-11.PG-CASE-SET-PREPARED `[Issue #61]` UPDATE SET CASE with prepared $N params is silently a no-op
**Status:** Known Issue
**Platforms:** PostgreSQL-PDO (confirmed); MySQL-PDO, MySQLi, SQLite-PDO (works correctly)
**Related specs:** [SPEC-4.2](04-write-operations.ears.md)
**Tests:** `Pdo/PostgresUpdateCaseInSetTest`

UPDATE statements with CASE expressions in the SET clause work correctly via `exec()` for simple CASE WHEN conditions, but when using `prepare()` + `execute()` with `$N` parameters inside the CASE WHEN conditions, the UPDATE is silently a no-op — no rows are modified and no error is thrown. Non-prepared simple CASE in SET works on all platforms.

However, when the CASE expression contains correlated subqueries (EXISTS or scalar), even exec() fails on PostgreSQL — the UPDATE is a no-op (CASE WHEN EXISTS) or produces a grouping error (CASE with scalar correlated subquery). MySQL handles all CASE+subquery variants correctly via both exec() and prepare(). SQLite fails with "near FROM: syntax error" (same as Issue #51).

Related: Issue #47 (TRIM/SUBSTRING FROM in SET), Issue #51 (correlated subquery in SET).

## SPEC-11.PG-FILTER-PREPARED `[Issue #62]` Aggregate FILTER (WHERE col = $N) returns wrong results
**Status:** Known Issue
**Platforms:** PostgreSQL-PDO (confirmed)
**Related specs:** [SPEC-3.1](03-read-operations.ears.md)
**Tests:** `Pdo/PostgresAggregateFilterClauseTest`

Aggregate FILTER clauses work correctly via `query()`, but when `prepare()` is used with `$N` parameters inside the FILTER condition — `SUM(revenue) FILTER (WHERE event_type = $1)` — the filtered aggregate returns 0/NULL. The parameter inside FILTER is either not bound or bound to the wrong position.

Related: Issue #22 (HAVING with prepared params), Issue #45 (UDF in WHERE with prepared params).

## SPEC-11.PG-USING-PREPARED `[Issue #63]` JOIN USING with $N WHERE parameter returns empty
**Status:** Known Issue
**Platforms:** PostgreSQL-PDO (confirmed); MySQL-PDO, MySQLi, SQLite-PDO (works correctly)
**Related specs:** [SPEC-3.1](03-read-operations.ears.md)
**Tests:** `Pdo/PostgresJoinUsingSyntaxTest`

SELECT with `JOIN ... USING (col)` works correctly via `query()`, but when combined with `prepare()` and `$N` parameters in the WHERE clause, the query returns empty results. The same query with `JOIN ... ON t1.col = t2.col` syntax and prepared params works correctly. The USING keyword likely confuses parameter position tracking in the CTE rewriter.

Workaround: Use `JOIN ... ON t1.col = t2.col` instead of `USING (col)`.

Related: Issue #22 (HAVING with prepared params), Issue #62 (FILTER with prepared params).

## SPEC-11.SQLITE-UPSERT-SELFREF `[Issue #64]` ON CONFLICT DO UPDATE self-referencing expression loses value (SQLite)
**Status:** Known Issue
**Platforms:** SQLite-PDO (confirmed); MySQL has same class of bug (Issue #16)
**Related specs:** [SPEC-4.2a](04-write-operations.ears.md)
**Tests:** `Pdo/SqliteReplaceAndUpsertEdgeCasesTest`

`INSERT ... ON CONFLICT(id) DO UPDATE SET version = table.version + 1` evaluates the self-referencing expression as `0 + 1 = 0` instead of `original_value + 1`. The shadow store does not resolve the existing row's column value when computing the SET expression. Using `excluded.column` (the incoming value) works correctly.

This is the SQLite counterpart of MySQL Issue #16 (`ON DUPLICATE KEY UPDATE stock = stock + VALUES(stock)` loses original value). The root cause likely resides in the shared upsert mutation logic.

Workaround: Perform the read and update as separate operations.

## SPEC-11.UPDATE-PK-GHOST `[Issue #65]` UPDATE SET <pk_column> creates ghost row
**Status:** Known Issue
**Platforms:** MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Related specs:** [SPEC-4.3](04-write-operations.ears.md)
**Tests:** `Pdo/SqliteUpdatePrimaryKeyValueTest`, `Pdo/MysqlUpdatePrimaryKeyValueTest`, `Pdo/PostgresUpdatePrimaryKeyValueTest`

When `UPDATE SET id = new_value WHERE id = old_value` changes a primary key column, the shadow store creates a new row with the new PK but does not remove the old row. This results in ghost data: both old and new PK rows exist, `COUNT(*)` is inflated, and subsequent DELETE by new PK cannot clean up the ghost. Affects integer PKs, text PKs, and composite PK member changes.

## SPEC-11.PG-NOPK-JOIN `[Issue #66]` PostgreSQL: JOIN involving no-PK table fails
**Status:** Known Issue
**Platforms:** PostgreSQL-PDO
**Related specs:** [SPEC-3.3](03-read-operations.ears.md)
**Tests:** `Pdo/PostgresNoPrimaryKeyTableTest`

On PostgreSQL, JOIN between a table without a primary key and a table with a primary key fails with "ZTD Write Protection: Cannot determine columns SQL statement". The same JOIN works on MySQL and SQLite. INSERT and SELECT on no-PK tables work on all platforms.

## SPEC-11.SQLITE-STRING-LITERAL `[Issue #67]` SQLite: CTE rewriter replaces table references in string literals
**Status:** Known Issue
**Platforms:** SQLite-PDO
**Related specs:** [SPEC-3.1](03-read-operations.ears.md)
**Tests:** `Pdo/SqliteStringLiteralRewriteTest`, `Pdo/SqliteEdgeCaseSqlPatternsTest`

On SQLite, when a SQL string literal contains `FROM <tablename>` or `JOIN <tablename>` (case-insensitive), the CTE rewriter incorrectly treats it as a SQL clause and rewrites the table reference. This causes queries to silently return 0 rows. MySQL and PostgreSQL are not affected.

## SPEC-11.PG-DOLLAR-INSERT `[Issue #68]` PostgreSQL: INSERT with $N params stores NULL values
**Status:** Known Issue
**Platforms:** PostgreSQL-PDO
**Related specs:** [SPEC-4.1](04-write-operations.ears.md)
**Tests:** `Pdo/PostgresUnicodeAndSpecialCharsTest`

On PostgreSQL, INSERT via prepared statement using `$N` parameter syntax (PostgreSQL's native numbered placeholders) stores NULL for all column values in the shadow store. The row is created but all data is lost. Using `?` placeholders works correctly.

## SPEC-11.SQL-COMMENT-DML `[Issue #69]` SQL block comments break DML statement parsing
**Status:** Known Issue
**Platforms:** SQLite-PDO (severe), PostgreSQL-PDO (partial), MySQLi/MySQL-PDO (not affected)
**Related specs:** [SPEC-3.1](03-read-operations.ears.md), [SPEC-4.1](04-write-operations.ears.md)
**Tests:** `Pdo/SqliteSqlCommentRewriteTest`, `Pdo/SqliteCommentPositionTest`, `Pdo/SqliteSqlCommentHandlingTest`, `Pdo/MysqlSqlCommentRewriteTest`, `Pdo/PostgresSqlCommentRewriteTest`

SQL block comments (`/* ... */`) near SQL keywords break the CTE rewriter's SQL parser. The parser does not strip comments before identifying statement type and table references. This is a common pattern in ORM-generated SQL (e.g., Doctrine, Eloquent, DBAL query builders add `/* query name */` annotations).

**Failure modes by platform:**

**SQLite (systematic):**
- `SELECT * FROM /* comment */ table` → returns empty results (table ref not recognized)
- `SELECT ... JOIN /* comment */ table` → returns empty results
- `/* comment */ UPDATE table SET ...` → "Cannot resolve UPDATE target SQL statement"
- `UPDATE /* comment */ table SET ...` → "no such table: /*" (CTE rewriter treats `/*` as table name)
- `INSERT INTO /* comment */ table VALUES ...` → "Cannot determine columns SQL statement"
- `DELETE FROM /* comment */ table WHERE ...` → **DELETE silently ignored** (no error, no rows deleted)

**PostgreSQL (partial):**
- `UPDATE /* comment */ table SET ...` → "Cannot resolve UPDATE target SQL statement"
- `DELETE FROM /* comment */ table WHERE ...` → "Cannot resolve DELETE target SQL statement"
- SELECT with comments and leading comments on DML work correctly.

**MySQL:** All comment patterns work correctly.

The most dangerous failure is SQLite DELETE with comment between FROM and table name: the operation silently does nothing with no error, meaning data that should be deleted is retained.

Workaround: strip all SQL comments before passing queries to ZTD.

## SPEC-11.DERIVED-TABLE-ALIAS-COLLISION `[Issue #13]` Derived table aliased with existing table name returns empty
**Status:** Known Issue
**Platforms:** SQLite-PDO (confirmed)
**Related specs:** [SPEC-3.3](03-read-operations.ears.md)
**Tests:** `Pdo/SqliteTableAliasConfusionTest`

When a derived table (subquery in FROM clause) is aliased with the name of an actual table that has shadow data, the query returns empty results. The CTE rewriter likely confuses the alias with a physical table reference.

```sql
-- Returns empty on SQLite:
SELECT tac_items.name, tac_items.total_orders
FROM (
    SELECT c.name, COUNT(o.id) AS total_orders
    FROM tac_customers c LEFT JOIN tac_orders o ON o.customer_id = c.id
    GROUP BY c.name
) tac_items
ORDER BY tac_items.name
```

Workaround: use aliases that do not match any table name in the schema.

## SPEC-11.LAST-INSERT-ID `[Issue #77]` PDO::lastInsertId() returns '0' after shadow INSERT
**Status:** Known Issue
**Platforms:** SQLite-PDO (likely all PDO platforms)
**Related specs:** [SPEC-4.7](04-write-operations.ears.md)
**Tests:** `Pdo/SqliteLastInsertIdShadowTest`

`PDO::lastInsertId()` always returns `'0'` after INSERT operations in ZTD mode. Shadow INSERTs don't execute on the physical database, so the underlying PDO's `last_insert_rowid()` is never updated. This breaks ORM workflows (Laravel, Doctrine) that rely on `lastInsertId()` to retrieve auto-generated primary keys.

Affects: exec INSERT, prepared INSERT, explicit PK INSERT. The shadow store tracks the inserted row internally but does not propagate the ID to `lastInsertId()`.

## SPEC-11.MULTI-STATEMENT `[Issue #78]` Multi-statement SQL throws undocumented error
**Status:** Known Issue
**Platforms:** SQLite-PDO (likely all platforms)
**Related specs:** [SPEC-6.1](06-unsupported-sql.ears.md)
**Tests:** `Pdo/SqliteMultiStatementExecTest`

Executing multiple SQL statements separated by semicolons in a single `exec()` call throws `ZTD Write Protection: Multi-statement SQL statement`. While this may be intentional, the limitation is undocumented and the error message does not suggest a workaround. Native `PDO::exec()` supports multi-statement execution on SQLite.

## SPEC-11.DML-TABLE-ALIAS `[Issue #79]` SQLite: Table alias in UPDATE/DELETE causes "no such column" error
**Status:** Known Issue
**Platforms:** SQLite-PDO
**Related specs:** [SPEC-4.2](04-write-operations.ears.md), [SPEC-4.3](04-write-operations.ears.md)
**Tests:** `Pdo/SqliteTableAliasInDmlTest`, `Pdo/MysqlTableAliasInDmlTest`, `Pdo/PostgresTableAliasInDmlTest`, `Mysqli/TableAliasInDmlTest`

On SQLite, UPDATE and DELETE statements with table aliases (`UPDATE t AS alias SET ... WHERE alias.col = ...` or `DELETE FROM t AS alias WHERE alias.col = ...`) fail with "no such column: alias.col". The CTE rewriter wraps the table in a CTE but does not preserve the alias, so alias-qualified column references become unresolvable. This syntax is supported natively by SQLite (3.33+) and is commonly emitted by ORMs. MySQL and PostgreSQL are not affected. Workaround: use unqualified column names (`WHERE col = ...` instead of `WHERE alias.col = ...`).

## SPEC-11.NULLIF-PREPARED-PARAM `[Issue #80]` NULLIF with prepared parameter returns wrong results
**Status:** Known Issue
**Platforms:** SQLite-PDO (confirmed), likely MySQL-PDO, PostgreSQL-PDO
**Related specs:** [SPEC-3.1](03-read-operations.ears.md), [SPEC-3.2](03-read-operations.ears.md)
**Tests:** `Pdo/SqliteCoalesceWithParamsTest`, `Pdo/MysqlNullifWithParamsTest`, `Pdo/PostgresNullifWithParamsTest`

`NULLIF(column, ?)` with a prepared parameter returns incorrect results. For example, `SELECT name FROM t WHERE NULLIF(score, ?) IS NULL` with param `100` should match rows where `score = 100` (NULLIF returns NULL) AND rows where `score IS NULL`. Through ZTD, only the `score IS NULL` rows are returned — the parameter inside NULLIF is not properly evaluated, so `NULLIF(100, 100)` does not return NULL as expected. The same query via `query()` (without parameters) works correctly. This is related to but distinct from Issue #75 (CASE in WHERE with params).

## SPEC-11.VIEW-JOIN-SHADOW `View JOIN with shadow table produces inconsistent results
**Status:** Known Limitation
**Platforms:** SQLite-PDO (confirmed), likely all platforms
**Related specs:** [SPEC-3.3b](03-read-operations.ears.md)
**Tests:** `Pdo/SqliteViewJoinShadowTest`

When a query JOINs a view with a shadow-modified table, the results are inconsistent: the view reads physical data while the joined table reads shadow data. For example, after shadow-inserting a row into the base table, a `SELECT ... FROM view JOIN table` does not include the shadow-inserted row in the join result. After shadow-deleting a row, the deleted row still appears in the join because the view sees the physical (undeleted) row. This is a consequence of views not being CTE-rewritten, but the JOIN inconsistency may silently produce incorrect results in applications that combine views with DML-modified tables.

## SPEC-11.JSON-TABLE-FUNCTION `[Issue #81]` SQLite: json_each()/json_tree() table-valued functions return empty results
**Status:** Known Issue
**Platforms:** SQLite-PDO
**Related specs:** [SPEC-3.1](03-read-operations.ears.md), [SPEC-3.3](03-read-operations.ears.md)
**Tests:** `Pdo/SqliteJsonTableFunctionTest`

SQLite table-valued functions `json_each()` and `json_tree()` return zero rows when used on shadow-stored data. These functions appear in the FROM clause like tables (e.g., `FROM products p, json_each(p.tags) j`) but the CTE rewriter does not handle them. All query patterns are affected: SELECT, JOIN, WHERE filter, aggregation, and prepared statements. This is distinct from #13 (derived tables) — table-valued functions have different syntax from subqueries.

## SPEC-11.LINE-COMMENT-DML `[Issue #82]` Line comments (--) break CTE rewriter for UPDATE and SELECT
**Status:** Known Issue
**Platforms:** SQLite-PDO, likely PostgreSQL-PDO
**Related specs:** [SPEC-3.1](03-read-operations.ears.md), [SPEC-4.2](04-write-operations.ears.md)
**Tests:** `Pdo/SqliteLineCommentNearKeywordTest`

SQL line comments (`--`) break the CTE rewriter in two cases: (1) a line comment immediately before UPDATE causes "Cannot resolve UPDATE target SQL statement" error; (2) a line comment containing SQL keywords like `SELECT`, `FROM`, `DELETE` causes the following actual query to return 0 rows. Line comments before SELECT, INSERT, and DELETE, and between clauses, work correctly. Related to #69 (block comments break parser) but affects `--` syntax rather than `/* */`.

## SPEC-11.INSERT-SELECT-LITERAL-NULLS `[Issue #83]` INSERT...SELECT with literal values stores NULLs (SQLite/PostgreSQL)
**Status:** Known Issue
**Platforms:** SQLite-PDO, PostgreSQL-PDO (MySQL not affected)
**Related specs:** [SPEC-4.1](04-write-operations.ears.md), [SPEC-4.1a](04-write-operations.ears.md)
**Tests:** `Pdo/SqliteConditionalInsertNotExistsTest`, `Pdo/PostgresConditionalInsertNotExistsTest`, `Pdo/MysqlConditionalInsertNotExistsTest`

`INSERT INTO t (cols) SELECT literal_values WHERE [NOT] EXISTS (...)` stores NULLs instead of the intended values on SQLite and PostgreSQL. This breaks the common portable conditional-insert pattern. Consequences: (1) NOT EXISTS checks fail to find the "existing" row because the stored value is NULL; (2) sequential conditional inserts create duplicates; (3) cross-table conditional inserts store NULLs. MySQL works correctly. Related to #20 (INSERT...SELECT with computed columns produces NULLs).

## SPEC-11.PG-FORMAT-UPDATE-SET `[Issue #84]` PostgreSQL: format() in UPDATE SET produces NULL
**Status:** Known Issue
**Platforms:** PostgreSQL-PDO
**Related specs:** [SPEC-4.2](04-write-operations.ears.md)
**Tests:** `Pdo/PostgresFormatFunctionTest`

PostgreSQL's `format()` function works correctly in SELECT, WHERE, and prepared-statement contexts, but produces NULL when used in `UPDATE SET`. For example, `UPDATE users SET code = format('USR-%s', first_name) WHERE code IS NULL` sets `code` to NULL instead of the expected formatted string. This is distinct from #47 (which is about FROM keyword in TRIM/SUBSTRING/EXTRACT syntax) and similar to the pattern in #61 (CASE in UPDATE SET).

## SPEC-11.PG-DOLLAR-SELECT `[Issue #85]` PostgreSQL: SELECT with $N prepared params returns empty results
**Status:** Known Issue
**Platforms:** PostgreSQL-PDO
**Related specs:** [SPEC-3.2](03-read-operations.ears.md)
**Tests:** `Pdo/PostgresDollarParamSelectTest`, `Pdo/PostgresBetweenWithParamsTest`, `Pdo/PostgresGroupByCaseExpressionTest`, `Pdo/PostgresTypeCastOperatorTest`, `Pdo/PostgresStringAggTest`, `Pdo/PostgresUnionAllShadowTablesTest`, `Pdo/PostgresReplaceFunctionInQueryTest`

The most basic SELECT pattern — `SELECT name FROM t WHERE id = $1` — returns empty results through the CTE rewriter on PostgreSQL, even when the shadow store contains matching data. The same query using `?` placeholders returns correct results. This affects ALL SELECT queries using PostgreSQL's native `$N` parameter syntax:

- `WHERE col = $1` — basic equality
- `WHERE col BETWEEN $1 AND $2` — range queries
- `WHERE col > $1 AND col2 = $2` — multi-condition
- `GROUP BY CASE WHEN col >= $1 THEN ... END` — expression grouping (returns wrong groups)
- `UNION ALL ... WHERE col > $1` — set operations
- `REPLACE(col, $1, $2)` in UPDATE SET — string functions with params

This broadens the scope of known $N param issues (#62 FILTER, #63 JOIN USING, #68 INSERT) to all SELECT contexts. The root cause is likely in the CTE rewriter's parameter handling during query transformation.

Workaround: Use `?` placeholders instead of `$N` on PostgreSQL.

## SPEC-11.KEYWORD-TABLE-NAME `[Issue #86]` Table names "select" and "values" break CTE rewriter
**Status:** Known Issue
**Platforms:** SQLite-PDO (confirmed), likely MySQL-PDO, PostgreSQL-PDO
**Related specs:** [SPEC-3.1](03-read-operations.ears.md), [SPEC-4.1](04-write-operations.ears.md)
**Tests:** `Pdo/SqliteKeywordTableNameProbeTest`, `Pdo/SqliteReservedWordTableCrudTest`

Tables whose names are SQL statement keywords `SELECT` or `VALUES` (quoted with double-quotes) cause all DML operations to fail through the CTE rewriter. Other reserved words (`order`, `group`, `user`, `insert`, `update`, `delete`, `from`, `where`, `table`, `index`, `create`) work correctly when quoted.

**`"select"` table:** `INSERT INTO "select" (id, val) VALUES (1, 'test')` fails with `unrecognized token`. The SQL parser strips the quoted table name and misinterprets the remaining SQL.

**`"values"` table:** `INSERT INTO "values" (id, val) VALUES (1, 'test')` fails with `Insert statement has no values to project`. The parser confuses the quoted table name `"values"` with the VALUES keyword in INSERT syntax.

Both keywords that fail are used in INSERT statement parsing (`SELECT` for INSERT...SELECT, `VALUES` for INSERT...VALUES). Other DML keywords as table names work because they are parsed at the statement-type level, not within INSERT clause parsing.

Workaround: Rename tables to avoid `select` and `values` as table names, or disable ZTD for queries referencing these tables.

## SPEC-11.PREPARED-SELECT-REEXECUTE-STALE `[Issue #87]` Prepared SELECT re-execution returns stale shadow data
**Status:** Known Issue
**Platforms:** SQLite-PDO, MySQL-PDO, PostgreSQL-PDO, MySQLi (all confirmed)
**Related specs:** [SPEC-3.2](03-read-operations.ears.md)
**Tests:** `Pdo/SqlitePreparedSelectReexecuteStaleTest`, `Pdo/MysqlPreparedSelectReexecuteStaleTest`, `Pdo/PostgresPreparedSelectReexecuteStaleTest`, `Mysqli/MysqliPreparedSelectReexecuteStaleTest`, `Pdo/SqliteCloseCursorReexecuteTest`

When a prepared SELECT statement is re-executed after intervening DML operations (INSERT, UPDATE, DELETE) via other statements, the re-executed SELECT returns stale results from the first execution instead of reflecting the shadow store mutations.

Fresh `query()` calls correctly reflect mutations. Only re-executed prepared SELECT statements are affected. The issue occurs regardless of whether `closeCursor()` is called between executions. DML prepared re-execution (INSERT/UPDATE/DELETE) is not affected.

Root cause: The CTE-rewritten SQL is baked into the inner PDO prepared statement at first `execute()` time. On subsequent `execute()` calls, the shadow store CTE data is not regenerated.

Workaround: Use `query()` instead of re-executing a prepared SELECT, or prepare a new statement for each execution after DML mutations.

## SPEC-11.PG-DOLLAR-QUOTING `[Issue #88]` Dollar-quoted strings break CTE rewriter (PostgreSQL)
**Status:** Known Issue
**Platforms:** PostgreSQL-PDO
**Related specs:** [SPEC-3.1](03-read-operations.ears.md), [SPEC-4.1](04-write-operations.ears.md)
**Tests:** `Pdo/PostgresDollarQuotingTest`, `Pdo/PostgresStringLiteralTableNameTest`

PostgreSQL dollar-quoted string constants (`$$...$$` and `$tag$...$tag$`) are not recognized by the CTE rewriter. The rewriter strips the `$$` delimiters and attempts to rewrite table name references found inside the string literal, producing invalid SQL.

Affected patterns:
- `UPDATE t SET col = $$text containing t reference$$` — syntax error
- `INSERT INTO t VALUES ($$text$$)` — syntax error
- `SELECT ... WHERE col = $$value$$` — syntax error on some patterns
- `$tag$...$tag$` tagged dollar-quoting — same issue
- `$$$$` empty dollar-quoted string — syntax error

Root cause: The `stripStringLiterals` method in the CTE rewriter does not handle PostgreSQL dollar-quoting syntax. It only strips single-quoted strings, so dollar-quoted content is parsed as SQL tokens.

Workaround: Use standard single-quoted strings with doubled single-quote escaping (`''`) instead of dollar-quoting.

## SPEC-11.PG-ESCAPE-STRING `[Issue #89]` E-string escape syntax breaks CTE rewriter (PostgreSQL)
**Status:** Known Issue
**Platforms:** PostgreSQL-PDO
**Related specs:** [SPEC-3.1](03-read-operations.ears.md), [SPEC-4.1](04-write-operations.ears.md)
**Tests:** `Pdo/PostgresEscapeStringTest`, `Pdo/PostgresStringLiteralTableNameTest`

PostgreSQL escape string syntax (`E'...'`) is not recognized by the CTE rewriter. The `E` prefix is treated as a separate identifier token, causing the rewriter to produce SQL like `WHERE E id = 1` instead of correctly handling the escape string as a string literal.

Affected patterns:
- `UPDATE t SET col = E'text\nwith\tescapes' WHERE id = 1` — `E` becomes stray token
- `INSERT INTO t VALUES (E'escaped\\string')` — may produce syntax error
- Any DML using E-string syntax in SET or VALUES clauses

The issue only affects DML statements (INSERT, UPDATE, DELETE) that go through CTE rewriting. SELECT queries with E-strings in WHERE clauses appear to work because the E-string is on the comparison side rather than being rewritten.

Root cause: The CTE rewriter's string literal stripping does not account for the `E` prefix on PostgreSQL escape strings.

Workaround: Use standard single-quoted strings with PostgreSQL's default `standard_conforming_strings = on` setting, or use prepared statement parameters instead of E-string literals.

## SPEC-11.PG-BIT-TYPE `[Issue #90]` BIT type values corrupted in shadow store (PostgreSQL)
**Status:** Known Issue
**Platforms:** PostgreSQL-PDO
**Related specs:** [SPEC-3.1](03-read-operations.ears.md), [SPEC-4.1](04-write-operations.ears.md)
**Tests:** `Pdo/PostgresBitTypeTest`, `Pdo/PostgresStringPrefixTest`

PostgreSQL BIT and BIT VARYING type values are silently corrupted when stored in the CTE shadow store. Bit string literals like `B'11111111'` are converted to boolean-like integers (`1` or `0`) instead of preserving the 8-bit pattern.

Impact:
- `B'11111111'` becomes `1`, `B'00001111'` becomes `0`, `B'00000001'` becomes `0`
- WHERE comparisons with BIT literals return 0 rows (shadow store has `1` not `11111111`)
- BIT operations (`&`, `|`) fail with "cannot AND bit strings of different sizes"
- All BIT(n) and BIT VARYING(n) columns are affected

Root cause: The CTE shadow store's CAST logic likely maps BIT types to INTEGER or BOOLEAN, losing the bit string representation.

Workaround: Store BIT values as TEXT and cast at query time, or avoid BIT columns with ZTD enabled.

## SPEC-11.INSERT-IGNORE-UNIQUE `[Issue #91]` INSERT IGNORE / ON CONFLICT DO NOTHING does not enforce non-PK UNIQUE constraints
**Status:** Known Issue
**Platforms:** MySQLi (confirmed), MySQL-PDO (confirmed), PostgreSQL-PDO (confirmed), SQLite-PDO (confirmed)
**Related specs:** [SPEC-4.2e](04-write-operations.ears.md), [SPEC-8.1](08-constraints.ears.md)
**Tests:** `Mysqli/InsertIgnoreTest`, `Pdo/MysqlInsertIgnoreTest`, `Pdo/SqliteDeleteReinsertPkCycleTest`

INSERT IGNORE (MySQL), INSERT OR IGNORE (SQLite), and INSERT...ON CONFLICT (col) DO NOTHING (PostgreSQL) only check for primary key duplicates in the shadow store. Non-PK UNIQUE key constraint violations are silently inserted as duplicate rows, causing data integrity violations. The shadow store does not reflect or enforce UNIQUE constraints from the DDL.

- Multi-row INSERT IGNORE with UNIQUE duplicates inserts all rows instead of skipping conflicts.
- Affects exec() and prepared statement paths.
- Common pattern in idempotent insert workflows.

Workaround: Check for existing rows via SELECT before inserting, or use a different deduplication strategy at the application level.

## SPEC-11.ENUM-ORDERING `[Issue #92]` MySQL ENUM column type loses internal index ordering in CTE shadow store
**Status:** Known Issue
**Platforms:** MySQLi (confirmed), MySQL-PDO (confirmed)
**Related specs:** [SPEC-3.1](03-read-operations.ears.md), [SPEC-4.1](04-write-operations.ears.md)
**Tests:** `Mysqli/EnumTypeTest`, `Pdo/MysqlEnumTypeTest`

MySQL ENUM columns lose their internal index semantics when stored in the CTE shadow store. The CTE rewriter casts ENUM values to VARCHAR/TEXT, causing ORDER BY to use alphabetical ordering instead of the ENUM definition order. For example, `ENUM('small', 'medium', 'large')` sorted ASC produces `large, medium, small` (alphabetical) instead of `small, medium, large` (definition order).

Additional failures:
- Comparison operators (`>`, `<`, `>=`, `<=`) use alphabetical instead of internal index
- GROUP BY ENUM produces wrong group ordering
- DEFAULT ENUM values not applied (returns NULL instead of declared DEFAULT)

Workaround: Use an integer column with application-level mapping instead of ENUM, or add an explicit sort column.

## SPEC-11.HEX-LITERAL-UPDATE `[Issue #93]` X'...' hex literal in UPDATE SET parsed as column name
**Status:** Known Issue
**Platforms:** MySQLi (confirmed), MySQL-PDO (likely affected)
**Related specs:** [SPEC-4.2](04-write-operations.ears.md)
**Tests:** `Mysqli/HexLiteralTest`

The CTE rewriter's SQL parser treats `X'...'` hex literal syntax in UPDATE SET clauses as a column identifier `X` followed by a string literal, producing "Unknown column 'X' in 'field list'" error. INSERT with `X'...'` and the `0x...` prefix syntax work correctly in all contexts.

Workaround: Use `0x...` prefix syntax instead of `X'...'`, or use prepared statements with binary parameters.

## SPEC-11.UPDATE-WHERE-ORDER-DESC-LIMIT `[Issue #94]` UPDATE WHERE + ORDER BY DESC + LIMIT updates wrong row
**Status:** Known Issue
**Platforms:** MySQL-PDO (confirmed), MySQLi (likely affected)
**Related specs:** [SPEC-4.2](04-write-operations.ears.md)
**Tests:** `Pdo/MysqlDeleteLimitTest`

`UPDATE ... SET ... WHERE ... ORDER BY col DESC LIMIT 1` does not respect the DESC ordering. Instead of updating the row with the highest value (as DESC ordering specifies), it updates a row from the start of the natural table order. Basic DELETE/UPDATE with ORDER BY LIMIT (without WHERE+DESC combination) works correctly.

Workaround: Query the target row ID first via SELECT with ORDER BY DESC LIMIT, then UPDATE by explicit ID.

## SPEC-11.SQLITE-NESTED-FUNC-PARAMS `[Issue #95]` SQLite: Nested function WHERE expressions with prepared params return empty
**Status:** Known Issue
**Platforms:** SQLite-PDO (confirmed); MySQL-PDO, PostgreSQL-PDO NOT affected
**Related specs:** [SPEC-3.2](03-read-operations.ears.md)
**Tests:** `Pdo/SqliteNestedFunctionWhereTest`

On SQLite, WHERE clauses with nested function expressions that include prepared statement parameters return 0 rows, even when matching rows exist. The same queries work correctly via `query()` with literal values. The CTE rewriter appears to mishandle parameter binding positions when nested function calls precede the `?` placeholders.

Affected patterns:
- `WHERE LENGTH(REPLACE(col, 'a', '')) > ?` — 0 rows
- `WHERE ABS(col1 - col2) BETWEEN ? AND ?` — 0 rows
- Combined nested functions with multiple params — 0 rows

Non-nested function patterns and single-nesting without params work correctly.

**Additional finding:** This issue also affects prepared DELETE statements. `DELETE FROM t WHERE score > ? AND LENGTH(name) > ?` with params deletes 0 rows on SQLite, while the non-prepared (exec) version works correctly.

Related: Issue #22 (HAVING with prepared params), Issue #75 (CASE with prepared params), Issue #80 (NULLIF with prepared param).

## SPEC-11.MYSQL-CASE-WHERE-DML `[Issue #96]` MySQL: DELETE/UPDATE with CASE in WHERE matches ALL rows
**Status:** Known Issue
**Platforms:** MySQLi, MySQL-PDO
**Related specs:** [SPEC-4.2](04-write-operations.ears.md), [SPEC-4.3](04-write-operations.ears.md)
**Tests:** `Mysqli/ExpressionWhereClauseDmlTest`, `Pdo/MysqlExpressionWhereClauseDmlTest`

On MySQL (both MySQLi and PDO adapters), DELETE and UPDATE statements with a CASE expression in the WHERE clause incorrectly match ALL rows instead of only the rows where the CASE evaluates to the matching value. The shadow store's DELETE/UPDATE resolver appears to ignore the CASE expression entirely, treating it as if all rows match.

- `DELETE FROM t WHERE CASE WHEN score > 80 THEN 1 ELSE 0 END = 1` — deletes ALL rows (expected: only rows with score > 80)
- `UPDATE t SET score = 0 WHERE CASE WHEN score > 80 THEN 1 ELSE 0 END = 1` — updates ALL rows (expected: only rows with score > 80)
- `SELECT * FROM t WHERE CASE WHEN score > 80 THEN 1 ELSE 0 END = 1` — works correctly (returns only matching rows)

PostgreSQL and SQLite are NOT affected. Native MySQL (without ZTD) handles CASE in WHERE correctly for all operations.

## SPEC-11.INSERT-DEFAULT-VALUES `[Issue #97]` INSERT DEFAULT VALUES / INSERT () VALUES () not supported
**Status:** Known Issue
**Platforms:** MySQL-PDO, MySQLi, PostgreSQL-PDO, SQLite-PDO
**Related specs:** [SPEC-4.1](04-write-operations.ears.md)
**Tests:** `Pdo/MysqlInsertDefaultValuesTest`, `Pdo/PostgresInsertDefaultValuesTest`, `Pdo/SqliteInsertDefaultValuesTest`, `Mysqli/InsertDefaultValuesTest`

The CTE rewriter cannot handle INSERT statements with no explicit values:

- **PostgreSQL**: `INSERT INTO t DEFAULT VALUES` → "ZTD Write Protection: Cannot extract INSERT values SQL statement."
- **SQLite**: `INSERT INTO t DEFAULT VALUES` → "Insert statement has no values to project."
- **MySQL**: `INSERT INTO t () VALUES ()` → "Insert values count does not match column count."

Additionally, on MySQL the `DEFAULT` keyword in VALUES (`INSERT INTO t (col) VALUES (DEFAULT)`) generates invalid rewritten SQL (`DEFAULT AS column`).

These are standard SQL patterns used by ORMs when inserting rows where all columns have defaults.

## SPEC-11.CASE-EXISTS-WHERE-MULTISTATEMENT `[Issue #98]` MySQL: SELECT with CASE WHEN EXISTS in WHERE misdetected as multi-statement
**Status:** Known Issue
**Platforms:** MySQL-PDO, MySQLi
**Related specs:** [SPEC-3.1](03-read-operations.ears.md)
**Tests:** `Pdo/MysqlCaseExistsSubqueryTest`, `Mysqli/CaseExistsSubqueryTest`

On MySQL (PDO and MySQLi), a SELECT with `CASE WHEN EXISTS (subquery) THEN ... END` in the WHERE clause is rejected with "ZTD Write Protection: Multi-statement SQL statement." The CTE rewriter's statement boundary detection confuses the nested subquery or the `END` keyword with a statement separator.

- `SELECT u.id FROM users u WHERE CASE WHEN EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id) THEN 1 ELSE 0 END = 1` → multi-statement error
- The same `CASE WHEN EXISTS` pattern works correctly in the SELECT list on all platforms.
- PostgreSQL and SQLite are NOT affected.

## SPEC-11.INSERT-SELECT-DISTINCT `[Issue #99]` INSERT...SELECT DISTINCT ignores DISTINCT keyword
**Status:** Known Issue
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Related specs:** [SPEC-4.1](04-write-operations.ears.md)
**Tests:** `Mysqli/InsertSelectDistinctTest`, `Pdo/MysqlInsertSelectDistinctTest`, `Pdo/PostgresInsertSelectDistinctTest`, `Pdo/SqliteInsertSelectDistinctTest`

`INSERT INTO t2 SELECT DISTINCT col FROM t1` through ZTD ignores the DISTINCT keyword. On MySQL, all rows are inserted instead of unique rows. On PostgreSQL and SQLite, NULL values are stored instead of actual column values.

- MySQL: 5 rows inserted instead of 3 distinct (DISTINCT completely ignored)
- PostgreSQL: correct row count but `name` column is NULL
- SQLite: correct row count but `name` column is NULL
- The same issue affects `INSERT...SELECT DISTINCT ... WHERE` and `INSERT...SELECT DISTINCT ON (...)` (PostgreSQL)
- Workaround: use `GROUP BY` instead of `DISTINCT`

## SPEC-11.PG-CROSS-TABLE-SUBQUERY-DML `[Issue #100]` PostgreSQL: UPDATE/DELETE with IN(subquery on different table) syntax error
**Status:** Known Issue
**Platforms:** PostgreSQL-PDO
**Related specs:** [SPEC-4.3](04-write-operations.ears.md), [SPEC-4.5](04-write-operations.ears.md)
**Tests:** `Pdo/PostgresCrossTableShadowDeleteTest`, `Pdo/PostgresAnyAllSubqueryComparisonTest`

On PostgreSQL, UPDATE or DELETE with `WHERE col IN (SELECT ... FROM other_table)` where `other_table` is a different table produces a syntax error. The CTE rewriter incorrectly adds the subquery's table reference to the outer statement's FROM clause, generating invalid SQL like `FROM "target_table", other_table)`.

- Distinct from Issue #74 (self-referencing subqueries on same table)
- Also affects `= ANY (SELECT ... FROM other_table)` in UPDATE WHERE clause
- MySQL and SQLite handle these patterns correctly

## SPEC-11.SQLITE-PREPARED-DELETE-EXPR-WHERE `[Issue #101]` SQLite: Prepared DELETE with arithmetic expression in WHERE deletes all rows
**Status:** Known Issue
**Platforms:** SQLite-PDO
**Related specs:** [SPEC-4.5](04-write-operations.ears.md)
**Tests:** `Pdo/SqlitePreparedExpressionDmlTest`

On SQLite, a prepared DELETE with an arithmetic expression in WHERE (e.g., `DELETE FROM t WHERE price * quantity < ?`) deletes ALL rows instead of only matching ones. The prepared parameter combined with the arithmetic expression causes the WHERE condition to match every row.

## SPEC-11.DERIVED-TABLE-JOIN `[Issue #102]` Derived table with multi-table JOIN returns empty
**Status:** Known Issue
**Platforms:** MySQLi (confirmed), MySQL-PDO (confirmed), SQLite-PDO (confirmed); PostgreSQL-PDO (works correctly)
**Related specs:** [SPEC-3.3](03-read-operations.ears.md), [SPEC-10.2.213](10-platform-notes.ears.md)
**Tests:** `Mysqli/DerivedTableAndNestingTest`, `Pdo/MysqlDerivedTableAndNestingTest`, `Pdo/SqliteDerivedTableAndNestingTest`, `Pdo/PostgresDerivedTableAndNestingTest`

A SELECT from a derived table (subquery in FROM) containing a multi-table JOIN with GROUP BY returns 0 rows on MySQL (MySQLi, MySQL-PDO) and SQLite-PDO, even when using `query()` (not `prepare()`). PostgreSQL handles the same query correctly.

This extends the derived table family of issues (Issue #13) beyond window functions (SPEC-11.WINDOW-DERIVED), UNION ALL (SPEC-11.UNION-ALL-DERIVED), and prepared statements (SPEC-11.DERIVED-TABLE-PREPARED). Unlike SPEC-11.WINDOW-DERIVED which affects all platforms, this JOIN variant works on PostgreSQL.

```sql
-- Returns empty on MySQL and SQLite:
SELECT sub.name, sub.total
FROM (
    SELECT p.name, SUM(s.qty) AS total
    FROM products p
    JOIN sales s ON s.product_id = p.id
    GROUP BY p.id, p.name
) sub
ORDER BY sub.total DESC;

-- Workaround: use top-level JOIN with GROUP BY instead of derived table:
SELECT p.name, SUM(s.qty) AS total
FROM products p
JOIN sales s ON s.product_id = p.id
GROUP BY p.id, p.name
ORDER BY total DESC;
```

Non-derived-table patterns with JOINs work correctly on all platforms: top-level JOINs, three-table JOIN chains, self-JOINs, CROSS JOINs, JOINs with aggregates and HAVING. Only the derived table wrapper causes the issue.

## SPEC-11.INSERT-SELECT-JOIN-ALIAS `[Issue #49]` INSERT...SELECT with JOIN produces errors or NULL columns
**Status:** Known Issue (extended)
**Platforms:** MySQLi (error), MySQL-PDO (error), PostgreSQL-PDO (NULL columns), SQLite-PDO (NULL columns)
**Related specs:** [SPEC-4.1a](04-write-operations.ears.md), [SPEC-10.2.214](10-platform-notes.ears.md)
**Tests:** `Mysqli/MultiTableDmlPatternsTest`, `Pdo/MysqlMultiTableDmlPatternsTest`, `Pdo/PostgresMultiTableDmlPatternsTest`, `Pdo/SqliteMultiTableDmlPatternsTest`

`INSERT INTO t SELECT ... FROM t1 JOIN t2 ON ... WHERE t2.col = value` fails across all platforms. On MySQL, the InsertTransformer cannot resolve column references from JOINed table aliases (`Unknown column 'w.active' in 'where clause'`). On PostgreSQL and SQLite, 0 rows are inserted. This confirms and extends SPEC-11.INSERT-SELECT-JOIN [Issue #49] with a different JOIN pattern (inventory + warehouse lookup).

## SPEC-11.MYSQL-INSERT-SELECT-UNION `[Issue #103]` MySQL: INSERT...SELECT with UNION/UNION ALL rejected as multi-statement
**Status:** Known Issue
**Platforms:** MySQLi, MySQL-PDO
**Related specs:** [SPEC-4.1](04-write-operations.ears.md)
**Tests:** `Mysqli/InsertFromUnionTest`, `Pdo/MysqlInsertFromUnionTest`

`INSERT INTO t SELECT ... FROM t1 UNION ALL SELECT ... FROM t2` is rejected with "ZTD Write Protection: Multi-statement SQL statement." on MySQL (both MySQLi and MySQL-PDO). The CTE rewriter's statement boundary detection confuses the UNION keyword within the INSERT...SELECT as a statement separator. All variants are affected: UNION ALL, UNION (distinct), and prepared statements.

- **SQLite**: Works correctly — all rows inserted, shadow store updated
- **PostgreSQL**: Works correctly — all rows inserted, shadow store updated
- Related to Issue #14 (EXCEPT/INTERSECT as multi-statement) — same parser limitation
- Workaround: perform separate `INSERT...SELECT` statements for each source table

## SPEC-11.MYSQL-UPDATE-JOIN-DERIVED `[Issue #104]` MySQL: UPDATE JOIN with derived table treats subquery as identifier
**Status:** Known Issue
**Platforms:** MySQLi (confirmed), MySQL-PDO (confirmed)
**Related specs:** [SPEC-4.2](04-write-operations.ears.md)
**Tests:** `Mysqli/MultiSubqueryUpdateSetTest`, `Pdo/MysqlUpdateJoinPatternTest`, `Mysqli/WindowFunctionDmlTest`, `Pdo/MysqlWindowFunctionDmlTest`

MySQL `UPDATE t1 JOIN (SELECT ... FROM t2 GROUP BY ...) alias ON ... SET t1.col = alias.col` fails with "Identifier name '(SELECT ... FROM ...' is too long". The CTE rewriter parses the entire derived table subquery as a table identifier/name rather than recognizing it as a subquery in the JOIN clause.

- UPDATE JOIN with a **direct table** works correctly: `UPDATE t1 JOIN t2 ON ... SET t1.col = t2.col`
- Prepared UPDATE JOIN with direct table + params also works correctly
- Only the **derived table (subquery in JOIN)** form is affected
- Window function subqueries in JOIN also trigger this: `UPDATE r JOIN (SELECT player, DENSE_RANK() OVER (...) FROM scores GROUP BY player) s ON ... SET r.rank_pos = s.drank` — same "Identifier name too long" error. Tests: `Mysqli/WindowFunctionDmlTest`, `Pdo/MysqlWindowFunctionDmlTest`. [Issue #115]
- Related to Issue #44 (comma-syntax multi-table UPDATE) — the comma syntax recommends "Prefer JOIN syntax" as a workaround, but JOIN with derived tables is also broken
- Related to Issue #102 (derived table with JOIN returns empty in SELECT) — same class of derived-table parsing issue but different statement type

```sql
-- Fails on MySQL (both MySQLi and PDO):
UPDATE summary s
JOIN (
    SELECT category, COUNT(*) AS cnt, MIN(price) AS mn
    FROM products GROUP BY category
) p ON s.category = p.category
SET s.min_price = p.mn, s.item_count = p.cnt;

-- Workaround: use multiple correlated subqueries in SET (works on MySQL):
UPDATE summary SET
    min_price = (SELECT MIN(price) FROM products WHERE category = summary.category),
    item_count = (SELECT COUNT(*) FROM products WHERE category = summary.category);
```

## SPEC-11.UPSERT-SUBQUERY-SET `[Issue #105]` UPSERT with subquery in SET evaluates incorrectly (all platforms)
**Status:** Known Issue
**Platforms:** MySQLi (confirmed), MySQL-PDO (confirmed), PostgreSQL-PDO (confirmed), SQLite-PDO (confirmed)
**Related specs:** [SPEC-4.2a](04-write-operations.ears.md)
**Tests:** `Pdo/MysqlUpsertSubqueryInSetTest`, `Mysqli/UpsertSubqueryInSetTest`, `Pdo/PostgresUpsertSubqueryInSetTest`, `Pdo/SqliteUpsertSubqueryInSetTest`

INSERT with upsert clause (ON DUPLICATE KEY UPDATE on MySQL, ON CONFLICT DO UPDATE on PostgreSQL/SQLite) containing a subquery in the SET expression fails on all platforms:

- **MySQL/SQLite exec()**: subquery evaluates to 0 instead of actual result
- **PostgreSQL exec()**: CTE rewriter CASTs the subquery text as a literal string value, producing "invalid input syntax for type numeric"
- **All prepared**: parameter count/index mismatch — MySQL: "number of bound variables does not match number of tokens", PostgreSQL: "supplies N parameters, requires N-1", SQLite: "column index out of range"

UPSERT with simple expressions (VALUES/EXCLUDED, col + 1, literals) works correctly. Only subqueries in the SET expression are affected. New row inserts (no conflict path) work correctly.

## SPEC-11.PG-UPDATE-FROM-PREPARED `[Issue #106]` PostgreSQL: UPDATE...FROM with $N prepared params does not apply
**Status:** Known Issue
**Platforms:** PostgreSQL-PDO (confirmed)
**Related specs:** [SPEC-4.2](04-write-operations.ears.md)
**Tests:** `Pdo/PostgresUpdateFromPreparedTest`

`UPDATE t SET col = s.val FROM s WHERE t.id = s.id AND s.filter = $1` with prepared `$N` parameters silently fails — the UPDATE does not apply and rows remain unchanged. The same query works correctly with `?` placeholders (PDO converts internally) and `:name` named parameters. UPDATE...FROM via exec() with literal values also works correctly.

This affects all variants: single param, multiple params, expression in SET with param, and shadow-inserted data. The CTE rewriter likely mishandles `$N` parameter positions when processing the FROM clause in UPDATE context.

**Extended scope (2026-03-10):** The `$N` parameter issue extends beyond UPDATE...FROM:
- **LIMIT $1 OFFSET $2:** Returns all rows (LIMIT/OFFSET ignored entirely). `?` placeholders work correctly. Tests: `Pdo/PostgresPreparedLimitOffsetParamsTest`.
- **WHERE $1 LIMIT $2 OFFSET $3:** Returns empty results (WHERE not applied). Three `?` placeholders work correctly.
- **INSERT...SELECT WHERE category = $1 AND NOT EXISTS(...):** Inserts only 1 row instead of expected 2. `?` placeholder works correctly. Tests: `Pdo/PostgresInsertWhereNotExistsTest`.
- **DELETE WHERE IN (SELECT ... HAVING COUNT(*) < $1):** Doesn't filter (all 4 rows remain). `?` placeholder works correctly. Tests: `Pdo/PostgresDeleteWithAggregatedInSubqueryTest`.

Root cause appears to be that the CTE rewriter doesn't correctly track `$N` parameter positions across all SQL statement types, not just UPDATE...FROM.

**Extended scope (2026-03-10, cont.):**
- **3-table JOIN DELETE with $1:** `DELETE WHERE id IN (SELECT ... JOIN ... WHERE category = $1)` doesn't filter (all rows remain). Exec variant works. Tests: `Pdo/PostgresThreeTableJoinDmlTest`.
- **Chained EXISTS with $1:** `DELETE WHERE EXISTS(...) AND NOT EXISTS(... status = $1)` deletes wrong rows. Exec variant works. Tests: `Pdo/PostgresDeleteChainedExistsTest`.
- **UPDATE SET REPLACE($1, $2):** `SET code = REPLACE(code, $1, $2)` stores NULL values instead of replaced string. Tests: `Pdo/PostgresStringFunctionDmlTest`. [Issue #108]
- **UPDATE SET concatenation with $1:** `SET label = label || $1` doesn't apply the concatenation (value unchanged). Tests: `Pdo/PostgresStringFunctionDmlTest`. [Issue #108]
- **DELETE USING with $1:** `DELETE FROM t USING s WHERE t.id = s.id AND s.reason = $1` doesn't apply (all rows remain). `?` placeholder works correctly. Tests: `Pdo/PostgresDeleteUsingTest`.
- **DELETE WHERE (a,b) = ($1, $2):** Row value constructor with `$N` params doesn't delete (all rows remain). `?` placeholders work correctly. Tests: `Pdo/PostgresRowValueConstructorDmlTest`.
- **INSERT...SELECT HAVING SUM(qty) > $1:** Returns 0 rows (no data inserted). `?` placeholder also returns 0 rows on SQLite. Tests: `Pdo/PostgresUpdateDistinctSubqueryTest`.

## SPEC-11.PG-STRING-FUNC-PARAMS `[Issue #108]` UPDATE SET with string functions and $N params produces NULL
**Status:** Known Issue
**Platforms:** PostgreSQL-PDO
**Related specs:** [SPEC-10.2.257](10-platform-notes.ears.md)
**Tests:** `Pdo/PostgresStringFunctionDmlTest`

`UPDATE SET col = REPLACE(col, $1, $2)` with `$N` prepared parameters stores NULL in all affected columns instead of the expected replaced string. `UPDATE SET col = col || $1` with a `$N` parameter silently fails — the concatenation does not apply and the value remains unchanged. Non-prepared variants with literal values work correctly. MySQL handles all variants correctly. Extends the `$N` parameter handling issues documented in Issues #61, #106.

## SPEC-11.EXPLAIN-BLOCKED `[Issue #107]` EXPLAIN, DESCRIBE, SHOW blocked by ZTD Write Protection
**Status:** Known Issue
**Platforms:** SQLite-PDO (confirmed), MySQL-PDO (confirmed), PostgreSQL-PDO (likely)
**Tests:** `Pdo/SqliteExplainThroughZtdTest`, `Pdo/MysqlExplainThroughZtdTest`

Read-only diagnostic statements (EXPLAIN, EXPLAIN QUERY PLAN, DESCRIBE, SHOW CREATE TABLE) are blocked with "ZTD Write Protection: Statement type not supported SQL statement." These statements should be passed through to the physical database since they are purely read-only and do not modify data.

## SPEC-11.CTE-DML `[Issue #109]` User-written CTEs in DML (INSERT, DELETE) broken on all platforms
**Status:** Known Issue
**Platforms:** SQLite-PDO, MySQL-PDO, MySQLi, PostgreSQL-PDO
**Related specs:** [SPEC-3.3](03-read-operations.ears.md), [SPEC-4.1a](04-write-operations.ears.md)
**Tests:** `Pdo/SqliteCteDrivenDmlTest`, `Pdo/MysqlCteDrivenDmlTest`, `Pdo/PostgresCteDrivenDmlTest`, `Mysqli/CteDrivenDmlTest`, `Pdo/SqliteRecursiveCteDmlTest`, `Pdo/MysqlRecursiveCteDmlTest`, `Pdo/PostgresRecursiveCteDmlTest`, `Mysqli/RecursiveCteDmlTest`

`WITH cte AS (...) INSERT INTO target SELECT FROM cte` and `WITH cte AS (...) DELETE FROM target WHERE id IN (SELECT id FROM cte)` fail on all platforms. The CTE rewriter does not handle user-written CTEs combined with DML. Errors vary by platform: SQLite reports "no such table" (user CTE stripped), MySQL-PDO reports "Missing shadow mutation" (misdetected), MySQLi reports syntax errors, PostgreSQL reports syntax errors. `WITH RECURSIVE ... INSERT/DELETE` exhibits the same failure. Multiple user CTEs driving DML also fail.

## SPEC-11.CTE-NAME-COLLISION `[Issue #110]` CTE name collision when user CTE matches physical table name
**Status:** Known Issue
**Platforms:** SQLite-PDO, MySQL-PDO, MySQLi, PostgreSQL-PDO
**Related specs:** [SPEC-3.3](03-read-operations.ears.md)
**Tests:** `Pdo/SqliteCteNameCollisionTest`, `Pdo/MysqlCteNameCollisionTest`, `Pdo/PostgresCteNameCollisionTest`, `Mysqli/CteNameCollisionTest`

When a user writes `WITH tablename AS (SELECT ... FROM tablename WHERE ...) SELECT FROM tablename`, the CTE rewriter also creates a CTE named `tablename`, producing a duplicate. SQLite: "duplicate WITH table name". MySQL/MySQLi: "Not unique table/alias". PostgreSQL: "WITH query name specified more than once" (3 patterns) or returns 0 rows (mixed CTE pattern). This is a common pattern — users often write `WITH orders AS (SELECT ... FROM orders WHERE status = 'pending') ...`.

## SPEC-11.MULTI-UNION-DERIVED `[Issue #111]` 3+ UNION ALL branches in derived table return empty
**Status:** Known Issue
**Platforms:** SQLite-PDO, MySQL-PDO, MySQLi (NOT PostgreSQL-PDO)
**Related specs:** [SPEC-3.3a](03-read-operations.ears.md)
**Tests:** `Pdo/SqliteMultiUnionDerivedTest`, `Pdo/MysqlMultiUnionDerivedTest`, `Mysqli/MultiUnionDerivedTest`, `Pdo/PostgresMultiUnionDerivedTest`

`SELECT ... FROM (SELECT ... UNION ALL SELECT ... UNION ALL SELECT ...) sub` returns 0 rows on SQLite, MySQL, and MySQLi. PostgreSQL handles this correctly. The CTE rewriter does not rewrite table references in all UNION branches of a derived table. `WHERE ... IN (SELECT ... UNION SELECT ...)` subqueries work correctly. Prepared statement variant also returns empty.

## SPEC-11.UPSERT-ACCUMULATE `[Issue #112]` Upsert self-referencing accumulate expression evaluates to 0
**Status:** Known Issue
**Platforms:** MySQLi (confirmed), MySQL-PDO (confirmed), SQLite-PDO (confirmed)
**Related specs:** [SPEC-4.2a](04-write-operations.ears.md)
**Tests:** `Mysqli/MultiRowUpsertTest`, `Pdo/MysqlMultiRowUpsertTest`, `Pdo/SqliteMultiRowUpsertTest`

Upsert with a self-referencing accumulate expression in the SET clause evaluates the existing column value to 0 instead of the actual current value. The shadow store does not properly resolve table-qualified column references that refer to the row being updated.

- **MySQL**: `INSERT INTO t VALUES (1, 'x', 5) ON DUPLICATE KEY UPDATE qty = t.qty + VALUES(qty)` — sets qty=0 instead of qty=105 (original 100 + 5)
- **SQLite**: `INSERT INTO t VALUES (1, 'x', 5) ON CONFLICT(id) DO UPDATE SET qty = t.qty + excluded.qty` — sets qty=0 instead of qty=105
- Multi-row variants also affected: all conflicting rows get qty=0+excluded instead of existing+excluded
- Simple `VALUES(col)` / `excluded.col` references without self-reference work correctly
- `ON DUPLICATE KEY UPDATE price = VALUES(price)` (direct replacement, no self-reference) works correctly

## SPEC-11.PREPARED-JSON-FUNC-COMPARE `[Issue #113]` Prepared SELECT with JSON function + comparison operator returns empty
**Status:** Known Issue
**Platforms:** SQLite-PDO (confirmed), MySQL-PDO (confirmed)
**Related specs:** [SPEC-3.2](03-read-operations.ears.md)
**Tests:** `Pdo/SqliteJsonDmlTest`, `Pdo/MysqlJsonDmlTest`

Prepared SELECT with a JSON extraction function followed by a comparison operator (`>`, `<`, `>=`, `<=`) and a `?` parameter returns 0 rows, even when matching rows exist.

- **SQLite**: `SELECT name FROM t WHERE json_extract(meta, '$.weight') > ?` with param `[8]` → 0 rows (expected 2)
- **MySQL**: `SELECT name FROM t WHERE JSON_EXTRACT(meta, '$.weight') > ?` with param `[8]` → 0 rows (expected 2)
- Non-prepared variants (`query()`) with literal values work correctly
- String equality with JSON function (`json_extract(meta, '$.type') = ?`) works correctly
- Related to Issue #95 (nested function WHERE with prepared params) but this affects single (non-nested) function calls with comparison operators

## SPEC-11.UPSERT-JSON-FUNC-SET `[Issue #114]` Upsert SET with JSON function call produces invalid JSON or syntax error
**Status:** Known Issue
**Platforms:** MySQLi (confirmed), MySQL-PDO (confirmed), PostgreSQL-PDO (confirmed)
**Related specs:** [SPEC-4.2a](04-write-operations.ears.md)
**Tests:** `Mysqli/JsonDmlTest`, `Pdo/MysqlJsonDmlTest`, `Pdo/PostgresJsonDmlTest`

INSERT with upsert clause containing a JSON function call (JSON_SET, jsonb_set) in the ON DUPLICATE/ON CONFLICT SET expression fails on all tested platforms. The CTE rewriter CASTs the function call as a literal string value in the shadow CTE VALUES, producing invalid JSON.

- **MySQL (MySQLi, PDO)**: `ON DUPLICATE KEY UPDATE meta = JSON_SET(meta, '$.color', 'purple')` → "Invalid JSON text in argument 1 to function cast_as_json"
- **PostgreSQL**: `ON CONFLICT DO UPDATE SET meta = jsonb_set(pg_jdml_items.meta, '{color}', '"purple"')` → "invalid input syntax for type json... Token 'jsonb_set' is invalid"
- Non-upsert UPDATE with JSON_SET/jsonb_set works correctly on all platforms
- Related to Issue #105 (upsert with subquery in SET) — same class of issue where complex expressions in upsert SET are not handled by the CTE rewriter

## SPEC-11.WINDOW-FUNC-DML `[Issue #115]` Window function in DML subquery breaks CTE rewriter
**Status:** Known Issue
**Platforms:** PostgreSQL-PDO (confirmed), MySQLi (confirmed), MySQL-PDO (confirmed), SQLite-PDO (confirmed)
**Related specs:** [SPEC-4.2](04-write-operations.ears.md), [SPEC-4.3](04-write-operations.ears.md)
**Tests:** `Pdo/PostgresWindowFunctionDmlTest`, `Pdo/MysqlWindowFunctionDmlTest`, `Pdo/SqliteWindowFunctionDmlTest`, `Mysqli/WindowFunctionDmlTest`

DML statements containing window functions (ROW_NUMBER, DENSE_RANK, RANK) in subqueries break the CTE rewriter. The failure mode differs by platform:

- **PostgreSQL**: `DELETE WHERE id IN (SELECT id FROM (SELECT id, ROW_NUMBER() OVER (PARTITION BY player ...) ...) WHERE rn > 1)` — syntax error: the OVER clause is truncated during rewrite
- **MySQL (MySQLi, PDO)**: `UPDATE r JOIN (SELECT player, DENSE_RANK() OVER (...) FROM scores GROUP BY player) s ON ... SET r.rank_pos = s.drank` — "Identifier name '(SELECT player, DENSE_RANK() OVER ...' is too long" (subquery text treated as identifier, extends Issue #104)
- **SQLite**: `UPDATE t SET col = (SELECT drank FROM (SELECT player, DENSE_RANK() OVER (...) FROM scores GROUP BY player) WHERE player = t.player)` — "near FROM: syntax error"
- DELETE with ROW_NUMBER works on MySQL and SQLite but not PostgreSQL
- INSERT...SELECT with window function (RANK() OVER ...) works correctly on all platforms
- Non-DML SELECT with window functions works correctly on all platforms

## SPEC-11.PG-ROW-VALUE-UPDATE `[Issue #116]` Row value constructor in UPDATE WHERE produces syntax error (PostgreSQL)
**Status:** Known Issue
**Platforms:** PostgreSQL-PDO (confirmed)
**Related specs:** [SPEC-4.2](04-write-operations.ears.md)
**Tests:** `Pdo/PostgresRowValueConstructorDmlTest`

`UPDATE t SET col = 0 WHERE (col1, col2) IN (SELECT a, b FROM s)` produces a syntax error on PostgreSQL. The CTE rewriter generates invalid SQL when the UPDATE WHERE clause contains a row value constructor (tuple comparison).

- `DELETE FROM t WHERE (col1, col2) IN (SELECT ...)` works correctly on PostgreSQL
- Both UPDATE and DELETE with row value constructors work correctly on MySQL (MySQLi, PDO) and SQLite
- Prepared DELETE WHERE (a,b) = ($1, $2) with `$N` params also fails on PostgreSQL (Issue #106)
- Prepared DELETE WHERE (a,b) = (?, ?) with `?` params works correctly on PostgreSQL

## SPEC-11.INSERT-SELECT-HAVING `[Issue #117]` INSERT...SELECT with GROUP BY HAVING produces "no such column" error
**Status:** Known Issue
**Platforms:** SQLite-PDO (confirmed), PostgreSQL-PDO (confirmed)
**Related specs:** [SPEC-4.1a](04-write-operations.ears.md)
**Tests:** `Pdo/SqliteUpdateDistinctSubqueryTest`, `Pdo/PostgresUpdateDistinctSubqueryTest`, `Mysqli/UpdateDistinctSubqueryTest`, `Pdo/MysqlUpdateDistinctSubqueryTest`

`INSERT INTO summary (product_id, total_qty, order_count) SELECT product_id, SUM(qty), COUNT(*) FROM orders GROUP BY product_id HAVING COUNT(*) >= 2` fails on SQLite and PostgreSQL with "no such column: total_qty". The CTE rewriter references INSERT target column names in the verification SELECT, but those names don't exist in the source query's output.

- Works correctly on MySQL (MySQLi, PDO)
- Prepared INSERT...SELECT HAVING with `?` param returns 0 rows on SQLite (related to Issue #22)
- Prepared INSERT...SELECT HAVING with `$N` param returns 0 rows on PostgreSQL (related to Issue #106)
- Related to Issue #20 (INSERT...SELECT with computed columns/aggregation)

## SPEC-11.PREPARED-BETWEEN-DML `[Issue #118]` Prepared BETWEEN in DML (UPDATE/DELETE) has no effect
**Status:** Known Issue
**Platforms:** MySQL-PDO (confirmed), PostgreSQL-PDO (confirmed), MySQLi (confirmed)
**Not affected:** SQLite-PDO (works correctly)
**Related specs:** [SPEC-4.2](04-write-operations.ears.md), [SPEC-4.3](04-write-operations.ears.md)
**Tests:** `Pdo/MysqlBetweenParamDmlTest`, `Pdo/PostgresBetweenParamDmlTest`, `Mysqli/BetweenParamDmlTest`, `Pdo/SqliteBetweenParamDmlTest`

Prepared `UPDATE` and `DELETE` statements using `WHERE col BETWEEN ? AND ?` with bound parameters have no effect on MySQL and PostgreSQL. The statement executes without error but 0 rows are affected.

- **MySQL (PDO, MySQLi)**: `UPDATE t SET stock = stock + 10 WHERE price BETWEEN ? AND ?` with params `[20.00, 75.00]` — 0 rows updated, all values unchanged
- **PostgreSQL**: Same pattern, 0 rows affected
- **SQLite**: Works correctly — rows matching the BETWEEN range are properly updated/deleted
- Combining BETWEEN with additional `AND` conditions also fails: `WHERE price BETWEEN ? AND ? AND stock < ?`
- Non-prepared `BETWEEN` with literal values (via `exec()`) works correctly on all platforms
- The CTE rewriter likely confuses the `AND` inside `BETWEEN ? AND ?` with a logical `AND` operator

## SPEC-11.CAST-IN-DML `[Issue #119]` CAST expression in DML produces wrong values or is ignored
**Status:** Known Issue
**Platforms:** All platforms affected in different patterns
**Related specs:** [SPEC-4.1a](04-write-operations.ears.md), [SPEC-4.2](04-write-operations.ears.md), [SPEC-4.3](04-write-operations.ears.md)
**Tests:** `Pdo/SqliteCastInDmlTest`, `Pdo/MysqlCastInDmlTest`, `Pdo/PostgresCastInDmlTest`, `Mysqli/CastInDmlTest`

`CAST(column AS type)` expressions inside DML statements produce incorrect results. The `AS` keyword inside CAST confuses the CTE rewriter, which interprets it as a column or table alias.

- **INSERT...SELECT with CAST** (SQLite, PostgreSQL): `INSERT INTO t SELECT label, CAST(str_amount AS REAL) FROM raw` — inserts rows but CAST values become 0.0 (SQLite) or empty (PostgreSQL). MySQL works correctly.
- **CAST arithmetic** (SQLite, PostgreSQL): `CAST(a AS REAL) * CAST(b AS INTEGER)` in INSERT...SELECT produces 0 on SQLite and PostgreSQL. MySQL works correctly.
- **DELETE WHERE CAST** (MySQL, PostgreSQL, MySQLi): `DELETE FROM t WHERE CAST(col AS DECIMAL) > 100` — no rows deleted (expression ignored). SQLite works correctly.
- **UPDATE SET with CAST subquery** (SQLite, PostgreSQL, MySQL): Syntax errors or 0 values depending on platform
- Non-CAST expressions in the same positions work correctly
- Related to Issue #33 (PostgreSQL array types with CAST) — same root cause of AS keyword confusion
