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
**Tests:** `Pdo/SqlitePreparedAggregateParamsTest`, `Pdo/SqliteSubscriptionRenewalTest`, `Pdo/SqliteStudentGradeReportTest`, `Pdo/PostgresSubscriptionRenewalTest`, `Pdo/PostgresStudentGradeReportTest`

On SQLite, HAVING with bound parameters returns empty results. HAVING with literal values works. MySQL works correctly. PostgreSQL also returns empty for complex multi-table HAVING with `$N` params (e.g., `HAVING SUM(amount) >= $2` with JOINs), extending this issue beyond SQLite-only.

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
**Tests:** `Pdo/SqliteInsertSelectComputedColumnsTest`, `Pdo/SqliteInsertSelectAggregateTest`

Computed columns and aggregated values become NULL when using INSERT...SELECT on SQLite and PostgreSQL. MySQL works correctly.

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

## SPEC-11.UPDATE-SET-CORRELATED-SUBQUERY `[Issue #51]` UPDATE with correlated subquery in SET clause produces errors
**Status:** Known Issue
**Platforms:** SQLite-PDO (confirmed), PostgreSQL-PDO (confirmed); MySQL-PDO NOT affected
**Related specs:** [SPEC-4.2](04-write-operations.ears.md)
**Tests:** `Pdo/SqliteUpdateSubqueryTest`, `Pdo/PostgresUpdateSubqueryTest`, `Pdo/MysqlUpdateSubqueryTest`

UPDATE statements with correlated subqueries in the SET clause fail through the CTE rewriter on SQLite and PostgreSQL. On SQLite, the rewriter produces `near "FROM": syntax error`. On PostgreSQL, it produces `column "price" does not exist` due to incorrect aliasing. Self-referencing scalar subqueries in SET (e.g., `SET price = (SELECT MAX(price) FROM same_table WHERE ...)`) also fail with different errors per platform. MySQL is NOT affected — all correlated SET patterns work correctly. UPDATE with subqueries in WHERE clause works correctly on all platforms. DELETE with correlated subqueries also works correctly on all platforms.

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

UPDATE statements with CASE expressions in the SET clause work correctly via `exec()`, but when using `prepare()` + `execute()` with `$N` parameters inside the CASE WHEN conditions, the UPDATE is silently a no-op — no rows are modified and no error is thrown. Non-prepared CASE in SET works on all platforms.

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
