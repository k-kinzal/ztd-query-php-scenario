# 11. Known Issues and Cross-Platform Inconsistencies

## SPEC-11.UNKNOWN-UPDATE `[Unreported]` Unknown schema UPDATE (Passthrough mode)
**Status:** Known Issue
**Platforms:** MySQL-PDO (fromPdo), PostgreSQL-PDO, SQLite-PDO
**Related specs:** [SPEC-7.1](07-unknown-schema.ears.md), [SPEC-7.2](07-unknown-schema.ears.md)
**Tests:** `Pdo/MysqlUnknownSchemaTest`, `Pdo/PostgresUnknownSchemaTest`, `Pdo/SqliteUnknownSchemaTest`

On MySQL via `ZtdPdo::fromPdo()`, PostgreSQL, and SQLite, UPDATE on unreflected tables throws `RuntimeException` ("UPDATE simulation requires primary keys") instead of passing through. The `unknownSchemaBehavior` setting does not take effect for UPDATE operations on these platforms. On MySQL via `fromPdo()`, behavior depends on operation history — if no prior shadow operations touched the table, Passthrough works; after a shadow INSERT, it fails.

## SPEC-11.UNKNOWN-EXCEPTION `[Unreported]` Unknown schema UPDATE (Exception mode)
**Status:** Known Issue
**Platforms:** PostgreSQL-PDO, SQLite-PDO
**Related specs:** [SPEC-7.2](07-unknown-schema.ears.md)

On PostgreSQL and SQLite, UPDATE throws `RuntimeException` ("UPDATE simulation requires primary keys") instead of `ZtdPdoException` ("Unknown table"). Exception type and message differ from MySQL.

## SPEC-11.UNKNOWN-DELETE `[Unreported]` Unknown schema DELETE inconsistency
**Status:** Known Issue
**Platforms:** PostgreSQL-PDO, SQLite-PDO
**Related specs:** [SPEC-7.1](07-unknown-schema.ears.md)

On PostgreSQL, DELETE in Exception mode throws `RuntimeException` rather than `ZtdPdoException`. On SQLite, same behavior.

## SPEC-11.SQLITE-ON-CONFLICT `[Unreported]` INSERT ... ON CONFLICT DO NOTHING (SQLite)
**Status:** Known Issue
**Platforms:** SQLite-PDO
**Related specs:** [SPEC-4.2a](04-write-operations.ears.md), [SPEC-4.2e](04-write-operations.ears.md)

On SQLite, `INSERT ... ON CONFLICT DO NOTHING` inserts both rows (shadow store does not enforce PK constraints). Use `INSERT OR IGNORE` instead. PostgreSQL handles this correctly.

## SPEC-11.MYSQL-INSERT-SELECT-STAR `[Unreported]` INSERT ... SELECT * (MySQL)
**Status:** Known Issue
**Platforms:** MySQLi, MySQL-PDO
**Related specs:** [SPEC-4.1a](04-write-operations.ears.md)

On MySQL, `INSERT INTO t SELECT * FROM s` throws `RuntimeException` because the InsertTransformer counts `SELECT *` as 1 column. Workaround: use explicit column lists. SQLite and PostgreSQL work correctly.

## SPEC-11.PG-CTE `[By-Design]` User-written CTEs (PostgreSQL)
**Status:** Known Issue (By-Design)
**Platforms:** PostgreSQL-PDO
**Related specs:** [SPEC-3.3](03-read-operations.ears.md)
**Tests:** `Pdo/PostgresUserCteTest`

On PostgreSQL, table references inside user CTEs are NOT rewritten — the inner CTE reads from the physical table, returning 0 rows. MySQL and SQLite work correctly.

## SPEC-11.PG-SCHEMA-QUALIFIED `[Unreported]` Schema-qualified table names (PostgreSQL)
**Status:** Known Issue
**Platforms:** PostgreSQL-PDO
**Tests:** `Pdo/PostgresSchemaQualifiedTest`

INSERT/UPDATE/DELETE with `public.tablename` work. SELECT returns empty (CTE rewriter doesn't recognize schema-qualified names). Workaround: use unqualified table names in SELECT.

## SPEC-11.EXECUTE-QUERY-UPSERT `[Unreported]` execute_query vs prepare+bind_param for UPSERT/REPLACE (MySQLi)
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

## SPEC-11.PDO-UPSERT `[Unreported]` Upsert via PDO prepared statements
**Status:** Known Issue
**Platforms:** MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Related specs:** [SPEC-4.2a](04-write-operations.ears.md), [SPEC-4.2b](04-write-operations.ears.md)
**Tests:** `Pdo/MysqlPreparedUpsertTest`, `Pdo/PostgresPreparedUpsertTest`, `Pdo/SqlitePreparedUpsertTest`

PDO prepared REPLACE INTO and INSERT ... ON CONFLICT DO UPDATE do NOT update existing rows. Use `exec()` instead.

## SPEC-11.SQLITE-HAVING-PARAMS `[Issue #22]` HAVING with prepared params (SQLite)
**Status:** Known Issue
**Platforms:** SQLite-PDO
**Tests:** `Pdo/SqlitePreparedAggregateParamsTest`

On SQLite, HAVING with bound parameters returns empty results. HAVING with literal values works. MySQL and PostgreSQL work correctly.

## SPEC-11.MYSQL-BACKSLASH `[Unreported]` Backslash corruption in MySQL shadow store
**Status:** Known Issue
**Platforms:** MySQLi, MySQL-PDO
**Tests:** `Mysqli/BackslashCorruptionTest`, `Pdo/MysqlBackslashCorruptionTest`

Backslash characters are corrupted in shadow store: `\t` → tab, `\n` → newline, etc. CTE rewriter embeds values without escaping backslashes. SQLite and PostgreSQL not affected.

## SPEC-11.PG-BOOLEAN-FALSE `[Unreported]` PostgreSQL BOOLEAN false casting
**Status:** Known Issue
**Platforms:** PostgreSQL-PDO
**Tests:** `Pdo/PostgresTypeEdgeCaseTest`

Inserting `false` into BOOLEAN column via prepared statement succeeds, but SELECT fails (CTE generates `CAST('' AS BOOLEAN)`). `true` works correctly.

## SPEC-11.PG-BIGINT `[Unreported]` PostgreSQL BIGINT overflow
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

## SPEC-11.MYSQL-EXCEPT-INTERSECT `[Unreported]` EXCEPT/INTERSECT on MySQL
**Status:** Known Issue
**Platforms:** MySQLi, MySQL-PDO
**Related specs:** [SPEC-3.3d](03-read-operations.ears.md)
**Tests:** `Mysqli/ExceptIntersectTest`, `Pdo/MysqlExceptIntersectTest`

EXCEPT and INTERSECT throw `UnsupportedSqlException` on MySQL. The CTE rewriter misparses them as multi-statement queries. Workaround: NOT IN / IN subqueries.

## SPEC-11.PG-EXTRACT `[Unreported]` PostgreSQL EXTRACT on shadow dates
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

## SPEC-11.PG-ARRAY-TYPE `[Unreported]` PostgreSQL array types broken
**Status:** Known Issue
**Platforms:** PostgreSQL-PDO
**Tests:** `Pdo/PostgresArrayTypeTest`

INSERT with INTEGER[] array values succeeds, but SELECT fails (CastRenderer emits base type without array suffix). TEXT[] is unaffected. ARRAY constructor syntax causes column count errors.

## SPEC-11.BINARY-DATA `[Unreported]` BLOB/BINARY data with binary bytes
**Status:** Known Issue
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO

Inserting binary data (null bytes, high-byte values) via prepared statements succeeds, but SELECT fails (CTE rewriter embeds binary bytes as string literals). Text-only BLOB payloads work. Workaround: base64 encode or disable ZTD.

## SPEC-11.PG-QUOTE-ESCAPE `[Issue #25]` Doubled single-quote escaping (PostgreSQL)
**Status:** Known Issue
**Platforms:** PostgreSQL-PDO
**Tests:** `Pdo/PostgresEscapedQuoteTest` (if exists)

PostgreSQL PgSqlParser's regex doesn't handle `''` escaping, causing incorrect WHERE clause extraction. Workaround: use prepared statements.

## SPEC-11.SQLITE-CTAS `[Unreported]` CREATE TABLE AS SELECT (SQLite)
**Status:** Known Issue
**Platforms:** SQLite-PDO
**Related specs:** [SPEC-5.1c](05-ddl-operations.ears.md)
**Tests:** `Pdo/SqliteCtasEmptyResultTest`

SELECT immediately after CTAS fails with "no such table". After INSERT, original CTAS data is lost.

## SPEC-11.INSERT-SELECT-COMPUTED `[Unreported]` INSERT...SELECT with computed columns (SQLite/PostgreSQL)
**Status:** Known Issue
**Platforms:** SQLite-PDO, PostgreSQL-PDO
**Related specs:** [SPEC-4.1a](04-write-operations.ears.md)
**Tests:** `Pdo/SqliteInsertSelectComputedColumnsTest`, `Pdo/SqliteInsertSelectAggregateTest`

Computed columns and aggregated values become NULL when using INSERT...SELECT on SQLite and PostgreSQL. MySQL works correctly.

## SPEC-11.MYSQL-COMMA-UPDATE `[Unreported]` MySQL comma-syntax multi-table UPDATE
**Status:** Known Issue
**Platforms:** MySQLi, MySQL-PDO
**Related specs:** [SPEC-4.2c](04-write-operations.ears.md)

`UPDATE t1, t2 SET ... WHERE ...` is partially supported. Prefer JOIN syntax.

## SPEC-11.UPDATE-SUBQUERY-SET `[Unreported]` UPDATE SET col = (subquery) platform differences
**Status:** Known Issue
**Platforms:** PostgreSQL-PDO, SQLite-PDO (fails); MySQLi, MySQL-PDO (works)

Non-correlated scalar subqueries in UPDATE SET work on MySQL and SQLite but fail on PostgreSQL. Correlated subqueries in SET fail on SQLite.
