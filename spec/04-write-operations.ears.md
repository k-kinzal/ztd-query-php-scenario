# 4. Write Operations

## SPEC-4.1 INSERT
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Scenarios/BasicCrudScenario::testInsertAndSelect` (all platforms), `Scenarios/WriteOperationScenario::testMultiRowInsert` (all platforms), `Mysqli/BatchInsertTest`, `Pdo/MysqlBatchInsertTest`, `Pdo/PostgresBatchInsertTest`, `Pdo/SqliteBatchInsertTest`

When an INSERT is executed with ZTD enabled, the system shall track the inserted rows in the shadow store without modifying the physical table.

Subsequent SELECT queries shall include the inserted rows.

Multi-row INSERT (e.g., `INSERT INTO t VALUES (1, 'a'), (2, 'b')`) is supported. The affected row count reflects the total number of inserted rows.

INSERT with NULL values is supported. NULL values are correctly stored in the shadow store and queryable via `IS NULL` / `IS NOT NULL`.

**Limitation (PDO prepared INSERT — all PDO platforms):** On the PDO adapter (MySQL, PostgreSQL, and SQLite), rows inserted via prepared statements (`prepare()` + `execute()`) cannot be subsequently updated or deleted — UPDATE/DELETE operations report affected rows but the shadow store data remains unchanged. Rows inserted via `exec()` work correctly. The MySQLi adapter is NOT affected. See [SPEC-11.PDO-PREPARED-INSERT](11-known-issues.ears.md) for details.

**Verified behavior:** INSERT without column list works. INSERT with SQL expressions in VALUES (arithmetic, functions, COALESCE, CASE, CONCAT, ABS, LENGTH, negative numbers, GREATEST, IF) are correctly evaluated. INSERT ... SET syntax (MySQL) works. Multi-row REPLACE INTO works. 50+ sequential INSERTs and 200-row bulk INSERTs work correctly.

#### Verification Matrix — MySQL (MySQLi, PDO)

| PHP | 5.6 | 5.7 | 8.0 | 8.4 | 9.1 |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | ✓   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

#### Verification Matrix — PostgreSQL (PDO)

| PHP | 14  | 15  | 16  | 17  | 18  |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | ✓   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

#### Verification Matrix — SQLite (PDO)

| PHP | 3.x |
|-----|-----|
| 8.1 | -   |
| 8.2 | -   |
| 8.3 | ✓   |
| 8.4 | -   |
| 8.5 | -   |

## SPEC-4.1a INSERT ... SELECT
**Status:** Partially Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/InsertSelectUpsertTest`, `Mysqli/InsertSubqueryPatternsTest`, `Pdo/MysqlInsertSelectUpsertTest`, `Pdo/MysqlInsertSubqueryPatternsTest`, `Pdo/PostgresInsertSubqueryPatternsTest`, `Pdo/SqliteInsertSubqueryPatternsTest`

When an `INSERT ... SELECT` is executed with ZTD enabled, the system shall insert rows from the SELECT result (which reads from the shadow store) into the target table's shadow store.

On MySQL, `INSERT ... SELECT` requires explicit column lists on both sides. Using `SELECT *` throws `RuntimeException` ("INSERT column count does not match SELECT column count") because the MySQL InsertTransformer counts `*` as 1 column instead of expanding it. On SQLite, `INSERT ... SELECT *` works correctly.

**Limitation (SQLite and PostgreSQL)**: `INSERT ... SELECT` with computed columns (e.g., `price * 2`), aggregate functions (COUNT, AVG, SUM), or GROUP BY does NOT correctly transfer values. The rows are inserted with correct count, but computed/aggregated column values become NULL. On MySQL (both MySQLi and PDO), all of these work correctly. The workaround on SQLite/PostgreSQL is to SELECT first, then INSERT the results manually.

`INSERT ... SELECT WHERE NOT EXISTS` (conditional insert) works correctly on all platforms.

`INSERT ... SELECT` with UNION ALL works correctly on all platforms.

Self-referencing INSERT (`INSERT INTO t SELECT FROM t`) works correctly — SELECT snapshot is taken before INSERT starts.

#### Verification Matrix — MySQL (MySQLi, PDO)

| PHP | 5.6 | 5.7 | 8.0 | 8.4 | 9.1 |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | ✓   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

#### Verification Matrix — PostgreSQL (PDO)

| PHP | 14  | 15  | 16  | 17  | 18  |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | ✓   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

#### Verification Matrix — SQLite (PDO)

| PHP | 3.x |
|-----|-----|
| 8.1 | -   |
| 8.2 | -   |
| 8.3 | ✓   |
| 8.4 | -   |
| 8.5 | -   |

## SPEC-4.2 UPDATE
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Scenarios/BasicCrudScenario::testUpdateAndVerify` (all platforms), `Scenarios/WriteOperationScenario::testUpdateMultipleRows` (all platforms)

When an UPDATE is executed with ZTD enabled, the system shall track the updated rows in the shadow store without modifying the physical table.

Subsequent SELECT queries shall reflect the updated values.

UPDATE operations require the table schema (including primary keys) to be known. If the schema was not reflected at session creation time, the behavior depends on `unknownSchemaBehavior` (see [SPEC-7.1](07-unknown-schema.ears.md)).

**Verified behavior:** Self-referencing UPDATE (SET col = col + N) works. CASE expressions in UPDATE SET work. String concatenation in SET works. Multiple sequential UPDATEs to same row work. MySQL DELETE/UPDATE with ORDER BY + LIMIT works. Optimistic locking pattern (UPDATE WHERE version = N, check affected rows, bump version) works correctly — stale version matches 0 rows. Soft delete pattern (UPDATE SET deleted_at = timestamp, then filter with IS NULL/IS NOT NULL) works correctly.

#### Verification Matrix — MySQL (MySQLi, PDO)

| PHP | 5.6 | 5.7 | 8.0 | 8.4 | 9.1 |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | ✓   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

#### Verification Matrix — PostgreSQL (PDO)

| PHP | 14  | 15  | 16  | 17  | 18  |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | ✓   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

#### Verification Matrix — SQLite (PDO)

| PHP | 3.x |
|-----|-----|
| 8.1 | -   |
| 8.2 | -   |
| 8.3 | ✓   |
| 8.4 | -   |
| 8.5 | -   |

## SPEC-4.2a UPSERT
**Status:** Partially Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/UpsertTest`, `Mysqli/PreparedUpsertTest`, `Pdo/MysqlUpsertTest`, `Pdo/PostgresUpsertTest`, `Pdo/SqliteUpsertTest`, `Pdo/MysqlPreparedUpsertTest`, `Pdo/PostgresPreparedUpsertTest`, `Pdo/SqlitePreparedUpsertTest`

When an `INSERT ... ON DUPLICATE KEY UPDATE` (MySQL) or `INSERT ... ON CONFLICT DO UPDATE` (PostgreSQL) is executed with ZTD enabled, the system shall:
- Insert the row if no duplicate primary key exists in the shadow store.
- Update the matching row if a duplicate primary key exists in the shadow store.

When `INSERT ... ON CONFLICT DO NOTHING` (PostgreSQL) is executed and a duplicate exists, the insert is silently ignored. **Note**: On SQLite, `ON CONFLICT DO NOTHING` inserts both rows because the shadow store does not enforce PK constraints. Use `INSERT OR IGNORE` instead on SQLite.

**Limitation (PDO prepared statements):** On the PDO adapter, upsert via `prepare()` + `execute()` does NOT update existing rows. Use `exec()` instead. MySQLi `prepare()` + `bind_param()` + `execute()` works correctly. MySQLi `execute_query()` has the same limitation as PDO prepared statements.

**Known Issue:** `ON DUPLICATE KEY UPDATE stock = stock + VALUES(stock)` — self-referencing expression loses the original row's value ([Issue #16](11-known-issues.ears.md)).

#### Verification Matrix — MySQL (MySQLi, PDO)

| PHP | 5.6 | 5.7 | 8.0 | 8.4 | 9.1 |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | ✓   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

#### Verification Matrix — PostgreSQL (PDO)

| PHP | 14  | 15  | 16  | 17  | 18  |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | ✓   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

#### Verification Matrix — SQLite (PDO)

| PHP | 3.x |
|-----|-----|
| 8.1 | -   |
| 8.2 | -   |
| 8.3 | ✓   |
| 8.4 | -   |
| 8.5 | -   |

## SPEC-4.2b REPLACE
**Status:** Partially Verified
**Platforms:** MySQLi, MySQL-PDO, SQLite-PDO
**Tests:** `Mysqli/HavingAndReplaceTest`, `Mysqli/ReplaceMultiRowTest`, `Pdo/MysqlHavingAndReplaceTest`, `Pdo/MysqlReplaceMultiRowTest`, `Pdo/SqliteConflictResolutionTest`

When a `REPLACE INTO` statement (MySQL) is executed with ZTD enabled, the system shall delete any existing row with matching primary key and insert the new row in the shadow store.

**Limitation (PDO prepared statements):** On the PDO adapter, `REPLACE INTO` via `prepare()` + `execute()` does NOT replace existing rows. Use `exec()` instead. MySQLi `prepare()` + `bind_param()` + `execute()` works correctly. MySQLi `execute_query()` has the same limitation.

SQLite additionally supports `INSERT OR REPLACE INTO` syntax as a synonym for `REPLACE INTO`. The same exec/prepared statement behavior applies.

#### Verification Matrix — MySQL (MySQLi, PDO)

| PHP | 5.6 | 5.7 | 8.0 | 8.4 | 9.1 |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | ✓   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

#### Verification Matrix — SQLite (PDO)

| PHP | 3.x |
|-----|-----|
| 8.1 | -   |
| 8.2 | -   |
| 8.3 | ✓   |
| 8.4 | -   |
| 8.5 | -   |

## SPEC-4.2c Multi-Table UPDATE
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO
**Tests:** `Mysqli/MultiTableOperationsTest`, `Pdo/MysqlMultiTableOperationsTest`, `Pdo/PostgresMultiTableOperationsTest`, `Pdo/MultiTableOperationsTest`

When a multi-table UPDATE statement is executed with ZTD enabled, the system shall update the target table rows in the shadow store based on the JOIN condition, without modifying the physical database.

Platform-specific syntax:
- **MySQL**: `UPDATE users u JOIN orders o ON u.id = o.user_id SET u.active = 0 WHERE o.amount > 150`
- **PostgreSQL**: `UPDATE users SET active = 0 FROM orders WHERE users.id = orders.user_id AND orders.amount > 150`

**Note:** MySQL comma-syntax multi-table UPDATE (`UPDATE t1, t2 SET ...`) is partially supported — prefer JOIN syntax.

#### Verification Matrix — MySQL (MySQLi, PDO)

| PHP | 5.6 | 5.7 | 8.0 | 8.4 | 9.1 |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | ✓   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

#### Verification Matrix — PostgreSQL (PDO)

| PHP | 14  | 15  | 16  | 17  | 18  |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | ✓   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

## SPEC-4.2d Multi-Table DELETE
**Status:** Partially Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO
**Tests:** `Mysqli/MultiTableDeleteTest`, `Pdo/MysqlMultiTableDeleteTest`, `Pdo/PostgresMultiTableOperationsTest`

When a multi-table DELETE statement is executed with ZTD enabled, the system shall delete the specified rows from the shadow store based on the JOIN condition.

Platform-specific syntax:
- **MySQL**: `DELETE o FROM orders o JOIN users u ON o.user_id = u.id WHERE u.name = 'Bob'`
- **PostgreSQL**: `DELETE FROM orders USING users WHERE orders.user_id = users.id AND users.name = 'Bob'`

**Known Issue:** Multi-target DELETE (`DELETE t1, t2 FROM ...`) only affects the first table ([Issue #26](11-known-issues.ears.md)).

#### Verification Matrix — MySQL (MySQLi, PDO)

| PHP | 5.6 | 5.7 | 8.0 | 8.4 | 9.1 |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | ✓   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

#### Verification Matrix — PostgreSQL (PDO)

| PHP | 14  | 15  | 16  | 17  | 18  |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | ✓   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

## SPEC-4.2e INSERT IGNORE / Ignore-Duplicate Syntax
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/InsertIgnoreTest`, `Pdo/MysqlInsertIgnoreTest`, `Pdo/PostgresInsertIgnoreTest`, `Pdo/SqliteInsertIgnoreTest`

When an ignore-duplicate INSERT is executed, the system shall silently skip rows with duplicate primary keys. Non-duplicate rows are inserted normally.

Platform-specific syntax: MySQL uses `INSERT IGNORE INTO`, SQLite uses `INSERT OR IGNORE INTO`, PostgreSQL uses `INSERT ... ON CONFLICT (col) DO NOTHING`.

**Note**: On SQLite, the standard SQL `INSERT ... ON CONFLICT DO NOTHING` does NOT correctly skip duplicates; use `INSERT OR IGNORE` instead.

#### Verification Matrix — MySQL (MySQLi, PDO)

| PHP | 5.6 | 5.7 | 8.0 | 8.4 | 9.1 |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | ✓   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

#### Verification Matrix — PostgreSQL (PDO)

| PHP | 14  | 15  | 16  | 17  | 18  |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | ✓   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

#### Verification Matrix — SQLite (PDO)

| PHP | 3.x |
|-----|-----|
| 8.1 | -   |
| 8.2 | -   |
| 8.3 | ✓   |
| 8.4 | -   |
| 8.5 | -   |

## SPEC-4.3 DELETE
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Scenarios/BasicCrudScenario::testDeleteAndVerify` (all platforms), `Mysqli/DeleteWithoutWhereTest`, `Pdo/MysqlDeleteWithoutWhereTest`, `Pdo/PostgresDeleteWithoutWhereTest`, `Pdo/SqliteDeleteWithoutWhereTest`

When a DELETE is executed with ZTD enabled, the system shall track the deletion in the shadow store without modifying the physical table.

**Limitation (SQLite)**: `DELETE FROM table` without a WHERE clause is silently ignored on SQLite — the shadow store retains all rows. The workaround is to use `DELETE FROM table WHERE 1=1` which works correctly on all platforms ([Issue #7](11-known-issues.ears.md)).

**Verified behavior:** DELETE with correlated subqueries (EXISTS, NOT EXISTS, IN, scalar comparison) works. MySQL DELETE with ORDER BY + LIMIT works.

#### Verification Matrix — MySQL (MySQLi, PDO)

| PHP | 5.6 | 5.7 | 8.0 | 8.4 | 9.1 |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | ✓   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

#### Verification Matrix — PostgreSQL (PDO)

| PHP | 14  | 15  | 16  | 17  | 18  |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | ✓   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

#### Verification Matrix — SQLite (PDO)

| PHP | 3.x |
|-----|-----|
| 8.1 | -   |
| 8.2 | -   |
| 8.3 | ✓   |
| 8.4 | -   |
| 8.5 | -   |

## SPEC-4.4 Affected Row Count
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/ExecReturnValueTest`, `Mysqli/RowCountEdgeCasesTest`, `Pdo/MysqlExecReturnValueTest`, `Pdo/MysqlRowCountTest`, `Pdo/PostgresExecReturnValueTest`, `Pdo/PostgresRowCountTest`, `Pdo/SqliteExecReturnValueTest`, `Pdo/SqliteRowCountTest`

After a write operation via `ZtdMysqli::query()`, `lastAffectedRows()` shall return the number of rows affected.

After a write operation via `ZtdPdo::exec()`, the return value shall be the number of affected rows.

After a write operation via `ZtdPdoStatement::execute()`, `rowCount()` shall return the number of affected rows.

After a write operation via `ZtdMysqliStatement::execute()`, `ztdAffectedRows()` shall return the number of affected rows. Note: The `$stmt->affected_rows` property may not be available; `ztdAffectedRows()` is the reliable accessor.

#### Verification Matrix — MySQL (MySQLi, PDO)

| PHP | 5.6 | 5.7 | 8.0 | 8.4 | 9.1 |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | ✓   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

#### Verification Matrix — PostgreSQL (PDO)

| PHP | 14  | 15  | 16  | 17  | 18  |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | ✓   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

#### Verification Matrix — SQLite (PDO)

| PHP | 3.x |
|-----|-----|
| 8.1 | -   |
| 8.2 | -   |
| 8.3 | ✓   |
| 8.4 | -   |
| 8.5 | -   |

## SPEC-4.5 Write Result Sets
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/WriteResultSetTest`, `Pdo/MysqlWriteResultSetTest`, `Pdo/PostgresWriteResultSetTest`, `Pdo/SqliteWriteResultSetTest`, `Pdo/WriteResultSetTest`

When a write operation is executed with ZTD enabled, the affected row data is consumed internally for shadow processing.

For `ZtdMysqli::query()`, the returned `mysqli_result` object from a write operation has an exhausted cursor; calling `fetch_assoc()` returns `null`.

For `ZtdPdoStatement`, calling `fetchAll()` after a write operation returns an empty array because `hasResultSet()` is `false`.

#### Verification Matrix — MySQL (MySQLi, PDO)

| PHP | 5.6 | 5.7 | 8.0 | 8.4 | 9.1 |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | ✓   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

#### Verification Matrix — PostgreSQL (PDO)

| PHP | 14  | 15  | 16  | 17  | 18  |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | ✓   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

#### Verification Matrix — SQLite (PDO)

| PHP | 3.x |
|-----|-----|
| 8.1 | -   |
| 8.2 | -   |
| 8.3 | ✓   |
| 8.4 | -   |
| 8.5 | -   |

## SPEC-4.6 real_query (mysqli)
**Status:** Verified
**Platforms:** MySQLi
**Tests:** `Mysqli/RealQueryTest`

When `real_query()` is called with ZTD enabled, write operations shall be tracked in the shadow store and return `true`.

When `real_query()` is called for a SELECT query, `real_query()` returns `true`, but `store_result()` returns `false`. Use `query()` instead for SELECT queries in ZTD mode.

#### Verification Matrix — MySQL (MySQLi)

| PHP | 5.6 | 5.7 | 8.0 | 8.4 | 9.1 |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | ✓   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

## SPEC-4.7 Property Access (mysqli)
**Status:** Verified (By-Design)
**Platforms:** MySQLi
**Tests:** `Mysqli/StatementIntrospectionTest`, `Mysqli/InsertIdBehaviorTest`, `Mysqli/LastInsertIdTest`

When ZTD is enabled, accessing mysqli properties (e.g., `$mysqli->insert_id`, `$mysqli->affected_rows`) throws an `Error` ("Property access is not allowed yet"). Use dedicated methods instead: `lastAffectedRows()` for affected row count.

`lastInsertId()` / `insert_id` returns 0 after shadow INSERT on all platforms (no physical INSERT occurs). On PostgreSQL PDO, `lastInsertId()` with sequence name throws `PDOException`.

#### Verification Matrix — MySQL (MySQLi)

| PHP | 5.6 | 5.7 | 8.0 | 8.4 | 9.1 |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | ✓   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

## SPEC-4.8 Transactions
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Scenarios/TransactionScenario` (all platforms), `Mysqli/TransactionTest`, `Mysqli/TransactionWithShadowTest`, `Mysqli/TransactionShadowInteractionTest`, `Pdo/MysqlTransactionTest`, `Pdo/PostgresTransactionTest`, `Pdo/SqliteTransactionTest`, `Pdo/TransactionTest`

Transaction control methods are delegated directly to the underlying connection. They do not affect the shadow store.

Shadow data remains visible after `commit()` or `rollBack()` because it is stored independently of the physical transaction state.

**Verified behavior:** Transaction-shadow independence — shadow store is completely independent of physical transaction state. Shadow data persists after rollBack(). commit() does not flush shadow data. Multiple transaction cycles accumulate shadow data.

#### Verification Matrix — MySQL (MySQLi, PDO)

| PHP | 5.6 | 5.7 | 8.0 | 8.4 | 9.1 |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | ✓   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

#### Verification Matrix — PostgreSQL (PDO)

| PHP | 14  | 15  | 16  | 17  | 18  |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | ✓   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

#### Verification Matrix — SQLite (PDO)

| PHP | 3.x |
|-----|-----|
| 8.1 | -   |
| 8.2 | -   |
| 8.3 | ✓   |
| 8.4 | -   |
| 8.5 | -   |

## SPEC-4.9 Utility Methods
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/DelegatedMethodsTest`, `Mysqli/StmtInitBypassTest`, `Mysqli/SelectDbInteractionTest`, `Pdo/MysqlUtilityMethodsTest`, `Pdo/PostgresUtilityMethodsTest`, `Pdo/SqliteUtilityMethodsTest`

`real_escape_string()` (mysqli) and `quote()` (PDO) are delegated to the underlying connection.

`multi_query()` bypasses ZTD entirely — queries operate directly on the physical database.

`select_db()` changes the physical connection's active database but does NOT update the ZTD session's schema reflection or shadow store.

`stmt_init()` returns a raw `mysqli_stmt` (NOT a `ZtdMysqliStatement`), bypassing ZTD entirely. Users should use `ZtdMysqli::prepare()` instead.

For PDO, `setAttribute()`, `getAttribute()`, `errorCode()`, `errorInfo()` are delegated.

For `ZtdPdoStatement`, `closeCursor()`, `setFetchMode()`, `bindColumn()`, `getColumnMeta()`, `errorCode()`, `errorInfo()`, `debugDumpParams()` are delegated.

**debugDumpParams() note**: Outputs the rewritten SQL, not the original user SQL.

#### Verification Matrix — MySQL (MySQLi, PDO)

| PHP | 5.6 | 5.7 | 8.0 | 8.4 | 9.1 |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | ✓   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

#### Verification Matrix — PostgreSQL (PDO)

| PHP | 14  | 15  | 16  | 17  | 18  |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | ✓   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

#### Verification Matrix — SQLite (PDO)

| PHP | 3.x |
|-----|-----|
| 8.1 | -   |
| 8.2 | -   |
| 8.3 | ✓   |
| 8.4 | -   |
| 8.5 | -   |

## SPEC-4.10 FETCH_CLASS / FETCH_INTO
**Status:** Verified
**Platforms:** MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Pdo/MysqlFetchClassTest`, `Pdo/PostgresFetchClassTest`, `Pdo/SqliteFetchClassTest`

`setFetchMode(PDO::FETCH_CLASS, ClassName)`, `fetchAll(PDO::FETCH_CLASS, ClassName)`, constructor args, `FETCH_PROPS_LATE`, and `FETCH_INTO` all work correctly with shadow store data.

#### Verification Matrix — MySQL (PDO)

| PHP | 5.6 | 5.7 | 8.0 | 8.4 | 9.1 |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | ✓   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

#### Verification Matrix — PostgreSQL (PDO)

| PHP | 14  | 15  | 16  | 17  | 18  |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | ✓   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

#### Verification Matrix — SQLite (PDO)

| PHP | 3.x |
|-----|-----|
| 8.1 | -   |
| 8.2 | -   |
| 8.3 | ✓   |
| 8.4 | -   |
| 8.5 | -   |

## SPEC-4.11 PDO Error Modes
**Status:** Verified
**Platforms:** MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Pdo/MysqlErrorModeInteractionTest`, `Pdo/PostgresErrorModeInteractionTest`, `Pdo/SqliteErrorModeInteractionTest`

When `ATTR_ERRMODE` is set to `ERRMODE_EXCEPTION`, invalid SQL shall throw `PDOException`.

When `ATTR_ERRMODE` is set to `ERRMODE_SILENT`, invalid SQL shall return `false` from `query()`.

When `ATTR_ERRMODE` is set to `ERRMODE_WARNING`, invalid SQL shall emit a PHP warning and return `false`.

Shadow store remains intact after errors in any mode. Switching error modes mid-session takes effect immediately.

#### Verification Matrix — MySQL (PDO)

| PHP | 5.6 | 5.7 | 8.0 | 8.4 | 9.1 |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | ✓   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

#### Verification Matrix — PostgreSQL (PDO)

| PHP | 14  | 15  | 16  | 17  | 18  |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | ✓   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

#### Verification Matrix — SQLite (PDO)

| PHP | 3.x |
|-----|-----|
| 8.1 | -   |
| 8.2 | -   |
| 8.3 | ✓   |
| 8.4 | -   |
| 8.5 | -   |

## SPEC-4.12 Statement Property Access (MySQLi)
**Status:** Verified (By-Design)
**Platforms:** MySQLi
**Tests:** `Mysqli/StatementIntrospectionTest`, `Mysqli/StatementMethodsTest`

Accessing `param_count` on `ZtdMysqliStatement` throws `Error` ("ZtdMysqliStatement object is already closed").

Use `ztdAffectedRows()` for affected row counts. `store_result()` works on prepared SELECT after `execute()`. `field_count` and `num_rows` work correctly on `mysqli_result` objects.

**Verified behavior:** MySQLi statement methods — `ztdAffectedRows()`, `get_result()` + `fetch_all()`, `bind_result()` + `fetch()`, `reset()`, `free_result()` all work correctly.

#### Verification Matrix — MySQL (MySQLi)

| PHP | 5.6 | 5.7 | 8.0 | 8.4 | 9.1 |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | ✓   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |
