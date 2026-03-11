# 6. Unsupported SQL

## SPEC-6.1 Default Behavior
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/UnsupportedSqlTest`, `Pdo/MysqlUnsupportedSqlTest`, `Pdo/PostgresUnsupportedSqlTest`, `Pdo/SqliteUnsupportedSqlTest`, `Pdo/UnsupportedSqlTest`

If unsupported SQL is executed and `unsupportedBehavior` is `Exception` (the default), the system shall throw a `ZtdMysqliException` or `ZtdPdoException`.

If `unsupportedBehavior` is `Ignore`, the system shall silently skip the statement and return `false` (mysqli) or `0` (PDO exec).

If `unsupportedBehavior` is `Notice`, the system shall emit a user notice/warning and skip the statement.

Platform-specific examples: MySQL uses `SET @var = 1`, PostgreSQL uses `SET search_path TO public`, SQLite uses `PRAGMA journal_mode=WAL`.

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

## SPEC-6.2 Behavior Rules
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/BehaviorRuleConfigTest`, `Mysqli/BehaviorRuleCombinationsTest`, `Pdo/BehaviorRuleCombinationsTest`, `Pdo/MysqlBehaviorRuleRegexTest`

When `behaviorRules` are configured in `ZtdConfig`, the system shall apply the first matching rule's behavior for unsupported SQL.

Rules support two pattern types:
- Prefix match (case-insensitive): e.g., `'SET'` matches any SQL starting with "SET".
- Regex match: e.g., `'/^SET\s+/i'` matches SQL matching the regex.

**Verified behavior:** Rule ordering is critical — first match wins. Broad prefix rules should be placed AFTER specific rules.

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

## SPEC-6.3 Transaction Statements
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/SavepointTest`, `Mysqli/SavepointBehaviorTest`, `Pdo/MysqlSavepointTest`, `Pdo/MysqlSavepointBehaviorTest`, `Pdo/PostgresSavepointTest`, `Pdo/PostgresSavepointBehaviorTest`, `Pdo/SqliteSavepointTest`, `Pdo/SqliteSavepointBehaviorTest`

Transaction control statements (BEGIN, COMMIT, ROLLBACK) are not rewritten and shall be delegated directly to the underlying connection.

SAVEPOINT, RELEASE SAVEPOINT, and ROLLBACK TO SAVEPOINT are NOT supported:
- **SQLite**: All three throw `UnsupportedSqlException`.
- **MySQL**: Throw "Empty or unparseable SQL statement" / "Statement type not supported."
- **PostgreSQL**: Silently pass through, but shadow store does NOT participate in savepoint semantics.

For `ZtdMysqli`, use dedicated methods (`begin_transaction()`, `commit()`, `rollback()`) rather than SQL strings.

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

## SPEC-6.4 EXPLAIN Statements
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/ExplainQueryTest`, `Pdo/MysqlExplainQueryTest`, `Pdo/PostgresExplainQueryTest`, `Pdo/SqliteExplainPragmaTest`

EXPLAIN is a utility statement used for performance debugging. Behavior through ZTD varies by platform — EXPLAIN may execute against the CTE-rewritten query or may throw an unsupported SQL exception. Both outcomes are acceptable.

- **MySQL (MySQLi and PDO)**: `EXPLAIN SELECT`, `EXPLAIN UPDATE`, `EXPLAIN DELETE`, and `EXPLAIN FORMAT=JSON` execute or throw. When they execute, the query plan reflects the CTE-rewritten query structure.
- **PostgreSQL**: `EXPLAIN`, `EXPLAIN ANALYZE`, `EXPLAIN (FORMAT JSON)`, `EXPLAIN (COSTS OFF)` execute or throw. When they execute, plan output reflects CTE rewriting.
- **SQLite**: `EXPLAIN QUERY PLAN` and `EXPLAIN` execute or throw. `PRAGMA table_info()`, `PRAGMA foreign_keys`, `PRAGMA journal_mode` may execute or throw depending on the adapter.

**Verified behavior:** EXPLAIN does not modify shadow state. Shadow operations (INSERT, SELECT, UPDATE, DELETE) continue to work correctly after an EXPLAIN attempt, whether it succeeds or throws. EXPLAIN does not corrupt the shadow store.

#### Verification Matrix — MySQL (MySQLi, PDO)

| PHP | 5.6 | 5.7 | 8.0 | 8.4 | 9.1 |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | -   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | ✓   | -   | -   |

#### Verification Matrix — PostgreSQL (PDO)

| PHP | 14  | 15  | 16  | 17  | 18  |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | -   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | ✓   | -   | -   |

#### Verification Matrix — SQLite (PDO)

| PHP | 3.x |
|-----|-----|
| 8.1 | -   |
| 8.2 | -   |
| 8.3 | -   |
| 8.4 | -   |
| 8.5 | ✓   |

## SPEC-6.5 CALL Stored Procedures
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO
**Tests:** `Mysqli/StoredProcedureTest`, `Pdo/MysqlStoredProcedureTest`

`CALL procedure_name(args)` is MySQL-specific syntax for invoking stored procedures. CALL statements are treated as unsupported SQL by the ZTD adapter — they throw an exception (default behavior) or are handled by behavior rules.

- Stored procedures that perform SELECT internally read from the physical table, not the shadow store.
- Stored procedures that perform INSERT/UPDATE/DELETE modify the physical table, bypassing the shadow store.

**Verified behavior:** Shadow operations continue to work correctly after a CALL attempt (whether it succeeds or throws). The shadow store is not corrupted by a failed CALL attempt. PostgreSQL does not use CALL for function invocation (uses `SELECT func()` instead, which is covered by [SPEC-3.3g](#spec-33g-user-defined-functions-in-queries)).

#### Verification Matrix — MySQL (MySQLi, PDO)

| PHP | 5.6 | 5.7 | 8.0 | 8.4 | 9.1 |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | -   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | ✓   | -   | -   |
