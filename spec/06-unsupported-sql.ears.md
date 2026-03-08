# 6. Unsupported SQL

## SPEC-6.1 Default Behavior
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tested versions:** ztd-query-mysqli-adapter v0.1.1, ztd-query-pdo-adapter v0.1.1, MySQL 8.0, PostgreSQL 16, SQLite 3.x, PHP 8.3
**Tests:** `Mysqli/UnsupportedSqlTest`, `Pdo/MysqlUnsupportedSqlTest`, `Pdo/PostgresUnsupportedSqlTest`, `Pdo/SqliteUnsupportedSqlTest`, `Pdo/UnsupportedSqlTest`

If unsupported SQL is executed and `unsupportedBehavior` is `Exception` (the default), the system shall throw a `ZtdMysqliException` or `ZtdPdoException`.

If `unsupportedBehavior` is `Ignore`, the system shall silently skip the statement and return `false` (mysqli) or `0` (PDO exec).

If `unsupportedBehavior` is `Notice`, the system shall emit a user notice/warning and skip the statement.

Platform-specific examples: MySQL uses `SET @var = 1`, PostgreSQL uses `SET search_path TO public`, SQLite uses `PRAGMA journal_mode=WAL`.

## SPEC-6.2 Behavior Rules
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tested versions:** ztd-query-mysqli-adapter v0.1.1, ztd-query-pdo-adapter v0.1.1, MySQL 8.0, PostgreSQL 16, SQLite 3.x, PHP 8.3
**Tests:** `Mysqli/BehaviorRuleConfigTest`, `Mysqli/BehaviorRuleCombinationsTest`, `Pdo/BehaviorRuleCombinationsTest`, `Pdo/MysqlBehaviorRuleRegexTest`

When `behaviorRules` are configured in `ZtdConfig`, the system shall apply the first matching rule's behavior for unsupported SQL.

Rules support two pattern types:
- Prefix match (case-insensitive): e.g., `'SET'` matches any SQL starting with "SET".
- Regex match: e.g., `'/^SET\s+/i'` matches SQL matching the regex.

**Verified behavior:** Rule ordering is critical — first match wins. Broad prefix rules should be placed AFTER specific rules.

## SPEC-6.3 Transaction Statements
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tested versions:** ztd-query-mysqli-adapter v0.1.1, ztd-query-pdo-adapter v0.1.1, MySQL 8.0, PostgreSQL 16, SQLite 3.x, PHP 8.3
**Tests:** `Mysqli/SavepointTest`, `Mysqli/SavepointBehaviorTest`, `Pdo/MysqlSavepointTest`, `Pdo/MysqlSavepointBehaviorTest`, `Pdo/PostgresSavepointTest`, `Pdo/PostgresSavepointBehaviorTest`, `Pdo/SqliteSavepointTest`, `Pdo/SqliteSavepointBehaviorTest`

Transaction control statements (BEGIN, COMMIT, ROLLBACK) are not rewritten and shall be delegated directly to the underlying connection.

SAVEPOINT, RELEASE SAVEPOINT, and ROLLBACK TO SAVEPOINT are NOT supported:
- **SQLite**: All three throw `UnsupportedSqlException`.
- **MySQL**: Throw "Empty or unparseable SQL statement" / "Statement type not supported."
- **PostgreSQL**: Silently pass through, but shadow store does NOT participate in savepoint semantics.

For `ZtdMysqli`, use dedicated methods (`begin_transaction()`, `commit()`, `rollback()`) rather than SQL strings.
