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
**Status:** Pending Verification
**Platforms:** PostgreSQL-PDO
**Tests:** `Pdo/PostgresEnumTypeTest`

PostgreSQL native ENUM types (CREATE TYPE ... AS ENUM) work through ZTD shadow store. INSERT, UPDATE, WHERE comparison, NULL values, and prepared statements with ENUM parameters all function correctly. Cross-platform parity with MySQL column-level ENUM (MySQLi/EnumTypeTest, Pdo/MysqlEnumTypeTest).

## SPEC-10.2.20 Schema introspection queries
**Status:** Pending Verification
**Platforms:** MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Pdo/MysqlInformationSchemaTest`, `Pdo/PostgresInformationSchemaTest`, `Pdo/SqliteSchemaIntrospectionTest`

Schema introspection queries (INFORMATION_SCHEMA on MySQL/PostgreSQL, sqlite_master/PRAGMA on SQLite) may pass through or be treated as unsupported SQL depending on the adapter. These queries read physical database metadata, not shadow store state. Shadow operations should continue to work alongside schema queries.

## SPEC-10.2.21 Multi-tenant query patterns
**Status:** Pending Verification
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/MultiTenantPatternTest`, `Pdo/MysqlMultiTenantPatternTest`, `Pdo/PostgresMultiTenantPatternTest`, `Pdo/SqliteMultiTenantPatternTest`

Multi-tenant query patterns (WHERE tenant_id = ?) work correctly through ZTD shadow store for all CRUD operations. Tenant-filtered SELECT, INSERT, UPDATE, DELETE, JOINs across tenant-filtered tables, and per-tenant aggregation (GROUP BY tenant_id) all return correct results. Mutations for one tenant do not affect other tenants' data.
