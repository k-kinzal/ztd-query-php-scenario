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
