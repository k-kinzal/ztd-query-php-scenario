# 8. Constraint Enforcement

## SPEC-8.1 Shadow Store Constraints
**Status:** Verified (By-Design)
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tested versions:** ztd-query-mysqli-adapter v0.1.1, ztd-query-pdo-adapter v0.1.1, MySQL 8.0, PostgreSQL 16, SQLite 3.x, PHP 8.3
**Tests:** `Mysqli/ConstraintBehaviorTest`, `Pdo/MysqlConstraintBehaviorTest`, `Pdo/PostgresConstraintBehaviorTest`, `Pdo/SqliteConstraintBehaviorTest`, `Pdo/ConstraintBehaviorTest`

The shadow store does NOT enforce database constraints:
- **PRIMARY KEY**: Duplicate primary key values are accepted.
- **UNIQUE**: Duplicate values in unique columns are accepted.
- **NOT NULL**: NULL values are accepted even for NOT NULL columns.
- **FOREIGN KEY**: References to non-existent parent rows are accepted.
- **DEFAULT**: Column default values are NOT applied. When INSERT omits columns with DEFAULT values, the shadow store inserts NULL (not the default).
- **CHECK**: CHECK constraints are NOT triggered (ZTD rewrites to CTE-based operations).
- **GENERATED COLUMNS**: Generated column values are typically NULL in shadow queries.

This is by design — the shadow store is an in-memory simulation layer.

## SPEC-8.2 Error Recovery
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tested versions:** ztd-query-mysqli-adapter v0.1.1, ztd-query-pdo-adapter v0.1.1, MySQL 8.0, PostgreSQL 16, SQLite 3.x, PHP 8.3
**Tests:** `Mysqli/ErrorRecoveryTest`, `Pdo/MysqlErrorRecoveryTest`, `Pdo/PostgresErrorRecoveryTest`, `Pdo/SqliteErrorRecoveryTest`, `Pdo/ErrorRecoveryTest`, `Pdo/MysqlErrorBoundaryTest`, `Pdo/PostgresErrorBoundaryTest`, `Pdo/SqliteErrorBoundaryTest`

### SPEC-8.2a Transformer Errors
When malformed SQL is executed with ZTD enabled, the ztd-query transformer may throw `RuntimeException` before the query reaches the database engine.

### SPEC-8.2b Shadow Store Consistency After Errors
When a SQL error occurs, the shadow store shall remain consistent. Previously inserted/updated/deleted shadow data is not rolled back or corrupted by a subsequent error. Subsequent valid operations after an error shall execute correctly against the intact shadow store.

**Verified behavior:** Multiple consecutive errors do not compound. Error code cleared after successful recovery. Mid-workflow error recovery maintains shadow consistency.

## SPEC-8.3 Trigger Behavior
**Status:** Pending Verification
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/TriggerInteractionTest`, `Pdo/MysqlTriggerInteractionTest`, `Pdo/PostgresTriggerInteractionTest`, `Pdo/SqliteTriggerInteractionTest`

Database triggers (BEFORE/AFTER INSERT/UPDATE/DELETE) do NOT fire for shadow operations. Since ZTD rewrites DML to CTE-based simulations, no actual INSERT/UPDATE/DELETE reaches the physical table, and therefore triggers defined on those tables are not executed.

This applies to all trigger types:
- **AFTER INSERT**: Not fired when shadow INSERT occurs.
- **AFTER UPDATE**: Not fired when shadow UPDATE occurs.
- **AFTER DELETE**: Not fired when shadow DELETE occurs.
- **BEFORE** triggers: Same behavior — not fired.

The presence of triggers on a table does NOT interfere with shadow CRUD operations — INSERT, UPDATE, DELETE, and SELECT all work correctly through ZTD even when triggers are defined on the physical table.

This is by design — the shadow store operates at the SQL rewrite level, not the storage engine level.
