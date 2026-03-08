# 2. ZTD Mode

## SPEC-2.1 Enable/Disable
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tested versions:** ztd-query-mysqli-adapter v0.1.1, ztd-query-pdo-adapter v0.1.1, MySQL 8.0, PostgreSQL 16, SQLite 3.x, PHP 8.3
**Tests:** `Scenarios/BasicCrudScenario::testEnableDisableToggle` (all platforms), `Mysqli/ZtdLifecycleTest`, `Pdo/MysqlZtdLifecycleTest`, `Pdo/PostgresZtdLifecycleTest`, `Pdo/SqliteZtdLifecycleTest`

When ZTD mode is enabled, the system shall rewrite SQL queries using CTE (Common Table Expression) shadowing.

When ZTD mode is disabled, the system shall pass queries directly to the underlying connection without rewriting.

**Verified behavior:** Shadow data persists across enable/disable toggle cycles. Physical data inserted while ZTD is off is not visible when ZTD is re-enabled (shadow replaces physical). Multiple toggle cycles correctly accumulate shadow data.

## SPEC-2.2 Isolation
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tested versions:** ztd-query-mysqli-adapter v0.1.1, ztd-query-pdo-adapter v0.1.1, MySQL 8.0, PostgreSQL 16, SQLite 3.x, PHP 8.3
**Tests:** `Scenarios/BasicCrudScenario::testZtdIsolation` (all platforms), `Mysqli/PhysicalShadowOverlayTest`, `Pdo/MysqlPhysicalShadowOverlayTest`, `Pdo/PostgresPhysicalShadowOverlayTest`, `Pdo/SqlitePhysicalShadowOverlayTest`

While ZTD mode is enabled, all write operations (INSERT, UPDATE, DELETE) shall be tracked in an in-memory shadow store and shall NOT modify the physical database.

While ZTD mode is enabled, SELECT queries on **reflected** tables shall read from the shadow store via CTE rewriting. The shadow store replaces the physical table entirely; data present only in the physical table is NOT visible through ZTD-enabled SELECT queries. When ZTD mode is disabled, SELECT queries read directly from the physical table.

**Note:** SELECT queries on unreflected tables, views, and derived table subqueries may pass through to the physical database (see [SPEC-3.3a](03-read-operations.ears.md), [SPEC-3.3b](03-read-operations.ears.md)).

**Verified behavior:** Physical data replacement (not overlay) — when a table has pre-existing physical data, the CTE shadow REPLACES the physical table entirely. Physical data is NOT visible through ZTD queries. Concurrent ZTD instances maintain fully independent shadow stores.

## SPEC-2.3 Toggle
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tested versions:** ztd-query-mysqli-adapter v0.1.1, ztd-query-pdo-adapter v0.1.1, MySQL 8.0, PostgreSQL 16, SQLite 3.x, PHP 8.3
**Tests:** `Scenarios/BasicCrudScenario::testEnableDisableToggle` (all platforms)

The system shall provide `enableZtd()`, `disableZtd()`, and `isZtdEnabled()` methods to control and inspect ZTD mode.

**Verified behavior:** ZTD toggle error resilience — errors during ZTD-enabled or ZTD-disabled operations do not corrupt the shadow store. Shadow data persists through toggle cycles even after errors.

## SPEC-2.4 Session State
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tested versions:** ztd-query-mysqli-adapter v0.1.1, ztd-query-pdo-adapter v0.1.1, MySQL 8.0, PostgreSQL 16, SQLite 3.x, PHP 8.3
**Tests:** `Mysqli/SessionIsolationTest`, `Mysqli/ConcurrentInstancesTest`, `Pdo/MysqlSessionIsolationTest`, `Pdo/MysqlConcurrentInstancesTest`, `Pdo/PostgresSessionIsolationTest`, `Pdo/PostgresConcurrentInstancesTest`, `Pdo/SqliteSessionIsolationTest`, `Pdo/SqliteConcurrentInstancesTest`, `Pdo/SessionIsolationTest`

Each ZTD adapter instance maintains its own session state. Shadow data is not shared between instances and is not persisted across instance lifecycle.

**Verified behavior:** Multiple ZtdPdo/ZtdMysqli instances connected to the same physical database maintain fully independent shadow stores. Interleaved INSERT/UPDATE/DELETE operations are isolated.
