# 1. Connection

## SPEC-1.1 New Connection (mysqli)
**Status:** Verified
**Platforms:** MySQLi
**Tested versions:** ztd-query-mysqli-adapter v0.1.1, MySQL 8.0, PHP 8.3
**Tests:** `Mysqli/BasicCrudTest` (via BasicCrudScenario), `Mysqli/FromMysqliTest`

When a user creates a new `ZtdMysqli` instance with valid connection parameters, the system shall establish a connection and enable ZTD mode by default.

## SPEC-1.2 New Connection (PDO)
**Status:** Verified
**Platforms:** MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tested versions:** ztd-query-pdo-adapter v0.1.1, MySQL 8.0, PostgreSQL 16, SQLite 3.x, PHP 8.3
**Tests:** `Pdo/MysqlBasicCrudTest`, `Pdo/PostgresBasicCrudTest`, `Pdo/SqliteBasicCrudTest`

When a user creates a new `ZtdPdo` instance with a valid DSN and credentials, the system shall establish a connection and enable ZTD mode by default.

## SPEC-1.3 Wrap Existing Connection (mysqli)
**Status:** Verified
**Platforms:** MySQLi
**Tested versions:** ztd-query-mysqli-adapter v0.1.1, MySQL 8.0, PHP 8.3
**Tests:** `Mysqli/FromMysqliTest`

When a user calls `ZtdMysqli::fromMysqli()` with an existing `mysqli` instance, the system shall create a ZTD-enabled wrapper that delegates to the inner connection.

## SPEC-1.4 Wrap Existing Connection (PDO)
**Status:** Verified
**Platforms:** MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tested versions:** ztd-query-pdo-adapter v0.1.1, MySQL 8.0, PostgreSQL 16, SQLite 3.x, PHP 8.3
**Tests:** `Pdo/MysqlFromPdoBehaviorTest`, `Pdo/PostgresFromPdoBehaviorTest`, `Pdo/SqliteFromPdoBehaviorTest`

When a user calls `ZtdPdo::fromPdo()` with an existing `PDO` instance, the system shall create a ZTD-enabled wrapper that delegates to the inner connection.

## SPEC-1.4a Static Factory (PDO)
**Status:** Verified
**Platforms:** MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tested versions:** ztd-query-pdo-adapter v0.1.1, MySQL 8.0, PostgreSQL 16, SQLite 3.x, PHP 8.3
**Tests:** `Pdo/MysqlConnectFactoryTest`, `Pdo/PostgresConnectFactoryTest`, `Pdo/SqliteConnectFactoryTest`

When a user calls `ZtdPdo::connect()` with a valid DSN and credentials, the system shall create a ZTD-enabled wrapper equivalent to `ZtdPdo::fromPdo(PDO::connect(...))`.

**Note:** Requires PHP 8.4+.

## SPEC-1.5 Auto-detection (PDO)
**Status:** Verified
**Platforms:** MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tested versions:** ztd-query-pdo-adapter v0.1.1, MySQL 8.0, PostgreSQL 16, SQLite 3.x, PHP 8.3
**Tests:** `Pdo/AutoDetectionTest`, `Pdo/BasicCrudTest`

When a user creates a `ZtdPdo` without specifying a `SessionFactory`, the system shall detect the appropriate factory from the PDO driver name (mysql, pgsql, sqlite).

If the PDO driver is not supported, the system shall throw a `RuntimeException`.

If the required platform package is not installed, the system shall throw a `RuntimeException` with installation instructions.

## SPEC-1.6 Schema Reflection
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tested versions:** ztd-query-mysqli-adapter v0.1.1, ztd-query-pdo-adapter v0.1.1, MySQL 8.0, PostgreSQL 16, SQLite 3.x, PHP 8.3
**Tests:** `Mysqli/SchemaReflectionTest`, `Pdo/MysqlSchemaReflectionTest`, `Pdo/PostgresSchemaReflectionTest`, `Pdo/SqliteSchemaReflectionTest`, `Pdo/SchemaReflectionTest`

When a ZTD adapter is constructed, the system shall reflect all existing table schemas from the physical database via `SchemaReflector::reflectAll()`.

The ZTD adapter MUST be constructed AFTER the physical tables exist; otherwise, schema-dependent operations (UPDATE, DELETE) will fail with `RuntimeException` ("UPDATE simulation requires primary keys" / "DELETE simulation requires primary keys").

INSERT and SELECT operations on unreflected tables may still work, as they do not require primary key information.

## SPEC-1.7 Supported Platforms
**Status:** Verified
**Platforms:** MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tested versions:** ztd-query-pdo-adapter v0.1.1, PHP 8.3
**Tests:** `Pdo/AutoDetectionTest`

The PDO adapter supports the following platforms via auto-detection:
- MySQL (driver: `mysql`, package: `k-kinzal/ztd-query-mysql`)
- PostgreSQL (driver: `pgsql`, package: `k-kinzal/ztd-query-postgres`)
- SQLite (driver: `sqlite`, package: `k-kinzal/ztd-query-sqlite`)
