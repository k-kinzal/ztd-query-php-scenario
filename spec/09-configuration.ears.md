# 9. Configuration

## SPEC-9.1 ZtdConfig
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tested versions:** ztd-query-mysqli-adapter v0.1.1, ztd-query-pdo-adapter v0.1.1, MySQL 8.0, PostgreSQL 16, SQLite 3.x, PHP 8.3
**Tests:** `Mysqli/ConfigurationTest`, `Pdo/MysqlConfigurationTest`, `Pdo/PostgresConfigurationTest`, `Pdo/SqliteConfigurationTest`, `Pdo/ConfigurationTest`

The `ZtdConfig` class accepts three parameters:
- `unsupportedBehavior` (`UnsupportedSqlBehavior`): Default `Exception`. Controls handling of unsupported SQL.
- `unknownSchemaBehavior` (`UnknownSchemaBehavior`): Default `Passthrough`. Controls handling of queries on unreflected tables.
- `behaviorRules` (`array<string, UnsupportedSqlBehavior>`): Pattern-to-behavior mapping for fine-grained unsupported SQL control.

## SPEC-9.2 Default Configuration
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tested versions:** ztd-query-mysqli-adapter v0.1.1, ztd-query-pdo-adapter v0.1.1, MySQL 8.0, PostgreSQL 16, SQLite 3.x, PHP 8.3
**Tests:** `Mysqli/ConfigurationTest`, `Pdo/MysqlConfigurationTest`, `Pdo/PostgresConfigurationTest`, `Pdo/SqliteConfigurationTest`

`ZtdConfig::default()` creates a config with `Exception` unsupported behavior and `Passthrough` unknown schema behavior.
