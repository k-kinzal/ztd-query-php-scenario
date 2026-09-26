# 9. Configuration

> **Baseline:** Statuses, observations, and verification marks below were recorded against v0.1.1. The matrix columns follow the current support range; these historical results have not all been revalidated against `dev-main`. See the [current baseline and support policy](00-index.ears.md).

## SPEC-9.1 ZtdConfig
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/ConfigurationTest`, `Pdo/MysqlConfigurationTest`, `Pdo/PostgresConfigurationTest`, `Pdo/SqliteConfigurationTest`, `Pdo/ConfigurationTest`

The `ZtdConfig` class accepts three parameters:
- `unsupportedBehavior` (`UnsupportedSqlBehavior`): Default `Exception`. Controls handling of unsupported SQL.
- `unknownSchemaBehavior` (`UnknownSchemaBehavior`): Default `Passthrough`. Controls handling of queries on unreflected tables.
- `behaviorRules` (`array<string, UnsupportedSqlBehavior>`): Pattern-to-behavior mapping for fine-grained unsupported SQL control.

#### Verification Matrix — MySQL (MySQLi, PDO)

| PHP | 8.0 | 8.4 | 9.1 |
|-----|-----|-----|-----|
| 8.1 | -   | -   | -   |
| 8.2 | -   | -   | -   |
| 8.3 | ✓   | -   | -   |
| 8.4 | -   | -   | -   |
| 8.5 | -   | -   | -   |

#### Verification Matrix — PostgreSQL (PDO)

| PHP | 16  | 17  |
|-----|-----|-----|
| 8.1 | -   | -   |
| 8.2 | -   | -   |
| 8.3 | ✓   | -   |
| 8.4 | -   | -   |
| 8.5 | -   | -   |

#### Verification Matrix — SQLite (PDO)

| PHP | 3.x |
|-----|-----|
| 8.1 | -   |
| 8.2 | -   |
| 8.3 | ✓   |
| 8.4 | -   |
| 8.5 | -   |

## SPEC-9.2 Default Configuration
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/ConfigurationTest`, `Pdo/MysqlConfigurationTest`, `Pdo/PostgresConfigurationTest`, `Pdo/SqliteConfigurationTest`

`ZtdConfig::default()` creates a config with `Exception` unsupported behavior and `Passthrough` unknown schema behavior.

#### Verification Matrix — MySQL (MySQLi, PDO)

| PHP | 8.0 | 8.4 | 9.1 |
|-----|-----|-----|-----|
| 8.1 | -   | -   | -   |
| 8.2 | -   | -   | -   |
| 8.3 | ✓   | -   | -   |
| 8.4 | -   | -   | -   |
| 8.5 | -   | -   | -   |

#### Verification Matrix — PostgreSQL (PDO)

| PHP | 16  | 17  |
|-----|-----|-----|
| 8.1 | -   | -   |
| 8.2 | -   | -   |
| 8.3 | ✓   | -   |
| 8.4 | -   | -   |
| 8.5 | -   | -   |

#### Verification Matrix — SQLite (PDO)

| PHP | 3.x |
|-----|-----|
| 8.1 | -   |
| 8.2 | -   |
| 8.3 | ✓   |
| 8.4 | -   |
| 8.5 | -   |
