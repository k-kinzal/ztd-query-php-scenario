# 7. Unknown Schema

## SPEC-7.1 Passthrough (default)
**Status:** Partially Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/UnknownSchemaTest`, `Pdo/MysqlUnknownSchemaTest`, `Pdo/PostgresUnknownSchemaTest`, `Pdo/SqliteUnknownSchemaTest`, `Pdo/UnknownSchemaTest`, `*UnknownSchemaWorkflowTest`

If `unknownSchemaBehavior` is `Passthrough` (the default) and a write operation references an unreflected table, the system shall pass the operation directly to the underlying connection.

**Platform inconsistency:** This passthrough behavior is verified on MySQL via `new ZtdMysqli(...)` and `new ZtdPdo(...)` constructors for operations on tables with no shadow data. On MySQL via `ZtdPdo::fromPdo()`, PostgreSQL, and SQLite, UPDATE on unreflected tables throws `RuntimeException` ("UPDATE simulation requires primary keys") instead of passing through. See [SPEC-11.UNKNOWN-UPDATE](11-known-issues.ears.md).

**Post-INSERT behavior:** After INSERT into an unreflected table, the shadow store registers the table. Subsequent DELETE operates on shadow data (not passthrough to physical DB). UPDATE still throws `RuntimeException` ("UPDATE simulation requires primary keys") because INSERT does not capture PK metadata. This behavior is consistent across all platforms and all behavior modes.

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

## SPEC-7.2 Exception
**Status:** Partially Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/UnknownSchemaTest`, `Pdo/MysqlUnknownSchemaTest`, `Pdo/PostgresUnknownSchemaTest`, `Pdo/SqliteUnknownSchemaTest`, `*UnknownSchemaWorkflowTest`

If `unknownSchemaBehavior` is `Exception`, write operations on unreflected tables shall throw `ZtdMysqliException`/`ZtdPdoException` ("Unknown table").

**Platform inconsistency:** On PostgreSQL and SQLite, UPDATE throws `RuntimeException` ("UPDATE simulation requires primary keys") instead.

**Post-INSERT behavior:** After INSERT, DELETE operates on shadow data instead of throwing "Unknown table", because the INSERT registered the table in the shadow store.

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

## SPEC-7.3 EmptyResult
**Status:** Partially Verified
**Platforms:** MySQL-PDO (full); PostgreSQL-PDO, SQLite-PDO (DELETE only)
**Tests:** `Pdo/MysqlUnknownSchemaTest`, `Pdo/PostgresUnknownSchemaTest`, `Pdo/SqliteUnknownSchemaTest`, `*UnknownSchemaWorkflowTest`

If `unknownSchemaBehavior` is `EmptyResult`, write operations shall return an empty result without modifying the physical database.

**Platform inconsistency:** On PostgreSQL and SQLite, UPDATE throws `RuntimeException` regardless of EmptyResult mode.

#### Verification Matrix — MySQL (PDO)

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

## SPEC-7.4 Notice
**Status:** Partially Verified
**Platforms:** MySQLi, MySQL-PDO (full); PostgreSQL-PDO, SQLite-PDO (DELETE only)
**Tests:** `Mysqli/UnknownSchemaTest`, `Pdo/MysqlUnknownSchemaTest`, `Pdo/PostgresUnknownSchemaTest`, `Pdo/SqliteUnknownSchemaTest`, `*UnknownSchemaWorkflowTest`

If `unknownSchemaBehavior` is `Notice`, write operations shall emit a user notice/warning and return an empty result.

**Platform inconsistency:** On PostgreSQL and SQLite, UPDATE throws `RuntimeException` regardless of Notice mode.

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
